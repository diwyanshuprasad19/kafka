# Cafeteria Checkpoint Aggregation Platform

Kafka + Flask + PostgreSQL/AlloyDB. Same code for **local** and **GCP**.

**Prod scale target: 50,000 events / minute** (≈ 833 / second).

## Quick local

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp configs/local.env .env
docker compose up -d --build
make bootstrap && make verify
make chaos
curl -s http://localhost:8080/ops/status | python -m json.tool
```

| URL | |
|-----|--|
| API | http://localhost:8080 |
| Ops status | http://localhost:8080/ops/status |
| Metrics | http://localhost:8080/metrics |
| Grafana | http://localhost:3000 — dashboard **Checkpoint Aggregation Ops (50k/min)** |

## Kafka without Docker

Docker is optional. A single-node KRaft broker (Kafka 4.3) can run natively:

```bash
brew install kafka
./scripts/kafka_local.sh start     # formats on first run; config in infra/kafka/
python scripts/create_topics.py
./scripts/kafka_local.sh status
./scripts/kafka_local.sh stop
```

## Prod scale (50k/min)

| Knob | Value |
|------|-------|
| Target | **50,000 events/minute** |
| Partitions | **12** |
| Consumers | 3 (scale toward 12) |
| Mode | `KAFKA_THROUGHPUT_MODE=high` |
| Logging | JSON stdout + `correlation_id` / `request_id` |
| Metrics | Prometheus + Grafana ops dashboard |

```bash
make plan50k
make load50k-dry
# with stack up:
make load50k          # 50k events @ 50k/min
make ramp             # 5k → 15k → 30k → 50k /min
```

### Measured throughput

Measured on one laptop (M-series, single-broker KRaft, local PostgreSQL). Both
methods agree that a consumer process saturates at roughly 500 events/sec, so the
bottleneck is the per-event database work rather than Kafka.

| Consumers | Sustained | Per minute | Behaviour past that point |
|-----------|-----------|------------|---------------------------|
| 1 | 500 ev/s | 30,000 | at 1,000 ev/s lag reaches ~5,000 and p95 latency ~9.9 s |
| 3 | 2,000 ev/s | 120,000 | backlog still drains; ~2.4x the 50k/min target |

```bash
# Step the rate up and watch lag, latency and DLQ at each level.
python scripts/load_ladder.py --rates 100 250 500 1000 2000 --consumers 3
python scripts/load_ladder.py --rates 250 500 1000 --consumers 1

# Ingest path against PostgreSQL only, no Kafka in the way.
python scripts/bench_ingest.py --events 15000              # one process
python scripts/bench_ingest.py --events 30000 --instances 3 # as deployed
```

"Keeping up" is judged on consumer lag, not on consumed-per-second: consumer
throughput can never exceed the offered rate, so comparing the two proves nothing.

## Failure scenarios through real Kafka

`scripts/e2e_kafka.py` produces through the broker, waits for the group to commit,
then asserts the database. Every event is worth exactly 1 kg, so an aggregate that
reads back exactly N proves no loss and no double counting.

```bash
python scripts/e2e_kafka.py            # all five
python scripts/e2e_kafka.py --list
python scripts/e2e_kafka.py --only ordering dlq
```

| Scenario | What it does | Assertion |
|----------|--------------|-----------|
| `ordering` | 20 kg → 15 kg, then a duplicate and a stale v1 replay | aggregate = 15 kg |
| `dlq` | unparseable bytes, missing `cafe_id`, invalid domain | ≥3 DLQ rows, aggregate untouched |
| `rebalance` | SIGTERM one of three consumers mid-load | 3000/3000 stored, aggregate = 3000 |
| `crash` | SIGKILL a consumer so offsets are never committed | 2000/2000 stored, aggregate = 2000 |
| `restart` | stop the group, produce into the gap, restart | backlog waits, then 1000/1000 stored |

## Aggregate grains

Counter level is maintained synchronously in the consumer's transaction. Cafe and
client level are restated from it by a worker, because a cafe row is shared by every
counter beneath it and a client row by every cafe — updating those inline would
serialise concurrent consumers on a few hot rows.

```bash
python -m checkpoint_platform.interfaces.workers.rollup --once
python -m checkpoint_platform.interfaces.workers.rollup   # loop
```

The rollup restates rather than accumulates, so re-running it is a no-op. Cafe and
client reads fall back to a live scan of the counter grain when the worker has not
caught up, so correctness never depends on the worker having run; responses carry
`source` (`rollup` or `counter_scan`) and `rollup_age_seconds`.

## Working data + ops

- `make seed` / `make bootstrap` — demo cafeteria aggregates
- `GET /ops/status` — deps, scale plan, DLQ pending, logging contract, metrics list
- `GET /aggregations/counter/...` — Redis-cached reads
- Centralized logs: structlog JSON with `service`, `env`, `correlation_id`

## Query + history (prod read APIs)

| Endpoint | Purpose |
|----------|---------|
| `GET /aggregations/counter\|cafe\|client/...` | Current day aggregates |
| `GET /history/aggregations/counter\|cafe\|client/...?from_date=&to_date=` | Date-range aggregate history |
| `GET /history/events/checkpoint/<id>` | Version history for one checkpoint |
| `GET /history/events/counter\|cafe/<id>` | Event audit trail (+ filters) |
| `GET /checkpoints/state/<id>` | Current checkpoint_state |
| `GET /checkpoints/state/counter/<id>` | All current states for a counter |
| `GET /audit/processed/<event_id>` | Idempotency lookup |
| `GET /reporting/counter/<id>` | Downstream reporting snapshot |
| `GET /dlq` · `GET /dlq/<id>` | DLQ inspect |
| `GET /ops/status` · `/ops/outbox` | Ops + outbox backlog |

History is append-only in `checkpoint_history` on every successful apply.

## Bad scenarios (33)

`make chaos-list` · `make chaos` — see `bad_scenarios.py`

## Local vs GCP

Same code, same image. `APP_ENV=local` vs `prod`/`gcp` (`production` and `dev`/`docker`
are accepted aliases). Every deployment-specific value — brokers, credentials,
database, Redis, partition counts, timezone — comes from the environment;
`configs/{local,prod}.env` then `.env` then real env vars, later winning.
Nothing about a target is baked into the build.

### Prove a config before deploying with it

```bash
APP_ENV=prod python scripts/preflight.py
```

It resolves the settings the process would actually use, then connects to
PostgreSQL, Redis and Kafka for real. It exits non-zero on anything blocking, so
it works as a deploy gate or a readiness step. Under production rules it refuses
to pass while a shipped localhost default is still in place, a `CHANGE_ME`
placeholder survives in a credential, Kafka is on `PLAINTEXT`, SASL is selected
without credentials, `BUSINESS_TIMEZONE` is not a real zone, or the schema is
behind the migration head. Unencrypted Redis, missing `sslmode` and replication
factor below 3 are warnings rather than blocks. Redis being unreachable is also
only a warning, because it is a cache and reads fall back to PostgreSQL.

CI runs it both ways: green against the local stack, and asserting it *fails* on
a localhost config under `APP_ENV=prod`, so the gate can't quietly stop gating.

### What prod needs beyond local

Fill `configs/prod.env` from `configs/prod.env.example`, which documents each
value and why it matters. The parts that are genuinely different:

| | Local | Production |
|---|---|---|
| Kafka auth | `PLAINTEXT` | `SASL_SSL` + mechanism, username, password |
| Replication | 1 | 3, and topics get `min.insync.replicas=2` |
| Database | local PostgreSQL | AlloyDB via Auth Proxy or private IP + TLS |
| WSGI | Flask dev server | gunicorn (already the Dockerfile `CMD`) |
| Migrations | falls back to `create_all` | fails the rollout instead |

Two durability details are handled in code rather than left to whoever writes the
config. Topics are created with `min.insync.replicas=2` whenever replication
factor exceeds 1, because `acks=all` means "all *in-sync* replicas" and Kafka's
default of 1 would let an acknowledged write live on a single broker. And the
entrypoint refuses to fall back to `create_all` in prod: that fallback builds the
schema from the models while leaving `alembic_version` behind, so a failed
migration would look like a successful deploy and skip any data migration it
contained. Locally the fallback is still there for convenience.

Migrations also take a PostgreSQL advisory lock, since replicas start together
and each runs `alembic upgrade head`; without it they race on the same DDL.
Verified with three concurrent `alembic upgrade head` runs against a fresh
database — all succeed and the schema lands at head exactly once.

Confluent Cloud (`PLAIN`) and MSK with SCRAM (`SCRAM-SHA-512`) are the same code
path, only a different mechanism. MSK **IAM** auth is not supported by this
build: it needs an `OAUTHBEARER` token callback, so use SCRAM or TLS client
certs there.

`deploy/gcp/` holds the deployment manifests.
