# GCP / AlloyDB / Cloud SQL — same Python code as local. Change env only.

## One codebase

| Concern | Local | GCP Cloud SQL (`USE_ALLOYDB=false`) | AlloyDB (`USE_ALLOYDB=true`) |
|---------|-------|-------------------------------------|------------------------------|
| `APP_ENV` | `local` | `prod` | `prod` |
| `USE_ALLOYDB` | `false` | `false` | `true` |
| `DEMO_SEED` | `true` | `false` (required) | `false` (required) |
| Config | `configs/local.env` + `.env` | Secret Manager + Cloud Run env | Auth Proxy + Secret Manager |
| DB | Compose Postgres | Cloud SQL (`/cloudsql/…` socket) | AlloyDB Auth Proxy / private IP |
| Migrations | `make alembic` / entrypoint | **Cloud Run Job** `checkpoint-migrate` | same job with AlloyDB URL |
| Kafka | Docker KRaft PLAINTEXT | Confluent / Managed Kafka SASL_SSL | same |
| Redis | Compose | Memorystore (or `REDIS_ENABLED=false`) | same |

```bash
# Local (demo OK)
./scripts/switch_backend.sh local
docker compose up -d --build
python scripts/bootstrap.py --seed

# Flip toward GCP Cloud SQL (no demo seed)
./scripts/switch_backend.sh cloudsql

# Or AlloyDB
./scripts/switch_backend.sh alloydb
# start Auth Proxy, export DATABASE_URL, then:
alembic upgrade head
```

## Migrations on GCP (keep working on every release)

Prefer the **migrate job** over `RUN_MIGRATIONS=true` on the API (avoids multi-replica races):

```bash
# One-shot now
./scripts/migrate_gcp.sh
# or
make -C ../platform-ops alembic-gcp REPO=kafka

# Automatic: Cloud Build step `migrate` runs before `deploy`
# (deploy/gcp/cloudbuild.yaml → gcloud run jobs execute checkpoint-migrate --wait)
```

API deploys with `RUN_MIGRATIONS=false`, `DEMO_SEED=false`, `USE_ALLOYDB=false` (Cloud SQL today).

## AlloyDB (when you flip the flag)

```bash
./alloydb-auth-proxy \
  "projects/$GCP_PROJECT_ID/locations/$GCP_REGION/clusters/$ALLOYDB_CLUSTER/instances/$ALLOYDB_INSTANCE" \
  --port 5433
export USE_ALLOYDB=true DEMO_SEED=false APP_ENV=prod
export DATABASE_URL=postgresql+psycopg://checkpoint:SECRET@127.0.0.1:5433/aggregation
alembic upgrade head
```

## Kafka SASL (Confluent / compatible)

```
KAFKA_BOOTSTRAP_SERVERS=pkc-....gcp.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=...
KAFKA_SASL_PASSWORD=...
KAFKA_REPLICATION_FACTOR=3
```

`Settings.kafka_client_config()` is shared — no code fork for cloud.

## Services to deploy

1. **API** — Cloud Run — `gunicorn ... checkpoint_platform.interfaces.http.app:app`
2. **checkpoint-migrate** — Cloud Run Job — `alembic upgrade head` (before each release)
3. **aggregation-consumer** (min 2 replicas) — same image, different command
4. **outbox-publisher** / **reporting-consumer**

See `cloud-run-*.yaml`, `cloud-run-migrate-job.yaml`, and `cloudbuild.yaml`.
