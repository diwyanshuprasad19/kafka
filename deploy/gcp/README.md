# GCP / AlloyDB — same Python code as local. Change env only.

## One codebase

| Concern | Local | GCP |
|---------|-------|-----|
| `APP_ENV` | `local` | `prod` or `gcp` |
| Config | `configs/local.env` + `.env` | `configs/prod.env` + Secret Manager / Cloud Run env |
| Kafka | Docker KRaft PLAINTEXT | Confluent Cloud / Managed Kafka SASL_SSL |
| DB | Postgres Compose | AlloyDB (Auth Proxy or private IP) |
| Redis | Compose Redis | Memorystore |
| Replication | `KAFKA_REPLICATION_FACTOR=1` | `3` (cluster must support) |

```bash
# Local
docker compose up -d --build
python scripts/bootstrap.py --seed
python scripts/verify.py
python scripts/failure_scenarios.py

# GCP (after secrets + AlloyDB + Kafka exist)
cp configs/prod.env.example configs/prod.env   # fill values locally; do not commit
export APP_ENV=prod
# Or inject env in Cloud Run — runtime env vars override files
gcloud builds submit --config deploy/gcp/cloudbuild.yaml
```

## AlloyDB

```bash
./alloydb-auth-proxy \
  "projects/$GCP_PROJECT_ID/locations/$GCP_REGION/clusters/$ALLOYDB_CLUSTER/instances/$ALLOYDB_INSTANCE" \
  --port 5433
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

1. **API** — Cloud Run / GKE — `gunicorn ... checkpoint_platform.interfaces.http.app:app`
2. **aggregation-consumer** (min 2 replicas) — same image, different command
3. **outbox-publisher**
4. **reporting-consumer**

See `cloud-run-*.yaml` and `cloudbuild.yaml`.
