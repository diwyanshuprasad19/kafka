#!/usr/bin/env bash
# Ensure + execute Cloud Run Job that runs `alembic upgrade head` against GCP DB.
# Uses the same image/secrets as checkpoint-api (Cloud SQL or AlloyDB via DATABASE_URL).
#
# Usage:
#   ./scripts/migrate_gcp.sh
#   IMAGE=gcr.io/PROJECT/checkpoint-api:v0.1.3 ./scripts/migrate_gcp.sh
#   make -C ../platform-ops alembic-gcp REPO=kafka
#
# Env (optional overrides):
#   GCP_PROJECT REGION SERVICE JOB IMAGE CLOUD_SQL_INSTANCE
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GCP_PROJECT="${GCP_PROJECT:-project-ca974159-bdd0-4647-b53}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-checkpoint-api}"
JOB="${JOB:-checkpoint-migrate}"
CLOUD_SQL_INSTANCE="${CLOUD_SQL_INSTANCE:-${GCP_PROJECT}:${REGION}:checkpoint-pg}"
IMAGE="${IMAGE:-gcr.io/${GCP_PROJECT}/${SERVICE}:latest}"

export CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT="${CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT:-}"

echo "==> Deploy/update migrate job ${JOB}"
echo "    image=${IMAGE}"
echo "    cloudsql=${CLOUD_SQL_INSTANCE}"

gcloud run jobs deploy "$JOB" \
  --project="$GCP_PROJECT" \
  --region="$REGION" \
  --image="$IMAGE" \
  --command=alembic \
  --args=upgrade,head \
  --set-cloudsql-instances="$CLOUD_SQL_INSTANCE" \
  --set-secrets="DATABASE_URL=checkpoint-database-url:latest" \
  --set-env-vars="APP_ENV=prod,USE_ALLOYDB=false,DEMO_SEED=false,RUN_MIGRATIONS=false,CHECKPOINT_ROOT=/app,LOG_FORMAT=json" \
  --max-retries=1 \
  --task-timeout=15m \
  --memory=512Mi \
  --cpu=1 \
  --quiet

echo "==> Execute ${JOB} (wait)"
gcloud run jobs execute "$JOB" \
  --project="$GCP_PROJECT" \
  --region="$REGION" \
  --wait \
  --quiet

echo "OK — alembic upgrade head finished on GCP"
