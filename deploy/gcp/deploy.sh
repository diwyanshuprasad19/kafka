#!/usr/bin/env bash
# Manual GCP deploy checklist (does not create paid resources automatically).
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${GCP_REGION:-asia-south1}"

echo "Project: ${PROJECT_ID:-unset}  Region: ${REGION}"
echo "1. Create AlloyDB + Auth Proxy / private IP"
echo "2. Create Kafka (Confluent Cloud or GCP Managed Kafka)"
echo "3. Create Memorystore Redis (optional)"
echo "4. Put secrets in Secret Manager (DATABASE_URL, KAFKA_SASL_*)"
echo "5. Copy configs/prod.env.example → configs/prod.env (local only) or inject Cloud Run env"
echo "6. Build:"
echo "     gcloud builds submit --config deploy/gcp/cloudbuild.yaml --project \$PROJECT_ID"
echo "7. Deploy API + consumers (edit PROJECT_ID in yaml first)"
echo "8. alembic upgrade head against AlloyDB"
echo "9. python scripts/verify.py --api https://YOUR_CLOUD_RUN_URL"
echo "See deploy/gcp/README.md"
