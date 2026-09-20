#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export PYTHONPATH=.
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}"

echo "==> Creating topics"
python scripts/create_topics.py

echo "==> Seeding demo events via Kafka"
python scripts/seed_demo.py

echo "==> Publishing idempotency demo (expect wastage=15kg)"
python scripts/generate_events.py --scenario demo

echo "Done. Query:"
echo '  curl "http://localhost:8080/aggregations/counter/counter-demo-1?meal_type=LUNCH"'
