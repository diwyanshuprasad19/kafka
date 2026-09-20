.PHONY: install up down setup-db bootstrap topics seed demo fail chaos load50k verify test api

install:
	python3.12 -m venv .venv
	. .venv/bin/activate && pip install -e ".[dev]"

setup-db:
	python scripts/setup_db.py

up:
	docker compose up -d --build

down:
	docker compose down -v

bootstrap:
	python scripts/bootstrap.py --seed

seed:
	python scripts/seed_demo.py --direct

seed-kafka:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python scripts/seed_demo.py

demo:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python scripts/generate_events.py --scenario demo

fail:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python scripts/failure_scenarios.py

chaos:
	python scripts/chaos_suite.py --full

chaos-list:
	python scripts/chaos_suite.py --list-only

load50k:
	KAFKA_THROUGHPUT_MODE=high KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
	python scripts/load_50k.py --events 50000 --rate-per-min 50000 --workers 2 --counters 5000

load50k-dry:
	python scripts/load_50k.py --dry-run --events 50000 --workers 2

plan50k:
	python scripts/load_50k.py --plan

verify:
	python scripts/verify.py --skip-api || true

test:
	pytest tests/ -q

api:
	checkpoint-api

consumer:
	checkpoint-consumer
