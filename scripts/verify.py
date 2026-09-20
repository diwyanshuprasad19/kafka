#!/usr/bin/env python3
"""Verify local or GCP deployment health (API, Postgres, Kafka, Redis)."""

from __future__ import annotations

import argparse
import sys

import httpx
from sqlalchemy import text


def check_api(base: str) -> None:
    r = httpx.get(f"{base}/health", timeout=5)
    r.raise_for_status()
    print(f"✓ api health {r.json()}")
    r2 = httpx.get(f"{base}/ready", timeout=5)
    print(f"✓ api ready status={r2.status_code} body={r2.text[:200]}")
    r3 = httpx.get(f"{base}/metrics", timeout=5)
    r3.raise_for_status()
    print(f"✓ api metrics bytes={len(r3.content)}")


def check_db() -> None:
    from checkpoint_platform.infrastructure.persistence.session import engine

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ postgres")


def check_kafka() -> None:
    from confluent_kafka.admin import AdminClient

    from checkpoint_platform.config import get_settings

    settings = get_settings()
    admin = AdminClient(settings.kafka_client_config())
    md = admin.list_topics(timeout=10)
    topics = sorted(md.topics.keys())
    print(f"✓ kafka brokers={len(md.brokers)} topics={topics[:12]}")


def check_redis() -> None:
    import redis

    from checkpoint_platform.config import get_settings

    settings = get_settings()
    if not settings.redis_enabled:
        print("· redis disabled")
        return
    client = redis.from_url(settings.redis_url, socket_connect_timeout=2)
    client.ping()
    print("✓ redis")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8080")
    parser.add_argument("--skip-api", action="store_true")
    args = parser.parse_args()

    from checkpoint_platform.config import get_settings
    from checkpoint_platform.infrastructure.observability.logging import setup_logging

    setup_logging(service="verify")
    s = get_settings()
    print(
        f"APP_ENV={s.app_env} kafka={s.kafka_bootstrap_servers} db={s.database_url.split('@')[-1]}"
    )

    errors: list[str] = []
    for name, fn in [
        ("postgres", check_db),
        ("kafka", check_kafka),
        ("redis", check_redis),
    ]:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")
            print(f"✗ {name}: {exc}")

    if not args.skip_api:
        try:
            check_api(args.api.rstrip("/"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"api: {exc}")
            print(f"✗ api: {exc}")

    if errors:
        print("FAILED:", "; ".join(errors))
        sys.exit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
