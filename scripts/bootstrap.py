#!/usr/bin/env python3
"""
Bootstrap local or GCP environment:
  - wait for Postgres / Kafka
  - alembic migrate (or create_all)
  - create topics
  - optional seed

Usage:
  python scripts/bootstrap.py
  python scripts/bootstrap.py --seed
  APP_ENV=prod python scripts/bootstrap.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from sqlalchemy import text


def _root() -> Path:
    from checkpoint_platform.config.settings import _find_repo_root

    return _find_repo_root()


def wait_postgres(timeout: int = 60) -> None:
    from checkpoint_platform.infrastructure.persistence.session import engine

    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✓ postgres ready")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2)
    raise SystemExit(f"Postgres not ready: {last}")


def wait_kafka(timeout: int = 60) -> None:
    from confluent_kafka.admin import AdminClient

    from checkpoint_platform.config import get_settings

    settings = get_settings()
    admin = AdminClient(settings.kafka_client_config())
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=5)
            if md.brokers:
                print(f"✓ kafka ready ({settings.kafka_bootstrap_servers})")
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2)
    raise SystemExit(f"Kafka not ready: {last}")


def migrate() -> None:
    from alembic.config import Config

    from alembic import command

    root = _root()
    cfg = Config(str(root / "alembic.ini"))
    try:
        command.upgrade(cfg, "head")
        print("✓ alembic upgrade head")
    except Exception as exc:  # noqa: BLE001
        print(f"! alembic failed ({exc}); using create_all")
        from checkpoint_platform.infrastructure.persistence.session import init_db

        init_db()
        print("✓ create_all")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--skip-kafka", action="store_true")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    from checkpoint_platform.config import get_settings
    from checkpoint_platform.infrastructure.observability.logging import setup_logging

    setup_logging(service="bootstrap")
    settings = get_settings()
    print(f"APP_ENV={settings.app_env} kafka={settings.kafka_bootstrap_servers}")

    wait_postgres(timeout=args.timeout)
    migrate()

    if not args.skip_kafka:
        wait_kafka(timeout=args.timeout)
        from checkpoint_platform.infrastructure.messaging.kafka_admin import (
            create_topics,
        )

        create_topics()
        print("✓ topics")

    if args.seed:
        seed = _root() / "scripts" / "seed_demo.py"
        subprocess.check_call([sys.executable, str(seed)], cwd=str(_root()))
        print("✓ demo seed published")

    print("bootstrap complete")


if __name__ == "__main__":
    main()
