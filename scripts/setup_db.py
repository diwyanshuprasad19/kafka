#!/usr/bin/env python3
"""
One-shot local DB setup:
  - wait for Postgres
  - alembic upgrade head (or create_all fallback)
  - stamp if create_all was used
  - seed demo working data (direct DB)

Usage:
  python scripts/setup_db.py
  python scripts/setup_db.py --no-seed
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def wait_pg(timeout: int = 60) -> None:
    from sqlalchemy import create_engine, text

    from checkpoint_platform.config import get_settings

    url = get_settings().database_url
    engine = create_engine(url, future=True)
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✓ postgres ({url.split('@')[-1]})")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(1)
    raise SystemExit(f"Postgres not ready: {last}")


def migrate() -> None:
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    try:
        command.upgrade(cfg, "head")
        print("✓ alembic upgrade head")
    except Exception as exc:  # noqa: BLE001
        print(f"! alembic failed ({exc}); create_all + stamp")
        from checkpoint_platform.infrastructure.persistence.session import init_db

        init_db()
        try:
            command.stamp(cfg, "head")
            print("✓ create_all + alembic stamp head")
        except Exception as stamp_exc:  # noqa: BLE001
            print(f"! stamp skipped: {stamp_exc}")
            print("✓ create_all")


def seed() -> None:
    # Direct DB seed (no Kafka required)
    rc = subprocess.call(
        [sys.executable, str(ROOT / "scripts" / "seed_demo.py"), "--direct"],
        cwd=str(ROOT),
    )
    if rc != 0:
        raise SystemExit("seed failed")
    print("✓ demo data seeded")


def show_counts() -> None:
    from sqlalchemy import create_engine, text

    from checkpoint_platform.config import get_settings

    engine = create_engine(get_settings().database_url, future=True)
    tables = [
        "checkpoint_state",
        "checkpoint_history",
        "daily_counter_aggregation",
        "processed_events",
        "outbox_events",
        "dlq_records",
        "reporting_snapshots",
    ]
    with engine.connect() as conn:
        for t in tables:
            try:
                n = conn.execute(text(f"SELECT count(*) FROM {t}")).scalar()
                print(f"  {t}: {n}")
            except Exception as exc:  # noqa: BLE001
                print(f"  {t}: missing ({exc.__class__.__name__})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-seed", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    from checkpoint_platform.config import reload_settings
    from checkpoint_platform.infrastructure.observability.logging import setup_logging

    reload_settings()
    setup_logging(service="setup-db")
    wait_pg(args.timeout)
    migrate()
    if not args.no_seed:
        seed()
    print("\nTable counts:")
    show_counts()
    print("\nDB setup complete. Example:")
    print('  curl "http://localhost:8080/aggregations/counter/counter-450?meal_type=LUNCH"')


if __name__ == "__main__":
    main()
