#!/usr/bin/env python3
"""
Run all production bad scenarios (18 catalogued, >=15 required).

  python scripts/chaos_suite.py           # unit + list catalog
  python scripts/chaos_suite.py --full    # pytest all S01–S18 (DB tests skip if no Postgres)
  python scripts/chaos_suite.py --with-kafka
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def print_catalog() -> None:
    from checkpoint_platform.application.bad_scenarios import (
        PROD_BAD_SCENARIOS,
        require_min_count,
    )

    require_min_count(15)
    print(f"\n=== Production bad scenarios ({len(PROD_BAD_SCENARIOS)}) ===\n")
    for s in PROD_BAD_SCENARIOS:
        print(f"{s.id}")
        print(f"  title:    {s.title}")
        print(f"  failure:  {s.what_goes_wrong}")
        print(f"  expect:   {s.expected_behavior}")
        print()


def run_pytest_bad() -> int:
    return subprocess.call(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_prod_bad_scenarios.py",
            "-v",
            "--tb=short",
        ],
        cwd=str(ROOT),
    )


def run_kafka_publish() -> int:
    return subprocess.call(
        [sys.executable, str(ROOT / "scripts" / "failure_scenarios.py")],
        cwd=str(ROOT),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        help="Execute pytest suite for all bad scenarios",
    )
    parser.add_argument("--with-kafka", action="store_true")
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()

    print_catalog()
    if args.list_only:
        return

    # Always run the executable suite when --full or by default
    if args.full or not args.with_kafka:
        print("=== executing tests/test_prod_bad_scenarios.py ===\n")
        rc = run_pytest_bad()
        if rc != 0:
            sys.exit(rc)

    if args.with_kafka:
        print("=== kafka failure_scenarios publish ===\n")
        rc = run_kafka_publish()
        if rc != 0:
            sys.exit(rc)

    print("\nAll requested bad-scenario checks finished.")


if __name__ == "__main__":
    main()
