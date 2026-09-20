#!/usr/bin/env python3
"""Benchmark the ingest path against the 50k events/minute target.

Kafka is not the constraint at this scale — a single broker partition handles far
more than 833 messages/second. The constraint is the database work per event:
claim the event_id, lock and rewrite the checkpoint row, UPSERT the aggregate,
append history, enqueue the outbox row. This script drives exactly that path
through ``EventProcessor`` in the same batched-transaction shape the consumer uses,
so the number it reports is the number that matters for capacity planning.

  python scripts/bench_ingest.py --events 50000 --batch-size 200
  python scripts/bench_ingest.py --events 50000 --compare-unbatched
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Set before settings are constructed: a JSON log line per event would otherwise
# dominate the measurement and hide the database cost being measured.
import os

os.environ.setdefault("LOG_LEVEL", "WARNING")

TARGET_PER_MINUTE = 50_000
TARGET_PER_SEC = TARGET_PER_MINUTE / 60.0

MEALS = ["BREAKFAST", "LUNCH", "SNACKS", "DINNER"]
TYPES = [
    ("FOOD_WASTAGE", "COMPLETED", "KG"),
    ("FOOD_PREPARED", "COMPLETED", "KG"),
    ("FOOD_CONSUMED", "COMPLETED", "KG"),
    ("STAFF_HYGIENE", "PASS", None),
    ("HOT_FOOD_TEMPERATURE", "PASS", "C"),
    ("MEAL_READINESS", "COMPLETED", None),
]


class NullPublisher:
    """Stands in for Kafka so the measurement isolates database cost."""

    def publish_checkpoint(self, event) -> None: ...
    def publish_retry(self, event, key) -> None: ...
    def publish_dlq(self, payload, key="dlq") -> None: ...
    def publish_aggregation(self, event, key) -> None: ...

    def flush(self, timeout: float = 10.0) -> int:
        return 0


def _slot(i: int, counters: int) -> tuple[str, str, str, str, str | None]:
    """Stable identity for event index `i`: which counter, meal and checkpoint type."""
    counter = f"bench-counter-{(i % counters) + 1}"
    ctype, status, unit = TYPES[i % len(TYPES)]
    meal = MEALS[(i // len(TYPES)) % len(MEALS)]
    # The index is part of the id so every non-correction event is a genuinely new
    # checkpoint. Without it the ids cycle and the run degenerates into stale
    # rejections, which measures the cheapest path instead of the real one.
    checkpoint_id = f"cp-{counter}-{meal}-{ctype}-{i}"
    return checkpoint_id, counter, meal, ctype, status, unit  # type: ignore[return-value]


def build_payload(i: int, counters: int, corrections_every: int) -> bytes:
    # Every Nth event corrects a checkpoint created earlier in the run, exercising
    # the expensive path: lock the prior state, reverse its contribution, apply the
    # new one.
    is_correction = corrections_every and i % corrections_every == 0 and i >= corrections_every
    source = i - corrections_every if is_correction else i
    checkpoint_id, counter, meal, ctype, status, unit = _slot(source, counters)

    payload = {
        "event_id": str(uuid4()),
        "event_type": "checkpoint.updated" if is_correction else "checkpoint.completed",
        "checkpoint_id": checkpoint_id,
        "checkpoint_version": 2 if is_correction else 1,
        "client_id": "client-bench",
        "cafe_id": f"cafe-{(i % 20) + 1}",
        "counter_id": counter,
        "meal_type": meal,
        "checkpoint_type": ctype,
        "status": status,
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    if unit == "KG":
        payload["value"] = round(5 + (i % 90) * 0.5, 3)
        payload["unit"] = "KG"
    elif unit == "C":
        payload["value"] = 60 + (i % 15)
        payload["unit"] = "C"
    return json.dumps(payload).encode()


def cleanup() -> None:
    from sqlalchemy import text

    from checkpoint_platform.config.container import get_session

    session = get_session()
    try:
        for table, column in (
            ("checkpoint_history", "counter_id"),
            ("checkpoint_state", "counter_id"),
            ("daily_counter_aggregation", "counter_id"),
        ):
            session.execute(text(f"DELETE FROM {table} WHERE {column} LIKE 'bench-counter-%'"))
        session.execute(
            text("DELETE FROM outbox_events WHERE payload->>'counter_id' LIKE 'bench-counter-%'")
        )
        session.commit()
    finally:
        session.close()


def run(
    events: int,
    batch_size: int,
    counters: int,
    corrections_every: int,
    shard: int = 0,
) -> dict:
    from checkpoint_platform.config.container import get_event_processor, get_session

    publisher = NullPublisher()
    payloads = [
        build_payload(shard * events + i, counters, corrections_every) for i in range(events)
    ]

    outcomes: dict[str, int] = {}
    batch_durations: list[float] = []
    offset = 0
    started = time.perf_counter()

    for start in range(0, events, batch_size):
        chunk = payloads[start : start + batch_size]
        session = get_session()
        batch_started = time.perf_counter()
        try:
            processor = get_event_processor(
                session=session,
                publisher=publisher,
                manage_transaction=batch_size == 1,
            )
            for raw in chunk:
                outcome = processor.process_raw(raw, "checkpoint.events.v1", 0, offset)
                outcomes[outcome] = outcomes.get(outcome, 0) + 1
                offset += 1
            if batch_size > 1:
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        batch_durations.append(time.perf_counter() - batch_started)

    elapsed = time.perf_counter() - started
    per_sec = events / elapsed if elapsed else 0.0
    # A run dominated by stale rejections is not measuring the write path.
    applied = outcomes.get("processed", 0)
    if applied < events * 0.5:
        raise SystemExit(
            f"invalid benchmark: only {applied}/{events} events were applied "
            f"({outcomes}) — the generator is producing duplicate checkpoint ids"
        )
    return {
        "events": events,
        "batch_size": batch_size,
        "elapsed_seconds": round(elapsed, 2),
        "events_per_second": round(per_sec, 1),
        "events_per_minute": round(per_sec * 60),
        "headroom_vs_target": f"{per_sec / TARGET_PER_SEC:.1f}x",
        "meets_target": per_sec >= TARGET_PER_SEC,
        "batch_p50_ms": round(statistics.median(batch_durations) * 1000, 2),
        "batch_p95_ms": round(
            sorted(batch_durations)[int(len(batch_durations) * 0.95) - 1] * 1000, 2
        )
        if len(batch_durations) > 1
        else round(batch_durations[0] * 1000, 2),
        "outcomes": outcomes,
    }


def _worker(args) -> dict:
    # Each instance works a disjoint slice of the id space — the same isolation Kafka
    # gives real consumers by partitioning on counter_id.
    shard, events, batch_size, counters, corrections_every = args
    return run(events, batch_size, counters, corrections_every, shard=shard)


def run_fleet(
    instances: int,
    events: int,
    batch_size: int,
    counters: int,
    corrections_every: int,
) -> dict:
    """Measure N consumer processes writing concurrently, as deployed.

    A single process is not the capacity of the system: the design runs 12
    partitions across 3 consumers. This measures the fleet for real rather than
    multiplying one process's number by three and hoping the database scales.
    """
    import multiprocessing as mp

    per_instance = events // instances
    started = time.perf_counter()
    with mp.Pool(instances) as pool:
        results = pool.map(
            _worker,
            [
                (shard, per_instance, batch_size, counters, corrections_every)
                for shard in range(instances)
            ],
        )
    elapsed = time.perf_counter() - started

    total = sum(r["events"] for r in results)
    per_sec = total / elapsed if elapsed else 0.0
    return {
        "instances": instances,
        "events": total,
        "elapsed_seconds": round(elapsed, 2),
        "events_per_second": round(per_sec, 1),
        "events_per_minute": round(per_sec * 60),
        "per_instance_events_per_minute": [r["events_per_minute"] for r in results],
        "headroom_vs_target": f"{per_sec / TARGET_PER_SEC:.1f}x",
        "meets_target": per_sec >= TARGET_PER_SEC,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--counters", type=int, default=500)
    parser.add_argument(
        "--instances",
        type=int,
        default=1,
        help="consumer processes to run concurrently (deployed topology uses 3)",
    )
    parser.add_argument(
        "--corrections-every",
        type=int,
        default=10,
        help="every Nth event is a version-2 correction (0 disables)",
    )
    parser.add_argument(
        "--compare-unbatched",
        action="store_true",
        help="also measure commit-per-message to show what batching buys",
    )
    parser.add_argument("--keep", action="store_true", help="keep benchmark rows")
    args = parser.parse_args()

    import logging

    from checkpoint_platform.infrastructure.observability.logging import setup_logging

    setup_logging(service="bench")
    # Per-event info logs would dominate the measurement.
    logging.getLogger().setLevel(logging.WARNING)

    print(f"target: {TARGET_PER_MINUTE:,}/min ({TARGET_PER_SEC:.0f}/sec)\n")

    cleanup()
    if args.instances > 1:
        batched = run_fleet(
            args.instances,
            args.events,
            args.batch_size,
            args.counters,
            args.corrections_every,
        )
        print(f"fleet of {args.instances} consumers (deployed topology)")
    else:
        batched = run(args.events, args.batch_size, args.counters, args.corrections_every)
        print("single consumer process")
    for key, value in batched.items():
        print(f"  {key}: {value}")

    if args.compare_unbatched:
        sample = min(args.events, 3_000)
        cleanup()
        single = run(sample, 1, args.counters, args.corrections_every)
        print("\ncommit-per-message (previous shape)")
        for key, value in single.items():
            print(f"  {key}: {value}")
        speedup = batched["events_per_second"] / max(single["events_per_second"], 0.01)
        print(f"\nbatching speedup: {speedup:.1f}x")

    if not args.keep:
        cleanup()

    print(
        f"\nRESULT: {'PASS' if batched['meets_target'] else 'FAIL'} — "
        f"{batched['events_per_minute']:,} events/minute sustained"
    )
    sys.exit(0 if batched["meets_target"] else 1)


if __name__ == "__main__":
    main()
