#!/usr/bin/env python3
"""
Load generator for prod scale: 50,000 events / minute (≈ 833 / sec).

Examples:
  python scripts/load_50k.py --dry-run --events 50000 --workers 2
  python scripts/load_50k.py --events 50000 --rate-per-min 50000 --workers 2
  python scripts/load_50k.py --ramp --duration 30
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

MEALS = ["BREAKFAST", "LUNCH", "SNACKS", "DINNER"]
TYPES = [
    "FOOD_WASTAGE",
    "FOOD_PREPARED",
    "FOOD_CONSUMED",
    "KITCHEN_CLEANING",
    "STAFF_HYGIENE",
    "HOT_FOOD_TEMPERATURE",
    "MEAL_READINESS",
]


def _make_event(i: int, counters: int) -> dict:
    counter_idx = (i % counters) + 1
    cp_type = TYPES[i % len(TYPES)]
    meal = MEALS[i % len(MEALS)]
    value = None
    unit = None
    status = "COMPLETED"
    if cp_type in {"FOOD_WASTAGE", "FOOD_PREPARED", "FOOD_CONSUMED"}:
        value = round((i % 97) + 1.5, 2)
        unit = "KG"
    elif "TEMPERATURE" in cp_type:
        value = 65.0
        unit = "CELSIUS"
        status = "PASS"
    return {
        "event_id": str(uuid4()),
        "event_type": "checkpoint.completed",
        "checkpoint_id": f"cp-{counter_idx}-{meal}-{cp_type}-{i // max(counters, 1)}",
        "checkpoint_version": 1,
        "client_id": f"client-{(counter_idx % 50) + 1}",
        "cafe_id": f"cafe-{(counter_idx % 500) + 1}",
        "counter_id": f"counter-{counter_idx}",
        "meal_type": meal,
        "checkpoint_type": cp_type,
        "status": status,
        "value": value,
        "unit": unit,
        "occurred_at": datetime.now(UTC).isoformat(),
        "retry_count": 0,
    }


def worker_dry(args: tuple) -> dict:
    worker_id, start_i, count, counters = args
    t0 = time.perf_counter()
    for i in range(start_i, start_i + count):
        json.dumps(_make_event(i, counters))
    elapsed = time.perf_counter() - t0
    return {
        "worker": worker_id,
        "events": count,
        "elapsed": elapsed,
        "rate": count / elapsed if elapsed else 0,
    }


def worker_produce(args: tuple) -> dict:
    worker_id, start_i, count, counters, rate_per_sec, topic = args
    os.environ.setdefault("KAFKA_THROUGHPUT_MODE", "high")
    from checkpoint_platform.config import reload_settings
    from checkpoint_platform.infrastructure.messaging.kafka_producer import (
        KafkaEventPublisher,
    )

    reload_settings()
    publisher = KafkaEventPublisher(throughput_mode="high")
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    next_tick = time.perf_counter()
    t0 = next_tick
    sent = 0
    for i in range(start_i, start_i + count):
        ev = _make_event(i, counters)
        publisher.publish(topic, ev["counter_id"], ev)
        sent += 1
        if interval > 0:
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
        if sent % 5000 == 0:
            publisher._producer.poll(0)
    publisher.flush(60)
    elapsed = time.perf_counter() - t0
    stats = publisher.stats
    return {
        "worker": worker_id,
        "events": sent,
        "elapsed": elapsed,
        "rate": sent / elapsed if elapsed else 0,
        "delivered": stats["delivered"],
        "failed": stats["failed"],
    }


def run_load(
    *, events: int, rate_per_sec: float, workers: int, counters: int, dry_run: bool
) -> None:
    workers = max(1, workers)
    chunk = events // workers
    rem = events % workers
    jobs = []
    idx = 0
    per_worker_rate = rate_per_sec / workers if rate_per_sec > 0 else 0
    from checkpoint_platform.config import get_settings

    topic = get_settings().checkpoint_topic
    for w in range(workers):
        n = chunk + (1 if w < rem else 0)
        if n <= 0:
            continue
        if dry_run:
            jobs.append((w, idx, n, counters))
        else:
            jobs.append((w, idx, n, counters, per_worker_rate, topic))
        idx += n

    t0 = time.perf_counter()
    with mp.Pool(processes=workers) as pool:
        results = pool.map(worker_dry if dry_run else worker_produce, jobs)
    wall = time.perf_counter() - t0
    total = sum(r["events"] for r in results)
    per_min = (total / wall) * 60 if wall else 0
    print(
        f"workers={workers} events={total} wall={wall:.2f}s → "
        f"{total / wall:.0f} evt/s ({per_min:.0f} evt/min)"
    )
    for r in results:
        extra = "" if dry_run else f" delivered={r.get('delivered')} failed={r.get('failed')}"
        print(f"  worker-{r['worker']}: {r['rate']:.0f} evt/s{extra}")
    target_min = rate_per_sec * 60 if rate_per_sec else per_min
    pct = (per_min / target_min * 100) if target_min else 0
    print(f"achieved={per_min:.0f} evt/min / target={target_min:.0f} evt/min ({pct:.1f}%)")


def run_ramp(duration: int, workers: int, counters: int, dry_run: bool) -> None:
    # Ramp in events/minute toward 50k/min
    steps = [5_000, 15_000, 30_000, 50_000]
    print("=== RAMP toward 50k events/minute ===")
    for target_min in steps:
        rate = target_min / 60.0
        events = int(rate * duration)
        print(f"\n--- {target_min} evt/min for {duration}s (~{events} events) ---")
        run_load(
            events=events,
            rate_per_sec=rate,
            workers=workers,
            counters=counters,
            dry_run=dry_run,
        )


def print_plan() -> None:
    from checkpoint_platform.infrastructure.messaging.tuning import SCALE_50K_PER_MIN

    print("\n=== Prod scale: 50k events/minute ===")
    for k, v in SCALE_50K_PER_MIN.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--events",
        type=int,
        default=50_000,
        help="Total events (default 1 min @ 50k/min)",
    )
    parser.add_argument(
        "--rate-per-min", type=float, default=50_000, help="Target events per minute"
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--counters", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ramp", action="store_true")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()

    os.environ.setdefault("KAFKA_THROUGHPUT_MODE", "high")
    print_plan()
    if args.plan:
        return
    rate_per_sec = args.rate_per_min / 60.0
    if args.ramp:
        run_ramp(args.duration, args.workers, args.counters, args.dry_run)
    else:
        run_load(
            events=args.events,
            rate_per_sec=rate_per_sec,
            workers=args.workers,
            counters=args.counters,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
