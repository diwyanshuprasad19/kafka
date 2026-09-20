#!/usr/bin/env python3
"""
Synthetic checkpoint event generator + load tester.

Examples:
  python scripts/generate_events.py --events 100
  python scripts/generate_events.py --events 1000000 --rate 500 --counters 30000
  python scripts/generate_events.py --scenario demo   # wastage 20→15 + dup + stale
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from checkpoint_platform.config import get_settings
from checkpoint_platform.domain.enums import (
    CheckpointStatus,
    CheckpointType,
    EventType,
    MealType,
)
from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.infrastructure.messaging.kafka_producer import (
    CheckpointProducer,
)
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import PRODUCER_EVENTS

setup_logging()
logger = get_logger(__name__)

MEALS = [MealType.BREAKFAST, MealType.LUNCH, MealType.SNACKS, MealType.DINNER]
CHECKPOINTS = list(CheckpointType)
QUANTITY = {
    CheckpointType.FOOD_WASTAGE,
    CheckpointType.FOOD_PREPARED,
    CheckpointType.FOOD_CONSUMED,
}


def _random_event(
    counter_idx: int,
    meal: MealType,
    checkpoint_type: CheckpointType,
    day_offset: int = 0,
    version: int = 1,
    value: Decimal | None = None,
    status: CheckpointStatus | None = None,
    event_id=None,
    checkpoint_id: str | None = None,
) -> CheckpointEvent:
    client_id = f"client-{(counter_idx % 50) + 1}"
    cafe_id = f"cafe-{(counter_idx % 500) + 1}"
    counter_id = f"counter-{counter_idx}"
    cp_id = checkpoint_id or f"cp-{counter_id}-{meal.value}-{checkpoint_type.value}"

    if status is None:
        status = random.choices(
            [
                CheckpointStatus.COMPLETED,
                CheckpointStatus.FAILED,
                CheckpointStatus.PASS,
                CheckpointStatus.FAIL,
            ],
            weights=[70, 5, 20, 5],
        )[0]

    if checkpoint_type in QUANTITY and value is None:
        value = Decimal(str(round(random.uniform(5, 80), 2)))
        status = CheckpointStatus.COMPLETED
        unit = "KG"
    elif checkpoint_type in {
        CheckpointType.HOT_FOOD_TEMPERATURE,
        CheckpointType.COLD_STORAGE_TEMPERATURE,
    }:
        if value is None:
            value = Decimal(str(round(random.uniform(2, 85), 1)))
        unit = "CELSIUS"
        if status not in (CheckpointStatus.PASS, CheckpointStatus.FAIL):
            status = (
                CheckpointStatus.PASS
                if float(value) >= 60 or float(value) <= 8
                else CheckpointStatus.FAIL
            )
    else:
        unit = None

    occurred = datetime.now(UTC) - timedelta(days=day_offset)

    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=cp_id,
        checkpoint_version=version,
        client_id=client_id,
        cafe_id=cafe_id,
        counter_id=counter_id,
        meal_type=meal,
        checkpoint_type=checkpoint_type,
        status=status,
        value=value,
        unit=unit,
        occurred_at=occurred,
    )


def run_demo_scenario(producer: CheckpointProducer) -> None:
    """
    Prove idempotency + ordering:
      v1 wastage=20 → v2 wastage=15 → duplicate v2 → stale v1
    Final aggregate food_wastage_kg must be 15.
    """
    counter = "counter-demo-1"
    cp_id = "cp-demo-wastage-lunch"
    base = {
        "checkpoint_id": cp_id,
        "client_id": "client-demo",
        "cafe_id": "cafe-demo",
        "counter_id": counter,
        "meal_type": MealType.LUNCH,
        "checkpoint_type": CheckpointType.FOOD_WASTAGE,
        "status": CheckpointStatus.COMPLETED,
        "unit": "KG",
        "occurred_at": datetime.now(UTC),
    }

    e1 = CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.COMPLETED,
        checkpoint_version=1,
        value=Decimal(20),
        **base,
    )
    e2_id = uuid4()
    e2 = CheckpointEvent(
        event_id=e2_id,
        event_type=EventType.UPDATED,
        checkpoint_version=2,
        value=Decimal(15),
        **base,
    )
    # Duplicate of e2
    e2_dup = e2.model_copy()
    # Stale v1 replay with new event_id (simulates late sync)
    e1_stale = CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.UPDATED,
        checkpoint_version=1,
        value=Decimal(20),
        **base,
    )

    for label, ev in [
        ("v1_20kg", e1),
        ("v2_15kg", e2),
        ("dup_v2", e2_dup),
        ("stale_v1", e1_stale),
    ]:
        producer.publish_checkpoint(ev)
        PRODUCER_EVENTS.labels(source="generator").inc()
        print(
            f"published {label}: event_id={ev.event_id} version={ev.checkpoint_version} value={ev.value}"
        )

    producer.flush()
    print("Demo scenario published. Expected final food_wastage_kg = 15")
    print(f"Query: GET /aggregations/counter/{counter}?meal_type=LUNCH")


def run_load(
    producer: CheckpointProducer,
    events: int,
    counters: int,
    rate: float,
    days: int,
) -> None:
    settings = get_settings()
    print(
        f"Producing {events} events across {counters} counters "
        f"at ~{rate} evt/s → {settings.checkpoint_topic}"
    )
    start = time.perf_counter()
    interval = 1.0 / rate if rate > 0 else 0
    next_tick = start

    for i in range(events):
        counter_idx = (i % counters) + 1
        meal = MEALS[i % len(MEALS)]
        cp_type = CHECKPOINTS[i % len(CHECKPOINTS)]
        day_offset = i % max(days, 1)
        event = _random_event(counter_idx, meal, cp_type, day_offset=day_offset)
        producer.publish_checkpoint(event)
        PRODUCER_EVENTS.labels(source="generator").inc()

        if rate > 0:
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

        if (i + 1) % 10000 == 0:
            elapsed = time.perf_counter() - start
            print(f"  {i + 1}/{events}  ({(i + 1) / elapsed:.0f} evt/s)")

    producer.flush()
    elapsed = time.perf_counter() - start
    print(f"Done. {events} events in {elapsed:.2f}s → {events / elapsed:.1f} evt/s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Checkpoint event generator")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--counters", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=0, help="Target events/sec (0=unlimited)")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument(
        "--scenario",
        choices=["load", "demo"],
        default="load",
        help="demo = wastage correction + duplicate + stale",
    )
    args = parser.parse_args()

    producer = CheckpointProducer()
    if args.scenario == "demo":
        run_demo_scenario(producer)
    else:
        run_load(producer, args.events, args.counters, args.rate, args.days)


if __name__ == "__main__":
    main()
