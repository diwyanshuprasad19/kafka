#!/usr/bin/env python3
"""
Publish failure / reliability scenarios used in interviews:

  1. wastage v1=20 → v2=15 → duplicate v2 → stale v1  (expect 15kg)
  2. malformed / invalid → DLQ
  3. same event_id twice (duplicate)
  4. out-of-order versions (v3 then v2)
  5. retry-topic message

Usage:
  python scripts/failure_scenarios.py
  python scripts/failure_scenarios.py --scenario duplicates
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
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
    KafkaEventPublisher,
)
from checkpoint_platform.infrastructure.observability.logging import setup_logging
from checkpoint_platform.infrastructure.observability.metrics import PRODUCER_EVENTS


def _event(
    *,
    counter_id: str,
    checkpoint_id: str,
    version: int,
    value: str,
    event_id=None,
    retry_count: int = 0,
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=checkpoint_id,
        checkpoint_version=version,
        client_id="client-fail",
        cafe_id="cafe-fail",
        counter_id=counter_id,
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value=Decimal(value),
        unit="KG",
        occurred_at=datetime.now(UTC),
        retry_count=retry_count,
    )


def scenario_idempotency(publisher: KafkaEventPublisher) -> None:
    print("\n=== scenario: idempotency + ordering (expect wastage=15) ===")
    e1 = _event(
        counter_id="counter-fail-1",
        checkpoint_id="cp-fail-wastage",
        version=1,
        value="20",
    )
    e2 = _event(
        counter_id="counter-fail-1",
        checkpoint_id="cp-fail-wastage",
        version=2,
        value="15",
    )
    e2_dup = e2.model_copy()
    e1_stale = _event(
        counter_id="counter-fail-1",
        checkpoint_id="cp-fail-wastage",
        version=1,
        value="20",
    )
    for label, ev in [
        ("v1_20", e1),
        ("v2_15", e2),
        ("dup_v2", e2_dup),
        ("stale_v1", e1_stale),
    ]:
        publisher.publish_checkpoint(ev)
        PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
        print(
            f"  published {label} event_id={ev.event_id} v={ev.checkpoint_version} value={ev.value}"
        )
    publisher.flush()
    print("  → curl 'http://localhost:8080/aggregations/counter/counter-fail-1?meal_type=LUNCH'")


def scenario_duplicates(publisher: KafkaEventPublisher) -> None:
    print("\n=== scenario: same event_id twice ===")
    eid = uuid4()
    ev = _event(
        counter_id="counter-fail-dup",
        checkpoint_id="cp-fail-dup",
        version=1,
        value="7",
        event_id=eid,
    )
    publisher.publish_checkpoint(ev)
    publisher.publish_checkpoint(ev)
    PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
    PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
    publisher.flush()
    print(f"  published duplicate event_id={eid}")


def scenario_out_of_order(publisher: KafkaEventPublisher) -> None:
    print("\n=== scenario: out-of-order versions (v3 then v2) ===")
    for ver, val in [(1, "10"), (3, "30"), (2, "20")]:
        ev = _event(
            counter_id="counter-fail-ooo",
            checkpoint_id="cp-fail-ooo",
            version=ver,
            value=val,
        )
        publisher.publish_checkpoint(ev)
        PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
        print(f"  published v{ver}={val}")
    publisher.flush()
    print("  → final value must remain 30 (v2 ignored)")


def scenario_malformed(publisher: KafkaEventPublisher) -> None:
    print("\n=== scenario: malformed + invalid → DLQ ===")
    settings = get_settings()
    publisher.publish(
        topic=settings.checkpoint_topic,
        key="counter-fail-bad",
        value={"not": "a checkpoint", "counter_id": "counter-fail-bad"},
    )
    publisher.publish(
        topic=settings.checkpoint_topic,
        key="counter-fail-bad",
        value={"event_id": str(uuid4()), "counter_id": "x"},
    )
    PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
    PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
    publisher.flush()
    print("  → curl 'http://localhost:8080/dlq?status=pending'")


def scenario_retry(publisher: KafkaEventPublisher) -> None:
    print("\n=== scenario: retry topic message (retry_count=1) ===")
    settings = get_settings()
    ev = _event(
        counter_id="counter-fail-retry",
        checkpoint_id="cp-fail-retry",
        version=1,
        value="3",
        retry_count=1,
    )
    payload = ev.model_dump(mode="json")
    payload["retry_count"] = 1
    publisher.publish(settings.retry_topic, ev.counter_id, payload)
    PRODUCER_EVENTS.labels(source="failure_scenarios").inc()
    publisher.flush()
    print(f"  published to {settings.retry_topic}")


SCENARIOS = {
    "idempotency": scenario_idempotency,
    "duplicates": scenario_duplicates,
    "out_of_order": scenario_out_of_order,
    "malformed": scenario_malformed,
    "retry": scenario_retry,
}


def main() -> None:
    setup_logging(service="failure-scenarios")
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["all", *SCENARIOS.keys()], default="all")
    args = parser.parse_args()
    publisher = KafkaEventPublisher()
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    for name in names:
        SCENARIOS[name](publisher)
    print("\nDone. Wait for consumers, then check aggregates / DLQ / metrics.")


if __name__ == "__main__":
    main()
