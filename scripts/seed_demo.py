#!/usr/bin/env python3
"""Seed a small, inspectable demo dataset via Kafka (or direct DB if --direct)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from checkpoint_platform.infrastructure.persistence.session import init_db, session_scope
from checkpoint_platform.infrastructure.messaging.kafka_producer import CheckpointProducer
from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.domain.exceptions import DuplicateEventError, StaleVersionError
from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType, EventType, MealType
from checkpoint_platform.infrastructure.observability.logging import setup_logging


DEMO_COUNTERS = [
    ("client-10", "cafe-22", "counter-450"),
    ("client-10", "cafe-22", "counter-451"),
    ("client-10", "cafe-23", "counter-500"),
    ("client-11", "cafe-30", "counter-900"),
]


def _event(
    client_id: str,
    cafe_id: str,
    counter_id: str,
    now: datetime,
    checkpoint_type: CheckpointType,
    status: CheckpointStatus,
    *,
    value: Decimal | None = None,
    unit: str | None = None,
    version: int = 1,
    event_type: EventType = EventType.COMPLETED,
) -> CheckpointEvent:
    """One LUNCH checkpoint. The id is derived so re-seeding is a correction, not a
    duplicate row."""
    return CheckpointEvent(
        event_id=uuid4(),
        event_type=event_type,
        checkpoint_id=f"cp-{counter_id}-LUNCH-{checkpoint_type.value}",
        checkpoint_version=version,
        client_id=client_id,
        cafe_id=cafe_id,
        counter_id=counter_id,
        meal_type=MealType.LUNCH,
        checkpoint_type=checkpoint_type,
        status=status,
        value=value,
        unit=unit,
        occurred_at=now,
    )


def build_demo_events() -> list[CheckpointEvent]:
    now = datetime.now(timezone.utc)
    events: list[CheckpointEvent] = []

    for client_id, cafe_id, counter_id in DEMO_COUNTERS:
        # Meal readiness completed
        events.append(
            CheckpointEvent(
                event_id=uuid4(),
                event_type=EventType.COMPLETED,
                checkpoint_id=f"cp-{counter_id}-LUNCH-MEAL_READINESS",
                checkpoint_version=1,
                client_id=client_id,
                cafe_id=cafe_id,
                counter_id=counter_id,
                meal_type=MealType.LUNCH,
                checkpoint_type=CheckpointType.MEAL_READINESS,
                status=CheckpointStatus.COMPLETED,
                occurred_at=now,
            )
        )
        # Food prepared / consumed / wastage
        for cp_type, value in [
            (CheckpointType.FOOD_PREPARED, Decimal("500")),
            (CheckpointType.FOOD_CONSUMED, Decimal("455")),
            (CheckpointType.FOOD_WASTAGE, Decimal("45")),
        ]:
            events.append(
                CheckpointEvent(
                    event_id=uuid4(),
                    event_type=EventType.COMPLETED,
                    checkpoint_id=f"cp-{counter_id}-LUNCH-{cp_type.value}",
                    checkpoint_version=1,
                    client_id=client_id,
                    cafe_id=cafe_id,
                    counter_id=counter_id,
                    meal_type=MealType.LUNCH,
                    checkpoint_type=cp_type,
                    status=CheckpointStatus.COMPLETED,
                    value=value,
                    unit="KG",
                    occurred_at=now,
                )
            )
        # Hygiene + temp
        events.append(
            CheckpointEvent(
                event_id=uuid4(),
                event_type=EventType.COMPLETED,
                checkpoint_id=f"cp-{counter_id}-LUNCH-STAFF_HYGIENE",
                checkpoint_version=1,
                client_id=client_id,
                cafe_id=cafe_id,
                counter_id=counter_id,
                meal_type=MealType.LUNCH,
                checkpoint_type=CheckpointType.STAFF_HYGIENE,
                status=CheckpointStatus.PASS,
                occurred_at=now,
            )
        )
        events.append(
            CheckpointEvent(
                event_id=uuid4(),
                event_type=EventType.COMPLETED,
                checkpoint_id=f"cp-{counter_id}-LUNCH-HOT_FOOD_TEMPERATURE",
                checkpoint_version=1,
                client_id=client_id,
                cafe_id=cafe_id,
                counter_id=counter_id,
                meal_type=MealType.LUNCH,
                checkpoint_type=CheckpointType.HOT_FOOD_TEMPERATURE,
                status=CheckpointStatus.PASS,
                value=Decimal("68"),
                unit="CELSIUS",
                occurred_at=now,
            )
        )

        # Received quantity, so "received vs prepared" is answerable.
        events.append(
            _event(
                client_id,
                cafe_id,
                counter_id,
                now,
                CheckpointType.FOOD_RECEIVED,
                CheckpointStatus.COMPLETED,
                value=Decimal("520"),
                unit="KG",
            )
        )
        # Work still outstanding, so compliance is not trivially 100%.
        for cp_type in (CheckpointType.FIFO_CHECK, CheckpointType.PEST_CONTROL):
            events.append(
                _event(
                    client_id,
                    cafe_id,
                    counter_id,
                    now,
                    cp_type,
                    CheckpointStatus.PENDING,
                )
            )
        # A failure of each kind, so the fail counters are non-zero.
        events.append(
            _event(
                client_id,
                cafe_id,
                counter_id,
                now,
                CheckpointType.KITCHEN_CLEANING,
                CheckpointStatus.FAIL,
            )
        )
        events.append(
            _event(
                client_id,
                cafe_id,
                counter_id,
                now,
                CheckpointType.COLD_STORAGE_TEMPERATURE,
                CheckpointStatus.FAIL,
                value=Decimal("11"),
                unit="CELSIUS",
            )
        )
        # An open incident: tracked on its own lifecycle, not as a failed checkpoint.
        events.append(
            _event(
                client_id,
                cafe_id,
                counter_id,
                now,
                CheckpointType.INCIDENT,
                CheckpointStatus.OPEN,
            )
        )

    # Correction on first counter: wastage 45 → 40
    client_id, cafe_id, counter_id = DEMO_COUNTERS[0]
    events.append(
        _event(
            client_id,
            cafe_id,
            counter_id,
            now,
            CheckpointType.FOOD_WASTAGE,
            CheckpointStatus.COMPLETED,
            value=Decimal("40"),
            unit="KG",
            version=2,
            event_type=EventType.UPDATED,
        )
    )
    # Corrective action closes the incident on the first counter, so open_incidents
    # falls back to zero without the incident_count changing.
    events.append(
        _event(
            client_id,
            cafe_id,
            counter_id,
            now,
            CheckpointType.INCIDENT,
            CheckpointStatus.CLOSED,
            version=2,
            event_type=EventType.UPDATED,
        )
    )
    return events


def seed_via_kafka() -> None:
    producer = CheckpointProducer()
    events = build_demo_events()
    for ev in events:
        producer.publish_checkpoint(ev)
        print(f"→ {ev.checkpoint_id} v{ev.checkpoint_version} {ev.checkpoint_type.value}")
    producer.flush()
    print(f"Published {len(events)} demo events to Kafka.")


def seed_direct() -> None:
    init_db()
    events = build_demo_events()
    with session_scope() as session:
        svc = AggregationService(session)
        for ev in events:
            try:
                svc.process(ev)
                print(f"✓ {ev.checkpoint_id} v{ev.checkpoint_version}")
            except (DuplicateEventError, StaleVersionError) as exc:
                print(f"skip {ev.checkpoint_id}: {exc}")
    print(f"Direct-seeded {len(events)} events into PostgreSQL.")


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Write aggregates directly to DB (no Kafka) — useful before Docker Kafka is up",
    )
    args = parser.parse_args()
    if args.direct:
        seed_direct()
    else:
        seed_via_kafka()


if __name__ == "__main__":
    main()
