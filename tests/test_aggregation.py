"""Unit tests for stateful aggregation, idempotency, and versioning."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType, EventType, MealType
from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.domain.exceptions import DuplicateEventError, StaleVersionError
from checkpoint_platform.infrastructure.persistence.models import (
    CheckpointState,
    DailyCounterAggregation,
    ProcessedEvent,
)


def _wastage_event(
    version: int,
    value: str,
    event_id=None,
    checkpoint_id: str = "cp-test-wastage",
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=checkpoint_id,
        checkpoint_version=version,
        client_id="client-10",
        cafe_id="cafe-22",
        counter_id="counter-450",
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value=Decimal(value),
        unit="KG",
        occurred_at=datetime.now(timezone.utc),
    )


def test_wastage_delta_update(session: Session):
    svc = AggregationService(session)
    svc.process(_wastage_event(1, "20"))
    session.flush()
    svc.process(_wastage_event(2, "15"))
    session.flush()

    agg = session.execute(
        select(DailyCounterAggregation).where(
            DailyCounterAggregation.counter_id == "counter-450",
            DailyCounterAggregation.meal_type == "LUNCH",
        )
    ).scalar_one()
    assert float(agg.food_wastage_kg) == 15.0


def test_duplicate_event_id(session: Session):
    svc = AggregationService(session)
    eid = uuid4()
    svc.process(_wastage_event(1, "10", event_id=eid))
    session.flush()
    with pytest.raises(DuplicateEventError):
        svc.process(_wastage_event(1, "10", event_id=eid))


def test_stale_version_ignored(session: Session):
    svc = AggregationService(session)
    svc.process(_wastage_event(1, "20"))
    session.flush()
    svc.process(_wastage_event(2, "15"))
    session.flush()
    with pytest.raises(StaleVersionError):
        svc.process(_wastage_event(1, "99"))
    session.flush()

    state = session.get(CheckpointState, "cp-test-wastage")
    assert state.checkpoint_version == 2
    assert float(state.value) == 15.0

    agg = session.execute(
        select(DailyCounterAggregation).where(
            DailyCounterAggregation.counter_id == "counter-450"
        )
    ).scalar_one()
    assert float(agg.food_wastage_kg) == 15.0


def test_processed_events_table(session: Session):
    svc = AggregationService(session)
    ev = _wastage_event(1, "5")
    svc.process(ev)
    session.flush()
    assert session.get(ProcessedEvent, ev.event_id) is not None
