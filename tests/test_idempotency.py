"""Pure unit tests that do not require a live database."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType, EventType, MealType
from checkpoint_platform.domain.events import CheckpointEvent


def test_event_schema_valid():
    ev = CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.COMPLETED,
        checkpoint_id="cp-1",
        checkpoint_version=1,
        client_id="c1",
        cafe_id="cafe-1",
        counter_id="counter-1",
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value=Decimal("12.5"),
        unit="KG",
        occurred_at=datetime.now(timezone.utc),
    )
    assert ev.partition_key() == "counter-1"


def test_event_schema_rejects_missing_counter():
    with pytest.raises(ValidationError):
        CheckpointEvent(
            event_id=uuid4(),
            event_type=EventType.COMPLETED,
            checkpoint_id="cp-1",
            checkpoint_version=1,
            client_id="c1",
            cafe_id="cafe-1",
            counter_id="",
            meal_type=MealType.LUNCH,
            checkpoint_type=CheckpointType.KITCHEN_CLEANING,
            status=CheckpointStatus.COMPLETED,
            occurred_at=datetime.now(timezone.utc),
        )


def test_delta_math():
    old, new = Decimal("18"), Decimal("12")
    assert new - old == Decimal("-6")
