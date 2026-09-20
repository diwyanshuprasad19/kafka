"""Reverse-then-apply contribution math — no database required."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from checkpoint_platform.application.aggregation import Contribution
from checkpoint_platform.domain.enums import (
    CheckpointStatus,
    CheckpointType,
    EventType,
    MealType,
)
from checkpoint_platform.domain.events import CheckpointEvent


def _event(
    value: str,
    status=CheckpointStatus.COMPLETED,
    checkpoint_type=CheckpointType.FOOD_WASTAGE,
    unit: str = "KG",
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.UPDATED,
        checkpoint_id="cp-1",
        checkpoint_version=2,
        client_id="c",
        cafe_id="cafe",
        counter_id="counter-1",
        meal_type=MealType.LUNCH,
        checkpoint_type=checkpoint_type,
        status=status,
        value=Decimal(value),
        unit=unit,
        occurred_at=datetime.now(UTC),
    )


def _net(previous: Contribution | None, event: CheckpointEvent) -> dict:
    """What the aggregate row moves by when `event` replaces `previous`."""
    applied = Contribution(
        checkpoint_type=event.checkpoint_type,
        status=event.status,
        value_kg=event.quantity_kg(),
    ).deltas(sign=1)
    if previous is None:
        return applied
    reversal = previous.deltas(sign=-1)
    return {name: applied[name] + reversal[name] for name in applied}


def test_wastage_correction_18_to_12():
    previous = Contribution(
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value_kg=Decimal(18),
    )
    assert _net(previous, _event("12"))["food_wastage_kg"] == Decimal(-6)


def test_wastage_first_write():
    # FOOD_WASTAGE is not a COMPLETION_TYPE — total stays 0; quantity applies.
    deltas = _net(None, _event("20"))
    assert deltas["food_wastage_kg"] == Decimal(20)
    assert deltas["total_checkpoints"] == 0


def test_type_correction_reverses_the_old_column():
    previous = Contribution(
        checkpoint_type=CheckpointType.FOOD_PREPARED,
        status=CheckpointStatus.COMPLETED,
        value_kg=Decimal(100),
    )
    deltas = _net(previous, _event("5", checkpoint_type=CheckpointType.FOOD_WASTAGE))
    assert deltas["food_prepared_kg"] == Decimal(-100)
    assert deltas["food_wastage_kg"] == Decimal(5)


def test_status_flip_moves_one_completion_between_columns():
    previous = Contribution(
        checkpoint_type=CheckpointType.STAFF_HYGIENE,
        status=CheckpointStatus.PASS,
        value_kg=Decimal(0),
    )
    deltas = _net(
        previous,
        _event(
            "0",
            status=CheckpointStatus.FAIL,
            checkpoint_type=CheckpointType.STAFF_HYGIENE,
        ),
    )
    assert deltas["hygiene_pass_count"] == -1
    assert deltas["hygiene_fail_count"] == 1
    assert deltas["completed_checkpoints"] == -1
    assert deltas["failed_checkpoints"] == 1
    # The checkpoint still exists, so it must not be counted twice.
    assert deltas["total_checkpoints"] == 0


def test_grams_are_normalized_before_aggregation():
    deltas = _net(None, _event("45000", unit="G"))
    assert deltas["food_wastage_kg"] == Decimal("45.000")


def test_pounds_are_normalized_before_aggregation():
    deltas = _net(None, _event("10", unit="LB"))
    assert deltas["food_wastage_kg"] == Decimal("4.536")


def test_replaying_the_same_state_is_a_no_op():
    previous = Contribution(
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value_kg=Decimal(18),
    )
    deltas = _net(previous, _event("18"))
    assert all(not value for value in deltas.values())
