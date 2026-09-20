"""Query helpers — history row serialization (no DB required)."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from types import SimpleNamespace

from checkpoint_platform.application.query_aggregates import _agg_row, _history_row, _state_row


def test_history_row_serializer():
    row = SimpleNamespace(
        id=1,
        event_id=uuid4(),
        checkpoint_id="cp-1",
        checkpoint_version=2,
        client_id="c",
        cafe_id="cafe",
        counter_id="counter-1",
        meal_type="LUNCH",
        checkpoint_type="FOOD_WASTAGE",
        status="COMPLETED",
        value=Decimal("15.5"),
        unit="KG",
        event_type="checkpoint.updated",
        correlation_id="corr-1",
        occurred_at=datetime.now(timezone.utc),
        recorded_at=datetime.now(timezone.utc),
    )
    payload = _history_row(row)
    assert payload["checkpoint_id"] == "cp-1"
    assert payload["value"] == 15.5
    assert payload["correlation_id"] == "corr-1"


def test_state_row_serializer():
    row = SimpleNamespace(
        checkpoint_id="cp-1",
        checkpoint_version=1,
        client_id="c",
        cafe_id="cafe",
        counter_id="counter-1",
        meal_type="LUNCH",
        checkpoint_type="MEAL_READINESS",
        status="COMPLETED",
        value=None,
        unit=None,
        updated_at=datetime.now(timezone.utc),
    )
    payload = _state_row(row)
    assert payload["status"] == "COMPLETED"
    assert payload["value"] is None


def _aggregation_row(**overrides):
    """Build a real model instance so this test cannot drift from the schema."""
    from datetime import date

    from checkpoint_platform.infrastructure.persistence.models import (
        DailyCounterAggregation,
    )

    defaults = dict(
        aggregation_date=date(2026, 9, 15),
        client_id="c",
        cafe_id="cafe",
        counter_id="counter-1",
        meal_type="LUNCH",
        total_checkpoints=10,
        completed_checkpoints=9,
        failed_checkpoints=1,
        pending_checkpoints=0,
        food_received_kg=Decimal("110"),
        food_prepared_kg=Decimal("100"),
        food_consumed_kg=Decimal("90"),
        food_wastage_kg=Decimal("10"),
        hygiene_pass_count=1,
        hygiene_fail_count=0,
        temperature_pass_count=1,
        temperature_fail_count=0,
        incident_count=0,
        open_incidents=0,
        updated_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return DailyCounterAggregation(**defaults)


def test_agg_row_compliance():
    payload = _agg_row(_aggregation_row())
    assert payload["compliance_percentage"] == 90.0
    assert payload["wastage_percentage"] == 10.0
    assert payload["food_received_kg"] == 110.0


def test_agg_row_incident_closure():
    payload = _agg_row(_aggregation_row(incident_count=4, open_incidents=1))
    assert payload["incident_closure_percentage"] == 75.0


def test_agg_row_handles_zero_denominators():
    payload = _agg_row(
        _aggregation_row(
            total_checkpoints=0,
            completed_checkpoints=0,
            food_prepared_kg=Decimal("0"),
            incident_count=0,
        )
    )
    assert payload["compliance_percentage"] == 0.0
    assert payload["wastage_percentage"] == 0.0
    assert payload["incident_closure_percentage"] == 0.0
