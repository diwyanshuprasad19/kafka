"""Cafe and client rollups.

The properties that matter are that a rollup equals the sum of its counters, that
re-running the worker does not double-count (it restates rather than accumulates),
and that a correction at the counter grain propagates upward.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.domain.business_day import business_date
from checkpoint_platform.domain.enums import (
    CheckpointStatus,
    CheckpointType,
    EventType,
    MealType,
)
from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.infrastructure.persistence.models import (
    CafeDailyAggregation,
    ClientDailyAggregation,
)
from checkpoint_platform.infrastructure.persistence.rollups import RollupRepo


def _wastage(session, counter: str, cafe: str, kg: str, *, version: int = 1) -> None:
    event = CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.UPDATED if version > 1 else EventType.COMPLETED,
        checkpoint_id=f"cp-{counter}-wastage",
        checkpoint_version=version,
        client_id="client-roll",
        cafe_id=cafe,
        counter_id=counter,
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value=Decimal(kg),
        unit="KG",
        occurred_at=datetime.now(UTC),
    )
    AggregationService(session).process(event)
    return event


def _cafe(session, cafe_id: str, day):
    return (
        session.query(CafeDailyAggregation)
        .filter_by(cafe_id=cafe_id, aggregation_date=day, meal_type="LUNCH")
        .one_or_none()
    )


def _client(session, client_id: str, day):
    return (
        session.query(ClientDailyAggregation)
        .filter_by(client_id=client_id, aggregation_date=day, meal_type="LUNCH")
        .one_or_none()
    )


@pytest.fixture()
def seeded(session):
    """Two counters in one cafe, one counter in a second cafe, all one client."""
    events = [
        _wastage(session, "counter-r1", "cafe-r1", "10"),
        _wastage(session, "counter-r2", "cafe-r1", "15"),
        _wastage(session, "counter-r3", "cafe-r2", "7"),
    ]
    session.flush()
    return business_date(events[0].occurred_at)


def test_cafe_rollup_sums_its_counters(session, seeded):
    RollupRepo(session).refresh(since=datetime(1970, 1, 1, tzinfo=UTC))

    cafe_r1 = _cafe(session, "cafe-r1", seeded)
    assert cafe_r1.counters == 2
    assert cafe_r1.food_wastage_kg == Decimal("25.000")

    cafe_r2 = _cafe(session, "cafe-r2", seeded)
    assert cafe_r2.counters == 1
    assert cafe_r2.food_wastage_kg == Decimal("7.000")


def test_client_rollup_sums_every_cafe(session, seeded):
    RollupRepo(session).refresh(since=datetime(1970, 1, 1, tzinfo=UTC))

    client = _client(session, "client-roll", seeded)
    assert client.cafes == 2
    assert client.counters == 3
    assert client.food_wastage_kg == Decimal("32.000")


def test_repeated_refresh_does_not_double_count(session, seeded):
    """The rollup restates, so running it repeatedly is idempotent."""
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    repo = RollupRepo(session)
    for _ in range(3):
        repo.refresh(since=epoch)

    assert _cafe(session, "cafe-r1", seeded).food_wastage_kg == Decimal("25.000")
    assert _client(session, "client-roll", seeded).food_wastage_kg == Decimal("32.000")


def test_correction_propagates_to_both_levels(session, seeded):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    repo = RollupRepo(session)
    repo.refresh(since=epoch)

    # 15 kg was wrong; it was really 5 kg.
    _wastage(session, "counter-r2", "cafe-r1", "5", version=2)
    session.flush()
    repo.refresh(since=epoch)

    assert _cafe(session, "cafe-r1", seeded).food_wastage_kg == Decimal("15.000")
    assert _client(session, "client-roll", seeded).food_wastage_kg == Decimal("22.000")


def _newest_counter_change(session) -> datetime:
    from sqlalchemy import func, select

    from checkpoint_platform.infrastructure.persistence.models import (
        DailyCounterAggregation,
    )

    return session.execute(select(func.max(DailyCounterAggregation.updated_at))).scalar_one()


def test_watermark_advances_and_stops_when_nothing_changed(session, seeded):
    repo = RollupRepo(session)
    first = repo.refresh()
    assert first.rows > 0

    # Ask for changes strictly newer than everything written so far.
    second = repo.refresh(since=_newest_counter_change(session))
    assert second.rows == 0


def test_watermark_is_rewound_so_a_late_commit_is_not_skipped(session, seeded):
    """A transaction can stamp updated_at and commit moments later. If the watermark
    advanced to the exact high-water mark, that row would never be rolled up."""
    result = RollupRepo(session).refresh()
    assert result.watermark < _newest_counter_change(session)


def test_rollup_only_restates_changed_keys(session, seeded):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    repo = RollupRepo(session)
    repo.refresh(since=epoch)

    # Everything seeded so far is now "old" as far as this pass is concerned.
    checkpoint = _newest_counter_change(session)
    untouched_before = _cafe(session, "cafe-r2", seeded).refreshed_at

    _wastage(session, "counter-r1", "cafe-r1", "1", version=2)
    session.flush()

    result = repo.refresh(since=checkpoint)

    assert result.cafe_rows == 1, "only the cafe whose counter changed"
    assert _cafe(session, "cafe-r2", seeded).refreshed_at == untouched_before


def test_api_falls_back_when_rollup_is_empty(session, seeded):
    """A dashboard must be correct even if the rollup worker never ran."""
    from checkpoint_platform.application.query_aggregates import AggregateQueryService

    class _NoCache:
        def get(self, *_a, **_k):
            return None

        def set(self, *_a, **_k):
            return None

        def get_cafe(self, *_a, **_k):
            return None

        def set_cafe(self, *_a, **_k):
            return None

    service = AggregateQueryService(session, _NoCache())
    payload = service.get_cafe("cafe-r1", str(seeded))
    assert payload["source"] == "counter_scan"
    assert payload["food_wastage_kg"] == 25.0

    RollupRepo(session).refresh(since=datetime(1970, 1, 1, tzinfo=UTC))
    session.flush()
    payload = service.get_cafe("cafe-r1", str(seeded))
    assert payload["source"] == "rollup"
    assert payload["food_wastage_kg"] == 25.0


def test_rollup_and_live_scan_agree(session, seeded):
    """The fast path and the fallback must not disagree about the same question."""
    from checkpoint_platform.application.query_aggregates import AggregateQueryService

    class _NoCache:
        def get(self, *_a, **_k):
            return None

        def set(self, *_a, **_k):
            return None

        def get_cafe(self, *_a, **_k):
            return None

        def set_cafe(self, *_a, **_k):
            return None

    service = AggregateQueryService(session, _NoCache())
    live = service._cafe_from_counters("cafe-r1", str(seeded))
    RollupRepo(session).refresh(since=datetime(1970, 1, 1, tzinfo=UTC))
    session.flush()
    rolled = service._cafe_from_rollup("cafe-r1", str(seeded))

    compared = [
        "total_checkpoints",
        "completed_checkpoints",
        "food_wastage_kg",
        "food_prepared_kg",
        "compliance_percentage",
        "wastage_percentage",
        "counters",
    ]
    assert {k: live[k] for k in compared} == {k: rolled[k] for k in compared}
