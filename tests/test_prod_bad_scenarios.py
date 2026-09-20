"""
At least 15 production bad scenarios — executable checks.

Uses FakePublisher + real AggregationService when Postgres is up;
unit-level checks always run (no Docker required).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from checkpoint_platform.application.bad_scenarios import (
    PROD_BAD_SCENARIOS,
    require_min_count,
    scenario_ids,
)
from checkpoint_platform.application.event_processing import EventProcessor
from checkpoint_platform.domain.enums import (
    CheckpointStatus,
    CheckpointType,
    EventType,
    MealType,
)
from checkpoint_platform.domain.events import CheckpointEvent, ReingestDlqRequest
from checkpoint_platform.domain.exceptions import (
    DuplicateEventError,
    PermanentValidationError,
    StaleVersionError,
    TransientProcessingError,
    is_transient_error,
    retry_delay_seconds,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakePublisher:
    def __init__(self) -> None:
        self.checkpoints: list[Any] = []
        self.retries: list[dict] = []
        self.dlqs: list[dict] = []
        self.aggregations: list[dict] = []

    def publish_checkpoint(self, event: dict | object) -> None:
        self.checkpoints.append(event)

    def publish_retry(self, event: dict, key: str) -> None:
        self.retries.append({"key": key, "event": event})

    def publish_dlq(self, dlq_payload: dict, key: str = "dlq") -> None:
        self.dlqs.append({"key": key, "payload": dlq_payload})

    def publish_aggregation(self, event: dict, key: str) -> None:
        self.aggregations.append({"key": key, "event": event})

    def flush(self, timeout: float = 10.0) -> int:
        return 0


def _wastage(
    *,
    checkpoint_id: str,
    counter_id: str,
    version: int,
    value: str,
    event_id=None,
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=checkpoint_id,
        checkpoint_version=version,
        client_id="client-bad",
        cafe_id="cafe-bad",
        counter_id=counter_id,
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.FOOD_WASTAGE,
        status=CheckpointStatus.COMPLETED,
        value=Decimal(value),
        unit="KG",
        occurred_at=datetime.now(timezone.utc),
    )


def _hygiene(
    *,
    checkpoint_id: str,
    counter_id: str,
    version: int,
    status: CheckpointStatus,
    event_id=None,
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=checkpoint_id,
        checkpoint_version=version,
        client_id="client-bad",
        cafe_id="cafe-bad",
        counter_id=counter_id,
        meal_type=MealType.LUNCH,
        checkpoint_type=CheckpointType.STAFF_HYGIENE,
        status=status,
        occurred_at=datetime.now(timezone.utc),
    )


@pytest.fixture()
def db_session(session):
    """Alias shared session fixture (uses aggregation_test, not demo DB)."""
    return session


def _read_agg(session, counter_id: str):
    from sqlalchemy import select

    from checkpoint_platform.infrastructure.persistence.models import DailyCounterAggregation

    return session.execute(
        select(DailyCounterAggregation).where(
            DailyCounterAggregation.counter_id == counter_id,
            DailyCounterAggregation.meal_type == "LUNCH",
        )
    ).scalar_one()


# ---------------------------------------------------------------------------
# Catalog integrity
# ---------------------------------------------------------------------------


def test_s00_catalog_has_at_least_15():
    require_min_count(15)
    assert len(PROD_BAD_SCENARIOS) >= 15
    assert len(set(scenario_ids())) == len(PROD_BAD_SCENARIOS)


# ---------------------------------------------------------------------------
# Always-on unit scenarios (no Postgres)
# ---------------------------------------------------------------------------


def test_s06_malformed_json_to_dlq():
    pub = FakePublisher()
    session = MagicMock()
    # DlqRepo.add will be called — allow it
    processor = EventProcessor(session=session, publisher=pub)
    outcome = processor.process_raw(
        raw_value=b"{not-json",
        topic="checkpoint.events.v1",
        partition=0,
        offset=1,
    )
    assert outcome == "dlq"
    assert len(pub.dlqs) == 1


def test_s07_schema_validation_to_dlq():
    pub = FakePublisher()
    session = MagicMock()
    processor = EventProcessor(session=session, publisher=pub)
    bad = json.dumps({"event_id": str(uuid4()), "counter_id": "x"}).encode()
    outcome = processor.process_raw(bad, "checkpoint.events.v1", 0, 2)
    assert outcome == "dlq"
    assert len(pub.dlqs) == 1


def test_s08_transient_db_retry(monkeypatch):
    from checkpoint_platform.application import event_processing as ep

    pub = FakePublisher()
    session = MagicMock()
    session.commit.side_effect = Exception("connection timed out")
    session.rollback = MagicMock()

    ev = _wastage(
        checkpoint_id="cp-t",
        counter_id="counter-t",
        version=1,
        value="1",
    )
    monkeypatch.setattr(
        ep,
        "AggregationService",
        lambda s: MagicMock(process=MagicMock(return_value={})),
    )
    # commit fails after process → transient → retry
    processor = EventProcessor(session=session, publisher=pub)
    raw = ev.model_dump(mode="json")
    raw["retry_count"] = 0
    outcome = processor.process_raw(
        json.dumps(raw).encode(),
        "checkpoint.events.v1",
        0,
        3,
    )
    assert outcome == "retry"
    assert len(pub.retries) == 1
    assert pub.retries[0]["event"]["retry_count"] == 1
    assert "next_retry_at" in pub.retries[0]["event"]


def test_s09_max_retries_dlq(monkeypatch):
    from checkpoint_platform.application import event_processing as ep
    from checkpoint_platform.config import get_settings

    pub = FakePublisher()
    session = MagicMock()
    session.commit.side_effect = Exception("connection timed out")
    monkeypatch.setattr(
        ep,
        "AggregationService",
        lambda s: MagicMock(process=MagicMock(return_value={})),
    )
    processor = EventProcessor(session=session, publisher=pub)
    ev = _wastage(checkpoint_id="cp-m", counter_id="counter-m", version=1, value="1")
    raw = ev.model_dump(mode="json")
    raw["retry_count"] = get_settings().max_retries
    outcome = processor.process_raw(
        json.dumps(raw).encode(),
        "checkpoint.events.v1",
        0,
        4,
    )
    assert outcome == "dlq"
    assert len(pub.dlqs) == 1


def test_s10_permanent_no_retry(monkeypatch):
    from checkpoint_platform.application import event_processing as ep

    pub = FakePublisher()
    session = MagicMock()
    monkeypatch.setattr(
        ep,
        "AggregationService",
        lambda s: MagicMock(
            process=MagicMock(side_effect=PermanentValidationError("impossible value"))
        ),
    )
    processor = EventProcessor(session=session, publisher=pub)
    ev = _wastage(checkpoint_id="cp-p", counter_id="counter-p", version=1, value="1")
    outcome = processor.process_raw(
        json.dumps(ev.model_dump(mode="json")).encode(),
        "checkpoint.events.v1",
        0,
        5,
    )
    assert outcome == "dlq"
    assert len(pub.retries) == 0


def test_s11_empty_counter_id():
    with pytest.raises(ValidationError):
        CheckpointEvent(
            event_id=uuid4(),
            event_type=EventType.COMPLETED,
            checkpoint_id="cp-1",
            checkpoint_version=1,
            client_id="c",
            cafe_id="cafe",
            counter_id="",
            meal_type=MealType.LUNCH,
            checkpoint_type=CheckpointType.FOOD_WASTAGE,
            status=CheckpointStatus.COMPLETED,
            value=Decimal("1"),
            unit="KG",
            occurred_at=datetime.now(timezone.utc),
        )


def test_s14_reingest_new_event_id():
    req = ReingestDlqRequest(dlq_ids=[10, 11], new_event_id=True)
    assert req.new_event_id is True
    assert req.force is False


def test_s15_retry_backoff_metadata():
    assert retry_delay_seconds(0, 500) == 0.5
    assert retry_delay_seconds(1, 500) == 1.0
    assert retry_delay_seconds(2, 500) == 2.0
    assert retry_delay_seconds(99, 500) == 30.0


def test_s18_is_transient_classification_matrix():
    assert is_transient_error(Exception("connection timed out")) is True
    assert is_transient_error(Exception("OperationalError: server closed")) is True
    assert is_transient_error(TransientProcessingError("x")) is True
    assert is_transient_error(PermanentValidationError("x")) is False
    assert is_transient_error(DuplicateEventError("x")) is False
    assert is_transient_error(StaleVersionError("x")) is False


# ---------------------------------------------------------------------------
# DB-backed scenarios
# ---------------------------------------------------------------------------


def test_s01_duplicate_event_id(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s01-{uuid4().hex[:8]}"
    cpid = f"cp-s01-{uuid4().hex[:8]}"
    eid = uuid4()
    svc = AggregationService(db_session)
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="10", event_id=eid))
    db_session.commit()
    with pytest.raises(DuplicateEventError):
        AggregationService(db_session).process(
            _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="10", event_id=eid)
        )
    db_session.rollback()


def test_s02_wastage_delta_correction(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s02-{uuid4().hex[:8]}"
    cpid = f"cp-s02-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="20"))
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=2, value="15"))
    db_session.commit()
    assert float(_read_agg(db_session, cid).food_wastage_kg) == 15.0


def test_s03_stale_version(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s03-{uuid4().hex[:8]}"
    cpid = f"cp-s03-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="5"))
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=2, value="8"))
    db_session.commit()
    with pytest.raises(StaleVersionError):
        AggregationService(db_session).process(
            _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="99")
        )
    db_session.commit()
    assert float(_read_agg(db_session, cid).food_wastage_kg) == 8.0


def test_s04_out_of_order_versions(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s04-{uuid4().hex[:8]}"
    cpid = f"cp-s04-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="10"))
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=3, value="30"))
    db_session.commit()
    with pytest.raises(StaleVersionError):
        AggregationService(db_session).process(
            _wastage(checkpoint_id=cpid, counter_id=cid, version=2, value="20")
        )
    db_session.commit()
    assert float(_read_agg(db_session, cid).food_wastage_kg) == 30.0


def test_s05_crash_before_commit(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s05-{uuid4().hex[:8]}"
    cpid = f"cp-s05-{uuid4().hex[:8]}"
    eid = uuid4()
    AggregationService(db_session).process(
        _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="5", event_id=eid)
    )
    db_session.rollback()
    AggregationService(db_session).process(
        _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="5", event_id=eid)
    )
    db_session.commit()
    assert float(_read_agg(db_session, cid).food_wastage_kg) == 5.0
    with pytest.raises(DuplicateEventError):
        AggregationService(db_session).process(
            _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="5", event_id=eid)
        )
    db_session.rollback()


def test_s12_status_flip_completed_to_failed(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s12-{uuid4().hex[:8]}"
    cpid = f"cp-s12-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(
        CheckpointEvent(
            event_id=uuid4(),
            event_type=EventType.COMPLETED,
            checkpoint_id=cpid,
            checkpoint_version=1,
            client_id="c",
            cafe_id="cafe",
            counter_id=cid,
            meal_type=MealType.LUNCH,
            checkpoint_type=CheckpointType.MEAL_READINESS,
            status=CheckpointStatus.COMPLETED,
            occurred_at=datetime.now(timezone.utc),
        )
    )
    svc.process(
        CheckpointEvent(
            event_id=uuid4(),
            event_type=EventType.FAILED,
            checkpoint_id=cpid,
            checkpoint_version=2,
            client_id="c",
            cafe_id="cafe",
            counter_id=cid,
            meal_type=MealType.LUNCH,
            checkpoint_type=CheckpointType.MEAL_READINESS,
            status=CheckpointStatus.FAILED,
            occurred_at=datetime.now(timezone.utc),
        )
    )
    db_session.commit()
    agg = _read_agg(db_session, cid)
    assert agg.completed_checkpoints == 0
    assert agg.failed_checkpoints == 1


def test_s13_hygiene_pass_then_fail(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s13-{uuid4().hex[:8]}"
    cpid = f"cp-s13-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(
        _hygiene(checkpoint_id=cpid, counter_id=cid, version=1, status=CheckpointStatus.PASS)
    )
    svc.process(
        _hygiene(checkpoint_id=cpid, counter_id=cid, version=2, status=CheckpointStatus.FAIL)
    )
    db_session.commit()
    agg = _read_agg(db_session, cid)
    assert agg.hygiene_pass_count == 0
    assert agg.hygiene_fail_count == 1


def test_s16_prepared_consumed_wastage(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s16-{uuid4().hex[:8]}"

    def qty(cp_type, cpid, ver, val):
        return CheckpointEvent(
            event_id=uuid4(),
            event_type=EventType.COMPLETED if ver == 1 else EventType.UPDATED,
            checkpoint_id=cpid,
            checkpoint_version=ver,
            client_id="c",
            cafe_id="cafe",
            counter_id=cid,
            meal_type=MealType.LUNCH,
            checkpoint_type=cp_type,
            status=CheckpointStatus.COMPLETED,
            value=Decimal(val),
            unit="KG",
            occurred_at=datetime.now(timezone.utc),
        )

    svc = AggregationService(db_session)
    svc.process(qty(CheckpointType.FOOD_PREPARED, f"cp-p-{cid}", 1, "100"))
    svc.process(qty(CheckpointType.FOOD_CONSUMED, f"cp-c-{cid}", 1, "80"))
    svc.process(qty(CheckpointType.FOOD_WASTAGE, f"cp-w-{cid}", 1, "20"))
    svc.process(qty(CheckpointType.FOOD_WASTAGE, f"cp-w-{cid}", 2, "15"))
    db_session.commit()
    agg = _read_agg(db_session, cid)
    assert float(agg.food_prepared_kg) == 100.0
    assert float(agg.food_consumed_kg) == 80.0
    assert float(agg.food_wastage_kg) == 15.0


def test_s17_same_version_different_event_id(db_session):
    from checkpoint_platform.application.aggregation import AggregationService

    cid = f"c-s17-{uuid4().hex[:8]}"
    cpid = f"cp-s17-{uuid4().hex[:8]}"
    svc = AggregationService(db_session)
    svc.process(_wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="10"))
    db_session.commit()
    with pytest.raises(StaleVersionError):
        AggregationService(db_session).process(
            _wastage(checkpoint_id=cpid, counter_id=cid, version=1, value="10")
        )
    db_session.commit()
    assert float(_read_agg(db_session, cid).food_wastage_kg) == 10.0
