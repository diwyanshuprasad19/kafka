"""Edge cases that silently corrupt aggregates if handled naively.

Every test here corresponds to a scenario in the bad-scenario catalog and asserts
against real Postgres, because the guarantees being tested (atomic claims, row
locks, version-guarded updates, ON CONFLICT arithmetic) do not exist in a mock.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.application.bad_scenarios import PROD_BAD_SCENARIOS, scenario_ids
from checkpoint_platform.application.event_processing import EventProcessor
from checkpoint_platform.domain.business_day import business_date
from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType, EventType, MealType
from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.domain.exceptions import DuplicateEventError, StaleVersionError
from checkpoint_platform.domain.units import to_kilograms
from checkpoint_platform.infrastructure.persistence.models import (
    CheckpointHistory,
    DailyCounterAggregation,
    DlqRecord,
)

COUNTER = "counter-edge"


def make_event(
    *,
    version: int = 1,
    value: str | None = "10",
    unit: str | None = "KG",
    checkpoint_type: CheckpointType = CheckpointType.FOOD_WASTAGE,
    status: CheckpointStatus = CheckpointStatus.COMPLETED,
    meal_type: MealType = MealType.LUNCH,
    checkpoint_id: str = "cp-edge",
    occurred_at: datetime | None = None,
    counter_id: str = COUNTER,
    event_id=None,
) -> CheckpointEvent:
    return CheckpointEvent(
        event_id=event_id or uuid4(),
        event_type=EventType.COMPLETED if version == 1 else EventType.UPDATED,
        checkpoint_id=checkpoint_id,
        checkpoint_version=version,
        client_id="client-edge",
        cafe_id="cafe-edge",
        counter_id=counter_id,
        meal_type=meal_type,
        checkpoint_type=checkpoint_type,
        status=status,
        value=Decimal(value) if value is not None else None,
        unit=unit,
        occurred_at=occurred_at or datetime.now(timezone.utc),
    )


def read_agg(session, *, date, meal_type="LUNCH", counter_id=COUNTER):
    return session.execute(
        select(DailyCounterAggregation)
        .where(
            DailyCounterAggregation.aggregation_date == date,
            DailyCounterAggregation.counter_id == counter_id,
            DailyCounterAggregation.meal_type == meal_type,
        )
        .execution_options(populate_existing=True)
    ).scalar_one_or_none()


def apply(session, event) -> None:
    AggregationService(session).process(event)
    session.flush()


# ---------------------------------------------------------------------------
# S19 — unit normalization
# ---------------------------------------------------------------------------


def test_s19_grams_are_stored_as_kilograms(session):
    event = make_event(value="45000", unit="G")
    apply(session, event)

    agg = read_agg(session, date=business_date(event.occurred_at))
    assert agg.food_wastage_kg == Decimal("45.000")


def test_s19_unknown_unit_is_permanent(session):
    with pytest.raises(Exception) as exc:
        make_event(value="10", unit="BUCKETS")
    assert "not valid" in str(exc.value) or "unsupported" in str(exc.value)


def test_s19_unit_correction_reverses_in_kilograms(session):
    # Reported in grams, corrected to the same reading in kilograms.
    first = make_event(version=1, value="45000", unit="G")
    apply(session, first)
    apply(session, make_event(version=2, value="45", unit="KG"))

    agg = read_agg(session, date=business_date(first.occurred_at))
    assert agg.food_wastage_kg == Decimal("45.000")


def test_s19_pound_conversion():
    assert to_kilograms(Decimal("10"), "LB") == Decimal("4.536")


# ---------------------------------------------------------------------------
# S20 / S21 / S22 — corrections that move the aggregate row
# ---------------------------------------------------------------------------


def test_s20_meal_correction_reverses_the_old_meal(session):
    first = make_event(version=1, value="12", meal_type=MealType.LUNCH)
    apply(session, first)
    apply(session, make_event(version=2, value="12", meal_type=MealType.DINNER))

    day = business_date(first.occurred_at)
    lunch = read_agg(session, date=day, meal_type="LUNCH")
    dinner = read_agg(session, date=day, meal_type="DINNER")

    assert lunch.food_wastage_kg == Decimal("0.000")
    assert dinner.food_wastage_kg == Decimal("12.000")


def test_s21_type_correction_reverses_the_old_column(session):
    first = make_event(
        version=1, value="100", checkpoint_type=CheckpointType.FOOD_PREPARED
    )
    apply(session, first)
    apply(
        session,
        make_event(version=2, value="5", checkpoint_type=CheckpointType.FOOD_WASTAGE),
    )

    agg = read_agg(session, date=business_date(first.occurred_at))
    assert agg.food_prepared_kg == Decimal("0.000")
    assert agg.food_wastage_kg == Decimal("5.000")


def test_s22_cross_day_correction_reverses_the_old_day(session):
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    first = make_event(version=1, value="30", occurred_at=yesterday)
    apply(session, first)

    today = datetime.now(timezone.utc)
    apply(session, make_event(version=2, value="30", occurred_at=today))

    old_day = read_agg(session, date=business_date(yesterday))
    new_day = read_agg(session, date=business_date(today))

    assert old_day.food_wastage_kg == Decimal("0.000")
    assert new_day.food_wastage_kg == Decimal("30.000")


def test_s22_completion_count_does_not_double_across_days(session):
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    first = make_event(
        version=1,
        value=None,
        unit=None,
        checkpoint_type=CheckpointType.STAFF_HYGIENE,
        status=CheckpointStatus.PASS,
        occurred_at=yesterday,
    )
    apply(session, first)
    apply(
        session,
        make_event(
            version=2,
            value=None,
            unit=None,
            checkpoint_type=CheckpointType.STAFF_HYGIENE,
            status=CheckpointStatus.PASS,
            occurred_at=datetime.now(timezone.utc),
        ),
    )

    old_day = read_agg(session, date=business_date(yesterday))
    new_day = read_agg(session, date=business_date(datetime.now(timezone.utc)))
    assert old_day.total_checkpoints == 0
    assert old_day.hygiene_pass_count == 0
    assert new_day.total_checkpoints == 1
    assert new_day.hygiene_pass_count == 1


# ---------------------------------------------------------------------------
# S23 — business-day boundary
# ---------------------------------------------------------------------------


def test_s23_late_evening_ist_stays_on_the_local_day():
    # 23:40 IST on the 20th is 18:10 UTC the same day.
    moment = datetime(2026, 3, 20, 18, 10, tzinfo=timezone.utc)
    assert business_date(moment, "Asia/Kolkata").isoformat() == "2026-03-20"


def test_s23_early_morning_ist_belongs_to_the_local_day():
    # 01:30 IST on the 21st is 20:00 UTC on the 20th — UTC would file it a day early.
    moment = datetime(2026, 3, 20, 20, 0, tzinfo=timezone.utc)
    assert business_date(moment, "Asia/Kolkata").isoformat() == "2026-03-21"
    assert moment.date().isoformat() == "2026-03-20"


def test_s23_naive_timestamps_are_read_as_utc():
    # The common producer bug is datetime.utcnow(), which returns naive UTC.
    naive_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    event = make_event(occurred_at=naive_utc)
    assert event.occurred_at.tzinfo is not None
    assert event.occurred_at == naive_utc.replace(tzinfo=timezone.utc)


def test_s23_naive_local_timestamp_is_rejected_rather_than_misbucketed():
    # A naive IST timestamp looks 5.5h in the future once read as UTC. Rejecting it
    # is safer than silently filing the event under the wrong business day.
    naive_ist = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)
    with pytest.raises(Exception) as exc:
        make_event(occurred_at=naive_ist)
    assert "naive local timestamp" in str(exc.value)


# ---------------------------------------------------------------------------
# S24 / S25 — hostile inputs rejected before they reach an aggregate
# ---------------------------------------------------------------------------


def test_s24_future_clock_skew_is_rejected():
    with pytest.raises(Exception) as exc:
        make_event(occurred_at=datetime.now(timezone.utc) + timedelta(days=365))
    assert "future" in str(exc.value)


def test_s24_small_skew_is_tolerated():
    event = make_event(occurred_at=datetime.now(timezone.utc) + timedelta(seconds=60))
    assert event.checkpoint_version == 1


def test_s25_negative_quantity_is_rejected():
    with pytest.raises(Exception) as exc:
        make_event(value="-5")
    assert "negative" in str(exc.value)


def test_s25_absurd_quantity_is_rejected():
    with pytest.raises(Exception) as exc:
        make_event(value="1000000000")
    assert "sanity ceiling" in str(exc.value)


def test_s25_quantity_type_requires_a_value():
    with pytest.raises(Exception) as exc:
        make_event(value=None, unit=None)
    assert "requires a numeric value" in str(exc.value)


def test_s25_nan_is_rejected():
    with pytest.raises(Exception):
        make_event(value="NaN")


# ---------------------------------------------------------------------------
# S26 / S27 / S33 — consumer-loop survival
# ---------------------------------------------------------------------------


class RecordingPublisher:
    def __init__(self, fail_dlq: bool = False) -> None:
        self.fail_dlq = fail_dlq
        self.dlqs: list[dict] = []
        self.retries: list[dict] = []

    def publish_checkpoint(self, event) -> None:
        pass

    def publish_retry(self, event, key) -> None:
        self.retries.append(event)

    def publish_dlq(self, payload, key="dlq") -> None:
        if self.fail_dlq:
            raise ConnectionError("broker unreachable")
        self.dlqs.append(payload)

    def publish_aggregation(self, event, key) -> None:
        pass

    def flush(self, timeout: float = 10.0) -> int:
        return 0


def test_s26_empty_payload_goes_to_dlq(session):
    publisher = RecordingPublisher()
    processor = EventProcessor(session, publisher)

    assert processor.process_raw(None, "checkpoint.events.v1", 0, 1) == "dlq"
    assert processor.process_raw(b"", "checkpoint.events.v1", 0, 2) == "dlq"

    rows = session.execute(select(DlqRecord)).scalars().all()
    assert len(rows) == 2
    assert all("EmptyPayload" in row.error for row in rows)


def test_s26_json_array_payload_goes_to_dlq(session):
    processor = EventProcessor(session, RecordingPublisher())
    assert processor.process_raw(b"[1,2,3]", "checkpoint.events.v1", 0, 3) == "dlq"


def test_s27_dlq_persists_even_when_the_broker_is_down(session):
    processor = EventProcessor(session, RecordingPublisher(fail_dlq=True))

    # Must still report "dlq" so the consumer can advance past the poison message.
    assert processor.process_raw(b"{not-json", "checkpoint.events.v1", 0, 9) == "dlq"

    row = session.execute(select(DlqRecord)).scalars().one()
    assert row.original_offset == 9


def test_s33_hostile_retry_count_is_coerced(session):
    processor = EventProcessor(session, RecordingPublisher())
    payload = json.dumps(
        {"event_id": str(uuid4()), "retry_count": "many", "counter_id": "c"}
    ).encode()

    # Routed on schema validation, not crashed on the retry_count read.
    assert processor.process_raw(payload, "checkpoint.events.v1", 0, 4) == "dlq"


def test_s33_retry_count_of_ignores_garbage():
    assert EventProcessor._retry_count_of({"retry_count": "many"}) == 0
    assert EventProcessor._retry_count_of({"retry_count": -5}) == 0
    assert EventProcessor._retry_count_of({"retry_count": None}) == 0
    assert EventProcessor._retry_count_of({"retry_count": 2}) == 2


# ---------------------------------------------------------------------------
# S28 / S29 / S30 — batching and concurrency
# ---------------------------------------------------------------------------


def test_s28_poison_event_does_not_discard_its_batch(session):
    publisher = RecordingPublisher()
    processor = EventProcessor(session, publisher, manage_transaction=False)

    good = make_event(value="10")
    outcomes = [
        processor.process_raw(good.model_dump_json().encode(), "checkpoint.events.v1", 0, 1),
        processor.process_raw(b"{broken", "checkpoint.events.v1", 0, 2),
        processor.process_raw(
            make_event(checkpoint_id="cp-edge-2", value="4").model_dump_json().encode(),
            "checkpoint.events.v1",
            0,
            3,
        ),
    ]
    session.commit()

    assert outcomes == ["processed", "dlq", "processed"]
    agg = read_agg(session, date=business_date(good.occurred_at))
    assert agg.food_wastage_kg == Decimal("14.000")
    assert session.execute(select(DlqRecord)).scalars().all()


def test_s29_two_versions_in_one_batch_apply_exactly_once(session):
    """The identity-map trap: v2 must reverse v1's real value, not a cached copy."""
    processor = EventProcessor(session, RecordingPublisher(), manage_transaction=False)

    first = make_event(version=1, value="45")
    second = make_event(version=2, value="40")
    processor.process_raw(first.model_dump_json().encode(), "checkpoint.events.v1", 0, 1)
    processor.process_raw(second.model_dump_json().encode(), "checkpoint.events.v1", 0, 2)
    session.commit()

    agg = read_agg(session, date=business_date(first.occurred_at))
    assert agg.food_wastage_kg == Decimal("40.000")


def test_s30_duplicate_claim_lets_exactly_one_writer_through(session):
    event = make_event(value="10")
    apply(session, event)

    # A redelivery of the identical event_id must not reach the aggregate.
    with pytest.raises(DuplicateEventError):
        AggregationService(session).process(event)
    session.rollback()

    apply(session, make_event(version=2, value="10", event_id=uuid4()))
    agg = read_agg(session, date=business_date(event.occurred_at))
    assert agg.food_wastage_kg == Decimal("10.000")


def test_s30_claim_is_atomic(session):
    repo_event_id = uuid4()
    from checkpoint_platform.infrastructure.persistence.repositories import (
        ProcessedEventRepo,
    )

    repo = ProcessedEventRepo(session)
    assert repo.claim(repo_event_id, "cp-claim") is True
    assert repo.claim(repo_event_id, "cp-claim") is False


# ---------------------------------------------------------------------------
# S31 — retry backoff must not stall the partition
# ---------------------------------------------------------------------------


def test_s31_not_due_retry_is_requeued_without_incrementing(session, monkeypatch):
    publisher = RecordingPublisher()
    processor = EventProcessor(session, publisher)
    monkeypatch.setattr(processor.settings, "retry_max_inline_wait_seconds", 0.01)

    payload = make_event(value="10").model_dump(mode="json")
    payload["retry_count"] = 1
    payload["next_retry_at"] = (
        datetime.now(timezone.utc) + timedelta(seconds=30)
    ).isoformat()

    outcome = processor.process_raw(
        json.dumps(payload).encode(), processor.settings.retry_topic, 0, 1
    )

    assert outcome == "retry_wait"
    assert publisher.retries[0]["retry_count"] == 1


def test_s31_due_retry_is_processed(session):
    processor = EventProcessor(session, RecordingPublisher())
    payload = make_event(value="10").model_dump(mode="json")
    payload["retry_count"] = 1
    payload["next_retry_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    outcome = processor.process_raw(
        json.dumps(payload).encode(), processor.settings.retry_topic, 0, 1
    )
    assert outcome == "processed"


def test_s31_corrupt_next_retry_at_does_not_crash(session):
    processor = EventProcessor(session, RecordingPublisher())
    assert processor._backoff_wait_seconds({"next_retry_at": "not-a-date"}, 0) == 0.0


# ---------------------------------------------------------------------------
# S32 — retention
# ---------------------------------------------------------------------------


def test_s32_maintenance_prunes_expired_idempotency_keys(session):
    from sqlalchemy import text

    from checkpoint_platform.interfaces.workers.maintenance import prune_once

    event = make_event(value="10")
    apply(session, event)
    session.commit()

    session.execute(
        text("UPDATE processed_events SET processed_at = now() - interval '30 days'")
    )
    session.execute(
        text("UPDATE checkpoint_history SET recorded_at = now() - interval '400 days'")
    )
    session.execute(
        text(
            "UPDATE outbox_events SET published = true, "
            "published_at = now() - interval '30 days'"
        )
    )
    session.commit()

    removed = prune_once(session)
    assert removed["processed_events"] == 1
    assert removed["checkpoint_history"] == 1
    assert removed["outbox_events"] == 1


def test_s32_pending_dlq_records_are_never_pruned(session):
    from sqlalchemy import text

    from checkpoint_platform.interfaces.workers.maintenance import prune_once

    EventProcessor(session, RecordingPublisher()).process_raw(
        b"{broken", "checkpoint.events.v1", 0, 1
    )
    session.execute(
        text("UPDATE dlq_records SET created_at = now() - interval '400 days'")
    )
    session.commit()

    assert prune_once(session)["dlq_records"] == 0
    assert session.execute(select(DlqRecord)).scalars().all()


# ---------------------------------------------------------------------------
# Audit trail
# ---------------------------------------------------------------------------


def test_rejected_correction_is_recorded_in_history(session):
    first = make_event(version=2, value="20")
    apply(session, first)

    with pytest.raises(StaleVersionError):
        AggregationService(session).process(make_event(version=1, value="99"))
    session.flush()

    rows = (
        session.execute(
            select(CheckpointHistory)
            .where(CheckpointHistory.checkpoint_id == "cp-edge")
            .order_by(CheckpointHistory.id)
        )
        .scalars()
        .all()
    )
    assert [row.outcome for row in rows] == ["applied", "stale"]

    agg = read_agg(session, date=business_date(first.occurred_at))
    assert agg.food_wastage_kg == Decimal("20.000")


def test_history_records_normalized_kilograms(session):
    apply(session, make_event(value="2500", unit="G"))
    row = session.execute(select(CheckpointHistory)).scalars().one()
    assert row.value == Decimal("2500.000")
    assert row.value_kg == Decimal("2.500")


# ---------------------------------------------------------------------------
# Full operational domain: incidents, pending work, received quantity
# ---------------------------------------------------------------------------


def test_incident_lifecycle_closes_out(session):
    """An incident opens, then a corrective action closes it — open count returns to 0."""
    opened = make_event(
        version=1,
        value=None,
        unit=None,
        checkpoint_type=CheckpointType.INCIDENT,
        status=CheckpointStatus.OPEN,
        checkpoint_id="cp-incident-1",
    )
    apply(session, opened)
    agg = read_agg(session, date=business_date(opened.occurred_at))
    assert (agg.incident_count, agg.open_incidents) == (1, 1)
    # An incident is not a failed compliance checkpoint.
    assert agg.total_checkpoints == 0
    assert agg.failed_checkpoints == 0

    apply(
        session,
        make_event(
            version=2,
            value=None,
            unit=None,
            checkpoint_type=CheckpointType.INCIDENT,
            status=CheckpointStatus.CLOSED,
            checkpoint_id="cp-incident-1",
        ),
    )
    agg = read_agg(session, date=business_date(opened.occurred_at))
    assert (agg.incident_count, agg.open_incidents) == (1, 0)


def test_pending_checkpoint_moves_to_completed(session):
    pending = make_event(
        version=1,
        value=None,
        unit=None,
        checkpoint_type=CheckpointType.MEAL_READINESS,
        status=CheckpointStatus.PENDING,
    )
    apply(session, pending)
    agg = read_agg(session, date=business_date(pending.occurred_at))
    assert (agg.total_checkpoints, agg.pending_checkpoints, agg.completed_checkpoints) == (1, 1, 0)

    apply(
        session,
        make_event(
            version=2,
            value=None,
            unit=None,
            checkpoint_type=CheckpointType.MEAL_READINESS,
            status=CheckpointStatus.COMPLETED,
        ),
    )
    agg = read_agg(session, date=business_date(pending.occurred_at))
    assert (agg.total_checkpoints, agg.pending_checkpoints, agg.completed_checkpoints) == (1, 0, 1)


def test_food_received_is_tracked_separately(session):
    received = make_event(
        value="500", checkpoint_type=CheckpointType.FOOD_RECEIVED
    )
    apply(session, received)
    agg = read_agg(session, date=business_date(received.occurred_at))
    assert agg.food_received_kg == Decimal("500.000")
    assert agg.food_prepared_kg == Decimal("0.000")


def test_corrective_action_closure_counts_as_compliance(session):
    action = make_event(
        value=None,
        unit=None,
        checkpoint_type=CheckpointType.CORRECTIVE_ACTION,
        status=CheckpointStatus.CLOSED,
    )
    apply(session, action)
    agg = read_agg(session, date=business_date(action.occurred_at))
    assert (agg.total_checkpoints, agg.completed_checkpoints) == (1, 1)


def test_vending_temperature_uses_temperature_columns(session):
    event = make_event(
        value="4",
        unit="C",
        checkpoint_type=CheckpointType.VENDING_TEMPERATURE,
        status=CheckpointStatus.FAIL,
    )
    apply(session, event)
    agg = read_agg(session, date=business_date(event.occurred_at))
    assert agg.temperature_fail_count == 1


def test_every_checkpoint_type_is_classified():
    """A new type must land in at least one aggregate bucket, or it silently no-ops."""
    from checkpoint_platform.domain.enums import (
        COMPLETION_TYPES,
        FEEDBACK_TYPES,
        HYGIENE_TYPES,
        INCIDENT_TYPES,
        QUANTITY_TYPES,
        TEMPERATURE_TYPES,
    )

    classified = (
        COMPLETION_TYPES
        | HYGIENE_TYPES
        | TEMPERATURE_TYPES
        | QUANTITY_TYPES
        | INCIDENT_TYPES
        | FEEDBACK_TYPES
    )
    unclassified = set(CheckpointType) - classified
    assert not unclassified, f"unclassified checkpoint types: {unclassified}"


def test_delta_columns_match_the_table():
    """The delta dict, the UPSERT and the table must agree on every metric."""
    from checkpoint_platform.application.aggregation import empty_deltas
    from checkpoint_platform.infrastructure.persistence.models import (
        DailyCounterAggregation,
    )
    from checkpoint_platform.infrastructure.persistence.repositories import DELTA_COLUMNS

    table_columns = set(DailyCounterAggregation.__table__.c.keys())
    assert set(DELTA_COLUMNS) <= table_columns
    assert set(empty_deltas()) == set(DELTA_COLUMNS)


def test_catalog_covers_every_new_scenario():
    ids = scenario_ids()
    assert len(ids) == len(set(ids))
    assert len(PROD_BAD_SCENARIOS) >= 33
    for expected in ("S19", "S26", "S30", "S32", "S33"):
        assert any(sid.startswith(expected) for sid in ids), expected
