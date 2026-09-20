from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from checkpoint_platform.domain.events import CheckpointEvent
from checkpoint_platform.domain.exceptions import DuplicateEventError
from checkpoint_platform.infrastructure.persistence.models import (
    CheckpointHistory,
    CheckpointState,
    DailyCounterAggregation,
    DlqRecord,
    OutboxEvent,
    ProcessedEvent,
)

# Single source of truth for the accumulated aggregate columns. The delta dict, the
# UPSERT arithmetic and the API response are all derived from these, so adding a
# metric cannot leave one of the three behind.
COUNTER_DELTA_COLUMNS = (
    "total_checkpoints",
    "completed_checkpoints",
    "failed_checkpoints",
    "pending_checkpoints",
    "hygiene_pass_count",
    "hygiene_fail_count",
    "temperature_pass_count",
    "temperature_fail_count",
    "incident_count",
    "open_incidents",
)
QUANTITY_DELTA_COLUMNS = (
    "food_received_kg",
    "food_prepared_kg",
    "food_consumed_kg",
    "food_wastage_kg",
)
DELTA_COLUMNS = COUNTER_DELTA_COLUMNS + QUANTITY_DELTA_COLUMNS


@dataclass(frozen=True)
class AggregationKey:
    """Identity of one aggregate row: a counter's meal on a business day."""

    aggregation_date: date
    counter_id: str
    meal_type: str

    def as_dict(self) -> dict:
        return {
            "aggregation_date": str(self.aggregation_date),
            "counter_id": self.counter_id,
            "meal_type": self.meal_type,
        }


@dataclass(frozen=True)
class AggregateTotals:
    """The aggregate fields the outbox event needs, read straight from RETURNING."""

    aggregation_date: date
    total_checkpoints: int
    completed_checkpoints: int
    food_prepared_kg: Decimal
    food_wastage_kg: Decimal

    @classmethod
    def from_row(cls, row) -> AggregateTotals:
        return cls(
            aggregation_date=row.aggregation_date,
            total_checkpoints=row.total_checkpoints or 0,
            completed_checkpoints=row.completed_checkpoints or 0,
            food_prepared_kg=row.food_prepared_kg or Decimal(0),
            food_wastage_kg=row.food_wastage_kg or Decimal(0),
        )


class CheckpointRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, checkpoint_id: str) -> CheckpointState | None:
        return self._select_one(checkpoint_id, for_update=False)

    def get_for_update(self, checkpoint_id: str) -> CheckpointState | None:
        """Read the row holding its lock, bypassing any stale identity-map copy.

        ``populate_existing`` matters when a consumer processes a batch of events in
        one session: without it the ORM would hand back the pre-upsert object and
        the checkpoint's previous contribution would be reversed twice.
        """
        return self._select_one(checkpoint_id, for_update=True)

    def _select_one(self, checkpoint_id: str, *, for_update: bool):
        stmt = select(CheckpointState).where(CheckpointState.checkpoint_id == checkpoint_id)
        if for_update:
            stmt = stmt.with_for_update()
        stmt = stmt.execution_options(populate_existing=True)
        return self.session.execute(stmt).scalar_one_or_none()

    def _row_values(
        self,
        event: CheckpointEvent,
        key: AggregationKey,
        value_kg: Decimal,
    ) -> dict:
        return {
            "checkpoint_id": event.checkpoint_id,
            "checkpoint_version": event.checkpoint_version,
            "client_id": event.client_id,
            "cafe_id": event.cafe_id,
            "counter_id": event.counter_id,
            "meal_type": key.meal_type,
            "checkpoint_type": event.checkpoint_type.value,
            "status": event.status.value,
            "value": event.value,
            "value_kg": value_kg,
            "unit": event.unit,
            "aggregation_date": key.aggregation_date,
            "updated_at": datetime.now(UTC),
        }

    def insert_if_absent(
        self,
        event: CheckpointEvent,
        key: AggregationKey,
        value_kg: Decimal,
    ) -> bool:
        """Claim a brand-new checkpoint. False means another writer got there first.

        This is what lets the caller trust "this checkpoint had no prior state":
        only one concurrent insert can return a row.
        """
        stmt = (
            insert(CheckpointState.__table__)
            .values(**self._row_values(event, key, value_kg))
            .on_conflict_do_nothing(index_elements=["checkpoint_id"])
            .returning(CheckpointState.__table__.c.checkpoint_id)
        )
        return self.session.execute(stmt).scalar_one_or_none() is not None

    def update_to(
        self,
        event: CheckpointEvent,
        key: AggregationKey,
        value_kg: Decimal,
    ) -> bool:
        """Advance an existing checkpoint, guarded so an older version can't win."""
        table = CheckpointState.__table__
        values = self._row_values(event, key, value_kg)
        values.pop("checkpoint_id")
        stmt = (
            table.update()
            .where(
                table.c.checkpoint_id == event.checkpoint_id,
                table.c.checkpoint_version < event.checkpoint_version,
            )
            .values(**values)
            .returning(table.c.checkpoint_id)
        )
        return self.session.execute(stmt).scalar_one_or_none() is not None

    def append_history(
        self,
        event: CheckpointEvent,
        *,
        aggregation_date: date | None = None,
        value_kg: Decimal | None = None,
        outcome: str = "applied",
    ) -> None:
        self.session.add(
            CheckpointHistory(
                event_id=event.event_id,
                checkpoint_id=event.checkpoint_id,
                checkpoint_version=event.checkpoint_version,
                client_id=event.client_id,
                cafe_id=event.cafe_id,
                counter_id=event.counter_id,
                meal_type=event.meal_type.value if event.meal_type else None,
                checkpoint_type=event.checkpoint_type.value,
                status=event.status.value,
                value=event.value,
                value_kg=value_kg,
                unit=event.unit,
                event_type=event.event_type.value,
                correlation_id=event.correlation_id,
                occurred_at=event.occurred_at,
                aggregation_date=aggregation_date,
                outcome=outcome,
            )
        )

    def list_by_counter(
        self,
        counter_id: str,
        *,
        limit: int = 100,
        checkpoint_type: str | None = None,
    ) -> list[CheckpointState]:
        stmt = select(CheckpointState).where(CheckpointState.counter_id == counter_id)
        if checkpoint_type:
            stmt = stmt.where(CheckpointState.checkpoint_type == checkpoint_type)
        stmt = stmt.order_by(CheckpointState.updated_at.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())


class HistoryRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_for_checkpoint(
        self, checkpoint_id: str, *, limit: int = 100
    ) -> list[CheckpointHistory]:
        stmt = (
            select(CheckpointHistory)
            .where(CheckpointHistory.checkpoint_id == checkpoint_id)
            .order_by(CheckpointHistory.checkpoint_version.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def list_for_counter(
        self,
        counter_id: str,
        *,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        meal_type: str | None = None,
        checkpoint_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CheckpointHistory]:
        stmt = select(CheckpointHistory).where(CheckpointHistory.counter_id == counter_id)
        if from_ts:
            stmt = stmt.where(CheckpointHistory.occurred_at >= from_ts)
        if to_ts:
            stmt = stmt.where(CheckpointHistory.occurred_at <= to_ts)
        if meal_type:
            stmt = stmt.where(CheckpointHistory.meal_type == meal_type)
        if checkpoint_type:
            stmt = stmt.where(CheckpointHistory.checkpoint_type == checkpoint_type)
        stmt = stmt.order_by(CheckpointHistory.occurred_at.desc()).offset(offset).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def list_for_cafe(
        self,
        cafe_id: str,
        *,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CheckpointHistory]:
        stmt = select(CheckpointHistory).where(CheckpointHistory.cafe_id == cafe_id)
        if from_ts:
            stmt = stmt.where(CheckpointHistory.occurred_at >= from_ts)
        if to_ts:
            stmt = stmt.where(CheckpointHistory.occurred_at <= to_ts)
        stmt = stmt.order_by(CheckpointHistory.occurred_at.desc()).offset(offset).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def prune_older_than(self, cutoff: datetime, *, limit: int = 50_000) -> int:
        ids = (
            select(CheckpointHistory.id)
            .where(CheckpointHistory.recorded_at < cutoff)
            .limit(limit)
            .scalar_subquery()
        )
        result = self.session.execute(
            delete(CheckpointHistory.__table__).where(CheckpointHistory.__table__.c.id.in_(ids))
        )
        return result.rowcount or 0


class AggregationRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, key: AggregationKey) -> DailyCounterAggregation | None:
        stmt = (
            select(DailyCounterAggregation)
            .where(
                DailyCounterAggregation.aggregation_date == key.aggregation_date,
                DailyCounterAggregation.counter_id == key.counter_id,
                DailyCounterAggregation.meal_type == key.meal_type,
            )
            # The row was just updated by a Core UPSERT, so refresh rather than
            # returning the identity map's pre-update snapshot.
            .execution_options(populate_existing=True)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def totals_for(self, key: AggregationKey) -> AggregateTotals | None:
        row = self.get(key)
        if row is None:
            return None
        return AggregateTotals.from_row(row)

    def apply_deltas(
        self,
        key: AggregationKey,
        *,
        client_id: str,
        cafe_id: str,
        deltas: dict,
    ) -> DailyCounterAggregation | None:
        """Add deltas to one aggregate row and return the result.

        Returning the updated row saves a follow-up SELECT for the outbox payload —
        one fewer round trip on every single event.
        """
        table = DailyCounterAggregation.__table__
        stmt = insert(table).values(
            aggregation_date=key.aggregation_date,
            client_id=client_id,
            cafe_id=cafe_id,
            counter_id=key.counter_id,
            meal_type=key.meal_type,
            updated_at=datetime.now(UTC),
            **{name: deltas[name] for name in DELTA_COLUMNS},
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_counter_meal",
            set_={
                # Every metric is accumulated by the database itself, so concurrent
                # consumers can never lose an update to a read-modify-write race.
                **{name: table.c[name] + stmt.excluded[name] for name in DELTA_COLUMNS},
                "updated_at": datetime.now(UTC),
                "client_id": stmt.excluded.client_id,
                "cafe_id": stmt.excluded.cafe_id,
            },
        ).returning(
            table.c.aggregation_date,
            table.c.total_checkpoints,
            table.c.completed_checkpoints,
            table.c.food_prepared_kg,
            table.c.food_wastage_kg,
        )
        row = self.session.execute(stmt).one_or_none()
        return AggregateTotals.from_row(row) if row is not None else None

    def list_by_cafe(self, aggregation_date: date, cafe_id: str) -> list[DailyCounterAggregation]:
        return list(
            self.session.execute(
                select(DailyCounterAggregation).where(
                    DailyCounterAggregation.aggregation_date == aggregation_date,
                    DailyCounterAggregation.cafe_id == cafe_id,
                )
            )
            .scalars()
            .all()
        )


class ProcessedEventRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def exists(self, event_id: UUID) -> bool:
        return self.session.get(ProcessedEvent, event_id) is not None

    def claim(self, event_id: UUID, checkpoint_id: str) -> bool:
        """Reserve an event_id for processing. False means it is a duplicate.

        A single INSERT ... ON CONFLICT DO NOTHING is the whole idempotency check.
        A read-then-insert pair would let two consumers both pass the read during a
        rebalance window and each apply the same event to the aggregate.
        """
        stmt = (
            insert(ProcessedEvent.__table__)
            .values(event_id=event_id, checkpoint_id=checkpoint_id)
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(ProcessedEvent.__table__.c.event_id)
        )
        return self.session.execute(stmt).scalar_one_or_none() is not None

    def mark(self, event_id: UUID, checkpoint_id: str) -> None:
        try:
            with self.session.begin_nested():
                self.session.add(ProcessedEvent(event_id=event_id, checkpoint_id=checkpoint_id))
                self.session.flush()
        except IntegrityError:
            raise DuplicateEventError(str(event_id)) from None

    def delete(self, event_id: UUID) -> bool:
        row = self.session.get(ProcessedEvent, event_id)
        if row is None:
            return False
        self.session.delete(row)
        return True

    def prune_older_than(self, cutoff: datetime, *, limit: int = 50_000) -> int:
        """Drop expired idempotency keys.

        At 50k events/minute this table gains ~72M rows/day, so the dedupe window
        is bounded by time. Deletes are chunked to avoid long lock holds.
        """
        ids = (
            select(ProcessedEvent.event_id)
            .where(ProcessedEvent.processed_at < cutoff)
            .limit(limit)
            .scalar_subquery()
        )
        result = self.session.execute(
            delete(ProcessedEvent.__table__).where(ProcessedEvent.__table__.c.event_id.in_(ids))
        )
        return result.rowcount or 0


class OutboxRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def enqueue(self, event_type: str, payload: dict) -> None:
        self.session.add(OutboxEvent(event_type=event_type, payload=payload, published=False))

    def fetch_unpublished(self, limit: int = 50) -> list[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published.is_(False))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(self.session.execute(stmt).scalars())

    def pending_count(self) -> int:
        from sqlalchemy import func as sa_func

        return int(
            self.session.execute(
                select(sa_func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.published.is_(False))
            ).scalar()
            or 0
        )

    def prune_published_older_than(self, cutoff: datetime, *, limit: int = 50_000) -> int:
        ids = (
            select(OutboxEvent.id)
            .where(OutboxEvent.published.is_(True), OutboxEvent.published_at < cutoff)
            .limit(limit)
            .scalar_subquery()
        )
        result = self.session.execute(
            delete(OutboxEvent.__table__).where(OutboxEvent.__table__.c.id.in_(ids))
        )
        return result.rowcount or 0


class DlqRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(
        self,
        *,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        error: str,
        retry_count: int,
        failed_at: datetime,
        original_event: dict,
    ) -> None:
        self.session.add(
            DlqRecord(
                original_topic=original_topic,
                original_partition=original_partition,
                original_offset=original_offset,
                error=error,
                retry_count=retry_count,
                failed_at=failed_at,
                original_event=original_event,
            )
        )

    def list_recent(self, limit: int = 50, status: str | None = None) -> list[DlqRecord]:
        stmt = select(DlqRecord).order_by(DlqRecord.id.desc()).limit(limit)
        if status:
            stmt = stmt.where(DlqRecord.reingest_status == status)
        return list(self.session.execute(stmt).scalars().all())

    def get_by_id(self, dlq_id: int) -> DlqRecord | None:
        return self.session.get(DlqRecord, dlq_id)

    def get_by_ids(self, dlq_ids: list[int]) -> list[DlqRecord]:
        if not dlq_ids:
            return []
        return list(
            self.session.execute(select(DlqRecord).where(DlqRecord.id.in_(dlq_ids))).scalars().all()
        )

    def mark_reingested(
        self,
        row: DlqRecord,
        *,
        event_id: str,
        correlation_id: str,
    ) -> None:
        from datetime import datetime

        row.reingest_status = "reingested"
        row.reingested_at = datetime.now(UTC)
        row.reingest_event_id = event_id
        row.reingest_correlation_id = correlation_id
        self.session.add(row)

    def prune_reingested_older_than(self, cutoff: datetime, *, limit: int = 10_000) -> int:
        """Only completed DLQ records expire; pending poison stays for triage."""
        ids = (
            select(DlqRecord.id)
            .where(
                DlqRecord.reingest_status.in_(("reingested", "skipped")),
                DlqRecord.created_at < cutoff,
            )
            .limit(limit)
            .scalar_subquery()
        )
        result = self.session.execute(
            delete(DlqRecord.__table__).where(DlqRecord.__table__.c.id.in_(ids))
        )
        return result.rowcount or 0

    def pending_count(self) -> int:
        from sqlalchemy import func as sa_func

        return int(
            self.session.execute(
                select(sa_func.count())
                .select_from(DlqRecord)
                .where(DlqRecord.reingest_status == "pending")
            ).scalar()
            or 0
        )
