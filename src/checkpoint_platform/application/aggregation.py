"""Stateful checkpoint aggregation.

The aggregate row is a running total, so every event is applied as
*reverse the checkpoint's previous contribution, then apply its new one*. That
single rule is what makes corrections safe in all their forms:

  value change     45kg → 40kg              -45 then +40 on the same row
  status flip      COMPLETED → FAILED       -completed then +failed
  type change      FOOD_PREPARED → WASTAGE  -prepared_kg then +wastage_kg
  meal correction  LUNCH → DINNER           reverse the LUNCH row, apply to DINNER
  day correction   near-midnight resend     reverse yesterday, apply to today

Computing a single "delta" against only the changed field, as an earlier version
did, silently corrupts the last three cases: the old row keeps a contribution
that no longer belongs to it.

Concurrency: the write to ``checkpoint_state`` is the serialization point. An
existing checkpoint is read ``FOR UPDATE`` before its previous contribution is
read, and a brand-new checkpoint is claimed with ``INSERT ... ON CONFLICT DO
NOTHING`` so that exactly one writer can believe "this checkpoint had no prior
state". Neither path needs an advisory lock.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from checkpoint_platform.domain.business_day import business_date
from checkpoint_platform.domain.enums import (
    COMPLETION_TYPES,
    HYGIENE_TYPES,
    INCIDENT_TYPES,
    OPEN_STATUSES,
    QUANTITY_TYPES,
    TEMPERATURE_TYPES,
    CheckpointStatus,
    CheckpointType,
)
from checkpoint_platform.domain.events import AggregationCompletedEvent, CheckpointEvent
from checkpoint_platform.domain.exceptions import (
    ConcurrentUpdateError,
    DuplicateEventError,
    StaleVersionError,
)
from checkpoint_platform.infrastructure.observability.logging import get_logger
from checkpoint_platform.infrastructure.persistence.models import CheckpointState
from checkpoint_platform.infrastructure.persistence.repositories import (
    COUNTER_DELTA_COLUMNS,
    QUANTITY_DELTA_COLUMNS,
    AggregateTotals,
    AggregationKey,
    AggregationRepo,
    CheckpointRepo,
    OutboxRepo,
    ProcessedEventRepo,
)

logger = get_logger(__name__)

ZERO = Decimal("0")

_QUANTITY_COLUMN_BY_TYPE = {
    CheckpointType.FOOD_RECEIVED: "food_received_kg",
    CheckpointType.FOOD_PREPARED: "food_prepared_kg",
    CheckpointType.FOOD_CONSUMED: "food_consumed_kg",
    CheckpointType.FOOD_WASTAGE: "food_wastage_kg",
}

# CLOSED counts as success so corrective-action closure feeds compliance.
SUCCESS_STATUSES = {
    CheckpointStatus.COMPLETED,
    CheckpointStatus.PASS,
    CheckpointStatus.CLOSED,
}
FAILURE_STATUSES = {CheckpointStatus.FAILED, CheckpointStatus.FAIL}


def empty_deltas() -> dict:
    deltas: dict = {name: 0 for name in COUNTER_DELTA_COLUMNS}
    deltas.update({name: ZERO for name in QUANTITY_DELTA_COLUMNS})
    return deltas


def _is_zero(deltas: dict) -> bool:
    return all(not value for value in deltas.values())


def _merge(into: dict, other: dict) -> dict:
    for name, value in other.items():
        into[name] = into[name] + value
    return into


@dataclass(frozen=True)
class Contribution:
    """What one checkpoint currently contributes to its aggregate row."""

    checkpoint_type: CheckpointType
    status: CheckpointStatus
    value_kg: Decimal

    def deltas(self, sign: int) -> dict:
        deltas = empty_deltas()
        is_success = self.status in SUCCESS_STATUSES
        is_failure = self.status in FAILURE_STATUSES

        if self.checkpoint_type in COMPLETION_TYPES:
            deltas["total_checkpoints"] = sign
            if is_success:
                deltas["completed_checkpoints"] = sign
            if is_failure:
                deltas["failed_checkpoints"] = sign
            if self.status == CheckpointStatus.PENDING:
                deltas["pending_checkpoints"] = sign

        if self.checkpoint_type in INCIDENT_TYPES:
            deltas["incident_count"] = sign
            if self.status in OPEN_STATUSES:
                deltas["open_incidents"] = sign

        if self.checkpoint_type in HYGIENE_TYPES:
            if is_success:
                deltas["hygiene_pass_count"] = sign
            if is_failure:
                deltas["hygiene_fail_count"] = sign

        if self.checkpoint_type in TEMPERATURE_TYPES:
            if is_success:
                deltas["temperature_pass_count"] = sign
            if is_failure:
                deltas["temperature_fail_count"] = sign

        if self.checkpoint_type in QUANTITY_TYPES:
            column = _QUANTITY_COLUMN_BY_TYPE[self.checkpoint_type]
            deltas[column] = self.value_kg * sign

        return deltas


@dataclass(frozen=True)
class PriorState:
    """Snapshot of a checkpoint taken under a row lock, before it is overwritten."""

    key: AggregationKey
    client_id: str
    cafe_id: str
    version: int
    contribution: Contribution

    @classmethod
    def from_row(cls, row: CheckpointState) -> "PriorState":
        return cls(
            key=AggregationKey(
                aggregation_date=row.aggregation_date,
                counter_id=row.counter_id,
                meal_type=row.meal_type or "NA",
            ),
            client_id=row.client_id,
            cafe_id=row.cafe_id,
            version=row.checkpoint_version,
            contribution=Contribution(
                checkpoint_type=CheckpointType(row.checkpoint_type),
                status=CheckpointStatus(row.status),
                value_kg=Decimal(row.value_kg if row.value_kg is not None else 0),
            ),
        )


class AggregationService:
    """
    Stateful checkpoint aggregation with:
    - event_id idempotency (atomic claim in processed_events)
    - checkpoint_version ordering (stale corrections rejected)
    - reverse-then-apply so value/status/type/meal/day corrections stay exact
    - atomic UPSERTs into daily_counter_aggregation
    - transactional outbox for aggregation.completed
    """

    def __init__(
        self,
        session: Session,
        *,
        checkpoint_repo: CheckpointRepo | None = None,
        aggregation_repo: AggregationRepo | None = None,
        processed_repo: ProcessedEventRepo | None = None,
        outbox_repo: OutboxRepo | None = None,
    ) -> None:
        self.session = session
        self.checkpoints = checkpoint_repo or CheckpointRepo(session)
        self.aggregations = aggregation_repo or AggregationRepo(session)
        self.processed = processed_repo or ProcessedEventRepo(session)
        self.outbox = outbox_repo or OutboxRepo(session)

    def process(self, event: CheckpointEvent) -> dict:
        # History and outbox rows are append-only and nothing in this method reads
        # them back, so autoflush is suppressed to let the ORM insert them together
        # at commit instead of one statement per event.
        with self.session.no_autoflush:
            return self._process(event)

    def _process(self, event: CheckpointEvent) -> dict:
        if not self.processed.claim(event.event_id, event.checkpoint_id):
            raise DuplicateEventError(str(event.event_id))

        agg_date = business_date(event.occurred_at)
        new_key = AggregationKey(
            aggregation_date=agg_date,
            counter_id=event.counter_id,
            meal_type=event.meal_type.value,
        )
        value_kg = event.quantity_kg()

        prior = self._claim_state(event, new_key, value_kg)

        if prior is not None and event.checkpoint_version <= prior.version:
            self.checkpoints.append_history(
                event, aggregation_date=agg_date, value_kg=value_kg, outcome="stale"
            )
            raise StaleVersionError(
                f"incoming={event.checkpoint_version} current={prior.version}"
            )

        applied, totals = self._apply_contributions(event, new_key, value_kg, prior)
        self.checkpoints.append_history(
            event, aggregation_date=agg_date, value_kg=value_kg, outcome="applied"
        )

        self._enqueue_outbox(event, totals or self.aggregations.totals_for(new_key))

        return {
            "checkpoint_id": event.checkpoint_id,
            "is_new": prior is None,
            "aggregation_date": str(agg_date),
            "deltas": {
                name: float(value) if isinstance(value, Decimal) else value
                for name, value in applied.items()
            },
        }

    def _claim_state(
        self,
        event: CheckpointEvent,
        key: AggregationKey,
        value_kg: Decimal,
    ) -> Optional[PriorState]:
        """Write checkpoint_state and return the contribution it replaced.

        Returns ``None`` when this event created the checkpoint. Raises
        ``ConcurrentUpdateError`` only if two writers keep trading the insert race,
        which the processor treats as transient and retries.

        The insert is attempted first because most events are a checkpoint's first
        report, and that ordering makes the common case a single statement. It is
        also the atomic claim: only one concurrent writer can be told it inserted.
        """
        for _ in range(3):
            if self.checkpoints.insert_if_absent(event, key, value_kg):
                return None

            existing = self.checkpoints.get_for_update(event.checkpoint_id)
            if existing is None:
                # Deleted between the insert attempt and the read; try again.
                continue

            prior = PriorState.from_row(existing)
            if event.checkpoint_version <= prior.version:
                return prior

            self.checkpoints.update_to(event, key, value_kg)
            return prior

        raise ConcurrentUpdateError(
            f"could not claim checkpoint_state for {event.checkpoint_id}"
        )

    def _apply_contributions(
        self,
        event: CheckpointEvent,
        new_key: AggregationKey,
        value_kg: Decimal,
        prior: Optional[PriorState],
    ) -> tuple[dict, Optional[AggregateTotals]]:
        application = Contribution(
            checkpoint_type=event.checkpoint_type,
            status=event.status,
            value_kg=value_kg,
        ).deltas(sign=1)

        if prior is None:
            totals = self.aggregations.apply_deltas(
                new_key,
                client_id=event.client_id,
                cafe_id=event.cafe_id,
                deltas=application,
            )
            return application, totals

        reversal = prior.contribution.deltas(sign=-1)

        if prior.key == new_key:
            combined = _merge(dict(application), reversal)
            if _is_zero(combined):
                # A replay of identical state: no arithmetic to do.
                return combined, None
            totals = self.aggregations.apply_deltas(
                new_key,
                client_id=event.client_id,
                cafe_id=event.cafe_id,
                deltas=combined,
            )
            return combined, totals

        # The correction moved the checkpoint to a different day, counter or meal:
        # the old row must give its contribution back before the new row takes it.
        self.aggregations.apply_deltas(
            prior.key,
            client_id=prior.client_id,
            cafe_id=prior.cafe_id,
            deltas=reversal,
        )
        totals = self.aggregations.apply_deltas(
            new_key,
            client_id=event.client_id,
            cafe_id=event.cafe_id,
            deltas=application,
        )
        logger.info(
            "aggregation_key_moved",
            checkpoint_id=event.checkpoint_id,
            from_key=prior.key.as_dict(),
            to_key=new_key.as_dict(),
        )
        return application, totals

    def _enqueue_outbox(
        self,
        event: CheckpointEvent,
        agg: Optional[AggregateTotals],
    ) -> None:
        if agg is None:
            return

        total = agg.total_checkpoints
        completed = agg.completed_checkpoints
        compliance = (completed / total * 100.0) if total else 0.0
        prepared = float(agg.food_prepared_kg)
        wastage = float(agg.food_wastage_kg)
        wastage_pct = (wastage / prepared * 100.0) if prepared else 0.0

        payload = AggregationCompletedEvent(
            counter_id=event.counter_id,
            cafe_id=event.cafe_id,
            client_id=event.client_id,
            meal_type=event.meal_type.value,
            aggregation_date=str(agg.aggregation_date),
            compliance_percentage=round(compliance, 2),
            wastage_percentage=round(wastage_pct, 2),
            total_checkpoints=total,
            completed_checkpoints=completed,
            food_wastage_kg=wastage,
        ).model_dump(mode="json")

        self.outbox.enqueue("aggregation.completed", payload)


def aggregation_date_for(event: CheckpointEvent) -> date:
    """Public helper so producers/scripts bucket events the same way."""
    return business_date(event.occurred_at)
