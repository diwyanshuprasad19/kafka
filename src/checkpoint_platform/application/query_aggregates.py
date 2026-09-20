from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from checkpoint_platform.application.ports import AggregateCachePort
from checkpoint_platform.infrastructure.persistence.models import (
    CafeDailyAggregation,
    CheckpointHistory,
    CheckpointState,
    ClientDailyAggregation,
    DailyCounterAggregation,
    DlqRecord,
    OutboxEvent,
    ProcessedEvent,
    ReportingSnapshot,
)
from checkpoint_platform.infrastructure.persistence.repositories import (
    CheckpointRepo,
    DlqRepo,
    HistoryRepo,
)


def _age_seconds(rows) -> float:
    """How stale a rollup answer is — the oldest restatement among the rows read."""
    from datetime import datetime, timezone

    oldest = min(r.refreshed_at for r in rows)
    return round((datetime.now(timezone.utc) - oldest).total_seconds(), 1)


def _rollup_totals(rows) -> dict[str, Any]:
    """Sum a set of aggregate rows and derive the percentages.

    Works on counter rows and rollup rows alike, since both carry the same metric
    columns — so the live fallback and the rollup fast path cannot report different
    shapes for the same question.
    """
    totals = {name: sum(int(getattr(r, name) or 0) for r in rows) for name in _SUM_INT}
    totals.update(
        {name: float(sum(Decimal(str(getattr(r, name) or 0)) for r in rows)) for name in _SUM_DEC}
    )

    total = totals["total_checkpoints"]
    completed = totals["completed_checkpoints"]
    prepared = totals["food_prepared_kg"]
    wastage = totals["food_wastage_kg"]
    incidents = totals["incident_count"]
    open_incidents = totals["open_incidents"]

    totals["compliance_percentage"] = round(
        (completed / total * 100.0) if total else 0.0, 2
    )
    totals["wastage_percentage"] = round(
        (wastage / prepared * 100.0) if prepared else 0.0, 2
    )
    totals["incident_closure_percentage"] = round(
        ((incidents - open_incidents) / incidents * 100.0) if incidents else 0.0, 2
    )
    return totals


_SUM_INT = (
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
_SUM_DEC = (
    "food_received_kg",
    "food_prepared_kg",
    "food_consumed_kg",
    "food_wastage_kg",
)


def _agg_row(row: DailyCounterAggregation) -> dict[str, Any]:
    total = row.total_checkpoints or 0
    completed = row.completed_checkpoints or 0
    received = float(row.food_received_kg or 0)
    prepared = float(row.food_prepared_kg or 0)
    wastage = float(row.food_wastage_kg or 0)
    incidents = row.incident_count or 0
    open_incidents = row.open_incidents or 0
    return {
        "aggregation_date": str(row.aggregation_date),
        "client_id": row.client_id,
        "cafe_id": row.cafe_id,
        "counter_id": row.counter_id,
        "meal_type": row.meal_type,
        "total_checkpoints": total,
        "completed_checkpoints": completed,
        "failed_checkpoints": row.failed_checkpoints,
        "pending_checkpoints": row.pending_checkpoints or 0,
        "compliance_percentage": round((completed / total * 100.0) if total else 0.0, 2),
        "food_received_kg": received,
        "food_prepared_kg": prepared,
        "food_consumed_kg": float(row.food_consumed_kg or 0),
        "food_wastage_kg": wastage,
        "wastage_percentage": round((wastage / prepared * 100.0) if prepared else 0.0, 2),
        "hygiene_pass_count": row.hygiene_pass_count,
        "hygiene_fail_count": row.hygiene_fail_count,
        "temperature_pass_count": row.temperature_pass_count,
        "temperature_fail_count": row.temperature_fail_count,
        "incident_count": incidents,
        "open_incidents": open_incidents,
        "incident_closure_percentage": round(
            ((incidents - open_incidents) / incidents * 100.0) if incidents else 0.0, 2
        ),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _history_row(row: CheckpointHistory) -> dict[str, Any]:
    return {
        "id": row.id,
        "event_id": str(row.event_id),
        "checkpoint_id": row.checkpoint_id,
        "checkpoint_version": row.checkpoint_version,
        "client_id": row.client_id,
        "cafe_id": row.cafe_id,
        "counter_id": row.counter_id,
        "meal_type": row.meal_type,
        "checkpoint_type": row.checkpoint_type,
        "status": row.status,
        "value": float(row.value) if row.value is not None else None,
        "unit": row.unit,
        "event_type": row.event_type,
        "correlation_id": row.correlation_id,
        "occurred_at": row.occurred_at.isoformat() if row.occurred_at else None,
        "recorded_at": row.recorded_at.isoformat() if row.recorded_at else None,
    }


def _state_row(row: CheckpointState) -> dict[str, Any]:
    return {
        "checkpoint_id": row.checkpoint_id,
        "checkpoint_version": row.checkpoint_version,
        "client_id": row.client_id,
        "cafe_id": row.cafe_id,
        "counter_id": row.counter_id,
        "meal_type": row.meal_type,
        "checkpoint_type": row.checkpoint_type,
        "status": row.status,
        "value": float(row.value) if row.value is not None else None,
        "unit": row.unit,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


class AggregateQueryService:
    """Read-side queries: current aggregates, history, state, audit, DLQ, reporting."""

    def __init__(self, session: Session, cache: AggregateCachePort) -> None:
        self.session = session
        self.cache = cache
        self.dlq_repo = DlqRepo(session)
        self.history_repo = HistoryRepo(session)
        self.checkpoint_repo = CheckpointRepo(session)

    # ----- single-day aggregates (cached) -----

    def get_counter(
        self, counter_id: str, day: str, meal_type: str
    ) -> Optional[dict[str, Any]]:
        cached = self.cache.get(day, counter_id, meal_type)
        if cached:
            cached = dict(cached)
            cached["cache"] = "hit"
            return cached

        row = self.session.execute(
            select(DailyCounterAggregation).where(
                DailyCounterAggregation.aggregation_date == date.fromisoformat(day),
                DailyCounterAggregation.counter_id == counter_id,
                DailyCounterAggregation.meal_type == meal_type,
            )
        ).scalar_one_or_none()
        if not row:
            return None
        payload = _agg_row(row)
        payload["cache"] = "miss"
        self.cache.set(day, counter_id, meal_type, payload)
        return payload

    def get_cafe(self, cafe_id: str, day: str) -> Optional[dict[str, Any]]:
        cached = self.cache.get_cafe(day, cafe_id)
        if cached:
            cached = dict(cached)
            cached["cache"] = "hit"
            return cached

        payload = self._cafe_from_rollup(cafe_id, day) or self._cafe_from_counters(
            cafe_id, day
        )
        if payload is None:
            return None
        payload["cache"] = "miss"
        self.cache.set_cafe(day, cafe_id, payload)
        return payload

    def _cafe_from_rollup(self, cafe_id: str, day: str) -> Optional[dict[str, Any]]:
        rows = (
            self.session.execute(
                select(CafeDailyAggregation).where(
                    CafeDailyAggregation.aggregation_date == date.fromisoformat(day),
                    CafeDailyAggregation.cafe_id == cafe_id,
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return None
        payload = _rollup_totals(rows)
        payload.update(
            {
                "aggregation_date": day,
                "cafe_id": cafe_id,
                "client_id": rows[0].client_id,
                "counters": sum(r.counters for r in rows),
                "meals": {r.meal_type: _rollup_totals([r]) for r in rows},
                "source": "rollup",
                "rollup_age_seconds": _age_seconds(rows),
            }
        )
        return payload

    def _cafe_from_counters(self, cafe_id: str, day: str) -> Optional[dict[str, Any]]:
        """Fallback when the rollup worker has not covered this key yet.

        Correctness must not depend on a background worker having run, so the API
        can always compute the answer live from the counter grain.
        """
        rows = (
            self.session.execute(
                select(DailyCounterAggregation).where(
                    DailyCounterAggregation.aggregation_date == date.fromisoformat(day),
                    DailyCounterAggregation.cafe_id == cafe_id,
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return None
        payload = _rollup_totals(rows)
        payload.update(
            {
                "aggregation_date": day,
                "cafe_id": cafe_id,
                "client_id": rows[0].client_id,
                "counters": len(rows),
                "meals": self._meal_breakdown(rows),
                "source": "counter_scan",
            }
        )
        return payload

    def get_client(self, client_id: str, day: str) -> dict[str, Any]:
        rows = (
            self.session.execute(
                select(ClientDailyAggregation).where(
                    ClientDailyAggregation.aggregation_date == date.fromisoformat(day),
                    ClientDailyAggregation.client_id == client_id,
                )
            )
            .scalars()
            .all()
        )
        if rows:
            payload = _rollup_totals(rows)
            payload.update(
                {
                    "aggregation_date": day,
                    "client_id": client_id,
                    "cafes": max(r.cafes for r in rows),
                    "aggregation_rows": sum(r.counters for r in rows),
                    "source": "rollup",
                    "rollup_age_seconds": _age_seconds(rows),
                }
            )
            return payload

        counter_rows = (
            self.session.execute(
                select(DailyCounterAggregation).where(
                    DailyCounterAggregation.aggregation_date == date.fromisoformat(day),
                    DailyCounterAggregation.client_id == client_id,
                )
            )
            .scalars()
            .all()
        )
        payload = _rollup_totals(counter_rows)
        payload.update(
            {
                "aggregation_date": day,
                "client_id": client_id,
                "cafes": len({r.cafe_id for r in counter_rows}),
                "aggregation_rows": len(counter_rows),
                "source": "counter_scan",
            }
        )
        return payload

    # ----- date-range history (aggregates) -----

    def counter_history(
        self,
        counter_id: str,
        *,
        from_date: str,
        to_date: str,
        meal_type: str | None = None,
    ) -> dict[str, Any]:
        stmt = select(DailyCounterAggregation).where(
            DailyCounterAggregation.counter_id == counter_id,
            DailyCounterAggregation.aggregation_date >= date.fromisoformat(from_date),
            DailyCounterAggregation.aggregation_date <= date.fromisoformat(to_date),
        )
        if meal_type:
            stmt = stmt.where(DailyCounterAggregation.meal_type == meal_type)
        stmt = stmt.order_by(
            DailyCounterAggregation.aggregation_date.asc(),
            DailyCounterAggregation.meal_type.asc(),
        )
        rows = list(self.session.execute(stmt).scalars().all())
        return {
            "counter_id": counter_id,
            "from_date": from_date,
            "to_date": to_date,
            "meal_type": meal_type,
            "count": len(rows),
            "items": [_agg_row(r) for r in rows],
        }

    def cafe_history(
        self, cafe_id: str, *, from_date: str, to_date: str
    ) -> dict[str, Any]:
        rows = list(
            self.session.execute(
                select(DailyCounterAggregation).where(
                    DailyCounterAggregation.cafe_id == cafe_id,
                    DailyCounterAggregation.aggregation_date >= date.fromisoformat(from_date),
                    DailyCounterAggregation.aggregation_date <= date.fromisoformat(to_date),
                ).order_by(DailyCounterAggregation.aggregation_date.asc())
            )
            .scalars()
            .all()
        )
        # roll up per day
        by_day: dict[str, list] = {}
        for r in rows:
            by_day.setdefault(str(r.aggregation_date), []).append(r)
        days = []
        for day, day_rows in sorted(by_day.items()):
            total = sum(x.total_checkpoints for x in day_rows)
            completed = sum(x.completed_checkpoints for x in day_rows)
            prepared = sum(float(x.food_prepared_kg or 0) for x in day_rows)
            wastage = sum(float(x.food_wastage_kg or 0) for x in day_rows)
            days.append(
                {
                    "aggregation_date": day,
                    "counters": len({x.counter_id for x in day_rows}),
                    "total_checkpoints": total,
                    "completed_checkpoints": completed,
                    "compliance_percentage": round(
                        (completed / total * 100.0) if total else 0.0, 2
                    ),
                    "food_prepared_kg": prepared,
                    "food_wastage_kg": wastage,
                    "wastage_percentage": round(
                        (wastage / prepared * 100.0) if prepared else 0.0, 2
                    ),
                }
            )
        return {
            "cafe_id": cafe_id,
            "from_date": from_date,
            "to_date": to_date,
            "days": days,
            "day_count": len(days),
        }

    def client_history(
        self, client_id: str, *, from_date: str, to_date: str
    ) -> dict[str, Any]:
        rows = list(
            self.session.execute(
                select(DailyCounterAggregation).where(
                    DailyCounterAggregation.client_id == client_id,
                    DailyCounterAggregation.aggregation_date >= date.fromisoformat(from_date),
                    DailyCounterAggregation.aggregation_date <= date.fromisoformat(to_date),
                ).order_by(DailyCounterAggregation.aggregation_date.asc())
            )
            .scalars()
            .all()
        )
        by_day: dict[str, list] = {}
        for r in rows:
            by_day.setdefault(str(r.aggregation_date), []).append(r)
        days = []
        for day, day_rows in sorted(by_day.items()):
            total = sum(x.total_checkpoints for x in day_rows)
            completed = sum(x.completed_checkpoints for x in day_rows)
            prepared = sum(float(x.food_prepared_kg or 0) for x in day_rows)
            wastage = sum(float(x.food_wastage_kg or 0) for x in day_rows)
            days.append(
                {
                    "aggregation_date": day,
                    "cafes": len({x.cafe_id for x in day_rows}),
                    "counters": len({x.counter_id for x in day_rows}),
                    "total_checkpoints": total,
                    "completed_checkpoints": completed,
                    "compliance_percentage": round(
                        (completed / total * 100.0) if total else 0.0, 2
                    ),
                    "food_prepared_kg": prepared,
                    "food_wastage_kg": wastage,
                }
            )
        return {
            "client_id": client_id,
            "from_date": from_date,
            "to_date": to_date,
            "days": days,
            "day_count": len(days),
        }

    # ----- checkpoint current state + event history -----

    def get_checkpoint_state(self, checkpoint_id: str) -> Optional[dict[str, Any]]:
        row = self.session.get(CheckpointState, checkpoint_id)
        return _state_row(row) if row else None

    def list_counter_states(
        self, counter_id: str, *, checkpoint_type: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        rows = self.checkpoint_repo.list_by_counter(
            counter_id, limit=limit, checkpoint_type=checkpoint_type
        )
        return {
            "counter_id": counter_id,
            "count": len(rows),
            "items": [_state_row(r) for r in rows],
        }

    def checkpoint_event_history(
        self, checkpoint_id: str, *, limit: int = 100
    ) -> dict[str, Any]:
        rows = self.history_repo.list_for_checkpoint(checkpoint_id, limit=limit)
        return {
            "checkpoint_id": checkpoint_id,
            "count": len(rows),
            "items": [_history_row(r) for r in rows],
        }

    def counter_event_history(
        self,
        counter_id: str,
        *,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        meal_type: str | None = None,
        checkpoint_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        rows = self.history_repo.list_for_counter(
            counter_id,
            from_ts=from_ts,
            to_ts=to_ts,
            meal_type=meal_type,
            checkpoint_type=checkpoint_type,
            limit=limit,
            offset=offset,
        )
        return {
            "counter_id": counter_id,
            "limit": limit,
            "offset": offset,
            "count": len(rows),
            "items": [_history_row(r) for r in rows],
        }

    def cafe_event_history(
        self,
        cafe_id: str,
        *,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        rows = self.history_repo.list_for_cafe(
            cafe_id, from_ts=from_ts, to_ts=to_ts, limit=limit, offset=offset
        )
        return {
            "cafe_id": cafe_id,
            "limit": limit,
            "offset": offset,
            "count": len(rows),
            "items": [_history_row(r) for r in rows],
        }

    # ----- audit / ops reads -----

    def processed_event(self, event_id: str) -> Optional[dict[str, Any]]:
        try:
            uid = UUID(event_id)
        except ValueError:
            return None
        row = self.session.get(ProcessedEvent, uid)
        if not row:
            return None
        return {
            "event_id": str(row.event_id),
            "checkpoint_id": row.checkpoint_id,
            "processed_at": row.processed_at.isoformat() if row.processed_at else None,
        }

    def list_processed_for_checkpoint(
        self, checkpoint_id: str, *, limit: int = 50
    ) -> dict[str, Any]:
        rows = list(
            self.session.execute(
                select(ProcessedEvent)
                .where(ProcessedEvent.checkpoint_id == checkpoint_id)
                .order_by(ProcessedEvent.processed_at.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        return {
            "checkpoint_id": checkpoint_id,
            "count": len(rows),
            "items": [
                {
                    "event_id": str(r.event_id),
                    "processed_at": r.processed_at.isoformat() if r.processed_at else None,
                }
                for r in rows
            ],
        }

    def reporting_snapshot(
        self, counter_id: str, day: str, meal_type: str
    ) -> Optional[dict[str, Any]]:
        row = self.session.execute(
            select(ReportingSnapshot).where(
                ReportingSnapshot.counter_id == counter_id,
                ReportingSnapshot.aggregation_date == date.fromisoformat(day),
                ReportingSnapshot.meal_type == meal_type,
            )
        ).scalar_one_or_none()
        if not row:
            return None
        return {
            "counter_id": row.counter_id,
            "cafe_id": row.cafe_id,
            "client_id": row.client_id,
            "meal_type": row.meal_type,
            "aggregation_date": str(row.aggregation_date),
            "payload": row.payload,
            "received_at": row.received_at.isoformat() if row.received_at else None,
        }

    def outbox_stats(self) -> dict[str, Any]:
        pending = self.session.execute(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published.is_(False))
        ).scalar()
        published = self.session.execute(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published.is_(True))
        ).scalar()
        return {
            "pending": int(pending or 0),
            "published": int(published or 0),
        }

    def list_dlq(self, limit: int = 50, status: str | None = None) -> list[dict[str, Any]]:
        rows = self.dlq_repo.list_recent(limit=limit, status=status)
        return [
            {
                "id": r.id,
                "error": r.error,
                "retry_count": r.retry_count,
                "original_topic": r.original_topic,
                "original_partition": r.original_partition,
                "original_offset": r.original_offset,
                "failed_at": r.failed_at.isoformat(),
                "original_event": r.original_event,
                "reingest_status": getattr(r, "reingest_status", "pending"),
                "reingested_at": r.reingested_at.isoformat()
                if getattr(r, "reingested_at", None)
                else None,
                "reingest_event_id": getattr(r, "reingest_event_id", None),
                "reingest_correlation_id": getattr(r, "reingest_correlation_id", None),
            }
            for r in rows
        ]

    def get_dlq(self, dlq_id: int) -> Optional[dict[str, Any]]:
        row = self.dlq_repo.get_by_id(dlq_id)
        if not row:
            return None
        return {
            "id": row.id,
            "error": row.error,
            "retry_count": row.retry_count,
            "original_topic": row.original_topic,
            "original_partition": row.original_partition,
            "original_offset": row.original_offset,
            "failed_at": row.failed_at.isoformat(),
            "original_event": row.original_event,
            "reingest_status": row.reingest_status,
            "reingested_at": row.reingested_at.isoformat() if row.reingested_at else None,
        }

    @staticmethod
    def _meal_breakdown(rows: list[DailyCounterAggregation]) -> list[dict[str, Any]]:
        by_meal: dict[str, list] = {}
        for r in rows:
            by_meal.setdefault(r.meal_type, []).append(r)
        out = []
        for meal, meal_rows in sorted(by_meal.items()):
            total = sum(x.total_checkpoints for x in meal_rows)
            completed = sum(x.completed_checkpoints for x in meal_rows)
            out.append(
                {
                    "meal_type": meal,
                    "counters": len(meal_rows),
                    "total_checkpoints": total,
                    "completed_checkpoints": completed,
                    "compliance_percentage": round(
                        (completed / total * 100.0) if total else 0.0, 2
                    ),
                }
            )
        return out
