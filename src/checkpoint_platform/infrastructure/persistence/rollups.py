"""Cafe and client rollups of the counter-level aggregate.

Why these are not maintained on the ingest path
-----------------------------------------------
The obvious implementation is to apply each event's deltas to the counter row, the
cafe row and the client row in one transaction. It is consistent, and it does not
scale: thousands of counters map to one cafe and every cafe maps to one client, so
each event would take a row lock on a row that every other event also wants. With
concurrent consumers the client row serialises the entire pipeline — the throughput
gain from partitioning by counter_id would be given straight back.

So the rollups are recomputed from the counter grain by a worker instead. Two
properties make that safe:

* Absolute, not incremental. A cafe row is set to whatever its counters currently
  sum to, so a re-run is a no-op rather than a double-count. A worker that dies
  half-way through simply repeats the pass.
* Watermarked and overlapping. Only keys whose counter rows changed since the last
  pass are touched, and the watermark is rewound slightly on each pass so a row
  committed just after the previous read is not skipped. Overlap is free precisely
  because recomputation is absolute.

The cost is that cafe and client figures lag the counter figures by one pass.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from checkpoint_platform.infrastructure.persistence.models import (
    CafeDailyAggregation,
    ClientDailyAggregation,
    DailyCounterAggregation,
    RollupWatermark,
)

# Summed straight through from the counter grain to both rollup levels.
SUMMED_COLUMNS = (
    "total_checkpoints",
    "completed_checkpoints",
    "failed_checkpoints",
    "pending_checkpoints",
    "food_received_kg",
    "food_prepared_kg",
    "food_consumed_kg",
    "food_wastage_kg",
    "hygiene_pass_count",
    "hygiene_fail_count",
    "temperature_pass_count",
    "temperature_fail_count",
    "incident_count",
    "open_incidents",
)

WATERMARK_NAME = "daily_rollups"

# A counter row committed just before the previous pass read the watermark would
# otherwise be missed; re-reading a little history is harmless here.
WATERMARK_OVERLAP = timedelta(seconds=5)

# Before the first pass there is no watermark, so start from the epoch.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class RollupResult:
    cafe_rows: int
    client_rows: int
    watermark: datetime

    @property
    def rows(self) -> int:
        return self.cafe_rows + self.client_rows


class RollupRepo:
    def __init__(self, session: Session) -> None:
        self.session = session

    # ----- watermark -----

    def watermark(self) -> datetime:
        row = self.session.get(RollupWatermark, WATERMARK_NAME)
        return row.watermark if row else _EPOCH

    def _save_watermark(self, value: datetime) -> None:
        table = RollupWatermark.__table__
        stmt = insert(table).values(
            name=WATERMARK_NAME,
            watermark=value,
            updated_at=datetime.now(timezone.utc),
        )
        self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[table.c.name],
                set_={
                    "watermark": stmt.excluded.watermark,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
        )

    def _high_water(self, since: datetime) -> datetime | None:
        """Newest counter-row change at or after `since`, or None if nothing changed."""
        return self.session.execute(
            select(func.max(DailyCounterAggregation.updated_at)).where(
                DailyCounterAggregation.updated_at > since
            )
        ).scalar_one_or_none()

    # ----- refresh -----

    def refresh(self, since: datetime | None = None) -> RollupResult:
        """Recompute the rollups for every key whose counters changed since `since`."""
        start = self.watermark() if since is None else since
        high_water = self._high_water(start)
        if high_water is None:
            return RollupResult(0, 0, start)

        cafe_rows = self._refresh_cafes(start)
        client_rows = self._refresh_clients(start)

        next_watermark = high_water - WATERMARK_OVERLAP
        self._save_watermark(next_watermark)
        return RollupResult(cafe_rows, client_rows, next_watermark)

    def _changed_keys(self, since: datetime, *columns):
        """The (date, meal, ...) keys touched since `since`, as a subquery."""
        return (
            select(*columns)
            .where(DailyCounterAggregation.updated_at > since)
            .distinct()
            .subquery()
        )

    def _refresh_cafes(self, since: datetime) -> int:
        counter = DailyCounterAggregation
        changed = self._changed_keys(
            since,
            counter.aggregation_date,
            counter.cafe_id,
            counter.meal_type,
        )

        # Recompute each touched cafe from *all* of its counters, not just the
        # changed ones, otherwise the sum would only cover the recent delta.
        source = (
            select(
                counter.aggregation_date,
                # A cafe belongs to one client, so this is a constant per group.
                func.min(counter.client_id).label("client_id"),
                counter.cafe_id,
                counter.meal_type,
                func.count().label("counters"),
                *[
                    func.sum(getattr(counter, name)).label(name)
                    for name in SUMMED_COLUMNS
                ],
            )
            .join(
                changed,
                (counter.aggregation_date == changed.c.aggregation_date)
                & (counter.cafe_id == changed.c.cafe_id)
                & (counter.meal_type == changed.c.meal_type),
            )
            .group_by(counter.aggregation_date, counter.cafe_id, counter.meal_type)
        )
        return self._upsert(CafeDailyAggregation, source, "uq_cafe_daily_meal")

    def _refresh_clients(self, since: datetime) -> int:
        counter = DailyCounterAggregation
        changed = self._changed_keys(
            since, counter.aggregation_date, counter.client_id, counter.meal_type
        )

        source = (
            select(
                counter.aggregation_date,
                counter.client_id,
                counter.meal_type,
                func.count().label("counters"),
                func.count(func.distinct(counter.cafe_id)).label("cafes"),
                *[
                    func.sum(getattr(counter, name)).label(name)
                    for name in SUMMED_COLUMNS
                ],
            )
            .join(
                changed,
                (counter.aggregation_date == changed.c.aggregation_date)
                & (counter.client_id == changed.c.client_id)
                & (counter.meal_type == changed.c.meal_type),
            )
            .group_by(counter.aggregation_date, counter.client_id, counter.meal_type)
        )
        return self._upsert(ClientDailyAggregation, source, "uq_client_daily_meal")

    def _upsert(self, model, source_select, constraint: str) -> int:
        table = model.__table__
        columns = [name for name in source_select.selected_columns.keys()]
        stmt = insert(table).from_select(columns, source_select)
        stmt = stmt.on_conflict_do_update(
            constraint=constraint,
            # Assignment, not accumulation: the rollup is a restatement of the
            # counter grain, so re-running it cannot drift.
            set_={
                **{name: getattr(stmt.excluded, name) for name in columns},
                "refreshed_at": datetime.now(timezone.utc),
            },
        )
        # rowcount is -1 for INSERT ... FROM SELECT, so count what came back. The
        # row count here is the number of cafe/client keys, not of counters, so
        # materialising the ids is cheap.
        return len(self.session.execute(stmt.returning(table.c.id)).scalars().all())
