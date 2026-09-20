from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from checkpoint_platform.infrastructure.persistence.base import Base


class CheckpointState(Base):
    __tablename__ = "checkpoint_state"

    checkpoint_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    checkpoint_version: Mapped[int] = mapped_column(Integer, nullable=False)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    cafe_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    counter_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    meal_type: Mapped[str | None] = mapped_column(String(32))
    checkpoint_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    value: Mapped[Decimal | None] = mapped_column(Numeric(14, 3))
    unit: Mapped[str | None] = mapped_column(String(16))
    # Normalized to kilograms once, on write, so reversing a checkpoint's previous
    # contribution never depends on re-interpreting the reported unit.
    value_kg: Mapped[Decimal | None] = mapped_column(Numeric(16, 3))
    # Business day this checkpoint currently counts towards; needed to reverse the
    # right aggregate row when a correction moves the checkpoint to another day.
    aggregation_date: Mapped[date | None] = mapped_column(Date, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class CheckpointHistory(Base):
    """Append-only history of every successfully applied checkpoint event (queryable audit)."""

    __tablename__ = "checkpoint_history"
    __table_args__ = (
        Index("ix_history_counter_occurred", "counter_id", "occurred_at"),
        Index("ix_history_cafe_occurred", "cafe_id", "occurred_at"),
        Index("ix_history_checkpoint_id", "checkpoint_id"),
        Index("ix_history_event_id", "event_id"),
        # Retention pruning scans by recorded_at.
        Index("ix_history_recorded_at", "recorded_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    checkpoint_id: Mapped[str] = mapped_column(String(128), nullable=False)
    checkpoint_version: Mapped[int] = mapped_column(Integer, nullable=False)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    cafe_id: Mapped[str] = mapped_column(String(64), nullable=False)
    counter_id: Mapped[str] = mapped_column(String(64), nullable=False)
    meal_type: Mapped[str | None] = mapped_column(String(32))
    checkpoint_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    value: Mapped[Decimal | None] = mapped_column(Numeric(14, 3))
    value_kg: Mapped[Decimal | None] = mapped_column(Numeric(16, 3))
    unit: Mapped[str | None] = mapped_column(String(16))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    aggregation_date: Mapped[date | None] = mapped_column(Date)
    # applied | stale — a rejected correction is still recorded so operators can
    # answer "why didn't my update show up?" without reading consumer logs.
    outcome: Mapped[str] = mapped_column(String(16), nullable=False, server_default="applied")
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (
        Index("ix_processed_events_processed_at", "processed_at"),
        Index("ix_processed_events_checkpoint_id", "checkpoint_id"),
    )

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    checkpoint_id: Mapped[str | None] = mapped_column(String(128))


class DailyCounterAggregation(Base):
    __tablename__ = "daily_counter_aggregation"
    __table_args__ = (
        UniqueConstraint(
            "aggregation_date",
            "counter_id",
            "meal_type",
            name="uq_daily_counter_meal",
        ),
        Index("ix_agg_cafe_date", "cafe_id", "aggregation_date"),
        Index("ix_agg_client_date", "client_id", "aggregation_date"),
        # The rollup worker scans by updated_at on every pass.
        Index("ix_agg_updated_at", "updated_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    aggregation_date: Mapped[date] = mapped_column(Date, nullable=False)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    cafe_id: Mapped[str] = mapped_column(String(64), nullable=False)
    counter_id: Mapped[str] = mapped_column(String(64), nullable=False)
    meal_type: Mapped[str] = mapped_column(String(32), nullable=False)

    total_checkpoints: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    completed_checkpoints: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    failed_checkpoints: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    pending_checkpoints: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    food_received_kg: Mapped[Decimal] = mapped_column(Numeric(14, 3), default=0, server_default="0")
    food_prepared_kg: Mapped[Decimal] = mapped_column(Numeric(14, 3), default=0, server_default="0")
    food_consumed_kg: Mapped[Decimal] = mapped_column(Numeric(14, 3), default=0, server_default="0")
    food_wastage_kg: Mapped[Decimal] = mapped_column(Numeric(14, 3), default=0, server_default="0")

    hygiene_pass_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    hygiene_fail_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    temperature_pass_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    temperature_fail_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    # Incidents are counted separately from compliance; open_incidents falls back to
    # zero as corrective actions close them, via the same reverse-then-apply rule.
    incident_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    open_incidents: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class _RollupColumns:
    """Metrics shared by the cafe and client rollups.

    Every column here is the plain SUM of the counter-level rows beneath it, plus a
    count of how many counters contributed. Rollups are recomputed absolutely rather
    than accumulated with deltas: a cafe row is whatever its counters currently add
    up to, which makes a re-run harmless if the rollup worker dies mid-pass.
    """

    counters: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    total_checkpoints: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completed_checkpoints: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    failed_checkpoints: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    pending_checkpoints: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    food_received_kg: Mapped[Decimal] = mapped_column(
        Numeric(16, 3), nullable=False, server_default="0"
    )
    food_prepared_kg: Mapped[Decimal] = mapped_column(
        Numeric(16, 3), nullable=False, server_default="0"
    )
    food_consumed_kg: Mapped[Decimal] = mapped_column(
        Numeric(16, 3), nullable=False, server_default="0"
    )
    food_wastage_kg: Mapped[Decimal] = mapped_column(
        Numeric(16, 3), nullable=False, server_default="0"
    )

    hygiene_pass_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    hygiene_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    temperature_pass_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    temperature_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    incident_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    refreshed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CafeDailyAggregation(Base, _RollupColumns):
    """Cafe-level rollup of daily_counter_aggregation.

    Maintained by the rollup worker, not by the ingest path. Writing it inline would
    funnel every event for a cafe onto one row, and with concurrent consumers that
    hot row becomes the bottleneck — the counter grain exists precisely so ingest
    spreads across many rows.
    """

    __tablename__ = "cafe_daily_aggregation"
    __table_args__ = (
        UniqueConstraint("aggregation_date", "cafe_id", "meal_type", name="uq_cafe_daily_meal"),
        Index("ix_cafe_rollup_client_date", "client_id", "aggregation_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    aggregation_date: Mapped[date] = mapped_column(Date, nullable=False)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    cafe_id: Mapped[str] = mapped_column(String(64), nullable=False)
    meal_type: Mapped[str] = mapped_column(String(32), nullable=False)


class ClientDailyAggregation(Base, _RollupColumns):
    """Client-level rollup, one grain above the cafe."""

    __tablename__ = "client_daily_aggregation"
    __table_args__ = (
        UniqueConstraint("aggregation_date", "client_id", "meal_type", name="uq_client_daily_meal"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    aggregation_date: Mapped[date] = mapped_column(Date, nullable=False)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    meal_type: Mapped[str] = mapped_column(String(32), nullable=False)
    cafes: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")


class RollupWatermark(Base):
    """How far the rollup worker has consumed daily_counter_aggregation.updated_at."""

    __tablename__ = "rollup_watermarks"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class OutboxEvent(Base):
    """Transactional outbox — published after DB commit by outbox publisher."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index("ix_outbox_unpublished", "published", "created_at"),
        # The publisher only ever scans unpublished rows; a partial index keeps that
        # lookup constant-time as the published backlog grows.
        Index(
            "ix_outbox_pending_only",
            "id",
            postgresql_where=text("published = false"),
        ),
        Index("ix_outbox_published_at", "published_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ReportingSnapshot(Base):
    """Downstream reporting consumer materializes latest aggregates here."""

    __tablename__ = "reporting_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    counter_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    cafe_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    meal_type: Mapped[str] = mapped_column(String(32), nullable=False)
    aggregation_date: Mapped[date] = mapped_column(Date, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "aggregation_date",
            "counter_id",
            "meal_type",
            name="uq_reporting_counter_meal",
        ),
    )


class DlqRecord(Base):
    __tablename__ = "dlq_records"
    __table_args__ = (Index("ix_dlq_status_created", "reingest_status", "created_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    original_topic: Mapped[str] = mapped_column(String(128), nullable=False)
    original_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    original_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    original_event: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    # Re-ingestion tracking
    reingest_status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="pending"
    )  # pending | reingested | skipped
    reingested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reingest_event_id: Mapped[str | None] = mapped_column(String(64))
    reingest_correlation_id: Mapped[str | None] = mapped_column(String(64))
