"""Exact corrections + retention support.

Adds the columns the aggregation service needs to reverse a checkpoint's previous
contribution exactly (normalized kilograms and the business day it was counted
under), records the outcome of rejected corrections, and indexes the columns the
maintenance worker prunes on.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0004_exactness_and_retention"
down_revision: str | None = "0003_checkpoint_history"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("checkpoint_state", sa.Column("value_kg", sa.Numeric(16, 3)))
    op.add_column("checkpoint_state", sa.Column("aggregation_date", sa.Date()))
    op.create_index(
        "ix_checkpoint_state_aggregation_date", "checkpoint_state", ["aggregation_date"]
    )

    op.add_column("checkpoint_history", sa.Column("value_kg", sa.Numeric(16, 3)))
    op.add_column("checkpoint_history", sa.Column("aggregation_date", sa.Date()))
    op.add_column(
        "checkpoint_history",
        sa.Column("outcome", sa.String(16), nullable=False, server_default="applied"),
    )
    op.create_index("ix_history_recorded_at", "checkpoint_history", ["recorded_at"])

    op.create_index("ix_processed_events_processed_at", "processed_events", ["processed_at"])
    op.create_index(
        "ix_outbox_pending_only",
        "outbox_events",
        ["id"],
        postgresql_where=sa.text("published = false"),
    )
    op.create_index("ix_outbox_published_at", "outbox_events", ["published_at"])
    op.create_index("ix_dlq_status_created", "dlq_records", ["reingest_status", "created_at"])

    # Existing rows predate unit normalization; they were all recorded in kg.
    op.execute("UPDATE checkpoint_state SET value_kg = value WHERE value IS NOT NULL")
    op.execute("UPDATE checkpoint_history SET value_kg = value WHERE value IS NOT NULL")

    # Backfill the business day these rows were already counted under, using the
    # same timezone the aggregation service resolves dates in.
    op.execute(
        """
        UPDATE checkpoint_state s
        SET aggregation_date = a.aggregation_date
        FROM daily_counter_aggregation a
        WHERE s.aggregation_date IS NULL
          AND a.counter_id = s.counter_id
          AND a.meal_type = s.meal_type
        """
    )
    op.execute(
        """
        UPDATE checkpoint_state
        SET aggregation_date = (updated_at AT TIME ZONE 'Asia/Kolkata')::date
        WHERE aggregation_date IS NULL
        """
    )
    op.execute(
        """
        UPDATE checkpoint_history
        SET aggregation_date = (occurred_at AT TIME ZONE 'Asia/Kolkata')::date
        WHERE aggregation_date IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ix_dlq_status_created", table_name="dlq_records")
    op.drop_index("ix_outbox_published_at", table_name="outbox_events")
    op.drop_index("ix_outbox_pending_only", table_name="outbox_events")
    op.drop_index("ix_processed_events_processed_at", table_name="processed_events")
    op.drop_index("ix_history_recorded_at", table_name="checkpoint_history")
    op.drop_column("checkpoint_history", "outcome")
    op.drop_column("checkpoint_history", "aggregation_date")
    op.drop_column("checkpoint_history", "value_kg")
    op.drop_index("ix_checkpoint_state_aggregation_date", table_name="checkpoint_state")
    op.drop_column("checkpoint_state", "aggregation_date")
    op.drop_column("checkpoint_state", "value_kg")
