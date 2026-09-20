"""Initial schema for checkpoint aggregation (Postgres / AlloyDB compatible)."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "checkpoint_state",
        sa.Column("checkpoint_id", sa.String(128), primary_key=True),
        sa.Column("checkpoint_version", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("cafe_id", sa.String(64), nullable=False),
        sa.Column("counter_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32)),
        sa.Column("checkpoint_type", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("value", sa.Numeric(14, 3)),
        sa.Column("unit", sa.String(16)),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_checkpoint_state_client_id", "checkpoint_state", ["client_id"])
    op.create_index("ix_checkpoint_state_cafe_id", "checkpoint_state", ["cafe_id"])
    op.create_index("ix_checkpoint_state_counter_id", "checkpoint_state", ["counter_id"])

    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("checkpoint_id", sa.String(128)),
    )

    op.create_table(
        "daily_counter_aggregation",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("aggregation_date", sa.Date(), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("cafe_id", sa.String(64), nullable=False),
        sa.Column("counter_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32), nullable=False),
        sa.Column("total_checkpoints", sa.Integer(), server_default="0"),
        sa.Column("completed_checkpoints", sa.Integer(), server_default="0"),
        sa.Column("failed_checkpoints", sa.Integer(), server_default="0"),
        sa.Column("food_prepared_kg", sa.Numeric(14, 3), server_default="0"),
        sa.Column("food_consumed_kg", sa.Numeric(14, 3), server_default="0"),
        sa.Column("food_wastage_kg", sa.Numeric(14, 3), server_default="0"),
        sa.Column("hygiene_pass_count", sa.Integer(), server_default="0"),
        sa.Column("hygiene_fail_count", sa.Integer(), server_default="0"),
        sa.Column("temperature_pass_count", sa.Integer(), server_default="0"),
        sa.Column("temperature_fail_count", sa.Integer(), server_default="0"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "aggregation_date",
            "counter_id",
            "meal_type",
            name="uq_daily_counter_meal",
        ),
    )
    op.create_index(
        "ix_agg_cafe_date",
        "daily_counter_aggregation",
        ["cafe_id", "aggregation_date"],
    )
    op.create_index(
        "ix_agg_client_date",
        "daily_counter_aggregation",
        ["client_id", "aggregation_date"],
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("published", sa.Boolean(), server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox_events",
        ["published", "created_at"],
    )

    op.create_table(
        "reporting_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("counter_id", sa.String(64), nullable=False),
        sa.Column("cafe_id", sa.String(64), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32), nullable=False),
        sa.Column("aggregation_date", sa.Date(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "aggregation_date",
            "counter_id",
            "meal_type",
            name="uq_reporting_counter_meal",
        ),
    )
    op.create_index("ix_reporting_snapshots_counter_id", "reporting_snapshots", ["counter_id"])
    op.create_index("ix_reporting_snapshots_cafe_id", "reporting_snapshots", ["cafe_id"])

    op.create_table(
        "dlq_records",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("original_topic", sa.String(128), nullable=False),
        sa.Column("original_partition", sa.Integer(), nullable=False),
        sa.Column("original_offset", sa.BigInteger(), nullable=False),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("retry_count", sa.Integer(), server_default="0"),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("original_event", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("dlq_records")
    op.drop_table("reporting_snapshots")
    op.drop_table("outbox_events")
    op.drop_table("daily_counter_aggregation")
    op.drop_table("processed_events")
    op.drop_table("checkpoint_state")
