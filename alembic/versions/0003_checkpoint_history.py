"""Add checkpoint_history table for queryable audit trail."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0003_checkpoint_history"
down_revision: str | None = "0002_dlq_reingest"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "checkpoint_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("checkpoint_id", sa.String(128), nullable=False),
        sa.Column("checkpoint_version", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("cafe_id", sa.String(64), nullable=False),
        sa.Column("counter_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32)),
        sa.Column("checkpoint_type", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("value", sa.Numeric(14, 3)),
        sa.Column("unit", sa.String(16)),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("correlation_id", sa.String(64)),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_history_counter_occurred",
        "checkpoint_history",
        ["counter_id", "occurred_at"],
    )
    op.create_index(
        "ix_history_cafe_occurred",
        "checkpoint_history",
        ["cafe_id", "occurred_at"],
    )
    op.create_index("ix_history_checkpoint_id", "checkpoint_history", ["checkpoint_id"])
    op.create_index("ix_history_event_id", "checkpoint_history", ["event_id"])
    op.create_index("ix_processed_events_checkpoint_id", "processed_events", ["checkpoint_id"])


def downgrade() -> None:
    # if_exists: databases created by create_all + stamp never ran this create_index.
    op.drop_index(
        "ix_processed_events_checkpoint_id",
        table_name="processed_events",
        if_exists=True,
    )
    op.drop_index("ix_history_event_id", table_name="checkpoint_history")
    op.drop_index("ix_history_checkpoint_id", table_name="checkpoint_history")
    op.drop_index("ix_history_cafe_occurred", table_name="checkpoint_history")
    op.drop_index("ix_history_counter_occurred", table_name="checkpoint_history")
    op.drop_table("checkpoint_history")
