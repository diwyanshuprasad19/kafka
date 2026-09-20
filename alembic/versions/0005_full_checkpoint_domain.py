"""Widen the aggregate to the full checkpoint domain.

Adds the metrics the operational model calls for but the schema was missing:
received quantity, pending checkpoints, and incident tracking (incidents are not
"failed checkpoints" — they have their own open/closed lifecycle).
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005_full_checkpoint_domain"
down_revision: Union[str, None] = "0004_exactness_and_retention"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NEW_COLUMNS = (
    "pending_checkpoints",
    "incident_count",
    "open_incidents",
)


def upgrade() -> None:
    for name in NEW_COLUMNS:
        op.add_column(
            "daily_counter_aggregation",
            sa.Column(name, sa.Integer(), nullable=False, server_default="0"),
        )
    op.add_column(
        "daily_counter_aggregation",
        sa.Column(
            "food_received_kg", sa.Numeric(14, 3), nullable=False, server_default="0"
        ),
    )
    # Redundant since 0004 added ix_dlq_status_created on (reingest_status,
    # created_at): a composite index already serves lookups on its leading column.
    op.drop_index("ix_dlq_reingest_status", table_name="dlq_records", if_exists=True)


def downgrade() -> None:
    op.create_index("ix_dlq_reingest_status", "dlq_records", ["reingest_status"])
    op.drop_column("daily_counter_aggregation", "food_received_kg")
    for name in reversed(NEW_COLUMNS):
        op.drop_column("daily_counter_aggregation", name)
