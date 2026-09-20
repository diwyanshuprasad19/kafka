"""Cafe and client rollups of the counter-level aggregate.

Kept out of the ingest transaction on purpose: a cafe row is shared by thousands of
counters and a client row by every cafe, so updating them per event would serialise
concurrent consumers on a handful of hot rows. A worker restates them from the
counter grain instead.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007_daily_rollups"
down_revision: Union[str, None] = "0006_not_null_counters"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

COUNT_COLUMNS = (
    "counters",
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
QUANTITY_COLUMNS = (
    "food_received_kg",
    "food_prepared_kg",
    "food_consumed_kg",
    "food_wastage_kg",
)


def _metric_columns() -> list[sa.Column]:
    columns = [
        sa.Column(name, sa.Integer(), nullable=False, server_default="0")
        for name in COUNT_COLUMNS
    ]
    columns += [
        sa.Column(name, sa.Numeric(16, 3), nullable=False, server_default="0")
        for name in QUANTITY_COLUMNS
    ]
    columns.append(
        sa.Column(
            "refreshed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        )
    )
    return columns


def upgrade() -> None:
    op.create_table(
        "cafe_daily_aggregation",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("aggregation_date", sa.Date(), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("cafe_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32), nullable=False),
        *_metric_columns(),
        sa.UniqueConstraint(
            "aggregation_date", "cafe_id", "meal_type", name="uq_cafe_daily_meal"
        ),
    )
    op.create_index(
        "ix_cafe_rollup_client_date",
        "cafe_daily_aggregation",
        ["client_id", "aggregation_date"],
    )

    op.create_table(
        "client_daily_aggregation",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("aggregation_date", sa.Date(), nullable=False),
        sa.Column("client_id", sa.String(64), nullable=False),
        sa.Column("meal_type", sa.String(32), nullable=False),
        sa.Column("cafes", sa.Integer(), nullable=False, server_default="0"),
        *_metric_columns(),
        sa.UniqueConstraint(
            "aggregation_date", "client_id", "meal_type", name="uq_client_daily_meal"
        ),
    )

    op.create_table(
        "rollup_watermarks",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # The rollup worker scans counter rows by updated_at every pass.
    op.create_index(
        "ix_agg_updated_at", "daily_counter_aggregation", ["updated_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_agg_updated_at", table_name="daily_counter_aggregation")
    op.drop_table("rollup_watermarks")
    op.drop_table("client_daily_aggregation")
    op.drop_index("ix_cafe_rollup_client_date", table_name="cafe_daily_aggregation")
    op.drop_table("cafe_daily_aggregation")
