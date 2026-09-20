"""Enforce NOT NULL on accumulated columns.

These columns were created nullable while the ORM models declared them NOT NULL,
so ``create_all`` and ``alembic upgrade head`` produced different schemas. It is not
cosmetic: the aggregate UPSERT accumulates with ``column + excluded.column``, and in
SQL ``NULL + 5`` is NULL — a single null would silently erase a counter's running
total instead of raising. The database now refuses to hold one.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0006_not_null_counters"
down_revision: str | None = "0005_full_checkpoint_domain"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# table -> ((column, type, zero-value backfill), ...)
NOT_NULL_COLUMNS: dict[str, tuple[tuple[str, sa.types.TypeEngine, str], ...]] = {
    "daily_counter_aggregation": (
        ("total_checkpoints", sa.Integer(), "0"),
        ("completed_checkpoints", sa.Integer(), "0"),
        ("failed_checkpoints", sa.Integer(), "0"),
        ("food_prepared_kg", sa.Numeric(14, 3), "0"),
        ("food_consumed_kg", sa.Numeric(14, 3), "0"),
        ("food_wastage_kg", sa.Numeric(14, 3), "0"),
        ("hygiene_pass_count", sa.Integer(), "0"),
        ("hygiene_fail_count", sa.Integer(), "0"),
        ("temperature_pass_count", sa.Integer(), "0"),
        ("temperature_fail_count", sa.Integer(), "0"),
        ("updated_at", sa.DateTime(timezone=True), "now()"),
    ),
    "dlq_records": (
        ("retry_count", sa.Integer(), "0"),
        ("created_at", sa.DateTime(timezone=True), "now()"),
    ),
    "outbox_events": (("published", sa.Boolean(), "false"),),
    "reporting_snapshots": (("received_at", sa.DateTime(timezone=True), "now()"),),
}


def upgrade() -> None:
    for table, columns in NOT_NULL_COLUMNS.items():
        for column, type_, zero in columns:
            # Existing nulls must go before the constraint can be trusted.
            op.execute(f"UPDATE {table} SET {column} = {zero} WHERE {column} IS NULL")
            op.alter_column(table, column, existing_type=type_, nullable=False)


def downgrade() -> None:
    for table, columns in NOT_NULL_COLUMNS.items():
        for column, type_, _zero in columns:
            op.alter_column(table, column, existing_type=type_, nullable=True)
