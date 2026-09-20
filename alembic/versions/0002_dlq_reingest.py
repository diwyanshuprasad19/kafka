"""Add DLQ re-ingestion tracking columns."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0002_dlq_reingest"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "dlq_records",
        sa.Column("reingest_status", sa.String(32), server_default="pending", nullable=False),
    )
    op.add_column("dlq_records", sa.Column("reingested_at", sa.DateTime(timezone=True)))
    op.add_column("dlq_records", sa.Column("reingest_event_id", sa.String(64)))
    op.add_column("dlq_records", sa.Column("reingest_correlation_id", sa.String(64)))
    op.create_index("ix_dlq_reingest_status", "dlq_records", ["reingest_status"])


def downgrade() -> None:
    op.drop_index("ix_dlq_reingest_status", table_name="dlq_records")
    op.drop_column("dlq_records", "reingest_correlation_id")
    op.drop_column("dlq_records", "reingest_event_id")
    op.drop_column("dlq_records", "reingested_at")
    op.drop_column("dlq_records", "reingest_status")
