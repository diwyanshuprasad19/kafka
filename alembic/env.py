"""Alembic environment — works with local Postgres and GCP AlloyDB."""

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context
from checkpoint_platform.config import get_settings
from checkpoint_platform.infrastructure.persistence.base import Base
from checkpoint_platform.infrastructure.persistence.models import (  # noqa: F401
    CheckpointHistory,
    CheckpointState,
    DailyCounterAggregation,
    DlqRecord,
    OutboxEvent,
    ProcessedEvent,
    ReportingSnapshot,
)

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Settings supply the URL for normal use (local, Docker, AlloyDB), but an explicitly
# configured URL wins so callers — the schema-drift tests especially — can migrate a
# throwaway database instead of the one the app is pointed at.
if not config.get_main_option("sqlalchemy.url", None):
    config.set_main_option("sqlalchemy.url", get_settings().database_url)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


# Any 64-bit constant; it only has to be the same in every process that migrates.
MIGRATION_LOCK_ID = 8_242_119_004_517_336_001


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    # Replicas start together and each runs `alembic upgrade head`. Without a lock
    # they race: one applies the revision, the others fail on objects that now
    # already exist. This serialises them — the losers block here, then find
    # nothing left to do.
    #
    # The lock is taken on its own connection, deliberately. Issuing it on the
    # migration connection would leave that connection inside an open transaction,
    # and alembic's begin_transaction() becomes a no-op when it finds one already
    # active — it assumes the caller will commit. Nothing would, so every migration
    # would roll back on close while still reporting success.
    lock_connection = None
    if connectable.dialect.name == "postgresql":
        lock_connection = connectable.connect()
        lock_connection.exec_driver_sql(f"SELECT pg_advisory_lock({MIGRATION_LOCK_ID})")

    try:
        with connectable.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        if lock_connection is not None:
            # Session-level locks also release on disconnect; this is the tidy path.
            lock_connection.exec_driver_sql(f"SELECT pg_advisory_unlock({MIGRATION_LOCK_ID})")
            lock_connection.close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
