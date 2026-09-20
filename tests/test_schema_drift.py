"""Guard against the migration chain and the ORM models disagreeing.

This drift is silent and nasty: ``create_all`` builds one schema, ``alembic upgrade
head`` builds another, and whichever path a given environment took decides which
indexes and columns actually exist. A real instance of it shipped here — an index
that existed only in a migration — and it only surfaced when a downgrade tried to
drop something that had never been created.
"""

from __future__ import annotations

from sqlalchemy import text

from conftest import TEST_DB_URL, alembic_config

IGNORED_TABLES = {"alembic_version"}


def test_models_match_migrations(migrated_engine):
    """A migrated database must be exactly what the ORM models describe."""
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    import checkpoint_platform.infrastructure.persistence.models  # noqa: F401
    from checkpoint_platform.infrastructure.persistence.base import Base

    with migrated_engine.connect() as conn:
        context = MigrationContext.configure(conn)
        diff = compare_metadata(context, Base.metadata)

    meaningful = [
        entry
        for entry in diff
        if not (
            isinstance(entry, tuple)
            and len(entry) > 1
            and getattr(entry[1], "name", None) in IGNORED_TABLES
        )
    ]
    assert not meaningful, f"models and migrations disagree: {meaningful}"


def test_migrations_are_reversible(migrated_engine):
    from alembic import command

    config = alembic_config(TEST_DB_URL)
    command.downgrade(config, "base")

    with migrated_engine.connect() as conn:
        remaining = (
            conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "AND table_name <> 'alembic_version'"
                )
            )
            .scalars()
            .all()
        )
    assert not remaining, f"downgrade left tables behind: {remaining}"

    command.upgrade(config, "head")
