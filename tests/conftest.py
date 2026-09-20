"""Shared DB fixtures — never drop the demo/aggregation database."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

TEST_DB_URL = "postgresql+psycopg://checkpoint:checkpoint@localhost:5432/aggregation_test"


def _ensure_test_db() -> None:
    """Create aggregation_test DB owned by checkpoint if missing."""
    admin = create_engine(
        "postgresql+psycopg://checkpoint:checkpoint@localhost:5432/postgres",
        isolation_level="AUTOCOMMIT",
        future=True,
    )
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname='aggregation_test'")
            ).scalar()
            if not exists:
                conn.execute(text("CREATE DATABASE aggregation_test OWNER checkpoint"))
    except OperationalError:
        # fallback: connect as current OS user via peer/trust on local socket
        admin2 = create_engine(
            "postgresql+psycopg://localhost:5432/postgres",
            isolation_level="AUTOCOMMIT",
            future=True,
        )
        with admin2.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname='aggregation_test'")
            ).scalar()
            if not exists:
                conn.execute(text("CREATE DATABASE aggregation_test OWNER checkpoint"))
    finally:
        admin.dispose()


@pytest.fixture()
def db_engine():
    pytest.importorskip("psycopg")
    import checkpoint_platform.infrastructure.persistence.models  # noqa: F401
    from checkpoint_platform.infrastructure.persistence.base import Base

    try:
        _ensure_test_db()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Cannot create test DB: {exc}")

    engine = create_engine(TEST_DB_URL, future=True)
    try:
        with engine.connect() as conn:
            conn.execute(select(1))
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL test DB not available: {exc}")

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def alembic_config(url: str = TEST_DB_URL):
    from pathlib import Path

    from alembic.config import Config

    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("sqlalchemy.url", url)
    return config


@pytest.fixture()
def migrated_engine():
    """A test database built by the migration chain rather than by create_all."""
    pytest.importorskip("alembic")
    from alembic import command

    try:
        _ensure_test_db()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Cannot create test DB: {exc}")

    engine = create_engine(TEST_DB_URL, future=True)
    try:
        with engine.connect() as conn:
            conn.execute(select(1))
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL test DB not available: {exc}")

    def reset() -> None:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA public CASCADE"))
            conn.execute(text("CREATE SCHEMA public"))

    reset()
    command.upgrade(alembic_config(), "head")
    yield engine
    reset()
    engine.dispose()


@pytest.fixture()
def session(db_engine):
    Session = sessionmaker(bind=db_engine, future=True)
    s = Session()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()
