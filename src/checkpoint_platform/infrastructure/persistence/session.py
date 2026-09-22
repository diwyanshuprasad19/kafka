from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from checkpoint_platform.config import get_settings

_settings = get_settings()

_engine_kwargs: dict = {
    "pool_size": _settings.db_pool_size,
    "max_overflow": _settings.db_max_overflow,
    "pool_pre_ping": True,
    "pool_timeout": 30,
    "future": True,
}
# Avoid hanging workers/API on unreachable Postgres (sqlite tests skip connect_args).
if _settings.database_url.startswith("postgresql"):
    _engine_kwargs["connect_args"] = {"connect_timeout": 5}

engine = create_engine(_settings.database_url, **_engine_kwargs)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Create tables (Alembic preferred in prod; useful for local/demo)."""
    from checkpoint_platform.infrastructure.persistence import (
        models as _models,  # noqa: F401
    )
    from checkpoint_platform.infrastructure.persistence.base import Base

    Base.metadata.create_all(bind=engine)
