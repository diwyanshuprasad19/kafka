#!/usr/bin/env python3
"""Initialize PostgreSQL schema (create_all). Prefer Alembic in production."""

from checkpoint_platform.infrastructure.observability.logging import setup_logging
from checkpoint_platform.infrastructure.persistence.session import init_db


def main() -> None:
    setup_logging()
    init_db()
    print("Database schema ready.")


if __name__ == "__main__":
    main()
