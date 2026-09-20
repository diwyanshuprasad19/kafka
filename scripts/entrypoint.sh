#!/usr/bin/env bash
# Docker / Cloud Run entrypoint — migrate then exec the service command.
set -euo pipefail

echo "[entrypoint] APP_ENV=${APP_ENV:-local} starting..."

APP_ENV_NORMALIZED="$(printf '%s' "${APP_ENV:-local}" | tr '[:upper:]' '[:lower:]')"

if [[ "${RUN_MIGRATIONS:-true}" == "true" ]]; then
  echo "[entrypoint] alembic upgrade head"
  if ! alembic upgrade head; then
    case "$APP_ENV_NORMALIZED" in
      prod|production|gcp)
        # Never paper over a failed migration in production. create_all builds the
        # schema from the models while leaving alembic_version behind, so the next
        # deploy would try to re-apply migrations against tables that already exist
        # — and any data migration in the failed revision would simply be skipped.
        # Crash instead, so the rollout halts and the failure is visible.
        echo "[entrypoint] FATAL: migrations failed in ${APP_ENV_NORMALIZED}." >&2
        echo "[entrypoint] Refusing to fall back to create_all; fix the migration." >&2
        exit 1
        ;;
      *)
        echo "[entrypoint] alembic failed — falling back to create_all (local only)"
        python -c "from checkpoint_platform.infrastructure.persistence.session import init_db; init_db()"
        ;;
    esac
  fi
fi

if [[ "${CREATE_TOPICS:-false}" == "true" ]]; then
  echo "[entrypoint] creating kafka topics"
  python scripts/create_topics.py || echo "[entrypoint] topic create skipped/failed (broker may not be ready)"
fi

exec "$@"
