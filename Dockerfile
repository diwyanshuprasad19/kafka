# Multi-stage, non-root, Cloud Run–friendly ($PORT).
FROM python:3.12-slim AS builder

WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim

WORKDIR /app

# Runtime Kafka client libs only (no gcc toolchain)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home /app --shell /usr/sbin/nologin app

COPY --from=builder /install /usr/local
COPY alembic ./alembic
COPY alembic.ini ./
COPY configs ./configs
COPY scripts ./scripts

RUN chmod +x /app/scripts/entrypoint.sh \
    && chown -R app:app /app

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    CHECKPOINT_ROOT=/app \
    APP_ENV=prod \
    RUN_MIGRATIONS=true \
    CREATE_TOPICS=false \
    LOG_FORMAT=json \
    PORT=8080 \
    WEB_CONCURRENCY=2

USER app
EXPOSE 8080

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
# Cloud Run injects PORT; gunicorn graceful drain for rolling deploys
CMD ["sh", "-c", "exec gunicorn -b 0.0.0.0:${PORT:-8080} -w ${WEB_CONCURRENCY:-2} --timeout 60 --graceful-timeout 30 --keep-alive 5 --access-logfile - --error-logfile - checkpoint_platform.interfaces.http.app:app"]
