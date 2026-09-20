FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
COPY configs ./configs
COPY scripts ./scripts

RUN pip install --no-cache-dir . \
    && chmod +x /app/scripts/entrypoint.sh

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV CHECKPOINT_ROOT=/app
ENV APP_ENV=local
ENV RUN_MIGRATIONS=true
ENV CREATE_TOPICS=false

EXPOSE 8080 8000

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["gunicorn", "-b", "0.0.0.0:8080", "-w", "2", "checkpoint_platform.interfaces.http.app:app"]
