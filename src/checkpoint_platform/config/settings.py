from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_repo_root() -> Path:
    """Locate project root (directory containing configs/)."""
    env_root = os.getenv("CHECKPOINT_ROOT")
    if env_root:
        return Path(env_root)

    cwd = Path.cwd()
    if (cwd / "configs").is_dir():
        return cwd

    for start in (Path(__file__).resolve().parent, cwd):
        for parent in [start, *start.parents]:
            if (parent / "configs").is_dir() and (
                (parent / "pyproject.toml").is_file() or (parent / "alembic.ini").is_file()
            ):
                return parent
    return cwd


def normalize_app_env(raw: str | None = None) -> str:
    """Map APP_ENV aliases: local | prod | gcp → local | prod."""
    value = (raw or os.getenv("APP_ENV", "local")).lower().strip()
    if value in {"gcp", "production", "prod"}:
        return "prod"
    if value in {"dev", "development", "local", "docker"}:
        return "local"
    return value or "local"


def _env_files() -> tuple[str, ...]:
    """
    Load order (later wins):
      1. configs/{local|prod}.env
      2. .env (secrets / machine overrides)
    Runtime env vars always win over files (pydantic-settings default).
    """
    app_env = normalize_app_env()
    root = _find_repo_root()
    files: list[str] = []
    env_path = root / "configs" / f"{app_env}.env"
    if env_path.is_file():
        files.append(str(env_path))
    dotenv = root / ".env"
    if dotenv.is_file():
        files.append(str(dotenv))
    return tuple(files)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_env_files() or None,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "local"
    log_level: str = "INFO"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_ssl_ca_location: str | None = None

    checkpoint_topic: str = "checkpoint.events.v1"
    retry_topic: str = "checkpoint.retry.v1"
    dlq_topic: str = "checkpoint.dlq.v1"
    aggregation_topic: str = "aggregation.completed.v1"
    checkpoint_partitions: int = 6
    kafka_replication_factor: int = 1
    # The DLQ has to outlive the incident that filled it, so it is retained far
    # longer than the live stream's default.
    dlq_topic_retention_days: int = 30
    consumer_group_id: str = "checkpoint-aggregation-service"
    reporting_consumer_group_id: str = "reporting-service"

    database_url: str = "postgresql+psycopg://checkpoint:checkpoint@localhost:5432/aggregation"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl_seconds: int = 30
    redis_enabled: bool = True

    max_retries: int = 3
    retry_base_delay_ms: int = 500
    metrics_port: int = 8000
    api_host: str = "0.0.0.0"
    api_port: int = 8080

    # Business day boundary for daily aggregates (cafeteria-local, not UTC).
    business_timezone: str = "Asia/Kolkata"
    # Reject device clock skew beyond this many seconds into the future.
    max_clock_skew_seconds: int = 900
    # Sanity ceiling for a single quantity reading (kg) — above this is a bad sensor.
    max_quantity_kg: float = 100_000.0

    # Consumer batching: one DB transaction + one offset commit per batch.
    consumer_batch_size: int = 200
    consumer_poll_timeout_seconds: float = 1.0
    # Retry messages not yet due are requeued instead of blocking the partition.
    retry_max_inline_wait_seconds: float = 1.0

    # Retention (rows grow at ~50k/min; pruned by the maintenance worker).
    # How often the rollup worker restates cafe/client aggregates. This is the upper
    # bound on how stale those two levels can be relative to counter-level data.
    rollup_interval_seconds: float = 5.0

    processed_events_retention_hours: int = 72
    history_retention_days: int = 90
    outbox_retention_hours: int = 24
    dlq_retention_days: int = 30
    maintenance_interval_seconds: int = 300

    # reliable | high  — high tunes batches/compression for 50k/s design target
    kafka_throughput_mode: str = "reliable"

    gcp_project_id: str | None = None
    gcp_region: str = "asia-south1"
    alloydb_instance: str | None = None
    alloydb_cluster: str | None = None

    @property
    def is_prod(self) -> bool:
        return normalize_app_env(self.app_env) == "prod"

    def kafka_client_config(self) -> dict:
        """Identical code path for local + GCP; SASL/SSL applied when configured."""
        config: dict = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
        }
        if self.kafka_security_protocol.upper() in {"SASL_SSL", "SASL_PLAINTEXT"}:
            if self.kafka_sasl_mechanism:
                config["sasl.mechanisms"] = self.kafka_sasl_mechanism
            if self.kafka_sasl_username:
                config["sasl.username"] = self.kafka_sasl_username
            if self.kafka_sasl_password:
                config["sasl.password"] = self.kafka_sasl_password
        if self.kafka_ssl_ca_location:
            config["ssl.ca.location"] = self.kafka_ssl_ca_location
        return config

    def kafka_producer_config(self) -> dict:
        from checkpoint_platform.infrastructure.messaging.tuning import producer_config

        return producer_config(
            self.kafka_client_config(),
            throughput_mode=self.kafka_throughput_mode,
        )

    def kafka_consumer_config(self) -> dict:
        from checkpoint_platform.infrastructure.messaging.tuning import consumer_config

        return consumer_config(
            self.kafka_client_config(),
            throughput_mode=self.kafka_throughput_mode,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


def reload_settings() -> Settings:
    """Clear cache — useful in tests or after env changes."""
    get_settings.cache_clear()
    return get_settings()
