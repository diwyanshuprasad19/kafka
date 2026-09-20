"""Operational status API — scale target, logging contract, dependency health."""

from __future__ import annotations

from flask import Blueprint, jsonify
from sqlalchemy import func, select, text

from checkpoint_platform.application.bad_scenarios import PROD_BAD_SCENARIOS
from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_session
from checkpoint_platform.infrastructure.messaging.tuning import SCALE_50K_PER_MIN
from checkpoint_platform.infrastructure.persistence.models import (
    DailyCounterAggregation,
    DlqRecord,
)

bp = Blueprint("ops", __name__)


@bp.get("/ops/status")
def ops_status():
    """Single pane for local + GCP: scale, deps, data volume, logging contract."""
    settings = get_settings()
    deps: dict = {"postgres": False, "redis": False, "kafka": "unknown"}
    data: dict = {}

    session = get_session()
    try:
        session.execute(text("SELECT 1"))
        deps["postgres"] = True
        agg_rows = session.execute(
            select(func.count()).select_from(DailyCounterAggregation)
        ).scalar()
        dlq_pending = session.execute(
            select(func.count())
            .select_from(DlqRecord)
            .where(DlqRecord.reingest_status == "pending")
        ).scalar()
        data = {
            "aggregation_rows": int(agg_rows or 0),
            "dlq_pending": int(dlq_pending or 0),
        }
    except Exception as exc:  # noqa: BLE001
        deps["postgres_error"] = str(exc)
    finally:
        session.close()

    try:
        if settings.redis_enabled:
            import redis

            redis.from_url(settings.redis_url, socket_connect_timeout=1).ping()
            deps["redis"] = True
    except Exception as exc:  # noqa: BLE001
        deps["redis_error"] = str(exc)

    try:
        from confluent_kafka.admin import AdminClient

        md = AdminClient(settings.kafka_client_config()).list_topics(timeout=3)
        deps["kafka"] = "ok"
        deps["kafka_brokers"] = len(md.brokers)
        deps["kafka_topics"] = sorted(md.topics.keys())[:20]
    except Exception as exc:  # noqa: BLE001
        deps["kafka"] = "down"
        deps["kafka_error"] = str(exc)

    return jsonify(
        {
            "env": settings.app_env,
            "scale": SCALE_50K_PER_MIN,
            "config": {
                "partitions": settings.checkpoint_partitions,
                "throughput_mode": settings.kafka_throughput_mode,
                "max_retries": settings.max_retries,
                "consumer_group": settings.consumer_group_id,
                "topics": {
                    "events": settings.checkpoint_topic,
                    "retry": settings.retry_topic,
                    "dlq": settings.dlq_topic,
                    "aggregation": settings.aggregation_topic,
                },
            },
            "dependencies": deps,
            "working_data": data,
            "logging": {
                "format": "json_stdout",
                "fields": [
                    "timestamp",
                    "level",
                    "event",
                    "service",
                    "env",
                    "request_id",
                    "correlation_id",
                ],
                "propagate_headers": ["X-Request-ID", "X-Correlation-ID"],
            },
            "metrics": {
                "prometheus": "/metrics",
                "key_series": [
                    "checkpoint_events_processed_total",
                    "checkpoint_processing_seconds",
                    "checkpoint_consumer_lag",
                    "checkpoint_dlq_total",
                    "checkpoint_retry_total",
                    "checkpoint_target_events_per_minute",
                    "http_requests_total",
                ],
            },
            "bad_scenarios_cataloged": len(PROD_BAD_SCENARIOS),
            "ready": bool(deps.get("postgres")),
        }
    )


@bp.get("/ops/scale")
def ops_scale():
    return jsonify(SCALE_50K_PER_MIN)
