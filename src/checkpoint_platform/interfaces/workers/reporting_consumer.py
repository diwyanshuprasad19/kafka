"""Reporting consumer — reads aggregation.completed events."""

from __future__ import annotations

import signal
from datetime import UTC, date, datetime

from sqlalchemy.dialects.postgresql import insert

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_session
from checkpoint_platform.infrastructure.messaging.kafka_consumer import build_consumer
from checkpoint_platform.infrastructure.messaging.serializer import deserialize
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    EVENTS_PROCESSED,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.models import ReportingSnapshot
from checkpoint_platform.infrastructure.persistence.session import init_db

setup_logging(service="reporting-consumer")
logger = get_logger(__name__)

_running = True


def _handle_signal(signum, frame) -> None:
    global _running
    _running = False


def run() -> None:
    settings = get_settings()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    init_db()
    start_metrics_server(settings.metrics_port)
    consumer = build_consumer(
        group_id=settings.reporting_consumer_group_id,
        topics=[settings.aggregation_topic],
    )
    logger.info("reporting_consumer_starting")

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("kafka_error", error=str(msg.error()))
                continue

            session = get_session()
            try:
                payload = deserialize(msg.value())
                agg_date = date.fromisoformat(payload["aggregation_date"])
                table = ReportingSnapshot.__table__
                stmt = insert(table).values(
                    counter_id=payload["counter_id"],
                    cafe_id=payload["cafe_id"],
                    client_id=payload["client_id"],
                    meal_type=payload["meal_type"],
                    aggregation_date=agg_date,
                    payload=payload,
                    received_at=datetime.now(UTC),
                )
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_reporting_counter_meal",
                    set_={
                        "payload": stmt.excluded.payload,
                        "cafe_id": stmt.excluded.cafe_id,
                        "client_id": stmt.excluded.client_id,
                        "received_at": stmt.excluded.received_at,
                    },
                )
                session.execute(stmt)
                session.commit()
                consumer.commit(message=msg, asynchronous=False)
                EVENTS_PROCESSED.labels(outcome="reporting").inc()
            except Exception:
                session.rollback()
                logger.exception("reporting_failed")
            finally:
                session.close()
    finally:
        consumer.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
