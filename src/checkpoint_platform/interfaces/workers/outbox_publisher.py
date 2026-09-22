"""Transactional outbox publisher — DB commit then Kafka publish.

Two things here are easy to get wrong and both matter in production:

Publish-then-commit is at-least-once, so a broker acknowledgement followed by a
failed DB commit republishes the row. Downstream consumers must be idempotent,
which the reporting consumer achieves by upserting on its natural key.

The read cache is invalidated here rather than in the aggregation service, because
this is the first point at which the new totals are known to be durable. Doing it
before commit would evict on writes that later roll back, and skipping it entirely
would serve stale aggregates for the whole cache TTL after every correction.
"""

from __future__ import annotations

import signal
import time
from datetime import UTC, datetime

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_cache, get_publisher, get_session
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    OUTBOX_PENDING,
    OUTBOX_PUBLISHED,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.repositories import OutboxRepo
from checkpoint_platform.infrastructure.persistence.session import init_db

setup_logging(service="outbox-publisher")
logger = get_logger(__name__)

_running = True
BATCH_SIZE = 500
IDLE_SLEEP_SECONDS = 0.2


def _handle_signal(signum, frame) -> None:
    global _running
    _running = False


def _invalidate(cache, payload: dict) -> None:
    date = payload.get("aggregation_date")
    counter_id = payload.get("counter_id")
    meal_type = payload.get("meal_type")
    if not (date and counter_id and meal_type):
        return
    try:
        cache.invalidate(date, counter_id, meal_type)
    except Exception as exc:  # noqa: BLE001
        logger.warning("cache_invalidate_failed", error=str(exc))


def run() -> None:
    settings = get_settings()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    init_db()
    start_metrics_server(settings.metrics_port)
    publisher = get_publisher()
    cache = get_cache()
    logger.info("outbox_publisher_starting", batch_size=BATCH_SIZE)

    while _running:
        session = get_session()
        try:
            repo = OutboxRepo(session)
            rows = repo.fetch_unpublished(limit=BATCH_SIZE)
            if not rows:
                OUTBOX_PENDING.set(0)
                session.commit()
                time.sleep(IDLE_SLEEP_SECONDS)
                continue

            published_payloads = []
            now = datetime.now(UTC)
            for row in rows:
                key = row.payload.get("counter_id", "outbox")
                publisher.publish_aggregation(row.payload, key=key)
                row.published = True
                row.published_at = now
                published_payloads.append(row.payload)

            publisher.flush()
            session.commit()

            OUTBOX_PUBLISHED.inc(len(published_payloads))
            OUTBOX_PENDING.set(repo.pending_count())
            for payload in published_payloads:
                _invalidate(cache, payload)

            logger.info("outbox_batch_published", count=len(published_payloads))
        except Exception:
            session.rollback()
            logger.exception("outbox_publish_failed")
            time.sleep(1.0)
        finally:
            session.close()

    publisher.flush()
    logger.info("outbox_publisher_stopped")


def main() -> None:
    run()


if __name__ == "__main__":  # pragma: no cover
    main()
