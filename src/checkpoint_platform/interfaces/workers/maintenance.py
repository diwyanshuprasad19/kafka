"""Retention worker.

At the design target of 50,000 events/minute the append-only tables grow by ~72
million rows a day. Without pruning, ``processed_events`` alone would outgrow the
instance within a week and the idempotency lookup — the hottest read in the
pipeline — would degrade with it. Retention is therefore part of the system, not
an afterthought for the DBA.

What is safe to drop, and why:
  processed_events   idempotency keys older than the redelivery window
  checkpoint_history audit rows past the reporting horizon
  outbox_events      rows already published to Kafka
  dlq_records        only records already re-ingested or explicitly skipped;
                     pending poison is kept forever until an operator triages it
"""

from __future__ import annotations

import signal
import time
from datetime import UTC, datetime, timedelta

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_session
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    DLQ_PENDING,
    OUTBOX_PENDING,
    ROWS_PRUNED,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.repositories import (
    DlqRepo,
    HistoryRepo,
    OutboxRepo,
    ProcessedEventRepo,
)
from checkpoint_platform.infrastructure.persistence.session import init_db

setup_logging(service="maintenance")
logger = get_logger(__name__)

_running = True
CHUNK = 20_000


def _handle_signal(signum, frame) -> None:
    global _running
    _running = False


def prune_once(session) -> dict[str, int]:
    """One pruning pass. Chunked deletes keep each statement's lock hold short."""
    settings = get_settings()
    now = datetime.now(UTC)

    removed = {
        "processed_events": ProcessedEventRepo(session).prune_older_than(
            now - timedelta(hours=settings.processed_events_retention_hours),
            limit=CHUNK,
        ),
        "checkpoint_history": HistoryRepo(session).prune_older_than(
            now - timedelta(days=settings.history_retention_days),
            limit=CHUNK,
        ),
        "outbox_events": OutboxRepo(session).prune_published_older_than(
            now - timedelta(hours=settings.outbox_retention_hours),
            limit=CHUNK,
        ),
        "dlq_records": DlqRepo(session).prune_reingested_older_than(
            now - timedelta(days=settings.dlq_retention_days),
            limit=CHUNK,
        ),
    }
    session.commit()

    for table, count in removed.items():
        if count:
            ROWS_PRUNED.labels(table=table).inc(count)
    return removed


def report_backlogs(session) -> dict[str, int]:
    backlogs = {
        "outbox_pending": OutboxRepo(session).pending_count(),
        "dlq_pending": DlqRepo(session).pending_count(),
    }
    OUTBOX_PENDING.set(backlogs["outbox_pending"])
    DLQ_PENDING.set(backlogs["dlq_pending"])
    return backlogs


def run(once: bool = False) -> None:
    settings = get_settings()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    init_db()
    if not once:
        start_metrics_server(settings.metrics_port)
    logger.info(
        "maintenance_starting",
        processed_events_retention_hours=settings.processed_events_retention_hours,
        history_retention_days=settings.history_retention_days,
        outbox_retention_hours=settings.outbox_retention_hours,
        dlq_retention_days=settings.dlq_retention_days,
    )

    while True:
        removed: dict[str, int] = {}
        session = get_session()
        try:
            removed = prune_once(session)
            backlogs = report_backlogs(session)
            logger.info("maintenance_pass", removed=removed, **backlogs)
        except Exception:
            session.rollback()
            logger.exception("maintenance_pass_failed")
        finally:
            session.close()

        if once or not _running:
            break
        # A pass that filled its chunk means there is more to delete; come back
        # immediately instead of waiting out the full interval.
        slept = 0.0
        interval = (
            1.0
            if any(count >= CHUNK for count in removed.values())
            else settings.maintenance_interval_seconds
        )
        while _running and slept < interval:
            time.sleep(min(1.0, interval - slept))
            slept += 1.0

    logger.info("maintenance_stopped")


def main() -> None:
    import sys

    run(once="--once" in sys.argv)


if __name__ == "__main__":  # pragma: no cover
    main()
