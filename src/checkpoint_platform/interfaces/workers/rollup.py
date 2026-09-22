"""Rollup worker: restates cafe and client aggregates from the counter grain.

Run alongside the consumers:

    python -m checkpoint_platform.interfaces.workers.rollup
    python -m checkpoint_platform.interfaces.workers.rollup --once
"""

from __future__ import annotations

import argparse
import signal
import time
from datetime import UTC

import structlog

from checkpoint_platform.config import get_settings
from checkpoint_platform.infrastructure.observability.logging import setup_logging
from checkpoint_platform.infrastructure.observability.metrics import (
    ROLLUP_LAG_SECONDS,
    ROLLUP_ROWS,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.rollups import RollupRepo
from checkpoint_platform.infrastructure.persistence.session import session_scope

log = structlog.get_logger(__name__)

_running = True


def _stop(_signum, _frame) -> None:
    global _running
    _running = False


def refresh_once() -> int:
    from datetime import datetime

    with session_scope() as session:
        result = RollupRepo(session).refresh()

    if result.rows:
        ROLLUP_ROWS.labels(level="cafe").inc(result.cafe_rows)
        ROLLUP_ROWS.labels(level="client").inc(result.client_rows)
        log.info(
            "rollup_refreshed",
            cafe_rows=result.cafe_rows,
            client_rows=result.client_rows,
            watermark=result.watermark.isoformat(),
        )

    # How stale the rollups are: the gap between now and the point in the counter
    # table the worker has caught up to.
    lag = (datetime.now(UTC) - result.watermark).total_seconds()
    ROLLUP_LAG_SECONDS.set(max(lag, 0.0))
    return result.rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="single pass, then exit")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="seconds between passes (default from settings)",
    )
    parser.add_argument("--metrics-port", type=int, default=9105)
    args = parser.parse_args()

    setup_logging(service="rollup-worker")
    settings = get_settings()
    interval = args.interval or settings.rollup_interval_seconds

    if args.once:
        rows = refresh_once()
        print(f"rollup refreshed {rows} rows")
        return

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    start_metrics_server(args.metrics_port)
    log.info("rollup_worker_started", interval_seconds=interval)

    while _running:
        try:
            refresh_once()
        except Exception as exc:
            # A failed pass is recoverable: the watermark only advances on success,
            # so the next pass covers the same ground.
            log.exception("rollup_pass_failed", error=str(exc))
        time.sleep(interval)

    log.info("rollup_worker_stopped")


if __name__ == "__main__":  # pragma: no cover
    main()
