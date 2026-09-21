"""Aggregation consumer — batched transactions, manual offset commit after DB success.

Throughput shape: the production target is 50,000 events/minute (~833/second). A
commit-per-message loop cannot reach that, because each event would cost a
round-trip for the DB commit plus another for the offset commit. Messages are
therefore drained in batches and committed once per batch, which amortises both.

Correctness is not traded away for it: each message still writes inside its own
savepoint, so one poison event cannot discard its batch, and offsets are only
committed after the database transaction commits. If a batch fails at the
transaction level, it is replayed message-by-message to isolate the bad one.
"""

from __future__ import annotations

import os
import signal
import socket
import sys
import time
from collections import Counter as OutcomeCounter
from collections import deque

from confluent_kafka import KafkaError, TopicPartition

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import (
    get_event_processor,
    get_publisher,
    get_session,
)
from checkpoint_platform.infrastructure.messaging.kafka_consumer import build_consumer
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    ACTIVE_CONSUMERS,
    BATCH_FALLBACKS,
    CONSUMER_BATCH_SIZE,
    CONSUMER_LAG,
    EVENTS_PER_MINUTE,
    set_app_info,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.session import init_db

setup_logging(service="aggregation-consumer")
logger = get_logger(__name__)

_running = True
LAG_REFRESH_SECONDS = 15.0


def _handle_signal(signum, frame) -> None:
    global _running
    logger.info("shutdown_signal", signal=signum)
    _running = False


class ThroughputWindow:
    """Rolling one-minute count, reported so ops can compare against the target."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self.window_seconds = window_seconds
        self._events: deque[float] = deque()

    def record(self, count: int) -> None:
        now = time.monotonic()
        for _ in range(count):
            self._events.append(now)
        self._trim(now)

    def _trim(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    def per_minute(self) -> int:
        self._trim(time.monotonic())
        return len(self._events)


def _offsets_to_commit(messages) -> list[TopicPartition]:
    """Highest offset seen per partition, plus one — what Kafka expects next."""
    highest: dict[tuple[str, int], int] = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        if msg.offset() > highest.get(key, -1):
            highest[key] = msg.offset()
    return [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in highest.items()
    ]


def _process_batch(publisher, messages) -> OutcomeCounter:
    """One DB transaction for the whole batch; each message in its own savepoint."""
    session = get_session()
    outcomes: OutcomeCounter = OutcomeCounter()
    try:
        processor = get_event_processor(
            session=session, publisher=publisher, manage_transaction=False
        )
        for msg in messages:
            outcome = processor.process_raw(
                raw_value=msg.value(),
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
            outcomes[outcome] += 1
        session.commit()
        return outcomes
    finally:
        session.close()


def _process_individually(publisher, messages) -> OutcomeCounter:
    """Fallback after a batch-level failure: isolate the offending message.

    Each message gets its own transaction, so a failure that poisoned the shared
    transaction (a dropped connection, a statement that aborted it) is attributed to
    exactly one event and routed to retry or the DLQ on its own.
    """
    outcomes: OutcomeCounter = OutcomeCounter()
    for msg in messages:
        session = get_session()
        try:
            processor = get_event_processor(session=session, publisher=publisher)
            outcome = processor.process_raw(
                raw_value=msg.value(),
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
            outcomes[outcome] += 1
        except Exception:
            session.rollback()
            outcomes["failed"] += 1
            logger.exception(
                "message_failed_in_fallback",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
        finally:
            session.close()
    return outcomes


def _refresh_lag(consumer) -> None:
    """Publish per-partition lag so alerting sees a backlog before users do."""
    try:
        assignment = consumer.assignment()
        if not assignment:
            return
        committed = consumer.committed(assignment, timeout=5.0)
        for tp in committed:
            try:
                _, high = consumer.get_watermark_offsets(tp, timeout=5.0, cached=True)
            except Exception:  # noqa: BLE001
                continue
            if high is None or high < 0:
                continue
            position = tp.offset if tp.offset and tp.offset >= 0 else 0
            CONSUMER_LAG.labels(topic=tp.topic, partition=str(tp.partition)).set(
                max(0, high - position)
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("lag_refresh_failed", error=str(exc))


def run() -> None:
    settings = get_settings()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    init_db()
    start_metrics_server(settings.metrics_port)
    set_app_info(env=settings.app_env, service="aggregation-consumer")

    instance = f"{socket.gethostname()}-{os.getpid()}"
    ACTIVE_CONSUMERS.labels(instance=instance).set(1)

    def on_assign(consumer, partitions) -> None:
        logger.info(
            "partitions_assigned",
            partitions=[f"{p.topic}:{p.partition}" for p in partitions],
        )

    def on_revoke(consumer, partitions) -> None:
        # Offsets are committed after every batch, so there is normally nothing
        # outstanding — but committing here closes the window where a rebalance
        # lands between the DB commit and the offset commit and re-delivers work.
        try:
            consumer.commit(asynchronous=False)
        except Exception as exc:  # noqa: BLE001
            logger.debug("revoke_commit_skipped", error=str(exc))
        for p in partitions:
            CONSUMER_LAG.labels(topic=p.topic, partition=str(p.partition)).set(0)
        logger.info(
            "partitions_revoked",
            partitions=[f"{p.topic}:{p.partition}" for p in partitions],
        )

    logger.info(
        "consumer_starting",
        group=settings.consumer_group_id,
        topics=[settings.checkpoint_topic, settings.retry_topic],
        metrics_port=settings.metrics_port,
        instance=instance,
        batch_size=settings.consumer_batch_size,
        target_events_per_minute=50_000,
    )

    consumer = build_consumer(
        group_id=settings.consumer_group_id,
        topics=[settings.checkpoint_topic, settings.retry_topic],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )
    publisher = get_publisher()
    throughput = ThroughputWindow()
    last_lag_refresh = 0.0

    try:
        while _running:
            batch = consumer.consume(
                num_messages=settings.consumer_batch_size,
                timeout=settings.consumer_poll_timeout_seconds,
            )

            now = time.monotonic()
            if now - last_lag_refresh > LAG_REFRESH_SECONDS:
                _refresh_lag(consumer)
                EVENTS_PER_MINUTE.labels(service="aggregation-consumer").set(
                    throughput.per_minute()
                )
                last_lag_refresh = now

            if not batch:
                continue

            messages = []
            for msg in batch:
                error = msg.error()
                if error is None:
                    messages.append(msg)
                elif error.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("kafka_message_error", error=str(error))
            if not messages:
                continue

            try:
                outcomes = _process_batch(publisher, messages)
            except Exception:
                BATCH_FALLBACKS.inc()
                logger.exception("batch_failed_replaying_individually", size=len(messages))
                outcomes = _process_individually(publisher, messages)

            # Offsets move only after the data is durable.
            consumer.commit(offsets=_offsets_to_commit(messages), asynchronous=False)

            CONSUMER_BATCH_SIZE.observe(len(messages))
            throughput.record(len(messages))
            logger.info(
                "batch_handled",
                size=len(messages),
                outcomes=dict(outcomes),
                events_per_minute=throughput.per_minute(),
            )
    finally:
        try:
            consumer.close()
        finally:
            publisher.flush()
            ACTIVE_CONSUMERS.labels(instance=instance).set(0)
            logger.info("consumer_stopped")


def main() -> None:
    run()
    sys.exit(0)


if __name__ == "__main__":  # pragma: no cover
    main()
