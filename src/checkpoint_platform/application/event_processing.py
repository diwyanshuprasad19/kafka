"""Per-message processing: idempotency, ordering, retry and DLQ routing.

Every message leaves this class with exactly one outcome and, critically, in a
state where the consumer can commit its offset. A message that can neither be
applied nor parked would otherwise block its partition forever.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from pydantic import ValidationError
from sqlalchemy.orm import Session

from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.application.ports import EventPublisher
from checkpoint_platform.config import get_settings
from checkpoint_platform.domain.events import CheckpointEvent, DlqEvent
from checkpoint_platform.domain.exceptions import (
    DuplicateEventError,
    PermanentValidationError,
    StaleVersionError,
    is_transient_error,
    retry_delay_seconds,
)
from checkpoint_platform.infrastructure.messaging.serializer import deserialize
from checkpoint_platform.infrastructure.observability.logging import (
    bind_context,
    clear_context,
    get_logger,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    DLQ_COUNT,
    DLQ_REASONS,
    DUPLICATE_EVENTS,
    EVENTS_PROCESSED,
    KAFKA_PUBLISH_ERRORS,
    PROCESSING_LATENCY,
    RETRY_COUNT,
    STALE_EVENTS,
)
from checkpoint_platform.infrastructure.persistence.repositories import DlqRepo

logger = get_logger(__name__)

MAX_RAW_PREVIEW = 4096


class EventProcessor:
    """
    At-least-once processing with:
    - duplicate protection (event_id claimed atomically)
    - stale version ignore
    - transient → retry topic (exponential backoff metadata)
    - permanent / max retries → DLQ, persisted before it is published
    """

    def __init__(
        self,
        session: Session,
        publisher: EventPublisher,
        *,
        manage_transaction: bool = True,
    ) -> None:
        self.session = session
        self.publisher = publisher
        self.settings = get_settings()
        self.dlq_repo = DlqRepo(session)
        # False when a consumer batches many messages into one transaction: each
        # message then gets a savepoint so one bad event can't discard the batch.
        self.manage_transaction = manage_transaction

    # ------------------------------------------------------------------
    # transaction scoping
    # ------------------------------------------------------------------

    def _begin_scope(self):
        """Open a scope for one message's writes.

        Standalone: the session transaction itself. Batched: a savepoint, so a bad
        event discards only its own writes and the rest of the batch still commits.
        """
        return None if self.manage_transaction else self.session.begin_nested()

    def _keep_scope(self, savepoint) -> None:
        if savepoint is None:
            self.session.commit()
        elif savepoint.is_active:
            savepoint.commit()

    def _discard_scope(self, savepoint) -> None:
        if savepoint is None:
            self.session.rollback()
        elif savepoint.is_active:
            savepoint.rollback()

    # ------------------------------------------------------------------
    # main entry point
    # ------------------------------------------------------------------

    def process_raw(
        self,
        raw_value: bytes | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> str:
        """Returns outcome: processed | duplicate | stale | retry | dlq | retry_wait"""
        start = time.perf_counter()
        try:
            return self._process_raw(raw_value, topic, partition, offset)
        finally:
            PROCESSING_LATENCY.observe(time.perf_counter() - start)

    def _process_raw(
        self,
        raw_value: bytes | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> str:
        if not raw_value:
            # Tombstone or zero-length record: nothing to parse, nothing to retry.
            self._to_dlq(
                original_event={"raw": None},
                error="EmptyPayload: message had no value (tombstone or truncated)",
                topic=topic,
                partition=partition,
                offset=offset,
                retry_count=0,
                reason="empty_payload",
            )
            return "dlq"

        try:
            data = deserialize(raw_value)
        except Exception as exc:  # noqa: BLE001
            self._to_dlq(
                original_event={"raw": self._preview(raw_value)},
                error=f"JSONDecodeError: {exc}",
                topic=topic,
                partition=partition,
                offset=offset,
                retry_count=0,
                reason="malformed_json",
            )
            return "dlq"

        if not isinstance(data, dict):
            self._to_dlq(
                original_event={"raw": self._preview(raw_value)},
                error=f"UnexpectedPayloadType: expected object, got {type(data).__name__}",
                topic=topic,
                partition=partition,
                offset=offset,
                retry_count=0,
                reason="not_an_object",
            )
            return "dlq"

        retry_count = self._retry_count_of(data)

        if topic == self.settings.retry_topic:
            deferred = self._defer_if_not_due(data, retry_count)
            if deferred is not None:
                return deferred

        try:
            event = CheckpointEvent.model_validate(data)
        except (ValidationError, PermanentValidationError) as exc:
            self._to_dlq(
                original_event=data,
                error=f"{type(exc).__name__}: {exc}",
                topic=topic,
                partition=partition,
                offset=offset,
                retry_count=retry_count,
                reason="schema_validation",
            )
            return "dlq"

        clear_context()
        bind_context(
            correlation_id=event.correlation_id or str(event.event_id),
            event_id=str(event.event_id),
            checkpoint_id=event.checkpoint_id,
            counter_id=event.counter_id,
            service="aggregation-consumer",
            flow="process_event",
            retry_count=retry_count,
            source_topic=topic,
        )

        scope = self._begin_scope()
        try:
            AggregationService(self.session).process(event)
            self._keep_scope(scope)
            EVENTS_PROCESSED.labels(outcome="processed").inc()
            logger.info("event_processed", checkpoint_id=event.checkpoint_id)
            return "processed"

        except DuplicateEventError:
            self._discard_scope(scope)
            DUPLICATE_EVENTS.inc()
            EVENTS_PROCESSED.labels(outcome="duplicate").inc()
            logger.info("duplicate_skipped", event_id=str(event.event_id))
            return "duplicate"

        except StaleVersionError as exc:
            # Committed, not discarded: the event_id claim and the audit row for the
            # rejected correction must survive so a redelivery is a cheap duplicate
            # and operators can see why the update never showed up.
            self._keep_scope(scope)
            STALE_EVENTS.inc()
            EVENTS_PROCESSED.labels(outcome="stale").inc()
            logger.info("stale_version_ignored", detail=str(exc))
            return "stale"

        except PermanentValidationError as exc:
            self._discard_scope(scope)
            self._to_dlq(
                original_event=data,
                error=str(exc),
                topic=topic,
                partition=partition,
                offset=offset,
                retry_count=retry_count,
                reason="permanent_business_rule",
            )
            return "dlq"

        except Exception as exc:  # noqa: BLE001
            self._discard_scope(scope)
            transient = is_transient_error(exc)
            logger.exception(
                "processing_failed",
                error=str(exc),
                event_id=data.get("event_id"),
                transient=transient,
                retry_count=retry_count,
            )
            if (not transient) or retry_count >= self.settings.max_retries:
                self._to_dlq(
                    original_event=data,
                    error=f"{type(exc).__name__}: {exc}",
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    retry_count=retry_count,
                    reason="retries_exhausted" if transient else "permanent_error",
                )
                return "dlq"
            self._to_retry(data, event.counter_id, retry_count)
            return "retry"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _preview(raw_value: bytes) -> str:
        return raw_value[:MAX_RAW_PREVIEW].decode("utf-8", errors="replace")

    @staticmethod
    def _retry_count_of(data: dict) -> int:
        """A hostile producer can put anything in retry_count; never let it throw."""
        try:
            return max(0, int(data.get("retry_count", 0)))
        except (TypeError, ValueError):
            return 0

    def _defer_if_not_due(self, data: dict, retry_count: int) -> Optional[str]:
        """Park a retry that isn't due yet instead of sleeping on the partition.

        Sleeping here would stall every other message in the partition — at ~833
        events/second that is the difference between keeping up and falling behind.
        """
        wait = self._backoff_wait_seconds(data, retry_count)
        if wait <= 0:
            return None

        if wait <= self.settings.retry_max_inline_wait_seconds:
            time.sleep(wait)
            return None

        # Sleep briefly so an early requeue can't spin, then hand it back to Kafka
        # with retry_count untouched: waiting is not an attempt.
        time.sleep(self.settings.retry_max_inline_wait_seconds)
        self.publisher.publish_retry(data, str(data.get("counter_id", "retry")))
        EVENTS_PROCESSED.labels(outcome="retry_wait").inc()
        logger.info(
            "retry_requeued_not_due",
            seconds_remaining=round(wait, 3),
            retry_count=retry_count,
        )
        return "retry_wait"

    def _backoff_wait_seconds(self, data: dict, retry_count: int) -> float:
        next_retry_at = data.get("next_retry_at")
        if next_retry_at:
            try:
                target = datetime.fromisoformat(str(next_retry_at).replace("Z", "+00:00"))
                if target.tzinfo is None:
                    target = target.replace(tzinfo=timezone.utc)
                return max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError):
                pass
        if retry_count:
            return retry_delay_seconds(retry_count - 1, self.settings.retry_base_delay_ms)
        return 0.0

    def _to_retry(self, data: dict, key: str, retry_count: int) -> None:
        delay = retry_delay_seconds(retry_count, self.settings.retry_base_delay_ms)
        payload = dict(data)
        payload["retry_count"] = retry_count + 1
        payload["retried_at"] = datetime.now(timezone.utc).isoformat()
        payload["next_retry_at"] = (
            datetime.now(timezone.utc) + timedelta(seconds=delay)
        ).isoformat()

        RETRY_COUNT.inc()
        EVENTS_PROCESSED.labels(outcome="retry").inc()
        self.publisher.publish_retry(payload, key)
        self.publisher.flush()
        logger.warning(
            "event_retried",
            event_id=payload.get("event_id"),
            retry=payload["retry_count"],
            delay_seconds=delay,
        )

    def _to_dlq(
        self,
        original_event: dict,
        error: str,
        topic: str,
        partition: int,
        offset: int,
        retry_count: int,
        reason: str = "unknown",
    ) -> None:
        """Park a message durably.

        The DB row is written first and the Kafka publish is best-effort: if the
        broker is unreachable, a poison message must still be parked so the offset
        can advance. Publishing first would raise, leave the offset uncommitted, and
        redeliver the same poison message forever.
        """
        failed_at = datetime.now(timezone.utc)

        scope = self._begin_scope()
        try:
            self.dlq_repo.add(
                original_topic=topic,
                original_partition=partition,
                original_offset=offset,
                error=error,
                retry_count=retry_count,
                failed_at=failed_at,
                original_event=original_event,
            )
            self._keep_scope(scope)
        except Exception:  # noqa: BLE001
            self._discard_scope(scope)
            logger.exception("dlq_db_persist_failed", reason=reason)

        payload = DlqEvent(
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            error=error,
            retry_count=retry_count,
            failed_at=failed_at,
            original_event=original_event,
        ).model_dump(mode="json")

        try:
            self.publisher.publish_dlq(
                payload, key=str(original_event.get("counter_id", "dlq"))
            )
            self.publisher.flush()
        except Exception as exc:  # noqa: BLE001
            KAFKA_PUBLISH_ERRORS.inc()
            logger.error("dlq_publish_failed", error=str(exc), reason=reason)

        DLQ_COUNT.inc()
        DLQ_REASONS.labels(reason=reason).inc()
        EVENTS_PROCESSED.labels(outcome="dlq").inc()
        logger.error(
            "event_sent_to_dlq",
            error=error,
            reason=reason,
            topic=topic,
            offset=offset,
        )
