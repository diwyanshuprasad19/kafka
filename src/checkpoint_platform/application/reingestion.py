"""Re-ingestion use cases — DLQ replay and manual event reinjection."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy.orm import Session

from checkpoint_platform.application.ports import EventPublisher
from checkpoint_platform.domain.events import (
    CheckpointEvent,
    ReingestDlqRequest,
    ReingestEventsRequest,
)
from checkpoint_platform.infrastructure.observability.logging import (
    bind_context,
    get_correlation_id,
    get_logger,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    REINGEST_EVENTS,
    REINGEST_FAILURES,
)
from checkpoint_platform.infrastructure.persistence.repositories import (
    DlqRepo,
    ProcessedEventRepo,
)

logger = get_logger(__name__)


class ReIngestionService:
    def __init__(self, session: Session, publisher: EventPublisher) -> None:
        self.session = session
        self.publisher = publisher
        self.dlq_repo = DlqRepo(session)
        self.processed_repo = ProcessedEventRepo(session)

    def reingest_dlq(self, req: ReingestDlqRequest) -> dict[str, Any]:
        correlation_id = get_correlation_id() or str(uuid4())
        bind_context(correlation_id=correlation_id, flow="reingest_dlq")

        rows = self.dlq_repo.get_by_ids(req.dlq_ids)
        found_ids = {r.id for r in rows}
        missing = [i for i in req.dlq_ids if i not in found_ids]

        results: list[dict[str, Any]] = []
        for row in rows:
            try:
                result = self._reingest_one_dlq(
                    row=row,
                    new_event_id=req.new_event_id,
                    force=req.force,
                    reset_retry_count=req.reset_retry_count,
                    correlation_id=correlation_id,
                )
                results.append(result)
                REINGEST_EVENTS.labels(source="dlq", outcome="success").inc()
            except Exception as exc:  # noqa: BLE001
                REINGEST_EVENTS.labels(source="dlq", outcome="failure").inc()
                REINGEST_FAILURES.inc()
                logger.exception(
                    "reingest_dlq_failed",
                    dlq_id=row.id,
                    error=str(exc),
                )
                results.append(
                    {
                        "dlq_id": row.id,
                        "status": "failed",
                        "error": str(exc),
                    }
                )

        self.session.commit()
        self.publisher.flush()

        logger.info(
            "reingest_dlq_completed",
            requested=len(req.dlq_ids),
            succeeded=sum(1 for r in results if r.get("status") == "reingested"),
            missing=missing,
            correlation_id=correlation_id,
        )
        return {
            "correlation_id": correlation_id,
            "results": results,
            "missing_ids": missing,
        }

    def reingest_events(self, req: ReingestEventsRequest) -> dict[str, Any]:
        correlation_id = get_correlation_id() or str(uuid4())
        bind_context(correlation_id=correlation_id, flow="reingest_events")

        results: list[dict[str, Any]] = []
        for raw in req.events:
            try:
                payload = dict(raw)
                if req.new_event_id or "event_id" not in payload:
                    payload["event_id"] = str(uuid4())
                payload["retry_count"] = 0
                payload["correlation_id"] = correlation_id
                payload["reingested"] = True

                event = CheckpointEvent.model_validate(payload)
                self.publisher.publish_checkpoint(event)
                results.append(
                    {
                        "status": "published",
                        "event_id": str(event.event_id),
                        "checkpoint_id": event.checkpoint_id,
                        "counter_id": event.counter_id,
                    }
                )
                REINGEST_EVENTS.labels(source="manual", outcome="success").inc()
            except ValidationError as exc:
                REINGEST_EVENTS.labels(source="manual", outcome="failure").inc()
                REINGEST_FAILURES.inc()
                results.append({"status": "failed", "error": exc.errors()})
            except Exception as exc:  # noqa: BLE001
                REINGEST_EVENTS.labels(source="manual", outcome="failure").inc()
                REINGEST_FAILURES.inc()
                results.append({"status": "failed", "error": str(exc)})

        self.publisher.flush()
        logger.info(
            "reingest_events_completed",
            count=len(results),
            correlation_id=correlation_id,
        )
        return {"correlation_id": correlation_id, "results": results}

    def _reingest_one_dlq(
        self,
        *,
        row,
        new_event_id: bool,
        force: bool,
        reset_retry_count: bool,
        correlation_id: str,
    ) -> dict[str, Any]:
        original = row.original_event or {}
        # DLQ wrapper may nest the checkpoint under original_event
        payload = dict(original)
        if "original_event" in payload and isinstance(payload["original_event"], dict):
            # already unwrapped style — keep as-is if it looks like a checkpoint
            if "checkpoint_id" not in payload and "checkpoint_id" in payload["original_event"]:
                payload = dict(payload["original_event"])

        if "checkpoint_id" not in payload:
            raise ValueError("DLQ original_event missing checkpoint_id — cannot reingest")

        old_event_id = payload.get("event_id")
        if new_event_id:
            payload["event_id"] = str(uuid4())
        elif force and old_event_id:
            try:
                self.processed_repo.delete(UUID(str(old_event_id)))
            except Exception:  # noqa: BLE001
                pass

        if reset_retry_count:
            payload["retry_count"] = 0
        payload["correlation_id"] = correlation_id
        payload["reingested"] = True
        payload["reingest_from_dlq_id"] = row.id

        event = CheckpointEvent.model_validate(payload)
        self.publisher.publish_checkpoint(event)

        self.dlq_repo.mark_reingested(
            row,
            event_id=str(event.event_id),
            correlation_id=correlation_id,
        )

        logger.info(
            "dlq_reingested",
            dlq_id=row.id,
            event_id=str(event.event_id),
            checkpoint_id=event.checkpoint_id,
            correlation_id=correlation_id,
        )
        return {
            "dlq_id": row.id,
            "status": "reingested",
            "event_id": str(event.event_id),
            "checkpoint_id": event.checkpoint_id,
            "topic_key": event.partition_key(),
        }
