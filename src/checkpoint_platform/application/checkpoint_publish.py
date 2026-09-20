from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from checkpoint_platform.application.ports import EventPublisher
from checkpoint_platform.domain.events import CheckpointCreateRequest, CheckpointEvent
from checkpoint_platform.infrastructure.observability.logging import get_correlation_id, get_logger

logger = get_logger(__name__)


class CheckpointPublishService:
    """Accepts API/checkpoint writes and publishes to Kafka (async aggregation)."""

    def __init__(self, publisher: EventPublisher) -> None:
        self.publisher = publisher

    def publish_from_request(self, req: CheckpointCreateRequest) -> CheckpointEvent:
        event = CheckpointEvent(
            event_id=uuid4(),
            event_type=req.event_type,
            checkpoint_id=req.checkpoint_id or f"cp-{uuid4().hex[:12]}",
            checkpoint_version=req.checkpoint_version,
            client_id=req.client_id,
            cafe_id=req.cafe_id,
            counter_id=req.counter_id,
            meal_type=req.meal_type,
            checkpoint_type=req.checkpoint_type,
            status=req.status,
            value=req.value,
            unit=req.unit,
            occurred_at=req.occurred_at or datetime.now(timezone.utc),
            correlation_id=get_correlation_id(),
        )
        self.publisher.publish_checkpoint(event)
        self.publisher.flush()
        logger.info(
            "checkpoint_published",
            event_id=str(event.event_id),
            checkpoint_id=event.checkpoint_id,
            counter_id=event.counter_id,
            correlation_id=event.correlation_id,
        )
        return event
