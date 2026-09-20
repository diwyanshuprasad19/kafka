from flask import Blueprint, current_app, jsonify, request
from pydantic import ValidationError

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_publish_service
from checkpoint_platform.domain.events import CheckpointCreateRequest
from checkpoint_platform.infrastructure.observability.logging import get_correlation_id, get_logger
from checkpoint_platform.infrastructure.observability.metrics import PRODUCER_EVENTS

bp = Blueprint("checkpoints", __name__)
logger = get_logger(__name__)


@bp.post("/checkpoints")
def create_checkpoint():
    try:
        req = CheckpointCreateRequest.model_validate(request.get_json(force=True))
    except ValidationError as exc:
        return jsonify({"error": "validation_failed", "detail": exc.errors()}), 400

    publisher = current_app.extensions["publisher"]
    event = get_publish_service(publisher).publish_from_request(req)
    PRODUCER_EVENTS.labels(source="api").inc()
    settings = get_settings()
    logger.info(
        "checkpoint_accepted",
        event_id=str(event.event_id),
        checkpoint_id=event.checkpoint_id,
        counter_id=event.counter_id,
        correlation_id=get_correlation_id(),
    )
    return (
        jsonify(
            {
                "event_id": str(event.event_id),
                "checkpoint_id": event.checkpoint_id,
                "status": "accepted",
                "topic": settings.checkpoint_topic,
                "correlation_id": get_correlation_id(),
            }
        ),
        202,
    )
