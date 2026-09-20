from flask import Blueprint, current_app, jsonify, request
from pydantic import ValidationError

from checkpoint_platform.application.reingestion import ReIngestionService
from checkpoint_platform.config.container import get_session
from checkpoint_platform.domain.events import ReingestDlqRequest, ReingestEventsRequest
from checkpoint_platform.infrastructure.observability.logging import get_logger

bp = Blueprint("reingest", __name__)
logger = get_logger(__name__)


@bp.post("/reingest/dlq")
def reingest_dlq_bulk():
    """Re-publish one or more DLQ records onto checkpoint.events.v1."""
    try:
        req = ReingestDlqRequest.model_validate(request.get_json(force=True))
    except ValidationError as exc:
        return jsonify({"error": "validation_failed", "detail": exc.errors()}), 400

    session = get_session()
    try:
        publisher = current_app.extensions["publisher"]
        result = ReIngestionService(session, publisher).reingest_dlq(req)
        return jsonify(result), 202
    except Exception as exc:
        session.rollback()
        logger.exception("reingest_dlq_api_failed", error=str(exc))
        return jsonify({"error": "reingest_failed", "detail": str(exc)}), 500
    finally:
        session.close()


@bp.post("/reingest/dlq/<int:dlq_id>")
def reingest_dlq_one(dlq_id: int):
    body = request.get_json(silent=True) or {}
    body["dlq_ids"] = [dlq_id]
    try:
        req = ReingestDlqRequest.model_validate(body)
    except ValidationError as exc:
        return jsonify({"error": "validation_failed", "detail": exc.errors()}), 400

    session = get_session()
    try:
        publisher = current_app.extensions["publisher"]
        result = ReIngestionService(session, publisher).reingest_dlq(req)
        return jsonify(result), 202
    except Exception as exc:
        session.rollback()
        logger.exception("reingest_dlq_one_failed", dlq_id=dlq_id, error=str(exc))
        return jsonify({"error": "reingest_failed", "detail": str(exc)}), 500
    finally:
        session.close()


@bp.post("/reingest/events")
def reingest_events():
    """Manually inject checkpoint event payloads back into Kafka."""
    try:
        req = ReingestEventsRequest.model_validate(request.get_json(force=True))
    except ValidationError as exc:
        return jsonify({"error": "validation_failed", "detail": exc.errors()}), 400

    session = get_session()
    try:
        publisher = current_app.extensions["publisher"]
        result = ReIngestionService(session, publisher).reingest_events(req)
        return jsonify(result), 202
    except Exception as exc:
        logger.exception("reingest_events_api_failed", error=str(exc))
        return jsonify({"error": "reingest_failed", "detail": str(exc)}), 500
    finally:
        session.close()
