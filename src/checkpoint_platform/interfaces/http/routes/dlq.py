from flask import Blueprint, current_app, jsonify, request

from checkpoint_platform.config.container import get_query_service, get_session

bp = Blueprint("dlq", __name__)


def _parse_limit(raw: str | None, *, default: int = 50, max_value: int = 200) -> int:
    try:
        value = int(default if raw is None else raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid limit: {raw!r}") from exc
    if value < 1:
        raise ValueError(f"limit must be >= 1, got {value}")
    return min(value, max_value)


@bp.get("/dlq")
def list_dlq():
    try:
        limit = _parse_limit(request.args.get("limit"), default=50, max_value=200)
    except ValueError as exc:
        return jsonify({"error": "invalid_limit", "detail": str(exc)}), 400
    status = request.args.get("status")  # pending | reingested | skipped
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.list_dlq(limit=limit, status=status))
    finally:
        session.close()


@bp.get("/dlq/<int:dlq_id>")
def get_dlq(dlq_id: int):
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.get_dlq(dlq_id)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()
