from flask import Blueprint, current_app, jsonify, request

from checkpoint_platform.config.container import get_query_service, get_session

bp = Blueprint("dlq", __name__)


@bp.get("/dlq")
def list_dlq():
    limit = min(int(request.args.get("limit", 50)), 200)
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
