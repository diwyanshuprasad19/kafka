from flask import Blueprint, current_app, jsonify, request

from checkpoint_platform.config.container import get_query_service, get_session
from checkpoint_platform.domain.business_day import business_today

bp = Blueprint("aggregations", __name__)


@bp.get("/aggregations/counter/<counter_id>")
def get_counter_aggregation(counter_id: str):
    day = request.args.get("date", business_today().isoformat())
    meal = request.args.get("meal_type", "LUNCH")
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.get_counter(counter_id, day, meal)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()


@bp.get("/aggregations/cafe/<cafe_id>")
def get_cafe_aggregation(cafe_id: str):
    day = request.args.get("date", business_today().isoformat())
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.get_cafe(cafe_id, day)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()


@bp.get("/aggregations/client/<client_id>")
def get_client_aggregation(client_id: str):
    day = request.args.get("date", business_today().isoformat())
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.get_client(client_id, day))
    finally:
        session.close()
