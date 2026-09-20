"""History + state + audit query routes (prod read model)."""

from __future__ import annotations

from datetime import datetime

from flask import Blueprint, current_app, jsonify, request

from checkpoint_platform.config.container import get_query_service, get_session
from checkpoint_platform.domain.business_day import business_today

bp = Blueprint("history", __name__)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


@bp.get("/history/aggregations/counter/<counter_id>")
def counter_agg_history(counter_id: str):
    """Daily aggregate history for a counter (date range)."""
    from_date = request.args.get("from_date", business_today().isoformat())
    to_date = request.args.get("to_date", from_date)
    meal = request.args.get("meal_type")
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.counter_history(counter_id, from_date=from_date, to_date=to_date, meal_type=meal)
        )
    finally:
        session.close()


@bp.get("/history/aggregations/cafe/<cafe_id>")
def cafe_agg_history(cafe_id: str):
    from_date = request.args.get("from_date", business_today().isoformat())
    to_date = request.args.get("to_date", from_date)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.cafe_history(cafe_id, from_date=from_date, to_date=to_date))
    finally:
        session.close()


@bp.get("/history/aggregations/client/<client_id>")
def client_agg_history(client_id: str):
    from_date = request.args.get("from_date", business_today().isoformat())
    to_date = request.args.get("to_date", from_date)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.client_history(client_id, from_date=from_date, to_date=to_date))
    finally:
        session.close()


@bp.get("/history/events/checkpoint/<checkpoint_id>")
def checkpoint_versions(checkpoint_id: str):
    """Append-only applied versions for one checkpoint_id."""
    limit = min(int(request.args.get("limit", 100)), 500)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.checkpoint_event_history(checkpoint_id, limit=limit))
    finally:
        session.close()


@bp.get("/history/events/counter/<counter_id>")
def counter_events(counter_id: str):
    """Event-level history for a counter (filters + pagination)."""
    limit = min(int(request.args.get("limit", 100)), 500)
    offset = max(int(request.args.get("offset", 0)), 0)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.counter_event_history(
                counter_id,
                from_ts=_parse_dt(request.args.get("from")),
                to_ts=_parse_dt(request.args.get("to")),
                meal_type=request.args.get("meal_type"),
                checkpoint_type=request.args.get("checkpoint_type"),
                limit=limit,
                offset=offset,
            )
        )
    finally:
        session.close()


@bp.get("/history/events/cafe/<cafe_id>")
def cafe_events(cafe_id: str):
    limit = min(int(request.args.get("limit", 100)), 500)
    offset = max(int(request.args.get("offset", 0)), 0)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.cafe_event_history(
                cafe_id,
                from_ts=_parse_dt(request.args.get("from")),
                to_ts=_parse_dt(request.args.get("to")),
                limit=limit,
                offset=offset,
            )
        )
    finally:
        session.close()


@bp.get("/checkpoints/state/<checkpoint_id>")
def checkpoint_state(checkpoint_id: str):
    """Current checkpoint_state row."""
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.get_checkpoint_state(checkpoint_id)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()


@bp.get("/checkpoints/state/counter/<counter_id>")
def counter_states(counter_id: str):
    limit = min(int(request.args.get("limit", 100)), 500)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.list_counter_states(
                counter_id,
                checkpoint_type=request.args.get("checkpoint_type"),
                limit=limit,
            )
        )
    finally:
        session.close()


@bp.get("/audit/processed/<event_id>")
def audit_processed(event_id: str):
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.processed_event(event_id)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()


@bp.get("/audit/processed/checkpoint/<checkpoint_id>")
def audit_processed_checkpoint(checkpoint_id: str):
    limit = min(int(request.args.get("limit", 50)), 200)
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.list_processed_for_checkpoint(checkpoint_id, limit=limit))
    finally:
        session.close()


@bp.get("/reporting/counter/<counter_id>")
def reporting_counter(counter_id: str):
    day = request.args.get("date", business_today().isoformat())
    meal = request.args.get("meal_type", "LUNCH")
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        payload = svc.reporting_snapshot(counter_id, day, meal)
        if not payload:
            return jsonify({"error": "not_found"}), 404
        return jsonify(payload)
    finally:
        session.close()


@bp.get("/ops/outbox")
def outbox_stats():
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.outbox_stats())
    finally:
        session.close()
