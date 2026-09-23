"""History + state + audit query routes (prod read model)."""

from __future__ import annotations

from datetime import date, datetime

from flask import Blueprint, current_app, jsonify, request

from checkpoint_platform.config.container import get_query_service, get_session
from checkpoint_platform.domain.business_day import business_today

bp = Blueprint("history", __name__)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid datetime: {value!r}") from exc


def _parse_date(value: str) -> str:
    """Validate YYYY-MM-DD (or ISO date) and return the original string."""
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date: {value!r}") from exc
    return value


def _parse_limit(raw: str | None, *, default: int = 100, max_value: int = 500) -> int:
    try:
        value = int(default if raw is None else raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid limit: {raw!r}") from exc
    if value < 1:
        raise ValueError(f"limit must be >= 1, got {value}")
    return min(value, max_value)


def _parse_offset(raw: str | None, *, default: int = 0) -> int:
    try:
        value = int(default if raw is None else raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid offset: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"offset must be >= 0, got {value}")
    return value


@bp.get("/history/aggregations/counter/<counter_id>")
def counter_agg_history(counter_id: str):
    """Daily aggregate history for a counter (date range)."""
    try:
        from_date = _parse_date(request.args.get("from_date", business_today().isoformat()))
        to_date = _parse_date(request.args.get("to_date", from_date))
    except ValueError as exc:
        return jsonify({"error": "invalid_date", "detail": str(exc)}), 400
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
    try:
        from_date = _parse_date(request.args.get("from_date", business_today().isoformat()))
        to_date = _parse_date(request.args.get("to_date", from_date))
    except ValueError as exc:
        return jsonify({"error": "invalid_date", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.cafe_history(cafe_id, from_date=from_date, to_date=to_date))
    finally:
        session.close()


@bp.get("/history/aggregations/client/<client_id>")
def client_agg_history(client_id: str):
    try:
        from_date = _parse_date(request.args.get("from_date", business_today().isoformat()))
        to_date = _parse_date(request.args.get("to_date", from_date))
    except ValueError as exc:
        return jsonify({"error": "invalid_date", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.client_history(client_id, from_date=from_date, to_date=to_date))
    finally:
        session.close()


@bp.get("/history/events/checkpoint/<checkpoint_id>")
def checkpoint_versions(checkpoint_id: str):
    """Append-only applied versions for one checkpoint_id."""
    try:
        limit = _parse_limit(request.args.get("limit"), default=100, max_value=500)
    except ValueError as exc:
        return jsonify({"error": "invalid_limit", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.checkpoint_event_history(checkpoint_id, limit=limit))
    finally:
        session.close()


@bp.get("/history/events/counter/<counter_id>")
def counter_events(counter_id: str):
    """Event-level history for a counter (filters + pagination)."""
    try:
        limit = _parse_limit(request.args.get("limit"), default=100, max_value=500)
        offset = _parse_offset(request.args.get("offset"), default=0)
        from_ts = _parse_dt(request.args.get("from"))
        to_ts = _parse_dt(request.args.get("to"))
    except ValueError as exc:
        return jsonify({"error": "invalid_query", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.counter_event_history(
                counter_id,
                from_ts=from_ts,
                to_ts=to_ts,
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
    try:
        limit = _parse_limit(request.args.get("limit"), default=100, max_value=500)
        offset = _parse_offset(request.args.get("offset"), default=0)
        from_ts = _parse_dt(request.args.get("from"))
        to_ts = _parse_dt(request.args.get("to"))
    except ValueError as exc:
        return jsonify({"error": "invalid_query", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(
            svc.cafe_event_history(
                cafe_id,
                from_ts=from_ts,
                to_ts=to_ts,
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
    try:
        limit = _parse_limit(request.args.get("limit"), default=100, max_value=500)
    except ValueError as exc:
        return jsonify({"error": "invalid_limit", "detail": str(exc)}), 400
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
    try:
        limit = _parse_limit(request.args.get("limit"), default=50, max_value=200)
    except ValueError as exc:
        return jsonify({"error": "invalid_limit", "detail": str(exc)}), 400
    session = get_session()
    try:
        svc = get_query_service(session, current_app.extensions["cache"])
        return jsonify(svc.list_processed_for_checkpoint(checkpoint_id, limit=limit))
    finally:
        session.close()


@bp.get("/reporting/counter/<counter_id>")
def reporting_counter(counter_id: str):
    try:
        day = _parse_date(request.args.get("date", business_today().isoformat()))
    except ValueError as exc:
        return jsonify({"error": "invalid_date", "detail": str(exc)}), 400
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
