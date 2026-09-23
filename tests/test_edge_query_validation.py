"""Edge validation for history/dlq query params + reingest flush."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from flask import Flask


def _app(session):
    from checkpoint_platform.interfaces.http.middleware import register_observability
    from checkpoint_platform.interfaces.http.routes import dlq, history

    app = Flask("edge_q")
    app.extensions["cache"] = MagicMock()
    register_observability(app, service="edge_q")

    def _session():
        return session

    patches = [
        patch("checkpoint_platform.interfaces.http.routes.history.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.dlq.get_session", _session),
    ]
    for p in patches:
        p.start()
    app.register_blueprint(history.bp)
    app.register_blueprint(dlq.bp)
    app._patches = patches  # type: ignore[attr-defined]
    return app


def test_history_and_dlq_invalid_params_return_400():
    session = MagicMock()
    qsvc = MagicMock()
    qsvc.counter_history.return_value = []
    qsvc.cafe_history.return_value = []
    qsvc.client_history.return_value = []
    qsvc.checkpoint_event_history.return_value = []
    qsvc.counter_event_history.return_value = []
    qsvc.cafe_event_history.return_value = []
    qsvc.list_counter_states.return_value = []
    qsvc.list_processed_for_checkpoint.return_value = []
    qsvc.reporting_snapshot.return_value = None
    qsvc.list_dlq.return_value = []
    app = _app(session)
    try:
        client = app.test_client()
        with (
            patch(
                "checkpoint_platform.interfaces.http.routes.history.get_query_service",
                return_value=qsvc,
            ),
            patch(
                "checkpoint_platform.interfaces.http.routes.dlq.get_query_service",
                return_value=qsvc,
            ),
        ):
            assert client.get("/history/aggregations/cafe/c1?from_date=bad").status_code == 400
            assert client.get("/history/aggregations/client/c1?to_date=bad").status_code == 400
            assert client.get("/history/events/checkpoint/x?limit=0").status_code == 400
            assert client.get("/history/events/cafe/c1?offset=-1").status_code == 400
            assert client.get("/history/events/cafe/c1?offset=abc").status_code == 400
            assert client.get("/history/events/cafe/c1?from=not-dt").status_code == 400
            assert client.get("/checkpoints/state/counter/c1?limit=xyz").status_code == 400
            assert client.get("/audit/processed/checkpoint/x?limit=0").status_code == 400
            assert client.get("/reporting/counter/c1?date=nope").status_code == 400
            assert client.get("/dlq?limit=0").status_code == 400
            # happy defaults still work
            assert client.get("/history/aggregations/cafe/c1").status_code == 200
            assert client.get("/history/events/checkpoint/x").status_code == 200
            assert client.get("/checkpoints/state/counter/c1").status_code == 200
            assert client.get("/audit/processed/checkpoint/x").status_code == 200
            assert client.get("/reporting/counter/c1").status_code == 404
            assert client.get("/dlq").status_code == 200
    finally:
        for p in app._patches:  # type: ignore[attr-defined]
            p.stop()


def test_reingest_events_flush_incomplete():
    from checkpoint_platform.application.reingestion import ReIngestionService
    from checkpoint_platform.domain.events import ReingestEventsRequest

    session = MagicMock()
    publisher = MagicMock()
    publisher.flush.return_value = 2
    svc = ReIngestionService(session, publisher)
    payload = {
        "event_id": "00000000-0000-4000-8000-000000000001",
        "event_type": "checkpoint.created",
        "checkpoint_id": "cp-1",
        "checkpoint_type": "MEAL_READINESS",
        "counter_id": "c1",
        "cafe_id": "cafe-1",
        "client_id": "cli-1",
        "status": "PENDING",
        "occurred_at": "2024-01-01T12:00:00Z",
    }
    with __import__("pytest").raises(RuntimeError, match="flush incomplete"):
        svc.reingest_events(ReingestEventsRequest(events=[payload], new_event_id=False))
