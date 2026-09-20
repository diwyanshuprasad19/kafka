"""Tests for re-ingestion request models and logging helpers."""

from checkpoint_platform.domain.events import ReingestDlqRequest, ReingestEventsRequest
from checkpoint_platform.infrastructure.observability.logging import (
    bind_context,
    clear_context,
    get_correlation_id,
    new_request_ids,
)
from checkpoint_platform.infrastructure.observability.metrics import metrics_payload


def test_reingest_dlq_request():
    req = ReingestDlqRequest(dlq_ids=[1, 2], new_event_id=True)
    assert req.force is False
    assert len(req.dlq_ids) == 2


def test_reingest_events_request():
    req = ReingestEventsRequest(
        events=[
            {
                "event_id": "11111111-1111-1111-1111-111111111111",
                "event_type": "checkpoint.completed",
                "checkpoint_id": "cp-1",
                "checkpoint_version": 1,
                "client_id": "c",
                "cafe_id": "cafe",
                "counter_id": "counter-1",
                "meal_type": "LUNCH",
                "checkpoint_type": "FOOD_WASTAGE",
                "status": "COMPLETED",
                "value": 10,
                "unit": "KG",
                "occurred_at": "2026-09-15T12:00:00Z",
            }
        ]
    )
    assert req.new_event_id is True


def test_correlation_context():
    clear_context()
    rid, cid = new_request_ids(incoming_correlation_id="corr-abc")
    bind_context(request_id=rid, correlation_id=cid)
    assert get_correlation_id() == "corr-abc"
    clear_context()
    assert get_correlation_id() is None


def test_metrics_payload_bytes():
    payload, content_type = metrics_payload()
    assert b"checkpoint_" in payload or b"http_" in payload or len(payload) > 0
    assert "text/plain" in content_type or "openmetrics" in content_type
