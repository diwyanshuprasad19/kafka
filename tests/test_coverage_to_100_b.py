"""Bring remaining src coverage gaps to 100% with mocks (no live Kafka/Postgres)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from flask import Flask

# ---------------------------------------------------------------------------
# domain / settings / small helpers
# ---------------------------------------------------------------------------


def test_query_aggregates_cache_and_history():
    from checkpoint_platform.application.query_aggregates import AggregateQueryService

    cache = MagicMock()
    cache.get.return_value = {"x": 1}
    cache.get_cafe.return_value = {"y": 2}
    session = MagicMock()
    svc = AggregateQueryService(session, cache)

    hit = svc.get_counter("c", "2024-01-01", "LUNCH")
    assert hit["cache"] == "hit"
    cafe_hit = svc.get_cafe("cafe", "2024-01-01")
    assert cafe_hit["cache"] == "hit"

    cache.get.return_value = None
    row = MagicMock()
    row.aggregation_date = date(2024, 1, 1)
    row.client_id = "cli"
    row.cafe_id = "cafe"
    row.counter_id = "c"
    row.meal_type = "LUNCH"
    row.total_checkpoints = 10
    row.completed_checkpoints = 8
    row.failed_checkpoints = 1
    row.pending_checkpoints = 1
    row.hygiene_pass_count = 0
    row.hygiene_fail_count = 0
    row.temperature_pass_count = 0
    row.temperature_fail_count = 0
    row.incident_count = 0
    row.open_incidents = 0
    row.food_received_kg = 0
    row.food_prepared_kg = 5
    row.food_consumed_kg = 0
    row.food_wastage_kg = 1
    session.execute.return_value.scalar_one_or_none.return_value = row
    miss = svc.get_counter("c", "2024-01-01", "LUNCH")
    assert miss["cache"] == "miss"

    # client from rollup
    rrow = MagicMock()
    rrow.cafes = 2
    rrow.counters = 3
    rrow.refreshed_at = datetime.now(UTC)
    for name in (
        "total_checkpoints",
        "completed_checkpoints",
        "failed_checkpoints",
        "pending_checkpoints",
        "hygiene_pass_count",
        "hygiene_fail_count",
        "temperature_pass_count",
        "temperature_fail_count",
        "incident_count",
        "open_incidents",
        "food_received_kg",
        "food_prepared_kg",
        "food_consumed_kg",
        "food_wastage_kg",
    ):
        setattr(rrow, name, 1 if "kg" not in name else Decimal(1))
    session.execute.return_value.scalars.return_value.all.return_value = [rrow]
    client = svc.get_client("cli", "2024-01-01")
    assert client["source"] == "rollup"

    # counter_history with meal filter
    session.execute.return_value.scalars.return_value.all.return_value = [row]
    hist = svc.counter_history("c", from_date="2024-01-01", to_date="2024-01-02", meal_type="LUNCH")
    assert hist["count"] == 1

    cafe_h = svc.cafe_history("cafe", from_date="2024-01-01", to_date="2024-01-02")
    assert cafe_h["day_count"] == 1
    client_h = svc.client_history("cli", from_date="2024-01-01", to_date="2024-01-02")
    assert client_h["day_count"] == 1

    assert svc.processed_event("not-a-uuid") is None
    pe = MagicMock()
    pe.event_id = uuid4()
    pe.checkpoint_id = "cp"
    pe.processed_at = datetime.now(UTC)
    session.get.return_value = pe
    assert svc.processed_event(str(pe.event_id))["checkpoint_id"] == "cp"

    snap = MagicMock()
    snap.counter_id = "c"
    snap.cafe_id = "cafe"
    snap.client_id = "cli"
    snap.meal_type = "LUNCH"
    snap.aggregation_date = date(2024, 1, 1)
    snap.payload = {}
    snap.received_at = datetime.now(UTC)
    session.execute.return_value.scalar_one_or_none.return_value = snap
    assert svc.reporting_snapshot("c", "2024-01-01", "LUNCH")["counter_id"] == "c"

    dlq_row = MagicMock()
    dlq_row.id = 1
    _ = session.execute.return_value
    svc.dlq_repo = MagicMock()
    svc.dlq_repo.get_by_id.return_value = None
    assert svc.get_dlq(1) is None
    svc.dlq_repo.get_by_id.return_value = SimpleNamespace(
        id=1,
        error="e",
        retry_count=0,
        original_topic="t",
        original_partition=0,
        original_offset=1,
        failed_at=datetime.now(UTC),
        original_event={},
        reingest_status="pending",
        reingested_at=None,
        reingest_event_id=None,
        reingest_correlation_id=None,
    )
    assert svc.get_dlq(1)["id"] == 1


# ---------------------------------------------------------------------------
# reingestion
# ---------------------------------------------------------------------------


def _checkpoint_payload(**overrides):
    base = {
        "event_id": str(uuid4()),
        "event_type": "checkpoint.completed",
        "checkpoint_id": "cp-r",
        "checkpoint_version": 1,
        "client_id": "c",
        "cafe_id": "cafe",
        "counter_id": "ctr",
        "meal_type": "LUNCH",
        "checkpoint_type": "FOOD_PREPARED",
        "status": "COMPLETED",
        "value": "1",
        "unit": "KG",
        "occurred_at": "2024-01-01T12:00:00Z",
    }
    base.update(overrides)
    return base


def test_reingestion_service_paths():
    from checkpoint_platform.application.reingestion import ReIngestionService
    from checkpoint_platform.domain.events import ReingestDlqRequest, ReingestEventsRequest

    session = MagicMock()
    publisher = MagicMock()
    publisher.flush.return_value = 0
    svc = ReIngestionService(session, publisher)

    good = MagicMock()
    good.id = 1
    good.original_event = _checkpoint_payload()
    nested = MagicMock()
    nested.id = 2
    nested.original_event = {"original_event": _checkpoint_payload(checkpoint_id="cp-nested")}
    bad = MagicMock()
    bad.id = 3
    bad.original_event = {"no": "checkpoint"}

    svc.dlq_repo = MagicMock()
    svc.dlq_repo.get_by_ids.return_value = [good, nested, bad]
    svc.processed_repo = MagicMock()
    svc.processed_repo.delete.side_effect = RuntimeError("ignore")

    req = ReingestDlqRequest(
        dlq_ids=[1, 2, 3, 99], new_event_id=True, force=True, reset_retry_count=True
    )
    result = svc.reingest_dlq(req)
    assert 99 in result["missing_ids"]
    assert any(r.get("status") == "reingested" for r in result["results"])
    assert any(r.get("status") == "failed" for r in result["results"])

    # force without new_event_id deletes processed
    good2 = MagicMock()
    good2.id = 10
    good2.original_event = _checkpoint_payload()
    svc.dlq_repo.get_by_ids.return_value = [good2]
    svc.processed_repo.delete.side_effect = None
    req2 = ReingestDlqRequest(dlq_ids=[10], new_event_id=False, force=True, reset_retry_count=False)
    assert svc.reingest_dlq(req2)["results"][0]["status"] == "reingested"

    publisher.publish_checkpoint.side_effect = None

    def pub_side(event):
        if getattr(event, "checkpoint_id", None) == "boom":
            raise RuntimeError("pub fail")

    # rebuild events request carefully
    ok_payload = _checkpoint_payload()
    boom = _checkpoint_payload(checkpoint_id="boom")
    publisher.publish_checkpoint.side_effect = [None, RuntimeError("pub fail")]
    out = svc.reingest_events(
        ReingestEventsRequest(events=[ok_payload, {"not": "valid"}, boom], new_event_id=False)
    )
    statuses = [r["status"] for r in out["results"]]
    assert "published" in statuses
    assert "failed" in statuses

    # flush incomplete after successful buffer → rollback, no commit
    svc.dlq_repo.get_by_ids.return_value = [good2]
    publisher.flush.return_value = 3
    with pytest.raises(RuntimeError, match="flush incomplete"):
        svc.reingest_dlq(ReingestDlqRequest(dlq_ids=[10], new_event_id=True))
    session.rollback.assert_called()
    publisher.flush.return_value = 0


# ---------------------------------------------------------------------------
# HTTP route edge paths (mocked session/query)
# ---------------------------------------------------------------------------


def _flask_app(session, cache=None, publisher=None):
    from checkpoint_platform.interfaces.http.middleware import register_observability
    from checkpoint_platform.interfaces.http.routes import (
        aggregations,
        dlq,
        health,
        history,
        ops,
        reingest,
    )

    app = Flask("cov")
    app.extensions["cache"] = cache or MagicMock()
    app.extensions["publisher"] = publisher or MagicMock()
    register_observability(app, service="cov")

    def _session():
        return session

    patches = [
        patch("checkpoint_platform.interfaces.http.routes.health.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.aggregations.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.dlq.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.ops.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.history.get_session", _session),
        patch("checkpoint_platform.interfaces.http.routes.reingest.get_session", _session),
    ]
    for p in patches:
        p.start()
    for mod in (health, aggregations, history, dlq, reingest, ops):
        app.register_blueprint(mod.bp)
    app._patches = patches  # type: ignore[attr-defined]
    return app


def test_http_route_error_and_not_found_paths():
    session = MagicMock()
    session.execute.side_effect = RuntimeError("db down")
    app = _flask_app(session)
    try:
        client = app.test_client()
        r = client.get("/ready")
        assert r.status_code == 503

        qsvc = MagicMock()
        qsvc.get_counter.return_value = None
        qsvc.get_cafe.return_value = None
        qsvc.get_checkpoint_state.return_value = None
        qsvc.processed_event.return_value = None
        qsvc.get_dlq.return_value = None
        with patch(
            "checkpoint_platform.interfaces.http.routes.aggregations.get_query_service",
            return_value=qsvc,
        ):
            assert client.get("/aggregations/counter/x").status_code == 404
            assert client.get("/aggregations/cafe/x").status_code == 404
        with patch(
            "checkpoint_platform.interfaces.http.routes.history.get_query_service",
            return_value=qsvc,
        ):
            assert client.get("/checkpoints/state/x").status_code == 404
            assert client.get("/audit/processed/bad").status_code == 404
            # parse_dt
            from checkpoint_platform.interfaces.http.routes import history as hist

            assert hist._parse_dt(None) is None
            assert hist._parse_dt("2024-01-01T00:00:00") is not None
            assert client.get(
                "/history/aggregations/counter/c1?from_date=not-a-date"
            ).status_code == 400
            assert client.get("/history/events/counter/c1?limit=abc").status_code == 400
            assert client.get("/history/events/counter/c1?limit=-1").status_code == 400
            assert client.get("/dlq?limit=nope").status_code == 400
            # success path for state/audit when found
            qsvc.get_checkpoint_state.return_value = {"checkpoint_id": "x"}
            qsvc.processed_event.return_value = {"event_id": "e"}
            assert client.get("/checkpoints/state/x").status_code == 200
            assert client.get("/audit/processed/e").status_code == 200
            # reporting snapshot 404 path if any
            if hasattr(hist, "reporting_snapshot") or True:
                pass
        with patch(
            "checkpoint_platform.interfaces.http.routes.dlq.get_query_service",
            return_value=qsvc,
        ):
            assert client.get("/dlq/99").status_code == 404
            qsvc.get_dlq.return_value = {"id": 1}
            assert client.get("/dlq/1").status_code == 200

        # ops status with failing deps + scale
        settings = MagicMock()
        settings.app_env = "local"
        settings.redis_enabled = True
        settings.redis_url = "redis://localhost:0"
        settings.checkpoint_partitions = 6
        settings.kafka_throughput_mode = "reliable"
        settings.max_retries = 3
        settings.consumer_group_id = "g"
        settings.checkpoint_topic = "a"
        settings.retry_topic = "b"
        settings.dlq_topic = "c"
        settings.aggregation_topic = "d"
        settings.kafka_client_config.return_value = {}
        with (
            patch(
                "checkpoint_platform.interfaces.http.routes.ops.get_settings", return_value=settings
            ),
            patch("redis.from_url", side_effect=RuntimeError("redis down")),
            patch(
                "confluent_kafka.admin.AdminClient",
                side_effect=RuntimeError("kafka down"),
            ),
        ):
            st = client.get("/ops/status")
            assert st.status_code == 200
            assert st.json["dependencies"]["kafka"] == "down"
        # kafka AdminClient success path (ops.py:61-63)
        md = MagicMock()
        md.brokers = {0: MagicMock(), 1: MagicMock()}
        md.topics = {"a": MagicMock(), "b": MagicMock()}
        admin = MagicMock()
        admin.list_topics.return_value = md
        with (
            patch(
                "checkpoint_platform.interfaces.http.routes.ops.get_settings", return_value=settings
            ),
            patch("redis.from_url", return_value=MagicMock()),
            patch("confluent_kafka.admin.AdminClient", return_value=admin),
        ):
            st_ok = client.get("/ops/status")
            assert st_ok.status_code == 200
            assert st_ok.json["dependencies"]["kafka"] == "ok"
            assert st_ok.json["dependencies"]["kafka_brokers"] == 2
        assert client.get("/ops/scale").status_code == 200

        # reingest validation + 500 paths
        with patch("checkpoint_platform.interfaces.http.routes.reingest.ReIngestionService") as RIS:
            RIS.return_value.reingest_dlq.side_effect = RuntimeError("fail")
            assert client.post("/reingest/dlq", json={"dlq_ids": [1]}).status_code == 500
            assert client.post("/reingest/dlq/1", json={}).status_code == 500
            RIS.return_value.reingest_events.side_effect = RuntimeError("fail")
            assert (
                client.post(
                    "/reingest/events",
                    json={"events": [_checkpoint_payload()]},
                ).status_code
                == 500
            )
        assert client.post("/reingest/dlq", json={"dlq_ids": "bad"}).status_code == 400
        assert client.post("/reingest/dlq/1", json={"force": "nope"}).status_code == 400
        assert client.post("/reingest/events", json={"events": []}).status_code in {400, 202}
    finally:
        for p in app._patches:  # type: ignore[attr-defined]
            p.stop()


def test_app_cors_and_main_pragmas():
    from checkpoint_platform.interfaces.http import app as app_mod

    flask_app = Flask("x")
    settings = MagicMock()
    settings.cors_origins = ""
    app_mod._configure_cors(flask_app, settings)
    settings.cors_origins = "http://a.com, http://b.com"
    app_mod._configure_cors(flask_app, settings)

    with patch.object(
        app_mod,
        "get_settings",
        return_value=MagicMock(api_host="0.0.0.0", api_port=8080, app_env="local"),
    ):
        with patch.object(app_mod.app, "run") as run:
            app_mod.main()
            run.assert_called()


def test_final_thirteen_misses(tmp_path, monkeypatch):
    """Close the last uncovered branches to hit TOTAL 100%."""
    from decimal import Decimal

    from checkpoint_platform.application.aggregation import Contribution
    from checkpoint_platform.application.reingestion import ReIngestionService
    from checkpoint_platform.config import settings as settings_mod
    from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType
    from checkpoint_platform.domain.events import CheckpointEvent
    from checkpoint_platform.interfaces.http.routes import aggregations, history

    # aggregation.py:133 — temperature success delta
    c = Contribution(
        checkpoint_type=CheckpointType.HOT_FOOD_TEMPERATURE,
        status=CheckpointStatus.PASS,
        value_kg=Decimal("0"),
    )
    d = c.deltas(1)
    assert d["temperature_pass_count"] == 1

    # events.py:68 — reject non-finite value (call validator directly;
    # pydantic also rejects Infinity before/alongside this path)
    with pytest.raises(ValueError, match="finite"):
        CheckpointEvent.value_must_be_finite(Decimal("Infinity"))
    with pytest.raises(ValueError, match="finite"):
        CheckpointEvent.value_must_be_finite(Decimal("NaN"))

    # settings.py:20-26 + :55 — parent walk and switch env file
    configs = tmp_path / "configs"
    configs.mkdir()
    (tmp_path / "pyproject.toml").write_text("[project]\nname='t'\n")
    (configs / "local.env").write_text("APP_ENV=local\n")
    (configs / "backend.switch.env").write_text("X=1\n")
    monkeypatch.setenv("CHECKPOINT_ROOT", str(tmp_path))
    assert settings_mod._find_repo_root() == tmp_path
    files2 = settings_mod._env_files()
    assert any(f.endswith("backend.switch.env") for f in files2)

    monkeypatch.delenv("CHECKPOINT_ROOT", raising=False)
    empty_cwd = tmp_path / "empty_cwd"
    empty_cwd.mkdir()
    monkeypatch.chdir(empty_cwd)
    # Walk from a fake __file__ under tmp_path → hits lines 20-25
    fake_under = tmp_path / "pkg" / "settings.py"
    fake_under.parent.mkdir(parents=True)
    fake_under.write_text("#")
    with patch.object(settings_mod, "__file__", str(fake_under)):
        assert settings_mod._find_repo_root() == tmp_path
    # No configs anywhere above fake __file__ → line 26 return cwd
    lonely_root = tmp_path.parent / f"lonely-{tmp_path.name}"
    lonely_root.mkdir(exist_ok=True)
    lonely = lonely_root / "lonely_settings.py"
    lonely.write_text("#")
    monkeypatch.chdir(lonely_root)
    with patch.object(settings_mod, "__file__", str(lonely)):
        assert settings_mod._find_repo_root() == lonely_root

    # reingestion.py — force delete failure must fail the reingest (no silent skip)
    publisher = MagicMock()
    session = MagicMock()
    processed = MagicMock()
    processed.delete.side_effect = RuntimeError("gone")
    dlq = MagicMock()
    with (
        patch("checkpoint_platform.application.reingestion.DlqRepo", return_value=dlq),
        patch(
            "checkpoint_platform.application.reingestion.ProcessedEventRepo",
            return_value=processed,
        ),
        patch("checkpoint_platform.application.reingestion.CheckpointEvent") as CE,
        patch("checkpoint_platform.application.reingestion.REINGEST_EVENTS"),
    ):
        CE.model_validate.return_value = MagicMock(
            event_id=uuid4(), checkpoint_id="cp", counter_id="c"
        )
        svc = ReIngestionService(session=session, publisher=publisher)
        row = MagicMock()
        row.id = 9
        row.original_event = {
            "event_id": str(uuid4()),
            "checkpoint_id": "cp-1",
            "counter_id": "ctr",
            "cafe_id": "cafe",
            "client_id": "cli",
            "checkpoint_type": CheckpointType.HOT_FOOD_TEMPERATURE.value,
            "status": CheckpointStatus.PASS.value,
            "occurred_at": datetime.now(UTC).isoformat(),
        }
        with pytest.raises(ValueError, match="force reingest cannot clear"):
            svc._reingest_one_dlq(
                row=row,
                new_event_id=False,
                force=True,
                reset_retry_count=True,
                correlation_id="corr",
            )
        processed.delete.assert_called()
        publisher.publish_checkpoint.assert_not_called()

    # aggregations.py:19,33 + history.py:181 — success jsonify paths
    session2 = MagicMock()
    qs = MagicMock()
    qs.get_counter.return_value = {"ok": 1}
    qs.get_cafe.return_value = {"ok": 2}
    qs.reporting_snapshot.return_value = {"ok": 3}
    app = Flask("gap")
    app.extensions["cache"] = MagicMock()
    app.register_blueprint(aggregations.bp)
    app.register_blueprint(history.bp)
    with (
        patch.object(aggregations, "get_session", return_value=session2),
        patch.object(aggregations, "get_query_service", return_value=qs),
        patch.object(history, "get_session", return_value=session2),
        patch.object(history, "get_query_service", return_value=qs),
        app.test_client() as client,
    ):
        r1 = client.get("/aggregations/counter/c1")
        assert r1.status_code == 200 and r1.json["ok"] == 1
        r2 = client.get("/aggregations/cafe/cafe1")
        assert r2.status_code == 200 and r2.json["ok"] == 2
        r3 = client.get("/reporting/counter/c1")
        assert r3.status_code == 200 and r3.json["ok"] == 3
        # history datetime validation → 400 (not 500)
        bad = client.get("/history/events/counter/c1?from=not-a-date")
        assert bad.status_code == 400
        bad2 = client.get("/history/events/cafe/cafe1?to=also-bad")
        assert bad2.status_code == 400
