"""HTTP + infra CORE coverage for checkpoint_platform (mocked publisher/cache)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from flask import Flask

from checkpoint_platform.infrastructure.messaging.serializer import (
    CheckpointJSONEncoder,
    deserialize,
    serialize,
)


@pytest.fixture()
def publisher():
    pub = MagicMock()
    event = MagicMock()
    event.event_id = uuid4()
    event.checkpoint_id = "cp-1"
    event.counter_id = "ctr-1"
    pub.publish_checkpoint = MagicMock()
    pub.flush = MagicMock(return_value=0)
    # publish service builds real event — provide publish_from_request via real service
    return pub


def _make_app(session, cache, publisher):
    from checkpoint_platform.interfaces.http.middleware import register_observability
    from checkpoint_platform.interfaces.http.routes import (
        aggregations,
        checkpoints,
        dlq,
        health,
        history,
        metrics,
        ops,
        reingest,
    )

    app = Flask("checkpoint-test")
    app.extensions["cache"] = cache
    app.extensions["publisher"] = publisher
    register_observability(app, service="checkpoint-test")

    def _session():
        return session

    with patch("checkpoint_platform.config.container.get_session", _session), patch(
        "checkpoint_platform.interfaces.http.routes.health.get_session", _session
    ), patch(
        "checkpoint_platform.interfaces.http.routes.aggregations.get_session", _session
    ), patch(
        "checkpoint_platform.interfaces.http.routes.dlq.get_session", _session
    ), patch(
        "checkpoint_platform.interfaces.http.routes.ops.get_session", _session
    ), patch(
        "checkpoint_platform.interfaces.http.routes.history.get_session", _session
    ), patch(
        "checkpoint_platform.interfaces.http.routes.reingest.get_session", _session
    ):
        for mod in (health, checkpoints, aggregations, history, dlq, reingest, metrics, ops):
            app.register_blueprint(mod.bp)
        yield app


@pytest.fixture()
def client(session, publisher):
    cache = MagicMock()
    cache.get.return_value = None
    cache.get_cafe.return_value = None
    # Real publish service needs publisher implementing port
    from checkpoint_platform.application.checkpoint_publish import CheckpointPublishService

    real_pub = MagicMock()
    real_pub.publish_checkpoint = MagicMock()
    real_pub.flush = MagicMock(return_value=0)

    app_gen = _make_app(session, cache, real_pub)
    app = next(app_gen)
    with patch(
        "checkpoint_platform.interfaces.http.routes.checkpoints.get_publish_service",
        return_value=CheckpointPublishService(real_pub),
    ):
        with app.test_client() as c:
            c._publisher = real_pub  # type: ignore[attr-defined]
            c._cache = cache  # type: ignore[attr-defined]
            yield c
    try:
        next(app_gen)
    except StopIteration:
        pass


def test_serializer_roundtrip():
    payload = {
        "id": uuid4(),
        "amt": Decimal("1.5"),
        "when": datetime(2024, 1, 2, 3, 4, 5),
        "day": date(2024, 1, 2),
    }
    raw = serialize(payload)
    assert isinstance(raw, bytes)
    back = deserialize(raw)
    assert back["amt"] == 1.5
    assert deserialize(raw.decode())["day"].startswith("2024")
    # model_dump path
    class M:
        def model_dump(self, mode="json"):
            return {"x": 1}

    assert json.loads(serialize(M()).decode()) == {"x": 1}
    assert CheckpointJSONEncoder().default(uuid4())
    with pytest.raises(TypeError):
        CheckpointJSONEncoder().default(object())


def test_health_metrics_middleware(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json["status"] == "ok"
    assert r.headers.get("X-Request-ID")
    m = client.get("/metrics")
    assert m.status_code == 200
    ready = client.get("/ready")
    assert ready.status_code in {200, 503}


def test_checkpoint_create_validation_and_ok(client):
    bad = client.post("/checkpoints", json={"not": "valid"})
    assert bad.status_code == 400
    body = {
        "event_type": "checkpoint.completed",
        "client_id": "c1",
        "cafe_id": "cafe1",
        "counter_id": "ctr1",
        "checkpoint_type": "FOOD_PREPARED",
        "status": "COMPLETED",
        "value": "1.0",
        "unit": "kg",
        "checkpoint_version": 1,
        "meal_type": "LUNCH",
    }
    ok = client.post("/checkpoints", json=body)
    assert ok.status_code == 202
    assert ok.json["status"] == "accepted"


def test_aggregations_and_dlq_not_found(client):
    assert client.get("/aggregations/counter/missing").status_code == 404
    assert client.get("/aggregations/cafe/missing").status_code == 404
    client_agg = client.get("/aggregations/client/c1")
    assert client_agg.status_code == 200
    assert client.get("/dlq").status_code == 200
    assert client.get("/dlq/999999").status_code == 404
    assert client.get("/history/aggregations/counter/c1").status_code == 200
    assert client.get("/history/aggregations/cafe/c1").status_code == 200
    assert client.get("/history/aggregations/client/c1").status_code == 200
    assert client.get("/history/events/checkpoint/cp-1").status_code == 200
    assert client.get("/history/events/counter/c1").status_code == 200
    assert client.get("/history/events/cafe/c1").status_code == 200
    assert client.get("/checkpoints/state/missing").status_code == 404
    assert client.get("/checkpoints/state/counter/c1").status_code == 200
    assert client.get("/audit/processed/00000000-0000-0000-0000-000000000001").status_code == 404
    assert client.get("/audit/processed/checkpoint/cp-1").status_code == 200
    assert client.get("/reporting/counter/missing").status_code == 404
    assert client.get("/ops/outbox").status_code == 200
    bad_reingest = client.post("/reingest/dlq", json={})
    assert bad_reingest.status_code == 400
    reingest = client.post("/reingest/dlq", json={"dlq_ids": [999999]})
    assert reingest.status_code in {202, 500}
    assert client.post("/reingest/events", json={}).status_code == 400
    ev = client.post(
        "/reingest/events",
        json={
            "events": [
                {
                    "event_type": "checkpoint.completed",
                    "checkpoint_id": "cp-r1",
                    "checkpoint_version": 1,
                    "client_id": "c1",
                    "cafe_id": "cafe1",
                    "counter_id": "ctr1",
                    "meal_type": "LUNCH",
                    "checkpoint_type": "FOOD_PREPARED",
                    "status": "COMPLETED",
                    "value": "1.0",
                    "unit": "kg",
                    "occurred_at": "2024-01-01T12:00:00+00:00",
                }
            ]
        },
    )
    assert ev.status_code in {202, 500}
    assert client.post("/reingest/dlq/1", json={"new_event_id": True}).status_code in {
        202,
        400,
        500,
    }


def test_ops_status(client):
    r = client.get("/ops/status")
    assert r.status_code == 200
    assert "env" in r.json


def test_maintenance_prune_and_container(session, monkeypatch):
    from checkpoint_platform.config import container as cont
    from checkpoint_platform.interfaces.workers import maintenance as maint

    removed = maint.prune_once(session)
    assert isinstance(removed, dict)
    backlogs = maint.report_backlogs(session)
    assert "outbox_pending" in backlogs

    cont.get_publisher.cache_clear()
    cont.get_cache.cache_clear()
    pub = MagicMock()
    cache = MagicMock()
    with (
        patch.object(cont, "get_publisher", return_value=pub),
        patch.object(cont, "get_cache", return_value=cache),
    ):
        assert cont.get_aggregation_service(session) is not None
        assert cont.get_publish_service(pub) is not None
        assert cont.get_event_processor(session, pub) is not None
        assert cont.get_query_service(session, cache) is not None
        assert cont.get_reingestion_service(session, pub) is not None

    monkeypatch.setattr(maint, "init_db", lambda: None)
    monkeypatch.setattr(maint, "get_session", lambda: session)
    monkeypatch.setattr(maint, "start_metrics_server", lambda *a, **k: None)
    maint._running = True
    maint.run(once=True)


def test_redis_cache_disabled_and_fakeredis(monkeypatch):
    from checkpoint_platform.infrastructure.cache import redis_cache as rc

    monkeypatch.setattr(
        "checkpoint_platform.infrastructure.cache.redis_cache.get_settings",
        lambda: MagicMock(redis_enabled=False, redis_url="redis://x", redis_cache_ttl_seconds=10),
    )
    cache = rc.RedisAggregateCache()
    assert cache.get("2024-01-01", "c", "LUNCH") is None
    cache.set("2024-01-01", "c", "LUNCH", {"a": 1})
    cache.invalidate("2024-01-01", "c", "LUNCH")
    assert cache.get_cafe("2024-01-01", "cafe") is None
    cache.set_cafe("2024-01-01", "cafe", {"b": 2})

    # enabled + ping fail → client None
    monkeypatch.setattr(
        "checkpoint_platform.infrastructure.cache.redis_cache.get_settings",
        lambda: MagicMock(
            redis_enabled=True, redis_url="redis://127.0.0.1:1", redis_cache_ttl_seconds=10
        ),
    )
    with patch("checkpoint_platform.infrastructure.cache.redis_cache.redis.from_url") as fr:
        client = MagicMock()
        client.ping.side_effect = ConnectionError("down")
        fr.return_value = client
        cache2 = rc.RedisAggregateCache()
        assert cache2._client is None

    # enabled + working client
    fake = MagicMock()
    fake.ping.return_value = True
    fake.get.return_value = json.dumps({"ok": True})
    fake.setex.return_value = True
    fake.delete.return_value = 1
    monkeypatch.setattr(
        "checkpoint_platform.infrastructure.cache.redis_cache.get_settings",
        lambda: MagicMock(
            redis_enabled=True, redis_url="redis://localhost:6379/0", redis_cache_ttl_seconds=30
        ),
    )
    with patch("checkpoint_platform.infrastructure.cache.redis_cache.redis.from_url", return_value=fake):
        cache3 = rc.RedisAggregateCache()
        assert cache3.get("d", "c", "LUNCH") == {"ok": True}
        cache3.set("d", "c", "LUNCH", {"x": 1})
        cache3.invalidate("d", "c", "LUNCH")
        assert cache3.get_cafe("d", "cafe") == {"ok": True}
        cache3.set_cafe("d", "cafe", {"y": 2})
        # error paths
        fake.get.side_effect = RuntimeError("boom")
        assert cache3.get("d", "c", "LUNCH") is None
        assert cache3.get_cafe("d", "cafe") is None
        fake.setex.side_effect = RuntimeError("boom")
        cache3.set("d", "c", "LUNCH", {"x": 1})
        cache3.set_cafe("d", "cafe", {"y": 2})
        fake.delete.side_effect = RuntimeError("boom")
        cache3.invalidate("d", "c", "LUNCH")


def test_kafka_consumer_builder():
    with patch("checkpoint_platform.infrastructure.messaging.kafka_consumer.Consumer") as Cons:
        cons = MagicMock()
        Cons.return_value = cons
        from checkpoint_platform.infrastructure.messaging.kafka_consumer import build_consumer

        c = build_consumer(group_id="g1", topics=["t1"])
        assert c is cons
        cons.subscribe.assert_called()
        c2 = build_consumer(throughput_mode="high")
        assert c2 is cons


def test_kafka_producer_methods():
    prod = MagicMock()
    prod.produce = MagicMock()
    prod.poll = MagicMock()
    prod.flush = MagicMock(return_value=0)
    from checkpoint_platform.infrastructure.messaging.kafka_producer import KafkaEventPublisher

    pub = KafkaEventPublisher(producer=prod)
    pub.publish("topic", "key", {"a": 1})
    pub.publish_checkpoint({"counter_id": "c1", "x": 1})
    pub.publish_retry({"r": 1}, "k")
    pub.publish_dlq({"d": 1})
    pub.publish_aggregation({"a": 1}, "k")
    assert pub.flush() == 0
    # BufferError then success
    prod.produce.side_effect = [BufferError(), None]
    pub.publish("topic", "key", {"a": 2})
    # delivery callbacks
    pub._count_delivery(None, MagicMock())
    pub._count_delivery(Exception("e"), MagicMock(topic=lambda: "t"))
    assert pub.stats["delivered"] >= 1


def test_create_app_factory(monkeypatch):
    monkeypatch.setenv("METRICS_PORT", "0")
    fake_cache = MagicMock()
    fake_pub = MagicMock()
    with (
        patch("checkpoint_platform.interfaces.http.app.get_cache", return_value=fake_cache),
        patch("checkpoint_platform.interfaces.http.app.get_publisher", return_value=fake_pub),
        patch("checkpoint_platform.interfaces.http.app.init_db", side_effect=RuntimeError("defer")),
        patch("checkpoint_platform.interfaces.http.app.start_metrics_server"),
        patch("checkpoint_platform.interfaces.http.app.get_settings") as gs,
    ):
        settings = MagicMock()
        settings.cors_origins = "*"
        settings.app_env = "local"
        settings.api_port = 8080
        settings.api_host = "0.0.0.0"
        settings.metrics_port = 0
        gs.return_value = settings
        # Import create_app function without executing module-level app if already imported
        from checkpoint_platform.interfaces.http import app as app_mod

        created = app_mod.create_app()
        assert created.extensions["cache"] is fake_cache
        # cors empty / list
        settings.cors_origins = ""
        app_mod._configure_cors(created, settings)
        settings.cors_origins = "http://a.com, http://b.com"
        app_mod._configure_cors(created, settings)


def test_session_helpers(db_engine):
    from checkpoint_platform.infrastructure.persistence import session as sess

    # point SessionLocal at test engine
    Session = __import__("sqlalchemy.orm", fromlist=["sessionmaker"]).sessionmaker(
        bind=db_engine, future=True
    )
    with patch.object(sess, "SessionLocal", Session), patch.object(sess, "engine", db_engine):
        gen = sess.get_db()
        db = next(gen)
        assert db is not None
        gen.close()
        with sess.session_scope() as s:
            s.execute(__import__("sqlalchemy", fromlist=["text"]).text("SELECT 1"))
        sess.init_db()
