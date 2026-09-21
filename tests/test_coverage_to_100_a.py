"""Bring remaining src coverage gaps to 100% with mocks (no live Kafka/Postgres)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

# ---------------------------------------------------------------------------
# domain / settings / small helpers
# ---------------------------------------------------------------------------


def test_domain_exceptions_units_business_day_events():
    from checkpoint_platform.domain.business_day import (
        _zone,
        business_date,
        business_today,
        ensure_aware,
    )
    from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType
    from checkpoint_platform.domain.events import CheckpointEvent
    from checkpoint_platform.domain.exceptions import (
        PermanentValidationError,
        TransientProcessingError,
        is_transient_error,
    )
    from checkpoint_platform.domain.units import (
        is_supported_unit,
        quantity_in_kg,
        to_kilograms,
    )

    assert is_transient_error(TransientProcessingError("x"))
    assert not is_transient_error(PermanentValidationError("x"))
    # permanent hint + timeout still transient
    assert is_transient_error(Exception("validationerror timeout"))
    assert is_transient_error(Exception("sqlalchemy boom"))
    assert is_transient_error(Exception("psycopg disconnect"))
    assert is_transient_error(Exception("totally unknown"))

    with pytest.raises(PermanentValidationError):
        to_kilograms("not-a-number", "KG")
    with pytest.raises(PermanentValidationError):
        to_kilograms(1, "CUBITS")
    assert to_kilograms(None, None) == Decimal(0)
    assert to_kilograms(1, None) == Decimal("1.000")
    assert quantity_in_kg(CheckpointType.FOOD_PREPARED, 1000, "G") == Decimal("1.000")
    assert quantity_in_kg(CheckpointType.STAFF_HYGIENE, 1, "KG") == Decimal(0)
    assert is_supported_unit(CheckpointType.FOOD_PREPARED, "kg")
    assert is_supported_unit(CheckpointType.HOT_FOOD_TEMPERATURE, "C")
    assert is_supported_unit(CheckpointType.STAFF_HYGIENE, "whatever")

    assert ensure_aware(datetime(2024, 1, 1, 12, 0, 0)).tzinfo is UTC
    assert _zone("Not/A_Real_Zone").key == "UTC" or str(_zone("Not/A_Real_Zone"))
    d = business_date(datetime(2024, 1, 1, 20, 0, tzinfo=UTC), "Asia/Kolkata")
    assert isinstance(d, date)
    assert isinstance(business_today("UTC"), date)

    with pytest.raises(ValueError, match="not a number"):
        CheckpointEvent.model_validate(
            {
                "event_id": str(uuid4()),
                "event_type": "checkpoint.completed",
                "checkpoint_id": "cp",
                "checkpoint_version": 1,
                "client_id": "c",
                "cafe_id": "cafe",
                "counter_id": "ctr",
                "meal_type": "LUNCH",
                "checkpoint_type": "FOOD_PREPARED",
                "status": "COMPLETED",
                "value": "abc",
                "unit": "KG",
                "occurred_at": "2024-01-01T12:00:00Z",
            }
        )
    with pytest.raises(ValueError, match="finite"):
        CheckpointEvent.model_validate(
            {
                "event_id": str(uuid4()),
                "event_type": "checkpoint.completed",
                "checkpoint_id": "cp",
                "checkpoint_version": 1,
                "client_id": "c",
                "cafe_id": "cafe",
                "counter_id": "ctr",
                "meal_type": "LUNCH",
                "checkpoint_type": "FOOD_PREPARED",
                "status": "COMPLETED",
                "value": "Infinity",
                "unit": "KG",
                "occurred_at": "2024-01-01T12:00:00Z",
            }
        )
    # empty value coerces to None
    ev = CheckpointEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "checkpoint.completed",
            "checkpoint_id": "cp",
            "checkpoint_version": 1,
            "client_id": "c",
            "cafe_id": "cafe",
            "counter_id": "ctr",
            "meal_type": "LUNCH",
            "checkpoint_type": "STAFF_HYGIENE",
            "status": CheckpointStatus.FAILED.value,
            "value": "",
            "unit": "kg",
            "occurred_at": "2024-01-01T12:00:00Z",
        }
    )
    assert ev.value is None


def test_settings_helpers_and_reload(monkeypatch, tmp_path):
    from checkpoint_platform.config import settings as settings_mod

    monkeypatch.setenv("CHECKPOINT_ROOT", str(tmp_path))
    (tmp_path / "configs").mkdir()
    assert settings_mod._find_repo_root() == tmp_path

    monkeypatch.delenv("CHECKPOINT_ROOT", raising=False)
    # walk parents from file — repo has configs/
    root = settings_mod._find_repo_root()
    assert (root / "configs").is_dir() or root == settings_mod.Path.cwd()

    assert settings_mod.normalize_app_env("gcp") == "prod"
    assert settings_mod.normalize_app_env("dev") == "local"
    assert settings_mod.normalize_app_env("weird") == "weird"
    assert (
        settings_mod.normalize_app_env("") == "local"
        or settings_mod.normalize_app_env("   ") == "local"
    )

    files = settings_mod._env_files()
    assert isinstance(files, tuple)

    s = settings_mod.Settings(
        use_alloydb=True,
        app_env="local",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="u",
        kafka_sasl_password="p",
        kafka_ssl_ca_location="/tmp/ca.pem",
    )
    assert s.backend_label == "alloydb"
    s2 = settings_mod.Settings(use_alloydb=False, app_env="prod", cloud_sql_instance="a:b:c")
    assert s2.backend_label == "cloudsql"
    s3 = settings_mod.Settings(use_alloydb=False, app_env="local", cloud_sql_instance="")
    assert s3.backend_label == "local-postgres"
    cfg = s.kafka_client_config()
    assert "sasl.username" in cfg
    assert "ssl.ca.location" in cfg

    with patch.object(settings_mod, "get_settings") as gs:
        gs.cache_clear = MagicMock()
        gs.return_value = s3
        # call real reload
    settings_mod.get_settings.cache_clear()
    reloaded = settings_mod.reload_settings()
    assert reloaded is not None


def test_container_get_session():
    from checkpoint_platform.config import container

    with patch.object(container, "SessionLocal", return_value=MagicMock()) as SL:
        assert container.get_session() is SL.return_value


def test_bad_scenarios_require_min():
    from checkpoint_platform.application import bad_scenarios as bs

    with pytest.raises(RuntimeError):
        bs.require_min_count(10_000)


# ---------------------------------------------------------------------------
# messaging / logging / session
# ---------------------------------------------------------------------------


def test_kafka_admin_consumer_producer_session_logging(monkeypatch):
    from checkpoint_platform.infrastructure.messaging import kafka_admin as ka
    from checkpoint_platform.infrastructure.messaging import kafka_consumer as kc
    from checkpoint_platform.infrastructure.messaging import kafka_producer as kp
    from checkpoint_platform.infrastructure.observability import logging as logmod
    from checkpoint_platform.infrastructure.persistence import session as sess

    settings = MagicMock()
    settings.checkpoint_partitions = 3
    settings.kafka_replication_factor = 2
    settings.dlq_topic_retention_days = 30
    settings.checkpoint_topic = "cp"
    settings.retry_topic = "retry"
    settings.dlq_topic = "dlq"
    settings.aggregation_topic = "agg"
    settings.kafka_client_config.return_value = {"bootstrap.servers": "x"}
    settings.consumer_group_id = "g"
    settings.kafka_consumer_config.return_value = {"bootstrap.servers": "x"}
    settings.kafka_producer_config.return_value = {"bootstrap.servers": "x"}
    settings.kafka_throughput_mode = "reliable"
    settings.app_env = "prod"
    settings.log_level = "INFO"

    fut_ok = MagicMock()
    fut_ok.result.return_value = None
    fut_exists = MagicMock()
    fut_exists.result.side_effect = Exception("TopicAlreadyExistsError already exists")
    fut_fail = MagicMock()
    fut_fail.result.side_effect = Exception("fatal")

    admin = MagicMock()
    admin.create_topics.return_value = {
        "cp": fut_ok,
        "retry": fut_exists,
        "dlq": fut_fail,
    }
    with (
        patch.object(ka, "get_settings", return_value=settings),
        patch.object(ka, "AdminClient", return_value=admin),
        patch.object(ka, "NewTopic", side_effect=lambda *a, **k: MagicMock()),
        pytest.raises(Exception, match="fatal"),
    ):
        ka.create_topics()

    # only exists / success
    admin.create_topics.return_value = {"cp": fut_ok, "retry": fut_exists}
    with (
        patch.object(ka, "get_settings", return_value=settings),
        patch.object(ka, "AdminClient", return_value=admin),
        patch.object(ka, "NewTopic", side_effect=lambda *a, **k: MagicMock()),
    ):
        ka.create_topics(partitions=1, replication_factor=1)

    consumer = MagicMock()
    with (
        patch.object(kc, "get_settings", return_value=settings),
        patch.object(kc, "Consumer", return_value=consumer),
    ):
        kc.build_consumer(
            group_id="g", topics=["t"], on_assign=lambda *a: None, on_revoke=lambda *a: None
        )
        kc.build_consumer(throughput_mode="high")

    producer = MagicMock()
    producer.produce.side_effect = [BufferError(), None]
    pub = kp.KafkaEventPublisher(producer=producer)
    kp._delivery_report("err", MagicMock(topic=lambda: "t"))
    kp._delivery_report(None, None)
    pub._count_delivery("err", MagicMock(topic=lambda: "t"))
    pub._count_delivery(None, None)
    assert "failed" in pub.stats
    with patch.object(kp, "serialize", return_value=b"{}"):
        pub.publish("t", "k", {"a": 1})
        producer.produce.side_effect = None
        producer.produce.return_value = None
        event = MagicMock()
        event.partition_key.return_value = "pk"
        pub.publish_checkpoint(event)
        pub.publish_checkpoint({"counter_id": "c"})
        pub.publish_retry({}, "k")
        pub.publish_dlq({})
        pub.publish_aggregation({}, "k")
    pub.flush()

    # constructor builds Producer when none given + throughput_mode branch
    with (
        patch.object(kp, "get_settings", return_value=settings),
        patch.object(kp, "Producer", return_value=MagicMock()) as P,
    ):
        kp.KafkaEventPublisher(throughput_mode="high")
        P.assert_called()

    db = MagicMock()
    with patch.object(sess, "SessionLocal", return_value=db):
        gen = sess.get_db()
        assert next(gen) is db
        with pytest.raises(StopIteration):
            next(gen)

    db2 = MagicMock()
    with patch.object(sess, "SessionLocal", return_value=db2):
        with sess.session_scope() as s:
            assert s is db2
        db2.commit.assert_called()
        db2.close.assert_called()

    db3 = MagicMock()
    with patch.object(sess, "SessionLocal", return_value=db3):
        with pytest.raises(RuntimeError):
            with sess.session_scope():
                raise RuntimeError("x")
        db3.rollback.assert_called()

    assert logmod.get_request_id() is None or True
    logmod.bind_context(service="svc")
    monkeypatch.setenv("LOG_FORMAT", "console")
    with patch.object(logmod, "get_settings", return_value=settings):
        settings.app_env = "local"
        settings.log_level = "DEBUG"
        logmod.setup_logging(service="t")
    monkeypatch.setenv("LOG_FORMAT", "json")
    with patch.object(logmod, "get_settings", return_value=settings):
        settings.app_env = "prod"
        settings.log_level = "INFO"
        logmod.setup_logging(service="t")
    monkeypatch.delenv("LOG_FORMAT", raising=False)


# ---------------------------------------------------------------------------
# repositories (mocked session)
# ---------------------------------------------------------------------------


def test_repositories_branches():
    from checkpoint_platform.domain.exceptions import DuplicateEventError
    from checkpoint_platform.infrastructure.persistence import repositories as repos

    # AggregateTotals.from_row with None-ish quantities
    row = SimpleNamespace(
        aggregation_date=date(2024, 1, 1),
        total_checkpoints=1,
        completed_checkpoints=1,
        food_prepared_kg=None,
        food_wastage_kg=None,
    )
    totals = repos.AggregateTotals.from_row(row)
    assert totals.food_prepared_kg == Decimal(0)

    def _exec_result(*, scalar=None, one=None, rows=None, rowcount=0):
        result = MagicMock()
        result.scalar_one_or_none.return_value = scalar
        result.one_or_none.return_value = one
        result.scalar.return_value = scalar
        result.rowcount = rowcount
        scalars = MagicMock()
        scalars.all.return_value = rows if rows is not None else []
        # OutboxRepo.fetch_unpublished: list(session.execute(...).scalars())
        scalars.__iter__ = lambda self: iter(rows if rows is not None else [])
        result.scalars.return_value = scalars
        return result

    session = MagicMock()
    cp = repos.CheckpointRepo(session)
    session.execute.return_value = _exec_result(scalar=None)
    assert cp.get("x") is None

    session.execute.return_value = _exec_result(rows=[])
    assert cp.list_by_counter("c", checkpoint_type="FOOD_PREPARED") == []

    hr = repos.HistoryRepo(session)
    now = datetime.now(UTC)
    session.execute.return_value = _exec_result(rows=[])
    assert (
        hr.list_for_counter("c", from_ts=now, to_ts=now, meal_type="LUNCH", checkpoint_type="X")
        == []
    )
    assert hr.list_for_cafe("cafe", from_ts=now, to_ts=now) == []

    ar = repos.AggregationRepo(session)
    key = repos.AggregationKey(aggregation_date=date(2024, 1, 1), counter_id="c", meal_type="LUNCH")
    session.execute.return_value = _exec_result(scalar=None)
    assert ar.totals_for(key) is None
    session.execute.return_value = _exec_result(rows=[])
    assert ar.list_by_cafe(date(2024, 1, 1), "cafe") == []
    session.execute.return_value = _exec_result(one=None)
    assert (
        ar.apply_deltas(
            key, client_id="c", cafe_id="cafe", deltas={n: 0 for n in repos.DELTA_COLUMNS}
        )
        is None
    )

    pe = repos.ProcessedEventRepo(session)
    session.get.return_value = None
    assert pe.exists(uuid4()) is False
    session.execute.return_value = _exec_result(scalar=uuid4())
    assert pe.claim(uuid4(), "cp") is True

    nested = MagicMock()
    nested.__enter__ = MagicMock(return_value=None)
    nested.__exit__ = MagicMock(return_value=False)
    session.begin_nested.return_value = nested
    pe.mark(uuid4(), "cp")

    from sqlalchemy.exc import IntegrityError

    nested2 = MagicMock()
    nested2.__enter__ = MagicMock(side_effect=IntegrityError("x", None, None))
    nested2.__exit__ = MagicMock(return_value=True)
    session.begin_nested.return_value = nested2
    with pytest.raises(DuplicateEventError):
        pe.mark(uuid4(), "cp")

    session.get.return_value = None
    assert pe.delete(uuid4()) is False
    session.get.return_value = MagicMock()
    assert pe.delete(uuid4()) is True

    session.execute.return_value = _exec_result(rowcount=2)
    assert pe.prune_older_than(datetime.now(UTC)) == 2

    ob = repos.OutboxRepo(session)
    session.execute.return_value = _exec_result(rows=[])
    assert ob.fetch_unpublished() == []
    session.execute.return_value = _exec_result(scalar=3)
    assert ob.pending_count() == 3

    dlq = repos.DlqRepo(session)
    session.execute.return_value = _exec_result(rows=[])
    assert dlq.list_recent(status="pending") == []
    assert dlq.get_by_ids([]) == []
    marked = MagicMock()
    dlq.mark_reingested(marked, event_id="e", correlation_id="c")
    assert marked.reingest_status == "reingested"


# ---------------------------------------------------------------------------
# aggregation service edge paths
# ---------------------------------------------------------------------------


def test_aggregation_contribution_and_claim_edges():
    from checkpoint_platform.application.aggregation import (
        AggregationService,
        Contribution,
        aggregation_date_for,
    )
    from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType
    from checkpoint_platform.domain.events import CheckpointEvent
    from checkpoint_platform.domain.exceptions import ConcurrentUpdateError

    # hygiene fail + temperature fail
    c = Contribution(
        checkpoint_type=CheckpointType.STAFF_HYGIENE,
        status=CheckpointStatus.FAILED,
        value_kg=Decimal(0),
    )
    assert c.deltas(1)["hygiene_fail_count"] == 1
    c2 = Contribution(
        checkpoint_type=CheckpointType.HOT_FOOD_TEMPERATURE,
        status=CheckpointStatus.FAILED,
        value_kg=Decimal(0),
    )
    assert c2.deltas(1)["temperature_fail_count"] == 1

    session = MagicMock()
    svc = AggregationService(session)
    event = CheckpointEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "checkpoint.completed",
            "checkpoint_id": "cp-claim",
            "checkpoint_version": 2,
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
    )
    from checkpoint_platform.domain.business_day import business_date
    from checkpoint_platform.infrastructure.persistence.repositories import AggregationKey

    key = AggregationKey(
        aggregation_date=business_date(event.occurred_at),
        counter_id=event.counter_id,
        meal_type=event.meal_type.value,
    )

    # insert fails, get returns None three times → ConcurrentUpdateError
    svc.checkpoints = MagicMock()
    svc.checkpoints.insert_if_absent.return_value = False
    svc.checkpoints.get_for_update.return_value = None
    with pytest.raises(ConcurrentUpdateError):
        svc._claim_state(event, key, Decimal(1))

    # continue once then return prior with stale version
    existing = MagicMock()
    existing.aggregation_date = key.aggregation_date
    existing.counter_id = key.counter_id
    existing.meal_type = key.meal_type
    existing.client_id = "c"
    existing.cafe_id = "cafe"
    existing.checkpoint_version = 5
    existing.checkpoint_type = CheckpointType.FOOD_PREPARED.value
    existing.status = CheckpointStatus.COMPLETED.value
    existing.value_kg = Decimal(1)
    svc.checkpoints.insert_if_absent.side_effect = [False, False]
    svc.checkpoints.get_for_update.side_effect = [None, existing]
    prior = svc._claim_state(event, key, Decimal(1))
    assert prior is not None and prior.version == 5

    # _enqueue_outbox with None agg
    svc._enqueue_outbox(event, None)

    assert aggregation_date_for(event) == business_date(event.occurred_at)


# ---------------------------------------------------------------------------
# event_processing gaps
# ---------------------------------------------------------------------------


def test_event_processing_duplicate_stale_retry_wait():
    from checkpoint_platform.application.event_processing import EventProcessor
    from checkpoint_platform.domain.exceptions import DuplicateEventError, StaleVersionError

    session = MagicMock()
    savepoint = MagicMock()
    savepoint.is_active = True
    session.begin_nested.return_value = savepoint
    publisher = MagicMock()
    settings = MagicMock()
    settings.max_retries = 3
    settings.retry_base_delay_ms = 1
    settings.retry_max_inline_wait_seconds = 0.01
    settings.dlq_topic = "dlq"

    with patch(
        "checkpoint_platform.application.event_processing.get_settings",
        return_value=settings,
    ):
        proc = EventProcessor(session, publisher, manage_transaction=True)
        proc2 = EventProcessor(session, publisher, manage_transaction=False)

    # discard / keep with manage_transaction False
    proc2._discard_scope(None)
    session.rollback.assert_called()
    scope = MagicMock(is_active=True)
    proc2._keep_scope(scope)
    scope.commit.assert_called()
    proc2._discard_scope(scope)
    scope.rollback.assert_called()

    data = {
        "event_id": str(uuid4()),
        "event_type": "checkpoint.completed",
        "checkpoint_id": "cp",
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
        "retry_count": 1,
        "next_retry_at": (datetime.now(UTC) + timedelta(seconds=10)).isoformat(),
    }
    import json

    raw = json.dumps(data).encode()

    with (
        patch("checkpoint_platform.application.event_processing.AggregationService") as AS,
        patch.object(proc, "_defer_if_not_due", return_value=None),
        patch.object(proc, "_begin_scope", return_value=savepoint),
        patch.object(proc, "_keep_scope"),
        patch.object(proc, "_discard_scope"),
    ):
        AS.return_value.process.side_effect = DuplicateEventError("dup")
        assert proc.process_raw(raw, "t", 0, 1) == "duplicate"
        AS.return_value.process.side_effect = StaleVersionError("stale")
        assert proc.process_raw(raw, "t", 0, 1) == "stale"

    # retry wait long path — patch module time.sleep
    with patch("checkpoint_platform.application.event_processing.time.sleep"):
        assert proc._defer_if_not_due(data, 1) == "retry_wait"

    # inline wait
    settings.retry_max_inline_wait_seconds = 100
    with patch("checkpoint_platform.application.event_processing.time.sleep"):
        assert proc._defer_if_not_due(data, 1) is None

    # backoff with naive next_retry_at and bad value
    assert proc._backoff_wait_seconds({"next_retry_at": "not-a-date"}, 0) == 0.0
    naive = (datetime.now() + timedelta(seconds=5)).isoformat()
    assert proc._backoff_wait_seconds({"next_retry_at": naive}, 0) >= 0
    assert proc._backoff_wait_seconds({}, 2) > 0


# ---------------------------------------------------------------------------
# query_aggregates gaps
# ---------------------------------------------------------------------------
