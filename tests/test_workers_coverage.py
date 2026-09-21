"""Unit coverage for worker entrypoints — mocked Kafka/DB, no live brokers."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# aggregation_consumer
# ---------------------------------------------------------------------------


def test_throughput_window_and_offsets():
    from checkpoint_platform.interfaces.workers import aggregation_consumer as ac

    win = ac.ThroughputWindow(window_seconds=60.0)
    win.record(3)
    assert win.per_minute() == 3
    win._trim(win._events[0] + 120)  # expire all
    assert win.per_minute() == 0

    m1 = MagicMock()
    m1.topic.return_value = "t"
    m1.partition.return_value = 0
    m1.offset.return_value = 5
    m2 = MagicMock()
    m2.topic.return_value = "t"
    m2.partition.return_value = 0
    m2.offset.return_value = 7
    m3 = MagicMock()
    m3.topic.return_value = "t"
    m3.partition.return_value = 1
    m3.offset.return_value = 1
    with patch.object(ac, "TopicPartition", side_effect=lambda t, p, o: (t, p, o)):
        parts = ac._offsets_to_commit([m1, m2, m3])
    assert ("t", 0, 8) in parts
    assert ("t", 1, 2) in parts


def test_aggregation_process_batch_and_individual():
    from checkpoint_platform.interfaces.workers import aggregation_consumer as ac

    msg = MagicMock()
    msg.value.return_value = b"{}"
    msg.topic.return_value = "t"
    msg.partition.return_value = 0
    msg.offset.return_value = 1

    session = MagicMock()
    processor = MagicMock()
    processor.process_raw.return_value = "processed"
    pub = MagicMock()

    with (
        patch.object(ac, "get_session", return_value=session),
        patch.object(ac, "get_event_processor", return_value=processor),
    ):
        out = ac._process_batch(pub, [msg])
    assert out["processed"] == 1
    session.commit.assert_called_once()
    session.close.assert_called_once()

    processor2 = MagicMock()
    processor2.process_raw.side_effect = RuntimeError("boom")
    session2 = MagicMock()
    with (
        patch.object(ac, "get_session", return_value=session2),
        patch.object(ac, "get_event_processor", return_value=processor2),
    ):
        out2 = ac._process_individually(pub, [msg])
    assert out2["failed"] == 1
    session2.rollback.assert_called()

    processor3 = MagicMock()
    processor3.process_raw.return_value = "duplicate"
    session3 = MagicMock()
    with (
        patch.object(ac, "get_session", return_value=session3),
        patch.object(ac, "get_event_processor", return_value=processor3),
    ):
        assert ac._process_individually(pub, [msg])["duplicate"] == 1


def test_aggregation_refresh_lag():
    from checkpoint_platform.interfaces.workers import aggregation_consumer as ac

    consumer = MagicMock()
    consumer.assignment.return_value = []
    ac._refresh_lag(consumer)

    tp = SimpleNamespace(topic="t", partition=0, offset=10)
    consumer.assignment.return_value = [tp]
    consumer.committed.return_value = [tp]
    consumer.get_watermark_offsets.return_value = (0, 25)
    ac._refresh_lag(consumer)

    consumer.get_watermark_offsets.side_effect = RuntimeError("wm")
    ac._refresh_lag(consumer)

    consumer.get_watermark_offsets.side_effect = None
    consumer.get_watermark_offsets.return_value = (0, -1)
    ac._refresh_lag(consumer)

    consumer.get_watermark_offsets.return_value = (0, 5)
    tp2 = SimpleNamespace(topic="t", partition=0, offset=-1)
    consumer.committed.return_value = [tp2]
    ac._refresh_lag(consumer)

    consumer.assignment.side_effect = RuntimeError("assign fail")
    ac._refresh_lag(consumer)


def test_aggregation_handle_signal_and_run_paths():
    from confluent_kafka import KafkaError

    from checkpoint_platform.interfaces.workers import aggregation_consumer as ac

    ac._running = True
    ac._handle_signal(15, None)
    assert ac._running is False

    settings = MagicMock()
    settings.metrics_port = 9101
    settings.app_env = "local"
    settings.consumer_group_id = "g"
    settings.checkpoint_topic = "cp"
    settings.retry_topic = "retry"
    settings.consumer_batch_size = 5
    settings.consumer_poll_timeout_seconds = 0.01

    good = MagicMock()
    good.error.return_value = None
    good.value.return_value = b"{}"
    good.topic.return_value = "cp"
    good.partition.return_value = 0
    good.offset.return_value = 3

    eof_err = MagicMock()
    eof_err.code.return_value = KafkaError._PARTITION_EOF
    eof_msg = MagicMock()
    eof_msg.error.return_value = eof_err

    bad_err = MagicMock()
    bad_err.code.return_value = 42
    bad_msg = MagicMock()
    bad_msg.error.return_value = bad_err

    consumer = MagicMock()
    state = {"n": 0}

    def consume(**_kw):
        state["n"] += 1
        if state["n"] == 1:
            return []  # empty → continue
        if state["n"] == 2:
            return [eof_msg, bad_msg]  # no processable → continue
        if state["n"] == 3:
            return [good]
        if state["n"] == 4:
            # batch fails → individual
            return [good]
        ac._running = False
        return []

    consumer.consume.side_effect = consume
    consumer.assignment.return_value = []
    captured = {}

    def build_consumer(**kwargs):
        captured["on_assign"] = kwargs.get("on_assign")
        captured["on_revoke"] = kwargs.get("on_revoke")
        return consumer

    pub = MagicMock()
    batch_outcomes = Counter({"processed": 1})
    calls = {"batch": 0}

    def process_batch(_p, _m):
        calls["batch"] += 1
        if calls["batch"] == 2:
            raise RuntimeError("batch boom")
        return batch_outcomes

    with (
        patch.object(ac, "get_settings", return_value=settings),
        patch.object(ac, "init_db"),
        patch.object(ac, "start_metrics_server"),
        patch.object(ac, "set_app_info"),
        patch.object(ac, "build_consumer", side_effect=build_consumer),
        patch.object(ac, "get_publisher", return_value=pub),
        patch.object(ac, "_process_batch", side_effect=process_batch),
        patch.object(ac, "_process_individually", return_value=Counter({"failed": 1})),
        patch.object(ac, "_refresh_lag"),
        patch.object(ac, "TopicPartition", side_effect=lambda t, p, o: (t, p, o)),
        patch.object(ac, "signal"),
        patch.object(ac, "LAG_REFRESH_SECONDS", 0.0),
    ):
        ac._running = True
        ac.run()

    assert captured["on_assign"] is not None
    captured["on_assign"](consumer, [SimpleNamespace(topic="cp", partition=0)])
    captured["on_revoke"](consumer, [SimpleNamespace(topic="cp", partition=0)])
    consumer.commit.side_effect = RuntimeError("no commit")
    captured["on_revoke"](consumer, [SimpleNamespace(topic="cp", partition=1)])

    with (
        patch.object(ac, "run"),
        patch.object(ac.sys, "exit") as ex,
    ):
        ac.main()
        ex.assert_called_with(0)


# ---------------------------------------------------------------------------
# outbox_publisher
# ---------------------------------------------------------------------------


def test_outbox_invalidate_and_run():
    from checkpoint_platform.interfaces.workers import outbox_publisher as op

    op._handle_signal(2, None)
    assert op._running is False

    cache = MagicMock()
    op._invalidate(cache, {})
    op._invalidate(cache, {"aggregation_date": "2024-01-01"})
    op._invalidate(
        cache,
        {"aggregation_date": "2024-01-01", "counter_id": "c", "meal_type": "LUNCH"},
    )
    cache.invalidate.assert_called_once()
    cache.invalidate.side_effect = RuntimeError("cache down")
    op._invalidate(
        cache,
        {"aggregation_date": "2024-01-01", "counter_id": "c", "meal_type": "LUNCH"},
    )

    settings = MagicMock()
    settings.metrics_port = 9102
    row = SimpleNamespace(
        payload={"counter_id": "ctr", "aggregation_date": "2024-01-01", "meal_type": "LUNCH"},
        published=False,
        published_at=None,
    )
    session = MagicMock()
    repo = MagicMock()
    state = {"n": 0}

    def fetch(limit=500):
        state["n"] += 1
        if state["n"] == 1:
            return []
        if state["n"] == 2:
            return [row]
        return []

    repo.fetch_unpublished.side_effect = fetch
    repo.pending_count.return_value = 0

    def get_session():
        if state["n"] >= 3:
            op._running = False
        return session

    pub = MagicMock()
    cache2 = MagicMock()

    with (
        patch.object(op, "get_settings", return_value=settings),
        patch.object(op, "init_db"),
        patch.object(op, "start_metrics_server"),
        patch.object(op, "get_publisher", return_value=pub),
        patch.object(op, "get_cache", return_value=cache2),
        patch.object(op, "get_session", side_effect=get_session),
        patch.object(op, "OutboxRepo", return_value=repo),
        patch.object(op, "signal"),
        patch.object(op, "time") as tmod,
    ):
        tmod.sleep = MagicMock()
        op._running = True

        # empty → sleep; then publish batch; then fail path; then stop
        def fetch2(limit=500):
            state["n"] += 1
            if state["n"] == 1:
                return []
            if state["n"] == 2:
                return [row]
            if state["n"] == 3:
                raise RuntimeError("db fail")
            op._running = False
            return []

        repo.fetch_unpublished.side_effect = fetch2
        state["n"] = 0
        op.run()

    with patch.object(op, "run"):
        op.main()


# ---------------------------------------------------------------------------
# reporting_consumer
# ---------------------------------------------------------------------------


def test_reporting_consumer_run():
    from checkpoint_platform.interfaces.workers import reporting_consumer as rc

    rc._handle_signal(15, None)
    assert rc._running is False

    settings = MagicMock()
    settings.metrics_port = 9103
    settings.reporting_consumer_group_id = "rg"
    settings.aggregation_topic = "agg"

    none_then_err_then_ok = {"n": 0}
    err_msg = MagicMock()
    err_msg.error.return_value = "kafka boom"

    ok_msg = MagicMock()
    ok_msg.error.return_value = None
    ok_msg.value.return_value = b"{}"

    consumer = MagicMock()

    def poll(_t):
        none_then_err_then_ok["n"] += 1
        if none_then_err_then_ok["n"] == 1:
            return None
        if none_then_err_then_ok["n"] == 2:
            return err_msg
        if none_then_err_then_ok["n"] == 3:
            return ok_msg
        if none_then_err_then_ok["n"] == 4:
            # processing failure
            return ok_msg
        rc._running = False
        return None

    consumer.poll.side_effect = poll
    session = MagicMock()
    session_calls = {"n": 0}

    def get_session():
        session_calls["n"] += 1
        return session

    payload = {
        "aggregation_date": "2024-01-02",
        "counter_id": "c1",
        "cafe_id": "cafe",
        "client_id": "cli",
        "meal_type": "LUNCH",
    }

    def deserialize(_v):
        if session_calls["n"] >= 2:
            raise RuntimeError("bad payload")
        return payload

    with (
        patch.object(rc, "get_settings", return_value=settings),
        patch.object(rc, "init_db"),
        patch.object(rc, "start_metrics_server"),
        patch.object(rc, "build_consumer", return_value=consumer),
        patch.object(rc, "get_session", side_effect=get_session),
        patch.object(rc, "deserialize", side_effect=deserialize),
        patch.object(rc, "insert") as ins,
        patch.object(rc, "signal"),
        patch.object(rc, "ReportingSnapshot") as RS,
    ):
        RS.__table__ = MagicMock()
        stmt = MagicMock()
        stmt.on_conflict_do_update.return_value = stmt
        stmt.excluded = MagicMock()
        ins.return_value = stmt
        ins.return_value.values.return_value = stmt
        rc._running = True
        rc.run()

    with patch.object(rc, "run"):
        rc.main()


# ---------------------------------------------------------------------------
# rollup worker
# ---------------------------------------------------------------------------


def test_rollup_refresh_once_and_main():
    from checkpoint_platform.interfaces.workers import rollup as rw

    rw._stop(15, None)
    assert rw._running is False

    result = SimpleNamespace(
        rows=2,
        cafe_rows=1,
        client_rows=1,
        watermark=datetime(2024, 1, 1, tzinfo=UTC),
    )
    empty = SimpleNamespace(
        rows=0,
        cafe_rows=0,
        client_rows=0,
        watermark=datetime(2024, 1, 1, tzinfo=UTC),
    )
    session = MagicMock()
    ctx = MagicMock()
    ctx.__enter__.return_value = session
    ctx.__exit__.return_value = None
    repo = MagicMock()
    repo.refresh.return_value = result

    with (
        patch.object(rw, "session_scope", return_value=ctx),
        patch.object(rw, "RollupRepo", return_value=repo),
    ):
        assert rw.refresh_once() == 2
        repo.refresh.return_value = empty
        assert rw.refresh_once() == 0

    settings = MagicMock()
    settings.rollup_interval_seconds = 0.01

    with (
        patch("argparse.ArgumentParser") as AP,
        patch.object(rw, "get_settings", return_value=settings),
        patch.object(rw, "setup_logging"),
        patch.object(rw, "refresh_once", return_value=3) as once,
        patch("builtins.print"),
    ):
        args = SimpleNamespace(once=True, interval=None, metrics_port=9105)
        AP.return_value.parse_args.return_value = args
        rw.main()
        once.assert_called_once()

    calls = {"n": 0}

    def refresh_side():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("pass fail")
        rw._running = False
        return 0

    with (
        patch("argparse.ArgumentParser") as AP,
        patch.object(rw, "get_settings", return_value=settings),
        patch.object(rw, "setup_logging"),
        patch.object(rw, "start_metrics_server"),
        patch.object(rw, "refresh_once", side_effect=refresh_side),
        patch.object(rw, "signal"),
        patch.object(rw, "time") as tmod,
    ):
        tmod.sleep = MagicMock()
        args = SimpleNamespace(once=False, interval=0.01, metrics_port=9105)
        AP.return_value.parse_args.return_value = args
        rw._running = True
        rw.main()


# ---------------------------------------------------------------------------
# maintenance worker
# ---------------------------------------------------------------------------


def test_maintenance_prune_report_and_run():
    from checkpoint_platform.interfaces.workers import maintenance as mw

    mw._handle_signal(15, None)
    assert mw._running is False

    settings = MagicMock()
    settings.processed_events_retention_hours = 1
    settings.history_retention_days = 1
    settings.outbox_retention_hours = 1
    settings.dlq_retention_days = 1
    settings.metrics_port = 9104
    settings.maintenance_interval_seconds = 0.01

    session = MagicMock()
    with (
        patch.object(mw, "get_settings", return_value=settings),
        patch.object(mw, "ProcessedEventRepo") as PE,
        patch.object(mw, "HistoryRepo") as HR,
        patch.object(mw, "OutboxRepo") as OR,
        patch.object(mw, "DlqRepo") as DR,
    ):
        PE.return_value.prune_older_than.return_value = 1
        HR.return_value.prune_older_than.return_value = 0
        OR.return_value.prune_published_older_than.return_value = 0
        DR.return_value.prune_reingested_older_than.return_value = 0
        OR.return_value.pending_count.return_value = 2
        DR.return_value.pending_count.return_value = 3
        removed = mw.prune_once(session)
        assert removed["processed_events"] == 1
        assert mw.report_backlogs(session)["dlq_pending"] == 3

    state = {"n": 0}

    def get_session():
        state["n"] += 1
        s = MagicMock()
        if state["n"] == 2:
            # fail path
            s_side = MagicMock()
            return s_side
        return s

    with (
        patch.object(mw, "get_settings", return_value=settings),
        patch.object(mw, "init_db"),
        patch.object(mw, "start_metrics_server"),
        patch.object(mw, "get_session", side_effect=get_session),
        patch.object(
            mw, "prune_once", side_effect=[{"processed_events": mw.CHUNK}, RuntimeError("x")]
        ),
        patch.object(mw, "report_backlogs", return_value={"outbox_pending": 0, "dlq_pending": 0}),
        patch.object(mw, "signal"),
        patch.object(mw, "time") as tmod,
    ):
        tmod.sleep = MagicMock(side_effect=lambda *_: setattr(mw, "_running", False))
        mw._running = True
        mw.run(once=False)

    with (
        patch.object(mw, "get_settings", return_value=settings),
        patch.object(mw, "init_db"),
        patch.object(mw, "get_session", return_value=MagicMock()),
        patch.object(mw, "prune_once", return_value={"processed_events": 0}),
        patch.object(mw, "report_backlogs", return_value={"outbox_pending": 0, "dlq_pending": 0}),
        patch.object(mw, "signal"),
    ):
        mw.run(once=True)

    with (
        patch.object(mw, "run") as run,
        patch.object(
            mw.sys if hasattr(mw, "sys") else __import__("sys"), "argv", ["maintenance", "--once"]
        ),
    ):
        # main imports sys locally
        with patch("sys.argv", ["maintenance", "--once"]):
            mw.main()
        run.assert_called()
