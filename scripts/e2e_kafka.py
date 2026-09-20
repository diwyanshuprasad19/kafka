#!/usr/bin/env python3
"""End-to-end verification through a real Kafka broker.

Unlike scripts/failure_scenarios.py, which publishes events and tells you what to go
and look at, this asserts the outcome. Each scenario produces through Kafka, waits
for the consumer group to catch up, then checks the database and reports PASS/FAIL.

    python scripts/e2e_kafka.py                  # every scenario
    python scripts/e2e_kafka.py --only ordering dlq
    python scripts/e2e_kafka.py --list

The invariant most scenarios lean on: every event carries a distinct checkpoint_id
worth exactly 1 kg of wastage, so after N events the aggregate must read exactly N.
Below N means events were lost; above N means something was counted twice.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

CATCH_UP_TIMEOUT = 90

# Helpers


@dataclass
class Outcome:
    name: str
    passed: bool
    detail: str


def _settings():
    from checkpoint_platform.config import get_settings

    return get_settings()


def _producer():
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": _settings().kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
        }
    )


def _publish(producer, topic: str, key: str, value, headers=None) -> None:
    payload = value if isinstance(value, (bytes, bytearray)) else json.dumps(value).encode()
    producer.produce(topic=topic, key=key, value=payload, headers=headers)
    producer.poll(0)


def wastage_event(run: str, i: int, *, version: int = 1, value: float = 1.0, event_id=None) -> dict:
    """One 1 kg wastage checkpoint. Counters are spread so events cover partitions."""
    return {
        "event_id": str(event_id or uuid4()),
        "event_type": "checkpoint.updated" if version > 1 else "checkpoint.completed",
        "checkpoint_id": f"cp-{run}-{i}",
        "checkpoint_version": version,
        "client_id": "client-e2e",
        "cafe_id": "cafe-e2e",
        "counter_id": f"counter-{run}-{i % 12}",
        "meal_type": "LUNCH",
        "checkpoint_type": "FOOD_WASTAGE",
        "status": "COMPLETED",
        "value": value,
        "unit": "KG",
        "occurred_at": datetime.now(UTC).isoformat(),
    }


def group_lag() -> int:
    from confluent_kafka import Consumer, TopicPartition

    settings = _settings()
    topic = settings.checkpoint_topic
    probe = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = probe.list_topics(topic, timeout=10)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        total = 0
        for tp in probe.committed(partitions, timeout=10):
            _low, high = probe.get_watermark_offsets(tp, timeout=10, cached=False)
            total += max(high - (max(tp.offset, 0)), 0)
        return total
    finally:
        probe.close()


def wait_for_catch_up(timeout: int = CATCH_UP_TIMEOUT) -> bool:
    """Wait until the group's committed offsets reach the end of the topic."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if group_lag() == 0:
            # Offsets commit after the database transaction, so zero lag means the
            # writes are already durable.
            return True
        time.sleep(1.0)
    return False


def wastage_total(run: str) -> float:
    from sqlalchemy import func, select

    from checkpoint_platform.infrastructure.persistence.models import (
        DailyCounterAggregation,
    )
    from checkpoint_platform.infrastructure.persistence.session import session_scope

    with session_scope() as session:
        total = session.execute(
            select(func.coalesce(func.sum(DailyCounterAggregation.food_wastage_kg), 0)).where(
                DailyCounterAggregation.counter_id.like(f"counter-{run}-%")
            )
        ).scalar_one()
    return float(total)


def checkpoint_count(run: str) -> int:
    from sqlalchemy import func, select

    from checkpoint_platform.infrastructure.persistence.models import CheckpointState
    from checkpoint_platform.infrastructure.persistence.session import session_scope

    with session_scope() as session:
        return session.execute(
            select(func.count())
            .select_from(CheckpointState)
            .where(CheckpointState.checkpoint_id.like(f"cp-{run}-%"))
        ).scalar_one()


def dlq_count() -> int:
    from sqlalchemy import func, select

    from checkpoint_platform.infrastructure.persistence.models import DlqRecord
    from checkpoint_platform.infrastructure.persistence.session import session_scope

    with session_scope() as session:
        return session.execute(select(func.count()).select_from(DlqRecord)).scalar_one()


# Consumer group control


class ConsumerGroup:
    """Runs aggregation consumers as child processes so they can be killed."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.procs: list[subprocess.Popen] = []
        self._seq = 0

    def scale_to(self, count: int, settle: float = 12.0) -> None:
        while len(self.procs) < count:
            self._spawn()
        while len(self.procs) > count:
            self._terminate(self.procs.pop())
        if count:
            time.sleep(settle)

    def _spawn(self) -> None:
        self._seq += 1
        log = (self.log_dir / f"consumer-{self._seq}.log").open("w")
        env = dict(
            os.environ,
            PYTHONPATH=str(ROOT / "src"),
            LOG_LEVEL="INFO",
            METRICS_PORT=str(9300 + self._seq),
        )
        self.procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "checkpoint_platform.interfaces.workers.aggregation_consumer",
                ],
                cwd=str(ROOT),
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        )

    def kill_one(self, *, graceful: bool) -> int:
        """Remove one consumer. Returns its pid."""
        proc = self.procs.pop()
        pid = proc.pid
        if graceful:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=25)
            except subprocess.TimeoutExpired:
                proc.kill()
        else:
            # SIGKILL: no chance to commit offsets or leave the group cleanly, which
            # is what a crashed pod actually looks like.
            proc.kill()
            proc.wait(timeout=10)
        return pid

    def add_one(self, settle: float = 12.0) -> None:
        self._spawn()
        time.sleep(settle)

    @staticmethod
    def _terminate(proc: subprocess.Popen) -> None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            proc.kill()

    def stop_all(self) -> None:
        self.scale_to(0, settle=0)

    def assignments(self) -> dict[int, list[int]]:
        """Partitions each consumer currently owns, parsed from its log."""
        owned: dict[int, list[int]] = {}
        for proc in self.procs:
            log = self.log_dir / f"consumer-{self.procs.index(proc) + 1}.log"
            if not log.exists():
                continue
            partitions: list[int] = []
            for line in log.read_text().splitlines():
                if "partitions_assigned" in line or "partitions_revoked" in line:
                    partitions.append(line)
            owned[proc.pid] = partitions[-1:] if partitions else []
        return owned


# Scenarios


def scenario_ordering(group: ConsumerGroup) -> Outcome:
    """20 kg, corrected to 15, then a duplicate and a stale replay. Must end at 15."""
    run = uuid4().hex[:8]
    producer = _producer()
    topic = _settings().checkpoint_topic

    first = wastage_event(run, 0, version=1, value=20.0)
    correction = wastage_event(run, 0, version=2, value=15.0)

    _publish(producer, topic, first["counter_id"], first)
    producer.flush(30)
    if not wait_for_catch_up():
        return Outcome("ordering", False, "group did not catch up after first event")

    _publish(producer, topic, correction["counter_id"], correction)
    # Same event_id again: the duplicate must be ignored by the idempotency claim.
    _publish(producer, topic, correction["counter_id"], correction)
    # A stale replay of v1: must be rejected by the version guard.
    _publish(
        producer,
        topic,
        first["counter_id"],
        wastage_event(run, 0, version=1, value=20.0),
    )
    producer.flush(30)

    if not wait_for_catch_up():
        return Outcome("ordering", False, "group did not catch up after corrections")

    total = wastage_total(run)
    ok = abs(total - 15.0) < 0.001
    return Outcome(
        "ordering",
        ok,
        f"aggregate={total} kg (expected 15.0 after 20 → 15, duplicate, stale replay)",
    )


def scenario_dlq(group: ConsumerGroup) -> Outcome:
    """Poison messages must land in the DLQ without disturbing the aggregate."""
    run = uuid4().hex[:8]
    producer = _producer()
    topic = _settings().checkpoint_topic
    before = dlq_count()

    good = wastage_event(run, 0)
    _publish(producer, topic, good["counter_id"], good)

    # Unparseable bytes.
    _publish(producer, topic, "poison", b"{not json at all")
    # Valid JSON, invalid domain: no cafe_id, impossible value.
    _publish(producer, topic, "poison", {"event_id": str(uuid4()), "checkpoint_id": "x"})
    broken = wastage_event(run, 99)
    broken.pop("cafe_id")
    _publish(producer, topic, "poison", broken)
    producer.flush(30)

    if not wait_for_catch_up():
        return Outcome("dlq", False, "group did not catch up")

    added = dlq_count() - before
    total = wastage_total(run)
    ok = added >= 3 and abs(total - 1.0) < 0.001
    return Outcome(
        "dlq",
        ok,
        f"{added} DLQ rows written (expected >=3); aggregate={total} kg "
        f"(expected 1.0 — poison must not corrupt it)",
    )


def _produce_burst(run: str, count: int, *, rate: float | None = None) -> None:
    producer = _producer()
    topic = _settings().checkpoint_topic
    interval = (1.0 / rate) if rate else 0.0
    started = time.perf_counter()
    for i in range(count):
        event = wastage_event(run, i)
        _publish(producer, topic, event["counter_id"], event)
        if interval:
            drift = (i + 1) * interval - (time.perf_counter() - started)
            if drift > 0:
                producer.poll(min(drift, 0.05))
    producer.flush(60)


def scenario_rebalance(group: ConsumerGroup) -> Outcome:
    """Kill one of three consumers mid-flight. No event may be lost or double-counted."""
    run = uuid4().hex[:8]
    total_events = 3000

    group.scale_to(3)

    import threading

    producing = threading.Thread(
        target=_produce_burst, args=(run, total_events), kwargs={"rate": 400}
    )
    producing.start()
    # Kill a consumer while events are still flowing, forcing a rebalance under load.
    time.sleep(3)
    killed = group.kill_one(graceful=True)
    producing.join()

    if not wait_for_catch_up():
        return Outcome("rebalance", False, "group did not catch up after rebalance")

    seen = checkpoint_count(run)
    total = wastage_total(run)
    ok = seen == total_events and abs(total - total_events) < 0.001
    return Outcome(
        "rebalance",
        ok,
        f"killed pid {killed} mid-load; {seen}/{total_events} checkpoints stored, "
        f"aggregate={total} kg (expected {total_events}.0 — exact means no loss and "
        f"no double count)",
    )


def scenario_crash(group: ConsumerGroup) -> Outcome:
    """SIGKILL a consumer so offsets are never committed, then let the group replay.

    This is the case that makes idempotency load-bearing: the killed consumer may
    have committed its database transaction without committing its Kafka offset, so
    those events are delivered a second time.
    """
    run = uuid4().hex[:8]
    total_events = 2000

    group.scale_to(2)

    import threading

    producing = threading.Thread(
        target=_produce_burst, args=(run, total_events), kwargs={"rate": 500}
    )
    producing.start()
    time.sleep(2)
    killed = group.kill_one(graceful=False)
    producing.join()

    # Replace it so the orphaned partitions are picked up again.
    group.add_one()

    if not wait_for_catch_up():
        return Outcome("crash", False, "group did not catch up after crash")

    seen = checkpoint_count(run)
    total = wastage_total(run)
    ok = seen == total_events and abs(total - total_events) < 0.001
    return Outcome(
        "crash",
        ok,
        f"SIGKILLed pid {killed}; {seen}/{total_events} checkpoints stored, "
        f"aggregate={total} kg (expected {total_events}.0 — replayed events must not "
        f"be added twice)",
    )


def scenario_restart_from_offset(group: ConsumerGroup) -> Outcome:
    """Stop the group entirely, produce into the gap, restart. Nothing may be lost."""
    run = uuid4().hex[:8]
    total_events = 1000

    group.stop_all()
    _produce_burst(run, total_events)

    # Nothing is consuming, so the backlog must simply be waiting.
    lag_while_down = group_lag()
    group.scale_to(2)

    if not wait_for_catch_up():
        return Outcome("restart", False, "group did not catch up after restart")

    seen = checkpoint_count(run)
    total = wastage_total(run)
    ok = seen == total_events and lag_while_down >= total_events
    return Outcome(
        "restart",
        ok,
        f"backlog while stopped={lag_while_down}; after restart {seen}/{total_events} "
        f"stored, aggregate={total} kg (resumed from the committed offset)",
    )


SCENARIOS = {
    "ordering": scenario_ordering,
    "dlq": scenario_dlq,
    "rebalance": scenario_rebalance,
    "crash": scenario_crash,
    "restart": scenario_restart_from_offset,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="+", choices=sorted(SCENARIOS), default=None)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--consumers", type=int, default=3)
    args = parser.parse_args()

    if args.list:
        for name, fn in SCENARIOS.items():
            summary = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"{name:<10} {summary}")
        return

    from checkpoint_platform.infrastructure.observability.logging import setup_logging

    setup_logging(service="e2e")
    import logging

    logging.getLogger().setLevel(logging.WARNING)

    names = args.only or list(SCENARIOS)
    group = ConsumerGroup(ROOT / ".e2e-logs")
    results: list[Outcome] = []

    try:
        print(f"Starting consumer group ({args.consumers}) ...")
        group.scale_to(args.consumers)
        for name in names:
            print(f"\n=== {name} ===")
            # Scenarios that manipulate the group size restore it themselves; the
            # rest expect the requested size.
            if name in {"ordering", "dlq"}:
                group.scale_to(args.consumers, settle=2.0)
            outcome = SCENARIOS[name](group)
            results.append(outcome)
            print(f"    {'PASS' if outcome.passed else 'FAIL'}: {outcome.detail}")
    finally:
        print("\nStopping consumers ...")
        group.stop_all()

    print("\n" + "=" * 72)
    for outcome in results:
        print(f"{'PASS' if outcome.passed else 'FAIL'}  {outcome.name:<10} {outcome.detail}")
    failed = [r for r in results if not r.passed]
    print("=" * 72)
    print(f"{len(results) - len(failed)}/{len(results)} scenarios passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
