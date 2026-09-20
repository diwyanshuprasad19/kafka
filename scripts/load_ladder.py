#!/usr/bin/env python3
"""Benchmark ladder: step the producer rate up and measure what actually happens.

Runs the whole pipeline — producer, Kafka, a consumer group, PostgreSQL — at a
series of target rates and records, per step: the producer rate actually achieved,
the consumer rate, peak and end-of-step consumer lag, end-to-end latency, database
writes, and retry/DLQ counts.

The interesting result is not the top number. It is the rate at which the consumer
group stops keeping up, because that is where lag starts accumulating and the
system, while not down, is falling behind.

    python scripts/load_ladder.py                     # 100 → 2000 ev/s, 3 consumers
    python scripts/load_ladder.py --rates 500 1000
    python scripts/load_ladder.py --consumers 1       # find the single-process limit
    python scripts/load_ladder.py --duration 20
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

DEFAULT_RATES = (100, 250, 500, 1000, 2000)
# Give the group time to drain after each step so lag is measured settling, not
# mid-flight, and one step's backlog is not billed to the next.
DRAIN_TIMEOUT_SECONDS = 90


@dataclass
class StepResult:
    target_rate: float
    produced: int
    producer_rate: float
    # Backlog cleared per second once producing stopped: the group's spare capacity.
    drain_rate: float
    peak_lag: int
    end_lag: int
    lag_at_end_of_production: int
    drained: bool
    drain_seconds: float
    p50_latency_ms: float
    p95_latency_ms: float
    db_rows_written: int
    dlq: int
    keeping_up: bool = field(init=False)

    def __post_init__(self) -> None:
        # Consumer throughput can never exceed the offered rate, so comparing the two
        # proves nothing. Lag is the real signal: a group that is keeping up never
        # accumulates more than a couple of seconds of work, and ends at zero.
        backlog_budget = max(2.0 * self.producer_rate, 200)
        self.keeping_up = (
            self.drained
            and self.end_lag == 0
            and self.lag_at_end_of_production <= backlog_budget
        )


# --------------------------------------------------------------------------- #
# Kafka introspection
# --------------------------------------------------------------------------- #


def _admin_lag(topic: str, group: str, bootstrap: str) -> int:
    """Total committed-offset lag for a consumer group across all partitions."""
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = probe.list_topics(topic, timeout=10)
        if topic not in metadata.topics or metadata.topics[topic].error:
            return 0
        partitions = [
            TopicPartition(topic, p) for p in metadata.topics[topic].partitions
        ]
        committed = probe.committed(partitions, timeout=10)

        total = 0
        for tp in committed:
            _low, high = probe.get_watermark_offsets(tp, timeout=10, cached=False)
            # A partition the group has never committed reports OFFSET_INVALID.
            position = tp.offset if tp.offset >= 0 else 0
            total += max(high - position, 0)
        return total
    finally:
        probe.close()


# --------------------------------------------------------------------------- #
# Database introspection
# --------------------------------------------------------------------------- #


def _db_counters() -> dict[str, int]:
    from sqlalchemy import func, select

    from checkpoint_platform.infrastructure.persistence.models import (
        CheckpointHistory,
        DailyCounterAggregation,
        DlqRecord,
        ProcessedEvent,
    )
    from checkpoint_platform.infrastructure.persistence.session import session_scope

    with session_scope() as session:
        return {
            "processed": session.execute(
                select(func.count()).select_from(ProcessedEvent)
            ).scalar_one(),
            "history": session.execute(
                select(func.count()).select_from(CheckpointHistory)
            ).scalar_one(),
            "aggregates": session.execute(
                select(func.count()).select_from(DailyCounterAggregation)
            ).scalar_one(),
            "dlq": session.execute(
                select(func.count()).select_from(DlqRecord)
            ).scalar_one(),
        }


def _latency_percentiles(since: datetime) -> tuple[float, float]:
    """End-to-end latency from event occurrence to database write."""
    from sqlalchemy import text

    from checkpoint_platform.infrastructure.persistence.session import session_scope

    sql = text(
        """
        SELECT
          percentile_disc(0.50) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (recorded_at - occurred_at)) * 1000
          ) AS p50,
          percentile_disc(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (recorded_at - occurred_at)) * 1000
          ) AS p95
        FROM checkpoint_history
        WHERE recorded_at >= :since
        """
    )
    with session_scope() as session:
        row = session.execute(sql, {"since": since}).one()
    return (round(row.p50 or 0.0, 1), round(row.p95 or 0.0, 1))


# --------------------------------------------------------------------------- #
# Consumer group lifecycle
# --------------------------------------------------------------------------- #


def start_consumers(count: int, log_dir: Path) -> list[subprocess.Popen]:
    log_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), LOG_LEVEL="WARNING")
    procs = []
    for i in range(count):
        log = (log_dir / f"consumer-{i + 1}.log").open("w")
        procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "checkpoint_platform.interfaces.workers.aggregation_consumer",
                ],
                cwd=str(ROOT),
                env=dict(env, METRICS_PORT=str(9200 + i)),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        )
    return procs


def stop_consumers(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        # SIGTERM so the consumer leaves the group cleanly and commits its offsets.
        proc.send_signal(signal.SIGTERM)
    for proc in procs:
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            proc.kill()


# --------------------------------------------------------------------------- #
# Producing
# --------------------------------------------------------------------------- #


def produce_at_rate(rate: float, duration: float, counters: int, topic: str) -> tuple[int, float]:
    """Publish at a target rate for `duration` seconds. Returns (count, elapsed)."""
    from confluent_kafka import Producer

    from checkpoint_platform.config import get_settings

    settings = get_settings()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "batch.num.messages": 5000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
        }
    )

    from load_ladder_events import build_event  # local helper, see below

    target_total = int(rate * duration)
    interval = 1.0 / rate if rate > 0 else 0.0
    started = time.perf_counter()
    sent = 0

    for i in range(target_total):
        event = build_event(i, counters)
        while True:
            try:
                producer.produce(
                    topic=topic,
                    key=event["counter_id"],
                    value=json.dumps(event),
                )
                break
            except BufferError:
                # Local queue full: let librdkafka drain rather than dropping events.
                producer.poll(0.1)
        sent += 1

        # Pace against elapsed time rather than sleeping a fixed interval, so the
        # measured rate does not drift when serialisation takes longer than planned.
        target_elapsed = (i + 1) * interval
        drift = target_elapsed - (time.perf_counter() - started)
        if drift > 0:
            producer.poll(min(drift, 0.05))
        else:
            producer.poll(0)

    producer.flush(60)
    return sent, time.perf_counter() - started


# --------------------------------------------------------------------------- #
# Ladder
# --------------------------------------------------------------------------- #


class LagSampler:
    """Polls consumer-group lag in the background while the producer runs.

    Sampling only after production would hide the distinction that matters: a group
    that tracks the offered rate versus one whose backlog climbs the whole time.

    Composes a thread rather than subclassing one — Thread reserves names like
    ``_stop`` and ``_bootstrap`` for its own machinery.
    """

    def __init__(self, topic: str, group: str, servers: str, interval: float = 1.0):
        self._topic = topic
        self._group = group
        self._servers = servers
        self._interval = interval
        self._halt = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.samples: list[int] = []

    def _loop(self) -> None:
        while not self._halt.is_set():
            try:
                self.samples.append(_admin_lag(self._topic, self._group, self._servers))
            except Exception:  # noqa: BLE001
                # A transient metadata error must not abort the benchmark.
                pass
            self._halt.wait(self._interval)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._halt.set()
        self._thread.join(timeout=15)

    @property
    def peak(self) -> int:
        return max(self.samples, default=0)


def run_step(
    rate: float,
    duration: float,
    counters: int,
    topic: str,
    group: str,
    bootstrap: str,
) -> StepResult:
    before = _db_counters()
    step_started = datetime.now(timezone.utc)

    sampler = LagSampler(topic, group, bootstrap)
    sampler.start()
    produced, elapsed = produce_at_rate(rate, duration, counters, topic)
    producer_rate = produced / elapsed if elapsed else 0.0
    sampler.stop()

    lag_at_end = _admin_lag(topic, group, bootstrap)
    peak_lag = max(sampler.peak, lag_at_end)

    # Now watch the backlog clear. How fast it clears with no incoming load is the
    # group's spare capacity.
    drain_started = time.perf_counter()
    drained = lag_at_end == 0
    while not drained and time.perf_counter() - drain_started < DRAIN_TIMEOUT_SECONDS:
        time.sleep(1.0)
        lag = _admin_lag(topic, group, bootstrap)
        peak_lag = max(peak_lag, lag)
        if lag == 0:
            drained = True
    drain_seconds = time.perf_counter() - drain_started
    end_lag = 0 if drained else _admin_lag(topic, group, bootstrap)
    drain_rate = (lag_at_end / drain_seconds) if drained and drain_seconds > 0 else 0.0

    after = _db_counters()
    p50, p95 = _latency_percentiles(step_started)

    return StepResult(
        target_rate=rate,
        produced=produced,
        producer_rate=round(producer_rate, 1),
        drain_rate=round(drain_rate, 1),
        peak_lag=peak_lag,
        end_lag=end_lag,
        lag_at_end_of_production=lag_at_end,
        drained=drained,
        drain_seconds=round(drain_seconds, 1),
        p50_latency_ms=p50,
        p95_latency_ms=p95,
        db_rows_written=after["history"] - before["history"],
        dlq=after["dlq"] - before["dlq"],
    )


def print_table(results: list[StepResult], consumers: int) -> None:
    header = (
        f"{'target/s':>9} {'prod/s':>8} {'peak lag':>9} {'lag@end':>8} "
        f"{'drain s':>8} {'drain/s':>8} {'p50 ms':>8} {'p95 ms':>9} "
        f"{'DLQ':>4}  verdict"
    )
    print(f"\nConsumer group: {consumers} instance(s)\n")
    print(header)
    print("-" * len(header))
    for r in results:
        verdict = "keeping up" if r.keeping_up else "FALLING BEHIND"
        print(
            f"{r.target_rate:>9.0f} {r.producer_rate:>8.1f} {r.peak_lag:>9} "
            f"{r.lag_at_end_of_production:>8} {r.drain_seconds:>8.1f} "
            f"{r.drain_rate:>8.1f} {r.p50_latency_ms:>8.1f} "
            f"{r.p95_latency_ms:>9.1f} {r.dlq:>4}  {verdict}"
        )

    print(
        "\nlag@end = backlog when producing stopped; drain/s = backlog cleared per\n"
        "second with no incoming load, i.e. the group's spare capacity."
    )

    sustained = [r for r in results if r.keeping_up]
    print()
    if sustained:
        best = max(sustained, key=lambda r: r.producer_rate)
        print(
            f"Highest sustained rate: {best.producer_rate:,.0f} events/sec "
            f"({best.producer_rate * 60:,.0f}/min) with {consumers} consumer(s)"
        )
    else:
        print("No step was sustained — the group fell behind at every rate tested.")
    behind = [r for r in results if not r.keeping_up]
    if behind:
        print(
            f"First step where lag accumulated: {behind[0].target_rate:,.0f} events/sec"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=float, nargs="+", default=list(DEFAULT_RATES))
    parser.add_argument("--duration", type=float, default=15.0, help="seconds per step")
    parser.add_argument("--counters", type=int, default=5000)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument(
        "--no-manage-consumers",
        action="store_true",
        help="assume a consumer group is already running",
    )
    parser.add_argument("--json", type=Path, default=None, help="also write results here")
    args = parser.parse_args()

    from checkpoint_platform.config import get_settings

    settings = get_settings()
    topic = settings.checkpoint_topic
    group = settings.consumer_group_id
    bootstrap = settings.kafka_bootstrap_servers

    procs: list[subprocess.Popen] = []
    if not args.no_manage_consumers:
        print(f"Starting {args.consumers} consumer(s) ...")
        procs = start_consumers(args.consumers, ROOT / ".bench-logs")
        # Let the group form and take its partition assignment before producing.
        time.sleep(12)

    results: list[StepResult] = []
    try:
        for rate in args.rates:
            print(f"\n=== step: {rate:,.0f} events/sec for {args.duration:.0f}s ===")
            result = run_step(
                rate, args.duration, args.counters, topic, group, bootstrap
            )
            results.append(result)
            print(
                f"    produced {result.produced} at {result.producer_rate}/s, "
                f"peak lag {result.peak_lag}, lag when producing stopped "
                f"{result.lag_at_end_of_production}, p95 {result.p95_latency_ms}ms, "
                f"{'drained' if result.drained else 'DID NOT DRAIN'}"
            )
    finally:
        if procs:
            print("\nStopping consumers ...")
            stop_consumers(procs)

    print_table(results, args.consumers)

    if args.json:
        args.json.write_text(
            json.dumps([r.__dict__ for r in results], indent=2, default=str)
        )
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
