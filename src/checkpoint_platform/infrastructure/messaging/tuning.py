"""Kafka client profiles + capacity plan for prod scale (50k events/minute)."""

from __future__ import annotations

from typing import Any

# Primary production target: 50,000 events / minute ≈ 833 events / second
TARGET_EVENTS_PER_MINUTE = 50_000
TARGET_EVENTS_PER_SEC = TARGET_EVENTS_PER_MINUTE / 60.0  # ~833.33


def producer_config(
    base: dict[str, Any],
    *,
    throughput_mode: str = "reliable",
) -> dict[str, Any]:
    """
    reliable — acks=all, idempotent (default prod correctness)
    high     — larger batches/lz4 for sustained ~50k/min and bursts
    """
    mode = (throughput_mode or "reliable").lower()
    cfg = {
        **base,
        "enable.idempotence": True,
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,
    }
    if mode == "high":
        cfg.update(
            {
                "acks": "all",
                "linger.ms": 20,
                "batch.size": 65536,
                "compression.type": "lz4",
                "queue.buffering.max.kbytes": 524288,
                "queue.buffering.max.messages": 500000,
            }
        )
    else:
        cfg.update(
            {
                "acks": "all",
                "linger.ms": 5,
                "batch.size": 32768,
                "compression.type": "lz4",
            }
        )
    return cfg


def consumer_config(
    base: dict[str, Any],
    *,
    throughput_mode: str = "reliable",
) -> dict[str, Any]:
    mode = (throughput_mode or "reliable").lower()
    cfg = {
        **base,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600000,
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
    }
    if mode == "high":
        cfg.update(
            {
                "fetch.min.bytes": 16384,
                "fetch.wait.max.ms": 100,
                "max.partition.fetch.bytes": 1048576,
            }
        )
    return cfg


# Prod-scale plan: 50k events/minute (local + GCP same architecture)
SCALE_50K_PER_MIN = {
    "target_events_per_minute": TARGET_EVENTS_PER_MINUTE,
    "target_events_per_sec": round(TARGET_EVENTS_PER_SEC, 2),
    "recommended_partitions": 12,
    "recommended_consumer_instances": 3,
    "recommended_producer_workers": 2,
    "partition_key": "counter_id",
    "db_pool_size": 20,
    "notes": [
        "50k events/minute ≈ 833 events/second — realistic SDE-2 / cafeteria checkpoint scale",
        "12 partitions → up to 12 useful consumers; start with 3 for HA",
        "Local Docker: RF=1; GCP/Confluent: RF=3",
        "Ops: watch consumer lag, processing p99, DLQ rate, retry rate, HTTP latency",
        "Centralized logs: JSON stdout with correlation_id / request_id / service / env",
    ],
}

SCALE_50K = {
    "target_events_per_sec": TARGET_EVENTS_PER_SEC,
    "target_events_per_minute": TARGET_EVENTS_PER_MINUTE,
    "recommended_partitions": SCALE_50K_PER_MIN["recommended_partitions"],
    "recommended_consumer_instances": SCALE_50K_PER_MIN["recommended_consumer_instances"],
    "recommended_producer_workers": SCALE_50K_PER_MIN["recommended_producer_workers"],
    "partition_key": "counter_id",
    "notes": SCALE_50K_PER_MIN["notes"],
}
