"""Prod scale: 50k events/minute."""

from checkpoint_platform.infrastructure.messaging.tuning import (
    SCALE_50K,
    SCALE_50K_PER_MIN,
    TARGET_EVENTS_PER_MINUTE,
    consumer_config,
    producer_config,
)


def test_scale_plan_50k_per_minute():
    assert TARGET_EVENTS_PER_MINUTE == 50_000
    assert SCALE_50K_PER_MIN["target_events_per_minute"] == 50_000
    assert abs(SCALE_50K_PER_MIN["target_events_per_sec"] - 50_000 / 60) < 0.1
    assert SCALE_50K_PER_MIN["recommended_partitions"] >= 12
    assert SCALE_50K["target_events_per_minute"] == 50_000


def test_high_throughput_producer_profile():
    cfg = producer_config({"bootstrap.servers": "localhost:9092"}, throughput_mode="high")
    assert cfg["linger.ms"] >= 20
    assert cfg["compression.type"] == "lz4"
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"


def test_reliable_vs_high():
    rel = producer_config({"bootstrap.servers": "x"}, throughput_mode="reliable")
    high = producer_config({"bootstrap.servers": "x"}, throughput_mode="high")
    assert high["batch.size"] >= rel["batch.size"]
    assert high["linger.ms"] >= rel["linger.ms"]


def test_high_consumer_fetch():
    cfg = consumer_config({"bootstrap.servers": "x"}, throughput_mode="high")
    assert cfg["fetch.min.bytes"] >= 16384
    assert cfg["enable.auto.commit"] is False
