"""Local and production must run the same code, differing only by configuration.

These tests pin the places where that promise is easy to break: credentials that
should only be applied when the protocol asks for them, durability settings that
depend on replication factor, and the preflight gate that refuses a production
deploy still pointing at localhost.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from checkpoint_platform.config.settings import Settings, normalize_app_env


# --------------------------------------------------------------------------- #
# Environment resolution
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("local", "local"),
        ("docker", "local"),
        ("dev", "local"),
        ("prod", "prod"),
        ("production", "prod"),
        ("gcp", "prod"),
        ("PROD", "prod"),
        (None, "local"),
    ],
)
def test_app_env_aliases(raw, expected, monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)
    assert normalize_app_env(raw) == expected


def test_settings_are_env_driven(monkeypatch):
    """Nothing about a deployment target may be baked into the image."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@db.internal:5432/agg")
    monkeypatch.setenv("REDIS_URL", "rediss://cache.internal:6379/0")
    monkeypatch.setenv("CHECKPOINT_PARTITIONS", "24")

    settings = Settings(_env_file=None)
    assert settings.kafka_bootstrap_servers == "broker.example.com:9092"
    assert settings.database_url.endswith("/agg")
    assert settings.redis_url.startswith("rediss://")
    assert settings.checkpoint_partitions == 24


# --------------------------------------------------------------------------- #
# Kafka client configuration
# --------------------------------------------------------------------------- #


def test_plaintext_carries_no_credentials():
    """A local broker must not be handed SASL settings it cannot interpret."""
    settings = Settings(_env_file=None, kafka_security_protocol="PLAINTEXT")
    config = settings.kafka_client_config()
    assert config["security.protocol"] == "PLAINTEXT"
    assert not any(key.startswith("sasl.") for key in config)


def test_sasl_ssl_applies_credentials():
    settings = Settings(
        _env_file=None,
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="key",
        kafka_sasl_password="secret",
    )
    config = settings.kafka_client_config()
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanisms"] == "PLAIN"
    assert config["sasl.username"] == "key"
    assert config["sasl.password"] == "secret"


def test_scram_mechanism_passes_through():
    """MSK with SCRAM is the same code path, only a different mechanism."""
    settings = Settings(
        _env_file=None,
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="u",
        kafka_sasl_password="p",
    )
    assert settings.kafka_client_config()["sasl.mechanisms"] == "SCRAM-SHA-512"


def test_custom_ca_is_forwarded():
    settings = Settings(
        _env_file=None,
        kafka_security_protocol="SSL",
        kafka_ssl_ca_location="/etc/ssl/private-ca.pem",
    )
    assert settings.kafka_client_config()["ssl.ca.location"] == "/etc/ssl/private-ca.pem"


# --------------------------------------------------------------------------- #
# Topic durability
# --------------------------------------------------------------------------- #


def _captured_topic_config(replication_factor: int) -> dict[str, dict]:
    from checkpoint_platform.infrastructure.messaging import kafka_admin

    captured: dict[str, dict] = {}

    def fake_admin(_config):
        client = MagicMock()

        def create_topics(topics, request_timeout=30):
            captured.update({t.topic: dict(t.config or {}) for t in topics})
            future = MagicMock()
            future.result.return_value = None
            return {t.topic: future for t in topics}

        client.create_topics = create_topics
        return client

    with patch.object(kafka_admin, "AdminClient", fake_admin):
        kafka_admin.create_topics(partitions=12, replication_factor=replication_factor)
    return captured


def test_single_broker_gets_no_min_insync():
    """min.insync.replicas=2 is unsatisfiable on one broker and would wedge writes."""
    config = _captured_topic_config(replication_factor=1)
    assert "min.insync.replicas" not in config["checkpoint.events.v1"]


def test_replicated_cluster_requires_two_in_sync():
    """acks=all only means 'all in-sync replicas'; without this it can mean one."""
    config = _captured_topic_config(replication_factor=3)
    assert config["checkpoint.events.v1"]["min.insync.replicas"] == "2"
    assert config["checkpoint.retry.v1"]["min.insync.replicas"] == "2"
    assert config["aggregation.completed.v1"]["min.insync.replicas"] == "2"


def test_dlq_is_retained_longer_than_the_live_stream():
    config = _captured_topic_config(replication_factor=1)
    retention_days = int(config["checkpoint.dlq.v1"]["retention.ms"]) / 86_400_000
    assert retention_days >= 7, "a DLQ that expires before anyone looks is useless"


# --------------------------------------------------------------------------- #
# Preflight gate
# --------------------------------------------------------------------------- #


def _preflight():
    """Load scripts/preflight.py, which is a script rather than an importable module."""
    import importlib.util
    import sys
    from pathlib import Path

    if "preflight" in sys.modules:
        return sys.modules["preflight"]

    path = Path(__file__).resolve().parents[1] / "scripts" / "preflight.py"
    spec = importlib.util.spec_from_file_location("preflight", path)
    module = importlib.util.module_from_spec(spec)
    # @dataclass resolves its own module from sys.modules, so register before exec.
    sys.modules["preflight"] = module
    spec.loader.exec_module(module)
    return module


def test_preflight_blocks_prod_on_localhost_defaults():
    preflight = _preflight()
    settings = Settings(_env_file=None, app_env="prod")

    results = preflight.check_config(settings)
    blocking = [r for r in results if r.blocking]
    names = {r.name for r in blocking}

    assert "config.no_local_defaults" in names
    # PLAINTEXT in production is unauthenticated and unencrypted.
    assert "config.kafka_auth" in names


def test_preflight_accepts_a_complete_prod_config():
    preflight = _preflight()
    settings = Settings(
        _env_file=None,
        app_env="prod",
        kafka_bootstrap_servers="pkc-abc.confluent.cloud:9092",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="key",
        kafka_sasl_password="secret",
        kafka_replication_factor=3,
        database_url="postgresql+psycopg://u:p@db.internal:5432/agg?sslmode=require",
        redis_url="rediss://cache.internal:6379/0",
    )

    blocking = [r for r in preflight.check_config(settings) if r.blocking]
    assert not blocking, [r.detail for r in blocking]


def test_preflight_rejects_leftover_placeholder():
    preflight = _preflight()
    settings = Settings(
        _env_file=None,
        app_env="prod",
        kafka_bootstrap_servers="pkc-abc.confluent.cloud:9092",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="key",
        kafka_sasl_password="secret",
        kafka_replication_factor=3,
        database_url="postgresql+psycopg://u:CHANGE_ME@db.internal:5432/agg",
        redis_url="rediss://cache.internal:6379/0",
    )
    names = {r.name for r in preflight.check_config(settings) if r.blocking}
    assert "config.placeholders" in names


def test_preflight_rejects_a_bogus_business_timezone():
    """The timezone decides which day an event lands in, so a typo is a data bug."""
    preflight = _preflight()
    settings = Settings(_env_file=None, business_timezone="Asia/Kolkatta")
    names = {r.name for r in preflight.check_config(settings) if r.blocking}
    assert "config.timezone" in names


def test_local_config_is_not_held_to_prod_rules():
    preflight = _preflight()
    settings = Settings(_env_file=None, app_env="local")
    assert not [r for r in preflight.check_config(settings) if r.blocking]
