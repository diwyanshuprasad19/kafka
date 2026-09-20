#!/usr/bin/env python3
"""Prove a configuration is deployable before anything starts serving traffic.

Checks the settings the process actually resolved — not a config file someone hopes
is being read — then makes a real connection to each dependency. Run it in the same
environment, with the same env vars, as the service:

    APP_ENV=local python scripts/preflight.py
    APP_ENV=prod  python scripts/preflight.py
    APP_ENV=prod  python scripts/preflight.py --skip kafka   # e.g. behind a proxy

Exits non-zero if any required check fails, so it works as a deploy gate or a
container readiness step.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Shipped defaults. Seeing one of these in production means the real value never
# arrived — the process silently fell back instead of failing.
LOCAL_DEFAULTS = {
    "database_url": "postgresql+psycopg://checkpoint:checkpoint@localhost:5432/aggregation",
    "redis_url": "redis://localhost:6379/0",
    "kafka_bootstrap_servers": "localhost:9092",
}

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


@dataclass
class Check:
    name: str
    status: str
    detail: str
    fix: str = ""

    @property
    def blocking(self) -> bool:
        return self.status == FAIL


# Configuration


def check_config(settings) -> list[Check]:
    results: list[Check] = []
    env = "prod" if settings.is_prod else "local"
    results.append(
        Check(
            "config.env",
            PASS,
            f"APP_ENV resolved to {env} (configs/{env}.env, then .env, then real env vars)",
        )
    )

    if settings.is_prod:
        leftovers = [
            field
            for field, default in LOCAL_DEFAULTS.items()
            if getattr(settings, field) == default
        ]
        results.append(
            Check(
                "config.no_local_defaults",
                FAIL if leftovers else PASS,
                f"still on shipped localhost defaults: {', '.join(leftovers)}"
                if leftovers
                else "no localhost defaults left in place",
                fix="Set these in configs/prod.env or the environment.",
            )
        )

        if "CHANGE_ME" in settings.database_url or "CHANGE_ME" in (
            settings.kafka_sasl_password or ""
        ):
            results.append(
                Check(
                    "config.placeholders",
                    FAIL,
                    "CHANGE_ME placeholder left in a credential",
                    fix="Fill the real secret, ideally from Secret Manager.",
                )
            )

        protocol = settings.kafka_security_protocol.upper()
        if protocol == "PLAINTEXT":
            results.append(
                Check(
                    "config.kafka_auth",
                    FAIL,
                    "KAFKA_SECURITY_PROTOCOL is PLAINTEXT in production — traffic is "
                    "unauthenticated and unencrypted",
                    fix="Use SASL_SSL (Confluent Cloud, MSK SCRAM) or SSL.",
                )
            )
        elif protocol.startswith("SASL") and not (
            settings.kafka_sasl_username and settings.kafka_sasl_password
        ):
            results.append(
                Check(
                    "config.kafka_auth",
                    FAIL,
                    f"{protocol} selected but SASL username/password are empty",
                    fix="Set KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD.",
                )
            )
        else:
            results.append(Check("config.kafka_auth", PASS, f"Kafka security protocol {protocol}"))

        if settings.kafka_replication_factor < 3:
            results.append(
                Check(
                    "config.replication",
                    WARN,
                    f"KAFKA_REPLICATION_FACTOR={settings.kafka_replication_factor}; "
                    f"a broker failure can lose data",
                    fix="Use 3 on any cluster with 3+ brokers.",
                )
            )
        else:
            results.append(
                Check(
                    "config.replication",
                    PASS,
                    f"replication factor {settings.kafka_replication_factor}, "
                    f"acks=all backed by min.insync.replicas=2",
                )
            )

        if settings.redis_enabled and settings.redis_url.startswith("redis://"):
            results.append(
                Check(
                    "config.redis_tls",
                    WARN,
                    "Redis URL is redis:// (unencrypted)",
                    fix="Use rediss:// if the managed instance offers TLS.",
                )
            )

        if "sslmode" not in settings.database_url and "127.0.0.1" not in settings.database_url:
            results.append(
                Check(
                    "config.db_tls",
                    WARN,
                    "DATABASE_URL has no sslmode and is not going through a local proxy",
                    fix="Append ?sslmode=require, or connect via the AlloyDB Auth Proxy.",
                )
            )

    # Timezone drives the daily aggregate boundary, so a typo silently files events
    # under the wrong business day.
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(settings.business_timezone)
        results.append(
            Check(
                "config.timezone",
                PASS,
                f"business day boundary uses {settings.business_timezone}",
            )
        )
    except Exception as exc:  # noqa: BLE001
        results.append(
            Check(
                "config.timezone",
                FAIL,
                f"BUSINESS_TIMEZONE {settings.business_timezone!r} is not a real zone: {exc}",
                fix="Use an IANA name such as Asia/Kolkata.",
            )
        )

    return results


# Dependencies


def check_database(settings) -> list[Check]:
    from sqlalchemy import create_engine, text

    results: list[Check] = []
    try:
        engine = create_engine(settings.database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            version = conn.execute(text("SHOW server_version")).scalar_one()
            applied = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
    except Exception as exc:  # noqa: BLE001
        return [
            Check(
                "database.connect",
                FAIL,
                f"cannot connect: {type(exc).__name__}: {exc}",
                fix="Check DATABASE_URL, network path and credentials.",
            )
        ]

    results.append(Check("database.connect", PASS, f"PostgreSQL {version}"))

    # A schema behind head means the code expects columns the database lacks.
    head = _alembic_head()
    if not applied:
        results.append(
            Check(
                "database.migrations",
                FAIL,
                "alembic_version is empty — migrations have never run",
                fix="Run: alembic upgrade head",
            )
        )
    elif head and applied[0] != head:
        results.append(
            Check(
                "database.migrations",
                FAIL,
                f"schema at {applied[0]}, code expects {head}",
                fix="Run: alembic upgrade head",
            )
        )
    else:
        results.append(Check("database.migrations", PASS, f"schema at head ({applied[0]})"))

    return results


def _alembic_head() -> str | None:
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        config = Config(str(ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(ROOT / "alembic"))
        return ScriptDirectory.from_config(config).get_current_head()
    except Exception:  # noqa: BLE001
        return None


def check_redis(settings) -> list[Check]:
    if not settings.redis_enabled:
        return [
            Check(
                "redis",
                SKIP,
                "REDIS_ENABLED=false — reads go straight to PostgreSQL",
            )
        ]
    try:
        import redis

        client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=5)
        client.ping()
        return [Check("redis.connect", PASS, "responded to PING")]
    except Exception as exc:  # noqa: BLE001
        # The cache is an optimisation, not a correctness dependency: the query layer
        # falls back to the database. So this degrades rather than blocks.
        return [
            Check(
                "redis.connect",
                WARN,
                f"unreachable ({type(exc).__name__}) — API still correct, just slower",
                fix="Check REDIS_URL, or set REDIS_ENABLED=false deliberately.",
            )
        ]


def check_kafka(settings) -> list[Check]:
    from confluent_kafka.admin import AdminClient, ConfigResource

    results: list[Check] = []
    try:
        admin = AdminClient(settings.kafka_client_config())
        cluster = admin.list_topics(timeout=15)
    except Exception as exc:  # noqa: BLE001
        return [
            Check(
                "kafka.connect",
                FAIL,
                f"cannot reach broker: {type(exc).__name__}: {exc}",
                fix="Check KAFKA_BOOTSTRAP_SERVERS, security protocol and credentials.",
            )
        ]

    results.append(Check("kafka.connect", PASS, f"{len(cluster.brokers)} broker(s) reachable"))

    required = {
        settings.checkpoint_topic: settings.checkpoint_partitions,
        settings.retry_topic: settings.checkpoint_partitions,
        settings.dlq_topic: None,
        settings.aggregation_topic: None,
    }
    missing = [name for name in required if name not in cluster.topics]
    if missing:
        results.append(
            Check(
                "kafka.topics",
                FAIL,
                f"missing topics: {', '.join(missing)}",
                fix="Run: python scripts/create_topics.py",
            )
        )
        return results

    details = []
    for name, expected in required.items():
        actual = len(cluster.topics[name].partitions)
        details.append(f"{name}={actual}p")
        if expected and actual < expected:
            # Partition count caps consumer parallelism, and Kafka cannot reduce it.
            results.append(
                Check(
                    f"kafka.partitions.{name}",
                    WARN,
                    f"{name} has {actual} partitions, config expects {expected} — "
                    f"parallelism is capped at {actual} consumers",
                    fix=f"Increase partitions to {expected} (Kafka cannot decrease them).",
                )
            )
    results.append(Check("kafka.topics", PASS, ", ".join(details)))

    # acks=all is only as durable as min.insync.replicas.
    if settings.kafka_replication_factor > 1:
        try:
            resource = ConfigResource(ConfigResource.Type.TOPIC, settings.checkpoint_topic)
            config = admin.describe_configs([resource], request_timeout=15)[resource].result()
            min_isr = int(config["min.insync.replicas"].value)
            results.append(
                Check(
                    "kafka.min_insync",
                    PASS if min_isr >= 2 else WARN,
                    f"min.insync.replicas={min_isr}"
                    + ("" if min_isr >= 2 else " — acks=all can be satisfied by one broker"),
                    fix="Set min.insync.replicas=2 on the topic.",
                )
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                Check(
                    "kafka.min_insync",
                    WARN,
                    f"could not read topic config ({type(exc).__name__})",
                )
            )

    return results


CHECKS: dict[str, Callable] = {
    "config": check_config,
    "database": check_database,
    "redis": check_redis,
    "kafka": check_kafka,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip", nargs="+", choices=sorted(CHECKS), default=[])
    args = parser.parse_args()

    from checkpoint_platform.config import get_settings

    settings = get_settings()

    print(
        f"Preflight — APP_ENV={settings.app_env} "
        f"({'production' if settings.is_prod else 'local'} rules)\n"
    )

    results: list[Check] = []
    for name, fn in CHECKS.items():
        if name in args.skip:
            results.append(Check(name, SKIP, "skipped by --skip"))
            continue
        try:
            results.extend(fn(settings))
        except Exception as exc:  # noqa: BLE001
            results.append(Check(name, FAIL, f"check itself failed: {type(exc).__name__}: {exc}"))

    width = max(len(r.name) for r in results)
    for r in results:
        print(f"  {r.status:<4} {r.name:<{width}}  {r.detail}")
        if r.fix and r.status in {FAIL, WARN}:
            print(f"       {'':<{width}}  → {r.fix}")

    blocking = [r for r in results if r.blocking]
    warnings = [r for r in results if r.status == WARN]
    print()
    if blocking:
        print(f"NOT DEPLOYABLE — {len(blocking)} blocking problem(s)")
        sys.exit(1)
    if warnings:
        print(f"Deployable, with {len(warnings)} warning(s) worth reading")
    else:
        print("Deployable — every check passed")


if __name__ == "__main__":
    main()
