#!/usr/bin/env python3
"""Create Kafka topics for the checkpoint aggregation platform."""

from checkpoint_platform.infrastructure.messaging.kafka_admin import create_topics
from checkpoint_platform.infrastructure.observability.logging import setup_logging


def main() -> None:
    setup_logging()
    create_topics()
    print("Topics ready.")


if __name__ == "__main__":
    main()
