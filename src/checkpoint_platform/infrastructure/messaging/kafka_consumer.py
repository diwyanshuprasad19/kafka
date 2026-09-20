from typing import Callable, Optional

from confluent_kafka import Consumer

from checkpoint_platform.config import get_settings


def build_consumer(
    group_id: Optional[str] = None,
    topics: Optional[list[str]] = None,
    *,
    throughput_mode: Optional[str] = None,
    on_assign: Optional[Callable] = None,
    on_revoke: Optional[Callable] = None,
) -> Consumer:
    settings = get_settings()
    if throughput_mode:
        from checkpoint_platform.infrastructure.messaging.tuning import consumer_config

        config = {
            **consumer_config(
                settings.kafka_client_config(),
                throughput_mode=throughput_mode,
            ),
            "group.id": group_id or settings.consumer_group_id,
        }
    else:
        config = {
            **settings.kafka_consumer_config(),
            "group.id": group_id or settings.consumer_group_id,
        }
    consumer = Consumer(config)
    if topics:
        callbacks = {}
        if on_assign is not None:
            callbacks["on_assign"] = on_assign
        if on_revoke is not None:
            callbacks["on_revoke"] = on_revoke
        consumer.subscribe(topics, **callbacks)
    return consumer
