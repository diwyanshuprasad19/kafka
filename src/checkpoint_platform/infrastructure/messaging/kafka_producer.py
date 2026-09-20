from typing import Callable, Optional

from confluent_kafka import Producer

from checkpoint_platform.config import get_settings
from checkpoint_platform.infrastructure.messaging.serializer import serialize
from checkpoint_platform.infrastructure.observability.logging import get_logger

logger = get_logger(__name__)


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic() if msg else None,
        )


class KafkaEventPublisher:
    """Kafka producer implementing EventPublisher port."""

    def __init__(
        self,
        producer: Optional[Producer] = None,
        *,
        throughput_mode: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        mode = throughput_mode or settings.kafka_throughput_mode
        config = settings.kafka_producer_config()
        if throughput_mode:
            from checkpoint_platform.infrastructure.messaging.tuning import producer_config

            config = producer_config(settings.kafka_client_config(), throughput_mode=mode)
        self._producer = producer or Producer(config)
        self._settings = settings
        self._delivered = 0
        self._failed = 0

    def _count_delivery(self, err, msg) -> None:
        if err is not None:
            self._failed += 1
            _delivery_report(err, msg)
        else:
            self._delivered += 1

    @property
    def stats(self) -> dict:
        return {"delivered": self._delivered, "failed": self._failed}

    def publish(
        self,
        topic: str,
        key: str,
        value: dict | object,
        callback: Optional[Callable] = None,
    ) -> None:
        payload = serialize(value)
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8") if key else None,
                    value=payload,
                    callback=callback or self._count_delivery,
                )
                break
            except BufferError:
                # Queue full under high load — drain callbacks then retry
                self._producer.poll(0.1)
        self._producer.poll(0)

    def publish_checkpoint(self, event: dict | object) -> None:
        if hasattr(event, "partition_key"):
            key = event.partition_key()
            payload = event
        else:
            key = event["counter_id"]
            payload = event
        self.publish(self._settings.checkpoint_topic, key, payload)

    def publish_retry(self, event: dict, key: str) -> None:
        self.publish(self._settings.retry_topic, key, event)

    def publish_dlq(self, dlq_payload: dict, key: str = "dlq") -> None:
        self.publish(self._settings.dlq_topic, key, dlq_payload)

    def publish_aggregation(self, event: dict, key: str) -> None:
        self.publish(self._settings.aggregation_topic, key, event)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)


# Backwards-compatible alias
CheckpointProducer = KafkaEventPublisher
