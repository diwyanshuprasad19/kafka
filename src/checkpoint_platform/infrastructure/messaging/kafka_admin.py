from confluent_kafka.admin import AdminClient, NewTopic

from checkpoint_platform.config import get_settings
from checkpoint_platform.infrastructure.observability.logging import get_logger

logger = get_logger(__name__)


def create_topics(
    partitions: int | None = None,
    replication_factor: int | None = None,
) -> None:
    settings = get_settings()
    partitions = partitions or settings.checkpoint_partitions
    replication_factor = (
        replication_factor if replication_factor is not None else settings.kafka_replication_factor
    )
    admin = AdminClient(settings.kafka_client_config())

    # The producer uses acks=all, but that only means "all *in-sync* replicas" —
    # and Kafka's default min.insync.replicas is 1. On a replicated cluster that
    # would let an acknowledged write live on a single broker and vanish with it.
    # Requiring two in-sync replicas is what makes acks=all actually durable.
    # A single-broker local cluster cannot satisfy 2, so it stays at 1.
    base_config = {}
    if replication_factor > 1:
        base_config["min.insync.replicas"] = str(min(2, replication_factor))

    # The DLQ is for humans: it must outlive the incident that filled it, so give it
    # far longer retention than the live stream.
    dlq_config = dict(base_config)
    dlq_config["retention.ms"] = str(settings.dlq_topic_retention_days * 86_400_000)

    topics = [
        NewTopic(
            settings.checkpoint_topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=dict(base_config),
        ),
        NewTopic(
            settings.retry_topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=dict(base_config),
        ),
        NewTopic(
            settings.dlq_topic,
            num_partitions=3,
            replication_factor=replication_factor,
            config=dlq_config,
        ),
        NewTopic(
            settings.aggregation_topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=dict(base_config),
        ),
    ]

    futures = admin.create_topics(topics, request_timeout=30)
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(
                "topic_created",
                topic=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                min_insync_replicas=base_config.get("min.insync.replicas", "1"),
            )
        except Exception as exc:
            if "already exists" in str(exc).lower() or "TopicExistsError" in type(exc).__name__:
                logger.info("topic_exists", topic=topic_name)
            else:
                logger.error("topic_create_failed", topic=topic_name, error=str(exc))
                raise
