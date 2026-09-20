"""Simple DI / factories for wiring infrastructure into application services."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy.orm import Session

from checkpoint_platform.application.aggregation import AggregationService
from checkpoint_platform.application.checkpoint_publish import CheckpointPublishService
from checkpoint_platform.application.event_processing import EventProcessor
from checkpoint_platform.application.ports import AggregateCachePort, EventPublisher
from checkpoint_platform.application.query_aggregates import AggregateQueryService
from checkpoint_platform.infrastructure.cache.redis_cache import RedisAggregateCache
from checkpoint_platform.infrastructure.messaging.kafka_producer import (
    KafkaEventPublisher,
)
from checkpoint_platform.infrastructure.persistence.session import SessionLocal


def get_session() -> Session:
    return SessionLocal()


@lru_cache
def get_publisher() -> EventPublisher:
    return KafkaEventPublisher()


@lru_cache
def get_cache() -> AggregateCachePort:
    return RedisAggregateCache()


def get_aggregation_service(session: Session) -> AggregationService:
    return AggregationService(session)


def get_publish_service(
    publisher: EventPublisher | None = None,
) -> CheckpointPublishService:
    return CheckpointPublishService(publisher or get_publisher())


def get_event_processor(
    session: Session,
    publisher: EventPublisher | None = None,
    *,
    manage_transaction: bool = True,
) -> EventProcessor:
    return EventProcessor(
        session=session,
        publisher=publisher or get_publisher(),
        manage_transaction=manage_transaction,
    )


def get_query_service(
    session: Session, cache: AggregateCachePort | None = None
) -> AggregateQueryService:
    return AggregateQueryService(session=session, cache=cache or get_cache())


def get_reingestion_service(session: Session, publisher: EventPublisher | None = None):
    from checkpoint_platform.application.reingestion import ReIngestionService

    return ReIngestionService(session=session, publisher=publisher or get_publisher())
