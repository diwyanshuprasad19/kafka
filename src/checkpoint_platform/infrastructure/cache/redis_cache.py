"""Redis cache for frequently read aggregates. Optional — disabled if Redis is down."""

from typing import Any

import redis

from checkpoint_platform.config import get_settings
from checkpoint_platform.infrastructure.observability.logging import get_logger

logger = get_logger(__name__)


class RedisAggregateCache:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: redis.Redis | None = None
        if self.settings.redis_enabled:
            try:
                self._client = redis.from_url(
                    self.settings.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=1.0,
                    socket_timeout=1.0,
                )
                self._client.ping()
            except Exception as exc:  # noqa: BLE001
                logger.warning("redis_unavailable", error=str(exc))
                self._client = None

    def _key(self, date: str, counter_id: str, meal_type: str) -> str:
        return f"agg:{date}:{counter_id}:{meal_type}"

    def get(self, date: str, counter_id: str, meal_type: str) -> dict | None:
        if not self._client:
            return None
        try:
            import json

            raw = self._client.get(self._key(date, counter_id, meal_type))
            return json.loads(raw) if raw else None
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_get_failed", error=str(exc))
            return None

    def set(self, date: str, counter_id: str, meal_type: str, payload: dict) -> None:
        if not self._client:
            return
        try:
            import json

            self._client.setex(
                self._key(date, counter_id, meal_type),
                self.settings.redis_cache_ttl_seconds,
                json.dumps(payload),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_set_failed", error=str(exc))

    def invalidate(self, date: str, counter_id: str, meal_type: str) -> None:
        if not self._client:
            return
        try:
            self._client.delete(self._key(date, counter_id, meal_type))
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_invalidate_failed", error=str(exc))

    def cafe_key(self, date: str, cafe_id: str) -> str:
        return f"cafe_agg:{date}:{cafe_id}"

    def get_cafe(self, date: str, cafe_id: str) -> dict[str, Any] | None:
        if not self._client:
            return None
        try:
            import json

            raw = self._client.get(self.cafe_key(date, cafe_id))
            return json.loads(raw) if raw else None
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_cafe_get_failed", error=str(exc))
            return None

    def set_cafe(self, date: str, cafe_id: str, payload: dict) -> None:
        if not self._client:
            return
        try:
            import json

            self._client.setex(
                self.cafe_key(date, cafe_id),
                self.settings.redis_cache_ttl_seconds,
                json.dumps(payload),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_cafe_set_failed", error=str(exc))


# Alias
AggregateCache = RedisAggregateCache
