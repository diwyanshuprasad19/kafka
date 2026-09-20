"""Business-day resolution for daily aggregates.

A cafeteria's "day" is local, not UTC. A dinner checkpoint at 23:40 IST is
19:10 UTC on the previous... no — 18:10 UTC the *same* day, but a 01:30 IST
breakfast prep event is 20:00 UTC the day before. Taking ``occurred_at.date()``
therefore buckets late-evening and early-morning events into the wrong day
depending on which offset the producer happened to send. Everything resolves the
date through this module so the boundary is one configured timezone.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@lru_cache(maxsize=8)
def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def ensure_aware(moment: datetime) -> datetime:
    """Treat naive timestamps as UTC rather than comparing across tz-awareness."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment


def business_date(moment: datetime, tz_name: str | None = None) -> date:
    if tz_name is None:
        from checkpoint_platform.config import get_settings

        tz_name = get_settings().business_timezone
    return ensure_aware(moment).astimezone(_zone(tz_name)).date()


def business_today(tz_name: str | None = None) -> date:
    return business_date(datetime.now(UTC), tz_name)
