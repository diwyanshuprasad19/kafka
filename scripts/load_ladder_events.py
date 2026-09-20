"""Event generator for the benchmark ladder.

Kept separate from the ladder itself so the shape of the synthetic workload is easy
to inspect and change. The important property is that most events are genuinely new
checkpoints: if ids repeat, the consumer rejects them as stale and the benchmark
measures the cheapest path instead of the real one.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

MEALS = ("BREAKFAST", "LUNCH", "SNACKS", "DINNER")

# (checkpoint_type, status, unit) — a realistic mix of quantity, temperature and
# pass/fail checkpoints rather than one hot type.
TYPES = (
    ("FOOD_WASTAGE", "COMPLETED", "KG"),
    ("FOOD_PREPARED", "COMPLETED", "KG"),
    ("FOOD_CONSUMED", "COMPLETED", "KG"),
    ("FOOD_RECEIVED", "COMPLETED", "KG"),
    ("HOT_FOOD_TEMPERATURE", "PASS", "C"),
    ("COLD_STORAGE_TEMPERATURE", "PASS", "C"),
    ("KITCHEN_CLEANING", "COMPLETED", None),
    ("STAFF_HYGIENE", "PASS", None),
    ("EQUIPMENT_CLEANING", "COMPLETED", None),
    ("MEAL_READINESS", "COMPLETED", None),
    ("FIFO_CHECK", "PASS", None),
    ("RAW_MATERIAL_LABELLING", "PASS", None),
)

# One in every N events corrects an earlier checkpoint, which is the expensive path:
# lock the prior state, reverse its contribution, apply the new one.
CORRECTION_EVERY = 10

# Distinct run prefix so repeated ladder runs do not collide on checkpoint ids and
# get rejected as stale versions of each other.
RUN = uuid4().hex[:8]


def build_event(i: int, counters: int) -> dict:
    is_correction = i % CORRECTION_EVERY == 0 and i >= CORRECTION_EVERY
    source = i - CORRECTION_EVERY if is_correction else i

    counter = f"counter-{(source % counters) + 1}"
    checkpoint_type, status, unit = TYPES[source % len(TYPES)]
    meal = MEALS[(source // len(TYPES)) % len(MEALS)]

    event = {
        "event_id": str(uuid4()),
        "event_type": "checkpoint.updated" if is_correction else "checkpoint.completed",
        "checkpoint_id": f"cp-{RUN}-{source}",
        "checkpoint_version": 2 if is_correction else 1,
        "client_id": f"client-{(source % 4) + 1}",
        "cafe_id": f"cafe-{(source % 40) + 1}",
        "counter_id": counter,
        "meal_type": meal,
        "checkpoint_type": checkpoint_type,
        "status": status,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    if unit == "KG":
        event["value"] = round(5 + (source % 90) * 0.5, 3)
        event["unit"] = "KG"
    elif unit == "C":
        event["value"] = 60 + (source % 15)
        event["unit"] = "C"
    return event
