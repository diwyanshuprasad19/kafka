from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from checkpoint_platform.domain.enums import CheckpointStatus, CheckpointType, EventType, MealType
from checkpoint_platform.domain.events import CheckpointCreateRequest, CheckpointEvent


def test_create_request_defaults():
    req = CheckpointCreateRequest(
        client_id="client-1",
        cafe_id="cafe-1",
        counter_id="counter-1",
        checkpoint_type=CheckpointType.MEAL_READINESS,
    )
    assert req.status == CheckpointStatus.COMPLETED
    assert req.meal_type == MealType.LUNCH


def test_serialize_roundtrip():
    from checkpoint_platform.infrastructure.messaging.serializer import deserialize, serialize

    ev = CheckpointEvent(
        event_id=uuid4(),
        event_type=EventType.UPDATED,
        checkpoint_id="cp-x",
        checkpoint_version=3,
        client_id="c",
        cafe_id="cafe",
        counter_id="counter-9",
        meal_type=MealType.DINNER,
        checkpoint_type=CheckpointType.FOOD_PREPARED,
        status=CheckpointStatus.COMPLETED,
        value=Decimal("100.5"),
        unit="KG",
        occurred_at=datetime.now(timezone.utc),
    )
    raw = serialize(ev)
    data = deserialize(raw)
    restored = CheckpointEvent.model_validate(data)
    assert restored.counter_id == "counter-9"
    assert float(restored.value) == 100.5
