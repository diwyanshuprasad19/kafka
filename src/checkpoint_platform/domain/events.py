from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from checkpoint_platform.domain.business_day import ensure_aware
from checkpoint_platform.domain.enums import (
    QUANTITY_TYPES,
    CheckpointStatus,
    CheckpointType,
    EventType,
    MealType,
)
from checkpoint_platform.domain.units import canonical_unit, is_supported_unit


class CheckpointEvent(BaseModel):
    """Canonical checkpoint event published to Kafka.

    Validation here is the permanent-failure boundary: anything rejected at this
    layer can never succeed on retry, so the processor sends it straight to the DLQ.
    """

    event_id: UUID
    event_type: EventType
    checkpoint_id: str = Field(..., min_length=1, max_length=128)
    checkpoint_version: int = Field(..., ge=1, le=1_000_000)

    client_id: str = Field(..., min_length=1, max_length=64)
    cafe_id: str = Field(..., min_length=1, max_length=64)
    counter_id: str = Field(..., min_length=1, max_length=64)

    meal_type: MealType = MealType.NA
    checkpoint_type: CheckpointType
    status: CheckpointStatus

    value: Decimal | None = None
    unit: str | None = None

    occurred_at: datetime
    retry_count: int = Field(default=0, ge=0)

    # Observability / re-ingestion metadata (optional on the wire)
    correlation_id: str | None = None
    reingested: bool = False
    reingest_from_dlq_id: int | None = None

    @field_validator("value", mode="before")
    @classmethod
    def coerce_value(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        if isinstance(v, str):
            try:
                return Decimal(v.strip())
            except InvalidOperation as exc:
                raise ValueError(f"value is not a number: {v!r}") from exc
        return v

    @field_validator("value")
    @classmethod
    def value_must_be_finite(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return None
        if not v.is_finite():
            raise ValueError("value must be finite (no NaN/Infinity)")
        return v

    @field_validator("unit", mode="before")
    @classmethod
    def normalize_unit(cls, v: Any) -> Any:
        return canonical_unit(v) if isinstance(v, str) else v

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_must_be_aware(cls, v: datetime) -> datetime:
        return ensure_aware(v)

    @model_validator(mode="after")
    def validate_business_rules(self) -> "CheckpointEvent":
        from checkpoint_platform.config import get_settings

        settings = get_settings()

        if not is_supported_unit(self.checkpoint_type, self.unit):
            raise ValueError(f"unit {self.unit!r} is not valid for {self.checkpoint_type.value}")

        if self.checkpoint_type in QUANTITY_TYPES:
            if self.value is None:
                raise ValueError(f"{self.checkpoint_type.value} requires a numeric value")
            if self.value < 0:
                raise ValueError("quantity value cannot be negative")

            from checkpoint_platform.domain.units import to_kilograms

            as_kg = to_kilograms(self.value, self.unit)
            if as_kg > Decimal(str(settings.max_quantity_kg)):
                raise ValueError(
                    f"quantity {as_kg}kg exceeds sanity ceiling "
                    f"{settings.max_quantity_kg}kg — likely a faulty scale"
                )

        skew = self.occurred_at - datetime.now(UTC)
        if skew > timedelta(seconds=settings.max_clock_skew_seconds):
            raise ValueError(
                f"occurred_at is {int(skew.total_seconds())}s in the future, beyond "
                f"the {settings.max_clock_skew_seconds}s clock-skew allowance — "
                "either the device clock is wrong or a naive local timestamp was "
                "sent (timestamps without an offset are read as UTC)"
            )

        return self

    def partition_key(self) -> str:
        return self.counter_id

    def quantity_kg(self) -> Decimal:
        """Value converted to kilograms (zero for non-quantity checkpoints)."""
        from checkpoint_platform.domain.units import quantity_in_kg

        return quantity_in_kg(self.checkpoint_type, self.value, self.unit)


class CheckpointCreateRequest(BaseModel):
    """HTTP API payload to create/update a checkpoint (async via Kafka)."""

    checkpoint_id: str | None = None
    checkpoint_version: int = 1
    client_id: str
    cafe_id: str
    counter_id: str
    meal_type: MealType = MealType.LUNCH
    checkpoint_type: CheckpointType
    status: CheckpointStatus = CheckpointStatus.COMPLETED
    value: Decimal | None = None
    unit: str | None = None
    event_type: EventType = EventType.COMPLETED
    occurred_at: datetime | None = None


class AggregationCompletedEvent(BaseModel):
    event_type: str = "aggregation.completed"
    counter_id: str
    cafe_id: str
    client_id: str
    meal_type: str
    aggregation_date: str
    compliance_percentage: float
    wastage_percentage: float
    total_checkpoints: int
    completed_checkpoints: int
    food_wastage_kg: float


class DlqEvent(BaseModel):
    original_topic: str
    original_partition: int
    original_offset: int
    error: str
    retry_count: int
    failed_at: datetime
    original_event: dict[str, Any]


class ReingestDlqRequest(BaseModel):
    """Re-publish DLQ record(s) back onto the main checkpoint topic."""

    dlq_ids: list[int] = Field(..., min_length=1, max_length=100)
    # Always mint a new event_id by default so idempotency allows reprocessing
    new_event_id: bool = True
    # If True and new_event_id=False, delete processed_events for the old event_id
    force: bool = False
    reset_retry_count: bool = True


class ReingestEventsRequest(BaseModel):
    """Manually re-inject checkpoint event payloads onto Kafka."""

    events: list[dict[str, Any]] = Field(..., min_length=1, max_length=100)
    new_event_id: bool = True


class AggregationResponse(BaseModel):
    aggregation_date: str
    client_id: str
    cafe_id: str
    counter_id: str
    meal_type: str
    total_checkpoints: int
    completed_checkpoints: int
    failed_checkpoints: int
    compliance_percentage: float
    food_prepared_kg: float
    food_consumed_kg: float
    food_wastage_kg: float
    wastage_percentage: float
    hygiene_pass_count: int
    hygiene_fail_count: int
    temperature_pass_count: int
    temperature_fail_count: int
