from enum import Enum


class EventType(str, Enum):
    CREATED = "checkpoint.created"
    COMPLETED = "checkpoint.completed"
    UPDATED = "checkpoint.updated"
    FAILED = "checkpoint.failed"


class MealType(str, Enum):
    BREAKFAST = "BREAKFAST"
    LUNCH = "LUNCH"
    SNACKS = "SNACKS"
    DINNER = "DINNER"
    NA = "NA"


class CheckpointType(str, Enum):
    # Meal service
    MEAL_READINESS = "MEAL_READINESS"
    COUNTER_OPERATIONAL = "COUNTER_OPERATIONAL"
    ITEM_AVAILABILITY = "ITEM_AVAILABILITY"

    # Food quantities
    FOOD_RECEIVED = "FOOD_RECEIVED"
    FOOD_PREPARED = "FOOD_PREPARED"
    FOOD_CONSUMED = "FOOD_CONSUMED"
    FOOD_WASTAGE = "FOOD_WASTAGE"

    # Temperature
    HOT_FOOD_TEMPERATURE = "HOT_FOOD_TEMPERATURE"
    COLD_STORAGE_TEMPERATURE = "COLD_STORAGE_TEMPERATURE"
    VENDING_TEMPERATURE = "VENDING_TEMPERATURE"

    # Hygiene / cleaning
    KITCHEN_CLEANING = "KITCHEN_CLEANING"
    STAFF_HYGIENE = "STAFF_HYGIENE"
    EQUIPMENT_CLEANING = "EQUIPMENT_CLEANING"
    PANTRY_CLEANING = "PANTRY_CLEANING"

    # Storage / food safety
    FIFO_CHECK = "FIFO_CHECK"
    RAW_MATERIAL_LABELLING = "RAW_MATERIAL_LABELLING"
    FOOD_SAMPLE = "FOOD_SAMPLE"
    PEST_CONTROL = "PEST_CONTROL"

    # Audits
    VENDOR_AUDIT = "VENDOR_AUDIT"
    SAFETY_WALK = "SAFETY_WALK"

    # Vending
    VENDING_REFILL = "VENDING_REFILL"

    # Incidents and follow-up
    INCIDENT = "INCIDENT"
    CORRECTIVE_ACTION = "CORRECTIVE_ACTION"
    CUSTOMER_FEEDBACK = "CUSTOMER_FEEDBACK"


class CheckpointStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PASS = "PASS"
    FAIL = "FAIL"
    # Incident / corrective-action lifecycle
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    CLOSED = "CLOSED"


# Types that count towards completion / compliance percentage.
COMPLETION_TYPES = {
    CheckpointType.MEAL_READINESS,
    CheckpointType.COUNTER_OPERATIONAL,
    CheckpointType.ITEM_AVAILABILITY,
    CheckpointType.HOT_FOOD_TEMPERATURE,
    CheckpointType.COLD_STORAGE_TEMPERATURE,
    CheckpointType.VENDING_TEMPERATURE,
    CheckpointType.KITCHEN_CLEANING,
    CheckpointType.STAFF_HYGIENE,
    CheckpointType.EQUIPMENT_CLEANING,
    CheckpointType.PANTRY_CLEANING,
    CheckpointType.FIFO_CHECK,
    CheckpointType.RAW_MATERIAL_LABELLING,
    CheckpointType.FOOD_SAMPLE,
    CheckpointType.PEST_CONTROL,
    CheckpointType.VENDOR_AUDIT,
    CheckpointType.SAFETY_WALK,
    CheckpointType.VENDING_REFILL,
    CheckpointType.CORRECTIVE_ACTION,
}

HYGIENE_TYPES = {
    CheckpointType.KITCHEN_CLEANING,
    CheckpointType.STAFF_HYGIENE,
    CheckpointType.EQUIPMENT_CLEANING,
    CheckpointType.PANTRY_CLEANING,
}

TEMPERATURE_TYPES = {
    CheckpointType.HOT_FOOD_TEMPERATURE,
    CheckpointType.COLD_STORAGE_TEMPERATURE,
    CheckpointType.VENDING_TEMPERATURE,
}

QUANTITY_TYPES = {
    CheckpointType.FOOD_RECEIVED,
    CheckpointType.FOOD_PREPARED,
    CheckpointType.FOOD_CONSUMED,
    CheckpointType.FOOD_WASTAGE,
}

# Incidents are tracked separately from compliance: a reported incident is not a
# checkpoint that was "failed", it is an event with an open/closed lifecycle.
INCIDENT_TYPES = {CheckpointType.INCIDENT}

FEEDBACK_TYPES = {CheckpointType.CUSTOMER_FEEDBACK}

OPEN_STATUSES = {CheckpointStatus.OPEN, CheckpointStatus.IN_PROGRESS}
