"""Unit normalization.

Quantity aggregates are stored in kilograms. Devices report in whatever unit the
counter was configured with (grams on small scales, pounds on imported hardware),
so every quantity is converted before it touches an aggregate. An unknown unit is
permanent — retrying will never make it convertible.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from checkpoint_platform.domain.enums import (
    QUANTITY_TYPES,
    TEMPERATURE_TYPES,
    CheckpointType,
)
from checkpoint_platform.domain.exceptions import PermanentValidationError

KG = Decimal("1")

MASS_UNITS_TO_KG: dict[str, Decimal] = {
    "KG": KG,
    "KGS": KG,
    "KILOGRAM": KG,
    "KILOGRAMS": KG,
    "G": Decimal("0.001"),
    "GM": Decimal("0.001"),
    "GRAM": Decimal("0.001"),
    "GRAMS": Decimal("0.001"),
    "MG": Decimal("0.000001"),
    "LB": Decimal("0.45359237"),
    "LBS": Decimal("0.45359237"),
    "POUND": Decimal("0.45359237"),
    "POUNDS": Decimal("0.45359237"),
    "OZ": Decimal("0.0283495231"),
    "TON": Decimal("1000"),
    "TONNE": Decimal("1000"),
    "MT": Decimal("1000"),
}

TEMPERATURE_UNITS = {"C", "CELSIUS", "F", "FAHRENHEIT", "K", "KELVIN"}

QUANTITY_PRECISION = Decimal("0.001")


def canonical_unit(unit: str | None) -> str | None:
    if unit is None:
        return None
    cleaned = unit.strip().upper().replace(".", "")
    return cleaned or None


def is_supported_unit(checkpoint_type: CheckpointType, unit: str | None) -> bool:
    cleaned = canonical_unit(unit)
    if checkpoint_type in QUANTITY_TYPES:
        return cleaned is None or cleaned in MASS_UNITS_TO_KG
    if checkpoint_type in TEMPERATURE_TYPES:
        return cleaned is None or cleaned in TEMPERATURE_UNITS
    return True


def to_kilograms(value: Decimal | float | int | str | None, unit: str | None) -> Decimal:
    """Convert a mass reading to kilograms, quantized to 3 decimal places."""
    if value is None:
        return Decimal("0")

    try:
        amount = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PermanentValidationError(f"unparseable quantity value: {value!r}") from exc

    cleaned = canonical_unit(unit)
    if cleaned is None:
        # Historical producers omit the unit for mass; kg is the documented default.
        factor = KG
    else:
        factor = MASS_UNITS_TO_KG.get(cleaned)
        if factor is None:
            raise PermanentValidationError(
                f"unsupported mass unit {unit!r}; expected one of "
                f"{sorted(MASS_UNITS_TO_KG)}"
            )

    return (amount * factor).quantize(QUANTITY_PRECISION)


def quantity_in_kg(
    checkpoint_type: CheckpointType,
    value: Decimal | float | int | str | None,
    unit: str | None,
) -> Decimal:
    """Kilograms for quantity checkpoints, zero for every other checkpoint type."""
    if checkpoint_type not in QUANTITY_TYPES:
        return Decimal("0")
    return to_kilograms(value, unit)
