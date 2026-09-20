"""Domain / application exceptions + error classification for retry vs DLQ."""


class DuplicateEventError(Exception):
    """Event already processed — safe to commit Kafka offset."""


class StaleVersionError(Exception):
    """Incoming checkpoint_version <= current — ignore, commit offset."""


class PermanentValidationError(Exception):
    """Non-retryable validation failure — send to DLQ."""


class TransientProcessingError(Exception):
    """Retryable failure (DB timeout, network, etc.)."""


class ConcurrentUpdateError(TransientProcessingError):
    """Lost a race for the same checkpoint row — safe to retry immediately."""


# Substrings / types treated as transient (retryable)
_TRANSIENT_HINTS = (
    "timeout",
    "timed out",
    "connection refused",
    "connection reset",
    "could not connect",
    "server closed the connection",
    "deadlock",
    "serialization failure",
    "too many connections",
    "temporarily unavailable",
    "network",
    "broken pipe",
    "operationalerror",
    "interfaceerror",
    "disconnectionerror",
)

_PERMANENT_HINTS = (
    "validationerror",
    "invalid input",
    "not null",
    "foreign key",
    "check constraint",
    "invalid uuid",
    "missing",
)


def is_transient_error(exc: BaseException) -> bool:
    """Decide whether a processing failure should go to retry vs DLQ."""
    if isinstance(exc, TransientProcessingError):
        return True
    if isinstance(exc, PermanentValidationError):
        return False
    if isinstance(exc, (DuplicateEventError, StaleVersionError)):
        return False

    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    blob = f"{name} {msg}"

    if any(h in blob for h in _PERMANENT_HINTS):
        # Still allow timeout/connection to win if also present
        if any(h in blob for h in ("timeout", "connection", "operationalerror")):
            return True
        return False
    if any(h in blob for h in _TRANSIENT_HINTS):
        return True

    # Unknown SQLAlchemy / DB errors: prefer retry
    if "sqlalchemy" in blob or "psycopg" in blob:
        return True
    return True  # fail-open to retry for unexpected errors (bounded by max_retries)


def retry_delay_seconds(retry_count: int, base_delay_ms: int) -> float:
    """Exponential backoff: base * 2^(retry_count), capped at 30s."""
    delay = (base_delay_ms / 1000.0) * (2 ** max(retry_count, 0))
    return min(delay, 30.0)
