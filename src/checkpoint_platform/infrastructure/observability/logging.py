"""Centralized structured logging with correlation / request context."""

from __future__ import annotations

import logging
import os
import sys
import uuid
from contextvars import ContextVar
from typing import Any

import structlog

from checkpoint_platform.config import get_settings

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_service_name: ContextVar[str] = ContextVar("service_name", default="checkpoint-platform")


def get_correlation_id() -> str | None:
    return _correlation_id.get()


def get_request_id() -> str | None:
    return _request_id.get()


def bind_context(**kwargs: Any) -> None:
    """Bind fields into structlog contextvars for the current request/task."""
    if kwargs.get("correlation_id"):
        _correlation_id.set(str(kwargs["correlation_id"]))
    if kwargs.get("request_id"):
        _request_id.set(str(kwargs["request_id"]))
    if kwargs.get("service"):
        _service_name.set(str(kwargs["service"]))
    structlog.contextvars.bind_contextvars(**{k: v for k, v in kwargs.items() if v is not None})


def clear_context() -> None:
    _correlation_id.set(None)
    _request_id.set(None)
    structlog.contextvars.clear_contextvars()


def new_request_ids(
    incoming_request_id: str | None = None,
    incoming_correlation_id: str | None = None,
) -> tuple[str, str]:
    request_id = incoming_request_id or str(uuid.uuid4())
    correlation_id = incoming_correlation_id or request_id
    return request_id, correlation_id


def _inject_context(logger: logging.Logger, method_name: str, event_dict: dict) -> dict:
    event_dict.setdefault("service", _service_name.get())
    settings = get_settings()
    event_dict.setdefault("env", settings.app_env)
    cid = _correlation_id.get()
    rid = _request_id.get()
    if cid:
        event_dict.setdefault("correlation_id", cid)
    if rid:
        event_dict.setdefault("request_id", rid)
    return event_dict


def setup_logging(service: str = "checkpoint-platform") -> None:
    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    _service_name.set(service)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
        force=True,
    )

    # Quiet noisy libraries
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        _inject_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.app_env == "local" and settings.log_level.upper() == "DEBUG":
        use_json = False
    else:
        use_json = True
    # Cloud Run / Alloy path: force JSON when LOG_FORMAT=json
    if os.environ.get("LOG_FORMAT", "").lower() == "json":
        use_json = True
    elif os.environ.get("LOG_FORMAT", "").lower() in {"console", "text"}:
        use_json = False

    renderer: Any = (
        structlog.processors.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            *shared_processors,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    bind_context(service=service, env=settings.app_env)


def get_logger(name: str = __name__):
    return structlog.get_logger(name)
