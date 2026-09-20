"""Flask middleware: request IDs, structured access logs, HTTP metrics."""

from __future__ import annotations

import time

from flask import Flask, Response, g, request

from checkpoint_platform.infrastructure.observability.logging import (
    bind_context,
    clear_context,
    get_logger,
    new_request_ids,
)
from checkpoint_platform.infrastructure.observability.metrics import HTTP_LATENCY, HTTP_REQUESTS

logger = get_logger(__name__)


def _endpoint_label() -> str:
    rule = request.url_rule.rule if request.url_rule else request.path
    # Limit cardinality
    if rule.startswith("/aggregations/"):
        parts = rule.split("/")
        return "/".join(parts[:3]) + ("/<id>" if len(parts) > 3 else "")
    if rule.startswith("/reingest/"):
        return rule
    return rule


def register_observability(app: Flask, *, service: str = "checkpoint-api") -> None:
    @app.before_request
    def _before_request() -> None:
        clear_context()
        request_id, correlation_id = new_request_ids(
            incoming_request_id=request.headers.get("X-Request-ID"),
            incoming_correlation_id=request.headers.get("X-Correlation-ID"),
        )
        g.request_id = request_id
        g.correlation_id = correlation_id
        g._start_time = time.perf_counter()
        bind_context(
            request_id=request_id,
            correlation_id=correlation_id,
            service=service,
            http_method=request.method,
            http_path=request.path,
        )

    @app.after_request
    def _after_request(response: Response) -> Response:
        response.headers["X-Request-ID"] = getattr(g, "request_id", "")
        response.headers["X-Correlation-ID"] = getattr(g, "correlation_id", "")

        elapsed = time.perf_counter() - getattr(g, "_start_time", time.perf_counter())
        endpoint = _endpoint_label()
        status = str(response.status_code)

        if request.path != "/metrics":
            HTTP_REQUESTS.labels(
                method=request.method,
                endpoint=endpoint,
                status=status,
            ).inc()
            HTTP_LATENCY.labels(method=request.method, endpoint=endpoint).observe(elapsed)
            logger.info(
                "http_request",
                method=request.method,
                path=request.path,
                status=response.status_code,
                duration_ms=round(elapsed * 1000, 2),
            )
        return response

    @app.teardown_request
    def _teardown_request(_exc=None) -> None:  # noqa: ANN001
        clear_context()
