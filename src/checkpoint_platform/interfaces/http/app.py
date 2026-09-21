"""Flask API factory — wires routes, logging middleware, and metrics."""

from __future__ import annotations

import os

from flask import Flask
from flask_cors import CORS

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_cache, get_publisher
from checkpoint_platform.infrastructure.observability.logging import (
    get_logger,
    setup_logging,
)
from checkpoint_platform.infrastructure.observability.metrics import (
    set_app_info,
    start_metrics_server,
)
from checkpoint_platform.infrastructure.persistence.session import init_db
from checkpoint_platform.interfaces.http.middleware import register_observability
from checkpoint_platform.interfaces.http.routes import (
    aggregations,
    checkpoints,
    dlq,
    health,
    history,
    metrics,
    ops,
    reingest,
)

setup_logging(service="checkpoint-api")
logger = get_logger(__name__)


def _configure_cors(app: Flask, settings) -> None:
    raw = (settings.cors_origins or "").strip()
    if not raw:
        return
    if raw == "*":
        CORS(app)
        return
    origins = [o.strip() for o in raw.split(",") if o.strip()]
    if origins:
        CORS(app, resources={r"/*": {"origins": origins}})


def create_app() -> Flask:
    settings = get_settings()
    app = Flask(__name__)
    _configure_cors(app, settings)

    app.extensions["cache"] = get_cache()
    app.extensions["publisher"] = get_publisher()

    register_observability(app, service="checkpoint-api")
    set_app_info(env=settings.app_env, service="checkpoint-api")

    try:
        init_db()
    except Exception as exc:  # noqa: BLE001
        logger.warning("init_db_deferred", error=str(exc))

    app.register_blueprint(health.bp)
    app.register_blueprint(checkpoints.bp)
    app.register_blueprint(aggregations.bp)
    app.register_blueprint(history.bp)
    app.register_blueprint(dlq.bp)
    app.register_blueprint(reingest.bp)
    app.register_blueprint(metrics.bp)
    app.register_blueprint(ops.bp)

    # Cloud Run exposes one port — scrape /metrics on the API port only.
    # Local/compose can still open the dedicated metrics_port.
    on_cloud_run = bool(os.environ.get("K_SERVICE"))
    if not on_cloud_run and settings.metrics_port > 0:
        try:
            start_metrics_server(settings.metrics_port)
        except OSError:
            logger.warning("metrics_port_in_use", port=settings.metrics_port)

    logger.info(
        "api_started",
        env=settings.app_env,
        port=settings.api_port,
        cloud_run=on_cloud_run,
    )
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    app.run(
        host=settings.api_host,
        port=int(os.environ.get("PORT", settings.api_port)),
        debug=settings.app_env == "local",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
