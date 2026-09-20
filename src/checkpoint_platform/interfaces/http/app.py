"""Flask API factory — wires routes, logging middleware, and metrics."""

from __future__ import annotations

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


def create_app() -> Flask:
    settings = get_settings()
    app = Flask(__name__)
    CORS(app)

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

    # Dedicated Prometheus scrape port (Compose) + also /metrics on API port
    try:
        start_metrics_server(settings.metrics_port)
    except OSError:
        logger.warning("metrics_port_in_use", port=settings.metrics_port)

    logger.info("api_started", env=settings.app_env, port=settings.api_port)
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    app.run(
        host=settings.api_host,
        port=settings.api_port,
        debug=settings.app_env == "local",
    )


if __name__ == "__main__":
    main()
