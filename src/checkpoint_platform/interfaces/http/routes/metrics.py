from flask import Blueprint, Response

from checkpoint_platform.infrastructure.observability.metrics import metrics_payload

bp = Blueprint("metrics", __name__)


@bp.get("/metrics")
def metrics():
    payload, content_type = metrics_payload()
    return Response(payload, mimetype=content_type)
