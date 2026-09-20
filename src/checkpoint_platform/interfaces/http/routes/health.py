from flask import Blueprint, jsonify
from sqlalchemy import select

from checkpoint_platform.config import get_settings
from checkpoint_platform.config.container import get_session

bp = Blueprint("health", __name__)


@bp.get("/health")
def health():
    settings = get_settings()
    return jsonify({"status": "ok", "env": settings.app_env})


@bp.get("/ready")
def ready():
    session = get_session()
    try:
        session.execute(select(1))
        return jsonify({"ready": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ready": False, "error": str(exc)}), 503
    finally:
        session.close()
