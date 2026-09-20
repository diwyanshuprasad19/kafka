import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


class CheckpointJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)


def serialize(payload: dict | Any) -> bytes:
    if hasattr(payload, "model_dump"):
        data = payload.model_dump(mode="json")
    else:
        data = payload
    return json.dumps(data, cls=CheckpointJSONEncoder).encode("utf-8")


def deserialize(raw: bytes | str) -> dict:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)
