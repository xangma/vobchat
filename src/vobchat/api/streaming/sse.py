from __future__ import annotations

import json

from vobchat.api.schemas.chat import SSEEventPayload


def encode_sse_event(payload: SSEEventPayload) -> str:
    serialized = json.dumps(payload.model_dump(mode="json"), ensure_ascii=True)
    return f"event: {payload.event.value}\ndata: {serialized}\n\n"


def sse_keepalive() -> str:
    return ": keep-alive\n\n"
