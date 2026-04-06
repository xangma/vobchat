from __future__ import annotations

import json
import logging
from threading import RLock
from uuid import uuid4

import httpx
from flask import Response, current_app, jsonify, request, session, stream_with_context
from flask_login import current_user

from vobchat.core.settings import get_settings


logger = logging.getLogger(__name__)


class InMemoryProxyThreadRegistry:
    def __init__(self) -> None:
        self._lock = RLock()
        self._owners: dict[str, str] = {}

    def bind(self, thread_id: str, owner_token: str) -> bool:
        if not thread_id or not owner_token:
            return False
        with self._lock:
            existing = self._owners.get(thread_id)
            if existing is not None and existing != owner_token:
                return False
            self._owners[thread_id] = owner_token
        return True

    def is_owned_by(self, thread_id: str, owner_token: str) -> bool:
        if not thread_id or not owner_token:
            return False
        with self._lock:
            return self._owners.get(thread_id) == owner_token

    def clear(self) -> None:
        with self._lock:
            self._owners.clear()


_thread_registry = InMemoryProxyThreadRegistry()


def reset_proxy_thread_registry() -> None:
    _thread_registry.clear()


def _route_prefix() -> str:
    return get_settings().dash.route_prefix


def _proxy_chat_prefix() -> str:
    return f"{_route_prefix()}/proxy/chat"


def _owner_token() -> str | None:
    if not current_user.is_authenticated:
        return None
    login_session_id = session.get("login_session_id")
    if not login_session_id:
        login_session_id = str(uuid4())
        session["login_session_id"] = login_session_id
    return f"{current_user.id}:{login_session_id}"


def _proxy_transport() -> httpx.BaseTransport | None:
    return current_app.config.get("VOBCHAT_PROXY_HTTPX_TRANSPORT")


def _proxy_timeout() -> float:
    return float(current_app.config.get("VOBCHAT_PROXY_TIMEOUT_SECONDS", 30.0))


def _api_base_url() -> str:
    return get_settings().api.internal_base_url.rstrip("/")


def _json_error(message: str, status_code: int) -> tuple[Response, int]:
    return jsonify({"detail": message}), status_code


def _build_json_client() -> httpx.Client:
    return httpx.Client(
        base_url=_api_base_url(),
        transport=_proxy_transport(),
        timeout=_proxy_timeout(),
    )


def _forward_json(
    method: str,
    path: str,
    *,
    json_body: dict | None = None,
    params: dict | None = None,
) -> tuple[dict | list | str, int, str]:
    with _build_json_client() as client:
        response = client.request(method, path, json=json_body, params=params)

    content_type = response.headers.get("content-type", "application/json")
    if "application/json" in content_type:
        payload: dict | list | str = response.json()
    else:
        payload = response.text
    return payload, response.status_code, content_type


def _require_owned_thread(thread_id: str) -> str | tuple[Response, int]:
    owner_token = _owner_token()
    if owner_token is None:
        return _json_error("Unauthorized", 401)
    if not _thread_registry.is_owned_by(thread_id, owner_token):
        return _json_error("Forbidden", 403)
    return owner_token


def register_proxy_routes(server) -> None:
    if getattr(server, "_vobchat_proxy_routes_registered", False):
        return

    chat_prefix = _proxy_chat_prefix()

    @server.post(f"{chat_prefix}/threads")
    def proxy_create_thread():
        owner_token = _owner_token()
        if owner_token is None:
            return _json_error("Unauthorized", 401)

        try:
            payload, status_code, content_type = _forward_json("POST", "/chat/threads")
        except httpx.HTTPError as exc:
            logger.warning("Proxy create thread failed: %s", exc)
            return _json_error("Backend chat service is unavailable", 502)
        if status_code in {200, 201} and isinstance(payload, dict):
            thread_id = payload.get("thread_id")
            if thread_id and not _thread_registry.bind(str(thread_id), owner_token):
                return _json_error("Forbidden", 403)

        return Response(
            json.dumps(payload) if "application/json" in content_type else str(payload),
            status=status_code,
            content_type=content_type,
        )

    @server.get(f"{chat_prefix}/threads/<thread_id>")
    def proxy_get_thread(thread_id: str):
        ownership = _require_owned_thread(thread_id)
        if not isinstance(ownership, str):
            return ownership

        try:
            payload, status_code, content_type = _forward_json("GET", f"/chat/threads/{thread_id}")
        except httpx.HTTPError as exc:
            logger.warning("Proxy get thread failed for %s: %s", thread_id, exc)
            return _json_error("Backend chat service is unavailable", 502)
        return Response(
            json.dumps(payload) if "application/json" in content_type else str(payload),
            status=status_code,
            content_type=content_type,
        )

    @server.post(f"{chat_prefix}/turn")
    def proxy_submit_turn():
        body = request.get_json(silent=True) or {}
        thread_id = body.get("thread_id")
        if not thread_id:
            return _json_error("thread_id is required", 400)

        ownership = _require_owned_thread(str(thread_id))
        if not isinstance(ownership, str):
            return ownership

        try:
            payload, status_code, content_type = _forward_json(
                "POST",
                "/chat/turn",
                json_body=body,
            )
        except httpx.HTTPError as exc:
            logger.warning("Proxy submit turn failed for %s: %s", thread_id, exc)
            return _json_error("Backend chat service is unavailable", 502)
        return Response(
            json.dumps(payload) if "application/json" in content_type else str(payload),
            status=status_code,
            content_type=content_type,
        )

    @server.get(f"{chat_prefix}/stream/<thread_id>")
    def proxy_stream_thread(thread_id: str):
        ownership = _require_owned_thread(thread_id)
        if not isinstance(ownership, str):
            return ownership

        client = httpx.Client(
            base_url=_api_base_url(),
            transport=_proxy_transport(),
            timeout=None,
        )
        backend_request = client.build_request(
            "GET",
            f"/chat/stream/{thread_id}",
            headers={"Accept": "text/event-stream"},
        )
        try:
            backend_response = client.send(backend_request, stream=True)
        except httpx.HTTPError as exc:
            client.close()
            logger.warning("Proxy stream connect failed for %s: %s", thread_id, exc)
            return Response(
                json.dumps({"detail": "Backend chat stream is unavailable"}),
                status=502,
                content_type="application/json",
            )

        if backend_response.status_code != 200:
            try:
                body = backend_response.read().decode("utf-8", errors="replace")
            finally:
                backend_response.close()
                client.close()
            return Response(
                body,
                status=backend_response.status_code,
                content_type=backend_response.headers.get("content-type", "text/plain"),
            )

        def generate():
            try:
                for chunk in backend_response.iter_text():
                    if chunk:
                        yield chunk
            except httpx.HTTPError as exc:
                logger.warning("Proxy SSE relay failed for thread %s: %s", thread_id, exc)
                payload = json.dumps(
                    {
                        "event": "error",
                        "thread_id": thread_id,
                        "detail": str(exc),
                    }
                )
                yield f"event: error\ndata: {payload}\n\n"
            finally:
                backend_response.close()
                client.close()

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    logger.info("Registered authenticated same-origin proxy routes under %s", chat_prefix)
    server._vobchat_proxy_routes_registered = True
