from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from vobchat.api.schemas.chat import (
    AssistantDeltaEventData,
    ChatMessage,
    ChatOperation,
    ChatSSEEventName,
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatUIStateDelta,
    ReportingGeographyRef,
    SSEEventPayload,
    UIProjection,
)
from vobchat.api.schemas.metadata import PlaceProfileResponse
from vobchat.api.schemas.places import PlaceCandidateResponse, ResolvedPlaceResponse, UnitDetailResponse
from vobchat.api.schemas.series import TimeSeriesResponse, TimeSeriesRowResponse
from vobchat.api.schemas.themes import CubeSummaryResponse, ThemeSummaryResponse
from vobchat.api.streaming.sse import encode_sse_event
from vobchat.auth.extensions import db
from vobchat.auth.models import User
from vobchat.core.settings import reset_settings_cache
from vobchat.web.proxy import reset_proxy_thread_registry
from vobchat.web.state import (
    apply_stream_assistant_delta,
    apply_stream_ui_delta,
    append_stream_user_message,
)
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_thread_state(thread_id: str = "thread-1") -> ChatThreadState:
    now = utc_now()
    return ChatThreadState(thread_id=thread_id, created_at=now, updated_at=now)


def make_created_thread(thread_id: str = "thread-1") -> ChatThreadCreateResponse:
    state = make_thread_state(thread_id)
    return ChatThreadCreateResponse(thread_id=thread_id, state=state)


def make_resolved_place(
    *,
    place_id: int = 1,
    place_name: str = "York",
    unit_id: int = 101,
    unit_type: str = "MOD_DIST",
) -> ResolvedPlaceResponse:
    return ResolvedPlaceResponse(
        place=PlaceCandidateResponse(
            place_id=place_id,
            name=place_name,
            unit_ids=[unit_id],
            unit_types=[unit_type],
            match_type="resolved",
        ),
        units=[
            UnitDetailResponse(
                unit_id=unit_id,
                unit_name=place_name,
                unit_type=unit_type,
                unit_type_label="Modern District",
            )
        ],
    )


def build_web_app(monkeypatch: pytest.MonkeyPatch, tmp_path):
    auth_db_path = tmp_path / "users.db"
    monkeypatch.setenv("AUTH_DATABASE_URL", f"sqlite:///{auth_db_path}")
    monkeypatch.setenv("SECRET_KEY", "phase6-test-secret")
    monkeypatch.setenv("SESSION_COOKIE_SECURE", "false")
    monkeypatch.setenv("WTF_CSRF_ENABLED", "false")
    monkeypatch.setenv("VOBCHAT_SKIP_WORKFLOW_STARTUP", "true")
    reset_settings_cache()

    monkeypatch.setattr(
        User,
        "verify_password",
        lambda self, raw_password: self.password_hash == raw_password,
    )

    from vobchat.web.app import create_app

    app = create_app()
    server = app.server
    return app, server


def create_user(server, *, email: str, password: str) -> None:
    with server.app_context():
        existing = db.session.scalar(db.select(User).filter_by(email=email))
        if existing is None:
            db.session.add(User(email=email, password_hash=password))
            db.session.commit()


def login(client, *, email: str, password: str):
    return client.post(
        "/login",
        data={"email": email, "password": password},
        follow_redirects=False,
    )


@pytest.fixture(autouse=True)
def reset_phase6_state():
    reset_proxy_thread_registry()
    yield
    reset_proxy_thread_registry()


def test_proxy_routes_require_authenticated_session(monkeypatch, tmp_path) -> None:
    _, server = build_web_app(monkeypatch, tmp_path)
    client = server.test_client()

    response = client.post("/proxy/chat/threads", follow_redirects=False)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_proxy_thread_routes_bind_thread_to_authenticated_session(monkeypatch, tmp_path) -> None:
    _, server = build_web_app(monkeypatch, tmp_path)
    create_user(server, email="alice@example.com", password="secret123")
    create_user(server, email="bob@example.com", password="secret123")

    created = make_created_thread("thread-owned")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/chat/threads":
            return httpx.Response(201, json=created.model_dump(mode="json"))
        if request.method == "GET" and request.url.path == "/chat/threads/thread-owned":
            return httpx.Response(200, json=created.state.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url.path}")

    server.config["VOBCHAT_PROXY_HTTPX_TRANSPORT"] = httpx.MockTransport(handler)

    alice = server.test_client()
    bob = server.test_client()
    assert login(alice, email="alice@example.com", password="secret123").status_code == 302
    assert login(bob, email="bob@example.com", password="secret123").status_code == 302

    created_response = alice.post("/proxy/chat/threads")
    assert created_response.status_code == 201
    assert created_response.get_json()["thread_id"] == "thread-owned"

    owned_thread = alice.get("/proxy/chat/threads/thread-owned")
    assert owned_thread.status_code == 200
    assert owned_thread.get_json()["thread_id"] == "thread-owned"

    forbidden = bob.get("/proxy/chat/threads/thread-owned")
    assert forbidden.status_code == 403
    assert forbidden.get_json()["detail"] == "Forbidden"


def test_proxy_turn_submission_and_sse_relay(monkeypatch, tmp_path) -> None:
    _, server = build_web_app(monkeypatch, tmp_path)
    create_user(server, email="alice@example.com", password="secret123")

    created = make_created_thread("thread-stream")
    delta_payload = SSEEventPayload(
        event=ChatSSEEventName.ASSISTANT_DELTA,
        thread_id="thread-stream",
        sequence=1,
        timestamp=utc_now(),
        data=AssistantDeltaEventData(
            turn_id="turn-1",
            delta="Hello",
            accumulated_text="Hello",
        ),
    )
    sse_body = encode_sse_event(delta_payload)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/chat/threads":
            return httpx.Response(201, json=created.model_dump(mode="json"))
        if request.method == "POST" and request.url.path == "/chat/turn":
            body = request.content.decode("utf-8")
            assert '"stream":true' in body
            return httpx.Response(
                200,
                json={
                    "status": "accepted",
                    "thread_id": "thread-stream",
                    "turn_id": "turn-1",
                },
            )
        if request.method == "GET" and request.url.path == "/chat/stream/thread-stream":
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text=sse_body,
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url.path}")

    server.config["VOBCHAT_PROXY_HTTPX_TRANSPORT"] = httpx.MockTransport(handler)

    client = server.test_client()
    assert login(client, email="alice@example.com", password="secret123").status_code == 302
    assert client.post("/proxy/chat/threads").status_code == 201

    turn_response = client.post(
        "/proxy/chat/turn",
        json={
            "thread_id": "thread-stream",
            "message": "Show me York",
            "stream": True,
        },
    )
    assert turn_response.status_code == 200
    assert turn_response.get_json()["status"] == "accepted"

    stream_response = client.open(
        "/proxy/chat/stream/thread-stream",
        method="GET",
        buffered=False,
    )
    stream_body = "".join(
        chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
        for chunk in stream_response.response
    )

    assert stream_response.status_code == 200
    assert stream_response.mimetype == "text/event-stream"
    assert "event: assistant.delta" in stream_body
    assert '"thread_id": "thread-stream"' in stream_body


def test_web_layout_exposes_phase6_proxy_runtime_config(monkeypatch, tmp_path) -> None:
    app, _ = build_web_app(monkeypatch, tmp_path)
    props_by_id: dict[str, dict[str, Any]] = {}

    for child in app.layout.children:
        if getattr(child, "id", None):
            props_by_id[child.id] = child.to_plotly_json()["props"]

    assert "web-runtime-config" in props_by_id
    assert props_by_id["web-runtime-config"]["data-proxy-chat-prefix"] == "/proxy/chat"
    assert "thread-bootstrap" not in props_by_id


def test_streamed_helpers_update_thread_and_structured_state() -> None:
    now = utc_now()
    thread_state = make_thread_state().model_dump(mode="json")
    user_message = ChatMessage(
        message_id="user-1",
        role="user",
        content="Show York population",
        created_at=now,
    ).model_dump(mode="json")

    threaded = append_stream_user_message(thread_state, user_message=user_message)
    threaded = apply_stream_assistant_delta(
        threaded,
        turn_id="turn-1",
        accumulated_text="Loading York population.",
    )

    selection_state, visualization_state, metadata_state, map_state = apply_stream_ui_delta(
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
        delta=ChatUIStateDelta(
            operation=ChatOperation.FETCH_TIME_SERIES,
            selected_places=[make_resolved_place()],
            selected_theme=ThemeSummaryResponse(theme_id="T_POP", label="Population"),
            selected_cubes=[
                CubeSummaryResponse(
                    theme_id="T_POP",
                    cube_id="N_POP_TOTAL",
                    label="Total population",
                    start_year=1801,
                    end_year=1901,
                    observation_count=5,
                    has_categories=False,
                )
            ],
            time_series=TimeSeriesResponse(
                unit_ids=[101],
                cube_ids=["N_POP_TOTAL"],
                row_count=1,
                rows=[
                    TimeSeriesRowResponse(
                        year=1901,
                        unit_id=101,
                        unit_name="York",
                        unit_type="MOD_DIST",
                        cube_id="N_POP_TOTAL",
                        cube_label="Total population",
                        value=42.0,
                    )
                ],
            ),
            place_profile=PlaceProfileResponse(
                place_id=1,
                name="York",
                text="Historic city",
            ),
            ui_projection=UIProjection(
                projection_id="ui-stream-1",
                selected_places=[make_resolved_place()],
                reporting_geography=ReportingGeographyRef(
                    unit_type="MOD_DIST",
                    unit_ids=[101],
                    label="Modern District",
                ),
                dataset_family=ThemeSummaryResponse(theme_id="T_POP", label="Population"),
                selected_cubes=[
                    CubeSummaryResponse(
                        theme_id="T_POP",
                        cube_id="N_POP_TOTAL",
                        label="Total population",
                        start_year=1801,
                        end_year=1901,
                        observation_count=5,
                        has_categories=False,
                    )
                ],
                active_output_mode="chart",
                notices=["Loaded York population."],
            ),
            notices=["Loaded York population."],
        ),
    )

    assert threaded["messages"][0]["role"] == "user"
    assert threaded["messages"][-1]["message_id"] == "draft-turn-1"
    assert threaded["messages"][-1]["content"] == "Loading York population."
    assert selection_state["selected_places"][0]["place"]["name"] == "York"
    assert selection_state["selected_theme"]["theme_id"] == "T_POP"
    assert selection_state["selected_cubes"][0]["cube_id"] == "N_POP_TOTAL"
    assert selection_state["notices"] == ["Loaded York population."]
    assert map_state["selected_ids"] == [101]
    assert map_state["unit_type"] == "MOD_DIST"
    assert visualization_state["active_tab"] == "line"
    assert visualization_state["time_series"]["row_count"] == 1
    assert metadata_state["place_profile"]["name"] == "York"
