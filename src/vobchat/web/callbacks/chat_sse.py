from __future__ import annotations

from typing import Any

import dash_bootstrap_components as dbc
from dash import Input, Output, State, html
from dash.exceptions import PreventUpdate

from vobchat.api.schemas.chat import ChatTurnRequest, ChatTurnResponse
from vobchat.web.clients import APIClient, get_api_client
from vobchat.web.state import (
    apply_chat_turn_response,
    coerce_map_state,
    coerce_metadata_state,
    coerce_request_status,
    coerce_selection_state,
    coerce_thread_state,
    coerce_visualization_state,
    first_selected_unit_type,
    hydrate_states_from_thread,
    hydrate_metadata_state,
    selected_unit_ids,
    status_banner_from_state,
    sync_selection_catalogs,
    thread_label,
)
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_request_status,
    initial_selection_state,
    initial_visualization_state,
)


# Retained as a thin fallback/test helper while the active runtime
# uses browser-side same-origin proxy streaming.
def bootstrap_runtime(api_client: APIClient) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    created = api_client.create_thread()
    thread_state = created.state.model_dump(mode="json")
    return (
        thread_state,
        initial_selection_state(),
        initial_map_state(),
        initial_visualization_state(),
        initial_metadata_state(),
        initial_request_status(),
    )


# Retained as a thin fallback/test helper while the active runtime
# uses browser-side same-origin proxy streaming.
def process_chat_turn(
    api_client: APIClient,
    *,
    thread_state_data: dict[str, Any] | None,
    selection_state_data: dict[str, Any] | None,
    map_state_data: dict[str, Any] | None,
    visualization_state_data: dict[str, Any] | None,
    metadata_state_data: dict[str, Any] | None,
    message: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    cleaned_message = message.strip()
    if not cleaned_message:
        raise ValueError("Message cannot be empty")

    thread_state = coerce_thread_state(thread_state_data)
    selection_state = coerce_selection_state(selection_state_data)
    map_state = coerce_map_state(map_state_data)
    visualization_state = coerce_visualization_state(visualization_state_data)
    metadata_state = coerce_metadata_state(metadata_state_data)

    if not thread_state or not thread_state.get("thread_id"):
        created = api_client.create_thread()
        thread_state = created.state.model_dump(mode="json")

    response = api_client.submit_turn(
        ChatTurnRequest(
            thread_id=str(thread_state["thread_id"]),
            message=cleaned_message,
            stream=False,
        )
    )
    if not isinstance(response, ChatTurnResponse):
        raise RuntimeError("Expected a completed chat turn response")

    selection_state, visualization_state, metadata_state, map_state = apply_chat_turn_response(
        selection_state=selection_state,
        visualization_state=visualization_state,
        metadata_state=metadata_state,
        map_state=map_state,
        response=response,
    )
    selection_state = sync_selection_catalogs(api_client, selection_state)
    metadata_state = hydrate_metadata_state(
        api_client,
        selection_state=selection_state,
        metadata_state=metadata_state,
    )

    request_status = initial_request_status()
    request_status["last_turn_id"] = response.turn_id
    return (
        response.thread_state.model_dump(mode="json"),
        selection_state,
        map_state,
        visualization_state,
        metadata_state,
        request_status,
    )


def _render_message(message: dict[str, Any]) -> html.Div:
    role = message.get("role", "assistant")
    is_user = role == "user"
    bubble_style = {
        "borderRadius": "0.75rem",
        "padding": "0.65rem 0.8rem",
        "marginBottom": "0.75rem",
        "maxWidth": "90%",
        "backgroundColor": "#0d6efd" if is_user else "#f8f9fa",
        "color": "#ffffff" if is_user else "#212529",
        "marginLeft": "auto" if is_user else "0",
        "border": "1px solid #dee2e6" if not is_user else "none",
    }
    return html.Div(
        [
            html.Div(role.capitalize(), className="small text-muted mb-1"),
            html.Div(message.get("content", ""), style=bubble_style),
        ]
    )


def _render_selection_summary(selection_state: dict[str, Any]) -> html.Div:
    badges: list[Any] = []
    details: list[Any] = []
    for place in selection_state.get("selected_places") or []:
        name = place.get("place", {}).get("name")
        if name:
            badges.append(dbc.Badge(name, color="primary", className="me-2"))

    selected_theme = selection_state.get("selected_theme")
    if isinstance(selected_theme, dict) and selected_theme.get("label"):
        badges.append(
            dbc.Badge(
                f"Theme: {selected_theme['label']}",
                color="info",
                className="me-2",
            )
        )
        if selected_theme.get("description"):
            details.append(
                html.Div(
                    selected_theme["description"],
                    className="small text-muted mt-2",
                )
            )

    for cube in selection_state.get("selected_cubes") or []:
        label = cube.get("label")
        if label:
            badges.append(dbc.Badge(label, color="secondary", className="me-2"))

    if not badges:
        return html.Div("No active place or theme selection yet.", className="text-muted small")
    return html.Div(badges + details)


def _render_pending_candidates(selection_state: dict[str, Any]) -> Any:
    candidates = selection_state.get("pending_place_candidates") or []
    if not candidates:
        return html.Div()

    items = []
    for candidate in candidates:
        items.append(
            dbc.ListGroupItem(
                [
                    html.Div(candidate.get("name", "Unnamed place"), className="fw-semibold"),
                    html.Div(
                        ", ".join(candidate.get("unit_types") or []),
                        className="small text-muted",
                    ),
                    dbc.Button(
                        "Choose",
                        color="link",
                        size="sm",
                        className="px-0 mt-2",
                        **{
                            "data-vob-place-candidate": "true",
                            "data-place-id": str(candidate.get("place_id", "")),
                            "data-place-name": candidate.get("name", ""),
                        },
                    ),
                ]
            )
        )

    return html.Div(
        [
            html.Div(
                "Multiple place matches were returned. Refine your query with one of these exact names:",
                className="small text-muted mb-2",
            ),
            dbc.ListGroup(items),
        ]
    )


def _render_status_banner(selection_state: dict[str, Any], request_status: dict[str, Any]) -> list[Any]:
    error, notices = status_banner_from_state(
        selection_state=selection_state,
        request_status=request_status,
    )
    banners: list[Any] = []
    if error:
        banners.append(dbc.Alert(error, color="danger", className="py-2"))
    for notice in notices:
        banners.append(dbc.Alert(notice, color="info", className="py-2"))
    return banners


def register_chat_callbacks(app, api_client: APIClient | None = None) -> None:
    client = api_client or get_api_client()

    @app.callback(
        Output("selection-state-store", "data", allow_duplicate=True),
        Output("visualization-state-store", "data", allow_duplicate=True),
        Output("metadata-state-store", "data", allow_duplicate=True),
        Output("map-state-store", "data", allow_duplicate=True),
        Input("thread-state-store", "data"),
        State("selection-state-store", "data"),
        State("visualization-state-store", "data"),
        State("metadata-state-store", "data"),
        State("map-state-store", "data"),
        prevent_initial_call="initial_duplicate",
    )
    def sync_states_from_thread(
        thread_state_data: dict[str, Any] | None,
        selection_state_data: dict[str, Any] | None,
        visualization_state_data: dict[str, Any] | None,
        metadata_state_data: dict[str, Any] | None,
        map_state_data: dict[str, Any] | None,
    ):
        (
            synced_selection_state,
            synced_visualization_state,
            synced_metadata_state,
            synced_map_state,
        ) = hydrate_states_from_thread(
            thread_state_data=thread_state_data,
            selection_state=coerce_selection_state(selection_state_data),
            visualization_state=coerce_visualization_state(visualization_state_data),
            metadata_state=coerce_metadata_state(metadata_state_data),
            map_state=coerce_map_state(map_state_data),
        )

        if (
            synced_selection_state == coerce_selection_state(selection_state_data)
            and synced_visualization_state == coerce_visualization_state(visualization_state_data)
            and synced_metadata_state == coerce_metadata_state(metadata_state_data)
            and synced_map_state == coerce_map_state(map_state_data)
        ):
            raise PreventUpdate

        return (
            synced_selection_state,
            synced_visualization_state,
            synced_metadata_state,
            synced_map_state,
        )

    @app.callback(
        Output("selection-state-store", "data", allow_duplicate=True),
        Output("map-state-store", "data", allow_duplicate=True),
        Input("selection-state-store", "data"),
        State("map-state-store", "data"),
        prevent_initial_call=True,
    )
    def sync_selection_state(
        selection_state_data: dict[str, Any] | None,
        map_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        map_state = coerce_map_state(map_state_data)

        synced_selection_state = sync_selection_catalogs(client, selection_state)
        synced_map_state = coerce_map_state(map_state)
        synced_map_state["selected_ids"] = selected_unit_ids(synced_selection_state)
        unit_type = first_selected_unit_type(synced_selection_state)
        if unit_type:
            synced_map_state["unit_type"] = unit_type

        if (
            synced_selection_state == selection_state
            and synced_map_state == map_state
        ):
            raise PreventUpdate
        return synced_selection_state, synced_map_state

    @app.callback(
        Output("metadata-state-store", "data", allow_duplicate=True),
        Input("selection-state-store", "data"),
        State("metadata-state-store", "data"),
        prevent_initial_call=True,
    )
    def sync_metadata_state(
        selection_state_data: dict[str, Any] | None,
        metadata_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        metadata_state = coerce_metadata_state(metadata_state_data)
        hydrated_metadata_state = hydrate_metadata_state(
            client,
            selection_state=selection_state,
            metadata_state=metadata_state,
        )
        if hydrated_metadata_state == metadata_state:
            raise PreventUpdate
        return hydrated_metadata_state

    @app.callback(
        Output("chat-thread-label", "children"),
        Output("chat-messages", "children"),
        Output("selection-summary", "children"),
        Output("chat-place-candidates", "children"),
        Output("chat-status-banner", "children"),
        Input("thread-state-store", "data"),
        Input("selection-state-store", "data"),
        Input("request-status-store", "data"),
    )
    def render_chat_surface(
        thread_state_data: dict[str, Any] | None,
        selection_state_data: dict[str, Any] | None,
        request_status_data: dict[str, Any] | None,
    ):
        thread_state = coerce_thread_state(thread_state_data)
        selection_state = coerce_selection_state(selection_state_data)
        request_status = coerce_request_status(request_status_data)

        if not thread_state or not thread_state.get("messages"):
            messages = [
                html.Div(
                    "Start a conversation to resolve places, themes, maps, and charts through the new API path.",
                    className="text-muted",
                )
            ]
        else:
            messages = [_render_message(message) for message in thread_state["messages"]]

        return (
            thread_label(thread_state),
            messages,
            _render_selection_summary(selection_state),
            _render_pending_candidates(selection_state),
            _render_status_banner(selection_state, request_status),
        )
