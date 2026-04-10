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

_OUTPUT_MODE_LABELS = {
    "chart": "Chart",
    "table": "Table",
    "line": "Chart",
    "boundary_map": "Boundary map",
    "thematic_map": "Thematic map",
    "category_chart": "Breakdown chart",
    "category_table": "Breakdown table",
}

_RESULT_TYPE_LABELS = {
    "executed_analysis": "Analysis",
    "catalog_discovery": "Explore",
    "info_lookup": "Reference",
}

_DISCOVERY_TOPIC_LABELS = {
    "catalog_overview": "Explore the available data",
    "themes": "Themes you can use here",
    "reporting_geographies": "Geography levels you can use here",
    "feasible_outputs": "Views you can use",
    "available_years": "Available years",
    "exact_slice_options": "Measures you can choose from",
    "comparable_options": "Comparison options",
}


def _titleize(value: str | None) -> str:
    if not value:
        return ""
    return str(value).replace("_", " ").replace("-", " ").strip().title()


def _friendly_output_label(value: str | None) -> str:
    if not value:
        return ""
    return _OUTPUT_MODE_LABELS.get(str(value), _titleize(str(value)))


def _friendly_result_type_label(selection_state: dict[str, Any]) -> str | None:
    provenance_summary = selection_state.get("provenance_summary") or {}
    result_type = provenance_summary.get("result_type") if isinstance(provenance_summary, dict) else None
    if isinstance(result_type, str):
        return _RESULT_TYPE_LABELS.get(result_type, _titleize(result_type))
    receipt_kind = selection_state.get("current_receipt_kind")
    if isinstance(receipt_kind, str):
        return {
            "analysis": "Analysis",
            "discovery": "Explore",
            "info": "Reference",
        }.get(receipt_kind, _titleize(receipt_kind))
    return None


def _friendly_topic_label(topic: str | None) -> str:
    if not topic:
        return "Explore the available data"
    return _DISCOVERY_TOPIC_LABELS.get(str(topic), _titleize(str(topic)))


def _friendly_notice_text(notice: str | None) -> str | None:
    if not isinstance(notice, str):
        return None
    text = notice.strip()
    if not text:
        return None
    exact_map = {
        "boundary_map_without_measure": (
            "Showing boundaries only because a measured thematic map is not safely available here yet."
        ),
        "I need to stay in discovery until the analysis spec is explicit and safe.": (
            "I found relevant data, but I need one more detail before I can turn it into a result."
        ),
        "That output is not safely supported by the current validated workflow subset.": (
            "That kind of result is not available safely here yet."
        ),
        "I can't run that analysis yet because the request only resolves to a family-level slice.": (
            "I found the broader data family, but not the exact measure needed to run this result yet."
        ),
        "I need a metadata-backed category slice before I can run that breakdown safely.": (
            "I can see that a breakdown exists, but I still need the exact breakdown option before I can run it safely."
        ),
        "Choose a place first so I know what to analyse.": (
            "Choose a place so I can show the right data."
        ),
        "Showing a boundary map only. Thematic maps are not safely supported in v1.": (
            "Showing a boundary map only. A measured thematic map is not safely available here yet."
        ),
        "I used the full available comparable range because you did not specify a time scope.": (
            "I used the full period that can be compared safely because no time range was specified."
        ),
        "Handled in deterministic degraded mode because the chat model is unavailable.": (
            "The chat model is unavailable, so I used a narrower fallback workflow."
        ),
    }
    if text in exact_map:
        return exact_map[text]
    lowered = text.lower()
    if "validated workflow subset" in lowered:
        return "That kind of result is not available safely here yet."
    if "analysis spec" in lowered:
        return "I need one more detail before I can turn this into a result."
    if "exact-slice candidate" in lowered:
        return "I found possible measures, but I still need one exact measure before I can run the result."
    if "discovery-only catalog guidance" in lowered:
        return "This is an exploration result. It shows what is available, not a completed analysis."
    if "deterministic degraded mode" in lowered:
        return "The chat model is unavailable, so I used a narrower fallback workflow."
    return text


def _compact_label_list(labels: list[str], *, limit: int = 4) -> list[str]:
    cleaned = [label for label in labels if label]
    if len(cleaned) <= limit:
        return cleaned
    hidden = len(cleaned) - limit
    return cleaned[:limit] + [f"+{hidden} more"]


def _render_badge_group(
    labels: list[str],
    *,
    color: str = "light",
) -> Any:
    cleaned = [label for label in labels if label]
    if not cleaned:
        return html.Div()
    return html.Div(
        [
            dbc.Badge(label, color=color, className="me-2 mb-2 px-2 py-1 border")
            for label in cleaned
        ],
        className="d-flex flex-wrap",
    )


def _context_badge(label: str, *, color: str = "secondary") -> dbc.Badge:
    return dbc.Badge(label, color=color, className="me-2 mb-2 px-2 py-1")


def _time_scope_label(selection_state: dict[str, Any]) -> str | None:
    time_scope = selection_state.get("time_scope") or {}
    if not isinstance(time_scope, dict):
        return None
    if isinstance(time_scope.get("label"), str) and time_scope.get("label"):
        return str(time_scope["label"])
    year = time_scope.get("year")
    if year is not None:
        return str(year)
    start_year = time_scope.get("start_year")
    end_year = time_scope.get("end_year")
    if start_year is not None and end_year is not None:
        return f"{int(start_year)} to {int(end_year)}"
    return None


def _resolved_context_bits(selection_state: dict[str, Any]) -> list[str]:
    bits: list[str] = []
    reporting_geography = selection_state.get("reporting_geography") or {}
    if isinstance(reporting_geography, dict):
        geography_label = reporting_geography.get("label") or reporting_geography.get("unit_type")
        if geography_label:
            bits.append(f"Geography: {geography_label}")
    time_label = _time_scope_label(selection_state)
    if time_label:
        bits.append(f"Time: {time_label}")
    output_mode = selection_state.get("active_output_mode")
    if isinstance(output_mode, str) and output_mode:
        bits.append(f"View: {_friendly_output_label(output_mode)}")
    result_type = _friendly_result_type_label(selection_state)
    if result_type:
        bits.append(f"Result: {result_type}")
    return bits


def _provenance_summary_text(selection_state: dict[str, Any]) -> str | None:
    provenance_summary = selection_state.get("provenance_summary") or {}
    if not isinstance(provenance_summary, dict):
        return None
    result_type = provenance_summary.get("result_type")
    resolved_context = provenance_summary.get("resolved_context") or {}
    runtime_mode = provenance_summary.get("runtime_mode")
    places = ", ".join(resolved_context.get("place_labels") or [])
    geography = resolved_context.get("reporting_geography_label")
    slice_label = resolved_context.get("exact_slice_label") or resolved_context.get("dataset_family_label")
    output_mode = _friendly_output_label(resolved_context.get("output_mode"))

    if result_type == "catalog_discovery":
        topic = (selection_state.get("discovery_result") or {}).get("title")
        summary = (
            f"{topic}." if isinstance(topic, str) and topic else "This is an exploration result."
        )
        if not summary.endswith("."):
            summary = f"{summary}."
        summary = f"{summary} It shows what is available from the current context, not a completed analysis."
    elif result_type == "info_lookup":
        target = places or slice_label or geography or "the selected place or data item"
        summary = f"This is background information about {target}, not a statistical result."
    elif result_type == "executed_analysis":
        result_label = output_mode or "Result"
        subject = places or "the current selection"
        detail_bits = [bit for bit in [geography, slice_label] if bit]
        if detail_bits:
            summary = f"{result_label} result for {subject} using " + " and ".join(detail_bits) + "."
        else:
            summary = f"{result_label} result for {subject}."
    else:
        summary = provenance_summary.get("summary")

    summary = _friendly_notice_text(summary) or summary
    if runtime_mode == "deterministic_degraded":
        runtime_state = selection_state.get("runtime_state") or {}
        runtime_notice = (
            runtime_state.get("notice") if isinstance(runtime_state, dict) else None
        )
        friendly_runtime = _friendly_notice_text(runtime_notice) or runtime_notice
        if isinstance(friendly_runtime, str) and friendly_runtime and friendly_runtime not in str(summary):
            return f"{summary} {friendly_runtime}"
    return summary


def _runtime_state_has_user_facing_content(runtime_state: dict[str, Any]) -> bool:
    if not isinstance(runtime_state, dict) or not runtime_state:
        return False
    notice = runtime_state.get("notice")
    setup_guidance = runtime_state.get("setup_guidance") or []
    mode = runtime_state.get("mode")
    return bool(notice or setup_guidance or (mode and str(mode) != "llm_assisted"))


def _render_trust_panel(selection_state: dict[str, Any]) -> Any:
    provenance_summary = selection_state.get("provenance_summary") or {}
    runtime_state = selection_state.get("runtime_state") or {}
    if not isinstance(provenance_summary, dict) and not isinstance(runtime_state, dict):
        return html.Div()
    if not provenance_summary and not _runtime_state_has_user_facing_content(runtime_state):
        return html.Div()

    badges: list[Any] = []
    result_type = _friendly_result_type_label(selection_state)
    if result_type:
        badges.append(_context_badge(result_type, color="dark"))
    output_mode = selection_state.get("active_output_mode")
    if isinstance(output_mode, str) and output_mode:
        badges.append(_context_badge(_friendly_output_label(output_mode), color="secondary"))
    if isinstance(runtime_state, dict) and runtime_state.get("mode") == "deterministic_degraded":
        badges.append(_context_badge("Fallback mode", color="warning"))

    resolved_context = _resolved_context_bits(selection_state)
    defaults_used = [
        _friendly_notice_text(item) or item
        for item in (provenance_summary.get("defaults_used") or [])
        if item
    ]
    safe_downgrades = [
        _friendly_notice_text(item) or item
        for item in (provenance_summary.get("safe_downgrades") or [])
        if item
    ]
    limits = [
        _friendly_notice_text(item) or item
        for item in (
            list(provenance_summary.get("availability_limits") or [])
            + list(provenance_summary.get("comparability_limits") or [])
        )
        if item
    ]
    guidance = [
        str(item)
        for item in (runtime_state.get("setup_guidance") or [])
        if isinstance(runtime_state, dict) and item
    ]
    highlights = _compact_label_list(
        list(dict.fromkeys(defaults_used + safe_downgrades + limits)),
        limit=3,
    )
    summary_text = _provenance_summary_text(selection_state)

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    className="d-flex flex-wrap justify-content-between align-items-start gap-2 mb-2",
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    "How to read this result",
                                    className="small text-uppercase text-muted fw-semibold",
                                ),
                                html.Div(
                                    summary_text or "The current result is backed by the latest receipt.",
                                    className="mb-0",
                                ),
                            ]
                        ),
                        html.Div(badges, className="d-flex flex-wrap justify-content-end"),
                    ],
                ),
                html.Div(
                    " · ".join(resolved_context),
                    className="small text-muted mb-2",
                )
                if resolved_context
                else None,
                html.Ul(
                    [html.Li(item) for item in highlights],
                    className="small mb-2 ps-3",
                )
                if highlights
                else None,
                html.Div(
                    [
                        html.Div("Need full chat mode?", className="small fw-semibold mb-1"),
                        html.Ul([html.Li(item) for item in guidance], className="small mb-0 ps-3"),
                    ],
                    className="small mt-2",
                )
                if guidance
                else None,
            ]
        ),
        className="border-0 shadow-sm",
        style={"backgroundColor": "#f7f5ee"},
    )


def _render_discovery_panel(selection_state: dict[str, Any]) -> Any:
    discovery_result = selection_state.get("discovery_result") or {}
    if not isinstance(discovery_result, dict) or not discovery_result:
        return html.Div()

    items = [item.get("label") for item in (discovery_result.get("items") or []) if item.get("label")]
    reporting_geographies = [
        item.get("label") or item.get("unit_type")
        for item in (discovery_result.get("reporting_geographies") or [])
        if item
    ]
    dataset_families = [
        item.get("label") or item.get("theme_id")
        for item in (discovery_result.get("dataset_families") or [])
        if item
    ]
    exact_slice_options = [
        item.get("label") or item.get("cube_id")
        for item in (discovery_result.get("exact_slice_options") or [])
        if item
    ]
    available_years = [
        item.get("label")
        for item in (discovery_result.get("available_years") or [])
        if item.get("label")
    ]
    supported_outputs = []
    for item in discovery_result.get("supported_outputs") or []:
        label = item.get("label") or _friendly_output_label(item.get("output_mode"))
        support_level = item.get("support_level")
        if support_level == "discovery_only":
            label = f"{label} (explore only)"
        elif support_level == "guarded":
            label = f"{label} (can run when the rest is clear)"
        supported_outputs.append(label)
    suggested_actions = [
        item.get("label")
        for item in (discovery_result.get("suggested_actions") or [])
        if item.get("label")
    ]
    summary_text = discovery_result.get("summary") or "I found options you can use next."
    summary_text = _friendly_notice_text(summary_text) or str(summary_text)

    sections: list[Any] = []
    section_specs = [
        ("What I found", _compact_label_list(items or dataset_families or exact_slice_options)),
        ("Geography levels", _compact_label_list(reporting_geographies)),
        ("Possible views", _compact_label_list(supported_outputs)),
        ("Available years", _compact_label_list(available_years)),
    ]
    for title, labels in section_specs:
        if labels:
            sections.append(
                html.Div(
                    [
                        html.Div(title, className="small text-uppercase text-muted fw-semibold mb-1"),
                        _render_badge_group(labels),
                    ],
                    className="mb-2",
                )
            )

    card_children: list[Any] = [
        html.Div(
            className="d-flex flex-wrap justify-content-between align-items-start gap-2 mb-2",
            children=[
                html.Div(
                    [
                        html.Div(
                            "Explore the data",
                            className="small text-uppercase text-muted fw-semibold",
                        ),
                        html.H5(
                            discovery_result.get("title")
                            or _friendly_topic_label(discovery_result.get("topic")),
                            className="mb-1",
                        ),
                        html.Div(summary_text, className="mb-0"),
                    ]
                ),
                dbc.Badge("Exploration only", color="info", className="px-2 py-1"),
            ],
        )
    ]
    card_children.extend(sections)
    if suggested_actions:
        card_children.append(
            html.Div(
                [
                    html.Div("Good next steps", className="small text-uppercase text-muted fw-semibold mb-1"),
                    html.Ul([html.Li(item) for item in _compact_label_list(suggested_actions, limit=5)], className="small mb-0 ps-3"),
                ],
                className="mt-2",
            )
        )

    return dbc.Card(
        dbc.CardBody(card_children),
        className="border-0 shadow-sm",
        style={"backgroundColor": "#eef6fb"},
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
    details = _resolved_context_bits(selection_state)
    for place in selection_state.get("selected_places") or []:
        name = place.get("place", {}).get("name")
        if name:
            badges.append(_context_badge(name, color="primary"))

    selected_theme = selection_state.get("selected_theme")
    if isinstance(selected_theme, dict) and selected_theme.get("label"):
        badges.append(_context_badge(f"Theme: {selected_theme['label']}", color="info"))

    for cube in selection_state.get("selected_cubes") or []:
        label = cube.get("label")
        if label:
            badges.append(_context_badge(label, color="secondary"))

    if details:
        for item in details:
            badges.append(_context_badge(item, color="light"))

    if not badges:
        return html.Div(
            [
                html.Div("Current context", className="small text-uppercase text-muted fw-semibold mb-1"),
                html.Div(
                    "No active place or data selection yet.",
                    className="text-muted small",
                ),
            ]
        )
    return html.Div(
        [
            html.Div("Current context", className="small text-uppercase text-muted fw-semibold mb-1"),
            html.Div(badges, className="d-flex flex-wrap"),
        ]
    )


def _render_pending_candidates(selection_state: dict[str, Any]) -> Any:
    clarification = selection_state.get("pending_clarification") or {}
    candidates = selection_state.get("pending_place_candidates") or []
    if not clarification and not candidates:
        return html.Div()

    option_items: list[Any] = []
    question = None
    helper_text = None

    if isinstance(clarification, dict) and clarification:
        options = clarification.get("options") or []
        slot = clarification.get("slot")
        question = clarification.get("question")
        if slot == "reporting_geography":
            question = "Choose the geography level you want to work with."
            helper_text = "Once you pick a geography, I can keep going from the same place and data context."
        elif slot == "time_scope":
            question = "Choose the time period you want to use."
            helper_text = "This keeps the rest of your current context unchanged."
        elif slot == "output_mode":
            question = "Choose the result view you want."
            helper_text = "I will keep the same result and switch to the matching view when that is safe."
        elif options:
            question = "I need one more choice before I can continue."
            helper_text = "Reply with one of these options to keep moving."

        for option in options:
            label = option.get("label") or option.get("option_id") or "Option"
            kind = option.get("kind")
            metadata = option.get("metadata") or {}
            detail_bits = []
            if metadata.get("unit_type"):
                detail_bits.append(str(metadata["unit_type"]))
            if metadata.get("description"):
                detail_bits.append(str(metadata["description"]))
            if metadata.get("theme_id"):
                detail_bits.append(str(metadata["theme_id"]))
            if kind == "place_candidate" and metadata.get("place_id"):
                option_items.append(
                    dbc.ListGroupItem(
                        [
                            html.Div(label, className="fw-semibold"),
                            html.Div(" · ".join(detail_bits), className="small text-muted")
                            if detail_bits
                            else None,
                            dbc.Button(
                                "Choose",
                                color="link",
                                size="sm",
                                className="px-0 mt-2",
                                **{
                                    "data-vob-place-candidate": "true",
                                    "data-place-id": str(metadata.get("place_id", "")),
                                    "data-place-name": label,
                                },
                            ),
                        ]
                    )
                )
            else:
                option_items.append(
                    dbc.ListGroupItem(
                        [
                            html.Div(label, className="fw-semibold"),
                            html.Div(" · ".join(detail_bits), className="small text-muted")
                            if detail_bits
                            else None,
                            html.Div(
                                "Reply with this option label to continue.",
                                className="small text-muted mt-2",
                            ),
                        ]
                    )
                )

    if not option_items and candidates:
        question = "I found more than one place that could match your request."
        helper_text = "Choose the place you want so I can keep the rest of the question moving."
        for candidate in candidates:
            option_items.append(
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

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div("I need one choice from you", className="small text-uppercase text-muted fw-semibold"),
                html.Div(_friendly_notice_text(question) or question or "Choose one option to continue.", className="mb-1"),
                html.Div(helper_text, className="small text-muted mb-3") if helper_text else None,
                dbc.ListGroup(option_items),
            ]
        ),
        className="border-0 shadow-sm",
        style={"backgroundColor": "#fbf7ef"},
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
        friendly_notice = _friendly_notice_text(notice)
        if not friendly_notice:
            continue
        banners.append(dbc.Alert(friendly_notice, color="secondary", className="py-2 mb-2"))
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
        reporting_geography = synced_selection_state.get("reporting_geography") or {}
        unit_type = reporting_geography.get("unit_type")
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
        Output("chat-trust-panel", "children"),
        Output("chat-discovery-panel", "children"),
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
                    "Ask a place-based question or explore what data is available for a place.",
                    className="text-muted",
                )
            ]
        else:
            messages = [_render_message(message) for message in thread_state["messages"]]

        return (
            thread_label(thread_state),
            messages,
            _render_selection_summary(selection_state),
            _render_trust_panel(selection_state),
            _render_discovery_panel(selection_state),
            _render_pending_candidates(selection_state),
            _render_status_banner(selection_state, request_status),
        )
