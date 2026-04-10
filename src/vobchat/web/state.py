from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from vobchat.api.schemas.chat import (
    ChatMessage,
    ChatThreadState,
    ChatTurnResponse,
    ChatUIStateDelta,
)
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.schemas.themes import CubeSummaryResponse, ThemeSummaryResponse
from vobchat.web.clients import APIClient, APIClientError
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_request_status,
    initial_selection_state,
    initial_thread_state,
    initial_visualization_state,
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_feature_collection() -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def coerce_thread_state(data: dict[str, Any] | None) -> dict[str, Any] | None:
    if not data:
        return initial_thread_state()
    return deepcopy(data)


def coerce_selection_state(data: dict[str, Any] | None) -> dict[str, Any]:
    state = initial_selection_state()
    if data:
        state.update(deepcopy(data))
    return state


def coerce_map_state(data: dict[str, Any] | None) -> dict[str, Any]:
    state = initial_map_state()
    if data:
        state.update(deepcopy(data))
    return state


def coerce_visualization_state(data: dict[str, Any] | None) -> dict[str, Any]:
    state = initial_visualization_state()
    if data:
        state.update(deepcopy(data))
    return state


def coerce_metadata_state(data: dict[str, Any] | None) -> dict[str, Any]:
    state = initial_metadata_state()
    if data:
        state.update(deepcopy(data))
    return state


def coerce_request_status(data: dict[str, Any] | None) -> dict[str, Any]:
    state = initial_request_status()
    if data:
        state.update(deepcopy(data))
    return state


def _selected_places(selection_state: dict[str, Any]) -> list[dict[str, Any]]:
    return list(selection_state.get("selected_places") or [])


def _authoritative_ui_projection(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
):
    if delta is not None:
        if delta.ui_projection is not None:
            return delta.ui_projection
        if delta.current_receipt is not None and delta.current_receipt.ui_projection is not None:
            return delta.current_receipt.ui_projection
    if thread_state is not None:
        if thread_state.ui_projection is not None:
            return thread_state.ui_projection
        if thread_state.current_receipt is not None and thread_state.current_receipt.ui_projection is not None:
            return thread_state.current_receipt.ui_projection
    return None


def _authoritative_render_projection(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
):
    if delta is not None:
        if delta.render_projection is not None:
            return delta.render_projection
        if delta.current_receipt is not None and delta.current_receipt.render_projection is not None:
            return delta.current_receipt.render_projection
    if thread_state is not None:
        if thread_state.render_projection is not None:
            return thread_state.render_projection
        if thread_state.current_receipt is not None and thread_state.current_receipt.render_projection is not None:
            return thread_state.current_receipt.render_projection
    return None


def _projected_selected_places(
    *,
    selected_places: list[Any],
    ui_projection: Any | None,
) -> list[Any]:
    if ui_projection is not None and ui_projection.selected_places:
        return list(ui_projection.selected_places)
    if selected_places:
        return list(selected_places)
    return []


def _projected_selected_theme(
    *,
    selected_theme: ThemeSummaryResponse | None,
    ui_projection: Any | None,
) -> ThemeSummaryResponse | None:
    if ui_projection is not None and ui_projection.dataset_family is not None:
        return ui_projection.dataset_family
    if selected_theme is not None:
        return selected_theme
    return None


def _projected_selected_cubes(
    *,
    selected_cubes: list[CubeSummaryResponse],
    ui_projection: Any | None,
) -> list[CubeSummaryResponse]:
    if ui_projection is not None and ui_projection.selected_cubes:
        return list(ui_projection.selected_cubes)
    if selected_cubes:
        return list(selected_cubes)
    return []


def _projected_unit_type(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> str | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.reporting_geography is not None:
        return projection.reporting_geography.unit_type
    if delta is not None:
        if delta.analysis_state is not None and delta.analysis_state.reporting_geography is not None:
            return delta.analysis_state.reporting_geography.unit_type
    if thread_state is not None:
        if (
            thread_state.analysis_state is not None
            and thread_state.analysis_state.reporting_geography is not None
        ):
            return thread_state.analysis_state.reporting_geography.unit_type
    return None


def _projected_reporting_geography(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.reporting_geography is not None:
        return projection.reporting_geography.model_dump(mode="json")
    if delta is not None:
        if delta.analysis_state is not None and delta.analysis_state.reporting_geography is not None:
            return delta.analysis_state.reporting_geography.model_dump(mode="json")
    if thread_state is not None:
        if (
            thread_state.analysis_state is not None
            and thread_state.analysis_state.reporting_geography is not None
        ):
            return thread_state.analysis_state.reporting_geography.model_dump(mode="json")
    return None


def _projected_pending_clarification(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.clarification is not None:
        return projection.clarification.model_dump(mode="json")
    if delta is not None:
        if delta.pending_clarification is not None:
            return delta.pending_clarification.model_dump(mode="json")
        if (
            delta.conversation_state is not None
            and delta.conversation_state.pending_clarification is not None
        ):
            return delta.conversation_state.pending_clarification.model_dump(mode="json")
    if thread_state is not None:
        if thread_state.pending_clarification is not None:
            return thread_state.pending_clarification.model_dump(mode="json")
        if thread_state.conversation_state.pending_clarification is not None:
            return thread_state.conversation_state.pending_clarification.model_dump(mode="json")
    return None


def _projected_discovery_result(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.discovery_result is not None:
        return projection.discovery_result.model_dump(mode="json")
    if delta is not None:
        if delta.discovery_result is not None:
            return delta.discovery_result.model_dump(mode="json")
        if delta.current_receipt is not None and delta.current_receipt.discovery_result is not None:
            return delta.current_receipt.discovery_result.model_dump(mode="json")
    if thread_state is not None:
        if thread_state.latest_discovery_result is not None:
            return thread_state.latest_discovery_result.model_dump(mode="json")
        if (
            thread_state.current_receipt is not None
            and thread_state.current_receipt.discovery_result is not None
        ):
            return thread_state.current_receipt.discovery_result.model_dump(mode="json")
    return None


def _projected_available_themes(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> list[dict[str, Any]]:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is None:
        return []
    return [item.model_dump(mode="json") for item in projection.available_themes]


def _projected_available_cubes(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> list[dict[str, Any]]:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is None:
        return []
    return [item.model_dump(mode="json") for item in projection.available_cubes]


def _projected_analysis_spec(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.analysis_spec is not None:
        return projection.analysis_spec.model_dump(mode="json")
    return None


def _projected_exact_slice(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.exact_slice is not None:
        return projection.exact_slice.model_dump(mode="json")
    return None


def _projected_time_scope(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any] | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.time_scope is not None:
        return projection.time_scope.model_dump(mode="json")
    return None


def _projected_output_mode(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> str | None:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is not None and projection.active_output_mode is not None:
        return projection.active_output_mode
    render_projection = _authoritative_render_projection(thread_state=thread_state, delta=delta)
    if render_projection is not None and render_projection.active_output_mode is not None:
        return render_projection.active_output_mode
    return None


def _projected_defaults_used(
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> list[str]:
    projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    if projection is None:
        return []
    return list(projection.defaults_used or [])


def selected_unit_ids(selection_state: dict[str, Any]) -> list[int]:
    reporting_geography = selection_state.get("reporting_geography")
    if isinstance(reporting_geography, dict):
        return [int(item) for item in (reporting_geography.get("unit_ids") or [])]
    return []


def selected_unit_types(selection_state: dict[str, Any]) -> list[str]:
    reporting_geography = selection_state.get("reporting_geography")
    if isinstance(reporting_geography, dict):
        projected_unit_type = reporting_geography.get("unit_type")
        if isinstance(projected_unit_type, str) and projected_unit_type:
            return [projected_unit_type]
    return []


def first_selected_place(selection_state: dict[str, Any]) -> dict[str, Any] | None:
    places = _selected_places(selection_state)
    return places[0] if places else None


def selected_theme_id(selection_state: dict[str, Any]) -> str | None:
    selected_theme = selection_state.get("selected_theme")
    if not isinstance(selected_theme, dict):
        return None
    theme_id = selected_theme.get("theme_id")
    return str(theme_id) if theme_id else None


def selected_cube_ids(selection_state: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for cube in selection_state.get("selected_cubes") or []:
        cube_id = cube.get("cube_id")
        if isinstance(cube_id, str):
            out.append(cube_id)
    return out


def theme_summary_from_store(selection_state: dict[str, Any]) -> ThemeSummaryResponse | None:
    selected_theme = selection_state.get("selected_theme")
    if not selected_theme:
        return None
    return ThemeSummaryResponse.model_validate(selected_theme)


def cube_summaries_from_store(selection_state: dict[str, Any]) -> list[CubeSummaryResponse]:
    return [
        CubeSummaryResponse.model_validate(item)
        for item in (selection_state.get("selected_cubes") or [])
    ]


def append_local_assistant_message(
    thread_state: dict[str, Any] | None,
    *,
    content: str,
) -> dict[str, Any] | None:
    if not thread_state:
        return thread_state
    updated = deepcopy(thread_state)
    updated.setdefault("messages", [])
    updated["messages"].append(
        ChatMessage(
            message_id=str(uuid4()),
            role="assistant",
            content=content,
            created_at=datetime.now(timezone.utc),
        ).model_dump(mode="json")
    )
    updated["updated_at"] = now_iso()
    return updated


def append_stream_user_message(
    thread_state: dict[str, Any] | None,
    *,
    user_message: dict[str, Any],
) -> dict[str, Any]:
    updated = coerce_thread_state(thread_state) or {}
    updated.setdefault("messages", [])
    updated["messages"] = list(updated["messages"]) + [deepcopy(user_message)]
    updated["updated_at"] = now_iso()
    return updated


def apply_stream_assistant_delta(
    thread_state: dict[str, Any] | None,
    *,
    turn_id: str,
    accumulated_text: str,
) -> dict[str, Any]:
    updated = coerce_thread_state(thread_state) or {}
    messages = list(updated.get("messages") or [])
    draft_message_id = f"draft-{turn_id}"
    draft_message = {
        "message_id": draft_message_id,
        "role": "assistant",
        "content": accumulated_text,
        "created_at": now_iso(),
    }

    if messages and messages[-1].get("message_id") == draft_message_id:
        messages[-1] = draft_message
    else:
        messages.append(draft_message)

    updated["messages"] = messages
    updated["updated_at"] = now_iso()
    return updated


def _selection_matches_projection(selection_state: dict[str, Any]) -> bool:
    projection = selection_state.get("ui_projection")
    if not isinstance(projection, dict):
        return False
    projected_place_ids = [
        item.get("place", {}).get("place_id") for item in (projection.get("selected_places") or [])
    ]
    selected_place_ids = [
        item.get("place", {}).get("place_id") for item in (selection_state.get("selected_places") or [])
    ]
    projected_cube_ids = [item.get("cube_id") for item in (projection.get("selected_cubes") or [])]
    selected_cube_id_values = [item.get("cube_id") for item in (selection_state.get("selected_cubes") or [])]
    projected_theme_id = (projection.get("dataset_family") or {}).get("theme_id")
    selected_theme_id_value = (selection_state.get("selected_theme") or {}).get("theme_id")
    projected_reporting_geography = projection.get("reporting_geography") or {}
    selected_reporting_geography = selection_state.get("reporting_geography") or {}
    return (
        projected_place_ids == selected_place_ids
        and projected_cube_ids == selected_cube_id_values
        and projected_theme_id == selected_theme_id_value
        and projected_reporting_geography.get("unit_type")
        == selected_reporting_geography.get("unit_type")
        and list(projected_reporting_geography.get("unit_ids") or [])
        == list(selected_reporting_geography.get("unit_ids") or [])
    )


def _selection_matches_ui_projection(
    selection_state: dict[str, Any],
    projection: dict[str, Any] | None,
) -> bool:
    if not isinstance(projection, dict):
        return False
    projected_place_ids = [
        item.get("place", {}).get("place_id")
        for item in (projection.get("selected_places") or [])
    ]
    selected_place_ids = [
        item.get("place", {}).get("place_id")
        for item in (selection_state.get("selected_places") or [])
    ]
    projected_cube_ids = [item.get("cube_id") for item in (projection.get("selected_cubes") or [])]
    selected_cube_id_values = [
        item.get("cube_id") for item in (selection_state.get("selected_cubes") or [])
    ]
    projected_theme_id = (projection.get("dataset_family") or {}).get("theme_id")
    selected_theme_id_value = (selection_state.get("selected_theme") or {}).get("theme_id")
    projected_reporting_geography = projection.get("reporting_geography") or {}
    selected_reporting_geography = selection_state.get("reporting_geography") or {}
    return (
        projected_place_ids == selected_place_ids
        and projected_cube_ids == selected_cube_id_values
        and projected_theme_id == selected_theme_id_value
        and projected_reporting_geography.get("unit_type")
        == selected_reporting_geography.get("unit_type")
        and list(projected_reporting_geography.get("unit_ids") or [])
        == list(selected_reporting_geography.get("unit_ids") or [])
    )


def _apply_projection_to_selection_state(
    selection_state: dict[str, Any],
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any]:
    updated_selection = coerce_selection_state(selection_state)
    ui_projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    render_projection = _authoritative_render_projection(thread_state=thread_state, delta=delta)
    if ui_projection is not None:
        updated_selection["ui_projection"] = ui_projection.model_dump(mode="json")
        updated_selection["selected_places"] = [
            item.model_dump(mode="json") for item in ui_projection.selected_places
        ]
        updated_selection["selected_theme"] = (
            ui_projection.dataset_family.model_dump(mode="json")
            if ui_projection.dataset_family is not None
            else None
        )
        updated_selection["selected_cubes"] = [
            item.model_dump(mode="json") for item in ui_projection.selected_cubes
        ]
        updated_selection["reporting_geography"] = (
            ui_projection.reporting_geography.model_dump(mode="json")
            if ui_projection.reporting_geography is not None
            else None
        )
        updated_selection["analysis_spec"] = (
            ui_projection.analysis_spec.model_dump(mode="json")
            if ui_projection.analysis_spec is not None
            else None
        )
        updated_selection["exact_slice"] = (
            ui_projection.exact_slice.model_dump(mode="json")
            if ui_projection.exact_slice is not None
            else None
        )
        updated_selection["time_scope"] = (
            ui_projection.time_scope.model_dump(mode="json")
            if ui_projection.time_scope is not None
            else None
        )
        updated_selection["active_output_mode"] = ui_projection.active_output_mode
        updated_selection["available_themes"] = [
            item.model_dump(mode="json") for item in ui_projection.available_themes
        ]
        updated_selection["available_cubes"] = [
            item.model_dump(mode="json") for item in ui_projection.available_cubes
        ]
        updated_selection["pending_clarification"] = (
            ui_projection.clarification.model_dump(mode="json")
            if ui_projection.clarification is not None
            else None
        )
        updated_selection["discovery_result"] = (
            ui_projection.discovery_result.model_dump(mode="json")
            if ui_projection.discovery_result is not None
            else None
        )
        updated_selection["defaults_used"] = list(ui_projection.defaults_used or [])
        updated_selection["notices"] = list(ui_projection.notices or [])
        updated_selection["runtime_state"] = ui_projection.runtime_state.model_dump(mode="json")
        updated_selection["provenance_summary"] = (
            ui_projection.provenance_summary.model_dump(mode="json")
            if ui_projection.provenance_summary is not None
            else None
        )
        updated_selection["current_receipt_id"] = ui_projection.current_receipt_id
        updated_selection["current_receipt_kind"] = (
            ui_projection.current_receipt_kind.value
            if ui_projection.current_receipt_kind is not None
            else None
        )
    elif thread_state is not None:
        updated_selection["selected_places"] = [
            item.model_dump(mode="json") for item in thread_state.selected_places
        ]
        updated_selection["selected_theme"] = (
            thread_state.selected_theme.model_dump(mode="json")
            if thread_state.selected_theme is not None
            else None
        )
        updated_selection["selected_cubes"] = [
            item.model_dump(mode="json") for item in thread_state.selected_cubes
        ]
        updated_selection["reporting_geography"] = _projected_reporting_geography(
            thread_state=thread_state
        )
        updated_selection["pending_clarification"] = _projected_pending_clarification(
            thread_state=thread_state
        )
        updated_selection["discovery_result"] = _projected_discovery_result(thread_state=thread_state)
        updated_selection["runtime_state"] = thread_state.runtime_state.model_dump(mode="json")
        updated_selection["provenance_summary"] = (
            thread_state.ui_projection.provenance_summary.model_dump(mode="json")
            if thread_state.ui_projection is not None
            and thread_state.ui_projection.provenance_summary is not None
            else None
        )
    elif delta is not None:
        updated_selection["selected_places"] = [
            item.model_dump(mode="json") for item in delta.selected_places
        ]
        updated_selection["selected_theme"] = (
            delta.selected_theme.model_dump(mode="json")
            if delta.selected_theme is not None
            else None
        )
        updated_selection["selected_cubes"] = [
            item.model_dump(mode="json") for item in delta.selected_cubes
        ]
        updated_selection["reporting_geography"] = _projected_reporting_geography(delta=delta)
        updated_selection["pending_clarification"] = _projected_pending_clarification(delta=delta)
        updated_selection["discovery_result"] = _projected_discovery_result(delta=delta)
        updated_selection["notices"] = list(delta.notices or [])
        updated_selection["runtime_state"] = delta.runtime_state.model_dump(mode="json")
        updated_selection["provenance_summary"] = (
            delta.ui_projection.provenance_summary.model_dump(mode="json")
            if delta.ui_projection is not None and delta.ui_projection.provenance_summary is not None
            else (
                delta.current_receipt.provenance_summary.model_dump(mode="json")
                if delta.current_receipt is not None and delta.current_receipt.provenance_summary is not None
                else None
            )
        )
    if render_projection is not None:
        updated_selection["render_projection"] = render_projection.model_dump(mode="json")
    if delta is not None and delta.place_search_results:
        updated_selection["pending_place_candidates"] = [
            item.model_dump(mode="json") for item in delta.place_search_results
        ]
    elif delta is not None and delta.operation is not None:
        updated_selection["pending_place_candidates"] = []
    if delta is not None and delta.cleared_places:
        updated_selection["selected_places"] = []
        updated_selection["selected_theme"] = None
        updated_selection["selected_cubes"] = []
        updated_selection["reporting_geography"] = None
        updated_selection["analysis_spec"] = None
        updated_selection["exact_slice"] = None
        updated_selection["time_scope"] = None
        updated_selection["active_output_mode"] = None
        updated_selection["available_themes"] = []
        updated_selection["available_cubes"] = []
        updated_selection["pending_clarification"] = None
        updated_selection["discovery_result"] = None
        updated_selection["defaults_used"] = []
        updated_selection["provenance_summary"] = None
    return updated_selection


def _apply_projection_to_map_state(
    map_state: dict[str, Any],
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any]:
    updated_map = coerce_map_state(map_state)
    ui_projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    render_projection = _authoritative_render_projection(thread_state=thread_state, delta=delta)
    if ui_projection is not None:
        updated_map["ui_projection"] = ui_projection.model_dump(mode="json")
        updated_map["reporting_geography"] = (
            ui_projection.reporting_geography.model_dump(mode="json")
            if ui_projection.reporting_geography is not None
            else None
        )
        updated_map["active_output_mode"] = ui_projection.active_output_mode
        updated_map["selected_ids"] = list(
            ui_projection.reporting_geography.unit_ids
            if ui_projection.reporting_geography is not None
            else []
        )
        if ui_projection.reporting_geography is not None and ui_projection.reporting_geography.unit_type:
            updated_map["unit_type"] = ui_projection.reporting_geography.unit_type
        updated_map["notices"] = list(ui_projection.notices or [])
        updated_map["provenance_summary"] = (
            ui_projection.provenance_summary.model_dump(mode="json")
            if ui_projection.provenance_summary is not None
            else None
        )
    else:
        projected_reporting_geography = _projected_reporting_geography(
            thread_state=thread_state,
            delta=delta,
        )
        if projected_reporting_geography is not None:
            updated_map["reporting_geography"] = deepcopy(projected_reporting_geography)
            updated_map["selected_ids"] = [int(item) for item in (projected_reporting_geography.get("unit_ids") or [])]
            projected_unit_type = projected_reporting_geography.get("unit_type")
            if projected_unit_type:
                updated_map["unit_type"] = projected_unit_type
    if render_projection is not None:
        updated_map["render_projection"] = render_projection.model_dump(mode="json")
        if render_projection.provenance_summary is not None:
            updated_map["provenance_summary"] = render_projection.provenance_summary.model_dump(
                mode="json"
            )
        if render_projection.boundary_map is not None:
            updated_map["feature_count"] = render_projection.boundary_map.feature_count
            if render_projection.boundary_map.unit_type:
                updated_map["unit_type"] = render_projection.boundary_map.unit_type
    elif delta is not None and delta.map_features is not None:
        updated_map["feature_count"] = delta.map_features.feature_count
        updated_map["unit_type"] = delta.map_features.unit_type
    return updated_map


def _apply_projection_to_visualization_state(
    visualization_state: dict[str, Any],
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any]:
    updated_visualization = coerce_visualization_state(visualization_state)
    ui_projection = _authoritative_ui_projection(thread_state=thread_state, delta=delta)
    render_projection = _authoritative_render_projection(thread_state=thread_state, delta=delta)
    if ui_projection is not None:
        updated_visualization["ui_projection"] = ui_projection.model_dump(mode="json")
        updated_visualization["current_receipt_id"] = ui_projection.current_receipt_id
        updated_visualization["dataset_family"] = (
            ui_projection.dataset_family.model_dump(mode="json")
            if ui_projection.dataset_family is not None
            else None
        )
        updated_visualization["exact_slice"] = (
            ui_projection.exact_slice.model_dump(mode="json")
            if ui_projection.exact_slice is not None
            else None
        )
        updated_visualization["time_scope"] = (
            ui_projection.time_scope.model_dump(mode="json")
            if ui_projection.time_scope is not None
            else None
        )
        updated_visualization["active_output_mode"] = ui_projection.active_output_mode
        updated_visualization["provenance_summary"] = (
            ui_projection.provenance_summary.model_dump(mode="json")
            if ui_projection.provenance_summary is not None
            else None
        )
    elif delta is not None and delta.current_receipt_id is not None:
        updated_visualization["current_receipt_id"] = delta.current_receipt_id
    if render_projection is not None:
        updated_visualization["render_projection"] = render_projection.model_dump(mode="json")
        if render_projection.provenance_summary is not None:
            updated_visualization["provenance_summary"] = (
                render_projection.provenance_summary.model_dump(mode="json")
            )
        if render_projection.active_output_mode is not None:
            updated_visualization["active_output_mode"] = render_projection.active_output_mode
        if render_projection.chart is not None:
            chart_payload = render_projection.chart.model_dump(mode="json")
            if "category_breakdown" in (render_projection.active_output_mode or ""):
                updated_visualization["category_breakdown"] = chart_payload
                updated_visualization["active_tab"] = "categories"
                updated_visualization["category_year"] = chart_payload.get("year")
            elif isinstance(chart_payload, dict) and "rows" in chart_payload:
                if chart_payload.get("rows") and "category_label" in chart_payload["rows"][0]:
                    updated_visualization["category_breakdown"] = chart_payload
                    updated_visualization["active_tab"] = "categories"
                    updated_visualization["category_year"] = chart_payload.get("year")
                else:
                    updated_visualization["time_series"] = chart_payload
                    updated_visualization["active_tab"] = "line"
        if render_projection.table is not None and updated_visualization.get("active_output_mode") == "table":
            updated_visualization["active_tab"] = "table"
        if render_projection.answer_text:
            updated_visualization["status"] = render_projection.answer_text
    else:
        if delta is not None and delta.time_series is not None:
            updated_visualization["time_series"] = delta.time_series.model_dump(mode="json")
            updated_visualization["active_tab"] = "line"
        if delta is not None and delta.category_breakdown is not None:
            updated_visualization["category_breakdown"] = delta.category_breakdown.model_dump(
                mode="json"
            )
            updated_visualization["active_tab"] = "categories"
            updated_visualization["category_year"] = delta.category_breakdown.year
    return updated_visualization


def _apply_projection_to_metadata_state(
    metadata_state: dict[str, Any],
    *,
    thread_state: ChatThreadState | None = None,
    delta: ChatUIStateDelta | None = None,
) -> dict[str, Any]:
    updated_metadata = coerce_metadata_state(metadata_state)
    render_projection = _authoritative_render_projection(thread_state=thread_state, delta=delta)
    if render_projection is not None:
        updated_metadata["render_projection"] = render_projection.model_dump(mode="json")
        updated_metadata["provenance_summary"] = (
            render_projection.provenance_summary.model_dump(mode="json")
            if render_projection.provenance_summary is not None
            else None
        )
        metadata_payload = render_projection.metadata_payload or {}
        for field in (
            "place_profile",
            "place_key_findings",
            "unit_type_info",
            "data_entity_resolution",
            "data_entity_info",
        ):
            updated_metadata[field] = metadata_payload.get(field)
        unit_type_info = metadata_payload.get("unit_type_info") or {}
        if unit_type_info.get("identifier"):
            updated_metadata["requested_unit_type"] = unit_type_info["identifier"]
        data_entity_info = metadata_payload.get("data_entity_info") or {}
        if data_entity_info.get("entity_id"):
            updated_metadata["requested_entity_id"] = data_entity_info["entity_id"]
        elif (metadata_payload.get("data_entity_resolution") or {}).get("result", {}).get("entity_id"):
            updated_metadata["requested_entity_id"] = metadata_payload["data_entity_resolution"]["result"]["entity_id"]
    elif delta is not None:
        if delta.place_profile is not None:
            updated_metadata["place_profile"] = delta.place_profile.model_dump(mode="json")
        if delta.unit_type_info is not None:
            updated_metadata["requested_unit_type"] = delta.unit_type_info.identifier
            updated_metadata["unit_type_info"] = delta.unit_type_info.model_dump(mode="json")
        if delta.data_entity_resolution is not None:
            updated_metadata["data_entity_resolution"] = delta.data_entity_resolution.model_dump(
                mode="json"
            )
            updated_metadata["requested_entity_id"] = delta.data_entity_resolution.result.entity_id
        if delta.data_entity_info is not None:
            updated_metadata["data_entity_info"] = delta.data_entity_info.model_dump(
                mode="json"
            )
            updated_metadata["requested_entity_id"] = delta.data_entity_info.entity_id
    return updated_metadata


def apply_chat_ui_delta(
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
    metadata_state: dict[str, Any],
    map_state: dict[str, Any],
    delta: ChatUIStateDelta,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    updated_selection = _apply_projection_to_selection_state(
        selection_state,
        delta=delta,
    )
    updated_visualization = _apply_projection_to_visualization_state(
        visualization_state,
        delta=delta,
    )
    updated_metadata = _apply_projection_to_metadata_state(
        metadata_state,
        delta=delta,
    )
    updated_map = _apply_projection_to_map_state(
        map_state,
        delta=delta,
    )

    if delta.ui_projection is None:
        projection_selected_places = _projected_selected_places(
            selected_places=delta.selected_places,
            ui_projection=delta.ui_projection,
        )
        projection_selected_theme = _projected_selected_theme(
            selected_theme=delta.selected_theme,
            ui_projection=delta.ui_projection,
        )
        projection_selected_cubes = _projected_selected_cubes(
            selected_cubes=delta.selected_cubes,
            ui_projection=delta.ui_projection,
        )
        if projection_selected_places:
            updated_selection["selected_places"] = [
                item.model_dump(mode="json") for item in projection_selected_places
            ]
        if projection_selected_theme is not None:
            updated_selection["selected_theme"] = projection_selected_theme.model_dump(mode="json")
        if projection_selected_cubes:
            updated_selection["selected_cubes"] = [
                item.model_dump(mode="json") for item in projection_selected_cubes
            ]
        projected_reporting_geography = _projected_reporting_geography(delta=delta)
        if projected_reporting_geography is not None:
            updated_selection["reporting_geography"] = projected_reporting_geography

    if delta.themes is not None and not updated_selection.get("available_themes"):
        updated_selection["available_themes"] = [
            item.model_dump(mode="json") for item in delta.themes.items
        ]
    if delta.cubes and not updated_selection.get("available_cubes"):
        updated_selection["available_cubes"] = [
            item.model_dump(mode="json") for item in delta.cubes
        ]
    if delta.time_series is not None and updated_visualization.get("time_series") is None:
        updated_visualization["time_series"] = delta.time_series.model_dump(mode="json")
        updated_visualization["active_tab"] = "line"
    if (
        delta.category_breakdown is not None
        and updated_visualization.get("category_breakdown") is None
    ):
        updated_visualization["category_breakdown"] = delta.category_breakdown.model_dump(
            mode="json"
        )
        updated_visualization["active_tab"] = "categories"
        updated_visualization["category_year"] = delta.category_breakdown.year
    if delta.map_features is not None and updated_map.get("feature_count") == 0:
        updated_map["unit_type"] = delta.map_features.unit_type
        updated_map["feature_count"] = delta.map_features.feature_count
    if delta.place_profile is not None and updated_metadata.get("place_profile") is None:
        updated_metadata["place_profile"] = delta.place_profile.model_dump(mode="json")
    if delta.unit_type_info is not None and updated_metadata.get("unit_type_info") is None:
        updated_metadata["requested_unit_type"] = delta.unit_type_info.identifier
        updated_metadata["unit_type_info"] = delta.unit_type_info.model_dump(mode="json")
    if (
        delta.data_entity_resolution is not None
        and updated_metadata.get("data_entity_resolution") is None
    ):
        updated_metadata["data_entity_resolution"] = delta.data_entity_resolution.model_dump(
            mode="json"
        )
        updated_metadata["requested_entity_id"] = delta.data_entity_resolution.result.entity_id
    if delta.data_entity_info is not None and updated_metadata.get("data_entity_info") is None:
        updated_metadata["data_entity_info"] = delta.data_entity_info.model_dump(mode="json")
        updated_metadata["requested_entity_id"] = delta.data_entity_info.entity_id
    return updated_selection, updated_visualization, updated_metadata, updated_map


def apply_stream_ui_delta(
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
    metadata_state: dict[str, Any],
    map_state: dict[str, Any],
    delta: ChatUIStateDelta,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    return apply_chat_ui_delta(
        selection_state=_apply_projection_to_selection_state(selection_state, delta=delta),
        visualization_state=_apply_projection_to_visualization_state(
            visualization_state,
            delta=delta,
        ),
        metadata_state=_apply_projection_to_metadata_state(metadata_state, delta=delta),
        map_state=_apply_projection_to_map_state(map_state, delta=delta),
        delta=delta,
    )


def hydrate_states_from_thread(
    *,
    thread_state_data: dict[str, Any] | None,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
    metadata_state: dict[str, Any],
    map_state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not thread_state_data:
        return (
            coerce_selection_state(selection_state),
            coerce_visualization_state(visualization_state),
            coerce_metadata_state(metadata_state),
            coerce_map_state(map_state),
        )

    thread_state = ChatThreadState.model_validate(thread_state_data)
    updated_selection = _apply_projection_to_selection_state(
        selection_state,
        thread_state=thread_state,
    )
    updated_visualization = _apply_projection_to_visualization_state(
        visualization_state,
        thread_state=thread_state,
    )
    updated_metadata = _apply_projection_to_metadata_state(
        metadata_state,
        thread_state=thread_state,
    )
    updated_map = _apply_projection_to_map_state(
        map_state,
        thread_state=thread_state,
    )

    if thread_state.latest_ui_delta is not None:
        (
            updated_selection,
            updated_visualization,
            updated_metadata,
            updated_map,
        ) = apply_chat_ui_delta(
            selection_state=updated_selection,
            visualization_state=updated_visualization,
            metadata_state=updated_metadata,
            map_state=updated_map,
            delta=thread_state.latest_ui_delta,
        )

    return updated_selection, updated_visualization, updated_metadata, updated_map


def apply_chat_turn_response(
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
    metadata_state: dict[str, Any],
    map_state: dict[str, Any],
    response: ChatTurnResponse,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    updated_selection = _apply_projection_to_selection_state(
        selection_state,
        thread_state=response.thread_state,
    )
    updated_visualization = _apply_projection_to_visualization_state(
        visualization_state,
        thread_state=response.thread_state,
    )
    updated_metadata = _apply_projection_to_metadata_state(
        metadata_state,
        thread_state=response.thread_state,
    )
    updated_map = _apply_projection_to_map_state(
        map_state,
        thread_state=response.thread_state,
    )
    updated_selection["pending_place_candidates"] = []

    (
        updated_selection,
        updated_visualization,
        updated_metadata,
        updated_map,
    ) = apply_chat_ui_delta(
        selection_state=updated_selection,
        visualization_state=updated_visualization,
        metadata_state=updated_metadata,
        map_state=updated_map,
        delta=response.ui_delta,
    )

    return updated_selection, updated_visualization, updated_metadata, updated_map


def sync_selection_catalogs(
    api_client: APIClient,
    selection_state: dict[str, Any],
) -> dict[str, Any]:
    updated = coerce_selection_state(selection_state)
    ui_projection = updated.get("ui_projection")
    if isinstance(ui_projection, dict):
        updated["available_themes"] = list(ui_projection.get("available_themes") or [])
        updated["available_cubes"] = list(ui_projection.get("available_cubes") or [])
        updated["selected_places"] = list(ui_projection.get("selected_places") or [])
        updated["selected_theme"] = ui_projection.get("dataset_family")
        updated["selected_cubes"] = list(ui_projection.get("selected_cubes") or [])
        updated["reporting_geography"] = ui_projection.get("reporting_geography")
        updated["pending_clarification"] = ui_projection.get("clarification")
        updated["discovery_result"] = ui_projection.get("discovery_result")
        updated["analysis_spec"] = ui_projection.get("analysis_spec")
        updated["exact_slice"] = ui_projection.get("exact_slice")
        updated["time_scope"] = ui_projection.get("time_scope")
        updated["active_output_mode"] = ui_projection.get("active_output_mode")
        updated["defaults_used"] = list(ui_projection.get("defaults_used") or [])
        updated["notices"] = list(ui_projection.get("notices") or [])
        updated["provenance_summary"] = ui_projection.get("provenance_summary")
        updated["current_receipt_id"] = ui_projection.get("current_receipt_id")
        current_receipt_kind = ui_projection.get("current_receipt_kind")
        updated["current_receipt_kind"] = (
            current_receipt_kind.get("value")
            if isinstance(current_receipt_kind, dict) and "value" in current_receipt_kind
            else current_receipt_kind
        )
        return updated

    reporting_geography = updated.get("reporting_geography") or {}
    representative_unit_id = next(
        (
            int(item)
            for item in (reporting_geography.get("unit_ids") or [])
            if isinstance(item, int) or str(item).isdigit()
        ),
        None,
    )
    if representative_unit_id is None:
        updated["available_themes"] = []
        updated["available_cubes"] = []
        updated["selected_cubes"] = []
        return updated

    themes = api_client.list_themes_for_unit(representative_unit_id)
    updated["available_themes"] = [item.model_dump(mode="json") for item in themes.items]
    theme_id = selected_theme_id(updated)
    available_theme_ids = {item["theme_id"] for item in updated["available_themes"]}
    if theme_id not in available_theme_ids:
        updated["selected_theme"] = None
        updated["available_cubes"] = []
        updated["selected_cubes"] = []
        return updated

    cubes = api_client.list_cubes_for_unit_theme(representative_unit_id, theme_id)
    updated["available_cubes"] = [item.model_dump(mode="json") for item in cubes.items]
    available_cube_ids = {item["cube_id"] for item in updated["available_cubes"]}
    updated["selected_cubes"] = [
        cube
        for cube in (updated.get("selected_cubes") or [])
        if cube.get("cube_id") in available_cube_ids
    ]
    return updated


def _safe_metadata_lookup(
    lookup: Callable[
        [],
        PlaceProfileResponse
        | PlaceKeyFindingsResponse
        | UnitTypeInfoResponse
        | DataEntityInfoResponse,
    ]
) -> dict[str, Any] | None:
    try:
        result = lookup()
    except APIClientError as exc:
        if exc.status_code == 404:
            return None
        raise
    return result.model_dump(mode="json")


def hydrate_metadata_state(
    api_client: APIClient,
    *,
    selection_state: dict[str, Any],
    metadata_state: dict[str, Any],
) -> dict[str, Any]:
    updated = coerce_metadata_state(metadata_state)
    render_projection = (
        selection_state.get("render_projection")
        if isinstance(selection_state.get("render_projection"), dict)
        else updated.get("render_projection")
    )
    if isinstance(render_projection, dict):
        updated["render_projection"] = deepcopy(render_projection)
        metadata_payload = render_projection.get("metadata_payload") or {}
        if metadata_payload:
            for field in (
                "place_profile",
                "place_key_findings",
                "unit_type_info",
                "data_entity_resolution",
                "data_entity_info",
            ):
                updated[field] = metadata_payload.get(field)
            unit_type_info = metadata_payload.get("unit_type_info") or {}
            if unit_type_info.get("identifier"):
                updated["requested_unit_type"] = unit_type_info["identifier"]
            data_entity_info = metadata_payload.get("data_entity_info") or {}
            if data_entity_info.get("entity_id"):
                updated["requested_entity_id"] = data_entity_info["entity_id"]
            elif (metadata_payload.get("data_entity_resolution") or {}).get("result", {}).get(
                "entity_id"
            ):
                updated["requested_entity_id"] = metadata_payload["data_entity_resolution"][
                    "result"
                ]["entity_id"]
            return updated

    first_place = first_selected_place(selection_state)

    if first_place is None:
        updated["place_profile"] = None
        updated["place_key_findings"] = None
    else:
        place_id = first_place.get("place", {}).get("place_id")
        first_unit = (first_place.get("units") or [{}])[0]
        unit_id = first_unit.get("unit_id")
        unit_type = first_unit.get("unit_type")

        if isinstance(place_id, int):
            updated["place_profile"] = _safe_metadata_lookup(
                lambda: api_client.get_place_profile(place_id)
            )
        if isinstance(unit_id, int):
            updated["place_key_findings"] = _safe_metadata_lookup(
                lambda: api_client.get_place_key_findings(unit_id)
            )
        if isinstance(unit_type, str):
            updated["requested_unit_type"] = updated.get("requested_unit_type") or unit_type

    requested_unit_type = updated.get("requested_unit_type")
    if isinstance(requested_unit_type, str):
        updated["unit_type_info"] = _safe_metadata_lookup(
            lambda: api_client.get_unit_type_info(requested_unit_type)
        )

    requested_entity_id = updated.get("requested_entity_id")
    if isinstance(requested_entity_id, str):
        updated["data_entity_info"] = _safe_metadata_lookup(
            lambda: api_client.get_data_entity_info(requested_entity_id)
        )

    return updated


def status_banner_from_state(
    *,
    selection_state: dict[str, Any],
    request_status: dict[str, Any],
) -> tuple[str | None, list[str]]:
    error = request_status.get("error")
    notices: list[str] = []
    for item in selection_state.get("notices") or []:
        if not item:
            continue
        text = str(item)
        if text not in notices:
            notices.append(text)
    runtime_state = selection_state.get("runtime_state") or {}
    if isinstance(runtime_state, dict):
        runtime_notice = runtime_state.get("notice")
        if isinstance(runtime_notice, str) and runtime_notice and runtime_notice not in notices:
            notices.append(runtime_notice)
    return error, notices


def thread_label(thread_state: dict[str, Any] | None) -> str:
    if not thread_state:
        return "Thread: loading"
    thread_id = thread_state.get("thread_id")
    if not thread_id:
        return "Thread: unavailable"
    return f"Thread: {str(thread_id)[:8]}"
