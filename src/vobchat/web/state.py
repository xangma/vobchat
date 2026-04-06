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


def selected_unit_ids(selection_state: dict[str, Any]) -> list[int]:
    unit_ids: list[int] = []
    for place in _selected_places(selection_state):
        for unit in place.get("units") or []:
            unit_id = unit.get("unit_id")
            if isinstance(unit_id, int) and unit_id not in unit_ids:
                unit_ids.append(unit_id)
    return unit_ids


def selected_unit_types(selection_state: dict[str, Any]) -> list[str]:
    unit_types: list[str] = []
    for place in _selected_places(selection_state):
        for unit in place.get("units") or []:
            unit_type = unit.get("unit_type")
            if isinstance(unit_type, str) and unit_type not in unit_types:
                unit_types.append(unit_type)
    return unit_types


def first_selected_place(selection_state: dict[str, Any]) -> dict[str, Any] | None:
    places = _selected_places(selection_state)
    return places[0] if places else None


def first_selected_unit_id(selection_state: dict[str, Any]) -> int | None:
    ids = selected_unit_ids(selection_state)
    return ids[0] if ids else None


def first_selected_unit_type(selection_state: dict[str, Any]) -> str | None:
    unit_types = selected_unit_types(selection_state)
    return unit_types[0] if unit_types else None


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


def apply_chat_ui_delta(
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
    metadata_state: dict[str, Any],
    map_state: dict[str, Any],
    delta: ChatUIStateDelta,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    updated_selection = coerce_selection_state(selection_state)
    updated_visualization = coerce_visualization_state(visualization_state)
    updated_metadata = coerce_metadata_state(metadata_state)
    updated_map = coerce_map_state(map_state)

    updated_selection["notices"] = list(delta.notices)

    if delta.cleared_places:
        updated_selection["selected_places"] = []
        updated_selection["selected_theme"] = None
        updated_selection["selected_cubes"] = []
        updated_selection["available_themes"] = []
        updated_selection["available_cubes"] = []
        updated_map["selected_ids"] = []

    if delta.place_search_results:
        updated_selection["pending_place_candidates"] = [
            item.model_dump(mode="json") for item in delta.place_search_results
        ]
    elif delta.operation is not None:
        updated_selection["pending_place_candidates"] = []

    if delta.themes is not None:
        updated_selection["available_themes"] = [
            item.model_dump(mode="json") for item in delta.themes.items
        ]

    if delta.cubes:
        updated_selection["available_cubes"] = [
            item.model_dump(mode="json") for item in delta.cubes
        ]

    if delta.time_series is not None:
        updated_visualization["time_series"] = delta.time_series.model_dump(mode="json")
        updated_visualization["active_tab"] = "line"

    if delta.category_breakdown is not None:
        updated_visualization["category_breakdown"] = delta.category_breakdown.model_dump(
            mode="json"
        )
        updated_visualization["active_tab"] = "categories"
        updated_visualization["category_year"] = delta.category_breakdown.year

    if delta.map_features is not None:
        updated_map["unit_type"] = delta.map_features.unit_type
        updated_map["feature_count"] = delta.map_features.feature_count

    if delta.place_profile is not None:
        updated_metadata["place_profile"] = delta.place_profile.model_dump(mode="json")

    if delta.unit_type_info is not None:
        updated_metadata["requested_unit_type"] = delta.unit_type_info.identifier
        updated_metadata["unit_type_info"] = delta.unit_type_info.model_dump(mode="json")

    if delta.data_entity_resolution is not None:
        updated_metadata["data_entity_resolution"] = delta.data_entity_resolution.model_dump(
            mode="json"
        )
        updated_metadata["requested_entity_id"] = (
            delta.data_entity_resolution.result.entity_id
        )

    if delta.data_entity_info is not None:
        updated_metadata["data_entity_info"] = delta.data_entity_info.model_dump(
            mode="json"
        )
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
    updated_selection = coerce_selection_state(selection_state)
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

    updated_visualization = coerce_visualization_state(visualization_state)
    updated_metadata = coerce_metadata_state(metadata_state)
    updated_map = coerce_map_state(map_state)
    updated_map["selected_ids"] = selected_unit_ids(updated_selection)

    unit_type = first_selected_unit_type(updated_selection)
    if unit_type:
        updated_map["unit_type"] = unit_type

    return apply_chat_ui_delta(
        selection_state=updated_selection,
        visualization_state=updated_visualization,
        metadata_state=updated_metadata,
        map_state=updated_map,
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
    updated_selection = coerce_selection_state(selection_state)
    updated_visualization = coerce_visualization_state(visualization_state)
    updated_metadata = coerce_metadata_state(metadata_state)
    updated_map = coerce_map_state(map_state)

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
    updated_map["selected_ids"] = selected_unit_ids(updated_selection)

    unit_type = first_selected_unit_type(updated_selection)
    if unit_type:
        updated_map["unit_type"] = unit_type

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
    updated_selection = coerce_selection_state(selection_state)
    updated_visualization = coerce_visualization_state(visualization_state)
    updated_metadata = coerce_metadata_state(metadata_state)
    updated_map = coerce_map_state(map_state)

    updated_selection["selected_places"] = [
        item.model_dump(mode="json") for item in response.thread_state.selected_places
    ]
    updated_selection["selected_theme"] = (
        response.thread_state.selected_theme.model_dump(mode="json")
        if response.thread_state.selected_theme is not None
        else None
    )
    updated_selection["selected_cubes"] = [
        item.model_dump(mode="json") for item in response.thread_state.selected_cubes
    ]

    updated_selection["pending_place_candidates"] = []
    updated_map["selected_ids"] = selected_unit_ids(updated_selection)

    unit_type = first_selected_unit_type(updated_selection)
    if unit_type:
        updated_map["unit_type"] = unit_type

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
    first_unit_id = first_selected_unit_id(updated)
    if first_unit_id is None:
        updated["available_themes"] = []
        updated["selected_theme"] = None
        updated["available_cubes"] = []
        updated["selected_cubes"] = []
        return updated

    themes = api_client.list_themes_for_unit(first_unit_id)
    updated["available_themes"] = [item.model_dump(mode="json") for item in themes.items]

    theme_id = selected_theme_id(updated)
    available_theme_ids = {item["theme_id"] for item in updated["available_themes"]}
    if theme_id not in available_theme_ids:
        updated["selected_theme"] = None
        updated["available_cubes"] = []
        updated["selected_cubes"] = []
        return updated

    cubes = api_client.list_cubes_for_unit_theme(first_unit_id, theme_id)
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
    notices = [str(item) for item in (selection_state.get("notices") or []) if item]
    return error, notices


def thread_label(thread_state: dict[str, Any] | None) -> str:
    if not thread_state:
        return "Thread: loading"
    thread_id = thread_state.get("thread_id")
    if not thread_id:
        return "Thread: unavailable"
    return f"Thread: {str(thread_id)[:8]}"
