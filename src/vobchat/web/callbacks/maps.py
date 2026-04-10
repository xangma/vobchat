from __future__ import annotations

from typing import Any

from dash import Input, Output, State
from dash.exceptions import PreventUpdate

from vobchat.api.schemas.maps import MapFeatureCollectionResponse, MapFeaturesByIdsQuery, MapFeaturesQuery
from vobchat.api.schemas.places import PlaceResolveQuery
from vobchat.web.clients import APIClient, APIClientError, get_api_client
from vobchat.web.state import (
    coerce_map_state,
    coerce_selection_state,
    empty_feature_collection,
    selected_unit_ids,
    selected_theme_id,
)


def _selected_place_names(selection_state: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for place in selection_state.get("selected_places") or []:
        name = place.get("place", {}).get("name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def _reporting_geography_label(
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
) -> str | None:
    reporting_geography = selection_state.get("reporting_geography") or map_state.get("reporting_geography") or {}
    if not isinstance(reporting_geography, dict):
        return None
    label = reporting_geography.get("label") or reporting_geography.get("unit_type")
    return str(label) if label else None


def _safe_downgrade_note(
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
) -> str | None:
    provenance_summary = (
        map_state.get("provenance_summary")
        or selection_state.get("provenance_summary")
        or {}
    )
    if not isinstance(provenance_summary, dict):
        return None
    for item in provenance_summary.get("safe_downgrades") or []:
        lowered = str(item).lower()
        if "boundary-map-only" in lowered or "boundary map" in lowered:
            return "This is a boundary-only view because a measured thematic map is not safely available here yet."
    return None


def _map_status_text(
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
    *,
    projected: bool,
    feature_count: int,
) -> str:
    places = _selected_place_names(selection_state)
    geography = _reporting_geography_label(selection_state, map_state)
    lead = "Boundary map"
    if places:
        lead += f" for {', '.join(places)}"
    if geography:
        geography_text = geography if geography.lower().endswith(" level") else f"{geography} level"
        lead += f" at {geography_text}"
    status = f"{lead}."
    downgrade_note = _safe_downgrade_note(selection_state, map_state)
    if downgrade_note:
        return f"{status} {downgrade_note}"
    if not projected and feature_count > 0 and not places:
        return f"{status} Showing {feature_count} available areas in the current map view."
    return status


def _initial_map_prompt(
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
) -> str:
    places = _selected_place_names(selection_state)
    geography = _reporting_geography_label(selection_state, map_state)
    if places and not geography:
        return f"Choose a geography level to load a map for {', '.join(places)}."
    if geography:
        geography_text = geography if geography.lower().endswith(" level") else f"{geography} level"
        return f"Choose a place to highlight at {geography_text}."
    return "Choose a place or geography level to load a map."


def _should_wait_for_map_context(
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
) -> bool:
    if selection_state.get("reporting_geography") or map_state.get("reporting_geography"):
        return False
    if map_state.get("selected_ids"):
        return False
    render_projection = map_state.get("render_projection") or {}
    if isinstance(render_projection, dict) and render_projection.get("boundary_map"):
        return False
    return True


def _friendly_map_error(exc: APIClientError) -> str:
    detail = exc.detail
    if isinstance(detail, str) and detail.strip():
        lowered = detail.lower()
        if "internal server error" in lowered:
            return "I couldn't load the map for this view right now."
        return f"I couldn't load the map for this view: {detail.strip()}"
    return "I couldn't load the map for this view right now."


def _feature_to_geojson(feature: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": feature["unit_id"],
        "geometry": feature["geometry"],
        "properties": {
            "g_unit": feature["unit_id"],
            "unit_name": feature["unit_name"],
            "g_unit_type": feature["unit_type"],
            "start_year": feature.get("start_year"),
            "end_year": feature.get("end_year"),
            "has_theme": feature.get("has_theme"),
        },
    }


def _parse_bbox(bounds: list[list[float]] | tuple[tuple[float, float], tuple[float, float]] | None) -> dict[str, float] | None:
    if not bounds or len(bounds) != 2:
        return None
    south_west, north_east = bounds
    if len(south_west) != 2 or len(north_east) != 2:
        return None
    min_y, min_x = float(south_west[0]), float(south_west[1])
    max_y, max_x = float(north_east[0]), float(north_east[1])
    return {
        "min_x": min_x,
        "min_y": min_y,
        "max_x": max_x,
        "max_y": max_y,
    }


def _clicked_unit_id(click_data: dict[str, Any] | None) -> int | None:
    if not click_data:
        return None
    feature = click_data.get("feature") if isinstance(click_data, dict) else None
    properties = feature.get("properties") if isinstance(feature, dict) else None
    candidates = (
        click_data.get("id") if isinstance(click_data, dict) else None,
        feature.get("id") if isinstance(feature, dict) else None,
        properties.get("g_unit") if isinstance(properties, dict) else None,
        properties.get("id") if isinstance(properties, dict) else None,
    )
    for value in candidates:
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _merge_feature_collections(
    collections: list[MapFeatureCollectionResponse],
    *,
    selected_ids: list[int],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    feature_map: dict[tuple[str, int], dict[str, Any]] = {}
    with_theme_ids: set[str] = set()

    for collection in collections:
        for feature in collection.features:
            key = (feature.unit_type, feature.unit_id)
            feature_map[key] = feature.model_dump(mode="json")
            if feature.has_theme:
                with_theme_ids.add(str(feature.unit_id))

    features = [_feature_to_geojson(feature) for feature in feature_map.values()]
    status = f"Loaded {len(features)} features across {len(collections)} map request(s)."
    return (
        {"type": "FeatureCollection", "features": features},
        {
            "selected": [str(item) for item in selected_ids],
            "withTheme": sorted(with_theme_ids),
        },
        status,
    )


def load_map_layer(
    api_client: APIClient,
    *,
    selection_state: dict[str, Any],
    map_state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    if _should_wait_for_map_context(selection_state, map_state):
        return empty_feature_collection(), {"selected": [], "withTheme": []}, _initial_map_prompt(
            selection_state,
            map_state,
        )

    reporting_geography = selection_state.get("reporting_geography") or {}
    selected_ids = [int(item) for item in (map_state.get("selected_ids") or selected_unit_ids(selection_state) or [])]
    year_range = map_state.get("year_range") or []
    start_year = int(year_range[0]) if len(year_range) == 2 else None
    end_year = int(year_range[1]) if len(year_range) == 2 else None
    current_unit_type = str(
        map_state.get("unit_type")
        or reporting_geography.get("unit_type")
        or "MOD_REG"
    )
    theme_id = selected_theme_id(selection_state)
    bbox = map_state.get("bbox") or {}
    render_projection = map_state.get("render_projection") or {}
    projected_boundary_map = render_projection.get("boundary_map")

    if (
        isinstance(projected_boundary_map, dict)
        and projected_boundary_map.get("unit_type") == current_unit_type
        and selected_ids == [int(item) for item in (reporting_geography.get("unit_ids") or [])]
    ):
        boundary_map = MapFeatureCollectionResponse.model_validate(projected_boundary_map)
        data, hideout, _status = _merge_feature_collections(
            [boundary_map],
            selected_ids=selected_ids,
        )
        return data, hideout, _map_status_text(
            selection_state,
            map_state,
            projected=True,
            feature_count=int(boundary_map.feature_count or 0),
        )

    collections: list[MapFeatureCollectionResponse] = []
    background = api_client.fetch_features(
        MapFeaturesQuery(
            unit_type=current_unit_type,
            start_year=start_year,
            end_year=end_year,
            theme_id=theme_id,
            min_x=bbox.get("min_x"),
            min_y=bbox.get("min_y"),
            max_x=bbox.get("max_x"),
            max_y=bbox.get("max_y"),
            exclude_ids=selected_ids,
        )
    )
    collections.append(background)

    if selected_ids:
        collections.append(
            api_client.fetch_features_by_ids(
                MapFeaturesByIdsQuery(
                    unit_type=current_unit_type,
                    ids=sorted(set(selected_ids)),
                    start_year=start_year,
                    end_year=end_year,
                    theme_id=theme_id,
                )
            )
        )

    data, hideout, _status = _merge_feature_collections(collections, selected_ids=selected_ids)
    feature_count = len(data.get("features") or [])
    return (
        data,
        hideout,
        _map_status_text(
            selection_state,
            map_state,
            projected=False,
            feature_count=feature_count,
        ),
    )


def toggle_selected_place_from_map_click(
    api_client: APIClient,
    *,
    selection_state: dict[str, Any],
    click_data: dict[str, Any] | None,
) -> dict[str, Any]:
    unit_id = _clicked_unit_id(click_data)
    if unit_id is None:
        return coerce_selection_state(selection_state)

    updated_selection = coerce_selection_state(selection_state)
    current_unit_ids = set(selected_unit_ids(updated_selection))
    current_unit_ids.update(
        int(unit.get("unit_id"))
        for place in (updated_selection.get("selected_places") or [])
        for unit in (place.get("units") or [])
        if unit.get("unit_id") is not None
    )
    if unit_id in current_unit_ids:
        updated_selection["selected_places"] = [
            place
            for place in (updated_selection.get("selected_places") or [])
            if unit_id not in {unit.get("unit_id") for unit in (place.get("units") or [])}
        ]
        return updated_selection

    resolved_place = api_client.get_place_for_unit(unit_id, PlaceResolveQuery())
    selected_places = list(updated_selection.get("selected_places") or [])
    selected_places = [
        place
        for place in selected_places
        if place.get("place", {}).get("place_id") != resolved_place.place.place_id
    ]
    selected_places.append(resolved_place.model_dump(mode="json"))
    updated_selection["selected_places"] = selected_places
    return updated_selection


def register_map_callbacks(app, api_client: APIClient | None = None) -> None:
    client = api_client or get_api_client()

    @app.callback(
        Output("map-state-store", "data", allow_duplicate=True),
        Input("map-unit-type-dropdown", "value"),
        Input("map-year-range-slider", "value"),
        Input("leaflet-map", "bounds"),
        State("map-state-store", "data"),
        prevent_initial_call=True,
    )
    def update_map_filters(
        unit_type: str,
        year_range: list[int],
        bounds: list[list[float]] | None,
        map_state_data: dict[str, Any] | None,
    ):
        if not unit_type or not year_range or len(year_range) != 2:
            raise PreventUpdate
        map_state = coerce_map_state(map_state_data)
        map_state["unit_type"] = unit_type
        map_state["year_range"] = [int(year_range[0]), int(year_range[1])]
        map_state["bbox"] = _parse_bbox(bounds)
        return map_state

    @app.callback(
        Output("selection-state-store", "data", allow_duplicate=True),
        Input("geojson-layer", "clickData"),
        State("selection-state-store", "data"),
        prevent_initial_call=True,
    )
    def update_selection_from_map_click(
        click_data: dict[str, Any] | None,
        selection_state_data: dict[str, Any] | None,
    ):
        if not click_data:
            raise PreventUpdate
        try:
            updated_selection = toggle_selected_place_from_map_click(
                client,
                selection_state=coerce_selection_state(selection_state_data),
                click_data=click_data,
            )
        except APIClientError as exc:
            raise PreventUpdate from exc
        if updated_selection == coerce_selection_state(selection_state_data):
            raise PreventUpdate
        return updated_selection

    @app.callback(
        Output("geojson-layer", "data"),
        Output("geojson-layer", "hideout"),
        Output("map-status", "children"),
        Input("selection-state-store", "data"),
        Input("map-state-store", "data"),
    )
    def render_map_layer(
        selection_state_data: dict[str, Any] | None,
        map_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        map_state = coerce_map_state(map_state_data)
        try:
            data, hideout, status = load_map_layer(
                client,
                selection_state=selection_state,
                map_state=map_state,
            )
        except APIClientError as exc:
            return (
                empty_feature_collection(),
                {"selected": [], "withTheme": []},
                _friendly_map_error(exc),
            )
        return data or empty_feature_collection(), hideout, status
