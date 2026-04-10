from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, State, ctx, html
from dash.exceptions import PreventUpdate

from vobchat.api.schemas.series import CategoryBreakdownRequest, TimeSeriesRequest
from vobchat.web.clients import APIClient, APIClientError, get_api_client
from vobchat.web.state import (
    coerce_metadata_state,
    coerce_selection_state,
    coerce_visualization_state,
    selected_cube_ids,
    selected_theme_id,
    selected_unit_ids,
)


def _friendly_output_label(output_mode: str | None, *, active_tab: str | None = None) -> str:
    if output_mode == "table" or active_tab == "table":
        return "Table"
    if output_mode in {"category_chart", "category_table"} or active_tab == "categories":
        return "Breakdown chart"
    return "Chart"


def _selected_place_names(selection_state: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for place in selection_state.get("selected_places") or []:
        name = place.get("place", {}).get("name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def _time_scope_label(visualization_state: dict[str, Any]) -> str | None:
    time_scope = visualization_state.get("time_scope") or {}
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


def _visualization_status_text(
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
) -> str:
    provenance_summary = visualization_state.get("provenance_summary") or {}
    result_type = (
        provenance_summary.get("result_type")
        if isinstance(provenance_summary, dict)
        else None
    )
    active_tab = visualization_state.get("active_tab")
    output_mode = visualization_state.get("active_output_mode")
    label = _friendly_output_label(output_mode, active_tab=active_tab)
    places = _selected_place_names(selection_state)
    place_text = f" for {', '.join(places)}" if places else ""
    time_label = _time_scope_label(visualization_state)
    time_text = f" over {time_label}" if time_label else ""
    has_render_projection = isinstance(visualization_state.get("render_projection"), dict)

    if result_type == "catalog_discovery":
        return "This panel is waiting for a completed result. The current answer is still an exploration step."
    if result_type == "info_lookup":
        return "This panel is showing supporting reference information rather than a statistical result."
    if result_type == "executed_analysis" or has_render_projection:
        return f"{label} view of the current result{place_text}{time_text}."
    return f"{label} ready{place_text}{time_text}."


def _as_dropdown_options(items: list[dict[str, Any]], *, value_key: str, label_key: str) -> list[dict[str, str]]:
    return [
        {"label": str(item.get(label_key, item.get(value_key, ""))), "value": str(item[value_key])}
        for item in items
        if item.get(value_key) is not None
    ]


def _build_category_year_options(selection_state: dict[str, Any]) -> list[dict[str, int]]:
    category_cube = next(
        (cube for cube in selection_state.get("selected_cubes") or [] if cube.get("has_categories")),
        None,
    )
    if not category_cube:
        return []

    start = int(category_cube.get("start_year") or category_cube.get("end_year") or 1901)
    end = int(category_cube.get("end_year") or category_cube.get("start_year") or start)
    step = 10 if end - start > 30 else 1
    years = list(range(start, end + 1, step))
    if years[-1] != end:
        years.append(end)
    return [{"label": str(year), "value": int(year)} for year in sorted(set(years))]


def _representative_unit_id(selection_state: dict[str, Any]) -> int | None:
    reporting_geography = selection_state.get("reporting_geography") or {}
    for unit_id in reporting_geography.get("unit_ids") or []:
        try:
            return int(unit_id)
        except (TypeError, ValueError):
            continue
    return None


def _selection_matches_projection(selection_state: dict[str, Any]) -> bool:
    ui_projection = selection_state.get("ui_projection")
    if not isinstance(ui_projection, dict):
        return False
    projected_place_ids = [
        item.get("place", {}).get("place_id")
        for item in (ui_projection.get("selected_places") or [])
    ]
    selected_place_ids = [
        item.get("place", {}).get("place_id")
        for item in (selection_state.get("selected_places") or [])
    ]
    projected_cube_ids = [item.get("cube_id") for item in (ui_projection.get("selected_cubes") or [])]
    selected_cube_id_values = [item.get("cube_id") for item in (selection_state.get("selected_cubes") or [])]
    projected_theme_id = (ui_projection.get("dataset_family") or {}).get("theme_id")
    selected_theme_id_value = (selection_state.get("selected_theme") or {}).get("theme_id")
    projected_reporting_geography = ui_projection.get("reporting_geography") or {}
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


def load_visualization_data(
    api_client: APIClient,
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
) -> dict[str, Any]:
    updated = coerce_visualization_state(visualization_state)
    render_projection = updated.get("render_projection") or {}
    if _selection_matches_projection(selection_state) and isinstance(render_projection, dict):
        chart_payload = render_projection.get("chart")
        table_payload = render_projection.get("table")
        if isinstance(chart_payload, dict):
            active_output_mode = render_projection.get("active_output_mode") or updated.get(
                "active_output_mode"
            )
            if "category" in str(active_output_mode or ""):
                updated["category_breakdown"] = chart_payload
                updated["active_tab"] = "categories"
                updated["category_year"] = chart_payload.get("year")
            else:
                updated["time_series"] = chart_payload
                updated["active_tab"] = "line"
        if table_payload is not None and (
            render_projection.get("active_output_mode") == "table"
            or updated.get("active_output_mode") == "table"
        ):
            updated["active_tab"] = "table"
        if chart_payload is not None or table_payload is not None:
            updated["status"] = _visualization_status_text(selection_state, updated)
            return updated

    unit_ids = selected_unit_ids(selection_state)
    cube_ids = selected_cube_ids(selection_state)

    if not unit_ids or not cube_ids:
        updated["time_series"] = None
        updated["category_breakdown"] = None
        updated["status"] = "Choose a place and a data theme to see a chart or table."
        return updated

    time_series = api_client.fetch_time_series(
        TimeSeriesRequest(unit_ids=unit_ids, cube_ids=cube_ids)
    )
    updated["time_series"] = time_series.model_dump(mode="json")
    updated["status"] = _visualization_status_text(selection_state, updated)

    category_cube = next(
        (cube for cube in selection_state.get("selected_cubes") or [] if cube.get("has_categories")),
        None,
    )
    if not category_cube:
        updated["category_breakdown"] = None
        return updated

    category_year = updated.get("category_year")
    if category_year is None:
        category_year = int(
            category_cube.get("end_year") or category_cube.get("start_year") or 1901
        )
        updated["category_year"] = category_year

    categories = api_client.fetch_category_breakdown(
        CategoryBreakdownRequest(
            unit_ids=unit_ids,
            cube_ids=[str(category_cube["cube_id"])],
            year=int(category_year),
        )
    )
    updated["category_breakdown"] = categories.model_dump(mode="json")
    updated["status"] = _visualization_status_text(selection_state, updated)
    return updated


def _empty_figure(message: str) -> go.Figure:
    figure = go.Figure()
    figure.update_layout(
        template="plotly_white",
        margin={"l": 30, "r": 20, "t": 40, "b": 30},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 14},
            }
        ],
    )
    return figure


def build_time_series_figure(visualization_state: dict[str, Any]) -> go.Figure:
    time_series = visualization_state.get("time_series")
    if not time_series or not time_series.get("rows"):
        return _empty_figure("No time-series data loaded yet.")

    rows = pd.DataFrame(time_series["rows"])
    if rows.empty:
        return _empty_figure("No time-series data loaded yet.")

    if rows["cube_id"].nunique() > 1:
        rows["series_name"] = rows["unit_name"].fillna("Unit") + " · " + rows["cube_label"].fillna(rows["cube_id"])
    else:
        rows["series_name"] = rows["unit_name"].fillna("Unit")

    figure = px.line(
        rows.sort_values("year"),
        x="year",
        y="value",
        color="series_name",
        markers=True,
    )
    figure.update_layout(
        template="plotly_white",
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
        xaxis_title="Year",
        yaxis_title="Value",
        legend_title="Series",
    )
    return figure


def build_category_figure(visualization_state: dict[str, Any]) -> go.Figure:
    categories = visualization_state.get("category_breakdown")
    if not categories or not categories.get("rows"):
        return _empty_figure("No category breakdown is available for the current selection.")

    rows = pd.DataFrame(categories["rows"])
    if rows.empty:
        return _empty_figure("No category breakdown is available for the current selection.")

    color = "unit_name" if rows["unit_name"].nunique() > 1 else None
    figure = px.bar(
        rows,
        x="category_label",
        y="value",
        color=color,
        barmode="group",
    )
    figure.update_layout(
        template="plotly_white",
        margin={"l": 40, "r": 20, "t": 40, "b": 80},
        xaxis_title="Category",
        yaxis_title="Value",
    )
    return figure


def build_table_payload(visualization_state: dict[str, Any]) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    render_projection = visualization_state.get("render_projection") or {}
    if isinstance(render_projection, dict) and render_projection.get("table") is not None:
        table_payload = render_projection.get("table") or {}
        rows = list(table_payload.get("rows") or [])
        columns = list(table_payload.get("columns") or [])
        if rows or columns:
            return columns, rows

    active_tab = visualization_state.get("active_tab", "line")
    source = visualization_state.get("category_breakdown") if active_tab == "categories" else visualization_state.get("time_series")
    rows = list((source or {}).get("rows") or [])
    if not rows:
        return [], []

    columns = [{"name": key.replace("_", " ").title(), "id": key} for key in rows[0].keys()]
    return columns, rows


def _render_place_profile(metadata_state: dict[str, Any]) -> Any:
    profile = metadata_state.get("place_profile")
    if not profile:
        return html.Div("Place profile will appear here.", className="text-muted")
    return html.Div(
        [
            html.H5(profile.get("name") or "Place profile"),
            html.P(profile.get("text") or "No profile text available.", className="mb-0"),
        ]
    )


def _render_key_findings(metadata_state: dict[str, Any]) -> Any:
    findings = (metadata_state.get("place_key_findings") or {}).get("items") or []
    if not findings:
        return html.Div("Key findings will appear here.", className="text-muted")
    return html.Div(
        [
            html.H5("Key Findings"),
            html.Ul([html.Li(item.get("label") or item.get("text") or "Finding") for item in findings]),
        ]
    )


def _render_unit_type(metadata_state: dict[str, Any]) -> Any:
    info = metadata_state.get("unit_type_info")
    if not info:
        return html.Div("Unit type info will appear here.", className="text-muted")
    return html.Div(
        [
            html.H5(info.get("label") or info.get("identifier") or "Unit type"),
            html.Div(f"Identifier: {info.get('identifier')}"),
            html.Div(f"Units: {info.get('unit_count')}"),
            html.P(info.get("description") or "No description available.", className="mb-0 mt-2"),
        ]
    )


def _render_data_entity(metadata_state: dict[str, Any]) -> Any:
    info = metadata_state.get("data_entity_info")
    resolution = metadata_state.get("data_entity_resolution")
    if not info and not resolution:
        return html.Div("Data entity info will appear here.", className="text-muted")
    if not info and resolution:
        result = resolution.get("result") or {}
        return html.Div(
            [
                html.H5(result.get("label") or result.get("entity_id") or "Data entity"),
                html.Div(f"Entity ID: {result.get('entity_id') or 'Unknown'}"),
                html.Div(f"Type: {result.get('entity_type') or 'Unknown'}"),
            ]
        )
    return html.Div(
        [
            html.H5(info.get("name") or info.get("entity_id") or "Data entity"),
            html.Div(f"Entity ID: {info.get('entity_id')}"),
            html.Div(f"Type: {info.get('type_name') or info.get('entity_type') or 'Unknown'}"),
            html.P(info.get("text") or "No description available.", className="mb-0 mt-2"),
        ]
    )


def register_visualization_callbacks(app, api_client: APIClient | None = None) -> None:
    client = api_client or get_api_client()

    @app.callback(
        Output("selection-state-store", "data", allow_duplicate=True),
        Input("theme-selector", "value"),
        Input("cube-selector", "value"),
        State("selection-state-store", "data"),
        prevent_initial_call=True,
    )
    def update_selection_from_controls(
        theme_id_value: str | None,
        cube_id_values: list[str] | None,
        selection_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        triggered = ctx.triggered_id
        changed = False

        if triggered == "theme-selector":
            current_theme_id = selected_theme_id(selection_state)
            if theme_id_value == current_theme_id:
                raise PreventUpdate

            if theme_id_value is None:
                selection_state["selected_theme"] = None
                selection_state["available_cubes"] = []
                selection_state["selected_cubes"] = []
                changed = True
            else:
                selected_theme = next(
                    (
                        item
                        for item in (selection_state.get("available_themes") or [])
                        if item.get("theme_id") == theme_id_value
                    ),
                    None,
                )
                if selected_theme is None:
                    raise PreventUpdate
                selection_state["selected_theme"] = selected_theme
                selection_state["selected_cubes"] = []
                if (
                    (selection_state.get("ui_projection") or {}).get("dataset_family", {}).get("theme_id")
                    == theme_id_value
                ):
                    selection_state["available_cubes"] = list(
                        (selection_state.get("ui_projection") or {}).get("available_cubes") or []
                    )
                else:
                    representative_unit_id = _representative_unit_id(selection_state)
                    if representative_unit_id is not None:
                        cubes = client.list_cubes_for_unit_theme(
                            representative_unit_id,
                            theme_id_value,
                        )
                        selection_state["available_cubes"] = [
                            item.model_dump(mode="json") for item in cubes.items
                        ]
                    else:
                        selection_state["available_cubes"] = []
                changed = True

        if triggered == "cube-selector":
            normalized_values = [str(item) for item in (cube_id_values or [])]
            current_values = selected_cube_ids(selection_state)
            if normalized_values == current_values:
                raise PreventUpdate
            cube_map = {
                item["cube_id"]: item for item in (selection_state.get("available_cubes") or [])
            }
            selection_state["selected_cubes"] = [
                cube_map[cube_id] for cube_id in normalized_values if cube_id in cube_map
            ]
            changed = True

        if not changed:
            raise PreventUpdate
        return selection_state

    @app.callback(
        Output("theme-selector", "options"),
        Output("theme-selector", "value"),
        Output("cube-selector", "options"),
        Output("cube-selector", "value"),
        Output("category-year-selector", "options"),
        Output("category-year-selector", "value"),
        Output("visualization-status", "children"),
        Input("selection-state-store", "data"),
        Input("visualization-state-store", "data"),
    )
    def sync_visualization_controls(
        selection_state_data: dict[str, Any] | None,
        visualization_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        visualization_state = coerce_visualization_state(visualization_state_data)

        theme_options = _as_dropdown_options(
            selection_state.get("available_themes") or [],
            value_key="theme_id",
            label_key="label",
        )
        cube_options = _as_dropdown_options(
            selection_state.get("available_cubes") or [],
            value_key="cube_id",
            label_key="label",
        )
        year_options = _build_category_year_options(selection_state)
        current_year = visualization_state.get("category_year")
        if year_options and current_year not in {item["value"] for item in year_options}:
            current_year = year_options[-1]["value"]

        return (
            theme_options,
            selected_theme_id(selection_state),
            cube_options,
            selected_cube_ids(selection_state),
            year_options,
            current_year,
            visualization_state.get("status"),
        )

    @app.callback(
        Output("visualization-state-store", "data", allow_duplicate=True),
        Input("selection-state-store", "data"),
        Input("category-year-selector", "value"),
        Input("visualization-tabs", "value"),
        State("visualization-state-store", "data"),
        prevent_initial_call=True,
    )
    def refresh_visualization_state(
        selection_state_data: dict[str, Any] | None,
        category_year: int | None,
        active_tab: str,
        visualization_state_data: dict[str, Any] | None,
    ):
        selection_state = coerce_selection_state(selection_state_data)
        visualization_state = coerce_visualization_state(visualization_state_data)
        visualization_state["active_tab"] = active_tab or visualization_state.get("active_tab", "line")
        if category_year is not None:
            visualization_state["category_year"] = int(category_year)
        try:
            return load_visualization_data(
                client,
                selection_state=selection_state,
                visualization_state=visualization_state,
            )
        except APIClientError as exc:
            visualization_state["time_series"] = None
            visualization_state["category_breakdown"] = None
            visualization_state["status"] = str(exc)
            return visualization_state

    @app.callback(
        Output("time-series-graph", "figure"),
        Output("category-graph", "figure"),
        Output("series-data-table", "columns"),
        Output("series-data-table", "data"),
        Output("place-profile-panel", "children"),
        Output("key-findings-panel", "children"),
        Output("unit-type-panel", "children"),
        Output("data-entity-panel", "children"),
        Input("visualization-state-store", "data"),
        Input("metadata-state-store", "data"),
    )
    def render_visualization_panels(
        visualization_state_data: dict[str, Any] | None,
        metadata_state_data: dict[str, Any] | None,
    ):
        visualization_state = coerce_visualization_state(visualization_state_data)
        metadata_state = coerce_metadata_state(metadata_state_data)
        columns, rows = build_table_payload(visualization_state)
        return (
            build_time_series_figure(visualization_state),
            build_category_figure(visualization_state),
            columns,
            rows,
            _render_place_profile(metadata_state),
            _render_key_findings(metadata_state),
            _render_unit_type(metadata_state),
            _render_data_entity(metadata_state),
        )
