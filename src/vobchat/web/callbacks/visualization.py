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
    first_selected_unit_id,
    selected_cube_ids,
    selected_theme_id,
    selected_unit_ids,
)


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


def load_visualization_data(
    api_client: APIClient,
    *,
    selection_state: dict[str, Any],
    visualization_state: dict[str, Any],
) -> dict[str, Any]:
    updated = coerce_visualization_state(visualization_state)
    unit_ids = selected_unit_ids(selection_state)
    cube_ids = selected_cube_ids(selection_state)

    if not unit_ids or not cube_ids:
        updated["time_series"] = None
        updated["category_breakdown"] = None
        updated["status"] = "Choose a place, theme, and cube to load chart data."
        return updated

    time_series = api_client.fetch_time_series(
        TimeSeriesRequest(unit_ids=unit_ids, cube_ids=cube_ids)
    )
    updated["time_series"] = time_series.model_dump(mode="json")
    updated["status"] = (
        f"Loaded {time_series.row_count} time-series rows for {len(unit_ids)} "
        f"unit(s) and {len(cube_ids)} cube(s)."
    )

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
                first_unit_id = first_selected_unit_id(selection_state)
                if first_unit_id is not None:
                    cubes = client.list_cubes_for_unit_theme(first_unit_id, theme_id_value)
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
