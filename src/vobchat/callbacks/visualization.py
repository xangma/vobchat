# Simple Visualization Callback - Clean rewrite
# Single responsibility: Show/hide visualization based on place-state data

import pandas as pd
import json
import io
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from vobchat.tools import get_all_cube_data
from vobchat.state_schema import get_selected_units

import logging

logger = logging.getLogger(__name__)


def register_simple_visualization_callbacks(app):
    """Register simplified visualization callbacks - no more loops!"""

    @app.callback(
        Output("visualization-panel-container", "style"),
        Output("visualization-area", "style"),
        Output("cube-selector", "options"),
        Output("cube-selector", "value", allow_duplicate=True),
        Input("place-state", "data"),  # ONLY listen to place-state
        State("cube-selector", "value"),
        prevent_initial_call=True,
    )
    def handle_visualization_display(place_state, current_cube_selection):
        """Simple visualization display - show/hide based on data availability"""

        logger.info(
            f"Visualization callback triggered with place_state keys: {list(place_state.keys()) if place_state else 'None'}"
        )

        # Styles for show/hide
        visible_container = {"flex": "0 0 40%", "display": "flex"}
        visible_area = {"height": "100%", "display": "flex", "flexDirection": "column"}
        hidden_container = {"flex": "0 0 0%", "display": "none"}
        hidden_area = {"height": "100%", "display": "none", "flexDirection": "column"}

        # Always hide when there are no selected places (authoritative rule)
        places = (place_state or {}).get("places") if place_state else None
        if not places:
            logger.info("Hiding visualization - no places selected")
            return hidden_container, hidden_area, [], []

        # Check if we have data to visualize beyond places
        has_cubes = bool((place_state or {}).get("cubes"))
        has_theme = bool((place_state or {}).get("selected_theme"))
        should_show = has_cubes or has_theme

        if not should_show:
            logger.info("Hiding visualization - no data available")
            return hidden_container, hidden_area, [], []

        try:
            # Get cubes data
            cubes = place_state.get("cubes", [])

            # If no cubes but have places+theme, try to generate options
            # But skip if we're currently processing cube data (avoid unnecessary calls)
            # Also skip if we have selected_cubes field (workflow is handling it)
            if (
                not cubes
                and places
                and has_theme
                and not place_state.get("show_visualization")
                and not place_state.get("selected_cubes")
            ):
                logger.info("No cubes but have places+theme - generating cube options")
                selected_theme = place_state.get("selected_theme")
                places = place_state.get("places", [])

                if places and selected_theme:
                    try:
                        # Get first place's g_unit for theme lookup
                        first_unit = next(
                            (p.get("g_unit") for p in places if p.get("g_unit")), None
                        )
                        if first_unit:
                            from vobchat.tools import find_themes_for_unit

                            theme_cubes_json = find_themes_for_unit(str(first_unit))
                            theme_cubes_df = pd.read_json(
                                io.StringIO(theme_cubes_json), orient="records"
                            )

                            # Filter to current theme
                            if isinstance(selected_theme, str):
                                theme_data = json.loads(selected_theme)
                            else:
                                theme_data = selected_theme

                            # Support both dict and list-of-dict formats
                            ent_id = None
                            if isinstance(theme_data, dict) and "ent_id" in theme_data:
                                ent_id = theme_data["ent_id"]
                            elif (
                                isinstance(theme_data, list)
                                and len(theme_data) > 0
                                and isinstance(theme_data[0], dict)
                            ):
                                ent_id = theme_data[0].get("ent_id")

                            if ent_id:
                                current_theme_cubes = theme_cubes_df[
                                    theme_cubes_df["ent_id"] == ent_id
                                ]
                                if not current_theme_cubes.empty:
                                    cubes = current_theme_cubes.to_json(
                                        orient="records",
                                        force_ascii=False,
                                        default_handler=str,
                                    )
                                    logger.info(
                                        f"Generated {len(current_theme_cubes)} cube options from theme"
                                    )
                    except Exception as e:
                        logger.warning(f"Error generating cube options: {e}")

            # If still no cubes, show empty visualization
            if not cubes:
                logger.info("No cubes available - showing empty visualization")
                return visible_container, visible_area, [], []

            # Parse cubes data
            if isinstance(cubes, str):
                cubes_df = pd.read_json(io.StringIO(cubes), orient="records")
            else:
                cubes_df = pd.DataFrame(cubes)

            # Find cube ID column
            cube_id_col = None
            for col in ["Cube_ID", "cube_id", "CubeID"]:
                if col in cubes_df.columns:
                    cube_id_col = col
                    break

            if not cube_id_col:
                logger.warning("No cube ID column found")
                return visible_container, visible_area, [], []

            # Create cube options (deduplicated by cube id)
            cube_col = "Cube" if "Cube" in cubes_df.columns else "cube"

            # Build a stable label for each cube id
            option_labels = {}
            for _, row in cubes_df.iterrows():
                cid = row[cube_id_col]
                if cid not in option_labels:
                    option_labels[cid] = (
                        row[cube_col] if cube_col in cubes_df.columns else f"Cube {cid}"
                    )

            # Determine if multiple unit types are selected overall
            selected_unit_types = set(
                [
                    p.get("g_unit_type")
                    for p in (place_state.get("places") or [])
                    if p.get("g_unit_type")
                ]
            )

            # Build cube_id -> unit_type coverage (which unit types have this cube)
            unit_type_map = {}
            for p in place_state.get("places", []) or []:
                if p and p.get("g_unit") is not None and p.get("g_unit_type"):
                    try:
                        unit_type_map[int(p.get("g_unit"))] = p.get("g_unit_type")
                    except Exception:
                        unit_type_map[str(p.get("g_unit"))] = p.get("g_unit_type")

            coverage = {}
            if "g_unit" in cubes_df.columns:
                for _, row in cubes_df.iterrows():
                    cid = row[cube_id_col]
                    gun = row.get("g_unit")
                    try:
                        gun = int(gun)
                    except Exception:
                        pass
                    utype = unit_type_map.get(gun)
                    coverage.setdefault(cid, set())
                    if utype:
                        coverage[cid].add(utype)

            # Palette to match map unit type colors
            palette = {
                "LG_DIST": "orange",
                "MOD_DIST": "brown",
                "MOD_REG": "teal",
                "UTLA": "#4e79a7",
                "LTLA": "#f28e2b",
            }

            def label_with_indicator(text: str, utype: str | None):
                if utype and len(selected_unit_types) > 1:
                    color = palette.get(utype)
                    if color:
                        return html.Span(
                            [
                                text,
                                html.Span(
                                    style={
                                        "display": "inline-block",
                                        "width": "10px",
                                        "height": "10px",
                                        "borderRadius": "50%",
                                        "backgroundColor": color,
                                        "marginLeft": "6px",
                                        "verticalAlign": "middle",
                                    }
                                ),
                            ]
                        )
                return text

            # Build final options with indicator when cube is unique to one unit type
            options = []
            for cid, base_label in option_labels.items():
                types = list(coverage.get(cid, []))
                indicator_utype = types[0] if len(types) == 1 else None
                options.append(
                    {
                        "label": label_with_indicator(base_label, indicator_utype),
                        "value": cid,
                    }
                )

            # Determine best default selection: prefer cubes available for all selected units
            selected_units = [
                p.get("g_unit")
                for p in (place_state.get("places") or [])
                if p.get("g_unit") is not None
            ]
            best_cube = None
            if "g_unit" in cubes_df.columns and selected_units:
                # Map cube_id -> set of units it appears for
                availability = (
                    cubes_df.groupby(cube_id_col)["g_unit"]
                    .apply(lambda s: set(s.dropna().astype(int).tolist()))
                    .to_dict()
                )

                total_units = len(set(int(u) for u in selected_units))
                # First try strict intersection
                intersection = [
                    cid
                    for cid, units in availability.items()
                    if len(units) == total_units
                ]
                candidates = intersection
                if not candidates:
                    # Fallback: cubes with maximum coverage
                    max_cover = 0
                    for units in availability.values():
                        if len(units) > max_cover:
                            max_cover = len(units)
                    candidates = [
                        cid
                        for cid, units in availability.items()
                        if len(units) == max_cover
                    ]

                # Pick a deterministic best: highest End if present, else by label
                if candidates:
                    if "End" in cubes_df.columns:
                        best_row = (
                            cubes_df[cubes_df[cube_id_col].isin(candidates)]
                            .sort_values(
                                ["End", cube_col]
                                if cube_col in cubes_df.columns
                                else ["End"],
                                ascending=[False, True],
                            )
                            .iloc[0]
                        )
                        best_cube = best_row[cube_id_col]
                    else:
                        best_cube = sorted(
                            candidates, key=lambda cid: option_labels.get(cid, str(cid))
                        )[0]

            # Preserve current selection if valid; otherwise choose best default
            cube_ids_all = list(option_labels.keys())
            if current_cube_selection and all(
                cube in cube_ids_all for cube in current_cube_selection
            ):
                cube_value = current_cube_selection
            else:
                cube_value = (
                    [best_cube]
                    if best_cube
                    else (cube_ids_all[:1] if cube_ids_all else [])
                )

            logger.info(f"Showing visualization with {len(options)} cube options")
            return visible_container, visible_area, options, cube_value

        except Exception as e:
            logger.error(f"Error in visualization callback: {e}", exc_info=True)
            return hidden_container, hidden_area, [], []

    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        Input("place-state", "data"),
        prevent_initial_call=True,
    )
    def update_visualization_plot(selected_cubes, place_state):
        """Simple plot update - generate chart from selected data"""

        logger.info(f"Plot update with cubes: {selected_cubes}")

        # Empty chart function
        def empty_chart(title="No data available"):
            return go.Figure().update_layout(
                title=dict(text=title, x=0.5, xanchor="center", pad=dict(t=0, b=4)),
                xaxis_title="Year",
                yaxis_title="Value",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.0,
                    xanchor="left",
                    x=0.0,
                    title=dict(text="Series"),
                ),
                margin={"l": 50, "r": 50, "t": 80, "b": 50},
                annotations=[
                    {
                        "text": title,
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "xanchor": "center",
                        "yanchor": "middle",
                        "showarrow": False,
                        "font": {"size": 16, "color": "gray"},
                    }
                ],
            )

        # Skip work entirely when the panel would be hidden
        if not place_state or not place_state.get("places"):
            raise PreventUpdate
        has_cubes = bool(place_state.get("cubes"))
        has_theme = bool(place_state.get("selected_theme"))
        if not (has_cubes or has_theme):
            raise PreventUpdate

        if not selected_cubes:
            return empty_chart("No data filters selected")

        try:
            # Get selected units
            places = place_state.get("places", [])
            selected_units = [p.get("g_unit") for p in places if p.get("g_unit")]

            if not selected_units:
                return empty_chart("No valid areas found")

            # Ensure selected_cubes is a list
            if not isinstance(selected_cubes, list):
                selected_cubes = [selected_cubes]

            # Fetch data for each unit
            all_data_list = []
            for g_unit in selected_units:
                try:
                    cube_data_json = get_all_cube_data.invoke(
                        {"g_unit": str(g_unit), "cube_ids": selected_cubes}
                    )
                    cube_data_df = pd.read_json(
                        io.StringIO(cube_data_json), orient="records"
                    )
                    if not cube_data_df.empty:
                        all_data_list.append(cube_data_df)
                except Exception as e:
                    logger.warning(f"Error fetching data for unit {g_unit}: {e}")
                    continue

            if not all_data_list:
                return empty_chart("No data available for selected areas")

            # Combine and process data
            all_data_df = pd.concat(all_data_list, ignore_index=True)

            # Melt data for plotting (preserve g_unit for unit_type mapping)
            id_vars = ["g_unit", "g_name", "year"]
            value_vars = [col for col in all_data_df.columns if col not in id_vars]

            if not value_vars:
                return empty_chart("No data columns found")

            chart_data = pd.melt(
                all_data_df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name="measurement",
                value_name="value",
            )

            # Clean data
            chart_data = chart_data.dropna(subset=["value"])
            chart_data["year"] = pd.to_numeric(chart_data["year"], errors="coerce")
            chart_data = chart_data.dropna(subset=["year"])

            if chart_data.empty:
                return empty_chart("No valid data after filtering")

            # Create display names and plot
            chart_data["display_name"] = (
                chart_data["g_name"] + " - " + chart_data["measurement"]
            )

            chart_data = chart_data.sort_values(["g_name", "measurement", "year"])

            fig = px.line(
                chart_data,
                x="year",
                y="value",
                color="display_name",
                title="Historical Data Visualization",
                markers=True,
            )

            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value",
                hovermode="x unified",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.0,
                    xanchor="left",
                    x=0.0,
                    title=dict(text="Series"),
                ),
                title=dict(
                    text="Historical Data Visualization",
                    x=0.5,
                    xanchor="center",
                    pad=dict(t=0, b=4),
                ),
                margin={"l": 50, "r": 50, "t": 80, "b": 50},
                height=None,
                autosize=True,
                legend_title_text="Series",
            )

            if (chart_data["value"].dropna() >= 0).all():
                fig.update_yaxes(rangemode="tozero")

            return fig

        except Exception as e:
            logger.error(f"Error updating plot: {e}", exc_info=True)
            return empty_chart(f"Error: {str(e)}")

    @app.callback(
        Output("visualization-panel-container", "style", allow_duplicate=True),
        Output("visualization-area", "style", allow_duplicate=True),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("data-plot", "figure", allow_duplicate=True),
        Input("clear-plot-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def clear_visualization(n_clicks):
        """Simple clear - just hide the visualization"""
        if n_clicks:
            hidden_container = {"flex": "0 0 0%", "display": "none"}
            hidden_area = {
                "height": "100%",
                "display": "none",
                "flexDirection": "column",
            }
            return hidden_container, hidden_area, [], go.Figure()
        raise PreventUpdate
