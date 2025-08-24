# Simple Visualization Callback - Clean rewrite
# Single responsibility: Show/hide visualization based on place-state data

import pandas as pd
import json
import io
import plotly.graph_objects as go
import plotly.express as px
import re
from dash import no_update, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from vobchat.tools import get_all_cube_data
from vobchat.state_schema import get_selected_units
from plotly.subplots import make_subplots

import logging

logger = logging.getLogger(__name__)


def register_simple_visualization_callbacks(app):
    """Register simplified visualization callbacks - no more loops!"""

    @app.callback(
        Output("visualization-panel-container", "style"),
        Output("visualization-area", "style"),
        Output("cube-selector", "options"),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("viz-year-slider-container", "style", allow_duplicate=True),
        Output("cube-selector", "multi", allow_duplicate=True),
        Input("place-state", "data"),  # ONLY listen to place-state
        Input("viz-tabs", "value"),
        State("cube-selector", "value"),
        prevent_initial_call=True,
    )
    def handle_visualization_display(place_state, current_tab, current_cube_selection):
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
            return hidden_container, hidden_area, [], [], {"display": "none"}, True

        # Check if we have data to visualize beyond places
        has_cubes = bool((place_state or {}).get("cubes"))
        has_theme = bool((place_state or {}).get("selected_theme"))
        should_show = has_cubes or has_theme

        if not should_show:
            logger.info("Hiding visualization - no data available")
            return hidden_container, hidden_area, [], [], {"display": "none"}, True

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
                        # Get first place's g_unit for cube lookup under the selected theme
                        first_unit = next(
                            (p.get("g_unit") for p in places if p.get("g_unit")), None
                        )
                        if first_unit:
                            # Parse the selected theme to extract ent_id
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
                                # Fetch cubes for this unit + theme
                                try:
                                    from vobchat.tools import find_cubes_for_unit_theme

                                    # LangChain tool: invoke with a single input dict
                                    cubes_json = find_cubes_for_unit_theme.invoke(
                                        {
                                            "g_unit": str(first_unit),
                                            "theme_id": str(ent_id),
                                        }
                                    )
                                    cubes_df = pd.read_json(
                                        io.StringIO(cubes_json), orient="records"
                                    )
                                    if not cubes_df.empty:
                                        cubes = cubes_json
                                        logger.info(
                                            f"Generated {len(cubes_df)} cube options from unit+theme"
                                        )
                                except Exception as e:
                                    logger.warning(
                                        f"Error fetching cubes for unit {first_unit} and theme {ent_id}: {e}"
                                    )
                    except Exception as e:
                        logger.warning(f"Error generating cube options: {e}")

            # If still no cubes, show empty visualization
            if not cubes:
                logger.info("No cubes available - showing empty visualization")
                return (
                    visible_container,
                    visible_area,
                    [],
                    [],
                    {"display": "none"},
                    True,
                )

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
                return (
                    visible_container,
                    visible_area,
                    [],
                    [],
                    {"display": "none"},
                    True,
                )

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
                        "value": str(cid),
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
            cube_ids_all_str = [str(cid) for cid in cube_ids_all]
            if current_cube_selection and isinstance(current_cube_selection, list):
                # Sanitize current selection and keep only valid string IDs
                current_sanitized = [
                    str(c) for c in current_cube_selection if c is not None
                ]
                current_sanitized = [
                    c for c in current_sanitized if c in cube_ids_all_str
                ]
                if current_sanitized:
                    cube_value = current_sanitized
                else:
                    cube_value = (
                        [str(best_cube)]
                        if best_cube is not None
                        else (cube_ids_all_str[:1] if cube_ids_all_str else [])
                    )
            else:
                cube_value = (
                    [str(best_cube)]
                    if best_cube is not None
                    else (cube_ids_all_str[:1] if cube_ids_all_str else [])
                )

            # Filter options and control multi-select depending on active tab
            multi_flag = True
            if current_tab == "categories":
                multi_flag = False
                way_re = re.compile(r".*?_\d*WAY.*$", re.IGNORECASE)
                filtered_ids = [
                    str(cid)
                    for cid, lbl in option_labels.items()
                    if way_re.search(str(cid))
                ]
                if filtered_ids:
                    opt_set = set(filtered_ids)
                    options = [opt for opt in options if opt.get("value") in opt_set]
                    allowed = set(opt["value"] for opt in options)
                    # Ensure a single scalar value when exclusive
                    if isinstance(cube_value, list):
                        cube_value = [v for v in (cube_value or []) if v in allowed]
                    if not cube_value:
                        cube_value = [options[0]["value"]] if options else []
                    cube_value = (
                        cube_value[0] if isinstance(cube_value, list) else cube_value
                    )
                year_style = {"display": "block"}
            else:
                multi_flag = True
                # Ensure list for multi-select mode
                if cube_value is None:
                    cube_value = []
                elif not isinstance(cube_value, list):
                    cube_value = [cube_value]
                year_style = {"display": "none"}
            logger.info(f"Showing visualization with {len(options)} cube options")
            return (
                visible_container,
                visible_area,
                options,
                cube_value,
                year_style,
                multi_flag,
            )

        except Exception as e:
            logger.error(f"Error in visualization callback: {e}", exc_info=True)
            return hidden_container, hidden_area, [], [], {"display": "none"}, True

    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Output("category-plot", "figure", allow_duplicate=True),
        Output("categories-tab", "disabled", allow_duplicate=True),
        Output("viz-year-slider", "marks", allow_duplicate=True),
        Output("viz-year-slider", "min", allow_duplicate=True),
        Output("viz-year-slider", "max", allow_duplicate=True),
        Output("viz-year-slider", "value", allow_duplicate=True),
        Output("viz-year-slider", "step", allow_duplicate=True),
        Input("cube-selector", "value"),
        Input("place-state", "data"),
        Input("viz-tabs", "value"),
        Input("viz-year-slider", "value"),
        State("cube-selector", "options"),
        prevent_initial_call=True,
    )
    def update_visualization_plot(
        selected_cubes, place_state, current_tab, selected_year, cube_options
    ):
        """Simple plot update - generate chart from selected data"""

        logger.info(f"Plot update with cubes: {selected_cubes}")

        # Empty chart function
        def empty_chart(title="No data available"):
            return go.Figure().update_layout(
                title=None,
                xaxis_title="Year",
                yaxis_title="Value",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0.0,
                ),
                margin={"l": 50, "r": 50, "t": 50, "b": 50},
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
            return (
                empty_chart("No data filters selected"),
                go.Figure(),
                True,
                {},
                0,
                0,
                no_update,
                None,
            )

        try:
            # Get selected units
            places = place_state.get("places", [])
            selected_units = [p.get("g_unit") for p in places if p.get("g_unit")]

            if not selected_units:
                return (
                    empty_chart("No valid areas found"),
                    go.Figure(),
                    True,
                    {},
                    0,
                    0,
                    no_update,
                    None,
                )

            # Ensure selected_cubes is a list of strings and drop Nones
            if not isinstance(selected_cubes, list):
                selected_cubes = [selected_cubes]
            selected_cubes = [str(c) for c in selected_cubes if c is not None]
            if not selected_cubes:
                return (
                    empty_chart("No data filters selected"),
                    go.Figure(),
                    True,
                    {},
                    0,
                    0,
                    no_update,
                    None,
                )

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
                return (
                    empty_chart("No data available for selected areas"),
                    go.Figure(),
                    True,
                    {},
                    0,
                    0,
                    no_update,
                    None,
                )

            # Combine and process data
            all_data_df = pd.concat(all_data_list, ignore_index=True)

            # Melt data for plotting (preserve g_unit for unit_type mapping)
            id_vars = ["g_unit", "g_name", "year"]
            value_vars = [col for col in all_data_df.columns if col not in id_vars]

            if not value_vars:
                return (
                    empty_chart("No data columns found"),
                    go.Figure(),
                    True,
                    {},
                    0,
                    0,
                    no_update,
                    None,
                )

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
                return (
                    empty_chart("No valid data after filtering"),
                    go.Figure(),
                    True,
                    {},
                    0,
                    0,
                    no_update,
                    None,
                )

            # Create display names and ensure categorical fields are strings
            chart_data["display_name"] = chart_data["measurement"].astype(str)
            try:
                chart_data["g_name"] = chart_data["g_name"].astype(str)
            except Exception:
                pass

            chart_data = chart_data.sort_values(["g_name", "measurement", "year"])

            fig = px.line(
                chart_data,
                x="year",
                y="value",
                color="display_name",
                line_dash="g_name",  # Different marker per place
                markers=True,
                template=go.layout.Template(),  # Use empty template to avoid cascade issues
            )

            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value",
                hovermode="x unified",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,  # above the plot area
                    xanchor="left",
                    x=0.0,
                ),
                margin={"l": 50, "r": 50, "t": 50, "b": 50},
                autosize=True,
                legend_title_text="Series (marker = place)",
            )

            if (chart_data["value"].dropna() >= 0).all():
                fig.update_yaxes(rangemode="tozero")

            # Build category pie (enable tab only if *_WAY:<category> measurements exist)
            cat_fig = go.Figure()
            cat_disabled = True
            try:
                # Choose first selected place and latest year
                first_place = places[0] if places else None
                target_unit = first_place.get("g_unit") if first_place else None
                target_name = (
                    (first_place.get("g_name") if first_place else None)
                    or (first_place.get("name") if first_place else None)
                    or (str(target_unit) if target_unit is not None else "Selection")
                )
                sub = chart_data.copy()
                if target_unit is not None:
                    sub = sub[sub["g_unit"] == target_unit]
                sub = sub.dropna(subset=["year"]).copy()
                sub["year"] = pd.to_numeric(sub["year"], errors="coerce")
                sub = sub.dropna(subset=["year"])
                if not sub.empty:
                    latest_year = int(sub["year"].max())
                    suby = sub[sub["year"] == latest_year].copy()
                    way_regex = re.compile(r".*?_\d*WAY.*$", re.IGNORECASE)
                    mask = suby["measurement"].astype(str).str.contains(way_regex)
                    suby = suby[mask]
                    suby["value"] = pd.to_numeric(suby["value"], errors="coerce")
                    suby = suby.dropna(subset=["value"])
                    if not suby.empty:
                        parts = (
                            suby["measurement"]
                            .astype(str)
                            .str.split(":", n=1, expand=True)
                        )
                        suby["prefix"] = parts[0]
                        suby["category"] = parts[1]
                        counts = (
                            suby.groupby("prefix").size().sort_values(ascending=False)
                        )
                        best_prefix = counts.index[0]
                        best = suby[suby["prefix"] == best_prefix]
                        if not best.empty:
                            cat_fig = px.pie(
                                best,
                                names="category",
                                values="value",
                            )
                            cat_fig.update_layout(
                                legend_title_text="Category",
                                legend=dict(
                                    orientation="h",
                                    yanchor="top",
                                    y=0.98,
                                    xanchor="left",
                                    x=0.0,
                                ),
                                margin={"l": 50, "r": 50, "t": 50, "b": 50},
                            )
                            cat_disabled = False
            except Exception:
                # Keep categories tab disabled if any error occurs
                cat_fig = go.Figure()
                cat_disabled = True

            # Augment: build pies for all selected places when categories tab is active and add year options
            years = sorted(chart_data["year"].dropna().astype(int).unique().tolist())
            year_options = [{"label": str(y), "value": int(y)} for y in years]
            current_year_value = years[-1] if years else None
            try:
                if current_tab == "categories" and years:
                    year_to_use = (
                        int(selected_year)
                        if (selected_year and int(selected_year) in years)
                        else years[-1]
                    )
                    current_year_value = year_to_use
                    way_regex = re.compile(r".*?_\d*WAY.*$", re.IGNORECASE)
                    sub = chart_data.copy()
                    sub = sub[sub["year"] == year_to_use]
                    mask = sub["measurement"].astype(str).str.contains(way_regex)
                    sub = sub[mask].copy()
                    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
                    sub = sub.dropna(subset=["value"])
                    if not sub.empty:
                        parts = (
                            sub["measurement"]
                            .astype(str)
                            .str.split(":", n=1, expand=True)
                        )
                        sub["prefix"] = parts[0]
                        sub["category"] = parts[1]
                        places_rows = (
                            sub[["g_unit", "g_name"]]
                            .drop_duplicates()
                            .reset_index(drop=True)
                        )
                        n = len(places_rows)
                        cols = 3 if n >= 3 else (2 if n == 2 else 1)
                        rows = (n + cols - 1) // cols
                        # Build subplot titles to indicate which pie corresponds to which place
                        place_names = [str(nm) for nm in places_rows["g_name"].tolist()]
                        total_cells = rows * cols
                        subplot_titles = [place_names[i] if i < len(place_names) else "" for i in range(total_cells)]
                        fig_multi = make_subplots(
                            rows=rows,
                            cols=cols,
                            specs=[[{"type": "domain"}] * cols for _ in range(rows)],
                            subplot_titles=subplot_titles,
                        )
                        r = c = 1
                        pies_added = 0
                        for _, prow in places_rows.iterrows():
                            pid = prow["g_unit"]
                            pname = prow["g_name"]
                            psub = sub[sub["g_unit"] == pid]
                            if psub.empty:
                                continue
                            counts = (
                                psub.groupby("prefix")
                                .size()
                                .sort_values(ascending=False)
                            )
                            best_prefix = counts.index[0]
                            best = psub[psub["prefix"] == best_prefix]
                            if best.empty:
                                continue
                            fig_multi.add_trace(
                                go.Pie(
                                    labels=best["category"],
                                    values=best["value"],
                                    showlegend=(r == 1 and c == 1),
                                ),
                                row=r,
                                col=c,
                            )
                            pies_added += 1
                            c += 1
                            if c > cols:
                                c = 1
                                r += 1
                        if pies_added:
                            fig_multi.update_layout(
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.12,
                                    xanchor="left",
                                    x=0.0,
                                    title=dict(text="Category"),
                                ),
                                title=None,
                                margin={"l": 50, "r": 50, "t": 90, "b": 40},
                            )
                            cat_fig = fig_multi
                            cat_disabled = False
            except Exception:
                pass

            # Prepare slider marks/min/max from available years
            marks = {int(y): str(int(y)) for y in years} if years else {}
            min_y = int(years[0]) if years else 0
            max_y = int(years[-1]) if years else 0

            # Final disabled state: enable if any cube option label contains _*WAY*
            disabled_final = cat_disabled
            try:
                if cube_options:
                    way_opt_re = re.compile(r".*?_\d*WAY.*$", re.IGNORECASE)
                    has_way_option = any(
                        way_opt_re.search(str(opt.get("value"))) for opt in cube_options
                    )
                    disabled_final = not has_way_option
            except Exception:
                pass
            return (
                fig,
                cat_fig,
                disabled_final,
                marks,
                min_y,
                max_y,
                current_year_value,
                None,
            )

        except Exception as e:
            logger.error(f"Error updating plot: {e}", exc_info=True)
            return (
                empty_chart(f"Error: {str(e)}"),
                go.Figure(),
                True,
                {},
                0,
                0,
                no_update,
                None,
            )

    @app.callback(
        Output("visualization-panel-container", "style", allow_duplicate=True),
        Output("visualization-area", "style", allow_duplicate=True),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("data-plot", "figure", allow_duplicate=True),
        Output("category-plot", "figure", allow_duplicate=True),
        Output("categories-tab", "disabled", allow_duplicate=True),
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
            return hidden_container, hidden_area, [], go.Figure(), go.Figure(), True
        raise PreventUpdate
