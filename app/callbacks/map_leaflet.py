# callbacks.py
import json
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
from dash import (
    no_update, callback_context as ctx,
    Input, Output, State, ALL
)
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from utils.constants import UNIT_TYPES
from mapinit import get_polygons_by_type

logger = logging.getLogger(__name__)

DEFAULT_MIN_YEAR = 1800
CURRENT_YEAR = datetime.now().year


def normalize_year(year):
    return min(year, CURRENT_YEAR)


def register_map_leaflet_callbacks(app, date_ranges_df):

    @app.callback(
        # Only ONE Output: the updated store
        Output("map-state", "data"),
        Output("counts-store", "data", allow_duplicate=True),
        [
            Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
            Input('reset-selections', 'n_clicks'),
            Input('year-range-slider', 'value'),   # read user’s chosen years
            Input('ctrl-pressed-store', 'data'),
            Input("geojson-layer", "n_clicks"),
        ],
        [
            State("map-state", "data"),
            State({'type': 'unit-filter', 'unit': ALL}, 'id'),
            State("geojson-layer", "clickData"),
            State("geojson-layer", "hideout"),
            State("counts-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def update_map_state(
        unit_filter_clicks,
        reset_selections_clicks,
        chosen_year_range,
        ctrl_pressed,
        geojson_n_clicks,
        map_state,
        button_ids,
        geojson_clickData,
        geojson_hideout,
        counts,
    ):
        """
        This callback merges ALL user interactions into one updated map-state.
        Dash sees only 1 property ("map-state.data") changed => single callback invocation.
        """
        triggered_prop_ids = [t["prop_id"] for t in ctx.triggered]
        if not triggered_prop_ids:
            raise PreventUpdate

        logger.info("update_map_state triggered by %s", triggered_prop_ids)

        # If map_state is None or missing keys, initialize defaults.
        new_map_state = (map_state or {}).copy()
        if "unit_types" not in new_map_state:
            new_map_state["unit_types"] = ["MOD_REG"]
        if "selected_polygons" not in new_map_state:
            new_map_state["selected_polygons"] = []

        # 1) Handle Filter / Year-Range changes
        filter_triggered = False

        # A) Reset filters
        if "reset-selections.n_clicks" in triggered_prop_ids and reset_selections_clicks:
            logger.debug("reset-selections: clearing to ['MOD_REG']")
            new_map_state["unit_types"] = ["MOD_REG"]
            new_map_state["selected_polygons"] = []
            counts = {}
            filter_triggered = True

        # B) Check if we clicked or ctrl-clicked a unit-filter button
        unit_type_trigs = [t for t in triggered_prop_ids if "unit-filter" in t]
        for i, t in enumerate(unit_type_trigs):
            dict_part = t.split(".")[0]
            try:
                dict_part = json.loads(dict_part)
            except:
                dict_part = None

            if dict_part and dict_part.get("type") == "unit-filter":
                clicked_type = dict_part["unit"]
                prev_types = set(new_map_state.get("unit_types", ["MOD_REG"]))
                current_types = set(prev_types)

                if ctrl_pressed:
                    # Toggle the clicked unit type.
                    if clicked_type in current_types:
                        current_types.remove(clicked_type)
                        if not current_types:
                            current_types = {"MOD_REG"}
                    else:
                        current_types.add(clicked_type)
                    ctrl_pressed = False  # we used ctrl
                else:
                    # Single choice: set to only the clicked type.
                    current_types = {clicked_type}

                new_map_state["unit_types"] = list(current_types)
                # Removed the code that cleared "selected_polygons" so that selections are retained.
                filter_triggered = True

        # C) If the user changed the year-range slider
        if "year-range-slider.value" in triggered_prop_ids and chosen_year_range:
            y0, y1 = chosen_year_range
            new_map_state["year_range"] = (
                normalize_year(y0),
                normalize_year(y1)
            )
            filter_triggered = True

        if filter_triggered:
            logger.debug("Filters updated => stored in map-state")

        # 3) Polygon selection logic
        # If geojson-layer clicked
        elif "geojson-layer.n_clicks" in triggered_prop_ids and geojson_n_clicks:
            if geojson_clickData:
                fid = geojson_clickData.get("id")
                if fid is not None:
                    selected_ids = new_map_state.get("selected_polygons", [])
                    if fid in selected_ids:
                        selected_ids.remove(fid)
                    else:
                        selected_ids.append(fid)
                    new_map_state["selected_polygons"] = selected_ids

        return new_map_state, counts

    @app.callback(
        [
            Output('year-range-container', 'style'),
            Output('year-range-slider', 'min'),
            Output('year-range-slider', 'max'),
            Output('year-range-slider', 'marks'),
            Output('geojson-layer', 'data'),
            Output('geojson-layer', 'hideout'),
            Output('debug-output', 'children'),
            # Update the style property for each unit-filter button.
            Output({'type': 'unit-filter', 'unit': ALL}, 'style'),
            # Store the counts of selected polygons for each unit type.
            Output("counts-store", "data", allow_duplicate=True),
        ],
        Input("map-state", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        State("app-state", "data"),
        State("counts-store", "data"),
        # Removed prevent_initial_call=True so the callback runs on page load
    )
    def render_map_display(map_state, button_ids, app_state, counts):
        """
        Fires each time map_state.data changes.
        Builds final UI: slider bounds, geojson, debug text, button styling, etc.
        Also updates each unit-filter button's label to include a badge with the count
        of selected polygons (if > 0) shown as a circle.
        """
        # Initialize default map_state if None.
        if not map_state:
            map_state = {"unit_types": ["MOD_REG"], "selected_polygons": []}

        # (A) Show/hide slider based on whether any unit type requires a year filter.
        unit_types = map_state.get("unit_types", ["MOD_REG"])
        timeless_unit_types = [k for k,v in UNIT_TYPES.items() if v['timeless']]
        if any(ut not in timeless_unit_types for ut in unit_types):
            container_style = {'display': 'block'}
        else:
            container_style = {'display': 'none'}

        # (B) Determine slider min/max values (using defaults/fallback).
        min_year = DEFAULT_MIN_YEAR
        max_year = CURRENT_YEAR
        step = max(1, (max_year - min_year) // 10)
        slider_marks = {str(y): str(y) for y in range(min_year, max_year+1, step)}

        # (C) Build polygons for each active unit type.
        year_range = map_state.get("year_range")
        gdfs = []
        timeless_unit_types = [
            k for k, v in UNIT_TYPES.items() if v['timeless']]
        for ut in unit_types:
            if ut not in timeless_unit_types and year_range:
                gdf = get_polygons_by_type(ut, year_range[0], year_range[1])
            else:
                gdf = get_polygons_by_type(ut)
            gdfs.append(gdf)

        if gdfs:
            combined = pd.concat(gdfs)
            if combined.empty:
                geojson_out = {"type": "FeatureCollection", "features": []}
                debug_msg = "No polygons found."
            else:
                geojson_out = json.loads(combined.to_json())
                debug_msg = f"Showing {len(combined)} polygons for {unit_types}"
        else:
            geojson_out = {"type": "FeatureCollection", "features": []}
            debug_msg = "No polygons loaded."

        # (D) Set hideout with selected polygons.
        new_hideout = {"selected": map_state.get("selected_polygons", [])}

        # (E) Button styles:
        # Mapping from unit type to colour (matching the GeoJSON outline colours).
        unit_colors = {k: v['color'] for k, v in UNIT_TYPES.items()}
        active_set = set(unit_types)
        button_styles = []
        for b in button_ids:
            unit = b["unit"]
            if unit in active_set:
                # Active buttons get the unit's specific colour with a filled background.
                style = {
                    'backgroundColor': unit_colors.get(unit, 'blue'),
                    'borderColor': unit_colors.get(unit, 'blue'),
                    'color': 'white'
                }
            else:
                # Unselected buttons now have a white background, but their outline (border)
                # is still the unit's specific color and the text is in that color.
                style = {
                    'backgroundColor': 'white',
                    'borderColor': unit_colors.get(unit, 'blue'),
                }
            button_styles.append(style)

        # (F) Update button labels to include a badge showing the count of selected polygons.
        # We'll derive the count by checking which features in geojson_out have been selected.
        for feature in geojson_out.get("features", []):
            # Determine the unit type from the feature properties.
            feat_unit = feature["properties"].get("g_unit_type", "MOD_REG")
            if feature["id"] in map_state.get("selected_polygons", []):
                if not counts.get(feat_unit + '_g_units'):
                    counts[feat_unit + '_g_units'] = []
                if feature["id"] not in counts[feat_unit + '_g_units']:
                    counts[feat_unit] = counts.get(feat_unit, 0) + 1
                    counts[feat_unit + '_g_units'].append(feature["id"])

        return (
            container_style,
            min_year,
            max_year,
            slider_marks,
            geojson_out,
            new_hideout,
            debug_msg,
            button_styles,
            counts,
        )

    @app.callback(
        Output({'type': 'unit-filter', 'unit': ALL},
               'children'),
        Input("counts-store", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
    )
    def update_buttons_w_counts(counts, button_ids):
        """
        Update the children (label) property for each unit-filter button.
        """
        button_children = []
        for b in button_ids:
            unit = b["unit"]
            label = UNIT_TYPES.get(unit).get("long_name", unit)
            count = counts.get(unit, 0)
            if count > 0:
                # Use dbc.Badge to display the count in a circle.
                children = [
                    label,
                    dbc.Badge(
                        str(count),
                        color="light",
                        text_color="dark",
                        pill=True,
                        className="ms-1",
                        style={"fontSize": "0.8em", "verticalAlign": "middle"}
                    )
                ]
            else:
                children = label
            button_children.append(children)
        return button_children
