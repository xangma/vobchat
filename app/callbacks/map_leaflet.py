# callbacks.py
import json
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
import dash
from dash import (
    no_update, callback_context as ctx,
    Input, Output, State, ALL
)
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from ..utils.constants import UNIT_TYPES
from ..mapinit import get_polygons_by_type

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
        # Update the toggle-unselected button
        Output("toggle-unselected", "children"),
        [
            Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
            Input('reset-selections', 'n_clicks'),
            Input('year-range-slider', 'value'),
            Input('ctrl-pressed-store', 'data'),
            Input("geojson-layer", "n_clicks"),
            Input("toggle-unselected", "n_clicks"),
        ],
        [
            State("map-state", "data"),
            State({'type': 'unit-filter', 'unit': ALL}, 'id'),
            State("geojson-layer", "clickData"),
            State("geojson-layer", "hideout"),
            State('geojson-layer', 'data'),
            State("counts-store", "data"),
            # update the toggle-unselected button
            State("toggle-unselected", "children"),
        ],
        prevent_initial_call=True,
    )
    def update_map_state(
        unit_filter_clicks,
        reset_selections_clicks,
        chosen_year_range,
        ctrl_pressed,
        geojson_n_clicks,
        toggle_unselected_clicks,
        map_state,
        button_ids,
        geojson_clickData,
        geojson_hideout,
        geojson_data,
        counts,
        toggle_unselected_children
    ):
        """
        This callback merges ALL user interactions into one updated map-state.
        Dash sees only 1 property ("map-state.data") changed => single callback invocation.
        """
        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
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
        if "selected_polygons_unit_types" not in new_map_state:
            new_map_state["selected_polygons_unit_types"] = []

        # 1) Handle Filter / Year-Range changes
        filter_triggered = False

        # A) Reset filters
        if "reset-selections.n_clicks" in triggered_prop_ids and reset_selections_clicks:
            logger.debug("reset-selections: clearing to ['MOD_REG']")
            new_map_state["unit_types"] = ["MOD_REG"]
            new_map_state["selected_polygons"] = []
            new_map_state["selected_polygons_unit_types"] = []
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
                unit_type = geojson_clickData['properties']['g_unit_type']
                if fid is not None:
                    selected_ids = new_map_state.get("selected_polygons", [])
                    selected_units = new_map_state.get("selected_polygons_unit_types", [])
                    if fid in selected_ids:
                        fid_index = selected_ids.index(fid)
                        selected_ids.pop(fid_index)
                        selected_units.pop(fid_index)
                    else:
                        selected_ids.append(fid)
                        selected_units.append(unit_type)
                    new_map_state["selected_polygons"] = selected_ids
                    new_map_state["selected_polygons_unit_types"] = selected_units

        # (D) Apply toggle: hide unselected polygons if the toggle is off.
        # The toggle returns ['show'] when on, otherwise an empty list.
        elif "toggle-unselected.n_clicks" in triggered_prop_ids:
            if map_state["show_unselected"]:
                new_map_state["show_unselected"] = False
                toggle_unselected_children = "Show unselected polygons"
                # debug_msg += " | Unselected polygons hidden."
            else:
                new_map_state["show_unselected"] = True
                toggle_unselected_children = "Hide unselected polygons"
                # debug_msg += " | Unselected polygons shown."

        # Return the updated map state but NOT the geojson data
        # Let the render_map_display callback handle updating the geojson
        return new_map_state, counts, toggle_unselected_children

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
            # Store the counts of selected polygons for each unit type
            Output("counts-store", "data", allow_duplicate=True),
        ],
        Input("map-state", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        State("app-state", "data"),
        State("counts-store", "data"),
        State('geojson-layer', 'data'),

        # Removed prevent_initial_call=True so the callback runs on page load
    )
    def render_map_display(map_state, button_ids, app_state, counts, current_geojson):
        """
        Fires each time map_state.data changes.
        Builds final UI: slider bounds, geojson, debug text, button styling, etc.
        Also updates each unit-filter button's label to include a badge with the count
        of selected polygons (if > 0) shown as a circle.
        """
        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
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
        
        # Check if we need to fetch polygons or if we can reuse existing ones
        need_to_fetch = True
        current_features = []
        
        if current_geojson and 'features' in current_geojson:
            current_features = current_geojson['features']
            
            # Get all feature IDs in the current geojson
            current_ids = set()
            current_unit_types = set()
            for feature in current_features:
                if 'id' in feature and 'properties' in feature and 'g_unit_type' in feature['properties']:
                    current_ids.add(feature['id'])
                    current_unit_types.add(feature['properties']['g_unit_type'])
            
            # If all requested unit types are already in the current geojson,
            # and we're not changing the year range for non-timeless types, we can skip fetching
            if set(unit_types).issubset(current_unit_types):
                non_timeless_requested = [ut for ut in unit_types if ut not in timeless_unit_types]
                if not non_timeless_requested or not year_range:
                    need_to_fetch = False
                    geojson_out = current_geojson
                    debug_msg = f"Reusing {len(current_features)} polygons for {unit_types}"
        
        if need_to_fetch:
            gdfs = []
            for ut in unit_types:
                if ut not in timeless_unit_types and year_range:
                    gdf = get_polygons_by_type(ut, year_range[0], year_range[1])
                else:
                    gdf = get_polygons_by_type(ut)
                gdfs.append(gdf)

            if gdfs:
                combined = pd.concat(gdfs)
                combined = combined.drop_duplicates()
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
            unit_color = unit_colors.get(unit, 'blue')
            # Pass the unit color as a CSS variable (--unit-color) so the CSS can use it on hover.
            style = {
                '--unit-color': unit_color,
                'borderColor': unit_color,
                'backgroundColor': 'white',
                'color': '#333',  # Use a dark/grey color for unselected text.
                'transition': 'background-color 0.3s, color 0.3s'
            }
            if unit in active_set:
                style.update({
                    'backgroundColor': unit_color,
                    'color': 'white'
                })
            button_styles.append(style)

        # (F) Update button labels to include a badge showing the count of selected polygons.
        # We'll derive the count by checking which features in geojson_out have been selected.
        counts = counts or {}
        for unit_type in UNIT_TYPES.keys():
            counts[unit_type + '_g_units'] = counts.get(unit_type + '_g_units', [])
            counts[unit_type] = 0
            
        for feature in geojson_out.get("features", []):
            # Determine the unit type from the feature properties.
            feat_unit = feature["properties"].get("g_unit_type", "MOD_REG")
            if feature["id"] in map_state.get("selected_polygons", []):
                # if not counts.get(feat_unit + '_g_units'):
                #     counts[feat_unit + '_g_units'] = []
                if feature["id"] not in counts[feat_unit + '_g_units']:
                    counts[feat_unit] = counts.get(feat_unit, 0) + 1
                    counts[feat_unit + '_g_units'].append(feature["id"])

        if not map_state.get("show_unselected", True):
            selected_ids = set(map_state.get("selected_polygons", []))
            geojson_out["features"] = [
                feature for feature in geojson_out.get("features", [])
                if feature["id"] in selected_ids
            ]
        
        if not need_to_fetch:
            geojson_out = no_update

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