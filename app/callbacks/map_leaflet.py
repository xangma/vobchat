import json
import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping

from dash import (
    Input, Output, State, ctx, no_update, ALL,
    callback_context  # backward compat
)
from dash.exceptions import PreventUpdate
from datetime import datetime

from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES
from mapinit import get_polygons_by_type
from utils.helpers import calculate_center_and_zoom

logger = logging.getLogger(__name__)

def register_map_leaflet_callbacks(app, date_ranges_df):
    """
    Registers the server-side callbacks for the Dash Leaflet version.
    """

    DEFAULT_MIN_YEAR = 1800
    CURRENT_YEAR = datetime.now().year

    def normalize_year(year):
        return min(year, CURRENT_YEAR)

    #----------------------------------------------
    # 1) Combined Filters + Year Range + Load polygons
    #----------------------------------------------
    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        Output('year-range-container', 'style'),
        Output('year-range-slider', 'min'),
        Output('year-range-slider', 'max'),
        Output('year-range-slider', 'value'),
        Output('year-range-slider', 'marks'),
        Output('geojson-layer', 'data'),   # the main difference: we set the GeoJSON data
        Output('geojson-layer', 'hideout'), # we can also update hideout for style logic
        Output('ctrl-pressed-store', 'data'),  
        Output('debug-output', 'children'),
        Output({'type': 'unit-filter', 'unit': ALL}, 'color'),
        Output({'type': 'unit-filter', 'unit': ALL}, 'outline'),
        [
            Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
            Input('reset-filters', 'n_clicks'),
            Input('year-range-slider', 'value'),
            Input('ctrl-pressed-store', 'data'),  
        ],
        [
            State("map-state", "data"),
            State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        ],
        prevent_initial_call=True
    )
    def update_map_and_filters(
        unit_filter_clicks,
        reset_clicks,
        chosen_year_range,
        ctrl_pressed,
        map_state,
        button_ids
    ):
        """
        Mimics your existing server logic: 
        - Toggle filters
        - Update year slider
        - Load polygons
        - Return updated GeoJSON
        - Also set hideout so we can style selected polygons clientside
        """
        
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        
        triggered_prop_ids = []
        for trigger in ctx.triggered:
            triggered_prop_ids.append(trigger['prop_id'])
        logger.info("Callback: update_map_and_filters triggered by %s", triggered_prop_ids)

        new_map_state = map_state.copy() if map_state else {}
        debug_msg = ""

        # A) Reset or toggle filter
        for triggered_prop_id in triggered_prop_ids:
            try :
                triggered_prop_id = triggered_prop_id.split(".")[0]
                triggered_prop_id = json.loads(triggered_prop_id)
            except:
                continue
            if isinstance(triggered_prop_id, dict):
                if triggered_prop_id.get("type") == "reset-filters":
                    logger.debug("Reset to ['MOD_REG']")
                    new_map_state["unit_types"] = ["MOD_REG"]
                    new_map_state["selected_polygons"] = []
                elif (triggered_prop_id.get("type") == "unit-filter" or triggered_prop_id.get("type") == "ctrl-pressed-store"):
                    # The user clicked a filter button
                    clicked_type = triggered_prop_id["unit"]
                    prev_types = set(new_map_state.get("unit_types", ["MOD_REG"]))
                    current_types = set(prev_types)

                    if ctrl_pressed:
                        # toggle
                        if clicked_type in current_types:
                            current_types.remove(clicked_type)
                            if not current_types:
                                current_types = {"MOD_REG"}
                        else:
                            current_types.add(clicked_type)
                        ctrl_pressed = False
                    else:
                        current_types = {clicked_type}

                    new_map_state["unit_types"] = list(current_types)

                    # If we added a new type, clear selected polygons
                    added = current_types - prev_types
                    if added:
                        new_map_state["selected_polygons"] = []

        # B) Year range
        if chosen_year_range:
            y0, y1 = chosen_year_range
            new_map_state["year_range"] = (normalize_year(y0), normalize_year(y1))

        # C) Show/hide year slider
        current_unit_types = new_map_state.get("unit_types", [])
        if any(u not in TIMELESS_UNIT_TYPES for u in current_unit_types):
            container_style = {'display': 'block'}
        else:
            container_style = {'display': 'none'}

        # D) Compute slider bounds
        year_bounds = new_map_state.get("year_bounds", None)
        if year_bounds:
            min_year = max(year_bounds[0], DEFAULT_MIN_YEAR)
            max_year = normalize_year(year_bounds[1])
        else:
            fallback = date_ranges_df[date_ranges_df['g_unit_type'] == 'MOD_REG'].iloc[0]
            min_year = max(int(fallback['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(fallback['max_year']))

        step = max(1, (max_year - min_year)//10)
        slider_marks = {str(y): str(y) for y in range(min_year, max_year+1, step)}

        stored_year = new_map_state.get("year_range", None)
        if stored_year:
            slider_val = [normalize_year(stored_year[0]), normalize_year(stored_year[1])]
        else:
            slider_val = [max_year, max_year]  # fallback

        # E) Load polygons & build geojson
        unit_types = new_map_state.get("unit_types", ["MOD_REG"])
        year_range = new_map_state.get("year_range", None)
        gdfs = []
        for ut in unit_types:
            if ut not in TIMELESS_UNIT_TYPES and year_range:
                gdf = get_polygons_by_type(ut, year_range[0], year_range[1])
            else:
                gdf = get_polygons_by_type(ut)
            gdfs.append(gdf)
        if gdfs:
            combined_gdf = pd.concat(gdfs)
        else:
            combined_gdf = gpd.GeoDataFrame()

        if combined_gdf.empty:
            debug_msg = "No polygons found for selected types."
            geojson_out = {"type": "FeatureCollection", "features": []}
        else:
            try:
                # Put the row index into the "id" property so style can highlight
                # Dash Leaflet's "feature.id" is typically read from geometry "id",
                # so we set that ourselves:
                combined_gdf = combined_gdf.reset_index(drop=False)  # ensure a column "index"
                combined_gdf.rename(columns={"index": "feature_id"}, inplace=True)

                # Convert to GeoJSON
                geojson_out = json.loads(combined_gdf.to_json())

                debug_msg = f"Showing {len(combined_gdf)} polygons for {unit_types}"
                if year_range and any(ut not in TIMELESS_UNIT_TYPES for ut in unit_types):
                    debug_msg += f" in {year_range[0]}-{year_range[1]}."
            except Exception as e:
                logger.error("Error converting gdf to GeoJSON: %s", e)
                geojson_out = {"type": "FeatureCollection", "features": []}
                debug_msg = f"Error loading polygons: {e}"

        # We also set "hideout" for style, giving it the current selection
        selected_ids = new_map_state.get("selected_polygons", [])
        hideout = {"selected": selected_ids}

        # F) Update filter button colors
        # For each filter button, highlight if active
        button_colors = []
        button_outlines = []
        active_set = set(new_map_state.get("unit_types", []))
        for b in button_ids:
            # b is like {"type": "unit-filter", "unit": "MOD_REG"}
            if b["unit"] in active_set:
                button_colors.append("primary")
                button_outlines.append(False)
            else:
                button_colors.append("secondary")
                button_outlines.append(True)

        return (
            new_map_state,
            container_style,
            min_year,
            max_year,
            slider_val,
            slider_marks,
            geojson_out,
            hideout,
            ctrl_pressed,
            debug_msg,
            button_colors,
            button_outlines
        )

    #----------------------------------------------
    # 2) Handle Single-Click Selections
    #----------------------------------------------
    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        Output("geojson-layer", "hideout", allow_duplicate=True),  # update the "selected" array
        Output("debug-output", "children", allow_duplicate=True),
        [
            Input("geojson-layer", "n_clicks"),
            Input("reset-btn", "n_clicks"),
        ],
        [
            State("map-state", "data"),
            State("geojson-layer", "clickData"),
            State("geojson-layer", "hideout"),
        ],
        prevent_initial_call=True
    )
    def handle_map_events(_, reset_n_clicks, map_state, geojson_clickData, geojson_hideout):
        """
        - If user clicks a polygon, toggle selection in map_state["selected_polygons"].
        - If user clicks "reset selections," clear them.
        """
        if not ctx.triggered:
            raise PreventUpdate

        triggered_id = ctx.triggered_id
        new_map_state = map_state.copy()
        new_hideout = geojson_hideout.copy() if geojson_hideout else {}
        debug_msg = no_update

        # 1) Reset selections
        if triggered_id == "reset-btn" and reset_n_clicks:
            new_map_state["selected_polygons"] = []
            new_hideout["selected"] = []
            return new_map_state, new_hideout, "Selections reset."

        # 2) Single click
        if triggered_id == "geojson-layer":
            if not geojson_clickData:
                raise PreventUpdate
            # The clicked feature's "id" is the row's "feature_id"
            fid = geojson_clickData.get("id", None)
            if fid is None:
                return new_map_state, new_hideout, "Clicked feature has no ID."
            selected_ids = new_map_state.get("selected_polygons", [])

            if fid in selected_ids:
                selected_ids.remove(fid)
                debug_msg = f"Deselected polygon {fid}"
            else:
                selected_ids.append(fid)
                debug_msg = f"Selected polygon {fid}"

            new_map_state["selected_polygons"] = selected_ids
            new_hideout["selected"] = selected_ids
            return new_map_state, new_hideout, debug_msg

        raise PreventUpdate
