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

from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES
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
        [
            Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
            Input('reset-filters', 'n_clicks'),
            Input('year-range-slider', 'value'),   # read user’s chosen years
            Input('ctrl-pressed-store', 'data'),
            Input("geojson-layer", "n_clicks"),
            Input("reset-btn", "n_clicks"),
        ],
        [
            State("map-state", "data"),
            State({'type': 'unit-filter', 'unit': ALL}, 'id'),
            State("geojson-layer", "clickData"),
            State("geojson-layer", "hideout"),
        ],
        prevent_initial_call=True
    )
    def update_map_state(
        unit_filter_clicks,
        reset_filters_clicks,
        chosen_year_range,
        ctrl_pressed,
        geojson_n_clicks,
        reset_btn_n_clicks,
        map_state,
        button_ids,
        geojson_clickData,
        geojson_hideout
    ):
        """
        This callback merges ALL user interactions into one updated map-state.
        Dash sees only 1 property ("map-state.data") changed => single callback invocation.
        """
        triggered_prop_ids = [t["prop_id"] for t in ctx.triggered]
        if not triggered_prop_ids:
            raise PreventUpdate

        logger.info("update_map_state triggered by %s", triggered_prop_ids)

        new_map_state = (map_state or {}).copy()

        # 1) Handle Filter / Year-Range changes
        filter_triggered = False

        # A) Reset filters
        if "reset-filters.n_clicks" in triggered_prop_ids and reset_filters_clicks:
            logger.debug("reset-filters: clearing to ['MOD_REG']")
            new_map_state["unit_types"] = ["MOD_REG"]
            new_map_state["selected_polygons"] = []
            filter_triggered = True

        # B) Check if we clicked or ctrl-clicked a unit-filter button
        unit_type_trigs = [t for t in triggered_prop_ids if "unit-filter" in t]
        for i, t in enumerate(unit_type_trigs):
            # if unit_filter_clicks[i]:
            dict_part = t.split(".")[0]
            try:
                dict_part = json.loads(dict_part)
            except:
                dict_part = None

            if dict_part and dict_part.get("type") == "unit-filter":
                clicked_type = dict_part["unit"]
                prev_types = set(new_map_state.get(
                    "unit_types", ["MOD_REG"]))
                current_types = set(prev_types)

                if ctrl_pressed:
                    # toggle
                    if clicked_type in current_types:
                        current_types.remove(clicked_type)
                        if not current_types:
                            current_types = {"MOD_REG"}
                    else:
                        current_types.add(clicked_type)
                    ctrl_pressed = False  # we used ctrl
                else:
                    # single choice
                    current_types = {clicked_type}

                new_map_state["unit_types"] = list(current_types)
                if current_types - prev_types:
                    # if new type was added, clear selected polygons
                    new_map_state["selected_polygons"] = []
                filter_triggered = True

        # C) If the user changed the year-range slider
        if "year-range-slider.value" in triggered_prop_ids and chosen_year_range:
            y0, y1 = chosen_year_range
            new_map_state["year_range"] = (
                normalize_year(y0),
                normalize_year(y1)
            )
            filter_triggered = True

        # 2) If filter-triggered, we can store a “needs_refresh” or do nothing
        if filter_triggered:
            logger.debug("Filters updated => stored in map-state")

        # 3) Polygon selection logic
        # A) If user clicked reset-btn
        if "reset-btn.n_clicks" in triggered_prop_ids and reset_btn_n_clicks:
            new_map_state["selected_polygons"] = []

        # B) If geojson-layer clicked
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

        return new_map_state

    @app.callback(
        [
            Output('year-range-container', 'style'),
            Output('year-range-slider', 'min'),
            Output('year-range-slider', 'max'),
            Output('year-range-slider', 'marks'),
            Output('geojson-layer', 'data'),
            Output('geojson-layer', 'hideout'),
            Output('debug-output', 'children'),
            Output({'type': 'unit-filter', 'unit': ALL}, 'color'),
            Output({'type': 'unit-filter', 'unit': ALL}, 'outline'),
            # Output("retrigger-chat", "data"),
        ],
        Input("map-state", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        State("app-state", "data"),
        prevent_initial_call=True
    )
    def render_map_display(map_state, button_ids, app_state):
        """
        Fires once each time map-state.data changes.
        Builds final UI: slider bounds, geojson, debug text, button styling, etc.
        """
        if not map_state:
            raise PreventUpdate

        # (A) Show/hide slider
        unit_types = map_state.get("unit_types", ["MOD_REG"])
        if any(ut not in TIMELESS_UNIT_TYPES for ut in unit_types):
            container_style = {'display': 'block'}
        else:
            container_style = {'display': 'none'}

        # (B) Figure out min/max (use date_ranges_df or fallback)
        min_year = DEFAULT_MIN_YEAR
        max_year = CURRENT_YEAR
        step = max(1, (max_year - min_year)//10)
        slider_marks = {str(y): str(y)
                        for y in range(min_year, max_year+1, step)}

        # (C) Build polygons
        year_range = map_state.get("year_range")
        gdfs = []
        for ut in unit_types:
            if ut not in TIMELESS_UNIT_TYPES and year_range:
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

        # (D) hideout with selected polygons
        new_hideout = {"selected": map_state.get("selected_polygons", [])}

        # (E) Button styles
        active_set = set(unit_types)
        out_colors = []
        out_outlines = []
        for b in button_ids:
            if b["unit"] in active_set:
                out_colors.append("primary")
                out_outlines.append(False)
            else:
                out_colors.append("secondary")
                out_outlines.append(True)

        # retrigger_chat = no_update
        # if app_state.get("retrigger_chat"):
        #     retrigger_chat = True
            
        return (
            container_style,
            min_year,
            max_year,
            slider_marks,
            geojson_out,
            new_hideout,
            debug_msg,
            out_colors,
            out_outlines,
            # retrigger_chat
        )
