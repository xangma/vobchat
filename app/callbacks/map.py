# app/callbacks/map.py
import json
import pandas as pd
import geopandas as gpd
from dash import no_update, callback_context
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from utils.helpers import calculate_center_and_zoom
from mapinit import get_polygons_by_type, get_date_ranges_by_type
from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def register_map_callbacks(app, date_ranges_df):

    # Set default year range
    DEFAULT_MIN_YEAR = 1800
    CURRENT_YEAR = datetime.now().year
    
    def normalize_year(year):
        """Normalize years to be within reasonable bounds"""
        if year > CURRENT_YEAR + 100:
            return CURRENT_YEAR
        return year

    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        # Existing inputs:
        Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
        Input('reset-filters', 'n_clicks'),
        # New input for ctrl-click
        State('ctrl-pressed-store', 'data'),
        State("map-state", "data"),
        prevent_initial_call=True
    )
    def update_unit_filter_state(unit_filter_clicks, reset_clicks, ctrl_pressed, map_state):
        """
        If ctrl_pressed is True, we toggle the clicked filter.
        If ctrl_pressed is False, we set the selection to ONLY the clicked filter.
        """
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        triggered_id = ctx.triggered[0]['prop_id']

        # If user reset the filters
        if 'reset-filters' in triggered_id:
            map_state["unit_types"] = ["MOD_REG"]
            return map_state

        # Otherwise, parse which button was clicked
        try:
            button_id = json.loads(triggered_id.split('.')[0])
            clicked_type = button_id['unit']
        except:
            return map_state  # Could not parse — fallback

        current_types = set(map_state.get('unit_types', ['MOD_REG']))

        if ctrl_pressed:
            # 1) Ctrl+Click => Toggle the clicked type
            if clicked_type in current_types:
                current_types.remove(clicked_type)
                if not current_types:
                    current_types.add('MOD_REG')
            else:
                current_types.add(clicked_type)
        else:
            # 2) Regular click => Single select the clicked type
            current_types = {clicked_type}

        map_state["unit_types"] = list(current_types)
        return map_state

    @app.callback(
        Output('year-range-container', 'style'),
        Input("map-state", "data"),
        prevent_initial_call=True
    )
    def update_year_range_visibility(map_state):
        """
        Show/Hide the year-range slider container based on currently active 
        (selected) unit types. If all selected types are timeless, hide it.
        """
        logger.info("Callback: update_year_range_visibility")
        unit_types = map_state.get('unit_types', [])
        # If no unit types, hide
        if not unit_types:
            logger.debug("No unit types found, hiding year range")
            return {'display': 'none'}

        # If *any* selected unit type is NOT timeless, show the year range
        # Otherwise, hide it
        if any(utype not in TIMELESS_UNIT_TYPES for utype in unit_types):
            logger.debug("Showing year range slider (at least one non-timeless type)")
            return {'display': 'block'}
        else:
            logger.debug("Hiding year range slider (all selected types are timeless)")
            return {'display': 'none'}

    @app.callback(
        Output('year-range-slider', 'min'),
        Output('year-range-slider', 'max'),
        Output('year-range-slider', 'value'),
        Output('year-range-slider', 'marks'),
        Input("map-state", "data")
    )
    def update_year_slider(map_state):
        """
        Update the year-range slider constraints and value based on
        map_state['year_bounds'] and map_state['year_range'].
        If no bounds are provided, fallback to the range for 'MOD_REG'.
        """
        logger.info("Callback: update_year_slider")
        # Attempt to get user-defined bounds from state
        year_bounds = map_state.get('year_bounds', None)
        
        if not year_bounds:
            # Fallback to default MOD_REG in date_ranges_df
            logger.debug("No year_bounds found in map_state, using 'MOD_REG' fallback")
            fallback = date_ranges_df[date_ranges_df['g_unit_type'] == 'MOD_REG'].iloc[0]
            min_year = max(int(fallback['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(fallback['max_year']))
        else:
            min_year = max(year_bounds[0], DEFAULT_MIN_YEAR)
            max_year = normalize_year(year_bounds[1])

        # Step for slider marks
        step = max(1, (max_year - min_year) // 10)
        marks = {str(y): str(y) for y in range(min_year, max_year + 1, step)}

        # Value is the user's chosen year range or the maximum year
        year_range = map_state.get('year_range', None)
        if not year_range:
            # If no user-chosen range, default to [max_year, max_year]
            value = [max_year, max_year]
        else:
            value = [
                normalize_year(year_range[0]),
                normalize_year(year_range[1])
            ]

        return min_year, max_year, value, marks

    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        Input('year-range-slider', 'value'),
        State("map-state", "data"),
        prevent_initial_call=True
    )
    def store_year_range_in_map_state(chosen_range, map_state):
        """
        Whenever the user adjusts the year-range slider, update 
        map_state['year_range'] accordingly.
        """
        logger.info("Callback: store_year_range_in_map_state")
        map_state['year_range'] = (
            normalize_year(chosen_range[0]),
            normalize_year(chosen_range[1])
        )
        logger.debug(f"Storing year_range={map_state['year_range']} in map-state")
        return map_state

    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        [Output({'type': 'unit-filter', 'unit': ALL}, 'color')],
        [Output({'type': 'unit-filter', 'unit': ALL}, 'outline')],
        Input("map-state", "data"),
        State('choropleth-map', 'figure'),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        prevent_initial_call=True
    )
    def update_map_filter(map_state, figure, button_ids):
        """
        1. Update filter button states (unit_types) -> button colors/outlines.
        2. Fetch polygons for all selected unit types and combine them.
        3. Update figure + store the resulting GeoJSON in map_state['current_geojson'].
        """
        ctx = callback_context
        triggered_id = ctx.triggered[0]['prop_id']
        logger.info("Callback: update_map_filter")

        try:
            unit_types = map_state.get('unit_types', ['MOD_REG'])
            year_range = map_state.get('year_range', None)
            logger.debug(f"Using unit_types={unit_types}, year_range={year_range}")

            # Update button states
            button_colors = ['secondary'] * len(UNIT_TYPES)
            button_outlines = [True] * len(UNIT_TYPES)

            # Mark the active ones as primary
            for i, bid in enumerate(button_ids):
                if bid['unit'] in unit_types:
                    button_colors[i] = 'primary'
                    button_outlines[i] = False

            # Gather polygons
            filtered_gdfs = []
            for utype in unit_types:
                if year_range and utype not in TIMELESS_UNIT_TYPES:
                    gdf = get_polygons_by_type(utype, year_range[0], year_range[1])
                else:
                    gdf = get_polygons_by_type(utype)
                filtered_gdfs.append(gdf)

            # Combine them
            if filtered_gdfs:
                combined_gdf = pd.concat(filtered_gdfs)
            else:
                combined_gdf = gpd.GeoDataFrame()

            if combined_gdf.empty:
                logger.warning("No polygons found for selected types")
                debug_msg = "No polygons found for selected types."
                # Clear the figure data but keep it from crashing
                if figure['data']:
                    figure['data'][0]['locations'] = []
                    figure['data'][0]['geojson'] = {}
                    figure['data'][0]['selectedpoints'] = []
                # Also clear current_geojson
                map_state["current_geojson"] = {}
                return map_state, figure, debug_msg, button_colors, button_outlines

            # Convert GDF to geojson
            try:
                geojson_data = json.loads(combined_gdf.to_json())
            except Exception as e:
                logger.error(f"Error converting combined_gdf to JSON: {str(e)}")
                debug_msg = f"Error updating map: {str(e)}"
                return map_state, figure, debug_msg, button_colors, button_outlines

            # Store in map_state
            map_state["current_geojson"] = geojson_data

            # Update figure
            if figure['data']:
                locations = combined_gdf.index.tolist()
                figure['data'][0]['locations'] = locations
                figure['data'][0]['geojson'] = geojson_data
                # If triggered by 'year-range-slider' or 'unit-filter'
                
                if 'year-range-slider' in triggered_id or '{"type":"unit-filter"' in triggered_id:
                    # The user changed filters => reset selection
                    figure['data'][0]['selectedpoints'] = None
                figure['data'][0]['z'] = [1] * len(locations)

                # Update map center (rough approach)
                center_lat = combined_gdf.to_crs('+proj=cea').centroid.to_crs(combined_gdf.crs).y.mean()
                center_lon = combined_gdf.to_crs('+proj=cea').centroid.to_crs(combined_gdf.crs).x.mean()
                figure['layout']['mapbox']['center'] = {"lat": center_lat, "lon": center_lon}

            debug_msg = f"Showing {unit_types} units ({len(combined_gdf)} polygons)"
            if year_range and any(ut not in TIMELESS_UNIT_TYPES for ut in unit_types):
                debug_msg += f" for years {year_range[0]}-{year_range[1]}"

            return map_state, figure, debug_msg, button_colors, button_outlines

        except Exception as e:
            logger.error(f"Error in update_map_filter: {str(e)}")
            return map_state, figure, "Error updating map filter", ['secondary'] * len(UNIT_TYPES), [True] * len(UNIT_TYPES)
        
    @app.callback(
        # We produce the same outputs: figure, map_state, debug-info
        # Only in reset_selection did we update map_state; here we always can.
        # If your code doesn't change map_state in other branches, you can return no_update.
        Output('choropleth-map', 'figure'),
        Output('map-state', 'data'),
        Output('debug-output', 'children'),
        # Combine all triggers:
        Input("map-state", "data"),            # triggers old update_map_from_selected_polygons
        Input('choropleth-map', 'clickData'),  # triggers old handle_click
        Input('choropleth-map', 'selectedData'),  # triggers old box/lasso
        Input('reset-btn', 'n_clicks'),        # triggers old reset_selection
        # States
        State('choropleth-map', 'figure'),
        State("map-state", "data"),
        prevent_initial_call=True
    )
    def handle_map_events(
        new_map_state,
        clickData,
        selectedData,
        n_clicks,
        current_fig,
        old_map_state
    ):
        """
        This single callback replaces four separate callbacks:
          1) update_map_from_selected_polygons
          2) handle_click
          3) handle_box_lasso
          4) reset_selection

        We detect which input triggered the callback and run the appropriate logic.
        """
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        # By default, assume we return the “old” figure, the “old” map_state, and no new debug-info
        figure_out = current_fig
        map_state_out = old_map_state
        debug_out = no_update

        triggered_prop_id = ctx.triggered[0]['prop_id']

        # ---------------------------------------------------------------------
        # 1) If "map-state" triggered => old `update_map_from_selected_polygons`
        if "map-state" in triggered_prop_id:
            # Access the just-updated map_state from new_map_state
            selected_ids = new_map_state.get("selected_polygons", [])
            current_geojson = new_map_state.get("current_geojson", {})

            if not selected_ids:
                logger.debug("No polygons selected in map_state")
                return figure_out, map_state_out, "No polygons selected."

            if not current_geojson:
                logger.warning("No GeoJSON available in map_state, cannot update map")
                return figure_out, map_state_out, "No polygons or no GeoJSON available."

            try:
                current_gdf = gpd.GeoDataFrame.from_features(current_geojson)
            except Exception as e:
                msg = f"Error converting current_geojson to GeoDataFrame: {str(e)}"
                logger.error(msg)
                return figure_out, map_state_out, msg

            # Filter for selected ids
            gdf_filtered = current_gdf[current_gdf.index.isin(selected_ids)]
            if gdf_filtered.empty:
                return figure_out, map_state_out, "No polygons found for the selected IDs"

            map_properties = calculate_center_and_zoom(gdf_filtered)
            if map_properties["center"] and map_properties["zoom"] is not None:
                figure_out['layout']['mapbox']['center'] = map_properties["center"]
                figure_out['layout']['mapbox']['zoom'] = map_properties["zoom"]

            for i, d in enumerate(figure_out['data']):
                if d['type'] == 'choroplethmapbox':
                    figure_out['data'][i]['selectedpoints'] = selected_ids

            debug_out = (
                f"Zoomed to {len(selected_ids)} polygons. "
                f"Center={map_properties['center']}, Zoom={map_properties['zoom']}"
            )
            return figure_out, map_state_out, debug_out

        # ---------------------------------------------------------------------
        # 2) If "choropleth-map.clickData" triggered => old `handle_click`
        if "choropleth-map.clickData" in triggered_prop_id:
            if not clickData:
                return figure_out, map_state_out, "No polygon selected."

            clicked_id = clickData['points'][0]['location']
            current_geojson = old_map_state.get("current_geojson", {})

            if not current_geojson:
                return figure_out, map_state_out, "No GeoDataFrame available"

            try:
                current_gdf = gpd.GeoDataFrame.from_features(current_geojson)
                ids = pd.json_normalize(current_geojson["features"])["id"].values
                current_gdf['id'] = ids
            except Exception as e:
                logger.error(f"Error loading GeoDataFrame: {str(e)}")
                return figure_out, map_state_out, "Error processing GeoDataFrame"

            if str(clicked_id) not in current_gdf.id.values.astype(str):
                return figure_out, map_state_out, f"Clicked polygon {clicked_id} not found in data."

            # Highlight the selected polygon
            debug_info = f"Single polygon selected with ID: {clicked_id}"
            for i, d in enumerate(figure_out['data']):
                if d['type'] == 'choroplethmapbox':
                    row_idx = current_gdf.index[
                        current_gdf.id.values.astype(str) == str(clicked_id)
                    ].tolist()
                    figure_out['data'][i]['selectedpoints'] = row_idx

            return figure_out, map_state_out, debug_info

        # ---------------------------------------------------------------------
        # 3) If "choropleth-map.selectedData" triggered => old `handle_box_lasso`
        if "choropleth-map.selectedData" in triggered_prop_id:
            if not selectedData:
                return figure_out, map_state_out, "No polygons selected."

            try:
                selected_ids = [p['location'] for p in selectedData['points']]
                debug_info = (
                    f"Box/Lasso selection: {len(selected_ids)} polygons selected.\n"
                    f"Selected polygon IDs: {selected_ids}\n"
                )
                for i, d in enumerate(figure_out['data']):
                    if d['type'] == 'choroplethmapbox':
                        figure_out['data'][i]['selectedpoints'] = selected_ids

                return figure_out, map_state_out, debug_info

            except Exception as e:
                logger.error(f"Error processing box/lasso selection: {str(e)}")
                return figure_out, map_state_out, "Error processing selection"

        # ---------------------------------------------------------------------
        # 4) If "reset-btn" triggered => old `reset_selection`
        if "reset-btn" in triggered_prop_id:
            if n_clicks > 0:
                try:
                    for i, d in enumerate(figure_out['data']):
                        if d['type'] == 'choroplethmapbox':
                            figure_out['data'][i]['selectedpoints'] = None

                    # Also clear map_state's selected_polygons
                    old_map_state["selected_polygons"] = []
                    debug_msg = "Reset button clicked. All selections cleared."
                    return figure_out, old_map_state, debug_msg
                except Exception as e:
                    logger.error(f"Error resetting selection: {str(e)}")
                    return figure_out, old_map_state, "Error resetting selections"

            return figure_out, old_map_state, no_update

        # ---------------------------------------------------------------------
        # If we got here but none of the conditions matched, do nothing
        raise PreventUpdate