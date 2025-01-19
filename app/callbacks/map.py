# app/callbacks/map.py
import json
import pandas as pd
import geopandas as gpd
from dash import no_update, callback_context
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from datetime import datetime
import logging

from utils.helpers import calculate_center_and_zoom
from mapinit import get_polygons_by_type, get_date_ranges_by_type
from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES

logger = logging.getLogger(__name__)


def register_map_callbacks(app, date_ranges_df):
    """
    Registers all map and filter callbacks with the Dash app instance.
    Includes:
      1. A single combined callback (update_map_and_filters) that handles:
         - Filter selection and reset
         - Year slider visibility and range changes
         - Updating the underlying map's geojson and styling
      2. A callback (handle_map_events) for user interactions on the map,
         such as clicking or box-lasso selecting polygons, and resetting selections.
    """
    # -------------------------------------------------
    # Constants & Helper for normalizing year
    # -------------------------------------------------
    DEFAULT_MIN_YEAR = 1800
    CURRENT_YEAR = datetime.now().year

    def normalize_year(year):
        """
        Clamp the given year to avoid nonsensical values far in the future.
        """
        if year > CURRENT_YEAR + 100:
            return CURRENT_YEAR
        return year

    # -------------------------------------------------
    # 1) Merged Filters + Year Range + Map Refresh
    # -------------------------------------------------
    @app.callback(
        Output("map-state", "data", allow_duplicate=True),
        Output('year-range-container', 'style'),
        Output('year-range-slider', 'min'),
        Output('year-range-slider', 'max'),
        Output('year-range-slider', 'value'),
        Output('year-range-slider', 'marks'),
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
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
            State('choropleth-map', 'figure'),
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
        figure,
        button_ids
    ):
        """
        This single callback handles:
          - Updating the selected unit filters in `map_state["unit_types"]`
            via either a normal click or ctrl-click (toggle).
          - Resetting the filters if the reset button is clicked.
          - Storing the chosen year range in `map_state["year_range"]`.
          - Determining whether to show or hide the year slider container,
            based on the selected unit types.
          - Computing the year slider min/max/marks from either `date_ranges_df`
            or user-supplied `map_state["year_bounds"]`.
          - Fetching polygons for all selected unit types and updating
            the `choropleth-map` figure's geojson, highlighting selected polygons.
          - Updating the debug output and filter button states (colors/outlines).

        Returns:
          A tuple that updates:
            1) map_state (dict)
            2) year-range-container.style (dict)
            3) year-range-slider.min (int)
            4) year-range-slider.max (int)
            5) year-range-slider.value ([int, int])
            6) year-range-slider.marks (dict)
            7) choropleth-map.figure (dict)
            8) debug-output.children (str)
            9) filter button 'color' (list of str)
            10) filter button 'outline' (list of bool)
        """
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        triggered_prop_id = ctx.triggered[0]['prop_id']
        logger.info(
            "Callback: update_map_and_filters triggered by %s", triggered_prop_id)

        # Clone the incoming map_state for safe mutation
        new_map_state = map_state.copy()
        debug_msg = ""

        # -------------------------------------------------
        # (A) Update unit filters in map_state if triggered
        # -------------------------------------------------
        if 'reset-filters' in triggered_prop_id:
            # Reset the filters to default
            logger.debug(
                "Reset-filters button clicked. Resetting to ['MOD_REG'].")
            new_map_state["unit_types"] = ["MOD_REG"]

        elif '{"type":"unit-filter"' in triggered_prop_id:
            # A filter button was clicked
            try:
                # Figure out which unit filter was clicked
                button_id = json.loads(triggered_prop_id.split('.')[0])
                clicked_type = button_id['unit']
            except Exception:
                logger.warning("Could not parse clicked filter button.")
                clicked_type = None

            if clicked_type:
                previous_types = set(
                    new_map_state.get('unit_types', ['MOD_REG']))
                current_types = set(new_map_state.get(
                    'unit_types', ['MOD_REG']))

                if ctrl_pressed:
                    # Ctrl+Click => toggle the clicked type
                    if clicked_type in current_types:
                        current_types.remove(clicked_type)
                        # Ensure we don't end up with an empty set
                        if not current_types:
                            current_types.add('MOD_REG')
                    else:
                        current_types.add(clicked_type)
                else:
                    # Regular click => single select
                    current_types = {clicked_type}

                new_map_state["unit_types"] = list(current_types)

                # If we added any new type, clear selected polygons
                # to avoid having stale "selected_polygons" from old sets
                added_types = current_types - previous_types
                if added_types:
                    new_map_state["selected_polygons"] = []

                logger.debug("Updated unit_types: %s",
                             new_map_state["unit_types"])

        # -------------------------------------------------
        # (B) Update the map_state's year_range if slider changed
        # -------------------------------------------------
        if 'year-range-slider.value' in triggered_prop_id and chosen_year_range:
            new_map_state['year_range'] = (
                normalize_year(chosen_year_range[0]),
                normalize_year(chosen_year_range[1])
            )
            logger.debug("Storing new year_range in map_state: %s",
                         new_map_state['year_range'])

        # -------------------------------------------------
        # (C) Show/hide the year-range slider container
        # -------------------------------------------------
        current_unit_types = new_map_state.get('unit_types', [])
        if not current_unit_types:
            # No unit types => hide
            container_style = {'display': 'none'}
        else:
            # If *any* selected unit type is NOT timeless => show slider
            if any(utype not in TIMELESS_UNIT_TYPES for utype in current_unit_types):
                container_style = {'display': 'block'}
            else:
                container_style = {'display': 'none'}

        # -------------------------------------------------
        # (D) Compute slider min/max/value/marks
        # -------------------------------------------------
        year_bounds = new_map_state.get('year_bounds', None)
        if year_bounds:
            min_year = max(year_bounds[0], DEFAULT_MIN_YEAR)
            max_year = normalize_year(year_bounds[1])
        else:
            # Fallback to the range for 'MOD_REG'
            fallback = date_ranges_df[date_ranges_df['g_unit_type']
                                      == 'MOD_REG'].iloc[0]
            min_year = max(int(fallback['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(fallback['max_year']))

        # Create evenly spaced marks for the slider
        step = max(1, (max_year - min_year) // 10)
        slider_marks = {str(y): str(y)
                        for y in range(min_year, max_year + 1, step)}

        # Determine the slider "value"
        stored_year_range = new_map_state.get('year_range', None)
        if not stored_year_range:
            slider_value = [max_year, max_year]
        else:
            slider_value = [
                normalize_year(stored_year_range[0]),
                normalize_year(stored_year_range[1]),
            ]

        # -------------------------------------------------
        # (E) Fetch polygons & update map figure
        # -------------------------------------------------
        unit_types = new_map_state.get('unit_types', ['MOD_REG'])
        year_range = new_map_state.get('year_range', None)
        logger.debug(
            "Fetching polygons for unit_types=%s, year_range=%s", unit_types, year_range)

        filtered_gdfs = []
        for utype in unit_types:
            if year_range and (utype not in TIMELESS_UNIT_TYPES):
                gdf = get_polygons_by_type(utype, year_range[0], year_range[1])
            else:
                gdf = get_polygons_by_type(utype)
            filtered_gdfs.append(gdf)

        if filtered_gdfs:
            combined_gdf = pd.concat(filtered_gdfs)
        else:
            combined_gdf = gpd.GeoDataFrame()

        if combined_gdf.empty:
            logger.warning(
                "No polygons found for selected types: %s", unit_types)
            debug_msg = "No polygons found for selected types."
            # Clear figure data
            if figure['data']:
                figure['data'][0]['locations'] = []
                figure['data'][0]['geojson'] = {}
                figure['data'][0]['selectedpoints'] = []
            # Clear current_geojson
            new_map_state["current_geojson"] = {}
        else:
            # Convert to GeoJSON
            try:
                geojson_data = json.loads(combined_gdf.to_json())
            except Exception as e:
                logger.error(
                    "Error converting combined_gdf to JSON: %s", str(e))
                debug_msg = f"Error updating map: {str(e)}"
                # Return partial results anyway
                if figure['data']:
                    figure['data'][0]['locations'] = []
                    figure['data'][0]['geojson'] = {}
                    figure['data'][0]['selectedpoints'] = []
                new_map_state["current_geojson"] = {}
            else:
                new_map_state["current_geojson"] = geojson_data
                # Update figure
                if figure['data']:
                    locations = combined_gdf.index.tolist()
                    figure['data'][0]['locations'] = locations
                    figure['data'][0]['geojson'] = geojson_data

                    # Because we changed filters/year => reset selection
                    figure['data'][0]['selectedpoints'] = None
                    figure['data'][0]['z'] = [1] * len(locations)

                    # Update the map's center in a naive way (mean of centroids)
                    center_lat = combined_gdf.to_crs(
                        '+proj=cea').centroid.to_crs(combined_gdf.crs).y.mean()
                    center_lon = combined_gdf.to_crs(
                        '+proj=cea').centroid.to_crs(combined_gdf.crs).x.mean()
                    figure['layout']['mapbox']['center'] = {
                        "lat": center_lat, "lon": center_lon}

                # Build debug message
                debug_msg = f"Showing {unit_types} units ({len(combined_gdf)} polygons)"
                if year_range and any(ut not in TIMELESS_UNIT_TYPES for ut in unit_types):
                    debug_msg += f" for years {year_range[0]}-{year_range[1]}."

        # -------------------------------------------------
        # (F) Update filter button colors + outlines
        # -------------------------------------------------
        button_colors = ['secondary'] * len(UNIT_TYPES)
        button_outlines = [True] * len(UNIT_TYPES)
        active_types = new_map_state.get('unit_types', [])
        for i, bid in enumerate(button_ids):
            if bid['unit'] in active_types:
                button_colors[i] = 'primary'
                button_outlines[i] = False

        # -------------------------------------------------
        # Return everything
        # -------------------------------------------------
        return (
            new_map_state,       # map-state
            container_style,     # year-range-container style
            min_year,            # year-range-slider.min
            max_year,            # year-range-slider.max
            slider_value,        # year-range-slider.value
            slider_marks,        # year-range-slider.marks
            figure,              # choropleth-map.figure
            debug_msg,           # debug-output.children
            button_colors,       # filter button color
            button_outlines,     # filter button outline
        )

    # -------------------------------------------------
    # 2) Handle Map Events (click, box-lasso, reset)
    # -------------------------------------------------
    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('map-state', 'data', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        [
            Input("map-state", "data"),
            Input('choropleth-map', 'clickData'),
            Input('choropleth-map', 'selectedData'),
            Input('reset-btn', 'n_clicks'),
        ],
        [
            State('choropleth-map', 'figure'),
            State("map-state", "data"),
        ],
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
        Handles user interactions with the map:
          - (a) Updating the figure after polygons have been selected in map_state.
          - (b) Handling single-click selection (clickData).
          - (c) Handling box/lasso selection (selectedData).
          - (d) Resetting all selections (reset button).

        We detect which input triggered the callback by inspecting `callback_context`.
        """
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        
        figure_out = current_fig
        map_state_out = old_map_state.copy()
        debug_out = no_update

        triggered_prop_id = ctx.triggered[0]['prop_id']
        logger.info("Callback: handle_map_events :triggered by %s",
                    triggered_prop_id)

        # 1) If "map-state" changed => zoom to selected polygons
        if "map-state" in triggered_prop_id:
            logger.debug(
                "Callback: handle_map_events: Map-state changed. Checking for selected polygons.")
            selected_ids = new_map_state.get("selected_polygons", [])
            current_geojson = new_map_state.get("current_geojson", {})
            if not selected_ids:
                return figure_out, map_state_out, "No polygons selected."
            if not current_geojson:
                return figure_out, map_state_out, "No GeoJSON available to zoom."

            try:
                current_gdf = gpd.GeoDataFrame.from_features(current_geojson)
            except Exception as e:
                msg = f"Error converting current_geojson to GeoDataFrame: {e}"
                logger.error(msg)
                return figure_out, map_state_out, msg

            # Filter to selected polygons
            gdf_filtered = current_gdf[current_gdf.index.isin(selected_ids)]
            if gdf_filtered.empty:
                debug_out = "No polygons match the selected IDs"
            else:
                # Compute new center + zoom
                map_props = calculate_center_and_zoom(gdf_filtered)
                if map_props["center"] and map_props["zoom"] is not None:
                    figure_out['layout']['mapbox']['center'] = map_props["center"]
                    figure_out['layout']['mapbox']['zoom'] = map_props["zoom"]
                # Also highlight them on the figure
                for i, d in enumerate(figure_out['data']):
                    if d['type'] == 'choroplethmapbox':
                        figure_out['data'][i]['selectedpoints'] = selected_ids
                debug_out = (
                    f"Zoomed to {len(selected_ids)} polygons. "
                    f"Center={map_props['center']}, Zoom={map_props['zoom']}"
                )
            return figure_out, map_state_out, debug_out

        if "choropleth-map.clickData" in triggered_prop_id:
            logger.debug("Callback: handle_map_events: Single-click detected.")
            if not clickData:
                return figure_out, map_state_out, "No polygon clicked."

            # Extract the clicked polygon ID from clickData
            clicked_id = clickData['points'][0]['location']

            # Get the current geojson from map_state or the figure
            current_geojson = old_map_state.get("current_geojson", {})
            if not current_geojson:
                # Fallback: try the figure's data
                try:
                    current_geojson = figure_out['data'][0]['geojson']
                except Exception:
                    logger.error("No GeoJSON available for single-click.")
                    return figure_out, map_state_out, "No GeoJSON available"

            # Convert the geojson to a GeoDataFrame
            try:
                current_gdf = gpd.GeoDataFrame.from_features(current_geojson)
                ids = pd.json_normalize(current_geojson["features"])["id"].values
                current_gdf['id'] = ids
            except Exception as e:
                logger.error("Error loading GeoDataFrame: %s", str(e))
                return figure_out, map_state_out, "Error processing GeoDataFrame"

            # Look up the row index in the gdf for the clicked polygon
            matching_rows = current_gdf.index[current_gdf.id.astype(
                str) == str(clicked_id)].tolist()
            if not matching_rows:
                return figure_out, map_state_out, f"Polygon {clicked_id} not found."

            row_idx = matching_rows[0]  # There should normally be exactly one match

            # Retrieve the existing selection (as a list of indices) from map_state
            selected_ids = map_state_out.get("selected_polygons", [])
            if not isinstance(selected_ids, list):
                selected_ids = []

            # Toggle logic:
            #   - If row_idx already in selected_ids => remove it
            #   - else => append it
            if row_idx in selected_ids:
                selected_ids.remove(row_idx)
                debug_info = f"Deselected polygon with ID: {clicked_id}"
            else:
                selected_ids.append(row_idx)
                debug_info = f"Selected polygon with ID: {clicked_id}"

            # Update map_state with the new selection
            map_state_out["selected_polygons"] = selected_ids

            # Update the figure’s selectedpoints to match
            for i, d in enumerate(figure_out['data']):
                if d['type'] == 'choroplethmapbox':
                    figure_out['data'][i]['selectedpoints'] = selected_ids

            return figure_out, map_state_out, debug_info

        # 3) If "choropleth-map.selectedData" => box/lasso selection
        if "choropleth-map.selectedData" in triggered_prop_id:
            logger.debug("Callback: handle_map_events: Box/Lasso selection detected.")
            if not selectedData:
                return figure_out, map_state_out, "No polygons box/lasso-selected."
            try:
                selected_ids = [pt['location']
                                for pt in selectedData['points']]
                debug_info = (
                    f"Box/Lasso selected {len(selected_ids)} polygons.\n"
                    f"Polygon IDs: {selected_ids}"
                )
                for i, d in enumerate(figure_out['data']):
                    if d['type'] == 'choroplethmapbox':
                        figure_out['data'][i]['selectedpoints'] = selected_ids
                return figure_out, map_state_out, debug_info
            except Exception as e:
                logger.error(
                    "Error processing box/lasso selection: %s", str(e))
                return figure_out, map_state_out, "Error processing selection."

        # 4) If "reset-btn" triggered => reset selection
        if "reset-btn" in triggered_prop_id:
            logger.debug("Callback: handle_map_events: Reset button clicked.")
            if n_clicks > 0:
                try:
                    for i, d in enumerate(figure_out['data']):
                        if d['type'] == 'choroplethmapbox':
                            figure_out['data'][i]['selectedpoints'] = None
                    map_state_out["selected_polygons"] = []
                    debug_msg = "Reset button clicked. All selections cleared."
                    return figure_out, map_state_out, debug_msg
                except Exception as e:
                    logger.error("Error resetting selection: %s", str(e))
                    return figure_out, map_state_out, "Error resetting selections."
            return figure_out, map_state_out, no_update

        # If we reach here, no relevant input triggered => do nothing
        raise PreventUpdate
