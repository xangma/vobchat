# app/callbacks/map_leaflet.py
"""
Map Leaflet Callbacks Module

This module manages all callbacks related to the Leaflet-based map component in the DDME prototype.
It handles unit filtering, map interactions, and maintains the map state throughout the application.

Key functionalities:
1. Filter polygons by unit type (e.g., Modern Regions, Modern Districts)
2. Toggle visibility of unselected polygons
3. Filter by year range for time-dependent administrative units
4. Handle map selections (clicking on polygons)
5. Maintain counts of selected polygons by unit type
6. Update the map display based on user interactions
"""

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

# Configure logger for this module
logger = logging.getLogger(__name__)

# Constants for year range validation
DEFAULT_MIN_YEAR = 1800
CURRENT_YEAR = datetime.now().year


def normalize_year(year):
    """
    Normalize a year value to ensure it doesn't exceed the current year.
    
    Args:
        year (int): The year to normalize
        
    Returns:
        int: The normalized year, capped at the current year
    """
    return min(year, CURRENT_YEAR)


def register_map_leaflet_callbacks(app, date_ranges_df):
    """
    Register all callbacks related to the Leaflet map component.
    
    Args:
        app (Dash): The Dash application instance
        date_ranges_df (pandas.DataFrame): DataFrame containing date ranges for each unit type
    """
    
    @app.callback(
        # Outputs
        Output("map-state", "data"),
        Output("counts-store", "data", allow_duplicate=True),
        Output("toggle-unselected", "children"),
        # Inputs
        [
            Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),  # All unit filter buttons
            Input('reset-selections', 'n_clicks'),                     # Reset button
            Input('year-range-slider', 'value'),                       # Year range slider
            Input('ctrl-pressed-store', 'data'),                       # Ctrl key state
            Input("geojson-layer", "n_clicks"),                        # Map click
            Input("toggle-unselected", "n_clicks"),                    # Toggle visibility button
        ],
        # States to retrieve
        [
            State("map-state", "data"),                                # Current map state
            State({'type': 'unit-filter', 'unit': ALL}, 'id'),         # Unit filter button IDs
            State("geojson-layer", "clickData"),                       # Data for clicked polygon
            State("geojson-layer", "hideout"),                         # Current hideout settings
            State('geojson-layer', 'data'),                            # Current GeoJSON data
            State("counts-store", "data"),                             # Polygon count data
            State("toggle-unselected", "children"),                    # Toggle button text
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
        Main callback to update the map state based on various user interactions.
        
        This callback is the central hub for all map-related user interactions.
        It determines which interaction triggered it and updates the map state accordingly.
        
        Args:
            unit_filter_clicks: Click counts for all unit filter buttons
            reset_selections_clicks: Click count for reset button
            chosen_year_range: Selected year range from slider
            ctrl_pressed: Whether Ctrl key was pressed during click
            geojson_n_clicks: Click count for GeoJSON layer
            toggle_unselected_clicks: Click count for toggle button
            map_state: Current map state data
            button_ids: IDs of all unit filter buttons
            geojson_clickData: Data for clicked polygon
            geojson_hideout: Current hideout settings
            geojson_data: Current GeoJSON data
            counts: Current counts of selected polygons
            toggle_unselected_children: Text for toggle button
            
        Returns:
            tuple: (new_map_state, counts, toggle_unselected_children)
        """
        # Determine which input triggered the callback
        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
        triggered_prop_ids = [t["prop_id"] for t in ctx.triggered]
        if not triggered_prop_ids:
            raise PreventUpdate

        logger.info("update_map_state triggered by %s", triggered_prop_ids)

        # Initialize or copy the map state
        new_map_state = (map_state or {}).copy()
        if "unit_types" not in new_map_state:
            new_map_state["unit_types"] = ["MOD_REG"]  # Default to Modern Regions
        if "selected_polygons" not in new_map_state:
            new_map_state["selected_polygons"] = []
        if "selected_polygons_unit_types" not in new_map_state:
            new_map_state["selected_polygons_unit_types"] = []

        # Flag to track if filtering options were changed
        filter_triggered = False

        # SECTION A: Handle Reset Button
        if "reset-selections.n_clicks" in triggered_prop_ids and reset_selections_clicks:
            logger.debug("reset-selections: clearing to ['MOD_REG']")
            # Reset to default state
            new_map_state["unit_types"] = ["MOD_REG"]
            new_map_state["selected_polygons"] = []
            new_map_state["selected_polygons_unit_types"] = []
            counts = {}
            filter_triggered = True

        # SECTION B: Handle Unit Filter Button Clicks
        unit_type_trigs = [t for t in triggered_prop_ids if "unit-filter" in t]
        for i, t in enumerate(unit_type_trigs):
            # Extract button ID from trigger
            dict_part = t.split(".")[0]
            try:
                dict_part = json.loads(dict_part)
            except:
                dict_part = None

            # Process the unit filter button click
            if dict_part and dict_part.get("type") == "unit-filter":
                clicked_type = dict_part["unit"]
                prev_types = set(new_map_state.get("unit_types", ["MOD_REG"]))
                current_types = set(prev_types)

                # Handle Ctrl+Click for multi-selection
                if ctrl_pressed:
                    # Toggle the clicked unit type
                    if clicked_type in current_types:
                        current_types.remove(clicked_type)
                        # Ensure at least one type is always selected
                        if not current_types:
                            current_types = {"MOD_REG"}
                    else:
                        current_types.add(clicked_type)
                    ctrl_pressed = False  # Reset ctrl flag
                else:
                    # Single choice (no Ctrl key): set to only the clicked type
                    current_types = {clicked_type}

                # Update map state with selected unit types
                new_map_state["unit_types"] = list(current_types)
                filter_triggered = True

        # SECTION C: Handle Year Range Slider Changes
        if "year-range-slider.value" in triggered_prop_ids and chosen_year_range:
            y0, y1 = chosen_year_range
            # Normalize years to ensure they don't exceed current year
            new_map_state["year_range"] = (
                normalize_year(y0),
                normalize_year(y1)
            )
            filter_triggered = True

        # Log if filters were updated
        if filter_triggered:
            logger.debug("Filters updated => stored in map-state")

        # SECTION D: Handle Polygon Selection (Map Clicks)
        elif "geojson-layer.n_clicks" in triggered_prop_ids and geojson_n_clicks:
            if geojson_clickData:
                # Extract the feature ID and unit type from the clicked polygon
                fid = geojson_clickData.get("id")
                unit_type = geojson_clickData['properties']['g_unit_type']
                if fid is not None:
                    # Toggle selection state of the clicked polygon
                    selected_ids = new_map_state.get("selected_polygons", [])
                    selected_units = new_map_state.get("selected_polygons_unit_types", [])
                    if fid in selected_ids:
                        # Deselect if already selected
                        fid_index = selected_ids.index(fid)
                        selected_ids.pop(fid_index)
                        selected_units.pop(fid_index)
                    else:
                        # Select if not already selected
                        selected_ids.append(fid)
                        selected_units.append(unit_type)
                    # Update map state with new selection
                    new_map_state["selected_polygons"] = selected_ids
                    new_map_state["selected_polygons_unit_types"] = selected_units

        # SECTION E: Handle Toggle Unselected Polygons Button
        elif "toggle-unselected.n_clicks" in triggered_prop_ids:
            # Toggle visibility of unselected polygons
            if map_state["show_unselected"]:
                new_map_state["show_unselected"] = False
                toggle_unselected_children = "Show unselected polygons"
            else:
                new_map_state["show_unselected"] = True
                toggle_unselected_children = "Hide unselected polygons"

        # Return the updated map state and related values
        return new_map_state, counts, toggle_unselected_children

    @app.callback(
        # Outputs
        [
            Output('year-range-container', 'style'),                  # Year range visibility
            Output('year-range-slider', 'min'),                       # Slider min value
            Output('year-range-slider', 'max'),                       # Slider max value
            Output('year-range-slider', 'marks'),                     # Slider marks
            Output("current_geojson", "data"),                        # GeoJSON store
            Output('geojson-layer', 'data'),                          # Map layer data
            Output('geojson-layer', 'hideout'),                       # Map layer settings
            Output('debug-output', 'children'),                       # Debug text
            Output({'type': 'unit-filter', 'unit': ALL}, 'style'),    # Unit button styles
            Output("counts-store", "data", allow_duplicate=True),     # Polygon counts
        ],
        # Inputs
        Input("map-state", "data"),                                  # Map state changes
        # States
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),            # Unit button IDs
        State("app-state", "data"),                                   # App state
        State("counts-store", "data"),                                # Polygon counts
        State("current_geojson", "data"),                             # Current GeoJSON
        State('geojson-layer', 'data'),                               # Map layer data
        # Run initially to set up the map
    )
    def render_map_display(map_state, button_ids, app_state, counts, current_geojson, map_geojson):
        """
        Update the map display based on the current map state.
        
        This callback handles:
        1. Showing/hiding the year range slider
        2. Setting the slider's min/max values
        3. Fetching and displaying polygons
        4. Styling unit filter buttons
        5. Counting selected polygons by unit type
        
        Args:
            map_state: Current map state data
            button_ids: IDs of all unit filter buttons
            app_state: Current application state
            counts: Current counts of selected polygons
            current_geojson: Current GeoJSON data store
            map_geojson: Current map GeoJSON layer data
            
        Returns:
            tuple: Multiple outputs for map display components
        """
        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
        
        # Initialize default map_state if None
        if not map_state:
            map_state = {"unit_types": ["MOD_REG"], "selected_polygons": []}

        # SECTION A: Show/hide year range slider based on selected unit types
        unit_types = map_state.get("unit_types", ["MOD_REG"])
        # Get list of unit types that don't require a year filter
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]
        # Show slider only if at least one selected unit type is time-dependent
        if any(ut not in timeless_unit_types for ut in unit_types):
            container_style = {'display': 'block'}
        else:
            container_style = {'display': 'none'}

        # SECTION B: Set up year range slider properties
        min_year = DEFAULT_MIN_YEAR
        max_year = CURRENT_YEAR
        step = max(1, (max_year - min_year) // 10)  # Calculate step for marks
        # Create marks for the slider
        slider_marks = {str(y): str(y) for y in range(min_year, max_year+1, step)}

        # SECTION C: Build polygons for active unit types
        year_range = map_state.get("year_range")
        
        # Initialize hideout for selected polygons
        new_hideout = {"type": "FeatureCollection", "features": []}
        # Determine if we need to fetch new polygons
        need_to_fetch = True
        current_features = []
        
        # Check if we can reuse existing polygons
        if current_geojson and 'features' in current_geojson:
            current_features = current_geojson['features']
            
            # Get all feature IDs and unit types in the current GeoJSON
            current_ids = set()
            current_unit_types = set()
            for feature in current_features:
                if 'id' in feature and 'properties' in feature and 'g_unit_type' in feature['properties']:
                    current_ids.add(feature['id'])
                    current_unit_types.add(feature['properties']['g_unit_type'])
            
            # If all requested unit types are already in the current GeoJSON,
            # and we're not changing the year range for non-timeless types, we can skip fetching
            if set(unit_types).issubset(current_unit_types):
                non_timeless_requested = [ut for ut in unit_types if ut not in timeless_unit_types]
                if not non_timeless_requested or not year_range:
                    need_to_fetch = False
                    debug_msg = f"Reusing {len(current_features)} polygons for {unit_types}"
        
        # Fetch polygons if needed
        if need_to_fetch:
            gdfs = []
            for ut in unit_types:
                # For time-dependent unit types, apply year range if specified
                if ut not in timeless_unit_types and year_range:
                    gdf = get_polygons_by_type(ut, year_range[0], year_range[1])
                else:
                    gdf = get_polygons_by_type(ut)
                gdfs.append(gdf)

            # Process the combined GeoDataFrames
            if gdfs:
                combined = pd.concat(gdfs)
                combined = combined.drop_duplicates()
                if combined.empty:
                    map_geojson = {"type": "FeatureCollection", "features": []}
                    debug_msg = "No polygons found."
                else:
                    map_geojson = json.loads(combined.to_json())
                    debug_msg = f"Showing {len(combined)} polygons for {unit_types}"
            else:
                debug_msg = "No polygons loaded."
        
        # SECTION D: Update hideout with selected polygons
        new_hideout["selected"] = map_state.get("selected_polygons", [])

        # SECTION E: Prepare button styles
        # Map unit types to their corresponding colors
        unit_colors = {k: v['color'] for k, v in UNIT_TYPES.items()}
        active_set = set(unit_types)
        button_styles = []

        # Create style for each unit filter button
        for b in button_ids:
            unit = b["unit"]
            unit_color = unit_colors.get(unit, 'blue')
            # Base style with CSS variable for hover effect
            style = {
                '--unit-color': unit_color,
                'borderColor': unit_color,
                'backgroundColor': 'white',
                'color': '#333',  # Default text color
                'transition': 'background-color 0.3s, color 0.3s'
            }
            # Apply active style if unit is selected
            if unit in active_set:
                style.update({
                    'backgroundColor': unit_color,
                    'color': 'white'
                })
            button_styles.append(style)

        # SECTION F: Update counts of selected polygons by unit type
        counts = counts or {}
        # Initialize counts for each unit type
        for unit_type in UNIT_TYPES.keys():
            counts[unit_type + '_g_units'] = counts.get(unit_type + '_g_units', [])
            counts[unit_type] = 0
        
        # Count selected polygons by unit type
        for selected_id, selected_id_unit_type in zip(
            map_state.get("selected_polygons", []), 
            map_state.get("selected_polygons_unit_types", [])
        ):
            if selected_id not in counts[selected_id_unit_type + '_g_units']:
                counts[selected_id_unit_type + '_g_units'].append(selected_id)
            counts[selected_id_unit_type] = counts.get(selected_id_unit_type, 0) + 1

        # SECTION G: Filter visible features based on show_unselected flag
        if not map_state.get("show_unselected", True):
            selected_ids = set(map_state.get("selected_polygons", []))
            new_hideout["features"] = [
                feature for feature in new_hideout.get("features", [])
                if feature["id"] in selected_ids
            ]
        
        # If we don't need to fetch new data, keep existing GeoJSON
        if not need_to_fetch:
            map_geojson = no_update

        # Return all updated values
        return (
            container_style,           # Year range container style
            min_year,                  # Year slider min
            max_year,                  # Year slider max
            slider_marks,              # Year slider marks
            current_geojson,           # Current GeoJSON store
            map_geojson,               # Map GeoJSON data
            new_hideout,               # Map hideout settings
            debug_msg,                 # Debug message
            button_styles,             # Button styles
            counts,                    # Polygon counts
        )