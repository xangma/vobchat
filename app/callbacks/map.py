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
        Output('year-range-container', 'style'),
        Input('unit-filter-state', 'data'),
        prevent_initial_call=True
    )
    def update_year_range_visibility(unit_state):
        if not unit_state:
            return {'display': 'none'}
        
        unit_type = unit_state.get('unit_type', 'MOD_REG')
        
        # Hide the slider for timeless unit types
        if unit_type in TIMELESS_UNIT_TYPES:
            return {'display': 'none'}
        
        return {'display': 'block'}

    # Callback to update the map from the selected polygons in dcc.Store
    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        Input('selected_ids', 'data'),
        State('choropleth-map', 'figure'),
        State('current-gdf', 'data'),
        prevent_initial_call=True
    )
    def update_map_from_store(selected_ids, current_fig, current_gdf_json):
        if not selected_ids:
            return current_fig, "No polygons selected."
        current_gdf = gpd.GeoDataFrame.from_features(json.loads(current_gdf_json))
        # Filter the GeoDataFrame based on selected IDs
        if type(selected_ids) is int:
            selected_ids = [selected_ids]
        gdf_filtered = current_gdf[current_gdf.index.isin(selected_ids)]

        # Get center and zoom from helper function
        map_properties = calculate_center_and_zoom(gdf_filtered)
        if map_properties["center"] and map_properties["zoom"] is not None:
            current_fig['layout']['mapbox']['center'] = map_properties["center"]
            current_fig['layout']['mapbox']['zoom'] = map_properties["zoom"]

        for i, d in enumerate(current_fig['data']):
            if d['type'] == 'choroplethmapbox':
                current_fig['data'][i]['selectedpoints'] = selected_ids

        debug_info = f"Zoomed to selected polygons: Center=({map_properties["center"]}), Zoom={map_properties["zoom"]}"
        return current_fig, debug_info

    # Callback to update slider based on filter state
    @app.callback(
        Output('year-range-slider', 'min'),
        Output('year-range-slider', 'max'),
        Output('year-range-slider', 'value'),
        Output('year-range-slider', 'marks'),
        Input('filter-state', 'data')
    )
    def update_slider_range(filter_state):
        if not filter_state or not filter_state.get('year_bounds'):
            # Default to MOD_REG range
            unit_range = date_ranges_df[date_ranges_df['g_unit_type'] == 'MOD_REG'].iloc[0]
            min_year = max(int(unit_range['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(unit_range['max_year']))
        else:
            min_year = max(filter_state['year_bounds'][0], DEFAULT_MIN_YEAR)
            max_year = normalize_year(filter_state['year_bounds'][1])
        
        # Create marks with reasonable intervals
        step = max(1, (max_year - min_year) // 10)
        marks = {str(year): str(year) for year in range(min_year, max_year + 1, step)}
        
        # Use current year range or default to max year
        current_year = normalize_year(max_year)
        value = filter_state.get('year_range', [current_year, current_year]) if filter_state['year_range'] else [current_year, current_year]
        value = [normalize_year(v) for v in value]
        
        return min_year, max_year, value, marks


    @app.callback(
        Output('year-range-bounds', 'data', allow_duplicate=True),
        Output('year-range-slider', 'min', allow_duplicate=True),
        Output('year-range-slider', 'max', allow_duplicate=True),
        Output('year-range-slider', 'marks', allow_duplicate=True),
        Output('year-range-slider', 'value', allow_duplicate=True),
        Input('unit-filter-state', 'data'),
        prevent_initial_call=True
    )
    def update_year_range_bounds(unit_state):
        if not unit_state:
            unit_type = 'MOD_REG'
        else:
            unit_type = unit_state.get('unit_type', 'MOD_REG')
        
        # Get range for current unit type
        unit_range = date_ranges_df[date_ranges_df['g_unit_type'] == unit_type].iloc[0]
        min_year = max(int(unit_range['min_year']), DEFAULT_MIN_YEAR)
        max_year = normalize_year(int(unit_range['max_year']))
        
        # Create marks with reasonable intervals
        step = max(1, (max_year - min_year) // 10)
        marks = {str(year): str(year) for year in range(min_year, max_year + 1, step)}
        
        # Set default value to current year or max year if less than current
        current_year = normalize_year(max_year)
        value = [current_year, current_year]
        
        bounds = {'min': min_year, 'max': max_year}
        
        return bounds, min_year, max_year, marks, value


    # Callback to update unit filter state
    @app.callback(
        Output('unit-filter-state', 'data'),
        [Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
        Input('reset-filters', 'n_clicks')],
        State('unit-filter-state', 'data'),
        prevent_initial_call=True
    )
    def update_unit_filter_state(unit_clicks, reset_clicks, current_state):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        
        triggered_id = ctx.triggered[0]['prop_id']
        
        if 'reset-filters' in triggered_id:
            return {'unit_type': 'MOD_REG'}
        
        try:
            button_id = json.loads(triggered_id.split('.')[0])
            return {'unit_type': button_id['unit']}
        except:
            return current_state if current_state else {'unit_type': 'MOD_REG'}


    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('current-gdf', 'data'),
        Output('debug-output', 'children'),
        [Output({'type': 'unit-filter', 'unit': ALL}, 'color')],
        [Output({'type': 'unit-filter', 'unit': ALL}, 'outline')],
        Input('unit-filter-state', 'data'),
        Input('year-range-slider', 'value'),
        [State('choropleth-map', 'figure'),
        State({'type': 'unit-filter', 'unit': ALL}, 'id')],
        prevent_initial_call=True
    )
    def update_map_filter(unit_state, year_range, figure, button_ids):
        if not unit_state:
            raise PreventUpdate
        
        unit_type = unit_state.get('unit_type', 'MOD_REG')
        
        # Initialize button colors and outlines
        button_colors = ['secondary'] * len(UNIT_TYPES)
        button_outlines = [True] * len(UNIT_TYPES)
        
        # Update button colors based on current unit type
        for i, bid in enumerate(button_ids):
            if bid['unit'] == unit_type:
                button_colors[i] = 'primary'
                button_outlines[i] = False
        
        # Get polygons for the selected unit type and year range
        if year_range and unit_type not in TIMELESS_UNIT_TYPES:
            filtered_gdf = get_polygons_by_type(unit_type, year_range[0], year_range[1])
        else:
            filtered_gdf = get_polygons_by_type(unit_type)

        if filtered_gdf.empty:
            debug_msg = f"No polygons found for {unit_type}"
            if year_range and unit_type not in TIMELESS_UNIT_TYPES:
                debug_msg += f" in year range {year_range[0]}-{year_range[1]}"
            return figure, filtered_gdf.to_json(), debug_msg, button_colors, button_outlines
        
        geojson_data = json.loads(filtered_gdf.to_json())
        locations = filtered_gdf.index.tolist()
        # Update the figure with new polygons
        figure['data'][0]['locations'] = locations
        figure['data'][0]['geojson'] = geojson_data
        figure['data'][0]['selectedpoints'] = None
        figure['data'][0]['z'] = [1] * len(locations)  # Set all polygons to the same color
        
        
        # Update map center and zoom
        center_lat = filtered_gdf.to_crs('+proj=cea').centroid.to_crs(filtered_gdf.crs).y.mean()
        center_lon = filtered_gdf.to_crs('+proj=cea').centroid.to_crs(filtered_gdf.crs).x.mean()
        figure['layout']['mapbox']['center'] = {"lat": center_lat, "lon": center_lon}
        
        debug_msg = f"Showing {unit_type} units ({len(filtered_gdf)} polygons)"
        if year_range and unit_type not in TIMELESS_UNIT_TYPES:
            debug_msg += f" for years {year_range[0]}-{year_range[1]}"
        
        return (
            figure,
            filtered_gdf.to_json(),
            debug_msg,
            button_colors,
            button_outlines
        )

    # Modify the handle_click callback to use the current filtered GeoDataFrame
    @app.callback(
        Output('choropleth-map', 'figure'),
        Output('debug-output', 'children', allow_duplicate=True),
        Input('choropleth-map', 'clickData'),
        [State('choropleth-map', 'figure'),
        State('current-filter', 'data'),
        State('current-gdf', 'data')],
        prevent_initial_call=True
    )
    def handle_click(clickData, current_fig, current_filter, current_gdf_json):
        if clickData:
            clicked_id = clickData['points'][0]['location']
            
            # Convert the current GeoDataFrame from JSON
            if current_gdf_json:
                current_gdf = gpd.GeoDataFrame.from_features(json.loads(current_gdf_json))
                current_gdf.set_index('id', inplace=True)
            else:
                return current_fig, "No GeoDataFrame available"
            
            # If there's a filter active, check if the clicked point matches the filter
            if current_filter and str(clicked_id) in current_gdf.index:
                if current_gdf.loc[str(clicked_id), 'g_unit_type'] != current_filter:
                    return current_fig, f"Selected polygon does not match current filter: {current_filter}"
            
            debug_info = f"Single polygon selected with ID: {clicked_id}\n"
            for i, d in enumerate(current_fig['data']):
                if d['type'] == 'choroplethmapbox':
                    current_fig['data'][i]['selectedpoints'] = [clicked_id]
            return current_fig, debug_info

        return current_fig, "No polygon selected."


    # Callback for selectedData (box/lasso selection)
    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        Input('choropleth-map', 'selectedData'),
        State('choropleth-map', 'figure'),
        prevent_initial_call=True
    )
    def handle_box_lasso(selectedData, current_fig):
        if selectedData:
            selected_ids = [p['location'] for p in selectedData['points']]
            debug_info = f"Box/Lasso selection made with {
                len(selected_ids)} polygons selected.\n"
            debug_info += f"Selected polygon IDs: {selected_ids}\n"
            for i, d in enumerate(current_fig['data']):
                if d['type'] == 'choroplethmapbox':
                    current_fig['data'][i]['selectedpoints'] = selected_ids
            return current_fig, debug_info

        return current_fig, "No polygons selected."

    # Reset button callback
    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output("selected_ids", "data", allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        Input('reset-btn', 'n_clicks'),
        State('choropleth-map', 'figure'),
        prevent_initial_call=True
    )
    def reset_selection(n_clicks, current_fig):
        if n_clicks > 0:
            for i, d in enumerate(current_fig['data']):
                if d['type'] == 'choroplethmapbox':
                    current_fig['data'][i]['selectedpoints'] = None
            return current_fig, [], "Reset button clicked. All selections cleared."

        return current_fig, [], ""