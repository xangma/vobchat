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
        Output('year-range-container', 'style'),
        Input('unit-filter-state', 'data'),
        prevent_initial_call=True
    )
    def update_year_range_visibility(unit_state):
        ctx = callback_context
        logger.info(f"""
            Callback: update_year_range_visibility
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Input unit_state: {unit_state}
        """)
        
        if not unit_state:
            logger.debug("No unit state provided, hiding year range")
            return {'display': 'none'}
        
        unit_type = unit_state.get('unit_type', 'MOD_REG')
        logger.debug(f"Unit type: {unit_type}")
        
        if unit_type in TIMELESS_UNIT_TYPES:
            logger.debug(f"Unit type {unit_type} is timeless, hiding year range")
            return {'display': 'none'}
        
        logger.debug(f"Showing year range for unit type {unit_type}")
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
        ctx = callback_context
        logger.info(f"""
            Callback: update_map_from_store
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Selected IDs: {selected_ids}
        """)
        
        if not selected_ids:
            logger.debug("No polygons selected")
            return current_fig, "No polygons selected."
            
        logger.debug("Loading GeoDataFrame from JSON")
        current_gdf = gpd.GeoDataFrame.from_features(json.loads(current_gdf_json))
        
        if type(selected_ids) is int:
            selected_ids = [selected_ids]
            logger.debug(f"Converted single ID to list: {selected_ids}")
            
        logger.debug(f"Filtering GeoDataFrame for IDs: {selected_ids}")
        gdf_filtered = current_gdf[current_gdf.index.isin(selected_ids)]

        map_properties = calculate_center_and_zoom(gdf_filtered)
        logger.debug(f"Calculated map properties: {map_properties}")
        
        if map_properties["center"] and map_properties["zoom"] is not None:
            current_fig['layout']['mapbox']['center'] = map_properties["center"]
            current_fig['layout']['mapbox']['zoom'] = map_properties["zoom"]
            logger.debug("Updated map center and zoom")

        for i, d in enumerate(current_fig['data']):
            if d['type'] == 'choroplethmapbox':
                current_fig['data'][i]['selectedpoints'] = selected_ids
                logger.debug(f"Updated selectedpoints for layer {i}")

        debug_info = f"Zoomed to selected polygons: Center=({map_properties['center']}), Zoom={map_properties['zoom']}"
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
        ctx = callback_context
        logger.info(f"""
            Callback: update_slider_range
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Filter state: {filter_state}
        """)
        
        if not filter_state or not filter_state.get('year_bounds'):
            logger.debug("Using default MOD_REG range")
            unit_range = date_ranges_df[date_ranges_df['g_unit_type'] == 'MOD_REG'].iloc[0]
            min_year = max(int(unit_range['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(unit_range['max_year']))
        else:
            logger.debug("Using filter state year bounds")
            min_year = max(filter_state['year_bounds'][0], DEFAULT_MIN_YEAR)
            max_year = normalize_year(filter_state['year_bounds'][1])
        
        step = max(1, (max_year - min_year) // 10)
        marks = {str(year): str(year) for year in range(min_year, max_year + 1, step)}
        logger.debug(f"Created marks with step {step}")
        
        current_year = normalize_year(max_year)
        value = filter_state.get('year_range', [current_year, current_year]) if filter_state.get('year_range') else [current_year, current_year]
        value = [normalize_year(v) for v in value]
        logger.debug(f"Set slider value to {value}")
        
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
        ctx = callback_context
        logger.info(f"""
            Callback: update_year_range_bounds
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Unit state: {unit_state}
        """)
        
        try:
            if not unit_state:
                unit_type = 'MOD_REG'
                logger.debug("No unit state provided, defaulting to MOD_REG")
            else:
                unit_type = unit_state.get('unit_type', 'MOD_REG')
                logger.debug(f"Using unit type: {unit_type}")
            
            # Get range for current unit type
            unit_range = date_ranges_df[date_ranges_df['g_unit_type'] == unit_type].iloc[0]
            min_year = max(int(unit_range['min_year']), DEFAULT_MIN_YEAR)
            max_year = normalize_year(int(unit_range['max_year']))
            logger.debug(f"Year range: {min_year} - {max_year}")
            
            # Create marks
            step = max(1, (max_year - min_year) // 10)
            marks = {str(year): str(year) for year in range(min_year, max_year + 1, step)}
            logger.debug(f"Created marks with step size {step}")
            
            current_year = normalize_year(max_year)
            value = [current_year, current_year]
            logger.debug(f"Set default value to {value}")
            
            bounds = {'min': min_year, 'max': max_year}
            logger.debug(f"Set year bounds: {bounds}")
            
            return bounds, min_year, max_year, marks, value
            
        except Exception as e:
            logger.error(f"Error updating year range bounds: {str(e)}")
            return no_update, no_update, no_update, no_update, no_update


    # Callback to update unit filter state
    @app.callback(
        Output('unit-filter-state', 'data'),
        [Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
        Input('reset-filters', 'n_clicks')],
        State('unit-filter-state', 'data'),
        prevent_initial_call=True
    )
    def update_unit_filter_state(unit_clicks, reset_clicks, current_state):
        logger.info(f"Callback: update_unit_filter_state")
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        
        triggered_id = ctx.triggered[0]['prop_id']
        
        if 'reset-filters' in triggered_id:
            return {'unit_types': ['MOD_REG']}
        
        try:
            button_id = json.loads(triggered_id.split('.')[0])
            current_types = set(current_state.get('unit_types', ['MOD_REG']))
            clicked_type = button_id['unit']
            
            # Toggle the clicked type
            if clicked_type in current_types:
                current_types.remove(clicked_type)
                # Ensure at least one type is selected
                if not current_types:
                    current_types.add('MOD_REG')
            else:
                current_types.add(clicked_type)
            
            return {'unit_types': list(current_types)}
        except:
            return current_state if current_state else {'unit_types': ['MOD_REG']}

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
        ctx = callback_context
        logger.info(f"""
            Callback: update_map_filter
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Unit state: {unit_state}
            Year range: {year_range}
        """)
        
        if not unit_state:
            logger.warning("No unit state provided")
            raise PreventUpdate
        
        try:
            unit_types = unit_state.get('unit_types', ['MOD_REG'])
            logger.debug(f"Processing unit types: {unit_types}")
            
            # Initialize button states
            button_colors = ['secondary'] * len(UNIT_TYPES)
            button_outlines = [True] * len(UNIT_TYPES)
            
            # Update button states based on current unit types
            for i, bid in enumerate(button_ids):
                if bid['unit'] in unit_types:
                    button_colors[i] = 'primary'
                    button_outlines[i] = False
            logger.debug(f"Updated button states for {len(button_ids)} buttons")
            
            # Get and combine polygons for all selected unit types
            filtered_gdfs = []
            for unit_type in unit_types:
                logger.debug(f"Fetching polygons for unit type: {unit_type}")
                if year_range and unit_type not in TIMELESS_UNIT_TYPES:
                    gdf = get_polygons_by_type(unit_type, year_range[0], year_range[1])
                    logger.debug(f"Retrieved {len(gdf)} polygons for years {year_range[0]}-{year_range[1]}")
                else:
                    gdf = get_polygons_by_type(unit_type)
                    logger.debug(f"Retrieved {len(gdf)} polygons (timeless)")
                filtered_gdfs.append(gdf)
            
            # Combine GeoDataFrames
            filtered_gdf = pd.concat(filtered_gdfs)
            logger.debug(f"Combined GeoDataFrame has {len(filtered_gdf)} total polygons")
            
            if filtered_gdf.empty:
                logger.warning("No polygons found for selected types")
                debug_msg = f"No polygons found for selected types"
                return figure, filtered_gdf.to_json(), debug_msg, button_colors, button_outlines

            # Update the figure
            try:
                geojson_data = json.loads(filtered_gdf.to_json())
                locations = filtered_gdf.index.tolist()
                figure['data'][0]['locations'] = locations
                figure['data'][0]['geojson'] = geojson_data
                figure['data'][0]['selectedpoints'] = None
                figure['data'][0]['z'] = [1] * len(locations)
                logger.debug("Successfully updated figure data")
                
                # Update map center and zoom
                center_lat = filtered_gdf.to_crs('+proj=cea').centroid.to_crs(filtered_gdf.crs).y.mean()
                center_lon = filtered_gdf.to_crs('+proj=cea').centroid.to_crs(filtered_gdf.crs).x.mean()
                figure['layout']['mapbox']['center'] = {"lat": center_lat, "lon": center_lon}
                logger.debug(f"Updated map center to lat: {center_lat}, lon: {center_lon}")
                
            except Exception as e:
                logger.error(f"Error updating figure: {str(e)}")
                return figure, filtered_gdf.to_json(), "Error updating map", button_colors, button_outlines
            
            debug_msg = f"Showing {unit_types} units ({len(filtered_gdf)} polygons)"
            if year_range and unit_types not in TIMELESS_UNIT_TYPES:
                debug_msg += f" for years {year_range[0]}-{year_range[1]}"
            
            return (
                figure,
                filtered_gdf.to_json(),
                debug_msg,
                button_colors,
                button_outlines
            )
            
        except Exception as e:
            logger.error(f"Error in update_map_filter: {str(e)}")
            return figure, None, "Error updating map filter", button_colors, button_outlines


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
        ctx = callback_context
        logger.info(f"""
            Callback: handle_click
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Click data: {clickData}
            Current filter: {current_filter}
        """)
        
        if not clickData:
            logger.debug("No click data received")
            return current_fig, "No polygon selected."
            
        clicked_id = clickData['points'][0]['location']
        logger.debug(f"Processing click on polygon ID: {clicked_id}")
        
        if not current_gdf_json:
            logger.warning("No GeoDataFrame available")
            return current_fig, "No GeoDataFrame available"
            
        try:
            gdf_jsons = json.loads(current_gdf_json)
            current_gdf = gpd.GeoDataFrame.from_features(gdf_jsons)
            ids = pd.DataFrame(gdf_jsons['features'])['id']
            current_gdf['id'] = ids
            logger.debug("Successfully loaded GeoDataFrame from JSON")
        except Exception as e:
            logger.error(f"Error loading GeoDataFrame: {str(e)}")
            return current_fig, "Error processing GeoDataFrame"
        
        if current_filter and str(clicked_id) in current_gdf.index:
            if current_gdf.loc[str(clicked_id), 'g_unit_type'] != current_filter:
                logger.debug(f"Selected polygon {clicked_id} does not match filter {current_filter}")
                return current_fig, f"Selected polygon does not match current filter: {current_filter}"
        
        debug_info = f"Single polygon selected with ID: {clicked_id}\n"
        
        try:
            for i, d in enumerate(current_fig['data']):
                if d['type'] == 'choroplethmapbox':
                    row_idx = current_gdf.index[current_gdf.id == str(clicked_id)].tolist()
                    current_fig['data'][i]['selectedpoints'] = row_idx
                    logger.debug(f"Updated selectedpoints for layer {i} with indices {row_idx}")
        except Exception as e:
            logger.error(f"Error updating figure: {str(e)}")
            return current_fig, "Error updating selection"
            
        return current_fig, debug_info


    @app.callback(
        Output('choropleth-map', 'figure', allow_duplicate=True),
        Output('debug-output', 'children', allow_duplicate=True),
        Input('choropleth-map', 'selectedData'),
        State('choropleth-map', 'figure'),
        prevent_initial_call=True
    )
    def handle_box_lasso(selectedData, current_fig):
        ctx = callback_context
        logger.info(f"""
            Callback: handle_box_lasso
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Selected data points: {len(selectedData['points']) if selectedData else 0}
        """)
        
        if not selectedData:
            logger.debug("No selection data received")
            return current_fig, "No polygons selected."
            
        try:
            selected_ids = [p['location'] for p in selectedData['points']]
            logger.debug(f"Extracted {len(selected_ids)} polygon IDs from selection")
            
            debug_info = f"Box/Lasso selection made with {len(selected_ids)} polygons selected.\n"
            debug_info += f"Selected polygon IDs: {selected_ids}\n"
            
            for i, d in enumerate(current_fig['data']):
                if d['type'] == 'choroplethmapbox':
                    current_fig['data'][i]['selectedpoints'] = selected_ids
                    logger.debug(f"Updated selectedpoints for layer {i}")
                    
            return current_fig, debug_info
            
        except Exception as e:
            logger.error(f"Error processing box/lasso selection: {str(e)}")
            return current_fig, "Error processing selection"


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
        ctx = callback_context
        logger.info(f"""
            Callback: reset_selection
            Triggered by: {ctx.triggered[0]['prop_id'] if ctx.triggered else 'No trigger'}
            Reset button clicks: {n_clicks}
        """)
        
        if n_clicks > 0:
            try:
                for i, d in enumerate(current_fig['data']):
                    if d['type'] == 'choroplethmapbox':
                        current_fig['data'][i]['selectedpoints'] = None
                        logger.debug(f"Cleared selection for layer {i}")
                logger.debug("Successfully reset all selections")
                return current_fig, [], "Reset button clicked. All selections cleared."
            except Exception as e:
                logger.error(f"Error resetting selection: {str(e)}")
                return current_fig, [], "Error resetting selections"

        return current_fig, [], ""