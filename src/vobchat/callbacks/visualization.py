# Simple Visualization Callback - Clean rewrite
# Single responsibility: Show/hide visualization based on place-state data

import pandas as pd
import json
import io
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from vobchat.tools import get_all_cube_data
from vobchat.state_schema import get_selected_units

import logging
logger = logging.getLogger(__name__)

def register_simple_visualization_callbacks(app):
    """Register simplified visualization callbacks - no more loops!"""
    
    @app.callback(
        Output("visualization-panel-container", "style"),
        Output("visualization-area", "style"), 
        Output("cube-selector", "options"),
        Output("cube-selector", "value", allow_duplicate=True),
        Input("place-state", "data"),  # ONLY listen to place-state
        State("cube-selector", "value"),
        prevent_initial_call=True
    )
    def handle_visualization_display(place_state, current_cube_selection):
        """Simple visualization display - show/hide based on data availability"""
        
        logger.info(f"Visualization callback triggered with place_state keys: {list(place_state.keys()) if place_state else 'None'}")
        
        # Styles for show/hide
        visible_container = {"flex": "0 0 40%", "display": "flex"}
        visible_area = {"height": "100%", "display": "flex", "flexDirection": "column"}
        hidden_container = {"flex": "0 0 0%", "display": "none"}
        hidden_area = {"height": "100%", "display": "none", "flexDirection": "column"}
        
        # Check if we have data to visualize
        has_cubes = place_state and place_state.get('cubes')
        has_places_and_theme = (
            place_state and 
            place_state.get('places') and 
            place_state.get('selected_theme')
        )
        
        should_show = has_cubes or has_places_and_theme
        
        if not should_show:
            logger.info("Hiding visualization - no data available")
            return hidden_container, hidden_area, [], []
        
        try:
            # Get cubes data
            cubes = place_state.get("cubes", [])
            
            # If no cubes but have places+theme, try to generate options
            # But skip if we're currently processing cube data (avoid unnecessary calls)
            # Also skip if we have selected_cubes field (workflow is handling it)
            if (not cubes and has_places_and_theme and 
                not place_state.get('show_visualization') and 
                not place_state.get('selected_cubes')):
                logger.info("No cubes but have places+theme - generating cube options")
                selected_theme = place_state.get('selected_theme')
                places = place_state.get('places', [])
                
                if places and selected_theme:
                    try:
                        # Get first place's g_unit for theme lookup
                        first_unit = next((p.get('g_unit') for p in places if p.get('g_unit')), None)
                        if first_unit:
                            from vobchat.tools import find_themes_for_unit
                            theme_cubes_json = find_themes_for_unit(str(first_unit))
                            theme_cubes_df = pd.read_json(io.StringIO(theme_cubes_json), orient='records')
                            
                            # Filter to current theme
                            if isinstance(selected_theme, str):
                                theme_data = json.loads(selected_theme)
                            else:
                                theme_data = selected_theme
                                
                            if 'ent_id' in theme_data:
                                current_theme_cubes = theme_cubes_df[theme_cubes_df['ent_id'] == theme_data['ent_id']]
                                if not current_theme_cubes.empty:
                                    cubes = current_theme_cubes.to_json(orient='records')
                                    logger.info(f"Generated {len(current_theme_cubes)} cube options from theme")
                    except Exception as e:
                        logger.warning(f"Error generating cube options: {e}")
            
            # If still no cubes, show empty visualization
            if not cubes:
                logger.info("No cubes available - showing empty visualization")
                return visible_container, visible_area, [], []
            
            # Parse cubes data
            if isinstance(cubes, str):
                cubes_df = pd.read_json(io.StringIO(cubes), orient='records')
            else:
                cubes_df = pd.DataFrame(cubes)
            
            # Find cube ID column
            cube_id_col = None
            for col in ['Cube_ID', 'cube_id', 'CubeID']:
                if col in cubes_df.columns:
                    cube_id_col = col
                    break
            
            if not cube_id_col:
                logger.warning("No cube ID column found")
                return visible_container, visible_area, [], []
            
            # Create cube options
            cube_col = 'Cube' if 'Cube' in cubes_df.columns else 'cube'
            if cube_col in cubes_df.columns:
                options = [
                    {"label": row[cube_col], "value": row[cube_id_col]}
                    for _, row in cubes_df.iterrows()
                ]
            else:
                options = [
                    {"label": f"Cube {row[cube_id_col]}", "value": row[cube_id_col]}
                    for _, row in cubes_df.iterrows()
                ]
            
            # Preserve current selection if valid
            cube_ids = cubes_df[cube_id_col].tolist()
            if current_cube_selection and all(cube in cube_ids for cube in current_cube_selection):
                cube_value = current_cube_selection
            else:
                cube_value = cube_ids[:1] if cube_ids else []
            
            logger.info(f"Showing visualization with {len(options)} cube options")
            return visible_container, visible_area, options, cube_value
            
        except Exception as e:
            logger.error(f"Error in visualization callback: {e}", exc_info=True)
            return hidden_container, hidden_area, [], []
    
    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        Input("place-state", "data"),
        prevent_initial_call=True
    )
    def update_visualization_plot(selected_cubes, place_state):
        """Simple plot update - generate chart from selected data"""
        
        logger.info(f"Plot update with cubes: {selected_cubes}")
        
        # Empty chart function
        def empty_chart(title="No data available"):
            return go.Figure().update_layout(
                title=title,
                xaxis_title="Year",
                yaxis_title="Value",
                annotations=[{
                    'text': title,
                    'xref': "paper", 'yref': "paper",
                    'x': 0.5, 'y': 0.5,
                    'xanchor': 'center', 'yanchor': 'middle',
                    'showarrow': False,
                    'font': {'size': 16, 'color': 'gray'}
                }]
            )
        
        # Check data availability
        if not place_state or not place_state.get('places'):
            return empty_chart("No areas selected")
        
        if not selected_cubes:
            return empty_chart("No data filters selected")
        
        try:
            # Get selected units
            places = place_state.get('places', [])
            selected_units = [p.get('g_unit') for p in places if p.get('g_unit')]
            
            if not selected_units:
                return empty_chart("No valid areas found")
            
            # Ensure selected_cubes is a list
            if not isinstance(selected_cubes, list):
                selected_cubes = [selected_cubes]
            
            # Fetch data for each unit
            all_data_list = []
            for g_unit in selected_units:
                try:
                    cube_data_json = get_all_cube_data.invoke({
                        "g_unit": str(g_unit), 
                        "cube_ids": selected_cubes
                    })
                    cube_data_df = pd.read_json(io.StringIO(cube_data_json), orient='records')
                    if not cube_data_df.empty:
                        all_data_list.append(cube_data_df)
                except Exception as e:
                    logger.warning(f"Error fetching data for unit {g_unit}: {e}")
                    continue
            
            if not all_data_list:
                return empty_chart("No data available for selected areas")
            
            # Combine and process data
            all_data_df = pd.concat(all_data_list, ignore_index=True)
            
            # Melt data for plotting
            id_vars = ['g_name', 'year']
            value_vars = [col for col in all_data_df.columns if col not in id_vars]
            
            if not value_vars:
                return empty_chart("No data columns found")
            
            chart_data = pd.melt(
                all_data_df, 
                id_vars=id_vars, 
                value_vars=value_vars,
                var_name='measurement', 
                value_name='value'
            )
            
            # Clean data
            chart_data = chart_data.dropna(subset=['value'])
            chart_data['year'] = pd.to_numeric(chart_data['year'], errors='coerce')
            chart_data = chart_data.dropna(subset=['year'])
            
            if chart_data.empty:
                return empty_chart("No valid data after filtering")
            
            # Create display names and plot
            chart_data['display_name'] = chart_data['g_name'] + ' - ' + chart_data['measurement']
            chart_data = chart_data.sort_values(['g_name', 'measurement', 'year'])
            
            fig = px.line(
                chart_data, 
                x='year', 
                y='value', 
                color='display_name',
                title="Historical Data Visualization",
                markers=True
            )
            
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value", 
                hovermode='x unified',
                margin={"l": 50, "r": 50, "t": 50, "b": 50},
                height=None,
                autosize=True,
                legend_title_text='Series'
            )
            
            if (chart_data['value'].dropna() >= 0).all():
                fig.update_yaxes(rangemode='tozero')
            
            return fig
            
        except Exception as e:
            logger.error(f"Error updating plot: {e}", exc_info=True)
            return empty_chart(f"Error: {str(e)}")
    
    @app.callback(
        Output("visualization-panel-container", "style", allow_duplicate=True),
        Output("visualization-area", "style", allow_duplicate=True),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("data-plot", "figure", allow_duplicate=True),
        Input("clear-plot-button", "n_clicks"),
        prevent_initial_call=True
    )
    def clear_visualization(n_clicks):
        """Simple clear - just hide the visualization"""
        if n_clicks:
            hidden_container = {"flex": "0 0 0%", "display": "none"}
            hidden_area = {"height": "100%", "display": "none", "flexDirection": "column"}
            return hidden_container, hidden_area, [], go.Figure()
        raise PreventUpdate