# app/callbacks/visualization.py

import pandas as pd
import json
import io
import asyncio
import threading
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from vobchat.tools import get_all_cube_data

# Background loop removed - now using fresh workflow instances with ThreadPoolExecutor

# This file contains all the callbacks for data visualization
def register_visualization_callbacks(app, compiled_workflow):

    @app.callback(
        Output("visualization-panel-container", "style"), # Controls the outer container
        Output("visualization-area", "style"),          # Controls the inner content div
        Output("cube-selector", "options"),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("visualization-area", "data-was-hidden"), # Keep this if JS uses it
        Input("app-state", "data"),
        Input("place-state", "data"),
        State("thread-id", "data"),
        State("visualization-area", "data-was-hidden"),
        State("cube-selector", "value"),
        prevent_initial_call=True
    )
    def handle_visualization_request(app_state, place_state, thread_id, current_visibility_data_attr, current_cube_selection):
        import logging
        logger = logging.getLogger(__name__)
        print(f"DEBUG: Visualization callback triggered! app_state={app_state is not None}, place_state={place_state is not None}")
        print(f"DEBUG: place_state keys: {list(place_state.keys()) if place_state else 'None'}")
        print(f"DEBUG: place_state cubes: {bool(place_state.get('cubes')) if place_state else 'None'}")

        # Define styles for visible/hidden states
        # Style for the outer container
        visible_container_style = {"flex": "0 0 40%", "display": "flex"}
        hidden_container_style = {"flex": "0 0 0%", "display": "none"}
        # Style for the inner visualization-area div
        visible_area_style = {"height": "100%", "display": "flex", "flexDirection": "column", "position": "relative"}
        hidden_area_style = {"height": "100%", "display": "none", "flexDirection": "column", "position": "relative"}

        logger.info(f"Visualization callback triggered - app_state: {app_state is not None}, show_visualization: {app_state.get('show_visualization') if app_state else 'None'}")
        logger.info(f"Place state cubes: {bool(place_state.get('cubes') if place_state else False)}")

        # Check if we have cube data - if so, show visualization regardless of app_state
        has_cubes = place_state and place_state.get('cubes')
        should_show = (app_state and app_state.get("show_visualization")) or has_cubes
        
        print(f"DEBUG: Visualization decision - has_cubes: {bool(has_cubes)}, should_show: {should_show}")
        print(f"DEBUG: app_state.show_visualization: {app_state.get('show_visualization') if app_state else 'None'}")
        logger.info(f"Visualization decision - has_cubes: {bool(has_cubes)}, should_show: {should_show}")

        if not should_show:
            print(f"DEBUG: HIDING visualization - no cubes and show_visualization=False")
            logger.info("Hiding visualization - no cubes and show_visualization=False")
            # Return styles to hide BOTH container and inner area
            hidden_container_style = {"flex": "0 0 0%", "display": "none"}
            hidden_area_style = {"height": "100%", "display": "none", "flexDirection": "column", "position": "relative"}
            return hidden_container_style, hidden_area_style, [], [], "true"

        try:
            print(f"DEBUG: SHOWING visualization - processing cube data")
            # Get cube data directly from place_state (populated by SSE system)
            cubes = place_state.get("cubes", [])
            print(f"DEBUG: Retrieved cubes: {type(cubes)} - {len(cubes) if isinstance(cubes, (list, str)) else 'N/A'}")
            logger.info(f"Retrieved cubes from place_state: {type(cubes)} - {len(cubes) if isinstance(cubes, (list, str)) else 'N/A'}")
            if not cubes:
                print(f"DEBUG: No cubes found in place_state - hiding visualization")
                logger.info("No cubes found in place_state - hiding visualization")
                # Hide both if no cubes
                return hidden_container_style, hidden_area_style, [], [], "true"

            # Handle cubes data - if it's a string (JSON), parse it; if it's already a list, use directly
            print(f"DEBUG: About to parse cube data")
            if isinstance(cubes, str):
                cubes_df = pd.read_json(io.StringIO(cubes), orient='records')
            else:
                cubes_df = pd.DataFrame(cubes)
            print(f"DEBUG: Parsed cubes_df successfully: {len(cubes_df)} rows")
            cube_ids = cubes_df['Cube_ID'].tolist()
            print(f"DEBUG: Extracted cube_ids: {cube_ids}")
            logger.info(f"Processed cubes_df: {len(cubes_df)} rows, cube_ids: {cube_ids}")

            # Preserve current cube selection if it's valid, otherwise use first cube as default
            if current_cube_selection and isinstance(current_cube_selection, list) and len(current_cube_selection) > 0:
                # Check if current selection is still valid (all selected cubes exist in available options)
                valid_selection = [cube for cube in current_cube_selection if cube in cube_ids]
                cube_selector_value = valid_selection if valid_selection else cube_ids[:1]
            else:
                cube_selector_value = cube_ids[:1]
            print(f"DEBUG: Selected cube_selector_value: {cube_selector_value}")
            
            g_units = place_state.get('selected_place_g_units', [])
            print(f"DEBUG: g_units: {g_units}")
            if not g_units:
                 print(f"DEBUG: No g_units - hiding visualization")
                 # Hide both if no units selected
                 return hidden_container_style, hidden_area_style, [], [], "true"

            print(f"DEBUG: About to process g_units data for visualization")
            cube_list = []
            for g_unit in g_units:
                 try:
                    print(f"DEBUG: Getting cube data for g_unit: {g_unit}, cube_ids: {cube_ids}")
                    # Ensure get_all_cube_data is defined and imported in tools.py
                    cube_data = pd.read_json(io.StringIO(get_all_cube_data({"g_unit": str(g_unit), "cube_ids": cube_ids})), orient='records')
                    print(f"DEBUG: Retrieved cube_data: {len(cube_data) if not cube_data.empty else 0} rows")
                    if not cube_data.empty:
                        cube_list.append(cube_data)
                 except Exception as e:
                     print(f"DEBUG: Error retrieving cube data for unit {g_unit}: {e}")
                     print(f"Error getting cube data for unit {g_unit}: {e}")
                     continue

            print(f"DEBUG: Total cube_list entries: {len(cube_list)}")
            if not cube_list: # No data found for any unit
                 print(f"DEBUG: No cube data found - hiding visualization")
                 # Hide both if no data retrieved
                 return hidden_container_style, hidden_area_style, [], [], "true"

            all_cube_data = pd.concat(cube_list).reset_index()


            # ── 1. keep only columns that contain at least one non-NaN ────────────
            non_empty_cols = all_cube_data.columns[~all_cube_data.isna().all()]
            all_cube_data = all_cube_data[non_empty_cols]

            # ── 2. keep only Cube_IDs for which a real data column survived ───────
            def cube_has_data(cid):
                pattern = cid[2:]                    # '6080' out of 'n6080'
                return any(pattern in col for col in non_empty_cols)

            cube_ids = [cid for cid in cube_ids if cube_has_data(cid)]
            cubes_df  = cubes_df[cubes_df['Cube_ID'].isin(cube_ids)]

            # Remove duplicates from cubes_df to prevent duplicate options
            cubes_df = cubes_df.drop_duplicates(subset=['Cube_ID'])

            options = [
                 {"label": row['Cube'], "value": row['Cube_ID']}
                 for idx, row in cubes_df.iterrows()
            ]
            # Return styles to SHOW BOTH container and inner area
            print(f"DEBUG: SUCCESS! Showing visualization with {len(options)} cube options")
            logger.info(f"Successfully showing visualization with {len(options)} cube options")
            return visible_container_style, visible_area_style, options, cube_selector_value, "false"

        except Exception as e:
            logger.error(f"Error handling visualization request: {e}", exc_info=True)
            # Hide both on error
            return hidden_container_style, hidden_area_style, [], [], "true"

    # --- update_visualization callback remains the same ---
    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        Input("place-state", "data"),
        prevent_initial_call=True
    )
    def update_visualization(selected_cubes, place_state):
        print(f"DEBUG: update_visualization called with selected_cubes: {selected_cubes}")
        print(f"DEBUG: place_state keys: {list(place_state.keys()) if place_state else 'None'}")
        
        # Check for cube data in the cubes field (from SSE) instead of cube_data field
        if not place_state.get("cubes") and not place_state.get("cube_data"):
             print(f"DEBUG: No cube data found in place_state, returning empty chart")
             # CRITICAL: Return empty chart instead of preventing update
             # This ensures that when data is cleared, the chart is also cleared
             return go.Figure().update_layout(
                 title="No data available",
                 xaxis_title="Year",
                 yaxis_title="Value",
                 annotations=[{
                     'text': "No areas selected or data has been removed",
                     'xref': "paper",
                     'yref': "paper", 
                     'x': 0.5,
                     'y': 0.5,
                     'xanchor': 'center',
                     'yanchor': 'middle',
                     'showarrow': False,
                     'font': {'size': 16, 'color': 'gray'}
                 }]
             )

        # If no cubes are selected, show an empty chart with a message
        if not selected_cubes:
            return go.Figure().update_layout(
                title="No data filters selected",
                xaxis_title="Year",
                yaxis_title="Value",
                annotations=[{
                    'text': "Select one or more data filters to view the data",
                    'xref': "paper",
                    'yref': "paper",
                    'x': 0.5,
                    'y': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'middle',
                    'showarrow': False,
                    'font': {'size': 16, 'color': 'gray'}
                }]
            )

        try:
            print(f"DEBUG: About to process cube data for chart")
            
            # Get the selected units from place_state
            selected_units = place_state.get("selected_place_g_units", [])
            print(f"DEBUG: Selected units from place_state: {selected_units}")
            
            if not selected_units:
                print(f"DEBUG: No selected units found in place_state")
                return go.Figure().update_layout(title="No areas selected")

            if not isinstance(selected_cubes, list):
                selected_cubes = [selected_cubes]
            
            print(f"DEBUG: Selected cubes for chart: {selected_cubes}")
            print(f"DEBUG: Selected units: {selected_units}")

            # Fetch actual statistical data using get_all_cube_data
            all_data_list = []
            for g_unit in selected_units:
                try:
                    print(f"DEBUG: Fetching chart data for unit {g_unit}, cubes {selected_cubes}")
                    cube_data_json = get_all_cube_data.invoke({"g_unit": str(g_unit), "cube_ids": selected_cubes})
                    cube_data_df = pd.read_json(io.StringIO(cube_data_json), orient='records')
                    print(f"DEBUG: Retrieved {len(cube_data_df)} rows for unit {g_unit}")
                    if not cube_data_df.empty:
                        all_data_list.append(cube_data_df)
                except Exception as e:
                    print(f"DEBUG: Error fetching data for unit {g_unit}: {e}")
                    continue

            if not all_data_list:
                print(f"DEBUG: No statistical data retrieved for any unit")
                return go.Figure().update_layout(title="No data available for selected cubes and areas")

            # Combine all data
            all_data_df = pd.concat(all_data_list, ignore_index=True)
            print(f"DEBUG: Combined data: {len(all_data_df)} rows")
            print(f"DEBUG: Data columns: {list(all_data_df.columns)}")

            # The data is already pivoted - each cube measurement is a separate column
            # We need to melt it back to get a normalized format for plotting
            
            # Identify value columns (everything except g_name and year)
            id_vars = ['g_name', 'year']
            value_vars = [col for col in all_data_df.columns if col not in id_vars]
            
            print(f"DEBUG: Value columns for chart: {value_vars}")
            
            if not value_vars:
                return go.Figure().update_layout(title="No data columns found")

            # Convert to long format for plotting
            chart_data = pd.melt(all_data_df, id_vars=id_vars, value_vars=value_vars, 
                               var_name='measurement', value_name='value')
            
            print(f"DEBUG: Melted data: {len(chart_data)} rows")

            # CRITICAL: Filter out rows with NaN values to prevent empty series in the plot
            chart_data = chart_data.dropna(subset=['value'])
            
            # Convert year to numeric for proper sorting
            chart_data['year'] = pd.to_numeric(chart_data['year'], errors='coerce')
            chart_data = chart_data.dropna(subset=['year'])

            if chart_data.empty:
                return go.Figure().update_layout(title="No valid data available after filtering")

            # Create display names for the legend
            chart_data['display_name'] = chart_data['g_name'] + ' - ' + chart_data['measurement']
            
            # Sort data for better visualization
            chart_data = chart_data.sort_values(['g_name', 'measurement', 'year'])
            
            print(f"DEBUG: Final chart data: {len(chart_data)} rows")
            print(f"DEBUG: Sample data: {chart_data.head()}")

            fig = px.line(chart_data, x='year', y='value', color='display_name',
                          title="Historical Data Visualization",
                          markers=True)  # Add markers to show single data points

            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value",
                hovermode='x unified',
                margin={"l": 50, "r": 50, "t": 50, "b": 50},
                height=None, # Let container control height
                autosize=True,
                legend_title_text='Series'
            )

            if (chart_data['value'].dropna() >= 0).all():
                 fig.update_yaxes(rangemode='tozero')

            return fig

        except Exception as e:
             print(f"Error updating visualization plot: {e}")
             return go.Figure().update_layout(title=f"Error generating plot: {e}")


    # --- clear_visualization callback ---
    @app.callback(
        Output("visualization-panel-container", "style", allow_duplicate=True), # Target container
        Output("visualization-area", "style", allow_duplicate=True),          # Target inner area
        # Output("resize-handle-2", "style", allow_duplicate=True), # Let JS handle handle visibility
        Output("cube-selector", "value"),
        Output("data-plot", "figure", allow_duplicate=True),
        Output("app-state", "data", allow_duplicate=True),
        Input("clear-plot-button", "n_clicks"),
        State("app-state", "data"),
        prevent_initial_call=True
    )
    def clear_visualization(n_clicks, current_app_state):
        if n_clicks:
            # Styles to hide both container and inner area
            hidden_container_style = {"flex": "0 0 0%", "display": "none"}
            hidden_area_style = {"height": "100%", "display": "none", "flexDirection": "column", "position": "relative"}
            # hidden_handle_style = {"display": "none"}

            if current_app_state:
                 current_app_state['show_visualization'] = False
            else:
                 current_app_state = {'show_visualization': False}

            return hidden_container_style, hidden_area_style, None, {}, current_app_state
        raise PreventUpdate
