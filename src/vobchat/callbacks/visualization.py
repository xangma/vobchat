# app/callbacks/visualization.py
import pandas as pd
import json
import io
import asyncio
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from vobchat.tools import get_all_cube_data

# This file contains all the callbacks for data visualization
def register_visualization_callbacks(app, compiled_workflow):

    @app.callback(
        Output("visualization-panel-container", "style"), # Controls the outer container
        Output("visualization-area", "style"),          # Controls the inner content div
        # Output("resize-handle-2", "style"), # Let JS handle handle visibility via observer
        Output("cube-selector", "options"),
        Output("cube-selector", "value", allow_duplicate=True),
        Output("place-state", "data"),
        Output("visualization-area", "data-was-hidden"), # Keep this if JS uses it
        Input("app-state", "data"),
        State("thread-id", "data"),
        State("place-state", "data"),
        State("visualization-area", "data-was-hidden"),
        prevent_initial_call=True
    )
    def handle_visualization_request(app_state, thread_id, place_state, current_visibility_data_attr):
        # Define styles for visible/hidden states
        # Style for the outer container
        visible_container_style = {"flex": "0 0 40%", "display": "flex"}
        hidden_container_style = {"flex": "0 0 0%", "display": "none"}
        # Style for the inner visualization-area div
        visible_area_style = {"height": "100%", "display": "flex", "flexDirection": "column", "position": "relative"}
        hidden_area_style = {"height": "100%", "display": "none", "flexDirection": "column", "position": "relative"}

        if not app_state or app_state.get("show_visualization") is False:
            # Return styles to hide BOTH container and inner area
            return hidden_container_style, hidden_area_style, [], [], place_state, "true"

        try:
            config = {"configurable": {"thread_id": thread_id}}
            state = asyncio.run(compiled_workflow.aget_state(config))
            cubes = place_state.get("cubes", [])
            if not cubes:
                 # Hide both if no cubes
                 return hidden_container_style, hidden_area_style, [], [], place_state, "true"

            cubes_df = pd.read_json(cubes, orient='records')
            cube_ids = cubes_df['Cube_ID'].tolist()
            default_value = cube_ids[:1]
            g_units = state.values.get('selected_place_g_units', [])
            if not g_units:
                 # Hide both if no units selected
                 return hidden_container_style, hidden_area_style, [], [], place_state, "true"

            cube_list = []
            for g_unit in g_units:
                 try:
                    # Ensure get_all_cube_data is defined and imported in tools.py
                    cube_data = pd.read_json(io.StringIO(get_all_cube_data({"g_unit": str(g_unit), "cube_ids": cube_ids})), orient='records')
                    if not cube_data.empty:
                        cube_list.append(cube_data)
                 except Exception as e:
                     print(f"Error getting cube data for unit {g_unit}: {e}")
                     continue

            if not cube_list: # No data found for any unit
                 # Hide both if no data retrieved
                 return hidden_container_style, hidden_area_style, [], [], place_state, "true"

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

            options = [
                 {"label": row['Cube'], "value": row['Cube_ID']}
                 for idx, row in cubes_df.iterrows()
            ]
            place_state['cube_data'] = all_cube_data.to_json(orient='records')

            # Return styles to SHOW BOTH container and inner area
            return visible_container_style, visible_area_style, options, default_value, place_state, "false"

        except Exception as e:
            print(f"Error handling visualization request: {e}")
            # Hide both on error
            return hidden_container_style, hidden_area_style, [], [], place_state, "true"

    # --- update_visualization callback remains the same ---
    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        Input("place-state", "data"),
        prevent_initial_call=True
    )
    def update_visualization(selected_cubes, place_state):
        if not selected_cubes or not place_state.get("cube_data"):
             raise PreventUpdate

        try:
            # Use read_json for potentially better type inference
            cubes_df = pd.read_json(io.StringIO(place_state["cube_data"]), orient='records')

            if not isinstance(selected_cubes, list):
                selected_cubes = [selected_cubes]

            # Map selected Cube IDs to actual column names in the pivoted DataFrame
            # The columns might be like 'n6080', 'n6081', etc. corresponding to Cube IDs
            relevant_cols = ['year', 'g_name']
            selected_cube_patterns = [cid[2:] for cid in selected_cubes] # Get pattern like '6080'
            value_vars = []

            for col in cubes_df.columns:
                 if col in relevant_cols:
                     continue
                 # Check if the column name contains any of the selected patterns
                 if any(pattern in col for pattern in selected_cube_patterns):
                      value_vars.append(col)
                      relevant_cols.append(col)


            if not value_vars: # No data columns match the selection
                  return go.Figure().update_layout(title="No data columns found for selected cubes")

            filtered_df = cubes_df[relevant_cols]

            unpivoted_df = pd.melt(filtered_df, id_vars=['year', 'g_name'], value_vars=value_vars)

            # Attempt to map back to cube names for legend (if available)
            cube_name_map = {row['Cube_ID']: row['Cube'] for idx, row in pd.read_json(io.StringIO(place_state.get("cubes", [])), orient='records').iterrows()}
            def get_cube_name_from_variable(variable_str):
                # Find the Cube ID whose pattern (e.g., '6080') is in the variable column name
                matched_id = next((cid for cid in cube_name_map if cid[2:] in variable_str), None)
                return cube_name_map.get(matched_id, variable_str) # Fallback to raw column

            unpivoted_df = unpivoted_df.sort_values(['g_name','year'])
            unpivoted_df['cube_name'] = unpivoted_df['variable'].apply(get_cube_name_from_variable)
            unpivoted_df['merged_name'] = unpivoted_df['g_name'] + ' - ' + unpivoted_df['variable']

            fig = px.line(unpivoted_df, x='year', y='value', color='merged_name',
                          title="Historical Data Visualization")

            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value",
                hovermode='x unified',
                margin={"l": 50, "r": 50, "t": 50, "b": 50},
                height=None, # Let container control height
                autosize=True,
                legend_title_text='Series'
            )

            if (unpivoted_df['value'].dropna() >= 0).all():
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