# app/callbacks/visualization.py
import pandas as pd
import json
import plotly.graph_objects as go
from dash import no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from tools import get_all_cube_data

# This file contains all the callbacks for data visualization
def register_visualization_callbacks(app, compiled_workflow):
    # Add new callbacks for data visualization

    @app.callback(
        Output("visualization-area", "style"),
        Output("cube-selector", "options"),
        Output("place-state", "data"),
        Input("app-state", "data"),
        State("thread-id", "data"),
        State("place-state", "data"),
        prevent_initial_call=True
    )
    def handle_visualization_request(app_state, thread_id, place_state):
        if not app_state or app_state.get("show_visualization") is False:
            raise PreventUpdate

        # messages = app_state["messages"]
        # if not messages:
        #     raise PreventUpdate

        # # The last message is presumably the new AI message
        # latest_message = messages[-1]

        try:
            # Extract message content from the HTML div
            # message_content = latest_message['props']['children']
            
            # # Try to get additional_kwargs from the message
            # if message_content.startswith("AI: Here are all the available data cubes"):
            # Get the state to access additional data
            config = {"configurable": {"thread_id": thread_id}}
            state = compiled_workflow.get_state(config)
            
            # Get the last message's additional_kwargs
            cubes = place_state.get("cubes", [])
            if not cubes:
                raise PreventUpdate
            
            # Get all cube data at once
            cubes_df = pd.DataFrame(cubes)
            
            cube_ids = cubes_df['Cube_ID'].tolist()
            g_unit = state.values.get('selected_place_g_unit')
            all_data = get_all_cube_data({"g_unit": str(g_unit), "cube_ids": cube_ids})
            
            # Create dropdown options from all columns except year
            options = [
                {"label": row['Cube'], "value": row['Cube_ID']}
                for idx, row in cubes_df.iterrows()
            ]
            place_state['cube_data'] = all_data.to_json(orient='split')
            return {"display": "block"}, options, place_state
                        
        except Exception as e:
            print(f"Error handling visualization request: {e}")
            
        raise PreventUpdate

    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        State("place-state", "data"),
        prevent_initial_call=True
    )
    def update_visualization(selected_cubes, place_state):
        if not selected_cubes or not place_state.get("cube_data"):
            raise PreventUpdate
        
        cube_data_dict = json.loads(place_state["cube_data"])
        cubes_df = pd.DataFrame(cube_data_dict['data'], columns=cube_data_dict['columns'])
        
        # Allow multiple column selection for comparison
        if not isinstance(selected_cubes, list):
            selected_cubes = [selected_cubes]
        
        fig = go.Figure()
        
        for cube in selected_cubes:
            tempdf = cubes_df.filter(regex=f"year|{cube[2:]}")
            for col in tempdf.columns:
                if col != 'year':
                    fig.add_trace(go.Scatter(
                        x=tempdf['year'].values,
                        y=tempdf[col].values,
                        name=col,
                        mode='lines+markers'
                    ))
        
        fig.update_layout(
            title="Historical Data Visualization",
            xaxis_title="Year",
            yaxis_title="Value",
            hovermode='x unified'
        )
        
        return fig

    @app.callback(
        Output("visualization-area", "style", allow_duplicate=True),
        Output("cube-selector", "value"),
        Output("data-plot", "figure", allow_duplicate=True),
        Input("clear-plot-button", "n_clicks"),
        prevent_initial_call=True
    )
    def clear_visualization(n_clicks):
        if n_clicks:
            return {"display": "none"}, None, {}
        raise PreventUpdate