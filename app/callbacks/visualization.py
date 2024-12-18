# app/callbacks/visualization.py
import pandas as pd
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
        Output("cube-data", "data"),
        Input("chat-display", "children"),
        State("thread_id", "data"),
        prevent_initial_call=True
    )
    def handle_visualization_request(chat_history, thread_id):
        if not chat_history:
            raise PreventUpdate
        
        # Get the latest AI message
        latest_message = chat_history[-1] if chat_history else None
        if not latest_message:
            raise PreventUpdate

        try:
            # Extract message content from the HTML div
            message_content = latest_message['props']['children']
            
            # Try to get additional_kwargs from the message
            if message_content.startswith("AI: Here are all the available data cubes"):
                # Get the state to access additional data
                config = {"configurable": {"thread_id": thread_id}}
                state = compiled_workflow.get_state(config)
                
                # Get the last message's additional_kwargs
                last_message = state["messages"][-1]
                if hasattr(last_message, "additional_kwargs"):
                    viz_data = last_message.additional_kwargs
                    if viz_data.get("show_visualization"):
                        cubes = viz_data.get("cubes", [])
                        if not cubes:
                            raise PreventUpdate
                        
                        # Get all cube data at once
                        cube_ids = [cube['Cube_ID'] for cube in cubes]
                        g_unit = state.get('selected_place_g_unit')
                        all_data = get_all_cube_data({"g_unit": str(g_unit), "cube_ids": cube_ids})
                        
                        # Create dropdown options from all columns except year
                        options = [
                            {"label": col, "value": col} 
                            for col in all_data.columns 
                            if col != 'year'
                        ]
                        
                        return {"display": "block"}, options, all_data.to_json(date_format='iso', orient='split')
                        
        except Exception as e:
            print(f"Error handling visualization request: {e}")
            
        raise PreventUpdate

    @app.callback(
        Output("data-plot", "figure", allow_duplicate=True),
        Input("cube-selector", "value"),
        State("cube-data", "data"),
        prevent_initial_call=True
    )
    def update_visualization(selected_columns, cube_data_json):
        if not selected_columns or not cube_data_json:
            raise PreventUpdate
            
        df = pd.read_json(cube_data_json, orient='split')
        
        # Allow multiple column selection for comparison
        if not isinstance(selected_columns, list):
            selected_columns = [selected_columns]
        
        fig = go.Figure()
        
        for col in selected_columns:
            fig.add_trace(go.Scatter(
                x=df['year'],
                y=df[col],
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