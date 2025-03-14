# app/components/visualization.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def create_visualization_layout():
    return html.Div([
        html.Div(
            id="visualization-area",
            style={"height": "100%", "display": "none", "flexDirection": "column"},
            # Add data attribute to track visibility state
            **{'data-was-hidden': 'true'},
            children=[
                html.H3("Data Visualization", className="mb-3"),
                
                # Main content area with flex layout
                html.Div(
                    style={"flex": "1", "display": "flex", "flexDirection": "column", "minHeight": "0"},
                    children=[
                        # Controls (fixed height)
                        html.Div(
                            style={"marginBottom": "10px", "flexShrink": "0"},
                            children=[
                                dcc.Dropdown(
                                    id="cube-selector",
                                    placeholder="Select data series to visualize",
                                    multi=True
                                )
                            ]
                        ),
                        
                        # Graph (flexible height)
                        html.Div(
                            style={"flex": "1", "minHeight": "0", "position": "relative"},
                            children=[
                                dcc.Graph(
                                    id="data-plot",
                                    style={"height": "100%"}
                                )
                            ]
                        ),
                        
                        # Button (fixed height)
                        html.Div(
                            style={"marginTop": "10px", "flexShrink": "0"},
                            children=[
                                dbc.Button(
                                    "Clear Plot",
                                    id="clear-plot-button",
                                    color="secondary"
                                )
                            ]
                        )
                    ]
                )
            ]
        )
    ], style={"height": "100%"})