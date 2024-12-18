# app/components/visualization.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def create_visualization_layout():
    return html.Div([
        html.Div(id="visualization-area", style={"display": "none"}, children=[
            html.H3("Data Visualization"),
            dcc.Dropdown(
                id="cube-selector",
                placeholder="Select data series to visualize",
                multi=True
            ),
            dcc.Graph(id="data-plot"),
            dbc.Button("Clear Plot", id="clear-plot-button", color="secondary", className="mt-2")
        ]),
        dcc.Store(id="cube-data"),
        dcc.Store(id="selected-cube"),
    ])