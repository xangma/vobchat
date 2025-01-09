# app/main.py
import os
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
import logging

from components.chat import create_chat_layout
from components.map import create_map_layout
from components.visualization import create_visualization_layout
from callbacks.chat import register_chat_callbacks
from callbacks.map import register_map_callbacks
from callbacks.visualization import register_visualization_callbacks
from config import load_config, get_db
from workflow import create_workflow, lg_State
from mapinit import get_polygons_by_type, get_date_ranges_by_type
from configure_logging import configure_logging, in_memory_log_handler

configure_logging()

logger = logging.getLogger(__name__)

# Settings
config = load_config()
db = get_db(config)

# Initialize app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Create workflow
initial_gdf = get_polygons_by_type('MOD_REG')
date_ranges_df = get_date_ranges_by_type()
compiled_workflow = create_workflow(lg_State, initial_gdf)

def create_debug_layout():
    return dbc.Card(
        [
            dbc.CardHeader("Debug Log"),
            dbc.CardBody(
                [
                    dcc.Interval(id="log-interval", interval=3000, n_intervals=0),
                    html.Pre(
                        id="debug-log", 
                        style={"whiteSpace": "pre-wrap", "maxHeight": "300px", "overflowY": "scroll"}
                    )
                ]
            ),
        ],
        className="mt-4"
    )

# Define the main layout
app.layout = dbc.Container([
    html.H1("DDME Prototype"),
    html.P("This is a prototype for a dashboard that combines a chat interface with a map."),
    dbc.Row([
        dbc.Col(create_chat_layout(), md=6),
        dbc.Col(create_map_layout(initial_gdf), md=6),
    ]),
    dbc.Row([
        dbc.Col(create_visualization_layout(), md=12),
    ]),
    # Insert the debug area at the bottom (or wherever convenient)
    dbc.Row([
        dbc.Col(create_debug_layout(), md=12),
    ])
], fluid=True)

# Register all callbacks
register_chat_callbacks(app, compiled_workflow)
register_map_callbacks(app, date_ranges_df)
register_visualization_callbacks(app, compiled_workflow)

# Callback to update the debug log text
@app.callback(
    dash.Output("debug-log", "children"),
    dash.Input("log-interval", "n_intervals")
)
def update_debug_log(n):
    # Return the contents of our in-memory log buffer
    return in_memory_log_handler.get_logs()

if __name__ == '__main__':
    os.environ["HOST"] = "127.0.0.1"
    app.run(debug=True)