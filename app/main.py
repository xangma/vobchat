# app/main.py
from dash_extensions.enrich import DashProxy, html, Output, Input, CycleBreakerTransform, CycleBreakerInput
from configure_logging import configure_enhanced_logging
configure_enhanced_logging()
import os
import dash
import dash_bootstrap_components as dbc
from dash import html
import logging

from components.chat import create_chat_layout
from components.map import create_map_layout
from components.visualization import create_visualization_layout
from callbacks.chat import register_chat_callbacks

from callbacks.map_leaflet import register_map_leaflet_callbacks
from callbacks.visualization import register_visualization_callbacks
from callbacks.clientside_callbacks import register_clientside_callbacks
from config import load_config, get_db
from workflow import create_workflow, lg_State
from mapinit import get_polygons_by_type, get_date_ranges_by_type
from stores import create_stores

logger = logging.getLogger(__name__)

# Settings
config = load_config()
db = get_db(config)

# Initialize app
# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app = DashProxy(transforms=[CycleBreakerTransform()], external_stylesheets=[dbc.themes.BOOTSTRAP])
# Create workflow
initial_gdf = get_polygons_by_type('MOD_REG')
date_ranges_df = get_date_ranges_by_type()
compiled_workflow = create_workflow(lg_State, initial_gdf)

# Define the main layout
app.layout = dbc.Container([
        # Add stores at the top level
        create_stores(),
        
        html.H1("DDME Prototype"),
        html.P("This is a prototype for a dashboard that combines a chat interface with a map."),
        dbc.Row([
            dbc.Col(create_chat_layout(), md=6),
            dbc.Col(create_map_layout(initial_gdf), md=6),
        ]),
        dbc.Row([
            dbc.Col(create_visualization_layout(), md=12),
        ])
    ], id="document", fluid=True)

# Register all callbacks
register_chat_callbacks(app, compiled_workflow)
register_map_leaflet_callbacks(app, date_ranges_df)
register_visualization_callbacks(app, compiled_workflow)
register_clientside_callbacks(app)

if __name__ == '__main__':
    os.environ["HOST"] = "127.0.0.1"
    app.run(debug=True)
    # app.run(debug=True, use_reloader=False, dev_tools_hot_reload=False)
