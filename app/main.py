import logging
from dash_extensions.enrich import DashProxy, CycleBreakerTransform
import dash_bootstrap_components as dbc
from .config import load_config, get_db
from .workflow import create_workflow, lg_State
from .mapinit import get_polygons_by_type, get_date_ranges_by_type
from .stores import create_stores
from .components.chat import create_chat_layout
from .components.map import create_map_layout
from .components.visualization import create_visualization_layout
from .callbacks.chat import register_chat_callbacks
from .callbacks.map_leaflet import register_map_leaflet_callbacks
from .callbacks.visualization import register_visualization_callbacks
from .callbacks.clientside_callbacks import register_clientside_callbacks

logger = logging.getLogger(__name__)

def create_app():
    """Initialize and configure the Dash app."""
    config = load_config()
    db = get_db(config)

    app = DashProxy(transforms=[CycleBreakerTransform()], external_stylesheets=[dbc.themes.BOOTSTRAP])

    initial_gdf = get_polygons_by_type('MOD_REG')
    date_ranges_df = get_date_ranges_by_type()
    compiled_workflow = create_workflow(lg_State)

    app.layout = dbc.Container([
        create_stores(),
        dbc.Row([
            dbc.Col([
                create_chat_layout()
            ], md=6),
            dbc.Col([
                create_map_layout(initial_gdf)
            ], md=6),
        ]),
        dbc.Row([
            dbc.Col(create_visualization_layout(), md=12),
        ])
    ], fluid=True)

    register_chat_callbacks(app, compiled_workflow)
    register_map_leaflet_callbacks(app, date_ranges_df)
    register_visualization_callbacks(app, compiled_workflow)
    register_clientside_callbacks(app)

    return app

# Create app and expose `server` for Gunicorn
app = create_app()
server = app.server

if __name__ == "__main__":
    app.run_server(debug=True)