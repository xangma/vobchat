import logging, os
from dash_extensions.enrich import DashProxy, CycleBreakerTransform, ServersideOutputTransform
import dash_bootstrap_components as dbc
from dash import html
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
from .api.polygon_routes import register_polygon_routes

logger = logging.getLogger(__name__)

def create_app():
    """Initialize and configure the Dash app."""
    config = load_config()
    db = get_db(config)

    assets_folder = os.path.join(os.path.dirname(__file__), 'assets')
    
    app = DashProxy(transforms=[CycleBreakerTransform()], external_stylesheets=[
                    dbc.themes.BOOTSTRAP], url_base_pathname=os.getenv("DASH_URL_BASE", None), suppress_callback_exceptions=True)

    initial_gdf = get_polygons_by_type('MOD_REG')
    date_ranges_df = get_date_ranges_by_type()
    compiled_workflow = create_workflow(lg_State)

    # Create a resizable layout
    app.layout = html.Div([
        create_stores(),
        html.Div(className="resizable-container", children=[
            html.Div(className="resizable-vertical", children=[
                # Top row with chat and map side by side (horizontal split)
                html.Div(className="resizable-horizontal", style={"flex": "1"}, children=[
                    # Chat panel
                    html.Div(className="resizable-panel", id="chat-panel", children=[
                        create_chat_layout()
                    ]),
                    # Horizontal resize handle
                    html.Div(className="resize-handle-horizontal"),
                    # Map panel
                    html.Div(className="resizable-panel", id="map-panel", children=[
                        create_map_layout(initial_gdf, assets_folder)
                    ]),
                ]),
                # Vertical resize handle (hidden by default, shown when visualization appears)
                html.Div(className="resize-handle-vertical", id="vertical-resize-handle", style={"display": "none"}),
                # Bottom row with visualization
                html.Div(className="resizable-panel", id="visualization-panel", children=[
                    create_visualization_layout()
                ], style={"minHeight": "200px"}),
            ]),
        ]),
    ],
    id="document")

    register_chat_callbacks(app, compiled_workflow)
    register_map_leaflet_callbacks(app, date_ranges_df)
    register_visualization_callbacks(app, compiled_workflow)
    register_clientside_callbacks(app)

    register_polygon_routes(app.server)
    
    return app

# Create app and expose `server` for Gunicorn
app = create_app()
server = app.server

if __name__ == "__main__":
    app.run_server(debug=True)