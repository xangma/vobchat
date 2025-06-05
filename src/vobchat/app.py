# src/vobchat/app.py
import logging, os
from dash_extensions.enrich import DashProxy, CycleBreakerTransform, ServersideOutputTransform
import dash_bootstrap_components as dbc
from dash import html
from vobchat.workflow import create_workflow, lg_State
from vobchat.tools import get_date_ranges_by_type
from vobchat.stores import create_stores
from vobchat.utils.polygon_cache import polygon_cache
from vobchat.components.chat import create_chat_layout
from vobchat.components.map import create_map_layout
from vobchat.components.visualization import create_visualization_layout
from vobchat.callbacks.chat import register_chat_callbacks
# from .callbacks.map_leaflet import register_map_leaflet_callbacks
from vobchat.callbacks.visualization import register_visualization_callbacks
from vobchat.callbacks.clientside_callbacks import register_clientside_callbacks
from vobchat.api.polygon_routes import register_polygon_routes
from vobchat.api.bounding_box_routes import register_bounding_box_routes
from vobchat.models import register_app_routes
from flask import render_template_string, redirect, url_for, request, session
from authlib.integrations.flask_client import OAuth
from flask_login import current_user
import os, json, functools, pathlib
from vobchat.models import db, lm, bp as auth_bp
from vobchat.cli import register_commands

logger = logging.getLogger(__name__)

from dash import DiskcacheManager, CeleryManager
from geoalchemy2 import Geometry

if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    logging.info("Using Redis & Celery for background callbacks.")
    from celery import Celery
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)
else:
    # Diskcache for non-production apps when developing locally
    import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)

DASH_PREFIX = os.getenv("DASH_URL_BASE", "/app").rstrip("/")

def create_app():
    """Initialize and configure the Dash app."""


    assets_folder = os.path.join(os.path.dirname(__file__), 'assets')

    app = DashProxy(transforms=[CycleBreakerTransform()],
                    external_stylesheets=[dbc.themes.BOOTSTRAP],
                    url_base_pathname=DASH_PREFIX + '/',
                    suppress_callback_exceptions=True,
                    background_callback_manager=background_callback_manager,)

    # initial_gdf = polygon_cache.get_polygons('MOD_REG')
    date_ranges_df = get_date_ranges_by_type()
    compiled_workflow = create_workflow(lg_State)

    # Create a resizable layout
    app.layout = html.Div([
        create_stores(),
        html.Div(className="resizable-container", children=[
            html.Div(className="resizable-horizontal", style={"display": "flex", "width": "100%", "height": "100%"}, children=[
                # 1. Chat panel on the left
                html.Div(className="resizable-panel", id="chat-panel", children=[
                    create_chat_layout()
                ], style={"flex": "0 0 30%"}), # Initial width 30%

                # First Horizontal resize handle
                html.Div(className="resize-handle-horizontal", id="resize-handle-1"),

                # 2. Visualization panel in the middle
                html.Div(className="resizable-panel", id="visualization-panel-container", children=[
                     # Wrap the viz component to control its container's visibility/style
                    create_visualization_layout()
                ], style={
                       "flex": "0 0 0%",  # Start collapsed
                       "display": "none",
                       }), # Initial width 0%, initially hidden
                # Second Horizontal resize handle
                html.Div(className="resize-handle-horizontal", id="resize-handle-2", style={"display": "flex"}), # Initially shown (or controlled by callback)

                # 3. Map panel on the right
                html.Div(className="resizable-panel", id="map-panel", children=[
                    create_map_layout(assets_folder)
                ], style={"flex": "1 1 30%"}), # Initial width 30% (flex-grow: 1 allows it to take remaining space initially)
            ]),
        ]),
    ],
    id="document")

    register_app_routes(app.server)

    register_chat_callbacks(app, compiled_workflow, background_callback_manager)
    # register_map_leaflet_callbacks(app, date_ranges_df)
    register_clientside_callbacks(app)
    register_visualization_callbacks(app, compiled_workflow)

    register_polygon_routes(app.server)
    register_bounding_box_routes(app.server)

    register_commands(app.server)

    return app

# Create app and expose `server` for Gunicorn
app = create_app()

server = app.server

server.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-only-change-me')

server.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL", "sqlite:///users.db")
server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
server.config.update(
    SESSION_COOKIE_SECURE   = True,
    SESSION_COOKIE_HTTPONLY = True,
    SESSION_COOKIE_SAMESITE = "Lax",
    WTF_CSRF_ENABLED        = True,          # if you use Flask-WTF for forms
)

db.init_app(server)
lm.init_app(server)

@server.before_request
def protect_dash():
    """
    Redirect unauthenticated users away from every URL that begins
    with /app … except the static/_dash asset endpoints that Dash
    needs while the login page is showing.
    """
    path = request.path

    if not path.startswith(DASH_PREFIX):
        return                       # not a Dash URL → ignore

    # Allow the pieces Dash needs to render its blank page assets
    SAFE_SUBPATHS = ("/_dash", "/assets", "/_favicon", "/_reload")
    if any(path.startswith(f"{DASH_PREFIX}{p}") for p in SAFE_SUBPATHS):
        return

    # Block everything else unless the user is logged in
    if not current_user.is_authenticated:
        # preserve destination so Flask-Login can send them back
        return redirect(url_for("auth.login_page", next=path))

server.register_blueprint(auth_bp)

# this is needed in order for database session calls (e.g. db.session.commit)
with server.app_context():
    try:
        db.create_all()
    except Exception as exception:
        print("got the following exception when attempting db.create_all() in __init__.py: " + str(exception))
    finally:
        print("db.create_all() in __init__.py was successfull - no exceptions were raised")



if 'REDIS_URL' in os.environ:
    app.register_celery_tasks()

if __name__ == "__main__":
    app.run_server(debug=True)
