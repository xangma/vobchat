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
# from vobchat.callbacks.chat import register_chat_callbacks  # Replaced with SSE
# from .callbacks.map_leaflet import register_map_leaflet_callbacks
from vobchat.callbacks.visualization import register_visualization_callbacks
from vobchat.callbacks.clientside_callbacks import register_clientside_callbacks
from vobchat.callbacks.chat_sse import register_sse_chat_callbacks
from vobchat.api.polygon_routes import register_polygon_routes
from vobchat.api.bounding_box_routes import register_bounding_box_routes
from vobchat.models import register_app_routes
from vobchat.sse_manager import sse_manager, create_sse_response
from flask import render_template_string, redirect, url_for, request, session
import uuid
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

def register_sse_routes(server, compiled_workflow, base_workflow):
    """Register Server-Sent Events endpoints"""

    @server.route('/api/sse/connect')
    def sse_connect():
        """Initialize SSE connection"""
        client_id = str(uuid.uuid4())
        thread_id = request.args.get('thread_id', str(uuid.uuid4()))
        workflow_input_param = request.args.get('workflow_input')

        print(f"DEBUG: SSE connection request - client_id: {client_id}, thread_id: {thread_id}")
        print(f"DEBUG: SSE workflow_input_param: {workflow_input_param}")
        print(f"DEBUG: SSE request.args: {dict(request.args)}")
        logger.info(f"SSE connection request - client_id: {client_id}, thread_id: {thread_id}")

        # Clear any existing clients for this thread first
        sse_manager.clear_thread_clients(thread_id)

        # Add client to SSE manager
        sse_manager.add_client(client_id, thread_id)

        # CRITICAL: If this is a new workflow (has workflow_input), start it automatically
        if workflow_input_param:
            try:
                import json
                workflow_input = json.loads(workflow_input_param)
                print(f"DEBUG: Starting workflow automatically for new SSE connection with input: {workflow_input}")

                # Start workflow in background thread
                def start_workflow():
                    try:
                        from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
                        import asyncio

                        workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)
                        config = {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": "",
                                "checkpoint_id": None
                            }
                        }

                        # Run workflow in new event loop
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                        async def run_workflow():
                            async for result in workflow_adapter.stream_workflow_execution(
                                workflow_input,
                                config,
                                thread_id
                            ):
                                print(f"DEBUG: Auto-started workflow result: {result}")

                        loop.run_until_complete(run_workflow())
                        loop.close()

                    except Exception as e:
                        print(f"DEBUG: Error auto-starting workflow: {e}")

                import threading
                workflow_thread = threading.Thread(target=start_workflow, daemon=True)
                workflow_thread.start()
                print(f"DEBUG: Auto-started workflow thread for new connection")

            except Exception as e:
                print(f"DEBUG: Error parsing workflow_input parameter: {e}")

        print(f"DEBUG: SSE client added to manager, creating response stream")
        return create_sse_response(client_id)

    @server.route('/api/sse/status')
    def sse_status():
        """Check SSE service status"""
        return {
            'status': 'active',
            'connected_clients': len(sse_manager.clients),
            'threads': list(set(sse_manager.client_threads.values()))
        }

    @server.route('/api/workflow/input', methods=['POST'])
    def workflow_input():
        """Handle user input for workflow via SSE"""
        print(f"DEBUG: /api/workflow/input called")
        try:
            data = request.get_json()
            print(f"DEBUG: Request data: {data}")
            if not data:
                print(f"DEBUG: No JSON data provided")
                return {'error': 'No JSON data provided'}, 400

            thread_id = data.get('thread_id')
            input_data = data.get('input_data', {})
            print(f"DEBUG: Parsed thread_id: {thread_id}, input_data: {input_data}")
            print(f"VOBCHAT DEBUG API RAW INPUT: {data}")

            if not thread_id:
                print(f"DEBUG: No thread_id provided")
                return {'error': 'thread_id is required'}, 400

            print(f"DEBUG: SSE workflow input for thread {thread_id}: {input_data}")
            logging.info(f"SSE workflow input for thread {thread_id}: {input_data}")

            # Import here to avoid circular imports
            from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
            import asyncio

            # Create workflow adapter and handle input asynchronously
            workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)

            # Create config for this thread
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": None
                }
            }

            # Run the async workflow processing in a background thread with its own event loop
            def process_workflow_in_thread():
                async def async_process():
                    result = await workflow_adapter.handle_user_input(
                        thread_id=thread_id,
                        input_data=input_data,
                        config=config
                    )
                    logging.info(f"Workflow processing completed for thread {thread_id}: {result.get('status')}")
                    return result

                # Create and run the event loop in this thread
                asyncio.run(async_process())

            # Start background processing in a separate thread
            import threading
            workflow_thread = threading.Thread(target=process_workflow_in_thread, daemon=True)
            workflow_thread.start()

            return {
                'status': 'success',
                'thread_id': thread_id,
                'message': 'Input received and workflow processing started via SSE'
            }

        except Exception as e:
            logging.error(f"Error processing workflow input: {e}", exc_info=True)
            return {'error': str(e)}, 500

    @server.route('/api/workflow/start', methods=['POST'])
    def workflow_start():
        """Start new workflow execution via SSE"""
        try:
            data = request.get_json()
            if not data:
                return {'error': 'No JSON data provided'}, 400

            thread_id = data.get('thread_id')
            workflow_input = data.get('workflow_input', {})

            if not thread_id:
                return {'error': 'thread_id is required'}, 400

            logging.info(f"SSE workflow start for thread {thread_id}: {list(workflow_input.keys())}")

            # Import here to avoid circular imports
            from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
            import asyncio

            # Create workflow adapter
            workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)

            # Create config for this thread
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": None
                }
            }

            # Run the async workflow processing in a background thread with its own event loop
            def process_workflow_in_thread():
                async def async_process():
                    # Stream workflow execution
                    results = []
                    async for result in workflow_adapter.stream_workflow_execution(
                        workflow_input=workflow_input,
                        config=config,
                        thread_id=thread_id
                    ):
                        results.append(result)

                    logging.info(f"Workflow execution completed for thread {thread_id}, {len(results)} events")
                    return results

                # Create and run the event loop in this thread
                asyncio.run(async_process())

            # Start background processing in a separate thread
            import threading
            workflow_thread = threading.Thread(target=process_workflow_in_thread, daemon=True)
            workflow_thread.start()

            return {
                'status': 'success',
                'thread_id': thread_id,
                'message': 'Workflow execution started via SSE'
            }

        except Exception as e:
            logging.error(f"Error starting workflow execution: {e}", exc_info=True)
            return {'error': str(e)}, 500

    @server.route('/api/save-frontend-logs', methods=['POST'])
    def save_frontend_logs():
        """Save frontend logs to file"""
        try:
            data = request.get_json()
            if not data or 'logs' not in data:
                return {'error': 'No logs provided'}, 400
            
            frontend_log_path = "/Users/xangma/Library/CloudStorage/OneDrive-Personal/repos/vobchat/frontend.log"
            
            # Write logs to file (overwrite mode)
            with open(frontend_log_path, 'w') as f:
                f.write(data['logs'])
            
            return {'status': 'success', 'message': 'Frontend logs saved'}
            
        except Exception as e:
            logging.error(f"Error saving frontend logs: {e}", exc_info=True)
            return {'error': str(e)}, 500

def create_app():
    """Initialize and configure the Dash app."""
    
    # Configure enhanced logging first
    from vobchat.configure_logging import configure_enhanced_logging
    configure_enhanced_logging()

    assets_folder = os.path.join(os.path.dirname(__file__), 'assets')

    app = DashProxy(transforms=[CycleBreakerTransform()],
                    external_stylesheets=[dbc.themes.BOOTSTRAP],
                    url_base_pathname=DASH_PREFIX + '/',
                    suppress_callback_exceptions=True,
                    background_callback_manager=background_callback_manager,)

    # initial_gdf = polygon_cache.get_polygons('MOD_REG')
    date_ranges_df = get_date_ranges_by_type()
    compiled_workflow, base_workflow = create_workflow(lg_State)

    # Create a resizable layout
    app.layout = html.Div([
        create_stores(),
        # Include SSE client script
        html.Script(src=f"{DASH_PREFIX}/assets/sse_client.js"),
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

    # Try SSE-based chat callbacks with proper error handling
    try:
        print("DEBUG: About to register SSE chat callbacks")
        register_sse_chat_callbacks(app, compiled_workflow, base_workflow)
        print("DEBUG: SSE chat callbacks registered successfully")
    except Exception as e:
        print(f"ERROR: Failed to register SSE chat callbacks: {e}")
        import traceback
        traceback.print_exc()

        # Fallback to old chat system if SSE fails
        print("FALLBACK: Using old chat callbacks due to SSE registration failure")
        from vobchat.callbacks.chat import register_chat_callbacks
        register_chat_callbacks(app, compiled_workflow, background_callback_manager)
    # register_map_leaflet_callbacks(app, date_ranges_df)
    register_clientside_callbacks(app)
    register_visualization_callbacks(app, compiled_workflow)

    register_polygon_routes(app.server)
    register_bounding_box_routes(app.server)

    # Register SSE endpoints
    register_sse_routes(app.server, compiled_workflow, base_workflow)

    register_commands(app.server)

    # Register Celery tasks if Redis is available
    if 'REDIS_URL' in os.environ:
        print("DEBUG: Registering Celery tasks")
        app.register_celery_tasks()

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



# Celery tasks are now registered inside create_app()

if __name__ == "__main__":
    app.run_server(debug=True)
