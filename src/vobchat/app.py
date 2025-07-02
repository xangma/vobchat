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
from vobchat.callbacks.visualization import register_visualization_callbacks
from vobchat.callbacks.clientside_callbacks import register_clientside_callbacks
from vobchat.callbacks.chat_sse import register_sse_chat_callbacks
from vobchat.api.polygon_routes import register_polygon_routes
from vobchat.api.bounding_box_routes import register_bounding_box_routes
from vobchat.api.map_state_routes import register_map_state_routes
from vobchat.models import register_app_routes
from vobchat.sse_manager import sse_manager, create_sse_response
from vobchat.utils.async_manager import async_manager
from flask import render_template_string, redirect, url_for, request, session
import uuid
from authlib.integrations.flask_client import OAuth
from flask_login import current_user
import os, json, functools, pathlib
from vobchat.models import db, lm, bp as auth_bp
from vobchat.cli import register_commands

logger = logging.getLogger(__name__)

from dash import DiskcacheManager, CeleryManager

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

        logger.debug(f"SSE connection request - client_id: {client_id}, thread_id: {thread_id}")
        logger.debug(f"SSE workflow_input_param: {workflow_input_param}")
        logger.debug(f"SSE request.args: {dict(request.args)}")
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
                logger.debug(f"Starting workflow automatically for new SSE connection with input: {workflow_input}")

                # Start workflow using async manager
                def start_workflow():
                    try:
                        from vobchat.workflow_sse_adapter import create_workflow_sse_adapter

                        workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)
                        config = {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": "",
                                "checkpoint_id": None
                            }
                        }

                        # Use async manager instead of creating new event loop
                        async def run_workflow():
                            async for result in workflow_adapter.stream_workflow_execution(
                                workflow_input,
                                config,
                                thread_id
                            ):
                                logger.debug(f"Auto-started workflow result: {result}")

                        # Run using the managed event loop
                        async_manager.run_async(run_workflow(), timeout=300)  # 5 minute timeout

                    except Exception as e:
                        logger.error(f"Error auto-starting workflow: {e}", exc_info=True)

                import threading
                workflow_thread = threading.Thread(target=start_workflow, daemon=True)
                workflow_thread.start()
                logger.debug(f"Auto-started workflow thread for new connection")

            except Exception as e:
                logger.error(f"Error parsing workflow_input parameter: {e}")

        logger.debug(f"SSE client added to manager, creating response stream")
        return create_sse_response(client_id)

    @server.route('/api/sse/status')
    def sse_status():
        """Check SSE service status"""
        if hasattr(sse_manager, 'get_connection_status'):
            # Use enhanced status if available
            return sse_manager.get_connection_status()
        else:
            # Fallback to basic status
            return {
                'status': 'active',
                'connected_clients': len(sse_manager.clients),
                'threads': list(set(sse_manager.client_threads.values()))
            }

    @server.route('/api/workflow/input', methods=['POST'])
    def workflow_input():
        """Handle user input for workflow via SSE"""
        logger.debug(f"/api/workflow/input called")
        try:
            data = request.get_json()
            logger.debug(f"Request data: {data}")
            if not data:
                logger.debug(f"No JSON data provided")
                return {'error': 'No JSON data provided'}, 400

            thread_id = data.get('thread_id')
            input_data = data.get('input_data', {})
            logger.debug(f"Parsed thread_id: {thread_id}, input_data: {input_data}")
            logger.debug(f"VOBCHAT API RAW INPUT: {data}")

            if not thread_id:
                logger.debug(f"No thread_id provided")
                return {'error': 'thread_id is required'}, 400

            logger.debug(f"SSE workflow input for thread {thread_id}: {input_data}")
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

            # Run the async workflow processing using async manager
            def process_workflow_in_thread():
                try:
                    async def async_process():
                        result = await workflow_adapter.handle_user_input(
                            thread_id=thread_id,
                            input_data=input_data,
                            config=config
                        )
                        logging.info(f"Workflow processing completed for thread {thread_id}: {result.get('status')}")
                        return result

                    # Use async manager instead of creating new event loop
                    async_manager.run_async(async_process(), timeout=300)  # 5 minute timeout
                    
                except Exception as e:
                    logging.error(f"Error in workflow processing thread: {e}", exc_info=True)

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

            # Run the async workflow processing using async manager
            def process_workflow_in_thread():
                try:
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

                    # Use async manager instead of creating new event loop
                    async_manager.run_async(async_process(), timeout=300)  # 5 minute timeout
                    
                except Exception as e:
                    logging.error(f"Error in workflow execution thread: {e}", exc_info=True)

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

    assets_folder = os.path.join(os.path.dirname(__file__), 'assets')

    app = DashProxy(transforms=[CycleBreakerTransform()],
                    external_stylesheets=[dbc.themes.BOOTSTRAP],
                    url_base_pathname=DASH_PREFIX + '/',
                    suppress_callback_exceptions=True,
                    background_callback_manager=background_callback_manager,
                    assets_folder=assets_folder)

    # initial_gdf = polygon_cache.get_polygons('MOD_REG')
    date_ranges_df = get_date_ranges_by_type()
    compiled_workflow, base_workflow = create_workflow(lg_State)

    # Create a resizable layout
    app.layout = html.Div([
        create_stores(),
        # Include SSE client script and new pure map state manager
        html.Script(src=f"{DASH_PREFIX}/assets/pure_map_state.js"),
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

    # Register SSE-based chat callbacks (single source of truth)
    logger.debug("About to register SSE chat callbacks")
    register_sse_chat_callbacks(app, compiled_workflow, base_workflow)
    logger.debug("SSE chat callbacks registered successfully")
    register_clientside_callbacks(app)
    register_visualization_callbacks(app, compiled_workflow)

    register_polygon_routes(app.server)
    register_bounding_box_routes(app.server)
    register_map_state_routes(app.server)

    # Register SSE endpoints
    register_sse_routes(app.server, compiled_workflow, base_workflow)

    register_commands(app.server)

    # Register Celery tasks if Redis is available
    if 'REDIS_URL' in os.environ:
        logger.debug("Registering Celery tasks")
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
        logger.error("Got the following exception when attempting db.create_all() in __init__.py: " + str(exception))
    finally:
        logger.info("db.create_all() in __init__.py was successful - no exceptions were raised")



# Celery tasks are now registered inside create_app()

if __name__ == "__main__":
    app.run(debug=True)
