"""
Application entrypoint and layout assembly.

This module wires together the simplified SSE architecture and the three core
panels (Chat, Visualization, Map). It exposes two HTTP endpoints used by the
client-side SSE code:

- `{base}/sse/<thread_id>` (GET): Server-Sent Events stream. Each client
  connects once per thread and receives state_update, interrupt, and error
  events.
- `{base}/workflow/<thread_id>` (POST): Advance the LangGraph workflow
  using a payload produced by user actions (chat send, option click, map select).

Layout highlights:
- Chat panel (left): chat history, options container, and input controls.
- Visualization panel (middle): hidden by default; shows graphs when data exists.
- Map panel (right): Dash Leaflet map with polygon layer and controls.

Authentication is enforced for all app URLs except Dash/static asset paths
and auth endpoints so the login page can render.
"""

# Simple App - Clean rewrite with simplified SSE architecture
import logging
import os
from dash_extensions.enrich import (
    DashProxy,
    CycleBreakerTransform,
)  # ServersideOutputTransform
import dash_bootstrap_components as dbc
from dash import html
from vobchat.workflow import create_workflow
from vobchat.state_schema import lg_State
from vobchat.stores import create_stores
from vobchat.components.chat import create_chat_layout
from vobchat.components.map import create_map_layout
from vobchat.components.visualization import create_visualization_layout

# Import simplified callbacks (now the main versions)
from vobchat.callbacks.visualization import (
    register_simple_visualization_callbacks,
)
from vobchat.callbacks.clientside_callbacks import (
    register_simple_clientside_callbacks,
)
from vobchat.callbacks.chat_sse import register_simple_chat_callbacks

# Import simplified SSE (now the main versions)
from vobchat.sse_manager import get_sse_manager
from vobchat.workflow_sse_adapter import create_simple_workflow_adapter

from vobchat.api.polygon_routes import register_polygon_routes
from vobchat.api.bounding_box_routes import register_bounding_box_routes
from vobchat.api.map_state_routes import register_map_state_routes
from vobchat.models import register_app_routes
from vobchat.utils.async_manager import async_manager
from flask import Response, request, redirect, url_for, jsonify, session
from flask_login import current_user
import pathlib
from vobchat.models import db, lm, bp as auth_bp
from vobchat.cli import register_commands

logger = logging.getLogger(__name__)

# No background callback manager needed for SSE-based workflow
background_callback_manager = None
simple_sse_manager = get_sse_manager()

# Normalize base path per Dash's url_base_pathname env var
from vobchat.config import get_dash_base_paths

ROUTE_PREFIX, URL_BASE_PATHNAME = get_dash_base_paths()
logger.info(
    "Dash base path configured: url_base_pathname=%s route_prefix=%s",
    URL_BASE_PATHNAME,
    ROUTE_PREFIX or "<root>",
)


def register_simple_sse_routes(server, workflow_adapter):
    """Register lightweight SSE endpoints used by the client.

    Exposes a long-lived streaming endpoint and a POST endpoint that advances
    the workflow and streams results back to the open SSE connection.
    """

    # ------------------------------------------------------------------ #
    # 1. STREAM                                                          #
    # ------------------------------------------------------------------ #
    @server.get(f"{ROUTE_PREFIX}/sse/<thread_id>")
    def sse_stream(thread_id):
        """Hold the EventSource connection open and push events."""
        # Enforce thread ownership: bind if unowned; otherwise require same (user, session)
        try:
            from vobchat.utils.thread_owner import get_thread_owner, bind_thread_owner

            if not current_user.is_authenticated:
                return Response("Unauthorized", status=401)
            sess_id = session.get("login_session_id")
            owner_token = f"{current_user.id}:{sess_id}"

            owner = get_thread_owner(thread_id)
            if owner is None:
                ok = bind_thread_owner(thread_id, owner_token)
                if not ok:
                    return Response("Forbidden", status=403)
            else:
                # Backward-compat: accept legacy owner that stored only user_id
                if str(owner) != owner_token and not (
                    ":" not in str(owner) and str(owner) == str(current_user.id)
                ):
                    return Response("Forbidden", status=403)
        except Exception:
            # Fail safe: if ownership checks blow up, deny access
            return Response("Forbidden", status=403)
        import queue
        import time

        q: queue.Queue[str | None] = queue.Queue()

        class Sender:
            def send(self, msg: str):
                q.put(msg)

            def close(self):
                q.put(None)

        sender = Sender()
        simple_sse_manager.add_client(thread_id, sender)

        def gen():
            try:
                # Tell browser to auto-retry after 2 s if the TCP link drops
                yield "retry: 2000\n"
                yield "data: Connected\n\n"
                last = time.time()

                while True:
                    try:
                        msg = q.get(timeout=1)
                        if msg is None:
                            break  # Sender.close() called
                        yield msg
                    except queue.Empty:
                        now = time.time()
                        if now - last > 30:  # heartbeat
                            yield "event: heartbeat\ndata: {}\n\n"
                            last = now
            finally:
                simple_sse_manager.remove_client(thread_id, sender)

        return Response(
            gen(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ------------------------------------------------------------------ #
    # 1b. THREAD MINT                                                    #
    # ------------------------------------------------------------------ #
    @server.post(f"{ROUTE_PREFIX}/threads/new")
    def mint_thread():
        """Create a new server-generated thread_id bound to this login session."""
        try:
            if not current_user.is_authenticated:
                return jsonify({"error": "Unauthorized"}), 401
            sess_id = session.get("login_session_id")
            owner_token = f"{current_user.id}:{sess_id}"
            from vobchat.utils.thread_owner import mint_thread_id

            tid = mint_thread_id(owner_token)
            if not tid:
                return jsonify({"error": "Failed to mint thread"}), 500
            return jsonify({"thread_id": tid})
        except Exception as exc:
            logger.error(f"Failed to mint thread: {exc}", exc_info=True)
            return jsonify({"error": "Failed"}), 500

    # ------------------------------------------------------------------ #
    # 2. WORKFLOW ADVANCE  (sync version)                                #
    # ------------------------------------------------------------------ #
    @server.route(f"{ROUTE_PREFIX}/workflow/<thread_id>", methods=["POST"])
    def workflow_continue(thread_id: str):
        """
        Synchronous handler: accept the button-click payload, enqueue the
        async LangGraph step, and return immediately.
        """
        # Enforce thread ownership: bind if unowned; otherwise require same (user, session)
        try:
            from vobchat.utils.thread_owner import get_thread_owner, bind_thread_owner

            if not current_user.is_authenticated:
                return (
                    jsonify({"status": "error", "message": "Unauthorized"}),
                    401,
                )
            sess_id = session.get("login_session_id")
            owner_token = f"{current_user.id}:{sess_id}"

            owner = get_thread_owner(thread_id)
            if owner is None:
                ok = bind_thread_owner(thread_id, owner_token)
                if not ok:
                    return (
                        jsonify({"status": "error", "message": "Forbidden"}),
                        403,
                    )
            else:
                # Backward-compat: accept legacy owner that stored only user_id
                if str(owner) != owner_token and not (
                    ":" not in str(owner) and str(owner) == str(current_user.id)
                ):
                    return (
                        jsonify({"status": "error", "message": "Forbidden"}),
                        403,
                    )
        except Exception:
            return (
                jsonify({"status": "error", "message": "Forbidden"}),
                403,
            )
        data = request.get_json(silent=True) or {}
        wf_input = data.get("workflow_input")

        if not wf_input:
            return (
                jsonify({"status": "error", "message": "No workflow_input"}),
                400,
            )

        # Wrap the async call in a coroutine and hand it to the async manager
        async def run():
            try:
                await workflow_adapter.run(thread_id, wf_input)
            except Exception as exc:
                logger.exception("Workflow failed: %s", exc)
                await simple_sse_manager.error(thread_id, str(exc))

        async_manager.submit_task(run())  # fire-and-forget
        return jsonify({"status": "accepted"}), 202


def create_app():
    """Create simplified Dash app with clean SSE architecture"""

    logger.info("Creating simplified Dash app")

    # Initialize Dash app
    app = DashProxy(
        __name__,
        # transforms=[CycleBreakerTransform(), ServersideOutputTransform()],
        transforms=[CycleBreakerTransform()],
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        assets_folder=str(pathlib.Path(__file__).parent / "assets"),
        url_base_pathname=URL_BASE_PATHNAME,
    )

    # Configure Flask server
    server = app.server
    server.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-key")
    server.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
        "DATABASE_URL", "sqlite:///users.db"
    )
    server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    server.config.update(
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        WTF_CSRF_ENABLED=True,
    )

    # Initialize Flask extensions
    db.init_app(server)
    lm.init_app(server)
    # Mount auth routes under the same base path so /login works behind subpaths
    server.register_blueprint(auth_bp, url_prefix=ROUTE_PREFIX or "")
    register_commands(server)

    # Logging is configured in vobchat.__init__ via configure_enhanced_logging()
    logger.info("Flask extensions initialized")

    # Skip date ranges loading - not needed for simplified version
    logger.info("Skipping date ranges loading")

    # Create workflow (pass class as expected)
    logger.info("Creating workflow")
    compiled_workflow, base_workflow = create_workflow(lg_State)

    # Create simplified workflow adapter
    workflow_adapter = create_simple_workflow_adapter(compiled_workflow)

    assets_folder = os.path.join(os.path.dirname(__file__), "assets")

    # App layout
    app.layout = html.Div(
        [
            create_stores(),
            # Expose the Dash base path to client JS (normalized with trailing slash)
            html.Script(
                children=f"window.DASH_URL_BASE_PATHNAME = '{URL_BASE_PATHNAME}';"
            ),
            # Include pure map state manager and SSE client (loaded from assets)
            html.Script(src=f"{URL_BASE_PATHNAME}assets/pure_map_state.js"),
            html.Script(src=f"{URL_BASE_PATHNAME}assets/sse_client.js"),
            html.Div(
                className="resizable-container",
                children=[
                    html.Div(
                        className="resizable-horizontal",
                        style={
                            "display": "flex",
                            "width": "100%",
                            "height": "100%",
                        },
                        children=[
                            # 1. Chat panel on the left: conversation + options + input
                            html.Div(
                                className="resizable-panel",
                                id="chat-panel",
                                children=[create_chat_layout()],
                                style={"flex": "0 0 30%"},
                            ),  # Initial width 30%
                            # First Horizontal resize handle
                            html.Div(
                                className="resize-handle-horizontal",
                                id="resize-handle-1",
                            ),
                            # 2. Visualization panel in the middle (hidden until data is available)
                            html.Div(
                                className="resizable-panel",
                                id="visualization-panel-container",
                                children=[
                                    # Wrap the viz component to control its container's visibility/style
                                    create_visualization_layout()
                                ],
                                style={
                                    "flex": "0 0 0%",  # Start collapsed
                                    "display": "none",
                                },
                            ),  # Initial width 0%, initially hidden
                            # Second Horizontal resize handle
                            html.Div(
                                className="resize-handle-horizontal",
                                id="resize-handle-2",
                                # Initially shown (or controlled by callback)
                                style={"display": "flex"},
                            ),
                            # 3. Map panel on the right: Dash Leaflet map + controls
                            html.Div(
                                className="resizable-panel",
                                id="map-panel",
                                children=[
                                    create_map_layout(assets_folder)
                                    # Initial width 30% (flex-grow: 1 allows it to take remaining space initially)
                                ],
                                style={"flex": "1 1 30%"},
                            ),
                        ],
                    ),
                ],
            ),
        ],
        id="document",
    )

    # Register simplified callbacks
    logger.info("Registering simplified callbacks")
    register_simple_chat_callbacks(app, compiled_workflow)
    register_simple_clientside_callbacks(app)
    register_simple_visualization_callbacks(app)

    # Register API routes
    register_polygon_routes(app.server)
    register_bounding_box_routes(app.server)
    register_map_state_routes(app.server)
    register_app_routes(app)

    # Register simplified SSE routes
    register_simple_sse_routes(app.server, workflow_adapter)

    # Add authentication protection
    @server.before_request
    def protect_dash():
        """
        Redirect unauthenticated users away from every URL that begins
        with / … except the static/_dash asset endpoints that Dash
        needs while the login page is showing.
        """
        path = request.path

        if not path.startswith(ROUTE_PREFIX):
            return  # not a Dash URL → ignore

        # Allow the pieces Dash needs to render its blank page assets
        # Allow only explicit auth endpoint: login. Everything else requires auth.
        safe_subpaths = ("/login",)
        if any(path.startswith(f"{ROUTE_PREFIX}{p}") for p in safe_subpaths):
            return

        # Special handling when serving at root (ROUTE_PREFIX == "")
        if ROUTE_PREFIX == "":
            # Always allow login page
            if path == "/login":
                return
            # For the bare root:
            if path == "/":
                if not current_user.is_authenticated:
                    return redirect(url_for("auth.login_page", next=path))
                else:
                    return  # authenticated users can load the Dash root

        # Block everything else unless the user is logged in
        if not current_user.is_authenticated:
            # preserve destination so Flask-Login can send them back
            return redirect(url_for("auth.login_page", next=path))

        # Ensure a per-login session id exists for owner-token binding
        try:
            if "login_session_id" not in session:
                import uuid
                session["login_session_id"] = str(uuid.uuid4())
        except Exception:
            pass

    # Create database tables
    with server.app_context():
        try:
            db.create_all()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")

    logger.info("Simplified Dash app created successfully")
    return app


# Create app instance for gunicorn
app = create_app()
server = app.server


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
