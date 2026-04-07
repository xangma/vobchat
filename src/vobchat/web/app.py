from __future__ import annotations

import logging
from pathlib import Path

import dash_bootstrap_components as dbc
from dash import Dash, html

from vobchat.auth import (
    auth_blueprint,
    db,
    ensure_auth_schema,
    install_auth_guards,
    login_manager,
    register_auth_commands,
)
from vobchat.core.llm import log_llm_startup_status
from vobchat.core.logging import configure_enhanced_logging
from vobchat.core.settings import get_settings
from vobchat.web.callbacks import (
    register_chat_callbacks,
    register_map_callbacks,
    register_visualization_callbacks,
)
from vobchat.web.clients import APIClient, get_api_client
from vobchat.web.components import (
    create_chat_layout,
    create_map_layout,
    create_visualization_layout,
)
from vobchat.web.proxy import register_proxy_routes
from vobchat.web.stores import create_web_stores


logger = logging.getLogger(__name__)


def _build_layout(settings) -> dbc.Container:
    route_prefix = settings.dash.route_prefix or ""
    proxy_chat_prefix = f"{route_prefix}/proxy/chat"
    return dbc.Container(
        fluid=True,
        className="py-3",
        children=[
            html.Div(
                id="web-runtime-config",
                style={"display": "none"},
                **{
                    "data-base-path": route_prefix,
                    "data-proxy-chat-prefix": proxy_chat_prefix,
                },
            ),
            create_web_stores(),
            dbc.Row(
                className="g-3",
                style={"minHeight": "95vh"},
                children=[
                    dbc.Col(create_chat_layout(), xl=4, lg=4, md=12),
                    dbc.Col(create_visualization_layout(), xl=4, lg=4, md=12),
                    dbc.Col(create_map_layout(), xl=4, lg=4, md=12),
                ],
            ),
        ],
    )


def create_app(api_client: APIClient | None = None):
    configure_enhanced_logging()
    settings = get_settings()
    log_llm_startup_status(component="web")
    logger.info(
        "Creating Dash web app: url_base_pathname=%s route_prefix=%s",
        settings.dash.url_base_pathname,
        settings.dash.route_prefix or "<root>",
    )

    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        assets_folder=str(Path(__file__).resolve().parents[1] / "assets"),
        url_base_pathname=settings.dash.url_base_pathname,
    )

    server = app.server
    server.config["SECRET_KEY"] = settings.auth.secret_key
    server.config["SQLALCHEMY_DATABASE_URI"] = settings.auth.database_url
    server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    server.config.update(
        SESSION_COOKIE_SECURE=settings.auth.session_cookie_secure,
        SESSION_COOKIE_HTTPONLY=settings.auth.session_cookie_httponly,
        SESSION_COOKIE_SAMESITE=settings.auth.session_cookie_samesite,
        WTF_CSRF_ENABLED=settings.auth.wtf_csrf_enabled,
    )

    db.init_app(server)
    login_manager.init_app(server)
    server.register_blueprint(auth_blueprint, url_prefix=settings.dash.route_prefix or "")
    register_auth_commands(server)
    install_auth_guards(server, settings.dash.route_prefix)

    app.layout = _build_layout(settings)

    client = api_client or get_api_client()
    register_chat_callbacks(app, client)
    register_map_callbacks(app, client)
    register_visualization_callbacks(app, client)

    register_proxy_routes(server)

    with server.app_context():
        ensure_auth_schema()

    logger.info("Dash web app created successfully")
    return app


def main() -> None:
    settings = get_settings()
    app.run(
        debug=settings.dash.debug,
        host=settings.dash.host,
        port=settings.dash.port,
    )


app = create_app()
server = app.server


if __name__ == "__main__":
    main()
