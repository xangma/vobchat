from __future__ import annotations

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_chat_layout() -> html.Div:
    return html.Div(
        [
            html.Div(
                className="d-flex justify-content-between align-items-center mb-3",
                children=[
                    html.H3("Chat", className="mb-0"),
                    dbc.Badge("Thread: loading", id="chat-thread-label", color="secondary"),
                ],
            ),
            html.Div(id="selection-summary", className="mb-2"),
            html.Div(id="chat-trust-panel", className="mb-2"),
            html.Div(id="chat-discovery-panel", className="mb-2"),
            html.Div(id="chat-status-banner", className="mb-2"),
            dcc.Loading(
                type="default",
                children=html.Div(
                    id="chat-messages",
                    style={
                        "flex": "1 1 auto",
                        "overflowY": "auto",
                        "border": "1px solid #dee2e6",
                        "borderRadius": "0.5rem",
                        "padding": "0.75rem",
                        "backgroundColor": "#ffffff",
                        "minHeight": "20rem",
                    },
                ),
            ),
            html.Div(id="chat-place-candidates", className="mt-3"),
            html.Div(
                className="mt-3",
                children=[
                    dbc.Input(
                        id="chat-input",
                        placeholder="Ask about places, themes, charts, maps, or metadata...",
                        type="text",
                        debounce=True,
                    ),
                    html.Div(
                        className="d-flex justify-content-between mt-2",
                        children=[
                            dbc.Button("Send", id="send-button", color="primary", n_clicks=0),
                            dbc.Button("Reset", id="reset-button", color="secondary", n_clicks=0),
                        ],
                    ),
                ],
            ),
        ],
        style={"height": "100%", "display": "flex", "flexDirection": "column"},
    )
