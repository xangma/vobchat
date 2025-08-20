"""
Chat panel layout.

This module defines the right-hand chat panel used by the application. It
contains three main areas stacked vertically using a flex column:

- chat-display: scrollable area that renders the conversation history. The
  SSE client (assets/sse_client.js) updates this element directly based on
  streamed state updates.
- options-container: transient area for contextual buttons (e.g., disambiguation
  choices, unit-type selections). Buttons are injected and cleared by the SSE
  client as the workflow requests user input. We purposely keep this separate
  from the chat history so they don’t scroll away.
- input area: a fixed input box and two buttons (Send/Reset) for user input and
  resetting the whole application state.

IDs here are referenced by server callbacks and the SSE client:
- "chat-display" is updated by SSE to reflect state.messages
- "options-container" is cleared/filled by the SSE client when interrupts fire
- "chat-input", "send-button", and "reset-button" are wired in callbacks
"""

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_chat_layout():
    """Create the chat panel layout.

    Returns a container with title, a scrollable chat history area, a dynamic
    options/button area, and a fixed input area. The fixed input avoids UI
    jitter when messages stream in, while options are managed entirely by the
    SSE client according to workflow interrupts.
    """
    return html.Div([
        html.H3("Chat", className="mb-3"),
        # Column flex box; the header above takes ~40px, so keep remaining space
        html.Div(
            style={"display": "flex", "flexDirection": "column", "height": "calc(100% - 40px)"},
            children=[
                # Scrollable history and options (no overlay spinner)
                html.Div(
                    style={
                        "flex": "1 1 auto",
                        "display": "flex",
                        "flexDirection": "column",
                        "position": "relative",
                        "minHeight": "0"
                    },
                    children=[
                        # Chat display (scrollable history rendered by SSE)
                        html.Div(
                            id="chat-display",
                            style={
                                "flex": "1 1 auto",
                                "overflowY": "auto",
                                "marginBottom": "10px",
                                "border": "1px solid #dee2e6",
                                "borderRadius": "5px",
                                "padding": "10px"
                            }
                        ),

                        # Transient options container for contextual buttons
                        html.Div(
                            id="options-container",
                            style={"marginBottom": "10px"}
                        ),

                        # Theme UI overlay: status + selection panel (anchored bottom-right)
                        html.Div(
                            id="theme-ui-container",
                            style={
                                "position": "absolute",
                                "right": "10px",
                                "bottom": "10px",
                                "zIndex": 20,
                            },
                            children=[
                                # Status pill/button (click to open selection panel)
                                html.Div(
                                    id="theme-status",
                                    n_clicks=0,
                                    children=[
                                        html.Span(id="theme-status-label", children="Theme: (none)")
                                    ],
                                    className="theme-status-pill",
                                ),
                                # Hidden selection panel; buttons injected by SSE client
                                html.Div(
                                    id="theme-selection-panel",
                                    className="theme-selection-panel",
                                    style={"display": "none"},
                                    children=[
                                        html.Div(
                                            className="theme-panel-header",
                                            children=[
                                                html.Span("Select a theme"),
                                                html.Button(
                                                    "Clear",
                                                    id="theme-panel-clear",
                                                    n_clicks=0,
                                                    className="theme-panel-clear"
                                                ),
                                                html.Button(
                                                    "×",
                                                    id="theme-panel-close",
                                                    n_clicks=0,
                                                    className="theme-panel-close"
                                                ),
                                            ],
                                        ),
                                        html.Div(id="theme-selection-buttons")
                                    ],
                                ),
                            ],
                        ),
                    ]
                ),

                # Fixed input area (never scrolls off screen)
                html.Div(
                    style={"flexShrink": "0"},
                    children=[
                        dbc.Input(
                            id="chat-input",
                            placeholder="Type your message here...",
                            type="text",
                            className="mb-2",
                            debounce=True,
                        ),
                        html.Div(
                            className="d-flex justify-content-between",
                            children=[
                                dbc.Button(
                                    "Send",
                                    id="send-button",
                                    color="primary",
                                    n_clicks=0
                                ),
                                dbc.Button(
                                    "Reset Application",
                                    id="reset-button",
                                    color="danger",
                                    n_clicks=0
                                )
                            ]
                        )
                    ]
                )
            ]
        )
    ], style={"height": "100%", "display": "flex", "flexDirection": "column"})
