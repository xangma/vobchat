# app/components/chat.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def create_chat_layout():
    return html.Div([
        html.H3("Chat"),
        dbc.Card([
            dbc.CardBody([
                html.Div(id="chat-display", style={"height": "60vh", "overflow-y": "scroll"}),
                html.Div(id="options-container"),
                dbc.Input(id="chat-input", placeholder="Type your message here...", type="text"),
                dbc.Button("Send", id="send-button", color="primary", className="mt-2", n_clicks=0),
                html.Br(),
                dbc.Button("Clear Chat", id="clear-button", color="danger", className="mt-2", n_clicks=0),
            ])
        ]),
    ])