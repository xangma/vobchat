import os, json
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html, ALL, MATCH, no_update, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import dash_bootstrap_components as dbc

from langchain_community.llms import OpenAI
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage, AnyMessage
from langchain_core.tools import tool, StructuredTool

from langchain_community.agent_toolkits.sql.prompt import SQL_FUNCTIONS_SUFFIX
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langchain_core.prompts.chat import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    MessagesPlaceholder,
)
from typing import Annotated, Literal, Optional, List, Dict, Any

from pydantic import BaseModel, Field
from typing_extensions import TypedDict
from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import AnyMessage, add_messages
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
import shapely
from config import load_config, get_db
from prompts import SQL_PREFIX
from tools import calculate_center_and_zoom

from workflow import lg_State, create_workflow
from mapinit import get_mapinit_polygons


# Settings
config = load_config()
db = get_db(config)

# Get the polygons
gdf, geojson = get_mapinit_polygons()

# Create the workflow
compiled_workflow = create_workflow(lg_State, gdf)

# Create Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

fig = px.choropleth_mapbox(geojson=geojson,
                            locations=gdf.index,
                            center={"lat": 51.50, "lon": -0.11},
                            zoom=5,
                            mapbox_style="open-street-map",
                            )

# Define the layout with a chat box on the left and a map on the right
app.layout = dbc.Container([
    html.H1("DDME Prototype"),
    html.P("This is a prototype for a dashboard that combines a chat interface with a map."),
    dbc.Row([
        dbc.Col([
            html.H3("Chat"),
            dbc.Card([
                dbc.CardBody([
                    html.Div(id="chat-display", style={"height": "60vh", "overflow-y": "scroll"}),
                    html.Div(id="options-container"),  # For dynamically generated buttons
                    dbc.Input(id="chat-input", placeholder="Type your message here...", type="text"),
                    # Send message button
                    dbc.Button("Send", id="send-button",
                                color="primary", className="mt-2", n_clicks=0),
                    # Space
                    html.Br(),
                    # Clear Chat button
                    dbc.Button("Clear Chat", id="clear-button",
                                color="danger", className="mt-2", n_clicks=0),
                    dcc.Store(id="thread_id", data=1),
                ])
            ]),
        ], md=6),
        dbc.Col([
            html.H3("Map"),
            dcc.Graph(id='choropleth-map', figure=fig,
                        style={"height": "70vh"},
                        ),
            html.Button("Reset Selections", id="reset-btn", n_clicks=0),
            # Debug messages
            html.Div(id='debug-output', style={'whiteSpace': 'pre-line'}),
            dcc.Store(id="selected_ids"),
        ], md=6),
    ]),
], fluid=True)

# Callback to update the map from the selected polygons in dcc.Store
@app.callback(
    Output('choropleth-map', 'figure', allow_duplicate=True),
    Output('debug-output', 'children', allow_duplicate=True),
    Input('selected_ids', 'data'),
    State('choropleth-map', 'figure'),
    prevent_initial_call=True
)
def update_map_from_store(selected_ids, current_fig):
    if not selected_ids:
        return current_fig, "No polygons selected."
    
    # Filter the GeoDataFrame based on selected IDs
    if type(selected_ids) is int:
        selected_ids = [selected_ids]
    gdf_filtered = gdf[gdf.index.isin(selected_ids)]

    # Get center and zoom from helper function
    map_properties = calculate_center_and_zoom(gdf_filtered)
    if map_properties["center"] and map_properties["zoom"] is not None:
        current_fig['layout']['mapbox']['center'] = map_properties["center"]
        current_fig['layout']['mapbox']['zoom'] = map_properties["zoom"]

    for i, d in enumerate(current_fig['data']):
        if d['type'] == 'choroplethmapbox':
            current_fig['data'][i]['selectedpoints'] = selected_ids

    debug_info = f"Zoomed to selected polygons: Center=({map_properties["center"]}), Zoom={map_properties["zoom"]}"
    return current_fig, debug_info


# Callback for clickData (single selection)
@app.callback(
    Output('choropleth-map', 'figure'),
    Output('debug-output', 'children'),
    Input('choropleth-map', 'clickData'),
    State('choropleth-map', 'figure'),
    prevent_initial_call=True
)
def handle_click(clickData, current_fig):
    if clickData:
        clicked_id = clickData['points'][0]['location']
        debug_info = f"Single polygon selected with ID: {clicked_id}\n"
        for i, d in enumerate(current_fig['data']):
            if d['type'] == 'choroplethmapbox':
                current_fig['data'][i]['selectedpoints'] = [clicked_id]
        return current_fig, debug_info

    return current_fig, "No polygon selected."


# Callback for selectedData (box/lasso selection)
@app.callback(
    Output('choropleth-map', 'figure', allow_duplicate=True),
    Output('debug-output', 'children', allow_duplicate=True),
    Input('choropleth-map', 'selectedData'),
    State('choropleth-map', 'figure'),
    prevent_initial_call=True
)
def handle_box_lasso(selectedData, current_fig):
    if selectedData:
        selected_ids = [p['location'] for p in selectedData['points']]
        debug_info = f"Box/Lasso selection made with {
            len(selected_ids)} polygons selected.\n"
        debug_info += f"Selected polygon IDs: {selected_ids}\n"
        for i, d in enumerate(current_fig['data']):
            if d['type'] == 'choroplethmapbox':
                current_fig['data'][i]['selectedpoints'] = selected_ids
        return current_fig, debug_info

    return current_fig, "No polygons selected."

# Reset button callback
@app.callback(
    Output('choropleth-map', 'figure', allow_duplicate=True),
    Output("selected_ids", "data", allow_duplicate=True),
    Output('debug-output', 'children', allow_duplicate=True),
    Input('reset-btn', 'n_clicks'),
    State('choropleth-map', 'figure'),
    prevent_initial_call=True
)
def reset_selection(n_clicks, current_fig):
    if n_clicks > 0:
        for i, d in enumerate(current_fig['data']):
            if d['type'] == 'choroplethmapbox':
                current_fig['data'][i]['selectedpoints'] = None
        return current_fig, [], "Reset button clicked. All selections cleared."

    return current_fig, [], ""

# Clear Chat callback
@app.callback(
    Output("chat-display", "children", allow_duplicate=True),
    Output("chat-input", "value"),
    Output("thread_id", "data", allow_duplicate=True),
    Input("clear-button", "n_clicks"),
    Input("thread_id", "data"),
    prevent_initial_call=True
)
def clear_chat(n_clicks, thread_id):
    if n_clicks is None:
        raise PreventUpdate
    thread_id = thread_id + 1

    return [], "", thread_id


def print_stream(stream):
    for s in stream:
        message = s["messages"][-1]
        if isinstance(message, tuple):
            print(message)
        else:
            message.pretty_print()


@app.callback(
    Output("chat-display", "children", allow_duplicate=True),
    Output("options-container", "children", allow_duplicate=True),
    Input({"type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
    State("chat-display", "children"),
    Input("options-container", "children"),
    prevent_initial_call=True,
)
def handle_button_click(n_clicks_list, chat_history, buttons):
    ctx = callback_context  # Get the callback context

    if not ctx.triggered:  # Ensure something triggered the callback
        return chat_history

    if len(buttons) > 0:
        # Get the ID of the clicked button
        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]  # Extract the JSON part
        try:
            triggered_id_dict = json.loads(triggered_id)  # Parse the JSON into a dictionary
            if triggered_id_dict.get("type") == "dynamic-button-user-choice":
                clicked_index = triggered_id_dict.get("index")  # Safely get the index
                if clicked_index is not None:
                    # Add user selection to chat
                    res_text = f"User selected: {clicked_index}"
                    chat_history.append(html.Div(res_text, className="mb-2"))
                    return chat_history, []
        except (json.JSONDecodeError, AttributeError):
            clicked_index = None
    return chat_history, buttons


@app.callback(
    Output("chat-display", "children", allow_duplicate=True),
    Output("chat-input", "value", allow_duplicate=True),
    Output("selected_ids", "data", allow_duplicate=True),
    Output("options-container", "children"),
    Input("send-button", "n_clicks"),
    Input("thread_id", "data"),
    State("chat-input", "value"),
    State("chat-display", "children"),
    Input("options-container", "children"),
    prevent_initial_call=True
)
def update_chat(n_clicks, thread_id, user_input, chat_history, buttons):
    # Check there was user input
    selection = None
    buttons = []

    if n_clicks is None or user_input is None or user_input.strip() == "":
        # Check if the last message in chat_history[-1]['props']['children'][0] starts with "User selected:"
        if chat_history and chat_history[-1]['props']['children'].startswith("User selected:"):
            selection = int(chat_history[-1]['props']['children'].split(": ")[1])
        else:
            return chat_history, "", no_update



    # Add user message
    user_message = html.Div(f"You: {user_input}", className="mb-2")
    
    config = {"configurable": {"thread_id": thread_id}}

    # user_input could be: How has the population of Portsmouth changed over time?
    inputs = {"messages": [("user", user_input)], "selection": str(selection)}
    db_res = compiled_workflow.invoke(
        inputs, config=config)
    for message in db_res['messages']:
        if isinstance(message, ToolMessage):
            print(f"ToolMessage: {message}")
    ai_out = db_res['messages'][-1].content
    ai_response = f"AI: {ai_out}"
    
    # Present buttons if required.
    if isinstance(ai_out, list):
        if isinstance(ai_out[0], dict):
            if ai_out[0].get("type") == "buttons":
                # Generate buttons from the response
                buttons = [
                    html.Button(
                        opt["label"], id={"type": "dynamic-button-user-choice", "index": opt["value"]}
                    )
                    for opt in ai_out
                ]
                # Add instruction message
                ai_response = ("AI: Please select an option.")

    gdf_id = db_res.get('selected_place_gdf_id')
    
    # Format response
    ai_response = ai_response.split("\n")
    ai_message_formatted = []
    for i, line in enumerate(ai_response):
        if line != "":
            ai_message_formatted.append(line)
            ai_message_formatted.append(html.Br())
    ai_message = html.Div(ai_message_formatted, className="mb-2 text-primary")

    if chat_history is None:
        chat_history = []
    
    return chat_history + [user_message, ai_message], "", gdf_id, buttons

# Run the app
if __name__ == '__main__':
    os.environ["HOST"] = "127.0.0.1"
    app.run(debug=True)
    