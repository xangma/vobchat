from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableConfig
from IPython.display import Image, display
import os
import re
import json
import pandas as pd
import geopandas as gpd
from pyproj import CRS
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash import no_update
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

###################
# Langchain Setup #
###################


# messages = [
#     HumanMessagePromptTemplate.from_template("{input}"),
#     AIMessage(content=SQL_PREFIX),
#     MessagesPlaceholder(variable_name="agent_scratchpad"),
# ]



# # Define the state for the agent
# class State(TypedDict):
#     messages: Annotated[list[AnyMessage], add_messages]


# # Define a new graph
# workflow = StateGraph(State)


# # Add a node for the first tool call
# def first_tool_call(state: State) -> dict[str, list[AIMessage]]:
#     return {
#         "messages": [
#             AIMessage(
#                 content="",
#                 tool_calls=[
#                     {
#                         "name": "sql_db_list_tables",
#                         "args": {},
#                         "id": "tool_abcd123",
#                     }
#                 ],
#             )
#         ]
#     }

# query_check_system = """You are a SQL expert with a strong attention to detail.
# Double check the PostGRESQL query for common mistakes, including:
# - Using NOT IN with NULL values
# - Using UNION when UNION ALL should have been used
# - Using BETWEEN for exclusive ranges
# - Data type mismatch in predicates
# - Properly quoting identifiers
# - Using the correct number of arguments for functions
# - Casting to the correct data type
# - Using the proper columns for joins

# If there are any of the above mistakes, rewrite the query. If there are no mistakes, just reproduce the original query.

# You will call the appropriate tool to execute the query after running this check."""

# query_check_prompt = ChatPromptTemplate.from_messages(
#     [("system", query_check_system), ("placeholder", "{messages}")]
# )
# # Give the model the query check tool, and set the tool_choice to "required" to ensure the tool is used.
# query_check = query_check_prompt | ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(
#     [db_query_tool], tool_choice="required"
# )

# # Define a tool to check the query before executing it
# def model_check_query(state: State) -> dict[str, list[AIMessage]]:
#     """
#     Use this tool to double-check if your query is correct before executing it.
#     """
#     return {"messages": [query_check.invoke({"messages": [state["messages"][-1]]})]}


# # Add the nodes to the graph
# workflow.add_node("first_tool_call", first_tool_call)

# # Add nodes for the first two tools
# workflow.add_node(
#     "list_tables_tool", create_tool_node_with_fallback([list_tables_tool])
# )
# workflow.add_node("get_schema_tool",
#                   create_tool_node_with_fallback([get_schema_tool]))

# # Add a node for a model to choose the relevant tables based on the question and available tables
# model_get_schema = ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(
#     [get_schema_tool]
# )
# workflow.add_node(
#     "model_get_schema",
#     lambda state: {
#         "messages": [model_get_schema.invoke(state["messages"])],
#     },
# )

# # Describe a tool to represent the end state
# class SubmitFinalAnswer(BaseModel):
#     """Submit the final answer to the user based on the query results."""

#     final_answer: str = Field(..., description="The final answer to the user")


# query_gen_prompt = ChatPromptTemplate.from_messages(
#     [("system", query_gen_system), ("placeholder", "{messages}")]
# )
# query_gen = query_gen_prompt | ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(
#     [SubmitFinalAnswer, model_check_query]
# )


# # Define a node for the query generation
# def query_gen_node(state: State):
#     message = query_gen.invoke(state)

#     # Sometimes, the LLM will hallucinate and call the wrong tool. We need to catch this and return an error message.
#     tool_messages = []
#     if message.tool_calls:
#         for tc in message.tool_calls:
#             if tc["name"] != "SubmitFinalAnswer":
#                 tool_messages.append(
#                     ToolMessage(
#                         content=f"Error: The wrong tool was called: {tc['name']}. Please fix your mistakes. Remember to only call SubmitFinalAnswer to submit the final answer. Generated queries should be outputted WITHOUT a tool call.",
#                         tool_call_id=tc["id"],
#                     )
#                 )
#     else:
#         tool_messages = []
#     return {"messages": [message] + tool_messages}


# # Add the nodes to the graph
# workflow.add_node("query_gen", query_gen_node)

# # Add a node for the model to check the query before executing it
# workflow.add_node("correct_query", model_check_query)

# # Add node for executing the query
# workflow.add_node(
#     "execute_query", create_tool_node_with_fallback([db_query_tool]))


# # Define a conditional edge to decide whether to continue or end the workflow
# def should_continue(state: State) -> Literal[END, "correct_query", "query_gen"]:
#     messages = state["messages"]
#     last_message = messages[-1]
#     # If there is a tool call, then we finish
#     if getattr(last_message, "tool_calls", None):
#         return END
#     if last_message.content.startswith("Error:"):
#         return "query_gen"
#     else:
#         return "correct_query"



# context = toolkit.get_context()


# tools = tools + [highlight_polygons_on_map, find_cubes_for_unit_theme, find_units_by_postcode, find_themes_for_unit]


# # Specify the edges between the nodes
# workflow.add_edge(START, "first_tool_call")
# workflow.add_edge("first_tool_call", "list_tables_tool")
# workflow.add_edge("list_tables_tool", "model_get_schema")
# workflow.add_edge("model_get_schema", "get_schema_tool")
# workflow.add_edge("get_schema_tool", "query_gen")
# workflow.add_conditional_edges(
#     "query_gen",
#     should_continue,
# )
# workflow.add_edge("correct_query", "execute_query")
# workflow.add_edge("execute_query", "query_gen")
# # Compile the workflow into a runnable
# compiled_workflow = workflow.compile(checkpointer=memory)



### OLD METHOD
# System message
# system_message = SQL_PREFIX

# langgraph_agent_executor = create_react_agent(
#     model=model,
#     tools=tools,
#     state_modifier=system_message,
#     checkpointer=memory
# )

# prompt = ChatPromptTemplate.from_messages(messages)
# prompt = prompt.partial(**context)
# tools_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# agent = create_openai_tools_agent(tools_llm, tools, prompt)

# agent_executor = AgentExecutor(
#     agent=agent,
#     tools=tools,
#     verbose=True,
# )


#############
# Callbacks #
#############

# Callback to update the dcc.Store based on LangChain's output
# @app.callback(
#     Output("selected_ids", "data"),
#     Input('send-button', 'n_clicks'),
#     State('chat-input', 'value'),
#     prevent_initial_call=True
# )
# def handle_langchain_query(n_clicks, user_input):
#     if n_clicks is None or not user_input:
#         raise PreventUpdate

    # config = {"configurable": {"thread_id": "1"}}
    # # Invoke LangChain agent and process the output
    # inputs = {"messages": [("user", user_input)]}
    # # db_res = agent_executor.invoke(
    # #     {"input": f"{user_input}"})
    # # ai_response = f"AI: {db_res['output']}"
    # db_res = compiled_workflow.invoke(
    #     inputs, config=config)

    # ai_out = db_res['messages'][-1]
    # # Debugging: check what db_res contains
    # print(f"LangChain result: {ai_out}")

    # # Extract the g_unit numbers from the agent output (assuming it's a list of strings)

    # # Ensure g_unit_numbers is a list of valid g_unit ids
    # if isinstance(ai_out, AIMessage):
    #     g_unit_numbers = re.findall(r'\d+', ai_out.content)

    # if not g_unit_numbers:
    #     return no_update
    # # Debugging: check the g_unit_numbers extracted
    # print(f"g_unit_numbers extracted: {g_unit_numbers}")
    
    # g_unit_numbers_ints = [int(num) for num in g_unit_numbers]
    
    # # find index from g_unit in gdf
    # gdffilt = gdf[gdf['g_unit'].isin(g_unit_numbers_ints)]
    
    # idx = list(gdffilt.index)

    # # Store the selected g_unit numbers in dcc.Store
    # return {"data": idx}


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
    Output("chat-input", "value", allow_duplicate=True),
    Output("selected_ids", "data", allow_duplicate=True),
    [Input("send-button", "n_clicks"),
    Input("thread_id", "data")],
    [State("chat-input", "value"),
    State("chat-display", "children")],
    prevent_initial_call=True
)
def update_chat(n_clicks, thread_id, user_input, chat_history):
    if n_clicks is None or user_input is None or user_input.strip() == "":
        return chat_history, "", no_update

    # Add user message
    user_message = html.Div(f"You: {user_input}", className="mb-2")
    
    config = {"configurable": {"thread_id": thread_id}}
    # user_input could be: How has the population of Portsmouth changed over time?
    inputs = {"messages": [("user", user_input)]}
    # db_res = agent_executor.invoke(
    #     {"input": f"{user_input}"})
    # ai_response = f"AI: {db_res['output']}"
    db_res = compiled_workflow.invoke(
        inputs, config=config)
    for message in db_res['messages']:
        if isinstance(message, ToolMessage):
            print(f"ToolMessage: {message}")
    ai_out = db_res['messages'][-1]
    ai_response = f"AI: {ai_out.content}"
    
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
    
    return chat_history + [user_message, ai_message], "", gdf_id

# Run the app
if __name__ == '__main__':
    os.environ["HOST"] = "127.0.0.1"
    app.run(debug=True)
    