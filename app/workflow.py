from typing import Annotated, Optional, List
import re
import pandas as pd
from typing_extensions import TypedDict

# Pydantic / Models
from pydantic import BaseModel, Field

# LangChain / LangGraph
from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import AnyMessage, add_messages
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage, AnyMessage
from langchain_core.prompts import ChatPromptTemplate
from langgraph.errors import NodeInterrupt
from langchain_core.runnables.graph import MermaidDrawMethod

# Local imports
from config import load_config, get_db
from tools import (
    find_cubes_for_unit_theme,
    find_units_by_postcode,
    find_themes_for_unit,
    find_places_by_name
)
from mapinit import get_polygons_by_type
from utils.polygon_cache import polygon_cache


# ----------------------------------------------------------------------------------------
# CONFIG & SETUP
# ----------------------------------------------------------------------------------------

config = load_config()
db = get_db(config)

# Get initial polygons (arbitrary example)
initial_gdf = get_polygons_by_type('MOD_REG')

# Memory for checkpointing
memory = MemorySaver()

# Model
# model = ChatOpenAI(model="gpt-4o-mini")
model = ChatOllama( model="phi4tools.modelfile:latest", base_url="https://roni1.uni.ds.port.ac.uk/ollama/", client_kwargs={"verify": False})

# Tools
toolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = toolkit.get_tools()

list_tables_tool = next(tool for tool in tools if tool.name == "sql_db_list_tables")
get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

# UK postcode regex
postcode_regex = (
    r"([Gg][Ii][Rr] 0[Aa]{2})|"
    r"((([A-Za-z][0-9]{1,2})|"
    r"(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|"
    r"(([A-Za-z][0-9][A-Za-z])|"
    r"([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))"
    r"))\s?[0-9][A-Za-z]{2})"
)

# ----------------------------------------------------------------------------------------
# STATE
# ----------------------------------------------------------------------------------------
class lg_State(TypedDict):
    messages: Annotated[List[AnyMessage], add_messages]
    selection_idx: Optional[int]
    selected_place: Optional[str]
    selected_place_g_place: Optional[int]
    selected_place_g_unit: Optional[int]
    selected_place_themes: Optional[str]
    selected_theme: Optional[str]
    is_postcode: bool
    extracted_postcode: Optional[str]
    extracted_place_name: Optional[str]
    selected_polygons: Optional[List[int]]  # New: store polygon selections here


# ----------------------------------------------------------------------------------------
# NODES
# ----------------------------------------------------------------------------------------

def validate_user_input(state: lg_State) -> lg_State:
    """
    Looks at the last user message to check if it contains a valid UK postcode.
    """
    print(f"Initial state: {state}")
    user_input = state["messages"][-1].content

    postcode_match = re.search(postcode_regex, user_input)
    if postcode_match:
        state["is_postcode"] = True
        state["extracted_postcode"] = postcode_match.group(0)
    else:
        state["is_postcode"] = False
        state["extracted_postcode"] = None

    print(f"Validated state: {state}")
    return state


def decide_next_node(state: lg_State) -> str:
    """
    Decide whether to handle the input as a postcode, place name, or need extraction.
    """
    if state.get("is_postcode"):
        return "postcode_tool_call"
    elif state.get("extracted_place_name"):
        return "place_tool_call"
    else:
        return "extract_place_name_node"


class Place(BaseModel):
    """Information about a place."""
    name: Optional[str] = Field(default=None, description="The name of the place")


class ExtractedData(BaseModel):
    """Extracted data about places."""
    places: List


place_extraction_prompt = ChatPromptTemplate.from_messages([
    (
        "assistant",
        "You are an expert extraction algorithm. "
        "Only extract place names from the text. "
        "If you do not know the name of an attribute asked to extract, return null for the attribute's value."
    ),
    ("user", "{text}"),
])

# Prompt + model combined into a runnable that returns ExtractedData
place_extraction_runnable = place_extraction_prompt | model.with_structured_output(schema=ExtractedData)


def extract_place_name_node(state: lg_State) -> lg_State:
    """
    Use the place_extraction_runnable chain to get a place name from the text.
    """
    print(f"State at start of extract_place_name_node: {state}")
    text = state["messages"][-1].content
    if type(text) == str:
        text = [text]
    extracted_data = place_extraction_runnable.invoke({"text": text})
    if extracted_data and extracted_data.places:
        state["extracted_place_name"] = extracted_data.places[0]
    else:
        state["extracted_place_name"] = None

    print(f"State at end of extract_place_name_node: {state}")
    return state


def postcode_tool_call(state: lg_State) -> lg_State:
    """
    If a postcode was extracted, find relevant units by postcode.
    """
    extracted_postcode = state.get("extracted_postcode")
    if not extracted_postcode:
        state["messages"].append(AIMessage(content="No valid postcode was found."))
        return state

    response = find_units_by_postcode(extracted_postcode)
    print("\n=== Postcode Search Results ===")
    print(f"Postcode: {extracted_postcode}")
    print(response.to_string())
    print("==============================\n")

    state["messages"].append(AIMessage(content=response.to_string()))
    if not response.empty:
        state["selected_place"] = response.to_json(index=True)
        state["selected_place_g_unit"] = int(response["g_unit"].values[0])
        state["selected_place_g_place"] = int(response["g_place"].values[0])
    else:
        state["messages"].append(AIMessage(content="No units found for that postcode."))

    return state


def place_tool_call(state: lg_State) -> lg_State:
    """
    If a place name was extracted, call the DB for matching places.
    """
    print(f"State in place_tool_call: {state}")
    place_name = state["extracted_place_name"]
    returned_places = find_places_by_name(place_name)
    print("\n=== Place Search Results ===")
    print(f"Place name: {place_name}")
    print(returned_places.to_string())
    print("===========================\n")

    state["selected_place"] = returned_places.to_json(index=True)
    return state


def place_tool_handler(state: lg_State) -> lg_State:
    """
    Handle the place tool results, possibly interrupting for user selection.
    """
    returned_places = pd.read_json(state["selected_place"])
    selection_idx = state.get("selection_idx")

    # If user has made a selection, narrow down to that row
    if selection_idx is not None:
        returned_places = returned_places.iloc[int(selection_idx)].to_frame().T
        state["selected_place"] = returned_places.to_json(index=True)
        state["selection_idx"] = None

    num_results = len(returned_places)
    if num_results == 1:
        state["selected_place_g_place"] = int(returned_places["g_place"].values[0])
        state["messages"].append(AIMessage(content=f"Place found: {returned_places.to_string()}"))
    elif num_results > 1:
        button_options = [
            {
                "option_type": "place",
                "label": row["g_name"] + ", " + row["county_name"],
                "value": index
            }
            for index, row in returned_places[["g_name", "county_name"]].iterrows()
        ]
        raise NodeInterrupt(value={
            "message": "Multiple places found. Please select one.",
            "options": button_options
        })
    else:
        state["messages"].append(AIMessage(content="No results found for that place."))

    print(f"State at end of place_tool_call: {state}")
    return state


def handle_user_place_selection(state: lg_State) -> lg_State:
    """
    Once the user or system has decided on a single place row, set g_unit and (optionally) g_unit_type.
    If multiple (g_unit, g_unit_type) combos are available, ask user which one they want.
    Otherwise, raise an interrupt that instructs the UI to update the filter and highlight the polygon.
    """
    selected_place = state.get("selected_place")
    if not selected_place:
        response_message = AIMessage(content="No place was selected previously.")
        state["messages"].append(response_message)
        return state

    # Convert JSON -> DataFrame
    selected_place_df = pd.read_json(selected_place)

    if selected_place_df.empty:
        response_message = AIMessage(content="The selected place DataFrame is empty.")
        state["messages"].append(response_message)
        return state

    # Provide a short summary message
    response_message = AIMessage(
        content=f"Place selected:\n{selected_place_df[['g_name', 'county_name']].to_string(index=False)}"
    )
    state["messages"].append(response_message)

    # Explode so that each (g_unit, g_unit_type) appears on its own row
    exploded_df = selected_place_df.explode(["g_unit", "g_unit_type"])
    exploded_df = exploded_df.dropna(subset=["g_unit"]).copy()
    exploded_df["g_unit"] = exploded_df["g_unit"].astype(int)

    # If we have a user selection (selection_idx), use it to pick exactly one row
    selection_idx = state.get("selection_idx")
    if selection_idx is not None:
        chosen_row = exploded_df.iloc[int(selection_idx)]
        state["selected_place_g_unit"] = int(chosen_row["g_unit"])
        state["selected_place_g_unit_type"] = chosen_row["g_unit_type"] or "MOD_DIST"
        # Clear selection_idx so we don't get stuck
        state["selection_idx"] = None

        # Raise a NodeInterrupt that instructs the Dash UI to update the filter + select the unit
        raise NodeInterrupt(value={
            "message": "map_selection",
            "g_unit": str(state["selected_place_g_unit"]),
            "g_unit_type": state["selected_place_g_unit_type"]
        })

    # If no selection_idx yet, check how many rows are available
    if len(exploded_df) == 0:
        # No valid g_units
        state["messages"].append(AIMessage(content="No valid g_unit was found for the selected place."))
        return state
    elif len(exploded_df) == 1:
        # Exactly one row => set that automatically
        single_row = exploded_df.iloc[0]
        state["selected_place_g_unit"] = int(single_row["g_unit"])
        state["selected_place_g_unit_type"] = single_row["g_unit_type"] or "MOD_DIST"

        raise NodeInterrupt(value={
            "message": "map_selection",
            "g_unit": str(state["selected_place_g_unit"]),
            "g_unit_type": state["selected_place_g_unit_type"]
        })
    else:
        # Multiple rows => ask the user which one
        button_options = []
        for i, row in exploded_df.iterrows():
            label = f"{row['g_unit_type']} (ID={row['g_unit']})"
            button_options.append({
                "option_type": "unit_selection",
                "label": label,
                "value": i  # We'll use this index to pick the row next time
            })

        raise NodeInterrupt(value={
            "message": "Multiple (g_unit, g_unit_type) options found. Please select one.",
            "options": button_options
        })

    # If we haven't got a selection from the user yet, check how many possible rows
    if len(exploded_df) == 0:
        # No valid g_units
        state["messages"].append(AIMessage(content="No valid g_unit was found for the selected place."))
        return state
    elif len(exploded_df) == 1:
        # Exactly one row => set that automatically
        single_row = exploded_df.iloc[0]
        state["selected_place_g_unit"] = int(single_row["g_unit"])
        # Attempt to retrieve polygons
        unit_type = single_row["g_unit_type"] or "MOD_DIST"
        gdf = polygon_cache.get_polygons(unit_type)
        if state["selected_place_g_unit"] in gdf.index:
            state["selected_polygons"] = [str(state["selected_place_g_unit"])]
            raise NodeInterrupt(
                value={
                    "message": "selected_polygons",
                    "value": [str(state["selected_place_g_unit"])]
                }
            )
        else:
            state["messages"].append(
                AIMessage(content=f"Polygon not found for g_unit={state['selected_place_g_unit']}.")
            )
        return state
    else:
        # Multiple rows => ask the user which one
        button_options = []
        for i, row in exploded_df.iterrows():
            # Construct a label, e.g. "MOD_DIST (ID=10168181)"
            label = f"{row['g_unit_type']} (ID={row['g_unit']})"
            # i is the index in exploded_df
            button_options.append({
                "option_type": "unit_selection",
                "label": label,
                "value": i  # We will pick up this index from the callback
            })

        raise NodeInterrupt(value={
            "message": "Multiple (g_unit, g_unit_type) options found. Please select one.",
            "options": button_options
        })


def get_place_themes_node(state: lg_State) -> lg_State:
    """
    Retrieve themes for the selected place.
    """
    selected_place_g_unit = state.get("selected_place_g_unit")
    if selected_place_g_unit:
        selected_place_themes = find_themes_for_unit(str(selected_place_g_unit))
        print("\n=== Themes for Selected Place ===")
        print(f"Unit ID: {selected_place_g_unit}")
        print(selected_place_themes.to_string())
        print("===============================\n")

        state["selected_place_themes"] = selected_place_themes.to_json(index=True)
    else:
        response_message = AIMessage(content="The selected place was not found.")
        state["messages"].append(response_message)
    return state


def get_place_themes_handler(state: lg_State) -> lg_State:
    """
    If multiple themes are available, raise an interrupt to let user choose.
    """
    selected_place_themes = pd.read_json(state["selected_place_themes"])
    if selected_place_themes.empty:
        response_message = AIMessage(content="No themes found for the selected place.")
        state["messages"].append(response_message)
        return state

    selection_idx = state.get("selection_idx")
    if selection_idx is not None:
        selected_theme = selected_place_themes.iloc[int(selection_idx)].to_frame().T
        state["selected_theme"] = selected_theme.to_json(index=True)

    if not state.get("selected_theme"):
        button_options = [
            {"option_type": "theme", "label": row["labl"], "value": index}
            for index, row in selected_place_themes.iterrows()
        ]
        raise NodeInterrupt(value={
            "message": "Select a theme for the selected place.",
            "options": button_options
        })

    return state


def find_cubes_node(state: lg_State) -> lg_State:
    """
    With a selected g_unit and theme, retrieve available data cubes.
    """
    print(f"State at start of find_cubes_node: {state}")
    selected_place_g_unit = state.get("selected_place_g_unit")
    selected_theme = state.get("selected_theme")
    if not selected_place_g_unit or not selected_theme:
        response_message = AIMessage(
            content="A unit or theme has not been selected."
        )
        state["messages"].append(response_message)
        return state

    selected_theme_df = pd.read_json(selected_theme)
    theme_id = str(selected_theme_df["ent_id"].values[0])

    cubes_df = find_cubes_for_unit_theme({"g_unit": str(selected_place_g_unit), "theme_id": theme_id})

    print("\n=== Available Cubes ===")
    print(f"Unit ID: {selected_place_g_unit}")
    print(f"Theme ID: {theme_id}")
    print(cubes_df.to_string())
    print("====================\n")

    if not cubes_df.empty:
        state["selected_cubes"] = cubes_df.to_json(index=True)
        response_message = AIMessage(
            content="Here are the available data cubes. Opening visualization panel...",
            additional_kwargs={
                "show_visualization": True,
                "cubes": cubes_df.to_dict("records")
            }
        )
    else:
        response_message = AIMessage(content="No cubes found for the selected unit and theme.")

    state["messages"].append(response_message)
    return state


# ----------------------------------------------------------------------------------------
# MAP SELECTION NODES
# ----------------------------------------------------------------------------------------

def check_map_selection_node(state: lg_State) -> lg_State:
    """
    If the user has polygons from the map, skip normal input parsing and go to theme selection.
    """
    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        state["selected_place_g_unit"] = selected_polygons[0]
        msg = f"Map selection detected: using g_unit={selected_polygons[0]}"
        state["messages"].append(AIMessage(content=msg))
    return state


def decide_if_map_selected(state: lg_State) -> str:
    """
    Conditional edge to skip input flow if map polygons are already chosen.
    """
    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        return "get_place_themes_node"
    else:
        return "validate_user_input"


# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------
def create_workflow(lg_state, gdf):
    workflow = StateGraph(lg_state)

    # Nodes
    workflow.add_node("check_map_selection_node", check_map_selection_node)
    workflow.add_node("validate_user_input", validate_user_input)
    workflow.add_node("extract_place_name_node", extract_place_name_node)
    workflow.add_node("postcode_tool_call", postcode_tool_call)
    workflow.add_node("place_tool_call", place_tool_call)
    workflow.add_node("place_tool_handler", place_tool_handler)
    workflow.add_node("handle_user_place_selection", handle_user_place_selection)
    workflow.add_node("get_place_themes_node", get_place_themes_node)
    workflow.add_node("get_place_themes_handler", get_place_themes_handler)
    workflow.add_node("find_cubes_node", find_cubes_node)

    # Edges
    workflow.add_edge(START, "check_map_selection_node")

    # If map selection was made, go to place themes
    # else proceed with normal user input flow
    workflow.add_conditional_edges(
        "check_map_selection_node",
        decide_if_map_selected,
        {
            "get_place_themes_node": "get_place_themes_node",
            "validate_user_input": "validate_user_input",
        }
    )

    # Normal flow
    workflow.add_edge("validate_user_input", "extract_place_name_node")
    workflow.add_conditional_edges(
        "validate_user_input",
        decide_next_node,
        {
            "postcode_tool_call": "postcode_tool_call",
            "place_tool_call": "place_tool_call",
            "extract_place_name_node": "extract_place_name_node",
        }
    )

    workflow.add_edge("extract_place_name_node", "place_tool_call")
    workflow.add_edge("place_tool_call", "place_tool_handler")
    workflow.add_edge("place_tool_handler", "handle_user_place_selection")
    workflow.add_edge("postcode_tool_call", "handle_user_place_selection")

    # After place_g_unit is set, get themes
    workflow.add_edge("handle_user_place_selection", "get_place_themes_node")
    workflow.add_edge("get_place_themes_node", "get_place_themes_handler")
    workflow.add_edge("get_place_themes_handler", "find_cubes_node")
    workflow.add_edge("find_cubes_node", END)

    # Compile
    compiled_workflow = workflow.compile(checkpointer=memory)

    # Save a Mermaid diagram (optional)
    compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
        draw_method=MermaidDrawMethod.API,
    )
    with open("compiled_workflow.png", "wb") as png:
        png.write(compiled_workflow_image)

    return compiled_workflow