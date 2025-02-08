# app/workflow.py
from typing import Annotated, Optional, List
import re
import pandas as pd
from typing_extensions import TypedDict
import logging

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

# Set up logger
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
# CONFIG & SETUP
# ----------------------------------------------------------------------------------------

logger.info("Loading configuration and initializing components...")
config = load_config()
db = get_db(config)

# Get initial polygons (arbitrary example)
logger.debug("Getting initial polygons for MOD_REG...")
initial_gdf = get_polygons_by_type('MOD_REG')

# Memory for checkpointing
logger.debug("Initializing memory saver for checkpointing...")
memory = MemorySaver()

# Model
logger.info("Initializing language model...")
model = ChatOllama(
    model="llama3.3:latest",
    base_url="https://148.197.150.162/ollama/", 
    client_kwargs={"verify": False}
)

# Tools
logger.info("Setting up database toolkit and tools...")
toolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = toolkit.get_tools()

list_tables_tool = next(tool for tool in tools if tool.name == "sql_db_list_tables")
get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

# UK postcode regex
logger.debug("Initializing UK postcode regex pattern...")
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
    selected_polygons: Optional[List[int]]
    interrupt_state: bool

# ----------------------------------------------------------------------------------------
# NODES
# ----------------------------------------------------------------------------------------

def validate_user_input(state: lg_State) -> lg_State:
    """
    Looks at the last user message to check if it contains a valid UK postcode.
    """
    logger.info("Starting user input validation...")
    logger.debug({"current_state": state})
    
    user_input = state["messages"][-1].content
    logger.debug({"user_input": user_input})

    postcode_match = re.search(postcode_regex, user_input)
    if postcode_match:
        logger.info(f"Valid postcode found: {postcode_match.group(0)}")
        state["is_postcode"] = True
        state["extracted_postcode"] = postcode_match.group(0)
    else:
        logger.info("No valid postcode found in input")
        state["is_postcode"] = False
        state["extracted_postcode"] = None

    logger.debug({"updated_state": state})
    return state

def decide_next_node(state: lg_State) -> str:
    """
    Decide whether to handle the input as a postcode, place name, or need extraction.
    """
    logger.info("Deciding next node based on current state...")
    logger.debug({"current_state": state})
    
    if state.get("is_postcode"):
        logger.info("Decision: Processing as postcode")
        return "postcode_tool_call"
    elif state.get("extracted_place_name"):
        logger.info("Decision: Processing as place name")
        return "place_tool_call"
    else:
        logger.info("Decision: Needs place name extraction")
        return "extract_place_name_node"

class Place(BaseModel):
    """Information about a place."""
    name: Optional[str] = Field(default=None, description="The name of the place")

class ExtractedData(BaseModel):
    """Extracted data about places."""
    places: List

logger.info("Initializing place extraction prompt and runnable...")
place_extraction_prompt = ChatPromptTemplate.from_messages([
    (
        "assistant",
        "You are an expert extraction algorithm. "
        "Only extract place names from the text. "
        "If you do not know the name of an attribute asked to extract, return null for the attribute's value."
    ),
    ("user", "{text}"),
])

place_extraction_runnable = place_extraction_prompt | model.with_structured_output(schema=ExtractedData)

def extract_place_name_node(state: lg_State) -> lg_State:
    """
    Use the place_extraction_runnable chain to get a place name from the text.
    """
    logger.info("Starting place name extraction...")
    logger.debug({"current_state": state})
    
    text = state["messages"][-1].content
    logger.debug({"input_text": text})
    
    if type(text) == str:
        text = [text]
        
    try:
        extracted_data = place_extraction_runnable.invoke({"text": text})
        logger.info({"extraction_result": extracted_data})
        
        if extracted_data and extracted_data.places:
            state["extracted_place_name"] = extracted_data.places[0]
            logger.info(f"Successfully extracted place name: {extracted_data.places[0]}")
        else:
            state["extracted_place_name"] = None
            logger.warning("No place names were extracted")
            
    except Exception as e:
        logger.error("Error during place name extraction", exc_info=True)
        state["extracted_place_name"] = None

    logger.debug({"updated_state": state})
    return state

def postcode_tool_call(state: lg_State) -> lg_State:
    """
    If a postcode was extracted, find relevant units by postcode.
    """
    logger.info("Starting postcode tool call...")
    logger.debug({"current_state": state})

    extracted_postcode = state.get("extracted_postcode")
    if not extracted_postcode:
        logger.warning("No valid postcode found in state")
        state["messages"].append(AIMessage(content="No valid postcode was found."))
        return state

    try:
        logger.info(f"Searching for units with postcode: {extracted_postcode}")
        response = find_units_by_postcode(extracted_postcode)
        logger.debug({"search_results": response})

        if not response.empty:
            logger.info("Units found for postcode")
            state["selected_place"] = response.to_json(index=True)
            state["selected_place_g_unit"] = int(response["g_unit"].values[0])
            state["selected_place_g_place"] = int(response["g_place"].values[0])
        else:
            logger.warning(f"No units found for postcode: {extracted_postcode}")
            state["messages"].append(AIMessage(content="No units found for that postcode."))

    except Exception as e:
        logger.error("Error in postcode tool call", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error processing postcode: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

def place_tool_call(state: lg_State) -> lg_State:
    """
    If a place name was extracted, call the DB for matching places.
    """
    logger.info("Starting place tool call...")
    logger.debug({"current_state": state})

    place_name = state["extracted_place_name"]
    logger.info(f"Searching for place: {place_name}")

    try:
        returned_places = find_places_by_name(place_name)
        logger.debug({"place_search_results": returned_places})
        
        state["selected_place"] = returned_places.to_json(index=True)
        logger.info(f"Found {len(returned_places)} matching places")

    except Exception as e:
        logger.error("Error in place tool call", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error searching for place: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

def place_tool_handler(state: lg_State) -> lg_State:
    """
    Handle the place tool results, possibly interrupting for user selection.
    """
    logger.info("Starting place tool handler...")
    logger.debug({"current_state": state})

    try:
        returned_places = pd.read_json(state["selected_place"])
        selection_idx = state.get("selection_idx")
        logger.debug({"selection_index": selection_idx})

        if selection_idx is not None:
            logger.info(f"Processing user selection with index: {selection_idx}")
            returned_places = returned_places.iloc[int(selection_idx)].to_frame().T
            state["selected_place"] = returned_places.to_json(index=True)
            state["selection_idx"] = None

        num_results = len(returned_places)
        logger.info(f"Number of places found: {num_results}")

        if num_results == 1:
            logger.info("Single place found - processing directly")
            state["selected_place_g_place"] = int(returned_places["g_place"].values[0])
            state["messages"].append(AIMessage(content=f"Place found: {returned_places.to_string()}"))
        elif num_results > 1:
            logger.info("Multiple places found - preparing selection options")
            button_options = [
                {
                    "option_type": "place",
                    "label": row["g_name"] + ", " + row["county_name"],
                    "value": index
                }
                for index, row in returned_places[["g_name", "county_name"]].iterrows()
            ]
            logger.debug({"button_options": button_options})
            state['interrupt_state'] = True
            raise NodeInterrupt(value={
                "message": "Multiple places found. Please select one.",
                "options": button_options
            })
        else:
            logger.warning("No places found")
            state["messages"].append(AIMessage(content="No results found for that place."))

    except NodeInterrupt:
        state['interrupt_state'] = True
        logger.info("Raising NodeInterrupt for place selection")
        raise
    except Exception as e:
        logger.error("Error in place tool handler", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error processing places: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

def handle_user_place_selection(state: lg_State) -> lg_State:
    """
    Process the user's place selection and determine available units.
    """
    logger.info("Starting user place selection handler...")
    logger.debug({"current_state": state})

    selected_place = state.get("selected_place")
    if not selected_place:
        logger.warning("No place was selected")
        response_message = AIMessage(content="No place was selected previously.")
        state["messages"].append(response_message)
        return state

    try:
        selected_place_df = pd.read_json(selected_place)
        logger.debug({"selected_place_data": selected_place_df})

        if selected_place_df.empty:
            logger.warning("Selected place DataFrame is empty")
            response_message = AIMessage(content="The selected place DataFrame is empty.")
            state["messages"].append(response_message)
            return state

        # Handle unit selection
        logger.info("Processing unit selection...")
        exploded_df = selected_place_df.explode(["g_unit", "g_unit_type"])
        exploded_df = exploded_df.dropna(subset=["g_unit"]).copy()
        exploded_df["g_unit"] = exploded_df["g_unit"].astype(int)
        logger.debug({"exploded_unit_data": exploded_df})

        # Handle user selection if present
        selection_idx = state.get("selection_idx")
        if selection_idx is not None:
            logger.info(f"Processing user unit selection with index: {selection_idx}")
            chosen_row = exploded_df.iloc[int(selection_idx)]
            state["selected_place_g_unit"] = int(chosen_row["g_unit"])
            state["selected_place_g_unit_type"] = chosen_row["g_unit_type"] or "MOD_DIST"
            state["selection_idx"] = None

            logger.info("Raising NodeInterrupt for map selection")
            state['interrupt_state'] = True

            # Raise interrupt for to show on map before continuing to next node
            raise NodeInterrupt(value={
                "message": "map_selection",
                "g_unit": str(state["selected_place_g_unit"]),
                "g_unit_type": state["selected_place_g_unit_type"]
            })
        
        if not state.get("selected_place_g_unit"):
            if len(exploded_df) == 0:
                logger.warning("No valid g_units found")
                state["messages"].append(AIMessage(content="No valid g_unit was found for the selected place."))
                return state
            elif len(exploded_df) == 1:
                logger.info("Single unit found - processing automatically")
                single_row = exploded_df.iloc[0]
                state["selected_place_g_unit"] = int(single_row["g_unit"])
                state["selected_place_g_unit_type"] = single_row["g_unit_type"] or "MOD_DIST"

                logger.info("Raising NodeInterrupt for map selection")
                state['interrupt_state'] = True

                raise NodeInterrupt(value={
                    "message": "map_selection",
                    "g_unit": str(state["selected_place_g_unit"]),
                    "g_unit_type": state["selected_place_g_unit_type"]
                })
            elif len(exploded_df) > 1:
                logger.info("Multiple units found - preparing selection options")
                button_options = []
                for i, row in exploded_df.iterrows():
                    label = f"{row['g_unit_type']} (ID={row['g_unit']})"
                    button_options.append({
                        "option_type": "unit_selection",
                        "label": label,
                        "value": i
                    })
                logger.debug({"button_options": button_options})
                state['interrupt_state'] = True

                raise NodeInterrupt(value={
                    "message": "Multiple (g_unit, g_unit_type) options found. Please select one.",
                    "options": button_options
                })

        # Process the selection
        response_message = AIMessage(
            content=f"Place selected:\n{selected_place_df[['g_name', 'county_name']].to_string(index=False)}"
        )
        state["messages"].append(response_message)
    
    except NodeInterrupt:
        state['interrupt_state'] = True

        logger.info("Raising NodeInterrupt for selection")
        raise
    except Exception as e:
        logger.error("Error in handle_user_place_selection", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error processing place selection: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

def get_place_themes_node(state: lg_State) -> lg_State:
    """
    Retrieve themes for the selected place.
    """
    logger.info("Starting theme retrieval for selected place...")
    logger.debug({"current_state": state})

    selected_place_g_unit = state.get("selected_place_g_unit")
    if selected_place_g_unit:
        try:
            logger.info(f"Retrieving themes for unit ID: {selected_place_g_unit}")
            selected_place_themes = find_themes_for_unit(str(selected_place_g_unit))
            logger.debug({"retrieved_themes": selected_place_themes})

            state["selected_place_themes"] = selected_place_themes.to_json(index=True)
            
        except Exception as e:
            logger.error("Error retrieving themes", exc_info=True)
            response_message = AIMessage(content=f"Error retrieving themes: {str(e)}")
            state["messages"].append(response_message)
    else:
        logger.warning("No place unit ID found in state")
        response_message = AIMessage(content="The selected place was not found.")
        state["messages"].append(response_message)

    logger.debug({"updated_state": state})
    return state

def get_place_themes_handler(state: lg_State) -> lg_State:
    """
    Process the retrieved themes and handle user selection if needed.
    """
    logger.info("Starting theme handler...")
    logger.debug({"current_state": state})

    try:
        selected_place_themes = pd.read_json(state["selected_place_themes"])
        logger.debug({"theme_data": selected_place_themes})

        if selected_place_themes.empty:
            logger.warning("No themes found for selected place")
            response_message = AIMessage(content="No themes found for the selected place.")
            state["messages"].append(response_message)
            return state

        selection_idx = state.get("selection_idx")
        if selection_idx is not None:
            logger.info(f"Processing theme selection with index: {selection_idx}")
            selected_theme = selected_place_themes.iloc[int(selection_idx)].to_frame().T
            state["selected_theme"] = selected_theme.to_json(index=True)

        if not state.get("selected_theme"):
            logger.info("Preparing theme selection options")
            button_options = [
                {"option_type": "theme", "label": row["labl"], "value": index}
                for index, row in selected_place_themes.iterrows()
            ]
            logger.debug({"button_options": button_options})
            state['interrupt_state'] = True

            raise NodeInterrupt(value={
                "message": "Select a theme for the selected place.",
                "options": button_options
            })

    except NodeInterrupt:
        state['interrupt_state'] = True

        logger.info("Raising NodeInterrupt for theme selection")
        raise
    except Exception as e:
        logger.error("Error in theme handler", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error processing themes: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

def find_cubes_node(state: lg_State) -> lg_State:
    """
    Retrieve available data cubes for the selected unit and theme.
    """
    logger.info("Starting cube retrieval...")
    logger.debug({"current_state": state})

    selected_place_g_unit = state.get("selected_place_g_unit")
    selected_theme = state.get("selected_theme")

    if not selected_place_g_unit or not selected_theme:
        logger.warning("Missing required unit or theme selection")
        response_message = AIMessage(content="A unit or theme has not been selected.")
        state["messages"].append(response_message)
        return state

    try:
        selected_theme_df = pd.read_json(selected_theme)
        theme_id = str(selected_theme_df["ent_id"].values[0])
        logger.info({"search_params": {
            "unit": selected_place_g_unit,
            "theme": theme_id
        }})

        cubes_df = find_cubes_for_unit_theme({
            "g_unit": str(selected_place_g_unit),
            "theme_id": theme_id
        })
        logger.debug({"retrieved_cubes": cubes_df})

        if not cubes_df.empty:
            state["selected_cubes"] = cubes_df.to_json(index=True)
            raise NodeInterrupt(value={
                "message": "Here are the available data cubes. Opening visualization panel...",
                "cubes": cubes_df.to_dict("records")
            })
        else:
            logger.warning("No cubes found for selected unit and theme")
            response_message = AIMessage(content="No cubes found for the selected unit and theme.")

        state["messages"].append(response_message)

    except NodeInterrupt:
        state['interrupt_state'] = True

        logger.info("Raising NodeInterrupt for theme selection")
        raise

    except Exception as e:
        logger.error("Error finding cubes", exc_info=True)
        state["messages"].append(AIMessage(content=f"Error retrieving data cubes: {str(e)}"))

    logger.debug({"updated_state": state})
    return state

# ----------------------------------------------------------------------------------------
# MAP SELECTION NODES
# ----------------------------------------------------------------------------------------

def check_map_selection_node(state: lg_State) -> lg_State:
    """
    Check if user has polygons from the map and skip input parsing if so.
    """
    logger.info("Checking for map selection...")
    logger.debug({"current_state": state})

    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        logger.info({"map_selection": {"g_unit": selected_polygons[0]}})
        state["selected_place_g_unit"] = selected_polygons[0]
        msg = f"Map selection detected: using g_unit={selected_polygons[0]}"
        state["messages"].append(AIMessage(content=msg))
    else:
        logger.info("No map selection found")

    logger.debug({"updated_state": state})
    return state

def decide_if_map_selected(state: lg_State) -> str:
    """
    Determine whether to skip input flow based on map selection.
    """
    logger.info("Deciding flow based on map selection...")
    logger.debug({"current_state": state})

    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        logger.info("Decision: Skip to themes due to map selection")
        return "get_place_themes_node"
    else:
        logger.info("Decision: Proceed with normal input flow")
        return "validate_user_input"

# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------
def create_workflow(lg_state, gdf):
    """
    Create and compile the workflow graph.
    """
    logger.info("Creating workflow graph...")
    workflow = StateGraph(lg_state)

    # Add nodes
    logger.debug({"action": "adding_nodes", "nodes": [
        "check_map_selection_node",
        "validate_user_input",
        "extract_place_name_node",
        "postcode_tool_call",
        "place_tool_call",
        "place_tool_handler",
        "handle_user_place_selection",
        "get_place_themes_node",
        "get_place_themes_handler",
        "find_cubes_node"
    ]})

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

    # Add edges
    logger.debug("Adding edges to workflow...")
    workflow.add_edge(START, "check_map_selection_node")

    # Add conditional edges
    logger.debug({"action": "adding_conditional_edges", "edges": {
        "check_map_selection_node": ["get_place_themes_node", "validate_user_input"],
        "validate_user_input": ["postcode_tool_call", "place_tool_call", "extract_place_name_node"]
    }})

    workflow.add_conditional_edges(
        "check_map_selection_node",
        decide_if_map_selected,
        {
            "get_place_themes_node": "get_place_themes_node",
            "validate_user_input": "validate_user_input",
        }
    )

    workflow.add_conditional_edges(
        "validate_user_input",
        decide_next_node,
        {
            "postcode_tool_call": "postcode_tool_call",
            "place_tool_call": "place_tool_call",
            "extract_place_name_node": "extract_place_name_node",
        }
    )

    # Add remaining edges
    logger.debug({"action": "adding_linear_edges", "edges": [
        ("extract_place_name_node", "place_tool_call"),
        ("place_tool_call", "place_tool_handler"),
        ("place_tool_handler", "handle_user_place_selection"),
        ("postcode_tool_call", "handle_user_place_selection"),
        ("handle_user_place_selection", "get_place_themes_node"),
        ("get_place_themes_node", "get_place_themes_handler"),
        ("get_place_themes_handler", "find_cubes_node"),
        ("find_cubes_node", "END")
    ]})

    workflow.add_edge("extract_place_name_node", "place_tool_call")
    workflow.add_edge("place_tool_call", "place_tool_handler")
    workflow.add_edge("place_tool_handler", "handle_user_place_selection")
    workflow.add_edge("postcode_tool_call", "handle_user_place_selection")
    workflow.add_edge("handle_user_place_selection", "get_place_themes_node")
    workflow.add_edge("get_place_themes_node", "get_place_themes_handler")
    workflow.add_edge("get_place_themes_handler", "find_cubes_node")
    workflow.add_edge("find_cubes_node", END)

    # Compile workflow
    logger.info("Compiling workflow...")
    try:
        compiled_workflow = workflow.compile(checkpointer=memory)
        logger.info("Workflow compilation successful")
    except Exception as e:
        logger.error("Error compiling workflow", exc_info=True)
        raise

    # Save Mermaid diagram
    logger.info("Generating Mermaid diagram...")
    try:
        logger.info(compiled_workflow.get_graph().draw_ascii())
    except Exception as e:
        logger.error("Error generating ascii Mermaid diagram", exc_info=True)
    try:
        compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
            draw_method=MermaidDrawMethod.API,
        )
        with open("compiled_workflow.png", "wb") as png:
            png.write(compiled_workflow_image)
        logger.info("Successfully saved workflow diagram to compiled_workflow.png")
    except Exception as e:
        logger.error("Error generating workflow diagram", exc_info=True)

    logger.info("Workflow creation completed successfully")
    return compiled_workflow