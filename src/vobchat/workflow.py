# app/workflow.py

# -------------------------------
# Import standard libraries and type hints
# -------------------------------
from typing import Annotated, Optional, List
import io
import json
import re  # For regular expression operations (e.g., postcode validation)
import pandas as pd  # For data manipulation, primarily with database results
from typing_extensions import TypedDict  # For defining the structure of the workflow state
import logging  # For logging information and debugging
# Import constant definitions for unit types from a local utility module
from vobchat.utils.constants import UNIT_TYPES
# Import the function to get themes dynamically from database
from vobchat.tools import get_all_themes

# -------------------------------
# Import Pydantic for data validation and models
# -------------------------------
# Used to define structured data models, especially for LLM outputs
from pydantic import BaseModel, Field

# -------------------------------
# Import LangChain and LangGraph modules
# -------------------------------
from langgraph.graph import END, StateGraph, START  # Core components for building the graph
from langgraph.graph.message import AnyMessage, add_messages  # For handling messages in the state
from langchain_ollama import ChatOllama  # Ollama LLM integration (used here)
from langchain_core.runnables import RunnableConfig  # For configuring LangChain runnables
from langgraph.checkpoint.memory import MemorySaver  # Basic in-memory checkpointer (not used here)
# Core message types used in LangChain/LangGraph conversations
from langchain_core.messages import SystemMessage, AIMessage, HumanMessage, ToolMessage, AnyMessage
from langchain_core.prompts import ChatPromptTemplate  # For creating prompts for the LLM
from langgraph.types import interrupt, Command  # For interrupting the graph execution and controlling flow
from langchain_core.runnables.graph import MermaidDrawMethod  # For generating graph visualizations

# -------------------------------
# Import local modules (configuration, DB setup, tools, etc.)
# -------------------------------
from vobchat.config import load_config, get_db  # Functions to load app config and get DB connection
from vobchat.tools import (  # Custom functions to interact with the database/data
    find_cubes_for_unit_theme,
    find_units_by_postcode,
    find_themes_for_unit,
    find_places_by_name,
    get_all_themes
)
# Import Redis checkpointer for persistent state saving
from vobchat.utils.redis_checkpoint import AsyncRedisSaver
from vobchat.utils.redis_pool import redis_pool_manager
from vobchat.state_nodes import (
    ShowState_node, ListThemesForSelection_node,
    ListAllThemes_node, Reset_node,
    AddPlace_node, RemovePlace_node,
    AddTheme_node, RemoveTheme_node,
    DescribeTheme_node,
    ask_followup_node
)
from vobchat.agent_routing import agent_node  # Main entry point for user interactions
from vobchat.intent_handling import AssistantIntent  # Enum for routing intents
from vobchat.state_schema import lg_State, get_selected_units, get_selected_unit_types, get_selected_place_names, get_selected_place_ids  # TypedDict for the workflow state

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
# CONFIGURATION & SETUP
# ----------------------------------------------------------------------------------------

logger.info("Loading configuration and initializing components...")

# Load application configuration (e.g., database credentials, API keys)
config = load_config()
# Get a database connection/engine based on the loaded configuration
db = get_db(config)


# Initialize memory saver for checkpointing the workflow state.
# NOTE: Although initialized, the Redis checkpointer is used in compilation later.
logger.debug("Initializing memory saver for checkpointing...")
memory = MemorySaver() # This instance isn't actually used later, AsyncRedisSaver is.

# Initialize the language model (ChatOllama in this case)
# Specifies the model name and the API endpoint for the Ollama service.
logger.info("Initializing language model...")
model = ChatOllama(
    model="deepseek-r1-wt:latest",  # The specific Ollama model to use
    base_url="http://localhost:11434/",  # URL of the Ollama API server
    # default_options={"format": "json"},``
    # base_url="https://148.197.150.162/ollama_api/",  # URL of the Ollama API server
    # client_kwargs={"verify": False}  # Disables SSL verification if needed (use cautiously)
)


# -------------------------------
# Define a regex for UK postcodes
# -------------------------------
# This pattern is used to identify UK postcodes in user input.
logger.debug("Initializing UK postcode regex pattern...")
postcode_regex = (
    r"([Gg][Ii][Rr] 0[Aa]{2})|"  # GIR 0AA
    r"((([A-Za-z][0-9]{1,2})|"  # A9, A99
    r"(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|"  # AA9, AA99
    r"(([A-Za-z][0-9][A-Za-z])|"  # A9A
    r"([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))"  # AA9A, AA9?
    r"))\s?[0-9][A-Za-z]{2})"  # Optional space + 9AA
)

# ----------------------------------------------------------------------------------------
# CHAINS AND PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ----------------------------------------------------------------------------------------

# Define a Pydantic model to structure the information extracted from the user's initial query.
# Ensures the LLM returns data in a predictable format.
class UserQuery(BaseModel):
    # `places`: Mandatory list of place names identified.
    places: List[str] = Field(
        ..., description="A list of place names mentioned in the user query"
    )
    # `counties`: Optional list of corresponding county codes/names.
    counties: Optional[List[str]] = Field(
        default=[], description="A list of county codes corresponding to the places (if any)"
    )
    # `theme`: Optional statistical theme requested.
    theme: Optional[str] = Field(
        default=None,
        description="The statistics theme requested by the user (e.g. population)"
    )
    # `min_year`: Optional start year for data.
    min_year: Optional[int] = Field(
        default=None, description="The start year for the statistics"
    )
    # `max_year`: Optional end year for data.
    max_year: Optional[int] = Field(
        default=None, description="The end year for the statistics"
    )


# Create a prompt template for the LLM to guide the extraction process based on the UserQuery model.
# The extraction prompt instructs the model to extract lists specifically.
initial_query_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert extraction algorithm. Only extract the following variables from the text: "
        "places (as a list of place names), counties (as a list, if mentioned), theme, min_year, and max_year. "
        "Return null or an empty list for any variable that is not mentioned."
    ),
    ("user", "{text}")  # Placeholder for the user's input message
])

# Create a LangChain "chain" that combines the prompt and the LLM.
# `.with_structured_output(UserQuery)` forces the LLM to return a JSON object matching the UserQuery model.
initial_query_chain = initial_query_prompt | model.with_structured_output(
    schema=UserQuery
)

# -------------------------------
# Dynamic theme retrieval with caching
# -------------------------------
_themes_cache = None

def get_themes_dict():
    """Get themes as a dictionary, with caching for performance."""
    global _themes_cache
    if _themes_cache is None:
        _load_themes_from_db()
    return _themes_cache

def _load_themes_from_db():
    """Load themes from database into cache."""
    global _themes_cache
    try:
        themes_json = get_all_themes("")  # Empty string parameter as required by the function
        themes_df = pd.read_json(io.StringIO(themes_json), orient='records')
        _themes_cache = dict(zip(themes_df['ent_id'], themes_df['labl']))
        logging.info(f"Loaded {len(_themes_cache)} themes dynamically from database")
    except Exception as e:
        logging.error(f"Failed to load themes dynamically, using fallback: {e}")
        # Fallback to minimal themes if database fails
        _themes_cache = {
            "T_POP": "Population",
            "T_WK": "Work & Poverty",
            "T_HOUS": "Housing"
        }

def refresh_themes_cache():
    """Force refresh of themes cache from database."""
    global _themes_cache
    _themes_cache = None
    return get_themes_dict()

# -------------------------------
# Define a Pydantic model for theme decision output
# -------------------------------
# Ensures the LLM returns a valid theme code from the available themes.
class ThemeDecision(BaseModel):
    theme_code: str = Field(...,
                            description="The selected theme code from available themes, e.g. T_POP")

def build_theme_prompt():
    """Build the theme selection prompt with current themes."""
    themes = get_themes_dict()
    if not themes:
        themes = {"T_POP": "Population"}  # Emergency fallback
    return ChatPromptTemplate.from_messages([
        (
            "system",
            "You are an expert in selecting the best statistical theme."
        ),
        (
            "system",
            "Available themes:\n" +
            "\n".join(f"{k}: {v}" for k, v in themes.items())
        ),
        (
            "user",
            # single braces → real variable
            "Question: {question}\n"
            # doubled braces → literal { and }
            "Return *only* this JSON (no code fences, no extra text):\n"
            "{{\"theme_code\": \"<one_of_the_codes_above>\"}}"
        )
    ])

# Build the theme chain dynamically when needed
def get_theme_chain():
    """Get the theme selection chain with current themes."""
    return build_theme_prompt() | model.with_structured_output(schema=ThemeDecision)

def postcode_tool_call(state: lg_State) -> lg_State:
    """
    If a postcode was previously extracted (`extracted_postcode` is set), this node calls
    the `find_units_by_postcode` tool to search the database for matching geographical units.
    Updates the state with the search results in the `places` array (single source of truth).
    """
    logger.info("Starting postcode tool call...")
    state["current_node"] = "postcode_tool_call"
    logger.debug({"current_state": state})

    # Get the postcode from the state.
    extracted_postcode = state.get("extracted_postcode")
    if not extracted_postcode:
        # If no postcode is present (shouldn't happen if routed correctly, but good practice to check).
        logger.warning("No valid postcode found in state for postcode_tool_call")
        state["messages"].append(
            AIMessage(
                content="I couldn't find a postcode to search for.",
                response_metadata={"stream_mode": "stream"}
            )
        )
        return state # Return early

    try:
        logger.info(f"Searching for units with postcode: {extracted_postcode}")
        # Call the database tool function with the postcode.
        response_df = find_units_by_postcode(extracted_postcode)
        logger.debug({"search_results_df": response_df})

        # Check if the database query returned any results.
        if not response_df.empty:
            logger.info("Units found for postcode")
            # Update the state with the details from the first found unit.
            # Converts the first row of the DataFrame to JSON for storage.
            state["selected_place"] = response_df.iloc[0:1].to_json(orient="records") # Store first result row as JSON
            # Initialize lists if they don't exist and append the g_unit and g_place IDs.
            existing_units = get_selected_units(state)
            existing_places = get_selected_place_names(state)
            new_unit = int(response_df["g_unit"].values[0])
            new_place = int(response_df["g_place"].values[0])

            # This data should be added through add_place_to_state helper function instead
            # The places array is the single source of truth
        else:
            # If no units were found, inform the user.
            logger.warning(
                f"No units found for postcode: {extracted_postcode}")
            state["messages"].append(
                AIMessage(content=f"Sorry, I couldn't find any data for the postcode '{extracted_postcode}'.")
            )

    except Exception as e:
        # Handle potential errors during the database call.
        logger.error("Error in postcode tool call", exc_info=True)
        state["messages"].append(
            AIMessage(content=f"Sorry, there was an error looking up the postcode: {str(e)}")
        )

    logger.debug({"updated_state": state})
    return state


def multi_place_tool_call(state: lg_State) -> lg_State:
    """
    Build state["places"] = [
        { "name": str,
          "candidate_rows": [ {...DB row...}, … ],
          "g_place": None, "g_unit": None, "g_unit_type": None }
    ]
    The heavy lifting (disambiguation / map prompt) is done by
    resolve_place_and_unit().
    """
    logger.info("multi_place_tool_call – searching DB for each place")
    # Get places from the single source of truth
    places = state.get("places", []) or []

    # Process database lookups for each place in the simplified state
    logger.info(f"multi_place_tool_call: Processing {len(places)} places from state")

    for place in places:
        place_name = place.get("name", "")
        unit_type = place.get("g_unit_type", "0")

        # Only do database lookup if we don't have candidate_rows yet
        if not place.get("candidate_rows"):
            try:
                df = pd.read_json(
                    io.StringIO(
                        find_places_by_name.invoke({
                            "place_name": place_name,
                            "county": "0",
                            "unit_type": unit_type or "0"
                        })
                    ),
                    orient="records",
                )
                place["candidate_rows"] = df.to_dict("records")
                logger.info(f"multi_place_tool_call: Found {len(place['candidate_rows'])} candidates for '{place_name}'")
            except Exception as exc:
                logger.error(f"DB error searching '{place_name}': {exc}", exc_info=True)
                place["candidate_rows"] = []

    # Update state with enriched places data
    state["places"] = places
    state["selection_idx"] = None  # clear any stale click

    logger.info("multi_place_tool_call: Completed place processing using simplified state")
    return state
    if False:  # This code is disabled
        place_name_old = enumerate([])
        county = counties[idx] if idx < len(counties) else "0"
        # CRITICAL: For map clicks, use selection_idx as unit type fallback instead of "0"
        if idx < len(unit_types):
            unit_type = unit_types[idx]
        else:
            # Use selection_idx if available (map clicks), otherwise default to "0"
            selection_idx = state.get("selection_idx")
            unit_type = selection_idx if selection_idx else "0"
        polygon_id = polygon_ids[idx] if idx < len(polygon_ids) else None
        try:
            df = pd.read_json(
                io.StringIO(
                    find_places_by_name.invoke({
                        "place_name": place_name,
                        "county": county,
                        "unit_type": unit_type
                    })
                ),
                orient="records",
            )
            candidate_rows = df.to_dict("records")
        except Exception as exc:
            logger.error(f"DB error searching “{place_name}”: {exc}",
                         exc_info=True)
            candidate_rows = []

        place_unit_type = unit_type if polygon_id else None
        logger.info(f"multi_place_tool_call: Creating place '{place_name}' with g_unit={polygon_id}, g_unit_type='{place_unit_type}' (from unit_types[{idx}]='{unit_type}')")

        places.append({
            "name":            place_name,
            "candidate_rows":  candidate_rows,
            "g_place":         None,
            "unit_rows":       [],        # filled later
            "g_unit":          polygon_id,  # Use polygon_id from map click if available
            "g_unit_type":     place_unit_type,
        })

    state["places"]        = places
    # current_place_index already tracks which place is being processed
    state["selection_idx"] = None       # clear any stale click

    # CRITICAL: Ensure last_intent_payload remains cleared after AddPlace_node processing
    # This prevents the same intent from being processed again if workflow loops back to start_router
    state["last_intent_payload"] = {}

    logger.info("multi_place_tool_call: Cleared selection_idx and ensured last_intent_payload remains cleared")
    return state


    # # Combine all the individual DataFrames into one large DataFrame.
    # if all_results_dfs:
    #     big_df = pd.concat(all_results_dfs, ignore_index=True)
    # else:
    #     # If no results were found for any place, create an empty DataFrame.
    #     logger.warning("No results found for any extracted place names.")
    #     big_df = pd.DataFrame() # Ensure big_df exists even if empty

    # # Store the combined DataFrame as a JSON string in the state.
    # # 'orient="records"' stores it as a list of dictionaries, which is often convenient.
    # state["multi_place_search_df"] = big_df.to_json(orient="records")
    # # Initialize the index for processing these places one by one.
    # # state["current_place_index"] = 0
    # logger.debug(f"Combined place search results stored for {len(place_names)} places.")
    # return state


def update_polygon_selection(state: lg_State) -> lg_State:
    """
    Node that ONLY updates polygon selection state - no interrupts.
    This handles map state updates that are safe to re-execute.
    """
    logger.debug("=== WORKFLOW TRACE: update_polygon_selection function called! ===")
    logger.info("Node: update_polygon_selection entered.")

    # Get the current state
    current_place_index = state.get("current_place_index")
    extracted_place_names = state.get("extracted_place_names", [])
    selected_workflow_units = get_selected_units(state)

    logger.info(f"URGENT DEBUG: update_polygon_selection state - current_place_index={current_place_index}, extracted_places={extracted_place_names}, selected_units={selected_workflow_units}")

    if selected_workflow_units:
        # Get the list of units selected *by the user on the map* (from frontend state).
        selected_map_polygons = state.get("selected_polygons", []) or []
        selected_map_polygons_str = [str(p) for p in selected_map_polygons]

        # Find missing units that need to be highlighted on the map
        missing_units = []
        for i, unit_id in enumerate(selected_workflow_units):
            if str(unit_id) not in selected_map_polygons_str:
                missing_units.append((i, unit_id))
                logger.info(f"URGENT DEBUG: Missing unit {unit_id} at index {i}")

        logger.info(f"URGENT DEBUG: Total missing units: {len(missing_units)}")

        if missing_units:
            # Update workflow selected_polygons to include missing units
            current_polygons = state.get("selected_polygons", []) or []
            for _, missing_unit in missing_units:
                if missing_unit not in current_polygons:
                    current_polygons.append(missing_unit)
                    logger.info(f"URGENT DEBUG: Added unit {missing_unit} to selected_polygons")

            # Update state with new polygon selections
            state["selected_polygons"] = current_polygons

            # CRITICAL: Trigger immediate map update via map_update_request
            state["map_update_request"] = {
                "action": "update_map_selection",
                "places": state.get("places", [])  # Send the single source of truth
            }
            logger.info(f"URGENT DEBUG: Set map_update_request: {state['map_update_request']}")
            logger.info(f"update_polygon_selection: Set map_update_request for units {selected_workflow_units}")

            # Store which units need map highlighting for the next node
            units_list = [unit for _, unit in missing_units]
            state.setdefault("units_needing_map_selection", [])
            state["units_needing_map_selection"] = units_list
            logger.info(f"URGENT DEBUG: Units needing map selection: {units_list}")
        else:
            # No missing units, but still need to ensure map is updated
            state.setdefault("units_needing_map_selection", [])
            state["units_needing_map_selection"] = []
            logger.info(f"URGENT DEBUG: No units need map selection")

            # CRITICAL: Even if no missing units, still send map update request to ensure map reflects current state
            # This is important when polygons are added through other means (e.g., resolve_place_and_unit)
            if selected_workflow_units:
                state["map_update_request"] = {
                    "action": "update_map_selection",
                    "places": state.get("places", [])  # Send the single source of truth
                }
                logger.info(f"URGENT DEBUG: Set map_update_request for existing units: {state['map_update_request']}")
                logger.info(f"update_polygon_selection: Set map_update_request for existing units {selected_workflow_units}")
    else:
        state.setdefault("units_needing_map_selection", [])
        state["units_needing_map_selection"] = []

    return state


def check_map_selection_needed_router(state: lg_State) -> str:
    """
    Router function that decides if user map interaction is needed.
    Returns the next node to execute.
    """
    logger.debug("=== WORKFLOW TRACE: check_map_selection_needed_router function called! ===")

    # Check if there are units that still need to be highlighted / confirmed
    units_needing_map_selection = state.get(
        "units_needing_map_selection", []) or []

    current_place_index = state.get("current_place_index")
    extracted_place_names = state.get("extracted_place_names", [])

    logger.info(
        f"URGENT DEBUG: check_map_selection_needed_router - units_needing_map_selection={units_needing_map_selection}"
    )
    logger.info(
        f"URGENT DEBUG: check_map_selection_needed_router - current_place_index={current_place_index}, extracted_places={extracted_place_names}"
    )

    # ------------------------------------------------------------------
    # 1. If there are units that still need map interaction, route to the
    #    dedicated interrupt node so the frontend can highlight / ask for
    #    confirmation before the workflow proceeds.
    # ------------------------------------------------------------------
    if units_needing_map_selection:
        logger.debug("URGENT DEBUG: Map selection still required – routing to request_map_selection")
        return "request_map_selection"

    # ------------------------------------------------------------------
    # 2. No pending map work – decide whether to process the next place or
    #    move on to theme resolution.
    # ------------------------------------------------------------------
    has_more_places = (
        current_place_index is not None and current_place_index < len(extracted_place_names)
    )

    logger.info(f"URGENT DEBUG: ROUTER LOGIC - current_place_index={current_place_index}, len(extracted_place_names)={len(extracted_place_names)}")
    logger.info(f"URGENT DEBUG: ROUTER LOGIC - has_more_places check: {current_place_index} < {len(extracted_place_names)} = {has_more_places}")

    if has_more_places:
        logger.info(
            f"URGENT DEBUG: No map selection needed and more places remain – continuing to resolve_place_and_unit (will process place {current_place_index})"
        )
        return "resolve_place_and_unit"

    # All places handled, continue with themes / final steps
    logger.debug("URGENT DEBUG: Place processing complete – routing to resolve_theme")
    return "resolve_theme"


def request_map_selection(state: lg_State) -> lg_State | Command:
    """
    Dedicated node for interrupt - ONLY interrupts, no side effects.
    This is where we properly ask for user map interaction.
    """
    logger.debug("=== WORKFLOW TRACE: request_map_selection function called! ===")
    logger.info("Node: request_map_selection entered.")

    # Get the units that need selection
    units_needing_map_selection = state.get("units_needing_map_selection", [])
    current_place_index = state.get("current_place_index")
    extracted_place_names = state.get("extracted_place_names", [])

    if not units_needing_map_selection:
        logger.debug("URGENT DEBUG: No units need selection, returning state unchanged")
        return state

    # Check if this is a multi-place workflow
    is_multi_place = len(extracted_place_names) > 1
    continue_to_next_place = current_place_index is not None and current_place_index < len(extracted_place_names)

    # Get the first unit that needs selection (for single-unit selection)
    target_unit = units_needing_map_selection[0]
    target_index = current_place_index - 1 if current_place_index is not None and current_place_index > 0 else 0
    place_name = extracted_place_names[target_index] if target_index < len(extracted_place_names) else "the area"

    logger.info(f"URGENT DEBUG: Requesting map selection for unit {target_unit} ({place_name})")
    logger.info(f"URGENT DEBUG: Multi-place workflow: {is_multi_place}, continue_to_next_place: {continue_to_next_place}")

    # CRITICAL: For multi-place workflows, trigger SSE update and continue without interrupting
    if is_multi_place and continue_to_next_place:
        logger.info(f"URGENT DEBUG: Multi-place workflow - triggering SSE update and continuing to next place")

        # Set map_update_request to trigger SSE update for ALL selected units, not just the target
        all_selected_units = get_selected_units(state)
        state["map_update_request"] = {
            "action": "update_map_selection",
            "places": state.get("places", [])  # Send the single source of truth
        }

        # Add AI message for user feedback
        message = f"Highlighting {place_name} on the map."
        if "messages" not in state:
            state["messages"] = []
        state["messages"].append(AIMessage(content=message))

        logger.info(f"URGENT DEBUG: Multi-place - continuing to resolve_place_and_unit via Command")
        # Clear the processed unit from units_needing_map_selection
        remaining_units = [
            unit for unit in units_needing_map_selection if unit != target_unit]
        logger.info(f"URGENT DEBUG: Removing processed unit {target_unit}, remaining units: {remaining_units}")

        # Continue to next place processing via Command
        return Command(
            goto="resolve_place_and_unit",
            update={
                "map_update_request": state["map_update_request"],
                "messages": state["messages"],
                "units_needing_map_selection": remaining_units
            }
        )
    else:
        logger.info(f"URGENT DEBUG: Single place or last place - creating standard map selection interrupt")
        return Command(
            goto="resolve_theme",
            update={
                "places": state.get("places", []),  # Pass the single source of truth
                "current_place_index": current_place_index,
                "extracted_place_names": extracted_place_names,
                "current_node": "request_map_selection",
                "selection_idx": None,
                "units_needing_map_selection": [],
            }
        )


def select_unit_on_map(state: lg_State) -> lg_State | Command:
    """
    Node intended to trigger map interaction in the frontend.
    It checks if units have been selected for the *most recently processed* place
    (using `current_place_index - 1` because the index was just incremented).
    If the unit hasn't already been added to the map's `selected_polygons` list (which is
    updated by the frontend), it issues an `interrupt`.
    This interrupt signals the frontend (`chat.py`) to:
    1. Potentially highlight or add the corresponding unit polygon(s) to the map.
    2. Wait for the user to potentially click/select polygons on the map.
    3. The frontend map callback updates `map-state` (specifically `selected_polygons`),
       which then triggers the `retrigger_chat_callback` in `chat.py`.
    4. `retrigger_chat_callback` triggers the main `update_chat` callback again, which
       resumes the LangGraph workflow, potentially entering the `decide_if_map_selected` router.

    Args:
        state (lg_State): The current workflow state.

    Returns:
        lg_State: The potentially updated state (though this node primarily interrupts).
    """
    logger.debug("=== WORKFLOW TRACE: select_unit_on_map function called! (LEGACY) ===")
    logger.info("Node: select_unit_on_map entered.")
    state["current_node"] = "select_unit_on_map"

    # DEBUG: Log key state variables
    current_place_index = state.get("current_place_index")
    extracted_place_names = state.get("extracted_place_names", [])
    selected_workflow_units = get_selected_units(state)
    logger.info(f"URGENT DEBUG: select_unit_on_map state - current_place_index={current_place_index}, extracted_places={extracted_place_names}, selected_units={selected_workflow_units}")

    last_intent = state.get("last_intent_payload")
    if last_intent:
        logger.info(f"URGENT DEBUG: select_unit_on_map has last_intent: {last_intent}")
        if last_intent.get("intent") == "AddPlace" or last_intent.get("intent") == "RemovePlace":
            # Hand control back to the normal router so e.g. AddPlace_node runs
            logger.info(f"URGENT DEBUG: select_unit_on_map returning to agent_node for intent: {last_intent}")
            logging.info(f"resolve_theme: last_intent_payload set to {last_intent}, returning to agent_node.")
            return Command(goto="agent_node")
    else:
        logger.debug("URGENT DEBUG: select_unit_on_map no last_intent")
    # Get the list of units selected so far by the workflow (place/unit selection nodes).
    selected_workflow_units = get_selected_units(state)
    # Get the list of units selected *by the user on the map* (from frontend state).
    selected_map_polygons_str = [str(p) for p in state.get("selected_polygons", [])] # Ensure string comparison

    # Check if there are any workflow-selected units that need to be added to the map
    if selected_workflow_units:
        # CRITICAL: Check if this is a map-originated workflow (map click)
        # If the last intent was AddPlace with polygon_id, the place was already selected on the map
        last_intent = state.get("last_intent_payload", {})
        is_map_click = (
            last_intent.get("intent") == "AddPlace" and
            last_intent.get("arguments", {}).get("polygon_id") is not None
        )

        # Also check if we have polygon_ids in extracted data (indicates map click origin)
        polygon_ids = state.get("extracted_polygon_ids", [])
        has_polygon_ids = any(pid is not None for pid in polygon_ids)

        if is_map_click or has_polygon_ids:
            logger.info(f"select_unit_on_map: Map click detected (is_map_click={is_map_click}, has_polygon_ids={has_polygon_ids}), skipping map interrupt")
            # For map clicks, assume the units are already selected on the map and continue
            # No need to interrupt for map selection since user already clicked the map
        else:
            # CRITICAL: For button-based unit type changes, send interrupt to update map state immediately
            # This ensures the frontend gets updated unit types and selected units
            current_place_index = state.get("current_place_index")
            extracted_place_names = state.get("extracted_place_names", [])


            if current_place_index is None:
                logger.info(f"URGENT DEBUG: select_unit_on_map unit type selection - current_place_index={current_place_index}, extracted_places={state.get('extracted_place_names', [])}")
                logger.info("select_unit_on_map: Unit type selection detected, issuing interrupt to update map state immediately")

                # CRITICAL FIX: Only send the current place's unit for map update, not all units
                # This prevents fetching multiple polygons simultaneously during multi-place workflows
                selected_units = get_selected_units(state)
                selected_unit_types = get_selected_unit_types(state)

                # Calculate which unit corresponds to the current place being processed
                if current_place_index is not None and current_place_index > 0 and current_place_index <= len(selected_units):
                    # Send only the current place's unit (most recently added)
                    current_unit = [selected_units[current_place_index - 1]]
                    current_unit_type = [selected_unit_types[current_place_index - 1]] if current_place_index - 1 < len(selected_unit_types) else []
                    logger.info(f"URGENT DEBUG: select_unit_on_map sending only current place unit: {current_unit} (index {current_place_index - 1})")
                else:
                    # Fallback: send all units (existing behavior)
                    current_unit = selected_units
                    current_unit_type = selected_unit_types
                    logger.info(f"URGENT DEBUG: select_unit_on_map fallback - sending all units: {current_unit}")

                interrupt(value={
                    # Only send the single source of truth - places array
                    "places": state.get("places", []),
                    "current_place_index": current_place_index,
                    "current_node": "select_unit_on_map",
                    "selection_idx": None,
                    # Map update info for frontend
                    "map_update_request": {
                        "action": "update_map_selection",
                        "units": current_unit,
                        "unit_types": current_unit_type
                    }
                })
                # WORKFLOW PAUSES HERE - interrupt() returns from the function

            # Find all units that are in the workflow but not yet on the map
            logger.info(f"URGENT DEBUG: select_unit_on_map checking missing units - workflow_units={selected_workflow_units}, map_polygons={selected_map_polygons_str}")
            missing_units = []
            for i, unit_id in enumerate(selected_workflow_units):
                if str(unit_id) not in selected_map_polygons_str:
                    missing_units.append((i, unit_id))
                    logger.info(f"URGENT DEBUG: Missing unit {unit_id} at index {i}")

            logger.info(f"URGENT DEBUG: Total missing units: {len(missing_units)}")

            # CRITICAL FIX: Handle missing units first, then check for more places
            # This ensures each place gets proper map selection before moving to the next
            extracted_place_names = state.get("extracted_place_names", [])

            if missing_units:
                # CRITICAL: For multi-place workflows, we need to select places on the map sequentially
                # Find the missing unit that corresponds to the most recently processed place
                # This ensures Portsmouth gets selected, then Southampton, etc.

                # Prioritize the missing unit that corresponds to the current place being processed
                target_missing_unit = None
                target_missing_index = None

                if current_place_index is not None and current_place_index > 0:
                    # Look for the missing unit that corresponds to the most recently processed place
                    recent_place_index = current_place_index - 1
                    if recent_place_index < len(missing_units):
                        for missing_index, missing_unit in missing_units:
                            if missing_index == recent_place_index:
                                target_missing_unit = missing_unit
                                target_missing_index = missing_index
                                break

                # If we didn't find the recent place's unit, take the first missing unit
                if target_missing_unit is None and missing_units:
                    target_missing_index, target_missing_unit = missing_units[0]

                if target_missing_unit is not None:
                    logger.info(f"URGENT DEBUG: Processing map selection for unit {target_missing_unit} (index {target_missing_index})")
                    logger.info(f"Unit {target_missing_unit} (index {target_missing_index}) not found in map selections. Updating map and continuing workflow.")

                    # CRITICAL FIX: Update workflow's selected_polygons to match the unit being selected
                    # This ensures the workflow knows the unit is selected and can continue processing
                    current_selected_polygons = state.get("selected_polygons", []) or []
                    target_missing_str = str(target_missing_unit)
                    current_polygons_str = [str(p) for p in current_selected_polygons]

                    if target_missing_str not in current_polygons_str:
                        updated_polygons = current_selected_polygons + [target_missing_unit]
                        state["selected_polygons"] = updated_polygons
                        logger.info(f"URGENT DEBUG: Updated workflow selected_polygons from {current_selected_polygons} to {updated_polygons}")
                    else:
                        logger.info(f"URGENT DEBUG: Unit {target_missing_unit} already in selected_polygons: {current_selected_polygons}")

                    # CRITICAL FIX: Use interrupt to ensure proper sequencing
                    # Portsmouth must be fully processed (including map selection) before Southampton starts
                    selected_unit_types = get_selected_unit_types(state)
                    missing_unit_type = [selected_unit_types[target_missing_index]] if target_missing_index is not None and target_missing_index < len(selected_unit_types) else []

                    logger.info(f"URGENT DEBUG: Using interrupt to ensure Portsmouth is selected before Southampton processing")

                    # CRITICAL FIX: Check if there are more places to process
                    extracted_place_names = state.get("extracted_place_names", [])
                    continue_to_next_place = current_place_index is not None and current_place_index < len(extracted_place_names)
                    if continue_to_next_place:
                        logger.info(f"URGENT DEBUG: More places to process ({current_place_index} of {len(extracted_place_names)}), will continue to next place after this interrupt")

                    # CRITICAL FIX: For multi-place workflows, don't interrupt - just continue processing
                    if continue_to_next_place:
                        logger.info(f"URGENT DEBUG: Skipping interrupt for multi-place workflow - continuing directly to next place")
                        # Update the state to ensure the polygon is marked as selected
                        # This ensures the frontend shows the polygon as selected even without user interaction
                        # Update the workflow state directly and trigger map update
                        current_polygons = state.get("selected_polygons", []) or []
                        if target_missing_unit not in current_polygons:
                            updated_polygons = current_polygons + [target_missing_unit]
                        else:
                            updated_polygons = current_polygons

                        # Update state directly
                        state["selected_polygons"] = updated_polygons

                        # CRITICAL: Use the map_update_request mechanism to trigger map highlighting
                        # Send ALL selected units, not just the current one being processed
                        all_selected_units = get_selected_units(state)
                        state["map_update_request"] = {
                            "action": "update_map_selection",
                            "places": state.get("places", [])  # Send the single source of truth
                        }

                        logger.info(f"URGENT DEBUG: Updating selected_polygons to include {target_missing_unit} and setting map_update_request")
                        logger.info(f"URGENT DEBUG: map_update_request = {state['map_update_request']}")

                        # Use Command with update to ensure map_update_request is persisted and triggers SSE
                        return Command(
                            goto="resolve_place_and_unit",
                            update={
                                "selected_polygons": state["selected_polygons"],
                                "map_update_request": state["map_update_request"]
                            }
                        )
                    else:
                        logger.info(f"URGENT DEBUG: Single place or last place - creating interrupt for map selection")
                        interrupt(value={
                            # Only send the single source of truth - places array
                            "places": state.get("places", []),
                            "current_place_index": current_place_index,
                            "current_node": "select_unit_on_map",
                            "selection_idx": None,
                            "message": f"Please select {extracted_place_names[target_missing_index] if target_missing_index is not None and target_missing_index < len(extracted_place_names) else 'the area'} on the map to continue.",
                            # Map update info for frontend to highlight the missing unit
                            "map_update_request": {
                                "action": "highlight_missing",
                                "missing_unit": target_missing_unit
                            }
                        })

                    # Return to pause workflow and wait for map selection to complete
                    return state

            # CRITICAL FIX: Only after handling missing units, check if there are more places to process
            if current_place_index is not None and current_place_index < len(extracted_place_names):
                logger.info(f"URGENT DEBUG: More places to process ({current_place_index} of {len(extracted_place_names)}), continuing to next place")
                logger.info(f"select_unit_on_map: More places to process ({current_place_index} of {len(extracted_place_names)}), continuing to next place")
                return Command(goto="resolve_place_and_unit", update=state)

            # CRITICAL FIX: If all places are processed and no missing units, exit cleanly
            if current_place_index is not None and current_place_index >= len(extracted_place_names):
                logger.info(f"URGENT DEBUG: All places processed ({current_place_index} >= {len(extracted_place_names)}), exiting select_unit_on_map cleanly")
                logger.info(f"select_unit_on_map: All places processed ({current_place_index} >= {len(extracted_place_names)}), proceeding to conditional routing")
                return state

    # Only proceed with routing if no interrupt was issued (i.e., all units are on map or no units exist)
    logger.info(f"All workflow units are already selected on map or no units exist. Proceeding with routing.")
    logger.info(f"select_unit_on_map: About to exit and use conditional routing. Current state: current_place_index={state.get('current_place_index')}, extracted_place_names={state.get('extracted_place_names')}")

    # Let the conditional edges handle routing - just return state
    return state


def find_cubes_node(state: lg_State) -> lg_State | Command:
    """
    Retrieves the data‑cubes (statistical datasets) for the **currently selected theme**
    (``state["selected_theme"]``) and every selected geographical unit
    (from the `places` array - single source of truth).

    Key steps
    ----------
    1. Merge the workflow‑selected and map‑selected units.
    2. Parse theme information from ``state['selected_theme']``.
    3. **Reuse already‑fetched cubes** in ``state['selected_cubes']`` where they satisfy the
       current theme + year filters, and **only request cubes that are missing**.
    4. Apply the optional ``min_year`` / ``max_year`` filters.
    5. Combine the cubes, update ``state['selected_cubes']``, and emit an ``interrupt``
       so the front‑end can visualise the data.
    """
    logger.info("Node: find_cubes_node entered.")
    state["current_node"] = "find_cubes_node"
    logger.debug({"current_state": state})

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Early‑exit for NEW AddPlace / RemovePlace intents, but not stale ones
    # ──────────────────────────────────────────────────────────────────────────
    last_intent = state.get("last_intent_payload")
    if last_intent and last_intent.get("intent") in {"AddPlace", "RemovePlace"}:
        # Check if this is a stale intent for polygons already selected
        intent_args = last_intent.get("arguments", {})
        intent_polygon_id = intent_args.get("polygon_id")
        current_selected_units = get_selected_units(state)

        # If this AddPlace intent is for a polygon already selected, it's stale - clear it and continue
        if (last_intent.get("intent") == "AddPlace" and
            intent_polygon_id and intent_polygon_id in current_selected_units):
            logging.info(
                f"find_cubes_node: Clearing stale AddPlace intent for already-selected polygon {intent_polygon_id}"
            )
            state["last_intent_payload"] = {}
            # Continue processing cubes since this was a stale intent
        else:
            # This is a fresh intent for a new/different polygon - route to agent_node
            logging.info(
                "find_cubes_node: last_intent_payload set to %s, returning to agent_node.",
                last_intent,
            )
            return Command(goto="agent_node")

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Collect the full list of selected geographical‑unit IDs
    # ──────────────────────────────────────────────────────────────────────────
    # Use simplified state schema - get units from places array
    workflow_units: list[int] = get_selected_units(state)

    # CRITICAL: Always use workflow units as authoritative source for find_cubes_node
    # This node is called after place/unit resolution is complete, so workflow_units
    # contains the definitive selection state including any removals from the single source of truth
    all_selected_unit_ids: list[int] = sorted(set(workflow_units))
    logger.info(f"find_cubes_node: Using workflow units as authoritative: {all_selected_unit_ids}")

    if not all_selected_unit_ids:
        logger.warning("No units selected to find cubes for.")
        state["messages"].append(AIMessage(content="No areas selected to fetch data for."))
        return state

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Parse the selected theme information
    # ──────────────────────────────────────────────────────────────────────────
    selected_theme_json: str | None = state.get("selected_theme")
    if not selected_theme_json:
        logger.warning("No theme selected to find cubes for.")
        state["messages"].append(AIMessage(content="Please select a theme first."))
        return state

    try:
        selected_theme_series = pd.read_json(io.StringIO(selected_theme_json), typ="series")
        if selected_theme_series.empty or "ent_id" not in selected_theme_series.index:
            raise ValueError("Selected theme data is invalid or missing 'ent_id'.")
        theme_id: str = selected_theme_series["ent_id"]
        theme_label: str = selected_theme_series["labl"]  # friendly name for the UI
    except (ValueError, KeyError) as err:
        logger.error("Error parsing selected theme JSON: %s", err, exc_info=True)
        state["messages"].append(
            AIMessage(content="Error reading the selected theme information.")
        )
        return state

    # Optional year filters
    min_year: int | None = state.get("min_year")
    max_year: int | None = state.get("max_year")

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Determine which units (if any) still need data
    # ──────────────────────────────────────────────────────────────────────────
    existing_cubes_json: str | None = state.get("selected_cubes")
    existing_cubes_df = pd.DataFrame()
    missing_unit_ids: list[int] = list(all_selected_unit_ids)  # start by assuming all missing

    if existing_cubes_json:
        try:
            existing_cubes_df = pd.read_json(
                io.StringIO(existing_cubes_json), orient="records", dtype=False
            )
            # The stored cubes may include other themes or incomplete year ranges.
            # Keep only rows matching the current theme.
            if "g_unit" in existing_cubes_df.columns:
                existing_cubes_df = existing_cubes_df[existing_cubes_df["Theme_ID"] == theme_id]
            else:
                existing_cubes_df = pd.DataFrame()  # Structure is unexpected – treat as empty
        except ValueError:
            # Bad JSON ⇒ ignore
            logger.warning("selected_cubes contained invalid JSON – ignoring it.")
            existing_cubes_df = pd.DataFrame()

        # Apply the same year filtering logic to the existing data so the coverage test is fair.
        def _apply_year_filter(df: pd.DataFrame) -> pd.DataFrame:
            if "Start" not in df.columns or "End" not in df.columns:
                return df  # Cannot filter without year columns – assume okay
            df = df.copy()
            df["Start"] = pd.to_numeric(df["Start"], errors="coerce")
            df["End"] = pd.to_numeric(df["End"], errors="coerce")
            if min_year is not None:
                df = df[df["End"] >= min_year]
            if max_year is not None:
                df = df[df["Start"] <= max_year]
            return df

        filtered_existing_df = _apply_year_filter(existing_cubes_df)

        # For each selected unit, check if we have *any* rows after filtering.
        missing_unit_ids = [
            u
            for u in all_selected_unit_ids
            if filtered_existing_df.empty
            or filtered_existing_df[filtered_existing_df["g_unit"] == u].empty
        ]

    logger.info(
        "Units requiring a fresh fetch: %s (out of %s)",
        missing_unit_ids,
        all_selected_unit_ids,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Fetch cubes for any missing units
    # ──────────────────────────────────────────────────────────────────────────
    newly_fetched_dfs: list[pd.DataFrame] = []
    for g_unit in missing_unit_ids:
        try:
            raw_json = find_cubes_for_unit_theme({"g_unit": str(g_unit), "theme_id": theme_id})
            cubes_df = pd.read_json(io.StringIO(raw_json), orient="records")
            if cubes_df.empty:
                logger.debug("No cubes found for unit %s, theme %s.", g_unit, theme_id)
                continue

            # Year‑filter the newly fetched data
            if "Start" in cubes_df.columns and "End" in cubes_df.columns:
                cubes_df["Start"] = pd.to_numeric(cubes_df["Start"], errors="coerce")
                cubes_df["End"] = pd.to_numeric(cubes_df["End"], errors="coerce")
                if min_year is not None:
                    cubes_df = cubes_df[cubes_df["End"] >= min_year]
                if max_year is not None:
                    cubes_df = cubes_df[cubes_df["Start"] <= max_year]

            if cubes_df.empty:
                logger.debug(
                    "No cubes remained for unit %s after year filtering (%s–%s).",
                    g_unit,
                    min_year,
                    max_year,
                )
                continue

            cubes_df["g_unit"] = g_unit  # tag with the unit ID
            newly_fetched_dfs.append(cubes_df)
            logger.debug(
                "Fetched %d cube rows for unit %s (theme %s).", len(cubes_df), g_unit, theme_id
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error finding cubes for unit %s, theme %s: %s", g_unit, theme_id, exc, exc_info=True
            )
            state["messages"].append(
                AIMessage(content=f"Error fetching data for one of the areas (Unit ID: {g_unit}).")
            )

    # ──────────────────────────────────────────────────────────────────────────
    # 6. Merge existing + newly‑fetched cubes and update state
    # ──────────────────────────────────────────────────────────────────────────
    combined_df_list: list[pd.DataFrame] = []
    if not existing_cubes_df.empty:
        # CRITICAL: Filter existing cubes to only include currently selected units
        # This prevents cube data from previously removed units from persisting
        existing_cubes_filtered = existing_cubes_df[existing_cubes_df["g_unit"].isin(all_selected_unit_ids)]
        if not existing_cubes_filtered.empty:
            combined_df_list.append(existing_cubes_filtered)
            logger.info(
                "Filtered existing cubes: %d rows (from %d) for currently selected units %s",
                len(existing_cubes_filtered),
                len(existing_cubes_df),
                all_selected_unit_ids,
            )
        else:
            logger.info(
                "No existing cubes match currently selected units %s (had %d rows for other units)",
                all_selected_unit_ids,
                len(existing_cubes_df),
            )
    combined_df_list.extend(newly_fetched_dfs)

    if not combined_df_list:
        logger.warning(
            "No cube data found for theme '%s' and selected units %s (Years: %s–%s).",
            theme_label,
            all_selected_unit_ids,
            min_year,
            max_year,
        )
        state["messages"].append(
            AIMessage(
                content=f"Sorry, I couldn't find any data matching '{theme_label}' for the specified criteria and selected area(s)."
            )
        )
        return state

    big_cubes_df = pd.concat(combined_df_list, ignore_index=True).drop_duplicates()

    # Before saving back to state, re‑apply year filter *one more time* to ensure consistency.
    if "Start" in big_cubes_df.columns and "End" in big_cubes_df.columns:
        big_cubes_df["Start"] = pd.to_numeric(big_cubes_df["Start"], errors="coerce")
        big_cubes_df["End"] = pd.to_numeric(big_cubes_df["End"], errors="coerce")
        if min_year is not None:
            big_cubes_df = big_cubes_df[big_cubes_df["End"] >= min_year]
        if max_year is not None:
            big_cubes_df = big_cubes_df[big_cubes_df["Start"] <= max_year]

    # Persist the up‑to‑date cubes so future invocations can reuse them
    state["selected_cubes"] = big_cubes_df.to_json(orient="records")

    # CRITICAL: Clear ALL remaining intents since we've reached the final data stage
    original_queue_size = len(state.get("intent_queue", []))
    if original_queue_size > 0:
        state["intent_queue"] = []
        logger.info(f"find_cubes_node: Cleared {original_queue_size} remaining intents from queue (workflow complete)")

    logger.info(
        "Combined %d cube rows across %d units (theme %s).",
        len(big_cubes_df),
        len(all_selected_unit_ids),
        theme_id,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # 7. Notify the front‑end via interrupt
    # ──────────────────────────────────────────────────────────────────────────
    logger.info(f"Emitting cube data interrupt with {len(big_cubes_df)} rows of data")
    # CRITICAL: Ensure map is updated with all selected units from simplified state
    all_selected_units = get_selected_units(state)
    all_selected_unit_types = get_selected_unit_types(state)
    state["map_update_request"] = {
        "action": "update_map_selection",
        "places": state.get("places", [])  # Send the single source of truth
    }

    # CRITICAL: Clear last_intent_payload in the actual state to prevent duplicate operations
    state["last_intent_payload"] = {}
    logger.info("find_cubes_node: Cleared last_intent_payload to prevent duplicate operations")

    interrupt(
        value={
            "message": f"Here is the data for '{theme_label}' across the selected area(s):",
            "cubes": state["selected_cubes"],
            "cube_data": state["selected_cubes"],
            "current_node": "find_cubes_node",
            "last_intent_payload": {},
            # CRITICAL: Clear selection_idx through interrupt to prevent stale values
            "selection_idx": None,
            # CRITICAL: Include places array - the single source of truth
            "places": state.get("places", []),
            # CRITICAL: Include map_update_request to ensure frontend updates map selection
            "map_update_request": state["map_update_request"],
        }
    )

    # The graph pauses after the interrupt; return state for completeness
    return state


def resolve_place_and_unit(state: lg_State) -> lg_State | Command:
    """
    Resolve exactly *one* place per call:
        • disambiguate place name   (may interrupt)
        • disambiguate unit type    (may interrupt)
        • write g_place / g_unit / g_unit_type
    It never mutates state *before* raising an interrupt.
    """
    logger.debug("=== WORKFLOW TRACE: resolve_place_and_unit function called! ===")
    logger.info(f"URGENT DEBUG: FUNCTION ENTRY - current_place_index={state.get('current_place_index')}, selection_idx={state.get('selection_idx')}")
    logger.info(f"URGENT DEBUG: FUNCTION ENTRY - extracted_place_names={state.get('extracted_place_names', [])}")
    logger.info(f"URGENT DEBUG: FUNCTION ENTRY - selected_units_from_places={get_selected_units(state)}")
    logger.info("Node: resolve_place_and_unit entered.")
    i       = state.get("current_place_index", 0) or 0
    places  = state.get("places", []) or []

    # Log current state for debugging
    current_selection_idx = state.get("selection_idx")
    logger.info(f"resolve_place_and_unit: Processing place {i}, current selection_idx={current_selection_idx}")

    # CRITICAL DEBUG: Log full state keys to understand what we're receiving
    logger.info(f"CRITICAL DEBUG: resolve_place_and_unit state keys: {list(state.keys())}")
    logger.info(f"CRITICAL DEBUG: resolve_place_and_unit selection_idx: {state.get('selection_idx')}")
    logger.info(f"CRITICAL DEBUG: resolve_place_and_unit current_place_index: {state.get('current_place_index')}")

    # SIMPLIFIED: Don't clear numeric selection_idx in resolve_place_and_unit
    # Let the normal selection logic handle it - if it's invalid, it will fall back to defaults
    # The interrupt mechanism already clears selection_idx when new prompts are issued
    logger.info(f"resolve_place_and_unit: selection_idx={current_selection_idx} - letting normal selection logic handle it")

    # done?
    if not places or i >= len(places):
        # All places processed, let conditional edges handle routing
        logger.info(f"resolve_place_and_unit: All places processed, clearing selection_idx and returning state for routing")
        # CRITICAL: Clear selection_idx when all places are processed to prevent stale values in theme processing
        state["selection_idx"] = None
        return state

    place   = places[i].copy()         # work on a private copy

    # CRITICAL DEBUG: Log the current place resolution status
    logger.info(f"URGENT DEBUG: place {i} '{place['name']}': g_place={place.get('g_place')}, g_unit={place.get('g_unit')}, selection_idx={current_selection_idx}")
    logger.info(f"resolve_place_and_unit: Processing place {i} '{place['name']}': g_place={place.get('g_place')}, g_unit={place.get('g_unit')}, selection_idx={current_selection_idx}")

    # If this place is already fully resolved, advance to next place
    if place.get("g_place") is not None and place.get("g_unit") is not None:
        logger.info(f"resolve_place_and_unit: Place {i} ({place['name']}) already resolved, skipping")
        old_index = state.get("current_place_index", 0)
        new_index = i + 1
        state["current_place_index"] = new_index
        logger.info(f"resolve_place_and_unit: UPDATED current_place_index from {old_index} to {new_index}")
        # CRITICAL: Clear selection_idx when skipping resolved places to prevent stale values
        state["selection_idx"] = None
        # Return state to let conditional edges handle routing - this preserves state
        return state

    # ───────────────────────────────────────── place disambiguation
    if place["g_place"] is None:
        # CRITICAL: Check if this place already has g_unit info from map click FIRST
        # This ensures map clicks take priority over database search results
        if place.get("g_unit") is not None:
            logger.info(f"Place '{place['name']}' has g_unit {place['g_unit']} from map click, using it directly")
            # This place already has unit info from map click
            # No need to update derived fields - the data is already in the places array
            # The places array is the single source of truth
            selected_units = get_selected_units(state)
            if place["g_unit"] in selected_units:
                logger.info(f"Unit {place['g_unit']} already exists in places array")
            else:
                logger.info(f"Place '{place['name']}' with g_unit {place['g_unit']} from map click ready to use")

            # CRITICAL: Also add to selected_polygons to keep lists in sync and prevent duplicates
            selected_polygons = state.get("selected_polygons", [])
            if place["g_unit"] not in selected_polygons:
                selected_polygons.append(place["g_unit"])
                state["selected_polygons"] = selected_polygons
                logger.info(f"resolve_place_and_unit: Added map-clicked unit {place['g_unit']} to selected_polygons")

            places[i] = place
            state["current_place_index"] = i + 1
            logger.info(f"Added map-clicked g_unit {place['g_unit']} to places array")
            return state

        # If no map click polygon, proceed with normal place disambiguation
        rows = place["candidate_rows"]

        # Handle case where no place candidates were found
        if not rows:
            logger.warning(f"No candidate rows found for place '{place['name']}'")
            # No place candidates and no existing unit info, skip this place
            logger.warning(f"Skipping place '{place['name']}' - no candidates and no unit info")
            state["current_place_index"] = i + 1
            return state

        multiple_options = len(rows) > 1
        sel_idx = state.get("selection_idx")      # refresh in case callback set it

        if multiple_options and sel_idx is None:
            options = [
                {
                    "option_type": "place",
                    "label": f"{r['g_name']}, {r['county_name']}",
                    "color": "#333",
                    "value": j,
                }
                for j, r in enumerate(rows)
            ]

            # CRITICAL: Add coordinate data for map visualization of place disambiguation
            place_coordinates = []
            for j, r in enumerate(rows):
                # Use lat/lon directly from PostGIS geometry if available
                if r.get('lat') is not None and r.get('lon') is not None:
                    try:
                        lat = float(r['lat'])
                        lon = float(r['lon'])

                        # Validate coordinates
                        import math
                        if math.isnan(lat) or math.isnan(lon):
                            logger.warning(f"NaN coordinates from database for {r['g_name']}: lat={lat}, lon={lon}")
                            continue

                        if math.isinf(lat) or math.isinf(lon):
                            logger.warning(f"Infinite coordinates from database for {r['g_name']}: lat={lat}, lon={lon}")
                            continue

                        # Validate UK geographic bounds
                        if not (49 <= lat <= 61 and -8 <= lon <= 2):
                            logger.warning(f"Coordinates outside UK bounds for {r['g_name']}: lat={lat}, lon={lon}")
                            continue

                        logger.info(f"Using database geometry coordinates for {r['g_name']}: lat={lat}, lon={lon}")
                        place_coordinates.append({
                            "index": j,
                            "name": r['g_name'],
                            "county": r['county_name'],
                            "lat": lat,
                            "lon": lon,
                            "g_place": r['g_place']
                        })

                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error processing geometry coordinates for {r['g_name']}: {e}")
                        continue

                else:
                    logger.info(f"Skipping {r['g_name']} - no geometry coordinates in database")

            interrupt(value={
                "message": f"More than one “{place['name']}”. Please choose:",
                "options": options,
                "place_coordinates": place_coordinates,  # Add coordinates for map display
                "current_node": "resolve_place_and_unit",
                "current_place_index": i,
                # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                "selection_idx": None,
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        if multiple_options and sel_idx is not None:
            # For place disambiguation, sel_idx should be an index
            try:
                choice = int(sel_idx)
                chosen_row = rows[choice]
            except ValueError:
                # If sel_idx is not a number (e.g., it's a unit type like "LG_DIST"),
                # and we have multiple place options, this means the sel_idx is stale
                # from a previous interaction (unit selection) but we need place selection
                logger.info(f"resolve_place_and_unit: sel_idx '{sel_idx}' is not numeric for place selection, clearing and triggering place disambiguation for '{place['name']}'")
                state["selection_idx"] = None
                # Trigger place disambiguation since we have multiple options
                options = [
                    {
                        "option_type": "place",
                        "label": f"{r['g_name']}, {r['county_name']}",
                        "color": "#333",
                        "value": j,
                    }
                    for j, r in enumerate(rows)
                ]
                interrupt(value={
                    "message": f"More than one \"{place['name']}\". Please choose:",
                    "options": options,
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": i,
                    # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                    "selection_idx": None,
                    # CRITICAL: Include places array - the single source of truth
                    "places": state.get("places", []),
                })
                return state
        elif multiple_options and sel_idx is None:
            # This is the normal case - show place disambiguation options
            # (This case is already handled above in the earlier if statement)
            pass
        else:
            # Single option or no selection needed - use first option
            logger.info(f"resolve_place_and_unit: Auto-selecting single place option for '{place['name']}': {rows[0]['g_name']}")
            chosen_row = rows[0]

        # commit local
        place["g_place"] = chosen_row["g_place"]
        # explode units into a list[{g_unit, g_unit_type}]
        g_units      = chosen_row["g_unit"]
        g_unit_types = chosen_row["g_unit_type"]
        if not isinstance(g_units, list):
            g_units, g_unit_types = [g_units], [g_unit_types]
        place["unit_rows"] = [
            {"g_unit": u, "g_unit_type": ut}
            for u, ut in zip(g_units, g_unit_types)
        ]
        # CRITICAL FIX: For multi-place workflows, each place should get its own unit type selection
        # Clear selection_idx for places after the first to allow independent selection
        current_selection = state.get("selection_idx")
        extracted_place_names = state.get("extracted_place_names", [])
        is_multi_place = len(extracted_place_names) > 1

        # For multi-place workflows, each place should get its own selection prompt.
        # Clear any unit-type selections for places after the first, regardless of
        # whether the place has units or not. This ensures each place gets its own choice.

        if (is_multi_place and i > 0 and current_selection is not None and
            str(current_selection) in [
            "LG_DIST",
            "MOD_DIST",
            "CONSTITUENCY",
            "WARD",
            "PARL_CONST",
            "MOD_REG",
            "COUNTY",
        ]):
            # Look at the current interrupt options to see if this place should be prompted
            current_options = state.get("options", [])
            has_unit_options = any(opt.get("option_type") == "unit" for opt in current_options)

            if not has_unit_options:
                # This place hasn't been prompted yet, clear inherited selection so it gets its own prompt
                logger.info(
                    f"URGENT DEBUG: Clearing inherited unit-type selection {current_selection} for place {i} ({place['name']}) to ensure independent choice"
                )
                state["selection_idx"] = None
                current_selection = None
            else:
                # This place has been prompted and user made a selection - preserve it
                logger.info(
                    f"URGENT DEBUG: Preserving user selection {current_selection} for place {i} ({place['name']}) (place was prompted)"
                )

        elif current_selection is not None and str(current_selection) in ["LG_DIST", "MOD_DIST", "CONSTITUENCY", "WARD", "PARL_CONST", "MOD_REG", "COUNTY"]:
            # Single place or first place in multi-place: preserve unit type selection
            logger.info(f"URGENT DEBUG: Preserving unit type selection: {current_selection}")
        else:
            # This was a place selection (numeric) or stale value, clear it
            logger.info(f"URGENT DEBUG: Clearing place selection: {current_selection}")
            state["selection_idx"] = None   # consume the click or stale selection

    # ───────────────────────────────────────── unit disambiguation

    logger.info(f"URGENT DEBUG: About to enter unit disambiguation - selection_idx={state.get('selection_idx')}")
    if place["g_unit"] is None:
        logger.info(f"resolve_place_and_unit: ENTERING unit disambiguation for '{place['name']}'")
        urows = place["unit_rows"]
        multiple_options = len(urows) > 1
        sel_idx = state.get("selection_idx")      # refresh in case callback set it
        logger.info(f"resolve_place_and_unit: Unit selection for '{place['name']}': {len(urows)} unit options, sel_idx='{sel_idx}'")

        # Always let users choose unit type - no automatic selection based on patterns

        # CRITICAL: For multi-place workflows, clear inherited selections if this is a subsequent place
        extracted_place_names = state.get("extracted_place_names", [])
        is_multi_place = len(extracted_place_names) > 1

        # Check if this place already has a resolved unit (indicating this is a re-run after selection)
        selected_units = get_selected_units(state)
        current_place_has_unit = len(selected_units) > i and selected_units[i] is not None
        current_place_index = state.get("current_place_index")
        logger.info(f"URGENT DEBUG: Unit disambiguation debug - place {i} ({place['name']}), selected_units={selected_units}, current_place_has_unit={current_place_has_unit}, sel_idx={sel_idx}, current_place_index={current_place_index}")

        # For multi-place workflows, apply the same logic as above - ensure each place gets prompted.
        # Only preserve selections if this place has already been prompted (has unit options).
        if (is_multi_place and i > 0 and sel_idx is not None and
            str(sel_idx) in ["LG_DIST", "MOD_DIST", "CONSTITUENCY", "WARD", "PARL_CONST", "MOD_REG", "COUNTY"]):

            current_options = state.get("options", [])
            has_unit_options = any(opt.get("option_type") == "unit" for opt in current_options)

            if not has_unit_options:
                # This place hasn't been prompted yet, clear inherited selection
                logger.info(
                    f"URGENT DEBUG: Multi-place unit disambiguation – clearing inherited selection {sel_idx} for place {i} ({place['name']}) (not yet prompted)"
                )
                sel_idx = None
                state["selection_idx"] = None
            else:
                # This place has been prompted and user responded - preserve selection
                logger.info(
                    f"URGENT DEBUG: Multi-place unit disambiguation – preserving user selection {sel_idx} for place {i} ({place['name']}) (place was prompted)"
                )

        if multiple_options and sel_idx is None:
            options = [
                {
                    "option_type": "unit",
                    "label": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("long_name", r["g_unit_type"]),
                    "color": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("color", "#333"),
                    "value": r["g_unit_type"],
                }
                for j, r in enumerate(urows)
            ]

            interrupt(value={
                "message": f"Which geography for “{place['name']}”?",
                "options": options,               #  persisted in state
                "current_node": "resolve_place_and_unit",
                # Preserve current_place_index through interrupt
                "current_place_index": state.get("current_place_index", 0),
                # CRITICAL: Preserve selection_idx through interrupt so button clicks are retained
                "selection_idx": state.get("selection_idx"),
                # CRITICAL: Include places array - the single source of truth
                "places": state.get("places", []),
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        if sel_idx is not None:
            logger.info(f"resolve_place_and_unit: sel_idx={sel_idx}, available unit types: {[r['g_unit_type'] for r in urows]}")
            # Find the unit row that matches the selected unit type
            chosen_unit = next((r for r in urows if r["g_unit_type"] == sel_idx), urows[0])
            logger.info(f"resolve_place_and_unit: chosen_unit for '{place['name']}': {chosen_unit}")
        else:
            chosen_unit = urows[0]
            logger.info(f"resolve_place_and_unit: No sel_idx, using first unit for '{place['name']}': {chosen_unit}")
        place["g_unit"]      = chosen_unit["g_unit"]
        place["g_unit_type"] = chosen_unit["g_unit_type"]
        # friendly confirmation
        long_name = UNIT_TYPES.get(place["g_unit_type"], {}) \
                            .get("long_name", place["g_unit_type"])
        state.setdefault("messages", []).append(
            AIMessage(content=f"Using {long_name} data for “{place['name']}”.")
        )

    # ───────────────────────────────────────── commit + advance
    places[i] = place
    state["places"]               = places
    state["current_place_index"]         = i + 1

    # CRITICAL DEBUG: Log the completion of place resolution
    logger.info(f"resolve_place_and_unit: COMPLETED place {i} '{place['name']}': g_place={place.get('g_place')}, g_unit={place.get('g_unit')}, g_unit_type={place.get('g_unit_type')}")
    logger.info(f"resolve_place_and_unit: ADVANCING current_place_index from {i} to {i + 1}")

    # CRITICAL: Clear selection state for next place and ensure it's persisted via Command
    logger.info(f"resolve_place_and_unit: About to clear selection_idx for next place. Current state: selection_idx={state.get('selection_idx')}, current_place_index={state.get('current_place_index')}")

    # Data is already stored in the places array - no need for derived fields
    # The places array is the single source of truth

    # CRITICAL: Use Command to ensure selection_idx clearing is persisted to checkpointer
    # But preserve the selection type for select_unit_on_map to detect button clicks
    last_selection = state.get("selection_idx")


    state_update = {
        "places": places,
        "current_place_index": i + 1,
        "selection_idx": None,  # CRITICAL: Clear for next place
        "options": [],  # Clear consumed options
    }

    logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit returning Command - place {i} completed! ===")
    logger.info(f"resolve_place_and_unit: Completed place {i} ({place['name']}), advancing to place {i + 1}, CLEARED selection_idx via Command update")

    # Use Command with update to ensure state changes are persisted properly
    # Let the conditional router handle the routing to update_polygon_selection
    logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit returning Command with update (router will decide next node) ===")
    logger.info(f"URGENT DEBUG: COMMAND UPDATE STATE - current_place_index advancing from {i} to {i + 1}")
    logger.info(f"URGENT DEBUG: COMMAND UPDATE STATE - selection_idx being cleared: {state_update.get('selection_idx')}")
    logger.info(f"URGENT DEBUG: COMMAND UPDATE STATE - places completed so far: {len(get_selected_units(state))}")
    return state_update


def _theme_already_matches(current_theme_json: str, query: str) -> bool:
    """Check if the extracted theme query already matches the current selected theme."""
    if not current_theme_json or not query:
        return False

    try:
        import json
        theme_data = json.loads(current_theme_json)
        theme_label = theme_data.get("labl", "").lower()
        query_lower = query.lower().strip()

        # Check if the query matches the current theme label
        return query_lower in theme_label or theme_label in query_lower
    except (json.JSONDecodeError, KeyError):
        return False


def resolve_theme(state: lg_State) -> lg_State | Command:
    logger.debug("=== URGENT DEBUG: resolve_theme FUNCTION ENTRY ===")
    logger.info("=== RESOLVE_THEME FUNCTION ENTRY ===")
    logger.info(
        "VOBCHAT DEBUG: resolve_theme: selection_idx=%s current_node=%s options type=%s options len=%s keys=%s",
        state.get("selection_idx"),
        state.get("current_node"),
        type(state.get("options")).__name__,
        len(state.get("options") or []),
        list(state.keys()),
    )
    logger.info(f"VOBCHAT RAW STATE: {state}")
    """Choose a theme and, if no units are known yet, prompt for a place."""
    # ------------------------------------------------------------------
    # Step 0 · CRITICAL: Ensure all places are processed before theme processing
    # ------------------------------------------------------------------
    logging.info("Node: resolve_theme entered.")
    logger.info(f"=== URGENT DEBUG: resolve_theme - about to check place processing ===")

    # Check if there are still places being processed
    current_place_index = state.get("current_place_index", 0) or 0
    # CRITICAL FIX: Use places array (single source of truth) instead of extracted_place_names
    # extracted_place_names only contains text-extracted places, not polygon-clicked places
    places = state.get("places", []) or []
    num_places = len(places)

    logger.info(f"=== URGENT DEBUG: resolve_theme - current_place_index={current_place_index}, num_places={num_places} ===")

    if current_place_index < num_places:
        logger.info(f"=== URGENT DEBUG: resolve_theme - EARLY RETURN - places still processing ===")
        logging.info(f"resolve_theme: Places still being processed ({current_place_index} of {num_places}), returning state to use conditional routing")
        return state

    logger.info(f"=== URGENT DEBUG: resolve_theme - continuing past place check ===")
    logging.info(f"resolve_theme: Call details - selected_theme={bool(state.get('selected_theme'))}, extracted_theme='{state.get('extracted_theme')}', units={len(get_selected_units(state))}")
    # Don't set current_node here - only set it when we actually interrupt for theme selection

    logger.info(f"=== URGENT DEBUG: resolve_theme - about to check intent queue ===")
    # Check intent queue for AddTheme intents and process them
    intent_queue = state.get("intent_queue", [])
    logger.info(f"=== URGENT DEBUG: resolve_theme - intent_queue length: {len(intent_queue) if intent_queue else 'None'} ===")
    if intent_queue:
        theme_intents = [intent for intent in intent_queue if intent.get("intent") == "AddTheme"]
        if theme_intents:
            # Process the first AddTheme intent
            theme_intent = theme_intents[0]
            theme_query = theme_intent.get("arguments", {}).get("theme_query")
            if theme_query:
                logging.info(f"resolve_theme: Processing AddTheme intent from queue: '{theme_query}'")
                # Set extracted_theme so the normal processing logic handles it
                state["extracted_theme"] = theme_query
                # Remove the processed intent from queue
                remaining_queue = [intent for intent in intent_queue if not (intent.get("intent") == "AddTheme" and intent.get("arguments", {}).get("theme_query") == theme_query)]
                state["intent_queue"] = remaining_queue
                logging.info(f"resolve_theme: Removed AddTheme intent from queue, {len(remaining_queue)} intents remaining")

    logger.info(f"=== URGENT DEBUG: resolve_theme - about to get units ===")
    selected_polygons = state.get("selected_polygons", []) or []
    logger.info(f"=== URGENT DEBUG: resolve_theme - selected_polygons: {selected_polygons} ===")
    # Use only places array as the single source of truth
    # Adding selected_polygons creates duplicates since they contain the same data
    units = get_selected_units(state)
    logger.info(f"=== URGENT DEBUG: resolve_theme - units: {units} ===")

    # ------------------------------------------------------------------
    # Step 1 · Build the ‹available› theme list
    #          → if *no* units yet, fall back to the catalogue
    # ------------------------------------------------------------------
    logger.info(f"=== URGENT DEBUG: resolve_theme - about to build theme list for units: {units} ===")
    if units:
        logger.info(f"=== URGENT DEBUG: resolve_theme - calling find_themes_for_unit for {len(set(units))} unique units ===")
        dfs = []
        for u in set(units):
            logger.info(f"=== URGENT DEBUG: resolve_theme - querying themes for unit {u} ===")
            try:
                df = pd.read_json(io.StringIO(find_themes_for_unit(str(u))), orient="records")
                dfs.append(df)
                logger.info(f"=== URGENT DEBUG: resolve_theme - got {len(df)} themes for unit {u} ===")
            except Exception as e:
                logger.info(f"=== URGENT DEBUG: resolve_theme - ERROR querying unit {u}: {e} ===")
        available_df = pd.concat(dfs).drop_duplicates("ent_id") if dfs else pd.DataFrame()
        logger.info(f"=== URGENT DEBUG: resolve_theme - combined themes: {len(available_df)} ===")
    else:
        logger.info(f"=== URGENT DEBUG: resolve_theme - no units, getting all themes ===")
        available_df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")

    if available_df.empty:
        logger.info(f"=== URGENT DEBUG: resolve_theme - NO THEMES FOUND - returning ===")
        state.setdefault("messages", []).append(
            AIMessage(content="I couldn't find any statistical themes.")
        )
        return state

    available = available_df[["ent_id", "labl"]].to_dict("records")
    logger.info(f"=== URGENT DEBUG: resolve_theme - available themes: {len(available)} ===")

    # ------------------------------------------------------------------
    # Step 2 · Has a theme been fixed already? (or is a new theme being requested?)
    # ------------------------------------------------------------------
    logger.info(f"=== URGENT DEBUG: resolve_theme - about to check theme processing ===")
    logger.info(f"resolve_theme: selected_theme={state.get('selected_theme')}, extracted_theme={state.get('extracted_theme')}")

    # ------------------------------------------------------------------
    # Step 2 · Has a theme been fixed already? (or is a new theme being requested?)
    # ------------------------------------------------------------------
    logger.info(f"=== URGENT DEBUG: resolve_theme - Step 2 started ===")
    logger.info(f"resolve_theme: selected_theme={state.get('selected_theme')}, extracted_theme={state.get('extracted_theme')}")

    # Check if we need to process a theme change
    current_theme = state.get("selected_theme")
    extracted_theme_query = state.get("extracted_theme")
    logger.info(f"=== URGENT DEBUG: resolve_theme - current_theme={bool(current_theme)}, extracted_theme_query={extracted_theme_query} ===")

    # Early return if theme is already resolved and no new theme query
    # BUT: Don't early return if we have a button click to process
    has_button_click = state.get("selection_idx") is not None and state.get("current_node") == "resolve_theme"
    logger.info(f"=== URGENT DEBUG: resolve_theme - has_button_click={has_button_click} (selection_idx={state.get('selection_idx')}, current_node={state.get('current_node')}) ===")

    if current_theme and not extracted_theme_query and not has_button_click:
        logger.info(f"=== URGENT DEBUG: resolve_theme - EARLY RETURN - theme already resolved ===")
        logging.info("resolve_theme: Theme already resolved, no new theme query, no button click. Returning early.")
        return state
    elif has_button_click:
        logger.info(f"=== URGENT DEBUG: resolve_theme - NOT EARLY RETURN - processing button click ===")
        logging.info(f"resolve_theme: NOT returning early - processing button click selection_idx={state.get('selection_idx')}")

    logger.info(f"=== URGENT DEBUG: resolve_theme - past early return check ===")

    # Only process theme change if:
    # 1. No theme is selected yet, OR
    # 2. There's a new theme query that differs from current theme
    should_process_theme_change = (
        not current_theme or
        (extracted_theme_query and not _theme_already_matches(current_theme, extracted_theme_query))
    )

    logger.info(f"=== URGENT DEBUG: resolve_theme - should_process_theme_change={should_process_theme_change} ===")

    logging.info(f"resolve_theme: should_process_theme_change={should_process_theme_change}, current_theme={bool(current_theme)}, extracted_theme_query='{extracted_theme_query}'")
    if current_theme and extracted_theme_query:
        matches = _theme_already_matches(current_theme, extracted_theme_query)
        logging.info(f"resolve_theme: _theme_already_matches returned {matches}")

    # Check for button click first (takes priority over theme query)
    selection_idx = state.get("selection_idx")
    current_node = state.get("current_node")
    has_theme_options = bool(state.get("options")) and current_node == "resolve_theme"

    logger.info(f"=== URGENT DEBUG: resolve_theme - BUTTON CLICK CHECK - selection_idx={selection_idx}, has_theme_options={has_theme_options} ===")
    logger.info(f"resolve_theme: DEBUGGING BUTTON CLICK - selection_idx={selection_idx}, current_node={current_node}, has_theme_options={has_theme_options}")
    logger.info(f"resolve_theme: DEBUGGING BUTTON CLICK - options={state.get('options')}, available_themes_count={len(available)}")
    logger.info(f"resolve_theme: DEBUGGING BUTTON CLICK - current_theme={bool(current_theme)}, extracted_theme_query={extracted_theme_query}")

    # 2 b · Button click (prioritize this over theme query)
    # CRITICAL: Only process selection_idx if we're actually in a theme selection context
    logger.info(f"=== URGENT DEBUG: resolve_theme - about to check button click condition ===")
    if selection_idx is not None and has_theme_options:
        logger.info(f"=== URGENT DEBUG: resolve_theme - PROCESSING BUTTON CLICK ===")
        # Validate that this is a numeric theme selection, not a unit type string
        try:
            theme_index = int(selection_idx)
            if 0 <= theme_index < len(available):
                logger.info(f"resolve_theme: Processing valid theme button click selection_idx={selection_idx}")
                state["selected_theme"] = json.dumps(available[theme_index])
                state["selection_idx"] = None
                # Clear interrupt state and extracted theme
                state.pop("options", None)
                state.pop("current_node", None)
                state.pop("extracted_theme", None)  # Clear the extracted theme to prevent re-matching
                logger.info(f"=== URGENT DEBUG: resolve_theme - THEME SET, CLEARED OPTIONS, RETURNING STATE ===")
                logger.info(f"resolve_theme: Button click processed, theme set to: {available[theme_index]['labl']}")
                return state
            else:
                logger.warning(f"resolve_theme: Invalid theme index {theme_index}, clearing selection_idx")
                state["selection_idx"] = None  # Clear invalid selection
        except (ValueError, TypeError):
            logger.info(f"resolve_theme: selection_idx '{selection_idx}' is not a valid theme index, clearing it")
            state["selection_idx"] = None  # Clear invalid selection
    else:
        logger.info(f"=== URGENT DEBUG: resolve_theme - NOT PROCESSING BUTTON CLICK - selection_idx={selection_idx}, has_theme_options={has_theme_options} ===")
    if selection_idx is not None and not has_theme_options:
        logger.info(f"resolve_theme: Ignoring stale selection_idx={selection_idx} - not in theme selection context (current_node={current_node}, has_options={bool(state.get('options'))})")
        # CRITICAL: Clear stale selection_idx to prevent it from interfering with future operations
        state["selection_idx"] = None

    if should_process_theme_change:
        theme_query = extracted_theme_query or ""
        logger.info(f"resolve_theme: Processing theme change - theme_query='{theme_query}'")

        # 2 a · Simple text matching (fallback when LLM doesn't work)
        if theme_query:
            logger.info(f"resolve_theme: Attempting to match theme_query: '{theme_query}'")
            logger.info(f"resolve_theme: Available themes: {[f'{t['ent_id']}: {t['labl']}' for t in available]}")

            # Simple text matching approach
            query_lower = theme_query.lower().strip()
            chosen = None

            # Direct label matching - check both directions
            for theme in available:
                theme_label_lower = theme["labl"].lower()
                # Check if query contains theme label OR theme label contains query
                if query_lower in theme_label_lower or theme_label_lower in query_lower:
                    chosen = theme
                    logger.info(f"resolve_theme: Found direct match: {theme['labl']}")
                    break

            # If no direct match, try keyword matching
            if not chosen:
                query_words = query_lower.split()
                for theme in available:
                    theme_words = theme["labl"].lower().split()
                    # Check if any query word matches any theme word
                    if any(qword in tword or tword in qword for qword in query_words for tword in theme_words):
                        chosen = theme
                        logger.info(f"resolve_theme: Found keyword match: {theme['labl']}")
                        break

            # If still no match, try LLM-based semantic matching using dynamic theme chain
            if not chosen:
                try:
                    logger.info(f"resolve_theme: No text match found, trying LLM semantic matching for '{theme_query}'")
                    # Use the LLM to semantically match the query to available themes
                    theme_chain = get_theme_chain()
                    theme_decision = theme_chain.invoke({"question": theme_query})

                    # Extract theme_code, handling both dict and object responses
                    if hasattr(theme_decision, 'theme_code'):
                        theme_code = theme_decision.theme_code
                    elif isinstance(theme_decision, dict):
                        theme_code = theme_decision.get('theme_code')
                    else:
                        logger.warning(f"resolve_theme: Unexpected LLM response format: {type(theme_decision)}")
                        theme_code = None

                    # Find the theme with matching ent_id
                    if theme_code:
                        for theme in available:
                            if theme["ent_id"] == theme_code:
                                chosen = theme
                                logger.info(f"resolve_theme: Found LLM semantic match: '{theme_query}' -> '{theme['labl']}' (LLM chose {theme_code})")
                                break

                        if not chosen:
                            logger.warning(f"resolve_theme: LLM chose theme_code '{theme_code}' but it's not in available themes")
                    else:
                        logger.warning(f"resolve_theme: Could not extract theme_code from LLM response")

                except Exception as e:
                    logger.error(f"resolve_theme: Error in LLM semantic matching: {e}", exc_info=True)
                    # Fallback: try simple pattern matching for common cases
                    logger.info(f"resolve_theme: Falling back to pattern matching for '{theme_query}'")
                    query_lower = theme_query.lower()

                    # Define common patterns for theme matching
                    theme_patterns = {
                        "education": "T_LEARN",
                        "learning": "T_LEARN",
                        "language": "T_LEARN",
                        "qualification": "T_LEARN",
                        "school": "T_LEARN",
                        "university": "T_LEARN",
                        "population": "T_POP",
                        "people": "T_POP",
                        "demographic": "T_POP",
                        "housing": "T_HOUS",
                        "home": "T_HOUS",
                        "property": "T_HOUS",
                        "work": "T_WK",
                        "employment": "T_WK",
                        "job": "T_WK",
                        "poverty": "T_WK",
                        "income": "T_WK",
                        "industry": "T_IND",
                        "business": "T_IND",
                        "economy": "T_IND",
                        "social": "T_SOC",
                        "health": "T_VITAL",
                        "death": "T_VITAL",
                        "birth": "T_VITAL",
                        "life": "T_VITAL"
                    }

                    # Check if any pattern matches
                    for pattern, theme_code in theme_patterns.items():
                        if pattern in query_lower:
                            # Find the matching theme
                            for theme in available:
                                if theme["ent_id"] == theme_code:
                                    chosen = theme
                                    logger.info(f"resolve_theme: Found pattern match: '{theme_query}' -> '{theme['labl']}' (pattern: {pattern})")
                                    break
                            if chosen:
                                break

            if chosen:
                state["selected_theme"] = json.dumps(chosen)
                state.setdefault("messages", []).append(
                    AIMessage(content=f"Changed theme to '{chosen['labl']}'")
                )
                # Clear interrupt state and extracted theme
                state.pop("options", None)
                state.pop("current_node", None)
                state.pop("extracted_theme", None)  # Clear the extracted theme to prevent re-matching
                # CRITICAL: Clear selection_idx when theme is automatically matched
                state["selection_idx"] = None
            else:
                # Theme not found - show available themes and clear current selection
                state.setdefault("messages", []).append(
                    AIMessage(content=f"Sorry, I couldn't find a theme matching '{theme_query}'. Let me show you what's available:")
                )
                # Clear the current theme to force theme selection
                state.pop("selected_theme", None)
                # CRITICAL: Clear selection_idx when theme not found to prevent stale values
                state["selection_idx"] = None
                # This will trigger the theme selection UI below

        # 2 c · Need manual choice
        if not state.get("selected_theme"):
            options = [
                {
                    "option_type": "theme",
                    "label": t["labl"],
                    "color": "#333",
                    "value": idx,
                }
                for idx, t in enumerate(available)
            ]

            # CRITICAL: Clear last_intent_payload before interrupt to prevent duplicate workflow execution
            # When workflow resumes after theme selection, it shouldn't re-process the AddPlace intent
            if state.get("last_intent_payload"):
                logger.info("resolve_theme: Clearing last_intent_payload before interrupt to prevent duplicate execution")
                state["last_intent_payload"] = {}

            interrupt(
                value={
                    "message": "Which statistical theme did you have in mind?",
                    "options": options,
                    "current_node": "resolve_theme",
                    # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                    "selection_idx": None,
                }
            )
            return state   # execution pauses here

    # If we reach here, we have a theme - clear extracted_theme to prevent reprocessing
    if state.get("selected_theme") and state.get("extracted_theme"):
        logger.info("resolve_theme: Theme resolved, clearing extracted_theme to prevent reprocessing")
        state.pop("extracted_theme", None)
        state.pop("options", None)
        state.pop("current_node", None)

    # ------------------------------------------------------------------
    # Step 3 · If we now *have* a theme *but* still no units → ask for a place
    # ------------------------------------------------------------------
    if state.get("selected_theme") and not units:
        chosen = pd.read_json(io.StringIO(state["selected_theme"]), typ='series')
        state.setdefault("messages", []).append(
            AIMessage(content=f"Got it – I'll use the **{chosen['labl']}** theme. ")
            )
        # interrupt(
        #     value={
        #         "message": (
        #             f"Got it – I'll use the **{chosen.labl}** theme. "
        #             "Which place or postcode should I fetch it for?"
        #         ),
        #         # "options": [
        #         #     {
        #         #         "option_type": "intent",
        #         #         "label": "Add a place",
        #         #         "value": 0,       # handled by ask_followup_node
        #         #         "color": "#333",
        #         #     }
        #         # ],
        #         "current_node": "resolve_theme",
        #     }
        # )
        state['current_node'] = "resolve_theme"
        state["last_intent_payload"] = {}
        return state                      # wait for user input
    else:
        logger.info("resolve_theme: No theme change needed - already have theme and no new extracted_theme")
    return state



def should_continue_to_themes(state: lg_State) -> str:
    """
    After a place’s geographical unit has been fixed, decide the next step:

    •  If there are still places left to disambiguate  → keep looping.
    •  If every place now has a unit AND a theme is
       already selected                               → jump straight to cubes.
    •  Otherwise                                       → fetch/choose a theme.
    """
    logging.info("Routing: should_continue_to_themes()")
    num_places   = len(state.get("extracted_place_names", []))
    current_index = state.get("current_place_index", 0)
    selected_units = get_selected_units(state)
    units_ready  = len(selected_units) >= num_places > 0
    have_theme   = bool(state.get("selected_theme"))

    logging.info(f"Routing decision: num_places={num_places}, current_index={current_index}, selected_units={len(selected_units)}, have_theme={have_theme}")

    # If no units are selected at all (e.g., after deselection), go to agent_node
    if not selected_units:
        logging.info("Routing to agent_node: no units selected")
        return "agent_node"

    # If no places to process, go to agent_node
    if num_places == 0:
        logging.info("Routing to agent_node: no places to process")
        return "agent_node"

    if current_index >= num_places:
        logging.info("Routing to resolve_theme: all places processed")
        return "resolve_theme"
    else:
        logging.info("Routing to resolve_place_and_unit: more places to process")
        return "resolve_place_and_unit"
# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------


def create_workflow(lg_state: TypedDict):
    """
    Constructs and compiles the LangGraph StateGraph.
    - Defines all the nodes.
    - Defines the edges (transitions) between nodes, including conditional edges based on router functions.
    - Compiles the graph with a persistent checkpointer (AsyncRedisSaver).
    - Optionally generates and saves visual diagrams of the graph (ASCII, PNG).

    Args:
        lg_state (TypedDict): The TypedDict class defining the workflow's state structure (lg_State).

    Returns:
        CompiledStateGraph: The compiled LangGraph workflow instance ready for execution.
    """
    logger.info("Creating workflow graph...")

    # Preload themes from database for immediate availability
    logger.info("Preloading themes from database...")
    themes = get_themes_dict()
    logger.info(f"Preloaded {len(themes)} themes for dynamic usage")
    # Initialize the StateGraph with the defined state structure.
    workflow = StateGraph(lg_state)

    # --- Add Nodes ---
    # Add each node function defined earlier to the graph, associating it with a unique name.
    workflow.add_node("agent_node", agent_node) # General LLM agent
    workflow.add_node("postcode_tool_call", postcode_tool_call) # Handles postcode search
    workflow.add_node("multi_place_tool_call", multi_place_tool_call) # Searches multiple places
    # NEW: Proper LangGraph pattern for map interaction
    workflow.add_node("update_polygon_selection", update_polygon_selection) # Updates map state (no interrupts)
    workflow.add_node("request_map_selection", request_map_selection) # Dedicated interrupt node
    workflow.add_node("select_unit_on_map", select_unit_on_map) # Legacy node (kept for compatibility)
    workflow.add_node("find_cubes_node", find_cubes_node) # Retrieves final data cubes (interrupt)

    workflow.add_node("ShowState_node", ShowState_node)
    workflow.add_node("ListThemesForSelection_node", ListThemesForSelection_node)
    workflow.add_node("ListAllThemes_node", ListAllThemes_node)
    workflow.add_node("Reset_node", Reset_node)
    workflow.add_node("AddPlace_node", AddPlace_node)
    workflow.add_node("RemovePlace_node", RemovePlace_node)
    workflow.add_node("AddTheme_node", AddTheme_node)
    workflow.add_node("RemoveTheme_node", RemoveTheme_node)

    workflow.add_node("DescribeTheme_node", DescribeTheme_node)
    workflow.add_node("ask_followup_node", ask_followup_node)
    workflow.add_node("resolve_place_and_unit", resolve_place_and_unit)

    workflow.add_node("resolve_theme", resolve_theme)

    # agent-edge - single mapping
    # workflow.add_conditional_edges(
    #     "agent_node",
    #     lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
    #     {
    #         **{i.value: f"{i.value}_node"
    #         for i in AssistantIntent
    #         if i is not AssistantIntent.CHAT},
    #         AssistantIntent.CHAT.value: END,
    #         "NO_INTENT": "ask_followup_node",
    #     },
    # )
    workflow.add_conditional_edges(
        "agent_node",
        lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
        {
            **{i.value: f"{i.value}_node"
            for i in AssistantIntent
            if i is not AssistantIntent.CHAT},
            AssistantIntent.CHAT.value: END,
            "NO_INTENT": END,
        },
    )

    for n in [
        "ShowState_node", "ListThemesForSelection_node", "ListAllThemes_node",
        "DescribeTheme_node", "RemoveTheme_node", "Reset_node",
        "AddPlace_node"
    ]:
        workflow.add_edge(n, END)

    # RemovePlace_node routes to agent_node to keep workflow alive for re-selection
    # workflow.add_edge("RemovePlace_node", "agent_node")


    # --- Define Edges (Workflow Logic) ---

    # START routing - check if we should resume from current_node or start fresh
    def start_router(state: lg_State) -> str:
        current_node = state.get("current_node")
        selection_idx = state.get("selection_idx")
        last_intent_payload = state.get("last_intent_payload", {})
        intent = last_intent_payload.get("intent")

        logger.info(f"=== URGENT DEBUG: start_router CALLED - current_node={current_node}, selection_idx={selection_idx}, intent={intent} ===")

        # PRIORITY 1: Check if there's a new user message that needs intent processing
        # This MUST come before checking old intent payloads to avoid stale intent loops
        messages = state.get("messages", [])
        has_new_user_message = False
        if messages and len(messages) > 0:
            last_message = messages[-1]
            # Check if the last message is from a human (user)
            if hasattr(last_message, 'type') and last_message.type == "human":
                has_new_user_message = True
            elif isinstance(last_message, tuple) and len(last_message) >= 2 and last_message[0] == "user":
                has_new_user_message = True

        logger.info(f"=== URGENT DEBUG: start_router message detection - messages_count={len(messages) if messages else 0}, has_new_user_message={has_new_user_message} ===")

        # If there's a new user message, always route to agent_node for intent extraction
        # This takes priority over any stale intent payloads from Redis checkpoints
        if has_new_user_message:
            logger.info(f"=== URGENT DEBUG: start_router ROUTING to agent_node (new user message detected) ===")
            logging.info(f"start_router: New user message detected, routing to agent_node for intent extraction")
            return "agent_node"

        # PRIORITY 2: Check for new intent payload (map clicks, etc.) before resuming from stale nodes
        # This prevents resuming from stale resolve_theme when user reselects a polygon
        if intent in ["AddPlace", "RemovePlace"]:
            logger.info(f"=== URGENT DEBUG: start_router PROCESSING {intent} INTENT - routing to agent_node ===")
            logging.info(f"start_router: Processing {intent} intent, routing to agent_node")
            return "agent_node"

        # PRIORITY 3: If we have a current_node and selection_idx (button click), but no new intent, resume from that node
        if current_node and selection_idx is not None:
            logger.info(f"=== URGENT DEBUG: start_router RESUMING from {current_node} (no new intent) ===")
            logging.info(f"start_router: Resuming from current_node={current_node} with selection_idx={selection_idx}")
            return current_node

        # PRIORITY 4: If we have a current_node but no selection_idx, we're waiting for user input - don't restart
        if current_node:
            logger.info(f"=== URGENT DEBUG: start_router WAITING for user input at {current_node} ===")
            logging.info(f"start_router: Waiting for user input at current_node={current_node}, not restarting workflow")
            return current_node

        # Otherwise start fresh with agent_node
        logger.debug("=== URGENT DEBUG: start_router STARTING FRESH with agent_node ===")
        logging.info("start_router: Starting fresh with agent_node")
        return "agent_node"

    workflow.add_conditional_edges(
        START,
        start_router,
        {
            "agent_node": "agent_node",
            "resolve_theme": "resolve_theme",
            "resolve_place_and_unit": "resolve_place_and_unit",
            "select_unit_on_map": "select_unit_on_map",
            "request_map_selection": "request_map_selection",
            "find_cubes_node": "find_cubes_node",
        }
    )

    workflow.add_edge("multi_place_tool_call", "resolve_place_and_unit")

    # Add conditional edges for resolve_place_and_unit to handle state preservation
    def resolve_place_and_unit_router(state: lg_State) -> str:
        logger.debug("=== WORKFLOW TRACE: resolve_place_and_unit_router CALLED ===")
        logging.info("==================== resolve_place_and_unit_router CALLED ====================")
        current_place_index = state.get("current_place_index", 0)
        extracted_place_names = state.get("extracted_place_names", [])
        num_places = len(extracted_place_names)
        selected_units = get_selected_units(state)
        units_needing_map_selection = state.get("units_needing_map_selection", []) or []

        logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - current_place_index={current_place_index}, num_places={num_places}, selected_units={len(selected_units)} ===")
        logging.info(f"resolve_place_and_unit_router: current_place_index={current_place_index}, num_places={num_places}, selected_units={len(selected_units)}")
        logger.info(f"resolve_place_and_unit_router: units_needing_map_selection={units_needing_map_selection}")

        # CRITICAL: Check if polygon selection is actually needed to prevent infinite loops
        # Only route to update_polygon_selection if there are units that actually need polygon selection
        if units_needing_map_selection:
            logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - routing to update_polygon_selection (units need selection) ===")
            logging.info("resolve_place_and_unit_router: Going to update_polygon_selection for polygon selection")
            return "update_polygon_selection"
        else:
            # No polygon selection needed, go directly to theme resolution
            logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - routing to resolve_theme (no polygon selection needed) ===")
            logging.info("resolve_place_and_unit_router: Going to resolve_theme (polygon selection complete)")
            return "resolve_theme"

    workflow.add_conditional_edges(
        "resolve_place_and_unit",
        resolve_place_and_unit_router,
        {
            "update_polygon_selection": "update_polygon_selection",
            "select_unit_on_map": "select_unit_on_map",  # Keep for backward compatibility
            "resolve_theme": "resolve_theme",
            "agent_node": "agent_node",
        },
    )

    def select_unit_on_map_router(state: lg_State) -> str:
        logger.debug("=== WORKFLOW TRACE: select_unit_on_map_router CALLED (LEGACY) ===")
        logging.info("==================== select_unit_on_map_router CALLED ====================")
        # Check if there are no units selected - this means we should go to agent_node
        selected_workflow_units = get_selected_units(state)
        current_place_index = state.get("current_place_index", 0)
        extracted_place_names = state.get("extracted_place_names", [])
        continue_to_next_place = state.get("continue_to_next_place", False)

        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - selected_workflow_units={selected_workflow_units} ===")
        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - current_place_index={current_place_index}, extracted_place_names={extracted_place_names} ===")
        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - continue_to_next_place={continue_to_next_place} ===")
        logging.info(f"select_unit_on_map_router: selected_workflow_units={selected_workflow_units}")
        logging.info(f"select_unit_on_map_router: current_place_index={current_place_index}, extracted_place_names={extracted_place_names}")
        logging.info(f"select_unit_on_map_router: continue_to_next_place={continue_to_next_place}")

        if not selected_workflow_units:
            logger.debug("=== WORKFLOW TRACE: select_unit_on_map_router returning agent_node (no units) ===")
            logging.info("select_unit_on_map_router: returning agent_node (no units)")
            return "agent_node"

        # CRITICAL: Check if we have a flag to continue to next place after map selection
        # This ensures Portsmouth selection completion triggers Southampton processing
        if continue_to_next_place:
            logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - continue_to_next_place=True, routing to resolve_place_and_unit ===")
            logging.info("select_unit_on_map_router: continue_to_next_place flag set, clearing it and continuing to resolve_place_and_unit")
            # Clear the flag since we're acting on it
            state["continue_to_next_place"] = False
            return "resolve_place_and_unit"

        # CRITICAL: Always prioritize place processing over theme processing
        # Check if there are still places to process first, regardless of intent queue
        num_places = len(extracted_place_names)
        if current_place_index is not None and current_place_index < num_places:
            logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router returning resolve_place_and_unit (place {current_place_index} of {num_places} still needs processing) ===")
            logging.info(f"select_unit_on_map_router: returning resolve_place_and_unit (place {current_place_index} of {num_places} still needs processing)")
            return "resolve_place_and_unit"

        # Only after ALL places are processed, check for theme intents
        intent_queue = state.get("intent_queue", [])
        if intent_queue:
            # Check if the intent is theme-related
            theme_intents = [intent for intent in intent_queue if intent.get("intent") == "AddTheme"]
            if theme_intents:
                logging.info(f"select_unit_on_map_router: All places processed, now handling AddTheme intents {theme_intents}")
                return "resolve_theme"
            else:
                logging.info(f"select_unit_on_map_router: returning agent_node (non-theme intent queue: {intent_queue})")
                return "agent_node"

        # Use normal routing logic for final decision
        result = should_continue_to_themes(state)
        logging.info(f"select_unit_on_map_router: should_continue_to_themes returned {result}")
        return result

    # Add conditional edges for select_unit_on_map to handle retrigger cases
    workflow.add_conditional_edges(
        "select_unit_on_map",
        select_unit_on_map_router,
        {
            "resolve_place_and_unit": "resolve_place_and_unit",
            "resolve_theme": "resolve_theme",
            "agent_node": "agent_node",
        },
    )

    # NEW: Proper LangGraph pattern edges
    # update_polygon_selection uses conditional routing to decide next step
    workflow.add_conditional_edges(
        "update_polygon_selection",
        check_map_selection_needed_router,  # Router function that returns the next node name
        {
            "request_map_selection": "request_map_selection",
            "resolve_place_and_unit": "resolve_place_and_unit",
            "resolve_theme": "resolve_theme",
        },
    )

    # request_map_selection creates an interrupt and should route back to resolve_place_and_unit when resumed
    workflow.add_edge("request_map_selection", "resolve_place_and_unit")

    def addtheme_router(state: lg_State) -> str:
        selected_theme = state.get("selected_theme")
        extracted_theme = state.get("extracted_theme")
        has_theme = bool(selected_theme)
        has_units = bool(get_selected_units(
            state) or state.get("selected_polygons"))

        logging.info(f"addtheme_router: has_theme={has_theme}, has_units={has_units}")
        logging.info(f"addtheme_router: selected_theme='{selected_theme}', extracted_theme='{extracted_theme}'")

        # CRITICAL: If we have extracted_theme, we need to process the theme change regardless of selected_theme
        if extracted_theme:
            logging.info("addtheme_router: returning resolve_theme (need to process extracted theme)")
            return "resolve_theme"
        elif has_theme and has_units:
            logging.info("addtheme_router: returning find_cubes_node (have both theme and units)")
            return "find_cubes_node"
        elif has_theme and not has_units:
            # Check if we're in a situation where places are being processed
            current_place_index = state.get("current_place_index", 0) or 0
            total_places = len(state.get("extracted_place_names", []))

            if current_place_index < total_places:
                logging.info("addtheme_router: returning resolve_place_and_unit (have theme, need to continue place processing)")
                return "resolve_place_and_unit"
            else:
                logging.info("addtheme_router: returning agent_node (have theme, need units)")
                return "agent_node"
        else:
            logging.info("addtheme_router: returning resolve_theme (need theme)")
            return "resolve_theme"

    workflow.add_conditional_edges(
        "AddTheme_node",
        addtheme_router,
        {
            "find_cubes_node": "find_cubes_node",
            "agent_node": "agent_node",
            "resolve_theme": "resolve_theme",
            "resolve_place_and_unit": "resolve_place_and_unit",
        },
    )

    def _have_any_units(s):
        """True if the user has supplied a unit in either slot."""
        from vobchat.state_schema import get_selected_units
        return bool(get_selected_units(s))

    def resolve_theme_router(state: lg_State) -> str:
        has_theme = bool(state.get("selected_theme"))
        has_units = _have_any_units(state)
        has_options = bool(state.get("options"))
        current_node = state.get("current_node")
        selection_idx = state.get("selection_idx")
        extracted_theme = state.get("extracted_theme")
        last_intent_payload = state.get("last_intent_payload", {})

        # Check if places still need processing
        current_place_index = state.get("current_place_index", 0) or 0
        places = state.get("places", []) or []
        num_places = len(places)

        logger.info(f"=== URGENT DEBUG: resolve_theme_router - has_theme={has_theme}, has_units={has_units}, has_options={has_options} ===")
        logging.info(f"resolve_theme_router: has_theme={has_theme}, has_units={has_units}, has_options={has_options}, current_node={current_node}, selection_idx={selection_idx}, extracted_theme={extracted_theme}")
        logging.info(f"resolve_theme_router: current_place_index={current_place_index}, num_places={num_places}")
        logging.info(f"resolve_theme_router: last_intent_payload={last_intent_payload}")

        # CRITICAL: If places still need processing, always go back to resolve_place_and_unit
        if current_place_index < num_places:
            logging.info(f"resolve_theme_router: returning resolve_place_and_unit (places still need processing: {current_place_index} of {num_places})")
            return "resolve_place_and_unit"

        # CRITICAL: RECURSION PREVENTION - If we have a theme and units but no new theme work to do,
        # NEVER route back to resolve_theme as it creates infinite loops
        if has_theme and has_units and not extracted_theme and not (selection_idx is not None and has_options):
            logger.info(f"=== RECURSION PREVENTION: resolve_theme_router - ROUTING TO CUBES (preventing loop) ===")
            logging.info("resolve_theme_router: returning find_cubes_node (recursion prevention - have theme and units, no new theme work)")
            return "find_cubes_node"

        # CRITICAL: If a theme button was clicked but not processed yet, stay in resolve_theme to process it
        # But if we already have a theme set, don't loop back - continue to cubes
        if selection_idx is not None and has_options and current_node == "resolve_theme" and not has_theme:
            logger.info(f"=== URGENT DEBUG: resolve_theme_router - ROUTING BACK TO RESOLVE_THEME - selection_idx={selection_idx}, has_options={has_options}, current_node={current_node} ===")
            logging.info(f"resolve_theme_router: returning resolve_theme (theme button clicked, selection_idx={selection_idx}, has_theme={has_theme})")
            return "resolve_theme"

        # If we have both theme and units, go to cubes
        if has_theme and has_units:
            logger.info(f"=== URGENT DEBUG: resolve_theme_router - ROUTING TO CUBES ===")
            logging.info("resolve_theme_router: returning find_cubes_node (have theme and units)")
            return "find_cubes_node"
        # If we have a theme but no units, go to agent to handle next steps
        elif has_theme and not has_units:
            logging.info("resolve_theme_router: returning agent_node (have theme, need units)")
            return "agent_node"
        # If we're actively waiting for user selection and no selection was made yet, stay in resolve_theme
        elif has_options and current_node == "resolve_theme" and selection_idx is None and not has_theme:
            logging.info("resolve_theme_router: returning resolve_theme (waiting for theme selection)")
            return "resolve_theme"
        # If we have an extracted theme to process but no theme yet selected, stay in resolve_theme
        elif extracted_theme and not has_theme:
            logging.info("resolve_theme_router: returning resolve_theme (processing extracted theme)")
            return "resolve_theme"
        # Otherwise go to agent
        else:
            logging.info("resolve_theme_router: returning agent_node (default)")
            return "agent_node"

    workflow.add_conditional_edges(
        "resolve_theme",
        resolve_theme_router,
        {
            "find_cubes_node": "find_cubes_node",
            "agent_node": "agent_node",
            "resolve_theme": "resolve_theme",
            "resolve_place_and_unit": "resolve_place_and_unit",
        },
    )

    workflow.add_edge("find_cubes_node", "agent_node")

    # workflow.add_edge("ask_followup_node", "agent_node")


    # workflow.add_edge("agent_node", END)


    # --- Compile the workflow ---
    logger.info("Compiling workflow with Redis checkpointer...")
    try:
        # Get asynchronous Redis connection from the pool for the checkpointer.
        # decode_responses=False is required for the checkpointer to work correctly
        conn = redis_pool_manager.get_async_client(decode_responses=False)

        # Initialize the asynchronous Redis checkpointer. This persists the state.
        checkpointer = AsyncRedisSaver(conn=conn)

        # Compile the graph definition with the checkpointer.
        # This creates the runnable workflow instance.
        compiled_workflow = workflow.compile(checkpointer=checkpointer)
        logger.info("Workflow compilation successful.")
    except Exception as e:
        # Catch errors during compilation (e.g., Redis connection issues).
        logger.error("Error compiling workflow", exc_info=True)
        raise # Re-raise the exception to prevent app startup if compilation fails.

    # --- Optionally produce diagrams ---
    # These are useful for visualizing and debugging the workflow structure.
    logger.info("Generating ASCII diagram of the workflow:")
    try:
        # logger.info a text-based representation of the graph to the console/logs.
        logger.info("\n" + compiled_workflow.get_graph().draw_ascii())
    except Exception as e:
        logger.warning("Could not generate ASCII diagram", exc_info=True) # Non-critical error

    logger.info("Attempting to generate Mermaid diagram and save as PNG:")
    try:
        # Generate a Mermaid diagram (requires Mermaid CLI or API access depending on method).
        # `draw_mermaid_png` might require internet access if using MermaidDrawMethod.API.
        compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
             draw_method=MermaidDrawMethod.API, # Or MermaidDrawMethod.PYPPETEER if playwright installed
        )
        # Save the generated image to a file.
        with open("compiled_workflow.png", "wb") as png:
            png.write(compiled_workflow_image)
        logger.info("Successfully saved workflow diagram to compiled_workflow.png")
    except Exception as e:
        # Log errors during diagram generation (e.g., Mermaid service unavailable).
        logger.warning("Could not generate or save Mermaid PNG diagram", exc_info=True) # Non-critical error

    logger.info("Workflow creation and compilation completed.")
    # Return both the compiled workflow and the base graph for fresh compilation
    return compiled_workflow, workflow
