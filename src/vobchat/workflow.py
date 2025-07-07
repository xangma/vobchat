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
# from vobchat.utils.constants import UNIT_TYPES  # Not used in this file anymore
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
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage, AnyMessage
from langchain_core.prompts import ChatPromptTemplate  # For creating prompts for the LLM
# from langgraph.types import interrupt, Command  # Moved to nodes files
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
from vobchat.nodes import (
    ShowState_node, ListThemesForSelection_node,
    ListAllThemes_node, Reset_node,
    AddPlace_node, RemovePlace_node,
    AddTheme_node, RemoveTheme_node,
    DescribeTheme_node,
    ask_followup_node,
    postcode_tool_call,
    multi_place_tool_call,
    find_cubes_node,
    resolve_theme,
    update_polygon_selection,
    check_map_selection_needed_router,
    request_map_selection,
    select_unit_on_map,
    resolve_place_and_unit,
    should_continue_to_themes
)
from vobchat.agent_routing import agent_node  # Main entry point for user interactions
from vobchat.intent_handling import AssistantIntent  # Enum for routing intents
from vobchat.state_schema import lg_State, get_selected_units  # TypedDict for the workflow state

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)

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
# NOTE: This will be created inside create_workflow function where model is defined

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
def get_theme_chain(model):
    """Get the theme selection chain with current themes."""
    return build_theme_prompt() | model.with_structured_output(schema=ThemeDecision)

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
        places = state.get("places", []) or []
        num_places = len(places)
        selected_units = get_selected_units(state)
        units_needing_map_selection = state.get("units_needing_map_selection", []) or []

        logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - current_place_index={current_place_index}, num_places={num_places}, selected_units={len(selected_units)} ===")
        logging.info(f"resolve_place_and_unit_router: current_place_index={current_place_index}, num_places={num_places}, selected_units={len(selected_units)}")
        logger.info(f"resolve_place_and_unit_router: units_needing_map_selection={units_needing_map_selection}")

        # CRITICAL: Check if polygon selection is actually needed to prevent infinite loops
        # Skip update_polygon_selection to avoid field conflicts - go directly to router
        if units_needing_map_selection:
            logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - routing directly to request_map_selection (units need selection) ===")
            logging.info("resolve_place_and_unit_router: Going directly to request_map_selection for polygon selection")
            return "request_map_selection"

        # CRITICAL: Check if there are more places to process BEFORE routing to theme resolution
        # This was the missing logic that caused the workflow to skip remaining places
        if current_place_index is not None and current_place_index < num_places:
            logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - routing to resolve_place_and_unit (place {current_place_index} of {num_places} still needs processing) ===")
            logging.info(f"resolve_place_and_unit_router: More places to process - continuing with place {current_place_index}")
            return "resolve_place_and_unit"
        else:
            # All places processed, go to theme resolution
            logger.info(f"=== WORKFLOW TRACE: resolve_place_and_unit_router - routing to resolve_theme (all places processed) ===")
            logging.info("resolve_place_and_unit_router: All places processed, going to resolve_theme")
            return "resolve_theme"

    workflow.add_conditional_edges(
        "resolve_place_and_unit",
        resolve_place_and_unit_router,
        {
            "update_polygon_selection": "update_polygon_selection",
            "request_map_selection": "request_map_selection",  # Direct route to avoid conflicts
            "select_unit_on_map": "select_unit_on_map",  # Keep for backward compatibility
            "resolve_place_and_unit": "resolve_place_and_unit",  # Added to support looping back for next place
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
        places = state.get("places", []) or []
        continue_to_next_place = state.get("continue_to_next_place", False)

        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - selected_workflow_units={selected_workflow_units} ===")
        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - current_place_index={current_place_index}, total_places={len(places)} ===")
        logger.info(f"=== WORKFLOW TRACE: select_unit_on_map_router - continue_to_next_place={continue_to_next_place} ===")
        logging.info(f"select_unit_on_map_router: selected_workflow_units={selected_workflow_units}")
        logging.info(f"select_unit_on_map_router: current_place_index={current_place_index}, total_places={len(places)}")
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
        num_places = len(places)
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
            total_places = len(state.get("places", []) or [])

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
