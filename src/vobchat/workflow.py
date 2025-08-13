"""LangGraph workflow definition for VobChat.

This module builds and compiles the LangGraph StateGraph that powers the chat
workflow. It wires together:

- Agent routing for interpreting user intent and delegating to nodes
- Place and unit resolution (via nodes in ``vobchat.nodes``)
- Theme selection and dynamic theme lookup
- Map selection updates and optional interrupts for UI interactions

Key concepts:
- State: typed by ``lg_State`` (see ``vobchat.state_schema``) and persisted using
  a Redis checkpointer when compiled by the app.
- Nodes: pure functions that operate on the state and optionally return a
  ``Command`` to route to the next node or interrupt.
- Start node: a router that decides whether to resume a node, process a
  newly-arrived user message, or continue place/unit resolution.

This file focuses on defining the graph and helpful utilities (e.g., theme
prompt and cache). It does not expose HTTP routes or SSE streaming; see
``app.py`` and ``workflow_sse_adapter.py`` for those concerns.
"""

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
from langgraph.types import Command  # For Command-based routing
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
    ShowState_node, ListThemes_node, Reset_node,
    AddPlace_node, RemovePlace_node, PlaceInfo_node,
    AddTheme_node, RemoveTheme_node,
    DescribeTheme_node,
    ask_followup_node,
    postcode_tool_call,
    multi_place_tool_call,
    find_cubes_node,
    resolve_theme,
    update_polygon_selection,
    # request_map_selection,
    select_unit_on_map,
    resolve_place_and_unit
)
from vobchat.agent_routing import agent_node  # Main entry point for user interactions
from vobchat.conversational_agent import conversational_agent_node  # Conversational agent (LLM-planned)
from vobchat.intent_handling import AssistantIntent  # Enum for routing intents
from vobchat.state_schema import lg_State, get_selected_units  # TypedDict for the workflow state

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
# CHAINS AND PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ----------------------------------------------------------------------------------------

# Define a Pydantic model to structure information extracted from user text.
# Ensures the LLM returns data in a predictable, validated format.
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
# Create a prompt template for the LLM to extract the UserQuery fields.
# The prompt explicitly requests lists for places/counties when present.
initial_query_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert extraction algorithm. Only extract the following variables from the text: "
        "places (as a list of place names), counties (as a list, if mentioned), theme, min_year, and max_year. "
        "Return null or an empty list for any variable that is not mentioned."
    ),
    ("user", "{text}")  # Placeholder for the user's input message
])

# The actual LangChain chain is created inside ``create_workflow`` once the
# model is instantiated. ``with_structured_output(UserQuery)`` ensures a JSON
# object matching the schema.

# -------------------------------
# Dynamic theme retrieval with caching
# -------------------------------
_themes_cache = None  # Lazily populated mapping: theme_code -> label

def get_themes_dict():
    """Return current theme dictionary from cache, loading from DB on first use.

    Returns:
        dict[str, str]: Mapping of theme codes to user-facing labels.
    """
    global _themes_cache
    if _themes_cache is None:
        _load_themes_from_db()
    return _themes_cache

def _load_themes_from_db():
    """Load themes from database into the local cache.

    Falls back to a minimal set if DB access fails so the workflow remains
    functional in degraded environments (e.g., tests without DB).
    """
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
    """Force-refresh the theme cache by reloading from the database."""
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

def build_theme_prompt(themes=None):
    """Build the theme-selection prompt with the current theme catalog.

    Args:
        themes: Optional mapping overriding the cached theme list.

    Returns:
        ChatPromptTemplate: A prompt asking the model to pick a single theme code.
    """
    if themes is None:
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

def get_theme_chain(model, themes=None):
    """Create the theme-selection chain binding the prompt to a model.

    Args:
        model: Chat model implementing ``with_structured_output``.
        themes: Optional mapping overriding the cached theme list.

    Returns:
        Runnable: A runnable that yields ``ThemeDecision``.
    """
    return build_theme_prompt(themes) | model.with_structured_output(schema=ThemeDecision)

# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------
def create_workflow(lg_state: TypedDict):
    """Construct the LangGraph StateGraph and wire node transitions.

    The compiled graph is consumed by the app at startup and executed per
    thread. This function focuses on:
    - Adding all node functions (agent, place/unit resolution, theme, map)
    - Defining edges between nodes, including the start router
    - Preparing any model and helper utilities needed by the nodes

    Args:
        lg_state: The ``TypedDict`` describing the workflow state shape.

    Returns:
        CompiledStateGraph: A compiled workflow ready to run.
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

    # Initialize the language model (ChatOllama) and endpoint settings.
    logger.info("Initializing language model...")
    import os
    ollama_host = os.getenv("OLLAMA_HOST", "localhost")
    ollama_port = os.getenv("OLLAMA_PORT", "11434")
    base_url = f"http://{ollama_host}:{ollama_port}/"

    model = ChatOllama(
        model="deepseek-r1-wt:latest",  # Keep in sync with intent_handling.py
        base_url=base_url,  # URL of the Ollama API server
        # default_options={"format": "json"},
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
    # workflow.add_node("request_map_selection", request_map_selection) # Dedicated interrupt node
    workflow.add_node("select_unit_on_map", select_unit_on_map) # Legacy node (kept for compatibility)
    workflow.add_node("find_cubes_node", find_cubes_node) # Retrieves final data cubes (interrupt)
    # Conversational agent node (behind feature flag)
    workflow.add_node("conversational_agent_node", conversational_agent_node)

    workflow.add_node("ShowState_node", ShowState_node)
    workflow.add_node("ListThemes_node", ListThemes_node)
    workflow.add_node("Reset_node", Reset_node)
    workflow.add_node("AddPlace_node", AddPlace_node)
    workflow.add_node("RemovePlace_node", RemovePlace_node)
    workflow.add_node("PlaceInfo_node", PlaceInfo_node)
    workflow.add_node("AddTheme_node", AddTheme_node)
    workflow.add_node("RemoveTheme_node", RemoveTheme_node)

    workflow.add_node("DescribeTheme_node", DescribeTheme_node)
    workflow.add_node("ask_followup_node", ask_followup_node)
    workflow.add_node("resolve_place_and_unit", resolve_place_and_unit)

    workflow.add_node("resolve_theme", resolve_theme)

    # agent_node handles its own routing with Command, no conditional edges needed

    for n in [
        "ShowState_node", "ListThemes_node",
        "DescribeTheme_node", "RemoveTheme_node", "Reset_node",
        "AddPlace_node", "RemovePlace_node"
    ]:
        workflow.add_edge(n, END)

    # --- Define Edges (Workflow Logic) ---

    # Create a dedicated start router node. It determines whether we should
    # resume a partially-completed node, handle a newly-arrived human message
    # (route to the agent), or continue the place/unit resolution flow.
    def start_node(state: lg_State) -> dict | Command:
        """Lightweight router that chooses the next node based on state.

        Priority order:
        1) If a new human message is present, go to ``agent_node`` for intent
           extraction and routing.
        2) If we received a new Add/RemovePlace intent, route to ``agent_node``
           to process it immediately.
        3) If resuming a node that expects a ``selection_idx`` from the UI,
           continue that node.
        4) If already waiting for user input at a node, stay there.
        5) Otherwise start fresh at ``agent_node``.
        """
        current_node = state.get("current_node")
        selection_idx = state.get("selection_idx")
        last_intent_payload = state.get("last_intent_payload") or {}
        intent = last_intent_payload.get("intent")

        logger.info(f"=== URGENT DEBUG: start_node CALLED - current_node={current_node}, selection_idx={selection_idx}, intent={intent} ===")

        # Frontend may have provided an interrupt message; append and clear it
        interrupt_message = state.get("interrupt_message")
        if interrupt_message:
            from vobchat.nodes.utils import _append_ai
            _append_ai(state, interrupt_message)
            logger.info(f"start_node: Added interrupt message to state: {interrupt_message}")
            # Clear the interrupt_message to prevent re-processing
            state["interrupt_message"] = None

        # Debug: Check if workflow_input was passed in
        if selection_idx is not None:
            logger.info(f"=== URGENT DEBUG: start_node - selection_idx={selection_idx} was passed, likely from button click ===")

        # PRIORITY 1: Check for a new human message that needs intent processing
        # This MUST precede checks of any prior intent payloads to avoid loops
        messages = state.get("messages", [])
        has_new_user_message = False
        if messages and len(messages) > 0:
            last_message = messages[-1]
            # Check if the last message is from a human (user)
            if hasattr(last_message, 'type') and last_message.type == "human":
                has_new_user_message = True
            elif isinstance(last_message, tuple) and len(last_message) >= 2 and last_message[0] == "user":
                has_new_user_message = True

        logger.info(f"=== URGENT DEBUG: start_node message detection - messages_count={len(messages) if messages else 0}, has_new_user_message={has_new_user_message} ===")

        # If there's a new user message, always route to agent_node for intent extraction
        if has_new_user_message:
            import os
            use_conv = os.getenv("CONVERSATIONAL_AGENT", "0").lower() in ("1", "true", "yes")
            target = "conversational_agent_node" if use_conv else "agent_node"
            logger.info(
                f"start_node ROUTING to {target} (new user message detected; CONVERSATIONAL_AGENT={use_conv})"
            )
            return Command(goto=target)

        # PRIORITY 2: Check for new intent payloads (e.g., map clicks) before
        # resuming a stale node. This prevents resuming ``resolve_theme`` when
        # the user reselects a polygon.
        if intent in ["AddPlace", "RemovePlace"]:
            logger.info(f"=== URGENT DEBUG: start_node PROCESSING {intent} INTENT - routing to agent_node ===")
            logging.info(f"start_node: Processing {intent} intent, routing to agent_node")
            return Command(goto="agent_node")

        # PRIORITY 3: If we have a current_node and selection_idx (button click),
        # but no new intent, resume from that node
        if current_node and selection_idx is not None:
            logger.info(f"=== URGENT DEBUG: start_node RESUMING from {current_node} (no new intent) ===")
            logging.info(f"start_node: Resuming from current_node={current_node} with selection_idx={selection_idx}")
            # If we added an interrupt message, include the updated messages and clear interrupt_message
            if interrupt_message:
                return Command(goto=current_node, update={
                    "messages": state.get("messages", []),
                    "interrupt_message": None
                })
            return Command(goto=current_node)

        # PRIORITY 4: If we have a current_node but no selection_idx, we're
        # waiting for user input — do not restart
        if current_node:
            logger.info(f"=== URGENT DEBUG: start_node WAITING for user input at {current_node} ===")
            logging.info(f"start_node: Waiting for user input at current_node={current_node}, not restarting workflow")
            # If we added an interrupt message, include the updated messages and clear interrupt_message
            if interrupt_message:
                return Command(goto=current_node, update={
                    "messages": state.get("messages", []),
                    "interrupt_message": None
                })
            return Command(goto=current_node)

        # Otherwise start fresh with agent_node
        logger.debug("=== URGENT DEBUG: start_node STARTING FRESH with agent_node ===")
        logging.info("start_node: Starting fresh with agent_node")
        return Command(goto="agent_node")

    # Add the start node and connect it to START
    workflow.add_node("start_node", start_node)
    workflow.add_edge(START, "start_node")

    workflow.add_edge("multi_place_tool_call", "resolve_place_and_unit")

    # resolve_place_and_unit handles its own routing with Command, no conditional edges needed

    # select_unit_on_map handles its own routing with Command, no conditional edges needed

    # update_polygon_selection handles its own routing with Command, no conditional edges needed

    # request_map_selection creates an interrupt and should route back to resolve_place_and_unit when resumed
    # workflow.add_edge("request_map_selection", "resolve_place_and_unit")

    # AddTheme_node handles its own routing with Command, no conditional edges needed

    # resolve_theme handles its own routing with Command, no conditional edges needed

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

    # logger.info("Attempting to generate Mermaid diagram and save as PNG:")
    # try:
    #     # Generate a Mermaid diagram (requires Mermaid CLI or API access depending on method).
    #     # `draw_mermaid_png` might require internet access if using MermaidDrawMethod.API.
    #     compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
    #          draw_method=MermaidDrawMethod.API, # Or MermaidDrawMethod.PYPPETEER if playwright installed
    #     )
    #     # Save the generated image to a file.
    #     with open("compiled_workflow.png", "wb") as png:
    #         png.write(compiled_workflow_image)
    #     logger.info("Successfully saved workflow diagram to compiled_workflow.png")
    # except Exception as e:
    #     # Log errors during diagram generation (e.g., Mermaid service unavailable).
    #     logger.warning("Could not generate or save Mermaid PNG diagram", exc_info=True) # Non-critical error

    logger.info("Workflow creation and compilation completed.")
    # Return both the compiled workflow and the base graph for fresh compilation
    return compiled_workflow, workflow
