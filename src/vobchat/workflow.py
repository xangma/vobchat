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
from typing import TypedDict
import json
import logging
import os

# -------------------------------
# Import LangChain and LangGraph modules
# -------------------------------
from langgraph.graph import (
    END,
    StateGraph,
    START,
)  # Core components for building the graph
from langchain_core.messages import HumanMessage  # For detecting human messages
from langgraph.types import Command  # For Command-based routing

# -------------------------------
# Import local modules (configuration, DB setup, tools, etc.)
# -------------------------------
from vobchat.config import load_config  # Functions to load app config if needed
from vobchat.tools import get_all_themes

# Import Redis checkpointer for persistent state saving
from vobchat.utils.redis_checkpoint import AsyncRedisSaver
from vobchat.utils.redis_pool import redis_pool_manager
from vobchat.nodes import (
    ShowState_node,
    ListThemes_node,
    Reset_node,
    AddPlace_node,
    RemovePlace_node,
    PlaceInfo_node,
    AddTheme_node,
    RemoveTheme_node,
    DescribeTheme_node,
    UnitTypeInfo_node,
    DataEntityInfo_node,
    ExplainVisibleData_node,
    ask_followup_node,
    postcode_tool_call,
    multi_place_tool_call,
    find_cubes_node,
    resolve_theme,
    update_polygon_selection,
    select_unit_on_map,
    resolve_place_and_unit,
)
from vobchat.conversational_agent import (
    conversational_agent_node,
    _intent_to_node,
)  # Conversational agent (LLM-planned) and intent→node mapper
from vobchat.state_schema import lg_State  # TypedDict for the workflow state

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
# (removed) Pydantic chains and prompts — theme logic lives in nodes/theme_nodes.py
# ----------------------------------------------------------------------------------------

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
        themes_json = get_all_themes("")
        items = json.loads(themes_json or "[]")
        if isinstance(items, dict):
            items = items.get("data", [])
        _themes_cache = {
            str(it.get("ent_id")): it.get("labl")
            for it in (items or [])
            if isinstance(it, dict)
            and it.get("ent_id") is not None
            and it.get("labl") is not None
        }
        logging.info("Loaded %d themes dynamically from database", len(_themes_cache))
    except Exception as e:
        logging.error("Failed to load themes dynamically, using fallback: %s", e)
        # Fallback to minimal themes if database fails
        _themes_cache = {
            "T_POP": "Population",
            "T_WK": "Work & Poverty",
            "T_HOUS": "Housing",
        }


def refresh_themes_cache():
    """Force-refresh the theme cache by reloading from the database."""
    global _themes_cache
    _themes_cache = None
    return get_themes_dict()


# (removed) duplicate theme prompt/chain — use nodes/theme_nodes.py instead


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
    _ = load_config()  # Reserved for future use; no heavy initialization here

    # Preload themes from database for immediate availability
    logger.info("Preloading themes from database...")
    themes = get_themes_dict()
    logger.info("Preloaded %d themes for dynamic usage", len(themes))
    # Initialize the StateGraph with the defined state structure.
    workflow = StateGraph(lg_state)

    # --- Add Nodes ---
    # Add each node function defined earlier to the graph, associating it with a unique name.
    # Conversational agent is now the single entry-point agent
    workflow.add_node(
        "postcode_tool_call", postcode_tool_call
    )  # Handles postcode search
    workflow.add_node(
        "multi_place_tool_call", multi_place_tool_call
    )  # Searches multiple places
    # NEW: Proper LangGraph pattern for map interaction
    workflow.add_node(
        "update_polygon_selection", update_polygon_selection
    )  # Updates map state (no interrupts)
    # workflow.add_node("request_map_selection", request_map_selection) # Dedicated interrupt node
    workflow.add_node(
        "select_unit_on_map", select_unit_on_map
    )  # Legacy node (kept for compatibility)
    workflow.add_node(
        "find_cubes_node", find_cubes_node
    )  # Retrieves final data cubes (interrupt)
    # Register v1 conversational agent node only
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
    workflow.add_node("UnitTypeInfo_node", UnitTypeInfo_node)
    workflow.add_node("DataEntityInfo_node", DataEntityInfo_node)
    workflow.add_node("ExplainVisibleData_node", ExplainVisibleData_node)
    workflow.add_node("ask_followup_node", ask_followup_node)
    workflow.add_node("resolve_place_and_unit", resolve_place_and_unit)

    workflow.add_node("resolve_theme", resolve_theme)

    # conversational_agent_node handles its own routing with Command; no conditional edges needed

    for n in [
        "ShowState_node",
        "ListThemes_node",
        "DescribeTheme_node",
        "UnitTypeInfo_node",
        "DataEntityInfo_node",
        "ExplainVisibleData_node",
        "RemoveTheme_node",
        "Reset_node",
        "AddPlace_node",
        "RemovePlace_node",
    ]:
        workflow.add_edge(n, END)

    # --- Define Edges (Workflow Logic) ---

    # No agent selector needed; the conversational agent is the single entry point

    # Create a dedicated start router node. It determines whether we should
    # resume a partially-completed node, handle a newly-arrived human message
    # (route to the agent), or continue the place/unit resolution flow.
    def start_node(state: lg_State) -> dict | Command:
        """Lightweight router that chooses the next node based on state.

        Priority order:
        1) If a new human message is present, go to ``conversational_agent_node`` for intent
           extraction and routing.
        2) If we received a new Add/RemovePlace intent, route to ``conversational_agent_node``
           to process it immediately.
        3) If resuming a node that expects a ``selection_idx`` from the UI,
           continue that node.
        4) If already waiting for user input at a node, stay there.
        5) Otherwise start fresh at ``conversational_agent_node``.
        """
        current_node = state.get("current_node")
        selection_idx = state.get("selection_idx")
        last_intent_payload = state.get("last_intent_payload") or {}
        intent = last_intent_payload.get("intent")

        logger.debug(
            "start_node: current_node=%s selection_idx=%s intent=%s",
            current_node,
            selection_idx,
            intent,
        )

        # Frontend may have provided an interrupt message; append and clear it
        interrupt_message = state.get("interrupt_message")
        if interrupt_message:
            from vobchat.nodes.utils import _append_ai

            _append_ai(state, interrupt_message)
            logger.debug("start_node: appended interrupt message to state")

        # Debug: Check if workflow_input was passed in
        if selection_idx is not None:
            logger.debug(
                "start_node: selection_idx present (likely from UI click): %s",
                selection_idx,
            )

        # PRIORITY 1: Check for a new human message that needs intent processing
        # This MUST precede checks of any prior intent payloads to avoid loops
        messages = state.get("messages", [])
        has_new_user_message = False
        if messages:
            last_message = messages[-1]
            if isinstance(last_message, HumanMessage):
                has_new_user_message = True
            elif (
                isinstance(last_message, tuple)
                and len(last_message) >= 2
                and last_message[0] == "user"
            ):
                has_new_user_message = True

        logger.debug(
            "start_node: messages_count=%s has_new_user_message=%s",
            len(messages) if messages else 0,
            has_new_user_message,
        )

        # If there's a new user message, route to the agent
        if has_new_user_message:
            logger.debug(
                "start_node: routing to conversational_agent_node (new user message)"
            )
            return Command(goto="conversational_agent_node")

        # PRIORITY 2: Check for new intent payloads (e.g., map clicks) before
        # resuming a stale node. Route direct map intents straight to their
        # target nodes (bypassing the agent which only reacts to HumanMessage).
        if intent in ["AddPlace", "RemovePlace"]:
            return Command(goto="conversational_agent_node")

        # PRIORITY 3: If we have a current_node and selection_idx (button click),
        # but no new intent, resume from that node
        if current_node and selection_idx is not None:
            logger.debug(
                "start_node: resuming node=%s with selection_idx=%s",
                current_node,
                selection_idx,
            )
            # Only clear interrupt flag; do not overwrite messages
            if interrupt_message:
                return Command(goto=current_node, update={"interrupt_message": None})
            return Command(goto=current_node)

        # PRIORITY 4: If there are queued planner actions and no outstanding
        # place resolution, execute the next queued action. This prevents
        # jumping to theme selection in the middle of per-place disambiguation.
        try:
            queue = list(state.get("intent_queue", []) or [])
        except Exception:
            queue = []
        # Detect unresolved places (g_unit is None)
        places = state.get("places", []) or []
        unresolved = any((p or {}).get("g_unit") is None for p in places)
        if queue and not unresolved:
            next_payload = queue.pop(0)
            next_intent = (next_payload or {}).get("intent")
            target = _intent_to_node(next_intent)
            logger.info(
                "start_node: dequeued next planner action → %s (target=%s)",
                next_intent,
                target,
            )
            if target:
                return Command(
                    goto=target,
                    update={
                        "last_intent_payload": next_payload,
                        "intent_queue": queue,
                    },
                )
            # If no routable target (e.g., Chat), drop it and continue
            return Command(
                goto="conversational_agent_node",
                update={
                    "last_intent_payload": None,
                    "intent_queue": queue,
                },
            )

        # PRIORITY 5: If we have a current_node but no selection_idx, we're
        # waiting for user input — do not restart (keeps long-running nodes
        # in control until the user provides a button/index selection).
        if current_node:
            # Special-case: avoid looping back into resolve_place_and_unit when
            # there is nothing left to resolve (all places have g_unit set or
            # current_place_index is beyond the list). In that case, clear the
            # pointer and hand control back to the agent (which will no-op if
            # there is no new HumanMessage), ending the turn cleanly.
            try:
                places = state.get("places", []) or []
                unresolved = any((p or {}).get("g_unit") is None for p in places)
                current_idx = state.get("current_place_index", 0) or 0
            except Exception:
                places = []
                unresolved = False
                current_idx = 0
            if current_node == "resolve_place_and_unit" and (
                not unresolved or current_idx >= len(places)
            ):
                logger.info(
                    "start_node: clearing stale resolve_place_and_unit pointer (all places resolved)"
                )
                return Command(
                    goto="conversational_agent_node",
                    update={"current_node": None, "selection_idx": None},
                )
            logger.debug("start_node: waiting for user input at node=%s", current_node)
            if interrupt_message:
                return Command(goto=current_node, update={"interrupt_message": None})
            return Command(goto=current_node)

    # Add the start node and connect it to START
    workflow.add_node("start_node", start_node)
    workflow.add_edge(START, "start_node")

    workflow.add_edge("multi_place_tool_call", "resolve_place_and_unit")

    workflow.add_edge("find_cubes_node", "conversational_agent_node")

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
    except Exception:
        # Catch errors during compilation (e.g., Redis connection issues).
        logger.error("Error compiling workflow", exc_info=True)
        raise  # Re-raise the exception to prevent app startup if compilation fails.

    # --- Optionally produce diagrams ---
    # These are useful for visualizing and debugging the workflow structure.
    logger.info("Generating ASCII diagram of the workflow:")
    try:
        # Log a text-based representation of the graph to the console/logs.
        logger.info("\n%s", compiled_workflow.get_graph().draw_ascii())
    except Exception:
        logger.warning(
            "Could not generate ASCII diagram", exc_info=True
        )  # Non-critical error

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
