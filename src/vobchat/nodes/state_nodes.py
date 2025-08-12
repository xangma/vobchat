"""State management nodes: ShowState and Reset.

These nodes summarize the current selection and reset the conversation state.
They do not interrupt; instead they return minimal state updates or a Command
that resets key fields and routes to ``START``.
"""
from __future__ import annotations
from typing import List
import pandas as pd
from langgraph.types import Command
from vobchat.state_schema import lg_State, get_selected_units
from .utils import _append_ai, _initial_state
import logging

logger = logging.getLogger(__name__)

def ShowState_node(state: lg_State) -> dict:
    """Display the current state of selections to the user.

    Returns:
        dict: Minimal updates including the appended AI message (via
        ``messages``) and a cleared ``last_intent_payload`` to avoid loops.
    """
    summary: List[str] = []

    g_units = get_selected_units(state)
    places = state.get("places", []) or []
    place_names = [p.get("name", f"Place {i}") for i, p in enumerate(places)]
    for idx, g_unit in enumerate(g_units):
        p_name = place_names[idx] if idx < len(place_names) else f"unit {g_unit}"
        summary.append(f"• {p_name} (g_unit {g_unit})")
    if not summary:
        summary.append("• no places selected yet")

    if state.get("selected_theme"):
        df = pd.read_json(state["selected_theme"], typ='series')
        summary.append(f"• theme: {df['labl']}")
    else:
        summary.append("• no theme selected yet")

    yrs = (state.get("min_year"), state.get("max_year"))
    if any(yrs):
        summary.append(f"• years: {yrs[0] or '…'} - {yrs[1] or '…'}")

    _append_ai(state, "Current selection:\n" + "\n".join(summary))

    # Only return the specific fields this node updates
    return {
        "messages": state.get("messages", []),  # Updated by _append_ai
        "last_intent_payload": {}  # Clear after processing to prevent loops
    }

def Reset_node(state: lg_State) -> Command:
    """Reset all state to start fresh.

    Returns:
        Command: A ``Command`` with ``goto='START'`` and a focused ``update``
        payload resetting core fields (places, theme, cubes, filters, etc.)
        without replacing the entire state dict.
    """
    # _append_ai(state, "Starting over - previous selections cleared.")
    # Get fresh state (selection_idx already set to None in _initial_state)
    reset_state = _initial_state()
    logger.info("Reset_node: Cleared all state including selection_idx")

    # Note: Streamed message IDs are cleared on the frontend when reset is received
    # Backend clearing would require thread_id context which is not easily accessible here

    # Only return the fields that need to be reset, not the entire state
    reset_places = reset_state.get("places", [])

    return Command(goto="START", update={
        "messages": state.get("messages", []),  # Include the "Starting over" message
        "places": reset_places,
        "selected_theme": reset_state.get("selected_theme"),
        "selected_cubes": reset_state.get("selected_cubes"),
        "min_year": reset_state.get("min_year"),
        "max_year": reset_state.get("max_year"),
        "last_intent_payload": reset_state.get("last_intent_payload", {}),
        "current_place_index": reset_state.get("current_place_index"),
        "extracted_theme": reset_state.get("extracted_theme"),
        "show_visualization": reset_state.get("show_visualization", False),
        "current_node": reset_state.get("current_node"),
        "selection_idx": reset_state.get("selection_idx"),
        "options": reset_state.get("options", []),
        "map_update_request": reset_state.get("map_update_request")
    })
