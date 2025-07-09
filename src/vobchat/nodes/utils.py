"""Shared utilities for node implementations."""
from __future__ import annotations
import uuid
from typing import Dict, List
from langchain_core.messages import AIMessage
from vobchat.state_schema import lg_State
import logging

logger = logging.getLogger(__name__)

def _append_ai(state: lg_State, text: str):
    """Append an AI message to the state with streaming metadata."""
    # Mark user-facing messages as streamable
    # Add a unique ID to each message to prevent duplicate streaming
    message = AIMessage(
        content=text,
        response_metadata={
            "stream_mode": "stream",
            "message_id": str(uuid.uuid4())  # Unique ID for duplicate detection
        }
    )
    state.setdefault("messages", []).append(message)

def _has_message_content(state: lg_State, search_content: str) -> bool:
    """Check if any existing message contains the specified content."""
    messages = state.get("messages", [])
    search_lower = search_content.lower().strip()

    for msg in messages:
        if hasattr(msg, 'content') and msg.content:
            msg_content = str(msg.content).lower().strip()
            # Check if the search content is contained in any existing message
            if search_lower in msg_content:
                return True
    return False

# def _maybe_route_to_cubes(state: lg_State):
#     """Jump to cube retrieval when both slots (theme + ≥1 unit) are filled."""
#     from vobchat.state_schema import get_selected_units
#     selected_units = get_selected_units(state)
#     if state.get("selected_theme") and selected_units:
#         from langgraph.types import Command
#         return Command(goto="find_cubes_node")
#     return state

def _initial_state() -> Dict:
    """Return a fresh lg_State dict that clears ALL state fields."""
    return {
        # conversation
        "messages": [],
        "intent_queue": [],

        # user-choice plumbing
        "selection_idx": None,

        # place + unit selections - single source of truth
        "places": [],

        # theme selection
        "selected_place_themes": None,
        "selected_theme": None,

        # cube selection
        "cubes": [],
        "selected_cubes": [],

        # extraction results
        # extracted_place_names removed - using places array as single source of truth
        "extracted_counties": [],
        "extracted_unit_types": [],
        "extracted_polygon_ids": [],
        "extracted_theme": None,
        "is_postcode": False,
        "extracted_postcode": None,

        # multi-place machinery
        # "multi_place_search_df": None,
        "current_place_index": 0,

        # year filters
        "min_year": None,
        "max_year": None,

        # misc / meta
        "current_node": None,
        "options": [],
        "message": None,
        "_prompted_for_place": False,
    }

def _clean_duplicate_intents_from_queue(state: lg_State):
    """Remove duplicate intents from the intent queue to prevent infinite loops."""
    intent_queue = state.get("intent_queue", [])
    if not intent_queue:
        return

    # Group intents by (intent, arguments) and keep only one of each
    seen_intents = set()
    cleaned_queue = []

    for intent in intent_queue:
        # Create a hashable representation of the intent
        intent_key = (
            intent.get("intent"),
            str(sorted(intent.get("arguments", {}).items()))
        )

        if intent_key not in seen_intents:
            seen_intents.add(intent_key)
            cleaned_queue.append(intent)

    original_length = len(intent_queue)
    if len(cleaned_queue) < original_length:
        logger.info(f"_clean_duplicate_intents_from_queue: Removed {original_length - len(cleaned_queue)} duplicate intents from queue")
        # Note: Removed intent_queue updates to prevent concurrent modification conflicts
