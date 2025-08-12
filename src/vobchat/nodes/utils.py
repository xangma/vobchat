"""Shared utilities for node implementations."""
from __future__ import annotations
import uuid
import json
import math
from typing import Dict, List, Any
from langchain_core.messages import AIMessage, HumanMessage
from vobchat.state_schema import lg_State
import logging

logger = logging.getLogger(__name__)

def make_json_safe(obj: Any) -> Any:
    """Return a JSON-safe version of ``obj``.

    Replaces NaN/Inf with None and recursively normalizes lists and dicts.

    Returns:
        Any: A structure that can be serialized by ``json.dumps`` without
        raising due to NaN/Inf or nested unsupported values.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    elif isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_json_safe(item) for item in obj]
    return obj

def safe_json_dumps(obj: Any, **kwargs) -> str:
    """Serialize to JSON after normalizing problematic numeric values.

    Returns:
        str: JSON string with NaN/Inf coerced to null and nested containers
        normalized for serialization.
    """
    safe_obj = make_json_safe(obj)
    return json.dumps(safe_obj, **kwargs)

def clean_database_text(text: str) -> str:
    """Clean database text with hard wraps, tabs, and mixed HTML/text.
    
    The Vision of Britain database contains text with:
    - Hard line breaks in the middle of sentences (from fixed-width formatting)
    - Tab characters for paragraph indentation
    - Mixed HTML tags like <br> for paragraph breaks
    
    This function normalizes the text for display while preserving paragraph structure.
    
    Args:
        text: Raw text from database
        
    Returns:
        str: Cleaned text with proper paragraph breaks and no hard wrapping.
    """
    import re
    
    if not text or not isinstance(text, str):
        return text
    
    # Replace tabs with nothing (they're used for paragraph indentation)
    text = text.replace('\t', '')
    
    # Split into paragraphs (separated by double newlines or <br><br>)
    # First normalize <br> tags
    text = re.sub(r'<br\s*/?\s*>', '<br>', text, flags=re.IGNORECASE)
    text = text.replace('<br><br>', '\n\n')
    
    # Split by double newlines to get paragraphs
    paragraphs = text.split('\n\n')
    
    # Within each paragraph, join lines that were hard-wrapped
    cleaned_paragraphs = []
    for para in paragraphs:
        # Remove single newlines within the paragraph
        para = para.replace('\n', ' ')
        # Clean up multiple spaces
        para = re.sub(r' +', ' ', para)
        para = para.strip()
        if para:
            cleaned_paragraphs.append(para)
    
    # Join paragraphs with double newlines for markdown
    return '\n\n'.join(cleaned_paragraphs)


def serialize_messages(messages):
    """Convert LangChain message objects to JSON-serializable dicts.

    Returns:
        list[dict]: Minimal message records for front-end consumption.
    """
    serialized = []
    for msg in messages:
        if isinstance(msg, AIMessage):
            serialized.append({
                "_type": "ai",
                "content": msg.content,
                "type": "ai"
            })
        elif isinstance(msg, HumanMessage):
            serialized.append({
                "_type": "human", 
                "content": msg.content,
                "type": "human"
            })
        elif hasattr(msg, 'content'):
            # Generic message with content
            serialized.append({
                "_type": getattr(msg, '_type', 'unknown'),
                "content": msg.content,
                "type": getattr(msg, 'type', 'unknown')
            })
    return serialized

def _append_ai(state: lg_State, text: str):
    """Append an AI message to the state with streaming metadata.

    Returns:
        None: Mutates ``state['messages']`` in place with a streamable message.
    """
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
    """Return True if any message contains the specified content (case-insensitive)."""
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
    """Return a fresh lg_State dict that clears ALL state fields.

    Returns:
        dict: New state with canonical keys initialized to empty/None values.
    """
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
    """Remove duplicate intents from the intent queue to prevent loops.

    Returns:
        None: Updates the queue in place if duplicates are detected.
    """
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
