"""Theme-related nodes: AddTheme, RemoveTheme, ListThemes, DescribeTheme, resolve_theme."""
from __future__ import annotations
import io
import json
import pandas as pd
from langchain_core.messages import AIMessage
from langgraph.types import Command
from langgraph.types import interrupt
from vobchat.state_schema import lg_State, get_selected_units
from vobchat.tools import (
    find_themes_for_unit,
    get_all_themes,
    get_theme_text
)
from .utils import _append_ai, _has_message_content, _maybe_route_to_cubes, _clean_duplicate_intents_from_queue
import logging

logger = logging.getLogger(__name__)

def ListThemesForSelection_node(state: lg_State):
    """List themes available for the current place selection."""
    g_units = get_selected_units(state)
    if not g_units:
        _append_ai(state, "No place selected yet.")
        return state

    rows = []
    for u in g_units:
        try:
            df = find_themes_for_unit(str(u))
            if not df.empty:
                rows.append(df)
        except Exception as exc:
            _append_ai(state, f"Error fetching themes for unit {u}: {exc}")

    if not rows:
        _append_ai(state, "No themes found for your selection.")
        return state

    big = pd.concat(rows).drop_duplicates("ent_id")
    listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in big.iterrows())
    _append_ai(state, "Themes available:\n" + listing)
    return state

def ListAllThemes_node(state: lg_State):
    """List all available themes in the system."""
    df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
    if df.empty:
        # Check if we already have this message to prevent duplicates
        if not _has_message_content(state, "Theme catalogue appears empty."):
            _append_ai(state, "Theme catalogue appears empty.")
        return state

    listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in df.iterrows())
    themes_message = listing + "\n… all themes shown. Use keywords to narrow."

    # Check if we already have this themes listing to prevent duplicates
    if not _has_message_content(state, listing):
        _append_ai(state, themes_message)

    state["last_intent_payload"] = {}
    state["needs_clarification"] = False
    return state

def AddTheme_node(state: lg_State):
    """Add a theme to the selection."""
    logger.info("AddTheme_node: adding a theme to the selection")

    # CRITICAL: Clear ALL AddTheme intents from queue immediately to prevent infinite loops
    intent_queue = state.get("intent_queue", [])
    original_queue_size = len(intent_queue)

    # Note: Removed intent_queue updates to prevent concurrent modification conflicts
    # Theme intent processing now relies on workflow routing instead of queue management
    cleaned_queue = [intent for intent in intent_queue if intent.get("intent") != "AddTheme"]

    removed_count = original_queue_size - len(cleaned_queue)
    if removed_count > 0:
        logger.info(f"AddTheme_node: AGGRESSIVELY removed {removed_count} AddTheme intents from queue (was {original_queue_size}, now {len(cleaned_queue)})")

    # Also clean other duplicate intents while we're at it
    _clean_duplicate_intents_from_queue(state)

    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}

    # direct code
    if "theme_code" in args:
        code = args["theme_code"].strip().upper()
        if not code.startswith("T_"):
            _append_ai(state, f"'{code}' doesn't look like a valid theme code.")
            return state
        state["selected_theme"] = pd.DataFrame({"ent_id": [code], "labl": [code]}).to_json(orient="records")
        _append_ai(state, f"Theme set to {code}.")
        return _maybe_route_to_cubes(state)

    # free text query
    elif "theme_query" in args:
        q = args["theme_query"].strip()
        logger.info(f"AddTheme_node: Processing theme query '{q}'")

        # Check if we already have this theme selected to avoid unnecessary changes
        current_theme = state.get("selected_theme")
        if current_theme:
            try:
                theme_data = json.loads(current_theme)
                current_label = theme_data.get("labl", "").lower()
                query_lower = q.lower().strip()

                # If the query matches the current theme, don't change anything
                if query_lower in current_label or current_label in query_lower:
                    logger.info(f"AddTheme_node: Theme query '{q}' matches current theme '{current_label}', keeping current selection")

                    # CRITICAL: Clear ALL AddTheme intents from the queue to prevent infinite processing
                    intent_queue = state.get("intent_queue", [])
                    original_queue_length = len(intent_queue)
                    # Note: Removed intent_queue updates to prevent concurrent modification conflicts
                    # Theme intent processing now relies on workflow routing instead of queue management
                    filtered_queue = [
                        intent for intent in intent_queue
                        if intent.get("intent") != "AddTheme"  # Remove ALL AddTheme intents, not just matching ones
                    ]
                    removed_count = original_queue_length - len(filtered_queue)
                    if removed_count > 0:
                        logger.info(f"AddTheme_node: Would remove {removed_count} ALL AddTheme intents from queue")

                    # Clear the processed intent payload to prevent reprocessing
                    state["last_intent_payload"] = {}
                    return state
            except (json.JSONDecodeError, KeyError):
                logger.warning(f"AddTheme_node: Error parsing current theme, proceeding with theme change")

        logger.info(f"AddTheme_node: Setting extracted_theme to '{q}' and clearing selected_theme for theme resolution")
        # Clear the processed intent payload to prevent reprocessing
        state["last_intent_payload"] = {}

        # CRITICAL: Use Command to route directly to resolve_theme with state updates
        # This ensures the state changes are applied before resolve_theme runs
        return Command(
            goto="resolve_theme",
            update={
                "extracted_theme": q,
                "selected_theme": None,  # Clear existing theme
            }
        )

    else:
        _append_ai(state, "AddTheme: no theme_code or theme_query provided.")

    # Clear the processed intent payload to prevent reprocessing
    state["last_intent_payload"] = {}
    return state

def RemoveTheme_node(state: lg_State):
    """Remove the currently selected theme."""
    if not state.get("selected_theme"):
        _append_ai(state, "No theme is currently selected.")
        return state
    state["selected_theme"] = None
    state["extracted_theme"] = None
    _append_ai(state, "Theme selection cleared.")
    return state

def DescribeTheme_node(state: lg_State):
    """
    Reply with the definition/metadata of a theme.
    • Works even if *no* theme is currently selected.
    • Clears last_intent_payload so the router won't loop.
    """

    payload = state.get("last_intent_payload") or {}
    args    = payload.get("arguments", {})
    query   = (args.get("theme") or "").strip()

    logger.info(f"DescribeTheme_node: Processing query '{query}' from payload: {payload}")

    # 1️⃣ Determine the theme code
    theme_df = None

    # a) use already-selected theme (if any)
    if state.get("selected_theme"):
        theme_df = pd.read_json(io.StringIO(state["selected_theme"]), orient="records")

    # b) otherwise, fuzzy-match the query against *all* themes
    if theme_df is None and query:
        all_df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
        logger.info(f"DescribeTheme_node: Loaded {len(all_df)} themes. Sample themes: {all_df.head(3)['labl'].tolist()}")

        # Try exact case-insensitive match first
        mask = all_df["labl"].str.contains(query, case=False, regex=False)
        matches = all_df[mask]
        logger.info(f"DescribeTheme_node: Exact match for '{query}': found {len(matches)} matches")
        if mask.any():
            theme_df = all_df[mask].head(1)
            logger.info(f"DescribeTheme_node: Using exact match: {theme_df['labl'].iloc[0]}")
        else:
            # Try partial word matching if exact match fails
            query_words = query.lower().split()
            logger.info(f"DescribeTheme_node: No exact match, trying word matching for: {query_words}")
            if query_words:
                # Look for themes that contain any of the query words
                combined_mask = pd.Series([False] * len(all_df))
                for word in query_words:
                    word_mask = all_df["labl"].str.contains(word, case=False, regex=False)
                    word_matches = all_df[word_mask]
                    logger.info(f"DescribeTheme_node: Word '{word}' matched {len(word_matches)} themes: {word_matches['labl'].tolist()[:3]}")
                    combined_mask |= word_mask

                if combined_mask.any():
                    theme_df = all_df[combined_mask].head(1)
                    logger.info(f"DescribeTheme_node: Using word match: {theme_df['labl'].iloc[0]}")
                else:
                    logger.info(f"DescribeTheme_node: No word matches found for '{query}'")

    # c) still nothing → ask a follow-up
    if theme_df is None or theme_df.empty:
        _append_ai(state, "I'm not sure which theme you mean. Try e.g. 'describe Population' or 'describe T_POP'.")
        state["last_intent_payload"] = {}
        return state

    code = theme_df["ent_id"].iat[0]
    labl = theme_df["labl"].iat[0]

    # 2️⃣ Fetch the long description
    desc_df = pd.read_json(io.StringIO(get_theme_text(code)), orient="records")
    text    = desc_df["text"].iat[0] if not desc_df.empty else "(no description available)"

    _append_ai(state, f"**{labl}** ({code})\n\n{text}")

    # 3️⃣ house-keeping
    state["last_intent_payload"] = {}      # avoid re-routing
    return state

def resolve_theme(state: lg_State) -> lg_State | Command:
    """Resolve an extracted theme query to a specific theme selection."""
    logger.debug("=== URGENT DEBUG: resolve_theme FUNCTION ENTRY ===")
    logger.info("resolve_theme: Starting theme resolution...")

    query = state.get("extracted_theme")

    # CRITICAL: If we already have a selected theme and no new query, skip processing
    if state.get("selected_theme") and not query:
        logger.info("resolve_theme: Already have selected_theme and no new query, skipping")
        # Clear state and jump to cube retrieval if ready
        state["extracted_theme"] = None
        return _maybe_route_to_cubes(state)

    if not query:
        logger.warning("resolve_theme: No extracted_theme to resolve")
        return state

    # Search for themes
    logger.info(f"resolve_theme: Searching for theme matching '{query}'")
    df = pd.read_json(io.StringIO(get_all_themes(query)), orient="records")

    if df.empty:
        state["messages"].append(
            AIMessage(
                content=f"I couldn't find any themes matching '{query}'. Try 'list all themes' to see what's available.",
                response_metadata={"stream_mode": "stream"}
            )
        )
        state["extracted_theme"] = None
        return state

    # Handle disambiguation if multiple themes found
    if len(df) > 1:
        logger.info(f"resolve_theme: Found {len(df)} themes matching '{query}', asking user to choose")

        # Create options for user selection
        options = []
        for idx, (_, row) in enumerate(df.head(10).iterrows()):  # Limit to 10 options
            options.append({
                "option_type": "theme",
                "label": f"{row['labl']} ({row['ent_id']})",
                "value": idx,
                "color": "#333"
            })

        # Ask user to select
        interrupt(
            value={
                "message": f"I found {len(df)} themes matching '{query}'. Please choose one:",
                "options": options,
                "current_node": "resolve_theme"
            }
        )
        return state

    # Single theme found - select it
    theme = df.iloc[0]
    logger.info(f"resolve_theme: Single theme found - selecting {theme['labl']} ({theme['ent_id']})")

    state["selected_theme"] = pd.DataFrame([theme]).to_json(orient="records")
    state["messages"].append(
        AIMessage(
            content=f"Selected theme: {theme['labl']} ({theme['ent_id']})",
            response_metadata={"stream_mode": "stream"}
        )
    )

    # Clear extracted theme to prevent re-processing
    state["extracted_theme"] = None

    # Check if we should jump to cube retrieval
    return _maybe_route_to_cubes(state)
