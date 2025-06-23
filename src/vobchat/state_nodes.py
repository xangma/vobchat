from __future__ import annotations
import io
from langgraph.types import interrupt
from langgraph.graph import END
"""Intent-handler nodes (no LLM) for the flexible DDME workflow.
Each node is registered under the name `<Intent>_node` to match AssistantIntent.
"""

from typing import List, Optional, Dict
import re
import pandas as pd
from langchain_core.messages import AIMessage
from langgraph.types import Command

from vobchat.state_schema import lg_State
from vobchat.intent_handling import AssistantIntent
from vobchat.tools import (
    find_themes_for_unit, get_all_themes,
    find_units_by_postcode, find_places_by_name,
    get_theme_text,
)
import logging

logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def _append_ai(state: lg_State, text: str):
    # Mark user-facing messages as streamable
    message = AIMessage(
        content=text,
        response_metadata={"stream_mode": "stream"}
    )
    state.setdefault("messages", []).append(message)


def _maybe_route_to_cubes(state: lg_State):
    """Jump to cube retrieval when both slots (theme + ≥1 unit) are filled."""
    if state.get("selected_theme") and state.get("selected_place_g_units"):
        return Command(goto="find_cubes_node")
    return state


def _initial_state() -> Dict:
    """Return a fresh lg_State dict that clears ALL state fields."""
    return {
        # conversation
        "messages": [],
        "intent_queue": [],

        # user-choice plumbing
        "selection_idx": None,

        # place + unit selections
        "places": [],
        "selected_place_g_places": [],
        "selected_place_g_units": [],
        "selected_place_g_unit_types": [],

        # theme selection
        "selected_place_themes": None,
        "selected_theme": None,

        # cube selection
        "cubes": [],
        "selected_cubes": [],

        # extraction results
        "extracted_place_names": [],
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

        # map interaction
        "selected_polygons": [],
        "selected_polygons_unit_types": [],

        # misc / meta
        "current_node": None,
        "last_intent_payload": {},
        "options": [],
        "message": None,
        "_theme_hint_done": False,
        "_prompted_for_place": False,
    }

# ─────────────────────────────────────────────────────────────────────────────
# 1. ShowState
# ─────────────────────────────────────────────────────────────────────────────

def ShowState_node(state: lg_State):
    summary: List[str] = []

    g_units = state.get("selected_place_g_units", [])
    place_names = state.get("extracted_place_names", [])
    for idx, g_unit in enumerate(g_units):
        p_name = place_names[idx] if idx < len(place_names) else f"unit {g_unit}"
        summary.append(f"• {p_name} (g_unit {g_unit})")
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
    state["last_intent_payload"] = {}
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 2. ListThemesForSelection
# ─────────────────────────────────────────────────────────────────────────────

def ListThemesForSelection_node(state: lg_State):
    g_units = state.get("selected_place_g_units", [])
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
            _append_ai(state, f"Error fetching themes for unit {u}: {exc}")

    if not rows:
        _append_ai(state, "No themes found for your selection.")
        return state

    big = pd.concat(rows).drop_duplicates("ent_id")
    listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in big.iterrows())
    _append_ai(state, "Themes available:\n" + listing)
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 3. ListAllThemes
# ─────────────────────────────────────────────────────────────────────────────

def ListAllThemes_node(state: lg_State):
    df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
    if df.empty:
        _append_ai(state, "Theme catalogue appears empty.")
        return state
    listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in df.iterrows())
    _append_ai(state, listing + "\n… all themes shown. Use keywords to narrow.")
    state["last_intent_payload"] = {}
    state["needs_clarification"] = False
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 4. Reset
# ─────────────────────────────────────────────────────────────────────────────

def Reset_node(state: lg_State):
    _append_ai(state, "Starting over - previous selections cleared.")
    # Ensure selection_idx is explicitly cleared in the reset state
    reset_state = _initial_state()
    reset_state["selection_idx"] = None
    logger.info("Reset_node: Cleared all state including selection_idx")
    return Command(goto="START", update=reset_state)

# ─────────────────────────────────────────────────────────────────────────────
# 5. AddPlace
# ─────────────────────────────────────────────────────────────────────────────

def AddPlace_node(state: lg_State):
    args = (state.get("last_intent_payload") or {}).get("arguments", {})

    # ── gather place names to add ───────────────────────────────────
    names_to_add: List[str] = []
    counties_to_add: List[str] = []
    unit_types_to_add: List[str] = []
    polygon_ids_to_add: List[Optional[int]] = []
    if "places" in args and isinstance(args["places"], list):
        names_to_add = [p.strip() for p in args["places"] if p.strip()]
    elif "place" in args:
        names_to_add = [args["place"].strip()]
    if "counties" in args and isinstance(args["counties"], list):
        counties_to_add = [p.strip() for p in args["counties"] if p.strip()]
    elif "county" in args:
        counties_to_add = [args["county"].strip()]
    if "unit_type" in args:
        unit_types_to_add = [args["unit_type"].strip()]
    # Extract polygon_id if provided (from map clicks)
    if "polygon_id" in args:
        polygon_ids_to_add = [args["polygon_id"]]


    if not names_to_add:
        _append_ai(state, "AddPlace: please specify at least one place name.")
        return state

    # ── extend the existing queues ─────────────────────────────────
    # CRITICAL: Only replace existing data for the FIRST map click in a new session
    # For additional map clicks, extend the existing selection
    existing_places = state.get("extracted_place_names", [])
    existing_units = state.get("selected_place_g_units", [])
    is_map_click_replacement = (
        "polygon_id" in args and
        len(names_to_add) == 1 and
        len(existing_places) == 0 and
        len(existing_units) == 0
    )

    if is_map_click_replacement:
        # Replace existing data for the first map click in a new session
        names = []
        counties = []
        unit_types = []
        polygon_ids = []
        logger.info("AddPlace_node: First map click detected - starting new selection")
    else:
        # Extend existing data for text-based additions or additional map clicks
        names    = state.get("extracted_place_names", [])
        counties = state.get("extracted_counties", [])
        unit_types = state.get("extracted_unit_types", [])
        polygon_ids = state.get("extracted_polygon_ids", [])
        if "polygon_id" in args:
            logger.info("AddPlace_node: Additional map click detected - extending existing selection")

    for idx, p in enumerate(names_to_add):
        names.append(p)
        # Add corresponding polygon_id if available, otherwise None
        if idx < len(polygon_ids_to_add):
            polygon_ids.append(polygon_ids_to_add[idx])
        else:
            polygon_ids.append(None)
    for c in counties_to_add:
        counties.append(c)
    for u in unit_types_to_add:
        unit_types.append(u)

    # pointer to the **first** new place
    new_idx = len(names) - len(names_to_add)

    plural = ", ".join(names_to_add)
    _append_ai(state, f"Okay - adding {plural}. Let me find them …")

    update = {
        "extracted_place_names": names,
        "extracted_counties": counties,
        "extracted_unit_types": unit_types,
        "extracted_polygon_ids": polygon_ids,
        # "multi_place_search_df": None,
        "current_place_index": new_idx,
        "last_intent_payload": {},
        # CRITICAL: Clear any stale extracted_theme to prevent theme reprocessing when adding places
        "extracted_theme": None,
    }

    return Command(goto="multi_place_tool_call", update=update)

# ─────────────────────────────────────────────────────────────────────────────
# 6. AddTheme
# ─────────────────────────────────────────────────────────────────────────────

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
        state["intent_queue"] = cleaned_queue


def AddTheme_node(state: lg_State):
    logger.info("AddTheme_node: adding a theme to the selection")

    # CRITICAL: Clear ALL AddTheme intents from queue immediately to prevent infinite loops
    intent_queue = state.get("intent_queue", [])
    original_queue_size = len(intent_queue)

    # Remove ALL AddTheme intents from the queue aggressively
    cleaned_queue = [intent for intent in intent_queue if intent.get("intent") != "AddTheme"]
    state["intent_queue"] = cleaned_queue

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
            _append_ai(state, f"‘{code}' doesn't look like a valid theme code.")
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
                import json
                theme_data = json.loads(current_theme)
                current_label = theme_data.get("labl", "").lower()
                query_lower = q.lower().strip()

                # If the query matches the current theme, don't change anything
                if query_lower in current_label or current_label in query_lower:
                    logger.info(f"AddTheme_node: Theme query '{q}' matches current theme '{current_label}', keeping current selection")

                    # CRITICAL: Clear ALL AddTheme intents from the queue to prevent infinite processing
                    intent_queue = state.get("intent_queue", [])
                    original_queue_length = len(intent_queue)
                    filtered_queue = [
                        intent for intent in intent_queue
                        if intent.get("intent") != "AddTheme"  # Remove ALL AddTheme intents, not just matching ones
                    ]
                    state["intent_queue"] = filtered_queue
                    removed_count = original_queue_length - len(filtered_queue)
                    if removed_count > 0:
                        logger.info(f"AddTheme_node: Removed {removed_count} ALL AddTheme intents from queue")

                    # Clear the processed intent payload to prevent reprocessing
                    state["last_intent_payload"] = {}
                    return state
            except (json.JSONDecodeError, KeyError):
                logger.warning(f"AddTheme_node: Error parsing current theme, proceeding with theme change")

        logger.info(f"AddTheme_node: Setting extracted_theme to '{q}' and clearing selected_theme for theme resolution")
        state["extracted_theme"] = q
        # Use pop to completely remove selected_theme from state
        state.pop("selected_theme", None)
        state["selection_idx"] = None  # Clear any stale button selection
        logger.info(f"AddTheme_node: State after update - extracted_theme: '{state.get('extracted_theme')}', selected_theme: {state.get('selected_theme')}")
        # _append_ai(state, f"Looking for a theme matching “{q}”…")

    else:
        _append_ai(state, "AddTheme: no theme_code or theme_query provided.")

    # Clear the processed intent payload to prevent reprocessing
    state["last_intent_payload"] = {}
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 7. RemoveTheme
# ─────────────────────────────────────────────────────────────────────────────

def RemoveTheme_node(state: lg_State):
    if not state.get("selected_theme"):
        _append_ai(state, "No theme is currently selected.")
        return state
    state["selected_theme"] = None
    state["extracted_theme"] = None
    # CRITICAL: Clear selection_idx when removing theme to prevent stale values
    state["selection_idx"] = None
    logger.info("RemoveTheme_node: Cleared selection_idx to prevent stale values")
    _append_ai(state, "Theme selection cleared.")
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 8. RemovePlace
# ─────────────────────────────────────────────────────────────────────────────

def RemovePlace_node(state: lg_State):
    print("=== URGENT DEBUG: RemovePlace_node ENTRY ===")
    logger.info("RemovePlace_node: removing a place from the selection")
    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}
    place: Optional[str] = args.get("place")
    print(f"=== URGENT DEBUG: RemovePlace_node - place={place}, payload={payload} ===")
    places = state.get("places")

    # EXTRA DEBUG: Log detailed state at entry
    logger.info(f"DEBUG RemovePlace_node: Entry - payload: {payload}")
    logger.info(f"DEBUG RemovePlace_node: Entry - place to remove: {place}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current selected_place_g_units: {state.get('selected_place_g_units', [])}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current selected_polygons: {state.get('selected_polygons', [])}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current places array: {places}")

    # CRITICAL: Clear selection_idx when removing places to prevent stale values
    state["selection_idx"] = None
    logger.info("RemovePlace_node: Cleared selection_idx to prevent stale values")

    if not place:
        _append_ai(state, "Tell me which place to remove, e.g. ‘remove Oxford'.")
        return state

    place_names = state.get("extracted_place_names", [])
    place_lower = place.lower()
    place_names_lower = [p.lower() for p in place_names]
    county_names = state.get("extracted_counties", [])
    county_names = [c.lower() for c in county_names]

    # Find the place in the places list (which contains the full place data)
    g_unit = None
    g_unit_type = None
    g_place = None

    if places:
        for i in places:
            if i.get('name', '').lower() == place_lower:
                g_unit = i.get('g_unit')
                g_unit_type = i.get('g_unit_type')
                g_place = i.get('g_place')
                break

    # Check if the place exists in our selection by name
    place_index = -1
    if place_lower in place_names_lower:
        place_index = place_names_lower.index(place_lower)
    else:
        # If not found by name, check if it's a polygon ID (for polygons without place names)
        selected_units = state.get("selected_place_g_units", [])
        selected_polygons = state.get("selected_polygons", [])

        # Try to match by polygon ID (when place name fallback was used)
        try:
            place_as_id = int(place)
            if place_as_id in selected_units:
                # Find the index in selected_place_g_units
                place_index = selected_units.index(place_as_id)
                # Update the place variable to use a more descriptive name
                place = f"Polygon {place_as_id}"
                logger.info(f"DEBUG RemovePlace_node: Found place by polygon ID {place_as_id} at index {place_index}")
            elif place_as_id in selected_polygons:
                # Also check selected_polygons
                polygon_index = selected_polygons.index(place_as_id)
                # Find corresponding unit if available
                if polygon_index < len(selected_units):
                    place_index = polygon_index
                    place = f"Polygon {place_as_id}"
                    logger.info(f"DEBUG RemovePlace_node: Found place by polygon in selected_polygons at index {place_index}")
        except (ValueError, TypeError):
            # place is not a numeric ID
            logger.info(f"DEBUG RemovePlace_node: Place '{place}' is not a numeric ID")

    if place_index == -1:
        _append_ai(state, f"{place} isn't in your selection.")
        state["last_intent_payload"] = {}
        return Command(goto=END)

    idx = place_index
    # Only remove from place_names if it was found there (not for polygon IDs)
    if idx < len(place_names):
        place_names.pop(idx)
    if idx < len(county_names):
        county_names.pop(idx)

    # CRITICAL: Also remove from extracted_polygon_ids array to prevent stale values
    extracted_polygon_ids = state.get("extracted_polygon_ids", [])
    if place_index >= 0 and place_index < len(extracted_polygon_ids):
        extracted_polygon_ids.pop(place_index)
        state["extracted_polygon_ids"] = extracted_polygon_ids

    # CRITICAL: Properly remove from state arrays using the correct approach
    # Use the place_index to remove from arrays that are indexed by place position
    for key in ("selected_place_g_places", "selected_place_g_units", "selected_place_g_unit_types"):
        lst = state.get(key, [])
        if lst and place_index >= 0 and place_index < len(lst):
            lst.pop(place_index)
            state[key] = lst
            logger.info(f"DEBUG RemovePlace_node: Removed index {place_index} from {key}: {lst}")

    # For polygon arrays, remove by value (not index) since they store actual polygon IDs
    if g_unit is not None:
        selected_polygons = state.get("selected_polygons", []) or []
        selected_polygons_unit_types = state.get("selected_polygons_unit_types", []) or []

        # Remove polygon ID from selected_polygons (compare as integers)
        if selected_polygons and g_unit in selected_polygons:
            polygon_idx = selected_polygons.index(g_unit)
            selected_polygons.pop(polygon_idx)
            # Also remove corresponding unit type
            if selected_polygons_unit_types and polygon_idx < len(selected_polygons_unit_types):
                selected_polygons_unit_types.pop(polygon_idx)
            state["selected_polygons"] = selected_polygons
            state["selected_polygons_unit_types"] = selected_polygons_unit_types
            logger.info(f"DEBUG RemovePlace_node: Removed polygon {g_unit} from polygon arrays")

    # CRITICAL: Also remove the place from the places array to clear resolved state
    if places:
        updated_places = []
        for place_entry in places:
            # Keep places that don't match the one being removed
            if (place_entry.get('name', '').lower() != place_lower and
                place_entry.get('g_unit') != g_unit):
                updated_places.append(place_entry)
        state["places"] = updated_places
        logger.info(f"RemovePlace_node: Removed place '{place}' from places array. {len(places)} -> {len(updated_places)} places")

    remaining_units = state.get("selected_place_g_units", [])
    cubes_filtered = pd.DataFrame(columns=["g_unit"])
    logger.info(f"DEBUG RemovePlace_node: After removal - remaining_units: {remaining_units}")

    # CRITICAL: If no polygons remain, also clear the theme selection
    if not remaining_units:
        logger.info("RemovePlace_node: No polygons remaining - clearing theme selection")
        state["selected_theme"] = None
        state["extracted_theme"] = None
        # Also clear theme hint flag so it can be shown again later
        state["_theme_hint_done"] = False
    if state.get("selected_cubes"):
        try:
            df = pd.read_json(state["selected_cubes"], orient="records")
            if not df.empty and "g_unit" in df.columns:
                df = df[df["g_unit"].isin(remaining_units)]
                cubes_filtered = df
        except Exception:          # defensive - fallback to clearing
            cubes_filtered = pd.DataFrame(columns=["g_unit"])

    # CRITICAL: If we have remaining units but no cube data, generate fresh cube data from theme
    if remaining_units and cubes_filtered.empty and state.get("selected_theme"):
        logger.info(f"DEBUG RemovePlace_node: No existing cube data, generating fresh cubes for {len(remaining_units)} units")
        try:
            from vobchat.tools import find_themes_for_unit
            import json

            # Parse selected theme
            selected_theme = state.get("selected_theme")
            if isinstance(selected_theme, str):
                theme_data = json.loads(selected_theme)
                theme_id = theme_data.get("ent_id")

                if theme_id:
                    # Get cube data for the first remaining unit and theme
                    from vobchat.tools import find_cubes_for_unit_theme
                    cubes_json = find_cubes_for_unit_theme.invoke({"g_unit": str(remaining_units[0]), "theme_id": theme_id})
                    cubes_df = pd.read_json(io.StringIO(cubes_json), orient='records')

                    if not cubes_df.empty:
                        # Tag with unit ID like in the normal workflow
                        cubes_df["g_unit"] = remaining_units[0]
                        cubes_filtered = cubes_df
                        logger.info(f"DEBUG RemovePlace_node: Generated {len(cubes_filtered)} fresh cube rows for unit {remaining_units[0]}, theme {theme_id}")
        except Exception as e:
            logger.warning(f"DEBUG RemovePlace_node: Error generating fresh cube data: {e}")

    # CRITICAL: Show visualization if there are remaining units AND a theme is selected
    # This ensures that when a polygon is removed, the visualization stays visible
    # for the remaining polygons (the visualization callback will refresh the cube data)
    show_viz = bool(remaining_units and state.get("selected_theme"))

    # CRITICAL: Persist the filtered cube data back to state to prevent old data from persisting
    if len(cubes_filtered) > 0:
        cubes_filtered_json = cubes_filtered.to_json(orient="records")
        state["selected_cubes"] = cubes_filtered_json  # Persist filtered cubes
        logger.info(f"RemovePlace_node: Filtered and persisted {len(cubes_filtered)} cube rows for remaining units {remaining_units}")
    else:
        cubes_filtered_json = []
        state["selected_cubes"] = None  # Clear all cube data when no units remain
        logger.info("RemovePlace_node: Cleared all cube data (no remaining units)")

    logger.info(f"RemovePlace_node: show_viz={show_viz} (remaining_units={len(remaining_units)}, has_theme={bool(state.get('selected_theme'))})")

    # CRITICAL: When show_viz=True, we need to either provide cube data or trigger
    # the visualization callback to refresh. If we have filtered cubes, use them.
    # If not, but we still want to show visualization, send empty cubes but ensure
    # show_visualization=True so the callback knows to refresh cube data.
    cubes_to_send = cubes_filtered_json if cubes_filtered_json else ([] if show_viz else None)

    # CRITICAL: Clear last_intent_payload in the actual state to prevent duplicate operations
    state["last_intent_payload"] = {}
    logger.info("RemovePlace_node: Cleared last_intent_payload to prevent duplicate operations")

    interrupt(value={
    "message": f"Removed {place} from the selection.",
    "extracted_place_names": place_names,
    "extracted_counties": county_names,
    "extracted_polygon_ids": state["extracted_polygon_ids"],
    "last_intent_payload": {},
    "selected_place_g_places": state.get("selected_place_g_places", []),
    "selected_place_g_units": state.get("selected_place_g_units", []),
    "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
    # CRITICAL: Include cube data or empty list to trigger visualization refresh
    "cubes": cubes_to_send,
    "show_visualization": show_viz,
    "selected_polygons": state.get("selected_polygons", []),
    "selected_polygons_unit_types": state.get("selected_polygons_unit_types", []),
    "current_node": "select_unit_on_map",
    # CRITICAL: Clear selection_idx in interrupt to ensure it's cleared
    "selection_idx": None,
    # CRITICAL: Include theme clearing in persistent state when last polygon is removed
    "selected_theme": state.get("selected_theme"),
    "extracted_theme": state.get("extracted_theme"),
    "_theme_hint_done": state.get("_theme_hint_done", False),
    })

# ────────────────────────────────────────────────────────────────────────────
# Node: just hint that the theme has a description
#    (no longer dumps the full text automatically)
# ────────────────────────────────────────────────────────────────────────────

def theme_hint_node(state: lg_State):
    """After a theme is picked, nudge the user that they can ask for details."""
    if not state.get("selected_theme"):
        return state  # nothing to do

    # ensure we run only once per theme (idempotent)
    if state.get("_theme_hint_done"):
        return state

    try:
        df = pd.read_json(io.StringIO(state["selected_theme"]), orient="records")
        code  = df["ent_id"].iat[0]
        label = df["labl"].iat[0]
    except Exception:
        return state  # malformed - skip

    msg = (
        f"Selected theme: **{label}** ({code}). "
        "If you'd like the full description just ask “describe theme” or "
        "“what does that theme mean?”."
    )
    state.setdefault("messages", []).append(AIMessage(content=msg))
    state["_theme_hint_done"] = True  # flag so we don't spam on retriggers
    return state

# ────────────────────────────────────────────────────────────────────────────
# Node: fetch & show theme description on demand
# ────────────────────────────────────────────────────────────────────────────

def DescribeTheme_node(state: lg_State):
    """
    Reply with the definition/metadata of a theme.
    • Works even if *no* theme is currently selected.
    • Clears last_intent_payload so the router won't loop.
    """

    payload = state.get("last_intent_payload") or {}
    args    = payload.get("arguments", {})
    query   = (args.get("theme") or "").strip()

    # 1️⃣ Determine the theme code
    theme_df = None

    # a) use already-selected theme (if any)
    if state.get("selected_theme"):
        theme_df = pd.read_json(io.StringIO(state["selected_theme"]), orient="records")

    # b) otherwise, fuzzy-match the query against *all* themes
    if theme_df is None and query:
        all_df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
        mask   = all_df["labl"].str.contains(query, case=False, regex=False)
        if mask.any():
            theme_df = all_df[mask].head(1)

    # c) still nothing → ask a follow-up
    if theme_df is None or theme_df.empty:
        state.setdefault("messages", []).append(
            AIMessage(content="I'm not sure which theme you mean. "
                              "Try e.g. “describe Population” or “describe T_POP”.")
        )
        state["last_intent_payload"] = {}
        return state

    code = theme_df["ent_id"].iat[0]
    labl = theme_df["labl"].iat[0]

    # 2️⃣ Fetch the long description
    desc_df = pd.read_json(io.StringIO(get_theme_text(code)), orient="records")
    text    = desc_df["text"].iat[0] if not desc_df.empty else "(no description available)"

    state.setdefault("messages", []).append(
        AIMessage(content=f"**{labl}** ({code})\n\n{text}")
    )

    # 3️⃣ house-keeping
    state["last_intent_payload"] = {}      # avoid re-routing
    return state

# ─────────────────────────────────────────────────────────────────────────────
# Node: ask for clarification when the user message is ambiguous
# ─────────────────────────────────────────────────────────────────────────────

def ask_followup_node(state: lg_State):
    """Generic follow-up / clarification node.

    ─────────────────────────────────────────────────────────────────────────────
    When the router in *agent_node* cannot map the user message to any of the
    explicit AssistantIntent values it used to jump straight to ``END``.  That
    was a dead-end for the conversation because the agent never got the chance
    to *ask* what the user actually wanted.

    This node fixes that:
    • The first time it runs (``selection_idx`` is *None*) it issues an
      ``interrupt`` with a small set of "quick-action" buttons plus a generic
      clarifying question.
    • When the user clicks one of those buttons the front-end writes
      ``selection_idx`` back into the state.  On re-entry we translate that
      choice into a fake ``last_intent_payload`` and immediately hand control
      back to the normal intent router in ``agent_node``.
    • If the user would rather *type* a clarification instead of clicking a
      button that's fine as well - their next text will be handled by the LLM
      intent-extract routine as usual.

    The quick-action list is deliberately short and generic - add / change them
    as you like.
    """

    already_waiting = (
        state.get("current_node") == "ask_followup_node"
        and state.get("options")                 # buttons were persisted by chat.py
        and state.get("selection_idx") is None   # user hasn't clicked yet
    )
    if already_waiting:
        logger.debug("ask_followup_node: duplicate call → re-issue buttons only.")
        interrupt(value={
            "options": state["options"],          # keep the same buttons alive
            "current_node": "ask_followup_node",
            # CRITICAL: Clear selection_idx through interrupt to prevent stale values
            "selection_idx": None,
        })
        return state

    # ------------------------------------------------------------------
    # 0.  Quick-action catalogue (index → intent name)
    # ------------------------------------------------------------------
    intents: List[str] = [
        "AddPlace",
        "AddTheme",
        "ShowState",
        "Reset",
    ]

    # ------------------------------------------------------------------
    # 1.  User *has* already clicked a button → we have a selection_idx
    # ------------------------------------------------------------------
    if state.get("selection_idx") is not None:
        try:
            idx = int(state["selection_idx"])
            chosen_intent = intents[idx]
        except (ValueError, IndexError):
            # bad index - start over cleanly
            state["selection_idx"] = None
            state["options"] = []
            logger.warning("ask_followup_node: invalid selection_idx - reprompting")
        else:
            logger.info(f"ask_followup_node: user picked quick action → {chosen_intent}")
            # clear one-shot fields before continuing
            state["selection_idx"] = None
            state["options"] = []
            # fake a minimal last_intent_payload so the usual router can do its job
            state["last_intent_payload"] = {"intent": chosen_intent, "arguments": {}}
            # jump right back to agent_node which will re-enter its routing cycle
            return Command(goto="agent_node", update=state)

    # ------------------------------------------------------------------
    # 2.  First entry - ask for clarification using an interrupt
    # ------------------------------------------------------------------
    logger.info("ask_followup_node: issuing clarification interrupt")

    quick_buttons = [
        {
            "option_type": "intent",
            "label": "Add a place",
            "value": 0,  # matches index in *intents* list
            "color": "#333",
        },
        {
            "option_type": "intent",
            "label": "Add a theme",
            "value": 1,
            "color": "#333",
        },
        {
            "option_type": "intent",
            "label": "Show current selection",
            "value": 2,
            "color": "#333",
        },
        {
            "option_type": "intent",
            "label": "Reset everything",
            "value": 3,
            "color": "#333",
        },
    ]

    interrupt(
        value={
            "message": "I'm not entirely sure what you need. Choose one of the quick actions below or rephrase your request:",
            "options": quick_buttons,
            "current_node": "ask_followup_node",
            # CRITICAL: Clear selection_idx through interrupt to prevent stale values
            "selection_idx": None,
        }
    )

    # Execution pauses here.  Front-end shows the buttons, user picks one, and
    # the graph will re-enter this node with selection_idx set.
    return state
# ─────────────────────────────────────────────────────────────────────────────
