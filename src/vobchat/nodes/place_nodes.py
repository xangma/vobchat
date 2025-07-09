# """Place-related nodes: AddPlace, RemovePlace, and place tool calls."""
# from __future__ import annotations
# import io
# from typing import List, Optional
# import pandas as pd
# from langgraph.types import Command
# from vobchat.state_schema import (
#     lg_State,
#     get_selected_units,
#     add_place_to_state,
#     remove_place_from_state
# )
# from vobchat.tools import find_units_by_postcode, find_places_by_name
# from .utils import _append_ai
# import logging

# logger = logging.getLogger(__name__)

# def AddPlace_node(state: lg_State) -> dict | Command:
#     """Add one or more places to the selection."""
#     logger.info("=== AddPlace_node ENTRY ===")
#     args = (state.get("last_intent_payload") or {}).get("arguments", {})
#     logger.info(f"AddPlace_node: Args received: {args}")

#     # ── gather place names to add ───────────────────────────────────
#     names_to_add: List[str] = []
#     counties_to_add: List[str] = []
#     unit_types_to_add: List[str] = []
#     polygon_ids_to_add: List[Optional[int]] = []
#     if "places" in args and isinstance(args["places"], list):
#         names_to_add = [str(p).strip() for p in args["places"] if str(p).strip()]
#     elif "place" in args:
#         place_str = str(args["place"]).strip() if args["place"] is not None else ""
#         if place_str:
#             names_to_add = [place_str]
#     if "counties" in args and isinstance(args["counties"], list):
#         counties_to_add = [p.strip() for p in args["counties"] if p.strip()]
#     elif "county" in args:
#         counties_to_add = [args["county"].strip()]
#     if "unit_type" in args:
#         unit_types_to_add = [args["unit_type"].strip()]
#     # Extract polygon_id if provided (from map clicks)
#     if "polygon_id" in args:
#         polygon_ids_to_add = [args["polygon_id"]]
#     # If no names but we have polygon_id (from map click), use polygon_id as the name
#     if not names_to_add and polygon_ids_to_add:
#         logger.info(f"AddPlace_node: No place name provided, using polygon_id {polygon_ids_to_add[0]} as place name")
#         names_to_add = [f"Polygon {polygon_ids_to_add[0]}"]

#     if not names_to_add:
#         logger.warning(f"AddPlace_node: No place names or polygon_id provided. Args: {args}")
#         _append_ai(state, "AddPlace: please specify at least one place name.")
#         return {"messages": state.get("messages", [])}

#     # ── Check for duplicates using simplified state BEFORE creating message ─────────
#     # existing_units = get_selected_units(state)

#     # Check if this polygon is already selected (duplicate detection)
#     # if "polygon_id" in args and args["polygon_id"] in existing_units:
#     #     logger.info(f"AddPlace_node: Polygon {args['polygon_id']} is already selected, preventing duplicate processing")
#     #     # Clear last_intent_payload to prevent workflow looping
#     #     # Also clear any stale resolve_theme state that might cause recursion
#     #     state["last_intent_payload"] = {}
#     #     state["current_node"] = None
#     #     # state["options"] = []

#     #     logger.info("AddPlace_node: Cleared stale state to prevent recursion loops")
#     #     return {
#     #         "last_intent_payload": {},
#     #         "current_node": None,
#     #         "extracted_theme": None
#     #     }

#     # ── Add places to the single source of truth ─────────────────────────────────
#     plural = ", ".join(names_to_add)
#     _append_ai(state, f"Okay - adding {plural}. Let me find them …")

#     # Add each place to the state using the simplified approach
#     for idx, place_name in enumerate(names_to_add):
#         polygon_id = polygon_ids_to_add[idx] if idx < len(polygon_ids_to_add) else None
#         unit_type = unit_types_to_add[idx] if idx < len(unit_types_to_add) else None

#         add_place_to_state(
#             state,
#             name=place_name,
#             g_unit=polygon_id,
#             g_unit_type=unit_type
#         )
#         logger.info(f"AddPlace_node: Added place '{place_name}' with g_unit={polygon_id}, g_unit_type='{unit_type}'")

#     # Clear payload and extracted theme
#     update = {
#         "last_intent_payload": {},
#         "extracted_theme": None,  # Clear to prevent theme reprocessing
#         "current_place_index": 0,  # Start processing from first place
#         # CRITICAL: Include places array to preserve it across Command
#         "places": state.get("places", []),
#     }

#     return Command(goto="multi_place_tool_call", update=update)

# def RemovePlace_node(state: lg_State) -> dict | Command:
#     """Remove a place from the selection."""
#     logger.debug("=== URGENT DEBUG: RemovePlace_node ENTRY ===")
#     logger.info("RemovePlace_node: removing a place from the selection")
#     payload = state.get("last_intent_payload", {})
#     args = payload.get("arguments", {}) if payload else {}
#     place: Optional[str] = args.get("place")
#     logger.debug(f"=== URGENT DEBUG: RemovePlace_node - place={place}, payload={payload} ===")
#     places = state.get("places")

#     # EXTRA DEBUG: Log detailed state at entry
#     logger.info(f"DEBUG RemovePlace_node: Entry - payload: {payload}")
#     logger.info(f"DEBUG RemovePlace_node: Entry - place to remove: {place}")
#     logger.info(f"DEBUG RemovePlace_node: Entry - current selected_units_from_places: {get_selected_units(state)}")
#     logger.info(f"DEBUG RemovePlace_node: Entry - current selected_polygons: {state.get('selected_polygons', [])}")
#     logger.info(f"DEBUG RemovePlace_node: Entry - current places array: {places}")

#     if not place:
#         _append_ai(state, "Tell me which place to remove, e.g. 'remove Oxford'.")
#         return {"messages": state.get("messages", [])}

#     # Try to remove the place using simplified state
#     place_identifier = place
#     place_lower = place.lower()

#     # If the place name is a number, treat it as a polygon ID
#     try:
#         place_as_id = int(place)
#         place_identifier = place_as_id
#         place = f"Polygon {place_as_id}"  # Update display name
#         logger.info(f"RemovePlace_node: Treating '{place}' as polygon ID {place_as_id}")
#     except (ValueError, TypeError):
#         logger.info(f"RemovePlace_node: Treating '{place}' as place name")

#     # Remove using the simplified state helper
#     was_removed = remove_place_from_state(state, place_identifier)

#     if not was_removed:
#         _append_ai(state, f"{place} isn't in your selection.")

#         # Pass only the fields that need to be updated
#         update_dict = {
#             "last_intent_payload": {},
#             # "current_node": None,
#             # "options": [],
#             "map_update_request": {
#                 "action": "update_map_selection",
#                 "places": state.get("places", [])  # Maintain current places array
#             },
#             "units_needing_map_selection": [],  # CRITICAL: Clear any pending map selections
#         }
#         return Command(goto="END", update=update_dict)

#     logger.info(f"RemovePlace_node: Successfully removed '{place}'")

#     # Check remaining places and handle theme preservation
#     remaining_units = get_selected_units(state)
#     cubes_filtered = pd.DataFrame(columns=["g_unit"])
#     logger.info(f"RemovePlace_node: After removal - remaining_units: {remaining_units}")

#     # If no polygons remain, preserve theme but allow re-selection
#     if not remaining_units:
#         logger.info("RemovePlace_node: No polygons remaining - preserving theme for re-selection")
#         # Don't clear selected_theme - let it persist for re-selection
#         # Only clear extracted_theme (LLM extraction state) and allow theme hint to show again
#         state["extracted_theme"] = None
#         logger.info("RemovePlace_node: Cleared extraction state to allow fresh re-selection flow")
#     if state.get("selected_cubes"):
#         try:
#             df = pd.read_json(state["selected_cubes"], orient="records")
#             if not df.empty and "g_unit" in df.columns:
#                 df = df[df["g_unit"].isin(remaining_units)]
#                 cubes_filtered = df
#         except Exception:          # defensive - fallback to clearing
#             cubes_filtered = pd.DataFrame(columns=["g_unit"])

#     # CRITICAL: If we have remaining units but no cube data, generate fresh cube data from theme
#     if remaining_units and cubes_filtered.empty and state.get("selected_theme"):
#         logger.info(f"DEBUG RemovePlace_node: No existing cube data, generating fresh cubes for {len(remaining_units)} units")
#         try:
#             from vobchat.tools import find_themes_for_unit
#             import json

#             # Parse selected theme
#             selected_theme = state.get("selected_theme")
#             if isinstance(selected_theme, str):
#                 theme_data = json.loads(selected_theme)
#                 theme_id = theme_data.get("ent_id")

#                 if theme_id:
#                     # Get cube data for the first remaining unit and theme
#                     from vobchat.tools import find_cubes_for_unit_theme
#                     cubes_json = find_cubes_for_unit_theme.invoke({"g_unit": str(remaining_units[0]), "theme_id": theme_id})
#                     cubes_df = pd.read_json(io.StringIO(cubes_json), orient='records')

#                     if not cubes_df.empty:
#                         # Tag with unit ID like in the normal workflow
#                         cubes_df["g_unit"] = remaining_units[0]
#                         cubes_filtered = cubes_df
#                         logger.info(f"DEBUG RemovePlace_node: Generated {len(cubes_filtered)} fresh cube rows for unit {remaining_units[0]}, theme {theme_id}")
#         except Exception as e:
#             logger.warning(f"DEBUG RemovePlace_node: Error generating fresh cube data: {e}")

#     # CRITICAL: Show visualization if there are remaining units AND a theme is selected
#     # This ensures that when a polygon is removed, the visualization stays visible
#     # for the remaining polygons (the visualization callback will refresh the cube data)
#     show_viz = bool(remaining_units and state.get("selected_theme"))

#     # CRITICAL: Persist the filtered cube data back to state to prevent old data from persisting
#     if len(cubes_filtered) > 0:
#         cubes_filtered_json = cubes_filtered.to_json(orient="records")
#         state["selected_cubes"] = cubes_filtered_json  # Persist filtered cubes
#         logger.info(f"RemovePlace_node: Filtered and persisted {len(cubes_filtered)} cube rows for remaining units {remaining_units}")
#     else:
#         cubes_filtered_json = []
#         state["selected_cubes"] = None  # Clear all cube data when no units remain
#         logger.info("RemovePlace_node: Cleared all cube data (no remaining units)")

#     logger.info(f"RemovePlace_node: show_viz={show_viz} (remaining_units={len(remaining_units)}, has_theme={bool(state.get('selected_theme'))})")

#     # Add the removal message to the conversation
#     _append_ai(state, f"Removed {place} from the selection.")

#     # Pass only the fields that need to be updated
#     # CRITICAL: Include all state changes made in this function
#     update_dict = {
#         "last_intent_payload": {},
#         # "current_node": None,
#         # "options": [],
#         "map_update_request": {
#             "action": "update_map_selection",
#             "places": state.get("places", [])  # Send updated places array to ensure map sync
#         },
#         "selected_cubes": state.get("selected_cubes"),  # Include the filtered cube data
#         "show_visualization": show_viz,  # CRITICAL: Include show_viz flag to control visualization visibility
#         "places": state.get("places", []),  # CRITICAL: Include updated places array for frontend
#         "units_needing_map_selection": [],  # CRITICAL: Clear any pending map selections after removal
#     }

#     return Command(goto="END", update=update_dict)

# def postcode_tool_call(state: lg_State) -> dict:
#     """
#     If a postcode was previously extracted (`extracted_postcode` is set), this node calls
#     the `find_units_by_postcode` tool to search the database for matching geographical units.
#     Updates the state with the search results in the `places` array (single source of truth).
#     """
#     logger.info("Starting postcode tool call...")
#     state["current_node"] = "postcode_tool_call"
#     logger.debug({"current_state": state})

#     # Get the postcode from the state.
#     extracted_postcode = state.get("extracted_postcode")
#     if not extracted_postcode:
#         # If no postcode is present (shouldn't happen if routed correctly, but good practice to check).
#         logger.warning("No valid postcode found in state for postcode_tool_call")
#         _append_ai(state, "I couldn't find a postcode to search for.")
#         return {"messages": state.get("messages", [])}

#     # Call the tool to find units by postcode.
#     logger.info(f"Searching for units with postcode: {extracted_postcode}")
#     json_result = find_units_by_postcode.invoke({"postcode": extracted_postcode})

#     # Parse the result.
#     try:
#         df = pd.read_json(io.StringIO(json_result), orient="records")
#         logger.info(f"Postcode search returned {len(df)} results")
#     except Exception as e:
#         logger.error(f"Error parsing postcode search results: {e}")
#         _append_ai(state, f"Sorry, there was an error searching for postcode {extracted_postcode}.")
#         return {"messages": state.get("messages", [])}

#     if df.empty:
#         logger.info(f"No results found for postcode: {extracted_postcode}")
#         _append_ai(state, f"I couldn't find any places for postcode {extracted_postcode}.")
#         return {"messages": state.get("messages", [])}

#     # Store results in the single source of truth
#     # For postcodes, we typically select all matching units
#     places = state.get("places", []) or []

#     for _, row in df.iterrows():
#         add_place_to_state(
#             state,
#             name=row.get("name", f"Unit {row.get('g_unit', '')}"),
#             g_unit=row.get("g_unit"),
#             g_unit_type=row.get("type_label")
#         )

#     logger.info(f"Added {len(df)} places from postcode search to state")

#     # Provide feedback to user
#     if len(df) == 1:
#         row = df.iloc[0]
#         _append_ai(state, f"Found {row['name']} ({row['type_label']}) for postcode {extracted_postcode}.")
#     else:
#         names = [f"{row['name']} ({row['type_label']})" for _, row in df.head(5).iterrows()]
#         if len(df) > 5:
#             names.append(f"and {len(df) - 5} more...")
#         _append_ai(state, f"Found {len(df)} places for postcode {extracted_postcode}:\n" + "\n".join(f"• {name}" for name in names))

#     # Return only the fields this node updates
#     return {
#         "messages": state.get("messages", []),
#         "places": state.get("places", []),  # Include updated places array
#         "extracted_postcode": None,  # Clear the postcode from state to prevent re-processing
#         "is_postcode": False,
#         "current_node": "postcode_tool_call"
#     }

# def multi_place_tool_call(state: lg_State) -> dict:
#     """
#     Build state["places"] = [
#         { "name": str,
#           "candidate_rows": [ {...DB row...}, … ],
#           "g_place": None, "g_unit": None, "g_unit_type": None }
#     ]
#     The heavy lifting (disambiguation / map prompt) is done by
#     resolve_place_and_unit().
#     """
#     logger.info("multi_place_tool_call – searching DB for each place")
#     # Get places from the single source of truth
#     places = state.get("places", []) or []

#     # Process database lookups for each place in the simplified state
#     logger.info(f"multi_place_tool_call: Processing {len(places)} places from state")

#     for place in places:
#         place_name = place.get("name", "")
#         unit_type = place.get("g_unit_type", "0")

#         # Skip database lookup if we already have g_unit (e.g., from map click)
#         if place.get("g_unit") is not None:
#             logger.info(f"multi_place_tool_call: Skipping database lookup for '{place_name}' - already have g_unit {place.get('g_unit')}")
#             continue

#         # Only do database lookup if we don't have candidate_rows yet
#         if not place.get("candidate_rows"):
#             try:
#                 df = pd.read_json(
#                     io.StringIO(
#                         find_places_by_name.invoke({
#                             "place_name": place_name,
#                             "county": "0",
#                             "unit_type": unit_type or "0"
#                         })
#                     ),
#                     orient="records",
#                 )
#                 place["candidate_rows"] = df.to_dict("records")
#                 logger.info(f"multi_place_tool_call: Found {len(place['candidate_rows'])} candidates for '{place_name}'")
#             except Exception as exc:
#                 logger.error(f"DB error searching '{place_name}': {exc}", exc_info=True)
#                 place["candidate_rows"] = []

#     # Return only the fields this node updates
#     logger.info("multi_place_tool_call: Completed place processing using simplified state")
#     return {"places": places}

# place_nodes.py – streamlined place‑handling for LangGraph
# ============================================================
# Exports four nodes:
#   • **AddPlace_node**         – add one or more places / polygons
#   • **RemovePlace_node**      – remove a place / polygon
#   • **postcode_tool_call**    – lookup places by UK‑style postcode
#   • **multi_place_tool_call** – DB lookup for every queued place name
#
# Utility assumptions:
#   _append_ai(), get_selected_units(), add_place_to_state(),
#   remove_place_from_state(), find_units_by_postcode.invoke(),
#   find_places_by_name.invoke(), interrupt(), Command.
# ============================================================

from __future__ import annotations

import io
import logging
from typing import Dict, List, Optional, Union

import pandas as pd
from langgraph.types import Command, interrupt  # type: ignore

from vobchat.state_schema import (
    lg_State,
    get_selected_units,
    add_place_to_state,
    remove_place_from_state,
)
from vobchat.tools import find_units_by_postcode, find_places_by_name
from .utils import _append_ai

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper – squeeze cube cache after a removal
# -----------------------------------------------------------------------------


def _filter_cubes(state: lg_State, remaining_units: List[int]):
    """Trim *state['selected_cubes']* down to *remaining_units* (in‑place)."""
    cubes_json = state.get("selected_cubes")
    if not cubes_json:
        return None
    try:
        df = pd.read_json(io.StringIO(cubes_json), orient="records")
    except ValueError:
        return None
    if "g_unit" not in df.columns:
        return None
    df = df[df["g_unit"].isin(remaining_units)]
    if df.empty:
        state["selected_cubes"] = None
        return None
    filtered = df.to_json(orient="records")
    state["selected_cubes"] = filtered
    return filtered

# -----------------------------------------------------------------------------
# Node – AddPlace_node
# -----------------------------------------------------------------------------


def AddPlace_node(state: lg_State) -> Dict[str, Union[str, list, dict]] | Command:
    args = (state.get("last_intent_payload") or {}).get("arguments", {})

    # 1️⃣  Harvest user parameters ------------------------------------------------
    names: List[str] = []
    polygon_ids: List[Optional[int]] = []
    unit_types: List[str] = []

    if isinstance(args.get("places"), list):
        names = [str(p).strip() for p in args["places"] if str(p).strip()]
    elif args.get("place") is not None:
        name = str(args["place"]).strip()
        if name:
            names = [name]

    if "polygon_id" in args:
        polygon_ids = [args["polygon_id"]]
    if "unit_type" in args:
        unit_types = [str(args["unit_type"]).strip()]

    # Allow bare polygon clicks
    if not names and polygon_ids:
        names = [f"Polygon {polygon_ids[0]}"]

    if not names:
        _append_ai(state, "AddPlace: please tell me which place to add.")
        return {"messages": state.get("messages", [])}

    # 2️⃣  Add to state -----------------------------------------------------------
    plural = ", ".join(names)
    _append_ai(state, f"Okay – adding {plural}. Let me look them up…")

    for i, nm in enumerate(names):
        add_place_to_state(
            state,
            name=nm,
            g_unit=polygon_ids[i] if i < len(polygon_ids) else None,
            g_unit_type=unit_types[i] if i < len(unit_types) else None,
        )

    return Command(
        goto="multi_place_tool_call",
        update={
            "last_intent_payload": {},
            "extracted_theme": None,
            "current_place_index": 0,
            "places": state.get("places", []),
        },
    )

# -----------------------------------------------------------------------------
# Node – RemovePlace_node
# -----------------------------------------------------------------------------


def RemovePlace_node(state: lg_State) -> Dict[str, Union[str, list, dict]] | Command:
    args = (state.get("last_intent_payload") or {}).get("arguments", {})
    target_raw: Optional[str] = args.get("place")

    if not target_raw:
        _append_ai(state, "Tell me which place to remove, e.g. ‘remove Oxford’.")
        return {"messages": state.get("messages", [])}

    # Accept either name or integer polygon id
    try:
        target_id = int(target_raw)
        target = target_id
        display = f"Polygon {target_id}"
    except (ValueError, TypeError):
        target = target_raw
        display = target_raw

    removed = remove_place_from_state(state, target)
    if not removed:
        _append_ai(state, f"{display} isn’t in your selection.")
        return {"messages": state.get("messages", [])}

    remaining_units = get_selected_units(state)
    cubes_json = _filter_cubes(state, remaining_units)

    show_viz = bool(remaining_units and state.get(
        "selected_theme") and cubes_json)

    _append_ai(state, f"Removed {display}.")

    return Command(
        goto="multi_place_tool_call",
        update={
            "last_intent_payload": {},
            "places": state.get("places", []),
            "selected_cubes": cubes_json,
            "show_visualization": show_viz,
            "map_update_request": {
                "action": "update_map_selection",
                "places": state.get("places", []),
            },
        }
    )

# -----------------------------------------------------------------------------
# Node – postcode_tool_call
# -----------------------------------------------------------------------------


def postcode_tool_call(state: lg_State):
    pcode = state.get("extracted_postcode")
    if not pcode:
        _append_ai(state, "I couldn’t find a postcode to search for.")
        return {"messages": state.get("messages", [])}

    json_res = find_units_by_postcode.invoke({"postcode": pcode})
    try:
        df = pd.read_json(io.StringIO(json_res), orient="records")
    except ValueError:
        _append_ai(state, f"Sorry – postcode lookup for {pcode} failed.")
        return {"messages": state.get("messages", [])}

    if df.empty:
        _append_ai(state, f"No places found for postcode {pcode}.")
        return {"messages": state.get("messages", [])}

    for _, row in df.iterrows():
        add_place_to_state(
            state,
            name=row.get("name", f"Unit {row.get('g_unit')}"),
            g_unit=row.get("g_unit"),
            g_unit_type=row.get("type_label"),
        )

    if len(df) == 1:
        row = df.iloc[0]
        _append_ai(
            state, f"Found {row['name']} ({row['type_label']}) for postcode {pcode}.")
    else:
        _append_ai(state, f"Added {len(df)} places for postcode {pcode}.")

    return {
        "messages": state.get("messages", []),
        "places": state.get("places", []),
        "extracted_postcode": None,
        "is_postcode": False,
    }

# -----------------------------------------------------------------------------
# Node – multi_place_tool_call
# -----------------------------------------------------------------------------


def multi_place_tool_call(state: lg_State):
    """DB lookup for each place without a resolved g_unit."""

    places = state.get("places", [])
    for entry in places:
        if entry.get("g_unit") is not None or entry.get("candidate_rows"):
            continue  # already resolved / looked up
        name = entry.get("name", "")
        unit_type = entry.get("g_unit_type", "0") or "0"
        try:
            res_json = find_places_by_name.invoke({
                "place_name": name,
                "county": "0",
                "unit_type": unit_type,
            })
            df = pd.read_json(io.StringIO(res_json), orient="records")
            entry["candidate_rows"] = df.to_dict("records")
        except Exception as exc:  # noqa: BLE001
            logger.warning("DB lookup failed for ‘%s’: %s", name, exc)
            entry["candidate_rows"] = []

    return {"places": places}
