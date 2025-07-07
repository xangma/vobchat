"""Place-related nodes: AddPlace, RemovePlace, and place tool calls."""
from __future__ import annotations
import io
from typing import List, Optional
import pandas as pd
from langchain_core.messages import AIMessage
from langgraph.types import Command
from vobchat.state_schema import (
    lg_State, 
    get_selected_units, 
    add_place_to_state, 
    remove_place_from_state
)
from vobchat.tools import find_units_by_postcode, find_places_by_name
from .utils import _append_ai
import logging

logger = logging.getLogger(__name__)

def AddPlace_node(state: lg_State):
    """Add one or more places to the selection."""
    logger.info("=== AddPlace_node ENTRY ===")
    args = (state.get("last_intent_payload") or {}).get("arguments", {})
    logger.info(f"AddPlace_node: Args received: {args}")

    # ── gather place names to add ───────────────────────────────────
    names_to_add: List[str] = []
    counties_to_add: List[str] = []
    unit_types_to_add: List[str] = []
    polygon_ids_to_add: List[Optional[int]] = []
    if "places" in args and isinstance(args["places"], list):
        names_to_add = [str(p).strip() for p in args["places"] if str(p).strip()]
    elif "place" in args:
        place_str = str(args["place"]).strip() if args["place"] is not None else ""
        if place_str:
            names_to_add = [place_str]
    if "counties" in args and isinstance(args["counties"], list):
        counties_to_add = [p.strip() for p in args["counties"] if p.strip()]
    elif "county" in args:
        counties_to_add = [args["county"].strip()]
    if "unit_type" in args:
        unit_types_to_add = [args["unit_type"].strip()]
    # Extract polygon_id if provided (from map clicks)
    if "polygon_id" in args:
        polygon_ids_to_add = [args["polygon_id"]]
    # If no names but we have polygon_id (from map click), use polygon_id as the name
    if not names_to_add and polygon_ids_to_add:
        logger.info(f"AddPlace_node: No place name provided, using polygon_id {polygon_ids_to_add[0]} as place name")
        names_to_add = [f"Polygon {polygon_ids_to_add[0]}"]

    if not names_to_add:
        logger.warning(f"AddPlace_node: No place names or polygon_id provided. Args: {args}")
        _append_ai(state, "AddPlace: please specify at least one place name.")
        return state

    # ── Check for duplicates using simplified state BEFORE creating message ─────────
    existing_units = get_selected_units(state)

    # Check if this polygon is already selected (duplicate detection)
    if "polygon_id" in args and args["polygon_id"] in existing_units:
        logger.info(f"AddPlace_node: Polygon {args['polygon_id']} is already selected, preventing duplicate processing")
        # Clear last_intent_payload to prevent workflow looping
        # Also clear any stale resolve_theme state that might cause recursion
        state["last_intent_payload"] = {}
        state["current_node"] = None
        # state["options"] = []

        state["extracted_theme"] = None  # Clear to prevent theme reprocessing
        logger.info("AddPlace_node: Cleared stale state to prevent recursion loops")
        return state

    # ── Add places to the single source of truth ─────────────────────────────────
    plural = ", ".join(names_to_add)
    _append_ai(state, f"Okay - adding {plural}. Let me find them …")

    # Add each place to the state using the simplified approach
    for idx, place_name in enumerate(names_to_add):
        polygon_id = polygon_ids_to_add[idx] if idx < len(polygon_ids_to_add) else None
        unit_type = unit_types_to_add[idx] if idx < len(unit_types_to_add) else None

        add_place_to_state(
            state,
            name=place_name,
            g_unit=polygon_id,
            g_unit_type=unit_type
        )
        logger.info(f"AddPlace_node: Added place '{place_name}' with g_unit={polygon_id}, g_unit_type='{unit_type}'")

    # Clear payload and extracted theme
    update = {
        "last_intent_payload": {},
        "extracted_theme": None,  # Clear to prevent theme reprocessing
        "current_place_index": 0,  # Start processing from first place
        # CRITICAL: Include places array to preserve it across Command
        "places": state.get("places", []),
    }

    return Command(goto="multi_place_tool_call", update=update)

def RemovePlace_node(state: lg_State):
    """Remove a place from the selection."""
    logger.debug("=== URGENT DEBUG: RemovePlace_node ENTRY ===")
    logger.info("RemovePlace_node: removing a place from the selection")
    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}
    place: Optional[str] = args.get("place")
    logger.debug(f"=== URGENT DEBUG: RemovePlace_node - place={place}, payload={payload} ===")
    places = state.get("places")

    # EXTRA DEBUG: Log detailed state at entry
    logger.info(f"DEBUG RemovePlace_node: Entry - payload: {payload}")
    logger.info(f"DEBUG RemovePlace_node: Entry - place to remove: {place}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current selected_units_from_places: {get_selected_units(state)}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current selected_polygons: {state.get('selected_polygons', [])}")
    logger.info(f"DEBUG RemovePlace_node: Entry - current places array: {places}")

    if not place:
        _append_ai(state, "Tell me which place to remove, e.g. 'remove Oxford'.")
        return state

    # Try to remove the place using simplified state
    place_identifier = place
    place_lower = place.lower()

    # If the place name is a number, treat it as a polygon ID
    try:
        place_as_id = int(place)
        place_identifier = place_as_id
        place = f"Polygon {place_as_id}"  # Update display name
        logger.info(f"RemovePlace_node: Treating '{place}' as polygon ID {place_as_id}")
    except (ValueError, TypeError):
        logger.info(f"RemovePlace_node: Treating '{place}' as place name")

    # Remove using the simplified state helper
    was_removed = remove_place_from_state(state, place_identifier)

    if not was_removed:
        _append_ai(state, f"{place} isn't in your selection.")

        # Pass only the fields that need to be updated
        update_dict = {
            "last_intent_payload": {},
            # "current_node": None,
            # "options": [],
            "map_update_request": {
                "action": "update_map_selection",
                "places": state.get("places", [])  # Maintain current places array
            }
        }
        return Command(goto="END", update=update_dict)

    logger.info(f"RemovePlace_node: Successfully removed '{place}'")

    # Check remaining places and handle theme preservation
    remaining_units = get_selected_units(state)
    cubes_filtered = pd.DataFrame(columns=["g_unit"])
    logger.info(f"RemovePlace_node: After removal - remaining_units: {remaining_units}")

    # If no polygons remain, preserve theme but allow re-selection
    if not remaining_units:
        logger.info("RemovePlace_node: No polygons remaining - preserving theme for re-selection")
        # Don't clear selected_theme - let it persist for re-selection
        # Only clear extracted_theme (LLM extraction state) and allow theme hint to show again
        state["extracted_theme"] = None
        logger.info("RemovePlace_node: Cleared extraction state to allow fresh re-selection flow")
    if state.get("selected_cubes"):
        try:
            df = pd.read_json(state["selected_cubes"], orient="records")
            if not df.empty and "g_unit" in df.columns:
                df = df[df["g_unit"].isin(remaining_units)]
                cubes_filtered = df
        except Exception:          # defensive - fallback to clearing
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

    # Add the removal message to the conversation
    _append_ai(state, f"Removed {place} from the selection.")

    # Pass only the fields that need to be updated
    # CRITICAL: Include all state changes made in this function
    update_dict = {
        "last_intent_payload": {},
        # "current_node": None,
        # "options": [],
        "map_update_request": {
            "action": "update_map_selection",
            "places": state.get("places", [])  # Send updated places array to ensure map sync
        },
        "selected_cubes": state.get("selected_cubes"),  # Include the filtered cube data
        "show_visualization": show_viz,  # CRITICAL: Include show_viz flag to control visualization visibility
        "places": state.get("places", []),  # CRITICAL: Include updated places array for frontend
    }

    return Command(goto="END", update=update_dict)

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
        return state

    # Call the tool to find units by postcode.
    logger.info(f"Searching for units with postcode: {extracted_postcode}")
    json_result = find_units_by_postcode.invoke({"postcode": extracted_postcode})

    # Parse the result.
    try:
        df = pd.read_json(io.StringIO(json_result), orient="records")
        logger.info(f"Postcode search returned {len(df)} results")
    except Exception as e:
        logger.error(f"Error parsing postcode search results: {e}")
        state["messages"].append(
            AIMessage(
                content=f"Sorry, there was an error searching for postcode {extracted_postcode}.",
                response_metadata={"stream_mode": "stream"}
            )
        )
        return state

    if df.empty:
        logger.info(f"No results found for postcode: {extracted_postcode}")
        state["messages"].append(
            AIMessage(
                content=f"I couldn't find any places for postcode {extracted_postcode}.",
                response_metadata={"stream_mode": "stream"}
            )
        )
        return state

    # Store results in the single source of truth
    # For postcodes, we typically select all matching units
    places = state.get("places", []) or []
    
    for _, row in df.iterrows():
        add_place_to_state(
            state,
            name=row.get("name", f"Unit {row.get('g_unit', '')}"),
            g_unit=row.get("g_unit"),
            g_unit_type=row.get("type_label")
        )
    
    logger.info(f"Added {len(df)} places from postcode search to state")
    
    # Provide feedback to user
    if len(df) == 1:
        row = df.iloc[0]
        state["messages"].append(
            AIMessage(
                content=f"Found {row['name']} ({row['type_label']}) for postcode {extracted_postcode}.",
                response_metadata={"stream_mode": "stream"}
            )
        )
    else:
        names = [f"{row['name']} ({row['type_label']})" for _, row in df.head(5).iterrows()]
        if len(df) > 5:
            names.append(f"and {len(df) - 5} more...")
        state["messages"].append(
            AIMessage(
                content=f"Found {len(df)} places for postcode {extracted_postcode}:\n" + "\n".join(f"• {name}" for name in names),
                response_metadata={"stream_mode": "stream"}
            )
        )

    # Clear the postcode from state to prevent re-processing
    state["extracted_postcode"] = None
    state["is_postcode"] = False
    
    return state

def multi_place_tool_call(state: lg_State) -> lg_State:
    """
    Build state["places"] = [
        { "name": str,
          "candidate_rows": [ {...DB row...}, … ],
          "g_place": None, "g_unit": None, "g_unit_type": None }
    ]
    The heavy lifting (disambiguation / map prompt) is done by
    resolve_place_and_unit().
    """
    logger.info("multi_place_tool_call – searching DB for each place")
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
    logger.info("multi_place_tool_call: Completed place processing using simplified state")
    return state