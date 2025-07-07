"""Workflow orchestration nodes for place resolution and map interaction."""
from __future__ import annotations
import logging
from typing import Optional
# AIMessage import removed - using _append_ai utility instead
from langgraph.types import interrupt, Command
from vobchat.state_schema import lg_State, get_selected_units, get_selected_unit_types
from vobchat.utils.constants import UNIT_TYPES

logger = logging.getLogger(__name__)


def update_polygon_selection(state: lg_State) -> lg_State:
    """
    Node that ONLY updates polygon selection state - no interrupts.
    This handles map state updates that are safe to re-execute.
    """
    logger.debug("=== WORKFLOW TRACE: update_polygon_selection function called! ===")
    logger.info("Node: update_polygon_selection entered.")

    # Get the current state
    current_place_index = state.get("current_place_index")
    places = state.get("places", []) or []
    selected_workflow_units = get_selected_units(state)

    logger.info(f"URGENT DEBUG: update_polygon_selection state - current_place_index={current_place_index}, total_places={len(places)}, selected_units={selected_workflow_units}")

    if selected_workflow_units:
        # Get the list of units selected *by the user on the map* (from frontend state).
        selected_map_polygons = state.get("selected_polygons", []) or []
        selected_map_polygons_str = [str(p) for p in selected_map_polygons]

        # Find missing units that need to be highlighted on the map
        missing_units = []
        for i, unit_id in enumerate(selected_workflow_units):
            if str(unit_id) not in selected_map_polygons_str:
                missing_units.append((i, unit_id))
                logger.info(f"URGENT DEBUG: Missing unit {unit_id} at index {i}")

        logger.info(f"URGENT DEBUG: Total missing units: {len(missing_units)}")

        if missing_units:
            # Store which units need map highlighting for the next node
            units_list = [unit for _, unit in missing_units]
            logger.info(f"URGENT DEBUG: Found {len(missing_units)} units needing map selection: {units_list}")
        else:
            # No missing units
            logger.info(f"URGENT DEBUG: No units need map selection")
    else:
        logger.info(f"URGENT DEBUG: No workflow units selected yet")

    # CRITICAL: Don't return any state updates to avoid conflicts
    # resolve_place_and_unit already set units_needing_map_selection
    # This node just analyzes the current state for routing decisions
    logger.error(f"🔍 update_polygon_selection RETURNING NO FIELDS (analysis only)")
    return {}


def check_map_selection_needed_router(state: lg_State) -> str:
    """
    Router function that decides if user map interaction is needed.
    Returns the next node to execute.
    """
    logger.debug("=== WORKFLOW TRACE: check_map_selection_needed_router function called! ===")

    # Check if there are units that still need to be highlighted / confirmed
    units_needing_map_selection = state.get(
        "units_needing_map_selection", []) or []

    current_place_index = state.get("current_place_index")
    places = state.get("places", []) or []

    logger.info(
        f"URGENT DEBUG: check_map_selection_needed_router - units_needing_map_selection={units_needing_map_selection}"
    )
    logger.info(
        f"URGENT DEBUG: check_map_selection_needed_router - current_place_index={current_place_index}, total_places={len(places)}"
    )

    # ------------------------------------------------------------------
    # 1. If there are units that still need map interaction, route to the
    #    dedicated interrupt node so the frontend can highlight / ask for
    #    confirmation before the workflow proceeds.
    # ------------------------------------------------------------------
    if units_needing_map_selection:
        logger.debug("URGENT DEBUG: Map selection still required – routing to request_map_selection")
        return "request_map_selection"

    # ------------------------------------------------------------------
    # 2. No pending map work – decide whether to process the next place or
    #    move on to theme resolution.
    # ------------------------------------------------------------------
    has_more_places = (
        current_place_index is not None and current_place_index < len(places)
    )

    logger.info(f"URGENT DEBUG: ROUTER LOGIC - current_place_index={current_place_index}, total_places={len(places)}")
    logger.info(f"URGENT DEBUG: ROUTER LOGIC - has_more_places check: {current_place_index} < {len(places)} = {has_more_places}")

    if has_more_places:
        logger.info(
            f"URGENT DEBUG: No map selection needed and more places remain – continuing to resolve_place_and_unit (will process place {current_place_index})"
        )
        return "resolve_place_and_unit"

    # All places handled, continue with themes / final steps
    logger.debug("URGENT DEBUG: Place processing complete – routing to resolve_theme")
    return "resolve_theme"


def request_map_selection(state: lg_State) -> lg_State | Command:
    """
    Dedicated node for interrupt - ONLY interrupts, no side effects.
    This is where we properly ask for user map interaction.
    """
    logger.debug("=== WORKFLOW TRACE: request_map_selection function called! ===")
    logger.info("Node: request_map_selection entered.")

    # Get the units that need selection
    units_needing_map_selection = state.get("units_needing_map_selection", [])
    current_place_index = state.get("current_place_index")
    places = state.get("places", []) or []

    if not units_needing_map_selection:
        logger.debug("URGENT DEBUG: No units need selection, returning state unchanged")
        return state
    
    # Check if we've already processed this specific request to prevent duplicates
    target_unit = units_needing_map_selection[0]
    from .utils import _has_message_content
    expected_message = f"Highlighting {places[current_place_index-1].get('name', 'the area') if current_place_index and current_place_index > 0 and current_place_index-1 < len(places) else 'the area'} on the map."
    
    if _has_message_content(state, expected_message):
        logger.info(f"URGENT DEBUG: Already processed highlighting message for unit {target_unit}, skipping duplicate")
        return state

    # Check if this is a multi-place workflow
    is_multi_place = len(places) > 1
    continue_to_next_place = current_place_index is not None and current_place_index < len(places)

    # Get the first unit that needs selection (for single-unit selection)
    target_unit = units_needing_map_selection[0]
    target_index = current_place_index - 1 if current_place_index is not None and current_place_index > 0 else 0
    place_name = places[target_index].get("name", "the area") if target_index < len(places) else "the area"

    logger.info(f"URGENT DEBUG: Requesting map selection for unit {target_unit} ({place_name})")
    logger.info(f"URGENT DEBUG: Multi-place workflow: {is_multi_place}, continue_to_next_place: {continue_to_next_place}")

    # CRITICAL: For multi-place workflows, trigger SSE update and continue without interrupting
    if is_multi_place and continue_to_next_place:
        logger.info(f"URGENT DEBUG: Multi-place workflow - triggering SSE update and continuing to next place")

        # Prepare data for Command update (don't modify state directly)
        all_selected_units = get_selected_units(state)
        all_selected_unit_types = get_selected_unit_types(state)
        map_update_request = {
            "action": "update_map_selection",
            "places": state.get("places", []),  # Send the single source of truth
            "selected_polygons": all_selected_units,  # Include unit IDs for count display
            "selected_polygons_unit_types": all_selected_unit_types  # Include unit types for count display
        }

        # Prepare AI message for user feedback using proper _append_ai function
        from .utils import _append_ai, _has_message_content
        message = f"Highlighting {place_name} on the map."
        
        # Only add message if it doesn't already exist to prevent duplicates
        if not _has_message_content(state, message):
            _append_ai(state, message)
        
        updated_messages = state.get("messages", [])

        logger.info(f"URGENT DEBUG: Multi-place - continuing to resolve_place_and_unit via Command")
        # Clear the processed unit from units_needing_map_selection
        remaining_units = [
            unit for unit in units_needing_map_selection if unit != target_unit]
        logger.info(f"URGENT DEBUG: Removing processed unit {target_unit}, remaining units: {remaining_units}")

        # CRITICAL: Clear processed units via direct state modification to avoid router loops
        state["units_needing_map_selection"] = remaining_units
        logger.info(f"URGENT DEBUG: Cleared processed units via direct state modification")

        # Check if map_update_request is different from current state to avoid duplicate updates
        current_map_request = state.get("map_update_request")
        if current_map_request == map_update_request:
            logger.info(f"URGENT DEBUG: Skipping duplicate map_update_request")
            # Return only messages to avoid map_update_request conflict
            return {
                "messages": updated_messages
            }
        else:
            # Return map update fields for SSE - don't return units_needing_map_selection to avoid conflicts
            return {
                "map_update_request": map_update_request,
                "messages": updated_messages
            }
    else:
        logger.info(f"URGENT DEBUG: Single place or last place - creating standard map selection interrupt")

        # CRITICAL: Clear processed units via direct state modification to avoid router loops
        state["units_needing_map_selection"] = []
        logger.info(f"URGENT DEBUG: Cleared all units via direct state modification")
        
        # Send map update for any remaining units
        remaining_selected_units = get_selected_units(state)
        remaining_selected_unit_types = get_selected_unit_types(state)
        map_update_request = {
            "action": "update_map_selection",
            "selected_polygons": remaining_selected_units,  # Include unit IDs for count display
            "selected_polygons_unit_types": remaining_selected_unit_types,  # Include unit types for count display
            "places": state.get("places", [])  # Send the single source of truth
        }
        
        # Check if map_update_request is different from current state to avoid duplicate updates
        current_map_request = state.get("map_update_request")
        if current_map_request == map_update_request:
            logger.info(f"URGENT DEBUG: Skipping duplicate map_update_request for single place")
            # Return only current_node to avoid map_update_request conflict
            return {
                "current_node": "request_map_selection"
            }
        else:
            # Return map update fields for SSE - don't return units_needing_map_selection to avoid conflicts
            return {
                "map_update_request": map_update_request,
                "current_node": "request_map_selection"
            }


def should_continue_to_themes(state: lg_State) -> str:
    """
    After a place's geographical unit has been fixed, decide the next step:

    •  If there are still places left to disambiguate  → keep looping.
    •  If every place now has a unit AND a theme is
       already selected                               → jump straight to cubes.
    •  Otherwise                                       → fetch/choose a theme.
    """
    logging.info("Routing: should_continue_to_themes()")
    places = state.get("places", []) or []
    num_places = len(places)
    current_index = state.get("current_place_index", 0)
    selected_units = get_selected_units(state)
    units_ready  = len(selected_units) >= num_places > 0
    have_theme   = bool(state.get("selected_theme"))

    logging.info(f"Routing decision: num_places={num_places}, current_index={current_index}, selected_units={len(selected_units)}, have_theme={have_theme}")

    # If no units are selected at all (e.g., after deselection), go to agent_node
    if not selected_units:
        logging.info("Routing to agent_node: no units selected")
        return "agent_node"

    # If no places to process, go to agent_node
    if num_places == 0:
        logging.info("Routing to agent_node: no places to process")
        return "agent_node"

    if current_index >= num_places:
        logging.info("Routing to resolve_theme: all places processed")
        return "resolve_theme"
    else:
        logging.info("Routing to resolve_place_and_unit: more places to process")
        return "resolve_place_and_unit"


def select_unit_on_map(state: lg_State) -> lg_State | Command:
    """
    Node intended to trigger map interaction in the frontend.
    It checks if units have been selected for the *most recently processed* place
    (using `current_place_index - 1` because the index was just incremented).
    If the unit hasn't already been added to the map's `selected_polygons` list (which is
    updated by the frontend), it issues an `interrupt`.
    This interrupt signals the frontend (`chat.py`) to:
    1. Potentially highlight or add the corresponding unit polygon(s) to the map.
    2. Wait for the user to potentially click/select polygons on the map.
    3. The frontend map callback updates `map-state` (specifically `selected_polygons`),
       which then triggers the `retrigger_chat_callback` in `chat.py`.
    4. `retrigger_chat_callback` triggers the main `update_chat` callback again, which
       resumes the LangGraph workflow, potentially entering the `decide_if_map_selected` router.

    Args:
        state (lg_State): The current workflow state.

    Returns:
        lg_State: The potentially updated state (though this node primarily interrupts).
    """
    logger.debug("=== WORKFLOW TRACE: select_unit_on_map function called! (LEGACY) ===")
    logger.info("Node: select_unit_on_map entered.")
    state["current_node"] = "select_unit_on_map"

    # DEBUG: Log key state variables
    current_place_index = state.get("current_place_index")
    places = state.get("places", []) or []
    selected_workflow_units = get_selected_units(state)
    logger.info(f"URGENT DEBUG: select_unit_on_map state - current_place_index={current_place_index}, total_places={len(places)}, selected_units={selected_workflow_units}")

    last_intent = state.get("last_intent_payload")
    if last_intent:
        logger.info(f"URGENT DEBUG: select_unit_on_map has last_intent: {last_intent}")
        if last_intent.get("intent") == "AddPlace" or last_intent.get("intent") == "RemovePlace":
            # Hand control back to the normal router so e.g. AddPlace_node runs
            logger.info(f"URGENT DEBUG: select_unit_on_map returning to agent_node for intent: {last_intent}")
            logging.info(f"resolve_theme: last_intent_payload set to {last_intent}, returning to agent_node.")
            return Command(goto="agent_node")
    else:
        logger.debug("URGENT DEBUG: select_unit_on_map no last_intent")
    # Get the list of units selected so far by the workflow (place/unit selection nodes).
    selected_workflow_units = get_selected_units(state)
    # Get the list of units selected *by the user on the map* (from frontend state).
    selected_map_polygons_str = [str(p) for p in state.get("selected_polygons", [])] # Ensure string comparison

    # Check if there are any workflow-selected units that need to be added to the map
    if selected_workflow_units:
        # CRITICAL: Check if this is a map-originated workflow (map click)
        # If the last intent was AddPlace with polygon_id, the place was already selected on the map
        last_intent = state.get("last_intent_payload", {})
        is_map_click = (
            last_intent.get("intent") == "AddPlace" and
            last_intent.get("arguments", {}).get("polygon_id") is not None
        )

        # Also check if we have polygon_ids in extracted data (indicates map click origin)
        polygon_ids = state.get("extracted_polygon_ids", [])
        has_polygon_ids = any(pid is not None for pid in polygon_ids)

        if is_map_click or has_polygon_ids:
            logger.info(f"select_unit_on_map: Map click detected (is_map_click={is_map_click}, has_polygon_ids={has_polygon_ids}), skipping map interrupt")
            # For map clicks, assume the units are already selected on the map and continue
            # No need to interrupt for map selection since user already clicked the map
        else:
            # CRITICAL: For button-based unit type changes, send interrupt to update map state immediately
            # This ensures the frontend gets updated unit types and selected units
            current_place_index = state.get("current_place_index")
            places = state.get("places", []) or []
            if current_place_index is None:
                logger.info(f"URGENT DEBUG: select_unit_on_map unit type selection - current_place_index={current_place_index}, total_places={len(state.get('places', []) or [])}")
                logger.info("select_unit_on_map: Unit type selection detected, issuing interrupt to update map state immediately")

                # CRITICAL FIX: Only send the current place's unit for map update, not all units
                # This prevents fetching multiple polygons simultaneously during multi-place workflows
                selected_units = get_selected_units(state)
                selected_unit_types = get_selected_unit_types(state)

                # Calculate which unit corresponds to the current place being processed
                if current_place_index is not None and current_place_index > 0 and current_place_index <= len(selected_units):
                    # Send only the current place's unit (most recently added)
                    current_unit = [selected_units[current_place_index - 1]]
                    current_unit_type = [selected_unit_types[current_place_index - 1]] if current_place_index - 1 < len(selected_unit_types) else []
                    logger.info(f"URGENT DEBUG: select_unit_on_map sending only current place unit: {current_unit} (index {current_place_index - 1})")
                else:
                    # Fallback: send all units (existing behavior)
                    current_unit = selected_units
                    current_unit_type = selected_unit_types
                    logger.info(f"URGENT DEBUG: select_unit_on_map fallback - sending all units: {current_unit}")

                interrupt(value={
                    # Only send the single source of truth - places array
                    "places": state.get("places", []),
                    "current_place_index": current_place_index,
                    "current_node": "select_unit_on_map"
                    # REMOVED: map_update_request to avoid conflicts with request_map_selection
                })
                # WORKFLOW PAUSES HERE - interrupt() returns from the function

            # Find all units that are in the workflow but not yet on the map
            logger.info(f"URGENT DEBUG: select_unit_on_map checking missing units - workflow_units={selected_workflow_units}, map_polygons={selected_map_polygons_str}")
            missing_units = []
            for i, unit_id in enumerate(selected_workflow_units):
                if str(unit_id) not in selected_map_polygons_str:
                    missing_units.append((i, unit_id))
                    logger.info(f"URGENT DEBUG: Missing unit {unit_id} at index {i}")

            logger.info(f"URGENT DEBUG: Total missing units: {len(missing_units)}")

            # CRITICAL FIX: Handle missing units first, then check for more places
            # This ensures each place gets proper map selection before moving to the next
            places = state.get("places", []) or []

            if missing_units:
                # CRITICAL: For multi-place workflows, we need to select places on the map sequentially
                # Find the missing unit that corresponds to the most recently processed place
                # This ensures Portsmouth gets selected, then Southampton, etc.

                # Prioritize the missing unit that corresponds to the current place being processed
                target_missing_unit = None
                target_missing_index = None

                if current_place_index is not None and current_place_index > 0:
                    # Look for the missing unit that corresponds to the most recently processed place
                    recent_place_index = current_place_index - 1
                    if recent_place_index < len(missing_units):
                        for missing_index, missing_unit in missing_units:
                            if missing_index == recent_place_index:
                                target_missing_unit = missing_unit
                                target_missing_index = missing_index
                                break

                # If we didn't find the recent place's unit, take the first missing unit
                if target_missing_unit is None and missing_units:
                    target_missing_index, target_missing_unit = missing_units[0]

                if target_missing_unit is not None:
                    logger.info(f"URGENT DEBUG: Processing map selection for unit {target_missing_unit} (index {target_missing_index})")
                    logger.info(f"Unit {target_missing_unit} (index {target_missing_index}) not found in map selections. Updating map and continuing workflow.")

                    # CRITICAL FIX: Update workflow's selected_polygons to match the unit being selected
                    # This ensures the workflow knows the unit is selected and can continue processing
                    current_selected_polygons = state.get("selected_polygons", []) or []
                    target_missing_str = str(target_missing_unit)
                    current_polygons_str = [str(p) for p in current_selected_polygons]

                    if target_missing_str not in current_polygons_str:
                        updated_polygons = current_selected_polygons + [target_missing_unit]
                        state["selected_polygons"] = updated_polygons
                        logger.info(f"URGENT DEBUG: Updated workflow selected_polygons from {current_selected_polygons} to {updated_polygons}")
                    else:
                        logger.info(f"URGENT DEBUG: Unit {target_missing_unit} already in selected_polygons: {current_selected_polygons}")

                    # CRITICAL FIX: Use interrupt to ensure proper sequencing
                    # Portsmouth must be fully processed (including map selection) before Southampton starts
                    selected_unit_types = get_selected_unit_types(state)
                    missing_unit_type = [selected_unit_types[target_missing_index]] if target_missing_index is not None and target_missing_index < len(selected_unit_types) else []

                    logger.info(f"URGENT DEBUG: Using interrupt to ensure Portsmouth is selected before Southampton processing")

                    # CRITICAL FIX: Check if there are more places to process
                    places = state.get("places", []) or []
                    continue_to_next_place = current_place_index is not None and current_place_index < len(places)
                    if continue_to_next_place:
                        logger.info(f"URGENT DEBUG: More places to process ({current_place_index} of {len(places)}), will continue to next place after this interrupt")

                    # REMOVED: Old multi-place path that caused map_update_request conflicts
                    # Now all map updates are handled by request_map_selection to avoid field conflicts
                    logger.info(f"URGENT DEBUG: Map updates now handled by request_map_selection - creating interrupt for user selection")
                    logger.info(f"URGENT DEBUG: Single place or last place - creating interrupt for map selection")
                    interrupt(value={
                        # Only send the single source of truth - places array
                        "places": state.get("places", []),
                        "current_place_index": current_place_index,
                        "current_node": "select_unit_on_map",
                        "message": f"Please select {places[target_missing_index].get('name', 'the area') if target_missing_index is not None and target_missing_index < len(places) else 'the area'} on the map to continue."
                        # REMOVED: map_update_request to avoid conflicts with request_map_selection
                    })

            # CRITICAL FIX: Only after handling missing units, check if there are more places to process
            if current_place_index is not None and current_place_index < len(places):
                logger.info(f"URGENT DEBUG: More places to process ({current_place_index} of {len(places)}), continuing to next place")
                logger.info(f"select_unit_on_map: More places to process ({current_place_index} of {len(places)}), continuing to next place")
                return Command(goto="resolve_place_and_unit", update={
                    "places": state.get("places", []),
                    "current_place_index": state.get("current_place_index"),
                    "units_needing_map_selection": state.get("units_needing_map_selection", []),
                })

            # CRITICAL FIX: If all places are processed and no missing units, exit cleanly
            if current_place_index is not None and current_place_index >= len(places):
                logger.info(f"URGENT DEBUG: All places processed ({current_place_index} >= {len(places)}), exiting select_unit_on_map cleanly")
                logger.info(f"select_unit_on_map: All places processed ({current_place_index} >= {len(places)}), proceeding to conditional routing")
                return state

    # Only proceed with routing if no interrupt was issued (i.e., all units are on map or no units exist)
    logger.info(f"All workflow units are already selected on map or no units exist. Proceeding with routing.")
    logger.info(f"select_unit_on_map: About to exit and use conditional routing. Current state: current_place_index={state.get('current_place_index')}, total_places={len(state.get('places', []) or [])}")

    # Let the conditional edges handle routing - just return state
    return state


def resolve_place_and_unit(state: lg_State) -> lg_State | Command:
    """
    Resolve exactly *one* place per call:
        • disambiguate place name   (may interrupt)
        • disambiguate unit type    (may interrupt)
        • write g_place / g_unit / g_unit_type

    Returns updated state. Router decides next node based on current_place_index.
    """
    logger.info("=== resolve_place_and_unit ENTRY ===")

    # Get current index and places array
    places = state.get("places", []) or []
    current_idx = state.get("current_place_index")

    # DETAILED DEBUG: Log the incoming state
    logger.info(f"TRACE: INCOMING STATE - places={[(i, p.get('name'), p.get('g_place')) for i, p in enumerate(places)]}")
    logger.info(f"TRACE: INCOMING STATE - current_place_index={current_idx}, selection_idx={state.get('selection_idx')}")

    # CRITICAL: Handle missing current_place_index after interrupt
    # When resuming from interrupt, current_place_index might be None
    if current_idx is None:
        # Determine index based on how many places are already resolved
        resolved_count = sum(1 for p in places if p.get("g_unit") is not None)
        current_idx = resolved_count
        logger.info(f"current_place_index was None, computed from resolved places: {current_idx}")

    i = current_idx
    selection_idx = state.get("selection_idx")
    logger.info(f"TRACE: Processing place index {i} of {len(places)}, selection_idx={selection_idx}")

    # DETAILED LOGGING: Show state of all places
    for idx, place in enumerate(places):
        g_place = place.get("g_place")
        g_unit = place.get("g_unit")
        name = place.get("name", f"Place {idx}")
        logger.info(f"TRACE: Place {idx} ({name}): g_place={g_place}, g_unit={g_unit}")

    logger.info(f"TRACE: About to check if all places processed: i={i}, len(places)={len(places)}")


    # Check if all places are processed
    if not places or i >= len(places):
        logger.info("TRACE: All places processed")
        update_dict = {"current_place_index": len(places)}
        logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE (all places done): {list(update_dict.keys())}")
        return Command(goto="agent_node", update=update_dict)

    # Get current place (work on a copy)
    place = places[i].copy()
    place_name = place.get('name', f'Place {i}')

    logger.info(f"TRACE: Processing '{place_name}': g_place={place.get('g_place')}, g_unit={place.get('g_unit')}")

    # If place is already fully resolved, skip to next
    if place.get("g_place") is not None and place.get("g_unit") is not None:
        logger.info(f"TRACE: Place '{place_name}' already resolved, advancing to next (early return)")
        places[i] = place
        update_dict = {"places": places, "current_place_index": i + 1}
        logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE (already resolved): {list(update_dict.keys())}")
        return Command(goto="agent_node", update=update_dict)

    # ─────────────────────────────────────────
    # STEP 1: Resolve place (g_place)
    # ─────────────────────────────────────────
    if place.get("g_place") is None:
        # Special case: place from map click already has g_unit
        if place.get("g_unit") is not None:
            logger.info(f"Place '{place_name}' has g_unit {place['g_unit']} from map click")
            # Update state and advance
            places[i] = place

            # Add the resolved unit to units_needing_map_selection so map can highlight it
            units_needing_map_selection = state.get("units_needing_map_selection", []) or []
            resolved_unit = place.get("g_unit")
            if resolved_unit is not None and resolved_unit not in units_needing_map_selection:
                units_needing_map_selection.append(resolved_unit)
                logger.info(f"Added map-click unit {resolved_unit} to units_needing_map_selection: {units_needing_map_selection}")

            update_dict = {
                "places": places,
                "current_place_index": i + 1,
                "units_needing_map_selection": units_needing_map_selection,
            }
            logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE (map click): {list(update_dict.keys())}")
            return Command(goto="agent_node", update=update_dict)

        # Get candidate rows for place disambiguation
        rows = place.get("candidate_rows", [])
        if not rows:
            logger.warning(f"No candidates found for '{place_name}', skipping")
            update_dict = {"current_place_index": i + 1}
            logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE (no candidates): {list(update_dict.keys())}")
            return Command(goto="agent_node", update=update_dict)

        # Single candidate - auto-select
        if len(rows) == 1:
            logger.info(f"Auto-selecting single candidate for '{place_name}'")
            place["g_place"] = rows[0]["g_place"]
            # Create unit_rows from the selected place
            g_units = rows[0]["g_unit"]
            g_unit_types = rows[0]["g_unit_type"]
            if not isinstance(g_units, list):
                g_units, g_unit_types = [g_units], [g_unit_types]
            place["unit_rows"] = [
                {"g_unit": u, "g_unit_type": ut}
                for u, ut in zip(g_units, g_unit_types)
            ]

        # Multiple candidates - need user selection
        elif len(rows) > 1:
            # Check if user has made a selection
            if selection_idx is not None:
                try:
                    choice = int(selection_idx)
                    if 0 <= choice < len(rows):
                        logger.info(f"User selected option {choice} for '{place_name}'")
                        selected_row = rows[choice]
                        place["g_place"] = selected_row["g_place"]
                        # Create unit_rows
                        g_units = selected_row["g_unit"]
                        g_unit_types = selected_row["g_unit_type"]
                        if not isinstance(g_units, list):
                            g_units, g_unit_types = [g_units], [g_unit_types]
                        place["unit_rows"] = [
                            {"g_unit": u, "g_unit_type": ut}
                            for u, ut in zip(g_units, g_unit_types)
                        ]
                        # Update specific keys in the places array
                        places[i]["g_place"] = place["g_place"]
                        places[i]["unit_rows"] = place["unit_rows"]
                        logger.info(f"TRACE: Updated places[{i}] with g_place={place['g_place']} for '{place_name}'")

                        # CRITICAL: Return updated state immediately to persist changes
                        # Let the normal router handle the next step to avoid Command conflicts
                        logger.info(f"TRACE: Returning updated state with Newport g_place={place['g_place']} to persist changes")
                        logger.error(f"🔍 resolve_place_and_unit STATE UPDATE (place selection): ['places']")
                        return {"places": places}
                    else:
                        logger.warning(f"Invalid selection index {choice}")

                except (ValueError, TypeError):
                    # selection_idx is not a number (might be unit type) - need place selection
                    logger.info(f"Non-numeric selection '{selection_idx}' - need place disambiguation")
            # If still need selection, interrupt for user input
            if place.get("g_place") is None:
                logger.info(f"Interrupting for place disambiguation: {len(rows)} options")
                options = [
                    {
                        "option_type": "place",
                        "label": f"{r['g_name']}, {r['county_name']}",
                        "color": "#333",
                        "value": j,
                    }
                    for j, r in enumerate(rows)
                ]

                # Add coordinate data for map display
                place_coordinates = []
                for j, r in enumerate(rows):
                    if r.get('lat') is not None and r.get('lon') is not None:
                        try:
                            lat, lon = float(r['lat']), float(r['lon'])
                            # Validate UK bounds
                            if 49 <= lat <= 61 and -8 <= lon <= 2:
                                place_coordinates.append({
                                    "index": j,
                                    "name": r['g_name'],
                                    "county": r['county_name'],
                                    "lat": lat,
                                    "lon": lon,
                                    "g_place": r['g_place']
                                })
                        except (ValueError, TypeError):
                            pass

                # CRITICAL: Use the most current places array from state
                current_places = state.get("places", places)
                interrupt(value={
                    "message": f"More than one \"{place_name}\". Please choose:",
                    "options": options,
                    "place_coordinates": place_coordinates,
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": i,
                    "places": current_places,
                })
                # interrupt doesn't return
                return state

    # ─────────────────────────────────────────
    # STEP 2: Resolve unit type (g_unit)
    # ─────────────────────────────────────────
    if place.get("g_unit") is None:
        unit_rows = place.get("unit_rows", [])
        if not unit_rows:
            logger.warning(f"No unit rows for '{place_name}', skipping")
            state["current_place_index"] = i + 1

            return state

        # Single unit type - auto-select
        if len(unit_rows) == 1:
            logger.info(f"Auto-selecting single unit type for '{place_name}'")
            place["g_unit"] = unit_rows[0]["g_unit"]
            place["g_unit_type"] = unit_rows[0]["g_unit_type"]
            # Show confirmation
            from vobchat.nodes.utils import _append_ai
            long_name = UNIT_TYPES.get(place["g_unit_type"], {}).get("long_name", place["g_unit_type"])
            _append_ai(state, f"Using {long_name} data for '{place_name}'.")

        # Multiple unit types - need user selection
        elif len(unit_rows) > 1:
            logger.info(f"TRACE: Multiple unit types for '{place_name}', checking selection")
            logger.info(f"TRACE: selection_idx={selection_idx}, state.current_place_index={state.get('current_place_index')}, i={i}")
            logger.info(f"TRACE: Available unit types: {[r['g_unit_type'] for r in unit_rows]}")

            # Check if user has made a unit type selection for THIS place
            # Only use selection_idx if it matches one of the available unit types for this place
            current_node = state.get("current_node")
            logger.info(f"TRACE: Checking selection - selection_idx={selection_idx}, current_node={current_node}, available_types={[r['g_unit_type'] for r in unit_rows]}")

            if (selection_idx is not None and
                selection_idx in [r["g_unit_type"] for r in unit_rows]):
                logger.info(f"TRACE: User selected unit type '{selection_idx}' for '{place_name}' (place index {i})")
                chosen_unit = next((r for r in unit_rows if r["g_unit_type"] == selection_idx), None)
                if chosen_unit:
                    place["g_unit"] = chosen_unit["g_unit"]
                    place["g_unit_type"] = chosen_unit["g_unit_type"]
                    logger.info(f"TRACE: Set g_unit={place['g_unit']}, g_unit_type={place['g_unit_type']} for '{place_name}'")
                    # Show confirmation
                    from vobchat.nodes.utils import _append_ai
                    long_name = UNIT_TYPES.get(place["g_unit_type"], {}).get("long_name", place["g_unit_type"])
                    _append_ai(state, f"Using {long_name} data for '{place_name}'.")

                    # Update the places array and return immediately to persist changes and clear UI
                    places[i] = place

                    # Add the resolved unit to units_needing_map_selection for map highlighting
                    units_needing_map_selection = state.get("units_needing_map_selection", []) or []
                    resolved_unit = place.get("g_unit")
                    if resolved_unit is not None and resolved_unit not in units_needing_map_selection:
                        units_needing_map_selection.append(resolved_unit)
                        logger.info(f"TRACE: Added resolved unit {resolved_unit} to units_needing_map_selection: {units_needing_map_selection}")

                    logger.info(f"TRACE: Returning updated state after unit type selection to clear buttons")
                    update_dict = {
                        "places": places,
                        "current_place_index": i + 1,  # Advance to next place
                        "units_needing_map_selection": units_needing_map_selection,  # Trigger map highlighting
                    }
                    logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE: {list(update_dict.keys())}")
                    return Command(goto="agent_node", update=update_dict)
                else:
                    logger.info(f"TRACE: No matching unit found for selection '{selection_idx}'")

            else:
                logger.info(f"TRACE: No selection_idx present, will need to interrupt for user selection")

            # If still need selection, interrupt for user input
            if place.get("g_unit") is None:
                logger.info(f"Interrupting for unit type selection: {len(unit_rows)} options")
                options = [
                    {
                        "option_type": "unit",
                        "label": UNIT_TYPES.get(r["g_unit_type"], {}).get("long_name", r["g_unit_type"]),
                        "color": UNIT_TYPES.get(r["g_unit_type"], {}).get("color", "#333"),
                        "value": r["g_unit_type"],
                    }
                    for r in unit_rows
                ]

                # CRITICAL: Use the updated places from state, not the local variable
                current_places = state.get("places", places)
                interrupt(value={
                    "message": f"Which geography for \"{place_name}\"?",
                    "options": options,
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": i,
                    "places": current_places,
                })
    # ─────────────────────────────────────────
    # STEP 3: Place is now fully resolved
    # ─────────────────────────────────────────
    # Update places array with resolved place
    places[i] = place
    logger.info(f"TRACE: Completed place {i} '{place_name}': g_place={place.get('g_place')}, g_unit={place.get('g_unit')}, g_unit_type={place.get('g_unit_type')}")
    logger.info(f"TRACE: Advancing to next place (index {i + 1})")

    # Add the resolved unit to units_needing_map_selection so map can highlight it
    units_needing_map_selection = state.get("units_needing_map_selection", []) or []
    resolved_unit = place.get("g_unit")
    if resolved_unit is not None and resolved_unit not in units_needing_map_selection:
        units_needing_map_selection.append(resolved_unit)
        logger.info(f"TRACE: Added resolved unit {resolved_unit} to units_needing_map_selection: {units_needing_map_selection}")

    # Return updated state - let router decide next node
    logger.info(f"TRACE: Returning from resolve_place_and_unit with current_place_index={i + 1}")
    update_dict = {
        "places": places,
        "current_place_index": i + 1,
        "units_needing_map_selection": units_needing_map_selection,
    }
    logger.error(f"🔍 resolve_place_and_unit COMMAND UPDATE (final): {list(update_dict.keys())}")
    return Command(goto="agent_node", update=update_dict)
