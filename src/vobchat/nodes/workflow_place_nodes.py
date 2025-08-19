"""Place resolution mini-workflow nodes.

This module hosts the nodes that resolve user-specified places into canonical
graph identifiers and unit types, and keeps the map selection synchronized:

- ``update_polygon_selection``: ensure polygons reflect resolved places and
  request UI updates; returns a ``Command`` to continue routing.
- ``resolve_place_and_unit``: disambiguate a single place (name → g_place,
  then unit type → g_unit), potentially interrupting to ask the user.
- ``select_unit_on_map``: legacy shim that routes to ``update_polygon_selection``.

Orchestration pattern:
    1) Resolve one place at a time in ``resolve_place_and_unit`` (may interrupt)
    2) When a place becomes fully resolved (has ``g_unit``), ensure its polygon
       appears in the UI via ``update_polygon_selection`` and either prompt the
       user or proceed to the next place.

Interrupts carry only the canonical ``places`` array to keep front-end and
backend views synchronized.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Union

# type: ignore - provided by LangGraph
from langgraph.types import Command

from vobchat.state_schema import (
    lg_State,
    get_selected_units,
)
from .utils import _append_ai

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def _collect_selected_place_coordinates(places: List[dict]) -> List[dict]:
    """Extract selected place coordinates for zoom purposes."""
    selected_place_coordinates = []
    for place in places:
        if place.get("selected_coordinates"):
            coord = place["selected_coordinates"]
            selected_place_coordinates.append({
                "lat": coord["lat"],
                "lon": coord["lon"], 
                "name": place.get("name", ""),
                "selected": True
            })
    return selected_place_coordinates


# -----------------------------------------------------------------------------
# Node - UpdatePolygonSelection_node
# -----------------------------------------------------------------------------


def update_polygon_selection(state: lg_State):
    """Send a map update request so the UI reflects the current places.

    The function computes whether all places are resolved; if yes, it routes
    back to the agent. Otherwise it continues to place/unit resolution. In both
    cases it includes a ``map_update_request`` payload that the front-end can
    use to highlight or zoom to selected coordinates.
    """

    workflow_units = get_selected_units(state)

    if workflow_units:
        logger.info("Sending map update for workflow units: %s", workflow_units)

        # Check if all places are resolved before routing back
        current_idx = state.get("current_place_index", 0) or 0
        places = state.get("places", []) or []

        if current_idx >= len(places):
            logger.info("update_polygon_selection: All places resolved, routing to next step")

            # Collect selected place coordinates for zoom
            selected_place_coordinates = _collect_selected_place_coordinates(places)

            # If no theme is selected yet, prompt the user to pick one for the selected units
            if not state.get("selected_theme"):
                return Command(
                    goto="resolve_theme",
                    update={
                        "units_needing_map_selection": [],
                        "places": places,
                        "selection_idx": None,
                        "map_update_request": {
                            "action": "update_map_selection",
                            "places": state.get("places", []),
                            "selected_place_coordinates": selected_place_coordinates,
                        },
                    },
                )

            # Otherwise, proceed back to the agent/start
            return Command(
                goto="start_node",
                update={
                    "units_needing_map_selection": [],
                    "places": places,
                    "selection_idx": None,
                    "map_update_request": {
                        "action": "update_map_selection",
                        "places": state.get("places", []),
                        "selected_place_coordinates": selected_place_coordinates,
                    },
                },
            )

        # Collect selected place coordinates for zoom (including partially processed places)
        places_list = state.get("places", []) or []
        selected_place_coordinates = _collect_selected_place_coordinates(places_list)
        
        return Command(goto="resolve_place_and_unit", update={
            "units_needing_map_selection": [],
            "places": state.get("places", []),
            "current_place_index": state.get("current_place_index"),  # Preserve current index
            "selection_idx": None,  # Clear selection_idx to prevent cross-place contamination
            "map_update_request": {
                "action": "update_map_selection",
                "places": state.get("places"),
                "selected_place_coordinates": selected_place_coordinates
            }
        })

    # All set - check if we need to continue resolving places or move to next step
    current_idx = state.get("current_place_index", 0) or 0
    places = state.get("places", []) or []

    # If we haven't processed all places yet, continue resolving
    if current_idx < len(places):
        return Command(goto="resolve_place_and_unit")

    # All places processed, move to next workflow step
    return Command(goto="start_node")

# -----------------------------------------------------------------------------
# Node - RequestMapSelection_node
# -----------------------------------------------------------------------------


# def request_map_selection(state: lg_State):
#     needed = state.get("units_needing_map_selection", [])
#     if not needed:
#         return Command(goto="start_node")

#     unit = needed[0]
#     place_name = next(
#         (p.get("name", "the area")
#          for p in state.get("places", []) if p.get("g_unit") == unit),
#         "the area",
#     )

#     interrupt({
#         "message": f"Please highlight **{place_name}** on the map to continue.",
#         "current_node": "request_map_selection",
#         "places": state.get("places", []),
#     })

#     # After the user clicks, the front‑end will resume the workflow.
#     return {}

# -----------------------------------------------------------------------------
# Node - ResolvePlaceAndUnit_node
# -----------------------------------------------------------------------------


def resolve_place_and_unit(state: lg_State):
    """Disambiguate exactly one place per call: name first, then unit type.

    Resolution is split into two steps, each of which may trigger an interrupt
    to collect a selection from the user. On success, updates the working
    ``places`` list in state and advances the ``current_place_index``.
    """

    places = state.get("places", []) or []
    idx = state.get("current_place_index", 0) or 0
    sel = state.get("selection_idx")

    logger.info(f"resolve_place_and_unit: starting with idx={idx}, places={[p.get('name') for p in places]}, selection_idx={sel}")
    
    # Debug: log all state keys to understand what we're getting
    logger.info(f"resolve_place_and_unit: state keys: {list(state.keys())}")
    
    # Debug: log place details if we're processing a specific place
    if idx < len(places):
        place = places[idx]
        logger.info(f"resolve_place_and_unit: current place {idx} details: name={place.get('name')}, g_place={place.get('g_place')}, g_unit={place.get('g_unit')}, candidate_rows_count={len(place.get('candidate_rows', []))}, unit_rows_count={len(place.get('unit_rows', []))}")

    # Skip past already‑resolved places
    while idx < len(places) and places[idx].get("g_unit") is not None:
        logger.info(f"resolve_place_and_unit: skipping resolved place {idx} ({places[idx].get('name')})")
        idx += 1

    if idx >= len(places):
        # All done → polygons
        logger.info(f"resolve_place_and_unit: all places resolved, moving to update_polygon_selection")
        return Command(goto="update_polygon_selection", update={"current_place_index": idx})

    place = places[idx]

    logger.info(f"resolve_place_and_unit: processing place {idx} ({place.get('name')}), g_unit={place.get('g_unit')}, selection_idx={sel}")

    # ── STEP 1: choose the correct *place* (g_place + unit_rows) ───────────
    if place.get("g_place") is None:
        from .place_nodes import _disambiguate_place_name
        
        # Use the shared place disambiguation logic
        # Temporarily store place in state for the disambiguation function
        candidate_rows = place.get("candidate_rows")
        # Don't pass empty list as None - let function fetch if needed
        if candidate_rows == []:
            candidate_rows = None
            
        temp_place_entry = {
            "name": place.get("name"),
            "candidate_rows": candidate_rows,
            "g_place": place.get("g_place"),
            "unit_rows": place.get("unit_rows")
        }
        state["place_entry"] = temp_place_entry
        
        try:
            disambiguated_place = _disambiguate_place_name(
                place.get("name", ""), 
                state, 
                current_node="resolve_place_and_unit",
                store_coordinates=True,
                current_place_index=idx,
                places=places
            )
            
            if disambiguated_place is None:
                # Either no place found or disambiguation needed (interrupt triggered)
                place_entry = state.get("place_entry")
                if place_entry is None or not place_entry.get("candidate_rows"):
                    # No place found
                    _append_ai(state, f"I couldn't find '{place['name']}'. Skipping...")
                    return Command(goto="start_node", update={"current_place_index": idx + 1})
                else:
                    # Disambiguation in progress - interrupt triggered, state is preserved
                    # Just return the current state, the interrupt has already been triggered
                    return {}
            
            # Place was successfully disambiguated - update our place object
            place.update({
                "g_place": disambiguated_place.get("g_place"),
                "unit_rows": disambiguated_place.get("unit_rows", []),
                "selected_coordinates": disambiguated_place.get("selected_coordinates")
            })
            places[idx] = place
            
            # Add success message
            place_data = disambiguated_place.get("place_data", {})
            if place_data.get("county_name"):
                _append_ai(state, f"Found **{place['name']}** in {place_data['county_name']}")
            
        except Exception as e:
            # Don't catch GraphInterrupt - let it bubble up to LangGraph
            from langgraph.errors import GraphInterrupt
            if isinstance(e, GraphInterrupt):
                raise  # Re-raise GraphInterrupt so LangGraph can handle it
            
            logger.error(f"Error in place disambiguation for {place.get('name')}: {e}", exc_info=True)
            _append_ai(state, f"Sorry, I encountered an error looking up '{place['name']}'. Skipping...")
            return Command(goto="start_node", update={"current_place_index": idx + 1})
        finally:
            # Clean up temporary state
            state.pop("place_entry", None)


    # ── STEP 2: choose the *unit type* ─────────────────────────────────────
    if place.get("g_unit") is None:
        from .place_nodes import _disambiguate_unit_type
        logger.info(f"resolve_place_and_unit: {place.get('name')} unit selection - {len(place.get('unit_rows', []))} unit types, g_place={place.get('g_place')}")
        
        try:
            unit_result = _disambiguate_unit_type(
                place, 
                state, 
                current_node="resolve_place_and_unit",
                current_place_index=idx,
                places=places
            )
            
            if unit_result is not None:
                # Unit was successfully selected
                logger.info(f"resolve_place_and_unit: selected unit type {unit_result.get('g_unit_type')} for {place.get('name')}")
                place.update(unit_result)
                places[idx] = place
                state["selection_idx"] = None
            else:
                # Disambiguation needed - interrupt was triggered
                return {}
                
        except Exception as e:
            from langgraph.errors import GraphInterrupt
            if isinstance(e, GraphInterrupt):
                raise
            
            logger.error(f"Error in unit disambiguation for {place.get('name')}: {e}", exc_info=True)
            # Skip this place and continue
            return Command(goto="start_node", update={"current_place_index": idx + 1})


    # ── done with this place ───────────────────────────────────────────────
    places[idx] = place
    units_sel = (state.get("units_needing_map_selection") or []) + [place["g_unit"]]

    return Command(
        goto="update_polygon_selection",
        update={
            "places": places,
            "current_place_index": idx + 1,
            "units_needing_map_selection": units_sel,
            "selection_idx": None,
            "options": None,
        },
    )

# -----------------------------------------------------------------------------
# (Optional) Node - SelectUnitOnMap_node
# -----------------------------------------------------------------------------
# Kept for backward compatibility with earlier graph definitions - simply routes
# to UpdatePolygonSelection_node so that legacy edges don’t break.


def select_unit_on_map(state: lg_State):
    """Legacy compatibility shim that routes to ``update_polygon_selection``."""
    _ = state  # Unused but required by interface
    return Command(goto="update_polygon_selection")
