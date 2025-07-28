# workflow_place_nodes.py - drive the *place‑resolution* mini‑workflow
# ====================================================================
# Four public nodes:
#   • **UpdatePolygonSelection_node**
#   • **RequestMapSelection_node**
#   • **ResolvePlaceAndUnit_node**
#   • **SelectUnitOnMap_node** (thin legacy shim - optional)
#
# The orchestration logic is deliberately simple:
#     1. Resolve one place at a time → ResolvePlaceAndUnit_node (may interrupt)
#     2. When a place becomes fully‑resolved (we have g_unit) its polygon must
#        exist in *selected_polygons*; UpdatePolygonSelection_node figures out
#        what’s missing and either
#           a) asks the front‑end to highlight via RequestMapSelection_node, or
#           b) continues straight to ResolvePlaceAndUnit_node for the next place.
#
# Each interrupt carries **only** the canonical  *places* array so the front‑end
# is always in sync.
# ====================================================================

from __future__ import annotations

import logging
from typing import Dict, List, Union

# type: ignore - provided by LangGraph
from langgraph.types import Command, interrupt

from vobchat.state_schema import (
    lg_State,
    get_selected_units,
)
from vobchat.utils.constants import UNIT_TYPES
from .utils import _append_ai, serialize_messages

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper - build button option dictionaries
# -----------------------------------------------------------------------------


def _make_options(rows: List[dict], *, kind: str) -> List[dict]:
    if kind == "place":
        return [
            {
                "option_type": "place",
                "label": f"{r['g_name']}, {r['county_name']}",
                "value": i,
                "color": "#333",
            }
            for i, r in enumerate(rows)
        ]
    if kind == "unit":
        return [
            {
                "option_type": "unit",
                "label": UNIT_TYPES.get(r["g_unit_type"], {}).get("long_name", r["g_unit_type"]),
                "value": r["g_unit_type"],
                "color": UNIT_TYPES.get(r["g_unit_type"], {}).get("color", "#333"),
            }
            for r in rows
        ]
    return []

# -----------------------------------------------------------------------------
# Node - UpdatePolygonSelection_node
# -----------------------------------------------------------------------------


def update_polygon_selection(state: lg_State):
    """Send map update request to sync frontend with current places."""

    workflow_units = get_selected_units(state)

    if workflow_units:
        logger.info("Sending map update for workflow units: %s", workflow_units)

        # Check if all places are resolved before routing back
        current_idx = state.get("current_place_index", 0) or 0
        places = state.get("places", []) or []

        if current_idx >= len(places):
            logger.info("update_polygon_selection: All places resolved, routing to agent_node")
            return Command(goto="agent_node", update={
                "units_needing_map_selection": [],
                "places": places,
                "selection_idx": None,
                "map_update_request": {
                    "action": "update_map_selection",
                    "places": state.get("places", []),
                }
            })

        return Command(goto="resolve_place_and_unit", update={
            "units_needing_map_selection": [],
            "places": state.get("places", []),
            "current_place_index": state.get("current_place_index"),  # Preserve current index
            "selection_idx": None,  # Clear selection_idx to prevent cross-place contamination
            "map_update_request": {
                "action": "update_map_selection",
                "places": state.get("places"),
            }
        })

    # All set - check if we need to continue resolving places or move to next step
    current_idx = state.get("current_place_index", 0) or 0
    places = state.get("places", []) or []

    # If we haven't processed all places yet, continue resolving
    if current_idx < len(places):
        return Command(goto="resolve_place_and_unit")

    # All places processed, move to next workflow step
    return Command(goto="agent_node")

# -----------------------------------------------------------------------------
# Node - RequestMapSelection_node
# -----------------------------------------------------------------------------


# def request_map_selection(state: lg_State):
#     needed = state.get("units_needing_map_selection", [])
#     if not needed:
#         return Command(goto="agent_node")

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
    """Disambiguate **one** place per call - name then unit type."""

    places = state.get("places", []) or []
    idx = state.get("current_place_index", 0) or 0

    logger.info(f"resolve_place_and_unit: starting with idx={idx}, places={[p.get('name') for p in places]}")

    # Skip past already‑resolved places
    while idx < len(places) and places[idx].get("g_unit") is not None:
        logger.info(f"resolve_place_and_unit: skipping resolved place {idx} ({places[idx].get('name')})")
        idx += 1

    if idx >= len(places):
        # All done → polygons
        logger.info(f"resolve_place_and_unit: all places resolved, moving to update_polygon_selection")
        return Command(goto="update_polygon_selection", update={"current_place_index": idx})

    place = places[idx]
    sel = state.get("selection_idx")

    logger.info(f"resolve_place_and_unit: processing place {idx} ({place.get('name')}), g_unit={place.get('g_unit')}, selection_idx={sel}")

    # ── STEP 1: choose the correct *place* (g_place + unit_rows) ───────────
    if place.get("g_place") is None:
        rows = place.get("candidate_rows", [])
        logger.info(f"resolve_place_and_unit: place {place.get('name')} has {len(rows)} candidate rows, g_place={place.get('g_place')}")
        if not rows:
            _append_ai(state, f"I couldn't find '{place['name']}'. Skipping...")
            return Command(goto="agent_node", update={"current_place_index": idx + 1})
        if len(rows) == 1:
            place["g_place"] = rows[0]["g_place"]
            gu, gut = rows[0]["g_unit"], rows[0]["g_unit_type"]
            if not isinstance(gu, list):
                gu, gut = [gu], [gut]
            place["unit_rows"] = [
                {"g_unit": u, "g_unit_type": t} for u, t in zip(gu, gut)
            ]
            places[idx] = place  # Ensure place is updated in places array
        else:
            logger.info(f"resolve_place_and_unit: multiple candidates for {place.get('name')}, sel={sel}, type={type(sel)}")
            if sel is not None and isinstance(sel, int) and 0 <= sel < len(rows):
                logger.info(f"resolve_place_and_unit: user selected place option {sel} for {place.get('name')}")
                r = rows[sel]
                place["g_place"] = r["g_place"]
                gu, gut = r["g_unit"], r["g_unit_type"]
                if not isinstance(gu, list):
                    gu, gut = [gu], [gut]
                place["unit_rows"] = [
                    {"g_unit": u, "g_unit_type": t} for u, t in zip(gu, gut)
                ]
                places[idx] = place  # Ensure place is updated in places array

                # Continue to next step in same place (unit selection) or next place
                return Command(goto="resolve_place_and_unit", update={
                    "places": places,
                    "current_place_index": idx,  # Keep processing same place for unit selection
                    "selection_idx": None
                })
            else:
                logger.info(f"resolve_place_and_unit: interrupting for place disambiguation of {place.get('name')} with {len(rows)} options")

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

                interrupt({
                    "message": f"More than one **{place['name']}** - which do you mean?",
                    "options": _make_options(rows, kind="place"),
                    "place_coordinates": place_coordinates,  # Add coordinates for map markers
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": idx,
                    "places": places,  # Use updated places array
                    "messages": serialize_messages(state.get("messages", []))
                })


    # ── STEP 2: choose the *unit type* ─────────────────────────────────────
    if place.get("g_unit") is None:
        units = place.get("unit_rows", [])
        logger.info(f"resolve_place_and_unit: {place.get('name')} unit selection - {len(units)} unit types, g_place={place.get('g_place')}")
        if len(units) == 1:
            logger.info(f"resolve_place_and_unit: auto-selecting single unit type for {place.get('name')}")
            place.update(units[0])
            places[idx] = place  # Ensure place is updated in places array
        else:
            logger.info(f"resolve_place_and_unit: multiple unit types for {place.get('name')}, sel={sel}, available_types={[u['g_unit_type'] for u in units]}")
            if sel and isinstance(sel, str):
                chosen = next(
                    (r for r in units if r["g_unit_type"] == sel), None)
                if chosen:
                    logger.info(f"resolve_place_and_unit: selected unit type {sel} for {place.get('name')}")
                    place.update(chosen)
                    places[idx] = place  # Ensure place is updated in places array
                    state["selection_idx"] = None
                else:
                    logger.info(f"resolve_place_and_unit: unit type {sel} not found for {place.get('name')}")
                    sel = None  # force re‑ask
            if place.get("g_unit") is None:
                interrupt({
                    "message": f"Which geography for **{place['name']}**?",
                    "options": _make_options(units, kind="unit"),
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": idx,
                    "places": places,  # Use updated places array
                    "messages": serialize_messages(state.get("messages", []))
                })

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
    return Command(goto="update_polygon_selection")
