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
from typing import Dict, List, Optional, Union, Any

import pandas as pd
from langgraph.types import Command, interrupt  # type: ignore

from vobchat.state_schema import (
    lg_State,
    get_selected_units,
    add_place_to_state,
    remove_place_from_state,
)
from vobchat.tools import find_units_by_postcode, find_places_by_name, get_place_key_findings
from vobchat.utils.constants import UNIT_TYPES
from .utils import _append_ai

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper – shared disambiguation utilities (reused from resolve_place_and_unit)
# -----------------------------------------------------------------------------

def _make_options(rows: List[Dict], kind: str = "place") -> List[Dict]:
    """Helper function to create options for disambiguation (reused from resolve_place_and_unit)."""
    if kind == "place":
        return [
            {
                "option_type": "place",
                "label": f"{r['g_name']}, {r.get('county_name', '')}",
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

def _disambiguate_place_name(place_name: str, state: lg_State, current_node: str = "PlaceInfo_node", store_coordinates: bool = False, **extra_interrupt_data) -> Optional[Dict]:
    """
    Disambiguate a place name to a specific g_place and return available unit rows.
    This handles ONLY the place name disambiguation step.
    
    Args:
        place_name: Name of the place to disambiguate
        state: Current workflow state
        current_node: The node calling this function (for interrupt context)
        store_coordinates: Whether to store selected coordinates for zoom functionality
    
    Returns:
        Dictionary with g_place, unit_rows, and other place info if resolved.
        None if no place found or disambiguation needed (will trigger interrupt).
    """
    from .utils import serialize_messages
    
    # Check if we're resuming from an interrupt
    place_entry = state.get("place_entry")
    if place_entry is None:
        # Create a new place entry for disambiguation logic
        place_entry = {
            "name": place_name,
            "g_place": None,
            "candidate_rows": None,
            "unit_rows": None
        }
    
    # Look up candidate rows if not already done
    if place_entry.get("candidate_rows") is None:
        try:
            res_json = find_places_by_name.invoke({
                "place_name": place_name,
                "county": "0",
                "unit_type": "0",
            })
            df = pd.read_json(io.StringIO(res_json), orient="records")
            place_entry["candidate_rows"] = df.to_dict("records")
        except Exception as e:
            logger.warning(f"Failed to look up place '{place_name}': {e}")
            return None
    
    rows = place_entry.get("candidate_rows", [])
    sel = state.get("selection_idx")
    
    # ── STEP 1: choose the correct *place* (g_place + unit_rows) ───────────
    if place_entry.get("g_place") is None:
        if not rows:
            return None  # No place found
            
        if len(rows) == 1:
            # Single match - auto-select
            r = rows[0]
            place_entry["g_place"] = r["g_place"]
            gu, gut = r["g_unit"], r["g_unit_type"]
            if not isinstance(gu, list):
                gu, gut = [gu], [gut]
            place_entry["unit_rows"] = [
                {"g_unit": u, "g_unit_type": t} for u, t in zip(gu, gut)
            ]
            # Also store the full place data for reference
            place_entry["place_data"] = r
            
            # Store coordinates for zoom functionality if requested
            if store_coordinates and r.get('lat') is not None and r.get('lon') is not None:
                try:
                    lat, lon = float(r['lat']), float(r['lon'])
                    if 49 <= lat <= 61 and -8 <= lon <= 2:  # UK bounds check
                        place_entry["selected_coordinates"] = {"lat": lat, "lon": lon}
                except (ValueError, TypeError):
                    pass
        else:
            # Multiple matches - need disambiguation
            if sel is not None and isinstance(sel, int) and 0 <= sel < len(rows):
                # User made a selection
                r = rows[sel]
                place_entry["g_place"] = r["g_place"]
                gu, gut = r["g_unit"], r["g_unit_type"]
                if not isinstance(gu, list):
                    gu, gut = [gu], [gut]
                place_entry["unit_rows"] = [
                    {"g_unit": u, "g_unit_type": t} for u, t in zip(gu, gut)
                ]
                place_entry["place_data"] = r
                
                # Store coordinates for zoom functionality if requested
                if store_coordinates and r.get('lat') is not None and r.get('lon') is not None:
                    try:
                        lat, lon = float(r['lat']), float(r['lon'])
                        if 49 <= lat <= 61 and -8 <= lon <= 2:  # UK bounds check
                            place_entry["selected_coordinates"] = {"lat": lat, "lon": lon}
                    except (ValueError, TypeError):
                        pass
                
                # Clear selection for next step
                state["selection_idx"] = None
            else:
                # Need to interrupt for place disambiguation
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
                
                interrupt_data = {
                    "message": f"More than one **{place_name}** - which do you mean?",
                    "options": _make_options(rows, kind="place"),
                    "place_coordinates": place_coordinates,
                    "current_node": current_node,
                    "place_entry": place_entry,  # Store for continuation
                    "messages": serialize_messages(state.get("messages", []))
                }
                # Add any extra interrupt data from the caller
                interrupt_data.update(extra_interrupt_data)
                interrupt(interrupt_data)
                return None  # Will resume after interrupt
    
    # Return the place entry with g_place and unit_rows resolved
    return place_entry

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
    filtered = df.to_json(orient='records', force_ascii=False, default_handler=str)
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
            "show_visualization": state.get("show_visualization", None),
            "current_place_index": 0,
            "places": state.get("places", []),
            "messages": state.get("messages", []),  # CRITICAL: Include messages to persist _append_ai changes
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

    _append_ai(state, f"Removed {display}.")

    return Command(
        goto="multi_place_tool_call",
        update={
            "last_intent_payload": {},
            "places": state.get("places", []),
            "selected_cubes": cubes_json,
            "show_visualization": state.get("show_visualization", None),
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

# -----------------------------------------------------------------------------
# Node – PlaceInfo_node
# -----------------------------------------------------------------------------

def PlaceInfo_node(state: lg_State) -> Dict[str, Any]:
    """Provide general information about a place, including key findings."""
    args = (state.get("last_intent_payload") or {}).get("arguments", {})
    place_name = args.get("place")
    
    if not place_name:
        _append_ai(state, "Please tell me which place you'd like to know about.")
        return {"messages": state.get("messages", [])}
    
    # Use the shared place disambiguation logic
    try:
        place_info = _disambiguate_place_name(place_name, state)
        
        if place_info is None:
            # Either no place found or disambiguation needed (interrupt triggered)
            place_entry = state.get("place_entry")
            if place_entry is None:
                # No place found
                _append_ai(state, f"Sorry, I couldn't find any information about '{place_name}'. Please check the spelling or try a different place name.")
                return {"messages": state.get("messages", []), "last_intent_payload": {}}
            else:
                # Disambiguation in progress - interrupt triggered
                return dict(state)
    
        # Now we have a disambiguated place with unit_rows available
        unit_rows = place_info.get("unit_rows", [])
        place_display_name = place_info.get("place_data", {}).get("g_name", place_name)
        
        if not unit_rows:
            _append_ai(state, f"**{place_display_name}** - Unfortunately, I couldn't find any administrative units for this place.")
            return {"messages": state.get("messages", []), "last_intent_payload": {}}
        
        # For PlaceInfo, just pick the first available unit (or prioritize certain types)
        # This avoids the complexity of unit type disambiguation for informational queries
        chosen_unit = unit_rows[0]  # Simple: take first unit
        
        # Could add more sophisticated selection logic here if needed:
        # - Prefer MOD_DIST over other types
        # - Prefer units with more recent data
        # For now, keep it simple
        
        g_unit = chosen_unit.get("g_unit")
        place_type = chosen_unit.get("g_unit_type", "Administrative unit")
        
        # Get user-friendly type name
        type_info = UNIT_TYPES.get(place_type, {})
        display_type = type_info.get("long_name", place_type)
        
        if not g_unit:
            _append_ai(state, f"**{place_display_name}** - Unfortunately, I couldn't find a valid unit identifier for this place.")
            return {"messages": state.get("messages", []), "last_intent_payload": {}}
        
        # Get key findings for this place
        try:
            findings_json = get_place_key_findings.invoke({"g_unit": g_unit})
            findings_df = pd.read_json(io.StringIO(findings_json), orient="records")
            
            if findings_df.empty:
                _append_ai(state, f"**{place_display_name}** is a {display_type}. Unfortunately, I don't have any specific key findings available for this place at the moment.")
            else:
                # Build the response with key findings
                response_parts = [
                    f"**{place_display_name}** is a {display_type}. Here are some key findings:"
                ]
                
                for _, finding in findings_df.iterrows():
                    label = finding.get('g_label', 'Finding')
                    text = finding.get('g_text')
                    
                    if text and text.strip():
                        response_parts.append(f"• **{label}**: {text}")
                    elif label:
                        response_parts.append(f"• {label}")
                
                if len(findings_df) >= 8:
                    response_parts.append("\n*This shows the top findings for this place.*")
                
                _append_ai(state, "\n\n".join(response_parts))
                
        except Exception as e:
            logger.warning(f"Failed to get key findings for g_unit {g_unit}: {e}")
            _append_ai(state, f"**{place_display_name}** is a {display_type}. I found the place but encountered an issue retrieving detailed information. Please try again later.")
            
    except Exception as e:
        logger.warning(f"Failed to look up place '{place_name}': {e}")
        _append_ai(state, f"Sorry, I encountered an issue while looking up information about '{place_name}'. Please try again later.")
    
    # Clear the intent payload and temporary state after processing
    return {
        "messages": state.get("messages", []),
        "last_intent_payload": {},
        "place_entry": None,
        "selection_idx": None
    }
