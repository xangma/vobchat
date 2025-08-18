"""Place-handling nodes for LangGraph.

Exports nodes that add/remove places, resolve postcodes and place names, and
provide informational text about places. These nodes update the single source
of truth in ``state['places']`` and coordinate with disambiguation helpers to
ask the user when multiple matches are possible.

Exported nodes:
- AddPlace_node: queue one or more places/polygons for resolution
- RemovePlace_node: remove a place by name or polygon id; trims cached cubes
- postcode_tool_call: resolve a UK-style postcode to one or more units
- multi_place_tool_call: DB lookup for each queued place needing candidates
- PlaceInfo_node: show a location overview and add a temporary info marker

Utilities expected from the wider app: ``_append_ai``, ``get_selected_units``,
``add_place_to_state``, ``remove_place_from_state``,
``find_units_by_postcode.invoke()``, ``find_places_by_name.invoke()``,
``get_place_information.invoke()``, ``interrupt``, and ``Command``.
"""

from __future__ import annotations

import io
import math
import logging
from typing import Dict, List, Optional, Union, Any

import pandas as pd
from langgraph.types import Command, interrupt  # type: ignore

try:
    # Newer langgraph exposes Interrupt for catching; fall back gracefully if not present
    from langgraph.types import Interrupt  # type: ignore
except Exception:  # pragma: no cover - optional import
    Interrupt = type(
        "Interrupt", (), {}
    )  # Sentinel fallback to allow isinstance checks
try:
    # Some LangGraph versions raise GraphInterrupt from langgraph.errors
    from langgraph.errors import GraphInterrupt  # type: ignore
except Exception:  # pragma: no cover - optional import
    GraphInterrupt = type("GraphInterrupt", (), {})

from vobchat.state_schema import (
    lg_State,
    get_selected_units,
    add_place_to_state,
    remove_place_from_state,
)
from vobchat.tools import (
    find_units_by_postcode,
    find_places_by_name,
    get_place_information,
)
from vobchat.utils.constants import UNIT_TYPES
from .utils import _append_ai, clean_database_text

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
                "label": UNIT_TYPES.get(r["g_unit_type"], {}).get(
                    "long_name", r["g_unit_type"]
                ),
                "value": r["g_unit_type"],
                "color": UNIT_TYPES.get(r["g_unit_type"], {}).get("color", "#333"),
            }
            for r in rows
        ]
    return []


def _disambiguate_unit_type(
    place: Dict,
    state: lg_State,
    current_node: str = "resolve_place_and_unit",
    **extra_interrupt_data,
) -> Optional[Dict]:
    """
    Disambiguate unit type for a place that already has g_place resolved.
    This handles ONLY the unit type disambiguation step.

    Args:
        place: Place dictionary with g_place and unit_rows already resolved
        state: Current workflow state
        current_node: The node calling this function (for interrupt context)
        **extra_interrupt_data: Additional data to pass to interrupt

    Returns:
        Dictionary with g_unit and g_unit_type if resolved.
        None if disambiguation needed (will trigger interrupt).
    """
    from .utils import serialize_messages

    units = place.get("unit_rows", [])
    if not units:
        return None

    if len(units) == 1:
        # Single unit - auto-select
        return units[0]

    # Multiple units - check for user selection
    sel = state.get("selection_idx")
    if sel and isinstance(sel, str):
        chosen = next((r for r in units if r["g_unit_type"] == sel), None)
        if chosen:
            return chosen

    # Need disambiguation - create place coordinates to keep marker visible
    place_coordinates = []
    rows = place.get("candidate_rows", [])

    if rows and len(rows) > 0:
        # Find the row that matches the selected g_place
        r = None
        if place.get("g_place"):
            r = next(
                (row for row in rows if row["g_place"] == place["g_place"]), rows[0]
            )
        else:
            r = rows[0]

        if r and r.get("lat") is not None and r.get("lon") is not None:
            try:
                lat, lon = float(r["lat"]), float(r["lon"])
                if 49 <= lat <= 61 and -8 <= lon <= 2:
                    place_coordinates.append(
                        {
                            "index": 0,
                            "name": place.get("name", r.get("g_name", "")),
                            "county": r.get("county_name", ""),
                            "lat": lat,
                            "lon": lon,
                            "g_place": place.get("g_place"),
                            "is_single": True,
                            "needs_unit_selection": True,
                        }
                    )
            except (ValueError, TypeError):
                pass

    interrupt_data = {
        "message": f"Which geography for **{place['name']}**?",
        "options": _make_options(units, kind="unit"),
        "place_coordinates": place_coordinates,
        "current_node": current_node,
        "messages": serialize_messages(state.get("messages", [])),
    }
    interrupt_data.update(extra_interrupt_data)
    interrupt(interrupt_data)
    return None  # Will resume after interrupt


def _disambiguate_place_name(
    place_name: str,
    state: lg_State,
    current_node: str,
    store_coordinates: bool = False,
    **extra_interrupt_data,
) -> Optional[Dict]:
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
            "unit_rows": None,
        }

    # Look up candidate rows if not already done
    if place_entry.get("candidate_rows") is None:
        try:
            res_json = find_places_by_name.invoke(
                {
                    "place_name": place_name,
                    "county": "0",
                    "unit_type": "0",
                }
            )
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
            if (
                store_coordinates
                and r.get("lat") is not None
                and r.get("lon") is not None
            ):
                try:
                    lat, lon = float(r["lat"]), float(r["lon"])
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
                if (
                    store_coordinates
                    and r.get("lat") is not None
                    and r.get("lon") is not None
                ):
                    try:
                        lat, lon = float(r["lat"]), float(r["lon"])
                        if 49 <= lat <= 61 and -8 <= lon <= 2:  # UK bounds check
                            place_entry["selected_coordinates"] = {
                                "lat": lat,
                                "lon": lon,
                            }
                    except (ValueError, TypeError):
                        pass

                # Clear selection for next step
                state["selection_idx"] = None
            else:
                # Need to interrupt for place disambiguation
                place_coordinates = []
                for j, r in enumerate(rows):
                    if r.get("lat") is not None and r.get("lon") is not None:
                        try:
                            lat, lon = float(r["lat"]), float(r["lon"])
                            # Validate UK bounds
                            if 49 <= lat <= 61 and -8 <= lon <= 2:
                                place_coordinates.append(
                                    {
                                        "index": j,
                                        "name": r["g_name"],
                                        "county": r["county_name"],
                                        "lat": lat,
                                        "lon": lon,
                                        "g_place": r["g_place"],
                                    }
                                )
                        except (ValueError, TypeError):
                            pass

                interrupt_data = {
                    "message": f"More than one **{place_name}** - which do you mean?",
                    "options": _make_options(rows, kind="place"),
                    "place_coordinates": place_coordinates,
                    "current_node": current_node,
                    "place_entry": place_entry,  # Store for continuation
                    "messages": serialize_messages(state.get("messages", [])),
                }
                # Add any extra interrupt data from the caller
                interrupt_data.update(extra_interrupt_data)
                interrupt(interrupt_data)

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
    filtered = df.to_json(orient="records", force_ascii=False, default_handler=str)
    state["selected_cubes"] = filtered
    return filtered


# -----------------------------------------------------------------------------
# Node – AddPlace_node
# -----------------------------------------------------------------------------


def AddPlace_node(state: lg_State) -> Dict[str, Union[str, list, dict]] | Command:
    """Queue one or more places for resolution and kick off lookup.

    Reads arguments from ``last_intent_payload`` supporting names, optional
    ``polygon_id`` and ``unit_type``. Appends provisional entries to
    ``state['places']`` and routes to ``multi_place_tool_call`` to fetch
    candidate rows. Returns a ``Command`` with minimal updates including
    messages so that any prompts are preserved.
    """
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
        msg = _append_ai(state, "AddPlace: please tell me which place to add.")
        return {"messages": [msg]}

    # 2️⃣  Add to state -----------------------------------------------------------
    plural = ", ".join(names)
    msg = _append_ai(state, f"Okay – adding {plural}. Let me look them up…")

    for i, nm in enumerate(names):
        gut = unit_types[i] if i < len(unit_types) else None
        gu = polygon_ids[i] if i < len(polygon_ids) else None
        add_place_to_state(
            state,
            name=nm,
            g_unit=gu,
            g_unit_type=gut,
        )
    # If we were given explicit polygon ids (map-click or direct), skip DB lookup
    # and move straight to updating the polygon selection on the map.
    if polygon_ids:
        return Command(
            goto="update_polygon_selection",
            update={
                "last_intent_payload": {},
                "extracted_theme": None,
                "show_visualization": state.get("show_visualization", None),
                "current_place_index": 0,
                "places": state.get("places", []),
                "messages": [msg],
            },
        )

    # Otherwise, proceed with lookup for unresolved place names
    return Command(
        goto="multi_place_tool_call",
        update={
            "last_intent_payload": {},
            "extracted_theme": None,
            "show_visualization": state.get("show_visualization", None),
            "current_place_index": 0,
            "places": state.get("places", []),
            "messages": [msg],  # Delta message for reducer
        },
    )


# -----------------------------------------------------------------------------
# Node – RemovePlace_node
# -----------------------------------------------------------------------------


def RemovePlace_node(state: lg_State) -> Dict[str, Union[str, list, dict]] | Command:
    """Remove a place by name or polygon id and update caches.

    Also prunes ``selected_cubes`` to match remaining units, then routes to
    ``multi_place_tool_call`` so place lookups remain in a consistent state.
    """
    args = (state.get("last_intent_payload") or {}).get("arguments", {})
    # Accept explicit polygon_id first if provided; else fall back to place name
    polygon_id = args.get("polygon_id")
    target_raw: Optional[str] = args.get("place")
    place_name_for_display: Optional[str] = None
    try:
        if isinstance(target_raw, str) and target_raw.strip():
            place_name_for_display = target_raw.strip()
    except Exception:
        place_name_for_display = None

    # Determine identifier to remove
    target = None
    display = None
    if polygon_id is not None:
        try:
            target_id = int(polygon_id)
            target = target_id
            # Prefer a human-friendly name if provided alongside the polygon id
            display = place_name_for_display or f"Polygon {target_id}"
        except (ValueError, TypeError):
            # Ignore invalid polygon_id and try place name
            target = None
            display = None

    if target is None:
        if not target_raw:
            msg = _append_ai(
                state, "Tell me which place to remove, e.g. ‘remove Oxford’."
            )
            return {"messages": [msg]}
        # Accept either name or integer polygon id in the place field
        try:
            target_id = int(target_raw)
            target = target_id
            display = place_name_for_display or f"Polygon {target_id}"
        except (ValueError, TypeError):
            target = target_raw
            display = place_name_for_display or target_raw

    removed = remove_place_from_state(state, target)
    if not removed:
        # Idempotent UX for map clicks with polygon ids: treat as removed
        src = str(args.get("source") or "").strip().lower()
        if src == "map_click" and polygon_id is not None:
            remaining_units = get_selected_units(state)
            cubes_json = _filter_cubes(state, remaining_units)
            msg = _append_ai(state, f"Removed {display}.")
            return Command(
                goto="multi_place_tool_call",
                update={
                    "last_intent_payload": {},
                    "places": state.get("places", []),
                    "selected_cubes": cubes_json,
                    # Clamp current_place_index to valid range after removal
                    "current_place_index": min(
                        int(state.get("current_place_index") or 0), len(state.get("places", []))
                    ),
                    "show_visualization": state.get("show_visualization", None),
                    "map_update_request": {
                        "action": "update_map_selection",
                        "places": state.get("places", []),
                    },
                    "messages": [msg],
                },
            )
        msg = _append_ai(state, f"{display} isn’t in your selection.")
        return {"messages": [msg]}

    remaining_units = get_selected_units(state)
    cubes_json = _filter_cubes(state, remaining_units)

    msg = _append_ai(state, f"Removed {display}.")

    return Command(
        goto="multi_place_tool_call",
        update={
            "last_intent_payload": {},
            "places": state.get("places", []),
            "selected_cubes": cubes_json,
            # Clamp current_place_index to valid range after removal
            "current_place_index": min(
                int(state.get("current_place_index") or 0), len(state.get("places", []))
            ),
            "show_visualization": state.get("show_visualization", None),
            "map_update_request": {
                "action": "update_map_selection",
                "places": state.get("places", []),
            },
            "messages": [msg],
        },
    )


# -----------------------------------------------------------------------------
# Node – postcode_tool_call
# -----------------------------------------------------------------------------


def postcode_tool_call(state: lg_State):
    """Resolve a UK-style postcode to units and add them as places.

    On success, appends one or more places with their resolved ``g_unit`` and
    type label, and clears ``extracted_postcode``. Leaves visualization control
    to later steps (no interrupt here).
    """
    pcode = state.get("extracted_postcode")
    if not pcode:
        msg = _append_ai(state, "I couldn’t find a postcode to search for.")
        return {"messages": [msg]}

    json_res = find_units_by_postcode.invoke({"postcode": pcode})
    try:
        df = pd.read_json(io.StringIO(json_res), orient="records")
    except ValueError:
        msg = _append_ai(state, f"Sorry – postcode lookup for {pcode} failed.")
        return {"messages": [msg]}

    if df.empty:
        msg = _append_ai(state, f"No places found for postcode {pcode}.")
        return {"messages": [msg]}

    for _, row in df.iterrows():
        add_place_to_state(
            state,
            name=row.get("name", f"Unit {row.get('g_unit')}"),
            g_unit=row.get("g_unit"),
            g_unit_type=row.get("type_label"),
        )

    if len(df) == 1:
        row = df.iloc[0]
        msg = _append_ai(
            state, f"Found {row['name']} ({row['type_label']}) for postcode {pcode}."
        )
    else:
        msg = _append_ai(state, f"Added {len(df)} places for postcode {pcode}.")

    return {
        "messages": [msg],
        "places": state.get("places", []),
        "extracted_postcode": None,
        "is_postcode": False,
    }


# -----------------------------------------------------------------------------
# Node – multi_place_tool_call
# -----------------------------------------------------------------------------


def multi_place_tool_call(state: lg_State):
    """DB lookup for each place lacking candidates or a resolved ``g_unit``.

    For each pending place, calls the DB tool to populate ``candidate_rows``
    so that downstream disambiguation can proceed without blocking.
    """

    places = state.get("places", [])
    for entry in places:
        if entry.get("g_unit") is not None or entry.get("candidate_rows"):
            continue  # already resolved / looked up
        name = entry.get("name", "")
        unit_type = entry.get("g_unit_type", "0") or "0"
        try:
            res_json = find_places_by_name.invoke(
                {
                    "place_name": name,
                    "county": "0",
                    "unit_type": unit_type,
                }
            )
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
    """Provide an overview of a place and show a temporary info marker.

    Uses the same name disambiguation helper to identify a specific place and
    then fetch enriched descriptive text and modern context for display. This
    does not add to the ``places`` selection; instead it may request the UI to
    show a one-off info marker via ``map_update_request``.
    """
    args = (state.get("last_intent_payload") or {}).get("arguments", {})
    place_name = args.get("place")

    if not place_name:
        msg = _append_ai(state, "Please tell me which place you'd like to know about.")
        return {"messages": [msg]}

    # Use the shared place disambiguation logic with coordinate storage for map marking
    try:
        place_info = _disambiguate_place_name(
            place_name, state, current_node="PlaceInfo_node", store_coordinates=True
        )
        if place_info is None:
            # Either no place found or disambiguation needed (interrupt triggered)
            place_entry = state.get("place_entry")
            if place_entry is None:
                # No place found
                msg = _append_ai(
                    state,
                    f"Sorry, I couldn't find any information about '{place_name}'. Please check the spelling or try a different place name.",
                )
                return {"messages": [msg], "last_intent_payload": {}}
            else:
                # Disambiguation in progress - interrupt triggered
                return dict(state)

        # Get the g_place from the disambiguated place
        g_place = place_info.get("g_place")
        if not g_place:
            msg = _append_ai(
                state,
                f"Sorry, I couldn't find complete information about '{place_name}'.",
            )
            return {"messages": [msg], "last_intent_payload": {}}

        # Get detailed place information using the new tool
        place_info_json = get_place_information.invoke({"g_place": g_place})
        place_df = pd.read_json(io.StringIO(place_info_json), orient="records")

        if place_df.empty:
            msg = _append_ai(
                state,
                f"Sorry, I couldn't find detailed information about '{place_name}'.",
            )
            return {"messages": [msg], "last_intent_payload": {}}

        place_data = place_df.iloc[0]
        place_display_name = place_data.get("g_name", place_name)

        # For PlaceInfo, we don't add to places array - just show a point marker

        # Build the response based on available information
        response_parts = []

        # Header with place name and location context
        location_context = []
        if place_data.get("county_name"):
            location_context.append(place_data["county_name"])
        if place_data.get("nation_name") and place_data[
            "nation_name"
        ] != place_data.get("county_name"):
            location_context.append(place_data["nation_name"])

        if location_context:
            response_parts.append(
                f"**{place_display_name}** in {', '.join(location_context)}"
            )
        else:
            response_parts.append(f"**{place_display_name}**")

        # Historical description from gazetteer (dg_text)
        if place_data.get("dg_text") and place_data.get("dg_text_auth"):
            dg_text = (
                str(place_data["dg_text"]).strip()
                if place_data["dg_text"] is not None
                else ""
            )
            if dg_text:
                response_parts.append("\n**Historical Description:**")
                response_parts.append(
                    f"{place_data['dg_text_auth']} described {place_display_name} like this:"
                )
                # Clean DB text and render as paragraphs (no manual wrapping/blockquote)
                cleaned = clean_database_text(dg_text)
                response_parts.append(f"\n{cleaned}")

        # Additional notes
        if place_data.get("notes"):
            notes = (
                str(place_data["notes"]).strip()
                if place_data["notes"] is not None
                else ""
            )
            if notes:
                response_parts.append("\n**Additional Information:**")
                response_parts.append(clean_database_text(notes))

        # See also place reference
        if place_data.get("see_also_place") and place_data.get("see_also_place_name"):
            response_parts.append(
                f"\n**See Also:** Additional information is available for {place_data['see_also_place_name']}."
            )

        # Modern administrative context
        if place_data.get("district_name"):
            response_parts.append(f"\n**Modern Context:**")
            if place_data.get("is_district") == "Y":
                response_parts.append(
                    f"{place_display_name} is currently the {place_data['district_name']} {place_data.get('district_type', 'district')}."
                )
            else:
                response_parts.append(
                    f"{place_display_name} is now part of {place_data['district_name']} {place_data.get('district_type', 'district')}."
                )

        # Add coordinate information if available and valid (avoid NaN/Inf)
        lat_raw = place_data.get("lat")
        lon_raw = place_data.get("lon")
        try:
            lat_val = float(lat_raw) if lat_raw is not None else None
            lon_val = float(lon_raw) if lon_raw is not None else None
        except (ValueError, TypeError):
            lat_val = None
            lon_val = None

        def _is_finite(x: float | None) -> bool:
            return x is not None and math.isfinite(x)

        if _is_finite(lat_val) and _is_finite(lon_val):
            lat_dir = "N" if lat_val >= 0 else "S"
            lon_dir = "E" if lon_val >= 0 else "W"
            response_parts.append(
                f"\n**Location:** {abs(lat_val):.3f}°{lat_dir}, {abs(lon_val):.3f}°{lon_dir}"
            )

        # Multiple names note
        if place_data.get("has_multiple_names") == "Y":
            response_parts.append(
                f"\n*This place is known by multiple historical names.*"
            )

        # Combine all parts and emit an assistant message
        if len(response_parts) > 1:
            out_msg = _append_ai(state, "\n".join(response_parts))
        else:
            out_msg = _append_ai(
                state,
                f"**{place_display_name}** - I found this place but don't have detailed historical information available.",
            )

        # Create a temporary info-only place entry for marker display
        info_place = None
        coordinates = None

        # Get coordinates from place info or place data
        if place_info.get("selected_coordinates"):
            coordinates = place_info["selected_coordinates"]
        elif place_data.get("lat") and place_data.get("lon"):
            try:
                lat, lon = float(place_data["lat"]), float(place_data["lon"])
                if 49 <= lat <= 61 and -8 <= lon <= 2:
                    coordinates = {"lat": lat, "lon": lon}
            except (ValueError, TypeError):
                pass

        # Create a temporary place entry with special marker flag
        if coordinates:
            info_place = {
                "name": place_display_name,
                "g_unit": None,  # No unit - just a point marker
                "g_unit_type": None,
                "g_place": g_place,
                "coordinates": coordinates,
                "is_info_marker": True,  # Special flag for frontend
                "county_name": place_data.get("county_name", ""),
            }

    except Exception as e:
        # Re-raise LangGraph interrupts so the frontend can handle disambiguation
        if isinstance(e, (Interrupt, GraphInterrupt)):
            raise
        logger.warning(f"Failed to look up place '{place_name}': {e}")
        err_msg = _append_ai(
            state,
            f"Sorry, I encountered an issue while looking up information about '{place_name}'. Please try again later.",
        )
        return {
            "messages": [err_msg],
            "last_intent_payload": {},
            "place_entry": None,
            "selection_idx": None,
        }

    # Clear the intent payload and temporary state after processing
    result = {
        "messages": [out_msg],
        "last_intent_payload": {},
        "place_entry": None,
        "selection_idx": None,
        "places": state.get("places", []),
    }

    # Add map update request with info marker if we have coordinates
    if "info_place" in locals() and info_place:
        # Create a special map update that shows the info marker
        result["map_update_request"] = {
            "action": "show_info_marker",
            "info_place": info_place,
            "places": state.get("places", []),  # Keep existing selection
        }

    return result
