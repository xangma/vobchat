from __future__ import annotations
import io
from langgraph.types import interrupt
from langgraph.graph import END
"""Intent‑handler nodes (no LLM) for the flexible DDME workflow.
Each node is registered under the name `<Intent>_node` to match AssistantIntent.
"""

from typing import List, Optional, Dict
import re
import pandas as pd
from langchain_core.messages import AIMessage
from langgraph.types import Command

from state_schema import lg_State
from intent_handling import AssistantIntent
from tools import (
    find_themes_for_unit, get_all_themes,
    find_units_by_postcode, find_places_by_name,
)
import logging
# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def _append_ai(state: lg_State, text: str):
    state.setdefault("messages", []).append(AIMessage(content=text))


def _maybe_route_to_cubes(state: lg_State):
    """Jump to cube retrieval when both slots (theme + ≥1 unit) are filled."""
    if state.get("selected_theme") and state.get("selected_place_g_units"):
        return Command(goto="find_cubes_node")
    return state


def _initial_state() -> Dict:
    """Return a fresh lg_State dict with only the keys we mutate here."""
    return {
        "messages": [],
        "selection_idx": None,
        "selected_place_g_places": [],
        "selected_place_g_units": [],
        "selected_place_g_unit_types": [],
        "selected_place_themes": None,
        "selected_theme": None,
        "extracted_place_names": [],
        "extracted_counties": [],
        "multi_place_search_df": None,
        "current_place_index": 0,
        "is_postcode": False,
        "extracted_postcode": None,
        "selected_polygons": [],
        "selected_polygons_unit_types": [],
        "min_year": None,
        "max_year": None,
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
        df = pd.read_json(state["selected_theme"], orient="records")
        summary.append(f"• theme: {df.iloc[0]['labl']}")
    else:
        summary.append("• no theme selected yet")

    yrs = (state.get("min_year"), state.get("max_year"))
    if any(yrs):
        summary.append(f"• years: {yrs[0] or '…'} – {yrs[1] or '…'}")

    _append_ai(state, "Current selection:\n" + "\n".join(summary))
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 2. ListThemesForSelection
# ─────────────────────────────────────────────────────────────────────────────

def ListThemesForSelection_node(state: lg_State):
    g_units = state.get("selected_place_g_units", [])
    if not g_units:
        _append_ai(state, "Pick a place first so I can list its themes.")
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
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 4. Reset
# ─────────────────────────────────────────────────────────────────────────────

def Reset_node(state: lg_State):
    _append_ai(state, "Starting over – previous selections cleared.")
    return Command(goto="START", update=_initial_state())

# ─────────────────────────────────────────────────────────────────────────────
# 5. AddPlace
# ─────────────────────────────────────────────────────────────────────────────

def AddPlace_node(state: lg_State):
    args = (state.get("last_intent_payload") or {}).get("arguments", {})

    # ── gather place names to add ───────────────────────────────────
    names_to_add: List[str] = []
    counties_to_add: List[str] = []
    if "places" in args and isinstance(args["places"], list):
        names_to_add = [p.strip() for p in args["places"] if p.strip()]
    elif "place" in args:
        names_to_add = [args["place"].strip()]
    if "counties" in args and isinstance(args["counties"], list):
        counties_to_add = [p.strip() for p in args["counties"] if p.strip()]
    elif "county" in args:
        counties_to_add = [args["county"].strip()]

    if not names_to_add:
        _append_ai(state, "AddPlace: please specify at least one place name.")
        return state

    # ── extend the existing queues ─────────────────────────────────
    names    = state.get("extracted_place_names", [])
    counties = state.get("extracted_counties", [])
    for p in names_to_add:
        names.append(p)
    for c in counties_to_add:
        counties.append(c)

    # pointer to the **first** new place
    new_idx = len(names) - len(names_to_add)

    plural = ", ".join(names_to_add)
    _append_ai(state, f"Okay – adding {plural}. Let me find them …")

    update = {
        "messages": state["messages"],
        "extracted_place_names": names,
        "extracted_counties": counties,
        "multi_place_search_df": None,
        "current_place_index": new_idx,
        "last_intent_payload": {},
    }


    return Command(goto="multi_place_tool_call", update=update)

# ─────────────────────────────────────────────────────────────────────────────
# 6. AddTheme
# ─────────────────────────────────────────────────────────────────────────────

def AddTheme_node(state: lg_State):
    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}

    # direct code
    if "theme_code" in args:
        code = args["theme_code"].strip().upper()
        if not code.startswith("T_"):
            _append_ai(state, f"‘{code}’ doesn’t look like a valid theme code.")
            return state
        state["selected_theme"] = pd.DataFrame({"ent_id": [code], "labl": [code]}).to_json(orient="records")
        _append_ai(state, f"Theme set to {code}.")
        return _maybe_route_to_cubes(state)

    # free text query
    if "theme_query" in args:
        q = args["theme_query"].strip()
        state["extracted_theme"] = q
        state["selected_theme"] = None
        _append_ai(state, f"Looking for a theme matching “{q}”…")
        return Command(goto="get_place_themes_node", update=state)

    _append_ai(state, "AddTheme: no theme_code or theme_query provided.")
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
    _append_ai(state, "Theme selection cleared.")
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 8. RemovePlace
# ─────────────────────────────────────────────────────────────────────────────

def RemovePlace_node(state: lg_State):
    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}
    place: Optional[str] = args.get("place")

    if not place:
        _append_ai(state, "Tell me which place to remove, e.g. ‘remove Oxford’.")
        return state

    place_names = state.get("extracted_place_names", [])
    place = place.lower()
    place_names = [p.lower() for p in place_names]
    county_names = state.get("extracted_counties", [])
    county_names = [c.lower() for c in county_names]
    
    if place not in place_names:
        _append_ai(state, f"{place} isn’t in your selection.")
        state["last_intent_payload"] = {} 
        return Command(goto=END)

    idx = place_names.index(place)
    place_names.pop(idx)
    if idx < len(county_names):
        county_names.pop(idx)
    for key in ("extracted_place_names", "selected_place_g_places", "selected_place_g_units", "selected_place_g_unit_types", "selected_polygons", "selected_polygons_unit_types"):
        lst = state.get(key, [])
        if idx < len(lst):
            lst.pop(idx)
        state[key] = lst

    remaining_units = state.get("selected_place_g_units", [])
    cubes_filtered: pd.DataFrame = pd.DataFrame(columns=["g_unit"]) 
    if state.get("selected_cubes"):
        try:
            df = pd.read_json(state["selected_cubes"], orient="records")
            if not df.empty and "g_unit" in df.columns:
                df = df[df["g_unit"].isin(remaining_units)]
                cubes_filtered = df.to_json(orient="records")
        except Exception:          # defensive – fallback to clearing
            cubes_filtered = pd.DataFrame(columns=["g_unit"]).to_json(orient="records")

    if len(cubes_filtered) > 0:
        show_viz = True
    else:
        show_viz = False

    interrupt(value={
    "message": f"Removed {place} from the selection.",
    "extracted_place_names": place_names,
    "extracted_counties": county_names,
    "last_intent_payload": {},
    "selected_place_g_places": state.get("selected_place_g_places", []),
    "selected_place_g_units": state.get("selected_place_g_units", []),
    "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
    "cubes": cubes_filtered,            # ↓ front‑end will overwrite its store
    "selected_cubes": cubes_filtered,   # ↓ persist for future turns
    "show_visualization_signal": show_viz,
    "selected_polygons": state.get("selected_polygons", []),
    "selected_polygons_unit_types": state.get("selected_polygons_unit_types", []),
    "current_node": "select_unit_on_map",
    })
# ─────────────────────────────────────────────────────────────────────────────