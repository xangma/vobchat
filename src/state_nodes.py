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

from state_schema import lg_State
from intent_handling import AssistantIntent
from tools import (
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
    state["needs_clarification"] = False
    return state

# ─────────────────────────────────────────────────────────────────────────────
# 4. Reset
# ─────────────────────────────────────────────────────────────────────────────

def Reset_node(state: lg_State):
    _append_ai(state, "Starting over - previous selections cleared.")
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
    _append_ai(state, f"Okay - adding {plural}. Let me find them …")

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
            _append_ai(state, f"‘{code}' doesn't look like a valid theme code.")
            return state
        state["selected_theme"] = pd.DataFrame({"ent_id": [code], "labl": [code]}).to_json(orient="records")
        _append_ai(state, f"Theme set to {code}.")
        return _maybe_route_to_cubes(state)

    # free text query
    elif "theme_query" in args:
        q = args["theme_query"].strip()
        state["extracted_theme"] = q
        state["selected_theme"] = None
        _append_ai(state, f"Looking for a theme matching “{q}”…")

    else:
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
        _append_ai(state, "Tell me which place to remove, e.g. ‘remove Oxford'.")
        return state

    place_names = state.get("extracted_place_names", [])
    place = place.lower()
    place_names = [p.lower() for p in place_names]
    county_names = state.get("extracted_counties", [])
    county_names = [c.lower() for c in county_names]
    
    if place not in place_names:
        _append_ai(state, f"{place} isn't in your selection.")
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
        except Exception:          # defensive - fallback to clearing
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
    "cubes": cubes_filtered,            # ↓ front-end will overwrite its store
    "selected_cubes": cubes_filtered,   # ↓ persist for future turns
    "show_visualization_signal": show_viz,
    "selected_polygons": state.get("selected_polygons", []),
    "selected_polygons_unit_types": state.get("selected_polygons_unit_types", []),
    "current_node": "select_unit_on_map",
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
        }
    )

    # Execution pauses here.  Front-end shows the buttons, user picks one, and
    # the graph will re-enter this node with selection_idx set.
    return state
# ─────────────────────────────────────────────────────────────────────────────