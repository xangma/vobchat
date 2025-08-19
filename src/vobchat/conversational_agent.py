"""Conversational agent node that plans actions and delegates to existing nodes.

Why this file exists:
- The existing router relies on rule/prioritized intents. This agent lets the
  LLM choose next steps using a structured "plan". It keeps your proven nodes
  intact by translating the plan back into ``last_intent_payload`` and the
  ``intent_queue`` that the workflow already understands.

High-level behavior:
- Build light context from state (selected places/theme/units)
- Ask the LLM for a short, JSON-only plan: actions + optional "final_reply"
- If actions exist → queue extras, route to the first action's node via Command
- If no actions → append the final reply (if any) and end the turn

Notes on logging:
- We avoid logging full message contents or whole state blobs; we prefer short
  summaries to aid debugging without leaking sensitive data.
"""

from __future__ import annotations

from typing import List, Dict, Optional, Set
import json
from enum import Enum
from pydantic import BaseModel, Field
import os
import logging

from vobchat.llm_factory import get_llm
from vobchat.configure_logging import get_llm_callback
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage
from langgraph.types import Command

from .state_schema import lg_State, get_selected_place_names, get_selected_units
from .intent_subagents import extract_intent_with_subagents
from .nodes.utils import _append_ai
from .configure_logging import log_llm_interaction

logger = logging.getLogger(__name__)

# Guardrail: limit number of actions executed per turn
MAX_ACTIONS = int(os.getenv("VOBCHAT_PLANNER_MAX_ACTIONS", "4"))


# --------------------------------------------------------------------------------------
# Pydantic schema for structured output
# --------------------------------------------------------------------------------------


class ActionName(str, Enum):
    AddPlace = "AddPlace"
    RemovePlace = "RemovePlace"
    AddTheme = "AddTheme"
    RemoveTheme = "RemoveTheme"
    ShowState = "ShowState"
    ListThemes = "ListThemes"
    PlaceInfo = "PlaceInfo"
    Reset = "Reset"
    FetchCubes = "FetchCubes"
    Chat = "Chat"
    UnitTypeInfo = "UnitTypeInfo"
    DescribeTheme = "DescribeTheme"
    DataEntityInfo = "DataEntityInfo"
    ExplainVisibleData = "ExplainVisibleData"


class PlannedAction(BaseModel):
    intent: ActionName = Field(..., description="Action to perform")
    arguments: Dict[str, object] = Field(
        default_factory=dict,
        description="Arguments for the action, e.g. {place: str} or {theme_query: str}",
    )


class AssistantPlan(BaseModel):
    actions: List[PlannedAction] = Field(
        default_factory=list,
        description="Ordered list of actions to perform. Empty means respond only.",
    )
    final_reply: Optional[str] = Field(
        default=None,
        description="Optional natural reply to the user when no more actions are necessary.",
    )


# --------------------------------------------------------------------------------------
# Prompt template
# --------------------------------------------------------------------------------------

_PLANNER_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
You are a helpful assistant for a statistics chat app. You can plan actions to
update the user's selection (places and theme) and fetch results. When missing
critical info, ask a brief clarifying question instead of guessing.

Available actions (intents) you can propose in JSON:
- AddPlace {{place: str}} or {{places: list[str]}} or {{postcode: str}}
- RemovePlace {{place: str}}
- AddTheme {{theme_query: str}}
- RemoveTheme {{}}
- ShowState {{}}
- ListThemes {{}}
- UnitTypeInfo {{unit_type: str}} (use when the user asks what a unit type is, e.g., "what is Local Government District" or "what is LG_DIST")
- DescribeTheme {{theme_query: str}} (use when asked to explain/describe a theme)
- DataEntityInfo {{entity_id: str}} (use when asked to explain a data entity by ID, e.g., codes like N_*, T_*, U_*, V_*)
- ExplainVisibleData {{}} (use when the user asks to explain/describe the data currently shown/visualised; summarise all visible data entities.)
- PlaceInfo {{place: str}}
- Reset {{}}
- FetchCubes {{}}
- Chat {{text: str}}  (use only when you just need to reply and not change state)

Rules:
- Prefer concrete actions over Chat when the user asks to do something.
- If the place is ambiguous or a theme is unclear, ask one concise question in final_reply and propose no actions.
- Use AddPlace for location queries like "where's X", "show me X", "find X".
- For "stats/data/statistics for X", include AddPlace and AddTheme.
- If the user asks what a unit type means (by code or label), propose UnitTypeInfo with that code/label in arguments.
- If UI option buttons are visible (see CONTEXT → UI options), you may reference them and avoid proposing duplicate actions.
- Return STRICT JSON matching the schema. Do not include code fences or extra text.
- If the user asks about this app or what it does, DO NOT propose actions; return a concise explanation in final_reply.
Return only valid JSON matching the schema; no explanation.
        """.strip(),
        ),
        (
            "user",
            """
CONTEXT
- Selected places: {selected_places}
- Selected theme: {selected_theme}
- Selected units: {selected_units}
- App overview: {app_overview}
- UI options (if any):\n{ui_options}
- UI option labels (if any): {ui_option_labels}
- Recent conversation (last turns):\n{recent_conversation}
- Domain hints (if any):\n{domain_hints}
- Memory summary: {memory_summary}

USER_MESSAGE
{user_text}

Respond with JSON only: {{"actions": [{{"intent": "...", "arguments": {{...}}}}], "final_reply": "..."}}.
        """.strip(),
        ),
    ]
)


def _try_parse_plan_from_text(txt: str) -> Optional["AssistantPlan"]:
    """Parse AssistantPlan from a JSON string with Pydantic v1/v2 compatibility."""
    if not txt:
        return None
    try:
        # Pydantic v2
        if hasattr(AssistantPlan, "model_validate_json"):
            return AssistantPlan.model_validate_json(txt)
    except Exception:
        pass
    try:
        # Pydantic v1
        if hasattr(AssistantPlan, "parse_raw"):
            return AssistantPlan.parse_raw(txt)
    except Exception:
        pass
    try:
        data = json.loads(txt)
        if isinstance(data, dict):
            return AssistantPlan(**data)
    except Exception:
        return None
    return None


def _summarize_selection(state: lg_State) -> Dict[str, object]:
    """Return a compact description of the user's current selection.

    The goal is to give the planner enough context without overloading tokens
    or leaking the raw state structure. We summarize as strings/integers.
    """
    places = get_selected_place_names(state)
    theme = state.get("selected_theme") or "(none)"
    units = get_selected_units(state)
    return {
        "selected_places": ", ".join(places) if places else "(none)",
        "selected_theme": theme,
        "selected_units": len(units) if units else 0,
    }


def _summarize_recent_conversation(
    state: lg_State, max_messages: int = 6, max_chars: int = 600
) -> str:
    """Return a short summary of the last few conversation turns.

    Includes role and truncated content. Avoids dumping large or sensitive text.
    """
    messages = state.get("messages", []) or []
    if not messages:
        return "(none)"
    recent = messages[-max_messages:]
    lines: List[str] = []
    for m in recent:
        try:
            role = getattr(m, "type", None) or getattr(m, "role", None) or "message"
            content = (m.content or "").strip()
            if len(content) > 200:
                content = content[:200] + "…"
            lines.append(f"{role}: {content}")
        except Exception:
            continue
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text if text else "(none)"


# --------------------------------------------------------------------------------------
# Hint providers (general mechanism)
# --------------------------------------------------------------------------------------


def _build_domain_hints(state: lg_State, user_text: str, max_chars: int = 800) -> str:
    """Collect compact, domain-specific hint blocks for the planner.

    This is a general mechanism: providers can look at state + user_text and
    decide whether to emit a small hint (e.g., catalog labels).
    """
    providers = [
        _theme_labels_hint,
        _unit_type_labels_hint,
        _place_candidates_hint,
        _unit_type_candidates_hint,
        _ready_to_fetch_hint,
        _visible_cubes_hint,
    ]
    parts: List[str] = []
    for prov in providers:
        try:
            h = prov(state, user_text)
            if h:
                parts.append(h)
        except Exception:
            continue
    if not parts:
        return "(none)"
    text = "\n".join(parts)
    if len(text) > max_chars:
        text = text[:max_chars] + "…"
    return text


def _theme_labels_hint(
    state: lg_State, user_text: str, max_items: int = 20
) -> Optional[str]:
    """If the current turn is theme-explanatory, provide a compact themes list.

    Trigger: user_text mentions both a theme intent and an explanatory verb
    like "what is", "describe", or "explain".
    """
    try:
        # Lazy import to avoid heavy deps; get_all_themes returns JSON
        from vobchat.tools import get_all_themes
        import io
        import pandas as pd  # used locally to keep it simple

        themes_json = get_all_themes("")
        df = pd.read_json(io.StringIO(themes_json), orient="records")
        if df.empty:
            return None
        labels = [str(x) for x in list(df["labl"])[:max_items]]
        return "Themes: " + "; ".join(labels)
    except Exception:
        return None


def _unit_type_labels_hint(
    state: lg_State, user_text: str, max_items: int = 12
) -> Optional[str]:
    """Provide a compact list of unit type labels when relevant.

    Uses human-readable names only (no internal codes) as these are not user-relevant.
    Trigger words: unit type, geography type, constituency, ward, district, parish, borough.
    """
    txt = (user_text or "").lower()
    triggers = [
        "unit type",
        "geography type",
        "constituency",
        "ward",
        "district",
        "parish",
        "borough",
    ]
    if not any(t in txt for t in triggers):
        return None
    try:
        # Pull labels from constants; avoid exposing codes
        from vobchat.utils.constants import (
            UNIT_TYPES,
        )  # dict[code] -> {long_name, color, ...}

        labels = [v.get("long_name") or k for k, v in UNIT_TYPES.items()]
        labels = [str(x) for x in labels if x]
        if not labels:
            return None
        labels = labels[:max_items]
        return "Geography types: " + "; ".join(labels)
    except Exception:
        return None


def _place_candidates_hint(
    state: lg_State, user_text: str, max_items: int = 10
) -> Optional[str]:
    """Surface current place candidates from options if present.

    Looks for option_type == 'place' in state.options and emits a one-line list.
    """
    opts = state.get("options") or []
    if not opts:
        return None
    places = [o for o in opts if o.get("option_type") == "place"]
    if not places:
        return None
    labels = []
    for o in places[:max_items]:
        lab = (o.get("label") or "").strip()
        if lab:
            labels.append(lab)
    return ("Place candidates: " + "; ".join(labels)) if labels else None


def _unit_type_candidates_hint(
    state: lg_State, user_text: str, max_items: int = 8
) -> Optional[str]:
    """Surface current unit-type choices from options if present.

    Looks for option_type == 'unit' in state.options and emits their labels only.
    """
    opts = state.get("options") or []
    if not opts:
        return None
    units = [o for o in opts if o.get("option_type") == "unit"]
    if not units:
        return None
    labels = []
    for o in units[:max_items]:
        lab = (o.get("label") or "").strip()
        if lab:
            labels.append(lab)
    return ("Geography choices: " + "; ".join(labels)) if labels else None


def _ready_to_fetch_hint(state: lg_State) -> Optional[str]:
    """Hint when both theme and at least one unit are present.

    Parses selected_theme to extract the label when possible.
    """
    try:
        units = get_selected_units(state)
        if not units:
            return None
        theme_json = state.get("selected_theme")
        if not theme_json:
            return None
        import json as _json
        import io as _io
        import pandas as _pd

        # selected_theme is JSON records; extract label if available
        try:
            df = _pd.read_json(_io.StringIO(theme_json), orient="records")
            theme_label = str(df.iloc[0]["labl"]) if not df.empty else None
        except Exception:
            # Fallback: try to parse as list/dict
            try:
                data = _json.loads(theme_json)
                if isinstance(data, list) and data:
                    theme_label = str(
                        data[0].get("labl") or data[0].get("label") or "(theme)"
                    )
                elif isinstance(data, dict):
                    theme_label = str(
                        data.get("labl") or data.get("label") or "(theme)"
                    )
                else:
                    theme_label = None
            except Exception:
                theme_label = None
        units_count = len(units)
        if theme_label:
            return f"Ready to fetch: theme={theme_label}; units={units_count}"
        return f"Ready to fetch: units={units_count}"
    except Exception:
        return None


def _visible_cubes_hint(state: lg_State, max_items: int = 8) -> Optional[str]:
    """If visualization is visible and cube ids exist, hint current cubes.

    Uses `state['show_visualization']` and `state['selected_cubes']`. Parses
    the cubes JSON (records) to extract unique cube ids and labels.
    """
    try:
        if not state.get("show_visualization"):
            return None
        cubes_json = state.get("selected_cubes")
        if not cubes_json:
            return None
        import io as _io
        import pandas as _pd

        df = _pd.read_json(_io.StringIO(cubes_json), orient="records")
        if df.empty:
            return None
        # Determine id, label, and text columns
        id_col = None
        for c in ("Cube_ID", "cube_id", "CubeID", "ent_ID", "ent_id"):
            if c in df.columns:
                id_col = c
                break
        if not id_col:
            return None
        label_col = (
            "Cube"
            if "Cube" in df.columns
            else ("cube" if "cube" in df.columns else None)
        )
        text_col = None
        for c in ("Cube_Text", "cube_text", "text"):
            if c in df.columns:
                text_col = c
                break
        # Build unique list up to max_items
        cols = [id_col]
        if label_col:
            cols.append(label_col)
        if text_col:
            cols.append(text_col)
        uniq = df[cols].drop_duplicates()
        items = []
        for _, r in uniq.head(max_items).iterrows():
            cid = str(r[id_col])
            label_val = (
                str(r[label_col])
                if label_col and r.get(label_col) is not None
                else None
            )
            text_val = (
                str(r[text_col]) if text_col and r.get(text_col) is not None else None
            )
            if label_val and text_val:
                items.append(f"{label_val} ({cid}) — {text_val}")
            elif label_val:
                items.append(f"{label_val} ({cid})")
            elif text_val:
                items.append(f"{cid} — {text_val}")
            else:
                items.append(cid)
        if not items:
            return None
        more = "…" if len(uniq) > len(items) else ""
        return "Visible cubes: " + "; ".join(items) + more
    except Exception:
        return None


def _collect_entity_candidates(state: lg_State, max_items: int = 12) -> list[dict]:
    """Return a structured list of currently visible/plotted data entities.

    Items: {id, label}. Uses the same parsing rules as _visible_cubes_hint.
    """
    try:
        cubes_json = state.get("selected_cubes") or state.get("cubes")
        if not cubes_json:
            return []
        import io as _io
        import pandas as _pd

        df = _pd.read_json(_io.StringIO(cubes_json), orient="records")
        if df is None or df.empty:
            return []
        id_col = None
        for c in ("Cube_ID", "cube_id", "CubeID", "ent_ID", "ent_id"):
            if c in df.columns:
                id_col = c
                break
        if not id_col:
            return []
        label_col = "Cube" if "Cube" in df.columns else ("cube" if "cube" in df.columns else None)

        uniq = df[[id_col] + ([label_col] if label_col else [])].drop_duplicates().head(max_items)
        items: list[dict] = []
        for _, r in uniq.iterrows():
            eid = str(r[id_col])
            lab = None
            if label_col and r.get(label_col) is not None:
                lab = str(r[label_col])
            items.append({"id": eid, "label": lab or eid})
        return items
    except Exception:
        return []


# No planner heuristics for explain-visible-data; the intent handler decides.


def _summarize_ui_options(state: lg_State) -> str:
    """Return a compact summary of current UI option buttons.

    Exposes only non-sensitive fields (label and value) so the LLM can be aware
    of what the user currently sees and avoid proposing duplicate actions.
    """
    options = state.get("options") or []
    if not options:
        return "(none)"
    lines: List[str] = []
    for idx, opt in enumerate(options):
        try:
            label = str(opt.get("label", f"Option {idx}"))
            value = opt.get("value")
            vtxt = str(value) if isinstance(value, (int, float)) else str(value)[:40]
            lines.append(f"[{idx}] {label} (value={vtxt})")
        except Exception:
            continue
    return "\n".join(lines) if lines else "(none)"


def _list_ui_option_labels(state: lg_State) -> str:
    """Return a semicolon-separated list of current option labels.

    Keeps labels exactly as shown to the user so the assistant can reference
    them verbatim (e.g., "Add a place; Add a theme; Show current selection").
    """
    options = state.get("options") or []
    labels: List[str] = []
    for opt in options:
        try:
            label = (opt.get("label") or "").strip()
            if label:
                labels.append(label)
        except Exception:
            continue
    return "; ".join(labels)


def conversational_agent_node(state: lg_State) -> dict | Command:
    """LLM-driven planning node that emits actions or replies.

    If there is a new human message, ask the LLM for a short plan. Convert the
    first action to a jump via Command(goto=...), and enqueue any remaining
    actions in `intent_queue`. If there are no actions but a `final_reply`,
    append it and end the turn.
    """
    # Build planner input: prefer a fresh HumanMessage; otherwise synthesize from UI intent
    messages = state.get("messages", []) or []
    user_text = None
    if messages:
        last_msg = messages[-1]
        if isinstance(last_msg, HumanMessage):
            user_text = (last_msg.content or "").strip()

    if not user_text:
        lip = state.get("last_intent_payload") or {}
        intent = (lip or {}).get("intent")
        args = dict((lip or {}).get("arguments") or {})
        if intent in ("AddPlace", "RemovePlace"):
            place = str(args.get("place") or "").strip()
            unit_type = str(args.get("unit_type") or "").strip()
            polygon_id = args.get("polygon_id")
            verb = "add" if intent == "AddPlace" else "remove"
            parts = [f"Map click: {verb} place"]
            details = []
            if place:
                details.append(f"name='{place}'")
            if unit_type:
                details.append(f"unit_type={unit_type}")
            if polygon_id is not None:
                details.append(f"polygon_id={polygon_id}")
            if details:
                parts.append("(" + ", ".join(details) + ")")
            user_text = " ".join(parts)

    if not user_text:
        logger.info(
            "conversational_agent_node: no messages and no UI intent → nothing to do"
        )
        return {}

    # Prepare context
    ctx = _summarize_selection(state)
    ui_opts_summary = _summarize_ui_options(state)
    ui_option_labels = _list_ui_option_labels(state)
    recent_conv = _summarize_recent_conversation(state)
    domain_hints = _build_domain_hints(state, user_text)
    memory_summary = (state.get("memory_summary") or "(none)").strip() or "(none)"
    # Log a short snapshot for debugging (not the full user text)
    logger.info(
        "conversational_agent_node: planning for user input: " + user_text[:120],
    )
    # Always use the LLM planner to decide next steps.
    # Use JSON mode with reasoning for the planner to enforce strict JSON output
    # Use strict JSON without reasoning to avoid non-JSON tokens in outputs
    planner_llm = get_llm(json_mode=True, reasoning=False)
    chain = (
        _PLANNER_PROMPT | planner_llm.with_structured_output(schema=AssistantPlan)
    ).with_config(
        {
            "tags": ["planner", "no_ui_stream"],
            "run_name": "planner",
            "callbacks": [get_llm_callback()],
        }
    )

    # Fast-path for map clicks: skip NLP/planner, use UI-provided intent/args directly
    lip = state.get("last_intent_payload") or {}
    lip_intent = (lip or {}).get("intent")
    lip_args = dict((lip or {}).get("arguments") or {})
    is_map_click = str(lip_args.get("source") or "").strip().lower() == "map_click"

    actions: List[PlannedAction] = []
    if is_map_click and lip_intent in ("AddPlace", "RemovePlace"):
        # Build a single planned action directly from the UI payload
        try:
            actions = [
                PlannedAction(
                    intent=ActionName(lip_intent),
                    arguments=lip_args,
                )
            ]
            logger.info(
                {
                    "event": "map_click_bypass",
                    "actions": _safe_actions_for_log(actions),
                }
            )
        except Exception:
            # If anything goes wrong, fall back to extraction below
            actions = []

    # Phase A: Use subagent-based extraction to build a deterministic intent skeleton
    if not actions:
        try:
            extracted = extract_intent_with_subagents(
                user_text, state.get("messages", [])
            )
        except Exception as e:
            logger.warning(
                f"conversational_agent_node: subagent extraction failed → {e}"
            )
            extracted = None
    reply = None
    if not actions and extracted and getattr(extracted, "intents", None):
        # Translate extracted intents to PlannedAction deterministically
        for intent_obj in extracted.intents:
            try:
                name = (
                    intent_obj.intent.value
                    if hasattr(intent_obj.intent, "value")
                    else str(intent_obj.intent)
                )
                args = dict(intent_obj.arguments or {})
                # Build PlannedAction if the intent exists in our ActionName enum
                if name in [a.value for a in ActionName]:
                    actions.append(
                        PlannedAction(intent=ActionName(name), arguments=args)
                    )
            except Exception:
                continue

        logger.info(
            {
                "event": "built_actions_from_subagent",
                "actions": _safe_actions_for_log(actions),
            }
        )
        # Let the intent handler decide all explain-data cases; no planner override here.
        # Cap number of actions from subagent translation
        if actions:
            actions = actions[:MAX_ACTIONS]
    elif not actions:
        # Phase A fallback to planner LLM only when no structured intents were extracted
        logger.info(
            "conversational_agent_node: no subagent intents → requesting plan from LLM"
        )
        try:
            app_overview = (
                "VobChat is a conversational app (Dash + Flask) that helps you "
                "select places and a statistics theme, then fetch and visualize "
                "relevant data cubes. It supports UK place/postcode lookup, theme "
                "selection, map-based unit selection, and showing your current selection."
            )
            # Log formatted prompt for planner
            try:
                _msgs = _PLANNER_PROMPT.format_messages(
                    {
                        "app_overview": app_overview,
                        "selected_places": ctx["selected_places"],
                        "selected_theme": ctx["selected_theme"],
                        "selected_units": ctx["selected_units"],
                        "ui_options": ui_opts_summary,
                        "ui_option_labels": ui_option_labels,
                        "recent_conversation": recent_conv,
                        "domain_hints": domain_hints,
                        "memory_summary": memory_summary,
                        "user_text": user_text,
                    }
                )
                log_llm_interaction(
                    name="planner",
                    prompt_vars={
                        "app_overview": app_overview,
                        "selected_places": ctx["selected_places"],
                        "selected_theme": ctx["selected_theme"],
                        "selected_units": ctx["selected_units"],
                        "ui_options": ui_opts_summary,
                        "ui_option_labels": ui_option_labels,
                        "recent_conversation": recent_conv,
                        "domain_hints": domain_hints,
                        "memory_summary": memory_summary,
                        "user_text": user_text,
                    },
                    formatted_messages=_msgs,
                    extra={"reasoning_enabled": True},
                )
            except Exception:
                pass

            plan: AssistantPlan = chain.invoke(
                {
                    "app_overview": app_overview,
                    "selected_places": ctx["selected_places"],
                    "selected_theme": ctx["selected_theme"],
                    "selected_units": ctx["selected_units"],
                    "ui_options": ui_opts_summary,
                    "ui_option_labels": ui_option_labels,
                    "recent_conversation": recent_conv,
                    "domain_hints": domain_hints,
                    "memory_summary": memory_summary,
                    "user_text": user_text,
                }
            )
            actions = (plan.actions or [])[:MAX_ACTIONS]
            reply = (plan.final_reply or "").strip() if plan.final_reply else None
            try:
                log_llm_interaction(
                    name="planner_result",
                    output=plan,
                    extra={"truncated_actions": len(actions), "reply": reply},
                )
            except Exception:
                pass
            logger.info(
                {
                    "event": "planner_result",
                    "actions": _safe_actions_for_log(
                        plan.actions if hasattr(plan, "actions") else []
                    ),
                    "has_final_reply": bool(getattr(plan, "final_reply", None)),
                }
            )
            try:
                planned_actions_log = [
                    {
                        "intent": (
                            a.intent.value
                            if isinstance(a.intent, ActionName)
                            else str(a.intent)
                        ),
                        "arguments": a.arguments or {},
                    }
                    for a in actions
                ]
                logger.info(
                    {"event": "planner_actions", "actions": planned_actions_log}
                )
            except Exception:
                pass
        except Exception as e:
            # Structured extraction failed — attempt JSON-mode raw parse as a fallback
            logger.warning(f"conversational_agent_node: plan extraction failed → {e}")
            try:
                raw_chain = (_PLANNER_PROMPT | planner_llm).with_config(
                    {
                        "tags": ["planner_fallback", "no_ui_stream"],
                        "run_name": "planner_fallback",
                        "callbacks": [get_llm_callback()],
                    }
                )
                # Log formatted prompt for fallback
                try:
                    _msgs_fb = _PLANNER_PROMPT.format_messages(
                        {
                            "app_overview": app_overview,
                            "selected_places": ctx["selected_places"],
                            "selected_theme": ctx["selected_theme"],
                            "selected_units": ctx["selected_units"],
                            "ui_options": ui_opts_summary,
                            "ui_option_labels": ui_option_labels,
                            "recent_conversation": recent_conv,
                            "domain_hints": domain_hints,
                            "memory_summary": memory_summary,
                            "user_text": user_text,
                        }
                    )
                    log_llm_interaction(
                        name="planner_fallback",
                        prompt_vars={
                            "app_overview": app_overview,
                            "selected_places": ctx["selected_places"],
                            "selected_theme": ctx["selected_theme"],
                            "selected_units": ctx["selected_units"],
                            "ui_options": ui_opts_summary,
                            "ui_option_labels": ui_option_labels,
                            "recent_conversation": recent_conv,
                            "domain_hints": domain_hints,
                            "memory_summary": memory_summary,
                            "user_text": user_text,
                        },
                        formatted_messages=_msgs_fb,
                        extra={"reasoning_enabled": True},
                    )
                except Exception:
                    pass

                raw = raw_chain.invoke(
                    {
                        "app_overview": app_overview,
                        "selected_places": ctx["selected_places"],
                        "selected_theme": ctx["selected_theme"],
                        "selected_units": ctx["selected_units"],
                        "ui_options": ui_opts_summary,
                        "ui_option_labels": ui_option_labels,
                        "recent_conversation": recent_conv,
                        "domain_hints": domain_hints,
                        "memory_summary": memory_summary,
                        "user_text": user_text,
                    }
                )
                raw_txt = getattr(raw, "content", raw) or ""
                plan = _try_parse_plan_from_text(raw_txt)
                try:
                    log_llm_interaction(
                        name="planner_fallback_result",
                        output=raw_txt,
                    )
                except Exception:
                    pass
                if plan is None:
                    logger.warning(
                        "conversational_agent_node: fallback parse failed (no plan)"
                    )
                    return {}
                actions = (plan.actions or [])[:MAX_ACTIONS]
                reply = (plan.final_reply or "").strip() if plan.final_reply else None
                logger.info(
                    "conversational_agent_node: received plan via fallback: "
                    + json.dumps(plan, default=str),
                )
            except Exception as e2:
                logger.warning(
                    f"conversational_agent_node: fallback planning failed → {e2}"
                )
                return {}

    # Normalize actions to plain payload dicts, order them, and cap to MAX_ACTIONS
    payloads: List[dict] = _actions_to_payloads(actions)
    payloads = _order_payloads(payloads)[:MAX_ACTIONS]

    # Log normalized actions
    try:
        logger.info({"event": "actions", "actions": _safe_actions_for_log(payloads)})
    except Exception:
        pass

    if not payloads:
        # No actionable items → Prefer the planner's final_reply if available for consistency.
        final_txt = (reply or "").strip()
        if not final_txt:
            # If the planner did not provide a reply, stream a natural reply from the user text.
            try:
                reply_llm = get_llm()
                streaming_prompt = ChatPromptTemplate.from_messages(
                    [
                        (
                            "system",
                            "You are VobChat, a concise, helpful UK statistics assistant. Reply directly to the user in natural language. Do not include JSON or code fences.",
                        ),
                        ("user", "{user_text}"),
                    ]
                )
                streaming_chain = (streaming_prompt | reply_llm).with_config(
                    {"tags": ["reply_stream"], "run_name": "reply_stream"}
                )
                parts: list[str] = []
                for chunk in streaming_chain.stream({"user_text": user_text}):
                    try:
                        txt = getattr(chunk, "content", "") or ""
                        if txt:
                            parts.append(txt)
                    except Exception:
                        continue
                final_txt = ("".join(parts)).strip()
            except Exception:
                final_txt = ""

        msg = _append_ai(state, final_txt)
        logger.info(
            "conversational_agent_node: replying without actions (streamed): "
            + final_txt[:120],
        )
        return {
            "messages": [msg],
            "last_intent_payload": state.get("last_intent_payload"),
        }

    # We have payloads: route the first, queue the rest
    queue: List[dict] = state.get("intent_queue", []) or []
    first = payloads[0]
    rest = payloads[1:]
    if rest:
        queue.extend(rest)
        logger.debug(
            {
                "event": "queued_additional_actions",
                "actions": _safe_actions_for_log(rest),
            }
        )

    intent = first.get("intent")
    target_node = _intent_to_node(intent)

    if intent == "Chat" or not target_node:
        # Just reply: Prefer Chat.text first, then planner final_reply. Only stream if neither exists.
        chat_txt = None
        try:
            chat_txt = (first.get("arguments") or {}).get("text")
        except Exception:
            chat_txt = None
        txt = (chat_txt or reply or "").strip()

        if txt:
            final_txt = txt
        else:
            try:
                reply_llm = get_llm()
                streaming_prompt = ChatPromptTemplate.from_messages(
                    [
                        (
                            "system",
                            "You are VobChat, a concise, helpful UK statistics assistant. Reply directly; no JSON or code fences.",
                        ),
                        ("user", "{user_text}"),
                    ]
                )
                streaming_chain = (streaming_prompt | reply_llm).with_config(
                    {
                        "tags": ["reply_stream"],
                        "run_name": "reply_stream",
                        "callbacks": [get_llm_callback()],
                    }
                )
                parts: list[str] = []
                try:
                    msgs_reply = streaming_prompt.format_messages({"user_text": user_text})
                    log_llm_interaction(
                        name="reply_stream",
                        prompt_vars={"user_text": user_text},
                        formatted_messages=msgs_reply,
                    )
                except Exception:
                    pass
                for chunk in streaming_chain.stream({"user_text": user_text}):
                    try:
                        t = getattr(chunk, "content", "") or ""
                        if t:
                            parts.append(t)
                    except Exception:
                        continue
                final_txt = ("".join(parts)).strip()
                try:
                    log_llm_interaction(name="reply_stream_result", output=final_txt)
                except Exception:
                    pass
            except Exception:
                final_txt = ""

        msg = _append_ai(state, final_txt)
        logger.info(
            "conversational_agent_node: Chat/No target → replying (streamed): "
            + final_txt[:120]
        )
        return {
            "messages": [msg],
            "intent_queue": queue,
            "last_intent_payload": None,
        }

    logger.info(
        "conversational_agent_node: routing to node: "
        + json.dumps({"target_node": target_node, "intent": intent}, default=str),
    )
    return Command(
        goto=target_node,
        update={
            "intent_queue": queue,
            "last_intent_payload": first,  # <-- already a plain dict
        },
    )


# --------------------------------------------------------------------------------------
# Helpers: normalization and mapping
# --------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------
# Helpers: serialization + normalization (Pydantic v1/v2 compatible)
# --------------------------------------------------------------------------------------


def _model_dump_compat(obj):
    """Return a plain dict from a pydantic model in v1 or v2; otherwise return obj if already a dict."""
    try:
        # pydantic v2
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        # pydantic v1
        if hasattr(obj, "dict"):
            return obj.dict()
    except Exception:
        pass
    return obj  # might already be a dict


def _actions_to_payloads(actions: List[PlannedAction]) -> List[dict]:
    """Normalize PlannedAction objects to plain dict payloads: {'intent': str, 'arguments': dict}."""
    payloads: List[dict] = []
    for a in actions or []:
        # If user accidentally handed us dicts already, keep them
        if isinstance(a, dict):
            # Ensure keys exist and types look right
            intent = a.get("intent")
            args = a.get("arguments") or {}
            if hasattr(intent, "value"):  # Enum or pydantic-wrapped enum
                intent = intent.value
            payloads.append(
                {
                    "intent": str(intent) if intent is not None else None,
                    "arguments": dict(args),
                }
            )
            continue

        # PlannedAction (pydantic)
        try:
            intent = a.intent.value if hasattr(a.intent, "value") else str(a.intent)
            args = dict(a.arguments or {})
            payloads.append({"intent": intent, "arguments": args})
        except Exception:
            # Last-resort: model dump then coerce
            d = _model_dump_compat(a)
            intent = d.get("intent")
            if hasattr(intent, "value"):
                intent = intent.value
            payloads.append(
                {
                    "intent": str(intent) if intent is not None else None,
                    "arguments": dict(d.get("arguments") or {}),
                }
            )
    return payloads


def _safe_actions_for_log(actions: List[PlannedAction | dict]) -> list:
    """Return a JSON-serializable representation of actions for logging."""
    out = []
    for a in actions or []:
        if isinstance(a, dict):
            out.append(
                {"intent": str(a.get("intent")), "arguments": a.get("arguments", {})}
            )
        else:
            try:
                out.append(_actions_to_payloads([a])[0])
            except Exception:
                out.append(str(a))
    return out


def _intent_to_node(intent: Optional[str]) -> Optional[str]:
    """Map an intent string to a workflow node name."""
    if not intent:
        return None
    mapping = {
        "AddPlace": "AddPlace_node",
        "RemovePlace": "RemovePlace_node",
        "AddTheme": "AddTheme_node",
        "RemoveTheme": "RemoveTheme_node",
        "ShowState": "ShowState_node",
        "ListThemes": "ListThemes_node",
        "PlaceInfo": "PlaceInfo_node",
        "Reset": "Reset_node",
        "FetchCubes": "find_cubes_node",
        "Chat": None,
        "UnitTypeInfo": "UnitTypeInfo_node",
        "DescribeTheme": "DescribeTheme_node",
        "DataEntityInfo": "DataEntityInfo_node",
        "ExplainVisibleData": "ExplainVisibleData_node",
    }
    return mapping.get(intent)


def _order_payloads(payloads: List[dict]) -> List[dict]:
    """Return a new list ordered by intent priority with stable grouping.

    General priority: AddPlace, RemovePlace, AddTheme, RemoveTheme, DescribeTheme,
    PlaceInfo, ListThemes, ShowState, Reset, FetchCubes, Chat.
    """
    priority = {
        "AddPlace": 10,
        "RemovePlace": 20,
        "AddTheme": 30,
        "RemoveTheme": 40,
        "DescribeTheme": 50,
        "DataEntityInfo": 55,
        "ExplainVisibleData": 57,
        "PlaceInfo": 60,
        "ListThemes": 70,
        "ShowState": 80,
        "Reset": 90,
        "FetchCubes": 100,
        "Chat": 110,
    }
    # Stable sort by priority then original index, then unwrap
    indexed = list(enumerate(payloads))
    indexed.sort(key=lambda iv: (priority.get(iv[1].get("intent"), 999), iv[0]))
    ordered = [p for _, p in indexed]
    return ordered
