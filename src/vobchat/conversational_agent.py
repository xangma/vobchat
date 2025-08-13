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

from typing import List, Dict, Optional
import json
from enum import Enum
from pydantic import BaseModel, Field
import os
import logging

from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.types import Command

from .state_schema import lg_State, get_selected_place_names, get_selected_units

logger = logging.getLogger(__name__)


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

_PLANNER_PROMPT = ChatPromptTemplate.from_messages([
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
- PlaceInfo {{place: str}}
- Reset {{}}
- FetchCubes {{}}
- Chat {{text: str}}  (use only when you just need to reply and not change state)
 - UnitTypeInfo {{unit_type: str}} (use when the user asks what a unit type is, e.g., "what is Local Government District" or "what is LG_DIST")

Rules:
- Prefer concrete actions over Chat when the user asks to do something.
- If the place is ambiguous or a theme is unclear, ask one concise question in final_reply and propose no actions.
- Use AddPlace for location queries like "where's X", "show me X", "find X".
- For "stats/data/statistics for X", include AddPlace and AddTheme.
- Return STRICT JSON matching the schema. Do not include code fences or extra text.
- If the user asks about this app or what it does, DO NOT propose actions; return a concise explanation in final_reply.
 - If the user asks what a unit type means (by code or label), propose UnitTypeInfo with that code/label in arguments.
 - If UI option buttons are visible (see CONTEXT → UI options), you may reference them and avoid proposing duplicate actions.
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

USER_MESSAGE
{user_text}

Respond with JSON only: {{"actions": [{{"intent": "...", "arguments": {{...}}}}], "final_reply": "..."}}.
        """.strip(),
    ),
])


def _make_llm() -> ChatOllama:
    """Create a deterministic LLM instance for planning.

    Respects environment variables:
    - OLLAMA_HOST / OLLAMA_PORT (endpoint)
    - VOBCHAT_LLM_MODEL (model name)
    """
    _OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
    _OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
    _OLLAMA_SUBPATH = os.getenv("OLLAMA_SUBPATH", "")
    _OLLAMA_USE_SSL = os.getenv("OLLAMA_USE_SSL", "true").lower() == "true"
    protocol = "https" if _OLLAMA_USE_SSL else "http"
    _BASE_URL = f"{protocol}://{_OLLAMA_HOST}:{_OLLAMA_PORT}/{_OLLAMA_SUBPATH}"

    model = os.getenv("VOBCHAT_LLM_MODEL", "deepseek-r1-wt:latest")
    logger.debug(
        f"conversational_agent: initializing LLM - base_url={_BASE_URL}, model={model}",
    )
    return ChatOllama(model=model, base_url=_BASE_URL, client_kwargs={"verify": False})


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
    # Fetch the current message list from state; we only act if there is a
    # fresh HumanMessage at the end.
    messages = state.get("messages", []) or []
    if not messages:
        logger.info("conversational_agent_node: no messages → nothing to do")
        return {}

    last_msg = messages[-1]
    if not isinstance(last_msg, HumanMessage):
        logger.debug("conversational_agent_node: last message is not human → nothing to plan")
        return {}

    user_text = (last_msg.content or "").strip()
    if not user_text:
        logger.debug("conversational_agent_node: empty user text")
        return {}

    # Prepare context
    ctx = _summarize_selection(state)
    ui_opts_summary = _summarize_ui_options(state)
    ui_option_labels = _list_ui_option_labels(state)
    # Log a short snapshot for debugging (not the full user text)
    logger.info(
        "conversational_agent_node: planning for user input",
        extra={
            "user_text_preview": user_text[:120],
            "ctx": ctx,
            "ui_options_present": bool(state.get("options"))
        },
    )
    llm = _make_llm()
    chain = _PLANNER_PROMPT | llm.with_structured_output(schema=AssistantPlan)

    logger.info("conversational_agent_node: requesting plan from LLM")
    try:
        # Provide a concise app overview to help the model answer meta questions
        app_overview = (
            "VobChat is a conversational app (Dash + Flask) that helps you "
            "select places and a statistics theme, then fetch and visualize "
            "relevant data cubes. It supports UK place/postcode lookup, theme "
            "selection, map-based unit selection, and showing your current selection."
        )
        plan: AssistantPlan = chain.invoke({
            "selected_places": ctx["selected_places"],
            "selected_theme": ctx["selected_theme"],
            "selected_units": ctx["selected_units"],
            "app_overview": app_overview,
            "ui_options": ui_opts_summary,
            "ui_option_labels": ui_option_labels,
            "user_text": user_text,
        })
    except Exception as e:
        logger.warning(f"conversational_agent_node: plan extraction failed → {e}")
        # Fallback: do nothing this turn
        return {}

    actions = plan.actions or []
    reply = (plan.final_reply or "").strip() if plan.final_reply else None

    # Log the plan outline without dumping full content
    logger.info(
        "conversational_agent_node: received plan",
        extra={
            "actions_count": len(actions),
            "first_action": actions[0].intent.value if actions else None,
            "reply_present": bool(reply),
        },
    )

    if not actions:
        if reply:
            # Append natural reply
            state["messages"].append(AIMessage(content=reply))
            logger.info("conversational_agent_node: replying without actions")
            return {
                "messages": state["messages"],
                "last_intent_payload": state.get("last_intent_payload"),
            }
        # No actions and no reply → nothing to do
        logger.debug("conversational_agent_node: no actions and no reply → noop")
        return {}

    # Convert actions to our queue format
    queue: List[dict] = state.get("intent_queue", []) or []
    payloads: List[dict] = []
    for act in actions:
        payloads.append({"intent": act.intent.value, "arguments": act.arguments or {}})

    first = payloads[0]
    rest = payloads[1:]
    if rest:
        queue.extend(rest)
        logger.debug(
            "conversational_agent_node: queued additional actions",
            extra={"queued_count": len(rest), "queue_len": len(queue)},
        )

    # Map first action to target node
    intent = first.get("intent")
    target_node = f"{intent}_node" if intent not in ("FetchCubes", "Chat") else (
        "find_cubes_node" if intent == "FetchCubes" else None
    )

    # Special handling for UnitTypeInfo: fetch from DB and reply directly
    if intent == "UnitTypeInfo":
        try:
            from vobchat.tools import get_unit_type_info
            ut_arg = (first.get("arguments") or {}).get("unit_type")
            info_json = get_unit_type_info.invoke({"unit_type": ut_arg}) if hasattr(get_unit_type_info, "invoke") else get_unit_type_info(ut_arg)
            info = json.loads(info_json) if info_json else {}
        except Exception as e:
            logger.warning(f"conversational_agent_node: UnitTypeInfo fetch failed: {e}")
            info = {}

        if not info:
            txt = f"I couldn't find details for unit type: {ut_arg}."
        else:
            # Build a concise summary
            parts = []
            parts.append(f"Type: {info.get('label', '')} ({info.get('identifier', '')})")
            lvl_label = info.get('level_label')
            lvl = info.get('level')
            if lvl or lvl_label:
                parts.append(f"Level: {lvl} ({lvl_label})" if lvl_label else f"Level: {lvl}")
            adl = info.get('adl_feature_type')
            if adl:
                parts.append(f"ADL Feature Type: {adl}")
            parts.append(f"Number of units: {info.get('unit_count', 0)}")

            def fmt_rel(key: str, title: str, lim: int = 6):
                rels = info.get(key) or []
                if not rels:
                    return None
                labels = [r.get('label') or r.get('unit_type') for r in rels]
                labels = [x for x in labels if x]
                if not labels:
                    return None
                if len(labels) > lim:
                    shown = ", ".join(labels[:lim]) + f", +{len(labels)-lim} more"
                else:
                    shown = ", ".join(labels)
                return f"{title}: {shown}"

            for key, title in (
                ("may_be_part_of", "May be part of"),
                ("may_have_parts", "May have parts"),
                ("may_have_succeeded", "May have succeeded"),
                ("may_have_preceded", "May have preceded"),
            ):
                s = fmt_rel(key, title)
                if s:
                    parts.append(s)

            statuses = info.get('statuses') or []
            if statuses:
                st = ", ".join(
                    [f"{x.get('label')} ({x.get('code')})" if x.get('label') and x.get('code') else (x.get('label') or x.get('code') or '') for x in statuses]
                )
                if st.strip():
                    parts.append(f"Possible statuses: {st}")

            desc = info.get('description') or info.get('full_description')
            if desc:
                parts.append(desc)

            txt = "\n".join([p for p in parts if p])

        state["messages"].append(AIMessage(content=txt))
        logger.info("conversational_agent_node: replied with UnitTypeInfo summary")
        return {"messages": state["messages"], "intent_queue": queue, "last_intent_payload": None}

    if intent == "Chat" or not target_node:
        # Just reply if provided; else a generic acknowledgment
        # If Chat action includes a text argument, prefer that.
        chat_txt = None
        try:
            chat_txt = (first.get("arguments") or {}).get("text")
        except Exception:
            chat_txt = None
        default_overview = (
            "This app helps you explore statistics by selecting places and a theme, "
            "and then retrieving visualizable data cubes."
        )
        txt = chat_txt or reply or default_overview
        state["messages"].append(AIMessage(content=txt))
        logger.info(
            "conversational_agent_node: Chat/No target → replying",
            extra={"reply_preview": txt[:120]},
        )
        return {
            "messages": state["messages"],
            "intent_queue": queue,
            "last_intent_payload": None,
        }

    logger.info(
        "conversational_agent_node: routing to node",
        extra={"target_node": target_node, "intent": intent},
    )
    return Command(
        goto=target_node,
        update={
            "intent_queue": queue,
            "last_intent_payload": first,
        },
    )
