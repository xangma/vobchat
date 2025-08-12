"""User interaction nodes: ask_followup.

Provides a generic clarification step when the router cannot map a message to a
specific intent. Uses an interrupt to present quick-action buttons and resumes
with ``selection_idx`` when the user clicks, or allows the user to type a new
message which is then routed by the LLM as usual.
"""
from __future__ import annotations
from typing import List
from langgraph.types import interrupt
from langgraph.types import Command
from vobchat.state_schema import lg_State
from .utils import _append_ai, serialize_messages
import logging

logger = logging.getLogger(__name__)

def ask_followup_node(state: lg_State) -> dict | Command:
    """Generic follow-up / clarification node.

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

    Returns:
        dict | Command: On first entry or when re-issuing buttons, returns an
        empty dict after ``interrupt`` (front-end shows options). When a button
        is selected, returns a ``Command`` that routes to ``agent_node`` with a
        minimal ``last_intent_payload``. For invalid selection indices, returns
        a dict with updated messages prompting the user again.
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
            "messages": serialize_messages(state.get("messages", []))
        })
        # CRITICAL: Don't return entire state to avoid duplicate selection_idx
        return {}  # No state changes needed

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
            # bad index - start over cleanly

            # state["options"] = []
            logger.warning("ask_followup_node: invalid selection_idx - reprompting")
        else:
            logger.info(f"ask_followup_node: user picked quick action → {chosen_intent}")
            # clear one-shot fields before continuing

            # state["options"] = []
            # fake a minimal last_intent_payload so the usual router can do its job
            # CRITICAL: Only update specific fields, not entire state to avoid duplicate selection_idx
            update_dict = {"last_intent_payload": {"intent": chosen_intent, "arguments": {}}}
            logger.error(f"🔍 ask_followup_node COMMAND UPDATE: {list(update_dict.keys())}")
            return Command(goto="agent_node", update=update_dict)

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
            "messages": serialize_messages(state.get("messages", []))
        }
    )

    # Execution pauses here.  Front-end shows the buttons, user picks one, and
    # the graph will re-enter this node with selection_idx set.
    # CRITICAL: Don't return entire state to avoid duplicate selection_idx
    return {}  # No state changes needed, interrupt handles everything
