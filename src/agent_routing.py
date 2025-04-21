from typing import cast

from langchain_core.messages import HumanMessage, AIMessage
from langgraph.types import Command
from langgraph.graph import END

from intent_handling import extract_intent, AssistantIntent, AssistantIntentPayload
from state_schema import lg_State  # TypedDict from your existing workflow file
import logging

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
#   agent_node - single entry point for every human turn
# ---------------------------------------------------------------------------
def agent_node(state: lg_State):
    # 0️⃣  safety
    if not state.get("messages"):
        return state

    last_msg = state["messages"][-1]

    # ────────────────────────────────────────────────────────────────
    # A.  Consume queued intents when the graph re‑enters on an AI
    #     or ToolMessage turn (i.e. no fresh human text).
    # ────────────────────────────────────────────────────────────────
    if not isinstance(last_msg, HumanMessage):
        queue = state.get("intent_queue", [])
        if queue:
            payload = queue.pop(0)                       # ← take first
            intent  = AssistantIntent(payload["intent"])
            args    = payload.get("arguments", {})
            state["last_intent_payload"] = payload
            # NB: we **do not** run the LLM; we have our intent already.
        else:
            return state                                 # nothing queued
    else:
        # ────────────────────────────────────────────────────────────
        # B.  Fresh human input → run the LLM once
        # ────────────────────────────────────────────────────────────
        user_text = cast(str, last_msg.content).strip()
        payload   = extract_intent(user_text, state["messages"])
        first, *rest = payload.intents
        intent  = first.intent
        args    = first.arguments
        state["last_intent_payload"] = first.model_dump()
        if rest:                                         # queue leftovers
            state.setdefault("intent_queue", []).extend(
                [r.model_dump() for r in rest]
            )

    # ────────────────────────────────────────────────────────────────
    # C.  Immediate CHAT handling
    # ────────────────────────────────────────────────────────────────
    if intent is AssistantIntent.CHAT:
        txt = args.get("text", "")
        if txt:
            state["messages"].append(AIMessage(content=txt))
        return state

    # ────────────────────────────────────────────────────────────────
    # D.  Route to the specific handler node
    # ────────────────────────────────────────────────────────────────
    return Command(
        goto=f"{intent.value}_node",
        update={
            "last_intent_payload": state["last_intent_payload"],
            "intent_queue": state.get("intent_queue", []),  # pass remaining
        },
    )
