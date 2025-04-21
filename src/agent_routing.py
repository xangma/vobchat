from typing import cast

from langchain_core.messages import HumanMessage, AIMessage
from langgraph.types import Command
from langgraph.graph import END

from intent_handling import extract_intent, AssistantIntent, AssistantIntentPayload
from state_schema import lg_State  # TypedDict from your existing workflow file
import logging

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
#   agent_node – single entry point for every human turn
# ---------------------------------------------------------------------------

def agent_node(state: lg_State):  # noqa: C901 – complexity fine for single function
    """Parse the latest *HumanMessage* into an intent and either respond
    directly (for free-form Chat) or route to the corresponding handler node.

    The recognised intent plus its arguments are stashed in
    `state["last_intent_payload"]` so downstream nodes don’t need to re-run
    the LLM classification.
    """

    # Safety: no messages in state – nothing to do
    if not state.get("messages"):
        return state

    last_msg = state["messages"][-1]

    # Only trigger on fresh human input – if the last message is AI / Tool we
    # simply return the state so the graph can proceed along its edges.
    if not isinstance(last_msg, HumanMessage):
        return state

    user_text: str = cast(str, last_msg.content).strip()

    # ------------------------------------------------------------------
    # 1.  LLM intent extraction (structured output)
    # ------------------------------------------------------------------
    try:
        intent_payload: AssistantIntentPayload = extract_intent(user_text, state["messages"])  # synchronous .invoke under the hood
        logger.info(f"User text: {user_text}")
        logger.info(f"Intent payload: {intent_payload}")
    except Exception as exc:  # pragma: no cover – defensive
        # Fallback: treat as normal chat
        assistant_err = (
            "I’m having trouble understanding that – could you rephrase?"
        )
        state["messages"].append(AIMessage(content=assistant_err))
        return state
    
    if not isinstance(last_msg, HumanMessage):
        return Command(goto=END)

    # Persist to state so handler nodes can reuse arguments cheaply
    state["last_intent_payload"] = intent_payload.model_dump()

    intent = intent_payload.intent
    args = intent_payload.arguments or {}

    # ------------------------------------------------------------------
    # 2.  Immediate handling for CHAT intent
    # ------------------------------------------------------------------
    if intent is AssistantIntent.CHAT:
        assistant_reply = args.get("text", "")
        if assistant_reply:
            state["messages"].append(AIMessage(content=assistant_reply))
        return state  # no routing – conversation continues

    # ------------------------------------------------------------------
    # 3.  Route to intent-specific node
    # ------------------------------------------------------------------
    return Command(
        goto=f"{intent.value}_node",  # node names follow the pattern <Intent>_node
        update={"last_intent_payload": state["last_intent_payload"]},
    )
