from typing import cast, Optional

from langchain_core.messages import HumanMessage, AIMessage
from langgraph.types import Command
from langgraph.graph import END

from intent_handling import extract_intent, AssistantIntent, AssistantIntentPayload
from state_schema import lg_State  # TypedDict from your existing workflow file
import logging

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
#   agent_node - single entry point for routing
# ---------------------------------------------------------------------------
def agent_node(state: lg_State):
    logging.info(f"agent_node entered. State snapshot (keys): {list(state.keys())}")
    # For brevity, log only keys or specific interesting values
    logging.debug(f"agent_node state details: last_intent_payload={state.get('last_intent_payload')}, queue_len={len(state.get('intent_queue', []))}")


    # Determine the source of the intent for this step
    final_intent: Optional[AssistantIntent] = None
    final_args: dict = {}
    payload_to_route: dict = {} # The specific dict used for the Command update

    # --- Determine Intent Source ---

    # Priority 1: Check for pre-set intent from Command update (e.g., map click)
    pre_set_payload = state.get("last_intent_payload")
    if pre_set_payload and pre_set_payload.get("intent"):
        try:
            intent_val = pre_set_payload["intent"]
            final_intent = AssistantIntent(intent_val)
            final_args = pre_set_payload.get("arguments", {})
            payload_to_route = pre_set_payload
            logging.info(f"agent_node: Using pre-set intent '{final_intent.value}' from Command/State.")
            target_node = f"{final_intent.value}_node"
            logging.info(f"agent_node: Routing to target node: {target_node}")
            # Ensure the payload used for the update is the one we determined needed routing
            update_for_command = {
                "last_intent_payload": payload_to_route,
                "intent_queue": state.get("intent_queue", []), # Pass remaining queue
            }
            return Command(goto=target_node, update=update_for_command)
            # Clear the payload from the main state *after* using it for routing
            # to prevent accidental re-processing if the graph loops back here unexpectedly.
            # The Command's update ensures the next node gets it this time.
            # state["last_intent_payload"] = None
        except ValueError:
            logging.warning(f"agent_node: Invalid intent '{pre_set_payload.get('intent')}' in last_intent_payload.")
            state["last_intent_payload"] = None # Clear invalid payload


    # Priority 2: Check for fresh Human input (only if no pre-set intent was used)
    last_msg = state.get("messages", [])[-1] if state.get("messages") else None
    if not final_intent and last_msg and isinstance(last_msg, HumanMessage):
        user_text = cast(str, last_msg.content).strip()
        logging.info(f"agent_node: Processing user text: '{user_text}'")
        extracted_payload_obj = extract_intent(user_text, state["messages"])

        if extracted_payload_obj.intents:
            first_intent_obj, *rest = extracted_payload_obj.intents
            final_intent = first_intent_obj.intent
            final_args = first_intent_obj.arguments
            payload_to_route = first_intent_obj.model_dump()
            # Store the extracted payload in case it's needed later (e.g. by the target node)
            # This replaces any pre-set payload as LLM extraction takes precedence on new text
            state["last_intent_payload"] = payload_to_route

            if rest: # Queue up remaining intents
                state.setdefault("intent_queue", []).extend([r.model_dump() for r in rest])
                logging.info(f"agent_node: Queued {len(rest)} additional intents.")
        else:
            logging.warning("agent_node: No intents extracted from user text.")
            # Fallback: Treat as CHAT or route to clarification? Let's route to ask_followup.
            final_intent = None # Signal to route to followup/end
            payload_to_route = {}


    # Priority 3: Check queue (only if no pre-set intent and not human message)
    elif not final_intent and last_msg: # Implies not HumanMessage
        logging.info("agent_node: Not human message & no pre-set payload, checking queue.")
        queue = state.get("intent_queue", [])
        if queue:
            payload_from_queue = queue.pop(0) # Take first item
            try:
                 intent_val = payload_from_queue["intent"]
                 final_intent = AssistantIntent(intent_val)
                 final_args = payload_from_queue.get("arguments", {})
                 payload_to_route = payload_from_queue
                 state["last_intent_payload"] = payload_to_route # Update state with processed payload
                 logging.info(f"agent_node: Processing intent '{final_intent.value}' from queue.")
            except ValueError:
                 logging.warning(f"agent_node: Invalid intent '{payload_from_queue.get('intent')}' found in queue. Skipping.")
                 # If queue item is bad, just return state and let next cycle handle it?
                 # Or try next queue item? For now, return state to avoid complexity.
                 return state
        else:
            logging.info("agent_node: No human message, no pre-set payload, queue empty. Ending turn.")
            return state # Nothing to process


    # --- Routing Logic ---
    if final_intent is AssistantIntent.CHAT:
        txt = final_args.get("text", "Okay.") # Default chat response
        logging.info(f"agent_node: Handling CHAT intent.")
        state.setdefault("messages", []).append(AIMessage(content=txt))
        # CHAT usually ends the turn for the AI.
        return state # Return state after adding message

    elif final_intent:
        # Route to the specific handler node based on the determined intent
        target_node = f"{final_intent.value}_node"
        logging.info(f"agent_node: Routing to target node: {target_node}")
        # Ensure the payload used for the update is the one we determined needed routing
        update_for_command = {
            "last_intent_payload": payload_to_route,
            "intent_queue": state.get("intent_queue", []), # Pass remaining queue
        }
        return Command(goto=target_node, update=update_for_command)

    else:
        # Fallback if no intent determined (e.g., LLM failed, queue empty/invalid, no pre-set)
        logging.warning("agent_node: No valid intent determined. Routing to ask_followup_node.")
        # Route to clarification node
        return Command(goto="ask_followup_node", update={
             "last_intent_payload": None, # Clear any potentially invalid payload
             "intent_queue": state.get("intent_queue", []),
        })
