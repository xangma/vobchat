from typing import cast, Optional

from langchain_core.messages import HumanMessage, AIMessage
from langgraph.types import Command
from langgraph.graph import END

from vobchat.intent_handling import extract_intent, AssistantIntent, AssistantIntentPayload
from vobchat.state_schema import lg_State  # TypedDict from your existing workflow file
import logging

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
#   agent_node - single entry point for routing
# ---------------------------------------------------------------------------


def agent_node(state: lg_State):
    # logging.debug(f"agent_node entered. State snapshot: {state}")
    logging.info("agent_node: Starting intent routing process.")
    # For brevity, log only keys or specific interesting values
    intent_queue = state.get('intent_queue', []) or []
    logging.debug(f"agent_node state details: last_intent_payload={state.get('last_intent_payload')}, queue_len={len(intent_queue)}")


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
                "intent_queue": state.get("intent_queue", []), # Pass cleaned queue (cleaning happened earlier)
                # CRITICAL: Clear selection_idx when routing to prevent stale values
                "selection_idx": None,
            }
            
            # CRITICAL: For AddPlace/RemovePlace intents, only clear theme state when there's actual interference
            if final_intent in [AssistantIntent.ADD_PLACE, AssistantIntent.REMOVE_PLACE]:
                # Only clear theme state if we have existing theme data that might interfere
                selected_theme = state.get("selected_theme")
                extracted_theme = state.get("extracted_theme")
                has_existing_theme = selected_theme or extracted_theme
                
                # Debug: Log the actual theme state values
                logging.info(f"agent_node: DEBUG theme state - selected_theme='{selected_theme}', extracted_theme='{extracted_theme}', has_existing_theme={has_existing_theme}")
                
                if has_existing_theme:
                    update_for_command.update({
                        "extracted_theme": None,  # Clear any pending theme queries
                        "current_node": None,     # Clear any pending node state
                        "options": [],            # Clear any pending options
                    })
                    logging.info(f"agent_node: Cleared existing theme state for {final_intent.value} intent to prevent interference")
                else:
                    # Don't clear theme state when there's no existing theme - let theme buttons appear
                    logging.info(f"agent_node: No existing theme state found for {final_intent.value} intent - preserving clean state for theme button generation")
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

        # CRITICAL: Only extract intents if this message hasn't been processed yet
        # Check if we're currently in an interrupt state waiting for user selection
        # In that case, this message might be unrelated to the selection and should be processed
        current_node = state.get("current_node")
        has_options = bool(state.get("options"))

        # Skip intent extraction only if we're actively waiting for a selection
        # and the state suggests this is a re-trigger of the same workflow step
        skip_extraction = (
            current_node in ["resolve_place_and_unit", "resolve_theme"] and
            has_options and
            state.get("selection_idx") is not None  # User made a selection, likely retriggering
        )

        if skip_extraction:
            logging.info(f"agent_node: Skipping intent extraction - currently handling selection (node={current_node}, has_options={has_options})")
        else:
            logging.info(f"agent_node: Processing user text: '{user_text}'")
            extracted_payload_obj = extract_intent(user_text, state["messages"])

            if extracted_payload_obj.intents:
                # CRITICAL: Implement intent prioritization to handle ambiguous cases
                # When multiple intents are detected, prioritize certain intents over others
                all_intents = extracted_payload_obj.intents

                # Priority order: AddPlace > AddTheme > DescribeTheme > other intents > Chat
                priority_order = [
                    AssistantIntent.ADD_PLACE,
                    AssistantIntent.REMOVE_PLACE,
                    AssistantIntent.ADD_THEME,
                    AssistantIntent.REMOVE_THEME,
                    AssistantIntent.SHOW_STATE,
                    AssistantIntent.DESCRIBE_THEME,  # Moved higher - specific theme descriptions should override general lists
                    AssistantIntent.LIST_SELECTION_THEMES,
                    AssistantIntent.LIST_ALL_THEMES,
                    AssistantIntent.RESET,
                    AssistantIntent.CHAT
                ]

                # Find the highest priority intent
                chosen_intent_obj = None
                for priority_intent in priority_order:
                    for intent_obj in all_intents:
                        if intent_obj.intent == priority_intent:
                            chosen_intent_obj = intent_obj
                            break
                    if chosen_intent_obj:
                        break

                # Fallback to first intent if no priority match (shouldn't happen)
                if not chosen_intent_obj:
                    chosen_intent_obj = all_intents[0]

                # Separate the chosen intent from the rest
                rest = [intent for intent in all_intents if intent != chosen_intent_obj]

                final_intent = chosen_intent_obj.intent
                final_args = chosen_intent_obj.arguments
                payload_to_route = chosen_intent_obj.model_dump()

                # Log the prioritization decision
                if len(all_intents) > 1:
                    intent_names = [intent.intent.value for intent in all_intents]
                    logging.info(f"agent_node: Prioritized '{final_intent.value}' over other intents: {intent_names}")
                # Store the extracted payload in case it's needed later (e.g. by the target node)
                # This replaces any pre-set payload as LLM extraction takes precedence on new text
                state["last_intent_payload"] = payload_to_route

                if rest: # Queue up remaining intents
                    # Filter out Chat intents when there are other more specific intents
                    filtered_rest = [r for r in rest if r.intent != AssistantIntent.CHAT]
                    if filtered_rest:
                        # Special handling for multiple AddPlace and AddTheme intents
                        add_place_intents = [r for r in filtered_rest if r.intent == AssistantIntent.ADD_PLACE]
                        add_theme_intents = [r for r in filtered_rest if r.intent == AssistantIntent.ADD_THEME]
                        other_intents = [r for r in filtered_rest if r.intent not in {AssistantIntent.ADD_PLACE, AssistantIntent.ADD_THEME}]

                        # If we have multiple AddPlace intents and the first intent was also AddPlace,
                        # combine them into a single intent to avoid sequential processing issues
                        if add_place_intents and final_intent == AssistantIntent.ADD_PLACE:
                            logging.info(f"agent_node: Combining {len(add_place_intents)} additional AddPlace intents with the first one")
                            # Extract all place names from the additional AddPlace intents
                            additional_places = []
                            for add_intent in add_place_intents:
                                place_arg = add_intent.arguments.get("place")
                                if place_arg:
                                    additional_places.append(place_arg)

                            # Update the current payload to include all places
                            if additional_places:
                                current_places = final_args.get("places", [final_args.get("place")] if final_args.get("place") else [])
                                if isinstance(current_places, str):
                                    current_places = [current_places]
                                all_places = current_places + additional_places
                                payload_to_route["arguments"] = {"places": all_places}
                                state["last_intent_payload"] = payload_to_route
                                logging.info(f"agent_node: Combined places: {all_places}")

                            # Only queue non-AddPlace intents
                            filtered_rest = other_intents + add_theme_intents

                        # CRITICAL: Prevent AddTheme multiplication by only keeping one AddTheme intent
                        if add_theme_intents and final_intent != AssistantIntent.ADD_THEME:
                            logging.info(f"agent_node: Found {len(add_theme_intents)} AddTheme intents, keeping only the first one to prevent multiplication")
                            # Only add the first AddTheme intent to prevent multiplication
                            if add_theme_intents:
                                filtered_rest = other_intents + add_theme_intents[:1]
                            else:
                                filtered_rest = other_intents
                        elif add_theme_intents and final_intent == AssistantIntent.ADD_THEME:
                            logging.info(f"agent_node: Already processing AddTheme, dropping {len(add_theme_intents)} duplicate AddTheme intents")
                            # If we're already processing an AddTheme, drop all additional ones
                            filtered_rest = other_intents

                        if filtered_rest:
                            # Deduplicate intents to avoid processing the same intent multiple times
                            existing_queue = state.get("intent_queue", [])
                            new_intents = []
                            seen_in_batch = set()  # Track intents already seen in this batch

                            for intent_obj in filtered_rest:
                                intent_dict = intent_obj.model_dump()
                                # Create a hashable key for this intent
                                intent_key = (
                                    intent_dict.get("intent"),
                                    str(sorted(intent_dict.get("arguments", {}).items()))
                                )

                                # Check if this exact intent is already in the existing queue
                                if existing_queue:
                                    already_exists = any(
                                        existing_intent.get("intent") == intent_dict.get("intent") and
                                        existing_intent.get("arguments") == intent_dict.get("arguments")
                                        for existing_intent in existing_queue
                                    )
                                else:
                                    already_exists = False

                                # Also check if the intent is the same as the current intent being processed
                                current_intent_match = (
                                    payload_to_route.get("intent") == intent_dict.get("intent") and
                                    payload_to_route.get("arguments") == intent_dict.get("arguments")
                                )

                                # CRITICAL: Check if we've already seen this intent in this batch
                                already_in_batch = intent_key in seen_in_batch

                                if not already_exists and not current_intent_match and not already_in_batch:
                                    new_intents.append(intent_dict)
                                    seen_in_batch.add(intent_key)  # Mark this intent as seen in this batch
                                    logging.debug(f"agent_node: Adding unique intent to queue: {intent_dict}")
                                else:
                                    logging.debug(f"agent_node: Skipping duplicate intent: {intent_dict} (already_exists={already_exists}, current_match={current_intent_match}, in_batch={already_in_batch})")

                            if new_intents:
                                intent_queue = state.get("intent_queue", [])
                                if intent_queue is None:
                                    intent_queue = []
                                intent_queue.extend(new_intents)
                                state["intent_queue"] = intent_queue
                                logging.info(f"agent_node: Queued {len(new_intents)} new intents (filtered out {len(filtered_rest) - len(new_intents)} duplicates and Chat intents).")
                            else:
                                logging.info(f"agent_node: All {len(filtered_rest)} intents were duplicates or Chat intents.")
                        else:
                            logging.info(f"agent_node: All AddPlace intents combined, no other intents to queue.")
                    else:
                        logging.info(f"agent_node: Filtered out {len(rest)} Chat intents since specific intent was processed.")
            else:
                logging.warning("agent_node: No intents extracted from user text.")
                # Fallback: Treat as CHAT or route to clarification? Let's route to ask_followup.
                final_intent = None # Signal to route to followup/end
                payload_to_route = {}


    # Priority 3: Check queue (only if no pre-set intent and not human message)
    elif not final_intent and last_msg: # Implies not HumanMessage
        logging.info("agent_node: Not human message & no pre-set payload, checking queue.")

        # CRITICAL: Don't process intent queue if user is being asked to make a selection
        current_node = state.get("current_node")
        has_options = bool(state.get("options"))
        selection_idx = state.get("selection_idx")

        if current_node == "resolve_place_and_unit" and has_options and selection_idx is None:
            logging.info("agent_node: User selection in progress, not processing intent queue")
            # Don't clear selection_idx here as we're waiting for user input
            return state
        elif current_node == "resolve_place_and_unit" and selection_idx is not None:
            logging.info(f"agent_node: User made selection {selection_idx}, routing to resolve_place_and_unit")
            # Pass selection_idx to resolve_place_and_unit but don't clear it here as it's needed
            return Command(goto="resolve_place_and_unit", update=state)

        queue = state.get("intent_queue", [])
        if queue:
            payload_from_queue = queue.pop(0) # Take first item
            state["intent_queue"] = queue  # Update queue after removal

            try:
                 intent_val = payload_from_queue["intent"]
                 final_intent = AssistantIntent(intent_val)
                 final_args = payload_from_queue.get("arguments", {})
                 payload_to_route = payload_from_queue
                 state["last_intent_payload"] = payload_to_route # Update state with processed payload
                 logging.info(f"agent_node: Processing intent '{final_intent.value}' from queue.")
            except ValueError:
                 logging.warning(f"agent_node: Invalid intent '{payload_from_queue.get('intent')}' found in queue. Skipping.")
                 # CRITICAL: Clear selection_idx on error to prevent stale values
                 state["selection_idx"] = None
                 # If queue item is bad, just return state and let next cycle handle it?
                 # Or try next queue item? For now, return state to avoid complexity.
                 return state
        else:
            logging.info("agent_node: No human message, no pre-set payload, queue empty. Checking for unfinished place processing.")
            # Check if there are more places to process, but avoid reprocessing places that are actively being handled
            current_node = state.get("current_node")
            num_places = len(state.get("extracted_place_names", []))
            current_index = state.get("current_place_index", 0) or 0
            has_pending_options = bool(state.get("options"))

            # Only block if we're specifically in resolve_place_and_unit with pending options (waiting for user input)
            # Don't block other cases where we need to continue the workflow
            if current_node == "resolve_place_and_unit" and has_pending_options:
                logging.info(f"agent_node: resolve_place_and_unit has pending user input. Ending turn.")
                # Don't clear selection_idx here as user input may be pending
                return state

            if num_places > 0 and current_index < num_places:
                logging.info(f"agent_node: Found {num_places - current_index} more places to process, routing to resolve_place_and_unit")
                # Don't clear selection_idx here as resolve_place_and_unit may need it
                return Command(goto="resolve_place_and_unit", update=state)
            else:
                logging.info("agent_node: No places to process. Ending turn.")
                # CRITICAL: Clear selection_idx when no work to do to prevent stale values
                state["selection_idx"] = None
                return state # Nothing to process


    # --- Routing Logic ---
    if final_intent is AssistantIntent.CHAT:
        txt = final_args.get("text", "Okay.") # Default chat response
        logging.info(f"agent_node: Handling CHAT intent.")
        # Mark chat responses as streamable
        chat_message = AIMessage(
            content=txt,
            response_metadata={"stream_mode": "stream"}
        )
        state.setdefault("messages", []).append(chat_message)
        state["last_intent_payload"] = {} # Clear the payload after processing
        # CRITICAL: Clear selection_idx for CHAT intent to prevent stale values
        state["selection_idx"] = None
        # CHAT usually ends the turn for the AI.
        return state # Return state after adding message

    elif final_intent:
        # Route to the specific handler node based on the determined intent
        target_node = f"{final_intent.value}_node"
        logging.info(f"agent_node: Routing to target node: {target_node}")
        # Ensure the payload used for the update is the one we determined needed routing
        update_for_command = {
            "last_intent_payload": payload_to_route,
            "intent_queue": state.get("intent_queue", []), # Pass cleaned queue (cleaning happened earlier)
            # CRITICAL: Clear selection_idx when routing to prevent stale values
            "selection_idx": None,
        }
        
        # CRITICAL: For AddPlace/RemovePlace intents, clear any pending theme state to prevent interference
        if final_intent in [AssistantIntent.ADD_PLACE, AssistantIntent.REMOVE_PLACE]:
            update_for_command.update({
                "extracted_theme": None,  # Clear any pending theme queries
                "current_node": None,     # Clear any pending node state
                "options": [],            # Clear any pending options
            })
            logging.info(f"agent_node: Cleared theme state for {final_intent.value} intent to prevent interference")
        
        return Command(goto=target_node, update=update_for_command)

    else:
        # Fallback if no intent determined (e.g., LLM failed, queue empty/invalid, no pre-set)
        logging.warning("agent_node: No valid intent determined. Routing to END.")
        # Route to clarification node
        # return Command(goto="ask_followup_node", update={
        #      "last_intent_payload": None, # Clear any potentially invalid payload
        #      "intent_queue": state.get("intent_queue", []),
        # })
        return Command(goto=END, update={
             "last_intent_payload": None, # Clear any potentially invalid payload
             "intent_queue": state.get("intent_queue", []),
             # CRITICAL: Clear selection_idx when ending to prevent stale values
             "selection_idx": None,
        })
