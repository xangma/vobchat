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

    # CRITICAL: Check if we have a new human message first
    # If so, prioritize extracting intent from the new message over any stale payloads
    last_msg = state.get("messages", [])[-1] if state.get("messages") else None
    has_new_human_message = last_msg and isinstance(last_msg, HumanMessage)

    # CRITICAL: Check if there are more places to process before any intent processing
    # This ensures multi-place workflows continue properly
    num_places = len(state.get("places", []) or [])
    current_index = state.get("current_place_index", 0) or 0
    if num_places > 0 and current_index < num_places:
        # Check if we're waiting for user input in resolve_place_and_unit
        current_node = state.get("current_node")
        has_pending_options = bool(state.get("options"))
        if not (current_node == "resolve_place_and_unit" and has_pending_options):
            logging.info(f"agent_node: Found {num_places - current_index} more places to process, routing to resolve_place_and_unit")
            return Command(goto="resolve_place_and_unit")

    # CRITICAL: If all places are processed and we have selected units, route to theme selection
    # But only if we haven't already tried and failed (to prevent infinite loops)
    elif num_places > 0 and current_index >= num_places:
        from vobchat.state_schema import get_selected_units
        selected_units = get_selected_units(state)
        current_node = state.get("current_node")
        if selected_units and not state.get("selected_theme") and current_node != "resolve_theme":
            # Check if there's an AddTheme intent in the queue that should be processed first
            queue = state.get("intent_queue", []) or []
            add_theme_intent = None
            for intent in queue:
                if intent.get("intent") == "AddTheme":
                    add_theme_intent = intent
                    break
            
            if add_theme_intent:
                logging.info(f"agent_node: Found AddTheme intent in queue, processing before theme selection")
                # Remove the AddTheme intent from queue and process it
                updated_queue = [intent for intent in queue if intent != add_theme_intent]
                return Command(goto="AddTheme_node", update={
                    "intent_queue": updated_queue,
                    "last_intent_payload": add_theme_intent
                })
            else:
                logging.info(f"agent_node: All places processed ({current_index}/{num_places}), routing to theme selection")
                return Command(goto="resolve_theme")

    # Priority 1: Check for pre-set intent from Command update (e.g., map click)
    # BUT only if there's no new human message to process
    pre_set_payload = state.get("last_intent_payload")
    if pre_set_payload and pre_set_payload.get("intent") and not has_new_human_message:
        try:
            intent_val = pre_set_payload["intent"]
            final_intent = AssistantIntent(intent_val)
            final_args = pre_set_payload.get("arguments", {})
            payload_to_route = pre_set_payload
            logging.info(f"agent_node: Using pre-set intent '{final_intent.value}' from Command/State.")
            target_node = f"{final_intent.value}_node"
            logging.info(f"agent_node: Routing to target node: {target_node}")
            # Ensure the payload used for the update is the one we determined needed routing
            # CRITICAL: Include last_intent_payload in Command update so target nodes can access it
            update_for_command = {
                "intent_queue": state.get("intent_queue", []), # Pass cleaned queue (cleaning happened earlier)
                # CRITICAL: Preserve last_intent_payload for target nodes that need it (like DescribeTheme_node)
                "last_intent_payload": payload_to_route,
            }

            # CRITICAL: DON'T clear last_intent_payload here - let the target node clear it after processing
            # If we clear it in the Command update, the target node won't see the arguments
            logger.info(f"agent_node: Routing to {target_node} with payload intact - target node will clear after processing")

            return Command(goto=target_node, update=update_for_command)

        except ValueError:
            logging.warning(f"agent_node: Invalid intent '{pre_set_payload.get('intent')}' in last_intent_payload.")
            state["last_intent_payload"] = None # Clear invalid payload
    # Priority 2: Check for fresh Human input (prioritized over stale payloads)
    if not final_intent and has_new_human_message:
        user_text = cast(str, last_msg.content).strip() if last_msg else ""

        # CRITICAL: Only extract intents if this message hasn't been processed yet
        # Check if we're currently in an interrupt state waiting for user selection
        # In that case, this message might be unrelated to the selection and should be processed
        current_node = state.get("current_node")
        has_options = bool(state.get("options"))

        # CRITICAL: Always process new human messages, even if we're in a selection state
        # The user typing a new message indicates they want to change direction
        skip_extraction = False

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
                 # If queue item is bad, just return state and let next cycle handle it?
                 # Or try next queue item? For now, return state to avoid complexity.
                 return state
        else:
            # Check if we have selected units and a theme but no cube data - route to find_cubes_node
            from vobchat.state_schema import get_selected_units
            selected_units = get_selected_units(state)
            selected_theme = state.get("selected_theme")
            existing_cubes = state.get("selected_cubes")

            if selected_units and selected_theme and not existing_cubes:
                logging.info("agent_node: Have selected units and theme but no cube data, routing to find_cubes_node")
                return Command(goto="find_cubes_node")

            logging.info("agent_node: No human message, no pre-set payload, queue empty. Ending turn.")
            return state # Nothing to process

    # --- Routing Logic ---
    if final_intent is AssistantIntent.CHAT:
        txt = final_args.get("text", "Okay.") # Default chat response
        logging.info(f"agent_node: Handling CHAT intent.")
        # Use _append_ai for consistent message handling
        from vobchat.nodes.utils import _append_ai
        _append_ai(state, txt)
        state["last_intent_payload"] = {} # Clear the payload after processing
        # CHAT usually ends the turn for the AI.
        return state # Return state after adding message

    elif final_intent:
        # Route to the specific handler node based on the determined intent
        target_node = f"{final_intent.value}_node"
        logging.info(f"agent_node: Routing to target node: {target_node}")
        # Ensure the payload used for the update is the one we determined needed routing
        # CRITICAL: Include last_intent_payload in Command update so target nodes can access it
        update_for_command = {
            "intent_queue": state.get("intent_queue", []), # Pass cleaned queue (cleaning happened earlier)
            # CRITICAL: Preserve last_intent_payload for target nodes that need it (like DescribeTheme_node)
            "last_intent_payload": payload_to_route,
        }

        # CRITICAL: DON'T clear last_intent_payload here - let the target node clear it after processing
        # If we clear it in the Command update, the target node won't see the arguments

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
        })
