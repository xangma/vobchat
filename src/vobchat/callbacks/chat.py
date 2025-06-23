# src/callbacks/chat.py (DEPRECATED - OLD CELERY/BACKGROUND CALLBACK METHOD)\n#\n# *** THIS FILE IS DEPRECATED ***\n# This file contains the old Celery-based background callback implementation.\n# The new SSE-based implementation is in chat_sse.py\n# This file is kept for reference and potential fallback scenarios.\n# *** DO NOT USE FOR NEW DEVELOPMENT ***

# =============================================================================
#  DDME - Dash Chat ↔ LangGraph Bridge
#  ---------------------------------------------------------------------------
#  What this file is
#  -----------------
#  • The single entry-point that wires our Dash UI to the LangGraph workflow
#    defined in `src/workflow.py`.
#  • Converts a heterogenous, event-driven front-end (buttons, map clicks,
#    text input) into a **linear stream** of messages + state snapshots that
#    LangGraph can reason over.
#
#  Mental model for an LLM reading this
#  ------------------------------------
#  Think of the Dash app as a *dumb* terminal. All real decisions live inside
#  the workflow.  The callback in this file merely:
#
#    1.  Collects the **trigger** that fired (send-button, dynamic button,
#        map retrigger, etc.).
#    2.  Packages the current Dash “stores” into a single `lg_State`-compatible
#        dict.
#    3.  Calls `compiled_workflow.astream(...)`, forwarding streamed
#        AIMessageChunks back to the UI with progressive updates.
#    4.  Detects **interrupts** emitted by the workflow and materialises them into Dash widgets:
#          - multiple-choice buttons
#          - map selection requests
#          - cube visualisation signals
#    5.  Persists / hydrates state on every turn so the graph can be
#        *paused* by the front-end and later *resumed* (e.g. after a map click).
#
#  Life-cycle of a user turn
#  -------------------------
#   UI event ─▶ `update_chat` (sync shell) ─▶ `_run_async_logic` (async) ─▶
#   LangGraph stream ─▶ progressive UI updates ─▶ (optional) interrupt
#
#  Key invariants this file must uphold
#  ------------------------------------
#  • **Exactly one** `background=True` Dash callback updates the chat area.
#  • `thread_id` stays constant for the life of a conversation so the
#    check-pointer can merge incremental state.
#  • `selection_idx` is written *only* when a dynamic button is clicked and
#    cleared immediately after the workflow consumes it.
#  • `retrigger_chat` is the sole “cycle breaker” that lets map-driven
#    changes re-enter the LangGraph loop without creating circular
#    dependencies.
#
#  Place workflow coupling
#  -----------------------
#  The callback does **not** implement any place-selection logic itself;
#  it merely honours the routing produced by the workflow:
#
#      multi_place_tool_call_node ⇢ agent ⇢ process_place_selection ⇢ agent
#      ⇢ process_unit_selection  ⇢ agent … (loops per place)
#
#  All loops are therefore driven by `state.current_place_index`, which
#  the two `process_*` nodes increment.  The callback must never touch that
#  counter.
#
#  Extending / modifying?
#  ----------------------
#  • To add a new interaction modality, fire an interrupt from the workflow
#    and teach this file how to render it.
#  • To add a new long-running tool, no changes here are required—just make
#    sure the workflow emits ToolMessages so the chat router can spot them.
# =============================================================================

import json
import asyncio
import nest_asyncio
# Apply nest_asyncio to allow nested event loops. This is often necessary when
# running an async framework (like LangGraph streaming) inside another async
# environment or one that manages its own event loop (like Dash/Flask underlying ASGI server).
nest_asyncio.apply()
import dash
from dash import html, set_props
from dash.exceptions import PreventUpdate
from uuid import uuid4

# Import application state stores defined elsewhere (presumably in stores.py)
from vobchat.stores import app_state_data, map_state_data, place_state_data
from vobchat.state_schema import lg_State  # Import the lg_State TypedDict for type hinting
from vobchat.intent_handling import AssistantIntent, AssistantIntentPayload

# Import Dash core components and Bootstrap components
from dash import Input, Output, State, ALL, ctx
import dash_bootstrap_components as dbc
import logging

from time import monotonic

# Import CycleBreakerInput for managing circular dependencies in callbacks if needed
from dash_extensions.enrich import CycleBreakerInput

# Import LangGraph types for interacting with the workflow
from langgraph.types import interrupt, Command
# Import necessary LangChain message types used within the workflow's state
from langchain_core.messages import AIMessage, HumanMessage, AIMessageChunk, ToolMessage

from flask import session

# Set up logging for this module
logger = logging.getLogger(__name__)


class StreamThrottler:
    """Batch UI updates so we don’t call set_props more than ~10× s‑1."""
    def __init__(self, interval: float = 0.10):
        self.interval = interval
        self._last_flush = monotonic()

    def ready(self) -> bool:
        return monotonic() - self._last_flush >= self.interval

    def mark_flushed(self):
        self._last_flush = monotonic()

def _msg_to_div(msg, idx: int):
    if isinstance(msg, HumanMessage):
        return html.Div(msg.content, className="speech-bubble user-bubble", key=f"user-{idx}")
    if isinstance(msg, AIMessage):
        return html.Div(msg.content, className="speech-bubble ai-bubble",   key=f"ai-{idx}")
    # Skip ToolMessage (or style differently)
    return None

def register_chat_callbacks(app, compiled_workflow, background_callback_manager):
    """
    Registers the Dash callbacks related to the chat interface.

    Args:
        app: The Dash application instance.
        compiled_workflow: The compiled LangGraph workflow instance.
        background_callback_manager: The Dash background callback manager instance.
    """

    @app.callback(
        # Define the outputs of the callback function
        Output("chat-display", "children", allow_duplicate=True),  # Update the chat message area
        Output("chat-input", "value", allow_duplicate=True),     # Clear the chat input field
        Output("app-state", "data", allow_duplicate=True),       # Update the main application state store
        Output("map-state", "data", allow_duplicate=True),       # Update the map-related state store
        Output("place-state", "data", allow_duplicate=True),     # Update the place/data-related state store
        Output("retrigger-chat", "data", allow_duplicate=True),  # Output to potentially clear the retrigger signal
        Output("options-container", "children"),                # Update the container holding dynamic buttons
        Output("counts-store", "data"),                         # Update a store potentially holding counts (not fully clear from context)
        Output("thread-id", "data"),                            # Output the current conversation thread ID
        Output("map-click-add-trigger", "data", allow_duplicate=True),
        Output("map-click-remove-trigger", "data", allow_duplicate=True),
        # Define the inputs and states for the callback function
        Input("send-button", "n_clicks"),                      # Triggered when the send button is clicked
        # Triggered when any dynamic button (e.g., place/unit/theme choice) is clicked. Uses ALL wildcard.
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        # Input used to break potential circular dependencies or manually retrigger the callback
        CycleBreakerInput("retrigger-chat", "data"),
        Input("reset-button", "n_clicks"),                     # Triggered when the reset button is clicked
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),
        State("thread-id", "data"),                            # Get the current conversation thread ID
        State("app-state", "data"),                            # Get the current application state
        State("map-state", "data"),                            # Get the current map state
        State("place-state", "data"),                          # Get the current place state
        State("chat-input", "value"),                          # Get the current value from the chat input box
        State("chat-display", "children"),                     # Get the current chat history elements
        State("options-container", "children"),                # Get the current dynamic buttons
        State("counts-store", "data"),                         # Get the current counts store data
        # Background callback configuration
        background=True,                                      # Run this callback in the background
        # Update chat display progressively during execution
        progress=Output("chat-display", "children"),
        manager=background_callback_manager,                  # Use the provided background callback manager
        # Define components to disable while the callback is running
        running=[
             (Output("send-button", "disabled"), True, False), # Disable send button while running
        #      (Output("chat-input", "disabled"), True, False), # Optionally disable input field
        #      (Output({"type": "dynamic-button-user-choice", "index": ALL}, "disabled"), True, False), # Optionally disable dynamic buttons
         ],
        prevent_initial_call=True                             # Don't run this callback when the app first loads
    )
    # Synchronous wrapper function for the main chat logic
    def update_chat(
        set_progress,  # Function provided by Dash to update the 'progress' Output
        n_clicks,      # Number of clicks for the send button
        button_clicks, # List of click counts for dynamic buttons
        retrigger_chat,# Data from the retrigger signal input
        reset__n_clicks, # Number of clicks for the reset button
        map_add_payload,
        map_remove_payload,
        thread_id,     # Current conversation thread ID
        app_state,     # Current app state data
        map_state,     # Current map state data
        place_state,   # Current place state data
        user_input,    # Current text in the chat input field
        chat_history,  # Current list of chat message components
        buttons,       # Current dynamic button components
        counts_store   # Current counts store data
    ):
        """
        Handles user input, button clicks, and workflow execution.
        This synchronous function orchestrates the call to the asynchronous
        workflow logic using asyncio.run().
        """
        # --- Define an inner async function for the core LangGraph interaction ---
        async def _run_async_logic(
            initial_chat_history, initial_app_state, initial_map_state, initial_place_state,
            current_user_input, current_thread_id, current_config, triggered_by_button, current_selection_idx,
            is_retrigger, current_map_intent_payload, is_triggered_by_map_click # Flag indicating if this run was triggered by the retrigger mechanism
        ):
            """
            Executes the asynchronous LangGraph workflow, handles state synchronization,
            streaming responses, and interrupts.
            """
            logger.info("Running async logic for LangGraph workflow.")
            # Make copies of mutable state objects to avoid modifying the outer scope's state directly
            # until the final results are ready. Use slicing for lists.
            history = initial_chat_history[:]
            app_state_async = initial_app_state.copy()
            map_state_async = initial_map_state.copy()
            place_state_async = initial_place_state.copy()

            # --- State Synchronization (Crucial Fix) ---
            # If this execution was triggered by the 'retrigger-chat' signal (likely after
            # an interaction outside the chat, like map selection, modified a Dash state like map_state),
            # we need to synchronize the relevant Dash state back into the persistent LangGraph state
            # stored by the checkpointer (e.g., Redis) before resuming the workflow.
            latest_state = await compiled_workflow.aget_state(current_config)
            if is_retrigger and initial_map_state.get('selected_polygons') and not latest_state.next:
                logger.info("Retrigger detected: Syncing map_state to LangGraph state (workflow not interrupted).")
                try:
                    # Retrieve the latest state snapshot from the LangGraph checkpointer

                    if latest_state:
                        # Copy the state values to modify
                        current_workflow_state_values = latest_state.values.copy()
                    else:
                        # Handle defensively if state couldn't be retrieved (shouldn't happen if workflow was interrupted)
                        current_workflow_state_values = {}
                        logger.warning("Could not retrieve workflow state before sync on retrigger.")

                    # Update the retrieved workflow state values with the latest data from the Dash map_state
                    # This assumes the workflow state has keys 'selected_polygons' and 'selected_polygons_unit_types'
                    existing_units = current_workflow_state_values.get("selected_place_g_units", [])
                    map_polygons = initial_map_state["selected_polygons"]
                    logger.info(f"State sync: existing_units={existing_units}, map_polygons={map_polygons}")

                    current_workflow_state_values["selected_place_g_units"] = list(
                        set(current_workflow_state_values.get("selected_place_g_units", [])) |
                        {int(p) for p in initial_map_state["selected_polygons"] if str(p).isdigit()}
                    )
                    logger.info(f"State sync: updated_units={current_workflow_state_values['selected_place_g_units']}")
                    # Ensure the interrupt flag in the workflow state is cleared, as we are now resuming
                    # *after* handling the interrupt condition that led to the retrigger (e.g., map selection).

                    # Persist the updated state back to the checkpointer
                    await compiled_workflow.aupdate_state(config=current_config, values=current_workflow_state_values, as_node="agent_node")
                    logger.info("Successfully updated workflow state with map selection data.")


                except Exception as sync_exc:
                    logger.error(f"Error syncing map state to workflow state on retrigger: {sync_exc}", exc_info=True)
                    # Add an error message to the chat history
                    history.insert(0, html.Div(f"Error syncing map state: {str(sync_exc)}", style={"color": "orange"}))
                    # Decide whether to stop or continue; currently continues.
            elif is_retrigger and initial_map_state.get('selected_polygons') and latest_state.next:
                logger.info(f"Retrigger detected: Workflow is interrupted with next nodes: {latest_state.next}. Not syncing state to preserve interrupt.")
            elif is_retrigger and initial_map_state.get('unit_types'):
                # CRITICAL: Handle unit type changes during retrigger - resume workflow for next place
                logger.info("Retrigger detected: Unit type changes may need resuming workflow.")
                workflow_input = None


            # --- Prepare inputs for the LangGraph workflow based on how this function was triggered ---
            workflow_input = None
            # Determine workflow input *after* potential state synchronization
            # Case 1: New user text input submitted via send button
            if current_user_input and current_user_input.strip() and not triggered_by_button and not is_retrigger:
                # Pass the new user message to the workflow. Assumes the workflow state handles 'messages'.
                workflow_input = {"messages": [("user", current_user_input)]}
                logger.info(f"User input detected: {current_user_input}")

            # Case 2: Dynamic button clicked (e.g., place/unit/theme choice)
            if current_selection_idx is not None and triggered_by_button:
                # Check workflow state to ensure it's interrupted and waiting
                button_state = await compiled_workflow.aget_state(current_config)
                logger.info(f"Button click: Current workflow state - next={button_state.next if button_state else None}")
                logger.info(f"Button click: Current workflow tasks={len(button_state.tasks) if button_state and button_state.tasks else 0}")

                if button_state and button_state.next:
                    logger.info(f"Button click: Workflow interrupted, updating state and resuming from {button_state.next}")

                    # CRITICAL: Determine the correct node based on the workflow context
                    # Check what node was interrupted to route button clicks correctly
                    current_node = button_state.values.get("current_node") if button_state.values else None
                    target_node = None

                    if current_node == "resolve_theme":
                        target_node = "resolve_theme"
                        logger.info(f"Button click: Theme selection button, routing to resolve_theme")
                        # Patch: persist theme selection context to the workflow state
                        theme_options = app_state_async.get("button_options")
                        workflow_update = {
                            "selection_idx": current_selection_idx,
                            "current_node": "resolve_theme",
                        }
                        if theme_options:
                            workflow_update["options"] = theme_options
                        else:
                            logger.warning("No theme options found in app_state_async['button_options'] – button click may not advance workflow!")
                        logger.info(f"VOBCHAT CHAT aupdate_state values: {workflow_update}")
                        await compiled_workflow.aupdate_state(
                            config=current_config, values=workflow_update, as_node=target_node
                        )
                        workflow_input = None
                    elif current_node == "resolve_place_and_unit":
                        target_node = "resolve_place_and_unit"
                        logger.info(f"Button click: Place/unit selection button, routing to resolve_place_and_unit")
                        await compiled_workflow.aupdate_state(
                            config=current_config,
                            values={"selection_idx": current_selection_idx},
                            as_node=target_node
                        )
                        # After a unit-type selection, re-trigger callback to resume next place
                        app_state_async["retrigger_chat"] = True
                        workflow_input = None
                    elif current_node == "ask_followup_node":
                        target_node = "ask_followup_node"
                        logger.info(f"Button click: Followup selection button, routing to ask_followup_node")
                        await compiled_workflow.aupdate_state(config=current_config, values={"selection_idx": current_selection_idx}, as_node=target_node)
                        workflow_input = None
                    else:
                        logger.info(f"Button click: Unknown context '{current_node}', letting workflow handle routing naturally")
                        await compiled_workflow.aupdate_state(config=current_config, values={"selection_idx": current_selection_idx})
                        workflow_input = None
                else:
                    logger.warning(f"Button click: Workflow not interrupted, setting selection_idx={current_selection_idx}")
                    workflow_input = {"selection_idx": current_selection_idx}
            # Case 3: Retrigger after map selection - resume the workflow
            if is_retrigger and initial_map_state.get('selected_polygons'):
                # Use the state we already retrieved
                current_state = latest_state
                current_units = current_state.values.get("selected_place_g_units", []) if current_state else []
                logger.info(f"Retrigger continuation: current units in state = {current_units}")

                # Check if workflow is in interrupted state
                if current_state and current_state.next:
                    logger.info(f"Workflow has next nodes to execute: {current_state.next}")
                    # Resume the workflow with None input to continue from interrupt
                    workflow_input = None
                elif current_state and hasattr(current_state, 'tasks') and current_state.tasks:
                    logger.info(f"Workflow has pending tasks: {[task.name for task in current_state.tasks]}")
                    # Resume the workflow with None input to continue from interrupt
                    workflow_input = None
                else:
                    logger.info("Workflow not in interrupted state, using empty workflow input to trigger routing")
                    # Use None input to trigger the routing logic without new messages
                    workflow_input = None
                    elif current_node == "resolve_place_and_unit":
                        target_node = "resolve_place_and_unit"
                        logger.info(f"Button click: Place/unit selection button, routing to resolve_place_and_unit")
                    elif current_node == "ask_followup_node":
                        target_node = "ask_followup_node"
                        logger.info(f"Button click: Followup selection button, routing to ask_followup_node")
                    else:
                        # Fallback: use the workflow's natural routing instead of forcing a specific node
                        target_node = None
                        logger.info(f"Button click: Unknown context '{current_node}', letting workflow handle routing naturally")

                    if target_node:
                        # Update state and resume from interrupt with the correct target node
                        await compiled_workflow.aupdate_state(config=current_config, values={"selection_idx": current_selection_idx}, as_node=target_node)
                    else:
                        # Let the workflow route naturally without forcing a specific node
                        await compiled_workflow.aupdate_state(config=current_config, values={"selection_idx": current_selection_idx})

                    # Resume with None input to continue from interrupt
                    workflow_input = None
                else:
                    logger.warning(f"Button click: Workflow not interrupted, setting selection_idx={current_selection_idx}")
                    workflow_input = {"selection_idx": current_selection_idx}

            # Case 3: Retrigger after map selection - resume the workflow
            if is_retrigger and initial_map_state.get('selected_polygons'):
                # Use the state we already retrieved
                current_state = latest_state
                current_units = current_state.values.get("selected_place_g_units", []) if current_state else []
                logger.info(f"Retrigger continuation: current units in state = {current_units}")

                # Check if workflow is in interrupted state
                if current_state and current_state.next:
                    logger.info(f"Workflow has next nodes to execute: {current_state.next}")
                    # Resume the workflow with None input to continue from interrupt
                    workflow_input = None
                elif current_state and hasattr(current_state, 'tasks') and current_state.tasks:
                    logger.info(f"Workflow has pending tasks: {[task.name for task in current_state.tasks]}")
                    # Resume the workflow with None input to continue from interrupt
                    workflow_input = None
                else:
                    logger.info("Workflow not in interrupted state, using empty workflow input to trigger routing")
                    # Use None input to trigger the routing logic without new messages
                    workflow_input = None

                logger.info("Retrigger with map selection detected: resuming workflow")

            # --- Execute the LangGraph workflow using asynchronous streaming ---
            full_ai_response = "" # Accumulator for the complete AI response message
            truncated_ai_response = "" # For displaying truncated messages
            final_state_values = {} # To store the final workflow state values after streaming
            try:
                throttler = StreamThrottler(interval=0.10)
                # Call the workflow's astream method to get events (messages, state updates, etc.)
                logger.info("Starting workflow streaming.")
                async for msg, metadata in compiled_workflow.astream(
                    workflow_input,    # Pass the prepared input (or None if resuming/retriggering)
                    config=current_config, # Pass the thread configuration
                    stream_mode="messages" # Request message-based streaming events
                    ):
                    # Check if the event contains a message chunk from the AI
                    if msg.content and isinstance(msg, AIMessageChunk):
                        full_ai_response += msg.content
                        if "'intent': 'Chat', 'arguments': {'text': '" in full_ai_response or '"intent": "Chat", "arguments": {"text": "' in full_ai_response:
                            truncated_ai_response = full_ai_response.split("'intent': 'Chat', 'arguments': {'text': '")[1] if "'intent': 'Chat', 'arguments': {'text': '" in full_ai_response else full_ai_response.split('"intent": "Chat", "arguments": {"text": "')[1]

                            if truncated_ai_response:
                                truncated_ai_response = truncated_ai_response.split("'}")[0] if "'intent': 'Chat', 'arguments': {'text': '" in full_ai_response else truncated_ai_response.split('"}')[0]

                        if not history or not isinstance(history[0], html.Div) \
                        or "ai-bubble" not in history[0].className:
                            # first chunk → start a new bubble
                            if truncated_ai_response != "":
                                history.insert(0, html.Div(truncated_ai_response,
                                                        className="speech-bubble ai-bubble"))
                        else:
                            # subsequent chunk → update last bubble
                            history[0].children = truncated_ai_response
                        if throttler.ready():             # only every 100 ms
                            set_props("chat-display", {"children": history})
                            throttler.mark_flushed()
                    # Note: Depending on the LangGraph version and stream_mode, you might get
                    # other types of events here containing state updates. This example primarily
                    # focuses on streaming AIMessageChunks for progressive text display.
                    set_props("chat-display", {"children": history})

            except Exception as stream_exc:
                logger.error(f"Error during workflow stream: {stream_exc}", exc_info=True)
                # Add an error message to the chat history
                history.insert(0, html.Div(f"Streaming Error: {str(stream_exc)}", style={"color": "orange"}))
                # Allow execution to continue to try and fetch the final state

            # --- Post-Stream State Retrieval and Interrupt Handling ---
            try:
                logger.info("Post-stream processing: retrieving final workflow state.")
                # Get the final state of the workflow for this thread
                final_state = await compiled_workflow.aget_state(current_config)
                final_state_values = final_state.values if final_state else {} # Extract the state dictionary
                logger.debug({"event": "workflow_state_after_stream", "final_state_values": final_state_values})

                already = len(history)
                all_msgs = final_state_values.get("messages", [])

                # create divs for *new* messages only
                new_divs = [
                    _msg_to_div(m, i) for i, m in enumerate(all_msgs[already:], start=already)
                    if _msg_to_div(m, i) is not None
                ]

                if new_divs:
                    history = new_divs + history
                    app_state_async["render_cursor"] = len(all_msgs)
                    # ensure at least one final flush (pairs with throttling from task 1)
                    set_props("chat-display", {"children": history})

                # history = final_chat_history_components
                app_state_async["messages"] = history
                # Reset button rendering list for this turn, but preserve existing buttons during retriggers
                buttons_to_render_async = buttons[:] if is_retrigger else []
                logger.info(f"Buttons initialization - is_retrigger: {is_retrigger}, existing buttons: {len(buttons) if buttons else 0}, new buttons: {len(buttons_to_render_async)}")

                interrupt_updates = {}
                interrupt_message = None
                # Check if the workflow ended in an interrupted state, requiring user input or action
                # LangGraph signals interrupts via the `tasks` attribute of the state or potentially custom flags.
                if final_state and final_state.tasks: # Check if there are pending tasks (interrupts)
                    interrupt_task = final_state.tasks[-1] # Assume one interrupt at a time for now
                    # Check for explicit interrupts or custom flags set by workflow nodes
                    if interrupt_task.interrupts:
                        logger.info("Interrupt detected after stream completion.")
                        # Get the data associated with the interrupt
                        # Prioritize explicit interrupts, fall back to data possibly stored in state by the node
                        interrupt_updates.update(interrupt_task.interrupts[0].value)
                        logger.info(f"CALLBACK: Processing interrupt updates: {interrupt_updates}")
                        if interrupt_updates: # Ensure we have interrupt data
                            logger.debug({"event": "processing_interrupt", "interrupt_updates": interrupt_updates})

                            # --- Handle different types of interrupts based on the interrupt_updates content ---

                            # 1. Multiple Choice Options Interrupt: Render buttons for user selection
                            if interrupt_updates.get("options", []):
                                logger.debug("Interrupt with multiple button options")
                                options = interrupt_updates.get("options", [])

                                # Create Dash Bootstrap buttons based on the options provided by the workflow node
                                buttons_to_render_async = [
                                    dbc.Button(
                                        opt["label"], # Text displayed on the button
                                        id={         # Pattern-matching ID for the button callback trigger
                                            "option_type": opt["option_type"], # Type identifier (e.g., 'place', 'unit', 'theme')
                                            "type": "dynamic-button-user-choice", # Fixed type for the callback input
                                            "index": opt["value"] # Value sent back when clicked (often the index or ID)
                                        },
                                        color="secondary", # Bootstrap color
                                        className="unit-filter-button me-2 mb-2", # CSS classes for styling
                                        outline=True, # Button style
                                        value=opt["value"], # HTML value attribute
                                        style={ # Custom styling, potentially using data from options (like color)
                                            '--unit-color': opt.get("color", "#333"), # CSS variable for potential hover effects etc.
                                            'borderColor': opt.get("color", "#333"),
                                            'backgroundColor': 'white',
                                            'color': '#333', # Text color
                                            'transition': 'background-color 0.3s, color 0.3s'
                                        }
                                    )
                                    for opt in options
                                ]

                                # Get the prompt message to display above the buttons
                                prompt_text = interrupt_updates.get("message", "Please choose:")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")

                                # Update the Dash app_state to potentially store button info (if needed elsewhere)
                                logger.info(f"Before theme interrupt update - retrigger_chat: {app_state_async.get('retrigger_chat')}")
                                app_state_async.update({
                                    "button_options": options,
                                    "retrigger_chat": False,  # Ensure retrigger is not set for theme selection
                                })
                                logger.info(f"After theme interrupt update - retrigger_chat: {app_state_async.get('retrigger_chat')}")

                            # Clear any existing place disambiguation state from previous interrupts
                            if not interrupt_updates.get("place_coordinates"):
                                if map_state_async.get("show_place_disambiguation"):
                                    logger.debug("Clearing place disambiguation state")
                                    map_state_async.update({
                                        "place_disambiguation": [],
                                        "show_place_disambiguation": False
                                    })

                            # 1.5. Place Disambiguation with Coordinates: Show places on map for disambiguation
                            if interrupt_updates.get("place_coordinates"):
                                logger.debug("Place disambiguation interrupt with coordinates")
                                place_coords = interrupt_updates.get("place_coordinates", [])
                                logger.info(f"Processing place disambiguation with {len(place_coords)} coordinate locations")

                                # Update map state to show disambiguation points
                                map_state_async.update({
                                    "place_disambiguation": place_coords,  # Add coordinates for map display
                                    "show_place_disambiguation": True  # Flag to show disambiguation mode
                                })

                                # Keep existing button functionality
                                options = interrupt_updates.get("options", [])
                                buttons_to_render_async = [
                                    dbc.Button(
                                        opt["label"],
                                        id={
                                            "option_type": opt["option_type"],
                                            "type": "dynamic-button-user-choice",
                                            "index": opt["value"]
                                        },
                                        color="secondary",
                                        className="unit-filter-button me-2 mb-2",
                                        outline=True,
                                        value=opt["value"],
                                        style={"color": opt.get("color", "#333")}
                                    )
                                    for opt in options
                                ]

                                prompt_text = interrupt_updates.get("message", "Please choose:")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")

                                app_state_async.update({
                                    "button_options": options,
                                    "retrigger_chat": False,
                                })

                            # 2. Map Selection Interrupt: Update map state to show/select units
                            elif interrupt_updates.get("current_node") == "select_unit_on_map":
                                logger.debug("Map selection interrupt")
                                # Extract necessary data from the interrupt payload
                                selected_place_g_units = interrupt_updates.get("selected_place_g_units", [])
                                selected_place_g_unit_types = interrupt_updates.get("selected_place_g_unit_types", [])

                                # CRITICAL: Use workflow selection as authoritative for polygon state
                                # This ensures removals are properly handled and workflow state is trusted
                                workflow_polygons = [str(g_unit) for g_unit in selected_place_g_units]
                                workflow_unit_types = selected_place_g_unit_types[:]  # Copy the list

                                # Get existing map selection for logging comparison
                                existing_polygons = map_state_async.get("selected_polygons", [])

                                logger.info(f"Map selection interrupt: Setting authoritative workflow units {workflow_polygons} (was {existing_polygons})")

                                # Set the authoritative selection from workflow (don't merge with existing)
                                map_state_async["selected_polygons"] = workflow_polygons
                                map_state_async["selected_polygons_unit_types"] = workflow_unit_types

                                # CRITICAL: Keep filter selection (unit_types) separate from polygon types
                                # map_state_async["unit_types"] represents the user's active filter choice
                                # selected_place_g_unit_types represents actual types of selected polygons
                                # These are different concepts - don't override polygon types with filter selection

                                # ENHANCED: Always zoom to show all selected polygons after each place is processed
                                # This provides immediate visual feedback for each polygon selection
                                current_place_index = interrupt_updates.get("current_place_index", 0)
                                extracted_place_names = interrupt_updates.get("extracted_place_names", [])
                                total_places = len(extracted_place_names)

                                # Always trigger zoom when polygons are selected to show immediate feedback
                                if workflow_polygons:
                                    logger.info(f"Map selection interrupt: Setting zoom_to_selection=True for immediate visual feedback (place {current_place_index + 1} of {total_places})")
                                    map_state_async["zoom_to_selection"] = True # Flag to tell the map component to zoom
                                    # Reset the workflow zoom flag for the next potential workflow run
                                    if current_place_index >= total_places - 1:
                                        map_state_async["zoom_triggered_for_workflow"] = False # Reset for next workflow
                                        logger.info(f"Map selection interrupt: Final place processed, reset zoom_triggered_for_workflow flag")
                                else:
                                    logger.info(f"Map selection interrupt: No polygons to zoom to (place {current_place_index + 1} of {total_places})")
                                # Flag indicating a programmatic change is pending (might be used by map callbacks)
                                # map_state_async["programmatic_unit_change_pending"] = interrupt_updates.get("selected_place_g_unit_types", [])
                                interrupt_updates.update({
                                    "selected_polygons": workflow_polygons,
                                    "selected_polygons_unit_types": workflow_unit_types,
                                })
                                # Set the retrigger flag in app_state. This will be picked up by the
                                # 'retrigger_chat_callback' which will then trigger this main 'update_chat'
                                # callback again via the CycleBreakerInput, allowing the workflow to resume
                                # *after* the map state has been updated and potentially interacted with by the user.
                                app_state_async.update({
                                    "button_options": [], # No buttons for map selection
                                    "retrigger_chat": True
                                })
                                # Clear any buttons as this interrupt is handled by map interaction + retriggering
                                buttons_to_render_async = []

                                prompt_text = interrupt_updates.get("message", "")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")

                            # 3. Cube Data Interrupt: Update place state to display visualizations
                            elif interrupt_updates.get("cubes"):
                                logger.info("Processing cube data interrupt")
                                cubes = interrupt_updates.get("cubes", [])
                                logger.info(f"Cube data received: {type(cubes)} - length: {len(cubes) if isinstance(cubes, (list, str)) else 'N/A'}")

                                # Update the Dash place_state_async with the retrieved cube data
                                place_state_async.update({"cubes": cubes})
                                place_state_async.update({"cube_data": cubes})
                                # Persist the selected cubes back into the workflow state if needed later
                                interrupt_updates.update({"selected_cubes": cubes})

                                # Display the message associated with the cube data
                                prompt_text = interrupt_updates.get("message", "Data retrieved.")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")
                                # Update app_state to signal UI to show visualization components
                                app_state_async.update({"show_visualization": True})
                                logger.info("Set show_visualization to True in app_state")

                            # 5. Text Input Interrupt: Prompt the user for text input (handled by next user message)
                            else:
                                # This case assumes the interrupt requires the user to type something
                                # in the chat input next, rather than click a button or interact with the map.
                                logger.debug("Text input interrupt")
                                prompt_text = interrupt_updates.get("message", "Please provide input:")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")



                            # Add interrupt message to chat history (but don't duplicate cube messages)
                            if interrupt_updates.get("message") and not interrupt_updates.get("cubes"):
                                # treat as ordinary assistant text (for non-cube interrupts)
                                txt = interrupt_updates["message"]
                                interrupt_message = html.Div(txt, className="speech-bubble ai-bubble")

                                # ① append to visible history
                                msgs = final_state_values.get("messages", [])
                                last_message = history[0] if history else None
                                if last_message and isinstance(last_message, html.Div):
                                    last_message_text = last_message.children
                                    if interrupt_message.children != last_message_text:
                                        history.insert(0, interrupt_message)

                                        msgs.append(AIMessage(content=txt))
                                elif last_message and last_message.get("props"):
                                    last_message_text = last_message.get("props").get("children")
                                    if interrupt_message.children != last_message_text:
                                        history.insert(0, interrupt_message)
                                        msgs.append(AIMessage(content=txt))

                                interrupt_updates["messages"] = msgs          # <- extra field to persist

                                app_state_async["messages"] = history

                            # For cube interrupts, the message was already handled above
                            elif interrupt_updates.get("cubes") and interrupt_message:
                                # Add the cube interrupt message to history
                                history.insert(0, interrupt_message)
                                msgs = final_state_values.get("messages", [])
                                msgs.append(AIMessage(content=str(interrupt_message.children or "")))
                                interrupt_updates["messages"] = msgs
                                app_state_async["messages"] = history

                # Persist relevant context from the interrupt (like current place index)
                # back into the workflow state. This is important if the interrupt occurred
                # mid-way through processing multiple items (like places).

                # if 'show_visualization_signal' in final_state_values:
                #     show_viz = final_state_values['show_visualization_signal']
                #     logger.info(f"Updating app_state show_visualization based on signal: {show_viz}")
                #     app_state_async['show_visualization'] = show_viz
                #     # Remove the signal flag from the state to be persisted
                #     # del final_state_values['show_visualization_signal']
                #     # Update the state in the checkpointer *if necessary*
                #     interrupt_updates.update({"show_visualization_signal": None}) # Or update with the dict minus the key

                # Persist the interrupt data back into the workflow state.
                # logger.debug(f"CALLBACK: Updating interrupt updates: {interrupt_updates}")
                logger.info(f"CALLBACK: Updating interrupt updates")
                # Only update state if there are actual interrupt updates
                if interrupt_updates:
                    await compiled_workflow.aupdate_state(
                        config=current_config, # Pass the thread configuration
                        values=interrupt_updates, # Update the workflow state with the interrupt data
                        as_node=interrupt_updates.get("current_node", None), # Optional: specify the node that triggered the interrupt
                    )
                if 'selected_polygons' in interrupt_updates:
                    # update map_state with the selected polygons
                    map_state_async['selected_polygons'] = interrupt_updates['selected_polygons']
                    map_state_async['selected_polygons_unit_types'] = interrupt_updates['selected_polygons_unit_types']
                logger.debug("Workflow state updated with interrupt data.")

                logger.info(f"Async logic completed successfully, returning {len(buttons_to_render_async)} buttons.")
                # logger.info("State after async logic: ", interrupt_updates)
                # Return the final computed states and buttons from the async function
                return history, app_state_async, map_state_async, place_state_async, buttons_to_render_async

            except Exception as post_stream_exc:
                logger.error(f"Error processing state/interrupts after stream: {post_stream_exc}", exc_info=True)
                # Add an error message to the chat history
                history.insert(0, html.Div(f"Post-Stream Error: {str(post_stream_exc)}", style={"color": "red"}))
                # Return the current state of things on error to prevent data loss
                return history, app_state_async, map_state_async, place_state_async, []


        # --- Back in the main synchronous callback function `update_chat` ---
        triggered_input = dash.callback_context.triggered[0] # Get info about what triggered the callback
        ctx_trigger = triggered_input["prop_id"] if dash.callback_context.triggered else "No trigger"
        logger.info(f"Callback triggered by: {ctx_trigger}")
        # Determine if the trigger was the retrigger mechanism (either via CycleBreakerInput or the flag in app_state)
        is_retrigger_event = "retrigger-chat_data" in ctx_trigger \
            or (app_state and app_state.get("retrigger_chat"))
        logging.info(f"Is retrigger event: {is_retrigger_event}")
        # Basic setup and reset logic (remains synchronous)
        logger.debug({"event": "update_chat_start (sync part)", "trigger": ctx_trigger, "is_retrigger": is_retrigger_event})
        chat_history = chat_history or [] # Initialize chat history if empty

        # ------------------------------------------------------------------
        #  🌟  First page‑load safety  (rebuild once if browser refreshed)
        # ------------------------------------------------------------------
        if (not chat_history) and app_state and app_state.get("render_cursor", 0):
            try:
                # we’re in a **sync** context → use asyncio.run to call the async API
                state_snapshot = asyncio.run(
                    compiled_workflow.aget_state({"configurable": {"thread_id": thread_id}})
                )
                all_msgs = state_snapshot.values.get("messages", []) if state_snapshot else []
                chat_history = [
                    _msg_to_div(m, i) for i, m in enumerate(all_msgs)
                    if _msg_to_div(m, i) is not None
                ]
            except Exception as exc:
                logger.warning(f"Could not rebuild chat after refresh: {exc}")
                chat_history = []

        if user_input and user_input.strip() and "send-button" in ctx_trigger and not is_retrigger_event:
            user_message_div = html.Div(f"{user_input}", className="speech-bubble user-bubble")
            chat_history.insert(0, user_message_div)
            set_props("chat-display", {"children": chat_history})
            app_state["render_cursor"] = len(chat_history) # Update the render cursor in app_state
            # Clear zoom trigger flag for new workflow
            map_state["zoom_triggered_for_workflow"] = False


        # Initialize clear trigger variables
        clear_add_trigger = dash.no_update
        clear_remove_trigger = dash.no_update

        # Handle Reset Button Click
        if "reset-button" in ctx_trigger:
            logger.info("Reset button clicked. Clearing state.")
            # Reset all relevant states to their initial values (deep copy to avoid mutation)
            app_state = app_state_data.copy()
            map_state = map_state_data.copy()
            place_state = place_state_data.copy()
            # Clear zoom trigger flag for new workflow
            map_state["zoom_triggered_for_workflow"] = False
            chat_history = []
            buttons = []
            thread_id = None # Clear thread ID to start a new conversation
            counts_store = {}
            # clear the state using the same comprehensive reset as Reset_node
            from vobchat.state_nodes import _initial_state
            state = _initial_state()
            # Generate a new thread ID for a completely fresh conversation
            thread_id = str(uuid4())
            state_snapshot = asyncio.run(
                compiled_workflow.aupdate_state(
                    config={"configurable": {"thread_id": thread_id}},
                    values=state,
                    as_node="agent_node"
                )
            )
            # Return the reset states
            return (
                chat_history,          # Empty history
                "",                    # Clear input field
                app_state,             # Initial app state
                map_state,             # Initial map state
                place_state,           # Initial place state
                None,                  # Clear retrigger data
                buttons,               # Empty buttons
                counts_store,          # Initial counts store
                thread_id,              # Cleared thread ID
                clear_add_trigger,       # Clear the add trigger
                clear_remove_trigger,    # Clear the remove trigger
            )

        # Conversation Thread ID and LangGraph Configuration Setup
        if not thread_id:
            # Generate a new unique ID for the conversation thread if one doesn't exist
            thread_id = str(uuid4())
            logger.info(f"Starting new conversation thread: {thread_id}")
        # Configuration object required by LangGraph, associating requests with the specific thread
        config = {"configurable": {"thread_id": thread_id}}

        map_intent_payload = None
        triggered_by_map_click = False
        state_updates_for_map_click = {} # Initialize here


        if ctx_trigger == 'map-click-add-trigger.data' and map_add_payload is not None:
            logger.info(f"Map click (Add) detected. Payload: {map_add_payload}")
            # Build AddPlace intent for any polygon click, using name and optional polygon_id
            id_str = str(map_add_payload.get("id", ""))
            unit_type = map_add_payload.get("type")
            place_name = map_add_payload.get("name", map_add_payload.get("id", "Unknown Place"))
            place_args = {"place": place_name, "unit_type": unit_type}
            if id_str.isdigit():
                place_args["polygon_id"] = int(id_str)
            map_intent_payload = {"intent": AssistantIntent.ADD_PLACE.value, "arguments": place_args}
            state_updates_for_map_click = {
                "last_intent_payload": map_intent_payload,
                "retrigger_chat": True, # Set the retrigger flag to true
                # CRITICAL: Clear selection_idx when processing map clicks to prevent stale values
                "selection_idx": None,
            }
            triggered_by_map_click = True
            # Clear the trigger by returning None for its output later
            clear_add_trigger = None
            clear_remove_trigger = dash.no_update # Don't clear the other one

        elif ctx_trigger == 'map-click-remove-trigger.data' and map_remove_payload is not None:
            logger.info(f"Map click (Remove) detected. Payload: {map_remove_payload}")
            # Build RemovePlace intent for any polygon deselect, using name and optional polygon_id
            id_str = str(map_remove_payload.get("id", ""))
            unit_type = map_remove_payload.get("unit_type")
            place_name = map_remove_payload.get("name", map_remove_payload.get("id", "Unknown Place"))
            place_args = {"place": place_name, "unit_type": unit_type}
            if id_str.isdigit():
                place_args["polygon_id"] = int(id_str)
            map_intent_payload = {"intent": AssistantIntent.REMOVE_PLACE.value, "arguments": place_args}
            state_updates_for_map_click = {
                "last_intent_payload": map_intent_payload,
                "retrigger_chat": True, # Set the retrigger flag to true
                # CRITICAL: Clear selection_idx when processing map clicks to prevent stale values
                "selection_idx": None,
            }
            triggered_by_map_click = True
            # Clear the trigger by returning None for its output later
            clear_add_trigger = dash.no_update
            clear_remove_trigger = None

        else:
            # Default: Don't clear triggers if not triggered by them
            clear_add_trigger = dash.no_update
            clear_remove_trigger = dash.no_update



        # Use asyncio.run to call the async state update *before* calling _run_async_logic (for normal workflow)
        if triggered_by_map_click and state_updates_for_map_click.get("last_intent_payload"):
            try:
                logger.info(f"Updating persistent state for map click with payload: {state_updates_for_map_click}")
                # *** Use asyncio.run here for the async state update ***
                asyncio.run(compiled_workflow.aupdate_state(
                    config=config,
                    values=state_updates_for_map_click,
                    as_node="agent_node",  # Specify that this update should be attributed to agent_node
                ))
                logger.info("Persistent state updated successfully for map click.")
            except Exception as state_update_exc:
                logger.error(f"Error updating persistent state for map click: {state_update_exc}", exc_info=True)
                # Add error message to chat_history?
                error_message = html.Div(f"Error processing map click state: {str(state_update_exc)}", style={"color": "red"})
                chat_history.insert(0, error_message)
                # Prevent further processing? Or try to continue? Let's prevent for now.
                raise PreventUpdate

        # Determine if a dynamic button click triggered the callback
        is_button_click = 'dynamic-button-user-choice' in ctx_trigger
        selection_idx = None
        if is_button_click:
             # Parse the ID of the clicked button to get the selection index/value
             # The ID is a JSON string like '{"index": 2, "option_type": "place", "type": "dynamic-button-user-choice"}'
             try:
                 selection_data = json.loads(ctx_trigger.split(".")[0])
                 selection_idx = selection_data["index"]
                 logger.info(f"Button clicked with selection index: {selection_idx}")
             except (json.JSONDecodeError, KeyError, IndexError) as e:
                 logger.error(f"Error parsing button ID: {ctx_trigger} - {e}")
                 # Handle error - perhaps display a message or prevent update?
                 raise PreventUpdate # Example: stop processing if ID is invalid
        else:
            # Keep existing buttons if this was not a button click
            # (they will be updated by interrupt handling logic if needed)
            pass

        # --- Execute the Asynchronous Workflow Logic ---
        try:
            # Use asyncio.run() to execute the async function `_run_async_logic` from the synchronous callback context.
            # This bridges the sync (Dash callback) and async (LangGraph) worlds.
            final_chat_history, final_app_state, final_map_state, final_place_state, final_buttons = asyncio.run(
                _run_async_logic(
                    initial_chat_history=chat_history, # Pass current history
                    initial_app_state=app_state,       # Pass current app state
                    initial_map_state=map_state,       # Pass current map state (potentially modified by map interaction)
                    initial_place_state=place_state,   # Pass current place state
                    current_user_input=user_input,     # Pass the user's input text (if any)
                    current_thread_id=thread_id,       # Pass the thread ID
                    current_config=config,             # Pass the LangGraph config
                    triggered_by_button=is_button_click, # Indicate if a button was clicked
                    current_selection_idx=selection_idx, # Pass the selected index from the button
                    is_retrigger=is_retrigger_event,    # Indicate if this was a retrigger event
                    current_map_intent_payload=map_intent_payload, # NEW
                    is_triggered_by_map_click=triggered_by_map_click, # NEW
                )
            )


            # Clear the retrigger flag in the returned app state *after* the async logic has successfully run.
            # This prevents immediate re-triggering in a loop.
            if is_retrigger_event and final_app_state:
                final_app_state['retrigger_chat'] = False
                logger.info("Cleared retrigger_chat flag in app_state after retrigger event.")

            logger.info(f"Returning final_app_state with retrigger_chat: {final_app_state.get('retrigger_chat') if final_app_state else 'None'}")
            logger.info(f"Returning final_app_state with show_visualization: {final_app_state.get('show_visualization') if final_app_state else 'None'}")

            # Return the final results obtained from the async function to update the Dash outputs
            return (
                final_chat_history, # Updated chat history
                "",                 # Clear the input field
                final_app_state,    # Updated application state
                final_map_state,    # Updated map state
                final_place_state,  # Updated place state
                None,               # Clear the retrigger signal data output
                final_buttons,      # Render any new buttons from interrupts
                counts_store,       # Pass through counts store (modify in async logic if needed)
                thread_id,          # Persist the thread ID
                clear_add_trigger,  # Clear the add trigger
                clear_remove_trigger, # Clear the remove trigger
            )

        except Exception as e:
            # Catch any exceptions that occur during the execution of the async logic or asyncio.run()
            logger.error(f"Error running async logic within callback: {e}", exc_info=True)
            # Fallback return on error: Show an error message in the chat
            error_message = html.Div(f"Callback Error: {str(e)}", style={"color": "red"})
            chat_history.insert(0, error_message)
            # Try to return current state where possible to avoid losing context entirely
            current_app_state = app_state.copy() if app_state else {}
            # Decide if the retrigger flag should be cleared on error. Maybe not,
            # as the condition causing the retrigger might still need handling.
            # current_app_state['retrigger_chat'] = False
            return (
                chat_history,            # History including the error message
                user_input or "",        # Keep user input in the box on error? Or clear? "" clears.
                current_app_state,       # Return potentially modified app state
                map_state,               # Return potentially modified map state
                place_state,             # Return potentially modified place state
                None,                    # Clear retrigger data
                buttons or [],           # Return existing buttons
                counts_store,            # Return existing counts store
                thread_id,                # Return existing thread ID,
                clear_add_trigger,       # Clear the add trigger
                clear_remove_trigger,    # Clear the remove trigger
            )

    # --- Other Synchronous Helper Callbacks ---

    @app.callback(
        Output("retrigger-chat", "data", allow_duplicate=True), # Output to the CycleBreakerInput
        Input("app-state", "data"),      # Watch the main application state
        # Input("map-state", "data"),    # Optionally watch map state changes directly if needed
        # State("retrigger-chat", "data"), # Get current retrigger data (optional)
        prevent_initial_call=True
    )
    def retrigger_chat_callback(app_state): # Removed map_state and retrigger_chat state if not used
        """
        Watches the app_state for a flag (`retrigger_chat`). If the flag is True,
        it outputs data to the `retrigger-chat` component, which acts as a
        CycleBreakerInput to the main `update_chat` callback, triggering it to run again.
        This is used to resume the workflow after an external action (like map selection)
        has completed and updated the necessary Dash state.
        """
        logger.info(f"retrigger_chat_callback called with app_state retrigger_chat: {app_state.get('retrigger_chat') if app_state else 'None'}")
        if app_state and app_state.get("retrigger_chat"):
            logger.info("TRIGGERING RETRIGGER: app_state['retrigger_chat'] is True.")
            # Outputting any non-None value will trigger the CycleBreakerInput
            # Using a simple boolean or timestamp can be useful.
            return True # Or use something like time.time()
        else:
            # If the flag is not set, prevent the callback from updating its output
            logger.info("NOT triggering retrigger: flag is False or missing")
            raise PreventUpdate

    @app.callback(
        Output("send-button", "n_clicks"), # Output: Increment the send button's click count
        Input("chat-input", "n_submit"),   # Input: Triggered when Enter key is pressed in the chat input
        State("send-button", "n_clicks"),  # State: Get the current click count of the send button
        prevent_initial_call=True
    )
    def trigger_send_on_enter(n_submit, current_n_clicks):
        """
        Allows the user to submit their chat message by pressing Enter in the input field.
        It simulates a click on the 'send-button' by incrementing its n_clicks property.
        """
        if n_submit:
            logger.debug("Enter key pressed in chat input. Simulating send button click.")
            # Increment the click count to trigger the main update_chat callback
            return (current_n_clicks or 0) + 1
        # If not triggered by n_submit, do nothing
        raise PreventUpdate
