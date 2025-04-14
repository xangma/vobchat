# app/callbacks/chat.py (Revised: Sync callback with asyncio.run)
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
from stores import app_state_data, map_state_data, place_state_data

# Import Dash core components and Bootstrap components
from dash import Input, Output, State, ALL, ctx
import dash_bootstrap_components as dbc
import logging

# Import CycleBreakerInput for managing circular dependencies in callbacks if needed
from dash_extensions.enrich import CycleBreakerInput

# Import LangGraph types for interacting with the workflow
from langgraph.types import interrupt, Command
# Import necessary LangChain message types used within the workflow's state
from langchain_core.messages import AIMessage, HumanMessage, AIMessageChunk

# Set up logging for this module
logger = logging.getLogger(__name__)


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
        # Define the inputs and states for the callback function
        Input("send-button", "n_clicks"),                      # Triggered when the send button is clicked
        # Triggered when any dynamic button (e.g., place/unit/theme choice) is clicked. Uses ALL wildcard.
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        # Input used to break potential circular dependencies or manually retrigger the callback
        CycleBreakerInput("retrigger-chat", "data"),
        Input("reset-button", "n_clicks"),                     # Triggered when the reset button is clicked
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
            is_retrigger # Flag indicating if this run was triggered by the retrigger mechanism
        ):
            """
            Executes the asynchronous LangGraph workflow, handles state synchronization,
            streaming responses, and interrupts.
            """

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
            if is_retrigger and initial_map_state.get('selected_polygons'):
                 logger.info("Retrigger detected: Syncing map_state to LangGraph state before resuming.")
                 try:
                     # Retrieve the latest state snapshot from the LangGraph checkpointer
                     latest_state = await compiled_workflow.aget_state(current_config)
                     if latest_state:
                         # Copy the state values to modify
                         current_workflow_state_values = latest_state.values.copy()
                     else:
                         # Handle defensively if state couldn't be retrieved (shouldn't happen if workflow was interrupted)
                         current_workflow_state_values = {}
                         logger.warning("Could not retrieve workflow state before sync on retrigger.")

                     # Update the retrieved workflow state values with the latest data from the Dash map_state
                     # This assumes the workflow state has keys 'selected_polygons' and 'selected_polygons_unit_types'
                     current_workflow_state_values['selected_polygons'] = initial_map_state['selected_polygons']
                     current_workflow_state_values['selected_polygons_unit_types'] = initial_map_state['selected_polygons_unit_types']
                     # Ensure the interrupt flag in the workflow state is cleared, as we are now resuming
                     # *after* handling the interrupt condition that led to the retrigger (e.g., map selection).
                     current_workflow_state_values['interrupt_state'] = False

                     # Persist the updated state back to the checkpointer
                     await compiled_workflow.aupdate_state(config=current_config, values=current_workflow_state_values)
                     logger.debug("Successfully updated workflow state with map selection data.")

                 except Exception as sync_exc:
                     logger.error(f"Error syncing map state to workflow state on retrigger: {sync_exc}", exc_info=True)
                     # Add an error message to the chat history
                     history.append(html.Div(f"Error syncing map state: {str(sync_exc)}", style={"color": "orange"}))
                     # Decide whether to stop or continue; currently continues.

            # --- Prepare inputs for the LangGraph workflow based on how this function was triggered ---
            workflow_input = None
            # Determine workflow input *after* potential state synchronization
            # Case 1: New user text input submitted via send button
            if current_user_input and current_user_input.strip() and not triggered_by_button and not is_retrigger:
                # Pass the new user message to the workflow. Assumes the workflow state handles 'messages'.
                workflow_input = {"messages": [("user", current_user_input)]}

            # Case 2: A dynamic button was clicked (e.g., place/unit/theme selection)
            elif triggered_by_button:
                 # Get the workflow state *before* applying the button selection
                 state_before_button = await compiled_workflow.aget_state(current_config)
                 # Check if the workflow was indeed waiting for this button click (in an interrupt state)
                 if state_before_button and state_before_button.values.get('interrupt_state'):
                     # Copy the current state values
                     current_values = state_before_button.values.copy()
                     # Add the user's selection index (from the clicked button) to the state
                     current_values["selection_idx"] = current_selection_idx
                     # Mark the interrupt as handled within the workflow state for this resumption path
                     current_values["interrupt_state"] = False
                     # Create a LangGraph Command to jump to the correct node and update the state.
                     # This assumes the interrupted node stored its name in 'current_node'.
                     workflow_input = Command(goto=current_values.get('current_node'), update=current_values)
                     # Conceptually clear the buttons after a click; they will be re-rendered if needed by the next interrupt
                     buttons_to_render_async = []
            
            # Case 3: Retriggered (e.g., after map selection) - workflow_input remains None.
            # The `astream` call will resume from the persisted state, which was just updated
            # in the state synchronization step above.

            # --- Execute the LangGraph workflow using asynchronous streaming ---
            full_ai_response = "" # Accumulator for the complete AI response message
            final_state_values = {} # To store the final workflow state values after streaming
            try:
                # Call the workflow's astream method to get events (messages, state updates, etc.)
                async for msg, metadata in compiled_workflow.astream(
                    workflow_input,    # Pass the prepared input (or None if resuming/retriggering)
                    config=current_config, # Pass the thread configuration
                    stream_mode="messages" # Request message-based streaming events
                ):
                    # Check if the event contains a message chunk from the AI
                    if msg.content and isinstance(msg, AIMessageChunk):
                        message_chunk = msg.content
                        # Append the chunk to the full response
                        full_ai_response += message_chunk
                        # Create a temporary Div to show the accumulating response
                        final_ai_message_div = html.Div(f"{full_ai_response}", className="speech-bubble ai-bubble")
                        # Update the chat display progressively using the set_progress function from Dash background callback
                        # This updates the 'progress' Output ("chat-display")
                        set_props("chat-display", {"children": history + [final_ai_message_div]})

                    # Note: Depending on the LangGraph version and stream_mode, you might get
                    # other types of events here containing state updates. This example primarily
                    # focuses on streaming AIMessageChunks for progressive text display.

            except Exception as stream_exc:
                 logger.error(f"Error during workflow stream: {stream_exc}", exc_info=True)
                 # Add an error message to the chat history
                 history.append(html.Div(f"Streaming Error: {str(stream_exc)}", style={"color": "orange"}))
                 # Allow execution to continue to try and fetch the final state

            # --- Post-Stream State Retrieval and Interrupt Handling ---
            try:
                # After streaming finishes (or errors out), get the final state of the workflow for this thread
                final_state = await compiled_workflow.aget_state(current_config)
                final_state_values = final_state.values if final_state else {} # Extract the state dictionary
                logger.debug({"event": "workflow_state_after_stream", "state_values": final_state_values})

                # If an AI response was streamed, replace the temporary accumulating div with the final complete message div
                if full_ai_response != "":
                    final_ai_message_div = html.Div(f"{full_ai_response}", className="speech-bubble ai-bubble")
                    history.append(final_ai_message_div) # Add the final, complete AI message to the history

                # Reset button rendering list for this turn
                buttons_to_render_async = []

                # Check if the workflow ended in an interrupted state, requiring user input or action
                # LangGraph signals interrupts via the `tasks` attribute of the state or potentially custom flags.
                if final_state and final_state.tasks: # Check if there are pending tasks (interrupts)
                    interrupt_task = final_state.tasks[0] # Assume one interrupt at a time for now
                    # Check for explicit interrupts or custom flags set by workflow nodes
                    if interrupt_task.interrupts or final_state_values.get("interrupt_state") or final_state_values.get("interrupt_data"):
                        logger.debug("Interrupt detected after stream completion.")
                        # Get the data associated with the interrupt
                        # Prioritize explicit interrupts, fall back to data possibly stored in state by the node
                        interrupt_value = interrupt_task.interrupts[0].value if interrupt_task.interrupts else final_state_values.get("interrupt_data")

                        if interrupt_value: # Ensure we have interrupt data
                            logger.debug({"event": "processing_interrupt", "interrupt_value": interrupt_value})
                            
                            # Persist the interrupt data back into the workflow state. This might seem redundant
                            # if it came from 'interrupt_data', but ensures consistency if it came from `interrupt_task.interrupts`.
                            # It also confirms the interrupt is being processed.
                            await compiled_workflow.aupdate_state(
                                config=current_config, values=interrupt_value)

                            interrupt_message = None # Placeholder for any message to show the user for the interrupt

                            # --- Handle different types of interrupts based on the interrupt_value content ---

                            # 1. Multiple Choice Options Interrupt: Render buttons for user selection
                            if interrupt_value.get("options", []):
                                logger.debug("Interrupt with multiple button options")
                                options = interrupt_value.get("options", [])

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
                                prompt_text = interrupt_value.get("message", "Please choose:")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")

                                # Update the Dash app_state to potentially store button info (if needed elsewhere)
                                app_state_async.update({
                                    "button_options": options,
                                })

                            # 2. Map Selection Interrupt: Update map state to show/select units
                            elif interrupt_value.get("current_node") == "select_unit_on_map":
                                logger.debug("Map selection interrupt")
                                # Extract necessary data from the interrupt payload
                                selected_place_g_units = interrupt_value.get("selected_place_g_units", [])
                                selected_place_g_unit_types = interrupt_value.get("selected_place_g_unit_types", [])
                                
                                # Update the Dash map_state_async to trigger map changes
                                # Add the unit IDs and types to the map state's selected polygons list
                                # This logic assumes map_state['selected_polygons'] triggers map updates
                                for i, g_unit in enumerate(selected_place_g_units):
                                    # Avoid adding duplicates if already selected? (Depends on desired map behavior)
                                    if str(g_unit) not in map_state_async.get("selected_polygons", []):
                                        map_state_async.setdefault("selected_polygons", []).append(str(g_unit))
                                        map_state_async.setdefault("selected_polygons_unit_types", []).append(selected_place_g_unit_types[i])

                                # Store unit types and trigger map zoom/update flags
                                map_state_async["unit_types"] = interrupt_value.get("selected_place_g_unit_types", [])
                                map_state_async["zoom_to_selection"] = True # Flag to tell the map component to zoom
                                # Flag indicating a programmatic change is pending (might be used by map callbacks)
                                map_state_async["programmatic_unit_change_pending"] = interrupt_value.get("selected_place_g_unit_types", [])
                                
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

                            # 3. Cube Data Interrupt: Update place state to display visualizations
                            elif interrupt_value.get("cubes"):
                                logger.debug("Cube data interrupt")
                                cubes = interrupt_value.get("cubes", [])
                                # Update the Dash place_state_async with the retrieved cube data
                                place_state_async.update({"cubes": cubes})
                                # Persist the selected cubes back into the workflow state if needed later
                                await compiled_workflow.aupdate_state(
                                    config=current_config, values={"selected_cubes": cubes})

                                # Display the message associated with the cube data
                                prompt_text = interrupt_value.get("message", "Data retrieved.")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")
                                # Update app_state to signal UI to show visualization components
                                app_state_async.update({"show_visualization": True})

                            # 4. Simple Assistant Message Interrupt: Display a message from the workflow
                            elif interrupt_value.get("assistant_message"):
                                logger.debug("Assistant message interrupt")
                                assistant_message = interrupt_value.get("message")
                                interrupt_message = html.Div(f"{assistant_message}", className="speech-bubble ai-bubble")

                            # 5. Text Input Interrupt: Prompt the user for text input (handled by next user message)
                            else:
                                # This case assumes the interrupt requires the user to type something
                                # in the chat input next, rather than click a button or interact with the map.
                                logger.debug("Text input interrupt")
                                prompt_text = interrupt_value.get("message", "Please provide input:")
                                interrupt_message = html.Div(f"{prompt_text}", className="speech-bubble ai-bubble")

                                # This part seems less common for typical interrupts waiting for *new* input.
                                # Usually, the workflow would wait for the *next* HumanMessage.
                                # If `user_input` contained the response *to this interrupt*, this logic
                                # would immediately send it back. This might be intended if the interrupt
                                # asks for clarification on the *current* input, but needs careful design.
                                # if user_input:
                                #     # We already have a user input, so we update the node
                                #     await compiled_workflow.aupdate_state(
                                #         config=config, values={"messages": [("user", user_input)]})

                            # Persist relevant context from the interrupt (like current place index)
                            # back into the workflow state. This is important if the interrupt occurred
                            # mid-way through processing multiple items (like places).
                            if interrupt_value.get("current_place_index") is not None:
                                await compiled_workflow.aupdate_state(
                                    config=current_config,
                                    values={
                                        "current_node": interrupt_value.get('current_node'),
                                        "selection_idx": None, # Clear selection index after processing interrupt context
                                        "current_place_index": interrupt_value.get("current_place_index"),
                                        "selected_place_g_places": interrupt_value.get("selected_place_g_places"),
                                        "selected_place_g_units": interrupt_value.get("selected_place_g_units"),
                                        "selected_place_g_unit_types": interrupt_value.get("selected_place_g_unit_types")
                                    }
                                )

                            # If an interrupt message was created, add it to the chat history
                            if interrupt_message and hasattr(interrupt_message, "children"):
                                if interrupt_message.children is not None and interrupt_message.children != "":
                                    history.append(interrupt_message)
                
                # Return the final computed states and buttons from the async function
                return history, app_state_async, map_state_async, place_state_async, buttons_to_render_async

            except Exception as post_stream_exc:
                logger.error(f"Error processing state/interrupts after stream: {post_stream_exc}", exc_info=True)
                # Add an error message to the chat history
                history.append(html.Div(f"Post-Stream Error: {str(post_stream_exc)}", style={"color": "red"}))
                # Return the current state of things on error to prevent data loss
                return history, app_state_async, map_state_async, place_state_async, []


        # --- Back in the main synchronous callback function `update_chat` ---
        triggered_input = dash.callback_context.triggered[0] # Get info about what triggered the callback
        ctx_trigger = triggered_input["prop_id"] if dash.callback_context.triggered else "No trigger"

        # Determine if the trigger was the retrigger mechanism (either via CycleBreakerInput or the flag in app_state)
        is_retrigger_event = "retrigger-chat.data" in ctx_trigger or (app_state and app_state.get("retrigger_chat"))

        # Basic setup and reset logic (remains synchronous)
        logger.debug({"event": "update_chat_start (sync part)", "trigger": ctx_trigger, "is_retrigger": is_retrigger_event})
        chat_history = chat_history or [] # Initialize chat history if empty

        # Handle Reset Button Click
        if "reset-button" in ctx_trigger:
            logger.info("Reset button clicked. Clearing state.")
            # Reset all relevant states to their initial values
            app_state = app_state_data
            map_state = map_state_data
            place_state = place_state_data
            chat_history = []
            buttons = []
            thread_id = None # Clear thread ID to start a new conversation
            counts_store = {}
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
                thread_id              # Cleared thread ID
            )

       # Add the user's message to the chat history *immediately* if they clicked Send
       # Avoid doing this on retrigger events, as no new user input was provided.
        if user_input and user_input.strip() and "send-button" in ctx_trigger and not is_retrigger_event:
             # Create a div for the user's message
             user_message_div = html.Div(f"You: {user_input}", className="speech-bubble user-bubble")
             chat_history.append(user_message_div)
             # Update the display immediately to show the user's message.
             # Note: Using set_props here might sometimes interfere with background callback progress updates.
             # If issues arise, consider only appending to chat_history and letting the async part handle all display updates.
             set_props("chat-display", {"children": chat_history})

        # Conversation Thread ID and LangGraph Configuration Setup
        if not thread_id:
            # Generate a new unique ID for the conversation thread if one doesn't exist
            thread_id = str(uuid4())
            logger.info(f"Starting new conversation thread: {thread_id}")
        # Configuration object required by LangGraph, associating requests with the specific thread
        config = {"configurable": {"thread_id": thread_id}}

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
                    is_retrigger=is_retrigger_event    # Indicate if this was a retrigger event
                )
            )

            # Clear the retrigger flag in the returned app state *after* the async logic has successfully run.
            # This prevents immediate re-triggering in a loop.
            if is_retrigger_event and final_app_state:
                 final_app_state['retrigger_chat'] = False
                 logger.debug("Cleared retrigger_chat flag in app_state.")


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
            )

        except Exception as e:
            # Catch any exceptions that occur during the execution of the async logic or asyncio.run()
            logger.error(f"Error running async logic within callback: {e}", exc_info=True)
            # Fallback return on error: Show an error message in the chat
            error_message = html.Div(f"Callback Error: {str(e)}", style={"color": "red"})
            chat_history.append(error_message)
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
                thread_id                # Return existing thread ID
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
        if app_state and app_state.get("retrigger_chat"):
            logger.debug("Retriggering chat via retrigger_chat_callback because app_state['retrigger_chat'] is True.")
            # Outputting any non-None value will trigger the CycleBreakerInput
            # Using a simple boolean or timestamp can be useful.
            return True # Or use something like time.time()
        else:
            # If the flag is not set, prevent the callback from updating its output
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