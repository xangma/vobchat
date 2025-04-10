# app/callbacks/chat.py (Revised: Sync callback with asyncio.run)
import json
import asyncio
import nest_asyncio
nest_asyncio.apply()  # Apply nest_asyncio to allow nested event loops
import dash
from dash import html, set_props
from dash.exceptions import PreventUpdate
from uuid import uuid4

# Assuming these are correctly imported
from stores import app_state_data, map_state_data, place_state_data

from dash import Input, Output, State, ALL, ctx

import dash_bootstrap_components as dbc
import logging

from dash_extensions.enrich import CycleBreakerInput

from langgraph.types import interrupt, Command
# Ensure necessary LangChain message types are imported if used in state
from langchain_core.messages import AIMessage, HumanMessage, AIMessageChunk # etc.


logger = logging.getLogger(__name__)


def register_chat_callbacks(app, compiled_workflow, background_callback_manager):

    @app.callback(
        # Outputs remain the same
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value", allow_duplicate=True),
        Output("app-state", "data", allow_duplicate=True),
        Output("map-state", "data", allow_duplicate=True),
        Output("place-state", "data", allow_duplicate=True),
        Output("retrigger-chat", "data", allow_duplicate=True),
        Output("options-container", "children"),
        Output("counts-store", "data"),
        Output("thread-id", "data"),
        # Inputs/States remain the same
        Input("send-button", "n_clicks"),
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        CycleBreakerInput("retrigger-chat", "data"),
        Input("reset-button", "n_clicks"),
        State("thread-id", "data"),
        State("app-state", "data"),
        State("map-state", "data"),
        State("place-state", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),
        State("options-container", "children"),
        State("counts-store", "data"),
        # Background setup
        background=True,
        progress=Output("chat-display", "children"),
        manager=background_callback_manager,
        # You might try re-enabling running state later if this works
        running=[
             (Output("send-button", "disabled"), True, False),
        #      (Output("chat-input", "disabled"), True, False),
        #      (Output({"type": "dynamic-button-user-choice", "index": ALL}, "disabled"), True, False),
         ],
        prevent_initial_call=True
    )
    # Make the function synchronous again
    def update_chat(
        set_progress, # Keep set_progress
        n_clicks,
        button_clicks,
        retrigger_chat,
        reset__n_clicks,
        thread_id,
        app_state,
        map_state,
        place_state,
        user_input,
        chat_history,
        buttons,
        counts_store
    ):

        # --- Define an inner async function for the core logic ---
        async def _run_async_logic(
            initial_chat_history, initial_app_state, initial_map_state, initial_place_state,
            current_user_input, current_thread_id, current_config, triggered_by_button, current_selection_idx
        ):
            nonlocal buttons # Allow modification of buttons defined in outer scope

            # Make copies to avoid modifying outer scope state directly until the end
            history = initial_chat_history
            app_state_async = initial_app_state.copy()
            map_state_async = initial_map_state.copy()
            place_state_async = initial_place_state.copy()
            
            # Prepare inputs based on trigger
            workflow_input = None
            if current_user_input and current_user_input.strip() and not triggered_by_button:
                workflow_input = {"messages": [("user", current_user_input)]}
                # set_progress(history) # Show user message right away
            elif triggered_by_button:
                 # Get state *before* applying button selection
                 state_before_button = await compiled_workflow.aget_state(current_config)
                 if state_before_button and state_before_button.values.get('interrupt_state'):
                     current_values = state_before_button.values.copy()
                     current_values["selection_idx"] = current_selection_idx
                     workflow_input = Command(goto=current_values.get('current_node'), update=current_values)
                     buttons = [] # Clear buttons after click conceptually

            # --- Streaming ---
            full_ai_response = ""
            final_state_values = {}
            try:
                async for msg, metadata in compiled_workflow.astream(
                    workflow_input,
                    config=current_config,
                    stream_mode="messages"
                ):
                    if msg.content and isinstance(msg, AIMessageChunk): 
                        message = msg.content
                        # Append the AI message to the history
                        full_ai_response += message
                        # create div with full chat history and full_ai_response
                        final_ai_message_div = html.Div(f"AI: {full_ai_response}", className="mb-2 text-primary")
                        # Update the chat display with the new message
                        set_props("chat-display", {"children": history + [final_ai_message_div]}) # Update with the new message

                    # Capture the latest state values if available in the event
                    # This depends heavily on your graph structure. Adjust as needed.
                    # Example: if 'some_state_key' in event: final_state_values = event

            except Exception as stream_exc:
                 logger.error(f"Error during workflow stream: {stream_exc}", exc_info=True)
                 history.append(html.Div(f"Streaming Error: {str(stream_exc)}", style={"color": "orange"}))
                 # Fall through to potentially get final state

            # --- Post-Stream State & Interrupt Handling ---
            try:
                final_state = await compiled_workflow.aget_state(current_config)
                final_state_values = final_state.values if final_state else {}
                logger.debug({"event": "workflow_state_after_stream", "state_values": final_state_values})

                # Replace placeholder in the final history
                final_ai_message_div = html.Div(f"AI: {full_ai_response}", className="mb-2 text-primary")
                history.append(final_ai_message_div) # Add final message

                buttons_to_render_async = [] # Reset buttons

                # Check and handle interrupts based on final_state_values
                if final_state and final_state.tasks:
                    interrupt_task = final_state.tasks[0]
                    if interrupt_task.interrupts or final_state_values.get("interrupt_state") or final_state_values.get("interrupt_data"):
                        logger.debug("Interrupt detected after stream completion.")
                        interrupt_value = interrupt_task.interrupts[0].value if interrupt_task.interrupts else final_state_values.get("interrupt_data")

                        if interrupt_value:
                            logger.debug({"event": "processing_interrupt", "interrupt_value": interrupt_value})
                            # update the workflow state with the interrupt data
                            compiled_workflow.update_state(
                                config=config, values=interrupt_value)
                            new_history = []
                            # Multiple choice "options" interrupt
                            if interrupt_value.get("options", []):
                                logger.debug("Interrupt with multiple button options")
                                options = interrupt_value.get("options", [])

                                # The node wants the user to pick from a list of options
                                buttons = [
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
                                        style={
                                            '--unit-color': opt["color"],
                                            'borderColor': opt["color"],
                                            'backgroundColor': 'white',
                                            # Use a dark/grey color for unselected text.
                                            'color': '#333',
                                            'transition': 'background-color 0.3s, color 0.3s'
                                        }
                                    )
                                    for opt in options
                                ]
                                
                                prompt_text = interrupt_value.get(
                                    "message", "Please choose:")
                                interrupt_message = html.Div(
                                    f"AI: {prompt_text}", className="mb-2 text-primary")

                                # Mark that we are waiting for user selection
                                app_state.update({
                                    "button_options": options,
                                })
                                new_history = history[:] + [interrupt_message]
                                


                            # Map selection
                            if interrupt_value.get("current_node") == "select_unit_on_map":
                                logger.debug("Map selection interrupt")
                                selected_place_g_units = interrupt_value["selected_place_g_units"]
                                selected_place_g_unit_types = interrupt_value["selected_place_g_unit_types"]
                                for i, g_unit in enumerate(selected_place_g_units):
                                    if g_unit not in map_state["selected_polygons"]:
                                        map_state["selected_polygons"].append(str(g_unit))
                                        map_state["selected_polygons_unit_types"].append(selected_place_g_unit_types[i])
                                        map_state["unit_types"] = interrupt_value["selected_place_g_unit_types"]
                                        # Add the zoom to selection flag to trigger zooming to the polygons
                                        map_state["zoom_to_selection"] = True
                                        map_state["programmatic_unit_change_pending"] = interrupt_value["selected_place_g_unit_types"]

                                app_state.update({
                                    "button_options": [],
                                    "retrigger_chat": True
                                })
                                retrigger_chat = None
                                buttons = []

                            elif interrupt_value.get("cubes"):
                                logger.debug("Cube selection interrupt")
                                cubes = interrupt_value.get("cubes", [])
                                place_state.update({"cubes": cubes})
                                compiled_workflow.update_state(
                                    config=config, values={"selected_cubes": cubes})

                                prompt_text = interrupt_value.get(
                                    "message")
                                interrupt_message = html.Div(
                                    f"AI: {prompt_text}", className="mb-2 text-primary")
                                new_history = history[:] + [interrupt_message]
                                app_state.update({"show_visualization": True})

                            elif interrupt_value.get("assistant_message"):
                                logger.debug("Assistant message interrupt")
                                assistant_message = interrupt_value.get(
                                    "message")
                                interrupt_message = html.Div(
                                    f"AI: {assistant_message}", className="mb-2 text-primary")
                                new_history = history[:] + [interrupt_message]
                            
                            # Otherwise it's a "text input" interrupt
                            else:
                                logger.debug("Text input interrupt")
                                prompt_text = interrupt_value.get(
                                    "message", "Please provide input.")
                                interrupt_message = html.Div(
                                    f"AI: {prompt_text}", className="mb-2 text-primary")

                                if user_input:
                                    # We already have a user input, so we update the node
                                    compiled_workflow.update_state(
                                        config=config, values={"messages": [("user", user_input)]})
                                else:
                                    # We need to prompt the user
                                    new_history = history[:] + [interrupt_message]

                            if interrupt_value.get("current_place_index") is not None:
                                compiled_workflow.update_state(
                                    config=config, values={"current_node": interrupt_value['current_node'], "selection_idx": None, "current_place_index": interrupt_value["current_place_index"], "selected_place_g_places": interrupt_value.get("selected_place_g_places"), "selected_place_g_units": interrupt_value.get("selected_place_g_units"), "selected_place_g_unit_types": interrupt_value.get("selected_place_g_unit_types")})
                        
                            if new_history:
                                history = new_history
            
                # Return the final computed states from the async function
                return history, app_state_async, map_state_async, place_state_async, buttons_to_render_async

            except Exception as post_stream_exc:
                logger.error(f"Error processing state/interrupts after stream: {post_stream_exc}", exc_info=True)
                history.append(html.Div(f"Post-Stream Error: {str(post_stream_exc)}", style={"color": "red"}))
                # Return current state on error
                return history, app_state_async, map_state_async, place_state_async, []


        # --- Back in the main synchronous callback function ---
        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"] if ctx.triggered else "No trigger"

        # Basic setup and reset logic (remains sync)
        logger.debug({"event": "update_chat_start (sync part)", "trigger": ctx_trigger})
        chat_history = chat_history or []

        if "reset-button" in ctx_trigger:
            app_state = app_state_data
            map_state = map_state_data
            place_state = place_state_data
            chat_history = []
            buttons = []
            thread_id = None
            counts_store = {}
            return (
                chat_history,
                "",
                app_state,
                map_state,
                place_state,
                retrigger_chat,
                buttons,
                counts_store,
                thread_id
            )


        # Add user message immediately if sent
        if user_input and user_input.strip() and "send-button" in ctx_trigger:
            chat_history.append(html.Div(f"You: {user_input}", className="mb-2"))
            set_props("chat-display", {"children": list(chat_history)}) # Show user message right away

        # Thread ID and Config setup
        if not thread_id:
            thread_id = str(uuid4())
        config = {"configurable": {"thread_id": thread_id}}

        # Determine button click state
        is_button_click = 'dynamic-button-user-choice' in ctx_trigger
        selection_idx = None
        if is_button_click:
             selection_data = json.loads(ctx_trigger.split(".")[0])
             selection_idx = selection_data["index"]


        # --- Run the async logic ---
        try:
            # Use asyncio.run to execute the inner async function
            final_chat_history, final_app_state, final_map_state, final_place_state, final_buttons = asyncio.run(
                _run_async_logic(
                    initial_chat_history=chat_history,
                    initial_app_state=app_state,
                    initial_map_state=map_state,
                    initial_place_state=place_state,
                    current_user_input=user_input,
                    current_thread_id=thread_id,
                    current_config=config,
                    triggered_by_button=is_button_click,
                    current_selection_idx=selection_idx
                )
            )

            # Return the results obtained from the async function
            return (
                final_chat_history,
                "", # Clear input
                final_app_state,
                final_map_state,
                final_place_state,
                None, # Clear retrigger
                final_buttons,
                counts_store, # Pass through
                thread_id,
            )

        except Exception as e:
            logger.error(f"Error running async logic within callback: {e}", exc_info=True)
            # Fallback return on error
            chat_history.append(html.Div(f"Callback Error: {str(e)}", style={"color": "red"}))
            return chat_history, user_input or "", app_state, map_state, place_state, None, buttons or [], counts_store, thread_id


    # --- Other Sync Callbacks (retrigger_chat_callback, trigger_send_on_enter) ---
    # These should remain synchronous as before
    @app.callback(
        Output("retrigger-chat", "data", allow_duplicate=True),
        Input("app-state", "data"),
        Input("map-state", "data"),
        State("retrigger-chat", "data"),
        prevent_initial_call=True
    )
    def retrigger_chat_callback(app_state, map_state, retrigger_chat):
        if app_state and app_state.get("retrigger_chat"):
            logger.debug("Retriggering chat")
            return True
        else:
            raise PreventUpdate

    @app.callback(
        Output("send-button", "n_clicks"),
        Input("chat-input", "n_submit"),
        State("send-button", "n_clicks"),
        prevent_initial_call=True
    )
    def trigger_send_on_enter(n_submit, current_n_clicks):
        if n_submit:
            return (current_n_clicks or 0) + 1
        raise PreventUpdate