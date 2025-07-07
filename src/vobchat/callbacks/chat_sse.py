# src/vobchat/callbacks/chat_sse.py

import logging
import threading
from typing import Dict, Any, Optional
from uuid import uuid4
import time
import json

import dash
from dash import html, Input, Output, State, ALL
from dash.exceptions import PreventUpdate

from vobchat.stores import app_state_data
from vobchat.intent_handling import AssistantIntent
from vobchat.workflow_sse_adapter import create_workflow_sse_adapter

logger = logging.getLogger(__name__)

# Note: Workflow locking is now handled by the centralized workflow_lock_manager

def register_sse_chat_callbacks(app, compiled_workflow, base_workflow=None):
    """Register SSE-based chat callbacks that replace the retriggering mechanism"""

    logger.debug("DEBUG: Registering SSE chat callbacks")
    logger.info("Registering SSE chat callbacks")

    # Create workflow SSE adapter
    workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)

    @app.callback(
        # Simplified outputs - no more retrigger management
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value", allow_duplicate=True),
        Output("app-state", "data", allow_duplicate=True),
        Output("thread-id", "data", allow_duplicate=True),
        Output("sse-connection-status", "data"),  # New output for SSE status
        Output("send-button", "disabled", allow_duplicate=True),  # Control send button state
        Output("map-click-add-trigger", "data", allow_duplicate=True),  # Clear map click triggers
        Output("map-click-remove-trigger", "data", allow_duplicate=True),  # Clear map click triggers

        # Inputs
        Input("send-button", "n_clicks"),
        Input("chat-input", "n_submit"),  # Handle Enter key
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        Input("reset-button", "n_clicks"),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),
        Input("sse-event-processor", "data"),  # Handle AI messages from SSE
        # REMOVED: Input("app-state", "data") - causes recursion loop with app-state SSE callback

        # States
        State("thread-id", "data"),
        State("app-state", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),

        prevent_initial_call=True
    )
    def update_chat_sse(
        n_clicks, n_submit, button_clicks, reset_n_clicks,
        map_add_payload, map_remove_payload, sse_event_data,
        thread_id, app_state, user_input, chat_history
    ):
        """SSE-based chat callback - no more background processing or retriggering"""

        triggered_input = dash.callback_context.triggered[0]
        ctx_trigger = triggered_input["prop_id"] if dash.callback_context.triggered else "No trigger"
        logger.info(f"SSE Chat callback triggered by: {ctx_trigger}")
        logger.info(f"SSE Chat callback inputs: n_clicks={n_clicks}, user_input='{user_input}', thread_id={thread_id}")

        # Debug: Check if this is actually being called
        callback_start_time = time.time()
        logger.debug(f"SSE chat callback called with trigger: {ctx_trigger}")
        logger.debug(f"User input: '{user_input}'")
        logger.debug(f"Map add payload: {map_add_payload}")
        logger.debug(f"Map remove payload: {map_remove_payload}")
        logger.debug(f"Thread ID passed to callback: {thread_id}")
        logger.debug(f"Callback started at {callback_start_time:.3f}")

        # Initialize or get thread ID
        if not thread_id:
            # CRITICAL FIX: Check if SSE client already has a thread ID before creating new one
            # This prevents thread ID mismatches when polygon clicks happen
            # The SSE client might already be connected with a thread ID
            logger.debug(f" No thread ID in store, checking for existing SSE connection")

            # For now, create a new thread ID but log a warning
            # In production, we should get this from the SSE client's current connection
            thread_id = str(uuid4())
            logger.warning(f"Creating new thread ID {thread_id} - SSE client may be on different thread!")
            logger.info(f"Starting new SSE conversation thread: {thread_id}")
        else:
            logger.info(f"Using existing SSE conversation thread: {thread_id}")

        # Initialize states
        chat_history = chat_history or []
        app_state = app_state or app_state_data.copy()

        # REMOVED: app-state.data handling since we removed that input to prevent recursion loop

        # Handle reset
        if "reset-button" in ctx_trigger:
            logger.info("Reset button clicked - clearing all state")
            return (
                [],  # Clear chat
                "",  # Clear input
                app_state_data.copy(),  # Reset app state
                str(uuid4()),  # New thread ID
                {"status": "reset", "thread_id": thread_id},
                False,  # Re-enable send button
                None,  # Clear map add trigger
                None   # Clear map remove trigger
            )

        # Prepare workflow input based on trigger
        workflow_input = None
        intent_payload = None

        # Handle SSE AI messages
        if "sse-event-processor" in ctx_trigger and sse_event_data and sse_event_data.get('type') == 'ai_message':
            content = sse_event_data.get('content')
            if content:
                logger.info(f"Adding AI message to chat: {content}")

                # Create AI message div with error styling if needed
                className = "speech-bubble ai-bubble"
                style = {}
                if sse_event_data.get('isError'):
                    style = {"color": "red"}

                ai_message_div = html.Div(content, className=className, style=style)
                chat_history.insert(0, ai_message_div)

                return (
                    chat_history,
                    "",  # Clear input
                    app_state,
                    thread_id,
                    {"status": "ai_message_added", "thread_id": thread_id},
                    False,  # Re-enable send button
                    None,  # Clear map add trigger
                    None   # Clear map remove trigger
                )

        # Handle text input (both Send button and Enter key)
        elif user_input and user_input.strip() and ("send-button" in ctx_trigger or "chat-input" in ctx_trigger):
            # Add user message to chat immediately
            user_message_div = html.Div(user_input, className="speech-bubble user-bubble")
            chat_history.insert(0, user_message_div)

            workflow_input = {"messages": [("user", user_input)]}
            logger.info(f"User text input: {user_input}")

        # Handle dynamic-button (unit/place/theme) clicks
        # SIMPLIFIED: Button clicks start fresh workflows with selection data
        elif "dynamic-button-user-choice" in ctx_trigger:
            try:
                selection_data = json.loads(ctx_trigger.split(".")[0])
                selection_idx = selection_data.get("index")
                btn_type = selection_data.get("option_type")

                logger.info(f"Button selection: {selection_idx} (type={btn_type}) - starting fresh workflow")

                # Create workflow input with button selection
                workflow_input = {
                    "selection_idx": selection_idx,
                    "button_type": btn_type
                }
                
                # This will trigger a fresh workflow execution via SSE
                # No more complex interrupt resumption
                return (
                    chat_history,
                    "",
                    app_state,
                    thread_id,
                    {
                        "status": "connect_and_start_workflow",
                        "thread_id": thread_id,
                        "workflow_input": workflow_input,
                        "connect_sse": True,
                        "clear_existing": True  # Signal to clear any existing execution
                    },
                    True,  # Disable send button while workflow is processing
                    None,  # Clear map add trigger
                    None   # Clear map remove trigger
                )
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing button selection: {e}")
                raise PreventUpdate

        # Handle map clicks with simple API (bypass workflow for direct polygon operations)
        elif ctx_trigger == 'map-click-add-trigger.data' and map_add_payload:
            logger.debug(f" Processing map click ADD with payload: {map_add_payload}")
            logger.info(f"Map click (Add): {map_add_payload}")

            # Always trigger workflow for polygon selection - let the workflow decide what to do
            logger.info("Polygon selection - triggering workflow")
            intent_payload = {
                "intent": AssistantIntent.ADD_PLACE.value,
                "arguments": {
                    "place": map_add_payload.get("name", map_add_payload.get("id", "Unknown Place")),
                    "unit_type": map_add_payload.get("type"),
                    "polygon_id": int(map_add_payload["id"]) if str(map_add_payload["id"]).isdigit() else None
                }
            }

        elif ctx_trigger == 'map-click-remove-trigger.data' and map_remove_payload:
            logger.debug(f" Processing map click REMOVE with payload: {map_remove_payload}")
            logger.info(f"Map click (Remove): {map_remove_payload}")

            # For remove, we need to trigger the workflow to handle any state cleanup
            intent_payload = {
                "intent": AssistantIntent.REMOVE_PLACE.value,
                "arguments": {
                    "place": map_remove_payload.get("name", map_remove_payload.get("id", "Unknown Place")),
                    "unit_type": map_remove_payload.get("type"),
                    "polygon_id": int(map_remove_payload["id"]) if str(map_remove_payload["id"]).isdigit() else None
                }
            }

        # If we have workflow input, trigger SSE connection first, then workflow
        logger.debug(f" Checking workflow execution - workflow_input: {bool(workflow_input)}, intent_payload: {bool(intent_payload)}")

        if workflow_input or intent_payload:
            if intent_payload:
                workflow_input = {"last_intent_payload": intent_payload}

            logger.debug(f" Starting SSE workflow execution")
            logger.debug(f" Workflow input being sent: {workflow_input}")
            logger.info(f"Starting SSE workflow for thread {thread_id} with input: {list(workflow_input.keys()) if workflow_input else 'None'}")
            logger.info(f"Workflow input details: {workflow_input}")

            # SIMPLIFIED ARCHITECTURE: All workflow execution goes through SSE trigger path
            # This eliminates race conditions and provides consistent execution flow

            callback_end_time = time.time()
            logger.debug(f" Callback completed at {callback_end_time:.3f} (took {callback_end_time - callback_start_time:.3f}s)")

            return (
                chat_history,
                "",  # Clear input
                app_state,
                thread_id,
                {
                    "status": "connect_and_start_workflow",
                    "thread_id": thread_id,
                    "workflow_input": workflow_input,
                    "connect_sse": True  # Signal to trigger SSE connection
                },
                True,  # Disable send button while workflow is processing
                None,  # Clear map add trigger - prevents stale payload accumulation
                None   # Clear map remove trigger - prevents stale payload accumulation
            )

        # No updates needed
        raise PreventUpdate

    @app.callback(
        Output("sse-event-processor", "data"),
        Input("sse-connection-status", "data"),
        State("thread-id", "data"),
        prevent_initial_call=True
    )
    def trigger_sse_workflow(sse_status, thread_id):
        """Trigger SSE workflow execution when needed"""
        import time
        trigger_start_time = time.time()

        logger.info(f"trigger_sse_workflow called with sse_status: {sse_status}, thread_id: {thread_id}")

        if not sse_status or not thread_id:
            logger.debug(f"trigger_sse_workflow: Missing sse_status or thread_id, preventing update")
            raise PreventUpdate

        status = sse_status.get("status")
        logger.info(f"trigger_sse_workflow: Processing status = {status}")

        if status == "button_acknowledged":
            # Button was acknowledged - no workflow execution needed
            logger.debug(f"Button click acknowledged for thread {thread_id}, no workflow execution needed")
            raise PreventUpdate
        # REMOVED: Complex button selection resumption logic
        # All button clicks now go through the standard workflow execution path
        elif status == "connect_and_start_workflow":
            workflow_input = sse_status.get("workflow_input", {})

            # CRITICAL FIX: Use centralized workflow lock manager to prevent concurrent executions
            from vobchat.utils.workflow_lock_manager import workflow_lock_manager

            # CRITICAL FIX: Use the thread ID from SSE status which should match the SSE connection
            # The thread_id parameter might be stale or None
            workflow_thread_id = sse_status.get("thread_id")
            if not workflow_thread_id:
                logger.debug(f" ERROR - No thread ID in SSE status, cannot execute workflow")
                raise PreventUpdate

            logger.debug(f" Using thread ID from SSE status for workflow execution: {workflow_thread_id}")

            # Replace the local thread_id variable to use the correct one
            thread_id = workflow_thread_id

            if workflow_lock_manager.is_workflow_running(thread_id):
                logger.debug(f" Workflow already running for thread {thread_id}, skipping duplicate execution")
                raise PreventUpdate

            # Check if we need to clear existing execution first
            clear_existing = sse_status.get("clear_existing", False)
            if clear_existing:
                logger.info(f"Clearing any existing workflow execution for thread {thread_id}")
                workflow_lock_manager.force_release_lock(thread_id)
                
                # Add small delay to ensure cleanup
                import time
                time.sleep(0.1)

            # Use centralized workflow lock manager for proper concurrency control
            try:
                with workflow_lock_manager.acquire_workflow_lock(thread_id) as execution:
                    workflow_start_time = time.time()
                    logger.debug(f" Starting workflow execution via SSE trigger for thread {thread_id} (execution: {execution.execution_id})")
                    logger.debug(f" Workflow start time: {workflow_start_time:.3f} (trigger took {workflow_start_time - trigger_start_time:.3f}s)")
                    logger.info(f"SSE workflow triggered for thread {thread_id} with input: {list(workflow_input.keys()) if workflow_input else 'None'}")

                    # Create workflow adapter
                    from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
                    workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)

                    # Create config for this thread
                    config = {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": "",
                            "checkpoint_id": None
                        }
                    }

                    # Execute workflow using the async manager
                    from vobchat.utils.async_manager import async_manager

                    async def execute_workflow():
                        try:
                            logger.debug(f" Starting workflow execution for thread {thread_id}")
                            event_count = 0
                            async for event in workflow_adapter.stream_workflow_execution(
                                workflow_input=workflow_input,
                                config=config,
                                thread_id=thread_id
                            ):
                                event_count += 1
                                logger.debug(f" Workflow event #{event_count}: {event.get('type')}")
                            logger.debug(f" Workflow execution completed successfully with {event_count} events")
                        except Exception as e:
                            logger.debug(f" ERROR in workflow execution: {e}")
                            logger.error(f"Workflow execution failed for thread {thread_id}: {e}", exc_info=True)

                    # Run the workflow execution in the background
                    async_manager.submit_task(execute_workflow())
                    logger.debug(f" Workflow triggered successfully")

                    trigger_end_time = time.time()
                    logger.debug(f" Trigger callback completed at {trigger_end_time:.3f} (total trigger time: {trigger_end_time - trigger_start_time:.3f}s)")

                    return {"processed": True, "thread_id": thread_id, "workflow_started": True}
            except RuntimeError as e:
                if "already running" in str(e):
                    logger.debug(f" Concurrent execution prevented for thread {thread_id}: {e}")
                    raise PreventUpdate
                else:
                    logger.debug(f" Workflow lock error for thread {thread_id}: {e}")
                    raise

        raise PreventUpdate
