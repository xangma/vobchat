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

    print("DEBUG: Registering SSE chat callbacks")
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

        # Inputs
        Input("send-button", "n_clicks"),
        Input("chat-input", "n_submit"),  # Handle Enter key
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        Input("reset-button", "n_clicks"),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),
        Input("app-state", "data"),  # Watch for AI messages

        # States
        State("thread-id", "data"),
        State("app-state", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),

        prevent_initial_call=True
    )
    def update_chat_sse(
        n_clicks, n_submit, button_clicks, reset_n_clicks,
        map_add_payload, map_remove_payload, app_state_input,
        thread_id, app_state, user_input, chat_history
    ):
        """SSE-based chat callback - no more background processing or retriggering"""

        triggered_input = dash.callback_context.triggered[0]
        ctx_trigger = triggered_input["prop_id"] if dash.callback_context.triggered else "No trigger"
        logger.info(f"SSE Chat callback triggered by: {ctx_trigger}")
        logger.info(f"SSE Chat callback inputs: n_clicks={n_clicks}, user_input='{user_input}', thread_id={thread_id}")

        # Debug: Check if this is actually being called
        callback_start_time = time.time()
        print(f"DEBUG: SSE chat callback called with trigger: {ctx_trigger}")
        print(f"DEBUG: User input: '{user_input}'")
        print(f"DEBUG: Map add payload: {map_add_payload}")
        print(f"DEBUG: Map remove payload: {map_remove_payload}")
        print(f"DEBUG: Thread ID passed to callback: {thread_id}")
        print(f"DEBUG: Callback started at {callback_start_time:.3f}")

        # Initialize or get thread ID
        if not thread_id:
            thread_id = str(uuid4())
            logger.info(f"Starting new SSE conversation thread: {thread_id}")
        else:
            logger.info(f"Using existing SSE conversation thread: {thread_id}")

        # Initialize states
        chat_history = chat_history or []
        app_state = app_state or app_state_data.copy()
        
        # Check if this callback was triggered by an AI message
        if "app-state.data" in ctx_trigger and app_state_input:
            pending_ai_message = app_state_input.get("pending_ai_message")
            if pending_ai_message:
                # Add AI message to chat history
                ai_message_div = html.Div(
                    pending_ai_message["props"]["children"],
                    className=pending_ai_message["props"]["className"]
                )
                chat_history.insert(0, ai_message_div)
                
                # Clear the pending message from app state
                app_state_input_copy = app_state_input.copy()
                app_state_input_copy.pop("pending_ai_message", None)
                app_state_input_copy.pop("ai_message_timestamp", None)
                
                return (
                    chat_history,
                    "",  # Clear input
                    app_state_input_copy,
                    thread_id,
                    {"status": "ai_message_added", "thread_id": thread_id},
                    False  # Re-enable send button
                )

        # Handle reset
        if "reset-button" in ctx_trigger:
            logger.info("Reset button clicked - clearing all state")
            return (
                [],  # Clear chat
                "",  # Clear input
                app_state_data.copy(),  # Reset app state
                str(uuid4()),  # New thread ID
                {"status": "reset", "thread_id": thread_id},
                False  # Re-enable send button
            )

        # Prepare workflow input based on trigger
        workflow_input = None
        intent_payload = None

        # Handle text input (both Send button and Enter key)
        if user_input and user_input.strip() and ("send-button" in ctx_trigger or "chat-input" in ctx_trigger):
            # Add user message to chat immediately
            user_message_div = html.Div(user_input, className="speech-bubble user-bubble")
            chat_history.insert(0, user_message_div)

            workflow_input = {"messages": [("user", user_input)]}
            logger.info(f"User text input: {user_input}")

        # Handle dynamic-button (unit/place/theme) clicks
        # NOTE: The actual workflow resumption is handled by JavaScript via /api/workflow/input
        # This callback just needs to acknowledge the button click without triggering duplicate workflow
        elif "dynamic-button-user-choice" in ctx_trigger:
            try:
                selection_data = json.loads(ctx_trigger.split(".")[0])
                selection_idx = selection_data.get("index")
                btn_type = selection_data.get("option_type")

                logger.info(f"Button selection: {selection_idx} (type={btn_type}) - will be handled by JavaScript/API")

                # Just return current state - JavaScript will handle the workflow input via API
                return (
                    chat_history,
                    "",
                    app_state,
                    thread_id,
                    {"status": "button_acknowledged", "thread_id": thread_id},
                    False  # Keep send button enabled for button clicks
                )
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing button selection: {e}")
                raise PreventUpdate

        # Handle map clicks
        elif ctx_trigger == 'map-click-add-trigger.data' and map_add_payload:
            print(f"DEBUG: Processing map click ADD with payload: {map_add_payload}")
            logger.info(f"Map click (Add): {map_add_payload}")

            # Build AddPlace intent
            place_name = map_add_payload.get("name", map_add_payload.get("id", "Unknown Place"))
            unit_type = map_add_payload.get("type")

            intent_payload = {
                "intent": AssistantIntent.ADD_PLACE.value,
                "arguments": {
                    "place": place_name,
                    "unit_type": unit_type,
                    "polygon_id": int(map_add_payload["id"]) if str(map_add_payload["id"]).isdigit() else None
                }
            }
            print(f"DEBUG: Created intent payload: {intent_payload}")

        elif ctx_trigger == 'map-click-remove-trigger.data' and map_remove_payload:
            print(f"DEBUG: Processing map click REMOVE with payload: {map_remove_payload}")
            logger.info(f"Map click (Remove): {map_remove_payload}")

            # Build RemovePlace intent
            place_name = map_remove_payload.get("name", map_remove_payload.get("id", "Unknown Place"))

            intent_payload = {
                "intent": AssistantIntent.REMOVE_PLACE.value,
                "arguments": {
                    "place": place_name,
                    "unit_type": map_remove_payload.get("type")  # Use 'type' instead of 'unit_type'
                }
            }
            print(f"DEBUG: Created REMOVE intent payload: {intent_payload}")

        # If we have workflow input, trigger SSE connection first, then workflow
        print(f"DEBUG: Checking workflow execution - workflow_input: {bool(workflow_input)}, intent_payload: {bool(intent_payload)}")

        if workflow_input or intent_payload:
            if intent_payload:
                workflow_input = {"last_intent_payload": intent_payload}

            print(f"DEBUG: Starting SSE workflow execution")
            print(f"DEBUG: Workflow input being sent: {workflow_input}")
            logger.info(f"Starting SSE workflow for thread {thread_id} with input: {list(workflow_input.keys()) if workflow_input else 'None'}")
            logger.info(f"Workflow input details: {workflow_input}")

            # SIMPLIFIED ARCHITECTURE: All workflow execution goes through SSE trigger path
            # This eliminates race conditions and provides consistent execution flow
            
            callback_end_time = time.time()
            print(f"DEBUG: Callback completed at {callback_end_time:.3f} (took {callback_end_time - callback_start_time:.3f}s)")

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
                True  # Disable send button while workflow is processing
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

        if not sse_status or not thread_id:
            raise PreventUpdate

        status = sse_status.get("status")

        # if status == "button_acknowledged":
        #     # Button was acknowledged - no workflow execution needed
        #     print(f"DEBUG: Button click acknowledged for thread {thread_id}, no workflow execution needed")
        #     raise PreventUpdate
        if status == "connect_and_start_workflow":
            workflow_input = sse_status.get("workflow_input", {})

            # CRITICAL FIX: Use centralized workflow lock manager to prevent concurrent executions
            from vobchat.utils.workflow_lock_manager import workflow_lock_manager
            
            if workflow_lock_manager.is_workflow_running(thread_id):
                print(f"DEBUG: Workflow already running for thread {thread_id}, skipping duplicate execution")
                raise PreventUpdate

            # Use centralized workflow lock manager for proper concurrency control
            try:
                with workflow_lock_manager.acquire_workflow_lock(thread_id) as execution:
                    workflow_start_time = time.time()
                    print(f"DEBUG: Starting workflow execution via SSE trigger for thread {thread_id} (execution: {execution.execution_id})")
                    print(f"DEBUG: Workflow start time: {workflow_start_time:.3f} (trigger took {workflow_start_time - trigger_start_time:.3f}s)")
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
                            print(f"DEBUG: Starting workflow execution for thread {thread_id}")
                            event_count = 0
                            async for event in workflow_adapter.stream_workflow_execution(
                                workflow_input=workflow_input,
                                config=config,
                                thread_id=thread_id
                            ):
                                event_count += 1
                                print(f"DEBUG: Workflow event #{event_count}: {event.get('type')}")
                            print(f"DEBUG: Workflow execution completed successfully with {event_count} events")
                        except Exception as e:
                            print(f"DEBUG: ERROR in workflow execution: {e}")
                            logger.error(f"Workflow execution failed for thread {thread_id}: {e}", exc_info=True)
                    
                    # Run the workflow execution in the background
                    async_manager.submit_task(execute_workflow())
                    print(f"DEBUG: Workflow triggered successfully")

                    trigger_end_time = time.time()
                    print(f"DEBUG: Trigger callback completed at {trigger_end_time:.3f} (total trigger time: {trigger_end_time - trigger_start_time:.3f}s)")

                    return {"processed": True, "thread_id": thread_id, "workflow_started": True}
            except RuntimeError as e:
                if "already running" in str(e):
                    print(f"DEBUG: Concurrent execution prevented for thread {thread_id}: {e}")
                    raise PreventUpdate
                else:
                    print(f"DEBUG: Workflow lock error for thread {thread_id}: {e}")
                    raise

        raise PreventUpdate
