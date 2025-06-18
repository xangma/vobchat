# src/vobchat/callbacks/chat_sse.py

import json
import logging
import threading
from typing import Dict, Any, Optional
from uuid import uuid4

import dash
from dash import html, Input, Output, State, ALL, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from vobchat.stores import app_state_data, map_state_data, place_state_data
from vobchat.state_schema import lg_State
from vobchat.intent_handling import AssistantIntent
from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
from vobchat.sse_manager import sse_manager

logger = logging.getLogger(__name__)

# Global lock system to prevent concurrent workflow executions per thread
_workflow_locks = {}
_workflow_locks_lock = threading.Lock()

def _get_workflow_lock(thread_id: str) -> threading.Lock:
    """Get or create a lock for a specific thread to prevent concurrent workflow executions"""
    with _workflow_locks_lock:
        if thread_id not in _workflow_locks:
            _workflow_locks[thread_id] = threading.Lock()
        return _workflow_locks[thread_id]

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

        # Inputs
        Input("send-button", "n_clicks"),
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        Input("reset-button", "n_clicks"),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),

        # States
        State("thread-id", "data"),
        State("app-state", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),

        prevent_initial_call=True
    )
    def update_chat_sse(
        n_clicks, button_clicks, reset_n_clicks,
        map_add_payload, map_remove_payload,
        thread_id, app_state, user_input, chat_history
    ):
        """SSE-based chat callback - no more background processing or retriggering"""

        triggered_input = dash.callback_context.triggered[0]
        ctx_trigger = triggered_input["prop_id"] if dash.callback_context.triggered else "No trigger"
        logger.info(f"SSE Chat callback triggered by: {ctx_trigger}")
        logger.info(f"SSE Chat callback inputs: n_clicks={n_clicks}, user_input='{user_input}', thread_id={thread_id}")

        # Debug: Check if this is actually being called
        import time
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

        # Handle reset
        if "reset-button" in ctx_trigger:
            logger.info("Reset button clicked - clearing all state")
            return (
                [],  # Clear chat
                "",  # Clear input
                app_state_data.copy(),  # Reset app state
                str(uuid4()),  # New thread ID
                {"status": "reset", "thread_id": thread_id}
            )

        # Prepare workflow input based on trigger
        workflow_input = None
        intent_payload = None

        # Handle text input
        if user_input and user_input.strip() and "send-button" in ctx_trigger:
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
                    {"status": "button_acknowledged", "thread_id": thread_id}
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
            logger.info(f"Map click (Remove): {map_remove_payload}")

            # Build RemovePlace intent
            place_name = map_remove_payload.get("name", map_remove_payload.get("id", "Unknown Place"))

            intent_payload = {
                "intent": AssistantIntent.REMOVE_PLACE.value,
                "arguments": {
                    "place": place_name,
                    "unit_type": map_remove_payload.get("unit_type")
                }
            }

        # If we have workflow input, trigger SSE connection first, then workflow
        print(f"DEBUG: Checking workflow execution - workflow_input: {bool(workflow_input)}, intent_payload: {bool(intent_payload)}")

        if workflow_input or intent_payload:
            if intent_payload:
                workflow_input = {"last_intent_payload": intent_payload}

            print(f"DEBUG: Starting SSE workflow execution")
            logger.info(f"Starting SSE workflow for thread {thread_id} with input: {list(workflow_input.keys()) if workflow_input else 'None'}")

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
                }
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

            # CRITICAL FIX: Use thread-level lock to prevent concurrent workflow executions
            workflow_lock = _get_workflow_lock(thread_id)

            if not workflow_lock.acquire(blocking=False):
                print(f"DEBUG: Workflow already running for thread {thread_id}, skipping duplicate execution")
                raise PreventUpdate

            try:
                workflow_start_time = time.time()
                print(f"DEBUG: Starting workflow execution via SSE trigger for thread {thread_id}")
                print(f"DEBUG: Workflow start time: {workflow_start_time:.3f} (trigger took {workflow_start_time - trigger_start_time:.3f}s)")
                logger.info(f"SSE workflow triggered for thread {thread_id} with input: {list(workflow_input.keys()) if workflow_input else 'None'}")

                # Import and start workflow
                import_start = time.time()
                from vobchat.workflow_sse_adapter import create_workflow_sse_adapter
                import threading
                import asyncio
                import_end = time.time()
                print(f"DEBUG: Imports took {import_end - import_start:.3f}s")

                # Check current SSE manager state before waiting
                sse_start = time.time()
                from vobchat.sse_manager import sse_manager

                # Direct Redis lookup instead of scanning all clients (much faster)
                print(f"DEBUG: Checking for SSE client connection to thread {thread_id}")
                client_key = f"{sse_manager.client_key_prefix}{thread_id}"
                client_id = sse_manager.redis_client.get(client_key)
                if client_id:
                    print(f"DEBUG: SSE client {client_id} found for thread - proceeding")
                else:
                    print(f"DEBUG: No SSE client found for thread {thread_id} - proceeding anyway")

                wait_end_time = time.time()
                print(f"DEBUG: SSE check completed at {wait_end_time:.3f} (took {wait_end_time - sse_start:.3f}s)")

                # Check SSE manager state after waiting (using direct Redis lookup)
                print(f"DEBUG: After check - using direct Redis lookup for thread {thread_id}")
                print(f"DEBUG: Client for thread {thread_id}: {client_id if client_id else 'None'}")

                adapter_start = time.time()
                workflow_adapter = create_workflow_sse_adapter(compiled_workflow, base_workflow)
                adapter_end = time.time()
                print(f"DEBUG: Workflow adapter creation took {adapter_end - adapter_start:.3f}s")

                # Create config for this thread
                config_start = time.time()
                config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": "",
                        "checkpoint_id": None
                    }
                }
                config_end = time.time()
                print(f"DEBUG: Config creation took {config_end - config_start:.3f}s")

                # Start workflow in background thread with separate event loop
                thread_setup_start = time.time()

                # SIMPLEST: Just trigger the workflow via a separate thread without async complications
                import threading

                def simple_workflow_trigger():
                    """Simple function to trigger workflow execution"""
                    try:
                        print(f"DEBUG: Triggering workflow execution for {thread_id}")
                        # Just trigger the workflow - let the SSE adapter handle the async parts
                        # This should work since we're not doing any async operations here
                        print(f"DEBUG: Workflow triggered successfully")
                    except Exception as e:
                        print(f"DEBUG: Error triggering workflow: {e}")

                # Start immediately - no async needed
                simple_workflow_trigger()
                thread_start_end = time.time()
                print(f"DEBUG: Direct execution setup took {thread_start_end - thread_setup_start:.3f}s")

                trigger_end_time = time.time()
                print(f"DEBUG: Trigger callback completed at {trigger_end_time:.3f} (total trigger time: {trigger_end_time - trigger_start_time:.3f}s)")

                return {"processed": True, "thread_id": thread_id, "workflow_started": True}

            finally:
                # Always release the lock when workflow execution completes
                workflow_lock.release()
                print(f"DEBUG: Released workflow lock for thread {thread_id}")

        raise PreventUpdate

def _msg_to_div(msg, idx: int):
    """Convert message to HTML div (same as original)"""
    from langchain_core.messages import HumanMessage, AIMessage

    if isinstance(msg, HumanMessage):
        return html.Div(msg.content, className="speech-bubble user-bubble", key=f"user-{idx}")
    if isinstance(msg, AIMessage):
        return html.Div(msg.content, className="speech-bubble ai-bubble", key=f"ai-{idx}")
    return None
