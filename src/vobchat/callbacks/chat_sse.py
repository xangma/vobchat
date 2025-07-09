# Simple Chat Callback - Clean rewrite
# Single responsibility: Handle user input and trigger workflows

import logging
import time
from uuid import uuid4
from typing import Dict, Any, Optional

import dash
from dash import html, Input, Output, State, no_update
from dash.exceptions import PreventUpdate

from vobchat.stores import app_state_data
from vobchat.intent_handling import AssistantIntent
from vobchat.utils.async_manager import async_manager
from vobchat.sse_manager import get_sse_manager

logger = logging.getLogger(__name__)

simple_sse_manager = get_sse_manager()

def register_simple_chat_callbacks(app, compiled_workflow):
    """Register simplified chat callbacks that work with clean SSE architecture"""

    logger.info("Registering simplified chat callbacks")

    @app.callback(
        Output("chat-input", "value"),
        Output("thread-id", "data", allow_duplicate=True),
        Output("send-button", "disabled"),
        Output("chat-display", "children", allow_duplicate=True),
        Output("sse-connection-status", "data", allow_duplicate=True),

        # Inputs
        Input("send-button", "n_clicks"),
        Input("chat-input", "n_submit"),
        Input("reset-button", "n_clicks"),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),

        # States
        State("thread-id", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),

        prevent_initial_call=True
    )
    def handle_user_input(
        n_clicks, n_submit, reset_clicks,
        map_add_payload, map_remove_payload,
        thread_id, user_input, chat_display
    ):
        """Simple chat handler - determines what triggered and starts appropriate workflow"""

        ctx = dash.callback_context
        if not ctx.triggered:
            raise PreventUpdate

        trigger = ctx.triggered[0]["prop_id"]
        logger.info(f"Chat triggered by: {trigger}")

        # Initialize thread ID if needed
        if not thread_id:
            thread_id = str(uuid4())
            logger.info(f"Generated new thread ID: {thread_id}")

        # Handle reset
        if "reset-button" in trigger:
            logger.info("Reset triggered - generating new thread ID and triggering reset workflow")
            new_thread_id = str(uuid4())

            # Remove all SSE clients for old threads across all workers

            # Get current active threads for logging
            active_threads = simple_sse_manager.get_all_active_threads()
            if active_threads:
                logger.info(f"Active SSE threads before cleanup: {active_threads}")

            # Broadcast cleanup signal for all threads except the new one
            logger.info(f"Broadcasting cleanup signal for all threads except {new_thread_id}")
            simple_sse_manager.broadcast_cleanup_all_except(new_thread_id)

            # Also cleanup all threads except the new one locally
            cleaned_count = simple_sse_manager.cleanup_all_threads_except(new_thread_id)
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} local SSE clients during reset")

            # Create workflow input for reset
            reset_workflow_input = {
                "last_intent_payload": {
                    "intent": "Reset",
                    "arguments": {}
                }
            }

            # Create SSE connection status that tells client to connect with reset workflow input
            sse_status = {
                "connect_sse": True,
                "thread_id": new_thread_id,
                "workflow_input": reset_workflow_input,
                "reset": True,  # Flag to tell SSE client this is a reset
                "timestamp": time.time()
            }

            # Clear chat display and return new thread with reset trigger
            return "", new_thread_id, False, [], sse_status

        # Prepare workflow input based on trigger type
        workflow_input = None

        # Handle text input
        if user_input and user_input.strip() and ("send-button" in trigger or "chat-input" in trigger):
            logger.info(f"Text input: {user_input}")

            # Add user message to chat immediately
            user_message_div = html.Div(user_input, className="speech-bubble user-bubble")
            updated_chat_display = (chat_display or []) + [user_message_div]

            workflow_input = {"messages": [("user", user_input)]}

        # Handle map clicks
        elif "map-click-add-trigger" in trigger and map_add_payload:
            logger.info(f"Map add click: {map_add_payload}")
            workflow_input = {
                "last_intent_payload": {
                    "intent": AssistantIntent.ADD_PLACE.value,
                    "arguments": {
                        "place": map_add_payload.get("name", "Unknown Place"),
                        "unit_type": map_add_payload.get("type"),
                        "polygon_id": int(map_add_payload["id"]) if str(map_add_payload["id"]).isdigit() else None
                    }
                }
            }

        elif "map-click-remove-trigger" in trigger and map_remove_payload:
            logger.info(f"Map remove click: {map_remove_payload}")
            workflow_input = {
                "last_intent_payload": {
                    "intent": AssistantIntent.REMOVE_PLACE.value,
                    "arguments": {
                        "place": map_remove_payload.get("name", "Unknown Place"),
                        "unit_type": map_remove_payload.get("type"),
                        "polygon_id": int(map_remove_payload["id"]) if str(map_remove_payload["id"]).isdigit() else None
                    }
                }
            }

        # If we have workflow input, signal SSE client to connect with workflow input
        if workflow_input:
            logger.info(f"Signaling SSE client to connect with workflow input for thread {thread_id}")

            # Create SSE connection status that tells client to connect with workflow input
            sse_status = {
                "connect_sse": True,
                "thread_id": thread_id,
                "workflow_input": workflow_input,
                "timestamp": time.time()
            }

            # Return updates: clear input, keep thread_id, disable button, update chat, trigger SSE
            if 'updated_chat_display' in locals():
                return "", thread_id, True, updated_chat_display, sse_status
            else:
                return "", thread_id, True, no_update, sse_status

        raise PreventUpdate


def start_workflow_background(compiled_workflow, thread_id: str, workflow_input: Dict[str, Any]):
    """Start workflow execution in background using async methods"""

    async def run_workflow_async():
        try:
            logger.info(f"Background workflow starting for thread {thread_id}")

            # Create config for this thread
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": None
                }
            }

            # Use the simplified workflow adapter with async execution
            from vobchat.workflow_sse_adapter import create_simple_workflow_adapter
            adapter = create_simple_workflow_adapter(compiled_workflow)

            # Execute workflow via adapter - this handles SSE streaming
            await adapter.run(thread_id, workflow_input)
            logger.info(f"Workflow completed for thread {thread_id}")

        except Exception as e:
            logger.error(f"Workflow error for thread {thread_id}: {e}", exc_info=True)
            # Send error via SSE
            await simple_sse_manager.error(thread_id, str(e))

    # Submit async task to the async manager
    from vobchat.utils.async_manager import async_manager
    async_manager.submit_task(run_workflow_async())
    logger.info(f"Background workflow task submitted for {thread_id}")
