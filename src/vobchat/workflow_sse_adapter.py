# src/vobchat/workflow_sse_adapter.py

import asyncio
import json
import logging
# import threading  # Not needed anymore, using workflow_lock_manager instead
import time
from typing import Dict, Any, Optional, AsyncGenerator
from langchain_core.messages import AIMessage, HumanMessage, AIMessageChunk
from langgraph.types import interrupt
from vobchat.utils.redis_checkpoint import AsyncRedisSaver
from vobchat.utils.redis_pool import redis_pool_manager
from vobchat.utils.workflow_lock_manager import workflow_lock_manager

from vobchat.sse_manager import (
    sse_manager,
    MessageEvent,
    InterruptEvent,
    StateUpdateEvent,
    ErrorEvent
)
# from vobchat.state_schema import lg_State  # Not used currently, kept for potential future use

logger = logging.getLogger(__name__)

# Note: Workflow locking is now handled by workflow_lock_manager
# The old thread-local locking has been replaced with a centralized lock manager

class WorkflowSSEAdapter:
    """Adapter that converts workflow events to SSE streams"""

    def __init__(self, compiled_workflow):
        self.compiled_workflow = compiled_workflow
        self.base_workflow = None

    def set_base_workflow(self, base_workflow):
        """Store reference to the base workflow graph for recompilation"""
        self.base_workflow = base_workflow

    async def stream_workflow_execution(
        self,
        workflow_input: Optional[Dict[str, Any]],
        config: Dict[str, Any],
        thread_id: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream workflow execution via SSE instead of using interrupts
        """
        try:
            start_time = time.time()
            logger.debug(f"Workflow execution started at {start_time:.3f}")

            # Determine if this is a resumption or new execution
            is_resumption = (
                workflow_input is None or  # Direct resumption
                (isinstance(workflow_input, dict) and workflow_input.get('button_type')) or  # Button clicks
                (isinstance(workflow_input, dict) and workflow_input.get('selection_idx') is not None)  # Selections
            )

            # Use the existing compiled workflow instance
            workflow_instance = self.compiled_workflow
            logger.debug(f"Using existing compiled workflow instance")

            # Stream workflow messages and state updates
            stream_start = time.time()
            logger.debug(f"Starting workflow stream at {stream_start:.3f}")

            message_count = 0
            last_streamed_message_index = -1  # Track the index of the last message we streamed

            # For workflow resumption, we need to track which messages have already been sent
            # Get the current state to see how many messages already exist
            if is_resumption:
                try:
                    current_state = await workflow_instance.aget_state(config)
                    if current_state and current_state.values and "messages" in current_state.values:
                        existing_messages = current_state.values["messages"]
                        # For resumption, we've already streamed all existing messages
                        last_streamed_message_index = len(existing_messages) - 1
                        logger.debug(f"Resumption - already streamed {last_streamed_message_index + 1} messages")
                except Exception as e:
                    logger.debug(f"Error getting existing messages for resumption: {e}")
                    # Continue without pre-populating - will stream all messages
            # CRITICAL: Use "values" streaming mode instead of "messages"
            # This prevents internal LLM operations (like intent extraction) from streaming
            # while still allowing us to get state updates and final results

            logger.debug(f"About to start workflow stream with workflow_input={workflow_input}")
            async for state_update in workflow_instance.astream(
                workflow_input,
                config=config,
                stream_mode="values"  # Stream state updates, not LLM messages
            ):
                message_count += 1
                msg_time = time.time()
                logger.debug(f"Received state update {message_count} at {msg_time:.3f}")

                # DEBUG: Log the current state's selection_idx in each update
                if isinstance(state_update, dict):
                    current_selection = state_update.get('selection_idx')
                    current_place_index = state_update.get('current_place_index')
                    logger.debug(f"State update {message_count} - selection_idx={current_selection}, current_place_index={current_place_index}")

                # Check if there are new messages in the state to stream to user
                if isinstance(state_update, dict) and "messages" in state_update:
                    messages = state_update["messages"]

                    # Only process messages that are newer than what we've already streamed
                    for i, message in enumerate(messages):
                        if i <= last_streamed_message_index:
                            continue  # Skip already streamed messages

                        # Only stream AI messages marked as streamable
                        if isinstance(message, AIMessage) and message.content:
                            content = str(message.content)

                            # Check message metadata for streaming preference
                            stream_mode = "stream"  # Default to streaming
                            if hasattr(message, 'response_metadata') and message.response_metadata:
                                stream_mode = message.response_metadata.get('stream_mode', 'stream')

                            # Only stream messages marked as "stream" mode
                            if stream_mode == "stream":
                                logger.debug(f"Streaming user-facing message at index {i}: {content[:50]}...")
                                last_streamed_message_index = i  # Update our tracker
                                sse_manager.broadcast_event(
                                    MessageEvent(
                                        content=content,
                                        thread_id=thread_id,
                                        is_partial=False
                                    )
                                )
                                yield {"type": "message", "content": content}
                            else:
                                logger.debug(f"Skipping internal message at index {i} (mode={stream_mode}): {content[:50]}...")
                                last_streamed_message_index = i  # Still update tracker for non-streamed messages

                # ------------------------------------------------------------------
                # Early-push map update requests to the client
                # ------------------------------------------------------------------
                # The normal flow only emits *one* consolidated ``state_update`` event
                # at the very end of the workflow execution.  For multi-place queries
                # this can be too late: the backend may have already determined the
                # correct polygon for the first place and stored it in
                # ``state['map_update_request']`` but the client won’t visualise it
                # until the workflow finishes resolving **all** places.  By emitting
                # a *partial* state update as soon as we detect the
                # ``map_update_request`` key we let the frontend highlight the
                # polygon immediately while the backend continues processing.
                if isinstance(state_update, dict) and "map_update_request" in state_update:
                    try:
                        frontend_state_partial = self._extract_frontend_state(state_update)

                        # Only broadcast if the map_update_request actually contains
                        # something – avoid flooding the client with empty events.
                        if frontend_state_partial.get("map_update_request"):
                            logger.debug(
                                "Early state update – map_update_request detected, "
                                "broadcasting partial state to frontend"
                            )

                            sse_manager.broadcast_event(
                                StateUpdateEvent(
                                    state_updates=frontend_state_partial,
                                    thread_id=thread_id,
                                )
                            )

                            # Also yield through the async generator so any upstream
                            # callers (e.g. tests) receive the update.
                            yield {
                                "type": "state_update",
                                "data": frontend_state_partial,
                            }
                    except Exception as e:
                        # Don’t fail the whole stream because of logging / SSE
                        # issues – just record the error and continue.
                        logger.error(
                            f"Error while broadcasting early map_update_request "
                            f"state update: {e}"
                        )

            # Get final state after streaming using the workflow instance
            final_state_start = time.time()
            logger.debug(f"Getting final state at {final_state_start:.3f}")
            final_state = await workflow_instance.aget_state(config)
            final_state_end = time.time()
            logger.debug(f"Got final state at {final_state_end:.3f} (took {final_state_end - final_state_start:.3f}s)")

            # Check for interrupts and convert them to SSE events
            interrupt_start = time.time()
            interrupt_processed = False
            if final_state and final_state.tasks:
                interrupt_task = final_state.tasks[-1]
                if interrupt_task.interrupts:
                    interrupt_data = interrupt_task.interrupts[0].value
                    logger.debug(f"Processing interrupt at {interrupt_start:.3f}")

                    # Send interrupt as SSE event instead of blocking
                    interrupt_broadcast_start = time.time()
                    sse_manager.broadcast_event(
                        InterruptEvent(
                            interrupt_data=interrupt_data,
                            thread_id=thread_id
                        )
                    )
                    interrupt_broadcast_end = time.time()
                    logger.debug(f"Interrupt broadcast took {interrupt_broadcast_end - interrupt_broadcast_start:.3f}s")

                    yield {
                        "type": "interrupt",
                        "data": interrupt_data
                    }

                    # Mark that we processed an interrupt
                    interrupt_processed = True

                    # Log the continue_to_next_place flag for debugging
                    continue_to_next_place = interrupt_data.get("continue_to_next_place", False)
                    logger.debug(f"Interrupt contains continue_to_next_place={continue_to_next_place}")
                    if continue_to_next_place:
                        logger.debug(f"This interrupt should trigger automatic continuation to next place")

            # Send final state update only if no interrupt was processed
            # When an interrupt occurs, the interrupt data contains the correct state
            # and final_state.values contains stale state from before the interrupt
            state_update_start = time.time()
            if final_state and final_state.values and not interrupt_processed:
                logger.debug(f"Processing state update at {state_update_start:.3f}")

                # Filter state to only include relevant frontend data
                extract_start = time.time()
                frontend_state = self._extract_frontend_state(final_state.values)
                extract_end = time.time()
                logger.debug(f"State extraction took {extract_end - extract_start:.3f}s")

                # CRITICAL DEBUG: Log what state is being sent to frontend
                logger.debug(f"Frontend state being sent: {frontend_state}")
                if 'map_update_request' in frontend_state:
                    logger.debug(f"map_update_request in state: {frontend_state['map_update_request']}")
                if 'selected_place_g_units' in frontend_state:
                    logger.debug(f"selected_place_g_units in state: {frontend_state['selected_place_g_units']}")

                broadcast_start = time.time()
                sse_manager.broadcast_event(
                    StateUpdateEvent(
                        state_updates=frontend_state,
                        thread_id=thread_id
                    )
                )
                broadcast_end = time.time()
                logger.debug(f"State broadcast took {broadcast_end - broadcast_start:.3f}s")

                yield {
                    "type": "state_update",
                    "data": frontend_state
                }
            elif interrupt_processed:
                logger.debug(f"Skipping final state update because interrupt was processed - interrupt data is authoritative")

            end_time = time.time()
            logger.debug(f"Workflow execution completed at {end_time:.3f} (total: {end_time - start_time:.3f}s)")

        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                error_time = time.time()
                logger.debug(f"Event loop closed error at {error_time:.3f}")

                # This shouldn't happen with our simplified approach
                # If it does, it indicates a deeper async context issue
                logger.debug(f"Unexpected event loop closure during workflow execution")
                logger.error(f"Event loop closed during workflow execution: {e}", exc_info=True)

                yield {
                    "type": "error",
                    "error": f"Workflow execution interrupted: {str(e)}"
                }
                return
            else:
                raise
        except Exception as e:
            error_time = time.time()
            logger.debug(f"Workflow error at {error_time:.3f}: {e}")
            logger.error(f"Error in workflow SSE streaming: {e}", exc_info=True)

            # Send error event
            sse_manager.broadcast_event(
                ErrorEvent(
                    error=str(e),
                    thread_id=thread_id
                )
            )

            yield {
                "type": "error",
                "error": str(e)
            }

    def _extract_frontend_state(self, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Extract only the state data needed by the frontend"""
        frontend_keys = [
            "selected_place_g_units",
            "selected_place_g_unit_types",
            "selected_place_g_places",
            "selected_polygons",
            "selected_polygons_unit_types",
            "extracted_place_names",
            "current_place_index",
            "current_node",
            "selected_theme",
            "selection_idx",
            "options",
            "cubes",
            "selected_cubes",
            "show_visualization",
            "map_update_request",  # CRITICAL: Include map update requests for frontend
            "units_needing_map_selection"
        ]

        # Build base frontend state
        frontend_state = {
            key: workflow_state.get(key)
            for key in frontend_keys
            if key in workflow_state
        }
        
        # CRITICAL: Convert places array to frontend-expected format
        # The workflow now uses 'places' as single source of truth, but frontend expects derived fields
        places = workflow_state.get("places", []) or []
        if places:
            from vobchat.state_schema import get_selected_units, get_selected_unit_types, get_selected_place_ids, get_selected_place_names
            
            # Derive frontend state from places array
            frontend_state["selected_place_g_units"] = get_selected_units(workflow_state)
            frontend_state["selected_place_g_unit_types"] = get_selected_unit_types(workflow_state)
            frontend_state["selected_place_g_places"] = get_selected_place_ids(workflow_state)
            frontend_state["extracted_place_names"] = get_selected_place_names(workflow_state)
            
            logger.debug(f"Derived frontend state from places array: selected_place_g_units={frontend_state['selected_place_g_units']}")
        else:
            # No places selected - clear the derived fields
            frontend_state["selected_place_g_units"] = []
            frontend_state["selected_place_g_unit_types"] = []
            frontend_state["selected_place_g_places"] = []
            frontend_state["extracted_place_names"] = []
            
            logger.debug("No places in state - cleared derived frontend fields")

        return frontend_state

    async def handle_user_input(
        self,
        thread_id: str,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle user input with fresh workflow instance to avoid Redis connection issues"""

        # Check if workflow is already running
        if workflow_lock_manager.is_workflow_running(thread_id):
            logger.debug(f"Workflow already running for thread {thread_id}, rejecting duplicate request")
            return {"status": "busy", "message": "Workflow is already processing. Please wait."}

        # Use workflow lock manager for proper concurrency control
        try:
            with workflow_lock_manager.acquire_workflow_lock(thread_id) as execution:
                logger.debug(f"Creating isolated workflow instance for user input handling (execution: {execution.execution_id})")

                # Create a completely fresh workflow instance with its own Redis connection
                # This ensures no event loop conflicts with existing connections
                if not self.base_workflow:
                    return {"status": "error", "error": "Base workflow not available"}

                # Create fresh Redis connection from pool for this specific operation
                # Don't decode responses - let AsyncRedisSaver handle the decoding internally
                fresh_redis = redis_pool_manager.get_async_client(decode_responses=False)
                fresh_checkpointer = AsyncRedisSaver(conn=fresh_redis)

                # Compile fresh workflow with isolated checkpointer
                fresh_workflow = self.base_workflow.compile(checkpointer=fresh_checkpointer)

                # Create temporary adapter with fresh workflow
                temp_adapter = WorkflowSSEAdapter(fresh_workflow)
                temp_adapter.set_base_workflow(self.base_workflow)

                logger.debug(f"Starting isolated workflow execution for user input")

                # Process the user input using the fresh workflow instance
                async for evt in temp_adapter.stream_workflow_execution(
                    workflow_input=input_data,
                    config=config,
                    thread_id=thread_id
                ):
                    logger.debug(f"User input event: {evt.get('type')}")
                    # Continue processing all events, including state updates

                # Redis connection will be handled by pool manager - no need to close explicitly
                # await fresh_redis.aclose()  # Commented out - let pool manage connections
                logger.debug(f"User input workflow completed successfully (execution: {execution.execution_id})")
                return {"status": "success"}

        except RuntimeError as e:
            # Handle workflow lock errors specifically
            if "already running" in str(e):
                logger.warning(f"Concurrent execution prevented for thread {thread_id}: {e}")
                return {"status": "error", "error": "Another workflow is already running for this conversation"}
            else:
                logger.error(f"Workflow lock error for thread {thread_id}: {e}")
                return {"status": "error", "error": str(e)}
        except Exception as e:
            logger.error(f"Error handling user input: {e}", exc_info=True)
            # Send error event to frontend
            try:
                sse_manager.broadcast_event(
                    ErrorEvent(
                        error=f"Failed to process input: {str(e)}",
                        thread_id=thread_id
                    )
                )
            except Exception as broadcast_error:
                logger.error(f"Failed to broadcast error event: {broadcast_error}")
            return {"status": "error", "error": str(e)}

class SSEInterruptHandler:
    """Replacement for LangGraph interrupt() that sends SSE events instead"""

    @staticmethod
    def interrupt_via_sse(value: Dict[str, Any], thread_id: str):
        """Send interrupt data via SSE instead of blocking workflow"""
        try:
            # Send interrupt event immediately
            sse_manager.broadcast_event(
                InterruptEvent(
                    interrupt_data=value,
                    thread_id=thread_id
                )
            )
            logger.info(f"Sent interrupt via SSE for thread {thread_id}: {list(value.keys())}")

        except Exception as e:
            logger.error(f"Error sending interrupt via SSE: {e}")

        # Don't actually interrupt the workflow - let it continue
        # The frontend will handle the interrupt event

# Monkey patch the interrupt function to use SSE
original_interrupt = interrupt

def sse_interrupt(value: Dict[str, Any]):
    """SSE-based interrupt that doesn't block workflow execution"""
    # Try to get thread_id from current context
    # This is a simplified approach - in practice you might need more sophisticated context tracking
    # import contextvars  # Might be needed for thread context tracking in the future

    # For now, we'll need to pass thread_id explicitly
    # This will be handled by modifying the workflow nodes
    logger.warning("SSE interrupt called without thread_id context - falling back to original interrupt")
    return original_interrupt(value)

# Export the adapter for use in callbacks
def create_workflow_sse_adapter(compiled_workflow, base_workflow=None):
    """Factory function to create workflow SSE adapter"""
    adapter = WorkflowSSEAdapter(compiled_workflow)
    if base_workflow:
        adapter.set_base_workflow(base_workflow)
    return adapter
