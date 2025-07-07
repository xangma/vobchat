# src/vobchat/workflow_sse_adapter.py

import asyncio
import json
import logging
# import threading  # Not needed anymore, using workflow_lock_manager instead
import time
from typing import Dict, Any, Optional, AsyncGenerator
from langchain_core.messages import AIMessage, HumanMessage, AIMessageChunk

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
        # Note: streamed_message_ids is now handled globally per thread

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

            # Determine if this is a fresh SSE connection (no new user action)
            is_fresh_connection = workflow_input is None

            # Use the existing compiled workflow instance
            workflow_instance = self.compiled_workflow
            logger.debug(f"Using existing compiled workflow instance")

            # Stream workflow messages and state updates
            stream_start = time.time()
            logger.debug(f"Starting workflow stream at {stream_start:.3f}")

            message_count = 0
            last_streamed_message_index = -1  # Track the index of the last message we streamed
            # Track last map_update_request to avoid duplicate early updates
            last_map_update_request = None

            # Get or create the set of streamed message IDs for this thread from global registry
            if thread_id not in _global_streamed_message_ids:
                _global_streamed_message_ids[thread_id] = set()
            streamed_message_ids = _global_streamed_message_ids[thread_id]

            # CRITICAL FIX: Pre-mark any existing messages from checkpoint state as already streamed
            # to prevent re-streaming old messages when a new workflow execution starts
            initial_state = None
            try:
                # Get the current state to check for existing messages
                current_state = await workflow_instance.aget_state(config)
                if current_state and current_state.values:
                    initial_state = current_state.values
                    existing_messages = initial_state.get("messages", [])
                    for msg in existing_messages:
                        if isinstance(msg, AIMessage) and hasattr(msg, 'response_metadata') and msg.response_metadata:
                            existing_message_id = msg.response_metadata.get(
                                'message_id')
                            if existing_message_id:
                                streamed_message_ids.add(existing_message_id)
                                logger.debug(
                                    f"Pre-marked existing message ID {existing_message_id} as streamed to prevent duplicates")
            except Exception as e:
                logger.debug(f"Could not pre-mark existing messages: {e}")
                # Continue anyway - this is just an optimization

            # CRITICAL: Use "values" streaming mode instead of "messages"
            # This prevents internal LLM operations (like intent extraction) from streaming
            # while still allowing us to get state updates and final results

            # Process workflow input normally
            # Button selections are now handled separately and don't go through this path
            processed_input = workflow_input

            # CRITICAL DEBUG: Log workflow input to track selection_idx
            if processed_input and "selection_idx" in processed_input:
                logger.error(f"🔍 WORKFLOW INPUT contains selection_idx: {processed_input['selection_idx']}")
            else:
                logger.debug(f"About to start workflow stream with processed_input={processed_input}")
                
            async for state_update in workflow_instance.astream(
                processed_input,
                config=config,
                stream_mode="values"  # Stream state updates, not LLM messages
            ):
                message_count += 1
                msg_time = time.time()
                
                # CRITICAL DEBUG: Track selection_idx updates
                if isinstance(state_update, dict):
                    current_selection = state_update.get('selection_idx')
                    current_place_index = state_update.get('current_place_index')
                    
                    if current_selection is not None:
                        logger.error(f"🔴 STATE UPDATE #{message_count} contains selection_idx={current_selection} - THIS MIGHT CAUSE DUPLICATE!")
                    else:
                        logger.info(f"✅ State update #{message_count} - no selection_idx, current_place_index={current_place_index}")

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

                            # Check message metadata for streaming preference and ID
                            stream_mode = "stream"  # Default to streaming
                            message_id = None
                            if hasattr(message, 'response_metadata') and message.response_metadata:
                                stream_mode = message.response_metadata.get(
                                    'stream_mode', 'stream')
                                message_id = message.response_metadata.get(
                                    'message_id')

                            # Only stream messages marked as "stream" mode
                            if stream_mode == "stream":
                                # Check if we've already streamed this message ID
                                if message_id and message_id in streamed_message_ids:
                                    logger.debug(
                                        f"Skipping already-streamed message with ID {message_id}: {content[:50]}...")
                                    last_streamed_message_index = i
                                    continue

                                logger.debug(
                                    f"Streaming user-facing message at index {i} (ID: {message_id}): {content[:50]}...")
                                last_streamed_message_index = i  # Update our tracker

                                # Track this message ID as streamed
                                if message_id:
                                    streamed_message_ids.add(message_id)

                                sse_manager.broadcast_event(
                                    MessageEvent(
                                        content=content,
                                        thread_id=thread_id,
                                        is_partial=False,
                                        message_id=message_id
                                    )
                                )
                                yield {"type": "message", "content": content}

                            else:
                                logger.debug(
                                    f"Skipping internal message at index {i} (mode={stream_mode}): {content[:50]}...")
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
                # Also handle UI clearing updates (when options is set to None)
                has_map_update = isinstance(
                    state_update, dict) and "map_update_request" in state_update
                has_ui_clear = isinstance(
                    state_update, dict) and "options" in state_update and state_update.get("options") is None

                # DEBUG: Log the detection logic
                if isinstance(state_update, dict):
                    if "options" in state_update:
                        logger.debug(
                            f"SSE: Detected options in state_update: {state_update.get('options')}")
                        logger.debug(f"SSE: has_ui_clear = {has_ui_clear}")
                        if has_ui_clear:
                            logger.info(
                                f"SSE: UI CLEARING DETECTED! options={state_update.get('options')}")
                    else:
                        logger.debug(
                            f"SSE: No options in state_update. Keys: {list(state_update.keys())}")

                if has_map_update or has_ui_clear:
                    try:
                        frontend_state_partial = self._extract_frontend_state(
                            state_update)

                        # DEBUG: Log what's being sent to frontend
                        if has_ui_clear:
                            logger.info(
                                f"SSE: Frontend state for UI clearing: {frontend_state_partial}")
                            logger.info(
                                f"SSE: options in frontend_state: {'options' in frontend_state_partial}")

                        current_map_update_request = state_update.get(
                            "map_update_request")

                        # Broadcast if:
                        # 1. map_update_request contains something AND it's different from the last one we sent, OR
                        # 2. UI clearing is needed (options set to None)
                        should_broadcast_map = (current_map_update_request and
                                                current_map_update_request != last_map_update_request)
                        should_broadcast_ui_clear = has_ui_clear

                        if should_broadcast_map or should_broadcast_ui_clear:

                            update_reason = []
                            if should_broadcast_map:
                                update_reason.append("map_update_request")
                            if should_broadcast_ui_clear:
                                update_reason.append(
                                    "UI clearing (options=None)")

                            logger.debug(
                                f"Early state update – {', '.join(update_reason)} detected, "
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

                            # Update our tracker to prevent duplicate broadcasts
                            last_map_update_request = current_map_update_request
                        else:
                            logger.debug(
                                f"Skipping duplicate map_update_request: {current_map_update_request}"
                            )
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
            logger.debug(
                f"Got final state at {final_state_end:.3f} (took {final_state_end - final_state_start:.3f}s)")

            # Check for interrupts and convert them to SSE events
            interrupt_start = time.time()
            interrupt_processed = False
            if final_state and final_state.tasks:
                interrupt_task = final_state.tasks[-1]
                if interrupt_task.interrupts:
                    interrupt_data = interrupt_task.interrupts[0].value
                    logger.debug(
                        f"Processing interrupt at {interrupt_start:.3f}")

                    # Send interrupt as SSE event instead of blocking
                    interrupt_broadcast_start = time.time()
                    sse_manager.broadcast_event(
                        InterruptEvent(
                            interrupt_data=interrupt_data,
                            thread_id=thread_id
                        )
                    )
                    interrupt_broadcast_end = time.time()
                    logger.debug(
                        f"Interrupt broadcast took {interrupt_broadcast_end - interrupt_broadcast_start:.3f}s")

                    yield {
                        "type": "interrupt",
                        "data": interrupt_data
                    }

                    # Mark that we processed an interrupt
                    interrupt_processed = True

                    # Log the continue_to_next_place flag for debugging
                    continue_to_next_place = interrupt_data.get(
                        "continue_to_next_place", False)
                    logger.debug(
                        f"Interrupt contains continue_to_next_place={continue_to_next_place}")
                    if continue_to_next_place:
                        logger.debug(
                            f"This interrupt should trigger automatic continuation to next place")

            # Send final state update only if no interrupt was processed
            # When an interrupt occurs, the interrupt data contains the correct state
            # and final_state.values contains stale state from before the interrupt
            state_update_start = time.time()
            if final_state and final_state.values and not interrupt_processed:
                logger.debug(
                    f"Processing state update at {state_update_start:.3f}")

                # Filter state to only include relevant frontend data
                extract_start = time.time()
                frontend_state = self._extract_frontend_state(
                    final_state.values)
                extract_end = time.time()
                logger.debug(
                    f"State extraction took {extract_end - extract_start:.3f}s")

                # CRITICAL DEBUG: Log what state is being sent to frontend
                logger.debug(f"Frontend state being sent: {frontend_state}")
                if 'map_update_request' in frontend_state:
                    logger.debug(
                        f"map_update_request in state: {frontend_state['map_update_request']}")
                if 'places' in frontend_state:
                    logger.debug(
                        f"places array in state: {frontend_state['places']}")

                broadcast_start = time.time()
                sse_manager.broadcast_event(
                    StateUpdateEvent(
                        state_updates=frontend_state,
                        thread_id=thread_id
                    )
                )
                broadcast_end = time.time()
                logger.debug(
                    f"State broadcast took {broadcast_end - broadcast_start:.3f}s")

                yield {
                    "type": "state_update",
                    "data": frontend_state
                }
            elif interrupt_processed:
                logger.debug(
                    f"Skipping final state update because interrupt was processed - interrupt data is authoritative")

            end_time = time.time()
            logger.debug(
                f"Workflow execution completed at {end_time:.3f} (total: {end_time - start_time:.3f}s)")

        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                error_time = time.time()
                logger.debug(f"Event loop closed error at {error_time:.3f}")

                # This shouldn't happen with our simplified approach
                # If it does, it indicates a deeper async context issue
                logger.debug(
                    f"Unexpected event loop closure during workflow execution")
                logger.error(
                    f"Event loop closed during workflow execution: {e}", exc_info=True)

                yield {
                    "type": "error",
                    "error": f"Workflow execution interrupted: {str(e)}"
                }
                return
            else:
                raise
        except Exception as e:
            error_time = time.time()
            
            # CRITICAL DEBUG: Enhanced error logging for selection_idx issues
            if "selection_idx" in str(e) and "Can receive only one value" in str(e):
                logger.error(f"🔥 DUPLICATE selection_idx ERROR: {e}")
                logger.error(f"🔥 This means selection_idx was set TWICE in the same workflow step!")
                if workflow_input and "selection_idx" in workflow_input:
                    logger.error(f"🔥 Workflow input had selection_idx: {workflow_input['selection_idx']}")
                logger.error(f"🔥 AND a workflow node also tried to update selection_idx!")
            else:
                logger.debug(f"Workflow error at {error_time:.3f}: {e}")
                
            logger.error(
                f"Error in workflow SSE streaming: {e}", exc_info=True)

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
            "places",  # CRITICAL: Single source of truth for place/unit data
            "selected_polygons",
            "selected_polygons_unit_types",
            # "extracted_place_names" removed - using places array as single source of truth
            "current_place_index",
            "current_node",
            "selected_theme",
            # "selection_idx",
            "options",
            "cubes",
            "cube_data",  # CRITICAL: Include cube data for visualization
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

        # Note: selected_polygons and selected_polygons_unit_types are now derived 
        # by the frontend SSE client from the places array for better consistency

        # Debug logging
        logger.debug(
            f"workflow_state has 'places': {'places' in workflow_state}")
        logger.debug(
            f"workflow_state['places']: {workflow_state.get('places', 'NOT FOUND')}")
        logger.debug(
            f"frontend_state has 'places': {'places' in frontend_state}")
        logger.debug(
            f"frontend_state['places']: {frontend_state.get('places', 'NOT FOUND')}")

        places = workflow_state.get("places", []) or []
        logger.debug(f"Sending places array to frontend: {len(places)} places")

        return frontend_state





# Global registry of streamed message IDs per thread to prevent duplicates across executions
_global_streamed_message_ids = {}



# Export the adapter for use in callbacks


def create_workflow_sse_adapter(compiled_workflow, base_workflow=None):
    """Factory function to create workflow SSE adapter"""
    adapter = WorkflowSSEAdapter(compiled_workflow)
    if base_workflow:
        adapter.set_base_workflow(base_workflow)
    return adapter
