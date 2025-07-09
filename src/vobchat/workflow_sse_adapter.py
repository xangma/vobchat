# # Simple Workflow SSE Adapter - Clean rewrite
# # Single responsibility: Execute workflow and stream events

# import logging
# from typing import Dict, Any, Optional
# from langchain_core.messages import AIMessage

# from vobchat.sse_manager import simple_sse_manager

# logger = logging.getLogger(__name__)

# # Global registry of streamed message IDs per thread to prevent duplicates
# _global_streamed_message_ids = {}

# class SimpleWorkflowSSEAdapter:
#     """Simplified workflow adapter that streams events via SSE"""

#     def __init__(self, compiled_workflow):
#         self.compiled_workflow = compiled_workflow

#     async def execute_workflow(self, thread_id: str, workflow_input: Optional[Dict[str, Any]] = None):
#         """Execute workflow and stream results via SSE using async methods"""

#         try:
#             logger.info(f"Starting workflow execution for thread {thread_id}")

#             # Create config for this thread
#             config = {
#                 "configurable": {
#                     "thread_id": thread_id,
#                     "checkpoint_ns": "",
#                     "checkpoint_id": None
#                 }
#             }

#             # Get or create the set of streamed message IDs for this thread
#             if thread_id not in _global_streamed_message_ids:
#                 _global_streamed_message_ids[thread_id] = set()
#             streamed_message_ids = _global_streamed_message_ids[thread_id]

#             # Pre-mark existing messages from checkpoint state as already streamed
#             try:
#                 current_state = await self.compiled_workflow.aget_state(config)
#                 if current_state and current_state.values:
#                     existing_messages = current_state.values.get("messages", [])
#                     for msg in existing_messages:
#                         if isinstance(msg, AIMessage) and hasattr(msg, 'response_metadata') and msg.response_metadata:
#                             existing_message_id = msg.response_metadata.get('message_id')
#                             if existing_message_id:
#                                 streamed_message_ids.add(existing_message_id)
#                                 logger.debug(f"Pre-marked existing message ID {existing_message_id} as streamed")
#             except Exception as e:
#                 logger.debug(f"Could not pre-mark existing messages: {e}")

#             # Stream workflow execution using async methods
#             async for state_update in self.compiled_workflow.astream(
#                 workflow_input,
#                 config=config,
#                 stream_mode="values"  # Get state updates
#             ):
#                 # Handle messages in state
#                 if isinstance(state_update, dict) and "messages" in state_update:
#                     messages = state_update["messages"]

#                     # Find new AI messages to stream
#                     for message in messages:
#                         if isinstance(message, AIMessage) and message.content:
#                             # Check if this should be streamed (default yes)
#                             stream_mode = "stream"
#                             message_id = None
#                             if hasattr(message, 'response_metadata') and message.response_metadata:
#                                 stream_mode = message.response_metadata.get('stream_mode', 'stream')
#                                 message_id = message.response_metadata.get('message_id')

#                             if stream_mode == "stream":
#                                 # Check if we've already streamed this message ID
#                                 if message_id and message_id in streamed_message_ids:
#                                     logger.debug(f"Skipping already-streamed message with ID {message_id}")
#                                     continue

#                                 # Stream the message
#                                 simple_sse_manager.send_message(thread_id, str(message.content))

#                                 # Track this message ID as streamed
#                                 if message_id:
#                                     streamed_message_ids.add(message_id)
#                                     logger.debug(f"Marked message ID {message_id} as streamed")

#                 # Send state updates for UI
#                 if isinstance(state_update, dict):
#                     # Extract relevant state for frontend
#                     frontend_state = self._extract_frontend_state(state_update)
#                     if frontend_state:
#                         simple_sse_manager.send_state_update(thread_id, frontend_state)

#             # Get final state and check for interrupts using async method
#             final_state = await self.compiled_workflow.aget_state(config)
#             if final_state and final_state.tasks:
#                 interrupt_task = final_state.tasks[-1]
#                 if interrupt_task.interrupts:
#                     interrupt_data = interrupt_task.interrupts[0].value
#                     simple_sse_manager.send_interrupt(thread_id, interrupt_data)
#                     logger.info(f"Sent interrupt for thread {thread_id}")

#             logger.info(f"Workflow execution completed for thread {thread_id}")

#         except Exception as e:
#             logger.error(f"Workflow execution failed for thread {thread_id}: {e}", exc_info=True)
#             simple_sse_manager.send_error(thread_id, str(e))

#     def _extract_frontend_state(self, workflow_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
#         """Extract only essential state data for frontend"""

#         # Only send state that affects UI
#         essential_keys = [
#             "places",  # Single source of truth for place/unit data
#             "selected_polygons",
#             "selected_polygons_unit_types",
#             "current_place_index",
#             "current_node",
#             "selected_theme",
#             "options",
#             "cubes",
#             "selected_cubes",
#             "show_visualization",
#             "map_update_request",  # Critical for map updates
#             "units_needing_map_selection"  # Critical for polygon selection
#         ]

#         frontend_state = {}
#         for key in essential_keys:
#             if key in workflow_state:
#                 frontend_state[key] = workflow_state[key]

#         # Only send if we have something useful
#         return frontend_state if frontend_state else None

# # Factory function
# def create_simple_workflow_adapter(compiled_workflow):
#     """Create simplified workflow adapter"""
#     return SimpleWorkflowSSEAdapter(compiled_workflow)


# workflow_sse_adapter.py – thin bridge workflow → SSE
# =====================================================
# Responsibilities
# ----------------
# • Run an **async LangGraph workflow**.
# • Stream AI messages + trimmed state deltas to the browser via *SSE_HUB*.
# • Emit interrupt payloads when the graph finishes with one.
#
# Simplifications
# ---------------
# • The adapter owns its own `streamed_ids` set → no globals.
# • All hub calls are *awaited* so ordering is preserved.
# • `_extract_state()` is a tiny whitelist of UI‑relevant keys.
# =====================================================

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Set

from langchain_core.messages import AIMessage

from vobchat.sse_manager import get_sse_manager

logger = logging.getLogger(__name__)

_UI_KEYS = {
    "places",
    "current_place_index",
    "current_node",
    "selected_theme",
    "options",
    "selected_cubes",
    "show_visualization",
    "map_update_request",
    "units_needing_map_selection",
}

simple_sse_manager = get_sse_manager()
class WorkflowSSEAdapter:
    """Run *compiled_workflow* and push updates to clients via SSE_HUB."""

    def __init__(self, compiled_workflow):
        self.wf = compiled_workflow
        self._streamed: Dict[str, Set[str]] = {}

    # ------------------------------------------------------------------
    # Public entry
    # ------------------------------------------------------------------

    async def run(self, thread_id: str, initial_input: Optional[dict] = None) -> None:
        cfg = {"configurable": {"thread_id": thread_id,
                                "checkpoint_ns": "", "checkpoint_id": None}}
        self._streamed.setdefault(thread_id, set())
        streamed_ids = self._streamed[thread_id]

        # Pre‑populate with message_ids already in checkpoint (if any)
        try:
            st = await self.wf.aget_state(cfg)
            for m in (st.values.get("messages", []) if st and st.values else []):
                mid = getattr(m, "response_metadata", {}).get(
                    "message_id") if isinstance(m, AIMessage) else None
                if mid:
                    streamed_ids.add(mid)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Checkpoint scan failed: %s", exc)

        # --------------------------------------------------------------
        # Stream the graph
        # --------------------------------------------------------------

        try:
            async for delta in self.wf.astream(initial_input, config=cfg, stream_mode="values"):
                # 1️⃣  Messages -------------------------------------------------
                for msg in delta.get("messages", []):
                    if isinstance(msg, AIMessage) and msg.content:
                        mid = getattr(msg, "response_metadata",
                                      {}).get("message_id")
                        if mid and mid in streamed_ids:
                            continue
                        await simple_sse_manager.message(thread_id, str(msg.content))
                        if mid:
                            streamed_ids.add(mid)
                # 2️⃣  State snip ---------------------------------------------
                ui_state = {k: v for k, v in delta.items() if k in _UI_KEYS}
                if ui_state:
                    logger.info(f"SSE Adapter: Streaming state update for thread {thread_id}: {list(ui_state.keys())}")
                    if 'map_update_request' in ui_state:
                        logger.info(f"SSE Adapter: map_update_request = {ui_state['map_update_request']}")
                    await simple_sse_manager.state(thread_id, ui_state)

            # ----------------------------------------------------------
            # Final interrupt (if any) ---------------------------------
            st = await self.wf.aget_state(cfg)
            if st and st.tasks and st.tasks[-1].interrupts:
                await simple_sse_manager.interrupt(thread_id, st.tasks[-1].interrupts[0].value)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workflow run failed for %s", thread_id)
            await simple_sse_manager.error(thread_id, str(exc))


def create_simple_workflow_adapter(compiled_workflow):
    """Create simplified workflow adapter"""
    return WorkflowSSEAdapter(compiled_workflow)
