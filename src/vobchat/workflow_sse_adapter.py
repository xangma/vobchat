"""SSE adapter for streaming LangGraph workflow state to the browser.

This module provides a thin, async bridge between the compiled LangGraph
workflow and the Server-Sent Events (SSE) hub. Its responsibilities are to:
- run a workflow for a given ``thread_id``
- stream minimally filtered state deltas to the UI
- toggle a lightweight ``llm_busy`` flag while the model is working
- emit interrupts (if any) when the graph finishes an async step

It deliberately avoids owning business logic; nodes and the graph encode the
behavior. The adapter focuses on transport and small UX niceties only.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    AIMessageChunk,
    HumanMessageChunk,
)

from vobchat.sse_manager import get_sse_manager
from vobchat.nodes.utils import serialize_messages as _base_serialize_messages

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
    "messages",  # Include full message history for proper ordering
}

simple_sse_manager = get_sse_manager()


def serialize_messages(messages: list[Any]) -> list[dict[str, Any]]:
    """Serialize messages using the shared util, then strip reasoning text.

    The shared serializer converts LangChain message objects to minimal dicts.
    We post-process to remove visible chain-of-thought segments from AI output.
    """
    out = _base_serialize_messages(messages)
    try:
        for item in out:
            if isinstance(item, dict) and item.get("_type") == "ai":
                item["content"] = item.get("content") or ""
    except Exception:
        pass
    return out


def _looks_like_planner_json(text: str) -> bool:
    """Heuristic to suppress planner JSON from being rendered as chat.

    Treat any content that begins with '{' or '[' as planner/JSON-ish until
    we later see natural language content. This is conservative but avoids
    flashing schema text like {"actions": ...} or {"places": ...} in the UI.
    """
    try:
        if not text:
            return False
        s = str(text).strip()
        if not s:
            return False
        return s[0] in "[{"
    except Exception:
        return False


class WorkflowSSEAdapter:
    """Run a compiled workflow and push updates to clients via the SSE hub.

    Each thread_id maintains a small set of streamed AI message IDs to skip
    duplicates that may be present after checkpoint restoration.
    """

    def __init__(self, compiled_workflow):
        self.wf = compiled_workflow

    # ------------------------------------------------------------------
    # Public entry
    # ------------------------------------------------------------------

    async def run(self, thread_id: str, initial_input: Optional[dict] = None) -> None:
        """Execute the workflow and stream deltas for a given thread.

        Args:
            thread_id: Logical conversation/workflow identifier.
            initial_input: Optional first input to the graph (often ``None`` as
                the graph pulls from checkpointed state).

        Behavior:
            - Proactively sets ``llm_busy=True``; cleared on first AI message or
              at the end of execution.
            - Streams only whitelisted state keys (see ``_UI_KEYS``) to reduce
              payload size and UI churn; message objects are serialized.
            - Emits the final interrupt payload (if present) when the run ends.
        """
        cfg = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": None,
            }
        }

        # --------------------------------------------------------------
        # Proactively signal that the assistant is working
        try:
            await simple_sse_manager.state(thread_id, {"llm_busy": True})
        except Exception:
            logger.debug("Could not send initial llm_busy state")
        llm_busy = True
        # Stream the graph using the event stream to capture token-level updates
        # --------------------------------------------------------------

        try:
            # Local render cache for token-level streaming
            render_messages: list[dict[str, Any]] | None = None
            # Delay rendering until a likely reply start appears to avoid meta text
            visible_started: bool = False

            events = self.wf.astream_events(initial_input, config=cfg, version="v1")
            # Flag to indicate that we have seen any chat-model stream for this turn.
            # While true, snapshot events should not send `messages` because the
            # streaming path is responsible for progressive chat rendering.
            saw_stream: bool = False
            async for event in events:
                kind = event.get("event") or ""

                # 1) Token stream from the model
                if kind == "on_chat_model_stream":
                    try:
                        data = event.get("data", {}) or {}
                        # If upstream marks this stream as non-UI (planner/subagent/no_ui_stream), skip it entirely
                        tags = event.get("tags") or data.get("tags") or []
                        try:
                            # normalize tags to a flat list of strings
                            if isinstance(tags, dict):
                                tags = list(tags.values())
                            if not isinstance(tags, list):
                                tags = [str(tags)]
                        except Exception:
                            tags = []
                        # Skip planner/subagent streams, but always allow explicit reply streams
                        if "reply_stream" not in tags:
                            if any(t in ("planner", "subagent", "no_ui_stream") for t in tags):
                                continue
                        saw_stream = True
                        chunk = data.get("chunk")
                        text = ""
                        if chunk is not None:
                            text = getattr(chunk, "content", "") or ""
                            # Ignore reasoning-only chunks when ChatOllama(reasoning=True)
                            try:
                                ak = getattr(chunk, "additional_kwargs", {}) or {}
                                rc = ak.get("reasoning_content")
                                if rc and not text:
                                    # Reasoning is streaming separately; keep busy, don't render
                                    continue
                            except Exception:
                                pass
                        else:
                            text = data.get("content", "") or ""

                        if text:
                            prev = ""
                            # On first visible token, seed the render buffer with the existing
                            # conversation history so the UI keeps the human turn while streaming.
                            if render_messages is None:
                                try:
                                    st_snap = await self.wf.aget_state(cfg)
                                    vals_snap: Dict[str, Any] = (
                                        (st_snap.values or {}) if st_snap else {}
                                    )
                                    render_messages = serialize_messages(
                                        vals_snap.get("messages") or []
                                    )
                                except Exception:
                                    render_messages = []
                            if (
                                render_messages
                                and isinstance(render_messages[-1], dict)
                                and render_messages[-1].get("_type") == "ai"
                            ):
                                prev = render_messages[-1].get("content") or ""
                            candidate = prev + str(text)
                            # Skip planner/JSON-like content entirely until natural text appears
                            if _looks_like_planner_json(candidate):
                                continue
                            # Skip empty/whitespace after stripping reasoning
                            if not candidate.strip():
                                continue
                            # If we haven't shown any visible content yet, mark the
                            # beginning of the visible stream. Do not attempt to trim
                            # content here — just render from the first token.
                            if not visible_started:
                                visible_started = True
                            if (
                                not render_messages
                                or render_messages[-1].get("_type") != "ai"
                            ):
                                render_messages.append(
                                    {"_type": "ai", "type": "ai", "content": ""}
                                )
                            render_messages[-1]["content"] = candidate
                            # First, stream the token update to the UI
                            await simple_sse_manager.state(
                                thread_id, {"messages": render_messages}
                            )
                            # Then clear the busy indicator after the first visible token is rendered
                            if llm_busy and render_messages[-1]["content"].strip():
                                try:
                                    await simple_sse_manager.state(
                                        thread_id, {"llm_busy": False}
                                    )
                                    llm_busy = False
                                except Exception:
                                    logger.debug(
                                        "Could not clear llm_busy on first token"
                                    )
                    except Exception:
                        continue
                    continue

                # 2) Snapshot/values events: forward whitelisted state
                if kind in (
                    "on_values",
                    "on_node_end",
                    "on_checkpoint",
                    "on_chain_stream",
                ):
                    try:
                        st = await self.wf.aget_state(cfg)
                        vals: Dict[str, Any] = (st.values or {}) if st else {}
                        ui_state: Dict[str, Any] = {
                            k: vals.get(k) for k in _UI_KEYS if k in vals
                        }
                        if "messages" in ui_state:
                            state_serialized = serialize_messages(
                                ui_state["messages"] or []
                            )
                            # While a chat stream is active this turn, suppress snapshot
                            # message payloads entirely to avoid duplicate updates. The
                            # streaming path is responsible for message rendering.
                            if saw_stream:
                                ui_state.pop("messages", None)
                            else:
                                # No token stream detected — trust the snapshot and
                                # refresh the buffer to reflect latest state.
                                render_messages = state_serialized
                                ui_state["messages"] = state_serialized
                        if ui_state:
                            await simple_sse_manager.state(thread_id, ui_state)
                    except Exception:
                        continue
                    continue

            # ----------------------------------------------------------
            # Final interrupt (if any) ---------------------------------
            st = await self.wf.aget_state(cfg)
            if st and st.tasks and st.tasks[-1].interrupts:
                # Not waiting on the assistant any more – ensure busy flag cleared
                if llm_busy:
                    try:
                        await simple_sse_manager.state(thread_id, {"llm_busy": False})
                        llm_busy = False
                    except Exception:
                        logger.debug("Could not clear llm_busy before interrupt")
                await simple_sse_manager.interrupt(
                    thread_id, st.tasks[-1].interrupts[0].value
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workflow run failed for %s", thread_id)
            try:
                if llm_busy:
                    await simple_sse_manager.state(thread_id, {"llm_busy": False})
            finally:
                await simple_sse_manager.error(thread_id, str(exc))
        finally:
            # If nothing triggered a clear, make sure to drop busy flag at end
            if llm_busy:
                try:
                    await simple_sse_manager.state(thread_id, {"llm_busy": False})
                except Exception:
                    logger.debug("Could not clear llm_busy in finally")


def create_simple_workflow_adapter(compiled_workflow):
    """Create simplified workflow adapter"""
    return WorkflowSSEAdapter(compiled_workflow)
