"""Thread-safe, multi-worker SSE manager backed by Redis.

Key fixes over the previous revision
------------------------------------
* One long-lived event-loop per listener thread (no per-message loops).
* Sync Redis calls moved to a background thread/executor so they never
  block an asyncio context.
* Uses a literal channel (``sse_cleanup:all``) for global cleanup instead
  of publishing to the pattern string ``sse_cleanup:*``.
* Lazy global singleton – Redis connection is created the first time the
  manager is actually needed, not at import time.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Any, Dict, Set

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SSEEvent:
    event_type: str
    data: Any

    def encode(self) -> str:
        from vobchat.nodes.utils import safe_json_dumps

        # Ensure no embedded new-lines break the SSE frame
        payload = safe_json_dumps(self.data, default=str).replace("\n", "\\n")
        return f"event: {self.event_type}\ndata: {payload}\n\n"


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class SSEManager:
    """Redis-based Server-Sent-Events hub that works across multiple workers."""

    def __init__(self) -> None:
        logger.info("DEBUG: SSEManager.__init__ called")
        logger.info("SSEManager.__init__ called")
        self._clients: Dict[str, Set[Any]] = {}
        self._lock = Lock()
        self._redis_listener_started = False
        self._listener_loop: asyncio.AbstractEventLoop | None = None
        logger.info("DEBUG: About to start Redis listener")
        self._start_redis_listener()
        logger.info("DEBUG: Redis listener started")

    # ------------------------------------------------------------------
    # Client registration (synchronous – safe for WSGI/Flask entrypoints)
    # ------------------------------------------------------------------

    def add_client(self, thread_id: str, sender: Any) -> None:
        """Register a new SSE sender for *thread_id*."""
        with self._lock:
            # Drop duplicates so each thread keeps at most one live sender.
            if thread_id in self._clients and self._clients[thread_id]:
                logger.warning(
                    "Replacing %d existing SSE clients for thread %s",
                    len(self._clients[thread_id]),
                    thread_id,
                )
                self._clients[thread_id].clear()
            self._clients.setdefault(thread_id, set()).add(sender)
        logger.debug(
            "SSE client added – thread=%s count=%d",
            thread_id,
            self.client_count(thread_id),
        )

    def remove_client(self, thread_id: str, sender: Any) -> None:
        with self._lock:
            if sender in self._clients.get(thread_id, set()):
                self._clients[thread_id].discard(sender)
                if not self._clients[thread_id]:
                    self._clients.pop(thread_id, None)
        logger.debug("SSE client removed – thread=%s", thread_id)

    # ------------------------------------------------------------------
    # Redis pub/sub listener – runs in its own dedicated thread
    # ------------------------------------------------------------------

    def _start_redis_listener(self) -> None:
        if self._redis_listener_started:
            return
        self._redis_listener_started = True

        def _listener_thread() -> None:
            try:
                print("DEBUG: Starting Redis listener thread")
                logger.info("Starting Redis listener thread")

                # local import – optional dependency
                from vobchat.utils.redis_pool import redis_pool_manager

                # Use a simpler approach: no event loop, just process messages synchronously
                redis_client = redis_pool_manager.get_sync_client()
                pubsub = redis_client.pubsub()
                pubsub.psubscribe("sse:*", "sse_cleanup:*", "sse_cleanup:all")

                logger.info("Redis SSE listener started")

                # Process Redis messages in a simple sync loop
                logger.debug("Starting Redis listener loop")
                for message in pubsub.listen():
                    #                    logger.debug("Redis message received: %s", message)
                    if message["type"] != "pmessage":
                        continue

                    channel = (
                        message["channel"].decode("utf-8")
                        if isinstance(message["channel"], bytes)
                        else message["channel"]
                    )
                    data_raw = (
                        message["data"].decode("utf-8")
                        if isinstance(message["data"], bytes)
                        else message["data"]
                    )

                    # logger.debug("Processing Redis message: channel=%s, data=%s", channel, data_raw)
                    # Process message synchronously
                    try:
                        self._process_redis_message_sync(channel, data_raw)
                        # logger.debug("Message processed successfully")
                    except Exception as e:
                        logger.error("Failed to process message: %s", e)
            except Exception as e:
                print(f"DEBUG: Exception in listener thread: {e}")
                logger.exception("Exception in listener thread: %s", e)

        Thread(
            target=_listener_thread, name="SSE-Redis-Listener", daemon=True
        ).start()

    # ------------------------------------------------------------------
    # Redis message handler (runs inside *listener_loop*)
    # ------------------------------------------------------------------

    def _process_redis_message_sync(self, channel: str, data_raw: str) -> None:
        """Synchronous version of message processing"""
        try:
            # logger.debug("_process_redis_message_sync called with channel=%s", channel)
            # logger.debug("Channel type: %s, startswith sse:: %s", type(channel), channel.startswith("sse:"))

            if channel == "sse_cleanup:all":
                # logger.debug("Matched cleanup:all channel")
                payload = json.loads(data_raw)
                keep = payload.get("keep_thread_id")
                # logger.info("Received global cleanup; keeping %s", keep)
                self.cleanup_all_threads_except(keep)
                return

            if channel.startswith("sse_cleanup:"):
                # logger.debug("Matched cleanup channel")
                payload = json.loads(data_raw)
                if payload.get("action") == "cleanup":
                    thread_id = channel.replace("sse_cleanup:", "")
                    # logger.info("Received cleanup for thread %s", thread_id)
                    self._cleanup_local_clients(thread_id)
                return

            if channel.startswith("sse:"):
                # logger.debug("Matched sse: channel")
                thread_id = channel.replace("sse:", "")
                event_data = json.loads(data_raw)
                # logger.debug("Creating SSEEvent: event_type=%s, data=%s", event_data["event_type"], event_data["data"])
                event = SSEEvent(
                    event_type=event_data["event_type"], data=event_data["data"]
                )

                # logger.debug("Broadcasting local event to thread %s", thread_id)
                # Broadcast synchronously
                self._broadcast_local_sync(thread_id, event)
            else:
                logger.debug("Channel did not match any patterns: %s", channel)
        except Exception as exc:  # noqa: BLE001 – log and continue listening
            logger.exception("Error processing Redis message: %s", exc)

    async def _process_redis_message(self, channel: str, data_raw: str) -> None:
        try:
            # logger.debug("_process_redis_message called with channel=%s", channel)
            # logger.debug("Channel type: %s, startswith sse:: %s", type(channel), channel.startswith("sse:"))

            if channel == "sse_cleanup:all":
                # logger.debug("Matched cleanup:all channel")
                payload = json.loads(data_raw)
                keep = payload.get("keep_thread_id")
                # logger.info("Received global cleanup; keeping %s", keep)
                self.cleanup_all_threads_except(keep)
                return

            if channel.startswith("sse_cleanup:"):
                # logger.debug("Matched cleanup channel")
                payload = json.loads(data_raw)
                if payload.get("action") == "cleanup":
                    thread_id = channel.replace("sse_cleanup:", "")
                    logger.info("Received cleanup for thread %s", thread_id)
                    self._cleanup_local_clients(thread_id)
                return

            if channel.startswith("sse:"):
                # logger.debug("Matched sse: channel")
                thread_id = channel.replace("sse:", "")
                event_data = json.loads(data_raw)
                # logger.debug("Creating SSEEvent: event_type=%s, data=%s", event_data["event_type"], event_data["data"])
                event = SSEEvent(
                    event_type=event_data["event_type"], data=event_data["data"]
                )

                # logger.debug("Broadcasting local event to thread %s", thread_id)
                # Create task directly since we're in an async context
                task = asyncio.create_task(
                    self._broadcast_local(thread_id, event)
                )
                # logger.debug("Task created successfully: %s", task)
            else:
                logger.debug("Channel did not match any patterns: %s", channel)
        except Exception as exc:  # noqa: BLE001 – log and continue listening
            logger.exception("Error processing Redis message: %s", exc)

    # ------------------------------------------------------------------
    # Broadcasting helpers
    # ------------------------------------------------------------------

    def _broadcast_local_sync(self, thread_id: str, event: SSEEvent) -> None:
        """Synchronous version of local broadcasting"""
        # logger.debug("_broadcast_local_sync called for thread %s with event %s", thread_id, event.event_type)
        payload = event.encode()
        dead = set()
        with self._lock:
            clients = list(self._clients.get(thread_id, set()))
            # logger.debug("Current clients for thread %s: %d", thread_id, len(clients))
            # logger.debug("All threads with clients: %s", list(self._clients.keys()))

        # logger.debug("Broadcasting to %d clients for thread %s: %s", len(clients), thread_id, payload[:100])
        for sender in clients:
            try:
                # logger.debug("Sending to client: %s", sender)
                sender.send(payload)  # Call synchronously
                # logger.debug("Successfully sent to client")
            except Exception as exc:
                logger.warning("Dropping dead SSE sender: %s", exc)
                dead.add(sender)

        for d in dead:
            self.remove_client(thread_id, d)

    async def _broadcast_local(self, thread_id: str, event: SSEEvent) -> None:
        """Deliver *event* to every local client listening on *thread_id*."""
        # logger.debug("_broadcast_local called for thread %s with event %s", thread_id, event.event_type)
        payload = event.encode()
        dead: Set[Any] = set()
        with self._lock:
            clients = list(self._clients.get(thread_id, set()))
            # logger.debug("Current clients for thread %s: %d", thread_id, len(clients))
            # logger.debug("All threads with clients: %s", list(self._clients.keys()))

        logger.debug(
            "Broadcasting to %d clients for thread %s: %s",
            len(clients),
            thread_id,
            payload[:100],
        )
        for sender in clients:
            try:
                # logger.debug("Sending to client: %s", sender)
                maybe_coro = sender.send(payload)
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
                # logger.debug("Successfully sent to client")
            except Exception:  # noqa: BLE001 – network can fail
                # logger.warning("Dropping dead SSE sender: %s", exc)
                dead.add(sender)

        for d in dead:
            self.remove_client(thread_id, d)

    async def _publish_to_redis(self, thread_id: str, event: SSEEvent) -> None:
        """Publish *event* on Redis without blocking the asyncio loop."""
        from vobchat.utils.redis_pool import redis_pool_manager

        event_data = {
            "event_type": event.event_type,
            "data": event.data,
            "timestamp": time.time(),
        }
        channel = f"sse:{thread_id}"
        # Off-load sync publish to the default executor (thread-pool)
        loop = asyncio.get_running_loop()
        from vobchat.nodes.utils import safe_json_dumps

        await loop.run_in_executor(
            None,
            lambda: redis_pool_manager.get_sync_client().publish(
                channel, safe_json_dumps(event_data)
            ),
        )
        # logger.debug("Published %s event to Redis for thread %s",
        # event.event_type, thread_id)

    async def _broadcast(self, thread_id: str, event: SSEEvent) -> None:
        await self._publish_to_redis(thread_id, event)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    async def send(self, thread_id: str, event_type: str, data: Any) -> None:
        await self._broadcast(thread_id, SSEEvent(event_type, data))

    async def message(self, thread_id: str, text: str) -> None:
        await self.send(thread_id, "message", {"content": text})

    async def state(self, thread_id: str, state: dict) -> None:
        await self.send(thread_id, "state_update", {"state": state})

    async def interrupt(self, thread_id: str, payload: dict) -> None:
        await self.send(thread_id, "interrupt", payload)

    async def error(self, thread_id: str, err: str) -> None:
        await self.send(thread_id, "error", {"error": err})

    # ------------------------------------------------------------------
    # Cleanup helpers – may be called from any thread
    # ------------------------------------------------------------------

    def _cleanup_local_clients(self, thread_id: str) -> None:
        with self._lock:
            if self._clients.get(thread_id):
                logger.info(
                    "Cleaning up %d local SSE clients for thread %s",
                    len(self._clients[thread_id]),
                    thread_id,
                )
                self._clients.pop(thread_id, None)

    def broadcast_cleanup_signal(self, thread_id: str) -> None:
        """Ask *all* workers to drop clients for *thread_id*."""
        from vobchat.utils.redis_pool import redis_pool_manager

        from vobchat.nodes.utils import safe_json_dumps

        cleanup_message = safe_json_dumps(
            {
                "action": "cleanup",
                "thread_id": thread_id,
                "timestamp": time.time(),
            }
        )
        redis_pool_manager.get_sync_client().publish(
            f"sse_cleanup:{thread_id}", cleanup_message
        )
        logger.info("Broadcast cleanup signal for thread %s", thread_id)

    def broadcast_cleanup_all_except(
        self, keep_thread_id: str | None = None
    ) -> None:
        """Ask workers to drop clients for every thread *except* ``keep_thread_id``."""
        from vobchat.utils.redis_pool import redis_pool_manager

        from vobchat.nodes.utils import safe_json_dumps

        cleanup_message = safe_json_dumps(
            {
                "action": "cleanup_all_except",
                "keep_thread_id": keep_thread_id,
                "timestamp": time.time(),
            }
        )
        redis_pool_manager.get_sync_client().publish(
            "sse_cleanup:all", cleanup_message
        )
        logger.info("Broadcast global cleanup (keeping %s)", keep_thread_id)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def client_count(self, thread_id: str) -> int:
        with self._lock:
            return len(self._clients.get(thread_id, set()))

    def get_all_active_threads(self) -> Dict[str, int]:
        with self._lock:
            return {
                tid: len(clients)
                for tid, clients in self._clients.items()
                if clients
            }

    def cleanup_all_threads_except(self, keep_thread_id: str | None) -> int:
        cleaned = 0
        with self._lock:
            for tid in list(self._clients):
                if keep_thread_id is None or tid != keep_thread_id:
                    count = len(self._clients[tid])
                    if count:
                        logger.info(
                            "Cleaning up %d SSE clients for old thread %s",
                            count,
                            tid,
                        )
                        cleaned += count
                        self._clients.pop(tid, None)
        return cleaned


# ---------------------------------------------------------------------------
# Lazy singleton helper
# ---------------------------------------------------------------------------

_simple_manager: SSEManager | None = None


def get_sse_manager() -> SSEManager:
    """Return the process-wide *singleton* ``SSEManager`` instance."""
    global _simple_manager
    if _simple_manager is None:
        print("DEBUG: Creating SSEManager singleton")
        logger.info("Creating SSEManager singleton")
        _simple_manager = SSEManager()
    return _simple_manager
