from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from functools import lru_cache
from threading import RLock
from uuid import uuid4

from vobchat.api.schemas.chat import (
    ChatSSEEventName,
    ChatThreadState,
    ErrorEventData,
    SSEEventPayload,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryChatThreadStore:
    def __init__(self) -> None:
        self._lock = RLock()
        self._threads: dict[str, ChatThreadState] = {}
        self._subscribers: dict[str, set[asyncio.Queue[SSEEventPayload]]] = defaultdict(set)
        self._sequence_by_thread: dict[str, int] = defaultdict(int)

    def create_thread(self) -> ChatThreadState:
        now = utc_now()
        state = ChatThreadState(
            thread_id=str(uuid4()),
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._threads[state.thread_id] = state.model_copy(deep=True)
        return state.model_copy(deep=True)

    def get_thread(self, thread_id: str) -> ChatThreadState | None:
        with self._lock:
            state = self._threads.get(thread_id)
            return state.model_copy(deep=True) if state else None

    def save_thread(self, state: ChatThreadState) -> ChatThreadState:
        updated = state.model_copy(deep=True)
        updated.updated_at = utc_now()
        with self._lock:
            self._threads[updated.thread_id] = updated.model_copy(deep=True)
        return updated.model_copy(deep=True)

    def update_thread(
        self,
        thread_id: str,
        updater: Callable[[ChatThreadState], None],
    ) -> ChatThreadState:
        with self._lock:
            existing = self._threads.get(thread_id)
            if existing is None:
                raise KeyError(thread_id)
            working = existing.model_copy(deep=True)
            updater(working)
            working.updated_at = utc_now()
            self._threads[thread_id] = working.model_copy(deep=True)
            return working.model_copy(deep=True)

    def require_thread(self, thread_id: str) -> ChatThreadState:
        state = self.get_thread(thread_id)
        if state is None:
            raise KeyError(thread_id)
        return state

    def subscribe(self, thread_id: str) -> asyncio.Queue[SSEEventPayload]:
        queue: asyncio.Queue[SSEEventPayload] = asyncio.Queue()
        with self._lock:
            self._subscribers[thread_id].add(queue)
        return queue

    def unsubscribe(self, thread_id: str, queue: asyncio.Queue[SSEEventPayload]) -> None:
        with self._lock:
            listeners = self._subscribers.get(thread_id)
            if not listeners:
                return
            listeners.discard(queue)
            if not listeners:
                self._subscribers.pop(thread_id, None)

    async def publish_event(
        self,
        thread_id: str,
        event: ChatSSEEventName,
        data: SSEEventPayload["data"],
    ) -> SSEEventPayload:
        with self._lock:
            self._sequence_by_thread[thread_id] += 1
            payload = SSEEventPayload(
                event=event,
                thread_id=thread_id,
                sequence=self._sequence_by_thread[thread_id],
                timestamp=utc_now(),
                data=data,
            )
            subscribers = list(self._subscribers.get(thread_id, set()))

        for queue in subscribers:
            await queue.put(payload)
        return payload

    async def publish_error(
        self,
        thread_id: str,
        error: str,
        *,
        turn_id: str | None = None,
    ) -> SSEEventPayload:
        return await self.publish_event(
            thread_id,
            ChatSSEEventName.ERROR,
            ErrorEventData(turn_id=turn_id, error=error),
        )


@lru_cache(maxsize=1)
def get_chat_thread_store() -> InMemoryChatThreadStore:
    return InMemoryChatThreadStore()
