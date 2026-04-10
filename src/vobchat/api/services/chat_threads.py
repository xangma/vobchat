from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from functools import lru_cache
from threading import RLock
from uuid import uuid4

from vobchat.api.schemas.chat import (
    AnalysisReceipt,
    ChatSSEEventName,
    ChatThreadState,
    ErrorEventData,
    RenderProjection,
    SSEEventPayload,
    UIProjection,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryChatThreadStore:
    def __init__(self) -> None:
        self._lock = RLock()
        self._threads: dict[str, ChatThreadState] = {}
        self._receipts_by_thread: dict[str, dict[str, AnalysisReceipt]] = defaultdict(dict)
        self._receipt_ids_by_thread: dict[str, list[str]] = defaultdict(list)
        self._subscribers: dict[str, set[asyncio.Queue[SSEEventPayload]]] = defaultdict(set)
        self._sequence_by_thread: dict[str, int] = defaultdict(int)
        self._recent_receipt_limit = 5

    def _ordered_receipts_locked(self, thread_id: str) -> list[AnalysisReceipt]:
        receipt_ids = self._receipt_ids_by_thread.get(thread_id, [])
        receipts = self._receipts_by_thread.get(thread_id, {})
        return [
            receipts[receipt_id].model_copy(deep=True)
            for receipt_id in receipt_ids
            if receipt_id in receipts
        ]

    def _hydrate_receipts_locked(self, state: ChatThreadState) -> ChatThreadState:
        hydrated = state.model_copy(deep=True)
        recent_receipts = self._ordered_receipts_locked(hydrated.thread_id)[: self._recent_receipt_limit]
        hydrated.recent_receipts = recent_receipts
        hydrated.current_receipt = recent_receipts[0] if recent_receipts else None
        hydrated.current_receipt_id = (
            hydrated.current_receipt.receipt_id if hydrated.current_receipt is not None else None
        )
        hydrated.conversation_state.current_receipt_id = hydrated.current_receipt_id
        hydrated.conversation_state.recent_receipt_ids = [
            receipt.receipt_id for receipt in recent_receipts
        ]
        return hydrated

    def create_thread(self) -> ChatThreadState:
        now = utc_now()
        state = ChatThreadState(
            thread_id=str(uuid4()),
            created_at=now,
            updated_at=now,
            ui_projection=UIProjection(projection_id=f"ui_{uuid4().hex[:12]}"),
            render_projection=RenderProjection(projection_id=f"rp_{uuid4().hex[:12]}"),
        )
        with self._lock:
            self._receipts_by_thread[state.thread_id] = {}
            self._receipt_ids_by_thread[state.thread_id] = []
            hydrated = self._hydrate_receipts_locked(state)
            self._threads[state.thread_id] = hydrated.model_copy(deep=True)
        return hydrated.model_copy(deep=True)

    def get_thread(self, thread_id: str) -> ChatThreadState | None:
        with self._lock:
            state = self._threads.get(thread_id)
            return self._hydrate_receipts_locked(state).model_copy(deep=True) if state else None

    def save_thread(self, state: ChatThreadState) -> ChatThreadState:
        with self._lock:
            updated = state.model_copy(deep=True)
            updated.updated_at = utc_now()
            updated = self._hydrate_receipts_locked(updated)
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
            working = self._hydrate_receipts_locked(working)
            self._threads[thread_id] = working.model_copy(deep=True)
            return working.model_copy(deep=True)

    def require_thread(self, thread_id: str) -> ChatThreadState:
        state = self.get_thread(thread_id)
        if state is None:
            raise KeyError(thread_id)
        return state

    def save_receipt(self, thread_id: str, receipt: AnalysisReceipt) -> AnalysisReceipt:
        stored = receipt.model_copy(deep=True)
        with self._lock:
            if thread_id not in self._threads:
                raise KeyError(thread_id)
            receipts = self._receipts_by_thread[thread_id]
            receipts[stored.receipt_id] = stored.model_copy(deep=True)

            receipt_ids = self._receipt_ids_by_thread[thread_id]
            if stored.receipt_id in receipt_ids:
                receipt_ids.remove(stored.receipt_id)
            receipt_ids.insert(0, stored.receipt_id)

            thread_state = self._hydrate_receipts_locked(self._threads[thread_id])
            self._threads[thread_id] = thread_state.model_copy(deep=True)
        return stored.model_copy(deep=True)

    def get_receipt(self, thread_id: str, receipt_id: str) -> AnalysisReceipt | None:
        with self._lock:
            receipt = self._receipts_by_thread.get(thread_id, {}).get(receipt_id)
            return receipt.model_copy(deep=True) if receipt is not None else None

    def get_latest_receipt(self, thread_id: str) -> AnalysisReceipt | None:
        with self._lock:
            receipts = self._ordered_receipts_locked(thread_id)
            return receipts[0].model_copy(deep=True) if receipts else None

    def list_receipts(
        self,
        thread_id: str,
        *,
        limit: int | None = None,
    ) -> list[AnalysisReceipt]:
        with self._lock:
            receipts = self._ordered_receipts_locked(thread_id)
            if limit is not None:
                receipts = receipts[:limit]
            return [receipt.model_copy(deep=True) for receipt in receipts]

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
