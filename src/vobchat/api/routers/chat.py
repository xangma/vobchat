from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from vobchat.api.schemas.chat import (
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatTurnAcceptedResponse,
    ChatTurnRequest,
    ChatTurnResponse,
)
from vobchat.api.services.chat_orchestrator import ChatOrchestrator, get_chat_orchestrator
from vobchat.api.services.chat_threads import InMemoryChatThreadStore, get_chat_thread_store
from vobchat.api.streaming import encode_sse_event, sse_keepalive


router = APIRouter(prefix="/chat", tags=["chat"])


@router.post("/threads", response_model=ChatThreadCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_thread(
    orchestrator: Annotated[ChatOrchestrator, Depends(get_chat_orchestrator)],
) -> ChatThreadCreateResponse:
    return await orchestrator.create_thread()


@router.get("/threads/{thread_id}", response_model=ChatThreadState)
def get_thread(
    thread_id: str,
    orchestrator: Annotated[ChatOrchestrator, Depends(get_chat_orchestrator)],
) -> ChatThreadState:
    state = orchestrator.get_thread_state(thread_id)
    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Thread '{thread_id}' was not found",
        )
    return state


@router.post("/turn", response_model=ChatTurnAcceptedResponse | ChatTurnResponse)
async def submit_turn(
    request: ChatTurnRequest,
    orchestrator: Annotated[ChatOrchestrator, Depends(get_chat_orchestrator)],
) -> ChatTurnAcceptedResponse | ChatTurnResponse:
    if orchestrator.get_thread_state(request.thread_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Thread '{request.thread_id}' was not found",
        )
    if request.stream:
        try:
            return await orchestrator.start_streamed_turn(request)
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Thread '{request.thread_id}' was not found",
            ) from exc
    return await orchestrator.handle_turn(request, emit_events=False)


@router.get("/stream/{thread_id}")
async def stream_thread_events(
    thread_id: str,
    thread_store: Annotated[InMemoryChatThreadStore, Depends(get_chat_thread_store)],
) -> StreamingResponse:
    if thread_store.get_thread(thread_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Thread '{thread_id}' was not found",
        )

    async def event_stream():
        queue = thread_store.subscribe(thread_id)
        try:
            yield sse_keepalive()
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield encode_sse_event(payload)
                except asyncio.TimeoutError:
                    yield sse_keepalive()
        finally:
            thread_store.unsubscribe(thread_id, queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
