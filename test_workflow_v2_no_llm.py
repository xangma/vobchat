from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatTurnRequest,
    DegradedModeOutcome,
    ProvenanceResultType,
    ReceiptKind,
    StatePatchType,
    WorkflowRuntimeMode,
)
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from vobchat.core.llm.client import LLMConfigurationError
from vobchat.core.llm.planner import ChatPlanner
from vobchat.web.state import hydrate_states_from_thread, status_banner_from_state
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)
from test_chat_phase4 import (
    AmbiguousPlacesService,
    build_orchestrator,
    make_resolved_place,
    seed_analysis_receipt,
)


class NoLLMClient:
    def __init__(self, message: str = "missing llm config") -> None:
        self.exc = LLMConfigurationError(message)

    async def complete_json(self, messages, response_model, *, temperature=0.0):
        raise self.exc

    async def complete_text(self, messages, *, temperature=None, max_tokens=None):
        raise self.exc

    async def stream_text(self, messages, *, temperature=None, max_tokens=None):
        raise self.exc
        yield ""


def build_no_llm_orchestrator(
    *,
    store: InMemoryChatThreadStore | None = None,
    places_service=None,
):
    no_llm_client = NoLLMClient()
    planner = ChatPlanner(llm_client=no_llm_client)
    return build_orchestrator(
        store=store,
        planner=planner,
        llm_client=no_llm_client,
        places_service=places_service,
    )


def test_v2_no_llm_exact_place_lookup_is_useful_and_explicit() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="York", stream=False)
        )
    )

    assert response.planner_result.action.operation == ChatOperation.RESOLVE_PLACE
    assert response.thread_state.selected_places[0].place.name == "York"
    assert response.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    assert (
        response.thread_state.runtime_state.degraded_outcome
        == DegradedModeOutcome.DETERMINISTIC_SUCCESS
    )
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY
    assert response.thread_state.current_receipt.provenance_summary is not None
    assert (
        response.thread_state.current_receipt.provenance_summary.runtime_mode
        == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    )
    assert any("deterministic degraded mode" in notice.lower() for notice in response.ui_delta.notices)


def test_v2_no_llm_exact_theme_lookup_and_info_lookup_work() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = store.create_thread()

    theme_response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="population", stream=False)
        )
    )
    assert theme_response.planner_result.action.operation == ChatOperation.RESOLVE_THEME
    assert theme_response.thread_state.selected_theme is not None
    assert theme_response.thread_state.selected_theme.theme_id == "T_POP"
    assert theme_response.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED

    info_response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="what is N_TOT_POP", stream=False)
        )
    )
    assert info_response.planner_result.action.operation == ChatOperation.FETCH_DATA_ENTITY_INFO
    assert info_response.thread_state.current_receipt is not None
    assert info_response.thread_state.current_receipt.kind == ReceiptKind.INFO
    assert info_response.thread_state.current_receipt.provenance_summary is not None
    assert (
        info_response.thread_state.current_receipt.provenance_summary.result_type
        == ProvenanceResultType.INFO_LOOKUP
    )
    assert (
        info_response.thread_state.current_receipt.provenance_summary.runtime_mode
        == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    )


def test_v2_no_llm_receipt_backed_follow_up_still_works_safely() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="show the table", stream=False)
        )
    )

    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.ANALYSIS
    assert response.thread_state.analysis_state.output_mode == "table"
    assert response.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    assert response.thread_state.current_receipt.provenance_summary is not None
    assert (
        response.thread_state.current_receipt.provenance_summary.source_context.follow_up_patch
        == StatePatchType.SET_OUTPUT_MODE
    )
    assert (
        response.thread_state.current_receipt.provenance_summary.runtime_mode
        == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    )


def test_v2_no_llm_guided_clarification_uses_known_candidates() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(
        store=store,
        places_service=AmbiguousPlacesService(),
    )
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="Newport", stream=False)
        )
    )

    assert response.planner_result.action.operation == ChatOperation.CLARIFY
    assert response.ui_delta.pending_clarification is not None
    assert len(response.ui_delta.pending_clarification.options) == 2
    assert len(response.ui_delta.place_search_results) == 2
    assert response.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    assert (
        response.thread_state.runtime_state.degraded_outcome
        == DegradedModeOutcome.GUIDED_CLARIFICATION
    )


def test_v2_no_llm_unsupported_freeform_request_is_explicit_and_actionable() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="could you compare York and Leeds over time?",
                stream=False,
            )
        )
    )

    assert response.planner_result.action.operation == ChatOperation.REPLY_ONLY
    assert response.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    assert response.thread_state.runtime_state.degraded_outcome == DegradedModeOutcome.SETUP_GUIDANCE
    assert any("exact place, theme, or metadata request" in notice.lower() for notice in response.ui_delta.notices)
    assert any("setup-llm" in notice for notice in response.ui_delta.notices)


def test_v2_no_llm_runtime_state_and_notices_hydrate_from_projections() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="York", stream=False)
        )
    )

    selection_state, visualization_state, metadata_state, map_state = hydrate_states_from_thread(
        thread_state_data=response.thread_state.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )
    error, notices = status_banner_from_state(
        selection_state=selection_state,
        request_status={"error": None},
    )

    assert error is None
    assert selection_state["runtime_state"]["mode"] == "deterministic_degraded"
    assert selection_state["provenance_summary"]["runtime_mode"] == "deterministic_degraded"
    assert visualization_state["provenance_summary"]["runtime_mode"] == "deterministic_degraded"
    assert metadata_state["provenance_summary"]["runtime_mode"] == "deterministic_degraded"
    assert map_state["provenance_summary"]["runtime_mode"] == "deterministic_degraded"
    assert any("deterministic degraded mode" in notice.lower() for notice in notices)
