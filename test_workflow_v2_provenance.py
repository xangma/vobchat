from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatTurnRequest,
    ExecutionGuardOutcome,
    PlannerAction,
    ProvenanceResultType,
    ReceiptKind,
    StatePatchType,
)
from vobchat.api.schemas.themes import ThemeSummaryResponse
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from vobchat.web.state import hydrate_states_from_thread, status_banner_from_state
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)
from test_chat_phase4 import (
    FakeLLMClient,
    StaticPlanner,
    build_orchestrator,
    make_resolved_place,
    seed_analysis_receipt,
)


def test_v2_analysis_receipt_carries_compact_provenance_summary() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                cube_query="total population",
                year=1901,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Loaded total population."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show the total population for York in 1901",
                stream=False,
            )
        )
    )

    receipt = response.thread_state.current_receipt
    assert receipt is not None
    assert receipt.kind == ReceiptKind.ANALYSIS
    assert receipt.provenance_summary is not None
    assert receipt.provenance_summary.result_type == ProvenanceResultType.EXECUTED_ANALYSIS
    assert receipt.provenance_summary.executed_analysis is True
    assert receipt.provenance_summary.discovery_only is False
    assert receipt.provenance_summary.exact_slice is not None
    assert receipt.provenance_summary.exact_slice.dataitem_id == "N_TOT_POP_4"
    assert receipt.provenance_summary.source_context.policy_outcome.value == "execute"
    assert (
        receipt.provenance_summary.source_context.guard_outcome
        == ExecutionGuardOutcome.RUNNABLE_ANALYSIS_SPEC
    )
    assert receipt.ui_projection is not None
    assert receipt.ui_projection.provenance_summary is not None
    assert receipt.render_projection is not None
    assert receipt.render_projection.provenance_summary is not None
    assert any(
        "validated analysis spec" in note.lower()
        for note in receipt.provenance_summary.capability_notes
    )


def test_v2_discovery_receipt_carries_discovery_specific_provenance() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        llm_client=FakeLLMClient(text="Breakdown options loaded."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what population breakdowns are available?",
                stream=False,
            )
        )
    )

    receipt = response.thread_state.current_receipt
    assert receipt is not None
    assert receipt.kind == ReceiptKind.DISCOVERY
    assert receipt.provenance_summary is not None
    assert receipt.provenance_summary.result_type == ProvenanceResultType.CATALOG_DISCOVERY
    assert receipt.provenance_summary.discovery_only is True
    assert receipt.provenance_summary.executed_analysis is False
    assert receipt.provenance_summary.exact_slice is None
    assert any(
        "not executed analysis" in note.lower()
        for note in receipt.provenance_summary.capability_notes
    )
    assert receipt.discovery_result is not None
    assert receipt.provenance_summary.availability_limits == receipt.discovery_result.availability_notes


def test_v2_info_receipt_carries_info_lookup_provenance() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_PLACE_PROFILE,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Here is the place profile."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="tell me about this place",
                stream=False,
            )
        )
    )

    receipt = response.thread_state.current_receipt
    assert receipt is not None
    assert receipt.kind == ReceiptKind.INFO
    assert receipt.provenance_summary is not None
    assert receipt.provenance_summary.result_type == ProvenanceResultType.INFO_LOOKUP
    assert any(
        "info lookup" in note.lower() for note in receipt.provenance_summary.capability_notes
    )
    assert receipt.provenance_summary.source_context.source_operation == ChatOperation.FETCH_PLACE_PROFILE


def test_v2_follow_up_output_switch_updates_provenance_with_anchor() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Here is the table."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="show the table", stream=False)
        )
    )

    receipt = response.thread_state.current_receipt
    assert receipt is not None
    assert receipt.provenance_summary is not None
    assert receipt.provenance_summary.source_context.source_receipt_id == "ar_seed"
    assert receipt.provenance_summary.source_context.follow_up_patch == StatePatchType.SET_OUTPUT_MODE
    assert receipt.provenance_summary.resolved_context.output_mode == "table"
    assert receipt.provenance_summary.executed_analysis is True


def test_v2_boundary_map_downgrade_is_recorded_in_provenance() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Mapped it."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="map that", stream=False)
        )
    )

    receipt = response.thread_state.current_receipt
    assert receipt is not None
    assert receipt.provenance_summary is not None
    assert any(
        "boundary-map-only downgrade" in note.lower()
        for note in receipt.provenance_summary.safe_downgrades
    )
    assert "boundary_map_without_measure" in receipt.provenance_summary.defaults_used


def test_v2_projections_and_hydration_surface_provenance_summary() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                cube_query="total population",
                year=1901,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Loaded total population."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show the total population for York in 1901",
                stream=False,
            )
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
    assert selection_state["provenance_summary"]["result_type"] == "executed_analysis"
    assert visualization_state["provenance_summary"]["result_type"] == "executed_analysis"
    assert metadata_state["provenance_summary"]["result_type"] == "executed_analysis"
    assert map_state["provenance_summary"]["result_type"] == "executed_analysis"
    assert not any("executed" in notice.lower() for notice in notices)
