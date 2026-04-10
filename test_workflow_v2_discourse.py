from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    AnalysisReceipt,
    AnalysisSpec,
    AvailabilityStatus,
    ChatOperation,
    ChatTurnRequest,
    ComparabilityStatus,
    DatasetFamilyRef,
    DegradedModeOutcome,
    DiscoveryTopic,
    ExecutionGuardOutcome,
    ExactSliceRef,
    OutputFeasibilityStatus,
    ProvenanceResolvedContext,
    ProvenanceResultType,
    ProvenanceSummary,
    ReceiptKind,
    ReportingGeographyRef,
    ReportingGeographyStatus,
    SliceStatus,
    StatePatchType,
    TimeScope,
    WorkflowPath,
    WorkflowRuntimeMode,
)
from vobchat.api.schemas.places import PlaceCandidateResponse, PlaceSearchResponse, ResolvedPlaceResponse, UnitDetailResponse
from vobchat.api.schemas.themes import ThemeSummaryResponse
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from test_chat_phase4 import (
    FakeLLMClient,
    FakePlacesService,
    build_orchestrator,
    make_multi_geography_place,
    make_resolved_place,
    seed_analysis_receipt,
    utc_now,
)
from test_workflow_v2_no_llm import build_no_llm_orchestrator


class RichPlacesService(FakePlacesService):
    def search_places(self, query) -> PlaceSearchResponse:
        lowered = query.query.lower()
        if lowered == "bradford":
            return PlaceSearchResponse(
                query=query.query,
                match_mode=query.match_mode,
                result_count=1,
                results=[
                    PlaceCandidateResponse(
                        place_id=5,
                        name="Bradford",
                        county_name="Yorkshire",
                        unit_ids=[103],
                        unit_types=["MOD_DIST"],
                        match_type=query.match_mode,
                    )
                ],
            )
        return super().search_places(query)

    def resolve_place(self, place_id, query) -> ResolvedPlaceResponse | None:
        if place_id == 5:
            return ResolvedPlaceResponse(
                place=PlaceCandidateResponse(
                    place_id=5,
                    name="Bradford",
                    unit_ids=[103],
                    unit_types=["MOD_DIST"],
                    match_type="resolved",
                ),
                units=[
                    UnitDetailResponse(
                        unit_id=103,
                        unit_name="Bradford",
                        unit_type="MOD_DIST",
                        unit_type_label="Modern District",
                    )
                ],
            )
        return super().resolve_place(place_id, query)


def _seed_recent_result_anchors(store: InMemoryChatThreadStore, orchestrator, thread_id: str) -> None:
    state = store.require_thread(thread_id)
    state.conversation_state.current_focus = "analysis"
    state.conversation_state.discourse_anchors = orchestrator._build_discourse_anchors(state)
    store.save_thread(state)


def _seed_supported_tot_pop_receipt(store: InMemoryChatThreadStore):
    thread = store.create_thread()
    place = make_resolved_place()
    spec = AnalysisSpec(
        places=[],
        reporting_geography=ReportingGeographyRef(
            unit_type="MOD_DIST",
            unit_ids=[101],
            label="Modern District",
            slot_status="resolved",
        ),
        dataset_family=DatasetFamilyRef(
            theme_id="T_POP",
            label="Population",
            description="Population counts",
            slot_status="resolved",
        ),
        exact_slice=ExactSliceRef(
            cube_id="N_TOT_POP",
            cube_ids=["N_TOT_POP"],
            label="Current Total Population",
            cellref="TOT_POP:now",
            dataitem_id="N_TOT_POP_4",
            cat_id="C_TOT_POP_4",
            start_year=1801,
            end_year=1901,
            slot_status="resolved",
            source="test",
        ),
        time_scope=TimeScope(mode="snapshot", year=1801, label="1801", source="test"),
        output_mode="trend_chart",
        comparison_mode="single_place",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status="resolved",
        slice_status=SliceStatus.UNIQUE_EXECUTABLE_SLICE,
        comparability_status=ComparabilityStatus.COMPARABLE,
        availability_status=AvailabilityStatus.AVAILABLE,
        output_feasibility_status=OutputFeasibilityStatus.AVAILABLE,
    )
    receipt = AnalysisReceipt(
        kind=ReceiptKind.ANALYSIS,
        receipt_id="ar_tot_pop",
        path=WorkflowPath.ANALYSIS,
        source_operation=ChatOperation.FETCH_TIME_SERIES,
        analysis_spec=spec,
        created_at=utc_now(),
    )
    thread.selected_places = [place]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread.analysis_state.resolved_places = [place]
    thread.analysis_state.reporting_geography = spec.reporting_geography
    thread.analysis_state.dataset_family = spec.dataset_family
    thread.analysis_state.candidate_slice = spec.exact_slice
    thread.analysis_state.time_scope = spec.time_scope
    thread.analysis_state.output_mode = spec.output_mode
    thread.analysis_state.availability_status = spec.availability_status
    thread.analysis_state.comparability_status = spec.comparability_status
    thread.analysis_state.analysis_spec = spec
    thread.current_receipt = receipt
    thread.current_receipt_id = receipt.receipt_id
    thread.recent_receipts = [receipt]
    thread = store.save_thread(thread)
    store.save_receipt(thread.thread_id, receipt)
    return store.require_thread(thread.thread_id)


def test_v2_discourse_group_place_reference_generates_multi_place_replace_patch() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        places_service=RichPlacesService(),
        llm_client=FakeLLMClient(text="Handled."),
    )
    thread = seed_analysis_receipt(store, places=[make_resolved_place()], year=1801)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="same for Leeds and Bradford",
                stream=False,
            )
        )
    )

    assert response.planner_result.action.operation == ChatOperation.FETCH_TIME_SERIES
    assert {place.place.name for place in response.thread_state.selected_places} == {"Leeds", "Bradford"}
    assert response.ui_delta.execution_guard_result is not None
    assert (
        response.ui_delta.execution_guard_result.outcome
        == ExecutionGuardOutcome.RUNNABLE_ANALYSIS_SPEC
    )
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.provenance_summary is not None
    assert (
        response.thread_state.current_receipt.provenance_summary.source_context.follow_up_patch
        == StatePatchType.REPLACE_PLACE
    )


def test_v2_discourse_ordinal_reference_can_select_exact_slice_option_from_discovery() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    first = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what population breakdowns are available?",
                stream=False,
            )
        )
    )

    assert first.thread_state.conversation_state.discourse_anchors.option_sets
    assert first.ui_delta.discovery_result is not None
    assert first.ui_delta.discovery_result.topic == DiscoveryTopic.EXACT_SLICE_OPTIONS

    second = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show the second option",
                stream=False,
            )
        )
    )

    assert second.planner_result.action.operation == ChatOperation.LIST_AVAILABLE_YEARS
    assert second.ui_delta.discovery_result is not None
    assert second.ui_delta.discovery_result.topic == DiscoveryTopic.AVAILABLE_YEARS
    assert second.ui_delta.discovery_result.available_years
    assert second.thread_state.current_receipt is not None
    assert second.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_discourse_can_select_reporting_geography_by_ordinal_option() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread = store.save_thread(thread)

    first = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="which geographies can I use here?",
                stream=False,
            )
        )
    )

    assert first.ui_delta.discovery_result is not None
    assert first.ui_delta.discovery_result.topic == DiscoveryTopic.REPORTING_GEOGRAPHIES
    expected_unit_type = first.ui_delta.discovery_result.reporting_geographies[1].unit_type

    second = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="pick the second geography",
                stream=False,
            )
        )
    )

    assert second.planner_result.action.operation == ChatOperation.LIST_THEMES
    assert second.thread_state.analysis_state.reporting_geography is not None
    assert second.thread_state.analysis_state.reporting_geography.unit_type == expected_unit_type
    assert second.ui_delta.discovery_result is not None
    assert second.ui_delta.discovery_result.topic == DiscoveryTopic.THEMES


def test_v2_discourse_latest_year_reuses_discovered_year_context_without_bypassing_guard() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = _seed_supported_tot_pop_receipt(store)

    first = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what years are available for this?",
                stream=False,
            )
        )
    )

    assert first.ui_delta.discovery_result is not None
    assert first.ui_delta.discovery_result.topic == DiscoveryTopic.AVAILABLE_YEARS

    second = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="use the latest year",
                stream=False,
            )
        )
    )

    assert second.planner_result.action.operation == ChatOperation.FETCH_TIME_SERIES
    assert second.ui_delta.execution_guard_result is not None
    assert (
        second.ui_delta.execution_guard_result.outcome
        == ExecutionGuardOutcome.RUNNABLE_ANALYSIS_SPEC
    )
    assert second.thread_state.analysis_state.time_scope is not None
    assert second.thread_state.analysis_state.time_scope.year == 1901
    assert second.thread_state.current_receipt is not None
    assert second.thread_state.current_receipt.analysis_spec is not None
    assert second.thread_state.current_receipt.analysis_spec.time_scope.year == 1901


def test_v2_discourse_ambiguous_multi_receipt_reference_requires_clarification() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = store.create_thread()

    analysis_receipt = AnalysisReceipt(
        kind=ReceiptKind.ANALYSIS,
        receipt_id="ar_mod_dist",
        path=WorkflowPath.ANALYSIS,
        source_operation=ChatOperation.FETCH_TIME_SERIES,
        provenance_summary=ProvenanceSummary(
            result_type=ProvenanceResultType.EXECUTED_ANALYSIS,
            receipt_kind=ReceiptKind.ANALYSIS,
            workflow_path=WorkflowPath.ANALYSIS,
            executed_analysis=True,
            resolved_context=ProvenanceResolvedContext(
                reporting_geography_label="Modern District",
                reporting_unit_type="MOD_DIST",
                output_mode="trend_chart",
            ),
        ),
        created_at=utc_now(),
    )
    discovery_receipt = AnalysisReceipt(
        kind=ReceiptKind.DISCOVERY,
        receipt_id="dr_parish",
        path=WorkflowPath.DISCOVERY,
        source_operation=ChatOperation.LIST_THEMES,
        provenance_summary=ProvenanceSummary(
            result_type=ProvenanceResultType.CATALOG_DISCOVERY,
            receipt_kind=ReceiptKind.DISCOVERY,
            workflow_path=WorkflowPath.DISCOVERY,
            discovery_only=True,
            resolved_context=ProvenanceResolvedContext(
                reporting_geography_label="Parish",
                reporting_unit_type="PAR_UNIT",
                output_mode="discovery",
            ),
        ),
        created_at=utc_now(),
    )
    store.save_receipt(thread.thread_id, analysis_receipt)
    store.save_receipt(thread.thread_id, discovery_receipt)
    _seed_recent_result_anchors(store, orchestrator, thread.thread_id)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="use that result",
                stream=False,
            )
        )
    )

    assert response.planner_result.action.operation == ChatOperation.CLARIFY
    assert response.ui_delta.pending_clarification is not None
    assert response.ui_delta.pending_clarification.slot == "receipt_reference"
    assert response.ui_delta.pending_clarification.options


def test_v2_discourse_degraded_mode_keeps_ordinal_references_narrow() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_no_llm_orchestrator(store=store)
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread = store.save_thread(thread)

    first = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="which geographies can I use here?",
                stream=False,
            )
        )
    )

    assert first.ui_delta.discovery_result is not None

    second = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="the second one",
                stream=False,
            )
        )
    )

    assert second.planner_result.action.operation == ChatOperation.CLARIFY
    assert second.thread_state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
    assert (
        second.thread_state.runtime_state.degraded_outcome
        == DegradedModeOutcome.GUIDED_CLARIFICATION
    )
    assert second.ui_delta.pending_clarification is not None
    assert "choose the option by label" in second.ui_delta.pending_clarification.question.lower()
