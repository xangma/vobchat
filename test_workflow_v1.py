from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from vobchat.api.schemas.chat import (
    AnalysisReceipt,
    ChatOperation,
    ChatThreadState,
    ChatTurnRequest,
    ExecutionGuardOutcome,
    OutputFeasibilityStatus,
    PlannerAction,
    ReceiptKind,
    RenderProjection,
    ReportingGeographyRef,
    StatePatchType,
    UIProjection,
    WorkflowPath,
)
from vobchat.api.schemas.themes import CubeSummaryResponse, ThemeSummaryResponse
from vobchat.api.services.chat_followups import ReferenceResolver, StatePatchService
from vobchat.api.services.chat_safety import CapabilityRegistry
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from vobchat.web.state import hydrate_states_from_thread
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)

from test_chat_phase4 import (
    FakeLLMClient,
    FakePlacesService,
    StaticPlanner,
    build_orchestrator,
    make_multi_geography_place,
    make_resolved_place,
    make_validated_analysis_spec,
    seed_analysis_receipt,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_total_population_cube() -> CubeSummaryResponse:
    return CubeSummaryResponse(
        theme_id="T_POP",
        cube_id="N_POP_TOTAL",
        label="Total population",
        start_year=1801,
        end_year=1911,
        observation_count=6,
        has_categories=False,
    )


def test_v1_observability_logs_structured_workflow_event(caplog) -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        llm_client=FakeLLMClient(text="Here is the table."),
    )
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    with caplog.at_level(logging.INFO, logger="vobchat.api.services.chat_orchestrator"):
        response = asyncio.run(
            orchestrator.handle_turn(
                ChatTurnRequest(
                    thread_id=thread.thread_id,
                    message="show the table",
                    stream=False,
                )
            )
        )

    workflow_records = [
        record for record in caplog.records if getattr(record, "workflow_event", None) is not None
    ]
    assert workflow_records
    workflow_event = workflow_records[-1].workflow_event
    assert workflow_event["classified_path"] == "analysis"
    assert workflow_event["decision_outcome"] == "execute"
    assert workflow_event["guard_outcome"] == "runnable_analysis_spec"
    assert workflow_event["reporting_geography_status"] == "reused"
    assert workflow_event["slice_status"] == "unique_executable_slice"
    assert workflow_event["receipt_id"] == response.thread_state.current_receipt_id


def test_v1_analysis_does_not_use_first_unit_fallback() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Need geography."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population over time",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert response.ui_delta.policy_decision.outcome.value == "ask"
    assert response.ui_delta.execution_guard_result.outcome == ExecutionGuardOutcome.NEEDS_CLARIFICATION
    assert response.thread_state.analysis_state.reporting_geography is None
    assert response.thread_state.current_receipt is None


def test_v1_preselected_cube_does_not_shortcut_analysis_without_receipt() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Stay in discovery."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread.selected_cubes = [make_total_population_cube()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population over time",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert response.ui_delta.policy_decision.outcome.value == "discovery"
    assert response.ui_delta.execution_guard_result.outcome == ExecutionGuardOutcome.DISCOVERY_RESPONSE
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v1_info_lookup_creates_info_receipt() -> None:
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

    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.INFO
    assert response.thread_state.current_receipt.path == WorkflowPath.INFO_LOOKUP
    assert response.thread_state.render_projection is not None
    assert response.thread_state.render_projection.metadata_payload is not None


def test_v1_reference_resolver_requires_receipt_anchor_for_output_switch() -> None:
    resolver = ReferenceResolver(places_service=FakePlacesService())
    state = ChatThreadState(
        thread_id="thread-1",
        created_at=utc_now(),
        updated_at=utc_now(),
    )

    resolution = resolver.resolve(state=state, message="show the table")

    assert resolution.matched is True
    assert resolution.patch is None
    assert resolution.clarification is not None
    assert resolution.reason == "follow_up_output_switch_missing_validated_anchor"


def test_v1_state_patch_service_replace_place_invalidates_dependent_state() -> None:
    store = InMemoryChatThreadStore()
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])
    resolver = ReferenceResolver(places_service=FakePlacesService())
    patch_service = StatePatchService()

    resolution = resolver.resolve(state=thread, message="same for Leeds")
    result = patch_service.apply(state=thread, resolution=resolution)

    assert result.patch.patch_type == StatePatchType.REPLACE_PLACE
    assert set(result.invalidated_fields) == {
        "place_identity",
        "reporting_geography",
        "availability",
        "comparability",
    }
    assert [place.place.name for place in thread.selected_places] == ["Leeds"]
    assert thread.selected_cubes == []
    assert thread.analysis_state.analysis_spec is None
    assert thread.analysis_state.reporting_geography is not None
    assert thread.analysis_state.reporting_geography.unit_ids == []
    assert result.inherited_action.exact_slice is not None


def test_v1_projection_reload_uses_receipt_and_projection_authoritatively() -> None:
    spec = make_validated_analysis_spec(
        places=[make_resolved_place()],
        unit_ids=[101],
        output_mode="table",
        year=1901,
    )
    selected_place = make_resolved_place()
    ui_projection = UIProjection(
        projection_id="ui_v1",
        current_receipt_id="ar_v1",
        current_receipt_kind=ReceiptKind.ANALYSIS,
        selected_places=[selected_place],
        reporting_geography=spec.reporting_geography,
        dataset_family=ThemeSummaryResponse(theme_id="T_POP", label="Population"),
        selected_cubes=[make_total_population_cube()],
        exact_slice=spec.exact_slice,
        analysis_spec=spec,
        time_scope=spec.time_scope,
        active_output_mode="table",
    )
    render_projection = RenderProjection(
        projection_id="rp_v1",
        receipt_id="ar_v1",
        active_output_mode="table",
        answer_text="Projected table ready.",
        table={
            "columns": [{"name": "Year", "id": "year"}],
            "rows": [{"year": 1901}],
        },
    )
    receipt = AnalysisReceipt(
        kind=ReceiptKind.ANALYSIS,
        receipt_id="ar_v1",
        path=WorkflowPath.ANALYSIS,
        source_operation=ChatOperation.FETCH_TIME_SERIES,
        analysis_spec=spec,
        ui_projection=ui_projection,
        render_projection=render_projection,
        created_at=utc_now(),
    )
    thread = ChatThreadState(
        thread_id="thread-v1",
        created_at=utc_now(),
        updated_at=utc_now(),
        selected_places=[make_resolved_place(name="Legacy York")],
        current_receipt=receipt,
        current_receipt_id=receipt.receipt_id,
        recent_receipts=[receipt],
        ui_projection=ui_projection,
        render_projection=render_projection,
    )

    selection_state, visualization_state, metadata_state, map_state = hydrate_states_from_thread(
        thread_state_data=thread.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )

    assert selection_state["current_receipt_id"] == "ar_v1"
    assert selection_state["selected_places"][0]["place"]["name"] == "York"
    assert selection_state["active_output_mode"] == "table"
    assert visualization_state["active_tab"] == "table"
    assert map_state["selected_ids"] == [101]
    assert metadata_state["render_projection"]["receipt_id"] == "ar_v1"


def test_v1_par_unit_policy_remains_narrow_for_boundary_maps() -> None:
    registry = CapabilityRegistry()
    par_unit = registry.geography("PAR_UNIT")

    assert par_unit is not None
    assert par_unit.discovery is True
    assert par_unit.map_loading_policy == "bbox_or_ids_only"

    unavailable = registry.output_feasibility(
        output_mode="boundary_map",
        reporting_geography=ReportingGeographyRef(
            unit_type="PAR_UNIT",
            unit_ids=[],
            label="Parish",
        ),
        dataset_family=None,
        exact_slice=None,
        wants_theme_context=False,
        requested_full_layer_map=True,
    )
    available = registry.output_feasibility(
        output_mode="boundary_map",
        reporting_geography=ReportingGeographyRef(
            unit_type="PAR_UNIT",
            unit_ids=[9001],
            label="Parish",
        ),
        dataset_family=None,
        exact_slice=None,
        wants_theme_context=False,
        requested_full_layer_map=False,
    )

    assert unavailable == OutputFeasibilityStatus.UNSUPPORTED
    assert available == OutputFeasibilityStatus.AVAILABLE
