from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    CategoryEntityRef,
    ChatOperation,
    ChatTurnRequest,
    DatasetFamilyRef,
    ExactSliceCandidate,
    ExactSliceRef,
    PlannerAction,
    PolicyOutcome,
    ReceiptKind,
    ReportingGeographyRef,
    SliceStatus,
)
from vobchat.api.services.chat_safety import CapabilityRegistry
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from test_chat_phase4 import (
    FakeLLMClient,
    FakeSeriesService,
    StaticPlanner,
    build_orchestrator,
    make_resolved_place,
    seed_analysis_receipt,
)


class RecordingSeriesService(FakeSeriesService):
    def __init__(self) -> None:
        self.time_series_requests = []
        self.category_requests = []

    def get_time_series(self, request):
        self.time_series_requests.append(request)
        return super().get_time_series(request)

    def get_category_breakdown(self, request):
        self.category_requests.append(request)
        return super().get_category_breakdown(request)


class AmbiguousExactSliceService:
    def resolve_exact_slice(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ):
        candidates = [
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Population 10 years earlier",
                description="Total Population",
                cellref="TOT_POP:prev_10yrs",
                dataitem_id="N_TOT_POP_3",
                cat_id="C_TOT_POP_3",
                has_categories=True,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="ambiguous_exact_slice_service",
            ),
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Current Total Population",
                description="Total Population",
                cellref="TOT_POP:now",
                dataitem_id="N_TOT_POP_4",
                cat_id="C_TOT_POP_4",
                has_categories=True,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="ambiguous_exact_slice_service",
            ),
        ]
        return type("ExactSliceResolution", (), {"candidates": candidates, "resolved": None})()


class GenderExactSliceService:
    def resolve_exact_slice(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ):
        candidate = ExactSliceCandidate(
            cube_id="N_GENDER",
            cube_ids=["N_GENDER"],
            label="Male",
            description="Males & Females",
            cellref="GENDER:male",
            dataitem_id="N_GENDER_1",
            cat_id="C_SEX_1",
            has_categories=True,
            category_entity=CategoryEntityRef(
                entity_id="C_SEX_1",
                label="Male",
                group_label="Males & Females",
                source="fake_metadata",
                provenance="fake_metadata",
            ),
            metadata_provenance="fake_metadata",
            slot_status="candidate_set",
            source="gender_exact_slice_service",
        )
        return type(
            "ExactSliceResolution",
            (),
            {
                "candidates": [candidate],
                "resolved": ExactSliceRef.from_candidate(
                    candidate,
                    source="gender_exact_slice_service",
                    slot_status="resolved",
                ),
            },
        )()


def test_v2_exact_slice_time_series_executes_for_live_validated_subset() -> None:
    store = InMemoryChatThreadStore()
    series_service = RecordingSeriesService()
    orchestrator = build_orchestrator(
        store=store,
        series_service=series_service,
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

    assert response.ui_delta.time_series is not None
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.EXECUTE
    assert series_service.time_series_requests[0].dataitem_ids == ["N_TOT_POP_4"]
    assert series_service.time_series_requests[0].cellrefs == ["TOT_POP:now"]
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.ANALYSIS
    assert response.thread_state.current_receipt.analysis_spec is not None
    assert response.thread_state.current_receipt.analysis_spec.exact_slice is not None
    assert (
        response.thread_state.current_receipt.analysis_spec.exact_slice.dataitem_id
        == "N_TOT_POP_4"
    )
    assert response.thread_state.current_receipt.analysis_spec.exact_slice.category_entity == (
        CategoryEntityRef(
            entity_id="C_TOT_POP_4",
            label="Current Total Population",
            group_label="Total Population",
            source="fake_metadata",
            provenance="fake_metadata",
        )
    )
    assert (
        response.thread_state.current_receipt.analysis_spec.exact_slice.metadata_provenance
        == "fake_metadata"
    )


def test_v2_family_level_theme_match_still_blocks_without_exact_slice() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                year=1901,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Need a more specific measure."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population for York in 1901",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.DISCOVERY
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_metadata_backed_category_inventory_is_not_treated_as_runnable_breakdown() -> None:
    store = InMemoryChatThreadStore()
    series_service = RecordingSeriesService()
    orchestrator = build_orchestrator(
        store=store,
        series_service=series_service,
        exact_slice_service=GenderExactSliceService(),
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                theme_query="Population",
                cube_query="gender",
                year=1901,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Category breakdown is not yet supported."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population by gender for York in 1901",
                stream=False,
            )
        )
    )

    assert response.ui_delta.category_breakdown is None
    assert series_service.category_requests == []
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES
    assert response.thread_state.current_receipt is None


def test_v2_ambiguous_exact_slice_candidate_set_blocks_execution() -> None:
    store = InMemoryChatThreadStore()
    series_service = RecordingSeriesService()
    orchestrator = build_orchestrator(
        store=store,
        series_service=series_service,
        exact_slice_service=AmbiguousExactSliceService(),
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                cube_id="N_TOT_POP",
                year=1901,
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Need clarification."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show total population for York in 1901",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert series_service.time_series_requests == []
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.DISCOVERY
    assert response.thread_state.analysis_state.analysis_spec is not None
    assert response.thread_state.analysis_state.analysis_spec.slice_status == SliceStatus.CANDIDATE_SET
    assert len(response.thread_state.resolution_state.slice_candidates) == 2


def test_v2_follow_up_table_reuses_live_validated_exact_slice_safely() -> None:
    store = InMemoryChatThreadStore()
    series_service = RecordingSeriesService()
    orchestrator = build_orchestrator(
        store=store,
        series_service=series_service,
        llm_client=FakeLLMClient(text="Here is the table."),
    )
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])
    thread = store.require_thread(thread.thread_id)
    assert thread.current_receipt is not None
    assert thread.current_receipt.analysis_spec is not None

    modified_receipt = thread.current_receipt.model_copy(deep=True)

    modified_receipt.analysis_spec.dataset_family = DatasetFamilyRef(
        theme_id="T_POP",
        label="Population",
        description="Population counts",
        slot_status="resolved",
    )
    modified_receipt.analysis_spec.reporting_geography = ReportingGeographyRef(
        unit_type="MOD_DIST",
        unit_ids=[101],
        label="Modern District",
        slot_status="resolved",
    )
    modified_receipt.analysis_spec.exact_slice = ExactSliceRef(
        cube_id="N_TOT_POP",
        cube_ids=["N_TOT_POP"],
        label="Current Total Population",
        cellref="TOT_POP:now",
        dataitem_id="N_TOT_POP_4",
        cat_id="C_TOT_POP_4",
        has_categories=True,
        category_entity=CategoryEntityRef(
            entity_id="C_TOT_POP_4",
            label="Current Total Population",
            group_label="Total Population",
            source="seed_override",
            provenance="seed_override",
        ),
        slot_status="resolved",
        source="seed_override",
    )
    thread.current_receipt = modified_receipt
    thread.analysis_state.analysis_spec = modified_receipt.analysis_spec.model_copy(deep=True)
    thread.analysis_state.candidate_slice = (
        modified_receipt.analysis_spec.exact_slice.model_copy(deep=True)
    )
    thread.analysis_state.dataset_family = modified_receipt.analysis_spec.dataset_family
    thread.analysis_state.reporting_geography = modified_receipt.analysis_spec.reporting_geography
    store.save_receipt(thread.thread_id, modified_receipt)
    thread = store.save_thread(thread)
    thread = store.require_thread(thread.thread_id)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show the table",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is not None
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.analysis_spec is not None
    assert response.thread_state.current_receipt.analysis_spec.output_mode == "table"
    assert series_service.time_series_requests[0].dataitem_ids == ["N_TOT_POP_4"]
    assert series_service.time_series_requests[0].cellrefs == ["TOT_POP:now"]


def test_v2_capability_policy_is_explicitly_narrow_after_live_validation() -> None:
    registry = CapabilityRegistry()

    assert registry.dataset("T_POP").category_support == (
        "metadata_inventory_only_after_live_validation"
    )
    assert registry.output_feasibility(
        output_mode="category_chart",
        reporting_geography=ReportingGeographyRef(
            unit_type="MOD_DIST",
            unit_ids=[101],
            label="Modern District",
        ),
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        exact_slice=ExactSliceRef(
            cube_id="N_GENDER",
            cube_ids=["N_GENDER"],
            label="Male",
            cellref="GENDER:male",
            dataitem_id="N_GENDER_1",
            cat_id="C_SEX_1",
            has_categories=True,
        ),
        wants_theme_context=True,
        requested_full_layer_map=False,
    ).value == "unsupported"
