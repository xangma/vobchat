from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    AnalysisSpec,
    CapabilitySupportLevel,
    ChatOperation,
    ChatTurnRequest,
    ComparisonCapabilityTier,
    ComparabilityStatus,
    DatasetFamilyRef,
    DatasetStatus,
    ExecutionGuardOutcome,
    ExactSliceRef,
    MapLoadingMode,
    OutputFeasibilityStatus,
    OutputSwitchCompatibility,
    PolicyOutcome,
    ReceiptKind,
    ReportingGeographyRef,
    ReportingGeographyStatus,
    SliceStatus,
    SlotStatus,
    TimeScope,
)
from vobchat.api.schemas.themes import ThemeSummaryResponse
from vobchat.api.services.chat_safety import (
    CapabilityRegistry,
    DecisionPolicyEngine,
    ExecutionGuard,
    ResolvedAnalysisContext,
)
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from test_chat_phase4 import (
    FakeLLMClient,
    build_orchestrator,
    make_resolved_place,
)


def _reporting_geography(unit_type: str = "MOD_DIST", unit_ids: list[int] | None = None) -> ReportingGeographyRef:
    return ReportingGeographyRef(
        unit_type=unit_type,
        unit_ids=[101] if unit_ids is None else list(unit_ids),
        label=unit_type,
        slot_status=SlotStatus.RESOLVED,
    )


def _exact_slice() -> ExactSliceRef:
    return ExactSliceRef(
        cube_id="N_TOT_POP",
        cube_ids=["N_TOT_POP"],
        label="Current Total Population",
        cellref="TOT_POP:now",
        dataitem_id="N_TOT_POP_4",
        cat_id="C_TOT_POP_4",
        slot_status=SlotStatus.RESOLVED,
    )


def test_v2_capability_registry_exposes_broader_typed_policy_dimensions() -> None:
    registry = CapabilityRegistry()

    mod_dist = registry.geography("MOD_DIST")
    par_unit = registry.geography("PAR_UNIT")
    population = registry.dataset("T_POP")
    land = registry.dataset("T_LAND")

    assert mod_dist is not None
    assert mod_dist.trend_capable is True
    assert mod_dist.boundary_map_capable is True
    assert mod_dist.thematic_map_capable is False
    assert mod_dist.map_loading.mode == MapLoadingMode.FULL_MAP
    assert mod_dist.comparison_capability == ComparisonCapabilityTier.GUARD_VERIFIABLE

    assert par_unit is not None
    assert par_unit.discovery is True
    assert par_unit.boundary_map_capable is True
    assert par_unit.map_loading.mode == MapLoadingMode.BBOX_OR_IDS
    assert par_unit.map_loading_policy == "bbox_or_ids_only"
    assert par_unit.comparison_policy.requires_explicit_geography is True

    assert population is not None
    assert population.trend_capable is True
    assert population.breakdown_capable is False
    assert population.boundary_map_capable is True
    assert population.thematic_map_capable is False
    assert population.supported_exact_slice_cubes == ("N_TOT_POP",)
    assert population.inventory_only_cubes == ("N_GENDER",)

    assert land is not None
    assert land.outputs["trend_chart"].level == CapabilitySupportLevel.DISCOVERY_ONLY
    assert land.boundary_map_capable is True
    assert land.exact_slice_required is True


def test_v2_comparison_policy_tiers_remain_explicit_and_safe() -> None:
    registry = CapabilityRegistry()
    places = [
        make_resolved_place(),
        make_resolved_place(place_id=4, name="Leeds", unit_id=102),
    ]
    dataset_family = DatasetFamilyRef(theme_id="T_POP", label="Population")
    exact_slice = _exact_slice()

    candidate = registry.comparison_assessment(
        places=places,
        reporting_geography=None,
        dataset_family=dataset_family,
        exact_slice=None,
        time_scope=None,
    )
    guard_verifiable = registry.comparison_assessment(
        places=places,
        reporting_geography=_reporting_geography(),
        dataset_family=dataset_family,
        exact_slice=exact_slice,
        time_scope=None,
    )
    executable = registry.comparison_assessment(
        places=places,
        reporting_geography=_reporting_geography(),
        dataset_family=dataset_family,
        exact_slice=exact_slice,
        time_scope=TimeScope(mode="snapshot", year=1901, label="1901"),
    )
    discovery_only = registry.comparison_assessment(
        places=places,
        reporting_geography=_reporting_geography(),
        dataset_family=DatasetFamilyRef(theme_id="T_LAND", label="Land"),
        exact_slice=exact_slice,
        time_scope=TimeScope(mode="snapshot", year=1901, label="1901"),
    )

    assert candidate.tier == ComparisonCapabilityTier.CANDIDATE_COMPARABLE
    assert guard_verifiable.tier == ComparisonCapabilityTier.GUARD_VERIFIABLE
    assert executable.tier == ComparisonCapabilityTier.CURRENTLY_EXECUTABLE
    assert discovery_only.tier == ComparisonCapabilityTier.DISCOVERY_ONLY


def test_v2_map_loading_and_output_switch_policy_are_explicit() -> None:
    registry = CapabilityRegistry()
    reporting_geography = _reporting_geography()
    dataset_family = DatasetFamilyRef(theme_id="T_POP", label="Population")
    exact_slice = _exact_slice()

    table_switch = registry.output_switch_assessment(
        source_output_mode="trend_chart",
        target_output_mode="table",
        reporting_geography=reporting_geography,
        dataset_family=dataset_family,
        exact_slice=exact_slice,
    )
    boundary_map_switch = registry.output_switch_assessment(
        source_output_mode="trend_chart",
        target_output_mode="boundary_map",
        reporting_geography=reporting_geography,
        dataset_family=dataset_family,
        exact_slice=exact_slice,
    )
    thematic_map_switch = registry.output_switch_assessment(
        source_output_mode="trend_chart",
        target_output_mode="thematic_map",
        reporting_geography=reporting_geography,
        dataset_family=dataset_family,
        exact_slice=exact_slice,
    )
    par_unit_boundary = registry.output_policy(
        output_mode="boundary_map",
        reporting_geography=_reporting_geography("PAR_UNIT", [9001]),
        dataset_family=None,
        exact_slice=None,
        wants_theme_context=False,
    )
    par_unit_full_layer = registry.output_feasibility(
        output_mode="boundary_map",
        reporting_geography=_reporting_geography("PAR_UNIT", []),
        dataset_family=None,
        exact_slice=None,
        wants_theme_context=False,
        requested_full_layer_map=True,
    )

    assert table_switch.compatibility == OutputSwitchCompatibility.REUSE_VALIDATED_SPEC
    assert boundary_map_switch.compatibility == OutputSwitchCompatibility.SAFE_DEFAULT_WITH_DISCLOSURE
    assert thematic_map_switch.compatibility == OutputSwitchCompatibility.UNSUPPORTED
    assert par_unit_boundary.map_loading is not None
    assert par_unit_boundary.map_loading.mode == MapLoadingMode.BBOX_OR_IDS
    assert par_unit_full_layer == OutputFeasibilityStatus.UNSUPPORTED


def test_v2_discovery_surfaces_policy_levels_without_claiming_execution() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Outputs loaded."))
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what outputs are feasible here?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.time_series is None
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY

    outputs = {item.output_mode: item for item in response.ui_delta.discovery_result.supported_outputs}
    assert outputs["trend_chart"].support_level == CapabilitySupportLevel.GUARDED
    assert outputs["trend_chart"].requires_exact_slice is True
    assert outputs["trend_chart"].comparison_tier == ComparisonCapabilityTier.GUARD_VERIFIABLE
    assert outputs["boundary_map"].map_loading_mode == MapLoadingMode.FULL_MAP
    assert outputs["category_chart"].support_level == CapabilitySupportLevel.DISCOVERY_ONLY
    assert outputs["category_chart"].feasibility == OutputFeasibilityStatus.UNSUPPORTED


def test_v2_policy_driven_guard_blocks_discovery_only_family_execution() -> None:
    registry = CapabilityRegistry()
    decision_engine = DecisionPolicyEngine()
    execution_guard = ExecutionGuard()
    reporting_geography = _reporting_geography()
    exact_slice = _exact_slice()
    context = ResolvedAnalysisContext(
        operation=ChatOperation.FETCH_TIME_SERIES,
        output_mode="trend_chart",
        places=[make_resolved_place()],
        reporting_geography_candidates=[reporting_geography],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_LAND", label="Land"),
        exact_slice=exact_slice,
        time_scope=TimeScope(mode="snapshot", year=1901, label="1901"),
        comparison_mode="single_place",
    )
    spec = AnalysisSpec(
        places=[],
        reporting_geography=reporting_geography,
        dataset_family=context.dataset_family,
        exact_slice=exact_slice,
        time_scope=context.time_scope,
        output_mode="trend_chart",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status=DatasetStatus.RESOLVED,
        slice_status=SliceStatus.UNIQUE_EXECUTABLE_SLICE,
        comparability_status=ComparabilityStatus.COMPARABLE,
        output_feasibility_status=OutputFeasibilityStatus.NEEDS_CLARIFICATION,
    )

    decision = decision_engine.decide(
        context=context,
        spec=spec,
        capability_registry=registry,
    )
    guard = execution_guard.evaluate(
        context=context,
        spec=spec,
        decision=decision,
        capability_registry=registry,
    )

    assert decision.outcome == PolicyOutcome.DISCOVERY
    assert guard.outcome == ExecutionGuardOutcome.DISCOVERY_RESPONSE
