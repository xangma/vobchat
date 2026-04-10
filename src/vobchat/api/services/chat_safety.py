from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from vobchat.api.schemas.chat import (
    AnalysisSpec,
    AvailabilityStatus,
    CapabilitySupportLevel,
    CategorySupportStatus,
    ChatOperation,
    ClarificationOption,
    ClarificationState,
    ComparisonCapabilityTier,
    ComparabilityStatus,
    DatasetFamilyRef,
    DatasetStatus,
    ExecutionGuardOutcome,
    ExecutionGuardResult,
    ExactSliceCandidate,
    ExactSliceRef,
    MapLoadingMode,
    OutputFeasibilityStatus,
    OutputSwitchCompatibility,
    PolicyDecision,
    PolicyOutcome,
    ReportingGeographyRef,
    ReportingGeographyStatus,
    SemanticPlaceRef,
    SliceStatus,
    SlotStatus,
    TimeScope,
    WorkflowPath,
)
from vobchat.api.schemas.maps import MapFeatureCollectionResponse, MapFeaturesByIdsQuery, MapFeaturesQuery
from vobchat.api.schemas.places import ResolvedPlaceResponse
from vobchat.api.schemas.series import CategoryBreakdownRequest, CategoryBreakdownResponse, TimeSeriesRequest, TimeSeriesResponse
from vobchat.api.schemas.themes import CubeSummaryResponse
from vobchat.api.services.maps import MapsService
from vobchat.api.services.series import SeriesService
from vobchat.utils.constants import UNIT_TYPES


@dataclass(frozen=True)
class OutputCapability:
    level: CapabilitySupportLevel
    requires_exact_slice: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class MapLoadingPolicy:
    mode: MapLoadingMode
    requires_bbox: bool = False
    requires_ids: bool = False
    legacy_label: str | None = None


@dataclass(frozen=True)
class ComparisonPolicy:
    tier: ComparisonCapabilityTier
    requires_same_unit_type: bool = True
    requires_same_exact_slice: bool = True
    requires_common_time_scope: bool = True
    requires_explicit_geography: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class GeographyCapability:
    unit_type: str
    enabled: bool
    discovery: bool
    info_lookup: bool
    outputs: dict[str, OutputCapability]
    comparison_policy: ComparisonPolicy
    map_loading: MapLoadingPolicy
    exact_slice_required_for_analysis: bool = True
    category_support: CategorySupportStatus = CategorySupportStatus.NONE_IN_V1

    @property
    def trend_capable(self) -> bool:
        return (
            self.outputs.get("trend_chart", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def breakdown_capable(self) -> bool:
        return (
            self.outputs.get("category_chart", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def info_capable(self) -> bool:
        return self.info_lookup

    @property
    def boundary_map_capable(self) -> bool:
        return (
            self.outputs.get("boundary_map", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            != CapabilitySupportLevel.UNSUPPORTED
        )

    @property
    def thematic_map_capable(self) -> bool:
        return (
            self.outputs.get("thematic_map", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def comparison_capability(self) -> ComparisonCapabilityTier:
        return self.comparison_policy.tier

    @property
    def chart_analysis(self) -> str:
        trend = self.outputs.get("trend_chart", OutputCapability(CapabilitySupportLevel.UNSUPPORTED))
        if self.comparison_policy.requires_explicit_geography:
            return "conditional_but_narrow"
        return "conditional" if trend.level == CapabilitySupportLevel.GUARDED else trend.level.value

    @property
    def boundary_map(self) -> bool:
        return self.boundary_map_capable

    @property
    def thematic_map(self) -> bool:
        return self.thematic_map_capable

    @property
    def comparison(self) -> str:
        if self.comparison_policy.requires_explicit_geography:
            return "conditional_and_explicit_only"
        if self.comparison_policy.tier == ComparisonCapabilityTier.DISCOVERY_ONLY:
            return "discovery_only"
        if self.comparison_policy.tier == ComparisonCapabilityTier.NOT_SUPPORTED:
            return "not_supported"
        return "conditional"

    @property
    def map_loading_policy(self) -> str:
        return self.map_loading.legacy_label or self.map_loading.mode.value


@dataclass(frozen=True)
class DatasetCapability:
    theme_id: str
    discovery: bool
    info_lookup: bool
    outputs: dict[str, OutputCapability]
    comparison_policy: ComparisonPolicy
    exact_slice_required: bool
    category_support: CategorySupportStatus
    supported_exact_slice_cubes: tuple[str, ...] = ()
    inventory_only_cubes: tuple[str, ...] = ()
    analysis_stance: str = "conditional"

    @property
    def trend_capable(self) -> bool:
        return (
            self.outputs.get("trend_chart", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def breakdown_capable(self) -> bool:
        return (
            self.outputs.get("category_chart", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def info_capable(self) -> bool:
        return self.info_lookup

    @property
    def boundary_map_capable(self) -> bool:
        return (
            self.outputs.get("boundary_map", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            != CapabilitySupportLevel.UNSUPPORTED
        )

    @property
    def thematic_map_capable(self) -> bool:
        return (
            self.outputs.get("thematic_map", OutputCapability(CapabilitySupportLevel.UNSUPPORTED)).level
            == CapabilitySupportLevel.GUARDED
        )

    @property
    def comparison_capability(self) -> ComparisonCapabilityTier:
        return self.comparison_policy.tier

    @property
    def boundary_map(self) -> bool:
        return self.boundary_map_capable

    @property
    def thematic_map(self) -> bool:
        return self.thematic_map_capable

    @property
    def comparison(self) -> str:
        return self.comparison_policy.tier.value


@dataclass(frozen=True)
class OutputPolicyAssessment:
    output_mode: str
    geography_support: CapabilitySupportLevel
    dataset_support: CapabilitySupportLevel
    combined_support: CapabilitySupportLevel
    requires_exact_slice: bool
    comparison_tier: ComparisonCapabilityTier
    map_loading: MapLoadingPolicy | None = None
    reason: str | None = None


@dataclass(frozen=True)
class ComparisonAssessment:
    tier: ComparisonCapabilityTier
    reason: str
    policy: ComparisonPolicy


@dataclass(frozen=True)
class OutputSwitchAssessment:
    compatibility: OutputSwitchCompatibility
    reason: str
    defaults_applied: tuple[str, ...] = ()


@dataclass
class ResolvedAnalysisContext:
    operation: ChatOperation
    output_mode: str
    places: list[ResolvedPlaceResponse] = field(default_factory=list)
    reporting_geography_candidates: list[ReportingGeographyRef] = field(default_factory=list)
    reporting_geography: ReportingGeographyRef | None = None
    dataset_family: DatasetFamilyRef | None = None
    dataset_family_candidates: list[DatasetFamilyRef] = field(default_factory=list)
    cube_candidates: list[CubeSummaryResponse] = field(default_factory=list)
    exact_slice_candidates: list[ExactSliceCandidate] = field(default_factory=list)
    exact_slice: ExactSliceRef | None = None
    time_scope: TimeScope | None = None
    comparison_mode: str | None = None
    requested_unit_type: str | None = None
    requested_theme_id: str | None = None
    requested_cube_id: str | None = None
    requested_cube_query: str | None = None
    wants_theme_context: bool = False
    requested_full_layer_map: bool = False
    notices: list[str] = field(default_factory=list)
    alternatives: list[str] = field(default_factory=list)

    def to_analysis_spec(self) -> AnalysisSpec | None:
        if (
            not self.places
            and self.reporting_geography is None
            and self.dataset_family is None
            and self.exact_slice is None
            and self.time_scope is None
            and not self.output_mode
        ):
            return None

        reporting_geography_status = (
            ReportingGeographyStatus.RESOLVED
            if self.reporting_geography is not None and self.reporting_geography.unit_ids
            else ReportingGeographyStatus.NEEDS_CLARIFICATION
        )
        dataset_status = (
            DatasetStatus.RESOLVED
            if self.dataset_family is not None
            else DatasetStatus.NEEDS_CLARIFICATION
        )
        slice_status = SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION
        if self.exact_slice is not None:
            slice_status = (
                SliceStatus.UNIQUE_EXECUTABLE_SLICE
                if self.exact_slice.cellref
                or self.exact_slice.dataitem_id
                or self.exact_slice.cat_id
                else SliceStatus.FAMILY_ONLY
            )
        elif self.exact_slice_candidates:
            slice_status = SliceStatus.CANDIDATE_SET
        comparability_status = (
            ComparabilityStatus.COMPARABLE
            if len(self.places) <= 1
            else ComparabilityStatus.NEEDS_CLARIFICATION
        )
        output_feasibility_status = OutputFeasibilityStatus.NEEDS_CLARIFICATION
        return AnalysisSpec(
            places=[
                SemanticPlaceRef.from_resolved_place(place, source="selected_place")
                for place in self.places
            ],
            reporting_geography=self.reporting_geography,
            dataset_family=self.dataset_family,
            exact_slice=self.exact_slice,
            time_scope=self.time_scope,
            output_mode=self.output_mode,
            comparison_mode=self.comparison_mode,
            reporting_geography_status=reporting_geography_status,
            dataset_status=dataset_status,
            slice_status=slice_status,
            comparability_status=comparability_status,
            availability_status=AvailabilityStatus.PARTIALLY_AVAILABLE,
            output_feasibility_status=output_feasibility_status,
        )


@dataclass
class AnalysisExecutionResult:
    time_series: TimeSeriesResponse | None = None
    category_breakdown: CategoryBreakdownResponse | None = None
    map_features: MapFeatureCollectionResponse | None = None
    table: dict[str, Any] | None = None
    execution_summary: dict[str, Any] = field(default_factory=dict)
    fallback_text: str = ""
    selected_cubes: list[CubeSummaryResponse] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)


class CapabilityRegistry:
    _DISCOVERY_OUTPUT_ORDER = (
        "trend_chart",
        "table",
        "boundary_map",
        "category_chart",
    )

    def __init__(self) -> None:
        self._geographies: dict[str, GeographyCapability] = {
            unit_type: GeographyCapability(
                unit_type=unit_type,
                enabled=True,
                discovery=True,
                info_lookup=True,
                outputs=self._standard_geography_outputs(),
                comparison_policy=ComparisonPolicy(
                    tier=ComparisonCapabilityTier.GUARD_VERIFIABLE,
                    requires_same_unit_type=True,
                    requires_same_exact_slice=True,
                    requires_common_time_scope=True,
                ),
                map_loading=MapLoadingPolicy(
                    mode=MapLoadingMode.FULL_MAP if config.get("cache_disk") else MapLoadingMode.BBOX_OR_IDS,
                    requires_bbox=not config.get("cache_disk"),
                    requires_ids=not config.get("cache_disk"),
                    legacy_label="full_layer_cached" if config.get("cache_disk") else "ids_or_bbox",
                ),
            )
            for unit_type, config in UNIT_TYPES.items()
        }
        self._geographies["PAR_UNIT"] = GeographyCapability(
            unit_type="PAR_UNIT",
            enabled=True,
            discovery=True,
            info_lookup=True,
            outputs=self._standard_geography_outputs(),
            comparison_policy=ComparisonPolicy(
                tier=ComparisonCapabilityTier.GUARD_VERIFIABLE,
                requires_same_unit_type=True,
                requires_same_exact_slice=True,
                requires_common_time_scope=True,
                requires_explicit_geography=True,
                notes=("PAR_UNIT comparisons remain explicit-only in the current runtime.",),
            ),
            map_loading=MapLoadingPolicy(
                mode=MapLoadingMode.BBOX_OR_IDS,
                requires_bbox=True,
                requires_ids=True,
                legacy_label="bbox_or_ids_only",
            ),
        )
        self._datasets: dict[str, DatasetCapability] = {
            "T_POP": DatasetCapability(
                theme_id="T_POP",
                discovery=True,
                info_lookup=True,
                outputs={
                    "trend_chart": OutputCapability(
                        CapabilitySupportLevel.GUARDED,
                        requires_exact_slice=True,
                    ),
                    "snapshot_chart": OutputCapability(
                        CapabilitySupportLevel.GUARDED,
                        requires_exact_slice=True,
                    ),
                    "table": OutputCapability(
                        CapabilitySupportLevel.GUARDED,
                        requires_exact_slice=True,
                    ),
                    "category_chart": OutputCapability(CapabilitySupportLevel.DISCOVERY_ONLY),
                    "boundary_map": OutputCapability(CapabilitySupportLevel.GUARDED),
                    "thematic_map": OutputCapability(CapabilitySupportLevel.UNSUPPORTED),
                },
                comparison_policy=ComparisonPolicy(
                    tier=ComparisonCapabilityTier.GUARD_VERIFIABLE,
                    requires_same_unit_type=True,
                    requires_same_exact_slice=True,
                    requires_common_time_scope=True,
                ),
                exact_slice_required=True,
                category_support=CategorySupportStatus.METADATA_INVENTORY_ONLY_AFTER_LIVE_VALIDATION,
                supported_exact_slice_cubes=("N_TOT_POP",),
                inventory_only_cubes=("N_GENDER",),
                analysis_stance="conditional",
            ),
            "T_HOUS": DatasetCapability(
                theme_id="T_HOUS",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
            "T_HOU": DatasetCapability(
                theme_id="T_HOU",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
            "T_IND": DatasetCapability(
                theme_id="T_IND",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
            "T_LAND": DatasetCapability(
                theme_id="T_LAND",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
            "T_POL": DatasetCapability(
                theme_id="T_POL",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="conditional_and_geography_restricted",
            ),
            "T_SOC": DatasetCapability(
                theme_id="T_SOC",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
            "T_WK": DatasetCapability(
                theme_id="T_WK",
                discovery=True,
                info_lookup=True,
                outputs=self._discovery_first_dataset_outputs(),
                comparison_policy=self._discovery_only_comparison_policy(),
                exact_slice_required=True,
                category_support=CategorySupportStatus.NONE_IN_V1,
                analysis_stance="discovery_first",
            ),
        }

    @staticmethod
    def _standard_geography_outputs() -> dict[str, OutputCapability]:
        return {
            "trend_chart": OutputCapability(CapabilitySupportLevel.GUARDED),
            "snapshot_chart": OutputCapability(CapabilitySupportLevel.GUARDED),
            "table": OutputCapability(CapabilitySupportLevel.GUARDED),
            "category_chart": OutputCapability(CapabilitySupportLevel.GUARDED),
            "boundary_map": OutputCapability(CapabilitySupportLevel.GUARDED),
            "thematic_map": OutputCapability(CapabilitySupportLevel.UNSUPPORTED),
        }

    @staticmethod
    def _discovery_first_dataset_outputs() -> dict[str, OutputCapability]:
        return {
            "trend_chart": OutputCapability(
                CapabilitySupportLevel.DISCOVERY_ONLY,
                requires_exact_slice=True,
            ),
            "snapshot_chart": OutputCapability(
                CapabilitySupportLevel.DISCOVERY_ONLY,
                requires_exact_slice=True,
            ),
            "table": OutputCapability(
                CapabilitySupportLevel.DISCOVERY_ONLY,
                requires_exact_slice=True,
            ),
            "category_chart": OutputCapability(CapabilitySupportLevel.UNSUPPORTED),
            "boundary_map": OutputCapability(CapabilitySupportLevel.GUARDED),
            "thematic_map": OutputCapability(CapabilitySupportLevel.UNSUPPORTED),
        }

    @staticmethod
    def _discovery_only_comparison_policy() -> ComparisonPolicy:
        return ComparisonPolicy(
            tier=ComparisonCapabilityTier.DISCOVERY_ONLY,
            requires_same_unit_type=True,
            requires_same_exact_slice=True,
            requires_common_time_scope=True,
        )

    def geography(self, unit_type: str | None) -> GeographyCapability | None:
        if not unit_type:
            return None
        return self._geographies.get(unit_type)

    def dataset(self, theme_id: str | None) -> DatasetCapability | None:
        if not theme_id:
            return None
        if theme_id in self._datasets:
            return self._datasets[theme_id]
        return DatasetCapability(
            theme_id=theme_id,
            discovery=True,
            info_lookup=True,
            outputs=self._discovery_first_dataset_outputs(),
            comparison_policy=self._discovery_only_comparison_policy(),
            exact_slice_required=True,
            category_support=CategorySupportStatus.NONE_IN_V1,
            analysis_stance="discovery_first",
        )

    def enabled_reporting_geography_candidates(
        self,
        places: list[ResolvedPlaceResponse],
    ) -> list[ReportingGeographyRef]:
        if not places:
            return []
        by_unit_type: dict[str, list[int]] = {}
        labels: dict[str, str] = {}
        complete_candidates: dict[str, bool] = {}

        for place in places:
            per_place: dict[str, list[int]] = {}
            if place.units:
                for unit in place.units:
                    if self.geography(unit.unit_type) is None:
                        continue
                    per_place.setdefault(unit.unit_type, [])
                    if unit.unit_id not in per_place[unit.unit_type]:
                        per_place[unit.unit_type].append(unit.unit_id)
                    labels[unit.unit_type] = unit.unit_type_label or unit.unit_type
            elif len(place.place.unit_types) == 1:
                unit_type = place.place.unit_types[0]
                if self.geography(unit_type) is not None:
                    per_place[unit_type] = list(place.place.unit_ids)
                    labels[unit_type] = unit_type

            for unit_type in per_place:
                complete_candidates[unit_type] = complete_candidates.get(unit_type, True)
            for enabled_unit_type in self._geographies:
                if enabled_unit_type not in per_place:
                    complete_candidates[enabled_unit_type] = False

            for unit_type, unit_ids in per_place.items():
                by_unit_type.setdefault(unit_type, [])
                for unit_id in unit_ids:
                    if unit_id not in by_unit_type[unit_type]:
                        by_unit_type[unit_type].append(unit_id)

        candidates: list[ReportingGeographyRef] = []
        for unit_type, unit_ids in sorted(by_unit_type.items()):
            if not complete_candidates.get(unit_type):
                continue
            candidates.append(
                ReportingGeographyRef(
                    unit_type=unit_type,
                    unit_ids=unit_ids,
                    label=labels.get(unit_type) or unit_type,
                    scope_rule="selected_places",
                    slot_status=SlotStatus.CANDIDATE_SET,
                    source="capability_registry",
                )
            )
        if len(candidates) == 1:
            candidates[0].slot_status = SlotStatus.RESOLVED
        return candidates

    def representative_unit_id(
        self,
        reporting_geography: ReportingGeographyRef | None,
    ) -> int | None:
        if reporting_geography is None or not reporting_geography.unit_ids:
            return None
        return reporting_geography.unit_ids[0]

    def map_policy(self, unit_type: str | None) -> MapLoadingPolicy | None:
        geography = self.geography(unit_type)
        return geography.map_loading if geography is not None else None

    def geography_output_support(
        self,
        *,
        unit_type: str | None,
        output_mode: str,
    ) -> OutputCapability:
        if unit_type is None:
            if output_mode == "thematic_map":
                return OutputCapability(CapabilitySupportLevel.UNSUPPORTED)
            return OutputCapability(CapabilitySupportLevel.GUARDED)
        capability = self.geography(unit_type)
        if capability is None:
            return OutputCapability(CapabilitySupportLevel.UNSUPPORTED)
        return capability.outputs.get(output_mode, OutputCapability(CapabilitySupportLevel.UNSUPPORTED))

    def dataset_output_support(
        self,
        *,
        theme_id: str | None,
        output_mode: str,
    ) -> OutputCapability:
        if theme_id is None:
            if output_mode == "boundary_map":
                return OutputCapability(CapabilitySupportLevel.GUARDED)
            return OutputCapability(CapabilitySupportLevel.DISCOVERY_ONLY)
        capability = self.dataset(theme_id)
        if capability is None:
            return OutputCapability(CapabilitySupportLevel.UNSUPPORTED)
        return capability.outputs.get(output_mode, OutputCapability(CapabilitySupportLevel.UNSUPPORTED))

    @staticmethod
    def _combine_support_levels(
        first: CapabilitySupportLevel,
        second: CapabilitySupportLevel,
    ) -> CapabilitySupportLevel:
        if CapabilitySupportLevel.UNSUPPORTED in {first, second}:
            return CapabilitySupportLevel.UNSUPPORTED
        if CapabilitySupportLevel.DISCOVERY_ONLY in {first, second}:
            return CapabilitySupportLevel.DISCOVERY_ONLY
        return CapabilitySupportLevel.GUARDED

    def output_policy(
        self,
        *,
        output_mode: str,
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None,
        exact_slice: ExactSliceRef | None,
        wants_theme_context: bool,
    ) -> OutputPolicyAssessment:
        geography_support = self.geography_output_support(
            unit_type=reporting_geography.unit_type if reporting_geography is not None else None,
            output_mode=output_mode,
        )
        dataset_support = self.dataset_output_support(
            theme_id=dataset_family.theme_id if dataset_family is not None else None,
            output_mode=output_mode,
        )
        combined_support = self._combine_support_levels(
            geography_support.level,
            dataset_support.level,
        )
        dataset_policy = self.dataset(dataset_family.theme_id if dataset_family is not None else None)
        comparison_policy = (
            dataset_policy.comparison_policy
            if dataset_policy is not None
            else ComparisonPolicy(tier=ComparisonCapabilityTier.DISCOVERY_ONLY)
        )
        reason: str | None = None
        if combined_support == CapabilitySupportLevel.UNSUPPORTED:
            reason = "capability_policy_marks_this_output_as_unsupported"
        elif output_mode in {"trend_chart", "snapshot_chart", "table"} and dataset_family is None:
            reason = "choose_a_theme_before_this_output_can_run"
        elif output_mode == "boundary_map" and reporting_geography is None:
            reason = "choose_a_reporting_geography_before_this_map_can_run"
        elif output_mode == "category_chart" and dataset_policy is not None:
            if (
                dataset_policy.category_support
                == CategorySupportStatus.METADATA_INVENTORY_ONLY_AFTER_LIVE_VALIDATION
            ):
                reason = "category_breakdowns_are_catalog_visible_but_inventory_only_in_the_current_subset"
                combined_support = CapabilitySupportLevel.DISCOVERY_ONLY
            elif dataset_policy.category_support != CategorySupportStatus.EXECUTABLE:
                reason = "category_breakdowns_are_not_supported_for_this_family_in_the_current_subset"
                combined_support = CapabilitySupportLevel.UNSUPPORTED
        elif output_mode == "thematic_map":
            reason = "thematic_maps_remain_blocked_in_the_current_workflow_subset"
            combined_support = CapabilitySupportLevel.UNSUPPORTED
        elif output_mode != "boundary_map" and (dataset_policy is not None and dataset_policy.exact_slice_required) and exact_slice is None:
            reason = "one_exact_slice_is_required_before_this_output_can_run"
        elif combined_support == CapabilitySupportLevel.DISCOVERY_ONLY:
            reason = "this_family_is_catalog_visible_but_execution_remains_discovery_only"

        return OutputPolicyAssessment(
            output_mode=output_mode,
            geography_support=geography_support.level,
            dataset_support=dataset_support.level,
            combined_support=combined_support,
            requires_exact_slice=(
                output_mode != "boundary_map"
                and dataset_policy is not None
                and dataset_policy.exact_slice_required
            ),
            comparison_tier=comparison_policy.tier,
            map_loading=(
                self.geography(reporting_geography.unit_type).map_loading
                if reporting_geography is not None and self.geography(reporting_geography.unit_type) is not None
                else None
            ),
            reason=reason,
        )

    def discoverable_output_policies(
        self,
        *,
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None,
        exact_slice: ExactSliceRef | None,
        wants_theme_context: bool,
    ) -> list[OutputPolicyAssessment]:
        policies: list[OutputPolicyAssessment] = []
        for output_mode in self._DISCOVERY_OUTPUT_ORDER:
            policy = self.output_policy(
                output_mode=output_mode,
                reporting_geography=reporting_geography,
                dataset_family=dataset_family,
                exact_slice=exact_slice,
                wants_theme_context=wants_theme_context,
            )
            if policy.combined_support == CapabilitySupportLevel.UNSUPPORTED:
                continue
            policies.append(policy)
        return policies

    def comparison_assessment(
        self,
        *,
        places: list[ResolvedPlaceResponse],
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None,
        exact_slice: ExactSliceRef | None,
        time_scope: TimeScope | None,
    ) -> ComparisonAssessment:
        if len(places) <= 1:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.CURRENTLY_EXECUTABLE,
                reason="single_place_context_does_not_need_comparison_proof",
                policy=ComparisonPolicy(tier=ComparisonCapabilityTier.CURRENTLY_EXECUTABLE),
            )

        dataset_policy = self.dataset(dataset_family.theme_id if dataset_family is not None else None)
        if dataset_policy is None:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.DISCOVERY_ONLY,
                reason="comparison_requires_one_dataset_family",
                policy=ComparisonPolicy(tier=ComparisonCapabilityTier.DISCOVERY_ONLY),
            )
        if dataset_policy.comparison_policy.tier == ComparisonCapabilityTier.NOT_SUPPORTED:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.NOT_SUPPORTED,
                reason="capability_policy_does_not_support_comparison_for_this_family",
                policy=dataset_policy.comparison_policy,
            )
        if dataset_policy.comparison_policy.tier == ComparisonCapabilityTier.DISCOVERY_ONLY:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.DISCOVERY_ONLY,
                reason="comparison_remains_discovery_only_for_this_family",
                policy=dataset_policy.comparison_policy,
            )
        if reporting_geography is None:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.CANDIDATE_COMPARABLE,
                reason="shared_reporting_geography_is_not_resolved",
                policy=dataset_policy.comparison_policy,
            )

        geography_policy = self.geography(reporting_geography.unit_type)
        if geography_policy is None:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.NOT_SUPPORTED,
                reason="comparison_requires_a_supported_reporting_geography",
                policy=ComparisonPolicy(tier=ComparisonCapabilityTier.NOT_SUPPORTED),
            )

        if ComparisonCapabilityTier.NOT_SUPPORTED in {
            geography_policy.comparison_policy.tier,
            dataset_policy.comparison_policy.tier,
        }:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.NOT_SUPPORTED,
                reason="capability_policy_does_not_support_comparison_for_this_combination",
                policy=geography_policy.comparison_policy,
            )
        if ComparisonCapabilityTier.DISCOVERY_ONLY in {
            geography_policy.comparison_policy.tier,
            dataset_policy.comparison_policy.tier,
        }:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.DISCOVERY_ONLY,
                reason="comparison_remains_discovery_only_for_this_family",
                policy=dataset_policy.comparison_policy,
            )
        if reporting_geography is None or not reporting_geography.unit_ids:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.CANDIDATE_COMPARABLE,
                reason="shared_reporting_geography_is_not_resolved",
                policy=geography_policy.comparison_policy,
            )
        if exact_slice is None or not (
            exact_slice.cellref or exact_slice.dataitem_id or exact_slice.cat_id
        ):
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.CANDIDATE_COMPARABLE,
                reason="shared_exact_slice_is_not_resolved",
                policy=dataset_policy.comparison_policy,
            )
        if time_scope is None:
            return ComparisonAssessment(
                tier=ComparisonCapabilityTier.GUARD_VERIFIABLE,
                reason="shared_time_scope_is_not_resolved",
                policy=dataset_policy.comparison_policy,
            )
        return ComparisonAssessment(
            tier=ComparisonCapabilityTier.CURRENTLY_EXECUTABLE,
            reason="comparison_context_is_guard_verifiable",
            policy=dataset_policy.comparison_policy,
        )

    def output_switch_assessment(
        self,
        *,
        source_output_mode: str | None,
        target_output_mode: str | None,
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None,
        exact_slice: ExactSliceRef | None,
        requested_full_layer_map: bool = False,
    ) -> OutputSwitchAssessment:
        if target_output_mode is None:
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.REQUIRES_DISCOVERY,
                reason="target_output_mode_missing",
            )
        if source_output_mode == target_output_mode:
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.REUSE_VALIDATED_SPEC,
                reason="same_output_mode_reuses_the_validated_spec",
            )
        if target_output_mode == "thematic_map":
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.UNSUPPORTED,
                reason="thematic_maps_remain_blocked_in_the_current_workflow_subset",
            )

        output_policy = self.output_policy(
            output_mode=target_output_mode,
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            wants_theme_context=dataset_family is not None,
        )
        if output_policy.combined_support == CapabilitySupportLevel.UNSUPPORTED:
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.UNSUPPORTED,
                reason=output_policy.reason or "target_output_mode_is_not_supported",
            )
        if target_output_mode == "boundary_map":
            feasibility = self.output_feasibility(
                output_mode="boundary_map",
                reporting_geography=reporting_geography,
                dataset_family=dataset_family,
                exact_slice=exact_slice,
                wants_theme_context=dataset_family is not None,
                requested_full_layer_map=requested_full_layer_map,
            )
            if feasibility == OutputFeasibilityStatus.UNSUPPORTED:
                return OutputSwitchAssessment(
                    compatibility=OutputSwitchCompatibility.UNSUPPORTED,
                    reason="map_loading_policy_forbids_this_boundary_map_switch",
                )
            if source_output_mode in {"trend_chart", "snapshot_chart", "table", "category_chart"}:
                return OutputSwitchAssessment(
                    compatibility=OutputSwitchCompatibility.SAFE_DEFAULT_WITH_DISCLOSURE,
                    reason="measure_led_output_switch_defaults_to_boundary_map_only",
                    defaults_applied=("boundary_map_without_measure",),
                )
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.REUSE_VALIDATED_SPEC,
                reason="boundary_map_reuses_the_existing_geography_context",
            )
        if target_output_mode in {"trend_chart", "snapshot_chart", "table"}:
            if output_policy.combined_support == CapabilitySupportLevel.DISCOVERY_ONLY:
                return OutputSwitchAssessment(
                    compatibility=OutputSwitchCompatibility.REQUIRES_DISCOVERY,
                    reason=output_policy.reason or "target_output_mode_requires_catalog_discovery",
                )
            if reporting_geography is None or dataset_family is None or exact_slice is None:
                return OutputSwitchAssessment(
                    compatibility=OutputSwitchCompatibility.REQUIRES_DISCOVERY,
                    reason="validated_chart_or_table_switch_requires_reporting_geography_theme_and_exact_slice",
                )
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.REUSE_VALIDATED_SPEC,
                reason="validated_chart_or_table_switch_reuses_the_exact_spec",
            )
        if target_output_mode == "category_chart":
            return OutputSwitchAssessment(
                compatibility=OutputSwitchCompatibility.UNSUPPORTED,
                reason="category_chart_switches_remain_unsupported_in_the_current_subset",
            )
        return OutputSwitchAssessment(
            compatibility=OutputSwitchCompatibility.REQUIRES_DISCOVERY,
            reason="target_output_mode_requires_catalog_discovery",
        )

    def output_feasibility(
        self,
        *,
        output_mode: str,
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None,
        exact_slice: ExactSliceRef | None,
        wants_theme_context: bool,
        requested_full_layer_map: bool,
    ) -> OutputFeasibilityStatus:
        policy = self.output_policy(
            output_mode=output_mode,
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            wants_theme_context=wants_theme_context,
        )
        geography_policy = self.geography(
            reporting_geography.unit_type if reporting_geography is not None else None
        )

        if output_mode == "category_chart":
            return OutputFeasibilityStatus.UNSUPPORTED
        if policy.combined_support == CapabilitySupportLevel.UNSUPPORTED:
            return OutputFeasibilityStatus.UNSUPPORTED

        if output_mode == "boundary_map":
            if reporting_geography is None:
                return OutputFeasibilityStatus.NEEDS_CLARIFICATION
            if geography_policy is None or not geography_policy.boundary_map:
                return OutputFeasibilityStatus.UNSUPPORTED
            if (
                geography_policy.map_loading.mode == MapLoadingMode.BBOX_OR_IDS
                and requested_full_layer_map
                and not reporting_geography.unit_ids
            ):
                return OutputFeasibilityStatus.UNSUPPORTED
            return OutputFeasibilityStatus.AVAILABLE

        if output_mode in {"trend_chart", "snapshot_chart", "table"}:
            if reporting_geography is None or dataset_family is None:
                return OutputFeasibilityStatus.NEEDS_CLARIFICATION
            if geography_policy is None:
                return OutputFeasibilityStatus.UNSUPPORTED
            if policy.combined_support == CapabilitySupportLevel.DISCOVERY_ONLY:
                return OutputFeasibilityStatus.NEEDS_CLARIFICATION
            return OutputFeasibilityStatus.AVAILABLE

        return OutputFeasibilityStatus.NEEDS_CLARIFICATION


class DecisionPolicyEngine:
    def decide(
        self,
        *,
        context: ResolvedAnalysisContext,
        spec: AnalysisSpec | None,
        capability_registry: CapabilityRegistry,
    ) -> PolicyDecision:
        missing_fields: list[str] = []
        output_policy = capability_registry.output_policy(
            output_mode=context.output_mode,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            wants_theme_context=context.wants_theme_context,
        )
        comparison_assessment = capability_registry.comparison_assessment(
            places=context.places,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            time_scope=context.time_scope,
        )

        if not context.places and context.reporting_geography is None:
            clarification = ClarificationState(
                question="Choose a place or reporting geography first so I know what to analyse.",
                slot="place_identity",
            )
            return PolicyDecision(
                outcome=PolicyOutcome.ASK,
                path=WorkflowPath.ANALYSIS,
                reason="analysis_request_missing_place_context",
                missing_fields=["place_identity"],
                clarification=clarification,
                requested_output_mode=context.output_mode,
            )

        if spec is None:
            return PolicyDecision(
                outcome=PolicyOutcome.ASK,
                path=WorkflowPath.ANALYSIS,
                reason="analysis_spec_missing",
                missing_fields=["analysis_spec"],
                requested_output_mode=context.output_mode,
            )

        if spec.output_feasibility_status == OutputFeasibilityStatus.UNSUPPORTED:
            return PolicyDecision(
                outcome=PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                path=WorkflowPath.ANALYSIS,
                reason="requested_output_mode_not_supported_by_current_capability_policy",
                missing_fields=[],
                alternatives=self._unsupported_alternatives(context.output_mode),
                requested_output_mode=context.output_mode,
            )

        if output_policy.combined_support == CapabilitySupportLevel.DISCOVERY_ONLY:
            return PolicyDecision(
                outcome=PolicyOutcome.DISCOVERY,
                path=WorkflowPath.DISCOVERY,
                reason=output_policy.reason
                or "capability_policy_marks_this_combination_as_discovery_only",
                alternatives=[
                    "Ask what outputs are feasible for the current place and theme.",
                    "Ask what exact-slice-capable options are available here.",
                ],
                requested_output_mode=context.output_mode,
            )

        if spec.reporting_geography_status != ReportingGeographyStatus.RESOLVED:
            missing_fields.append("reporting_geography")
            if len(context.reporting_geography_candidates) > 1:
                clarification = ClarificationState(
                    question="Choose a reporting geography before I run analysis.",
                    slot="reporting_geography",
                    options=[
                        ClarificationOption(
                            option_id=candidate.unit_type or "unknown",
                            label=candidate.label or candidate.unit_type or "Unknown geography",
                            kind="reporting_geography",
                            metadata={"unit_type": candidate.unit_type},
                        )
                        for candidate in context.reporting_geography_candidates
                    ],
                )
                return PolicyDecision(
                    outcome=PolicyOutcome.ASK,
                    path=WorkflowPath.ANALYSIS,
                    reason="reporting_geography_is_materially_ambiguous",
                    missing_fields=missing_fields,
                    clarification=clarification,
                    requested_output_mode=context.output_mode,
                )
            return PolicyDecision(
                outcome=PolicyOutcome.DISCOVERY,
                path=WorkflowPath.DISCOVERY,
                reason="reporting_geography_needs_safe_discovery",
                missing_fields=missing_fields,
                alternatives=[
                    "Ask which reporting geographies are available here.",
                    "Choose a specific geography such as MOD_DIST or PAR_UNIT.",
                ],
                requested_output_mode=context.output_mode,
            )

        if len(context.places) > 1:
            if comparison_assessment.tier == ComparisonCapabilityTier.NOT_SUPPORTED:
                return PolicyDecision(
                    outcome=PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                    path=WorkflowPath.ANALYSIS,
                    reason=comparison_assessment.reason,
                    alternatives=[
                        "Ask which reporting geographies are shared across these places.",
                        "Reduce the request to one place or one exact slice first.",
                    ],
                    requested_output_mode=context.output_mode,
                )
            if comparison_assessment.tier == ComparisonCapabilityTier.DISCOVERY_ONLY:
                return PolicyDecision(
                    outcome=PolicyOutcome.DISCOVERY,
                    path=WorkflowPath.DISCOVERY,
                    reason=comparison_assessment.reason,
                    alternatives=[
                        "Ask what comparable options exist for these places.",
                        "Choose one family and one reporting geography first.",
                    ],
                    requested_output_mode=context.output_mode,
                )

        if context.output_mode == "boundary_map" and context.wants_theme_context:
            return PolicyDecision(
                outcome=PolicyOutcome.DEFAULT_WITH_DISCLOSURE,
                path=WorkflowPath.ANALYSIS,
                reason="boundary_map_is_safe_but_thematic_map_is_not_supported_in_v1",
                defaults_applied=["boundary_map_without_measure"],
                alternatives=["Ask what data is available for this geography."],
                requested_output_mode=context.output_mode,
            )

        if spec.dataset_status != DatasetStatus.RESOLVED and context.output_mode != "boundary_map":
            return PolicyDecision(
                outcome=PolicyOutcome.DISCOVERY,
                path=WorkflowPath.DISCOVERY,
                reason="dataset_family_needs_safe_discovery",
                missing_fields=["dataset_family"],
                alternatives=[
                    "Ask what themes are available for this place.",
                    "Choose a specific theme first.",
                ],
                requested_output_mode=context.output_mode,
            )

        if context.output_mode != "boundary_map" and spec.slice_status != SliceStatus.UNIQUE_EXECUTABLE_SLICE:
            return PolicyDecision(
                outcome=PolicyOutcome.DISCOVERY,
                path=WorkflowPath.DISCOVERY,
                reason="exact_slice_is_not_proven_executable",
                missing_fields=["exact_slice"],
                alternatives=[
                    "Ask what data is available for this place and theme.",
                    "Choose a more specific measure instead of a family-level theme.",
                ],
                requested_output_mode=context.output_mode,
            )

        if len(context.places) > 1 and spec.comparability_status != ComparabilityStatus.COMPARABLE:
            return PolicyDecision(
                outcome=PolicyOutcome.ASK,
                path=WorkflowPath.ANALYSIS,
                reason="comparison_is_not_explicitly_proven_comparable",
                missing_fields=["comparability"],
                alternatives=["Compare places only after choosing one reporting geography and one exact slice."],
                requested_output_mode=context.output_mode,
            )

        if context.output_mode in {"trend_chart", "snapshot_chart"} and context.time_scope is None:
            return PolicyDecision(
                outcome=PolicyOutcome.DEFAULT_WITH_DISCLOSURE,
                path=WorkflowPath.ANALYSIS,
                reason="time_scope_can_be_defaulted_after_other_slots_are_safe",
                defaults_applied=["default_time_scope"],
                requested_output_mode=context.output_mode,
            )

        return PolicyDecision(
            outcome=PolicyOutcome.EXECUTE,
            path=WorkflowPath.ANALYSIS,
            reason="analysis_spec_is_safe_to_execute",
            requested_output_mode=context.output_mode,
        )

    @staticmethod
    def _unsupported_alternatives(output_mode: str) -> list[str]:
        if output_mode == "category_chart":
            return [
                "Ask what data is available for this place and theme.",
                "Try a boundary map or a discovery question instead.",
            ]
        if output_mode == "boundary_map":
            return ["Choose a supported reporting geography first."]
        return [
            "Ask what outputs are feasible for the current place and theme.",
            "Choose a more specific safe request.",
        ]


class ExecutionGuard:
    def evaluate(
        self,
        *,
        context: ResolvedAnalysisContext,
        spec: AnalysisSpec | None,
        decision: PolicyDecision,
        capability_registry: CapabilityRegistry,
    ) -> ExecutionGuardResult:
        output_policy = capability_registry.output_policy(
            output_mode=context.output_mode,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            wants_theme_context=context.wants_theme_context,
        )
        comparison_assessment = capability_registry.comparison_assessment(
            places=context.places,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            time_scope=context.time_scope,
        )

        if decision.outcome == PolicyOutcome.ASK:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
                reason=decision.reason,
                blocking_fields=list(decision.missing_fields),
                notices=[decision.clarification.question] if decision.clarification else [],
                alternatives=list(decision.alternatives),
            )

        if decision.outcome == PolicyOutcome.DISCOVERY:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.DISCOVERY_RESPONSE,
                reason=decision.reason,
                blocking_fields=list(decision.missing_fields),
                alternatives=list(decision.alternatives),
            )

        if decision.outcome == PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                reason=decision.reason,
                alternatives=list(decision.alternatives),
            )

        if spec is None:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
                reason="analysis_spec_missing",
                blocking_fields=["analysis_spec"],
            )

        if spec.reporting_geography_status != ReportingGeographyStatus.RESOLVED:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
                reason="reporting_geography_must_be_resolved",
                blocking_fields=["reporting_geography"],
            )

        if spec.output_feasibility_status == OutputFeasibilityStatus.UNSUPPORTED:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                reason="requested_output_mode_not_supported",
                alternatives=decision.alternatives,
            )

        if output_policy.combined_support == CapabilitySupportLevel.DISCOVERY_ONLY:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.DISCOVERY_RESPONSE,
                reason=output_policy.reason
                or "capability_policy_marks_this_combination_as_discovery_only",
                blocking_fields=["capability_policy"],
                alternatives=list(decision.alternatives),
            )

        if context.output_mode != "boundary_map":
            if spec.dataset_status != DatasetStatus.RESOLVED:
                return ExecutionGuardResult(
                    outcome=ExecutionGuardOutcome.DISCOVERY_RESPONSE,
                    reason="dataset_family_not_resolved",
                    blocking_fields=["dataset_family"],
                    alternatives=decision.alternatives,
                )
            if spec.slice_status != SliceStatus.UNIQUE_EXECUTABLE_SLICE:
                return ExecutionGuardResult(
                    outcome=ExecutionGuardOutcome.DISCOVERY_RESPONSE,
                    reason="exact_slice_not_proven_executable",
                    blocking_fields=["exact_slice"],
                    alternatives=decision.alternatives,
                )
        if len(context.places) > 1 and comparison_assessment.tier == ComparisonCapabilityTier.NOT_SUPPORTED:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                reason=comparison_assessment.reason,
                blocking_fields=["comparability"],
                alternatives=decision.alternatives,
            )
        if len(context.places) > 1 and comparison_assessment.tier == ComparisonCapabilityTier.DISCOVERY_ONLY:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.DISCOVERY_RESPONSE,
                reason=comparison_assessment.reason,
                blocking_fields=["comparability"],
                alternatives=decision.alternatives,
            )
        if len(context.places) > 1 and spec.comparability_status != ComparabilityStatus.COMPARABLE:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
                reason="comparison_not_proven_comparable",
                blocking_fields=["comparability"],
                alternatives=decision.alternatives,
            )

        geography_policy = capability_registry.geography(
            spec.reporting_geography.unit_type if spec.reporting_geography is not None else None
        )
        if (
            context.output_mode == "boundary_map"
            and geography_policy is not None
            and geography_policy.map_loading_policy == "bbox_or_ids_only"
            and context.requested_full_layer_map
            and not (spec.reporting_geography and spec.reporting_geography.unit_ids)
        ):
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
                reason="map_loading_policy_forbids_full_layer_request",
                blocking_fields=["map_loading_policy"],
                alternatives=[
                    "Choose a specific place or bounded map extent first.",
                    "Use ids-based loading for PAR_UNIT maps.",
                ],
            )

        if decision.outcome == PolicyOutcome.DEFAULT_WITH_DISCLOSURE:
            return ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.SAFE_DEFAULT_PLAN,
                reason=decision.reason,
                notices=list(decision.defaults_applied),
                runnable_analysis_spec=spec,
                alternatives=list(decision.alternatives),
            )

        return ExecutionGuardResult(
            outcome=ExecutionGuardOutcome.RUNNABLE_ANALYSIS_SPEC,
            reason=decision.reason,
            runnable_analysis_spec=spec,
        )


class AnalysisExecutor:
    def __init__(
        self,
        *,
        series_service: SeriesService,
        maps_service: MapsService,
    ) -> None:
        self.series_service = series_service
        self.maps_service = maps_service

    def execute(
        self,
        *,
        context: ResolvedAnalysisContext,
        spec: AnalysisSpec,
        guard_result: ExecutionGuardResult,
    ) -> AnalysisExecutionResult:
        notices = list(guard_result.notices)

        if context.output_mode in {"trend_chart", "snapshot_chart", "table"}:
            cube_id = spec.exact_slice.cube_id if spec.exact_slice is not None else None
            if cube_id is None or spec.reporting_geography is None:
                raise ValueError("Runnable chart analysis requires exact slice and reporting geography.")
            cellrefs = [spec.exact_slice.cellref] if spec.exact_slice and spec.exact_slice.cellref else []
            dataitem_ids = (
                [spec.exact_slice.dataitem_id]
                if spec.exact_slice and spec.exact_slice.dataitem_id
                else []
            )
            series = self.series_service.get_time_series(
                TimeSeriesRequest(
                    unit_ids=list(spec.reporting_geography.unit_ids),
                    cube_ids=[cube_id],
                    cellrefs=cellrefs,
                    dataitem_ids=dataitem_ids,
                    start_year=(
                        float(spec.time_scope.year)
                        if spec.time_scope is not None and spec.time_scope.year is not None
                        else (spec.time_scope.start_year if spec.time_scope is not None else None)
                    ),
                    end_year=(
                        float(spec.time_scope.year)
                        if spec.time_scope is not None and spec.time_scope.year is not None
                        else (spec.time_scope.end_year if spec.time_scope is not None else None)
                    ),
                )
            )
            label = spec.dataset_family.label if spec.dataset_family is not None else "the selected dataset"
            table_payload = (
                {
                    "columns": [
                        {"id": "year", "name": "Year"},
                        {"id": "unit_name", "name": "Place"},
                        {"id": "cube_label", "name": "Measure"},
                        {"id": "value", "name": "Value"},
                    ],
                    "rows": [
                        {
                            "year": row.year,
                            "unit_name": row.unit_name,
                            "cube_label": row.cube_label,
                            "value": row.value,
                        }
                        for row in series.rows
                    ],
                }
                if context.output_mode == "table"
                else None
            )
            return AnalysisExecutionResult(
                time_series=series,
                table=table_payload,
                execution_summary={
                    "output_mode": context.output_mode,
                    "row_count": series.row_count,
                    "unit_ids": list(series.unit_ids),
                    "cube_id": cube_id,
                    "dataitem_id": (
                        spec.exact_slice.dataitem_id if spec.exact_slice is not None else None
                    ),
                    "cellref": spec.exact_slice.cellref if spec.exact_slice is not None else None,
                },
                fallback_text=(
                    f"I prepared a table for {label}."
                    if context.output_mode == "table"
                    else f"I fetched a trend chart for {label}."
                ),
                selected_cubes=[
                    CubeSummaryResponse(
                        theme_id=spec.dataset_family.theme_id if spec.dataset_family else "",
                        cube_id=cube_id,
                        label=spec.exact_slice.label or cube_id,
                        description=spec.exact_slice.description,
                        observation_count=series.row_count,
                        has_categories=bool(spec.exact_slice.has_categories),
                    )
                ],
                notices=notices,
            )

        if context.output_mode == "boundary_map":
            if spec.reporting_geography is None or spec.reporting_geography.unit_type is None:
                raise ValueError("Runnable map analysis requires reporting geography.")
            if spec.reporting_geography.unit_ids:
                features = self.maps_service.get_features_by_ids(
                    MapFeaturesByIdsQuery(
                        unit_type=spec.reporting_geography.unit_type,
                        ids=list(spec.reporting_geography.unit_ids),
                    )
                )
            else:
                features = self.maps_service.get_features(
                    MapFeaturesQuery(unit_type=spec.reporting_geography.unit_type)
                )
            return AnalysisExecutionResult(
                map_features=features,
                execution_summary={
                    "output_mode": context.output_mode,
                    "feature_count": features.feature_count,
                    "unit_type": features.unit_type,
                },
                fallback_text=(
                    f"I fetched {features.feature_count} map feature"
                    f"{'' if features.feature_count == 1 else 's'}."
                ),
                notices=notices,
            )

        if context.output_mode == "category_chart":
            cube_id = spec.exact_slice.cube_id if spec.exact_slice is not None else None
            if cube_id is None or spec.reporting_geography is None or spec.time_scope is None:
                raise ValueError("Runnable category analysis requires exact slice, reporting geography, and time.")
            year = spec.time_scope.year
            if year is None:
                raise ValueError("Runnable category analysis requires a snapshot year.")
            categories = self.series_service.get_category_breakdown(
                CategoryBreakdownRequest(
                    unit_ids=list(spec.reporting_geography.unit_ids),
                    cube_ids=[cube_id],
                    cellrefs=[spec.exact_slice.cellref] if spec.exact_slice.cellref else [],
                    dataitem_ids=[spec.exact_slice.dataitem_id] if spec.exact_slice.dataitem_id else [],
                    cat_ids=[spec.exact_slice.cat_id] if spec.exact_slice.cat_id else [],
                    year=year,
                )
            )
            return AnalysisExecutionResult(
                category_breakdown=categories,
                execution_summary={
                    "output_mode": context.output_mode,
                    "row_count": categories.row_count,
                    "year": categories.year,
                    "cube_id": cube_id,
                    "dataitem_id": spec.exact_slice.dataitem_id,
                    "cat_id": spec.exact_slice.cat_id,
                },
                fallback_text=f"I fetched a category breakdown for {year}.",
                selected_cubes=[
                    CubeSummaryResponse(
                        theme_id=spec.dataset_family.theme_id if spec.dataset_family else "",
                        cube_id=cube_id,
                        label=spec.exact_slice.label or cube_id,
                        description=spec.exact_slice.description,
                        observation_count=categories.row_count,
                        has_categories=bool(spec.exact_slice.has_categories),
                    )
                ],
                notices=notices,
            )

        raise ValueError(f"Unsupported output mode {context.output_mode!r}.")
