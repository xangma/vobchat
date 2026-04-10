from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from vobchat.api.schemas.common import APIModel
from vobchat.api.schemas.maps import MapFeatureCollectionResponse
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityResolutionResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.schemas.places import (
    PlaceCandidateResponse,
    PostcodeLookupResponse,
    ResolvedPlaceResponse,
)
from vobchat.api.schemas.series import CategoryBreakdownResponse, TimeSeriesResponse
from vobchat.api.schemas.themes import (
    CubeSummaryResponse,
    ThemeListResponse,
    ThemeSummaryResponse,
)


class ChatOperation(str, Enum):
    SEARCH_PLACES = "search_places"
    LOOKUP_POSTCODE = "lookup_postcode"
    RESOLVE_PLACE = "resolve_place"
    REMOVE_PLACE = "remove_place"
    LIST_CATALOG_OVERVIEW = "list_catalog_overview"
    LIST_THEMES = "list_themes"
    LIST_REPORTING_GEOGRAPHIES = "list_reporting_geographies"
    LIST_FEASIBLE_OUTPUTS = "list_feasible_outputs"
    LIST_AVAILABLE_YEARS = "list_available_years"
    LIST_EXACT_SLICE_OPTIONS = "list_exact_slice_options"
    LIST_COMPARABLE_OPTIONS = "list_comparable_options"
    RESOLVE_THEME = "resolve_theme"
    LIST_CUBES_FOR_THEME_AND_UNIT = "list_cubes_for_theme_and_unit"
    FETCH_TIME_SERIES = "fetch_time_series"
    FETCH_CATEGORY_BREAKDOWN = "fetch_category_breakdown"
    FETCH_MAP_FEATURES = "fetch_map_features"
    FETCH_PLACE_PROFILE = "fetch_place_profile"
    FETCH_UNIT_TYPE_INFO = "fetch_unit_type_info"
    RESOLVE_DATA_ENTITY = "resolve_data_entity"
    FETCH_DATA_ENTITY_INFO = "fetch_data_entity_info"
    REPLY_ONLY = "reply_only"
    CLARIFY = "clarify"


class ChatSSEEventName(str, Enum):
    THREAD_CREATED = "thread.created"
    TURN_STARTED = "turn.started"
    ASSISTANT_DELTA = "assistant.delta"
    UI_DELTA = "ui.delta"
    ASSISTANT_COMPLETED = "assistant.completed"
    TURN_COMPLETED = "turn.completed"
    ERROR = "error"


class WorkflowPath(str, Enum):
    CHAT_ONLY = "chat_only"
    INFO_LOOKUP = "info_lookup"
    DISCOVERY = "discovery"
    ANALYSIS = "analysis"
    FOLLOW_UP_EDIT = "follow_up_edit"


class ReceiptKind(str, Enum):
    ANALYSIS = "analysis"
    DISCOVERY = "discovery"
    INFO = "info"


class SlotStatus(str, Enum):
    RESOLVED = "resolved"
    CANDIDATE_SET = "candidate_set"
    INHERITED_FROM_RECEIPT = "inherited_from_receipt"
    DEFAULTABLE = "defaultable"
    NOT_REQUIRED = "not_required"
    BLOCKED = "blocked"


class ReportingGeographyStatus(str, Enum):
    RESOLVED = "resolved"
    NEEDS_CLARIFICATION = "needs_clarification"


class DatasetStatus(str, Enum):
    RESOLVED = "resolved"
    NEEDS_CLARIFICATION = "needs_clarification"
    DISCOVERY_ONLY = "discovery_only"


class SliceStatus(str, Enum):
    UNIQUE_EXECUTABLE_SLICE = "unique_executable_slice"
    CANDIDATE_SET = "candidate_set"
    FAMILY_ONLY = "family_only"
    NEEDS_EXACT_SLICE_RESOLUTION = "needs_exact_slice_resolution"


class ComparabilityStatus(str, Enum):
    COMPARABLE = "comparable"
    NOT_COMPARABLE = "not_comparable"
    NEEDS_CLARIFICATION = "needs_clarification"


class AvailabilityStatus(str, Enum):
    AVAILABLE = "available"
    PARTIALLY_AVAILABLE = "partially_available"
    UNAVAILABLE = "unavailable"


class OutputFeasibilityStatus(str, Enum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    NEEDS_CLARIFICATION = "needs_clarification"


class CapabilitySupportLevel(str, Enum):
    GUARDED = "guarded"
    DISCOVERY_ONLY = "discovery_only"
    UNSUPPORTED = "unsupported"


class MapLoadingMode(str, Enum):
    FULL_MAP = "full_map"
    BBOX_ONLY = "bbox_only"
    IDS_ONLY = "ids_only"
    BBOX_OR_IDS = "bbox_or_ids"


class ComparisonCapabilityTier(str, Enum):
    NOT_SUPPORTED = "not_supported"
    DISCOVERY_ONLY = "discovery_only"
    CANDIDATE_COMPARABLE = "candidate_comparable"
    GUARD_VERIFIABLE = "guard_verifiable"
    CURRENTLY_EXECUTABLE = "currently_executable"


class OutputSwitchCompatibility(str, Enum):
    REUSE_VALIDATED_SPEC = "reuse_validated_spec"
    SAFE_DEFAULT_WITH_DISCLOSURE = "safe_default_with_disclosure"
    REQUIRES_DISCOVERY = "requires_discovery"
    UNSUPPORTED = "unsupported"


class CategorySupportStatus(str, Enum):
    NONE_IN_V1 = "none_in_v1"
    METADATA_INVENTORY_ONLY_AFTER_LIVE_VALIDATION = (
        "metadata_inventory_only_after_live_validation"
    )
    EXECUTABLE = "executable"


class StatePatchType(str, Enum):
    REPLACE_PLACE = "ReplacePlace"
    ADD_PLACE = "AddPlace"
    REMOVE_PLACE = "RemovePlace"
    SET_TIME = "SetTime"
    SET_OUTPUT_MODE = "SetOutputMode"


class PolicyOutcome(str, Enum):
    ASK = "ask"
    DEFAULT_WITH_DISCLOSURE = "default_with_disclosure"
    EXECUTE = "execute"
    DISCOVERY = "discovery"
    UNAVAILABLE_WITH_ALTERNATIVES = "unavailable_with_alternatives"


class ExecutionGuardOutcome(str, Enum):
    RUNNABLE_ANALYSIS_SPEC = "runnable_analysis_spec"
    NEEDS_CLARIFICATION = "needs_clarification"
    SAFE_DEFAULT_PLAN = "safe_default_plan"
    DISCOVERY_RESPONSE = "discovery_response"
    UNAVAILABLE_WITH_ALTERNATIVES = "unavailable_with_alternatives"


class DiscoveryTopic(str, Enum):
    CATALOG_OVERVIEW = "catalog_overview"
    THEMES = "themes"
    REPORTING_GEOGRAPHIES = "reporting_geographies"
    FEASIBLE_OUTPUTS = "feasible_outputs"
    AVAILABLE_YEARS = "available_years"
    EXACT_SLICE_OPTIONS = "exact_slice_options"
    COMPARABLE_OPTIONS = "comparable_options"


class ProvenanceResultType(str, Enum):
    EXECUTED_ANALYSIS = "executed_analysis"
    CATALOG_DISCOVERY = "catalog_discovery"
    INFO_LOOKUP = "info_lookup"


class WorkflowRuntimeMode(str, Enum):
    LLM_ASSISTED = "llm_assisted"
    DETERMINISTIC_DEGRADED = "deterministic_degraded"


class DegradedModeOutcome(str, Enum):
    DETERMINISTIC_SUCCESS = "deterministic_success"
    GUIDED_CLARIFICATION = "guided_clarification"
    UNSUPPORTED_IN_DEGRADED_MODE = "unsupported_in_degraded_mode"
    SETUP_GUIDANCE = "setup_guidance"


class ChatMessage(APIModel):
    message_id: str
    role: Literal["system", "user", "assistant"]
    content: str
    created_at: datetime


class SemanticPlaceRef(APIModel):
    place_id: int
    label: str
    unit_ids: list[int] = Field(default_factory=list)
    unit_types: list[str] = Field(default_factory=list)
    source: str | None = None

    @classmethod
    def from_resolved_place(
        cls,
        place: ResolvedPlaceResponse,
        *,
        source: str | None = None,
    ) -> "SemanticPlaceRef":
        return cls(
            place_id=place.place.place_id,
            label=place.place.name,
            unit_ids=list(place.place.unit_ids),
            unit_types=list(place.place.unit_types),
            source=source,
        )


class ReportingGeographyRef(APIModel):
    unit_type: str | None = None
    unit_ids: list[int] = Field(default_factory=list)
    label: str | None = None
    scope_rule: str | None = None
    slot_status: SlotStatus | None = None
    source: str | None = None


class DatasetFamilyRef(APIModel):
    theme_id: str | None = None
    cube_id: str | None = None
    label: str | None = None
    description: str | None = None
    slot_status: SlotStatus | None = None
    source: str | None = None

    @classmethod
    def from_theme(
        cls,
        theme: ThemeSummaryResponse,
        *,
        source: str | None = None,
        slot_status: SlotStatus | None = None,
    ) -> "DatasetFamilyRef":
        return cls(
            theme_id=theme.theme_id,
            label=theme.label,
            description=theme.description,
            source=source,
            slot_status=slot_status,
        )


class CategoryEntityRef(APIModel):
    entity_id: str | None = None
    label: str | None = None
    group_label: str | None = None
    source: str | None = None
    provenance: str | None = None


class ExactSliceRef(APIModel):
    cube_id: str | None = None
    cube_ids: list[str] = Field(default_factory=list)
    label: str | None = None
    description: str | None = None
    cellref: str | None = None
    dataitem_id: str | None = None
    cat_id: str | None = None
    view_id: str | None = None
    has_categories: bool | None = None
    category_entity: CategoryEntityRef | None = None
    start_year: float | None = None
    end_year: float | None = None
    observation_count: int | None = None
    metadata_provenance: str | None = None
    slot_status: SlotStatus | None = None
    source: str | None = None

    @classmethod
    def from_cube(
        cls,
        cube: CubeSummaryResponse,
        *,
        source: str | None = None,
        slot_status: SlotStatus | None = None,
    ) -> "ExactSliceRef":
        return cls(
            cube_id=cube.cube_id,
            cube_ids=[cube.cube_id],
            label=cube.label,
            description=cube.description,
            has_categories=cube.has_categories,
            start_year=cube.start_year,
            end_year=cube.end_year,
            observation_count=cube.observation_count,
            source=source,
            slot_status=slot_status,
        )

    @classmethod
    def from_candidate(
        cls,
        candidate: "ExactSliceCandidate",
        *,
        source: str | None = None,
        slot_status: SlotStatus | None = None,
    ) -> "ExactSliceRef":
        return cls(
            cube_id=candidate.cube_id,
            cube_ids=list(candidate.cube_ids),
            label=candidate.label,
            description=candidate.description,
            cellref=candidate.cellref,
            dataitem_id=candidate.dataitem_id,
            cat_id=candidate.cat_id,
            view_id=candidate.view_id,
            has_categories=candidate.has_categories,
            category_entity=(
                candidate.category_entity.model_copy(deep=True)
                if candidate.category_entity is not None
                else None
            ),
            start_year=candidate.start_year,
            end_year=candidate.end_year,
            observation_count=candidate.observation_count,
            metadata_provenance=candidate.metadata_provenance,
            source=source or candidate.source,
            slot_status=slot_status or candidate.slot_status,
        )


class ExactSliceCandidate(APIModel):
    cube_id: str | None = None
    cube_ids: list[str] = Field(default_factory=list)
    label: str | None = None
    description: str | None = None
    cellref: str | None = None
    dataitem_id: str | None = None
    cat_id: str | None = None
    view_id: str | None = None
    has_categories: bool | None = None
    category_entity: CategoryEntityRef | None = None
    start_year: float | None = None
    end_year: float | None = None
    observation_count: int | None = None
    metadata_provenance: str | None = None
    slot_status: SlotStatus | None = None
    source: str | None = None


class TimeScope(APIModel):
    mode: str | None = None
    year: int | None = None
    start_year: float | None = None
    end_year: float | None = None
    label: str | None = None
    source: str | None = None


class ClarificationOption(APIModel):
    option_id: str
    label: str
    kind: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClarificationState(APIModel):
    question: str
    slot: str | None = None
    options: list[ClarificationOption] = Field(default_factory=list)
    allow_freeform: bool = True
    source_receipt_id: str | None = None


class DiscourseOptionKind(str, Enum):
    SELECTED_PLACE = "selected_place"
    CLARIFICATION_OPTION = "clarification_option"
    PLACE_CANDIDATE = "place_candidate"
    REPORTING_GEOGRAPHY = "reporting_geography"
    THEME = "theme"
    EXACT_SLICE = "exact_slice"
    SUGGESTED_ACTION = "suggested_action"
    RECEIPT = "receipt"
    TIME_OPTION = "time_option"


class DiscourseOptionRef(APIModel):
    option_id: str
    label: str
    kind: DiscourseOptionKind
    ordinal: int | None = None
    source_receipt_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscourseOptionSet(APIModel):
    set_id: str
    title: str
    kind: DiscourseOptionKind
    source_receipt_id: str | None = None
    preferred: bool = False
    options: list[DiscourseOptionRef] = Field(default_factory=list)


class DiscourseAnchors(APIModel):
    active_receipt_id: str | None = None
    recent_receipt_ids: list[str] = Field(default_factory=list)
    selected_place_ids: list[int] = Field(default_factory=list)
    current_output_mode: str | None = None
    current_time_scope: TimeScope | None = None
    option_sets: list[DiscourseOptionSet] = Field(default_factory=list)


class ConversationState(APIModel):
    transcript: list[ChatMessage] = Field(default_factory=list)
    current_focus: str | None = None
    pending_clarification: ClarificationState | None = None
    current_receipt_id: str | None = None
    recent_receipt_ids: list[str] = Field(default_factory=list)
    discourse_anchors: DiscourseAnchors = Field(default_factory=DiscourseAnchors)


class ResolutionState(APIModel):
    place_candidates: list[PlaceCandidateResponse] = Field(default_factory=list)
    resolved_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    reporting_geography_candidates: list[ReportingGeographyRef] = Field(default_factory=list)
    selected_reporting_geography: ReportingGeographyRef | None = None
    dataset_family_candidates: list[DatasetFamilyRef] = Field(default_factory=list)
    slice_candidates: list[ExactSliceCandidate] = Field(default_factory=list)
    time_candidates: list[TimeScope] = Field(default_factory=list)
    ambiguity_flags: dict[str, bool] = Field(default_factory=dict)


class AnalysisSpec(APIModel):
    places: list[SemanticPlaceRef] = Field(default_factory=list)
    reporting_geography: ReportingGeographyRef | None = None
    dataset_family: DatasetFamilyRef | None = None
    exact_slice: ExactSliceRef | None = None
    time_scope: TimeScope | None = None
    output_mode: str | None = None
    comparison_mode: str | None = None
    reporting_geography_status: ReportingGeographyStatus = (
        ReportingGeographyStatus.NEEDS_CLARIFICATION
    )
    dataset_status: DatasetStatus = DatasetStatus.NEEDS_CLARIFICATION
    slice_status: SliceStatus = SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION
    comparability_status: ComparabilityStatus = ComparabilityStatus.NEEDS_CLARIFICATION
    availability_status: AvailabilityStatus = AvailabilityStatus.UNAVAILABLE
    output_feasibility_status: OutputFeasibilityStatus = (
        OutputFeasibilityStatus.NEEDS_CLARIFICATION
    )


class AnalysisState(APIModel):
    resolved_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    reporting_geography: ReportingGeographyRef | None = None
    dataset_family: DatasetFamilyRef | None = None
    candidate_slice: ExactSliceRef | None = None
    analysis_spec: AnalysisSpec | None = None
    time_scope: TimeScope | None = None
    output_mode: str | None = None
    defaults_used: list[str] = Field(default_factory=list)
    availability_status: AvailabilityStatus | None = None
    comparability_status: ComparabilityStatus | None = None


class WorkflowRuntimeState(APIModel):
    mode: WorkflowRuntimeMode = WorkflowRuntimeMode.LLM_ASSISTED
    degraded_reason: str | None = None
    degraded_outcome: DegradedModeOutcome | None = None
    detail: str | None = None
    notice: str | None = None
    setup_guidance: list[str] = Field(default_factory=list)


class ProjectionReferences(APIModel):
    ui_projection_id: str | None = None
    render_projection_id: str | None = None


class ProvenanceResolvedContext(APIModel):
    place_labels: list[str] = Field(default_factory=list)
    reporting_geography_label: str | None = None
    reporting_unit_type: str | None = None
    dataset_family_label: str | None = None
    theme_id: str | None = None
    exact_slice_label: str | None = None
    cube_id: str | None = None
    time_scope_label: str | None = None
    output_mode: str | None = None


class ExactSliceProvenance(APIModel):
    theme_id: str | None = None
    cube_id: str | None = None
    label: str | None = None
    cellref: str | None = None
    dataitem_id: str | None = None
    cat_id: str | None = None
    category_label: str | None = None
    metadata_provenance: str | None = None
    resolution_basis: str | None = None


class ProvenanceSourceContext(APIModel):
    source_operation: ChatOperation | None = None
    source_receipt_id: str | None = None
    follow_up_patch: StatePatchType | None = None
    policy_outcome: PolicyOutcome | None = None
    guard_outcome: ExecutionGuardOutcome | None = None
    runtime_mode: WorkflowRuntimeMode | None = None
    degraded_outcome: DegradedModeOutcome | None = None


class ProvenanceSummary(APIModel):
    result_type: ProvenanceResultType
    receipt_kind: ReceiptKind | None = None
    workflow_path: WorkflowPath | None = None
    summary: str | None = None
    discovery_only: bool = False
    executed_analysis: bool = False
    runtime_mode: WorkflowRuntimeMode = WorkflowRuntimeMode.LLM_ASSISTED
    degraded_outcome: DegradedModeOutcome | None = None
    resolved_context: ProvenanceResolvedContext = Field(default_factory=ProvenanceResolvedContext)
    exact_slice: ExactSliceProvenance | None = None
    defaults_used: list[str] = Field(default_factory=list)
    heuristics_used: list[str] = Field(default_factory=list)
    safe_downgrades: list[str] = Field(default_factory=list)
    availability_limits: list[str] = Field(default_factory=list)
    comparability_limits: list[str] = Field(default_factory=list)
    capability_notes: list[str] = Field(default_factory=list)
    source_context: ProvenanceSourceContext = Field(default_factory=ProvenanceSourceContext)


class UIProjection(APIModel):
    projection_id: str | None = None
    current_receipt_id: str | None = None
    current_receipt_kind: ReceiptKind | None = None
    runtime_state: WorkflowRuntimeState = Field(default_factory=WorkflowRuntimeState)
    selected_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    reporting_geography: ReportingGeographyRef | None = None
    dataset_family: ThemeSummaryResponse | None = None
    available_themes: list[ThemeSummaryResponse] = Field(default_factory=list)
    available_cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    selected_cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    exact_slice: ExactSliceRef | None = None
    analysis_spec: AnalysisSpec | None = None
    time_scope: TimeScope | None = None
    active_output_mode: str | None = None
    clarification: ClarificationState | None = None
    discovery_result: DiscoveryResult | None = None
    feasible_next_actions: list[str] = Field(default_factory=list)
    defaults_used: list[str] = Field(default_factory=list)
    notices: list[str] = Field(default_factory=list)
    provenance_summary: ProvenanceSummary | None = None


class RenderProjection(APIModel):
    projection_id: str | None = None
    receipt_id: str | None = None
    active_output_mode: str | None = None
    answer_text: str | None = None
    chart: TimeSeriesResponse | CategoryBreakdownResponse | None = None
    table: dict[str, Any] | None = None
    boundary_map: MapFeatureCollectionResponse | None = None
    thematic_map: dict[str, Any] | None = None
    thematic_map_status: str | None = None
    metadata_payload: dict[str, Any] | None = None
    legend: dict[str, Any] = Field(default_factory=dict)
    notices: list[str] = Field(default_factory=list)
    provenance_summary: ProvenanceSummary | None = None


class AnalysisReceipt(APIModel):
    kind: ReceiptKind
    receipt_id: str
    path: WorkflowPath
    source_operation: ChatOperation | None = None
    analysis_spec: AnalysisSpec | None = None
    discovery_result: DiscoveryResult | None = None
    anchors: dict[str, Any] = Field(default_factory=dict)
    catalog_summary: dict[str, Any] = Field(default_factory=dict)
    entity: dict[str, Any] = Field(default_factory=dict)
    defaults_used: list[str] = Field(default_factory=list)
    notices: list[str] = Field(default_factory=list)
    provenance_summary: ProvenanceSummary | None = None
    projections: ProjectionReferences = Field(default_factory=ProjectionReferences)
    ui_projection: UIProjection | None = None
    render_projection: RenderProjection | None = None
    execution_summary: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class StatePatchTarget(APIModel):
    raw_text: str | None = None
    place_id: int | None = None
    place_ids: list[int] = Field(default_factory=list)
    label: str | None = None
    labels: list[str] = Field(default_factory=list)
    option_id: str | None = None
    receipt_id: str | None = None


class StatePatch(APIModel):
    patch_type: StatePatchType
    source_receipt_id: str | None = None
    target: StatePatchTarget | None = None
    time_scope: TimeScope | None = None
    output_mode: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryItem(APIModel):
    item_id: str
    label: str
    kind: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryOutputOption(APIModel):
    output_mode: str
    label: str
    feasibility: OutputFeasibilityStatus | None = None
    support_level: CapabilitySupportLevel | None = None
    requires_exact_slice: bool = False
    comparison_tier: ComparisonCapabilityTier | None = None
    map_loading_mode: MapLoadingMode | None = None
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryYearCoverage(APIModel):
    item_id: str
    label: str
    theme_id: str | None = None
    cube_id: str | None = None
    cellref: str | None = None
    dataitem_id: str | None = None
    start_year: float | None = None
    end_year: float | None = None
    executable: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoverySuggestedAction(APIModel):
    label: str
    operation: ChatOperation | None = None
    unit_type: str | None = None
    theme_id: str | None = None
    cube_id: str | None = None
    output_mode: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryResult(APIModel):
    topic: DiscoveryTopic
    title: str
    subtype: str | None = None
    summary: str | None = None
    items: list[DiscoveryItem] = Field(default_factory=list)
    place_context: list[SemanticPlaceRef] = Field(default_factory=list)
    reporting_geographies: list[ReportingGeographyRef] = Field(default_factory=list)
    dataset_families: list[DatasetFamilyRef] = Field(default_factory=list)
    exact_slice_options: list[ExactSliceCandidate] = Field(default_factory=list)
    supported_outputs: list[DiscoveryOutputOption] = Field(default_factory=list)
    available_years: list[DiscoveryYearCoverage] = Field(default_factory=list)
    comparability_notes: list[str] = Field(default_factory=list)
    availability_notes: list[str] = Field(default_factory=list)
    suggested_actions: list[DiscoverySuggestedAction] = Field(default_factory=list)
    anchors: dict[str, Any] = Field(default_factory=dict)
    notices: list[str] = Field(default_factory=list)


class PolicyDecision(APIModel):
    outcome: PolicyOutcome
    path: WorkflowPath | None = None
    reason: str
    missing_fields: list[str] = Field(default_factory=list)
    defaults_applied: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    clarification: ClarificationState | None = None
    requested_output_mode: str | None = None


class ExecutionGuardResult(APIModel):
    outcome: ExecutionGuardOutcome
    reason: str
    blocking_fields: list[str] = Field(default_factory=list)
    notices: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    runnable_analysis_spec: AnalysisSpec | None = None


class PlannerAction(APIModel):
    operation: ChatOperation
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    output_mode: str | None = None
    match_mode: Literal["exact", "fuzzy"] | None = None
    place_query: str | None = None
    place_id: int | None = None
    postcode: str | None = None
    theme_query: str | None = None
    theme_id: str | None = None
    cube_query: str | None = None
    cube_id: str | None = None
    exact_slice: ExactSliceRef | None = None
    cube_ids: list[str] = Field(default_factory=list)
    year: int | None = None
    start_year: float | None = None
    end_year: float | None = None
    unit_type: str | None = None
    entity_query: str | None = None
    entity_id: str | None = None
    assistant_task: str | None = None

    @field_validator("cube_ids", mode="before")
    @classmethod
    def _normalize_cube_ids(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return [str(item) for item in value]


class PlannerResult(APIModel):
    source: Literal["llm", "fallback"] = "fallback"
    action: PlannerAction
    clarification_question: str | None = None
    notes: str | None = None


class ChatUIStateDelta(APIModel):
    operation: ChatOperation | None = None
    selected_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    selected_theme: ThemeSummaryResponse | None = None
    selected_cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    place_search_results: list[PlaceCandidateResponse] = Field(default_factory=list)
    postcode_lookup: PostcodeLookupResponse | None = None
    themes: ThemeListResponse | None = None
    cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    time_series: TimeSeriesResponse | None = None
    category_breakdown: CategoryBreakdownResponse | None = None
    map_features: MapFeatureCollectionResponse | None = None
    place_profile: PlaceProfileResponse | None = None
    unit_type_info: UnitTypeInfoResponse | None = None
    data_entity_resolution: DataEntityResolutionResponse | None = None
    data_entity_info: DataEntityInfoResponse | None = None
    discovery_result: DiscoveryResult | None = None
    conversation_state: ConversationState | None = None
    resolution_state: ResolutionState | None = None
    analysis_state: AnalysisState | None = None
    policy_decision: PolicyDecision | None = None
    execution_guard_result: ExecutionGuardResult | None = None
    runtime_state: WorkflowRuntimeState = Field(default_factory=WorkflowRuntimeState)
    current_receipt_id: str | None = None
    current_receipt: AnalysisReceipt | None = None
    ui_projection: UIProjection | None = None
    render_projection: RenderProjection | None = None
    pending_clarification: ClarificationState | None = None
    notices: list[str] = Field(default_factory=list)
    cleared_places: bool = False

    @model_validator(mode="after")
    def _sync_projection_compatibility(self) -> "ChatUIStateDelta":
        if self.ui_projection is not None:
            self.selected_places = list(self.ui_projection.selected_places)
            self.selected_theme = self.ui_projection.dataset_family
            self.selected_cubes = list(self.ui_projection.selected_cubes)
            self.discovery_result = self.ui_projection.discovery_result
            self.pending_clarification = self.ui_projection.clarification
            self.runtime_state = self.ui_projection.runtime_state.model_copy(deep=True)
            if self.ui_projection.notices:
                self.notices = list(self.ui_projection.notices)
        if self.current_receipt is not None and self.current_receipt_id is None:
            self.current_receipt_id = self.current_receipt.receipt_id
        return self


class ChatThreadState(APIModel):
    thread_id: str
    messages: list[ChatMessage] = Field(default_factory=list)
    conversation_state: ConversationState = Field(default_factory=ConversationState)
    resolution_state: ResolutionState = Field(default_factory=ResolutionState)
    analysis_state: AnalysisState = Field(default_factory=AnalysisState)
    runtime_state: WorkflowRuntimeState = Field(default_factory=WorkflowRuntimeState)
    policy_decision: PolicyDecision | None = None
    execution_guard_result: ExecutionGuardResult | None = None
    current_receipt_id: str | None = None
    current_receipt: AnalysisReceipt | None = None
    recent_receipts: list[AnalysisReceipt] = Field(default_factory=list)
    latest_discovery_result: DiscoveryResult | None = None
    ui_projection: UIProjection | None = None
    render_projection: RenderProjection | None = None
    notices: list[str] = Field(default_factory=list)
    pending_clarification: ClarificationState | None = None
    selected_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    selected_theme: ThemeSummaryResponse | None = None
    selected_cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    latest_ui_delta: ChatUIStateDelta | None = None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def _sync_semantic_views(self) -> "ChatThreadState":
        if self.messages:
            self.conversation_state.transcript = list(self.messages)
        elif self.conversation_state.transcript:
            self.messages = list(self.conversation_state.transcript)

        if self.pending_clarification is None:
            self.pending_clarification = self.conversation_state.pending_clarification
        else:
            self.conversation_state.pending_clarification = self.pending_clarification

        if self.current_receipt is not None and self.current_receipt_id is None:
            self.current_receipt_id = self.current_receipt.receipt_id
        if self.current_receipt_id is not None:
            self.conversation_state.current_receipt_id = self.current_receipt_id
        elif self.conversation_state.current_receipt_id is not None:
            self.current_receipt_id = self.conversation_state.current_receipt_id

        if self.recent_receipts:
            self.conversation_state.recent_receipt_ids = [
                receipt.receipt_id for receipt in self.recent_receipts
            ]
        elif self.conversation_state.recent_receipt_ids and self.current_receipt is not None:
            self.conversation_state.recent_receipt_ids = [
                self.current_receipt.receipt_id,
                *[
                    receipt_id
                    for receipt_id in self.conversation_state.recent_receipt_ids
                    if receipt_id != self.current_receipt.receipt_id
                ],
            ]

        if self.latest_discovery_result is None and self.current_receipt is not None:
            self.latest_discovery_result = self.current_receipt.discovery_result

        if self.ui_projection is not None:
            self.runtime_state = self.ui_projection.runtime_state.model_copy(deep=True)
            self.selected_places = list(self.ui_projection.selected_places)
            self.selected_theme = self.ui_projection.dataset_family
            self.selected_cubes = list(self.ui_projection.selected_cubes)
            if self.ui_projection.clarification is not None:
                self.pending_clarification = self.ui_projection.clarification
                self.conversation_state.pending_clarification = self.pending_clarification
            if self.ui_projection.notices:
                self.notices = list(self.ui_projection.notices)
        elif self.latest_ui_delta is not None:
            self.runtime_state = self.latest_ui_delta.runtime_state.model_copy(deep=True)
        if self.latest_ui_delta is not None and self.latest_ui_delta.ui_projection is not None:
            projection = self.latest_ui_delta.ui_projection
            self.runtime_state = projection.runtime_state.model_copy(deep=True)
            self.selected_places = list(projection.selected_places)
            self.selected_theme = projection.dataset_family
            self.selected_cubes = list(projection.selected_cubes)
            if projection.clarification is not None:
                self.pending_clarification = projection.clarification
                self.conversation_state.pending_clarification = self.pending_clarification
            if projection.notices:
                self.notices = list(projection.notices)

        if not self.resolution_state.resolved_places and self.selected_places:
            self.resolution_state.resolved_places = list(self.selected_places)
        if not self.analysis_state.resolved_places and self.selected_places:
            self.analysis_state.resolved_places = list(self.selected_places)

        return self


class ChatThreadCreateResponse(APIModel):
    thread_id: str
    state: ChatThreadState


class ChatTurnRequest(APIModel):
    thread_id: str = Field(min_length=1)
    message: str = Field(min_length=1)
    stream: bool = False

    @field_validator("message")
    @classmethod
    def _normalize_message(cls, value: str) -> str:
        return value.strip()


class ChatTurnAcceptedResponse(APIModel):
    status: Literal["accepted"] = "accepted"
    thread_id: str
    turn_id: str


class ChatTurnResponse(APIModel):
    status: Literal["completed"] = "completed"
    thread_id: str
    turn_id: str
    planner_result: PlannerResult
    assistant_message: ChatMessage
    ui_delta: ChatUIStateDelta
    thread_state: ChatThreadState


class ThreadCreatedEventData(APIModel):
    state: ChatThreadState


class TurnStartedEventData(APIModel):
    turn_id: str
    user_message: ChatMessage


class AssistantDeltaEventData(APIModel):
    turn_id: str
    delta: str
    accumulated_text: str


class UIDeltaEventData(APIModel):
    turn_id: str
    ui_delta: ChatUIStateDelta


class AssistantCompletedEventData(APIModel):
    turn_id: str
    assistant_message: ChatMessage


class TurnCompletedEventData(APIModel):
    response: ChatTurnResponse


class ErrorEventData(APIModel):
    turn_id: str | None = None
    error: str


class SSEEventPayload(APIModel):
    event: ChatSSEEventName
    thread_id: str
    sequence: int
    timestamp: datetime
    data: (
        ThreadCreatedEventData
        | TurnStartedEventData
        | AssistantDeltaEventData
        | UIDeltaEventData
        | AssistantCompletedEventData
        | TurnCompletedEventData
        | ErrorEventData
    )
