from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from vobchat.api.schemas.chat import (
    AnalysisSpec,
    AvailabilityStatus,
    CapabilitySupportLevel,
    ChatOperation,
    ChatThreadState,
    DatasetFamilyRef,
    DiscoveryItem,
    DiscoveryOutputOption,
    DiscoveryResult,
    DiscoverySuggestedAction,
    DiscoveryTopic,
    DiscoveryYearCoverage,
    ExactSliceCandidate,
    OutputFeasibilityStatus,
    PlannerAction,
    ReportingGeographyRef,
    SemanticPlaceRef,
    SlotStatus,
)
from vobchat.api.schemas.themes import CubeSummaryResponse, ThemeListQuery, ThemeListResponse
from vobchat.api.services.chat_safety import CapabilityRegistry
from vobchat.api.services.exact_slices import ExactSliceCatalogService
from vobchat.api.services.themes import ThemesService


@dataclass
class DiscoveryExecution:
    operation: ChatOperation
    result: DiscoveryResult
    fallback_text: str
    notices: list[str] = field(default_factory=list)
    themes: ThemeListResponse | None = None
    cubes: list[CubeSummaryResponse] = field(default_factory=list)


class DiscoveryService:
    _CATALOG_RE = re.compile(
        r"\bwhat data do you have\b|\bwhat data (?:is|are) available\b|\bwhat can i (?:do|see|map) here\b",
        re.IGNORECASE,
    )
    _THEMES_RE = re.compile(r"\b(what|which).*\bthemes?\b", re.IGNORECASE)
    _GEOGRAPHIES_RE = re.compile(r"\b(what|which).*\bgeograph", re.IGNORECASE)
    _OUTPUTS_RE = re.compile(
        r"\b(what|which).*\boutputs?\b|\bwhat can you show\b|\bwhat can i map\b",
        re.IGNORECASE,
    )
    _YEARS_RE = re.compile(
        r"\b(what|which).*\byears?\b|\bavailable years\b|\bshow the available years\b",
        re.IGNORECASE,
    )
    _EXACT_SLICE_RE = re.compile(
        r"\b(?:exact slice|measure|breakdown|breakdowns|options?)\b.*\bavailable\b"
        r"|\bwhat .*breakdowns? are available\b"
        r"|\bwhat population breakdowns are available\b",
        re.IGNORECASE,
    )
    _COMPARABLE_RE = re.compile(
        r"\bwhat comparable options exist\b|\bwhat can i compare\b|\bcomparable\b.*\boptions?\b",
        re.IGNORECASE,
    )
    _FOR_PLACE_RE = re.compile(r"\bfor (?P<place>[^?.,]+)\??$", re.IGNORECASE)
    _USE_GEOGRAPHY_RE = re.compile(
        r"^(?:use|show)\s+(?:the\s+)?(?P<label>.+?)(?:\s+level)?[?.!]*$",
        re.IGNORECASE,
    )
    _PRONOUN_PLACES = {"this", "that", "here", "it"}
    _THEME_HINTS = {
        "population": "Population",
    }

    def __init__(
        self,
        *,
        themes_service: ThemesService,
        capability_registry: CapabilityRegistry,
        exact_slice_service: ExactSliceCatalogService,
    ) -> None:
        self.themes_service = themes_service
        self.capability_registry = capability_registry
        self.exact_slice_service = exact_slice_service

    def detect_action(
        self,
        *,
        state: ChatThreadState,
        message: str,
    ) -> PlannerAction | None:
        stripped = message.strip()
        follow_up_action = self._detect_receipt_local_follow_up(state=state, message=stripped)
        if follow_up_action is not None:
            return follow_up_action

        operation: ChatOperation | None = None
        if self._GEOGRAPHIES_RE.search(stripped):
            operation = ChatOperation.LIST_REPORTING_GEOGRAPHIES
        elif self._COMPARABLE_RE.search(stripped):
            operation = ChatOperation.LIST_COMPARABLE_OPTIONS
        elif self._OUTPUTS_RE.search(stripped):
            operation = ChatOperation.LIST_FEASIBLE_OUTPUTS
        elif self._YEARS_RE.search(stripped):
            operation = ChatOperation.LIST_AVAILABLE_YEARS
        elif self._EXACT_SLICE_RE.search(stripped):
            operation = ChatOperation.LIST_EXACT_SLICE_OPTIONS
        elif self._THEMES_RE.search(stripped) and "theme" in stripped.lower():
            operation = ChatOperation.LIST_THEMES
        elif self._CATALOG_RE.search(stripped):
            operation = ChatOperation.LIST_CATALOG_OVERVIEW

        if operation is None:
            return None

        theme_query = self._inferred_theme_query(message=stripped, state=state)
        return PlannerAction(
            operation=operation,
            confidence=1.0,
            place_query=self._extract_place_query(stripped),
            theme_query=theme_query,
            cube_query=self._inferred_cube_query(stripped),
        )

    def execute(
        self,
        *,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        if action.operation == ChatOperation.LIST_CATALOG_OVERVIEW:
            return self._catalog_overview(state, action)
        if action.operation == ChatOperation.LIST_THEMES:
            return self._themes(state, action)
        if action.operation == ChatOperation.LIST_REPORTING_GEOGRAPHIES:
            return self._reporting_geographies(state, action)
        if action.operation == ChatOperation.LIST_FEASIBLE_OUTPUTS:
            return self._feasible_outputs(state, action)
        if action.operation == ChatOperation.LIST_AVAILABLE_YEARS:
            return self._available_years(state, action)
        if action.operation == ChatOperation.LIST_EXACT_SLICE_OPTIONS:
            return self._exact_slice_options(state, action)
        if action.operation == ChatOperation.LIST_COMPARABLE_OPTIONS:
            return self._comparable_options(state, action)
        raise ValueError(f"Unsupported discovery operation {action.operation!r}.")

    def _catalog_overview(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        reporting_candidates = self._reporting_candidates(state)
        active_reporting_geography = self._active_reporting_geography(state, action)
        theme_items, theme_refs, themes_response = self._theme_inventory(
            state=state,
            reporting_candidates=reporting_candidates,
            active_reporting_geography=active_reporting_geography,
        )
        exact_slice_options, availability_notes = self._discover_exact_slice_options(
            state=state,
            action=action,
            reporting_geography=active_reporting_geography,
        )
        supported_outputs = self._discover_output_options(
            state=state,
            action=action,
            reporting_geography=active_reporting_geography,
        )
        suggested_actions = [
            *self._geography_follow_up_actions(reporting_candidates, ChatOperation.LIST_THEMES),
            *self._theme_follow_up_actions(theme_refs),
        ]
        if exact_slice_options:
            suggested_actions.append(
                DiscoverySuggestedAction(
                    label="Show the available years",
                    operation=ChatOperation.LIST_AVAILABLE_YEARS,
                    theme_id=theme_refs[0].theme_id if len(theme_refs) == 1 else None,
                )
            )
        result = DiscoveryResult(
            topic=DiscoveryTopic.CATALOG_OVERVIEW,
            subtype="place_catalog",
            title="Data available from the current place context",
            summary=self._catalog_summary(
                theme_count=len(theme_items),
                geography_count=len(reporting_candidates),
                exact_slice_count=len(exact_slice_options),
            ),
            items=theme_items,
            place_context=self._place_context(state),
            reporting_geographies=[candidate.model_copy(deep=True) for candidate in reporting_candidates],
            dataset_families=theme_refs,
            exact_slice_options=exact_slice_options,
            supported_outputs=supported_outputs,
            comparability_notes=self._comparability_notes(state, reporting_candidates),
            availability_notes=availability_notes,
            suggested_actions=suggested_actions[:6],
            anchors=self._anchors(
                state,
                preferred_geography_operation=ChatOperation.LIST_THEMES,
            ),
            notices=self._catalog_notices(state, reporting_candidates),
        )
        preview = ", ".join(item.label for item in theme_items[:5]) or "no catalog entries yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_CATALOG_OVERVIEW,
            result=result,
            fallback_text=f"Available data includes {preview}.",
            notices=list(result.notices),
            themes=themes_response,
        )

    def _themes(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        reporting_candidates = self._reporting_candidates(state)
        active_reporting_geography = self._active_reporting_geography(state, action)
        items, dataset_families, response = self._theme_inventory(
            state=state,
            reporting_candidates=reporting_candidates,
            active_reporting_geography=active_reporting_geography,
        )
        notices: list[str] = []
        if active_reporting_geography is None and state.selected_places:
            notices.append("Choose a reporting geography to narrow the theme catalog for this place.")
        result = DiscoveryResult(
            topic=DiscoveryTopic.THEMES,
            subtype="themes_by_reporting_geography",
            title="Themes available from the current place context",
            summary=(
                f"Found {len(items)} theme option{'s' if len(items) != 1 else ''}."
                if items
                else "No theme catalog entries matched the current place context."
            ),
            items=items,
            place_context=self._place_context(state),
            reporting_geographies=(
                [active_reporting_geography.model_copy(deep=True)]
                if active_reporting_geography is not None
                else [candidate.model_copy(deep=True) for candidate in reporting_candidates]
            ),
            dataset_families=dataset_families,
            supported_outputs=self._discover_output_options(
                state=state,
                action=action,
                reporting_geography=active_reporting_geography,
            ),
            suggested_actions=self._theme_follow_up_actions(dataset_families),
            anchors=self._anchors(
                state,
                preferred_geography_operation=ChatOperation.LIST_THEMES,
                selected_unit_type=(
                    active_reporting_geography.unit_type
                    if active_reporting_geography is not None
                    else None
                ),
            ),
            notices=notices,
        )
        preview = ", ".join(item.label for item in items[:5]) or "no themes yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_THEMES,
            result=result,
            themes=response,
            notices=notices,
            fallback_text=f"Available themes include {preview}.",
        )

    def _reporting_geographies(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        candidates = self._reporting_candidates(state)
        notices: list[str] = []
        if not state.selected_places:
            notices.append("Choose a place first so I can list the reporting geographies that fit it.")
        result = DiscoveryResult(
            topic=DiscoveryTopic.REPORTING_GEOGRAPHIES,
            subtype="shared_place_geographies",
            title="Reporting geographies available from the current place context",
            summary=(
                f"Found {len(candidates)} reporting geograph{'ies' if len(candidates) != 1 else 'y'}."
                if candidates
                else "No shared reporting geographies are available from the current place context."
            ),
            items=[
                DiscoveryItem(
                    item_id=candidate.unit_type or "unknown",
                    label=candidate.label or candidate.unit_type or "Unknown geography",
                    kind="reporting_geography",
                    metadata={
                        "unit_type": candidate.unit_type,
                        "unit_ids": list(candidate.unit_ids),
                        "scope_rule": candidate.scope_rule,
                    },
                )
                for candidate in candidates
            ],
            place_context=self._place_context(state),
            reporting_geographies=[candidate.model_copy(deep=True) for candidate in candidates],
            supported_outputs=self._discover_output_options(
                state=state,
                action=action,
                reporting_geography=None,
            ),
            comparability_notes=self._comparability_notes(state, candidates),
            suggested_actions=self._geography_follow_up_actions(
                candidates,
                ChatOperation.LIST_THEMES,
            ),
            anchors=self._anchors(
                state,
                preferred_geography_operation=ChatOperation.LIST_THEMES,
            ),
            notices=notices,
        )
        preview = ", ".join(item.label for item in result.items[:5]) or "no reporting geographies yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_REPORTING_GEOGRAPHIES,
            result=result,
            notices=notices,
            fallback_text=f"Available reporting geographies include {preview}.",
        )

    def _feasible_outputs(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        spec = self._active_spec(state)
        reporting_geography = self._active_reporting_geography(state, action)
        dataset_family = (
            (spec.dataset_family if spec is not None else None)
            or state.analysis_state.dataset_family
            or (
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
                if state.selected_theme is not None
                else None
            )
        )
        supported_outputs = self._discover_output_options(
            state=state,
            action=action,
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=(spec.exact_slice if spec is not None else state.analysis_state.candidate_slice),
        )
        notices: list[str] = []
        if not supported_outputs and state.selected_places and reporting_geography is None:
            notices.append("Choose a reporting geography first to narrow the safely feasible outputs.")
        elif not supported_outputs and dataset_family is None:
            notices.append("I need a theme or a validated result before I can narrow the feasible outputs safely.")

        items = [
            DiscoveryItem(
                item_id=option.output_mode,
                label=option.label,
                kind="output_mode",
                metadata={
                    "output_mode": option.output_mode,
                    "feasibility": option.feasibility.value if option.feasibility is not None else None,
                    "reason": option.reason,
                },
            )
            for option in supported_outputs
        ]
        result = DiscoveryResult(
            topic=DiscoveryTopic.FEASIBLE_OUTPUTS,
            subtype="supported_output_modes",
            title="Outputs that are feasible from the current semantic state",
            summary=(
                f"Found {len(items)} supported output mode{'s' if len(items) != 1 else ''}."
                if items
                else "No safe output modes are available from the current state yet."
            ),
            items=items,
            place_context=self._place_context(state),
            reporting_geographies=(
                [reporting_geography.model_copy(deep=True)]
                if reporting_geography is not None
                else []
            ),
            dataset_families=[dataset_family] if dataset_family is not None else [],
            supported_outputs=supported_outputs,
            availability_notes=self._output_availability_notes(
                supported_outputs=supported_outputs,
                dataset_family=dataset_family,
            ),
            suggested_actions=self._output_follow_up_actions(supported_outputs),
            anchors=self._anchors(state),
            notices=notices,
        )
        preview = ", ".join(item.label for item in items) or "no safe outputs yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_FEASIBLE_OUTPUTS,
            result=result,
            notices=notices,
            fallback_text=f"Feasible outputs from the current state include {preview}.",
        )

    def _available_years(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        spec = self._active_spec(state)
        reporting_geography = self._active_reporting_geography(state, action)
        year_coverage = self._discover_year_coverage(
            state=state,
            action=action,
            reporting_geography=reporting_geography,
        )
        if state.selected_theme is not None:
            year_coverage = [
                coverage.model_copy(
                    update={"theme_id": coverage.theme_id or state.selected_theme.theme_id}
                )
                for coverage in year_coverage
            ]
        notices: list[str] = []
        if not year_coverage:
            notices.append("I can list years once the current state points to one family or exact slice safely.")

        items = [
            DiscoveryItem(
                item_id=coverage.item_id,
                label=coverage.label,
                kind="year_range",
                metadata={
                    "theme_id": coverage.theme_id,
                    "cube_id": coverage.cube_id,
                    "start_year": coverage.start_year,
                    "end_year": coverage.end_year,
                    "executable": coverage.executable,
                },
            )
            for coverage in year_coverage
        ]
        result = DiscoveryResult(
            topic=DiscoveryTopic.AVAILABLE_YEARS,
            subtype=(
                "exact_slice_year_coverage"
                if any(coverage.executable for coverage in year_coverage)
                else "family_year_coverage"
            ),
            title="Available years from the current catalog context",
            summary=(
                f"Found {len(year_coverage)} year range{'s' if len(year_coverage) != 1 else ''}."
                if year_coverage
                else "No safe year coverage is available from the current state yet."
            ),
            items=items,
            place_context=self._place_context(state),
            reporting_geographies=(
                [reporting_geography.model_copy(deep=True)]
                if reporting_geography is not None
                else []
            ),
            dataset_families=(
                [spec.dataset_family]
                if spec is not None and spec.dataset_family is not None
                else (
                    [
                        DatasetFamilyRef.from_theme(
                            state.selected_theme,
                            source="selected_theme",
                            slot_status=SlotStatus.RESOLVED,
                        )
                    ]
                    if state.selected_theme is not None
                    else []
                )
            ),
            available_years=year_coverage,
            exact_slice_options=self._anchored_exact_slice_options(state),
            suggested_actions=self._year_follow_up_actions(year_coverage),
            anchors=self._anchors(state),
            notices=notices,
        )
        preview = year_coverage[0].label if year_coverage else "no safe year range yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_AVAILABLE_YEARS,
            result=result,
            notices=notices,
            fallback_text=f"Available years include {preview}.",
        )

    def _exact_slice_options(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        reporting_geography = self._active_reporting_geography(state, action)
        exact_slice_options, availability_notes = self._discover_exact_slice_options(
            state=state,
            action=action,
            reporting_geography=reporting_geography,
        )
        grouped = self._group_exact_slice_items(exact_slice_options)
        notices: list[str] = []
        if not state.selected_theme and action.theme_query is None:
            notices.append("Choose or mention a theme first so I can inspect exact-slice-capable options safely.")
        elif reporting_geography is None and state.selected_places:
            notices.append("Choose a reporting geography first so I can inspect unit-specific exact-slice options.")

        result = DiscoveryResult(
            topic=DiscoveryTopic.EXACT_SLICE_OPTIONS,
            subtype="exact_slice_catalog",
            title="Exact-slice-capable options from the current catalog context",
            summary=(
                f"Found {len(exact_slice_options)} exact-slice candidate{'s' if len(exact_slice_options) != 1 else ''}."
                if exact_slice_options
                else "No exact-slice-capable options are currently available from this context."
            ),
            items=grouped,
            place_context=self._place_context(state),
            reporting_geographies=(
                [reporting_geography.model_copy(deep=True)]
                if reporting_geography is not None
                else []
            ),
            dataset_families=(
                [
                    DatasetFamilyRef.from_theme(
                        state.selected_theme,
                        source="selected_theme",
                        slot_status=SlotStatus.RESOLVED,
                    )
                ]
                if state.selected_theme is not None
                else []
            ),
            exact_slice_options=exact_slice_options,
            available_years=self._year_coverage_from_exact_slice_candidates(exact_slice_options),
            availability_notes=availability_notes,
            suggested_actions=[
                DiscoverySuggestedAction(
                    label="Show the available years",
                    operation=ChatOperation.LIST_AVAILABLE_YEARS,
                    theme_id=state.selected_theme.theme_id if state.selected_theme is not None else None,
                )
            ],
            anchors=self._anchors(state),
            notices=notices,
        )
        preview = ", ".join(item.label for item in grouped[:3]) or "no exact-slice options yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_EXACT_SLICE_OPTIONS,
            result=result,
            notices=notices,
            fallback_text=f"Exact-slice-capable options include {preview}.",
        )

    def _comparable_options(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> DiscoveryExecution:
        candidates = self._reporting_candidates(state)
        notes = self._comparability_notes(state, candidates)
        items = [
            DiscoveryItem(
                item_id=candidate.unit_type or "unknown",
                label=candidate.label or candidate.unit_type or "Unknown geography",
                kind="reporting_geography",
                metadata={
                    "unit_type": candidate.unit_type,
                    "unit_ids": list(candidate.unit_ids),
                    "comparison_ready": len(state.selected_places) > 1,
                },
            )
            for candidate in candidates
        ]
        result = DiscoveryResult(
            topic=DiscoveryTopic.COMPARABLE_OPTIONS,
            subtype="comparison_candidates",
            title="Comparable geography options from the current place set",
            summary=(
                f"Found {len(items)} shared comparison geograph{'ies' if len(items) != 1 else 'y'}."
                if items
                else "No shared comparison geographies are available from the current place set."
            ),
            items=items,
            place_context=self._place_context(state),
            reporting_geographies=[candidate.model_copy(deep=True) for candidate in candidates],
            comparability_notes=notes,
            suggested_actions=self._geography_follow_up_actions(
                candidates,
                ChatOperation.LIST_FEASIBLE_OUTPUTS,
            ),
            anchors=self._anchors(
                state,
                preferred_geography_operation=ChatOperation.LIST_FEASIBLE_OUTPUTS,
            ),
            notices=[],
        )
        preview = ", ".join(item.label for item in items[:5]) or "no shared comparison geographies yet"
        return DiscoveryExecution(
            operation=ChatOperation.LIST_COMPARABLE_OPTIONS,
            result=result,
            notices=[],
            fallback_text=f"Comparable options include {preview}.",
        )

    @staticmethod
    def _active_spec(state: ChatThreadState) -> AnalysisSpec | None:
        if state.current_receipt is not None and state.current_receipt.analysis_spec is not None:
            return state.current_receipt.analysis_spec
        return state.analysis_state.analysis_spec

    def _active_reporting_geography(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ReportingGeographyRef | None:
        candidates = self._reporting_candidates(state)
        if action.unit_type:
            for candidate in candidates:
                if candidate.unit_type == action.unit_type:
                    selected = candidate.model_copy(deep=True)
                    selected.slot_status = SlotStatus.RESOLVED
                    selected.source = "discovery_follow_up"
                    return selected
        if state.analysis_state.reporting_geography is not None:
            selected = state.analysis_state.reporting_geography.model_copy(deep=True)
            if not candidates or any(candidate.unit_type == selected.unit_type for candidate in candidates):
                selected.slot_status = SlotStatus.RESOLVED
                return selected
        if state.resolution_state.selected_reporting_geography is not None:
            selected = state.resolution_state.selected_reporting_geography.model_copy(deep=True)
            if not candidates or any(candidate.unit_type == selected.unit_type for candidate in candidates):
                selected.slot_status = SlotStatus.RESOLVED
                return selected
        if len(candidates) == 1:
            selected = candidates[0].model_copy(deep=True)
            selected.slot_status = SlotStatus.RESOLVED
            return selected
        return None

    def _reporting_candidates(
        self,
        state: ChatThreadState,
    ) -> list[ReportingGeographyRef]:
        return self.capability_registry.enabled_reporting_geography_candidates(
            list(state.selected_places)
        )

    def _theme_inventory(
        self,
        *,
        state: ChatThreadState,
        reporting_candidates: list[ReportingGeographyRef],
        active_reporting_geography: ReportingGeographyRef | None,
    ) -> tuple[list[DiscoveryItem], list[DatasetFamilyRef], ThemeListResponse | None]:
        notices: list[str] = []
        responses: list[tuple[ReportingGeographyRef | None, ThemeListResponse]] = []

        candidate_geographies = (
            [active_reporting_geography]
            if active_reporting_geography is not None
            else list(reporting_candidates)
        )
        if candidate_geographies:
            for geography in candidate_geographies:
                unit_id = self.capability_registry.representative_unit_id(geography)
                if unit_id is None:
                    continue
                unit_response = self.themes_service.list_themes_for_unit(unit_id)
                responses.append(
                    (
                        geography,
                        ThemeListResponse(
                            item_count=unit_response.item_count,
                            items=list(unit_response.items),
                        ),
                    )
                )
        elif state.selected_places:
            notices.append("Choose a reporting geography to narrow the theme catalog for this place.")
        else:
            responses.append((None, self.themes_service.list_themes(ThemeListQuery(limit=20))))

        by_theme: dict[str, dict[str, object]] = {}
        for geography, response in responses:
            for item in response.items:
                entry = by_theme.setdefault(
                    item.theme_id,
                    {
                        "theme": item,
                        "geographies": [],
                    },
                )
                geographies = entry["geographies"]
                if geography is not None and geography.unit_type not in geographies:
                    geographies.append(geography.unit_type)

        items = [
            DiscoveryItem(
                item_id=theme_id,
                label=entry["theme"].label,
                kind="theme",
                metadata={
                    "theme_id": theme_id,
                    "reporting_geographies": list(entry["geographies"]),
                },
            )
            for theme_id, entry in sorted(
                by_theme.items(),
                key=lambda item: (item[1]["theme"].label or item[0]).lower(),
            )
        ]
        refs = [
            DatasetFamilyRef.from_theme(
                entry["theme"],
                source="discovery_catalog",
                slot_status=SlotStatus.CANDIDATE_SET,
            )
            for _, entry in sorted(
                by_theme.items(),
                key=lambda item: (item[1]["theme"].label or item[0]).lower(),
            )
        ]
        representative_response = responses[0][1] if len(responses) == 1 else None
        return items, refs, representative_response

    def _discover_output_options(
        self,
        *,
        state: ChatThreadState,
        action: PlannerAction,
        reporting_geography: ReportingGeographyRef | None,
        dataset_family: DatasetFamilyRef | None = None,
        exact_slice=None,
    ) -> list[DiscoveryOutputOption]:
        spec = self._active_spec(state)
        dataset_family = dataset_family or (
            (spec.dataset_family if spec is not None else None)
            or state.analysis_state.dataset_family
            or (
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
                if state.selected_theme is not None
                else None
            )
        )
        exact_slice = exact_slice or (
            (spec.exact_slice if spec is not None else None) or state.analysis_state.candidate_slice
        )

        options: list[DiscoveryOutputOption] = []
        output_labels = {
            "trend_chart": "Trend chart",
            "table": "Table",
            "boundary_map": "Boundary map",
            "category_chart": "Category breakdown",
        }
        for policy in self.capability_registry.discoverable_output_policies(
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            wants_theme_context=dataset_family is not None,
        ):
            output_mode = policy.output_mode
            label = output_labels.get(output_mode, output_mode.replace("_", " ").title())
            feasibility = self.capability_registry.output_feasibility(
                output_mode=policy.output_mode,
                reporting_geography=reporting_geography,
                dataset_family=dataset_family,
                exact_slice=exact_slice,
                wants_theme_context=dataset_family is not None,
                requested_full_layer_map=False,
            )
            reason = policy.reason
            if output_mode in {"trend_chart", "table"} and reason is None:
                if dataset_family is None or reporting_geography is None:
                    reason = "Needs one reporting geography and one theme before analysis can run."
                elif exact_slice is None:
                    reason = "Needs one exact slice before analysis can run."
                    feasibility = OutputFeasibilityStatus.NEEDS_CLARIFICATION
            elif output_mode == "boundary_map" and dataset_family is not None and reason is None:
                reason = "Boundary maps stay measure-free unless a later capability wave enables thematic maps."
            elif (
                output_mode == "category_chart"
                and policy.combined_support == CapabilitySupportLevel.DISCOVERY_ONLY
                and reason is None
            ):
                reason = "Category breakdowns are catalog-visible here, but execution remains blocked."
            options.append(
                DiscoveryOutputOption(
                    output_mode=output_mode,
                    label=label,
                    feasibility=feasibility,
                    support_level=policy.combined_support,
                    requires_exact_slice=policy.requires_exact_slice,
                    comparison_tier=policy.comparison_tier,
                    map_loading_mode=(
                        policy.map_loading.mode
                        if policy.map_loading is not None
                        else None
                    ),
                    reason=reason,
                    metadata={
                        "theme_id": dataset_family.theme_id if dataset_family is not None else None,
                        "unit_type": reporting_geography.unit_type if reporting_geography is not None else None,
                    },
                )
            )
        return options

    def _discover_year_coverage(
        self,
        *,
        state: ChatThreadState,
        action: PlannerAction,
        reporting_geography: ReportingGeographyRef | None,
    ) -> list[DiscoveryYearCoverage]:
        spec = self._active_spec(state)
        if spec is not None and spec.exact_slice is not None:
            return self._year_coverage_from_exact_slice_ref(spec.exact_slice, spec.dataset_family)

        anchored_options = self._anchored_exact_slice_options(state)
        if anchored_options:
            return self._year_coverage_from_exact_slice_candidates(anchored_options)

        theme_id = state.selected_theme.theme_id if state.selected_theme is not None else None
        if theme_id is None or reporting_geography is None:
            return []

        unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if unit_id is None:
            return []

        exact_slice_candidates = self.exact_slice_service.list_exact_slices(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_query=action.cube_query,
        )
        if exact_slice_candidates:
            return self._year_coverage_from_exact_slice_candidates(exact_slice_candidates)

        cubes = self.themes_service.list_cubes_for_unit_theme(unit_id, theme_id).items
        return [
            DiscoveryYearCoverage(
                item_id=cube.cube_id,
                label=f"{cube.label}: {self._year_label(cube.start_year, cube.end_year)}",
                theme_id=theme_id,
                cube_id=cube.cube_id,
                start_year=cube.start_year,
                end_year=cube.end_year,
                executable=False,
                metadata={"family_level_only": True},
            )
            for cube in cubes
            if cube.start_year is not None or cube.end_year is not None
        ]

    def _discover_exact_slice_options(
        self,
        *,
        state: ChatThreadState,
        action: PlannerAction,
        reporting_geography: ReportingGeographyRef | None,
    ) -> tuple[list[ExactSliceCandidate], list[str]]:
        theme_id = state.selected_theme.theme_id if state.selected_theme is not None else None
        if theme_id is None or reporting_geography is None:
            return [], []
        unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if unit_id is None:
            return [], []

        inventory = self.exact_slice_service.list_catalog_inventory(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_query=action.cube_query,
        )
        if not inventory:
            return [], []

        supported_cubes = self.exact_slice_service.supported_cubes(theme_id)
        supported = [
            candidate.model_copy(deep=True)
            for candidate in inventory
            if candidate.cube_id in supported_cubes
        ]
        inventory_only = sorted(
            {
                candidate.cube_id or candidate.label or "unknown"
                for candidate in inventory
                if candidate.cube_id not in supported_cubes
            }
        )
        notes: list[str] = []
        if inventory_only:
            notes.append(
                "Metadata also exposes inventory-only options that remain discovery-only in the current workflow subset: "
                + ", ".join(inventory_only)
                + "."
            )
        if supported_cubes:
            notes.append("This theme has an executable subset only after one exact slice is proven.")
        return supported, notes

    def _comparability_notes(
        self,
        state: ChatThreadState,
        candidates: list[ReportingGeographyRef],
    ) -> list[str]:
        if not state.selected_places:
            return ["Choose a place first so I can inspect comparable geography options."]
        spec = self._active_spec(state)
        dataset_family = (
            (spec.dataset_family if spec is not None else None)
            or state.analysis_state.dataset_family
            or (
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
                if state.selected_theme is not None
                else None
            )
        )
        reporting_geography = (
            candidates[0] if len(candidates) == 1 else None
        )
        assessment = self.capability_registry.comparison_assessment(
            places=list(state.selected_places),
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=(spec.exact_slice if spec is not None else state.analysis_state.candidate_slice),
            time_scope=(spec.time_scope if spec is not None else state.analysis_state.time_scope),
        )

        if len(state.selected_places) == 1:
            return [
                "Add another place that supports one of these reporting geographies before comparison can run safely.",
            ]
        if not candidates:
            return ["No shared reporting geography is available across the selected places yet."]

        notes = ["These reporting geographies are shared across the current places."]
        if assessment.tier.value == "not_supported":
            notes.append("Comparison is not supported for the current family or geography policy.")
        elif assessment.tier.value == "discovery_only":
            notes.append("Comparison remains discovery-only for the current family policy.")
        elif assessment.tier.value == "candidate_comparable":
            notes.append("These are candidate-comparable only until one reporting geography and one exact slice are explicit.")
        elif assessment.tier.value == "guard_verifiable":
            notes.append("This combination is close to comparable, but it still needs one shared time selection.")
        else:
            notes.append("This combination is policy-compatible for comparison once you request an executable output.")
        return notes

    def _detect_receipt_local_follow_up(
        self,
        *,
        state: ChatThreadState,
        message: str,
    ) -> PlannerAction | None:
        current_discovery = self._current_discovery_result(state)
        if current_discovery is None:
            return None
        match = self._USE_GEOGRAPHY_RE.match(message)
        if match is None:
            return None
        candidate = self._match_reporting_geography_label(
            current_discovery.reporting_geographies,
            match.group("label"),
        )
        if candidate is None:
            return None
        preferred_operation = self._preferred_geography_follow_up_operation(current_discovery)
        return PlannerAction(
            operation=preferred_operation,
            confidence=1.0,
            unit_type=candidate.unit_type,
            theme_query=self._anchored_theme_query(state),
        )

    @staticmethod
    def _match_reporting_geography_label(
        candidates: list[ReportingGeographyRef],
        raw_label: str,
    ) -> ReportingGeographyRef | None:
        lowered = raw_label.strip().lower()
        for candidate in candidates:
            values = {
                (candidate.label or "").lower(),
                (candidate.unit_type or "").lower(),
            }
            if lowered in values:
                return candidate
            if lowered == "parish" and candidate.unit_type == "PAR_UNIT":
                return candidate
            if lowered in {"district", "modern district"} and candidate.unit_type == "MOD_DIST":
                return candidate
        return None

    @staticmethod
    def _preferred_geography_follow_up_operation(result: DiscoveryResult) -> ChatOperation:
        mapping = {
            DiscoveryTopic.REPORTING_GEOGRAPHIES: ChatOperation.LIST_THEMES,
            DiscoveryTopic.CATALOG_OVERVIEW: ChatOperation.LIST_THEMES,
            DiscoveryTopic.THEMES: ChatOperation.LIST_THEMES,
            DiscoveryTopic.FEASIBLE_OUTPUTS: ChatOperation.LIST_FEASIBLE_OUTPUTS,
            DiscoveryTopic.AVAILABLE_YEARS: ChatOperation.LIST_AVAILABLE_YEARS,
            DiscoveryTopic.EXACT_SLICE_OPTIONS: ChatOperation.LIST_EXACT_SLICE_OPTIONS,
            DiscoveryTopic.COMPARABLE_OPTIONS: ChatOperation.LIST_COMPARABLE_OPTIONS,
        }
        return mapping.get(result.topic, ChatOperation.LIST_THEMES)

    def _extract_place_query(self, message: str) -> str | None:
        match = self._FOR_PLACE_RE.search(message)
        if match is None:
            return None
        place = match.group("place").strip(" ?.!").strip()
        if place.lower() in self._PRONOUN_PLACES:
            return None
        return place

    def _inferred_theme_query(self, *, message: str, state: ChatThreadState) -> str | None:
        lowered = message.lower()
        for token, query in self._THEME_HINTS.items():
            if token in lowered:
                return query
        return self._anchored_theme_query(state)

    @staticmethod
    def _inferred_cube_query(message: str) -> str | None:
        lowered = message.lower()
        if "gender" in lowered:
            return "gender"
        if "total population" in lowered:
            return "total population"
        return None

    def _anchored_theme_query(self, state: ChatThreadState) -> str | None:
        if state.selected_theme is not None:
            return state.selected_theme.label or state.selected_theme.theme_id
        current_discovery = self._current_discovery_result(state)
        if current_discovery is not None and len(current_discovery.dataset_families) == 1:
            family = current_discovery.dataset_families[0]
            return family.label or family.theme_id
        return None

    @staticmethod
    def _current_discovery_result(state: ChatThreadState) -> DiscoveryResult | None:
        if state.latest_discovery_result is not None:
            return state.latest_discovery_result
        if state.current_receipt is not None:
            return state.current_receipt.discovery_result
        return None

    def _anchored_exact_slice_options(self, state: ChatThreadState) -> list[ExactSliceCandidate]:
        current_discovery = self._current_discovery_result(state)
        if current_discovery is None or not current_discovery.exact_slice_options:
            return []
        return [candidate.model_copy(deep=True) for candidate in current_discovery.exact_slice_options]

    @staticmethod
    def _place_context(state: ChatThreadState) -> list[SemanticPlaceRef]:
        return [
            SemanticPlaceRef.from_resolved_place(place, source="selected_place")
            for place in state.selected_places
        ]

    def _anchors(
        self,
        state: ChatThreadState,
        *,
        preferred_geography_operation: ChatOperation | None = None,
        selected_unit_type: str | None = None,
    ) -> dict[str, object]:
        return {
            "place_ids": [place.place.place_id for place in state.selected_places],
            "theme_id": state.selected_theme.theme_id if state.selected_theme is not None else None,
            "current_receipt_id": state.current_receipt_id,
            "availability_status": (
                state.analysis_state.availability_status.value
                if state.analysis_state.availability_status is not None
                else AvailabilityStatus.PARTIALLY_AVAILABLE.value
            ),
            "preferred_geography_operation": (
                preferred_geography_operation.value
                if preferred_geography_operation is not None
                else None
            ),
            "selected_unit_type": selected_unit_type,
        }

    def _theme_follow_up_actions(
        self,
        dataset_families: list[DatasetFamilyRef],
    ) -> list[DiscoverySuggestedAction]:
        actions: list[DiscoverySuggestedAction] = []
        for family in dataset_families[:4]:
            actions.append(
                DiscoverySuggestedAction(
                    label=f"Explore {family.label or family.theme_id}",
                    operation=ChatOperation.LIST_EXACT_SLICE_OPTIONS,
                    theme_id=family.theme_id,
                )
            )
        return actions

    @staticmethod
    def _geography_follow_up_actions(
        candidates: list[ReportingGeographyRef],
        operation: ChatOperation,
    ) -> list[DiscoverySuggestedAction]:
        actions: list[DiscoverySuggestedAction] = []
        for candidate in candidates[:4]:
            actions.append(
                DiscoverySuggestedAction(
                    label=f"Use {candidate.label or candidate.unit_type}",
                    operation=operation,
                    unit_type=candidate.unit_type,
                )
            )
        return actions

    @staticmethod
    def _output_follow_up_actions(
        supported_outputs: list[DiscoveryOutputOption],
    ) -> list[DiscoverySuggestedAction]:
        return [
            DiscoverySuggestedAction(
                label=f"Show the {item.label.lower()}",
                operation=ChatOperation.LIST_FEASIBLE_OUTPUTS,
                output_mode=item.output_mode,
            )
            for item in supported_outputs[:3]
        ]

    @staticmethod
    def _year_follow_up_actions(
        year_coverage: list[DiscoveryYearCoverage],
    ) -> list[DiscoverySuggestedAction]:
        if not year_coverage:
            return []
        return [
            DiscoverySuggestedAction(
                label="Show exact-slice options",
                operation=ChatOperation.LIST_EXACT_SLICE_OPTIONS,
                theme_id=year_coverage[0].theme_id,
                cube_id=year_coverage[0].cube_id,
            )
        ]

    @staticmethod
    def _output_availability_notes(
        *,
        supported_outputs: list[DiscoveryOutputOption],
        dataset_family: DatasetFamilyRef | None,
    ) -> list[str]:
        notes: list[str] = []
        if dataset_family is not None and not supported_outputs:
            notes.append(
                f"{dataset_family.label or dataset_family.theme_id} is still discovery-only until one exact slice is explicit."
            )
        if any(item.output_mode == "boundary_map" for item in supported_outputs):
            notes.append("Boundary maps are catalog-safe even when thematic maps remain blocked.")
        return notes

    @staticmethod
    def _year_coverage_from_exact_slice_ref(
        exact_slice,
        dataset_family: DatasetFamilyRef | None,
    ) -> list[DiscoveryYearCoverage]:
        if exact_slice.start_year is None and exact_slice.end_year is None:
            return []
        return [
            DiscoveryYearCoverage(
                item_id=exact_slice.cube_id or exact_slice.dataitem_id or "exact_slice",
                label=f"{exact_slice.label or exact_slice.cube_id}: {DiscoveryService._year_label(exact_slice.start_year, exact_slice.end_year)}",
                theme_id=dataset_family.theme_id if dataset_family is not None else None,
                cube_id=exact_slice.cube_id,
                cellref=exact_slice.cellref,
                dataitem_id=exact_slice.dataitem_id,
                start_year=exact_slice.start_year,
                end_year=exact_slice.end_year,
                executable=bool(exact_slice.cellref or exact_slice.dataitem_id or exact_slice.cat_id),
            )
        ]

    @staticmethod
    def _year_coverage_from_exact_slice_candidates(
        candidates: list[ExactSliceCandidate],
    ) -> list[DiscoveryYearCoverage]:
        seen: set[str] = set()
        coverage: list[DiscoveryYearCoverage] = []
        for candidate in candidates:
            key = candidate.dataitem_id or candidate.cellref or candidate.label or candidate.cube_id or ""
            if not key or key in seen:
                continue
            seen.add(key)
            coverage.append(
                DiscoveryYearCoverage(
                    item_id=key,
                    label=f"{candidate.label or candidate.cube_id}: {DiscoveryService._year_label(candidate.start_year, candidate.end_year)}",
                    cube_id=candidate.cube_id,
                    cellref=candidate.cellref,
                    dataitem_id=candidate.dataitem_id,
                    start_year=candidate.start_year,
                    end_year=candidate.end_year,
                    executable=bool(candidate.cellref or candidate.dataitem_id or candidate.cat_id),
                )
            )
        return coverage

    @staticmethod
    def _group_exact_slice_items(
        candidates: list[ExactSliceCandidate],
    ) -> list[DiscoveryItem]:
        grouped: dict[str, dict[str, object]] = defaultdict(
            lambda: {"label": None, "candidate_count": 0, "years": []}
        )
        for candidate in candidates:
            key = candidate.cube_id or candidate.label or "exact_slice"
            entry = grouped[key]
            entry["label"] = candidate.description or candidate.label or key
            entry["candidate_count"] += 1
            years = entry["years"]
            year_label = DiscoveryService._year_label(candidate.start_year, candidate.end_year)
            if year_label not in years:
                years.append(year_label)
        return [
            DiscoveryItem(
                item_id=cube_id,
                label=str(entry["label"] or cube_id),
                kind="exact_slice_family",
                metadata={
                    "cube_id": cube_id,
                    "candidate_count": entry["candidate_count"],
                    "year_ranges": list(entry["years"]),
                },
            )
            for cube_id, entry in sorted(grouped.items(), key=lambda item: item[0].lower())
        ]

    @staticmethod
    def _catalog_summary(
        *,
        theme_count: int,
        geography_count: int,
        exact_slice_count: int,
    ) -> str:
        return (
            f"{theme_count} theme option{'s' if theme_count != 1 else ''}, "
            f"{geography_count} reporting geograph{'ies' if geography_count != 1 else 'y'}, "
            f"and {exact_slice_count} exact-slice candidate{'s' if exact_slice_count != 1 else ''} "
            "are currently visible from this place context."
        )

    @staticmethod
    def _catalog_notices(
        state: ChatThreadState,
        reporting_candidates: list[ReportingGeographyRef],
    ) -> list[str]:
        notices: list[str] = []
        if state.selected_places and not reporting_candidates:
            notices.append("No shared reporting geography is available yet for the current place context.")
        return notices

    @staticmethod
    def _year_label(start_year: float | None, end_year: float | None) -> str:
        if start_year is None and end_year is None:
            return "year range unavailable"
        if start_year is None:
            return f"up to {int(end_year)}"
        if end_year is None:
            return f"from {int(start_year)}"
        if int(start_year) == int(end_year):
            return str(int(start_year))
        return f"{int(start_year)}-{int(end_year)}"
