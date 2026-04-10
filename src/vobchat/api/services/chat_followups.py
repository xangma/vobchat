from __future__ import annotations

import re
from dataclasses import dataclass, field

from vobchat.api.schemas.chat import (
    AnalysisReceipt,
    AnalysisSpec,
    ChatOperation,
    ChatThreadState,
    ClarificationOption,
    ClarificationState,
    DatasetFamilyRef,
    DiscourseAnchors,
    DiscourseOptionKind,
    DiscourseOptionRef,
    DiscourseOptionSet,
    ExactSliceRef,
    PlannerAction,
    ReceiptKind,
    ReportingGeographyRef,
    SlotStatus,
    StatePatch,
    StatePatchTarget,
    StatePatchType,
    TimeScope,
    WorkflowRuntimeMode,
)
from vobchat.api.schemas.places import PlaceResolveQuery, PlaceSearchQuery, ResolvedPlaceResponse
from vobchat.api.schemas.themes import ThemeSummaryResponse
from vobchat.api.services.places import PlacesService
from vobchat.api.services.chat_safety import CapabilityRegistry


@dataclass
class ReferenceResolutionResult:
    matched: bool
    reason: str
    patch: StatePatch | None = None
    resolved_action: PlannerAction | None = None
    source_receipt: AnalysisReceipt | None = None
    inherited_spec: AnalysisSpec | None = None
    resolved_place: ResolvedPlaceResponse | None = None
    resolved_places: list[ResolvedPlaceResponse] = field(default_factory=list)
    selected_option: DiscourseOptionRef | None = None
    clarification: ClarificationState | None = None
    notices: list[str] = field(default_factory=list)


@dataclass
class PatchApplicationResult:
    patch: StatePatch
    inherited_action: PlannerAction
    invalidated_fields: list[str] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)


class ReferenceResolver:
    _SAME_FOR_RE = re.compile(
        r"^(?:same(?: thing)?|do the same)(?: for)? (?P<place>.+)$",
        re.IGNORECASE,
    )
    _COMPARE_WITH_RE = re.compile(
        r"^(?:compare (?:that|it)? with|compare with) (?P<place>.+)$",
        re.IGNORECASE,
    )
    _REMOVE_RE = re.compile(r"^remove (?P<place>.+)$", re.IGNORECASE)
    _REMOVE_GROUP_RE = re.compile(r"^remove (?P<group>them|those|all)$", re.IGNORECASE)
    _COMPARE_GROUP_RE = re.compile(r"^compare (?P<group>them|those)$", re.IGNORECASE)
    _TABLE_RE = re.compile(
        r"\bshow (?:the )?table(?: for (?:them|those))?\b|\btable\b",
        re.IGNORECASE,
    )
    _MAP_RE = re.compile(
        r"\bmap (?:that|those|them)\b|\bshow (?:that|it|those|them) on (?:a )?map\b",
        re.IGNORECASE,
    )
    _YEAR_RE = re.compile(r"\b(?:use|show)\s+(?P<year>1[6-9]\d{2}|20\d{2})\s+instead\b", re.IGNORECASE)
    _LATEST_YEAR_RE = re.compile(r"\b(?:use|show)\s+(?:the\s+)?latest year\b", re.IGNORECASE)
    _SAME_PERIOD_RE = re.compile(r"\bsame (?:period|years?)\b", re.IGNORECASE)
    _ORDINAL_RE = re.compile(
        r"\b(?P<ordinal>first|second|third|fourth|fifth|last|1st|2nd|3rd|4th|5th)\b",
        re.IGNORECASE,
    )
    _OPTION_SELECTION_RE = re.compile(
        r"^(?:(?:use|pick|select|show)\s+)?(?:the\s+)?(?P<body>.+?)(?:\s+result)?[?.!]*$",
        re.IGNORECASE,
    )
    _RECEIPT_RESULT_RE = re.compile(
        r"^(?:use|show|pick|select)\s+(?:the\s+)?(?P<label>.+?)\s+result[?.!]*$",
        re.IGNORECASE,
    )

    def __init__(self, *, places_service: PlacesService) -> None:
        self.places_service = places_service

    def resolve(
        self,
        *,
        state: ChatThreadState,
        message: str,
    ) -> ReferenceResolutionResult:
        receipt, spec = self._active_anchor(state)
        stripped = message.strip()

        temporal_resolution = self._resolve_temporal_reference(
            state=state,
            raw_text=stripped,
            source_receipt=receipt,
            source_spec=spec,
        )
        if temporal_resolution is not None:
            return temporal_resolution

        year_match = self._YEAR_RE.search(stripped)
        if year_match:
            if spec is None:
                return self._missing_anchor(
                    question="I need an active validated result before I can change the time safely.",
                    slot="receipt_reference",
                    source_receipt=receipt,
                    reason="follow_up_time_missing_validated_anchor",
                )
            year = int(year_match.group("year"))
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_time_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_TIME,
                    source_receipt_id=receipt.receipt_id if receipt is not None else None,
                    time_scope=TimeScope(
                        mode="snapshot",
                        year=year,
                        label=str(year),
                        source="follow_up_reference",
                    ),
                ),
                source_receipt=receipt,
                inherited_spec=spec,
            )

        if self._REMOVE_GROUP_RE.match(stripped):
            return self._resolve_remove_group_patch(state=state)

        if self._COMPARE_GROUP_RE.match(stripped):
            return self._resolve_group_compare(
                state=state,
                source_receipt=receipt,
                source_spec=spec,
            )

        if self._MAP_RE.search(stripped):
            if spec is None:
                return self._missing_anchor(
                    question="I need an active validated result before I can switch that to a map.",
                    slot="receipt_reference",
                    source_receipt=receipt,
                    reason="follow_up_output_switch_missing_validated_anchor",
                )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_output_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_OUTPUT_MODE,
                    source_receipt_id=receipt.receipt_id if receipt is not None else None,
                    output_mode="boundary_map",
                ),
                source_receipt=receipt,
                inherited_spec=spec,
            )

        if self._TABLE_RE.search(stripped):
            if spec is None:
                return self._missing_anchor(
                    question="I need an active validated result before I can switch that to a table.",
                    slot="receipt_reference",
                    source_receipt=receipt,
                    reason="follow_up_output_switch_missing_validated_anchor",
                )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_output_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_OUTPUT_MODE,
                    source_receipt_id=receipt.receipt_id if receipt is not None else None,
                    output_mode="table",
                ),
                source_receipt=receipt,
                inherited_spec=spec,
            )

        receipt_resolution = self._resolve_receipt_reference(
            state=state,
            raw_text=stripped,
        )
        if receipt_resolution is not None:
            return receipt_resolution

        same_match = self._SAME_FOR_RE.match(stripped)
        if same_match:
            return self._resolve_place_patch(
                state=state,
                raw_text=same_match.group("place"),
                patch_type=StatePatchType.REPLACE_PLACE,
                reason="resolved_follow_up_replace_place_patch",
            )

        compare_match = self._COMPARE_WITH_RE.match(stripped)
        if compare_match:
            return self._resolve_place_patch(
                state=state,
                raw_text=compare_match.group("place"),
                patch_type=StatePatchType.ADD_PLACE,
                reason="resolved_follow_up_add_place_patch",
            )

        remove_match = self._REMOVE_RE.match(stripped)
        if remove_match:
            return self._resolve_remove_patch(state=state, raw_text=remove_match.group("place"))

        option_resolution = self._resolve_option_reference(
            state=state,
            raw_text=stripped,
            source_receipt=receipt,
            source_spec=spec,
        )
        if option_resolution is not None:
            return option_resolution

        return ReferenceResolutionResult(
            matched=False,
            reason="not_a_supported_follow_up",
        )

    def _resolve_place_patch(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
        patch_type: StatePatchType,
        reason: str,
    ) -> ReferenceResolutionResult:
        receipt, spec = self._active_anchor(state)
        resolution = self._resolve_place_reference_list(raw_text)
        if resolution.clarification is not None:
            return ReferenceResolutionResult(
                matched=True,
                reason="follow_up_place_reference_needs_clarification",
                source_receipt=receipt,
                inherited_spec=spec,
                clarification=resolution.clarification,
                notices=list(resolution.notices),
            )
        resolved_places = list(resolution.places)
        primary_place = resolved_places[0] if resolved_places else None
        return ReferenceResolutionResult(
            matched=True,
            reason=reason,
            patch=StatePatch(
                patch_type=patch_type,
                source_receipt_id=receipt.receipt_id if receipt is not None else None,
                target=StatePatchTarget(
                    raw_text=raw_text,
                    place_id=primary_place.place.place_id if primary_place is not None else None,
                    place_ids=[place.place.place_id for place in resolved_places],
                    label=primary_place.place.name if primary_place is not None else raw_text,
                    labels=[place.place.name for place in resolved_places],
                ),
            ),
            source_receipt=receipt,
            inherited_spec=spec,
            resolved_place=primary_place,
            resolved_places=resolved_places,
        )

    def _resolve_remove_patch(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
    ) -> ReferenceResolutionResult:
        receipt, spec = self._active_anchor(state)
        matches = [
            place
            for place in state.selected_places
            if raw_text.strip().lower() in place.place.name.lower()
        ]
        if not state.selected_places:
            clarification = ClarificationState(
                question="There is no selected place to remove yet.",
                slot="place_identity",
                source_receipt_id=receipt.receipt_id if receipt is not None else None,
            )
            return ReferenceResolutionResult(
                matched=True,
                reason="remove_place_missing_selected_context",
                source_receipt=receipt,
                inherited_spec=spec,
                clarification=clarification,
            )
        if len(matches) > 1:
            clarification = ClarificationState(
                question="Choose which selected place you want to remove.",
                slot="place_identity",
                options=[
                    ClarificationOption(
                        option_id=str(place.place.place_id),
                        label=place.place.name,
                        kind="selected_place",
                        metadata={"place_id": place.place.place_id},
                    )
                    for place in matches
                ],
                source_receipt_id=receipt.receipt_id if receipt is not None else None,
            )
            return ReferenceResolutionResult(
                matched=True,
                reason="remove_place_is_ambiguous",
                source_receipt=receipt,
                inherited_spec=spec,
                clarification=clarification,
            )
        if len(matches) == 1:
            place = matches[0]
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_remove_place_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.REMOVE_PLACE,
                    source_receipt_id=receipt.receipt_id if receipt is not None else None,
                    target=StatePatchTarget(
                        raw_text=raw_text,
                        place_id=place.place.place_id,
                        place_ids=[place.place.place_id],
                        label=place.place.name,
                        labels=[place.place.name],
                    ),
                ),
                source_receipt=receipt,
                inherited_spec=spec,
            )
        clarification = ClarificationState(
            question="I couldn't match that to one of the currently selected places.",
            slot="place_identity",
            source_receipt_id=receipt.receipt_id if receipt is not None else None,
        )
        return ReferenceResolutionResult(
            matched=True,
            reason="remove_place_not_in_current_selection",
            source_receipt=receipt,
            inherited_spec=spec,
            clarification=clarification,
        )

    def _resolve_remove_group_patch(
        self,
        *,
        state: ChatThreadState,
    ) -> ReferenceResolutionResult:
        receipt, spec = self._active_anchor(state)
        if not state.selected_places:
            return ReferenceResolutionResult(
                matched=True,
                reason="remove_group_missing_selected_context",
                source_receipt=receipt,
                inherited_spec=spec,
                clarification=ClarificationState(
                    question="There is no current group of selected places to remove.",
                    slot="place_identity",
                    source_receipt_id=receipt.receipt_id if receipt is not None else None,
                ),
            )
        return ReferenceResolutionResult(
            matched=True,
            reason="resolved_follow_up_remove_group_patch",
            patch=StatePatch(
                patch_type=StatePatchType.REMOVE_PLACE,
                source_receipt_id=receipt.receipt_id if receipt is not None else None,
                target=StatePatchTarget(
                    raw_text="them",
                    place_ids=[place.place.place_id for place in state.selected_places],
                    labels=[place.place.name for place in state.selected_places],
                ),
            ),
            source_receipt=receipt,
            inherited_spec=spec,
            resolved_places=list(state.selected_places),
        )

    def _resolve_group_compare(
        self,
        *,
        state: ChatThreadState,
        source_receipt: AnalysisReceipt | None,
        source_spec: AnalysisSpec | None,
    ) -> ReferenceResolutionResult:
        places = self._resolved_anchor_places(state=state, source_receipt=source_receipt)
        if len(places) < 2 or source_spec is None:
            return self._missing_anchor(
                question="I need an active grouped analytical result before I can compare them safely.",
                slot="receipt_reference",
                source_receipt=source_receipt,
                reason="follow_up_group_compare_missing_validated_anchor",
            )
        return ReferenceResolutionResult(
            matched=True,
            reason="resolved_follow_up_group_compare",
            resolved_action=self._build_action_from_spec(
                spec=source_spec,
                source_receipt=source_receipt,
            ),
            source_receipt=source_receipt,
            inherited_spec=source_spec,
            resolved_places=places,
        )

    def _active_anchor(
        self,
        state: ChatThreadState,
    ) -> tuple[AnalysisReceipt | None, AnalysisSpec | None]:
        if (
            state.current_receipt is not None
            and state.current_receipt.kind == ReceiptKind.ANALYSIS
            and state.current_receipt.analysis_spec is not None
        ):
            return state.current_receipt, state.current_receipt.analysis_spec
        for receipt in state.recent_receipts:
            if receipt.kind == ReceiptKind.ANALYSIS and receipt.analysis_spec is not None:
                return receipt, receipt.analysis_spec
        return state.current_receipt, state.analysis_state.analysis_spec

    def _missing_anchor(
        self,
        *,
        question: str,
        slot: str,
        source_receipt: AnalysisReceipt | None,
        reason: str,
    ) -> ReferenceResolutionResult:
        return ReferenceResolutionResult(
            matched=True,
            reason=reason,
            source_receipt=source_receipt,
            clarification=ClarificationState(
                question=question,
                slot=slot,
                source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
            ),
        )

    def _resolve_temporal_reference(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
        source_receipt: AnalysisReceipt | None,
        source_spec: AnalysisSpec | None,
    ) -> ReferenceResolutionResult | None:
        if self._SAME_PERIOD_RE.search(raw_text):
            if source_spec is None or source_spec.time_scope is None:
                return self._missing_anchor(
                    question="I need an active validated result before I can reuse the same period safely.",
                    slot="receipt_reference",
                    source_receipt=source_receipt,
                    reason="follow_up_same_period_missing_anchor",
                )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_same_period_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_TIME,
                    source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
                    time_scope=source_spec.time_scope.model_copy(deep=True),
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
            )

        if self._LATEST_YEAR_RE.search(raw_text):
            year = self._latest_year_anchor(state=state, source_spec=source_spec)
            if year is None:
                return self._missing_anchor(
                    question="I need a validated or discovered year context before I can pick the latest year.",
                    slot="time_scope",
                    source_receipt=source_receipt,
                    reason="follow_up_latest_year_missing_anchor",
                )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_follow_up_latest_year_patch",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_TIME,
                    source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
                    time_scope=TimeScope(
                        mode="snapshot",
                        year=year,
                        label=str(year),
                        source="follow_up_latest_year",
                    ),
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
            )
        return None

    def _resolve_receipt_reference(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
    ) -> ReferenceResolutionResult | None:
        match = self._RECEIPT_RESULT_RE.match(raw_text)
        if match is None:
            return None
        option = self._resolve_receipt_option(
            state=state,
            raw_text=match.group("label"),
        )
        if option is None:
            return ReferenceResolutionResult(
                matched=True,
                reason="receipt_reference_needs_clarification",
                clarification=ClarificationState(
                    question="I couldn't tell which previous result you meant.",
                    slot="receipt_reference",
                    options=self._receipt_clarification_options(state),
                    source_receipt_id=state.current_receipt_id,
                ),
            )
        return self._resolution_from_receipt_option(state=state, option=option)

    def _resolve_option_reference(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
        source_receipt: AnalysisReceipt | None,
        source_spec: AnalysisSpec | None,
    ) -> ReferenceResolutionResult | None:
        descriptor = self._parse_option_descriptor(raw_text)
        if descriptor is None:
            return None
        if (
            state.runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED
            and descriptor["ordinal"] is not None
            and descriptor["label"] is None
        ):
            return ReferenceResolutionResult(
                matched=True,
                reason="degraded_discourse_reference_requires_explicit_label",
                clarification=ClarificationState(
                    question="In degraded mode, choose the option by label instead of using ordinal references.",
                    slot="discourse_reference",
                    source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
                ),
            )

        option_selection = self._select_discourse_option(state=state, descriptor=descriptor)
        if option_selection is None:
            return None
        if option_selection.clarification is not None:
            return ReferenceResolutionResult(
                matched=True,
                reason="discourse_option_needs_clarification",
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                clarification=option_selection.clarification,
            )
        if option_selection.option is None:
            return None
        return self._resolution_from_selected_option(
            state=state,
            option=option_selection.option,
            source_receipt=source_receipt,
            source_spec=source_spec,
            raw_text=raw_text,
        )

    def _resolution_from_selected_option(
        self,
        *,
        state: ChatThreadState,
        option: DiscourseOptionRef,
        source_receipt: AnalysisReceipt | None,
        source_spec: AnalysisSpec | None,
        raw_text: str,
    ) -> ReferenceResolutionResult:
        if option.kind in {
            DiscourseOptionKind.SELECTED_PLACE,
            DiscourseOptionKind.PLACE_CANDIDATE,
            DiscourseOptionKind.CLARIFICATION_OPTION,
        }:
            place_id = option.metadata.get("place_id")
            if place_id is None:
                return ReferenceResolutionResult(matched=False, reason="option_missing_place_id")
            if raw_text.lower().startswith("remove"):
                return ReferenceResolutionResult(
                    matched=True,
                    reason="resolved_option_remove_place_patch",
                    patch=StatePatch(
                        patch_type=StatePatchType.REMOVE_PLACE,
                        source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
                        target=StatePatchTarget(
                            raw_text=option.label,
                            place_id=int(place_id),
                            place_ids=[int(place_id)],
                            label=option.label,
                            labels=[option.label],
                            option_id=option.option_id,
                        ),
                    ),
                    source_receipt=source_receipt,
                    inherited_spec=source_spec,
                )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_place_option",
                resolved_action=PlannerAction(
                    operation=ChatOperation.RESOLVE_PLACE,
                    place_id=int(place_id),
                    confidence=1.0,
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.REPORTING_GEOGRAPHY:
            operation = option.metadata.get("action_operation") or ChatOperation.LIST_THEMES.value
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_reporting_geography_option",
                resolved_action=PlannerAction(
                    operation=ChatOperation(operation),
                    unit_type=option.metadata.get("unit_type"),
                    theme_id=option.metadata.get("theme_id"),
                    confidence=1.0,
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.THEME:
            operation = option.metadata.get("action_operation") or ChatOperation.LIST_EXACT_SLICE_OPTIONS.value
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_theme_option",
                resolved_action=PlannerAction(
                    operation=ChatOperation(operation),
                    theme_id=option.metadata.get("theme_id"),
                    theme_query=option.label,
                    confidence=1.0,
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.EXACT_SLICE:
            operation = option.metadata.get("action_operation") or ChatOperation.LIST_AVAILABLE_YEARS.value
            exact_slice = (
                ExactSliceRef.model_validate(option.metadata["exact_slice"])
                if option.metadata.get("exact_slice") is not None
                else None
            )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_exact_slice_option",
                resolved_action=PlannerAction(
                    operation=ChatOperation(operation),
                    theme_id=option.metadata.get("theme_id"),
                    cube_id=option.metadata.get("cube_id"),
                    exact_slice=exact_slice,
                    confidence=1.0,
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.SUGGESTED_ACTION:
            action_operation = option.metadata.get("action_operation")
            if action_operation is None:
                return ReferenceResolutionResult(matched=False, reason="suggested_action_missing_operation")
            exact_slice = (
                ExactSliceRef.model_validate(option.metadata["exact_slice"])
                if option.metadata.get("exact_slice") is not None
                else None
            )
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_suggested_action",
                resolved_action=PlannerAction(
                    operation=ChatOperation(action_operation),
                    unit_type=option.metadata.get("unit_type"),
                    theme_id=option.metadata.get("theme_id"),
                    cube_id=option.metadata.get("cube_id"),
                    output_mode=option.metadata.get("output_mode"),
                    exact_slice=exact_slice,
                    confidence=1.0,
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.TIME_OPTION:
            year = option.metadata.get("year")
            start_year = option.metadata.get("start_year")
            end_year = option.metadata.get("end_year")
            return ReferenceResolutionResult(
                matched=True,
                reason="resolved_discourse_time_option",
                patch=StatePatch(
                    patch_type=StatePatchType.SET_TIME,
                    source_receipt_id=source_receipt.receipt_id if source_receipt is not None else None,
                    target=StatePatchTarget(option_id=option.option_id, raw_text=option.label),
                    time_scope=TimeScope(
                        mode="snapshot" if year is not None else "range",
                        year=int(year) if year is not None else None,
                        start_year=start_year,
                        end_year=end_year,
                        label=option.label,
                        source="discourse_option",
                    ),
                ),
                source_receipt=source_receipt,
                inherited_spec=source_spec,
                selected_option=option,
            )

        if option.kind == DiscourseOptionKind.RECEIPT:
            return self._resolution_from_receipt_option(state=state, option=option)

        return ReferenceResolutionResult(matched=False, reason="unsupported_discourse_option_kind")

    def _resolution_from_receipt_option(
        self,
        *,
        state: ChatThreadState,
        option: DiscourseOptionRef,
    ) -> ReferenceResolutionResult:
        receipt_id = option.metadata.get("receipt_id")
        receipt = next(
            (item for item in state.recent_receipts if item.receipt_id == receipt_id),
            None,
        )
        if receipt is None:
            return ReferenceResolutionResult(
                matched=True,
                reason="receipt_option_missing_receipt",
                clarification=ClarificationState(
                    question="That earlier result is no longer available in the current thread state.",
                    slot="receipt_reference",
                    source_receipt_id=state.current_receipt_id,
                ),
            )
        source_spec = receipt.analysis_spec
        if source_spec is None:
            operation = receipt.source_operation or ChatOperation.LIST_CATALOG_OVERVIEW
        else:
            operation = self._operation_for_output_mode(
                output_mode=source_spec.output_mode,
                source_receipt=receipt,
            )
        return ReferenceResolutionResult(
            matched=True,
            reason="resolved_discourse_receipt_option",
            resolved_action=PlannerAction(
                operation=operation,
                output_mode=source_spec.output_mode if source_spec is not None else None,
                theme_id=(
                    source_spec.dataset_family.theme_id
                    if source_spec is not None and source_spec.dataset_family is not None
                    else None
                ),
                cube_id=(
                    source_spec.exact_slice.cube_id
                    if source_spec is not None and source_spec.exact_slice is not None
                    else None
                ),
                exact_slice=(
                    source_spec.exact_slice.model_copy(deep=True)
                    if source_spec is not None and source_spec.exact_slice is not None
                    else None
                ),
                unit_type=(
                    source_spec.reporting_geography.unit_type
                    if source_spec is not None and source_spec.reporting_geography is not None
                    else None
                ),
                confidence=1.0,
            ),
            source_receipt=receipt,
            inherited_spec=source_spec,
            resolved_places=self._receipt_places(receipt),
            selected_option=option,
        )

    def _resolve_place_reference_list(self, raw_text: str) -> "_PlaceReferenceResolution":
        place_queries = self._split_place_list(raw_text)
        places: list[ResolvedPlaceResponse] = []
        for query in place_queries:
            resolution = self._resolve_single_place_reference(query)
            if resolution.clarification is not None:
                return resolution
            if resolution.place is not None:
                places.append(resolution.place)
        return _PlaceReferenceResolution(
            places=places,
            place=places[0] if places else None,
        )

    def _resolve_single_place_reference(self, raw_text: str):
        search = self._search_places(raw_text)
        if search.result_count == 1:
            resolved = self.places_service.resolve_place(
                search.results[0].place_id,
                PlaceResolveQuery(),
            )
            return _PlaceReferenceResolution(place=resolved, places=[resolved] if resolved is not None else [])
        if search.result_count > 1:
            return _PlaceReferenceResolution(
                clarification=ClarificationState(
                    question="Choose which place you mean before I reuse the previous result.",
                    slot="place_identity",
                    options=[
                        ClarificationOption(
                            option_id=str(item.place_id),
                            label=item.name,
                            kind="place_candidate",
                            metadata={"place_id": item.place_id},
                        )
                        for item in search.results
                    ],
                ),
                notices=["The replacement place name is ambiguous."],
            )
        return _PlaceReferenceResolution(
            clarification=ClarificationState(
                question=f"I couldn't find a place matching '{raw_text}'.",
                slot="place_identity",
            ),
            notices=[f"No places matched '{raw_text}'."],
        )

    def _search_places(self, query: str):
        exact = self.places_service.search_places(
            PlaceSearchQuery(query=query, match_mode="exact", limit=10)
        )
        if exact.result_count:
            return exact
        return self.places_service.search_places(
            PlaceSearchQuery(query=query, match_mode="fuzzy", limit=10)
        )

    @staticmethod
    def _split_place_list(raw_text: str) -> list[str]:
        parts = [
            item.strip()
            for item in re.split(r"\s*(?:,| and )\s*", raw_text.strip())
            if item.strip()
        ]
        return parts or [raw_text.strip()]

    def _resolve_receipt_option(
        self,
        *,
        state: ChatThreadState,
        raw_text: str,
    ) -> DiscourseOptionRef | None:
        anchors = self._anchors(state)
        receipt_set = next(
            (option_set for option_set in anchors.option_sets if option_set.kind == DiscourseOptionKind.RECEIPT),
            None,
        )
        if receipt_set is None:
            return None
        matches = [
            option
            for option in receipt_set.options
            if self._matches_receipt_label(option, raw_text)
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    @staticmethod
    def _matches_receipt_label(option: DiscourseOptionRef, raw_text: str) -> bool:
        lowered = raw_text.strip().lower()
        values = {
            option.label.lower(),
            str(option.metadata.get("reporting_unit_type") or "").lower(),
            str(option.metadata.get("reporting_geography_label") or "").lower(),
            str(option.metadata.get("output_mode") or "").lower(),
        }
        if lowered in values:
            return True
        if lowered == "parish" and option.metadata.get("reporting_unit_type") == "PAR_UNIT":
            return True
        if lowered in {"parish result", "parish"} and option.metadata.get("reporting_unit_type") == "PAR_UNIT":
            return True
        return False

    def _anchors(self, state: ChatThreadState) -> DiscourseAnchors:
        return state.conversation_state.discourse_anchors.model_copy(deep=True)

    def _select_discourse_option(
        self,
        *,
        state: ChatThreadState,
        descriptor: dict[str, object],
    ) -> "_DiscourseOptionSelection":
        anchors = self._anchors(state)
        candidate_sets = self._candidate_option_sets(
            anchors=anchors,
            kind_hint=descriptor.get("kind_hint"),
        )
        if not candidate_sets:
            return _DiscourseOptionSelection()

        label = descriptor.get("label")
        ordinal = descriptor.get("ordinal")
        if ordinal is not None:
            if len(candidate_sets) > 1 and descriptor.get("kind_hint") is None:
                preferred_sets = [item for item in candidate_sets if item.preferred]
                if len(preferred_sets) == 1:
                    candidate_sets = preferred_sets
                else:
                    return _DiscourseOptionSelection(
                        clarification=ClarificationState(
                            question="I need to know which list you mean before I can use that ordinal reference.",
                            slot="discourse_reference",
                        )
                    )
            option_set = candidate_sets[0]
            index = ordinal if isinstance(ordinal, int) else None
            if index == 999999:
                index = len(option_set.options)
            if index is None or index < 1 or index > len(option_set.options):
                return _DiscourseOptionSelection(
                    clarification=ClarificationState(
                        question="That ordinal does not match one of the currently anchored options.",
                        slot="discourse_reference",
                    )
                )
            return _DiscourseOptionSelection(option=option_set.options[index - 1])

        if label is None:
            return _DiscourseOptionSelection()

        if len(candidate_sets) == 1 and label in {"that", "those", "them"}:
            option_set = candidate_sets[0]
            if len(option_set.options) == 1:
                return _DiscourseOptionSelection(option=option_set.options[0])
            return _DiscourseOptionSelection(
                clarification=ClarificationState(
                    question="I need a more specific label because that reference still points to more than one option.",
                    slot="discourse_reference",
                )
            )

        matches: list[DiscourseOptionRef] = []
        for option_set in candidate_sets:
            matches.extend(
                option
                for option in option_set.options
                if self._matches_discourse_option(option, str(label))
            )
        if len(matches) == 1:
            return _DiscourseOptionSelection(option=matches[0])
        if len(matches) > 1:
            return _DiscourseOptionSelection(
                clarification=ClarificationState(
                    question="I matched more than one anchored option. Choose the exact one you want.",
                    slot="discourse_reference",
                    options=[
                        ClarificationOption(
                            option_id=option.option_id,
                            label=option.label,
                            kind=option.kind.value,
                            metadata=dict(option.metadata),
                        )
                        for option in matches[:6]
                    ],
                )
            )
        return _DiscourseOptionSelection()

    @staticmethod
    def _candidate_option_sets(
        *,
        anchors: DiscourseAnchors,
        kind_hint: object,
    ) -> list[DiscourseOptionSet]:
        if isinstance(kind_hint, DiscourseOptionKind):
            return [
                option_set
                for option_set in anchors.option_sets
                if option_set.kind == kind_hint and option_set.options
            ]
        return [option_set for option_set in anchors.option_sets if option_set.options]

    def _matches_discourse_option(
        self,
        option: DiscourseOptionRef,
        raw_label: str,
    ) -> bool:
        lowered = raw_label.strip().lower()
        if lowered == option.label.lower():
            return True
        if option.kind == DiscourseOptionKind.REPORTING_GEOGRAPHY:
            unit_type = str(option.metadata.get("unit_type") or "").lower()
            if lowered in {unit_type, f"{unit_type} level"}:
                return True
            if lowered in {"parish", "parish level"} and unit_type == "par_unit":
                return True
            if lowered in {"district", "modern district"} and unit_type == "mod_dist":
                return True
        if option.kind == DiscourseOptionKind.RECEIPT:
            return self._matches_receipt_label(option, raw_label)
        theme_id = str(option.metadata.get("theme_id") or "").lower()
        cube_id = str(option.metadata.get("cube_id") or "").lower()
        if lowered in {theme_id, cube_id}:
            return True
        return False

    @staticmethod
    def _parse_option_descriptor(raw_text: str) -> dict[str, object] | None:
        match = ReferenceResolver._OPTION_SELECTION_RE.match(raw_text)
        if match is None:
            return None
        body = match.group("body").strip().lower()
        if not body:
            return None
        ordinal_match = ReferenceResolver._ORDINAL_RE.search(body)
        ordinal = (
            ReferenceResolver._ordinal_value(ordinal_match.group("ordinal"))
            if ordinal_match is not None
            else None
        )
        kind_hint = None
        if "geograph" in body or "parish" in body or "district" in body:
            kind_hint = DiscourseOptionKind.REPORTING_GEOGRAPHY
        elif "theme" in body:
            kind_hint = DiscourseOptionKind.THEME
        elif "exact-slice" in body or "slice" in body:
            kind_hint = DiscourseOptionKind.EXACT_SLICE
        elif "result" in body:
            kind_hint = DiscourseOptionKind.RECEIPT
        elif (
            "year" in body
            or "period" in body
            or "earlier" in body
            or "later" in body
            or "latest" in body
        ):
            kind_hint = DiscourseOptionKind.TIME_OPTION
        if ordinal is None and "earlier" in body:
            ordinal = 1
        if ordinal is None and ("later" in body or "latest" in body):
            ordinal = 999999
        label = re.sub(
            r"\b(?:use|pick|select|show|the|one|option|result|first|second|third|fourth|fifth|last)\b",
            " ",
            body,
            flags=re.IGNORECASE,
        )
        normalized_label = " ".join(label.split()).strip()
        if normalized_label in {"that", "those", "them", "that geography", "that theme"}:
            normalized_label = normalized_label.replace(" geography", "").replace(" theme", "")
        return {
            "ordinal": ordinal,
            "kind_hint": kind_hint,
            "label": normalized_label or None,
        }

    @staticmethod
    def _ordinal_value(raw_value: str) -> int | None:
        mapping = {
            "first": 1,
            "1st": 1,
            "second": 2,
            "2nd": 2,
            "third": 3,
            "3rd": 3,
            "fourth": 4,
            "4th": 4,
            "fifth": 5,
            "5th": 5,
            "last": -1,
        }
        value = mapping.get(raw_value.lower())
        if value == -1:
            return 999999
        return value

    @staticmethod
    def _latest_year_anchor(
        *,
        state: ChatThreadState,
        source_spec: AnalysisSpec | None,
    ) -> int | None:
        anchors = state.conversation_state.discourse_anchors
        time_sets = [
            option_set
            for option_set in anchors.option_sets
            if option_set.kind == DiscourseOptionKind.TIME_OPTION and option_set.options
        ]
        years: list[int] = []
        for option_set in time_sets:
            for option in option_set.options:
                year = option.metadata.get("year")
                end_year = option.metadata.get("end_year")
                if year is not None:
                    years.append(int(year))
                elif end_year is not None:
                    years.append(int(end_year))
        if years:
            return max(years)
        if source_spec is not None and source_spec.exact_slice is not None and source_spec.exact_slice.end_year is not None:
            return int(source_spec.exact_slice.end_year)
        if source_spec is not None and source_spec.time_scope is not None:
            if source_spec.time_scope.year is not None:
                return source_spec.time_scope.year
            if source_spec.time_scope.end_year is not None:
                return int(source_spec.time_scope.end_year)
        return None

    @staticmethod
    def _receipt_places(receipt: AnalysisReceipt) -> list[ResolvedPlaceResponse]:
        if receipt.ui_projection is not None and receipt.ui_projection.selected_places:
            return [place.model_copy(deep=True) for place in receipt.ui_projection.selected_places]
        return []

    @staticmethod
    def _resolved_anchor_places(
        *,
        state: ChatThreadState,
        source_receipt: AnalysisReceipt | None,
    ) -> list[ResolvedPlaceResponse]:
        if len(state.selected_places) > 1:
            return [place.model_copy(deep=True) for place in state.selected_places]
        if source_receipt is not None and source_receipt.ui_projection is not None:
            return [
                place.model_copy(deep=True)
                for place in source_receipt.ui_projection.selected_places
            ]
        return [place.model_copy(deep=True) for place in state.selected_places]

    @staticmethod
    def _build_action_from_spec(
        *,
        spec: AnalysisSpec,
        source_receipt: AnalysisReceipt | None,
    ) -> PlannerAction:
        operation = ReferenceResolver._operation_for_output_mode(
            output_mode=spec.output_mode,
            source_receipt=source_receipt,
        )
        action = PlannerAction(
            operation=operation,
            confidence=1.0,
            output_mode=spec.output_mode,
            theme_id=spec.dataset_family.theme_id if spec.dataset_family is not None else None,
            cube_id=spec.exact_slice.cube_id if spec.exact_slice is not None else None,
            exact_slice=(
                spec.exact_slice.model_copy(deep=True)
                if spec.exact_slice is not None
                else None
            ),
            unit_type=(
                spec.reporting_geography.unit_type
                if spec.reporting_geography is not None
                else None
            ),
        )
        if spec.time_scope is not None:
            if spec.time_scope.year is not None:
                action.year = spec.time_scope.year
                action.start_year = float(spec.time_scope.year)
                action.end_year = float(spec.time_scope.year)
            else:
                action.start_year = spec.time_scope.start_year
                action.end_year = spec.time_scope.end_year
        return action

    @staticmethod
    def _operation_for_output_mode(
        *,
        output_mode: str | None,
        source_receipt: AnalysisReceipt | None,
    ) -> ChatOperation:
        if output_mode == "boundary_map":
            return ChatOperation.FETCH_MAP_FEATURES
        if output_mode in {"trend_chart", "snapshot_chart", "table"}:
            return ChatOperation.FETCH_TIME_SERIES
        if source_receipt is not None and source_receipt.source_operation is not None:
            return source_receipt.source_operation
        return ChatOperation.FETCH_TIME_SERIES

    def _receipt_clarification_options(
        self,
        state: ChatThreadState,
    ) -> list[ClarificationOption]:
        anchors = self._anchors(state)
        receipt_set = next(
            (option_set for option_set in anchors.option_sets if option_set.kind == DiscourseOptionKind.RECEIPT),
            None,
        )
        if receipt_set is None:
            return []
        return [
            ClarificationOption(
                option_id=option.option_id,
                label=option.label,
                kind=option.kind.value,
                metadata=dict(option.metadata),
            )
            for option in receipt_set.options[:6]
        ]


@dataclass
class _PlaceReferenceResolution:
    place: ResolvedPlaceResponse | None = None
    places: list[ResolvedPlaceResponse] = field(default_factory=list)
    clarification: ClarificationState | None = None
    notices: list[str] = field(default_factory=list)


@dataclass
class _DiscourseOptionSelection:
    option: DiscourseOptionRef | None = None
    clarification: ClarificationState | None = None


class StatePatchService:
    def __init__(self, *, capability_registry: CapabilityRegistry | None = None) -> None:
        self.capability_registry = capability_registry

    def apply(
        self,
        *,
        state: ChatThreadState,
        resolution: ReferenceResolutionResult,
    ) -> PatchApplicationResult:
        if resolution.patch is None:
            raise ValueError("Patch application requires a resolved patch.")

        patch = resolution.patch
        source_spec = resolution.inherited_spec or state.analysis_state.analysis_spec
        invalidated_fields: list[str] = []
        notices: list[str] = []

        if (
            patch.patch_type == StatePatchType.SET_OUTPUT_MODE
            and self.capability_registry is not None
            and source_spec is not None
        ):
            switch_assessment = self.capability_registry.output_switch_assessment(
                source_output_mode=source_spec.output_mode,
                target_output_mode=patch.output_mode,
                reporting_geography=source_spec.reporting_geography,
                dataset_family=source_spec.dataset_family,
                exact_slice=source_spec.exact_slice,
            )
            patch.metadata["output_switch_compatibility"] = switch_assessment.compatibility.value
            patch.metadata["output_switch_reason"] = switch_assessment.reason
            if switch_assessment.defaults_applied:
                patch.metadata["defaults_applied"] = list(switch_assessment.defaults_applied)
                notices.extend(switch_assessment.defaults_applied)

        if patch.patch_type == StatePatchType.REPLACE_PLACE:
            resolved_places = self._patch_places(resolution)
            if not resolved_places:
                raise ValueError("ReplacePlace requires a resolved place.")
            state.selected_places = [place.model_copy(deep=True) for place in resolved_places]
            state.selected_cubes = []
            invalidated_fields = ["place_identity", "reporting_geography", "availability", "comparability"]
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=source_spec.output_mode if source_spec is not None else None,
                time_scope=source_spec.time_scope if source_spec is not None else None,
                clear_reporting_geography_ids=True,
            )

        elif patch.patch_type == StatePatchType.ADD_PLACE:
            resolved_places = self._patch_places(resolution)
            if not resolved_places:
                raise ValueError("AddPlace requires a resolved place.")
            state.selected_places = self._merge_places(state.selected_places, resolved_places)
            state.selected_cubes = []
            invalidated_fields = ["reporting_geography", "availability", "comparability"]
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=source_spec.output_mode if source_spec is not None else None,
                time_scope=source_spec.time_scope if source_spec is not None else None,
                clear_reporting_geography_ids=True,
            )

        elif patch.patch_type == StatePatchType.REMOVE_PLACE:
            target_ids = set(patch.target.place_ids) if patch.target is not None else set()
            if not target_ids and patch.target is not None and patch.target.place_id is not None:
                target_ids = {patch.target.place_id}
            state.selected_places = [
                place
                for place in state.selected_places
                if place.place.place_id not in target_ids
            ]
            state.selected_cubes = []
            invalidated_fields = ["reporting_geography", "availability", "comparability"]
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=source_spec.output_mode if source_spec is not None else None,
                time_scope=source_spec.time_scope if source_spec is not None else None,
                clear_reporting_geography_ids=True,
            )

        elif patch.patch_type == StatePatchType.SET_TIME:
            invalidated_fields = ["time_scope", "availability", "comparability"]
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=source_spec.output_mode if source_spec is not None else None,
                time_scope=patch.time_scope,
                clear_reporting_geography_ids=False,
            )

        elif patch.patch_type == StatePatchType.SET_OUTPUT_MODE:
            invalidated_fields = ["output_mode", "availability"]
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=patch.output_mode,
                time_scope=source_spec.time_scope if source_spec is not None else None,
                clear_reporting_geography_ids=False,
            )
        else:
            raise ValueError(f"Unsupported patch type {patch.patch_type!r}.")

        inherited_action = self._build_inherited_action(
            state=state,
            patch=patch,
            source_receipt=resolution.source_receipt,
            source_spec=source_spec,
        )
        return PatchApplicationResult(
            patch=patch,
            inherited_action=inherited_action,
            invalidated_fields=invalidated_fields,
            notices=notices,
        )

    def seed_resolution_context(
        self,
        *,
        state: ChatThreadState,
        resolution: ReferenceResolutionResult,
    ) -> None:
        source_receipt = resolution.source_receipt
        source_spec = resolution.inherited_spec or (
            source_receipt.analysis_spec if source_receipt is not None else None
        )
        anchored_places = (
            [place.model_copy(deep=True) for place in resolution.resolved_places]
            if resolution.resolved_places
            else []
        )
        if not anchored_places and source_receipt is not None and source_receipt.ui_projection is not None:
            anchored_places = [
                place.model_copy(deep=True)
                for place in source_receipt.ui_projection.selected_places
            ]
        if anchored_places:
            state.selected_places = anchored_places

        if source_receipt is not None and source_receipt.ui_projection is not None:
            state.selected_cubes = [
                cube.model_copy(deep=True) for cube in source_receipt.ui_projection.selected_cubes
            ]
        elif source_spec is not None:
            state.selected_cubes = []

        if source_spec is not None:
            self._seed_inherited_analysis_state(
                state=state,
                source_spec=source_spec,
                output_mode=source_spec.output_mode,
                time_scope=source_spec.time_scope,
                clear_reporting_geography_ids=False,
            )
            return

        if source_receipt is None or source_receipt.ui_projection is None:
            return

        projection = source_receipt.ui_projection
        if projection.dataset_family is not None:
            state.selected_theme = projection.dataset_family.model_copy(deep=True)
        analysis_state = state.analysis_state.model_copy(deep=True)
        analysis_state.reporting_geography = (
            projection.reporting_geography.model_copy(deep=True)
            if projection.reporting_geography is not None
            else analysis_state.reporting_geography
        )
        analysis_state.dataset_family = (
            DatasetFamilyRef.from_theme(
                projection.dataset_family,
                source="receipt_projection",
                slot_status=SlotStatus.INHERITED_FROM_RECEIPT,
            )
            if projection.dataset_family is not None
            else analysis_state.dataset_family
        )
        analysis_state.candidate_slice = (
            projection.exact_slice.model_copy(deep=True)
            if projection.exact_slice is not None
            else analysis_state.candidate_slice
        )
        analysis_state.time_scope = (
            projection.time_scope.model_copy(deep=True)
            if projection.time_scope is not None
            else analysis_state.time_scope
        )
        analysis_state.output_mode = projection.active_output_mode or analysis_state.output_mode
        analysis_state.analysis_spec = None
        state.analysis_state = analysis_state

    @staticmethod
    def _merge_places(
        existing: list[ResolvedPlaceResponse],
        additions: list[ResolvedPlaceResponse],
    ) -> list[ResolvedPlaceResponse]:
        by_id = {place.place.place_id: place for place in existing}
        for place in additions:
            by_id[place.place.place_id] = place
        return list(by_id.values())

    @staticmethod
    def _patch_places(resolution: ReferenceResolutionResult) -> list[ResolvedPlaceResponse]:
        if resolution.resolved_places:
            return [place.model_copy(deep=True) for place in resolution.resolved_places]
        if resolution.resolved_place is not None:
            return [resolution.resolved_place.model_copy(deep=True)]
        return []

    def _seed_inherited_analysis_state(
        self,
        *,
        state: ChatThreadState,
        source_spec: AnalysisSpec | None,
        output_mode: str | None,
        time_scope: TimeScope | None,
        clear_reporting_geography_ids: bool,
    ) -> None:
        analysis_state = state.analysis_state.model_copy(deep=True)
        analysis_state.resolved_places = list(state.selected_places)
        analysis_state.analysis_spec = None
        analysis_state.availability_status = None
        analysis_state.comparability_status = None
        analysis_state.output_mode = output_mode
        analysis_state.time_scope = time_scope.model_copy(deep=True) if time_scope is not None else None

        if source_spec is not None:
            analysis_state.dataset_family = (
                source_spec.dataset_family.model_copy(deep=True)
                if source_spec.dataset_family is not None
                else None
            )
            analysis_state.candidate_slice = (
                source_spec.exact_slice.model_copy(deep=True)
                if source_spec.exact_slice is not None
                else None
            )
            if source_spec.reporting_geography is not None:
                reporting_geography = source_spec.reporting_geography.model_copy(deep=True)
                reporting_geography.slot_status = SlotStatus.INHERITED_FROM_RECEIPT
                if clear_reporting_geography_ids:
                    reporting_geography.unit_ids = []
                analysis_state.reporting_geography = reporting_geography
            else:
                analysis_state.reporting_geography = None
            if source_spec.dataset_family is not None and source_spec.dataset_family.theme_id:
                state.selected_theme = ThemeSummaryResponse(
                    theme_id=source_spec.dataset_family.theme_id,
                    label=source_spec.dataset_family.label or source_spec.dataset_family.theme_id,
                    description=source_spec.dataset_family.description,
                )
        else:
            analysis_state.dataset_family = state.analysis_state.dataset_family
            analysis_state.candidate_slice = state.analysis_state.candidate_slice
            analysis_state.reporting_geography = (
                None
                if clear_reporting_geography_ids
                else state.analysis_state.reporting_geography
            )

        state.analysis_state = analysis_state

    def _build_inherited_action(
        self,
        *,
        state: ChatThreadState,
        patch: StatePatch,
        source_receipt: AnalysisReceipt | None,
        source_spec: AnalysisSpec | None,
    ) -> PlannerAction:
        output_mode = patch.output_mode or (source_spec.output_mode if source_spec is not None else None)
        operation = self._operation_for_output_mode(
            output_mode=output_mode,
            source_receipt=source_receipt,
        )
        action = PlannerAction(
            operation=operation,
            confidence=1.0,
            output_mode=output_mode,
            theme_id=(
                source_spec.dataset_family.theme_id
                if source_spec is not None and source_spec.dataset_family is not None
                else (state.selected_theme.theme_id if state.selected_theme is not None else None)
            ),
            cube_id=(
                source_spec.exact_slice.cube_id
                if source_spec is not None and source_spec.exact_slice is not None
                else None
            ),
            exact_slice=(
                source_spec.exact_slice.model_copy(deep=True)
                if source_spec is not None and source_spec.exact_slice is not None
                else None
            ),
            unit_type=(
                source_spec.reporting_geography.unit_type
                if source_spec is not None and source_spec.reporting_geography is not None
                else (
                    state.analysis_state.reporting_geography.unit_type
                    if state.analysis_state.reporting_geography is not None
                    else None
                )
            ),
        )
        time_scope = patch.time_scope or (
            source_spec.time_scope if source_spec is not None else state.analysis_state.time_scope
        )
        if time_scope is not None:
            if time_scope.year is not None:
                action.year = time_scope.year
                action.start_year = float(time_scope.year)
                action.end_year = float(time_scope.year)
            else:
                action.start_year = time_scope.start_year
                action.end_year = time_scope.end_year
        return action

    @staticmethod
    def _operation_for_output_mode(
        *,
        output_mode: str | None,
        source_receipt: AnalysisReceipt | None,
    ) -> ChatOperation:
        if output_mode == "boundary_map":
            return ChatOperation.FETCH_MAP_FEATURES
        if output_mode in {"trend_chart", "snapshot_chart", "table"}:
            return ChatOperation.FETCH_TIME_SERIES
        if source_receipt is not None and source_receipt.source_operation is not None:
            return source_receipt.source_operation
        return ChatOperation.FETCH_TIME_SERIES
