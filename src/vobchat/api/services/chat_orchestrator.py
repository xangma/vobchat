from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
import logging
from typing import Any
from uuid import uuid4

from vobchat.api.schemas.chat import (
    AnalysisSpec,
    AnalysisReceipt,
    AssistantCompletedEventData,
    AssistantDeltaEventData,
    AvailabilityStatus,
    ChatMessage,
    ChatOperation,
    ChatSSEEventName,
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatTurnAcceptedResponse,
    ChatTurnRequest,
    ChatTurnResponse,
    ChatUIStateDelta,
    ClarificationOption,
    ClarificationState,
    ComparisonCapabilityTier,
    ComparabilityStatus,
    DatasetFamilyRef,
    DatasetStatus,
    DiscourseAnchors,
    DiscourseOptionKind,
    DiscourseOptionRef,
    DiscourseOptionSet,
    DiscoveryResult,
    ExecutionGuardOutcome,
    ExecutionGuardResult,
    DegradedModeOutcome,
    ExactSliceCandidate,
    ExactSliceProvenance,
    ErrorEventData,
    ExactSliceRef,
    OutputFeasibilityStatus,
    PlannerAction,
    PlannerResult,
    PolicyDecision,
    PolicyOutcome,
    ProvenanceResolvedContext,
    ProvenanceResultType,
    ProvenanceSourceContext,
    ProvenanceSummary,
    ProjectionReferences,
    ReceiptKind,
    RenderProjection,
    ReportingGeographyRef,
    ReportingGeographyStatus,
    ResolutionState,
    SemanticPlaceRef,
    SliceStatus,
    SlotStatus,
    StatePatch,
    StatePatchType,
    TimeScope,
    ThreadCreatedEventData,
    TurnCompletedEventData,
    TurnStartedEventData,
    UIDeltaEventData,
    UIProjection,
    WorkflowPath,
    WorkflowRuntimeMode,
    WorkflowRuntimeState,
)
from vobchat.api.services.chat_degraded import DegradedInterpreter
from vobchat.api.services.chat_discovery import DiscoveryService
from vobchat.api.services.exact_slices import (
    ExactSliceCatalogService,
    ExactSliceResolution,
    get_exact_slice_catalog_service,
)
from vobchat.api.services.chat_followups import ReferenceResolver, StatePatchService
from vobchat.api.schemas.maps import MapFeaturesByIdsQuery, MapFeaturesQuery
from vobchat.api.schemas.metadata import DataEntityResolveQuery
from vobchat.api.schemas.places import PlaceResolveQuery, PlaceSearchQuery, PostcodeLookupQuery
from vobchat.api.schemas.series import CategoryBreakdownRequest, TimeSeriesRequest
from vobchat.api.schemas.themes import ThemeListQuery, ThemeResolveQuery, ThemeSummaryResponse
from vobchat.api.services.chat_threads import InMemoryChatThreadStore, get_chat_thread_store, utc_now
from vobchat.api.services.chat_safety import (
    AnalysisExecutor,
    CapabilityRegistry,
    DecisionPolicyEngine,
    ExecutionGuard,
    ResolvedAnalysisContext,
)
from vobchat.api.services.maps import MapsService, get_maps_service
from vobchat.api.services.metadata import MetadataService, get_metadata_service
from vobchat.api.services.places import PlacesService, get_places_service
from vobchat.api.services.series import SeriesService, get_series_service
from vobchat.api.services.themes import ThemesService, get_themes_service
from vobchat.core.llm import (
    ChatPlanner,
    LLMClientError,
    LLMConfigurationError,
    LLMServiceUnavailableError,
    OpenAICompatibleLLMClient,
    get_chat_planner,
    get_llm_client,
    llm_setup_guidance,
)
from vobchat.core.llm.prompts import build_assistant_messages


logger = logging.getLogger(__name__)


@dataclass
class ExecutionOutcome:
    ui_delta: ChatUIStateDelta
    execution_summary: dict[str, Any]
    fallback_text: str
    workflow_path: WorkflowPath | None = None
    policy_decision: PolicyDecision | None = None
    execution_guard_result: ExecutionGuardResult | None = None
    analysis_spec: AnalysisSpec | None = None
    discovery_result: DiscoveryResult | None = None
    applied_patch: StatePatch | None = None
    table_payload: dict[str, Any] | None = None
    slice_candidates: list[ExactSliceCandidate] | None = None
    runtime_state: WorkflowRuntimeState | None = None


@dataclass
class AssistantGenerationResult:
    text: str
    notice: str | None = None
    error: str | None = None
    runtime_state: WorkflowRuntimeState | None = None


class ChatOrchestrator:
    def __init__(
        self,
        *,
        thread_store: InMemoryChatThreadStore | None = None,
        planner: ChatPlanner | None = None,
        llm_client: OpenAICompatibleLLMClient | None = None,
        places_service: PlacesService | None = None,
        themes_service: ThemesService | None = None,
        series_service: SeriesService | None = None,
        maps_service: MapsService | None = None,
        metadata_service: MetadataService | None = None,
        exact_slice_service: ExactSliceCatalogService | None = None,
    ) -> None:
        self.thread_store = thread_store or get_chat_thread_store()
        self.planner = planner or get_chat_planner()
        self.llm_client = llm_client or get_llm_client()
        self.places_service = places_service or get_places_service()
        self.themes_service = themes_service or get_themes_service()
        self.series_service = series_service or get_series_service()
        self.maps_service = maps_service or get_maps_service()
        self.metadata_service = metadata_service or get_metadata_service()
        self.exact_slice_service = exact_slice_service or get_exact_slice_catalog_service()
        self.capability_registry = CapabilityRegistry()
        self.decision_policy = DecisionPolicyEngine()
        self.execution_guard = ExecutionGuard()
        self.analysis_executor = AnalysisExecutor(
            series_service=self.series_service,
            maps_service=self.maps_service,
        )
        self.discovery_service = DiscoveryService(
            themes_service=self.themes_service,
            capability_registry=self.capability_registry,
            exact_slice_service=self.exact_slice_service,
        )
        self.degraded_interpreter = DegradedInterpreter(
            places_service=self.places_service,
            themes_service=self.themes_service,
        )
        self.reference_resolver = ReferenceResolver(places_service=self.places_service)
        self.state_patch_service = StatePatchService(
            capability_registry=self.capability_registry
        )

    async def create_thread(self) -> ChatThreadCreateResponse:
        state = self.thread_store.create_thread()
        await self.thread_store.publish_event(
            state.thread_id,
            ChatSSEEventName.THREAD_CREATED,
            ThreadCreatedEventData(state=state),
        )
        return ChatThreadCreateResponse(thread_id=state.thread_id, state=state)

    def get_thread_state(self, thread_id: str) -> ChatThreadState | None:
        return self.thread_store.get_thread(thread_id)

    async def start_streamed_turn(self, request: ChatTurnRequest) -> ChatTurnAcceptedResponse:
        if self.thread_store.get_thread(request.thread_id) is None:
            raise KeyError(request.thread_id)
        turn_id = str(uuid4())
        asyncio.create_task(self.handle_turn(request, emit_events=True, turn_id=turn_id))
        return ChatTurnAcceptedResponse(thread_id=request.thread_id, turn_id=turn_id)

    async def handle_turn(
        self,
        request: ChatTurnRequest,
        *,
        emit_events: bool | None = None,
        turn_id: str | None = None,
    ) -> ChatTurnResponse:
        publish_events = request.stream if emit_events is None else emit_events
        active_turn_id = turn_id or str(uuid4())
        state = self.thread_store.require_thread(request.thread_id)
        initial_reporting_geography = self._current_reporting_geography(state)
        user_message = ChatMessage(
            message_id=str(uuid4()),
            role="user",
            content=request.message,
            created_at=utc_now(),
        )
        state.messages.append(user_message)
        state = self.thread_store.save_thread(state)
        planner_result: PlannerResult | None = None

        if publish_events:
            await self.thread_store.publish_event(
                request.thread_id,
                ChatSSEEventName.TURN_STARTED,
                TurnStartedEventData(turn_id=active_turn_id, user_message=user_message),
            )

        try:
            planner_result, outcome = await self._plan_and_execute_turn(state, request.message)
            self._refresh_semantic_state(state, planner_result.action, outcome)
            receipt = self._persist_receipt_if_needed(state, planner_result.action, outcome)
            if receipt is not None:
                state.current_receipt = receipt
                state.current_receipt_id = receipt.receipt_id
                state.recent_receipts = self.thread_store.list_receipts(
                    request.thread_id,
                    limit=5,
                )
                state.conversation_state.current_receipt_id = receipt.receipt_id
                state.conversation_state.recent_receipt_ids = [
                    item.receipt_id for item in state.recent_receipts
                ]
                state.conversation_state.discourse_anchors = self._build_discourse_anchors(state)
                if state.ui_projection is not None:
                    state.ui_projection.current_receipt_id = receipt.receipt_id
                    state.ui_projection.current_receipt_kind = receipt.kind
            outcome.ui_delta = self._attach_semantic_delta(state, outcome.ui_delta)

            if publish_events:
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.UI_DELTA,
                    UIDeltaEventData(turn_id=active_turn_id, ui_delta=outcome.ui_delta),
                )

            assistant_result = await self._generate_assistant_text(
                state=state,
                planner_result=planner_result,
                execution_summary=outcome.execution_summary,
                fallback_text=outcome.fallback_text,
                thread_id=request.thread_id,
                turn_id=active_turn_id,
                publish_events=publish_events,
                runtime_state=outcome.runtime_state or state.runtime_state,
            )
            if assistant_result.runtime_state is not None:
                self._apply_runtime_state_override(
                    state=state,
                    runtime_state=assistant_result.runtime_state,
                    action=planner_result.action,
                    outcome=outcome,
                )
                outcome.runtime_state = assistant_result.runtime_state
            if assistant_result.notice and assistant_result.notice not in outcome.ui_delta.notices:
                outcome.ui_delta.notices.append(assistant_result.notice)
                self._sync_notice_state(state, outcome.ui_delta.notices)
                outcome.ui_delta = self._attach_semantic_delta(state, outcome.ui_delta)
                if publish_events:
                    await self.thread_store.publish_event(
                        request.thread_id,
                        ChatSSEEventName.UI_DELTA,
                        UIDeltaEventData(turn_id=active_turn_id, ui_delta=outcome.ui_delta),
                    )
            if publish_events and assistant_result.error:
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.ERROR,
                    ErrorEventData(turn_id=active_turn_id, error=assistant_result.error),
                )

            assistant_message = ChatMessage(
                message_id=str(uuid4()),
                role="assistant",
                content=assistant_result.text,
                created_at=utc_now(),
            )
            state.messages.append(assistant_message)
            self._finalize_semantic_state_after_assistant(
                state,
                assistant_text=assistant_result.text,
                ui_delta=outcome.ui_delta,
            )
            if state.current_receipt is not None:
                state.current_receipt = self.thread_store.save_receipt(
                    request.thread_id,
                    state.current_receipt,
                )
                state.recent_receipts = self.thread_store.list_receipts(
                    request.thread_id,
                    limit=5,
                )
                state.current_receipt_id = state.current_receipt.receipt_id
                if state.ui_projection is not None:
                    state.ui_projection.current_receipt_id = state.current_receipt.receipt_id
                    state.ui_projection.current_receipt_kind = state.current_receipt.kind
            outcome.ui_delta = self._attach_semantic_delta(state, outcome.ui_delta)
            state.latest_ui_delta = outcome.ui_delta
            state.updated_at = utc_now()
            state = self.thread_store.save_thread(state)

            response = ChatTurnResponse(
                thread_id=request.thread_id,
                turn_id=active_turn_id,
                planner_result=planner_result,
                assistant_message=assistant_message,
                ui_delta=outcome.ui_delta,
                thread_state=state,
            )

            if publish_events:
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.ASSISTANT_COMPLETED,
                    AssistantCompletedEventData(
                        turn_id=active_turn_id,
                        assistant_message=assistant_message,
                    ),
                )
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.TURN_COMPLETED,
                    TurnCompletedEventData(response=response),
                )
            self._log_workflow_event(
                thread_id=request.thread_id,
                turn_id=active_turn_id,
                action=planner_result.action,
                outcome=outcome,
                state=state,
                initial_reporting_geography=initial_reporting_geography,
            )
            return response
        except Exception as exc:
            planner_result = planner_result or PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.REPLY_ONLY,
                    confidence=0.0,
                    assistant_task="Return a concise error fallback.",
                ),
                notes="Planner or execution failed and the orchestrator fell back to an error response.",
            )
            ui_delta = ChatUIStateDelta(
                operation=planner_result.action.operation,
                notices=["I hit an error while handling that request. Try again or check the server logs."],
            )
            fallback_message = ChatMessage(
                message_id=str(uuid4()),
                role="assistant",
                content="I hit an error while handling that request. Try again or check the server logs.",
                created_at=utc_now(),
            )
            state.messages.append(fallback_message)
            self._sync_notice_state(state, ui_delta.notices)
            self._finalize_semantic_state_after_assistant(
                state,
                assistant_text=fallback_message.content,
                ui_delta=ui_delta,
            )
            ui_delta = self._attach_semantic_delta(state, ui_delta)
            state.latest_ui_delta = ui_delta
            state = self.thread_store.save_thread(state)
            response = ChatTurnResponse(
                thread_id=request.thread_id,
                turn_id=active_turn_id,
                planner_result=planner_result,
                assistant_message=fallback_message,
                ui_delta=ui_delta,
                thread_state=state,
            )
            if publish_events:
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.ERROR,
                    ErrorEventData(turn_id=active_turn_id, error=str(exc)),
                )
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.ASSISTANT_COMPLETED,
                    AssistantCompletedEventData(
                        turn_id=active_turn_id,
                        assistant_message=fallback_message,
                    ),
                )
                await self.thread_store.publish_event(
                    request.thread_id,
                    ChatSSEEventName.TURN_COMPLETED,
                    TurnCompletedEventData(response=response),
                )
            self._log_workflow_event(
                thread_id=request.thread_id,
                turn_id=active_turn_id,
                action=planner_result.action,
                outcome=None,
                state=state,
                initial_reporting_geography=initial_reporting_geography,
                error=str(exc),
            )
            return response

    async def _plan_and_execute_turn(
        self,
        state: ChatThreadState,
        message: str,
    ) -> tuple[PlannerResult, ExecutionOutcome]:
        follow_up_resolution = self.reference_resolver.resolve(state=state, message=message)
        if follow_up_resolution.matched:
            if follow_up_resolution.clarification is not None:
                action = PlannerAction(
                    operation=ChatOperation.CLARIFY,
                    confidence=1.0,
                    assistant_task=follow_up_resolution.clarification.question,
                )
                planner_result = PlannerResult(
                    source="fallback",
                    action=action,
                    notes=follow_up_resolution.reason,
                )
                return planner_result, self._follow_up_clarification_outcome(
                    clarification=follow_up_resolution.clarification,
                    reason=follow_up_resolution.reason,
                )
            if follow_up_resolution.patch is not None:
                patch_result = self.state_patch_service.apply(
                    state=state,
                    resolution=follow_up_resolution,
                )
                planner_result = PlannerResult(
                    source="fallback",
                    action=patch_result.inherited_action,
                    notes=f"follow_up_patch:{patch_result.patch.patch_type.value}",
                )
                outcome = await self._execute_action(state, patch_result.inherited_action)
                outcome.applied_patch = patch_result.patch
                outcome.execution_summary["follow_up_patch"] = patch_result.patch.model_dump(mode="json")
                outcome.execution_summary["follow_up_reason"] = follow_up_resolution.reason
                if patch_result.invalidated_fields:
                    outcome.execution_summary["invalidated_fields"] = list(
                        patch_result.invalidated_fields
                    )
                return planner_result, outcome

            if follow_up_resolution.resolved_action is not None:
                self.state_patch_service.seed_resolution_context(
                    state=state,
                    resolution=follow_up_resolution,
                )
                planner_result = PlannerResult(
                    source="fallback",
                    action=follow_up_resolution.resolved_action,
                    notes=f"discourse_reference:{follow_up_resolution.reason}",
                )
                outcome = await self._execute_action(state, follow_up_resolution.resolved_action)
                outcome.execution_summary["follow_up_reason"] = follow_up_resolution.reason
                if follow_up_resolution.source_receipt is not None:
                    outcome.execution_summary["source_receipt_id"] = (
                        follow_up_resolution.source_receipt.receipt_id
                    )
                if follow_up_resolution.selected_option is not None:
                    outcome.execution_summary["selected_option"] = (
                        follow_up_resolution.selected_option.model_dump(mode="json")
                    )
                return planner_result, outcome

            return PlannerResult(
                source="fallback",
                action=PlannerAction(operation=ChatOperation.CLARIFY, confidence=1.0),
                notes=follow_up_resolution.reason,
            ), self._follow_up_clarification_outcome(
                clarification=ClarificationState(
                    question="I couldn't safely reuse that reference.",
                    slot="discourse_reference",
                ),
                reason="follow_up_reference_unresolved",
            )

        explicit_discovery_action = self.discovery_service.detect_action(
            state=state,
            message=message,
        )
        if explicit_discovery_action is not None:
            planner_result = PlannerResult(
                source="fallback",
                action=explicit_discovery_action,
                notes="explicit_discovery_prepass",
            )
            outcome = await self._execute_action(state, explicit_discovery_action)
            outcome.runtime_state = WorkflowRuntimeState()
            return planner_result, outcome

        if hasattr(self.planner, "try_llm_plan"):
            try:
                planner_result = await self.planner.try_llm_plan(state, message)
                outcome = await self._execute_action(state, planner_result.action)
                outcome.runtime_state = WorkflowRuntimeState()
                return planner_result, outcome
            except Exception as exc:
                if not isinstance(
                    exc,
                    (LLMClientError, LLMConfigurationError, LLMServiceUnavailableError),
                ):
                    planner_result = await self.planner.plan(state, message)
                    outcome = await self._execute_action(state, planner_result.action)
                    outcome.runtime_state = WorkflowRuntimeState()
                    return planner_result, outcome
                degraded_result = self.degraded_interpreter.interpret(
                    state=state,
                    message=message,
                    degraded_reason=self._degraded_reason(exc),
                    detail=str(exc),
                )
                if degraded_result.clarification is not None:
                    planner_result = PlannerResult(
                        source="fallback",
                        action=PlannerAction(
                            operation=ChatOperation.CLARIFY,
                            confidence=1.0,
                            assistant_task=degraded_result.clarification.question,
                        ),
                        notes="degraded_guided_clarification",
                    )
                    return planner_result, self._degraded_clarification_outcome(
                        clarification=degraded_result.clarification,
                        reason="degraded_guided_clarification",
                        place_candidates=degraded_result.place_candidates,
                        notices=degraded_result.notices,
                        runtime_state=degraded_result.runtime_state,
                    )
                if degraded_result.planner_result is not None:
                    outcome = await self._execute_action(
                        state,
                        degraded_result.planner_result.action,
                    )
                    outcome.runtime_state = degraded_result.runtime_state
                    if degraded_result.notices:
                        outcome.ui_delta.notices = list(
                            dict.fromkeys([*outcome.ui_delta.notices, *degraded_result.notices])
                        )
                    outcome.execution_summary["degraded_mode"] = True
                    outcome.execution_summary["degraded_outcome"] = (
                        degraded_result.runtime_state.degraded_outcome.value
                        if degraded_result.runtime_state.degraded_outcome is not None
                        else None
                    )
                    outcome.execution_summary["degraded_reason"] = (
                        degraded_result.runtime_state.degraded_reason
                    )
                    return degraded_result.planner_result, outcome
                planner_result = PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.REPLY_ONLY,
                        confidence=1.0,
                        assistant_task=degraded_result.fallback_text,
                    ),
                    notes="degraded_unsupported_request",
                )
                return planner_result, self._degraded_reply_outcome(
                    runtime_state=degraded_result.runtime_state,
                    fallback_text=degraded_result.fallback_text
                    or "The chat model is unavailable, and that request is not supported in degraded mode.",
                    notices=degraded_result.notices,
                )

        planner_result = await self.planner.plan(state, message)
        outcome = await self._execute_action(state, planner_result.action)
        outcome.runtime_state = WorkflowRuntimeState()
        return planner_result, outcome

    async def _execute_action(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        handlers = {
            ChatOperation.SEARCH_PLACES: self._handle_search_places,
            ChatOperation.LOOKUP_POSTCODE: self._handle_lookup_postcode,
            ChatOperation.REMOVE_PLACE: self._handle_remove_place,
            ChatOperation.LIST_CATALOG_OVERVIEW: self._handle_list_catalog_overview,
            ChatOperation.LIST_THEMES: self._handle_list_themes,
            ChatOperation.LIST_REPORTING_GEOGRAPHIES: self._handle_list_reporting_geographies,
            ChatOperation.LIST_FEASIBLE_OUTPUTS: self._handle_list_feasible_outputs,
            ChatOperation.LIST_AVAILABLE_YEARS: self._handle_list_available_years,
            ChatOperation.LIST_EXACT_SLICE_OPTIONS: self._handle_list_exact_slice_options,
            ChatOperation.LIST_COMPARABLE_OPTIONS: self._handle_list_comparable_options,
            ChatOperation.RESOLVE_THEME: self._handle_resolve_theme,
            ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT: self._handle_list_cubes,
            ChatOperation.FETCH_TIME_SERIES: self._handle_time_series,
            ChatOperation.FETCH_CATEGORY_BREAKDOWN: self._handle_category_breakdown,
            ChatOperation.FETCH_MAP_FEATURES: self._handle_map_features,
            ChatOperation.FETCH_PLACE_PROFILE: self._handle_place_profile,
            ChatOperation.FETCH_UNIT_TYPE_INFO: self._handle_unit_type_info,
            ChatOperation.RESOLVE_DATA_ENTITY: self._handle_resolve_data_entity,
            ChatOperation.FETCH_DATA_ENTITY_INFO: self._handle_data_entity_info,
            ChatOperation.REPLY_ONLY: self._handle_reply_only,
            ChatOperation.CLARIFY: self._handle_clarify,
            ChatOperation.RESOLVE_PLACE: self._handle_resolve_place,
        }
        handler = handlers.get(action.operation)
        if handler is None:
            return await self._handle_reply_only(state, action)
        outcome = await handler(state, action)
        outcome.ui_delta.selected_places = list(state.selected_places)
        outcome.ui_delta.selected_theme = state.selected_theme
        outcome.ui_delta.selected_cubes = list(state.selected_cubes)
        return outcome

    async def _handle_reply_only(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(operation=ChatOperation.REPLY_ONLY),
            execution_summary={"operation": action.operation.value},
            fallback_text="How can I help you explore the Vision of Britain data?",
        )

    async def _handle_clarify(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        question = action.assistant_task or "Could you clarify what you want me to look up?"
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.CLARIFY,
                notices=[question],
            ),
            execution_summary={"operation": action.operation.value, "question": question},
            fallback_text=question,
        )

    def _follow_up_clarification_outcome(
        self,
        *,
        clarification: ClarificationState,
        reason: str,
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.CLARIFY,
                pending_clarification=clarification,
                notices=[clarification.question],
            ),
            execution_summary={
                "operation": ChatOperation.CLARIFY.value,
                "follow_up_reason": reason,
            },
            fallback_text=clarification.question,
            workflow_path=WorkflowPath.FOLLOW_UP_EDIT,
            policy_decision=PolicyDecision(
                outcome=PolicyOutcome.ASK,
                path=WorkflowPath.FOLLOW_UP_EDIT,
                reason=reason,
                clarification=clarification,
            ),
            execution_guard_result=ExecutionGuardResult(
                outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
                reason=reason,
                notices=[clarification.question],
            ),
        )

    def _degraded_clarification_outcome(
        self,
        *,
        clarification: ClarificationState,
        reason: str,
        place_candidates: list,
        notices: list[str],
        runtime_state: WorkflowRuntimeState,
    ) -> ExecutionOutcome:
        merged_notices = list(dict.fromkeys([clarification.question, *notices]))
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.CLARIFY,
                place_search_results=list(place_candidates),
                pending_clarification=clarification,
                notices=merged_notices,
            ),
            execution_summary={
                "operation": ChatOperation.CLARIFY.value,
                "degraded_mode": True,
                "degraded_outcome": (
                    runtime_state.degraded_outcome.value
                    if runtime_state.degraded_outcome is not None
                    else None
                ),
                "degraded_reason": runtime_state.degraded_reason,
            },
            fallback_text=clarification.question,
            workflow_path=WorkflowPath.CHAT_ONLY,
            runtime_state=runtime_state,
        )

    def _degraded_reply_outcome(
        self,
        *,
        runtime_state: WorkflowRuntimeState,
        fallback_text: str,
        notices: list[str],
    ) -> ExecutionOutcome:
        merged_notices = list(dict.fromkeys([*notices, fallback_text]))
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.REPLY_ONLY,
                notices=merged_notices,
            ),
            execution_summary={
                "operation": ChatOperation.REPLY_ONLY.value,
                "degraded_mode": True,
                "degraded_outcome": (
                    runtime_state.degraded_outcome.value
                    if runtime_state.degraded_outcome is not None
                    else None
                ),
                "degraded_reason": runtime_state.degraded_reason,
                "setup_guidance": list(runtime_state.setup_guidance),
            },
            fallback_text=fallback_text,
            workflow_path=WorkflowPath.CHAT_ONLY,
            runtime_state=runtime_state,
        )

    @staticmethod
    def _degraded_reason(exc: Exception) -> str:
        if isinstance(exc, LLMConfigurationError):
            return "llm_not_configured"
        if isinstance(exc, LLMServiceUnavailableError):
            return "llm_unavailable"
        if isinstance(exc, LLMClientError):
            return "llm_client_error"
        return "llm_interpretation_unavailable"

    async def _handle_search_places(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        query = action.place_query or state.messages[-1].content
        response = self._search_places_for_query(
            query,
            match_mode=action.match_mode or "fuzzy",
            limit=10,
        )
        delta = ChatUIStateDelta(
            operation=ChatOperation.SEARCH_PLACES,
            place_search_results=list(response.results),
        )
        if response.result_count == 1:
            resolved = self.places_service.resolve_place(
                response.results[0].place_id,
                PlaceResolveQuery(),
            )
            if resolved is not None:
                state.selected_places = self._merge_places(state.selected_places, [resolved])
                delta.place_search_results = []
                delta.notices.append(f"Selected {resolved.place.name}.")
                return ExecutionOutcome(
                    ui_delta=delta,
                    execution_summary={
                        "operation": action.operation.value,
                        "selected_place": resolved.place.name,
                    },
                    fallback_text=f"I added {resolved.place.name} to the current selection.",
                )
        if response.result_count == 0:
            delta.notices.append(f"No places matched '{query}'.")
            fallback_text = f"I couldn't find a place matching '{query}'."
        else:
            fallback_text = (
                f"I found {response.result_count} places matching '{query}'. "
                "Please choose one of the results."
            )
        return ExecutionOutcome(
            ui_delta=delta,
            execution_summary={
                "operation": action.operation.value,
                "query": query,
                "result_count": response.result_count,
                "results": [item.model_dump(mode="json") for item in response.results[:5]],
            },
            fallback_text=fallback_text,
        )

    async def _handle_lookup_postcode(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        postcode = action.postcode or state.messages[-1].content
        response = self.places_service.lookup_postcode(
            PostcodeLookupQuery(postcode=postcode)
        )
        delta = ChatUIStateDelta(
            operation=ChatOperation.LOOKUP_POSTCODE,
            postcode_lookup=response,
        )
        if response.result_count == 1 and response.results[0].place_id is not None:
            resolved = self.places_service.resolve_place(
                response.results[0].place_id,
                PlaceResolveQuery(),
            )
            if resolved is not None:
                state.selected_places = self._merge_places(state.selected_places, [resolved])
                delta.notices.append(f"Selected {resolved.place.name} from postcode {response.postcode}.")
                return ExecutionOutcome(
                    ui_delta=delta,
                    execution_summary={
                        "operation": action.operation.value,
                        "postcode": response.postcode,
                        "selected_place": resolved.place.name,
                    },
                    fallback_text=(
                        f"I looked up postcode {response.postcode} and selected {resolved.place.name}."
                    ),
                )
        return ExecutionOutcome(
            ui_delta=delta,
            execution_summary={
                "operation": action.operation.value,
                "postcode": response.postcode,
                "result_count": response.result_count,
            },
            fallback_text=(
                f"I found {response.result_count} matching unit"
                f"{'' if response.result_count == 1 else 's'} for postcode {response.postcode}."
            ),
        )

    async def _handle_resolve_place(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        if action.place_id is None:
            return await self._handle_search_places(state, action)
        resolved = self.places_service.resolve_place(action.place_id, PlaceResolveQuery())
        if resolved is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.RESOLVE_PLACE,
                    notices=[f"Place {action.place_id} was not found."],
                ),
                execution_summary={"operation": action.operation.value, "place_id": action.place_id},
                fallback_text=f"I couldn't resolve place {action.place_id}.",
            )
        state.selected_places = self._merge_places(state.selected_places, [resolved])
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(operation=ChatOperation.RESOLVE_PLACE),
            execution_summary={
                "operation": action.operation.value,
                "selected_place": resolved.place.name,
            },
            fallback_text=f"I selected {resolved.place.name}.",
        )

    async def _handle_remove_place(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        query = (action.place_query or "").strip().lower()
        if not state.selected_places:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.REMOVE_PLACE,
                    notices=["There are no selected places to remove."],
                ),
                execution_summary={"operation": action.operation.value, "removed": 0},
                fallback_text="There are no selected places to remove.",
            )

        if not query or query in {"all", "*"}:
            removed = len(state.selected_places)
            state.selected_places = []
            state.selected_cubes = []
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.REMOVE_PLACE,
                    cleared_places=True,
                ),
                execution_summary={"operation": action.operation.value, "removed": removed},
                fallback_text="I cleared the selected places.",
            )

        kept = [
            place
            for place in state.selected_places
            if query not in place.place.name.lower()
        ]
        removed = len(state.selected_places) - len(kept)
        state.selected_places = kept
        if not kept:
            state.selected_cubes = []
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.REMOVE_PLACE,
                notices=[f"Removed {removed} place(s)."] if removed else ["No selected place matched that name."],
            ),
            execution_summary={"operation": action.operation.value, "removed": removed},
            fallback_text=(
                f"I removed {removed} selected place{'s' if removed != 1 else ''}."
                if removed
                else "I couldn't find a selected place matching that name."
            ),
        )

    def _build_discovery_outcome(
        self,
        *,
        execution,
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=execution.operation,
                themes=execution.themes,
                cubes=list(execution.cubes),
                discovery_result=execution.result,
                notices=list(execution.notices),
            ),
            execution_summary={
                "operation": execution.operation.value,
                "discovery_topic": execution.result.topic.value,
                "item_count": len(execution.result.items),
            },
            fallback_text=execution.fallback_text,
            workflow_path=WorkflowPath.DISCOVERY,
            discovery_result=execution.result,
            slice_candidates=list(execution.result.exact_slice_options),
        )

    def _prepare_discovery_context(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome | None:
        if action.place_query and not state.selected_places:
            resolved = self._resolve_place_from_query(action.place_query)
            if resolved is None:
                return self._build_place_candidate_outcome(
                    operation=action.operation,
                    query=action.place_query,
                    fallback_notice="Choose which place you mean so I can inspect the catalog safely.",
                    fallback_text="I need one place before I can inspect that catalog safely.",
                )
            state.selected_places = self._merge_places(state.selected_places, [resolved])
        else:
            self._ensure_place_context(state, action)

        ensured_theme = self._ensure_theme(state, action)
        if action.theme_query or action.theme_id:
            if ensured_theme is None:
                query = action.theme_query or action.theme_id or "that theme"
                return ExecutionOutcome(
                    ui_delta=ChatUIStateDelta(
                        operation=action.operation,
                        notices=[f"Theme '{query}' was not found."],
                    ),
                    execution_summary={"operation": action.operation.value, "theme_query": query},
                    fallback_text=f"I couldn't resolve a theme from '{query}'.",
                    workflow_path=WorkflowPath.DISCOVERY,
                )
            if state.selected_theme is None or state.selected_theme.theme_id != ensured_theme.theme_id:
                state.selected_cubes = []
            state.selected_theme = ensured_theme
        return None

    async def _handle_list_catalog_overview(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_themes(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_reporting_geographies(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_feasible_outputs(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_available_years(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_exact_slice_options(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_list_comparable_options(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        pending = self._prepare_discovery_context(state, action)
        if pending is not None:
            return pending
        return self._build_discovery_outcome(
            execution=self.discovery_service.execute(
                state=state,
                action=action,
            )
        )

    async def _handle_resolve_theme(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        query = action.theme_query or action.theme_id
        if not query:
            return await self._handle_list_themes(state, action)
        response = self.themes_service.resolve_theme(ThemeResolveQuery(query=query))
        if response is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.RESOLVE_THEME,
                    notices=[f"Theme '{query}' was not found."],
                ),
                execution_summary={"operation": action.operation.value, "query": query},
                fallback_text=f"I couldn't resolve a theme from '{query}'.",
            )
        state.selected_theme = response.result
        state.selected_cubes = []
        delta = ChatUIStateDelta(operation=ChatOperation.RESOLVE_THEME)
        reporting_geography = self._resolve_reporting_geography_for_catalog(state, action)
        unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if state.selected_places and unit_id is not None:
            cubes = list(
                self.themes_service.list_cubes_for_unit_theme(
                    unit_id,
                    response.result.theme_id,
                ).items
            )
            delta.cubes = cubes
        elif state.selected_places:
            delta.notices.append(
                "Choose a reporting geography before I can list slice candidates for that theme."
            )
        return ExecutionOutcome(
            ui_delta=delta,
            execution_summary={
                "operation": action.operation.value,
                "theme": response.result.model_dump(mode="json"),
                "cube_count": len(delta.cubes),
            },
            fallback_text=f"I set the theme to {response.result.label}.",
            workflow_path=WorkflowPath.DISCOVERY,
        )

    async def _handle_list_cubes(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        resolved_theme = self._ensure_theme(state, action)
        if resolved_theme is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
                    notices=["Choose a theme first so I know which cubes to list."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a theme before I can list cubes.",
                workflow_path=WorkflowPath.DISCOVERY,
            )
        reporting_geography = self._resolve_reporting_geography_for_catalog(state, action)
        unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if unit_id is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
                    notices=["Choose a reporting geography first so I know which slice family to inspect."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a reporting geography before I can list cubes safely.",
                workflow_path=WorkflowPath.DISCOVERY,
            )
        cubes = self.themes_service.list_cubes_for_unit_theme(unit_id, resolved_theme.theme_id)
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
                cubes=list(cubes.items),
            ),
            execution_summary={
                "operation": action.operation.value,
                "theme": resolved_theme.label,
                "cube_count": cubes.item_count,
            },
            fallback_text=(
                f"I found {cubes.item_count} cube"
                f"{'' if cubes.item_count == 1 else 's'} for {resolved_theme.label}."
            ),
            workflow_path=WorkflowPath.DISCOVERY,
        )

    async def _handle_time_series(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        return await self._handle_guarded_analysis(state, action)

    async def _handle_category_breakdown(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        return await self._handle_guarded_analysis(state, action)

    async def _handle_map_features(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        return await self._handle_guarded_analysis(state, action)

    async def _handle_place_profile(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        resolved_place = self._ensure_single_place(state, action)
        if resolved_place is None:
            candidate_outcome = self._build_place_candidate_outcome(
                operation=ChatOperation.FETCH_PLACE_PROFILE,
                query=action.place_query,
                fallback_notice="Choose a place first so I know which profile to fetch.",
                fallback_text="I need a place before I can fetch a place profile.",
            )
            if candidate_outcome is not None:
                return candidate_outcome
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_PLACE_PROFILE,
                    notices=["Choose a place first so I know which profile to fetch."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a place before I can fetch a place profile.",
            )
        profile = self.metadata_service.get_place_profile(resolved_place.place.place_id)
        if profile is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_PLACE_PROFILE,
                    notices=[f"No place profile was available for {resolved_place.place.name}."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text=f"I couldn't find profile information for {resolved_place.place.name}.",
            )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_PLACE_PROFILE,
                place_profile=profile,
            ),
            execution_summary={
                "operation": action.operation.value,
                "place": resolved_place.place.name,
                "has_text": bool(profile.text),
            },
            fallback_text=f"I fetched the place profile for {resolved_place.place.name}.",
        )

    async def _handle_unit_type_info(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        unit_type = action.unit_type
        if not unit_type:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_UNIT_TYPE_INFO,
                    notices=["Tell me which unit type you want to inspect."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a unit type to look up.",
            )
        info = self.metadata_service.get_unit_type_info(unit_type)
        if info is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_UNIT_TYPE_INFO,
                    notices=[f"Unit type '{unit_type}' was not found."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text=f"I couldn't find information for unit type {unit_type}.",
            )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_UNIT_TYPE_INFO,
                unit_type_info=info,
            ),
            execution_summary={
                "operation": action.operation.value,
                "identifier": info.identifier,
                "label": info.label,
                "unit_count": info.unit_count,
            },
            fallback_text=f"I fetched the unit type information for {info.label}.",
        )

    async def _handle_resolve_data_entity(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        query = action.entity_query or action.entity_id or state.messages[-1].content
        resolution = self.metadata_service.resolve_data_entity(
            DataEntityResolveQuery(query=query)
        )
        if resolution is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.RESOLVE_DATA_ENTITY,
                    notices=[f"No data entity matched '{query}'."],
                ),
                execution_summary={"operation": action.operation.value, "query": query},
                fallback_text=f"I couldn't resolve a data entity from '{query}'.",
            )
        info = self.metadata_service.get_data_entity_info(resolution.result.entity_id)
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.RESOLVE_DATA_ENTITY,
                data_entity_resolution=resolution,
                data_entity_info=info,
            ),
            execution_summary={
                "operation": action.operation.value,
                "entity_id": resolution.result.entity_id,
                "label": resolution.result.label,
                "has_info": info is not None,
            },
            fallback_text=(
                f"I resolved '{query}' to {resolution.result.label}."
                if info is None
                else f"I resolved '{query}' to {resolution.result.label} and loaded its metadata."
            ),
        )

    async def _handle_data_entity_info(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        entity_id = action.entity_id
        resolution = None
        if not entity_id:
            query = action.entity_query or state.messages[-1].content
            resolution = self.metadata_service.resolve_data_entity(
                DataEntityResolveQuery(query=query)
            )
            entity_id = resolution.result.entity_id if resolution else None
        if not entity_id:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_DATA_ENTITY_INFO,
                    notices=["Tell me which data entity you want to inspect."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a data entity id or label to look up.",
            )
        info = self.metadata_service.get_data_entity_info(entity_id)
        if info is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_DATA_ENTITY_INFO,
                    notices=[f"Data entity '{entity_id}' was not found."],
                ),
                execution_summary={"operation": action.operation.value, "entity_id": entity_id},
                fallback_text=f"I couldn't find information for data entity {entity_id}.",
            )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_DATA_ENTITY_INFO,
                data_entity_resolution=resolution,
                data_entity_info=info,
            ),
            execution_summary={
                "operation": action.operation.value,
                "entity_id": info.entity_id,
                "name": info.name,
                "entity_type": info.entity_type,
            },
            fallback_text=f"I fetched the data entity information for {info.name or info.entity_id}.",
        )

    async def _handle_guarded_analysis(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        if not self._ensure_place_context(state, action) and not action.unit_type:
            candidate_outcome = self._build_place_candidate_outcome(
                operation=action.operation,
                query=action.place_query,
                fallback_notice="Choose a place so I can show the right data.",
                fallback_text="I need a place before I can run that analysis.",
            )
            if candidate_outcome is not None:
                return self._attach_policy_block(
                    candidate_outcome,
                    policy_outcome=PolicyOutcome.ASK,
                    reason="place_identity_is_ambiguous_or_missing",
                    missing_fields=["place_identity"],
                )

        context = self._build_analysis_context(state, action)
        spec = self._build_analysis_spec_from_context(context)
        decision = self.decision_policy.decide(
            context=context,
            spec=spec,
            capability_registry=self.capability_registry,
        )
        spec = self._apply_safe_defaults(context, spec, decision)
        guard_result = self.execution_guard.evaluate(
            context=context,
            spec=spec,
            decision=decision,
            capability_registry=self.capability_registry,
        )

        if guard_result.outcome in {
            ExecutionGuardOutcome.NEEDS_CLARIFICATION,
            ExecutionGuardOutcome.DISCOVERY_RESPONSE,
            ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
        }:
            return self._analysis_blocked_outcome(
                action=action,
                context=context,
                spec=spec,
                decision=decision,
                guard_result=guard_result,
            )

        execution_result = self.analysis_executor.execute(
            context=context,
            spec=guard_result.runnable_analysis_spec or spec,
            guard_result=guard_result,
        )
        if execution_result.selected_cubes:
            state.selected_cubes = list(execution_result.selected_cubes)

        notices = list(execution_result.notices)
        if decision.outcome == PolicyOutcome.DEFAULT_WITH_DISCLOSURE:
            notices.append(self._default_disclosure_notice(context, decision))

        ui_delta = ChatUIStateDelta(
            operation=action.operation,
            cubes=list(context.cube_candidates),
            time_series=execution_result.time_series,
            category_breakdown=execution_result.category_breakdown,
            map_features=execution_result.map_features,
            notices=list(dict.fromkeys(notices)),
        )
        return ExecutionOutcome(
            ui_delta=ui_delta,
            execution_summary={
                "operation": action.operation.value,
                **execution_result.execution_summary,
            },
            fallback_text=execution_result.fallback_text,
            workflow_path=decision.path or WorkflowPath.ANALYSIS,
            policy_decision=decision,
            execution_guard_result=guard_result,
            analysis_spec=guard_result.runnable_analysis_spec or spec,
            table_payload=execution_result.table,
        )

    def _attach_policy_block(
        self,
        blocked_outcome: ExecutionOutcome,
        *,
        policy_outcome: PolicyOutcome,
        reason: str,
        missing_fields: list[str],
    ) -> ExecutionOutcome:
        clarification = None
        if blocked_outcome.ui_delta.place_search_results:
            clarification = ClarificationState(
                question=blocked_outcome.ui_delta.notices[0]
                if blocked_outcome.ui_delta.notices
                else "Choose one of the matching places.",
                slot="place_identity",
                options=[
                    ClarificationOption(
                        option_id=str(item.place_id),
                        label=item.name,
                        kind="place_candidate",
                        metadata={"place_id": item.place_id},
                    )
                    for item in blocked_outcome.ui_delta.place_search_results
                ],
            )
        blocked_outcome.policy_decision = PolicyDecision(
            outcome=policy_outcome,
            path=WorkflowPath.ANALYSIS,
            reason=reason,
            missing_fields=missing_fields,
            clarification=clarification,
            requested_output_mode=self._output_mode_for_action(blocked_outcome.ui_delta.operation),
        )
        blocked_outcome.execution_guard_result = ExecutionGuardResult(
            outcome=ExecutionGuardOutcome.NEEDS_CLARIFICATION,
            reason=reason,
            blocking_fields=missing_fields,
            notices=[clarification.question] if clarification else list(blocked_outcome.ui_delta.notices),
        )
        blocked_outcome.workflow_path = WorkflowPath.ANALYSIS
        blocked_outcome.analysis_spec = None
        return blocked_outcome

    def _build_analysis_context(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ResolvedAnalysisContext:
        dataset_theme = self._ensure_theme(state, action)
        if dataset_theme is not None:
            state.selected_theme = dataset_theme

        reporting_geography_candidates = self.capability_registry.enabled_reporting_geography_candidates(
            list(state.selected_places)
        )
        reporting_geography = self._resolve_reporting_geography_for_analysis(
            state,
            action,
            reporting_geography_candidates,
        )
        representative_unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        cube_candidates: list = []
        if dataset_theme is not None and representative_unit_id is not None:
            cube_candidates = list(
                self.themes_service.list_cubes_for_unit_theme(
                    representative_unit_id,
                    dataset_theme.theme_id,
                ).items
            )

        exact_slice, exact_slice_candidates = self._resolve_exact_slice_candidate(
            state,
            action,
            dataset_theme=dataset_theme,
            representative_unit_id=representative_unit_id,
            cube_candidates=cube_candidates,
        )
        time_scope = self._build_requested_time_scope(action)

        return ResolvedAnalysisContext(
            operation=action.operation,
            output_mode=self._effective_output_mode(action) or "analysis",
            places=list(state.selected_places),
            reporting_geography_candidates=reporting_geography_candidates,
            reporting_geography=reporting_geography,
            dataset_family=(
                DatasetFamilyRef.from_theme(
                    dataset_theme,
                    source="selected_theme" if state.selected_theme is not None else "planner_action",
                    slot_status=SlotStatus.RESOLVED,
                )
                if dataset_theme is not None
                else None
            ),
            dataset_family_candidates=self._dataset_family_candidates_from_state(state, dataset_theme),
            cube_candidates=cube_candidates,
            exact_slice_candidates=exact_slice_candidates,
            exact_slice=exact_slice,
            time_scope=time_scope,
            comparison_mode="single_place" if len(state.selected_places) <= 1 else "multi_place",
            requested_unit_type=action.unit_type,
            requested_theme_id=dataset_theme.theme_id if dataset_theme is not None else action.theme_id,
            requested_cube_id=action.cube_id or (action.cube_ids[0] if action.cube_ids else None),
            requested_cube_query=action.cube_query,
            wants_theme_context=dataset_theme is not None or state.selected_theme is not None,
            requested_full_layer_map=(
                action.operation == ChatOperation.FETCH_MAP_FEATURES
                and not state.selected_places
                and action.unit_type is not None
            ),
            alternatives=self._analysis_alternatives(action),
        )

    def _build_analysis_spec_from_context(
        self,
        context: ResolvedAnalysisContext,
    ) -> AnalysisSpec | None:
        if (
            not context.places
            and context.reporting_geography is None
            and context.dataset_family is None
            and context.exact_slice is None
        ):
            return None

        dataset_status = DatasetStatus.RESOLVED
        if context.output_mode != "boundary_map" and context.dataset_family is None:
            dataset_status = DatasetStatus.NEEDS_CLARIFICATION

        slice_status = SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION
        if context.exact_slice is not None:
            slice_status = (
                SliceStatus.UNIQUE_EXECUTABLE_SLICE
                if context.exact_slice.cellref
                or context.exact_slice.dataitem_id
                or context.exact_slice.cat_id
                else SliceStatus.FAMILY_ONLY
            )
        elif context.exact_slice_candidates:
            slice_status = SliceStatus.CANDIDATE_SET

        reporting_geography_status = (
            ReportingGeographyStatus.RESOLVED
            if context.reporting_geography is not None and context.reporting_geography.unit_ids
            else ReportingGeographyStatus.NEEDS_CLARIFICATION
        )
        output_feasibility_status = self.capability_registry.output_feasibility(
            output_mode=context.output_mode,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            wants_theme_context=context.wants_theme_context,
            requested_full_layer_map=context.requested_full_layer_map,
        )
        availability_status = self._availability_status_for_context(context, output_feasibility_status)
        comparison_assessment = self.capability_registry.comparison_assessment(
            places=context.places,
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            time_scope=context.time_scope,
        )
        comparability_status = self._comparability_status_from_assessment(
            comparison_assessment.tier
        )

        return AnalysisSpec(
            places=[
                SemanticPlaceRef.from_resolved_place(place, source="selected_place")
                for place in context.places
            ],
            reporting_geography=context.reporting_geography,
            dataset_family=context.dataset_family,
            exact_slice=context.exact_slice,
            time_scope=context.time_scope,
            output_mode=context.output_mode,
            comparison_mode=context.comparison_mode,
            reporting_geography_status=reporting_geography_status,
            dataset_status=dataset_status,
            slice_status=slice_status,
            comparability_status=comparability_status,
            availability_status=availability_status,
            output_feasibility_status=output_feasibility_status,
        )

    def _resolve_reporting_geography_for_analysis(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        candidates: list[ReportingGeographyRef],
    ) -> ReportingGeographyRef | None:
        if action.unit_type:
            for candidate in candidates:
                if candidate.unit_type == action.unit_type:
                    selected = candidate.model_copy(deep=True)
                    selected.slot_status = SlotStatus.RESOLVED
                    selected.source = "planner_action"
                    return selected
            if self.capability_registry.geography(action.unit_type) is not None:
                return ReportingGeographyRef(
                    unit_type=action.unit_type,
                    unit_ids=[],
                    label=action.unit_type,
                    scope_rule="selected_places",
                    slot_status=SlotStatus.BLOCKED,
                    source="planner_action",
                )
            return None

        if state.analysis_state.reporting_geography is not None:
            selected = state.analysis_state.reporting_geography.model_copy(deep=True)
            if selected.unit_type and any(
                candidate.unit_type == selected.unit_type for candidate in candidates
            ):
                selected.slot_status = SlotStatus.RESOLVED
                return selected

        if len(candidates) == 1:
            selected = candidates[0].model_copy(deep=True)
            selected.slot_status = SlotStatus.RESOLVED
            return selected
        return None

    def _resolve_reporting_geography_for_catalog(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ReportingGeographyRef | None:
        candidates = self.capability_registry.enabled_reporting_geography_candidates(
            list(state.selected_places)
        )
        return self._resolve_reporting_geography_for_analysis(state, action, candidates)

    def _resolve_exact_slice_candidate(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        *,
        dataset_theme: ThemeSummaryResponse | None,
        representative_unit_id: int | None,
        cube_candidates: list,
    ) -> tuple[ExactSliceRef | None, list[ExactSliceCandidate]]:
        if action.exact_slice is not None:
            inherited = action.exact_slice.model_copy(deep=True)
            if inherited.cube_id is None:
                return inherited, []
            if inherited.cellref or inherited.dataitem_id or inherited.cat_id:
                inherited.slot_status = SlotStatus.INHERITED_FROM_RECEIPT
                inherited.source = inherited.source or "follow_up_receipt"
                return inherited, []
            if not cube_candidates or any(
                cube.cube_id == action.exact_slice.cube_id for cube in cube_candidates
            ):
                resolution = self._resolve_metadata_exact_slice(
                    dataset_theme=dataset_theme,
                    representative_unit_id=representative_unit_id,
                    requested_cube_id=inherited.cube_id,
                    requested_cube_query=None,
                )
                if resolution.resolved is not None:
                    resolved = resolution.resolved
                    resolved.slot_status = SlotStatus.INHERITED_FROM_RECEIPT
                    resolved.source = resolved.source or "follow_up_receipt"
                    return resolved, resolution.candidates
                if resolution.candidates:
                    return None, resolution.candidates
                return inherited, resolution.candidates

        if action.cube_id or (len(action.cube_ids) == 1):
            requested_cube_id = action.cube_id or action.cube_ids[0]
            resolution = self._resolve_metadata_exact_slice(
                dataset_theme=dataset_theme,
                representative_unit_id=representative_unit_id,
                requested_cube_id=requested_cube_id,
                requested_cube_query=None,
            )
            if resolution.resolved is not None:
                resolved = resolution.resolved
                resolved.source = "planner_action"
                return resolved, resolution.candidates
            if resolution.candidates:
                return None, resolution.candidates
            for cube in cube_candidates:
                if cube.cube_id == requested_cube_id:
                    return (
                        ExactSliceRef.from_cube(
                            cube,
                            source="planner_action",
                            slot_status=SlotStatus.RESOLVED,
                        ),
                        resolution.candidates,
                    )

        if action.cube_query:
            resolution = self._resolve_metadata_exact_slice(
                dataset_theme=dataset_theme,
                representative_unit_id=representative_unit_id,
                requested_cube_id=None,
                requested_cube_query=action.cube_query,
            )
            if resolution.resolved is not None:
                resolved = resolution.resolved
                resolved.source = "planner_action"
                return resolved, resolution.candidates
            if resolution.candidates:
                return None, resolution.candidates
            matches = [cube for cube in cube_candidates if action.cube_query.lower() in cube.label.lower()]
            if len(matches) == 1:
                cube = matches[0]
                return (
                    ExactSliceRef.from_cube(
                        cube,
                        source="planner_action",
                        slot_status=SlotStatus.RESOLVED,
                    ),
                    resolution.candidates,
                )

        if len(cube_candidates) == 1:
            resolution = self._resolve_metadata_exact_slice(
                dataset_theme=dataset_theme,
                representative_unit_id=representative_unit_id,
                requested_cube_id=cube_candidates[0].cube_id,
                requested_cube_query=None,
            )
            if resolution.resolved is not None:
                resolved = resolution.resolved
                resolved.source = "exact_slice_catalog"
                return resolved, resolution.candidates
        return None, []

    def _resolve_metadata_exact_slice(
        self,
        *,
        dataset_theme: ThemeSummaryResponse | None,
        representative_unit_id: int | None,
        requested_cube_id: str | None,
        requested_cube_query: str | None,
    ) -> ExactSliceResolution:
        if dataset_theme is None or representative_unit_id is None:
            return ExactSliceResolution()
        return self.exact_slice_service.resolve_exact_slice(
            unit_id=representative_unit_id,
            theme_id=dataset_theme.theme_id,
            cube_id=requested_cube_id,
            cube_query=requested_cube_query,
        )

    @staticmethod
    def _build_requested_time_scope(action: PlannerAction) -> TimeScope | None:
        if action.year is not None:
            return TimeScope(
                mode="snapshot",
                year=action.year,
                label=str(action.year),
                source="planner_action",
            )
        if action.start_year is not None or action.end_year is not None:
            return TimeScope(
                mode="range",
                start_year=action.start_year,
                end_year=action.end_year,
                label=f"{action.start_year or '?'}-{action.end_year or '?'}",
                source="planner_action",
            )
        return None

    @staticmethod
    def _dataset_family_candidates_from_state(
        state: ChatThreadState,
        dataset_theme: ThemeSummaryResponse | None,
    ) -> list[DatasetFamilyRef]:
        candidates: list[DatasetFamilyRef] = []
        if dataset_theme is not None:
            candidates.append(
                DatasetFamilyRef.from_theme(
                    dataset_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
            )
        elif state.selected_theme is not None:
            candidates.append(
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
            )
        return candidates

    @staticmethod
    def _availability_status_for_context(
        context: ResolvedAnalysisContext,
        output_feasibility_status: OutputFeasibilityStatus,
    ) -> AvailabilityStatus:
        if output_feasibility_status == OutputFeasibilityStatus.UNSUPPORTED:
            return AvailabilityStatus.UNAVAILABLE
        if context.output_mode == "boundary_map":
            return (
                AvailabilityStatus.AVAILABLE
                if context.reporting_geography is not None
                and (context.reporting_geography.unit_ids or context.requested_unit_type)
                else AvailabilityStatus.UNAVAILABLE
            )
        if context.dataset_family is None:
            return AvailabilityStatus.UNAVAILABLE
        if context.cube_candidates:
            return AvailabilityStatus.PARTIALLY_AVAILABLE
        return AvailabilityStatus.UNAVAILABLE

    @staticmethod
    def _comparability_status_from_assessment(
        tier: ComparisonCapabilityTier,
    ) -> ComparabilityStatus:
        if tier == ComparisonCapabilityTier.CURRENTLY_EXECUTABLE:
            return ComparabilityStatus.COMPARABLE
        if tier == ComparisonCapabilityTier.NOT_SUPPORTED:
            return ComparabilityStatus.NOT_COMPARABLE
        return ComparabilityStatus.NEEDS_CLARIFICATION

    def _apply_safe_defaults(
        self,
        context: ResolvedAnalysisContext,
        spec: AnalysisSpec | None,
        decision: PolicyDecision,
    ) -> AnalysisSpec | None:
        if spec is None or decision.outcome != PolicyOutcome.DEFAULT_WITH_DISCLOSURE:
            return spec
        updated = spec.model_copy(deep=True)
        if "default_time_scope" in decision.defaults_applied and updated.time_scope is None:
            cube = context.exact_slice
            if cube is not None and cube.cube_id:
                updated.time_scope = TimeScope(
                    mode="range",
                    start_year=next(
                        (candidate.start_year for candidate in context.cube_candidates if candidate.cube_id == cube.cube_id),
                        None,
                    ),
                    end_year=next(
                        (candidate.end_year for candidate in context.cube_candidates if candidate.cube_id == cube.cube_id),
                        None,
                    ),
                    source="policy_default",
                    label="full available range",
                )
        return updated

    def _analysis_blocked_outcome(
        self,
        *,
        action: PlannerAction,
        context: ResolvedAnalysisContext,
        spec: AnalysisSpec | None,
        decision: PolicyDecision,
        guard_result: ExecutionGuardResult,
    ) -> ExecutionOutcome:
        notices = list(dict.fromkeys(self._blocked_analysis_notices(decision, guard_result)))
        ui_delta = ChatUIStateDelta(
            operation=action.operation,
            cubes=list(context.cube_candidates),
            notices=notices,
        )
        discovery_themes = self._analysis_discovery_themes(context)
        if discovery_themes is not None:
            ui_delta.themes = discovery_themes
        return ExecutionOutcome(
            ui_delta=ui_delta,
            execution_summary={
                "operation": action.operation.value,
                "decision_outcome": decision.outcome.value,
                "guard_outcome": guard_result.outcome.value,
            },
            fallback_text=notices[0] if notices else "I stopped before running analysis.",
            workflow_path=decision.path,
            policy_decision=decision,
            execution_guard_result=guard_result,
            analysis_spec=spec,
            slice_candidates=list(context.exact_slice_candidates),
        )

    def _analysis_discovery_themes(
        self,
        context: ResolvedAnalysisContext,
    ):
        representative_unit_id = self.capability_registry.representative_unit_id(
            context.reporting_geography
        )
        if representative_unit_id is None:
            return None
        if context.dataset_family is not None:
            return None
        return self.themes_service.list_themes_for_unit(representative_unit_id)

    @staticmethod
    def _blocked_analysis_notices(
        decision: PolicyDecision,
        guard_result: ExecutionGuardResult,
    ) -> list[str]:
        if decision.outcome == PolicyOutcome.ASK and decision.clarification is not None:
            return [decision.clarification.question]
        if decision.outcome == PolicyOutcome.DISCOVERY:
            return ["I found relevant data, but I need one more detail before I can turn it into a result."]
        if decision.outcome == PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES:
            return ["That kind of result is not available safely here yet."]
        if guard_result.reason == "exact_slice_not_proven_executable":
            return ["I found the broader data family, but not the exact measure needed to run this result yet."]
        if guard_result.reason == "category_dimension_not_resolved":
            return ["I can see that a breakdown exists, but I still need the exact breakdown option before I can run it safely."]
        return [guard_result.reason.replace("_", " ")]

    @staticmethod
    def _default_disclosure_notice(
        context: ResolvedAnalysisContext,
        decision: PolicyDecision,
    ) -> str:
        if "boundary_map_without_measure" in decision.defaults_applied:
            return "Showing a boundary map only. A measured thematic map is not safely available here yet."
        if "default_time_scope" in decision.defaults_applied:
            return "I used the full period that can be compared safely because no time range was specified."
        return f"I applied a safe default for {context.output_mode}."

    @staticmethod
    def _analysis_alternatives(action: PlannerAction) -> list[str]:
        if action.operation == ChatOperation.FETCH_CATEGORY_BREAKDOWN:
            return [
                "Ask what data is available for this place and theme.",
                "Try a boundary map instead.",
            ]
        if action.operation == ChatOperation.FETCH_MAP_FEATURES:
            return [
                "Ask which reporting geographies are available here.",
                "Choose a specific geography first.",
            ]
        return [
            "Ask what themes are available for this place.",
            "Choose a more specific measure instead of a broad theme family.",
        ]

    def _refresh_semantic_state(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> None:
        state.conversation_state.transcript = list(state.messages)
        state.conversation_state.current_focus = self._focus_for_action(action.operation)
        state.runtime_state = (
            outcome.runtime_state.model_copy(deep=True)
            if outcome.runtime_state is not None
            else WorkflowRuntimeState()
        )
        state.policy_decision = outcome.policy_decision
        state.execution_guard_result = outcome.execution_guard_result
        state.latest_discovery_result = (
            outcome.discovery_result.model_copy(deep=True)
            if outcome.discovery_result is not None
            else None
        )

        pending_clarification = self._build_pending_clarification(action, outcome)
        state.pending_clarification = pending_clarification
        state.conversation_state.pending_clarification = pending_clarification

        resolution_state = self._build_resolution_state(state, action, outcome)
        state.resolution_state = resolution_state

        analysis_state = state.analysis_state.model_copy(deep=True)
        analysis_state.resolved_places = list(state.selected_places)
        if outcome.analysis_spec is not None:
            analysis_state.reporting_geography = outcome.analysis_spec.reporting_geography
            analysis_state.dataset_family = outcome.analysis_spec.dataset_family
            analysis_state.candidate_slice = outcome.analysis_spec.exact_slice
            analysis_state.time_scope = outcome.analysis_spec.time_scope
            analysis_state.output_mode = outcome.analysis_spec.output_mode
            analysis_state.availability_status = outcome.analysis_spec.availability_status
            analysis_state.comparability_status = outcome.analysis_spec.comparability_status
            analysis_state.analysis_spec = outcome.analysis_spec.model_copy(deep=True)
        else:
            analysis_state.reporting_geography = resolution_state.selected_reporting_geography
            analysis_state.dataset_family = (
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
                if state.selected_theme is not None
                else None
            )
            analysis_state.candidate_slice = self._current_slice(state, outcome)
            analysis_state.time_scope = self._build_time_scope(action, outcome)
            analysis_state.output_mode = self._effective_output_mode(action)
            analysis_state.availability_status = self._availability_status_for_outcome(
                action.operation,
                outcome,
            )
            analysis_state.comparability_status = self._comparability_status_for_state(state)
            analysis_state.analysis_spec = self._build_analysis_spec(state, action, outcome)
        analysis_state.defaults_used = self._build_defaults_used(state, action, outcome)
        state.analysis_state = analysis_state
        state.conversation_state.discourse_anchors = self._build_discourse_anchors(state)

        state.ui_projection = self._build_ui_projection(state, outcome)
        state.render_projection = self._build_render_projection(
            state,
            outcome,
            answer_text=state.render_projection.answer_text if state.render_projection else None,
        )
        self._sync_notice_state(state, outcome.ui_delta.notices)

    def _apply_runtime_state_override(
        self,
        *,
        state: ChatThreadState,
        runtime_state: WorkflowRuntimeState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> None:
        state.runtime_state = runtime_state.model_copy(deep=True)
        if state.ui_projection is not None:
            state.ui_projection.runtime_state = runtime_state.model_copy(deep=True)
        if state.current_receipt is None:
            return
        provenance_summary = self._build_provenance_summary(
            state=state,
            action=action,
            outcome=outcome,
            kind=state.current_receipt.kind,
            path=state.current_receipt.path,
            source_receipt_id=(
                state.current_receipt.provenance_summary.source_context.source_receipt_id
                if state.current_receipt.provenance_summary is not None
                else None
            ),
        )
        state.current_receipt.provenance_summary = provenance_summary
        if state.ui_projection is not None:
            state.ui_projection.provenance_summary = provenance_summary.model_copy(deep=True)
        if state.render_projection is not None:
            state.render_projection.provenance_summary = provenance_summary.model_copy(deep=True)
        state.current_receipt.ui_projection = (
            state.ui_projection.model_copy(deep=True) if state.ui_projection is not None else None
        )
        state.current_receipt.render_projection = (
            state.render_projection.model_copy(deep=True) if state.render_projection is not None else None
        )

    def _focus_for_action(self, operation: ChatOperation) -> str:
        focus_map = {
            ChatOperation.SEARCH_PLACES: "place_identity",
            ChatOperation.LOOKUP_POSTCODE: "place_identity",
            ChatOperation.RESOLVE_PLACE: "place_identity",
            ChatOperation.REMOVE_PLACE: "place_identity",
            ChatOperation.LIST_CATALOG_OVERVIEW: "discovery",
            ChatOperation.LIST_THEMES: "dataset_family",
            ChatOperation.LIST_REPORTING_GEOGRAPHIES: "reporting_geography",
            ChatOperation.LIST_FEASIBLE_OUTPUTS: "discovery",
            ChatOperation.LIST_AVAILABLE_YEARS: "time_scope",
            ChatOperation.LIST_EXACT_SLICE_OPTIONS: "dataset_slice",
            ChatOperation.LIST_COMPARABLE_OPTIONS: "comparability",
            ChatOperation.RESOLVE_THEME: "dataset_family",
            ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT: "dataset_slice",
            ChatOperation.FETCH_TIME_SERIES: "analysis",
            ChatOperation.FETCH_CATEGORY_BREAKDOWN: "analysis",
            ChatOperation.FETCH_MAP_FEATURES: "analysis",
            ChatOperation.FETCH_PLACE_PROFILE: "info_lookup",
            ChatOperation.FETCH_UNIT_TYPE_INFO: "info_lookup",
            ChatOperation.RESOLVE_DATA_ENTITY: "info_lookup",
            ChatOperation.FETCH_DATA_ENTITY_INFO: "info_lookup",
            ChatOperation.CLARIFY: "clarification",
            ChatOperation.REPLY_ONLY: "chat_only",
        }
        return focus_map.get(operation, "conversation")

    def _build_discourse_anchors(self, state: ChatThreadState) -> DiscourseAnchors:
        option_sets: list[DiscourseOptionSet] = []
        current_focus = state.conversation_state.current_focus

        if state.selected_places:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="selected_places",
                    title="Selected places",
                    kind=DiscourseOptionKind.SELECTED_PLACE,
                    source_receipt_id=state.current_receipt_id,
                    preferred=current_focus == "place_identity",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"selected_place:{place.place.place_id}",
                            label=place.place.name,
                            kind=DiscourseOptionKind.SELECTED_PLACE,
                            ordinal=index,
                            source_receipt_id=state.current_receipt_id,
                            metadata={
                                "place_id": place.place.place_id,
                                "unit_ids": list(place.place.unit_ids),
                                "unit_types": list(place.place.unit_types),
                            },
                        )
                        for index, place in enumerate(state.selected_places, start=1)
                    ],
                )
            )

        if state.pending_clarification is not None and state.pending_clarification.options:
            option_sets.append(
                DiscourseOptionSet(
                    set_id=f"clarification:{state.pending_clarification.slot or 'request'}",
                    title=state.pending_clarification.question,
                    kind=DiscourseOptionKind.CLARIFICATION_OPTION,
                    source_receipt_id=state.pending_clarification.source_receipt_id,
                    preferred=True,
                    options=[
                        DiscourseOptionRef(
                            option_id=option.option_id,
                            label=option.label,
                            kind=self._clarification_option_kind(option.kind),
                            ordinal=index,
                            source_receipt_id=state.pending_clarification.source_receipt_id,
                            metadata=dict(option.metadata),
                        )
                        for index, option in enumerate(
                            state.pending_clarification.options,
                            start=1,
                        )
                    ],
                )
            )

        option_sets.extend(self._discovery_option_sets(state, current_focus=current_focus))

        if state.recent_receipts:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="recent_receipts",
                    title="Recent results",
                    kind=DiscourseOptionKind.RECEIPT,
                    source_receipt_id=state.current_receipt_id,
                    preferred=current_focus in {"analysis", "discovery", "info_lookup"},
                    options=[
                        self._receipt_discourse_option(receipt, ordinal=index)
                        for index, receipt in enumerate(state.recent_receipts, start=1)
                    ],
                )
            )

        return DiscourseAnchors(
            active_receipt_id=state.current_receipt_id,
            recent_receipt_ids=[receipt.receipt_id for receipt in state.recent_receipts],
            selected_place_ids=[place.place.place_id for place in state.selected_places],
            current_output_mode=state.analysis_state.output_mode,
            current_time_scope=(
                state.analysis_state.time_scope.model_copy(deep=True)
                if state.analysis_state.time_scope is not None
                else None
            ),
            option_sets=option_sets,
        )

    @staticmethod
    def _clarification_option_kind(kind: str | None) -> DiscourseOptionKind:
        if kind == "place_candidate":
            return DiscourseOptionKind.PLACE_CANDIDATE
        if kind == "selected_place":
            return DiscourseOptionKind.SELECTED_PLACE
        return DiscourseOptionKind.CLARIFICATION_OPTION

    def _discovery_option_sets(
        self,
        state: ChatThreadState,
        *,
        current_focus: str | None,
    ) -> list[DiscourseOptionSet]:
        result = state.latest_discovery_result
        if result is None and state.current_receipt is not None:
            result = state.current_receipt.discovery_result
        if result is None:
            return []

        source_receipt_id = state.current_receipt_id
        if source_receipt_id is None and isinstance(result.anchors, dict):
            source_receipt_id = result.anchors.get("current_receipt_id")
        option_sets: list[DiscourseOptionSet] = []
        anchored_theme_id = (
            (result.dataset_families[0].theme_id if len(result.dataset_families) == 1 else None)
            or (state.selected_theme.theme_id if state.selected_theme is not None else None)
        )

        if result.reporting_geographies:
            preferred_operation = (
                result.anchors.get("preferred_geography_operation")
                if isinstance(result.anchors, dict)
                else None
            )
            option_sets.append(
                DiscourseOptionSet(
                    set_id="discovery_reporting_geographies",
                    title="Reporting geographies",
                    kind=DiscourseOptionKind.REPORTING_GEOGRAPHY,
                    source_receipt_id=source_receipt_id,
                    preferred=current_focus == "reporting_geography",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"reporting_geography:{item.unit_type or index}",
                            label=item.label or item.unit_type or f"Option {index}",
                            kind=DiscourseOptionKind.REPORTING_GEOGRAPHY,
                            ordinal=index,
                            source_receipt_id=source_receipt_id,
                            metadata={
                                "unit_type": item.unit_type,
                                "unit_ids": list(item.unit_ids),
                                "action_operation": preferred_operation,
                                "theme_id": anchored_theme_id,
                            },
                        )
                        for index, item in enumerate(result.reporting_geographies, start=1)
                    ],
                )
            )

        if result.dataset_families:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="discovery_themes",
                    title="Themes",
                    kind=DiscourseOptionKind.THEME,
                    source_receipt_id=source_receipt_id,
                    preferred=current_focus == "dataset_family",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"theme:{item.theme_id or index}",
                            label=item.label or item.theme_id or f"Theme {index}",
                            kind=DiscourseOptionKind.THEME,
                            ordinal=index,
                            source_receipt_id=source_receipt_id,
                            metadata={
                                "theme_id": item.theme_id,
                                "action_operation": ChatOperation.LIST_EXACT_SLICE_OPTIONS.value,
                            },
                        )
                        for index, item in enumerate(result.dataset_families, start=1)
                    ],
                )
            )

        if result.exact_slice_options:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="discovery_exact_slices",
                    title="Exact-slice options",
                    kind=DiscourseOptionKind.EXACT_SLICE,
                    source_receipt_id=source_receipt_id,
                    preferred=current_focus == "dataset_slice",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"exact_slice:{item.cube_id or index}",
                            label=item.label or item.cube_id or f"Exact slice {index}",
                            kind=DiscourseOptionKind.EXACT_SLICE,
                            ordinal=index,
                            source_receipt_id=source_receipt_id,
                            metadata={
                                "theme_id": anchored_theme_id,
                                "cube_id": item.cube_id,
                                "action_operation": ChatOperation.LIST_AVAILABLE_YEARS.value,
                                "exact_slice": item.model_dump(mode="json"),
                            },
                        )
                        for index, item in enumerate(result.exact_slice_options, start=1)
                    ],
                )
            )

        if result.available_years:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="discovery_years",
                    title="Available years",
                    kind=DiscourseOptionKind.TIME_OPTION,
                    source_receipt_id=source_receipt_id,
                    preferred=current_focus == "time_scope",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"time:{index}",
                            label=item.label,
                            kind=DiscourseOptionKind.TIME_OPTION,
                            ordinal=index,
                            source_receipt_id=source_receipt_id,
                            metadata={
                                "year": (
                                    int(item.end_year)
                                    if item.end_year is not None and item.start_year == item.end_year
                                    else None
                                ),
                                "start_year": item.start_year,
                                "end_year": item.end_year,
                            },
                        )
                        for index, item in enumerate(result.available_years, start=1)
                    ],
                )
            )

        if result.suggested_actions:
            option_sets.append(
                DiscourseOptionSet(
                    set_id="discovery_suggested_actions",
                    title="Suggested actions",
                    kind=DiscourseOptionKind.SUGGESTED_ACTION,
                    source_receipt_id=source_receipt_id,
                    preferred=current_focus == "discovery",
                    options=[
                        DiscourseOptionRef(
                            option_id=f"suggested_action:{index}",
                            label=item.label,
                            kind=DiscourseOptionKind.SUGGESTED_ACTION,
                            ordinal=index,
                            source_receipt_id=source_receipt_id,
                            metadata={
                                "action_operation": (
                                    item.operation.value if item.operation is not None else None
                                ),
                                "unit_type": item.unit_type,
                                "theme_id": item.theme_id,
                                "cube_id": item.cube_id,
                                "output_mode": item.output_mode,
                                **dict(item.metadata),
                            },
                        )
                        for index, item in enumerate(result.suggested_actions, start=1)
                    ],
                )
            )

        return option_sets

    def _receipt_discourse_option(
        self,
        receipt: AnalysisReceipt,
        *,
        ordinal: int,
    ) -> DiscourseOptionRef:
        resolved_context = receipt.provenance_summary.resolved_context if receipt.provenance_summary is not None else None
        label_parts = [
            (
                resolved_context.reporting_geography_label
                if resolved_context is not None
                else None
            ),
            (
                resolved_context.output_mode
                if resolved_context is not None
                else None
            ),
            receipt.kind.value,
        ]
        label = " ".join(str(part) for part in label_parts if part) or receipt.receipt_id
        return DiscourseOptionRef(
            option_id=f"receipt:{receipt.receipt_id}",
            label=label,
            kind=DiscourseOptionKind.RECEIPT,
            ordinal=ordinal,
            source_receipt_id=receipt.receipt_id,
            metadata={
                "receipt_id": receipt.receipt_id,
                "receipt_kind": receipt.kind.value,
                "workflow_path": receipt.path.value,
                "reporting_unit_type": (
                    resolved_context.reporting_unit_type if resolved_context is not None else None
                ),
                "reporting_geography_label": (
                    resolved_context.reporting_geography_label
                    if resolved_context is not None
                    else None
                ),
                "output_mode": (
                    resolved_context.output_mode if resolved_context is not None else None
                ),
            },
        )

    def _build_pending_clarification(
        self,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> ClarificationState | None:
        if outcome.policy_decision is not None and outcome.policy_decision.clarification is not None:
            return outcome.policy_decision.clarification.model_copy(deep=True)
        if outcome.ui_delta.place_search_results:
            options = [
                ClarificationOption(
                    option_id=str(item.place_id),
                    label=item.name,
                    kind="place_candidate",
                    metadata={
                        "place_id": item.place_id,
                        "unit_types": list(item.unit_types),
                    },
                )
                for item in outcome.ui_delta.place_search_results
            ]
            question = (
                outcome.ui_delta.notices[0]
                if outcome.ui_delta.notices
                else "Choose one of the matching places."
            )
            return ClarificationState(
                question=question,
                slot="place_identity",
                options=options,
            )

        if action.operation == ChatOperation.CLARIFY:
            question = action.assistant_task or "Could you clarify what you want me to look up?"
            return ClarificationState(question=question, slot="request")

        if not outcome.ui_delta.notices:
            return None

        notice = outcome.ui_delta.notices[0]
        lowered = notice.lower()
        clarification_markers = (
            "choose ",
            "tell me ",
            "could you ",
            "which ",
            "what ",
            "no places matched",
            "i need ",
        )
        if not any(marker in lowered for marker in clarification_markers):
            return None

        slot = "request"
        if "place first" in lowered or "place " in lowered:
            slot = "place_identity"
        elif "theme first" in lowered or "theme" in lowered:
            slot = "dataset_family"
        elif "unit type" in lowered:
            slot = "reporting_geography"
        elif "data entity" in lowered:
            slot = "dataset_family"

        return ClarificationState(question=notice, slot=slot)

    def _build_resolution_state(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> ResolutionState:
        reporting_candidates = self._build_reporting_geography_candidates(state)
        return ResolutionState(
            place_candidates=list(outcome.ui_delta.place_search_results),
            resolved_places=list(state.selected_places),
            reporting_geography_candidates=reporting_candidates,
            selected_reporting_geography=self._select_reporting_geography(
                state,
                action,
                reporting_candidates,
            ),
            dataset_family_candidates=self._build_dataset_family_candidates(state, outcome),
            slice_candidates=self._build_slice_candidates(state, outcome),
            time_candidates=self._build_time_candidates(action, outcome),
            ambiguity_flags={
                "place_identity": len(outcome.ui_delta.place_search_results) > 1,
                "reporting_geography": len(reporting_candidates) > 1,
                "dataset_family": (
                    len(outcome.ui_delta.themes.items) > 1
                    if outcome.ui_delta.themes is not None
                    else False
                ),
                "slice": len(outcome.ui_delta.cubes) > 1,
            },
        )

    def _build_reporting_geography_candidates(
        self,
        state: ChatThreadState,
    ) -> list[ReportingGeographyRef]:
        return self.capability_registry.enabled_reporting_geography_candidates(
            list(state.selected_places)
        )

    def _select_reporting_geography(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        candidates: list[ReportingGeographyRef],
    ) -> ReportingGeographyRef | None:
        return self._resolve_reporting_geography_for_analysis(state, action, candidates)

    def _build_dataset_family_candidates(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> list[DatasetFamilyRef]:
        candidates: list[DatasetFamilyRef] = []
        if outcome.ui_delta.themes is not None:
            candidates.extend(
                DatasetFamilyRef.from_theme(
                    item,
                    source="themes_listing",
                    slot_status=SlotStatus.CANDIDATE_SET,
                )
                for item in outcome.ui_delta.themes.items
            )
        if state.selected_theme is not None and not any(
            candidate.theme_id == state.selected_theme.theme_id for candidate in candidates
        ):
            candidates.insert(
                0,
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                ),
            )
        return candidates

    def _build_slice_candidates(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> list[ExactSliceCandidate]:
        candidates: list[ExactSliceCandidate] = []
        if outcome.slice_candidates:
            return [candidate.model_copy(deep=True) for candidate in outcome.slice_candidates]
        if outcome.ui_delta.cubes:
            candidates.extend(
                ExactSliceCandidate(
                    cube_id=cube.cube_id,
                    cube_ids=[cube.cube_id],
                    label=cube.label,
                    description=cube.description,
                    has_categories=cube.has_categories,
                    start_year=cube.start_year,
                    end_year=cube.end_year,
                    observation_count=cube.observation_count,
                    source="cube_listing",
                    slot_status=SlotStatus.CANDIDATE_SET,
                )
                for cube in outcome.ui_delta.cubes
            )
        if state.selected_cubes:
            for cube in state.selected_cubes:
                if any(candidate.cube_id == cube.cube_id for candidate in candidates):
                    continue
                candidates.insert(
                    0,
                    ExactSliceCandidate(
                        cube_id=cube.cube_id,
                        cube_ids=[cube.cube_id],
                        label=cube.label,
                        description=cube.description,
                        has_categories=cube.has_categories,
                        start_year=cube.start_year,
                        end_year=cube.end_year,
                        observation_count=cube.observation_count,
                        source="selected_cube",
                        slot_status=SlotStatus.DEFAULTABLE,
                    ),
                )
        return candidates

    def _build_time_candidates(
        self,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> list[TimeScope]:
        if outcome.ui_delta.category_breakdown is not None:
            return [
                TimeScope(
                    mode="snapshot",
                    year=outcome.ui_delta.category_breakdown.year,
                    label=str(outcome.ui_delta.category_breakdown.year),
                    source="category_breakdown",
                )
            ]

        if outcome.ui_delta.time_series is not None and outcome.ui_delta.time_series.rows:
            years = [row.year for row in outcome.ui_delta.time_series.rows]
            return [
                TimeScope(
                    mode="range",
                    start_year=min(years),
                    end_year=max(years),
                    label=f"{min(years)}-{max(years)}",
                    source="time_series",
                )
            ]

        if action.year is not None:
            return [
                TimeScope(
                    mode="snapshot",
                    year=action.year,
                    label=str(action.year),
                    source="planner_action",
                )
            ]

        if action.start_year is not None or action.end_year is not None:
            label = None
            if action.start_year is not None or action.end_year is not None:
                label = f"{action.start_year or '?'}-{action.end_year or '?'}"
            return [
                TimeScope(
                    mode="range",
                    start_year=action.start_year,
                    end_year=action.end_year,
                    label=label,
                    source="planner_action",
                )
            ]

        return []

    def _current_slice(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> ExactSliceRef | None:
        if state.analysis_state.candidate_slice is not None:
            return state.analysis_state.candidate_slice.model_copy(deep=True)
        candidates = self._build_slice_candidates(state, outcome)
        executable = [
            candidate
            for candidate in candidates
            if candidate.cellref or candidate.dataitem_id or candidate.cat_id
        ]
        if len(executable) == 1:
            return ExactSliceRef.from_candidate(
                executable[0],
                source=executable[0].source,
                slot_status=executable[0].slot_status,
            )
        return None

    def _build_time_scope(
        self,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> TimeScope | None:
        candidates = self._build_time_candidates(action, outcome)
        return candidates[0] if candidates else None

    def _output_mode_for_action(self, operation: ChatOperation | None) -> str | None:
        if operation is None:
            return None
        mapping = {
            ChatOperation.FETCH_TIME_SERIES: "trend_chart",
            ChatOperation.FETCH_CATEGORY_BREAKDOWN: "category_chart",
            ChatOperation.FETCH_MAP_FEATURES: "boundary_map",
            ChatOperation.FETCH_PLACE_PROFILE: "place_profile",
            ChatOperation.FETCH_UNIT_TYPE_INFO: "unit_type_info",
            ChatOperation.RESOLVE_DATA_ENTITY: "entity_info",
            ChatOperation.FETCH_DATA_ENTITY_INFO: "entity_info",
            ChatOperation.LIST_CATALOG_OVERVIEW: "discovery",
            ChatOperation.LIST_THEMES: "discovery",
            ChatOperation.LIST_REPORTING_GEOGRAPHIES: "discovery",
            ChatOperation.LIST_FEASIBLE_OUTPUTS: "discovery",
            ChatOperation.LIST_AVAILABLE_YEARS: "discovery",
            ChatOperation.LIST_EXACT_SLICE_OPTIONS: "discovery",
            ChatOperation.LIST_COMPARABLE_OPTIONS: "discovery",
            ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT: "discovery",
            ChatOperation.SEARCH_PLACES: "discovery",
            ChatOperation.RESOLVE_PLACE: "discovery",
            ChatOperation.RESOLVE_THEME: "discovery",
            ChatOperation.LOOKUP_POSTCODE: "discovery",
            ChatOperation.REMOVE_PLACE: "discovery",
            ChatOperation.REPLY_ONLY: "chat_only",
            ChatOperation.CLARIFY: "clarification",
        }
        return mapping.get(operation)

    def _effective_output_mode(self, action: PlannerAction) -> str | None:
        return action.output_mode or self._output_mode_for_action(action.operation)

    def _build_defaults_used(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> list[str]:
        if outcome.policy_decision is not None and outcome.policy_decision.defaults_applied:
            return list(outcome.policy_decision.defaults_applied)
        defaults_used: list[str] = []
        if action.operation == ChatOperation.FETCH_CATEGORY_BREAKDOWN and action.year is None:
            year = (
                outcome.ui_delta.category_breakdown.year
                if outcome.ui_delta.category_breakdown is not None
                else None
            )
            if year is not None:
                defaults_used.append(f"Defaulted category breakdown year to {year}.")
        return defaults_used

    def _availability_status_for_outcome(
        self,
        operation: ChatOperation,
        outcome: ExecutionOutcome,
    ) -> AvailabilityStatus | None:
        if operation == ChatOperation.FETCH_TIME_SERIES:
            if outcome.ui_delta.time_series is None:
                return AvailabilityStatus.UNAVAILABLE
            return (
                AvailabilityStatus.AVAILABLE
                if outcome.ui_delta.time_series.row_count > 0
                else AvailabilityStatus.UNAVAILABLE
            )
        if operation == ChatOperation.FETCH_CATEGORY_BREAKDOWN:
            if outcome.ui_delta.category_breakdown is None:
                return AvailabilityStatus.UNAVAILABLE
            return (
                AvailabilityStatus.AVAILABLE
                if outcome.ui_delta.category_breakdown.row_count > 0
                else AvailabilityStatus.UNAVAILABLE
            )
        if operation == ChatOperation.FETCH_MAP_FEATURES:
            if outcome.ui_delta.map_features is None:
                return AvailabilityStatus.UNAVAILABLE
            return (
                AvailabilityStatus.AVAILABLE
                if outcome.ui_delta.map_features.feature_count > 0
                else AvailabilityStatus.UNAVAILABLE
            )
        if operation in {
            ChatOperation.FETCH_PLACE_PROFILE,
            ChatOperation.FETCH_UNIT_TYPE_INFO,
            ChatOperation.RESOLVE_DATA_ENTITY,
            ChatOperation.FETCH_DATA_ENTITY_INFO,
        }:
            return AvailabilityStatus.AVAILABLE
        if operation in {
            ChatOperation.SEARCH_PLACES,
            ChatOperation.LIST_CATALOG_OVERVIEW,
            ChatOperation.LIST_THEMES,
            ChatOperation.LIST_REPORTING_GEOGRAPHIES,
            ChatOperation.LIST_FEASIBLE_OUTPUTS,
            ChatOperation.LIST_AVAILABLE_YEARS,
            ChatOperation.LIST_EXACT_SLICE_OPTIONS,
            ChatOperation.LIST_COMPARABLE_OPTIONS,
            ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
            ChatOperation.RESOLVE_PLACE,
            ChatOperation.RESOLVE_THEME,
            ChatOperation.REMOVE_PLACE,
            ChatOperation.LOOKUP_POSTCODE,
        }:
            return AvailabilityStatus.PARTIALLY_AVAILABLE
        return None

    def _comparability_status_for_state(
        self,
        state: ChatThreadState,
    ) -> ComparabilityStatus | None:
        if not state.selected_places:
            return None
        return (
            ComparabilityStatus.COMPARABLE
            if len(state.selected_places) <= 1
            else ComparabilityStatus.NEEDS_CLARIFICATION
        )

    def _build_analysis_spec(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ):
        if outcome.analysis_spec is not None:
            return outcome.analysis_spec.model_copy(deep=True)
        reporting_geography = state.resolution_state.selected_reporting_geography
        dataset_family = (
            DatasetFamilyRef.from_theme(
                state.selected_theme,
                source="selected_theme",
                slot_status=SlotStatus.RESOLVED,
            )
            if state.selected_theme is not None
            else None
        )
        exact_slice = self._current_slice(state, outcome)
        time_scope = self._build_time_scope(action, outcome)
        output_mode = self._effective_output_mode(action)

        if (
            not state.selected_places
            and reporting_geography is None
            and dataset_family is None
            and exact_slice is None
            and time_scope is None
            and output_mode is None
        ):
            return None

        availability_status = self._availability_status_for_outcome(action.operation, outcome)
        comparability_status = self._comparability_status_for_state(state)
        slice_status = SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION
        if exact_slice is not None:
            slice_status = (
                SliceStatus.UNIQUE_EXECUTABLE_SLICE
                if exact_slice.cellref or exact_slice.dataitem_id or exact_slice.cat_id
                else SliceStatus.FAMILY_ONLY
            )
        elif state.resolution_state.slice_candidates:
            slice_status = SliceStatus.CANDIDATE_SET

        output_feasibility_status = self.capability_registry.output_feasibility(
            output_mode=output_mode or "analysis",
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            wants_theme_context=dataset_family is not None,
            requested_full_layer_map=(
                action.operation == ChatOperation.FETCH_MAP_FEATURES
                and not state.selected_places
                and action.unit_type is not None
            ),
        )
        if action.operation == ChatOperation.REPLY_ONLY or output_mode is None:
            output_feasibility_status = OutputFeasibilityStatus.NEEDS_CLARIFICATION
        comparison_assessment = self.capability_registry.comparison_assessment(
            places=list(state.selected_places),
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            time_scope=time_scope,
        )

        return AnalysisSpec(
            places=[
                SemanticPlaceRef.from_resolved_place(place, source="selected_place")
                for place in state.selected_places
            ],
            reporting_geography=reporting_geography,
            dataset_family=dataset_family,
            exact_slice=exact_slice,
            time_scope=time_scope,
            output_mode=output_mode,
            comparison_mode="single_place" if len(state.selected_places) <= 1 else "multi_place",
            reporting_geography_status=(
                ReportingGeographyStatus.RESOLVED
                if reporting_geography is not None and reporting_geography.unit_ids
                else ReportingGeographyStatus.NEEDS_CLARIFICATION
            ),
            dataset_status=(
                DatasetStatus.RESOLVED
                if dataset_family is not None
                else DatasetStatus.DISCOVERY_ONLY
                if output_mode == "discovery"
                else DatasetStatus.NEEDS_CLARIFICATION
            ),
            slice_status=slice_status,
            comparability_status=(
                self._comparability_status_from_assessment(comparison_assessment.tier)
                if comparability_status is not None
                else ComparabilityStatus.NEEDS_CLARIFICATION
            ),
            availability_status=availability_status or AvailabilityStatus.PARTIALLY_AVAILABLE,
            output_feasibility_status=output_feasibility_status,
        )

    def _build_ui_projection(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> UIProjection:
        projection_id = (
            state.ui_projection.projection_id
            if state.ui_projection is not None and state.ui_projection.projection_id
            else f"ui_{uuid4().hex[:12]}"
        )
        reporting_geography = (
            state.analysis_state.reporting_geography
            or state.resolution_state.selected_reporting_geography
        )
        return UIProjection(
            projection_id=projection_id,
            current_receipt_id=state.current_receipt_id,
            current_receipt_kind=state.current_receipt.kind if state.current_receipt is not None else None,
            runtime_state=state.runtime_state.model_copy(deep=True),
            selected_places=list(state.selected_places),
            reporting_geography=reporting_geography,
            dataset_family=state.selected_theme,
            available_themes=self._projection_available_themes(state, outcome),
            available_cubes=self._projection_available_cubes(state, outcome),
            selected_cubes=list(state.selected_cubes),
            exact_slice=state.analysis_state.candidate_slice,
            analysis_spec=(
                state.analysis_state.analysis_spec.model_copy(deep=True)
                if state.analysis_state.analysis_spec is not None
                else None
            ),
            time_scope=state.analysis_state.time_scope,
            active_output_mode=state.analysis_state.output_mode,
            clarification=state.pending_clarification,
            discovery_result=(
                state.latest_discovery_result.model_copy(deep=True)
                if state.latest_discovery_result is not None
                else None
            ),
            feasible_next_actions=self._feasible_follow_ups(state),
            defaults_used=list(state.analysis_state.defaults_used),
            notices=list(state.notices),
            provenance_summary=(
                state.current_receipt.provenance_summary.model_copy(deep=True)
                if state.current_receipt is not None and state.current_receipt.provenance_summary is not None
                else (
                    state.ui_projection.provenance_summary.model_copy(deep=True)
                    if state.ui_projection is not None and state.ui_projection.provenance_summary is not None
                    else None
                )
            ),
        )

    def _build_render_projection(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
        *,
        answer_text: str | None,
    ) -> RenderProjection:
        projection_id = (
            state.render_projection.projection_id
            if state.render_projection is not None and state.render_projection.projection_id
            else f"rp_{uuid4().hex[:12]}"
        )
        return RenderProjection(
            projection_id=projection_id,
            receipt_id=state.current_receipt_id,
            active_output_mode=state.analysis_state.output_mode,
            answer_text=answer_text,
            chart=outcome.ui_delta.category_breakdown or outcome.ui_delta.time_series,
            table=outcome.table_payload,
            boundary_map=outcome.ui_delta.map_features,
            thematic_map_status=self._thematic_map_status(state, outcome),
            metadata_payload=self._render_metadata_payload(state, outcome),
            legend=self._render_legend(outcome),
            notices=list(state.notices),
            provenance_summary=(
                state.current_receipt.provenance_summary.model_copy(deep=True)
                if state.current_receipt is not None and state.current_receipt.provenance_summary is not None
                else (
                    state.render_projection.provenance_summary.model_copy(deep=True)
                    if state.render_projection is not None
                    and state.render_projection.provenance_summary is not None
                    else None
                )
            ),
        )

    def _build_provenance_summary(
        self,
        *,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
        kind: ReceiptKind,
        path: WorkflowPath,
        source_receipt_id: str | None,
    ) -> ProvenanceSummary:
        analysis_spec = (
            state.analysis_state.analysis_spec.model_copy(deep=True)
            if state.analysis_state.analysis_spec is not None
            else None
        )
        discovery_result = (
            outcome.discovery_result.model_copy(deep=True)
            if outcome.discovery_result is not None
            else (
                state.latest_discovery_result.model_copy(deep=True)
                if state.latest_discovery_result is not None
                else None
            )
        )
        entity = self._receipt_entity_payload(outcome)
        defaults_used = list(state.analysis_state.defaults_used)
        defaults_used.extend(
            str(item)
            for item in ((outcome.applied_patch.metadata if outcome.applied_patch is not None else {}).get("defaults_applied") or [])
            if item
        )
        defaults_used = list(dict.fromkeys(defaults_used))
        safe_downgrades = self._safe_downgrades_from_defaults(defaults_used)
        capability_notes = self._capability_notes_for_provenance(
            kind=kind,
            path=path,
            analysis_spec=analysis_spec,
            discovery_result=discovery_result,
            entity=entity,
            safe_downgrades=safe_downgrades,
            runtime_state=state.runtime_state,
        )
        return ProvenanceSummary(
            result_type=self._provenance_result_type(kind),
            receipt_kind=kind,
            workflow_path=path,
            summary=self._provenance_summary_text(
                kind=kind,
                analysis_spec=analysis_spec,
                discovery_result=discovery_result,
                entity=entity,
                runtime_state=state.runtime_state,
            ),
            discovery_only=(kind == ReceiptKind.DISCOVERY),
            executed_analysis=(kind == ReceiptKind.ANALYSIS),
            runtime_mode=state.runtime_state.mode,
            degraded_outcome=state.runtime_state.degraded_outcome,
            resolved_context=self._resolved_context_summary(
                state=state,
                analysis_spec=analysis_spec,
                entity=entity,
            ),
            exact_slice=self._exact_slice_provenance(
                analysis_spec=analysis_spec,
                state=state,
            ),
            defaults_used=defaults_used,
            heuristics_used=self._heuristics_used_for_provenance(
                analysis_spec=analysis_spec,
                outcome=outcome,
            ),
            safe_downgrades=safe_downgrades,
            availability_limits=self._availability_limits_for_provenance(
                kind=kind,
                analysis_spec=analysis_spec,
                discovery_result=discovery_result,
            ),
            comparability_limits=self._comparability_limits_for_provenance(
                kind=kind,
                analysis_spec=analysis_spec,
                discovery_result=discovery_result,
            ),
            capability_notes=capability_notes,
            source_context=ProvenanceSourceContext(
                source_operation=action.operation,
                source_receipt_id=source_receipt_id,
                follow_up_patch=(
                    outcome.applied_patch.patch_type
                    if outcome.applied_patch is not None
                    else None
                ),
                policy_outcome=(
                    outcome.policy_decision.outcome
                    if outcome.policy_decision is not None
                    else None
                ),
                guard_outcome=(
                    outcome.execution_guard_result.outcome
                    if outcome.execution_guard_result is not None
                    else None
                ),
                runtime_mode=state.runtime_state.mode,
                degraded_outcome=state.runtime_state.degraded_outcome,
            ),
        )

    @staticmethod
    def _provenance_result_type(kind: ReceiptKind) -> ProvenanceResultType:
        if kind == ReceiptKind.ANALYSIS:
            return ProvenanceResultType.EXECUTED_ANALYSIS
        if kind == ReceiptKind.INFO:
            return ProvenanceResultType.INFO_LOOKUP
        return ProvenanceResultType.CATALOG_DISCOVERY

    def _resolved_context_summary(
        self,
        *,
        state: ChatThreadState,
        analysis_spec: AnalysisSpec | None,
        entity: dict[str, Any],
    ) -> ProvenanceResolvedContext:
        reporting_geography = (
            analysis_spec.reporting_geography
            if analysis_spec is not None and analysis_spec.reporting_geography is not None
            else self._current_reporting_geography(state)
        )
        dataset_family = (
            analysis_spec.dataset_family
            if analysis_spec is not None and analysis_spec.dataset_family is not None
            else (
                DatasetFamilyRef.from_theme(
                    state.selected_theme,
                    source="selected_theme",
                    slot_status=SlotStatus.RESOLVED,
                )
                if state.selected_theme is not None
                else None
            )
        )
        exact_slice = (
            analysis_spec.exact_slice
            if analysis_spec is not None and analysis_spec.exact_slice is not None
            else state.analysis_state.candidate_slice
        )
        time_scope = (
            analysis_spec.time_scope
            if analysis_spec is not None and analysis_spec.time_scope is not None
            else state.analysis_state.time_scope
        )
        output_mode = (
            analysis_spec.output_mode
            if analysis_spec is not None and analysis_spec.output_mode is not None
            else state.analysis_state.output_mode
        )
        return ProvenanceResolvedContext(
            place_labels=[place.place.name for place in state.selected_places],
            reporting_geography_label=(
                reporting_geography.label or reporting_geography.unit_type
                if reporting_geography is not None
                else None
            ),
            reporting_unit_type=reporting_geography.unit_type if reporting_geography is not None else None,
            dataset_family_label=(
                dataset_family.label or dataset_family.theme_id if dataset_family is not None else entity.get("label")
            ),
            theme_id=dataset_family.theme_id if dataset_family is not None else None,
            exact_slice_label=exact_slice.label if exact_slice is not None else None,
            cube_id=exact_slice.cube_id if exact_slice is not None else None,
            time_scope_label=time_scope.label if time_scope is not None else None,
            output_mode=output_mode,
        )

    @staticmethod
    def _exact_slice_provenance(
        *,
        analysis_spec: AnalysisSpec | None,
        state: ChatThreadState,
    ) -> ExactSliceProvenance | None:
        exact_slice = (
            analysis_spec.exact_slice
            if analysis_spec is not None and analysis_spec.exact_slice is not None
            else state.analysis_state.candidate_slice
        )
        dataset_family = (
            analysis_spec.dataset_family
            if analysis_spec is not None and analysis_spec.dataset_family is not None
            else state.analysis_state.dataset_family
        )
        if exact_slice is None:
            return None
        return ExactSliceProvenance(
            theme_id=dataset_family.theme_id if dataset_family is not None else None,
            cube_id=exact_slice.cube_id,
            label=exact_slice.label,
            cellref=exact_slice.cellref,
            dataitem_id=exact_slice.dataitem_id,
            cat_id=exact_slice.cat_id,
            category_label=(
                exact_slice.category_entity.label
                if exact_slice.category_entity is not None
                else None
            ),
            metadata_provenance=exact_slice.metadata_provenance,
            resolution_basis=exact_slice.source,
        )

    def _provenance_summary_text(
        self,
        *,
        kind: ReceiptKind,
        analysis_spec: AnalysisSpec | None,
        discovery_result: DiscoveryResult | None,
        entity: dict[str, Any],
        runtime_state: WorkflowRuntimeState,
    ) -> str:
        if kind == ReceiptKind.ANALYSIS and analysis_spec is not None:
            place_labels = ", ".join(place.label for place in analysis_spec.places) or "the selected place"
            geography = (
                analysis_spec.reporting_geography.label
                or analysis_spec.reporting_geography.unit_type
                if analysis_spec.reporting_geography is not None
                else "the selected geography"
            )
            slice_label = (
                analysis_spec.exact_slice.label or analysis_spec.exact_slice.cube_id
                if analysis_spec.exact_slice is not None
                else analysis_spec.dataset_family.label if analysis_spec.dataset_family is not None else "the selected data"
            )
            output_mode = analysis_spec.output_mode or "analysis"
            summary = f"{output_mode.title()} result for {place_labels} using {geography} and {slice_label}."
            return self._append_runtime_mode_summary(summary, runtime_state)
        if kind == ReceiptKind.DISCOVERY and discovery_result is not None:
            topic = discovery_result.title or discovery_result.topic.value.replace("_", " ")
            summary = f"This is an exploration result about {topic}. It shows what is available, not a completed analysis."
            return self._append_runtime_mode_summary(summary, runtime_state)
        if kind == ReceiptKind.INFO and entity:
            label = entity.get("label") or entity.get("identifier") or "the selected entity"
            summary = f"Reference information for {label}."
            return self._append_runtime_mode_summary(summary, runtime_state)
        return self._append_runtime_mode_summary(
            "Result provenance is available from the current receipt and projections.",
            runtime_state,
        )

    @staticmethod
    def _append_runtime_mode_summary(
        summary: str,
        runtime_state: WorkflowRuntimeState,
    ) -> str:
        if runtime_state.mode != WorkflowRuntimeMode.DETERMINISTIC_DEGRADED:
            return summary
        suffix = runtime_state.notice or "Used deterministic degraded mode because the chat model was unavailable."
        if suffix in summary:
            return summary
        return f"{summary} {suffix}"

    @staticmethod
    def _safe_downgrades_from_defaults(defaults_used: list[str]) -> list[str]:
        downgrades: list[str] = []
        for item in defaults_used:
            lowered = item.lower()
            if "boundary_map_without_measure" in lowered or "boundary map" in lowered:
                downgrades.append("Used a boundary-map-only downgrade instead of a thematic map.")
            elif "default_time_scope" in lowered or "defaulted" in lowered:
                downgrades.append("Applied a safe time default instead of requiring a new clarification.")
        return list(dict.fromkeys(downgrades))

    @staticmethod
    def _heuristics_used_for_provenance(
        *,
        analysis_spec: AnalysisSpec | None,
        outcome: ExecutionOutcome,
    ) -> list[str]:
        heuristics: list[str] = []
        exact_slice = analysis_spec.exact_slice if analysis_spec is not None else None
        if exact_slice is not None and exact_slice.metadata_provenance is None and exact_slice.source:
            heuristics.append(f"Exact slice context was carried from {exact_slice.source}.")
        if outcome.applied_patch is not None and outcome.applied_patch.metadata.get("output_switch_reason"):
            heuristics.append(
                str(outcome.applied_patch.metadata["output_switch_reason"]).replace("_", " ")
            )
        return list(dict.fromkeys(heuristics))

    @staticmethod
    def _availability_limits_for_provenance(
        *,
        kind: ReceiptKind,
        analysis_spec: AnalysisSpec | None,
        discovery_result: DiscoveryResult | None,
    ) -> list[str]:
        if kind == ReceiptKind.DISCOVERY and discovery_result is not None:
            return list(dict.fromkeys(discovery_result.availability_notes))
        if analysis_spec is None:
            return []
        limits: list[str] = []
        if analysis_spec.output_mode == "boundary_map" and analysis_spec.dataset_family is not None:
            limits.append("Boundary maps in the current workflow remain measure-free rather than thematic.")
        if analysis_spec.availability_status != AvailabilityStatus.AVAILABLE:
            limits.append("Availability is only partially proven for the current context.")
        return limits

    @staticmethod
    def _comparability_limits_for_provenance(
        *,
        kind: ReceiptKind,
        analysis_spec: AnalysisSpec | None,
        discovery_result: DiscoveryResult | None,
    ) -> list[str]:
        if kind == ReceiptKind.DISCOVERY and discovery_result is not None:
            return list(dict.fromkeys(discovery_result.comparability_notes))
        if analysis_spec is None:
            return []
        if analysis_spec.comparability_status == ComparabilityStatus.NOT_COMPARABLE:
            return ["Comparability is not proven for this result."]
        if analysis_spec.comparability_status == ComparabilityStatus.NEEDS_CLARIFICATION:
            return ["Comparability still needs clarification before safe comparison can run."]
        return []

    def _capability_notes_for_provenance(
        self,
        *,
        kind: ReceiptKind,
        path: WorkflowPath,
        analysis_spec: AnalysisSpec | None,
        discovery_result: DiscoveryResult | None,
        entity: dict[str, Any],
        safe_downgrades: list[str],
        runtime_state: WorkflowRuntimeState,
    ) -> list[str]:
        notes: list[str] = []
        if kind == ReceiptKind.DISCOVERY:
            notes.append("This receipt describes catalog or policy knowledge, not executed analysis.")
            if discovery_result is not None and any(
                item.support_level is not None and item.support_level.value == "discovery_only"
                for item in discovery_result.supported_outputs
            ):
                notes.append("Some listed options remain discovery-only and are still blocked from execution.")
            if discovery_result is not None and discovery_result.exact_slice_options:
                notes.append("Exact-slice options here are candidates until one validated slice is chosen.")
        elif kind == ReceiptKind.ANALYSIS:
            notes.append("This receipt is anchored to one validated analysis spec.")
            if analysis_spec is not None and analysis_spec.exact_slice is not None:
                notes.append("The executed result used one explicit exact slice rather than a family-only match.")
        elif kind == ReceiptKind.INFO and entity:
            notes.append("This receipt is an info lookup and does not imply executable analysis.")

        if path == WorkflowPath.FOLLOW_UP_EDIT:
            notes.append("This receipt was produced from a receipt-backed follow-up edit.")
        if runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED:
            notes.append(
                "This result was produced through deterministic degraded-mode routing because the chat model was unavailable."
            )
        notes.extend(safe_downgrades)
        return list(dict.fromkeys(notes))

    def _projection_available_themes(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> list[ThemeSummaryResponse]:
        if outcome.ui_delta.themes is not None:
            return [item.model_copy(deep=True) for item in outcome.ui_delta.themes.items]
        if state.ui_projection is not None and state.ui_projection.available_themes:
            return [item.model_copy(deep=True) for item in state.ui_projection.available_themes]
        reporting_geography = (
            state.analysis_state.reporting_geography
            or state.resolution_state.selected_reporting_geography
        )
        representative_unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if representative_unit_id is not None:
            return list(self.themes_service.list_themes_for_unit(representative_unit_id).items)
        return [state.selected_theme.model_copy(deep=True)] if state.selected_theme is not None else []

    def _projection_available_cubes(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> list:
        if outcome.ui_delta.cubes:
            return [item.model_copy(deep=True) for item in outcome.ui_delta.cubes]
        if state.ui_projection is not None and state.ui_projection.available_cubes:
            return [item.model_copy(deep=True) for item in state.ui_projection.available_cubes]
        reporting_geography = (
            state.analysis_state.reporting_geography
            or state.resolution_state.selected_reporting_geography
        )
        representative_unit_id = self.capability_registry.representative_unit_id(reporting_geography)
        if representative_unit_id is not None and state.selected_theme is not None:
            return list(
                self.themes_service.list_cubes_for_unit_theme(
                    representative_unit_id,
                    state.selected_theme.theme_id,
                ).items
            )
        return [cube.model_copy(deep=True) for cube in state.selected_cubes]

    def _render_metadata_payload(
        self,
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> dict[str, Any]:
        payload = (
            dict(state.render_projection.metadata_payload)
            if state.render_projection is not None and state.render_projection.metadata_payload is not None
            else {}
        )
        if outcome.ui_delta.place_profile is not None:
            payload["place_profile"] = outcome.ui_delta.place_profile.model_dump(mode="json")
        elif len(state.selected_places) == 1:
            place_profile = self.metadata_service.get_place_profile(state.selected_places[0].place.place_id)
            if place_profile is not None:
                payload["place_profile"] = place_profile.model_dump(mode="json")

        reporting_geography = (
            state.analysis_state.reporting_geography
            or state.resolution_state.selected_reporting_geography
        )
        if reporting_geography is not None and reporting_geography.unit_type:
            if outcome.ui_delta.unit_type_info is not None:
                payload["unit_type_info"] = outcome.ui_delta.unit_type_info.model_dump(mode="json")
            else:
                unit_type_info = self.metadata_service.get_unit_type_info(reporting_geography.unit_type)
                if unit_type_info is not None:
                    payload["unit_type_info"] = unit_type_info.model_dump(mode="json")
            if len(reporting_geography.unit_ids) == 1:
                key_findings = self.metadata_service.get_place_key_findings(reporting_geography.unit_ids[0])
                payload["place_key_findings"] = key_findings.model_dump(mode="json")

        if outcome.ui_delta.data_entity_resolution is not None:
            payload["data_entity_resolution"] = outcome.ui_delta.data_entity_resolution.model_dump(mode="json")
        if outcome.ui_delta.data_entity_info is not None:
            payload["data_entity_info"] = outcome.ui_delta.data_entity_info.model_dump(mode="json")
        return payload

    @staticmethod
    def _render_legend(outcome: ExecutionOutcome) -> dict[str, Any]:
        if outcome.ui_delta.time_series is not None:
            return {
                "series_labels": sorted(
                    {
                        row.unit_name or row.cube_label or row.cube_id
                        for row in outcome.ui_delta.time_series.rows
                    }
                )
            }
        if outcome.ui_delta.category_breakdown is not None:
            return {
                "series_labels": sorted(
                    {
                        row.category_label or row.category_group or row.cube_id
                        for row in outcome.ui_delta.category_breakdown.rows
                    }
                )
            }
        if outcome.ui_delta.map_features is not None:
            return {
                "unit_type": outcome.ui_delta.map_features.unit_type,
                "feature_count": outcome.ui_delta.map_features.feature_count,
            }
        return {}

    @staticmethod
    def _thematic_map_status(
        state: ChatThreadState,
        outcome: ExecutionOutcome,
    ) -> str | None:
        if outcome.ui_delta.map_features is not None and state.selected_theme is not None:
            return "unsupported_in_v1_boundary_map_only"
        if state.selected_theme is not None:
            return "not_requested"
        return None

    def _feasible_follow_ups(self, state: ChatThreadState) -> list[str]:
        actions: list[str] = []
        active_spec = (
            state.current_receipt.analysis_spec
            if state.current_receipt is not None and state.current_receipt.analysis_spec is not None
            else state.analysis_state.analysis_spec
        )
        active_output = (
            active_spec.output_mode
            if active_spec is not None
            else state.analysis_state.output_mode
        )
        if active_spec is not None and active_output in {"trend_chart", "snapshot_chart"}:
            actions.extend(["show_table", "show_boundary_map", "set_time"])
        elif active_spec is not None and active_output == "table":
            actions.extend(["show_boundary_map", "set_time"])
        if state.selected_places:
            actions.extend(["replace_place", "add_place", "remove_place"])
        if state.selected_theme is not None:
            actions.append("change_theme")
        return actions

    def _persist_receipt_if_needed(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> AnalysisReceipt | None:
        if action.operation in {ChatOperation.REPLY_ONLY, ChatOperation.CLARIFY}:
            return None
        if outcome.workflow_path == WorkflowPath.CHAT_ONLY:
            return None
        if outcome.policy_decision is not None and outcome.policy_decision.outcome in {
            PolicyOutcome.ASK,
            PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES,
        }:
            return None
        receipt = self._build_receipt(state, action, outcome)
        return self.thread_store.save_receipt(state.thread_id, receipt)

    def _build_receipt(
        self,
        state: ChatThreadState,
        action: PlannerAction,
        outcome: ExecutionOutcome,
    ) -> AnalysisReceipt:
        path = outcome.workflow_path or self._path_for_operation(action.operation)
        kind = self._receipt_kind_for_path(path)
        receipt_id = f"{self._receipt_prefix(kind)}_{uuid4().hex[:12]}"
        source_receipt_id = (
            outcome.applied_patch.source_receipt_id
            if outcome.applied_patch is not None and outcome.applied_patch.source_receipt_id is not None
            else state.current_receipt_id
        )

        ui_projection = state.ui_projection.model_copy(deep=True) if state.ui_projection else None
        if ui_projection is not None and ui_projection.projection_id is None:
            ui_projection.projection_id = f"ui_{uuid4().hex[:12]}"
        render_projection = (
            state.render_projection.model_copy(deep=True) if state.render_projection else None
        )
        if render_projection is not None:
            if render_projection.projection_id is None:
                render_projection.projection_id = f"rp_{uuid4().hex[:12]}"
            render_projection.receipt_id = receipt_id

        provenance_summary = self._build_provenance_summary(
            state=state,
            action=action,
            outcome=outcome,
            kind=kind,
            path=path,
            source_receipt_id=source_receipt_id,
        )
        if ui_projection is not None:
            ui_projection.provenance_summary = provenance_summary.model_copy(deep=True)
        if render_projection is not None:
            render_projection.provenance_summary = provenance_summary.model_copy(deep=True)

        if ui_projection is not None:
            state.ui_projection = ui_projection
        if render_projection is not None:
            state.render_projection = render_projection

        return AnalysisReceipt(
            kind=kind,
            receipt_id=receipt_id,
            path=path,
            source_operation=action.operation,
            analysis_spec=(
                state.analysis_state.analysis_spec.model_copy(deep=True)
                if kind == ReceiptKind.ANALYSIS and state.analysis_state.analysis_spec is not None
                else None
            ),
            discovery_result=(
                outcome.discovery_result.model_copy(deep=True)
                if outcome.discovery_result is not None
                else None
            ),
            anchors={
                "places": [
                    SemanticPlaceRef.from_resolved_place(place, source="selected_place")
                    for place in state.selected_places
                ],
                "reporting_geography": (
                    state.analysis_state.reporting_geography.model_dump(mode="json")
                    if state.analysis_state.reporting_geography is not None
                    else None
                ),
            },
            catalog_summary={
                "place_candidates": [
                    item.model_dump(mode="json") for item in outcome.ui_delta.place_search_results
                ],
                "themes": [
                    item.model_dump(mode="json")
                    for item in (
                        outcome.ui_delta.themes.items if outcome.ui_delta.themes is not None else []
                    )
                ],
                "cubes": [item.model_dump(mode="json") for item in outcome.ui_delta.cubes],
                "discovery_items": (
                    [
                        item.model_dump(mode="json")
                        for item in outcome.discovery_result.items
                    ]
                    if outcome.discovery_result is not None
                    else []
                ),
            },
            entity=self._receipt_entity_payload(outcome),
            defaults_used=list(state.analysis_state.defaults_used),
            notices=list(state.notices),
            provenance_summary=provenance_summary,
            projections=ProjectionReferences(
                ui_projection_id=ui_projection.projection_id if ui_projection is not None else None,
                render_projection_id=(
                    render_projection.projection_id if render_projection is not None else None
                ),
            ),
            ui_projection=ui_projection,
            render_projection=render_projection,
            execution_summary=dict(outcome.execution_summary),
            created_at=utc_now(),
        )

    def _receipt_entity_payload(self, outcome: ExecutionOutcome) -> dict[str, Any]:
        if outcome.ui_delta.place_profile is not None:
            return {
                "type": "place_profile",
                "place_id": outcome.ui_delta.place_profile.place_id,
                "label": outcome.ui_delta.place_profile.name,
            }
        if outcome.ui_delta.unit_type_info is not None:
            return {
                "type": "unit_type",
                "identifier": outcome.ui_delta.unit_type_info.identifier,
                "label": outcome.ui_delta.unit_type_info.label,
            }
        if outcome.ui_delta.data_entity_info is not None:
            return {
                "type": "data_entity",
                "identifier": outcome.ui_delta.data_entity_info.entity_id,
                "label": outcome.ui_delta.data_entity_info.name,
            }
        return {}

    def _path_for_operation(self, operation: ChatOperation) -> WorkflowPath:
        if operation in {
            ChatOperation.FETCH_TIME_SERIES,
            ChatOperation.FETCH_CATEGORY_BREAKDOWN,
            ChatOperation.FETCH_MAP_FEATURES,
        }:
            return WorkflowPath.ANALYSIS
        if operation in {
            ChatOperation.FETCH_PLACE_PROFILE,
            ChatOperation.FETCH_UNIT_TYPE_INFO,
            ChatOperation.RESOLVE_DATA_ENTITY,
            ChatOperation.FETCH_DATA_ENTITY_INFO,
        }:
            return WorkflowPath.INFO_LOOKUP
        if operation in {ChatOperation.REPLY_ONLY, ChatOperation.CLARIFY}:
            return WorkflowPath.CHAT_ONLY
        return WorkflowPath.DISCOVERY

    def _receipt_kind_for_path(self, path: WorkflowPath) -> ReceiptKind:
        if path == WorkflowPath.ANALYSIS:
            return ReceiptKind.ANALYSIS
        if path == WorkflowPath.INFO_LOOKUP:
            return ReceiptKind.INFO
        return ReceiptKind.DISCOVERY

    def _receipt_prefix(self, kind: ReceiptKind) -> str:
        if kind == ReceiptKind.ANALYSIS:
            return "ar"
        if kind == ReceiptKind.INFO:
            return "ir"
        return "dr"

    @staticmethod
    def _current_reporting_geography(state: ChatThreadState) -> ReportingGeographyRef | None:
        reporting_geography = (
            state.analysis_state.reporting_geography
            or state.resolution_state.selected_reporting_geography
        )
        return reporting_geography.model_copy(deep=True) if reporting_geography is not None else None

    @staticmethod
    def _same_reporting_geography(
        first: ReportingGeographyRef | None,
        second: ReportingGeographyRef | None,
    ) -> bool:
        if first is None or second is None:
            return False
        return (
            first.unit_type == second.unit_type
            and list(first.unit_ids) == list(second.unit_ids)
        )

    def _observed_reporting_geography_status(
        self,
        *,
        initial_reporting_geography: ReportingGeographyRef | None,
        final_reporting_geography: ReportingGeographyRef | None,
        decision: PolicyDecision | None,
        analysis_spec: AnalysisSpec | None,
    ) -> str:
        if (
            final_reporting_geography is None
            or analysis_spec is None
            or analysis_spec.reporting_geography_status != ReportingGeographyStatus.RESOLVED
        ):
            return "blocked"
        if (
            decision is not None
            and decision.outcome == PolicyOutcome.DEFAULT_WITH_DISCLOSURE
            and any("reporting_geography" in item for item in decision.defaults_applied)
        ):
            return "defaulted"
        if self._same_reporting_geography(initial_reporting_geography, final_reporting_geography):
            return "reused"
        return "clarified"

    def _log_workflow_event(
        self,
        *,
        thread_id: str,
        turn_id: str,
        action: PlannerAction,
        outcome: ExecutionOutcome | None,
        state: ChatThreadState,
        initial_reporting_geography: ReportingGeographyRef | None,
        error: str | None = None,
    ) -> None:
        analysis_spec = (
            outcome.analysis_spec
            if outcome is not None and outcome.analysis_spec is not None
            else state.analysis_state.analysis_spec
        )
        decision = (
            outcome.policy_decision
            if outcome is not None and outcome.policy_decision is not None
            else state.policy_decision
        )
        guard = (
            outcome.execution_guard_result
            if outcome is not None and outcome.execution_guard_result is not None
            else state.execution_guard_result
        )
        workflow_path = (
            outcome.workflow_path
            if outcome is not None and outcome.workflow_path is not None
            else self._path_for_operation(action.operation)
        )
        final_reporting_geography = self._current_reporting_geography(state)
        workflow_event = {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "operation": action.operation.value,
            "classified_path": workflow_path.value if workflow_path is not None else None,
            "runtime_mode": state.runtime_state.mode.value,
            "degraded_outcome": (
                state.runtime_state.degraded_outcome.value
                if state.runtime_state.degraded_outcome is not None
                else None
            ),
            "degraded_reason": state.runtime_state.degraded_reason,
            "decision_outcome": decision.outcome.value if decision is not None else None,
            "guard_outcome": guard.outcome.value if guard is not None else None,
            "reporting_geography_status": self._observed_reporting_geography_status(
                initial_reporting_geography=initial_reporting_geography,
                final_reporting_geography=final_reporting_geography,
                decision=decision,
                analysis_spec=analysis_spec,
            ),
            "slice_status": analysis_spec.slice_status.value if analysis_spec is not None else None,
            "receipt_id": state.current_receipt_id,
            "decision_reason": decision.reason if decision is not None else None,
            "guard_reason": guard.reason if guard is not None else None,
            "error": error,
        }
        logger.info("workflow.turn.completed", extra={"workflow_event": workflow_event})

    def _attach_semantic_delta(
        self,
        state: ChatThreadState,
        delta: ChatUIStateDelta,
    ) -> ChatUIStateDelta:
        delta.selected_places = list(state.selected_places)
        delta.selected_theme = state.selected_theme
        delta.selected_cubes = list(state.selected_cubes)
        delta.conversation_state = state.conversation_state.model_copy(deep=True)
        delta.resolution_state = state.resolution_state.model_copy(deep=True)
        delta.analysis_state = state.analysis_state.model_copy(deep=True)
        delta.discovery_result = (
            state.latest_discovery_result.model_copy(deep=True)
            if state.latest_discovery_result is not None
            else None
        )
        delta.runtime_state = state.runtime_state.model_copy(deep=True)
        delta.policy_decision = (
            state.policy_decision.model_copy(deep=True) if state.policy_decision is not None else None
        )
        delta.execution_guard_result = (
            state.execution_guard_result.model_copy(deep=True)
            if state.execution_guard_result is not None
            else None
        )
        delta.current_receipt_id = state.current_receipt_id
        delta.current_receipt = (
            state.current_receipt.model_copy(deep=True) if state.current_receipt is not None else None
        )
        delta.ui_projection = (
            state.ui_projection.model_copy(deep=True) if state.ui_projection is not None else None
        )
        delta.render_projection = (
            state.render_projection.model_copy(deep=True)
            if state.render_projection is not None
            else None
        )
        delta.pending_clarification = (
            state.pending_clarification.model_copy(deep=True)
            if state.pending_clarification is not None
            else None
        )
        if state.notices:
            merged_notices = list(dict.fromkeys([*delta.notices, *state.notices]))
            delta.notices = merged_notices
        return delta

    def _sync_notice_state(self, state: ChatThreadState, notices: list[str]) -> None:
        state.notices = list(dict.fromkeys(notices))
        if state.ui_projection is not None:
            state.ui_projection.notices = list(state.notices)
        if state.current_receipt is not None:
            state.current_receipt.notices = list(state.notices)

    def _finalize_semantic_state_after_assistant(
        self,
        state: ChatThreadState,
        *,
        assistant_text: str,
        ui_delta: ChatUIStateDelta,
    ) -> None:
        state.conversation_state.transcript = list(state.messages)
        state.render_projection = self._build_render_projection(
            state,
            ExecutionOutcome(
                ui_delta=ui_delta,
                execution_summary={},
                fallback_text=assistant_text,
                table_payload=(
                    state.render_projection.table
                    if state.render_projection is not None
                    else None
                ),
            ),
            answer_text=assistant_text,
        )
        if state.current_receipt is not None:
            if state.ui_projection is not None and state.current_receipt.provenance_summary is not None:
                state.ui_projection.provenance_summary = (
                    state.current_receipt.provenance_summary.model_copy(deep=True)
                )
            state.current_receipt.render_projection = state.render_projection.model_copy(deep=True)
            state.current_receipt.ui_projection = (
                state.ui_projection.model_copy(deep=True) if state.ui_projection is not None else None
            )
            state.current_receipt.defaults_used = list(state.analysis_state.defaults_used)
            state.current_receipt.notices = list(state.notices)

    async def _generate_assistant_text(
        self,
        *,
        state: ChatThreadState,
        planner_result: PlannerResult,
        execution_summary: dict[str, Any],
        fallback_text: str,
        thread_id: str,
        turn_id: str,
        publish_events: bool,
        runtime_state: WorkflowRuntimeState,
    ) -> AssistantGenerationResult:
        if runtime_state.mode == WorkflowRuntimeMode.DETERMINISTIC_DEGRADED:
            if publish_events:
                await self.thread_store.publish_event(
                    thread_id,
                    ChatSSEEventName.ASSISTANT_DELTA,
                    AssistantDeltaEventData(
                        turn_id=turn_id,
                        delta=fallback_text,
                        accumulated_text=fallback_text,
                    ),
                )
            return AssistantGenerationResult(
                text=fallback_text,
                runtime_state=runtime_state.model_copy(deep=True),
            )
        messages = build_assistant_messages(state, planner_result, execution_summary)
        if publish_events:
            accumulated = ""
            try:
                async for delta in self.llm_client.stream_text(messages):
                    accumulated += delta
                    await self.thread_store.publish_event(
                        thread_id,
                        ChatSSEEventName.ASSISTANT_DELTA,
                        AssistantDeltaEventData(
                            turn_id=turn_id,
                            delta=delta,
                            accumulated_text=accumulated,
                        ),
                    )
                return AssistantGenerationResult(text=accumulated.strip() or fallback_text)
            except LLMClientError as exc:
                if accumulated.strip():
                    return AssistantGenerationResult(
                        text=accumulated.strip(),
                        notice=self._llm_unavailable_notice(exc),
                        error=str(exc),
                    )
                await self.thread_store.publish_event(
                    thread_id,
                    ChatSSEEventName.ASSISTANT_DELTA,
                    AssistantDeltaEventData(
                        turn_id=turn_id,
                        delta=fallback_text,
                        accumulated_text=fallback_text,
                    ),
                )
                return AssistantGenerationResult(
                    text=self._fallback_assistant_text(planner_result, fallback_text, exc),
                    notice=self._llm_unavailable_notice(exc),
                    error=str(exc),
                    runtime_state=self._degraded_runtime_state_from_failure(
                        exc=exc,
                        planner_result=planner_result,
                        execution_summary=execution_summary,
                    ),
                )
            except Exception:
                if accumulated.strip():
                    return AssistantGenerationResult(text=accumulated.strip())
                await self.thread_store.publish_event(
                    thread_id,
                    ChatSSEEventName.ASSISTANT_DELTA,
                    AssistantDeltaEventData(
                        turn_id=turn_id,
                        delta=fallback_text,
                        accumulated_text=fallback_text,
                    ),
                )
                return AssistantGenerationResult(text=fallback_text)

        try:
            text = await self.llm_client.complete_text(messages)
            return AssistantGenerationResult(text=text.strip() or fallback_text)
        except LLMClientError as exc:
            return AssistantGenerationResult(
                text=self._fallback_assistant_text(planner_result, fallback_text, exc),
                notice=self._llm_unavailable_notice(exc),
                error=str(exc),
                runtime_state=self._degraded_runtime_state_from_failure(
                    exc=exc,
                    planner_result=planner_result,
                    execution_summary=execution_summary,
                ),
            )
        except Exception:
            return AssistantGenerationResult(text=fallback_text)

    @staticmethod
    def _llm_unavailable_notice(exc: Exception) -> str:
        if isinstance(exc, LLMConfigurationError):
            return (
                "The chat model is not configured. "
                "Run `vobchat setup-llm` or set LLM_OPENAI_BASE_URL and LLM_MODEL."
            )
        if isinstance(exc, LLMServiceUnavailableError):
            return (
                "The configured chat model endpoint is unavailable. "
                "Run `vobchat doctor llm` or `vobchat setup-llm` and try again."
            )
        return f"LLM unavailable. {llm_setup_guidance()}"

    def _degraded_runtime_state_from_failure(
        self,
        *,
        exc: Exception,
        planner_result: PlannerResult,
        execution_summary: dict[str, Any],
    ) -> WorkflowRuntimeState:
        degraded_reason = self._degraded_reason(exc)
        degraded_outcome = execution_summary.get("degraded_outcome")
        if isinstance(degraded_outcome, str):
            degraded_outcome_value = DegradedModeOutcome(degraded_outcome)
        else:
            degraded_outcome_value = None
        if degraded_outcome_value is None:
            if planner_result.action.operation == ChatOperation.CLARIFY:
                degraded_outcome_value = DegradedModeOutcome.GUIDED_CLARIFICATION
            elif planner_result.action.operation == ChatOperation.REPLY_ONLY:
                degraded_outcome_value = (
                    DegradedModeOutcome.SETUP_GUIDANCE
                    if degraded_reason == "llm_not_configured"
                    else DegradedModeOutcome.UNSUPPORTED_IN_DEGRADED_MODE
                )
            else:
                degraded_outcome_value = DegradedModeOutcome.DETERMINISTIC_SUCCESS
        notice = (
            "Handled in deterministic degraded mode because the chat model is unavailable."
            if degraded_outcome_value == DegradedModeOutcome.DETERMINISTIC_SUCCESS
            else self._llm_unavailable_notice(exc)
        )
        return WorkflowRuntimeState(
            mode=WorkflowRuntimeMode.DETERMINISTIC_DEGRADED,
            degraded_reason=degraded_reason,
            degraded_outcome=degraded_outcome_value,
            detail=str(exc),
            notice=notice,
            setup_guidance=(
                ["Run `vobchat setup-llm` to configure a chat model."]
                if degraded_reason == "llm_not_configured"
                else ["Run `vobchat doctor llm` or `vobchat setup-llm` to restore full chat interpretation."]
            ),
        )

    @staticmethod
    def _fallback_assistant_text(
        planner_result: PlannerResult,
        fallback_text: str,
        exc: Exception,
    ) -> str:
        if planner_result.action.operation in {ChatOperation.REPLY_ONLY, ChatOperation.CLARIFY}:
            return ChatOrchestrator._llm_unavailable_notice(exc)
        return fallback_text

    @staticmethod
    def _merge_places(
        existing: list,
        additions: list,
    ) -> list:
        by_id = {place.place.place_id: place for place in existing}
        for place in additions:
            by_id[place.place.place_id] = place
        return list(by_id.values())

    def _ensure_place_context(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> list:
        if action.place_query:
            resolved = self._resolve_place_from_query(action.place_query)
            if resolved is not None:
                state.selected_places = self._merge_places(state.selected_places, [resolved])
        return state.selected_places

    def _build_place_candidate_outcome(
        self,
        *,
        operation: ChatOperation,
        query: str | None,
        fallback_notice: str,
        fallback_text: str,
    ) -> ExecutionOutcome | None:
        if not query:
            return None
        response = self._search_places_for_query(query, match_mode="exact", limit=10)
        if response.result_count == 0:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=operation,
                    notices=[f"No places matched '{query}'."],
                ),
                execution_summary={
                    "operation": operation.value,
                    "query": query,
                    "result_count": 0,
                },
                fallback_text=f"I couldn't find a place matching '{query}'.",
            )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=operation,
                place_search_results=list(response.results),
                notices=[fallback_notice],
            ),
            execution_summary={
                "operation": operation.value,
                "query": query,
                "result_count": response.result_count,
                "results": [item.model_dump(mode="json") for item in response.results[:5]],
            },
            fallback_text=(
                f"I found {response.result_count} places matching '{query}'. "
                "Please choose one of the results."
            ),
        )

    def _ensure_single_place(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ):
        places = self._ensure_place_context(state, action)
        return places[0] if places else None

    def _resolve_place_from_query(self, query: str):
        response = self._search_places_for_query(query, match_mode="exact", limit=5)
        if response.result_count != 1:
            return None
        return self.places_service.resolve_place(
            response.results[0].place_id,
            PlaceResolveQuery(),
        )

    def _search_places_for_query(
        self,
        query: str,
        *,
        match_mode: str,
        limit: int,
    ):
        initial = self.places_service.search_places(
            PlaceSearchQuery(query=query, match_mode=match_mode, limit=limit)
        )
        if initial.result_count or match_mode == "fuzzy":
            return initial
        return self.places_service.search_places(
            PlaceSearchQuery(query=query, match_mode="fuzzy", limit=limit)
        )

    def _ensure_theme(self, state: ChatThreadState, action: PlannerAction):
        if action.theme_query or action.theme_id:
            query = action.theme_query or action.theme_id or ""
            response = self.themes_service.resolve_theme(ThemeResolveQuery(query=query))
            return response.result if response else None
        if state.analysis_state.dataset_family is not None and state.selected_theme is None:
            dataset_family = state.analysis_state.dataset_family
            if dataset_family.theme_id and dataset_family.label:
                return ThemeSummaryResponse(
                    theme_id=dataset_family.theme_id,
                    label=dataset_family.label,
                    description=dataset_family.description,
                )
        return state.selected_theme

    @staticmethod
    def _selected_unit_ids(state: ChatThreadState) -> list[int]:
        if (
            state.analysis_state.reporting_geography is not None
            and state.analysis_state.reporting_geography.unit_ids
        ):
            return list(state.analysis_state.reporting_geography.unit_ids)
        if (
            state.resolution_state.selected_reporting_geography is not None
            and state.resolution_state.selected_reporting_geography.unit_ids
        ):
            return list(state.resolution_state.selected_reporting_geography.unit_ids)
        return []

    @staticmethod
    def _selected_unit_type(state: ChatThreadState) -> str | None:
        if state.analysis_state.reporting_geography is not None:
            return state.analysis_state.reporting_geography.unit_type
        if state.resolution_state.selected_reporting_geography is not None:
            return state.resolution_state.selected_reporting_geography.unit_type
        return None


@lru_cache(maxsize=1)
def get_chat_orchestrator() -> ChatOrchestrator:
    return ChatOrchestrator()
