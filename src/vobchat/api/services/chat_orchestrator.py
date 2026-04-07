from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from uuid import uuid4

from vobchat.api.schemas.chat import (
    AssistantCompletedEventData,
    AssistantDeltaEventData,
    ChatMessage,
    ChatOperation,
    ChatSSEEventName,
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatTurnAcceptedResponse,
    ChatTurnRequest,
    ChatTurnResponse,
    ChatUIStateDelta,
    ErrorEventData,
    PlannerAction,
    PlannerResult,
    ThreadCreatedEventData,
    TurnCompletedEventData,
    TurnStartedEventData,
    UIDeltaEventData,
)
from vobchat.api.schemas.maps import MapFeaturesByIdsQuery, MapFeaturesQuery
from vobchat.api.schemas.metadata import DataEntityResolveQuery
from vobchat.api.schemas.places import PlaceResolveQuery, PlaceSearchQuery, PostcodeLookupQuery
from vobchat.api.schemas.series import CategoryBreakdownRequest, TimeSeriesRequest
from vobchat.api.schemas.themes import ThemeListQuery, ThemeResolveQuery
from vobchat.api.services.chat_threads import InMemoryChatThreadStore, get_chat_thread_store, utc_now
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


@dataclass
class ExecutionOutcome:
    ui_delta: ChatUIStateDelta
    execution_summary: dict[str, Any]
    fallback_text: str


@dataclass
class AssistantGenerationResult:
    text: str
    notice: str | None = None
    error: str | None = None


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
    ) -> None:
        self.thread_store = thread_store or get_chat_thread_store()
        self.planner = planner or get_chat_planner()
        self.llm_client = llm_client or get_llm_client()
        self.places_service = places_service or get_places_service()
        self.themes_service = themes_service or get_themes_service()
        self.series_service = series_service or get_series_service()
        self.maps_service = maps_service or get_maps_service()
        self.metadata_service = metadata_service or get_metadata_service()

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
            planner_result = await self.planner.plan(state, request.message)
            outcome = await self._execute_action(state, planner_result.action)

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
            )
            if assistant_result.notice and assistant_result.notice not in outcome.ui_delta.notices:
                outcome.ui_delta.notices.append(assistant_result.notice)
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
            return response

    async def _execute_action(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        handlers = {
            ChatOperation.SEARCH_PLACES: self._handle_search_places,
            ChatOperation.LOOKUP_POSTCODE: self._handle_lookup_postcode,
            ChatOperation.REMOVE_PLACE: self._handle_remove_place,
            ChatOperation.LIST_THEMES: self._handle_list_themes,
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

    async def _handle_list_themes(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        self._ensure_place_context(state, action)
        unit_ids = self._selected_unit_ids(state)
        if unit_ids:
            response = self.themes_service.list_themes_for_unit(unit_ids[0])
        else:
            response = self.themes_service.list_themes(ThemeListQuery(limit=20))
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.LIST_THEMES,
                themes=response,
            ),
            execution_summary={
                "operation": action.operation.value,
                "item_count": response.item_count,
                "themes": [item.model_dump(mode="json") for item in response.items[:10]],
            },
            fallback_text=(
                ("Available themes for the selected place include " if unit_ids else "Available themes include ")
                + ", ".join(item.label for item in response.items[:5])
                + "."
            ),
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
        if state.selected_places:
            cubes = self._list_cubes_for_first_selected_place(state)
            delta.cubes = cubes
        return ExecutionOutcome(
            ui_delta=delta,
            execution_summary={
                "operation": action.operation.value,
                "theme": response.result.model_dump(mode="json"),
                "cube_count": len(delta.cubes),
            },
            fallback_text=f"I set the theme to {response.result.label}.",
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
            )
        unit_ids = self._selected_unit_ids(state)
        if not unit_ids:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
                    notices=["Choose a place first so I know which unit to use."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a selected place before I can list cubes.",
            )
        cubes = self.themes_service.list_cubes_for_unit_theme(unit_ids[0], resolved_theme.theme_id)
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
        )

    async def _handle_time_series(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        place_context = self._ensure_place_context(state, action)
        if place_context is None:
            candidate_outcome = self._build_place_candidate_outcome(
                operation=ChatOperation.FETCH_TIME_SERIES,
                query=action.place_query,
                fallback_notice="Choose a place first so I know what to chart.",
                fallback_text="I need a place before I can fetch a time series.",
            )
            if candidate_outcome is not None:
                return candidate_outcome
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    notices=["Choose a place first so I know what to chart."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a place before I can fetch a time series.",
            )
        resolved_theme = self._ensure_theme(state, action)
        if resolved_theme is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    notices=["Choose a theme first so I know what to chart."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a theme before I can fetch a time series.",
            )
        state.selected_theme = resolved_theme
        cubes = self._list_cubes_for_first_selected_place(state)
        chosen_cube = self._pick_cube(cubes, action)
        if chosen_cube is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    cubes=cubes,
                    notices=["No suitable cube was available for that theme and place."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I couldn't find a cube to use for that theme and place.",
            )
        state.selected_cubes = [chosen_cube]
        series = self.series_service.get_time_series(
            TimeSeriesRequest(
                unit_ids=self._selected_unit_ids(state),
                cube_ids=[chosen_cube.cube_id],
                start_year=action.start_year,
                end_year=action.end_year,
            )
        )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_TIME_SERIES,
                cubes=cubes,
                time_series=series,
            ),
            execution_summary={
                "operation": action.operation.value,
                "theme": resolved_theme.label,
                "cube": chosen_cube.label,
                "row_count": series.row_count,
                "unit_ids": series.unit_ids,
            },
            fallback_text=(
                f"I fetched a {resolved_theme.label.lower()} time series using {chosen_cube.label}."
            ),
        )

    async def _handle_category_breakdown(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        place_context = self._ensure_place_context(state, action)
        if place_context is None:
            candidate_outcome = self._build_place_candidate_outcome(
                operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                query=action.place_query,
                fallback_notice="Choose a place first so I know what to break down.",
                fallback_text="I need a place before I can fetch a category breakdown.",
            )
            if candidate_outcome is not None:
                return candidate_outcome
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                    notices=["Choose a place first so I know what to break down."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a place before I can fetch a category breakdown.",
            )
        resolved_theme = self._ensure_theme(state, action)
        if resolved_theme is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                    notices=["Choose a theme first so I know which dataset to use."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I need a theme before I can fetch a category breakdown.",
            )
        state.selected_theme = resolved_theme
        cubes = self._list_cubes_for_first_selected_place(state)
        chosen_cube = next((cube for cube in cubes if cube.has_categories), None)
        if chosen_cube is None:
            return ExecutionOutcome(
                ui_delta=ChatUIStateDelta(
                    operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                    cubes=cubes,
                    notices=["No category-capable cube was available for that theme and place."],
                ),
                execution_summary={"operation": action.operation.value},
                fallback_text="I couldn't find a category-capable cube for that theme and place.",
            )
        state.selected_cubes = [chosen_cube]
        year = action.year
        if year is None:
            year = int(chosen_cube.end_year or chosen_cube.start_year or utc_now().year)
        categories = self.series_service.get_category_breakdown(
            CategoryBreakdownRequest(
                unit_ids=self._selected_unit_ids(state),
                cube_ids=[chosen_cube.cube_id],
                year=year,
            )
        )
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                cubes=cubes,
                category_breakdown=categories,
            ),
            execution_summary={
                "operation": action.operation.value,
                "theme": resolved_theme.label,
                "cube": chosen_cube.label,
                "row_count": categories.row_count,
                "year": categories.year,
            },
            fallback_text=(
                f"I fetched a category breakdown for {resolved_theme.label.lower()} in {categories.year}."
            ),
        )

    async def _handle_map_features(
        self,
        state: ChatThreadState,
        action: PlannerAction,
    ) -> ExecutionOutcome:
        self._ensure_place_context(state, action)
        if state.selected_places:
            unit_ids = self._selected_unit_ids(state)
            unit_type = action.unit_type or self._selected_unit_type(state) or "MOD_DIST"
            features = self.maps_service.get_features_by_ids(
                MapFeaturesByIdsQuery(
                    unit_type=unit_type,
                    ids=unit_ids,
                    theme_id=state.selected_theme.theme_id if state.selected_theme else action.theme_id,
                )
            )
        else:
            candidate_outcome = self._build_place_candidate_outcome(
                operation=ChatOperation.FETCH_MAP_FEATURES,
                query=action.place_query,
                fallback_notice="Choose a place or unit type first so I know what to map.",
                fallback_text="I need a place or unit type before I can fetch map features.",
            )
            if candidate_outcome is not None:
                return candidate_outcome
            unit_type = action.unit_type or "MOD_DIST"
            features = self.maps_service.get_features(MapFeaturesQuery(unit_type=unit_type))
        return ExecutionOutcome(
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_MAP_FEATURES,
                map_features=features,
            ),
            execution_summary={
                "operation": action.operation.value,
                "feature_count": features.feature_count,
                "unit_type": features.unit_type,
            },
            fallback_text=(
                f"I fetched {features.feature_count} map feature"
                f"{'' if features.feature_count == 1 else 's'}."
            ),
        )

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
    ) -> AssistantGenerationResult:
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
        return state.selected_theme

    def _list_cubes_for_first_selected_place(self, state: ChatThreadState):
        unit_ids = self._selected_unit_ids(state)
        theme = state.selected_theme
        if not unit_ids or theme is None:
            return []
        response = self.themes_service.list_cubes_for_unit_theme(unit_ids[0], theme.theme_id)
        return list(response.items)

    @staticmethod
    def _pick_cube(cubes: list, action: PlannerAction):
        if not cubes:
            return None
        if action.cube_id:
            for cube in cubes:
                if cube.cube_id == action.cube_id:
                    return cube
        if action.cube_ids:
            for cube_id in action.cube_ids:
                for cube in cubes:
                    if cube.cube_id == cube_id:
                        return cube
        if action.cube_query:
            lowered = action.cube_query.lower()
            for cube in cubes:
                if lowered in cube.label.lower():
                    return cube
        return cubes[0]

    @staticmethod
    def _selected_unit_ids(state: ChatThreadState) -> list[int]:
        unit_ids: list[int] = []
        for place in state.selected_places:
            if place.units:
                unit_id = place.units[0].unit_id
            elif place.place.unit_ids:
                unit_id = place.place.unit_ids[0]
            else:
                continue
            if unit_id not in unit_ids:
                unit_ids.append(unit_id)
        return unit_ids

    @staticmethod
    def _selected_unit_type(state: ChatThreadState) -> str | None:
        for place in state.selected_places:
            if place.units:
                return place.units[0].unit_type
            if place.place.unit_types:
                return place.place.unit_types[0]
        return None


@lru_cache(maxsize=1)
def get_chat_orchestrator() -> ChatOrchestrator:
    return ChatOrchestrator()
