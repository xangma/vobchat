from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import Field, field_validator

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
    LIST_THEMES = "list_themes"
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


class ChatMessage(APIModel):
    message_id: str
    role: Literal["system", "user", "assistant"]
    content: str
    created_at: datetime


class PlannerAction(APIModel):
    operation: ChatOperation
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    match_mode: Literal["exact", "fuzzy"] | None = None
    place_query: str | None = None
    place_id: int | None = None
    postcode: str | None = None
    theme_query: str | None = None
    theme_id: str | None = None
    cube_query: str | None = None
    cube_id: str | None = None
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
    notices: list[str] = Field(default_factory=list)
    cleared_places: bool = False


class ChatThreadState(APIModel):
    thread_id: str
    messages: list[ChatMessage] = Field(default_factory=list)
    selected_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    selected_theme: ThemeSummaryResponse | None = None
    selected_cubes: list[CubeSummaryResponse] = Field(default_factory=list)
    latest_ui_delta: ChatUIStateDelta | None = None
    created_at: datetime
    updated_at: datetime


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
