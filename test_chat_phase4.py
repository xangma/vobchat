from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient as FastAPITestClient

from vobchat.api.main import app as api_app
from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatSSEEventName,
    ChatThreadState,
    ChatTurnRequest,
    PlannerAction,
    PlannerResult,
)
from vobchat.api.schemas.maps import MapFeatureCollectionResponse, MapFeatureResponse
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityReferenceResponse,
    DataEntityResolutionResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.schemas.places import (
    PlaceCandidateResponse,
    PlaceSearchResponse,
    PostcodeLookupResponse,
    PostcodeLookupResultResponse,
    ResolvedPlaceResponse,
    UnitDetailResponse,
)
from vobchat.api.schemas.series import (
    CategoryBreakdownResponse,
    CategoryRowResponse,
    TimeSeriesResponse,
    TimeSeriesRowResponse,
)
from vobchat.api.schemas.themes import (
    CubeListResponse,
    CubeSummaryResponse,
    ThemeListResponse,
    ThemeResolutionResponse,
    ThemeSummaryResponse,
)
from vobchat.api.services.chat_orchestrator import ChatOrchestrator, get_chat_orchestrator
from vobchat.api.services.chat_threads import InMemoryChatThreadStore, get_chat_thread_store
from vobchat.core.llm.planner import ChatPlanner


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_resolved_place(
    *,
    place_id: int = 1,
    name: str = "York",
    unit_id: int = 101,
    unit_type: str = "MOD_DIST",
) -> ResolvedPlaceResponse:
    return ResolvedPlaceResponse(
        place=PlaceCandidateResponse(
            place_id=place_id,
            name=name,
            unit_ids=[unit_id],
            unit_types=[unit_type],
            match_type="resolved",
        ),
        units=[
            UnitDetailResponse(
                unit_id=unit_id,
                unit_name=name,
                unit_type=unit_type,
                unit_type_label="Modern District",
            )
        ],
    )


class FailingPlannerClient:
    async def complete_json(self, messages, response_model, *, temperature=0.0):
        raise RuntimeError("planner unavailable")


class FakeLLMClient:
    def __init__(self, *, text: str = "Handled.", stream_chunks: list[str] | None = None) -> None:
        self.text = text
        self.stream_chunks = stream_chunks or ["Handled."]

    async def complete_text(self, messages, *, temperature=None, max_tokens=None) -> str:
        return self.text

    async def stream_text(self, messages, *, temperature=None, max_tokens=None):
        for chunk in self.stream_chunks:
            yield chunk



class FakePlanner:
    async def plan(self, state: ChatThreadState, user_message: str) -> PlannerResult:
        lowered = user_message.lower()
        if "york" in lowered:
            return PlannerResult(
                action=PlannerAction(
                    operation=ChatOperation.SEARCH_PLACES,
                    place_query="York",
                    match_mode="exact",
                    confidence=0.9,
                ),
                source="fallback",
            )
        if "population" in lowered:
            return PlannerResult(
                action=PlannerAction(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    theme_query="Population",
                    confidence=0.9,
                ),
                source="fallback",
            )
        return PlannerResult(
            action=PlannerAction(
                operation=ChatOperation.REPLY_ONLY,
                confidence=0.8,
                assistant_task="Reply politely.",
            ),
            source="fallback",
        )


class FakePlacesService:
    def search_places(self, query) -> PlaceSearchResponse:
        if query.query.lower() == "york":
            return PlaceSearchResponse(
                query=query.query,
                match_mode=query.match_mode,
                result_count=1,
                results=[
                    PlaceCandidateResponse(
                        place_id=1,
                        name="York",
                        county_name="Yorkshire",
                        latitude=53.96,
                        longitude=-1.08,
                        unit_ids=[101],
                        unit_types=["MOD_DIST"],
                        match_type=query.match_mode,
                    )
                ],
            )
        return PlaceSearchResponse(
            query=query.query,
            match_mode=query.match_mode,
            result_count=0,
            results=[],
        )

    def resolve_place(self, place_id, query) -> ResolvedPlaceResponse | None:
        if place_id == 1:
            return make_resolved_place()
        return None

    def lookup_postcode(self, query) -> PostcodeLookupResponse:
        return PostcodeLookupResponse(
            postcode=query.postcode,
            unit_type=query.unit_type,
            result_count=1,
            results=[
                PostcodeLookupResultResponse(
                    postcode=query.postcode,
                    unit_id=101,
                    place_id=1,
                    unit_name="York",
                    unit_type=query.unit_type,
                )
            ],
        )


class FakeThemesService:
    def list_themes(self, query) -> ThemeListResponse:
        return ThemeListResponse(
            item_count=1,
            items=[ThemeSummaryResponse(theme_id="T_POP", label="Population")],
        )

    def resolve_theme(self, query) -> ThemeResolutionResponse | None:
        return ThemeResolutionResponse(
            query=query.query,
            result=ThemeSummaryResponse(theme_id="T_POP", label="Population"),
        )

    def list_themes_for_unit(self, unit_id):
        return self.list_themes(type("Query", (), {"limit": None})())

    def list_cubes_for_unit_theme(self, unit_id, theme_id) -> CubeListResponse:
        return CubeListResponse(
            unit_id=unit_id,
            theme_id=theme_id,
            item_count=2,
            items=[
                CubeSummaryResponse(
                    theme_id=theme_id,
                    cube_id="N_POP_TOTAL",
                    label="Total population",
                    start_year=1801,
                    end_year=1901,
                    observation_count=5,
                    has_categories=False,
                ),
                CubeSummaryResponse(
                    theme_id=theme_id,
                    cube_id="N_POP_WAY",
                    label="Population by category",
                    start_year=1801,
                    end_year=1901,
                    observation_count=5,
                    has_categories=True,
                ),
            ],
        )


class FakeSeriesService:
    def get_time_series(self, request) -> TimeSeriesResponse:
        return TimeSeriesResponse(
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            row_count=1,
            rows=[
                TimeSeriesRowResponse(
                    year=1901,
                    unit_id=request.unit_ids[0],
                    unit_name="York",
                    unit_type="MOD_DIST",
                    cube_id=request.cube_ids[0],
                    cube_label="Total population",
                    value=42.0,
                )
            ],
        )

    def get_category_breakdown(self, request) -> CategoryBreakdownResponse:
        return CategoryBreakdownResponse(
            year=request.year,
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            row_count=1,
            rows=[
                CategoryRowResponse(
                    year=request.year,
                    unit_id=request.unit_ids[0],
                    unit_name="York",
                    cube_id=request.cube_ids[0],
                    category_group="sex",
                    category_label="Female",
                    value=21.0,
                )
            ],
        )


class FakeMapsService:
    def get_features(self, query) -> MapFeatureCollectionResponse:
        return MapFeatureCollectionResponse(
            mode="all",
            unit_type=query.unit_type,
            feature_count=1,
            features=[
                MapFeatureResponse(
                    unit_id=101,
                    unit_name="York",
                    unit_type=query.unit_type,
                    geometry={"type": "Polygon", "coordinates": []},
                )
            ],
        )

    def get_features_by_ids(self, query) -> MapFeatureCollectionResponse:
        return MapFeatureCollectionResponse(
            mode="ids",
            unit_type=query.unit_type,
            feature_count=len(query.ids),
            requested_ids=query.ids,
            features=[
                MapFeatureResponse(
                    unit_id=unit_id,
                    unit_name=f"Unit {unit_id}",
                    unit_type=query.unit_type,
                    geometry={"type": "Polygon", "coordinates": []},
                )
                for unit_id in query.ids
            ],
        )


class FakeMetadataService:
    def get_place_profile(self, place_id) -> PlaceProfileResponse | None:
        return PlaceProfileResponse(place_id=place_id, name="York", text="Historic city")

    def get_place_key_findings(self, unit_id):
        raise AssertionError("Not used in Phase 4 tests")

    def get_unit_type_info(self, unit_type) -> UnitTypeInfoResponse | None:
        return UnitTypeInfoResponse(identifier=unit_type, label="Modern District", unit_count=5)

    def resolve_data_entity(self, query) -> DataEntityResolutionResponse | None:
        return DataEntityResolutionResponse(
            query=query.query,
            result=DataEntityReferenceResponse(
                entity_id="N_POP_TOTAL",
                label="Total population",
                entity_type="N",
            ),
        )

    def get_data_entity_info(self, entity_id) -> DataEntityInfoResponse | None:
        return DataEntityInfoResponse(
            entity_id=entity_id,
            entity_type="N",
            name="Total population",
            type_name="Measure",
        )


def build_orchestrator(
    *,
    store: InMemoryChatThreadStore | None = None,
    planner: Any | None = None,
    llm_client: Any | None = None,
) -> ChatOrchestrator:
    return ChatOrchestrator(
        thread_store=store or InMemoryChatThreadStore(),
        planner=planner or FakePlanner(),
        llm_client=llm_client or FakeLLMClient(),
        places_service=FakePlacesService(),
        themes_service=FakeThemesService(),
        series_service=FakeSeriesService(),
        maps_service=FakeMapsService(),
        metadata_service=FakeMetadataService(),
    )


def test_chat_planner_fallback_detects_greeting_and_postcode() -> None:
    planner = ChatPlanner(llm_client=FailingPlannerClient())
    empty_state = ChatThreadState(
        thread_id="thread-1",
        created_at=utc_now(),
        updated_at=utc_now(),
    )

    greeting = asyncio.run(planner.plan(empty_state, "Hello"))
    postcode = asyncio.run(planner.plan(empty_state, "Use postcode YO1 7EP"))

    assert greeting.action.operation == ChatOperation.REPLY_ONLY
    assert postcode.action.operation == ChatOperation.LOOKUP_POSTCODE
    assert postcode.action.postcode == "YO17EP"


def test_chat_planner_fallback_detects_time_series_with_selected_context() -> None:
    planner = ChatPlanner(llm_client=FailingPlannerClient())
    state = ChatThreadState(
        thread_id="thread-2",
        created_at=utc_now(),
        updated_at=utc_now(),
        selected_places=[make_resolved_place()],
        selected_theme=ThemeSummaryResponse(theme_id="T_POP", label="Population"),
    )

    result = asyncio.run(planner.plan(state, "Show population over time"))

    assert result.action.operation == ChatOperation.FETCH_TIME_SERIES
    assert result.action.theme_query == "Population"


def test_chat_orchestrator_updates_state_for_place_search() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="I added York."))
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="Add York", stream=False)
        )
    )

    assert response.assistant_message.content == "I added York."
    assert response.ui_delta.operation == ChatOperation.SEARCH_PLACES
    assert len(response.thread_state.selected_places) == 1
    assert response.thread_state.selected_places[0].place.name == "York"


def test_chat_orchestrator_stream_turn_publishes_events() -> None:
    store = InMemoryChatThreadStore()
    thread = store.create_thread()
    queue = store.subscribe(thread.thread_id)
    orchestrator = build_orchestrator(
        store=store,
        llm_client=FakeLLMClient(stream_chunks=["Hel", "lo"]),
    )

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="Hello", stream=True),
            emit_events=True,
            turn_id="turn-1",
        )
    )

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    assert response.assistant_message.content == "Hello"
    assert [event.event for event in events] == [
        ChatSSEEventName.TURN_STARTED,
        ChatSSEEventName.UI_DELTA,
        ChatSSEEventName.ASSISTANT_DELTA,
        ChatSSEEventName.ASSISTANT_DELTA,
        ChatSSEEventName.ASSISTANT_COMPLETED,
        ChatSSEEventName.TURN_COMPLETED,
    ]
    assert events[2].data.accumulated_text == "Hel"
    assert events[3].data.accumulated_text == "Hello"


@pytest.fixture()
def chat_client():
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled.", stream_chunks=["Han", "dled."]))
    api_app.dependency_overrides[get_chat_orchestrator] = lambda: orchestrator
    api_app.dependency_overrides[get_chat_thread_store] = lambda: store
    with FastAPITestClient(api_app) as client:
        yield client, store
    api_app.dependency_overrides.clear()


def test_chat_router_create_get_and_turn(chat_client) -> None:
    client, _store = chat_client
    created = client.post("/chat/threads")
    assert created.status_code == 201
    thread_id = created.json()["thread_id"]

    fetched = client.get(f"/chat/threads/{thread_id}")
    assert fetched.status_code == 200
    assert fetched.json()["thread_id"] == thread_id

    turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Add York", "stream": False},
    )
    assert turn.status_code == 200
    assert turn.json()["status"] == "completed"
    assert turn.json()["thread_state"]["selected_places"][0]["place"]["name"] == "York"


def test_chat_stream_endpoint_returns_404_for_missing_thread(chat_client) -> None:
    client, _store = chat_client
    response = client.get("/chat/stream/missing-thread")

    assert response.status_code == 404
