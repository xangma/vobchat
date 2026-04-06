from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from vobchat.api.schemas.chat import (
    ChatMessage,
    ChatOperation,
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatTurnRequest,
    ChatTurnResponse,
    ChatUIStateDelta,
    PlannerAction,
    PlannerResult,
)
from vobchat.api.schemas.maps import (
    MapFeatureCollectionResponse,
    MapFeatureResponse,
    MapFeaturesQuery,
)
from vobchat.api.schemas.metadata import (
    PlaceKeyFindingResponse,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.schemas.places import (
    PlaceCandidateResponse,
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
    ThemeSummaryResponse,
    ThemesForUnitResponse,
)
from vobchat.web.callbacks.chat_sse import bootstrap_runtime, process_chat_turn
from vobchat.web.callbacks.maps import load_map_layer
from vobchat.web.callbacks.visualization import load_visualization_data
from vobchat.web.clients import APIClient, APIClientConfig
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_resolved_place(
    *,
    place_id: int = 1,
    place_name: str = "York",
    unit_id: int = 101,
    unit_type: str = "MOD_DIST",
) -> ResolvedPlaceResponse:
    return ResolvedPlaceResponse(
        place=PlaceCandidateResponse(
            place_id=place_id,
            name=place_name,
            unit_ids=[unit_id],
            unit_types=[unit_type],
            match_type="resolved",
        ),
        units=[
            UnitDetailResponse(
                unit_id=unit_id,
                unit_name=place_name,
                unit_type=unit_type,
                unit_type_label="Modern District",
            )
        ],
    )


def make_theme() -> ThemeSummaryResponse:
    return ThemeSummaryResponse(theme_id="T_POP", label="Population")


def make_cube(*, has_categories: bool = True) -> CubeSummaryResponse:
    return CubeSummaryResponse(
        theme_id="T_POP",
        cube_id="N_POP_TOTAL" if not has_categories else "N_POP_WAY",
        label="Total population" if not has_categories else "Population by category",
        start_year=1801,
        end_year=1901,
        observation_count=5,
        has_categories=has_categories,
    )


def make_thread_state(thread_id: str = "thread-1") -> ChatThreadState:
    now = utc_now()
    return ChatThreadState(thread_id=thread_id, created_at=now, updated_at=now)


class FakePhase5APIClient:
    def __init__(self) -> None:
        self.fetch_features_calls: list[Any] = []
        self.fetch_features_by_ids_calls: list[Any] = []
        self.fetch_time_series_calls: list[Any] = []
        self.fetch_category_breakdown_calls: list[Any] = []

    def create_thread(self) -> ChatThreadCreateResponse:
        state = make_thread_state("thread-1")
        return ChatThreadCreateResponse(thread_id=state.thread_id, state=state)

    def submit_turn(self, request: ChatTurnRequest) -> ChatTurnResponse:
        selected_place = make_resolved_place()
        selected_theme = make_theme()
        selected_cube = make_cube(has_categories=True)
        now = utc_now()
        thread_state = ChatThreadState(
            thread_id=request.thread_id,
            created_at=now,
            updated_at=now,
            messages=[
                ChatMessage(
                    message_id="user-1",
                    role="user",
                    content=request.message,
                    created_at=now,
                ),
                ChatMessage(
                    message_id="assistant-1",
                    role="assistant",
                    content="Loaded York population.",
                    created_at=now,
                ),
            ],
            selected_places=[selected_place],
            selected_theme=selected_theme,
            selected_cubes=[selected_cube],
        )
        return ChatTurnResponse(
            thread_id=request.thread_id,
            turn_id="turn-1",
            planner_result=PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    theme_query="Population",
                    confidence=0.9,
                ),
            ),
            assistant_message=thread_state.messages[-1],
            ui_delta=ChatUIStateDelta(
                operation=ChatOperation.FETCH_TIME_SERIES,
                notices=["Loaded York population."],
                selected_places=[selected_place],
                selected_theme=selected_theme,
                selected_cubes=[selected_cube],
            ),
            thread_state=thread_state,
        )

    def list_themes_for_unit(self, unit_id: int) -> ThemesForUnitResponse:
        return ThemesForUnitResponse(unit_id=unit_id, item_count=1, items=[make_theme()])

    def list_cubes_for_unit_theme(self, unit_id: int, theme_id: str) -> CubeListResponse:
        return CubeListResponse(
            unit_id=unit_id,
            theme_id=theme_id,
            item_count=2,
            items=[make_cube(has_categories=False), make_cube(has_categories=True)],
        )

    def get_place_profile(self, place_id: int) -> PlaceProfileResponse:
        return PlaceProfileResponse(place_id=place_id, name="York", text="Historic city")

    def get_place_key_findings(self, unit_id: int) -> PlaceKeyFindingsResponse:
        return PlaceKeyFindingsResponse(
            unit_id=unit_id,
            item_count=1,
            items=[PlaceKeyFindingResponse(label="Population", text="Population rose steadily.")],
        )

    def get_unit_type_info(self, unit_type: str) -> UnitTypeInfoResponse:
        return UnitTypeInfoResponse(identifier=unit_type, label="Modern District", unit_count=5)

    def get_data_entity_info(self, entity_id: str):
        raise AssertionError("Data entity info is not part of these Phase 5 tests")

    def fetch_features(self, query) -> MapFeatureCollectionResponse:
        self.fetch_features_calls.append(query)
        return MapFeatureCollectionResponse(
            mode="all",
            unit_type=query.unit_type,
            feature_count=1,
            features=[
                MapFeatureResponse(
                    unit_id=202,
                    unit_name="Region",
                    unit_type=query.unit_type,
                    geometry={"type": "Polygon", "coordinates": []},
                    has_theme=True,
                )
            ],
        )

    def fetch_features_by_ids(self, query) -> MapFeatureCollectionResponse:
        self.fetch_features_by_ids_calls.append(query)
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
                    has_theme=True,
                )
                for unit_id in query.ids
            ],
        )

    def fetch_time_series(self, request) -> TimeSeriesResponse:
        self.fetch_time_series_calls.append(request)
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

    def fetch_category_breakdown(self, request) -> CategoryBreakdownResponse:
        self.fetch_category_breakdown_calls.append(request)
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


def test_api_client_parses_chat_and_map_responses() -> None:
    created = ChatThreadCreateResponse(thread_id="thread-1", state=make_thread_state("thread-1"))
    turn = FakePhase5APIClient().submit_turn(
        ChatTurnRequest(thread_id="thread-1", message="Add York", stream=False)
    )
    map_features = MapFeatureCollectionResponse(
        mode="all",
        unit_type="MOD_REG",
        feature_count=1,
        features=[
            MapFeatureResponse(
                unit_id=202,
                unit_name="Region",
                unit_type="MOD_REG",
                geometry={"type": "Polygon", "coordinates": []},
            )
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/chat/threads":
            return httpx.Response(201, json=created.model_dump(mode="json"))
        if request.method == "POST" and request.url.path == "/chat/turn":
            return httpx.Response(200, json=turn.model_dump(mode="json"))
        if request.method == "GET" and request.url.path == "/maps/features":
            return httpx.Response(200, json=map_features.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = APIClient(
        APIClientConfig(base_url="http://testserver"),
        transport=httpx.MockTransport(handler),
    )

    created_response = client.create_thread()
    turn_response = client.submit_turn(
        ChatTurnRequest(thread_id=created_response.thread_id, message="Add York", stream=False)
    )
    map_response = client.fetch_features(MapFeaturesQuery(unit_type="MOD_REG"))

    assert created_response.thread_id == "thread-1"
    assert isinstance(turn_response, ChatTurnResponse)
    assert turn_response.thread_state.selected_places[0].place.name == "York"
    assert map_response.feature_count == 1
    assert map_response.features[0].unit_name == "Region"


def test_bootstrap_runtime_creates_thread_store() -> None:
    fake_client = FakePhase5APIClient()

    thread_state, selection_state, map_state, visualization_state, metadata_state, request_state = bootstrap_runtime(fake_client)

    assert thread_state["thread_id"] == "thread-1"
    assert selection_state["selected_places"] == []
    assert map_state["unit_type"] == "MOD_REG"
    assert visualization_state["time_series"] is None
    assert metadata_state["place_profile"] is None
    assert request_state["mode"] == "stream"


def test_process_chat_turn_updates_web_state_from_api_client() -> None:
    fake_client = FakePhase5APIClient()

    (
        thread_state,
        selection_state,
        map_state,
        visualization_state,
        metadata_state,
        request_state,
    ) = process_chat_turn(
        fake_client,
        thread_state_data=None,
        selection_state_data=initial_selection_state(),
        map_state_data=initial_map_state(),
        visualization_state_data=initial_visualization_state(),
        metadata_state_data=initial_metadata_state(),
        message="Show York population",
    )

    assert thread_state["selected_places"][0]["place"]["name"] == "York"
    assert selection_state["selected_theme"]["theme_id"] == "T_POP"
    assert len(selection_state["available_themes"]) == 1
    assert len(selection_state["available_cubes"]) == 2
    assert metadata_state["place_profile"]["name"] == "York"
    assert metadata_state["unit_type_info"]["identifier"] == "MOD_DIST"
    assert request_state["last_turn_id"] == "turn-1"
    assert map_state["unit_type"] == "MOD_DIST"
    assert visualization_state["active_tab"] == "line"


def test_load_map_layer_uses_maps_api_for_background_and_selected_ids() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [
        make_resolved_place(unit_id=101, unit_type="MOD_DIST").model_dump(mode="json"),
        make_resolved_place(place_id=2, place_name="North East", unit_id=202, unit_type="MOD_REG").model_dump(mode="json"),
    ]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")

    map_state = initial_map_state()
    map_state["selected_ids"] = [101, 202]
    map_state["unit_type"] = "MOD_REG"

    data, hideout, status = load_map_layer(
        fake_client,
        selection_state=selection_state,
        map_state=map_state,
    )

    assert fake_client.fetch_features_calls[0].unit_type == "MOD_REG"
    assert len(fake_client.fetch_features_by_ids_calls) == 2
    assert len(data["features"]) == 2
    assert hideout["selected"] == ["101", "202"]
    assert "202" in hideout["withTheme"]
    assert "Loaded 2 features" in status


def test_load_visualization_data_fetches_series_and_categories() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")
    selection_state["selected_cubes"] = [make_cube(has_categories=True).model_dump(mode="json")]

    visualization_state = initial_visualization_state()
    visualization_state["category_year"] = 1901

    updated = load_visualization_data(
        fake_client,
        selection_state=selection_state,
        visualization_state=visualization_state,
    )

    assert updated["time_series"]["row_count"] == 1
    assert updated["category_breakdown"]["row_count"] == 1
    assert fake_client.fetch_time_series_calls[0].cube_ids == ["N_POP_WAY"]
    assert fake_client.fetch_category_breakdown_calls[0].year == 1901
