from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient as FastAPITestClient

from vobchat.api.main import app as api_app
from vobchat.api.schemas.chat import ChatOperation, ChatThreadState
from vobchat.api.schemas.maps import MapFeatureCollectionResponse, MapFeatureResponse
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityReferenceResponse,
    DataEntityResolutionResponse,
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
    CubeSummaryResponse,
    ThemeSummaryResponse,
)
from vobchat.api.services.chat_orchestrator import ChatOrchestrator, get_chat_orchestrator
from vobchat.api.services.chat_threads import InMemoryChatThreadStore, get_chat_thread_store
from vobchat.auth.extensions import db
from vobchat.auth.models import User
from vobchat.core.llm.planner import ChatPlanner
from vobchat.core.settings import reset_settings_cache
from vobchat.web.callbacks.maps import load_map_layer, toggle_selected_place_from_map_click
from vobchat.web.callbacks.visualization import (
    build_category_figure,
    build_table_payload,
    build_time_series_figure,
    load_visualization_data,
)
from vobchat.web.state import (
    hydrate_metadata_state,
    hydrate_states_from_thread,
)
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
    place_id: int,
    place_name: str,
    unit_id: int,
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


YORK = make_resolved_place(place_id=1, place_name="York", unit_id=101)
LEEDS = make_resolved_place(place_id=4, place_name="Leeds", unit_id=102)
NEWPORT_GWENT = make_resolved_place(place_id=2, place_name="Newport", unit_id=201)
NEWPORT_IOW = make_resolved_place(place_id=3, place_name="Newport", unit_id=301)

POPULATION_THEME = ThemeSummaryResponse(
    theme_id="T_POP",
    label="Population",
    description="Population counts and related demographic measures.",
)
HOUSING_THEME = ThemeSummaryResponse(
    theme_id="T_HOU",
    label="Housing",
    description="Housing stock and occupancy measures.",
)

TOTAL_POPULATION_CUBE = CubeSummaryResponse(
    theme_id="T_POP",
    cube_id="N_POP_TOTAL",
    label="Total population",
    description="Total population count",
    start_year=1801,
    end_year=1911,
    observation_count=6,
    has_categories=False,
)
POPULATION_CATEGORY_CUBE = CubeSummaryResponse(
    theme_id="T_POP",
    cube_id="N_POP_AGE",
    label="Population by age",
    description="Age-group breakdown",
    start_year=1801,
    end_year=1911,
    observation_count=6,
    has_categories=True,
)


class AlwaysFailLocalModelClient:
    async def complete_json(self, messages, response_model, *, temperature=0.0):
        raise RuntimeError("planner unavailable")

    async def complete_text(self, messages, *, temperature=None, max_tokens=None):
        raise RuntimeError("assistant unavailable")

    async def stream_text(self, messages, *, temperature=None, max_tokens=None):
        raise RuntimeError("assistant unavailable")
        yield ""


class ParityPlacesService:
    def search_places(self, query):
        normalized = query.query.strip().lower()
        if normalized == "york":
            return self._search_response(query.query, query.match_mode, [YORK.place])
        if normalized == "yorrk":
            if query.match_mode == "fuzzy":
                return self._search_response(query.query, query.match_mode, [YORK.place])
            return self._search_response(query.query, query.match_mode, [])
        if normalized == "leeds":
            return self._search_response(query.query, query.match_mode, [LEEDS.place])
        if normalized == "newport":
            return self._search_response(
                query.query,
                query.match_mode,
                [NEWPORT_GWENT.place, NEWPORT_IOW.place],
            )
        return self._search_response(query.query, query.match_mode, [])

    @staticmethod
    def _search_response(query: str, match_mode: str, results: list[PlaceCandidateResponse]):
        from vobchat.api.schemas.places import PlaceSearchResponse

        return PlaceSearchResponse(
            query=query,
            match_mode=match_mode,
            result_count=len(results),
            results=results,
        )

    def resolve_place(self, place_id, query) -> ResolvedPlaceResponse | None:
        return {
            1: YORK,
            2: NEWPORT_GWENT,
            3: NEWPORT_IOW,
            4: LEEDS,
        }.get(place_id)

    def resolve_place_for_unit(self, unit_id, query=None) -> ResolvedPlaceResponse | None:
        return {
            101: YORK,
            102: LEEDS,
            201: NEWPORT_GWENT,
            301: NEWPORT_IOW,
        }.get(unit_id)

    def lookup_postcode(self, query):
        from vobchat.api.schemas.places import PostcodeLookupResponse, PostcodeLookupResultResponse

        postcode = query.postcode.replace(" ", "").upper()
        if postcode == "YO17EP":
            results = [
                PostcodeLookupResultResponse(
                    postcode=postcode,
                    unit_id=101,
                    place_id=1,
                    unit_name="York",
                    unit_type=query.unit_type,
                )
            ]
        else:
            results = []
        return PostcodeLookupResponse(
            postcode=postcode,
            unit_type=query.unit_type,
            result_count=len(results),
            results=results,
        )


class ParityThemesService:
    def list_themes(self, query):
        from vobchat.api.schemas.themes import ThemeListResponse

        return ThemeListResponse(item_count=2, items=[POPULATION_THEME, HOUSING_THEME])

    def resolve_theme(self, query):
        from vobchat.api.schemas.themes import ThemeResolutionResponse

        lowered = query.query.lower()
        if "population" in lowered:
            return ThemeResolutionResponse(query=query.query, result=POPULATION_THEME)
        if "housing" in lowered:
            return ThemeResolutionResponse(query=query.query, result=HOUSING_THEME)
        return None

    def list_themes_for_unit(self, unit_id):
        from vobchat.api.schemas.themes import ThemesForUnitResponse

        return ThemesForUnitResponse(unit_id=unit_id, item_count=2, items=[POPULATION_THEME, HOUSING_THEME])

    def list_cubes_for_unit_theme(self, unit_id, theme_id):
        from vobchat.api.schemas.themes import CubeListResponse

        if theme_id != POPULATION_THEME.theme_id:
            items = []
        else:
            items = [TOTAL_POPULATION_CUBE, POPULATION_CATEGORY_CUBE]
        return CubeListResponse(unit_id=unit_id, theme_id=theme_id, item_count=len(items), items=items)


class ParitySeriesService:
    def get_time_series(self, request):
        rows: list[TimeSeriesRowResponse] = []
        name_by_unit = {
            101: "York",
            102: "Leeds",
            201: "Newport",
            301: "Newport",
        }
        for unit_id in request.unit_ids:
            for year, value in ((1901, float(unit_id)), (1911, float(unit_id + 10))):
                rows.append(
                    TimeSeriesRowResponse(
                        year=year,
                        unit_id=unit_id,
                        unit_name=name_by_unit[unit_id],
                        unit_type="MOD_DIST",
                        cube_id=request.cube_ids[0],
                        cube_label="Total population",
                        value=value,
                    )
                )
        return TimeSeriesResponse(
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            row_count=len(rows),
            rows=rows,
        )

    def get_category_breakdown(self, request):
        rows: list[CategoryRowResponse] = []
        for unit_id in request.unit_ids:
            rows.extend(
                [
                    CategoryRowResponse(
                        year=request.year,
                        unit_id=unit_id,
                        unit_name="York" if unit_id == 101 else "Leeds",
                        cube_id=request.cube_ids[0],
                        category_group="age",
                        category_label="0-14",
                        value=21.0 + unit_id,
                    ),
                    CategoryRowResponse(
                        year=request.year,
                        unit_id=unit_id,
                        unit_name="York" if unit_id == 101 else "Leeds",
                        cube_id=request.cube_ids[0],
                        category_group="age",
                        category_label="15+",
                        value=42.0 + unit_id,
                    ),
                ]
            )
        return CategoryBreakdownResponse(
            year=request.year,
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            row_count=len(rows),
            rows=rows,
        )


class ParityMapsService:
    def get_features(self, query):
        return MapFeatureCollectionResponse(
            mode="all",
            unit_type=query.unit_type,
            feature_count=1,
            features=[
                MapFeatureResponse(
                    unit_id=900,
                    unit_name="Map background",
                    unit_type=query.unit_type,
                    geometry={"type": "Polygon", "coordinates": []},
                    has_theme=bool(query.theme_id),
                )
            ],
        )

    def get_features_by_ids(self, query):
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
                    has_theme=bool(query.theme_id),
                )
                for unit_id in query.ids
            ],
        )


class ParityMetadataService:
    def get_place_profile(self, place_id):
        place_name = {1: "York", 2: "Newport", 3: "Newport", 4: "Leeds"}.get(place_id, "Place")
        return PlaceProfileResponse(
            place_id=place_id,
            name=place_name,
            text=f"{place_name} profile text.",
        )

    def get_place_key_findings(self, unit_id):
        return PlaceKeyFindingsResponse(
            unit_id=unit_id,
            item_count=1,
            items=[PlaceKeyFindingResponse(label="Population", text="Population changed over time.")],
        )

    def get_unit_type_info(self, unit_type):
        return UnitTypeInfoResponse(
            identifier=unit_type,
            label="Modern District",
            description="Administrative district used in the modern system.",
            unit_count=42,
        )

    def resolve_data_entity(self, query):
        return DataEntityResolutionResponse(
            query=query.query,
            result=DataEntityReferenceResponse(
                entity_id="N_POP_TOTAL",
                label="Total population",
                entity_type="N",
            ),
        )

    def get_data_entity_info(self, entity_id):
        return DataEntityInfoResponse(
            entity_id=entity_id,
            entity_type="N",
            name="Total population",
            type_name="Measure",
            text="Total population count measure.",
        )


class WebParityAPIClient:
    def __init__(self) -> None:
        self.fetch_features_calls: list[Any] = []
        self.fetch_features_by_ids_calls: list[Any] = []

    def get_place_for_unit(self, unit_id: int, query=None) -> ResolvedPlaceResponse:
        result = {
            101: YORK,
            102: LEEDS,
            201: NEWPORT_GWENT,
            301: NEWPORT_IOW,
        }.get(unit_id)
        if result is None:
            raise AssertionError(f"Unexpected unit id {unit_id}")
        return result

    def fetch_features(self, query) -> MapFeatureCollectionResponse:
        self.fetch_features_calls.append(query)
        return MapFeatureCollectionResponse(
            mode="all",
            unit_type=query.unit_type,
            feature_count=1,
            features=[
                MapFeatureResponse(
                    unit_id=900,
                    unit_name="Background",
                    unit_type=query.unit_type,
                    geometry={"type": "Polygon", "coordinates": []},
                    has_theme=bool(query.theme_id),
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
        return ParitySeriesService().get_time_series(request)

    def fetch_category_breakdown(self, request) -> CategoryBreakdownResponse:
        return ParitySeriesService().get_category_breakdown(request)

    def get_place_profile(self, place_id: int) -> PlaceProfileResponse:
        return ParityMetadataService().get_place_profile(place_id)

    def get_place_key_findings(self, unit_id: int) -> PlaceKeyFindingsResponse:
        return ParityMetadataService().get_place_key_findings(unit_id)

    def get_unit_type_info(self, unit_type: str) -> UnitTypeInfoResponse:
        return ParityMetadataService().get_unit_type_info(unit_type)

    def get_data_entity_info(self, entity_id: str) -> DataEntityInfoResponse:
        return ParityMetadataService().get_data_entity_info(entity_id)


def build_orchestrator(store: InMemoryChatThreadStore | None = None) -> ChatOrchestrator:
    failing_client = AlwaysFailLocalModelClient()
    return ChatOrchestrator(
        thread_store=store or InMemoryChatThreadStore(),
        planner=ChatPlanner(llm_client=failing_client),
        llm_client=failing_client,
        places_service=ParityPlacesService(),
        themes_service=ParityThemesService(),
        series_service=ParitySeriesService(),
        maps_service=ParityMapsService(),
        metadata_service=ParityMetadataService(),
    )


@pytest.fixture()
def parity_chat_client():
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store)
    api_app.dependency_overrides[get_chat_orchestrator] = lambda: orchestrator
    api_app.dependency_overrides[get_chat_thread_store] = lambda: store
    with FastAPITestClient(api_app) as client:
        yield client
    api_app.dependency_overrides.clear()


def build_web_app(monkeypatch: pytest.MonkeyPatch, tmp_path):
    auth_db_path = tmp_path / "users.db"
    monkeypatch.setenv("AUTH_DATABASE_URL", f"sqlite:///{auth_db_path}")
    monkeypatch.setenv("SECRET_KEY", "phase7-test-secret")
    monkeypatch.setenv("SESSION_COOKIE_SECURE", "false")
    monkeypatch.setenv("WTF_CSRF_ENABLED", "false")
    monkeypatch.setenv("VOBCHAT_SKIP_WORKFLOW_STARTUP", "true")
    reset_settings_cache()

    monkeypatch.setattr(
        User,
        "verify_password",
        lambda self, raw_password: self.password_hash == raw_password,
    )

    from vobchat.web.app import create_app

    app = create_app()
    return app, app.server


def create_user(server, *, email: str, password: str) -> None:
    with server.app_context():
        existing = db.session.scalar(db.select(User).filter_by(email=email))
        if existing is None:
            db.session.add(User(email=email, password_hash=password))
            db.session.commit()


def login(client, *, email: str, password: str):
    return client.post(
        "/login",
        data={"email": email, "password": password},
        follow_redirects=False,
    )


def test_auth_runtime_redirect_login_and_logout_work(monkeypatch, tmp_path) -> None:
    _app, server = build_web_app(monkeypatch, tmp_path)
    create_user(server, email="alice@example.com", password="secret123")
    client = server.test_client()

    unauthorized = client.get("/", follow_redirects=False)
    assert unauthorized.status_code == 302
    assert "/login" in unauthorized.headers["Location"]

    login_page = client.get("/login")
    assert login_page.status_code == 200
    assert b"email" in login_page.data

    logged_in = login(client, email="alice@example.com", password="secret123")
    assert logged_in.status_code == 302

    home = client.get("/")
    assert home.status_code == 200

    logged_out = client.get("/logout", follow_redirects=False)
    assert logged_out.status_code == 302
    assert "/login" in logged_out.headers["Location"]


def test_chat_journey_exact_place_theme_chart_reload_and_resume(parity_chat_client) -> None:
    client = parity_chat_client
    created = client.post("/chat/threads")
    thread_id = created.json()["thread_id"]

    place_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Add York", "stream": False},
    )
    assert place_turn.status_code == 200
    assert place_turn.json()["thread_state"]["selected_places"][0]["place"]["name"] == "York"

    theme_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Population", "stream": False},
    )
    assert theme_turn.status_code == 200
    assert theme_turn.json()["thread_state"]["selected_theme"]["label"] == "Population"
    assert "demographic" in theme_turn.json()["thread_state"]["selected_theme"]["description"].lower()

    series_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "show population over time", "stream": False},
    )
    series_payload = series_turn.json()
    assert series_turn.status_code == 200
    assert series_payload["ui_delta"]["operation"] == ChatOperation.FETCH_TIME_SERIES.value
    assert series_payload["ui_delta"]["time_series"]["row_count"] == 2
    assert series_payload["thread_state"]["selected_cubes"][0]["cube_id"] == "N_POP_TOTAL"

    fetched = client.get(f"/chat/threads/{thread_id}")
    assert fetched.status_code == 200

    selection_state, visualization_state, metadata_state, map_state = hydrate_states_from_thread(
        thread_state_data=fetched.json(),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )
    assert selection_state["selected_theme"]["label"] == "Population"
    assert visualization_state["time_series"]["row_count"] == 2
    assert map_state["selected_ids"] == [101]
    assert metadata_state["place_profile"] is None

    category_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "show categories", "stream": False},
    )
    category_payload = category_turn.json()
    assert category_turn.status_code == 200
    assert category_payload["ui_delta"]["category_breakdown"]["row_count"] == 2
    assert category_payload["thread_state"]["selected_theme"]["label"] == "Population"


def test_chat_journey_fuzzy_ambiguous_postcode_remove_and_metadata(parity_chat_client) -> None:
    client = parity_chat_client
    created = client.post("/chat/threads")
    thread_id = created.json()["thread_id"]

    fuzzy_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Add Yorrk", "stream": False},
    )
    assert fuzzy_turn.status_code == 200
    assert fuzzy_turn.json()["thread_state"]["selected_places"][0]["place"]["name"] == "York"

    ambiguous_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Newport", "stream": False},
    )
    ambiguous_payload = ambiguous_turn.json()
    assert ambiguous_turn.status_code == 200
    assert ambiguous_payload["ui_delta"]["place_search_results"]
    assert len(ambiguous_payload["ui_delta"]["place_search_results"]) == 2

    choose_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "select place 2", "stream": False},
    )
    assert choose_turn.status_code == 200
    selected_ids = {
        place["place"]["place_id"] for place in choose_turn.json()["thread_state"]["selected_places"]
    }
    assert selected_ids == {1, 2}

    postcode_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Use postcode YO1 7EP", "stream": False},
    )
    assert postcode_turn.status_code == 200
    postcode_ids = {
        place["place"]["place_id"] for place in postcode_turn.json()["thread_state"]["selected_places"]
    }
    assert postcode_ids == {1, 2}

    remove_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "remove Newport", "stream": False},
    )
    remove_payload = remove_turn.json()
    assert remove_turn.status_code == 200
    assert [place["place"]["name"] for place in remove_payload["thread_state"]["selected_places"]] == ["York"]

    profile_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "tell me about this place", "stream": False},
    )
    assert profile_turn.status_code == 200
    assert profile_turn.json()["ui_delta"]["place_profile"]["place_id"] == 1

    unit_type_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "tell me about MOD_DIST unit type", "stream": False},
    )
    assert unit_type_turn.status_code == 200
    assert unit_type_turn.json()["ui_delta"]["unit_type_info"]["identifier"] == "MOD_DIST"

    entity_turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "data entity households", "stream": False},
    )
    entity_payload = entity_turn.json()
    assert entity_turn.status_code == 200
    assert entity_payload["ui_delta"]["data_entity_info"]["entity_id"] == "N_POP_TOTAL"
    assert entity_payload["ui_delta"]["data_entity_resolution"]["result"]["entity_id"] == "N_POP_TOTAL"


def test_chat_turn_clarifies_when_required_context_is_missing(parity_chat_client) -> None:
    client = parity_chat_client
    thread_id = client.post("/chat/threads").json()["thread_id"]

    response = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "show a line chart", "stream": False},
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["planner_result"]["action"]["operation"] == ChatOperation.CLARIFY.value
    assert "Which place" in payload["assistant_message"]["content"]


def test_map_bbox_loading_and_map_click_selection_use_rewrite_api_path() -> None:
    api_client = WebParityAPIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [YORK.model_dump(mode="json")]
    selection_state["selected_theme"] = POPULATION_THEME.model_dump(mode="json")

    map_state = initial_map_state()
    map_state["selected_ids"] = [101]
    map_state["year_range"] = [1881, 1911]
    map_state["bbox"] = {"min_x": -2.0, "min_y": 53.0, "max_x": -1.0, "max_y": 54.0}

    _data, _hideout, status = load_map_layer(
        api_client,
        selection_state=selection_state,
        map_state=map_state,
    )

    assert "Loaded" in status
    assert api_client.fetch_features_calls
    background_query = api_client.fetch_features_calls[0]
    assert background_query.min_x == -2.0
    assert background_query.max_y == 54.0
    assert background_query.exclude_ids == [101]
    assert api_client.fetch_features_by_ids_calls[0].ids == [101]

    clicked = toggle_selected_place_from_map_click(
        api_client,
        selection_state=selection_state,
        click_data={"feature": {"properties": {"g_unit": 102}}},
    )
    assert {place["place"]["place_id"] for place in clicked["selected_places"]} == {1, 4}

    toggled_off = toggle_selected_place_from_map_click(
        api_client,
        selection_state=clicked,
        click_data={"feature": {"properties": {"g_unit": 102}}},
    )
    assert {place["place"]["place_id"] for place in toggled_off["selected_places"]} == {1}


def test_visualization_multi_place_category_table_and_metadata_hydration() -> None:
    api_client = WebParityAPIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [
        YORK.model_dump(mode="json"),
        LEEDS.model_dump(mode="json"),
    ]
    selection_state["selected_theme"] = POPULATION_THEME.model_dump(mode="json")
    selection_state["selected_cubes"] = [POPULATION_CATEGORY_CUBE.model_dump(mode="json")]

    visualization_state = load_visualization_data(
        api_client,
        selection_state=selection_state,
        visualization_state=initial_visualization_state(),
    )
    line_figure = build_time_series_figure(visualization_state)
    category_figure = build_category_figure(visualization_state)

    assert visualization_state["time_series"]["row_count"] == 4
    assert len(line_figure.data) == 2
    assert len(category_figure.data) >= 1

    visualization_state["active_tab"] = "categories"
    columns, rows = build_table_payload(visualization_state)
    assert columns
    assert len(rows) == 4

    metadata_state = hydrate_metadata_state(
        api_client,
        selection_state=selection_state,
        metadata_state={"requested_entity_id": "N_POP_TOTAL"},
    )
    assert metadata_state["place_profile"]["name"] == "York"
    assert metadata_state["place_key_findings"]["item_count"] == 1
    assert metadata_state["unit_type_info"]["identifier"] == "MOD_DIST"
    assert metadata_state["data_entity_info"]["entity_id"] == "N_POP_TOTAL"
