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
    DiscoveryItem,
    DiscoveryOutputOption,
    DiscoveryResult,
    DiscoverySuggestedAction,
    DiscoveryTopic,
    DiscoveryYearCoverage,
    ExactSliceCandidate,
    PlannerAction,
    PlannerResult,
    RenderProjection,
    ReportingGeographyRef,
    SemanticPlaceRef,
    UIProjection,
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
    CategoryBreakdownRequest,
    CategoryBreakdownResponse,
    CategoryRowResponse,
    TimeSeriesRequest,
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
from vobchat.web.callbacks.chat_sse import (
    _render_discovery_panel,
    _render_pending_candidates,
    _render_selection_summary,
    _render_status_banner,
    _render_trust_panel,
)
from vobchat.web.callbacks.maps import load_map_layer
from vobchat.web.callbacks.visualization import load_visualization_data
from vobchat.web.clients import APIClient, APIClientConfig
from vobchat.web.state import hydrate_states_from_thread, selected_unit_ids
from vobchat.web.stores import (
    initial_map_state,
    initial_metadata_state,
    initial_selection_state,
    initial_visualization_state,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def component_text(node: Any) -> str:
    if node is None:
        return ""
    if isinstance(node, (list, tuple)):
        return " ".join(part for part in (component_text(child) for child in node) if part)
    if isinstance(node, (str, int, float)):
        return str(node)
    children = getattr(node, "children", None)
    if isinstance(children, (list, tuple)):
        return " ".join(part for part in (component_text(child) for child in children) if part)
    return component_text(children)


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
        available_cubes = [make_cube(has_categories=False), selected_cube]
        reporting_geography = ReportingGeographyRef(
            unit_type="MOD_DIST",
            unit_ids=[101],
            label="Modern District",
        )
        time_series = self.fetch_time_series(
            TimeSeriesRequest(unit_ids=[101], cube_ids=[selected_cube.cube_id])
        )
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
            ui_projection=UIProjection(
                projection_id="ui-thread-1",
                selected_places=[selected_place],
                reporting_geography=reporting_geography,
                dataset_family=selected_theme,
                available_themes=[selected_theme],
                available_cubes=available_cubes,
                selected_cubes=[selected_cube],
                active_output_mode="chart",
                notices=["Loaded York population."],
            ),
            render_projection=RenderProjection(
                projection_id="rp-thread-1",
                active_output_mode="chart",
                answer_text="Loaded York population.",
                chart=time_series,
                table={
                    "columns": [{"name": "Year", "id": "year"}],
                    "rows": [{"year": 1901}],
                },
                metadata_payload={
                    "place_profile": PlaceProfileResponse(
                        place_id=1,
                        name="York",
                        text="Historic city",
                    ).model_dump(mode="json"),
                    "place_key_findings": PlaceKeyFindingsResponse(
                        unit_id=101,
                        item_count=1,
                        items=[
                            PlaceKeyFindingResponse(
                                label="Population",
                                text="Population rose steadily.",
                            )
                        ],
                    ).model_dump(mode="json"),
                    "unit_type_info": UnitTypeInfoResponse(
                        identifier="MOD_DIST",
                        label="Modern District",
                        unit_count=5,
                    ).model_dump(mode="json"),
                },
            ),
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
                time_series=time_series,
                ui_projection=thread_state.ui_projection,
                render_projection=thread_state.render_projection,
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
    assert selection_state["reporting_geography"]["unit_ids"] == [101]
    assert visualization_state["render_projection"]["active_output_mode"] == "chart"


def test_hydrate_states_from_thread_uses_projection_reporting_geography() -> None:
    now = utc_now()
    selected_place = make_resolved_place(unit_id=101, unit_type="MOD_DIST")
    projected_thread = ChatThreadState(
        thread_id="thread-projected",
        created_at=now,
        updated_at=now,
        ui_projection=UIProjection(
            projection_id="ui-projected",
            selected_places=[selected_place],
            reporting_geography=ReportingGeographyRef(
                unit_type="MOD_REG",
                unit_ids=[202],
                label="Modern Region",
            ),
            dataset_family=make_theme(),
            selected_cubes=[make_cube(has_categories=False)],
        ),
    )

    selection_state, visualization_state, metadata_state, map_state = hydrate_states_from_thread(
        thread_state_data=projected_thread.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )

    assert selection_state["selected_places"][0]["place"]["name"] == "York"
    assert selection_state["selected_theme"]["theme_id"] == "T_POP"
    assert selection_state["selected_cubes"][0]["cube_id"] == "N_POP_TOTAL"
    assert selection_state["reporting_geography"]["unit_type"] == "MOD_REG"
    assert map_state["selected_ids"] == [202]
    assert map_state["unit_type"] == "MOD_REG"
    assert visualization_state["time_series"] is None
    assert metadata_state["place_profile"] is None


def test_hydrate_states_from_thread_preserves_discovery_result() -> None:
    now = utc_now()
    projected_thread = ChatThreadState(
        thread_id="thread-discovery",
        created_at=now,
        updated_at=now,
        latest_discovery_result=DiscoveryResult(
            topic=DiscoveryTopic.FEASIBLE_OUTPUTS,
            title="Feasible outputs",
            items=[
                DiscoveryItem(
                    item_id="table",
                    label="Table",
                    kind="output_mode",
                    metadata={"output_mode": "table"},
                )
            ],
        ),
    )

    selection_state, _visualization_state, _metadata_state, _map_state = hydrate_states_from_thread(
        thread_state_data=projected_thread.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )

    assert selection_state["discovery_result"]["topic"] == "feasible_outputs"
    assert selection_state["discovery_result"]["items"][0]["item_id"] == "table"


def test_hydrate_states_from_thread_preserves_richer_discovery_catalog_fields() -> None:
    now = utc_now()
    projected_thread = ChatThreadState(
        thread_id="thread-rich-discovery",
        created_at=now,
        updated_at=now,
        latest_discovery_result=DiscoveryResult(
            topic=DiscoveryTopic.EXACT_SLICE_OPTIONS,
            title="Exact-slice options",
            place_context=[SemanticPlaceRef(place_id=1, label="York", unit_ids=[101], unit_types=["MOD_DIST"])],
            reporting_geographies=[
                ReportingGeographyRef(unit_type="MOD_DIST", unit_ids=[101], label="Modern District")
            ],
            exact_slice_options=[
                ExactSliceCandidate(
                    cube_id="N_TOT_POP",
                    cube_ids=["N_TOT_POP"],
                    label="Current Total Population",
                    cellref="TOT_POP:now",
                    dataitem_id="N_TOT_POP_4",
                )
            ],
            supported_outputs=[
                DiscoveryOutputOption(
                    output_mode="table",
                    label="Table",
                    feasibility="available",
                )
            ],
            available_years=[
                DiscoveryYearCoverage(
                    item_id="N_TOT_POP_4",
                    label="Current Total Population: 1801-1901",
                    cube_id="N_TOT_POP",
                    start_year=1801,
                    end_year=1901,
                    executable=True,
                )
            ],
            suggested_actions=[
                DiscoverySuggestedAction(
                    label="Show the available years",
                    operation=ChatOperation.LIST_AVAILABLE_YEARS,
                    theme_id="T_POP",
                )
            ],
        ),
    )

    selection_state, _visualization_state, _metadata_state, _map_state = hydrate_states_from_thread(
        thread_state_data=projected_thread.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )

    assert selection_state["discovery_result"]["topic"] == "exact_slice_options"
    assert selection_state["discovery_result"]["exact_slice_options"][0]["cube_id"] == "N_TOT_POP"
    assert selection_state["discovery_result"]["available_years"][0]["item_id"] == "N_TOT_POP_4"
    assert (
        selection_state["discovery_result"]["suggested_actions"][0]["operation"]
        == "list_available_years"
    )


def test_render_discovery_panel_from_projected_state() -> None:
    selection_state = initial_selection_state()
    selection_state["discovery_result"] = DiscoveryResult(
        topic=DiscoveryTopic.REPORTING_GEOGRAPHIES,
        title="Geography levels you can use here",
        summary="I found two geography levels that work for York.",
        items=[
            DiscoveryItem(item_id="MOD_DIST", label="Modern District", kind="geography"),
            DiscoveryItem(item_id="PAR_UNIT", label="Parish", kind="geography"),
        ],
        supported_outputs=[
            DiscoveryOutputOption(output_mode="table", label="Table", support_level="guarded"),
            DiscoveryOutputOption(
                output_mode="category_chart",
                label="Breakdown chart",
                support_level="discovery_only",
            ),
        ],
        available_years=[
            DiscoveryYearCoverage(item_id="years-1", label="1801 to 1901"),
        ],
        suggested_actions=[
            DiscoverySuggestedAction(label="Use parish level"),
            DiscoverySuggestedAction(label="Show the available years"),
        ],
    ).model_dump(mode="json")

    panel_text = component_text(_render_discovery_panel(selection_state))

    assert "Explore the data" in panel_text
    assert "Geography levels you can use here" in panel_text
    assert "Exploration only" in panel_text
    assert "Modern District" in panel_text
    assert "Parish" in panel_text
    assert "Table (can run when the rest is clear)" in panel_text
    assert "Breakdown chart (explore only)" in panel_text
    assert "Good next steps" in panel_text
    assert "Use parish level" in panel_text


def test_render_trust_panel_surfaces_result_context_and_limits() -> None:
    selection_state = initial_selection_state()
    selection_state["active_output_mode"] = "table"
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_DIST",
        "unit_ids": [101],
        "label": "Modern District",
    }
    selection_state["time_scope"] = {"label": "1801 to 1901"}
    selection_state["provenance_summary"] = {
        "result_type": "executed_analysis",
        "resolved_context": {
            "place_labels": ["York"],
            "reporting_geography_label": "Modern District",
            "dataset_family_label": "Population",
            "exact_slice_label": "Total population",
            "time_scope_label": "1801 to 1901",
            "output_mode": "table",
        },
        "safe_downgrades": ["Used a boundary-only view instead of a thematic map."],
        "availability_limits": ["Some years are still unavailable for this result."],
        "comparability_limits": [],
    }

    panel_text = component_text(_render_trust_panel(selection_state))

    assert "How to read this result" in panel_text
    assert "Table result for York using Modern District and Total population." in panel_text
    assert "Geography: Modern District" in panel_text
    assert "Time: 1801 to 1901" in panel_text
    assert "View: Table" in panel_text
    assert "Used a boundary-only view instead of a thematic map." in panel_text
    assert "Some years are still unavailable for this result." in panel_text


def test_render_trust_panel_stays_hidden_without_meaningful_result_context() -> None:
    selection_state = initial_selection_state()
    selection_state["runtime_state"] = {"mode": "llm_assisted"}

    panel_text = component_text(_render_trust_panel(selection_state))

    assert panel_text == ""


def test_render_selection_summary_includes_semantic_context() -> None:
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")
    selection_state["selected_cubes"] = [make_cube(has_categories=False).model_dump(mode="json")]
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_DIST",
        "unit_ids": [101],
        "label": "Modern District",
    }
    selection_state["time_scope"] = {"label": "1801 to 1901"}
    selection_state["active_output_mode"] = "table"
    selection_state["current_receipt_kind"] = "analysis"

    summary_text = component_text(_render_selection_summary(selection_state))

    assert "Current context" in summary_text
    assert "York" in summary_text
    assert "Theme: Population" in summary_text
    assert "Geography: Modern District" in summary_text
    assert "Time: 1801 to 1901" in summary_text
    assert "View: Table" in summary_text
    assert "Result: Analysis" in summary_text


def test_render_pending_candidates_supports_general_clarification_copy() -> None:
    selection_state = initial_selection_state()
    selection_state["pending_clarification"] = {
        "question": "Please choose a geography.",
        "slot": "reporting_geography",
        "options": [
            {
                "option_id": "rg-1",
                "label": "Parish",
                "kind": "reporting_geography",
                "metadata": {"unit_type": "PAR_UNIT"},
            }
        ],
    }

    clarification_text = component_text(_render_pending_candidates(selection_state))

    assert "I need one choice from you" in clarification_text
    assert "Choose the geography level you want to work with." in clarification_text
    assert "Parish" in clarification_text
    assert "Reply with this option label to continue." in clarification_text


def test_render_status_banner_uses_friendlier_copy_and_excludes_provenance_summary() -> None:
    selection_state = initial_selection_state()
    selection_state["notices"] = [
        "I need to stay in discovery until the analysis spec is explicit and safe.",
        "boundary_map_without_measure",
    ]
    selection_state["provenance_summary"] = {"summary": "This should not appear in the status banner."}

    banner_text = component_text(
        _render_status_banner(selection_state, {"error": None, "busy": False, "mode": "stream"})
    )

    assert "I found relevant data, but I need one more detail before I can turn it into a result." in banner_text
    assert "Showing boundaries only because a measured thematic map is not safely available here yet." in banner_text
    assert "analysis spec" not in banner_text.lower()
    assert "This should not appear in the status banner." not in banner_text


def test_selected_unit_ids_do_not_fall_back_to_selected_place_units() -> None:
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]

    assert selected_unit_ids(selection_state) == []


def test_load_map_layer_uses_projection_reporting_geography() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [
        make_resolved_place(unit_id=101, unit_type="MOD_DIST").model_dump(mode="json"),
        make_resolved_place(place_id=2, place_name="North East", unit_id=202, unit_type="MOD_REG").model_dump(mode="json"),
    ]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_REG",
        "unit_ids": [202],
        "label": "Modern Region",
    }

    map_state = initial_map_state()
    map_state["selected_ids"] = [202]
    map_state["unit_type"] = "MOD_REG"

    data, hideout, status = load_map_layer(
        fake_client,
        selection_state=selection_state,
        map_state=map_state,
    )

    assert fake_client.fetch_features_calls[0].unit_type == "MOD_REG"
    assert len(fake_client.fetch_features_by_ids_calls) == 1
    assert fake_client.fetch_features_by_ids_calls[0].ids == [202]
    assert len(data["features"]) == 1
    assert hideout["selected"] == ["202"]
    assert "202" in hideout["withTheme"]
    assert "Boundary map" in status
    assert "Modern Region" in status
    assert "York, North East" in status


def test_load_map_layer_waits_for_context_before_fetching() -> None:
    fake_client = FakePhase5APIClient()

    data, hideout, status = load_map_layer(
        fake_client,
        selection_state=initial_selection_state(),
        map_state=initial_map_state(),
    )

    assert fake_client.fetch_features_calls == []
    assert fake_client.fetch_features_by_ids_calls == []
    assert data == {"type": "FeatureCollection", "features": []}
    assert hideout == {"selected": [], "withTheme": []}
    assert status == "Choose a place or geography level to load a map."


def test_load_map_layer_waits_for_geography_before_fetching_selected_place() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]

    data, hideout, status = load_map_layer(
        fake_client,
        selection_state=selection_state,
        map_state=initial_map_state(),
    )

    assert fake_client.fetch_features_calls == []
    assert fake_client.fetch_features_by_ids_calls == []
    assert data == {"type": "FeatureCollection", "features": []}
    assert hideout == {"selected": [], "withTheme": []}
    assert status == "Choose a geography level to load a map for York."


def test_load_map_layer_reuses_projected_boundary_map() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_REG",
        "unit_ids": [202],
        "label": "Modern Region",
    }

    map_state = initial_map_state()
    map_state["unit_type"] = "MOD_REG"
    map_state["selected_ids"] = [202]
    map_state["render_projection"] = {
        "boundary_map": MapFeatureCollectionResponse(
            mode="ids",
            unit_type="MOD_REG",
            feature_count=1,
            requested_ids=[202],
            features=[
                MapFeatureResponse(
                    unit_id=202,
                    unit_name="North East",
                    unit_type="MOD_REG",
                    geometry={"type": "Polygon", "coordinates": []},
                    has_theme=True,
                )
            ],
        ).model_dump(mode="json")
    }

    data, hideout, status = load_map_layer(
        fake_client,
        selection_state=selection_state,
        map_state=map_state,
    )

    assert fake_client.fetch_features_calls == []
    assert fake_client.fetch_features_by_ids_calls == []
    assert data["features"][0]["properties"]["g_unit_type"] == "MOD_REG"
    assert hideout["selected"] == ["202"]
    assert status == "Boundary map at Modern Region level."


def test_load_visualization_data_fetches_series_and_categories() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")
    selection_state["selected_cubes"] = [make_cube(has_categories=True).model_dump(mode="json")]
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_DIST",
        "unit_ids": [101],
        "label": "Modern District",
    }

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
    assert updated["status"] == "Chart ready for York."


def test_hydrate_states_from_thread_prefers_projection_over_legacy_lists() -> None:
    now = utc_now()
    projected_thread = ChatThreadState(
        thread_id="thread-authoritative",
        created_at=now,
        updated_at=now,
        selected_places=[make_resolved_place(place_name="Legacy York")],
        selected_theme=ThemeSummaryResponse(theme_id="OLD", label="Legacy theme"),
        selected_cubes=[make_cube(has_categories=False)],
        ui_projection=UIProjection(
            projection_id="ui-authoritative",
            selected_places=[make_resolved_place(place_name="Projected Leeds", unit_id=303)],
            reporting_geography=ReportingGeographyRef(
                unit_type="MOD_REG",
                unit_ids=[202],
                label="Modern Region",
            ),
            dataset_family=make_theme(),
            selected_cubes=[make_cube(has_categories=True)],
            available_themes=[make_theme()],
            available_cubes=[make_cube(has_categories=True)],
            active_output_mode="table",
            discovery_result=DiscoveryResult(
                topic=DiscoveryTopic.FEASIBLE_OUTPUTS,
                title="Feasible outputs",
                items=[DiscoveryItem(item_id="table", label="Table", kind="output_mode")],
            ),
        ),
        render_projection=RenderProjection(
            projection_id="rp-authoritative",
            active_output_mode="table",
            answer_text="Projected table ready.",
            table={
                "columns": [{"name": "Year", "id": "year"}],
                "rows": [{"year": 1901}],
            },
            metadata_payload={
                "unit_type_info": UnitTypeInfoResponse(
                    identifier="MOD_REG",
                    label="Modern Region",
                    unit_count=3,
                ).model_dump(mode="json"),
            },
        ),
    )

    selection_state, visualization_state, metadata_state, map_state = hydrate_states_from_thread(
        thread_state_data=projected_thread.model_dump(mode="json"),
        selection_state=initial_selection_state(),
        visualization_state=initial_visualization_state(),
        metadata_state=initial_metadata_state(),
        map_state=initial_map_state(),
    )

    assert selection_state["selected_places"][0]["place"]["name"] == "Projected Leeds"
    assert selection_state["selected_theme"]["theme_id"] == "T_POP"
    assert selection_state["active_output_mode"] == "table"
    assert selection_state["discovery_result"]["items"][0]["item_id"] == "table"
    assert visualization_state["active_tab"] == "table"
    assert metadata_state["unit_type_info"]["identifier"] == "MOD_REG"
    assert map_state["selected_ids"] == [202]
    assert map_state["unit_type"] == "MOD_REG"


def test_load_visualization_data_reuses_projected_render_payload() -> None:
    fake_client = FakePhase5APIClient()
    selection_state = initial_selection_state()
    selection_state["selected_places"] = [make_resolved_place().model_dump(mode="json")]
    selection_state["selected_theme"] = make_theme().model_dump(mode="json")
    selection_state["selected_cubes"] = [make_cube(has_categories=False).model_dump(mode="json")]
    selection_state["reporting_geography"] = {
        "unit_type": "MOD_DIST",
        "unit_ids": [101],
        "label": "Modern District",
    }
    selection_state["ui_projection"] = {
        "selected_places": list(selection_state["selected_places"]),
        "dataset_family": dict(selection_state["selected_theme"]),
        "selected_cubes": list(selection_state["selected_cubes"]),
        "reporting_geography": dict(selection_state["reporting_geography"]),
    }

    visualization_state = initial_visualization_state()
    visualization_state["render_projection"] = RenderProjection(
        projection_id="rp-viz",
        active_output_mode="table",
        answer_text="Projected table ready.",
        table={
            "columns": [{"name": "Year", "id": "year"}],
            "rows": [{"year": 1901}],
        },
    ).model_dump(mode="json")
    visualization_state["active_output_mode"] = "table"

    updated = load_visualization_data(
        fake_client,
        selection_state=selection_state,
        visualization_state=visualization_state,
    )

    assert fake_client.fetch_time_series_calls == []
    assert fake_client.fetch_category_breakdown_calls == []
    assert updated["active_tab"] == "table"
    assert updated["status"] == "Table view of the current result for York."
