from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient as FastAPITestClient

from vobchat.api.main import app as api_app
from vobchat.api.schemas.chat import (
    AnalysisReceipt,
    AnalysisSpec,
    AvailabilityStatus,
    CapabilitySupportLevel,
    CategoryEntityRef,
    ChatOperation,
    ChatSSEEventName,
    ChatThreadState,
    ChatTurnRequest,
    ComparabilityStatus,
    ConversationState,
    DatasetFamilyRef,
    DiscoveryTopic,
    ExecutionGuardOutcome,
    ExactSliceCandidate,
    ExactSliceRef,
    OutputFeasibilityStatus,
    PlannerAction,
    PlannerResult,
    PolicyOutcome,
    ReceiptKind,
    ReportingGeographyRef,
    ReportingGeographyStatus,
    ResolutionState,
    SliceStatus,
    TimeScope,
    UIProjection,
    WorkflowPath,
)
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
from vobchat.api.services.chat_safety import (
    CapabilityRegistry,
    DecisionPolicyEngine,
    ExecutionGuard,
    ResolvedAnalysisContext,
)
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


class StaticPlanner:
    def __init__(self, action: PlannerAction) -> None:
        self.action = action

    async def plan(self, state: ChatThreadState, user_message: str) -> PlannerResult:
        return PlannerResult(action=self.action, source="fallback")


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
        if query.query.lower() == "leeds":
            return PlaceSearchResponse(
                query=query.query,
                match_mode=query.match_mode,
                result_count=1,
                results=[
                    PlaceCandidateResponse(
                        place_id=4,
                        name="Leeds",
                        county_name="Yorkshire",
                        latitude=53.79,
                        longitude=-1.55,
                        unit_ids=[102],
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
        if place_id == 4:
            return make_resolved_place(place_id=4, name="Leeds", unit_id=102)
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


class AmbiguousPlacesService(FakePlacesService):
    def search_places(self, query) -> PlaceSearchResponse:
        if query.query.lower() == "newport":
            return PlaceSearchResponse(
                query=query.query,
                match_mode=query.match_mode,
                result_count=2,
                results=[
                    PlaceCandidateResponse(
                        place_id=2,
                        name="Newport",
                        county_name="Monmouthshire",
                        unit_ids=[201],
                        unit_types=["MOD_DIST"],
                        match_type=query.match_mode,
                    ),
                    PlaceCandidateResponse(
                        place_id=3,
                        name="Newport",
                        county_name="Isle of Wight",
                        unit_ids=[301],
                        unit_types=["MOD_DIST"],
                        match_type=query.match_mode,
                    ),
                ],
            )
        return super().search_places(query)


def make_multi_geography_place() -> ResolvedPlaceResponse:
    return ResolvedPlaceResponse(
        place=PlaceCandidateResponse(
            place_id=9,
            name="Layered York",
            unit_ids=[101, 901],
            unit_types=["MOD_DIST", "PAR_UNIT"],
            match_type="resolved",
        ),
        units=[
            UnitDetailResponse(
                unit_id=101,
                unit_name="Layered York District",
                unit_type="MOD_DIST",
                unit_type_label="Modern District",
            ),
            UnitDetailResponse(
                unit_id=901,
                unit_name="Layered York Parish",
                unit_type="PAR_UNIT",
                unit_type_label="Parish",
            ),
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


class FakeExactSliceCatalogService:
    def supported_cubes(self, theme_id: str | None) -> set[str]:
        if theme_id == "T_POP":
            return {"N_TOT_POP"}
        return set()

    @staticmethod
    def _supported_candidates(theme_id: str) -> list[ExactSliceCandidate]:
        if theme_id != "T_POP":
            return []
        return [
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Population 30 years earlier",
                description="Total Population",
                cellref="TOT_POP:prev_30yrs",
                dataitem_id="N_TOT_POP_1",
                cat_id="C_TOT_POP_1",
                has_categories=True,
                category_entity=CategoryEntityRef(
                    entity_id="C_TOT_POP_1",
                    label="Population 30 years earlier",
                    group_label="Total Population",
                    source="fake_metadata",
                    provenance="fake_metadata",
                ),
                start_year=1801,
                end_year=1901,
                observation_count=5,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="fake_exact_slice_catalog",
            ),
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Population 20 years earlier",
                description="Total Population",
                cellref="TOT_POP:prev_20yrs",
                dataitem_id="N_TOT_POP_2",
                cat_id="C_TOT_POP_2",
                has_categories=True,
                category_entity=CategoryEntityRef(
                    entity_id="C_TOT_POP_2",
                    label="Population 20 years earlier",
                    group_label="Total Population",
                    source="fake_metadata",
                    provenance="fake_metadata",
                ),
                start_year=1801,
                end_year=1901,
                observation_count=5,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="fake_exact_slice_catalog",
            ),
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Population 10 years earlier",
                description="Total Population",
                cellref="TOT_POP:prev_10yrs",
                dataitem_id="N_TOT_POP_3",
                cat_id="C_TOT_POP_3",
                has_categories=True,
                category_entity=CategoryEntityRef(
                    entity_id="C_TOT_POP_3",
                    label="Population 10 years earlier",
                    group_label="Total Population",
                    source="fake_metadata",
                    provenance="fake_metadata",
                ),
                start_year=1801,
                end_year=1901,
                observation_count=5,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="fake_exact_slice_catalog",
            ),
            ExactSliceCandidate(
                cube_id="N_TOT_POP",
                cube_ids=["N_TOT_POP"],
                label="Current Total Population",
                description="Total Population",
                cellref="TOT_POP:now",
                dataitem_id="N_TOT_POP_4",
                cat_id="C_TOT_POP_4",
                has_categories=True,
                category_entity=CategoryEntityRef(
                    entity_id="C_TOT_POP_4",
                    label="Current Total Population",
                    group_label="Total Population",
                    source="fake_metadata",
                    provenance="fake_metadata",
                ),
                start_year=1801,
                end_year=1901,
                observation_count=5,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="fake_exact_slice_catalog",
            ),
        ]

    def list_exact_slices(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ) -> list[ExactSliceCandidate]:
        candidates = self._supported_candidates(theme_id)
        if cube_id is not None:
            return [candidate for candidate in candidates if candidate.cube_id == cube_id]
        if cube_query:
            lowered = cube_query.lower()
            return [
                candidate
                for candidate in candidates
                if lowered in (candidate.label or "").lower()
                or lowered in (candidate.cube_id or "").lower()
            ]
        return candidates

    def list_catalog_inventory(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ) -> list[ExactSliceCandidate]:
        inventory = self._supported_candidates(theme_id)
        if cube_id is not None:
            inventory = [candidate for candidate in inventory if candidate.cube_id == cube_id]
        if theme_id != "T_POP":
            return inventory
        inventory.append(
            ExactSliceCandidate(
                cube_id="N_GENDER",
                cube_ids=["N_GENDER"],
                label="Population by gender",
                description="Gender breakdown",
                cellref="GENDER:male",
                dataitem_id="N_GENDER_1",
                cat_id="C_SEX_1",
                has_categories=True,
                category_entity=CategoryEntityRef(
                    entity_id="C_SEX_1",
                    label="Male",
                    group_label="Gender",
                    source="fake_metadata",
                    provenance="fake_metadata",
                ),
                start_year=1801,
                end_year=1901,
                observation_count=5,
                metadata_provenance="fake_metadata",
                slot_status="candidate_set",
                source="fake_exact_slice_catalog",
            )
        )
        if cube_query:
            lowered = cube_query.lower()
            inventory = [
                candidate
                for candidate in inventory
                if lowered in (candidate.label or "").lower()
                or lowered in (candidate.cube_id or "").lower()
                or lowered in (candidate.description or "").lower()
            ]
        return inventory

    def resolve_exact_slice(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ):
        candidates = self.list_exact_slices(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_id=cube_id,
            cube_query=cube_query,
        )
        resolved = None
        if len(candidates) == 1:
            resolved = ExactSliceRef.from_candidate(
                candidates[0],
                source="fake_exact_slice_catalog",
                slot_status="resolved",
            )
        return type("ExactSliceResolution", (), {"candidates": candidates, "resolved": resolved})()


class FakeSeriesService:
    def get_time_series(self, request) -> TimeSeriesResponse:
        unit_names = {101: "York", 102: "Leeds"}
        year = int(request.start_year) if request.start_year is not None else 1901
        return TimeSeriesResponse(
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            row_count=len(request.unit_ids),
            rows=[
                TimeSeriesRowResponse(
                    year=year,
                    unit_id=unit_id,
                    unit_name=unit_names.get(unit_id, f"Unit {unit_id}"),
                    unit_type="MOD_DIST",
                    cube_id=request.cube_ids[0],
                    cube_label="Total population",
                    cell_ref=request.cellrefs[0] if getattr(request, "cellrefs", None) else None,
                    dataitem_id=(
                        request.dataitem_ids[0]
                        if getattr(request, "dataitem_ids", None)
                        else None
                    ),
                    value=float(unit_id),
                )
                for unit_id in request.unit_ids
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
                    cell_ref=request.cellrefs[0] if getattr(request, "cellrefs", None) else None,
                    dataitem_id=(
                        request.dataitem_ids[0]
                        if getattr(request, "dataitem_ids", None)
                        else None
                    ),
                    cat_id=request.cat_ids[0] if getattr(request, "cat_ids", None) else None,
                    category_entity_id="C_SEX_F",
                    category_source="metadata",
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
        return PlaceKeyFindingsResponse(
            unit_id=unit_id,
            item_count=1,
            items=[
                PlaceKeyFindingResponse(
                    label="Population",
                    text="Population rose steadily.",
                )
            ],
        )

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
    places_service: Any | None = None,
    themes_service: Any | None = None,
    series_service: Any | None = None,
    maps_service: Any | None = None,
    metadata_service: Any | None = None,
    exact_slice_service: Any | None = None,
) -> ChatOrchestrator:
    return ChatOrchestrator(
        thread_store=store or InMemoryChatThreadStore(),
        planner=planner or FakePlanner(),
        llm_client=llm_client or FakeLLMClient(),
        places_service=places_service or FakePlacesService(),
        themes_service=themes_service or FakeThemesService(),
        series_service=series_service or FakeSeriesService(),
        maps_service=maps_service or FakeMapsService(),
        metadata_service=metadata_service or FakeMetadataService(),
        exact_slice_service=exact_slice_service or FakeExactSliceCatalogService(),
    )


def make_validated_analysis_spec(
    *,
    places: list[ResolvedPlaceResponse],
    unit_ids: list[int],
    output_mode: str = "trend_chart",
    year: int | None = None,
) -> AnalysisSpec:
    return AnalysisSpec(
        places=[],
        reporting_geography=ReportingGeographyRef(
            unit_type="MOD_DIST",
            unit_ids=unit_ids,
            label="Modern District",
            slot_status="resolved",
        ),
        dataset_family=DatasetFamilyRef(
            theme_id="T_POP",
            label="Population",
            description="Population counts",
            slot_status="resolved",
        ),
        exact_slice=ExactSliceRef(
            cube_id="N_POP_TOTAL",
            cube_ids=["N_POP_TOTAL"],
            label="Total population",
            dataitem_id="N_POP_TOTAL_1",
            slot_status="resolved",
        ),
        time_scope=(
            TimeScope(mode="snapshot", year=year, label=str(year), source="test")
            if year is not None
            else TimeScope(mode="range", start_year=1801, end_year=1901, label="1801-1901", source="test")
        ),
        output_mode=output_mode,
        comparison_mode="single_place" if len(places) <= 1 else "multi_place",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status="resolved",
        slice_status=SliceStatus.UNIQUE_EXECUTABLE_SLICE,
        comparability_status=ComparabilityStatus.COMPARABLE,
        availability_status=AvailabilityStatus.AVAILABLE,
        output_feasibility_status=OutputFeasibilityStatus.AVAILABLE,
    )


def seed_analysis_receipt(
    store: InMemoryChatThreadStore,
    *,
    places: list[ResolvedPlaceResponse],
    output_mode: str = "trend_chart",
    year: int | None = None,
) -> ChatThreadState:
    thread = store.create_thread()
    spec = make_validated_analysis_spec(
        places=places,
        unit_ids=[place.place.unit_ids[0] for place in places],
        output_mode=output_mode,
        year=year,
    )
    receipt = AnalysisReceipt(
        kind=ReceiptKind.ANALYSIS,
        receipt_id="ar_seed",
        path=WorkflowPath.ANALYSIS,
        source_operation=ChatOperation.FETCH_TIME_SERIES
        if output_mode != "boundary_map"
        else ChatOperation.FETCH_MAP_FEATURES,
        analysis_spec=spec,
        anchors={
            "places": [
                {"place_id": place.place.place_id, "label": place.place.name}
                for place in places
            ]
        },
        created_at=utc_now(),
    )
    thread.selected_places = list(places)
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread.analysis_state.resolved_places = list(places)
    thread.analysis_state.reporting_geography = spec.reporting_geography
    thread.analysis_state.dataset_family = spec.dataset_family
    thread.analysis_state.candidate_slice = spec.exact_slice
    thread.analysis_state.time_scope = spec.time_scope
    thread.analysis_state.output_mode = spec.output_mode
    thread.analysis_state.availability_status = spec.availability_status
    thread.analysis_state.comparability_status = spec.comparability_status
    thread.analysis_state.analysis_spec = spec
    thread.current_receipt = receipt
    thread.current_receipt_id = receipt.receipt_id
    thread.recent_receipts = [receipt]
    thread = store.save_thread(thread)
    store.save_receipt(thread.thread_id, receipt)
    return store.require_thread(thread.thread_id)


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
    assert response.thread_state.resolution_state.resolved_places[0].place.name == "York"
    assert response.thread_state.ui_projection is not None
    assert response.thread_state.ui_projection.selected_places[0].place.name == "York"
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


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


def test_semantic_models_validate_and_thread_state_supports_rich_state() -> None:
    now = utc_now()
    selected_place = make_resolved_place()
    reporting_geography = ReportingGeographyRef(
        unit_type="MOD_DIST",
        unit_ids=[101],
        label="Modern District",
    )
    analysis_spec = AnalysisSpec(
        places=[],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        exact_slice=ExactSliceRef(cube_id="N_POP_TOTAL", label="Total population"),
        time_scope=TimeScope(mode="range", start_year=1801, end_year=1901),
        output_mode="trend_chart",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status="resolved",
        slice_status=SliceStatus.FAMILY_ONLY,
        comparability_status=ComparabilityStatus.COMPARABLE,
        availability_status=AvailabilityStatus.AVAILABLE,
        output_feasibility_status="available",
    )
    receipt = AnalysisReceipt(
        kind=ReceiptKind.ANALYSIS,
        receipt_id="ar_test",
        path=WorkflowPath.ANALYSIS,
        analysis_spec=analysis_spec,
        defaults_used=["Used selected cube context."],
        projections={"ui_projection_id": "ui_test", "render_projection_id": "rp_test"},
        ui_projection=UIProjection(
            projection_id="ui_test",
            selected_places=[selected_place],
            reporting_geography=reporting_geography,
        ),
        created_at=now,
    )
    thread_state = ChatThreadState(
        thread_id="thread-rich",
        created_at=now,
        updated_at=now,
        messages=[],
        conversation_state=ConversationState(current_focus="analysis"),
        resolution_state=ResolutionState(resolved_places=[selected_place]),
        current_receipt=receipt,
        recent_receipts=[receipt],
        ui_projection=receipt.ui_projection,
    )

    assert thread_state.current_receipt_id == "ar_test"
    assert thread_state.conversation_state.current_receipt_id == "ar_test"
    assert thread_state.recent_receipts[0].analysis_spec.slice_status == SliceStatus.FAMILY_ONLY
    assert thread_state.ui_projection.reporting_geography.unit_type == "MOD_DIST"


def test_thread_store_persists_receipts_and_hydrates_thread_snapshot() -> None:
    store = InMemoryChatThreadStore()
    thread = store.create_thread()
    receipt = AnalysisReceipt(
        kind=ReceiptKind.DISCOVERY,
        receipt_id="dr_test",
        path=WorkflowPath.DISCOVERY,
        defaults_used=[],
        projections={"ui_projection_id": "ui_test"},
        created_at=utc_now(),
    )

    saved_receipt = store.save_receipt(thread.thread_id, receipt)
    hydrated_thread = store.get_thread(thread.thread_id)

    assert saved_receipt.receipt_id == "dr_test"
    assert store.get_latest_receipt(thread.thread_id).receipt_id == "dr_test"
    assert store.list_receipts(thread.thread_id)[0].receipt_id == "dr_test"
    assert hydrated_thread is not None
    assert hydrated_thread.current_receipt_id == "dr_test"
    assert hydrated_thread.current_receipt.receipt_id == "dr_test"
    assert hydrated_thread.conversation_state.recent_receipt_ids == ["dr_test"]


def test_chat_orchestrator_writes_guarded_discovery_state_for_family_only_analysis() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Loaded York population."))
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population over time",
                stream=False,
            )
        )
    )

    assert response.thread_state.analysis_state.reporting_geography is not None
    assert response.thread_state.analysis_state.reporting_geography.unit_type == "MOD_DIST"
    assert response.thread_state.analysis_state.analysis_spec is not None
    assert response.thread_state.analysis_state.analysis_spec.slice_status == SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.DISCOVERY
    assert response.ui_delta.execution_guard_result is not None
    assert response.ui_delta.execution_guard_result.outcome == ExecutionGuardOutcome.DISCOVERY_RESPONSE
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY
    assert response.thread_state.selected_cubes == []
    assert response.ui_delta.time_series is None
    assert [cube.cube_id for cube in response.ui_delta.cubes] == ["N_POP_TOTAL", "N_POP_WAY"]
    assert response.thread_state.render_projection is not None
    assert response.thread_state.render_projection.answer_text == "Loaded York population."
    assert store.get_latest_receipt(thread.thread_id).receipt_id == response.thread_state.current_receipt_id


def test_capability_registry_explicitly_represents_par_unit_policy() -> None:
    registry = CapabilityRegistry()

    par_unit = registry.geography("PAR_UNIT")

    assert par_unit is not None
    assert par_unit.discovery is True
    assert par_unit.info_lookup is True
    assert par_unit.boundary_map is True
    assert par_unit.thematic_map is False
    assert par_unit.map_loading_policy == "bbox_or_ids_only"


def test_decision_policy_and_guard_block_family_only_analysis() -> None:
    registry = CapabilityRegistry()
    decision_engine = DecisionPolicyEngine()
    execution_guard = ExecutionGuard()
    reporting_geography = ReportingGeographyRef(
        unit_type="MOD_DIST",
        unit_ids=[101],
        label="Modern District",
        slot_status="resolved",
    )
    context = ResolvedAnalysisContext(
        operation=ChatOperation.FETCH_TIME_SERIES,
        output_mode="trend_chart",
        places=[make_resolved_place()],
        reporting_geography_candidates=[reporting_geography],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        cube_candidates=FakeThemesService().list_cubes_for_unit_theme(101, "T_POP").items,
        exact_slice=ExactSliceRef(cube_id="N_POP_TOTAL", label="Total population"),
        comparison_mode="single_place",
    )
    spec = AnalysisSpec(
        places=[],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        exact_slice=ExactSliceRef(cube_id="N_POP_TOTAL", label="Total population"),
        output_mode="trend_chart",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status="resolved",
        slice_status=SliceStatus.FAMILY_ONLY,
        comparability_status=ComparabilityStatus.COMPARABLE,
        availability_status=AvailabilityStatus.PARTIALLY_AVAILABLE,
        output_feasibility_status=OutputFeasibilityStatus.AVAILABLE,
    )

    decision = decision_engine.decide(
        context=context,
        spec=spec,
        capability_registry=registry,
    )
    guard_result = execution_guard.evaluate(
        context=context,
        spec=spec,
        decision=decision,
        capability_registry=registry,
    )

    assert decision.outcome == PolicyOutcome.DISCOVERY
    assert decision.reason == "exact_slice_is_not_proven_executable"
    assert guard_result.outcome == ExecutionGuardOutcome.DISCOVERY_RESPONSE
    assert guard_result.blocking_fields == ["exact_slice"]


def test_unsupported_output_mode_returns_deterministic_policy_outcome() -> None:
    registry = CapabilityRegistry()
    decision_engine = DecisionPolicyEngine()
    execution_guard = ExecutionGuard()
    reporting_geography = ReportingGeographyRef(
        unit_type="MOD_DIST",
        unit_ids=[101],
        label="Modern District",
        slot_status="resolved",
    )
    context = ResolvedAnalysisContext(
        operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
        output_mode="category_chart",
        places=[make_resolved_place()],
        reporting_geography_candidates=[reporting_geography],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        comparison_mode="single_place",
    )
    spec = AnalysisSpec(
        places=[],
        reporting_geography=reporting_geography,
        dataset_family=DatasetFamilyRef(theme_id="T_POP", label="Population"),
        output_mode="category_chart",
        reporting_geography_status=ReportingGeographyStatus.RESOLVED,
        dataset_status="resolved",
        slice_status=SliceStatus.NEEDS_EXACT_SLICE_RESOLUTION,
        comparability_status=ComparabilityStatus.COMPARABLE,
        availability_status=AvailabilityStatus.PARTIALLY_AVAILABLE,
        output_feasibility_status=OutputFeasibilityStatus.UNSUPPORTED,
    )

    decision = decision_engine.decide(
        context=context,
        spec=spec,
        capability_registry=registry,
    )
    guard_result = execution_guard.evaluate(
        context=context,
        spec=spec,
        decision=decision,
        capability_registry=registry,
    )

    assert decision.outcome == PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES
    assert guard_result.outcome == ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES


def test_chat_orchestrator_clarifies_ambiguous_place_before_analysis() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                place_query="Newport",
                theme_query="Population",
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Need clarification."),
        places_service=AmbiguousPlacesService(),
    )
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population for Newport",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert len(response.ui_delta.place_search_results) == 2
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.ASK
    assert response.ui_delta.execution_guard_result is not None
    assert response.ui_delta.execution_guard_result.outcome == ExecutionGuardOutcome.NEEDS_CLARIFICATION
    assert response.thread_state.current_receipt is None


def test_chat_orchestrator_requires_explicit_reporting_geography_for_multi_geo_place() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_TIME_SERIES,
                theme_query="Population",
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Need geography."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show population over time",
                stream=False,
            )
        )
    )

    assert response.ui_delta.time_series is None
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.ASK
    assert response.ui_delta.pending_clarification is not None
    assert response.ui_delta.pending_clarification.slot == "reporting_geography"
    assert {option.option_id for option in response.ui_delta.pending_clarification.options} == {
        "MOD_DIST",
        "PAR_UNIT",
    }
    assert response.thread_state.analysis_state.reporting_geography is None
    assert response.thread_state.current_receipt is None


def test_chat_orchestrator_routes_exploratory_turns_to_discovery() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.LIST_THEMES,
                place_query="York",
                confidence=0.8,
            )
        ),
        llm_client=FakeLLMClient(text="Themes available."),
    )
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what themes are available for York?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.operation == ChatOperation.LIST_THEMES
    assert response.ui_delta.themes is not None
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_chat_orchestrator_blocks_category_breakdown_as_unavailable_in_v1() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        planner=StaticPlanner(
            PlannerAction(
                operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                theme_query="Population",
                confidence=0.9,
            )
        ),
        llm_client=FakeLLMClient(text="Categories unavailable."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show categories",
                stream=False,
            )
        )
    )

    assert response.ui_delta.category_breakdown is None
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.UNAVAILABLE_WITH_ALTERNATIVES
    assert response.ui_delta.execution_guard_result is not None
    assert (
        response.ui_delta.execution_guard_result.outcome
        == ExecutionGuardOutcome.UNAVAILABLE_WITH_ALTERNATIVES
    )
    assert response.thread_state.current_receipt is None


def test_follow_up_replace_place_reuses_receipt_with_replace_place_patch() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Leeds loaded."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="same for Leeds", stream=False)
        )
    )

    assert [place.place.name for place in response.thread_state.selected_places] == ["Leeds"]
    assert response.ui_delta.time_series is not None
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.ANALYSIS
    assert (
        response.thread_state.current_receipt.execution_summary["follow_up_patch"]["patch_type"]
        == "ReplacePlace"
    )
    assert response.thread_state.current_receipt.analysis_spec.reporting_geography.unit_ids == [102]


def test_follow_up_add_place_reuses_receipt_with_add_place_patch() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Compared York and Leeds."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="compare that with Leeds",
                stream=False,
            )
        )
    )

    assert {place.place.name for place in response.thread_state.selected_places} == {"York", "Leeds"}
    assert response.ui_delta.time_series is not None
    assert set(response.ui_delta.time_series.unit_ids) == {101, 102}
    assert (
        response.thread_state.current_receipt.execution_summary["follow_up_patch"]["patch_type"]
        == "AddPlace"
    )


def test_follow_up_remove_place_reuses_receipt_with_remove_place_patch() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Removed Leeds."))
    thread = seed_analysis_receipt(
        store,
        places=[
            make_resolved_place(),
            make_resolved_place(place_id=4, name="Leeds", unit_id=102),
        ],
    )

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="remove Leeds", stream=False)
        )
    )

    assert [place.place.name for place in response.thread_state.selected_places] == ["York"]
    assert response.ui_delta.time_series is not None
    assert response.ui_delta.time_series.unit_ids == [101]
    assert (
        response.thread_state.current_receipt.execution_summary["follow_up_patch"]["patch_type"]
        == "RemovePlace"
    )


def test_follow_up_set_time_reuses_receipt_with_set_time_patch() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Showing 1901."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="use 1901 instead", stream=False)
        )
    )

    assert response.ui_delta.time_series is not None
    assert response.ui_delta.time_series.rows[0].year == 1901
    assert response.thread_state.current_receipt.analysis_spec.time_scope.year == 1901
    assert (
        response.thread_state.current_receipt.execution_summary["follow_up_patch"]["patch_type"]
        == "SetTime"
    )


def test_follow_up_set_output_mode_to_table_reuses_receipt_safely() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Here is the table."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="show the table", stream=False)
        )
    )

    assert response.ui_delta.time_series is not None
    assert response.thread_state.render_projection is not None
    assert response.thread_state.render_projection.table is not None
    assert response.thread_state.current_receipt.analysis_spec.output_mode == "table"
    assert (
        response.thread_state.current_receipt.execution_summary["follow_up_patch"]["patch_type"]
        == "SetOutputMode"
    )


def test_follow_up_map_that_defaults_safely_to_boundary_map() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Showing a map."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="map that", stream=False)
        )
    )

    assert response.ui_delta.map_features is not None
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.DEFAULT_WITH_DISCLOSURE
    assert response.thread_state.current_receipt.analysis_spec.output_mode == "boundary_map"


def test_explicit_discovery_geographies_returns_structured_discovery_receipt() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Geographies available."))
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="which geographies can I use here?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.REPORTING_GEOGRAPHIES
    assert {item.item_id for item in response.ui_delta.discovery_result.items} == {
        "MOD_DIST",
        "PAR_UNIT",
    }
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY
    assert response.thread_state.current_receipt.discovery_result is not None


def test_explicit_discovery_outputs_returns_structured_feasible_outputs() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Safe outputs available."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what outputs are feasible here?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.FEASIBLE_OUTPUTS
    assert {item.item_id for item in response.ui_delta.discovery_result.items} == {
        "trend_chart",
        "table",
        "boundary_map",
        "category_chart",
    }
    output_map = {
        item.output_mode: item for item in response.ui_delta.discovery_result.supported_outputs
    }
    assert output_map["category_chart"].support_level == CapabilitySupportLevel.DISCOVERY_ONLY
    assert output_map["category_chart"].feasibility == OutputFeasibilityStatus.UNSUPPORTED
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_follow_up_ambiguous_reference_produces_clarification_instead_of_guess() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        llm_client=FakeLLMClient(text="Need clarification."),
        places_service=AmbiguousPlacesService(),
    )
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=thread.thread_id, message="same for Newport", stream=False)
        )
    )

    assert response.ui_delta.time_series is None
    assert response.ui_delta.pending_clarification is not None
    assert response.ui_delta.pending_clarification.slot == "place_identity"
    assert len(response.ui_delta.pending_clarification.options) == 2
    assert response.ui_delta.policy_decision is not None
    assert response.ui_delta.policy_decision.outcome == PolicyOutcome.ASK


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
    assert "conversation_state" in fetched.json()
    assert "resolution_state" in fetched.json()
    assert "analysis_state" in fetched.json()

    turn = client.post(
        "/chat/turn",
        json={"thread_id": thread_id, "message": "Add York", "stream": False},
    )
    assert turn.status_code == 200
    assert turn.json()["status"] == "completed"
    assert turn.json()["thread_state"]["selected_places"][0]["place"]["name"] == "York"
    assert turn.json()["thread_state"]["current_receipt"]["kind"] == "discovery"
    assert turn.json()["ui_delta"]["ui_projection"]["selected_places"][0]["place"]["name"] == "York"


def test_chat_stream_endpoint_returns_404_for_missing_thread(chat_client) -> None:
    client, _store = chat_client
    response = client.get("/chat/stream/missing-thread")

    assert response.status_code == 404
