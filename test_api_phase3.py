from __future__ import annotations

import pytest
from fastapi.testclient import TestClient as FastAPITestClient

from vobchat.api.main import app as api_app
from vobchat.api.schemas.maps import MapFeatureCollectionResponse, MapFeatureResponse
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityReferenceResponse,
    DataEntityResolutionResponse,
    PlaceKeyFindingResponse,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
    UnitTypeRelationResponse,
    UnitTypeStatusResponse,
)
from vobchat.api.schemas.places import (
    PlaceCandidateResponse,
    PlaceSearchResponse,
    PostcodeLookupResponse,
    PostcodeLookupResultResponse,
    ResolvedPlaceResponse,
    UnitDetailResponse,
    UnitDetailsResponse,
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
    ThemesForUnitResponse,
)
from vobchat.api.services.maps import get_maps_service
from vobchat.api.services.metadata import get_metadata_service
from vobchat.api.services.places import get_places_service
from vobchat.api.services.series import get_series_service
from vobchat.api.services.themes import get_themes_service


@pytest.fixture()
def client() -> FastAPITestClient:
    api_app.dependency_overrides.clear()
    with FastAPITestClient(api_app) as test_client:
        yield test_client
    api_app.dependency_overrides.clear()


def test_health_and_ready_endpoints(client: FastAPITestClient) -> None:
    health = client.get("/healthz")
    ready = client.get("/readyz")

    assert health.status_code == 200
    assert health.json()["service"] == "api"
    assert health.json()["status"] == "ok"

    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert "auth_db_url" in ready.json()["details"]


def test_places_routes_return_typed_shapes(client: FastAPITestClient) -> None:
    class FakePlacesService:
        def search_places(self, query):
            assert query.query == "York"
            return PlaceSearchResponse(
                query=query.query,
                match_mode=query.match_mode,
                result_count=1,
                results=[
                    PlaceCandidateResponse(
                        place_id=1,
                        name="York",
                        county_id=2,
                        county_name="Yorkshire",
                        nation_id=3,
                        nation_name="England",
                        latitude=53.96,
                        longitude=-1.08,
                        unit_ids=[101],
                        unit_types=["MOD_DIST"],
                        match_type=query.match_mode,
                    )
                ],
            )

        def lookup_postcode(self, query):
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
                        county_name="Yorkshire",
                        max_area=12.5,
                    )
                ],
            )

        def resolve_place(self, place_id, query):
            return ResolvedPlaceResponse(
                place=PlaceCandidateResponse(
                    place_id=place_id,
                    name="York",
                    unit_ids=[101],
                    unit_types=query.unit_types or ["MOD_DIST"],
                    match_type="resolved",
                ),
                units=[
                    UnitDetailResponse(
                        unit_id=101,
                        unit_name="York",
                        unit_type="MOD_DIST",
                        unit_type_label="Modern District",
                    )
                ],
            )

        def get_unit_details(self, query):
            return UnitDetailsResponse(
                unit_ids=query.unit_ids,
                units=[
                    UnitDetailResponse(
                        unit_id=unit_id,
                        unit_name=f"Unit {unit_id}",
                        unit_type="MOD_DIST",
                        unit_type_label="Modern District",
                    )
                    for unit_id in query.unit_ids
                ],
            )

    api_app.dependency_overrides[get_places_service] = lambda: FakePlacesService()

    search = client.get("/places/search", params={"query": "York", "match_mode": "fuzzy"})
    postcode = client.get("/places/postcode", params={"postcode": "yo1 7ep"})
    details = client.get("/places/units/details", params=[("unit_ids", 101), ("unit_ids", 102)])
    resolved = client.get("/places/1", params={"unit_types": ["MOD_DIST"]})

    assert search.status_code == 200
    assert search.json()["results"][0]["name"] == "York"
    assert search.json()["result_count"] == 1

    assert postcode.status_code == 200
    assert postcode.json()["results"][0]["postcode"] == "YO17EP"

    assert details.status_code == 200
    assert details.json()["unit_ids"] == [101, 102]
    assert details.json()["units"][1]["unit_name"] == "Unit 102"

    assert resolved.status_code == 200
    assert resolved.json()["place"]["match_type"] == "resolved"
    assert resolved.json()["units"][0]["unit_type_label"] == "Modern District"


def test_themes_routes_return_typed_shapes(client: FastAPITestClient) -> None:
    class FakeThemesService:
        def list_themes(self, query):
            return ThemeListResponse(
                item_count=1,
                items=[
                    ThemeSummaryResponse(
                        theme_id="T_POP",
                        label="Population",
                        description="Population measures",
                    )
                ],
            )

        def resolve_theme(self, query):
            return ThemeResolutionResponse(
                query=query.query,
                result=ThemeSummaryResponse(
                    theme_id="T_POP",
                    label="Population",
                    description="Population measures",
                ),
            )

        def list_themes_for_unit(self, unit_id):
            return ThemesForUnitResponse(
                unit_id=unit_id,
                item_count=1,
                items=[ThemeSummaryResponse(theme_id="T_POP", label="Population")],
            )

        def list_cubes_for_unit_theme(self, unit_id, theme_id):
            return CubeListResponse(
                unit_id=unit_id,
                theme_id=theme_id,
                item_count=1,
                items=[
                    CubeSummaryResponse(
                        theme_id=theme_id,
                        cube_id="N_POP_TOTAL",
                        label="Total population",
                        description="Population total",
                        start_year=1801,
                        end_year=1901,
                        observation_count=5,
                        has_categories=False,
                    )
                ],
            )

    api_app.dependency_overrides[get_themes_service] = lambda: FakeThemesService()

    listed = client.get("/themes")
    resolved = client.get("/themes/resolve", params={"query": "Population"})
    for_unit = client.get("/themes/for-unit/101")
    cubes = client.get("/themes/T_POP/cubes", params={"unit_id": 101})

    assert listed.status_code == 200
    assert listed.json()["items"][0]["theme_id"] == "T_POP"

    assert resolved.status_code == 200
    assert resolved.json()["result"]["label"] == "Population"

    assert for_unit.status_code == 200
    assert for_unit.json()["unit_id"] == 101

    assert cubes.status_code == 200
    assert cubes.json()["items"][0]["cube_id"] == "N_POP_TOTAL"


def test_series_routes_accept_typed_bodies_and_return_typed_shapes(
    client: FastAPITestClient,
) -> None:
    class FakeSeriesService:
        def get_time_series(self, request):
            assert request.unit_ids == [101]
            return TimeSeriesResponse(
                unit_ids=request.unit_ids,
                cube_ids=request.cube_ids,
                row_count=1,
                rows=[
                    TimeSeriesRowResponse(
                        year=1901,
                        unit_id=101,
                        unit_name="York",
                        unit_type="MOD_DIST",
                        cube_id="N_POP_TOTAL",
                        cube_label="Total population",
                        value=42.0,
                    )
                ],
            )

        def get_category_breakdown(self, request):
            assert request.year == 1901
            return CategoryBreakdownResponse(
                year=request.year,
                unit_ids=request.unit_ids,
                cube_ids=request.cube_ids,
                row_count=1,
                rows=[
                    CategoryRowResponse(
                        year=request.year,
                        unit_id=101,
                        unit_name="York",
                        cube_id="N_POP_WAY",
                        category_group="sex",
                        category_label="Female",
                        value=21.0,
                        cell_ref="sex:Female",
                    )
                ],
            )

    api_app.dependency_overrides[get_series_service] = lambda: FakeSeriesService()

    time_series = client.post(
        "/series/time",
        json={"unit_ids": [101], "cube_ids": ["N_POP_TOTAL"], "start_year": 1901},
    )
    categories = client.post(
        "/series/categories",
        json={"unit_ids": [101], "cube_ids": ["N_POP_WAY"], "year": 1901},
    )

    assert time_series.status_code == 200
    assert time_series.json()["rows"][0]["cube_id"] == "N_POP_TOTAL"
    assert time_series.json()["row_count"] == 1

    assert categories.status_code == 200
    assert categories.json()["rows"][0]["category_label"] == "Female"
    assert categories.json()["year"] == 1901


def test_maps_routes_return_typed_feature_collections(client: FastAPITestClient) -> None:
    class FakeMapsService:
        def get_features(self, query):
            assert query.unit_type == "MOD_DIST"
            return MapFeatureCollectionResponse(
                mode="bbox" if query.has_bbox else "all",
                unit_type=query.unit_type,
                feature_count=1,
                start_year=query.start_year,
                end_year=query.end_year,
                theme_id=query.theme_id,
                bbox=query.bbox,
                requested_ids=[],
                features=[
                    MapFeatureResponse(
                        unit_id=101,
                        unit_name="York",
                        unit_type=query.unit_type,
                        geometry={"type": "Polygon", "coordinates": []},
                        start_year=1901,
                        end_year=1901,
                        has_theme=True,
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
                    )
                    for unit_id in query.ids
                ],
            )

    api_app.dependency_overrides[get_maps_service] = lambda: FakeMapsService()

    features = client.get(
        "/maps/features",
        params={
            "unit_type": "MOD_DIST",
            "min_x": -2.0,
            "min_y": 53.0,
            "max_x": -1.0,
            "max_y": 54.0,
            "theme_id": "T_POP",
        },
    )
    by_ids = client.get(
        "/maps/features/by-ids",
        params=[("unit_type", "MOD_DIST"), ("ids", 101), ("ids", 102)],
    )

    assert features.status_code == 200
    assert features.json()["mode"] == "bbox"
    assert features.json()["features"][0]["geometry"]["type"] == "Polygon"

    assert by_ids.status_code == 200
    assert by_ids.json()["mode"] == "ids"
    assert by_ids.json()["requested_ids"] == [101, 102]


def test_metadata_routes_return_typed_shapes(client: FastAPITestClient) -> None:
    class FakeMetadataService:
        def get_place_profile(self, place_id):
            return PlaceProfileResponse(
                place_id=place_id,
                name="York",
                county_name="Yorkshire",
                text="Historic city",
            )

        def get_place_key_findings(self, unit_id):
            return PlaceKeyFindingsResponse(
                unit_id=unit_id,
                item_count=1,
                items=[
                    PlaceKeyFindingResponse(
                        label="Key point",
                        text="Population grew sharply",
                    )
                ],
            )

        def get_unit_type_info(self, unit_type):
            return UnitTypeInfoResponse(
                identifier=unit_type,
                label="Modern District",
                level=2,
                level_label="District",
                unit_count=5,
                may_be_part_of=[
                    UnitTypeRelationResponse(unit_type="MOD_CNTY", label="Modern County")
                ],
                statuses=[UnitTypeStatusResponse(code="A", label="Active")],
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
            )

    api_app.dependency_overrides[get_metadata_service] = lambda: FakeMetadataService()

    profile = client.get("/metadata/place-profile/1")
    findings = client.get("/metadata/place-key-findings/101")
    unit_type = client.get("/metadata/unit-types/MOD_DIST")
    resolved = client.get("/metadata/entities/resolve", params={"query": "population"})
    entity = client.get("/metadata/entities/N_POP_TOTAL")

    assert profile.status_code == 200
    assert profile.json()["name"] == "York"

    assert findings.status_code == 200
    assert findings.json()["items"][0]["label"] == "Key point"

    assert unit_type.status_code == 200
    assert unit_type.json()["statuses"][0]["code"] == "A"

    assert resolved.status_code == 200
    assert resolved.json()["result"]["entity_id"] == "N_POP_TOTAL"

    assert entity.status_code == 200
    assert entity.json()["type_name"] == "Measure"
