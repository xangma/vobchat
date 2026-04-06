from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, TypeVar

import httpx

from vobchat.api.schemas.chat import (
    ChatThreadCreateResponse,
    ChatThreadState,
    ChatTurnAcceptedResponse,
    ChatTurnRequest,
    ChatTurnResponse,
)
from vobchat.api.schemas.maps import (
    MapFeatureCollectionResponse,
    MapFeaturesByIdsQuery,
    MapFeaturesQuery,
)
from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityResolutionResponse,
    DataEntityResolveQuery,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.schemas.places import (
    PlaceResolveQuery,
    PlaceSearchQuery,
    PlaceSearchResponse,
    PostcodeLookupQuery,
    PostcodeLookupResponse,
    ResolvedPlaceResponse,
    UnitDetailsQuery,
    UnitDetailsResponse,
)
from vobchat.api.schemas.series import (
    CategoryBreakdownRequest,
    CategoryBreakdownResponse,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from vobchat.api.schemas.themes import (
    CubeListResponse,
    ThemeListQuery,
    ThemeListResponse,
    ThemeResolutionResponse,
    ThemeResolveQuery,
    ThemesForUnitResponse,
)
from vobchat.core.settings import get_settings


ResponseModelT = TypeVar("ResponseModelT")


@dataclass(frozen=True)
class APIClientConfig:
    base_url: str
    timeout_seconds: float = 30.0


class APIClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail

    @classmethod
    def from_response(cls, response: httpx.Response) -> "APIClientError":
        detail: Any
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if isinstance(payload, dict):
            detail = payload.get("detail") or payload
        else:
            detail = payload or response.text or response.reason_phrase

        message = f"API request failed with status {response.status_code}"
        if detail:
            message = f"{message}: {detail}"
        return cls(message, status_code=response.status_code, detail=detail)


class APIClient:
    def __init__(
        self,
        config: APIClientConfig | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        settings = get_settings()
        self.config = config or APIClientConfig(base_url=settings.api.internal_base_url)
        self._transport = transport

    @property
    def base_url(self) -> str:
        return self.config.base_url.rstrip("/")

    def _build_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.config.timeout_seconds,
            transport=self._transport,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ResponseModelT] | None = None,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> ResponseModelT | dict[str, Any]:
        with self._build_client() as client:
            response = client.request(method, path, params=params, json=json_body)

        if response.is_error:
            raise APIClientError.from_response(response)

        payload = response.json()
        if response_model is None:
            return payload
        return response_model.model_validate(payload)

    def create_thread(self) -> ChatThreadCreateResponse:
        return self._request(
            "POST",
            "/chat/threads",
            response_model=ChatThreadCreateResponse,
        )

    def get_thread(self, thread_id: str) -> ChatThreadState:
        return self._request(
            "GET",
            f"/chat/threads/{thread_id}",
            response_model=ChatThreadState,
        )

    def submit_turn(
        self,
        request: ChatTurnRequest,
    ) -> ChatTurnAcceptedResponse | ChatTurnResponse:
        payload = self._request(
            "POST",
            "/chat/turn",
            json_body=request.model_dump(mode="json"),
        )
        status = payload.get("status")
        if status == "accepted":
            return ChatTurnAcceptedResponse.model_validate(payload)
        return ChatTurnResponse.model_validate(payload)

    def search_places(self, query: PlaceSearchQuery) -> PlaceSearchResponse:
        return self._request(
            "GET",
            "/places/search",
            response_model=PlaceSearchResponse,
            params=query.model_dump(exclude_none=True),
        )

    def lookup_postcode(self, query: PostcodeLookupQuery) -> PostcodeLookupResponse:
        return self._request(
            "GET",
            "/places/postcode",
            response_model=PostcodeLookupResponse,
            params=query.model_dump(exclude_none=True),
        )

    def get_place(
        self,
        place_id: int,
        query: PlaceResolveQuery | None = None,
    ) -> ResolvedPlaceResponse:
        return self._request(
            "GET",
            f"/places/{place_id}",
            response_model=ResolvedPlaceResponse,
            params=(query or PlaceResolveQuery()).model_dump(exclude_none=True),
        )

    def get_unit_details(self, query: UnitDetailsQuery) -> UnitDetailsResponse:
        return self._request(
            "GET",
            "/places/units/details",
            response_model=UnitDetailsResponse,
            params=query.model_dump(exclude_none=True),
        )

    def get_place_for_unit(
        self,
        unit_id: int,
        query: PlaceResolveQuery | None = None,
    ) -> ResolvedPlaceResponse:
        return self._request(
            "GET",
            f"/places/units/{unit_id}/place",
            response_model=ResolvedPlaceResponse,
            params=(query or PlaceResolveQuery()).model_dump(exclude_none=True),
        )

    def list_themes(self, query: ThemeListQuery | None = None) -> ThemeListResponse:
        return self._request(
            "GET",
            "/themes",
            response_model=ThemeListResponse,
            params=(query or ThemeListQuery()).model_dump(exclude_none=True),
        )

    def resolve_theme(self, query: ThemeResolveQuery) -> ThemeResolutionResponse:
        return self._request(
            "GET",
            "/themes/resolve",
            response_model=ThemeResolutionResponse,
            params=query.model_dump(exclude_none=True),
        )

    def list_themes_for_unit(self, unit_id: int) -> ThemesForUnitResponse:
        return self._request(
            "GET",
            f"/themes/for-unit/{unit_id}",
            response_model=ThemesForUnitResponse,
        )

    def list_cubes_for_unit_theme(self, unit_id: int, theme_id: str) -> CubeListResponse:
        return self._request(
            "GET",
            f"/themes/{theme_id}/cubes",
            response_model=CubeListResponse,
            params={"unit_id": unit_id},
        )

    def fetch_time_series(self, request: TimeSeriesRequest) -> TimeSeriesResponse:
        return self._request(
            "POST",
            "/series/time",
            response_model=TimeSeriesResponse,
            json_body=request.model_dump(mode="json"),
        )

    def fetch_category_breakdown(
        self,
        request: CategoryBreakdownRequest,
    ) -> CategoryBreakdownResponse:
        return self._request(
            "POST",
            "/series/categories",
            response_model=CategoryBreakdownResponse,
            json_body=request.model_dump(mode="json"),
        )

    def fetch_features(self, query: MapFeaturesQuery) -> MapFeatureCollectionResponse:
        return self._request(
            "GET",
            "/maps/features",
            response_model=MapFeatureCollectionResponse,
            params=query.model_dump(exclude_none=True),
        )

    def fetch_features_by_ids(
        self,
        query: MapFeaturesByIdsQuery,
    ) -> MapFeatureCollectionResponse:
        return self._request(
            "GET",
            "/maps/features/by-ids",
            response_model=MapFeatureCollectionResponse,
            params=query.model_dump(exclude_none=True),
        )

    def get_place_profile(self, place_id: int) -> PlaceProfileResponse:
        return self._request(
            "GET",
            f"/metadata/place-profile/{place_id}",
            response_model=PlaceProfileResponse,
        )

    def get_place_key_findings(self, unit_id: int) -> PlaceKeyFindingsResponse:
        return self._request(
            "GET",
            f"/metadata/place-key-findings/{unit_id}",
            response_model=PlaceKeyFindingsResponse,
        )

    def get_unit_type_info(self, unit_type: str) -> UnitTypeInfoResponse:
        return self._request(
            "GET",
            f"/metadata/unit-types/{unit_type}",
            response_model=UnitTypeInfoResponse,
        )

    def resolve_data_entity(
        self,
        query: DataEntityResolveQuery,
    ) -> DataEntityResolutionResponse:
        return self._request(
            "GET",
            "/metadata/entities/resolve",
            response_model=DataEntityResolutionResponse,
            params=query.model_dump(exclude_none=True),
        )

    def get_data_entity_info(self, entity_id: str) -> DataEntityInfoResponse:
        return self._request(
            "GET",
            f"/metadata/entities/{entity_id}",
            response_model=DataEntityInfoResponse,
        )


@lru_cache(maxsize=1)
def get_api_client() -> APIClient:
    return APIClient()
