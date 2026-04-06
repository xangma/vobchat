from __future__ import annotations

from vobchat.api.schemas.places import (
    PlaceCandidateResponse,
    PlaceResolveQuery,
    PlaceSearchQuery,
    PlaceSearchResponse,
    PostcodeLookupQuery,
    PostcodeLookupResponse,
    PostcodeLookupResultResponse,
    ResolvedPlaceResponse,
    UnitDetailResponse,
    UnitDetailsQuery,
    UnitDetailsResponse,
)
from vobchat.db.repositories import PlacesRepository


class PlacesService:
    def __init__(self, repository: PlacesRepository | None = None) -> None:
        self.repository = repository or PlacesRepository()

    def search_places(self, query: PlaceSearchQuery) -> PlaceSearchResponse:
        unit_types = query.unit_types or None
        if query.match_mode == "exact":
            results = self.repository.search_places_exact(
                query.query,
                county_id=query.county_id,
                unit_types=unit_types,
                nation_id=query.nation_id,
                domain_id=query.domain_id,
                state_id=query.state_id,
                limit=query.limit,
            )
        else:
            results = self.repository.search_places_fuzzy(
                query.query,
                county_id=query.county_id,
                unit_types=unit_types,
                nation_id=query.nation_id,
                domain_id=query.domain_id,
                state_id=query.state_id,
                limit=query.limit,
            )
        return PlaceSearchResponse(
            query=query.query,
            match_mode=query.match_mode,
            result_count=len(results),
            results=[
                PlaceCandidateResponse.model_validate(result) for result in results
            ],
        )

    def lookup_postcode(self, query: PostcodeLookupQuery) -> PostcodeLookupResponse:
        results = self.repository.lookup_postcode_units(
            postcode=query.postcode,
            unit_type=query.unit_type,
        )
        return PostcodeLookupResponse(
            postcode=query.postcode,
            unit_type=query.unit_type,
            result_count=len(results),
            results=[
                PostcodeLookupResultResponse.model_validate(result) for result in results
            ],
        )

    def resolve_place(
        self,
        place_id: int,
        query: PlaceResolveQuery,
    ) -> ResolvedPlaceResponse | None:
        result = self.repository.resolve_place(
            place_id,
            unit_types=query.unit_types or None,
        )
        if result is None:
            return None
        return ResolvedPlaceResponse.model_validate(result)

    def resolve_place_for_unit(
        self,
        unit_id: int,
        query: PlaceResolveQuery | None = None,
    ) -> ResolvedPlaceResponse | None:
        result = self.repository.resolve_place_for_unit(
            unit_id,
            unit_types=(query.unit_types if query else None) or None,
        )
        if result is None:
            return None
        return ResolvedPlaceResponse.model_validate(result)

    def get_unit_details(self, query: UnitDetailsQuery) -> UnitDetailsResponse:
        units = self.repository.get_unit_details(query.unit_ids)
        return UnitDetailsResponse(
            unit_ids=query.unit_ids,
            units=[UnitDetailResponse.model_validate(unit) for unit in units],
        )


def get_places_service() -> PlacesService:
    return PlacesService()
