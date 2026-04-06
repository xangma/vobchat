from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

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
from vobchat.api.services.places import PlacesService, get_places_service


router = APIRouter(prefix="/places", tags=["places"])


@router.get("/search", response_model=PlaceSearchResponse)
def search_places(
    query: Annotated[PlaceSearchQuery, Depends(PlaceSearchQuery.as_query)],
    service: Annotated[PlacesService, Depends(get_places_service)],
) -> PlaceSearchResponse:
    try:
        return service.search_places(query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/postcode", response_model=PostcodeLookupResponse)
def lookup_postcode(
    query: Annotated[PostcodeLookupQuery, Depends(PostcodeLookupQuery.as_query)],
    service: Annotated[PlacesService, Depends(get_places_service)],
) -> PostcodeLookupResponse:
    try:
        return service.lookup_postcode(query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/units/details", response_model=UnitDetailsResponse)
def get_unit_details(
    query: Annotated[UnitDetailsQuery, Depends(UnitDetailsQuery.as_query)],
    service: Annotated[PlacesService, Depends(get_places_service)],
) -> UnitDetailsResponse:
    return service.get_unit_details(query)


@router.get("/units/{unit_id}/place", response_model=ResolvedPlaceResponse)
def get_place_for_unit(
    unit_id: int,
    query: Annotated[PlaceResolveQuery, Depends(PlaceResolveQuery.as_query)],
    service: Annotated[PlacesService, Depends(get_places_service)],
) -> ResolvedPlaceResponse:
    try:
        result = service.resolve_place_for_unit(unit_id, query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No place was found for unit {unit_id}",
        )
    return result


@router.get("/{place_id}", response_model=ResolvedPlaceResponse)
def get_place(
    place_id: int,
    query: Annotated[PlaceResolveQuery, Depends(PlaceResolveQuery.as_query)],
    service: Annotated[PlacesService, Depends(get_places_service)],
) -> ResolvedPlaceResponse:
    try:
        result = service.resolve_place(place_id, query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Place {place_id} was not found",
        )
    return result
