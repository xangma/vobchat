from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from vobchat.api.schemas.maps import (
    MapFeatureCollectionResponse,
    MapFeaturesByIdsQuery,
    MapFeaturesQuery,
)
from vobchat.api.services.maps import MapsService, get_maps_service


router = APIRouter(prefix="/maps", tags=["maps"])


@router.get("/features", response_model=MapFeatureCollectionResponse)
def get_map_features(
    query: Annotated[MapFeaturesQuery, Depends(MapFeaturesQuery.as_query)],
    service: Annotated[MapsService, Depends(get_maps_service)],
) -> MapFeatureCollectionResponse:
    try:
        return service.get_features(query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/features/by-ids", response_model=MapFeatureCollectionResponse)
def get_map_features_by_ids(
    query: Annotated[MapFeaturesByIdsQuery, Depends(MapFeaturesByIdsQuery.as_query)],
    service: Annotated[MapsService, Depends(get_maps_service)],
) -> MapFeatureCollectionResponse:
    try:
        return service.get_features_by_ids(query)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
