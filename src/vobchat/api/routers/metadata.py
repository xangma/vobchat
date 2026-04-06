from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityResolutionResponse,
    DataEntityResolveQuery,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.api.services.metadata import MetadataService, get_metadata_service


router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/place-profile/{place_id}", response_model=PlaceProfileResponse)
def get_place_profile(
    place_id: int,
    service: Annotated[MetadataService, Depends(get_metadata_service)],
) -> PlaceProfileResponse:
    result = service.get_place_profile(place_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Place profile {place_id} was not found",
        )
    return result


@router.get("/place-key-findings/{unit_id}", response_model=PlaceKeyFindingsResponse)
def get_place_key_findings(
    unit_id: int,
    service: Annotated[MetadataService, Depends(get_metadata_service)],
) -> PlaceKeyFindingsResponse:
    return service.get_place_key_findings(unit_id)


@router.get("/unit-types/{unit_type}", response_model=UnitTypeInfoResponse)
def get_unit_type_info(
    unit_type: str,
    service: Annotated[MetadataService, Depends(get_metadata_service)],
) -> UnitTypeInfoResponse:
    result = service.get_unit_type_info(unit_type)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unit type '{unit_type}' was not found",
        )
    return result


@router.get("/entities/resolve", response_model=DataEntityResolutionResponse)
def resolve_data_entity(
    query: Annotated[DataEntityResolveQuery, Depends(DataEntityResolveQuery.as_query)],
    service: Annotated[MetadataService, Depends(get_metadata_service)],
) -> DataEntityResolutionResponse:
    result = service.resolve_data_entity(query)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data entity '{query.query}' was not found",
        )
    return result


@router.get("/entities/{entity_id}", response_model=DataEntityInfoResponse)
def get_data_entity_info(
    entity_id: str,
    service: Annotated[MetadataService, Depends(get_metadata_service)],
) -> DataEntityInfoResponse:
    result = service.get_data_entity_info(entity_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data entity '{entity_id}' was not found",
        )
    return result
