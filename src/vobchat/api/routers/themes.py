from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from vobchat.api.schemas.themes import (
    CubeListResponse,
    ThemeCubesQuery,
    ThemeListQuery,
    ThemeListResponse,
    ThemeResolutionResponse,
    ThemeResolveQuery,
    ThemesForUnitResponse,
)
from vobchat.api.services.themes import ThemesService, get_themes_service


router = APIRouter(prefix="/themes", tags=["themes"])


@router.get("", response_model=ThemeListResponse)
def list_themes(
    query: Annotated[ThemeListQuery, Depends(ThemeListQuery.as_query)],
    service: Annotated[ThemesService, Depends(get_themes_service)],
) -> ThemeListResponse:
    return service.list_themes(query)


@router.get("/resolve", response_model=ThemeResolutionResponse)
def resolve_theme(
    query: Annotated[ThemeResolveQuery, Depends(ThemeResolveQuery.as_query)],
    service: Annotated[ThemesService, Depends(get_themes_service)],
) -> ThemeResolutionResponse:
    result = service.resolve_theme(query)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Theme '{query.query}' was not found",
        )
    return result


@router.get("/for-unit/{unit_id}", response_model=ThemesForUnitResponse)
def list_themes_for_unit(
    unit_id: int,
    service: Annotated[ThemesService, Depends(get_themes_service)],
) -> ThemesForUnitResponse:
    return service.list_themes_for_unit(unit_id)


@router.get("/{theme_id}/cubes", response_model=CubeListResponse)
def list_cubes_for_unit_theme(
    theme_id: str,
    query: Annotated[ThemeCubesQuery, Depends(ThemeCubesQuery.as_query)],
    service: Annotated[ThemesService, Depends(get_themes_service)],
) -> CubeListResponse:
    return service.list_cubes_for_unit_theme(query.unit_id, theme_id)
