from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from vobchat.api.schemas.series import (
    CategoryBreakdownRequest,
    CategoryBreakdownResponse,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from vobchat.api.services.series import SeriesService, get_series_service


router = APIRouter(prefix="/series", tags=["series"])


@router.post("/time", response_model=TimeSeriesResponse)
def get_time_series(
    request: TimeSeriesRequest,
    service: Annotated[SeriesService, Depends(get_series_service)],
) -> TimeSeriesResponse:
    try:
        return service.get_time_series(request)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/categories", response_model=CategoryBreakdownResponse)
def get_category_breakdown(
    request: CategoryBreakdownRequest,
    service: Annotated[SeriesService, Depends(get_series_service)],
) -> CategoryBreakdownResponse:
    try:
        return service.get_category_breakdown(request)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
