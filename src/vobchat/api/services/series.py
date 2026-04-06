from __future__ import annotations

from vobchat.api.schemas.series import (
    CategoryBreakdownRequest,
    CategoryBreakdownResponse,
    CategoryRowResponse,
    TimeSeriesRequest,
    TimeSeriesResponse,
    TimeSeriesRowResponse,
)
from vobchat.db.repositories import SeriesRepository


class SeriesService:
    def __init__(self, repository: SeriesRepository | None = None) -> None:
        self.repository = repository or SeriesRepository()

    def get_time_series(self, request: TimeSeriesRequest) -> TimeSeriesResponse:
        dataset = self.repository.fetch_series_for_units_and_cubes(
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            start_year=request.start_year,
            end_year=request.end_year,
        )
        return TimeSeriesResponse(
            unit_ids=list(dataset.unit_ids),
            cube_ids=list(dataset.cube_ids),
            row_count=len(dataset.rows),
            rows=[TimeSeriesRowResponse.model_validate(row) for row in dataset.rows],
        )

    def get_category_breakdown(
        self,
        request: CategoryBreakdownRequest,
    ) -> CategoryBreakdownResponse:
        dataset = self.repository.fetch_category_breakdown(
            unit_ids=request.unit_ids,
            cube_ids=request.cube_ids,
            year=request.year,
        )
        return CategoryBreakdownResponse(
            year=dataset.year or request.year,
            unit_ids=list(dataset.unit_ids),
            cube_ids=list(dataset.cube_ids),
            row_count=len(dataset.rows),
            rows=[CategoryRowResponse.model_validate(row) for row in dataset.rows],
        )


def get_series_service() -> SeriesService:
    return SeriesService()
