from __future__ import annotations

from vobchat.api.schemas.themes import (
    CubeListResponse,
    CubeSummaryResponse,
    ThemeListQuery,
    ThemeListResponse,
    ThemeResolutionResponse,
    ThemeResolveQuery,
    ThemeSummaryResponse,
    ThemesForUnitResponse,
)
from vobchat.db.repositories import ThemesRepository


class ThemesService:
    def __init__(self, repository: ThemesRepository | None = None) -> None:
        self.repository = repository or ThemesRepository()

    def list_themes(self, query: ThemeListQuery) -> ThemeListResponse:
        items = self.repository.list_themes(limit=query.limit)
        return ThemeListResponse(
            item_count=len(items),
            items=[ThemeSummaryResponse.model_validate(item) for item in items],
        )

    def resolve_theme(self, query: ThemeResolveQuery) -> ThemeResolutionResponse | None:
        result = self.repository.lookup_theme(query.query)
        if result is None:
            return None
        return ThemeResolutionResponse(
            query=query.query,
            result=ThemeSummaryResponse.model_validate(result),
        )

    def list_themes_for_unit(self, unit_id: int) -> ThemesForUnitResponse:
        items = self.repository.list_themes_for_unit(unit_id)
        return ThemesForUnitResponse(
            unit_id=unit_id,
            item_count=len(items),
            items=[ThemeSummaryResponse.model_validate(item) for item in items],
        )

    def list_cubes_for_unit_theme(self, unit_id: int, theme_id: str) -> CubeListResponse:
        items = self.repository.list_cubes_for_unit_theme(unit_id=unit_id, theme_id=theme_id)
        return CubeListResponse(
            unit_id=unit_id,
            theme_id=theme_id,
            item_count=len(items),
            items=[CubeSummaryResponse.model_validate(item) for item in items],
        )


def get_themes_service() -> ThemesService:
    return ThemesService()
