from __future__ import annotations

from typing import Annotated

from fastapi import Query
from pydantic import Field, field_validator

from vobchat.api.schemas.common import APIModel


class ThemeSummaryResponse(APIModel):
    theme_id: str
    label: str
    description: str | None = None


class CubeSummaryResponse(APIModel):
    theme_id: str
    cube_id: str
    label: str
    description: str | None = None
    start_year: float | None = None
    end_year: float | None = None
    observation_count: int
    has_categories: bool


class ThemeListQuery(APIModel):
    limit: int | None = Field(default=None, ge=1, le=100)

    @classmethod
    def as_query(
        cls,
        limit: Annotated[int | None, Query(ge=1, le=100)] = None,
    ) -> "ThemeListQuery":
        return cls(limit=limit)


class ThemeResolveQuery(APIModel):
    query: str = Field(min_length=1)

    @field_validator("query")
    @classmethod
    def _normalize_query(cls, value: str) -> str:
        return value.strip()

    @classmethod
    def as_query(
        cls,
        query: Annotated[str, Query(min_length=1)],
    ) -> "ThemeResolveQuery":
        return cls(query=query)


class ThemeCubesQuery(APIModel):
    unit_id: int

    @classmethod
    def as_query(
        cls,
        unit_id: Annotated[int, Query()],
    ) -> "ThemeCubesQuery":
        return cls(unit_id=unit_id)


class ThemeListResponse(APIModel):
    item_count: int
    items: list[ThemeSummaryResponse] = Field(default_factory=list)


class ThemesForUnitResponse(APIModel):
    unit_id: int
    item_count: int
    items: list[ThemeSummaryResponse] = Field(default_factory=list)


class ThemeResolutionResponse(APIModel):
    query: str
    result: ThemeSummaryResponse


class CubeListResponse(APIModel):
    unit_id: int
    theme_id: str
    item_count: int
    items: list[CubeSummaryResponse] = Field(default_factory=list)
