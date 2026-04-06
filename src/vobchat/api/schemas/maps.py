from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import Query
from pydantic import Field, field_validator, model_validator

from vobchat.api.schemas.common import APIModel, parse_multi_value


class BoundingBoxResponse(APIModel):
    min_x: float
    min_y: float
    max_x: float
    max_y: float


class MapFeatureResponse(APIModel):
    unit_id: int
    unit_name: str
    unit_type: str
    geometry: dict[str, Any]
    start_year: int | None = None
    end_year: int | None = None
    has_theme: bool | None = None


class MapFeaturesQuery(APIModel):
    unit_type: str = Field(min_length=1)
    start_year: int | None = None
    end_year: int | None = None
    theme_id: str | None = None
    min_x: float | None = None
    min_y: float | None = None
    max_x: float | None = None
    max_y: float | None = None
    exclude_ids: list[int] = Field(default_factory=list)

    @field_validator("unit_type")
    @classmethod
    def _normalize_unit_type(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("theme_id")
    @classmethod
    def _normalize_theme_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    @field_validator("exclude_ids", mode="before")
    @classmethod
    def _normalize_exclude_ids(cls, value: object) -> list[int]:
        return [int(item) for item in parse_multi_value(value)]

    @model_validator(mode="after")
    def _validate_query(self) -> "MapFeaturesQuery":
        if (
            self.start_year is not None
            and self.end_year is not None
            and self.start_year > self.end_year
        ):
            raise ValueError("start_year must be less than or equal to end_year")
        bbox_values = [self.min_x, self.min_y, self.max_x, self.max_y]
        if any(value is not None for value in bbox_values) and not all(
            value is not None for value in bbox_values
        ):
            raise ValueError(
                "Bounding box queries require min_x, min_y, max_x, and max_y"
            )
        return self

    @property
    def has_bbox(self) -> bool:
        return all(
            value is not None for value in (self.min_x, self.min_y, self.max_x, self.max_y)
        )

    @property
    def bbox(self) -> BoundingBoxResponse | None:
        if not self.has_bbox:
            return None
        return BoundingBoxResponse(
            min_x=self.min_x or 0.0,
            min_y=self.min_y or 0.0,
            max_x=self.max_x or 0.0,
            max_y=self.max_y or 0.0,
        )

    @classmethod
    def as_query(
        cls,
        unit_type: Annotated[str, Query(min_length=1)],
        start_year: Annotated[int | None, Query()] = None,
        end_year: Annotated[int | None, Query()] = None,
        theme_id: Annotated[str | None, Query()] = None,
        min_x: Annotated[float | None, Query()] = None,
        min_y: Annotated[float | None, Query()] = None,
        max_x: Annotated[float | None, Query()] = None,
        max_y: Annotated[float | None, Query()] = None,
        exclude_ids: Annotated[list[int] | None, Query()] = None,
    ) -> "MapFeaturesQuery":
        return cls(
            unit_type=unit_type,
            start_year=start_year,
            end_year=end_year,
            theme_id=theme_id,
            min_x=min_x,
            min_y=min_y,
            max_x=max_x,
            max_y=max_y,
            exclude_ids=exclude_ids or [],
        )


class MapFeaturesByIdsQuery(APIModel):
    unit_type: str = Field(min_length=1)
    ids: list[int] = Field(min_length=1)
    start_year: int | None = None
    end_year: int | None = None
    theme_id: str | None = None

    @field_validator("unit_type")
    @classmethod
    def _normalize_unit_type(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("ids", mode="before")
    @classmethod
    def _normalize_ids(cls, value: object) -> list[int]:
        return [int(item) for item in parse_multi_value(value)]

    @field_validator("theme_id")
    @classmethod
    def _normalize_theme_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    @model_validator(mode="after")
    def _validate_year_range(self) -> "MapFeaturesByIdsQuery":
        if (
            self.start_year is not None
            and self.end_year is not None
            and self.start_year > self.end_year
        ):
            raise ValueError("start_year must be less than or equal to end_year")
        return self

    @classmethod
    def as_query(
        cls,
        unit_type: Annotated[str, Query(min_length=1)],
        ids: Annotated[list[int], Query(min_length=1)],
        start_year: Annotated[int | None, Query()] = None,
        end_year: Annotated[int | None, Query()] = None,
        theme_id: Annotated[str | None, Query()] = None,
    ) -> "MapFeaturesByIdsQuery":
        return cls(
            unit_type=unit_type,
            ids=ids,
            start_year=start_year,
            end_year=end_year,
            theme_id=theme_id,
        )


class MapFeatureCollectionResponse(APIModel):
    mode: Literal["all", "bbox", "ids"]
    unit_type: str
    feature_count: int
    start_year: int | None = None
    end_year: int | None = None
    theme_id: str | None = None
    bbox: BoundingBoxResponse | None = None
    requested_ids: list[int] = Field(default_factory=list)
    features: list[MapFeatureResponse] = Field(default_factory=list)
