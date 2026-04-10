from __future__ import annotations

from pydantic import Field, field_validator, model_validator

from vobchat.api.schemas.common import APIModel, parse_multi_value


class TimeSeriesRowResponse(APIModel):
    year: float
    unit_id: int
    unit_name: str | None = None
    unit_type: str | None = None
    cube_id: str
    cube_label: str | None = None
    cube_text: str | None = None
    cell_ref: str | None = None
    dataitem_id: str | None = None
    cat_id: str | None = None
    value: float | None = None


class CategoryRowResponse(APIModel):
    year: int
    unit_id: int
    unit_name: str | None = None
    cube_id: str
    category_group: str | None = None
    category_label: str
    value: float | None = None
    cell_ref: str | None = None
    dataitem_id: str | None = None
    cat_id: str | None = None
    category_entity_id: str | None = None
    category_source: str | None = None


class TimeSeriesRequest(APIModel):
    unit_ids: list[int] = Field(min_length=1)
    cube_ids: list[str] = Field(min_length=1)
    cellrefs: list[str] = Field(default_factory=list)
    dataitem_ids: list[str] = Field(default_factory=list)
    start_year: float | None = None
    end_year: float | None = None

    @field_validator("unit_ids", mode="before")
    @classmethod
    def _normalize_unit_ids(cls, value: object) -> list[int]:
        return [int(item) for item in parse_multi_value(value)]

    @field_validator("cube_ids", mode="before")
    @classmethod
    def _normalize_cube_ids(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @field_validator("cellrefs", mode="before")
    @classmethod
    def _normalize_cellrefs(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @field_validator("dataitem_ids", mode="before")
    @classmethod
    def _normalize_dataitem_ids(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @model_validator(mode="after")
    def _validate_year_range(self) -> "TimeSeriesRequest":
        if (
            self.start_year is not None
            and self.end_year is not None
            and self.start_year > self.end_year
        ):
            raise ValueError("start_year must be less than or equal to end_year")
        return self


class TimeSeriesResponse(APIModel):
    unit_ids: list[int] = Field(default_factory=list)
    cube_ids: list[str] = Field(default_factory=list)
    row_count: int
    rows: list[TimeSeriesRowResponse] = Field(default_factory=list)


class CategoryBreakdownRequest(APIModel):
    unit_ids: list[int] = Field(min_length=1)
    cube_ids: list[str] = Field(min_length=1)
    cellrefs: list[str] = Field(default_factory=list)
    dataitem_ids: list[str] = Field(default_factory=list)
    cat_ids: list[str] = Field(default_factory=list)
    year: int

    @field_validator("unit_ids", mode="before")
    @classmethod
    def _normalize_unit_ids(cls, value: object) -> list[int]:
        return [int(item) for item in parse_multi_value(value)]

    @field_validator("cube_ids", mode="before")
    @classmethod
    def _normalize_cube_ids(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @field_validator("cellrefs", mode="before")
    @classmethod
    def _normalize_cellrefs(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @field_validator("dataitem_ids", mode="before")
    @classmethod
    def _normalize_dataitem_ids(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]

    @field_validator("cat_ids", mode="before")
    @classmethod
    def _normalize_cat_ids(cls, value: object) -> list[str]:
        return [str(item).strip() for item in parse_multi_value(value)]


class CategoryBreakdownResponse(APIModel):
    year: int
    unit_ids: list[int] = Field(default_factory=list)
    cube_ids: list[str] = Field(default_factory=list)
    row_count: int
    rows: list[CategoryRowResponse] = Field(default_factory=list)
