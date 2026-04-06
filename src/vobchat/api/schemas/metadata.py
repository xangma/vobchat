from __future__ import annotations

from typing import Annotated

from fastapi import Query
from pydantic import Field, field_validator

from vobchat.api.schemas.common import APIModel


class PlaceKeyFindingResponse(APIModel):
    url: str | None = None
    label: str | None = None
    text: str | None = None


class PlaceProfileResponse(APIModel):
    place_id: int
    name: str | None = None
    county: str | None = None
    county_name: str | None = None
    nation_name: str | None = None
    state_name: str | None = None
    domain_name: str | None = None
    district_name: str | None = None
    district_type: str | None = None
    text_author: str | None = None
    text: str | None = None
    notes: str | None = None
    see_also_place_id: int | None = None
    see_also_place_name: str | None = None
    key_findings: list[PlaceKeyFindingResponse] = Field(default_factory=list)


class PlaceKeyFindingsResponse(APIModel):
    unit_id: int
    item_count: int
    items: list[PlaceKeyFindingResponse] = Field(default_factory=list)


class UnitTypeRelationResponse(APIModel):
    unit_type: str
    label: str


class UnitTypeStatusResponse(APIModel):
    code: str
    label: str


class UnitTypeInfoResponse(APIModel):
    identifier: str
    label: str
    level: int | None = None
    level_label: str | None = None
    adl_feature_type: str | None = None
    description: str | None = None
    full_description: str | None = None
    unit_count: int
    may_be_part_of: list[UnitTypeRelationResponse] = Field(default_factory=list)
    may_have_parts: list[UnitTypeRelationResponse] = Field(default_factory=list)
    may_have_succeeded: list[UnitTypeRelationResponse] = Field(default_factory=list)
    may_have_preceded: list[UnitTypeRelationResponse] = Field(default_factory=list)
    statuses: list[UnitTypeStatusResponse] = Field(default_factory=list)


class DataEntityReferenceResponse(APIModel):
    entity_id: str
    label: str
    entity_type: str


class DataEntityRelationResponse(APIModel):
    entity_id: str
    label: str
    entity_type: str


class DataEntityInfoResponse(APIModel):
    entity_id: str
    entity_type: str | None = None
    name: str | None = None
    short_name: str | None = None
    text: str | None = None
    additivity: str | None = None
    continuous: bool | None = None
    rate_top: str | None = None
    rate_bottom: str | None = None
    rate_multiplier: float | None = None
    cube_root_unit: str | None = None
    cube_root_name: str | None = None
    theme_id: str | None = None
    rate_type: str | None = None
    cube_display: bool | None = None
    cube_download: bool | None = None
    type_name: str | None = None
    type_text: str | None = None
    higher_entities: list[DataEntityRelationResponse] = Field(default_factory=list)
    lower_entities: list[DataEntityRelationResponse] = Field(default_factory=list)


class DataEntityResolveQuery(APIModel):
    query: str = Field(min_length=1)

    @field_validator("query")
    @classmethod
    def _normalize_query(cls, value: str) -> str:
        return value.strip()

    @classmethod
    def as_query(
        cls,
        query: Annotated[str, Query(min_length=1)],
    ) -> "DataEntityResolveQuery":
        return cls(query=query)


class DataEntityResolutionResponse(APIModel):
    query: str
    result: DataEntityReferenceResponse
