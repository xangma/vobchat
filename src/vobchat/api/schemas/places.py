from __future__ import annotations

from typing import Annotated, Literal

from fastapi import Query
from pydantic import Field, field_validator

from vobchat.api.schemas.common import APIModel, parse_multi_value


class UnitDetailResponse(APIModel):
    unit_id: int
    unit_name: str
    unit_type: str
    unit_type_label: str | None = None


class PlaceCandidateResponse(APIModel):
    place_id: int
    name: str
    county_id: int | None = None
    county_name: str | None = None
    nation_id: int | None = None
    nation_name: str | None = None
    domain_id: int | None = None
    domain_name: str | None = None
    state_id: int | None = None
    state_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    unit_ids: list[int] = Field(default_factory=list)
    unit_types: list[str] = Field(default_factory=list)
    match_type: str = "exact"


class ResolvedPlaceResponse(APIModel):
    place: PlaceCandidateResponse
    units: list[UnitDetailResponse] = Field(default_factory=list)


class PostcodeLookupResultResponse(APIModel):
    postcode: str
    unit_id: int
    place_id: int | None = None
    unit_name: str
    unit_type: str
    county_name: str | None = None
    max_area: float | None = None


class PlaceSearchQuery(APIModel):
    query: str = Field(min_length=1)
    match_mode: Literal["exact", "fuzzy"] = "fuzzy"
    county_id: int | None = None
    unit_types: list[str] = Field(default_factory=list)
    nation_id: int | None = None
    domain_id: int | None = None
    state_id: int | None = None
    limit: int = Field(default=41, ge=1, le=100)

    @field_validator("query")
    @classmethod
    def _normalize_query(cls, value: str) -> str:
        return value.strip()

    @field_validator("unit_types", mode="before")
    @classmethod
    def _normalize_unit_types(cls, value: object) -> list[str]:
        return [str(item).upper() for item in parse_multi_value(value)]

    @classmethod
    def as_query(
        cls,
        query: Annotated[str, Query(min_length=1, description="Place name to search for")],
        match_mode: Annotated[
            Literal["exact", "fuzzy"],
            Query(description="Match mode for place lookup"),
        ] = "fuzzy",
        county_id: Annotated[int | None, Query()] = None,
        unit_types: Annotated[list[str] | None, Query()] = None,
        nation_id: Annotated[int | None, Query()] = None,
        domain_id: Annotated[int | None, Query()] = None,
        state_id: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 41,
    ) -> "PlaceSearchQuery":
        return cls(
            query=query,
            match_mode=match_mode,
            county_id=county_id,
            unit_types=unit_types or [],
            nation_id=nation_id,
            domain_id=domain_id,
            state_id=state_id,
            limit=limit,
        )


class PlaceResolveQuery(APIModel):
    unit_types: list[str] = Field(default_factory=list)

    @field_validator("unit_types", mode="before")
    @classmethod
    def _normalize_unit_types(cls, value: object) -> list[str]:
        return [str(item).upper() for item in parse_multi_value(value)]

    @classmethod
    def as_query(
        cls,
        unit_types: Annotated[list[str] | None, Query()] = None,
    ) -> "PlaceResolveQuery":
        return cls(unit_types=unit_types or [])


class PostcodeLookupQuery(APIModel):
    postcode: str = Field(min_length=1)
    unit_type: str = Field(default="MOD_DIST", min_length=1)

    @field_validator("postcode")
    @classmethod
    def _normalize_postcode(cls, value: str) -> str:
        return value.strip().upper().replace(" ", "")

    @field_validator("unit_type")
    @classmethod
    def _normalize_unit_type(cls, value: str) -> str:
        return value.strip().upper()

    @classmethod
    def as_query(
        cls,
        postcode: Annotated[str, Query(min_length=1)],
        unit_type: Annotated[str, Query(min_length=1)] = "MOD_DIST",
    ) -> "PostcodeLookupQuery":
        return cls(postcode=postcode, unit_type=unit_type)


class UnitDetailsQuery(APIModel):
    unit_ids: list[int] = Field(default_factory=list, min_length=1)

    @field_validator("unit_ids", mode="before")
    @classmethod
    def _normalize_unit_ids(cls, value: object) -> list[int]:
        return [int(item) for item in parse_multi_value(value)]

    @classmethod
    def as_query(
        cls,
        unit_ids: Annotated[list[int], Query(description="Selected unit ids")],
    ) -> "UnitDetailsQuery":
        return cls(unit_ids=unit_ids)


class PlaceSearchResponse(APIModel):
    query: str
    match_mode: Literal["exact", "fuzzy"]
    result_count: int
    results: list[PlaceCandidateResponse] = Field(default_factory=list)


class PostcodeLookupResponse(APIModel):
    postcode: str
    unit_type: str
    result_count: int
    results: list[PostcodeLookupResultResponse] = Field(default_factory=list)


class UnitDetailsResponse(APIModel):
    unit_ids: list[int] = Field(default_factory=list)
    units: list[UnitDetailResponse] = Field(default_factory=list)
