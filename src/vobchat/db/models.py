from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UnitSummary:
    unit_id: int
    unit_name: str
    unit_type: str
    unit_type_label: str | None = None


@dataclass(frozen=True)
class PlaceCandidate:
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
    unit_ids: tuple[int, ...] = ()
    unit_types: tuple[str, ...] = ()
    match_type: str = "exact"


@dataclass(frozen=True)
class ResolvedPlace:
    place: PlaceCandidate
    units: tuple[UnitSummary, ...] = ()


@dataclass(frozen=True)
class PostcodeLookupResult:
    postcode: str
    unit_id: int
    place_id: int | None
    unit_name: str
    unit_type: str
    county_name: str | None = None
    max_area: float | None = None


@dataclass(frozen=True)
class ThemeSummary:
    theme_id: str
    label: str
    description: str | None = None


@dataclass(frozen=True)
class CubeSummary:
    theme_id: str
    cube_id: str
    label: str
    description: str | None = None
    start_year: float | None = None
    end_year: float | None = None
    observation_count: int = 0
    has_categories: bool = False


@dataclass(frozen=True)
class TimeSeriesRow:
    year: float
    unit_id: int
    unit_name: str | None
    unit_type: str | None
    cube_id: str
    cube_label: str | None
    cube_text: str | None
    cell_ref: str | None
    value: float | None


@dataclass(frozen=True)
class TimeSeriesDataset:
    rows: tuple[TimeSeriesRow, ...] = ()
    unit_ids: tuple[int, ...] = ()
    cube_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class CategoryRow:
    year: int
    unit_id: int
    unit_name: str | None
    cube_id: str
    category_group: str | None
    category_label: str
    value: float | None
    cell_ref: str | None = None


@dataclass(frozen=True)
class CategoryDataset:
    rows: tuple[CategoryRow, ...] = ()
    year: int | None = None
    unit_ids: tuple[int, ...] = ()
    cube_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class BoundingBox:
    min_x: float
    min_y: float
    max_x: float
    max_y: float


@dataclass(frozen=True)
class MapFeatureRow:
    unit_id: int
    unit_name: str
    unit_type: str
    geometry_geojson: str
    start_year: int | None = None
    end_year: int | None = None
    has_theme: bool | None = None


@dataclass(frozen=True)
class PlaceKeyFinding:
    url: str | None
    label: str | None
    text: str | None


@dataclass(frozen=True)
class PlaceProfile:
    place_id: int
    name: str | None
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
    key_findings: tuple[PlaceKeyFinding, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class UnitTypeRelation:
    unit_type: str
    label: str


@dataclass(frozen=True)
class UnitTypeStatus:
    code: str
    label: str


@dataclass(frozen=True)
class UnitTypeInfo:
    identifier: str
    label: str
    level: int | None = None
    level_label: str | None = None
    adl_feature_type: str | None = None
    description: str | None = None
    full_description: str | None = None
    unit_count: int = 0
    may_be_part_of: tuple[UnitTypeRelation, ...] = ()
    may_have_parts: tuple[UnitTypeRelation, ...] = ()
    may_have_succeeded: tuple[UnitTypeRelation, ...] = ()
    may_have_preceded: tuple[UnitTypeRelation, ...] = ()
    statuses: tuple[UnitTypeStatus, ...] = ()


@dataclass(frozen=True)
class DataEntityReference:
    entity_id: str
    label: str
    entity_type: str


@dataclass(frozen=True)
class DataEntityRelation:
    entity_id: str
    label: str
    entity_type: str


@dataclass(frozen=True)
class DataEntityInfo:
    entity_id: str
    entity_type: str | None
    name: str | None
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
    higher_entities: tuple[DataEntityRelation, ...] = ()
    lower_entities: tuple[DataEntityRelation, ...] = ()
