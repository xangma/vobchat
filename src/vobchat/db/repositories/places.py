from __future__ import annotations

from collections.abc import Sequence

from vobchat.db.models import (
    PlaceCandidate,
    PostcodeLookupResult,
    ResolvedPlace,
    UnitSummary,
)
from vobchat.db.repositories._base import (
    BaseRepository,
    PREFERRED_UNIT_NAME_CTE,
    coerce_float,
    coerce_int,
    coerce_int_tuple,
    coerce_str,
    coerce_str_tuple,
    normalize_unit_types,
    statement,
)
from vobchat.utils.constants import UNIT_TYPES


class PlacesRepository(BaseRepository):
    def search_places_exact(
        self,
        place_name: str,
        *,
        county_id: int | None = None,
        unit_types: Sequence[str] | None = None,
        nation_id: int | None = None,
        domain_id: int | None = None,
        state_id: int | None = None,
        limit: int = 41,
    ) -> list[PlaceCandidate]:
        return self._search_places(
            place_name=place_name,
            match_type="exact",
            county_id=county_id,
            unit_types=unit_types,
            nation_id=nation_id,
            domain_id=domain_id,
            state_id=state_id,
            limit=limit,
        )

    def search_places_fuzzy(
        self,
        place_name: str,
        *,
        county_id: int | None = None,
        unit_types: Sequence[str] | None = None,
        nation_id: int | None = None,
        domain_id: int | None = None,
        state_id: int | None = None,
        limit: int = 41,
    ) -> list[PlaceCandidate]:
        return self._search_places(
            place_name=place_name,
            match_type="fuzzy",
            county_id=county_id,
            unit_types=unit_types,
            nation_id=nation_id,
            domain_id=domain_id,
            state_id=state_id,
            limit=limit,
        )

    def lookup_postcode_units(
        self,
        postcode: str,
        *,
        unit_type: str = "MOD_DIST",
    ) -> list[PostcodeLookupResult]:
        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        SELECT DISTINCT
            u.g_unit,
            gp.g_place,
            un.g_name AS g_name,
            u.g_unit_type,
            gp.county_name,
            MAX(public.ST_Area(f.g_foot_ertslcc)) AS max_area
        FROM hgis.g_unit u
        JOIN hgis.g_foot f
            ON f.g_unit = u.g_unit
        JOIN hgis.g_name gn
            ON gn.g_unit = u.g_unit
        JOIN hgis.g_place gp
            ON gn.g_place = gp.g_place
        JOIN unit_name un
            ON un.g_unit = u.g_unit
           AND un.rn = 1
        JOIN hgis.codepoint post
            ON public.ST_Contains(f.g_foot_ertslcc, post.g_point_etrs)
        WHERE post.postcode = :postcode
          AND u.g_unit_type = :unit_type
        GROUP BY u.g_unit, gp.g_place, un.g_name, u.g_unit_type, gp.county_name
        ORDER BY MAX(public.ST_Area(f.g_foot_ertslcc))
        """
        rows = self.executor.fetch_all(
            statement(sql),
            {
                "postcode": (postcode or "").strip().upper().replace(" ", ""),
                "unit_type": unit_type,
                "user_lang": "eng",
            },
        )
        return [self._normalize_postcode_row(row, postcode) for row in rows]

    def get_unit_details(self, unit_ids: Sequence[int | str]) -> list[UnitSummary]:
        normalized_ids = tuple(
            unit_id
            for unit_id in (coerce_int(value) for value in unit_ids)
            if unit_id is not None
        )
        if not normalized_ids:
            return []

        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        SELECT
            u.g_unit,
            COALESCE(un.g_name, 'Unknown Name') AS unit_name,
            u.g_unit_type
        FROM hgis.g_unit u
        LEFT JOIN unit_name un
            ON un.g_unit = u.g_unit
           AND un.rn = 1
        WHERE u.g_unit IN :unit_ids
        ORDER BY u.g_unit
        """
        rows = self.executor.fetch_all(
            statement(sql, expanding_params=("unit_ids",)),
            {"unit_ids": normalized_ids, "user_lang": "eng"},
        )
        return [self._normalize_unit_row(row) for row in rows]

    def resolve_place(
        self,
        place_id: int,
        *,
        unit_types: Sequence[str] | None = None,
    ) -> ResolvedPlace | None:
        requested_unit_types = normalize_unit_types(unit_types)
        sql = """
        SELECT
            p.g_place,
            p.g_name,
            p.g_county,
            p.g_nation,
            p.g_domain,
            p.g_state,
            p.county_name,
            p.nation_name,
            p.domain_name,
            p.state_name,
            public.ST_Y(public.ST_Transform(p.g_point, 4326)) AS lat,
            public.ST_X(public.ST_Transform(p.g_point, 4326)) AS lon,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT n.g_unit), NULL) AS g_unit,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT g.g_unit_type), NULL) AS g_unit_type
        FROM hgis.g_place p
        JOIN hgis.g_name n
            ON p.g_place = n.g_place
        LEFT JOIN hgis.g_unit g
            ON n.g_unit = g.g_unit
        WHERE p.g_place = :place_id
          AND (g.g_unit_type IS NULL OR g.g_unit_type IN :unit_types)
          AND g.g_point_source = 'Own centroid'
          AND p.g_point IS NOT NULL
        GROUP BY
            p.g_place,
            p.g_name,
            p.g_county,
            p.g_nation,
            p.g_domain,
            p.g_state,
            p.county_name,
            p.nation_name,
            p.domain_name,
            p.state_name,
            p.g_point
        LIMIT 1
        """
        row = self.executor.fetch_one(
            statement(sql, expanding_params=("unit_types",)),
            {"place_id": place_id, "unit_types": requested_unit_types},
        )
        if row is None:
            return None
        place = self._normalize_place_row(row, "resolved")
        units = tuple(self.get_unit_details(place.unit_ids))
        return ResolvedPlace(place=place, units=units)

    def resolve_place_for_unit(
        self,
        unit_id: int,
        *,
        unit_types: Sequence[str] | None = None,
    ) -> ResolvedPlace | None:
        requested_unit_types = normalize_unit_types(unit_types)
        sql = """
        SELECT DISTINCT
            p.g_place
        FROM hgis.g_name n
        JOIN hgis.g_place p
            ON p.g_place = n.g_place
        LEFT JOIN hgis.g_unit g
            ON n.g_unit = g.g_unit
        WHERE n.g_unit = :unit_id
          AND (g.g_unit_type IS NULL OR g.g_unit_type IN :unit_types)
          AND g.g_point_source = 'Own centroid'
          AND p.g_point IS NOT NULL
        ORDER BY p.g_place
        LIMIT 1
        """
        row = self.executor.fetch_one(
            statement(sql, expanding_params=("unit_types",)),
            {"unit_id": unit_id, "unit_types": requested_unit_types},
        )
        if row is None:
            return None
        resolved_place_id = coerce_int(row.get("g_place"))
        if resolved_place_id is None:
            return None
        return self.resolve_place(resolved_place_id, unit_types=requested_unit_types)

    def _search_places(
        self,
        *,
        place_name: str,
        match_type: str,
        county_id: int | None,
        unit_types: Sequence[str] | None,
        nation_id: int | None,
        domain_id: int | None,
        state_id: int | None,
        limit: int,
    ) -> list[PlaceCandidate]:
        requested_unit_types = normalize_unit_types(unit_types)
        comparator = "n.g_name = UPPER(:place_name)" if match_type == "exact" else "n.g_name ILIKE :place_name_pattern"
        ordering = "" if match_type == "exact" else "ORDER BY char_length(p.g_name) ASC, p.g_name ASC"
        sql = f"""
        SELECT
            p.g_place,
            p.g_name,
            p.g_county,
            p.g_nation,
            p.g_domain,
            p.g_state,
            p.county_name,
            p.nation_name,
            p.domain_name,
            p.state_name,
            public.ST_Y(public.ST_Transform(p.g_point, 4326)) AS lat,
            public.ST_X(public.ST_Transform(p.g_point, 4326)) AS lon,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT n.g_unit), NULL) AS g_unit,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT g.g_unit_type), NULL) AS g_unit_type
        FROM hgis.g_place p
        JOIN hgis.g_name n
            ON p.g_place = n.g_place
        LEFT JOIN hgis.g_unit g
            ON n.g_unit = g.g_unit
        WHERE (g.g_unit_type IS NULL OR g.g_unit_type IN :unit_types)
          AND {comparator}
          AND (:county_id IS NULL OR p.g_county = :county_id)
          AND (:nation_id IS NULL OR p.g_nation = :nation_id)
          AND (:domain_id IS NULL OR p.g_domain = :domain_id)
          AND (:state_id IS NULL OR p.g_state = :state_id)
          AND g.g_point_source = 'Own centroid'
          AND p.g_point IS NOT NULL
        GROUP BY
            p.g_place,
            p.g_name,
            p.g_county,
            p.g_nation,
            p.g_domain,
            p.g_state,
            p.county_name,
            p.nation_name,
            p.domain_name,
            p.state_name,
            p.g_point
        {ordering}
        LIMIT :limit
        """
        params = {
            "place_name": place_name,
            "place_name_pattern": f"%{place_name or ''}%",
            "county_id": county_id,
            "nation_id": nation_id,
            "domain_id": domain_id,
            "state_id": state_id,
            "unit_types": requested_unit_types,
            "limit": limit,
        }
        rows = self.executor.fetch_all(
            statement(sql, expanding_params=("unit_types",)),
            params,
        )
        return [self._normalize_place_row(row, match_type) for row in rows]

    @staticmethod
    def _normalize_place_row(row: dict[str, object], match_type: str) -> PlaceCandidate:
        return PlaceCandidate(
            place_id=coerce_int(row.get("g_place")) or 0,
            name=coerce_str(row.get("g_name")) or "",
            county_id=coerce_int(row.get("g_county")),
            county_name=coerce_str(row.get("county_name")),
            nation_id=coerce_int(row.get("g_nation")),
            nation_name=coerce_str(row.get("nation_name")),
            domain_id=coerce_int(row.get("g_domain")),
            domain_name=coerce_str(row.get("domain_name")),
            state_id=coerce_int(row.get("g_state")),
            state_name=coerce_str(row.get("state_name")),
            latitude=coerce_float(row.get("lat")),
            longitude=coerce_float(row.get("lon")),
            unit_ids=coerce_int_tuple(row.get("g_unit")),
            unit_types=coerce_str_tuple(row.get("g_unit_type"), skip_values={"NONE"}),
            match_type=match_type,
        )

    @staticmethod
    def _normalize_postcode_row(
        row: dict[str, object],
        postcode: str,
    ) -> PostcodeLookupResult:
        return PostcodeLookupResult(
            postcode=(postcode or "").strip().upper().replace(" ", ""),
            unit_id=coerce_int(row.get("g_unit")) or 0,
            place_id=coerce_int(row.get("g_place")),
            unit_name=coerce_str(row.get("g_name")) or "",
            unit_type=coerce_str(row.get("g_unit_type")) or "",
            county_name=coerce_str(row.get("county_name")),
            max_area=coerce_float(row.get("max_area")),
        )

    @staticmethod
    def _normalize_unit_row(row: dict[str, object]) -> UnitSummary:
        unit_type = coerce_str(row.get("g_unit_type")) or ""
        return UnitSummary(
            unit_id=coerce_int(row.get("g_unit")) or 0,
            unit_name=coerce_str(row.get("unit_name")) or "",
            unit_type=unit_type,
            unit_type_label=UNIT_TYPES.get(unit_type, {}).get("long_name"),
        )
