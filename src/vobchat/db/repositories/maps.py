from __future__ import annotations

from collections.abc import Sequence

from vobchat.db.models import BoundingBox, MapFeatureRow
from vobchat.db.repositories._base import (
    BaseRepository,
    PREFERRED_UNIT_NAME_CTE,
    coerce_bool,
    coerce_int,
    coerce_str,
    normalize_unit_types,
    statement,
    timeless_unit_types,
)


class MapsRepository(BaseRepository):
    def fetch_map_features(
        self,
        unit_type: str,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        theme_id: str | None = None,
    ) -> list[MapFeatureRow]:
        normalized_unit_type = normalize_unit_types((unit_type,))
        rows = self.executor.fetch_all(
            self._feature_statement("g.g_unit_type = :unit_type"),
            {
                "unit_type": normalized_unit_type[0],
                "timeless_unit_types": timeless_unit_types(normalized_unit_type),
                "start_year": start_year,
                "end_year": end_year,
                "theme_id": theme_id,
                "user_lang": "eng",
            },
        )
        return [self._normalize_feature_row(row) for row in rows]

    def fetch_map_features_by_bbox(
        self,
        unit_types: Sequence[str],
        bbox: BoundingBox,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        exclude_ids: Sequence[int | str] | None = None,
        theme_id: str | None = None,
    ) -> list[MapFeatureRow]:
        normalized_unit_types = normalize_unit_types(unit_types)
        normalized_exclude_ids = tuple(
            unit_id
            for unit_id in (coerce_int(value) for value in (exclude_ids or ()))
            if unit_id is not None
        )
        extra_sql = """
          AND public.ST_Intersects(
                g.g_foot_ertslcc,
                public.ST_Transform(
                    public.ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 4326),
                    3034
                )
              )
        """
        expanding = ["unit_types", "timeless_unit_types"]
        params = {
            "unit_types": normalized_unit_types,
            "timeless_unit_types": timeless_unit_types(normalized_unit_types),
            "start_year": start_year,
            "end_year": end_year,
            "theme_id": theme_id,
            "user_lang": "eng",
            "min_x": bbox.min_x,
            "min_y": bbox.min_y,
            "max_x": bbox.max_x,
            "max_y": bbox.max_y,
        }
        if normalized_exclude_ids:
            extra_sql += "\n  AND g.g_unit NOT IN :exclude_ids"
            expanding.append("exclude_ids")
            params["exclude_ids"] = normalized_exclude_ids

        rows = self.executor.fetch_all(
            self._feature_statement("g.g_unit_type IN :unit_types", extra_sql, expanding),
            params,
        )
        return [self._normalize_feature_row(row) for row in rows]

    def fetch_map_features_by_ids(
        self,
        unit_type: str,
        feature_ids: Sequence[int | str],
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        theme_id: str | None = None,
    ) -> list[MapFeatureRow]:
        normalized_unit_type = normalize_unit_types((unit_type,))
        normalized_ids = tuple(
            unit_id
            for unit_id in (coerce_int(value) for value in feature_ids)
            if unit_id is not None
        )
        if not normalized_ids:
            return []

        extra_sql = "\n  AND g.g_unit IN :feature_ids"
        rows = self.executor.fetch_all(
            self._feature_statement(
                "g.g_unit_type = :unit_type",
                extra_sql=extra_sql,
                expanding_params=("feature_ids", "timeless_unit_types"),
            ),
            {
                "unit_type": normalized_unit_type[0],
                "feature_ids": normalized_ids,
                "timeless_unit_types": timeless_unit_types(normalized_unit_type),
                "start_year": start_year,
                "end_year": end_year,
                "theme_id": theme_id,
                "user_lang": "eng",
            },
        )
        return [self._normalize_feature_row(row) for row in rows]

    def _feature_statement(
        self,
        unit_type_sql: str,
        extra_sql: str = "",
        expanding_params: Sequence[str] = ("timeless_unit_types",),
    ):
        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        SELECT
            g.g_unit,
            g.g_unit_type,
            un.g_name AS unit_name,
            public.ST_AsGeoJSON(public.ST_Transform(g.g_foot_ertslcc, 4326)) AS geometry_geojson,
            util.get_start_year(g.g_duration) AS start_year,
            util.get_end_year(g.g_duration) AS end_year,
            CASE
                WHEN :theme_id IS NULL THEN NULL
                ELSE EXISTS (
                    SELECT 1
                    FROM hgis.g_data d
                    JOIN hgis.g_data_map m
                        ON d.cellref = m.cellref
                    JOIN hgis.g_data_ent nc
                        ON nc.ent_id = m.ncuberef
                    WHERE d.g_unit = g.g_unit
                      AND nc.theme_id = :theme_id
                )
            END AS has_theme
        FROM hgis.g_foot g
        JOIN unit_name un
            ON un.g_unit = g.g_unit
           AND un.rn = 1
        WHERE g.use_for_stat_map = 'Y'
          AND {unit_type_sql}
          AND (
                g.g_unit_type IN :timeless_unit_types
             OR :start_year IS NULL
             OR util.get_end_year(g.g_duration) >= :start_year
          )
          AND (
                g.g_unit_type IN :timeless_unit_types
             OR :end_year IS NULL
             OR util.get_start_year(g.g_duration) <= :end_year
          )
          {extra_sql}
        ORDER BY g.g_unit
        """
        return statement(sql, expanding_params=expanding_params)

    @staticmethod
    def _normalize_feature_row(row: dict[str, object]) -> MapFeatureRow:
        return MapFeatureRow(
            unit_id=coerce_int(row.get("g_unit")) or 0,
            unit_name=coerce_str(row.get("unit_name")) or "",
            unit_type=coerce_str(row.get("g_unit_type")) or "",
            geometry_geojson=coerce_str(row.get("geometry_geojson")) or "",
            start_year=coerce_int(row.get("start_year")),
            end_year=coerce_int(row.get("end_year")),
            has_theme=coerce_bool(row.get("has_theme")),
        )
