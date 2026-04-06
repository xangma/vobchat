from __future__ import annotations

from collections.abc import Sequence

from vobchat.db.models import CubeSummary, ThemeSummary
from vobchat.db.repositories._base import (
    BaseRepository,
    WAY_MEASUREMENT_RE,
    coerce_float,
    coerce_int,
    coerce_str,
    statement,
)


class ThemesRepository(BaseRepository):
    def list_themes(self, *, limit: int | None = None) -> list[ThemeSummary]:
        sql = """
        SELECT ent_id, labl, text
        FROM hgis.g_data_ent
        WHERE ent_type = 'T'
        ORDER BY labl
        """
        params: dict[str, object] = {}
        if limit is not None:
            sql += " LIMIT :limit"
            params["limit"] = limit
        rows = self.executor.fetch_all(statement(sql), params)
        return [self._normalize_theme_row(row) for row in rows]

    def lookup_theme(self, theme_query: str) -> ThemeSummary | None:
        if not (theme_query or "").strip():
            return None
        sql = """
        SELECT ent_id, labl, text
        FROM hgis.g_data_ent
        WHERE ent_type = 'T'
          AND (
                UPPER(ent_id) = UPPER(:theme_query)
             OR LOWER(labl) = LOWER(:theme_query)
             OR labl ILIKE :theme_pattern
          )
        ORDER BY
            CASE
                WHEN UPPER(ent_id) = UPPER(:theme_query) THEN 0
                WHEN LOWER(labl) = LOWER(:theme_query) THEN 1
                ELSE 2
            END,
            char_length(labl) ASC,
            labl ASC
        LIMIT 1
        """
        row = self.executor.fetch_one(
            statement(sql),
            {
                "theme_query": theme_query,
                "theme_pattern": f"%{theme_query}%",
            },
        )
        return self._normalize_theme_row(row) if row else None

    def list_themes_for_unit(self, unit_id: int) -> list[ThemeSummary]:
        sql = """
        SELECT DISTINCT
            theme.ent_id,
            theme.labl,
            theme.text
        FROM hgis.g_data data
        JOIN hgis.g_data_map map
            ON data.cellref = map.cellref
        JOIN hgis.g_data_ent ncube
            ON ncube.ent_id = map.ncuberef
        JOIN hgis.g_data_ent theme
            ON theme.ent_id = ncube.theme_id
        WHERE data.g_unit = :unit_id
        ORDER BY theme.labl
        """
        rows = self.executor.fetch_all(statement(sql), {"unit_id": unit_id})
        return [self._normalize_theme_row(row) for row in rows]

    def list_cubes_for_unit_theme(
        self,
        unit_id: int,
        theme_id: str,
    ) -> list[CubeSummary]:
        sql = """
        SELECT
            ncube.theme_id AS theme_id,
            ncube.ent_id AS cube_id,
            ncube.labl AS cube,
            MIN(ncube.text) AS cube_text,
            MIN(data.end_date_decimal) AS start,
            MAX(data.end_date_decimal) AS end,
            COUNT(data.g_data) AS count
        FROM hgis.g_data data
        JOIN hgis.g_data_map map
            ON data.cellref = map.cellref
        JOIN hgis.g_data_ent ncube
            ON ncube.ent_id = map.ncuberef
        WHERE data.g_unit = :unit_id
          AND ncube.theme_id = :theme_id
        GROUP BY ncube.theme_id, ncube.ent_id, ncube.labl
        ORDER BY ncube.labl
        """
        rows = self.executor.fetch_all(
            statement(sql),
            {"unit_id": unit_id, "theme_id": theme_id},
        )
        return [self._normalize_cube_row(row) for row in rows]

    @staticmethod
    def _normalize_theme_row(row: dict[str, object]) -> ThemeSummary:
        return ThemeSummary(
            theme_id=coerce_str(row.get("ent_id")) or "",
            label=coerce_str(row.get("labl")) or "",
            description=coerce_str(row.get("text")),
        )

    @staticmethod
    def _normalize_cube_row(row: dict[str, object]) -> CubeSummary:
        cube_id = coerce_str(row.get("cube_id")) or ""
        return CubeSummary(
            theme_id=coerce_str(row.get("theme_id")) or "",
            cube_id=cube_id,
            label=coerce_str(row.get("cube")) or "",
            description=coerce_str(row.get("cube_text")),
            start_year=coerce_float(row.get("start")),
            end_year=coerce_float(row.get("end")),
            observation_count=coerce_int(row.get("count")) or 0,
            has_categories=bool(WAY_MEASUREMENT_RE.match(cube_id)),
        )
