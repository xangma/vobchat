from __future__ import annotations

import logging

from sqlalchemy.exc import SQLAlchemyError

from vobchat.db.models import CategoryEntitySummary, ExactSliceSummary
from vobchat.db.repositories._base import (
    BaseRepository,
    coerce_float,
    coerce_int,
    coerce_str,
    statement,
)


logger = logging.getLogger(__name__)


class ExactSliceRepository(BaseRepository):
    def list_exact_slices_for_unit_theme(
        self,
        unit_id: int,
        theme_id: str,
        *,
        cube_id: str | None = None,
    ) -> list[ExactSliceSummary]:
        rows = self._fetch_exact_slice_rows(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_id=cube_id,
        )
        if not rows:
            rows = self._fetch_cellref_rows(
                unit_id=unit_id,
                theme_id=theme_id,
                cube_id=cube_id,
            )
        return self._normalize_rows(rows)

    def _fetch_exact_slice_rows(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None,
    ) -> list[dict[str, object]]:
        cube_filter = "AND ncube.ent_id = :cube_id" if cube_id else ""
        sql = """
        SELECT
            ncube.theme_id AS theme_id,
            ncube.ent_id AS cube_id,
            ncube.labl AS cube_label,
            MIN(ncube.text) AS cube_text,
            COALESCE(
                NULLIF(
                    STRING_AGG(DISTINCT cat_ent.labl, ' / ' ORDER BY cat_ent.labl),
                    ''
                ),
                map.cellref,
                ncube.labl
            ) AS slice_label,
            MIN(ncube.text) AS slice_text,
            map.cellref AS cellref,
            map.dataitem_id AS dataitem_id,
            CASE
                WHEN COUNT(DISTINCT coord.cat_id) = 1 THEN MAX(coord.cat_id)
                ELSE NULL
            END AS cat_id,
            CAST(NULL AS TEXT) AS view_id,
            MIN(data.end_date_decimal) AS start_year,
            MAX(data.end_date_decimal) AS end_year,
            COUNT(data.g_data) AS observation_count,
            CASE WHEN COUNT(coord.cat_id) > 0 THEN TRUE ELSE FALSE END AS has_categories,
            CASE
                WHEN COUNT(DISTINCT coord.cat_id) = 1 THEN MAX(coord.cat_id)
                ELSE NULL
            END AS category_entity_id,
            CASE
                WHEN COUNT(DISTINCT coord.cat_id) = 1 THEN MAX(cat_ent.labl)
                ELSE NULL
            END AS category_label,
            COUNT(DISTINCT coord.coordno) AS coord_dimensions
        FROM hgis.g_data data
        JOIN hgis.g_data_map map
            ON data.cellref = map.cellref
        JOIN hgis.g_data_ent ncube
            ON ncube.ent_id = map.ncuberef
        LEFT JOIN hgis.g_data_coord coord
            ON coord.dataitem_id = map.dataitem_id
        LEFT JOIN hgis.g_data_ent cat_ent
            ON cat_ent.ent_id = coord.cat_id
        WHERE data.g_unit = :unit_id
          AND ncube.theme_id = :theme_id
          {cube_filter}
        GROUP BY
            ncube.theme_id,
            ncube.ent_id,
            ncube.labl,
            map.cellref,
            map.dataitem_id
        ORDER BY ncube.labl, slice_label, map.cellref
        """.format(cube_filter=cube_filter)
        return self._fetch_rows(sql, unit_id=unit_id, theme_id=theme_id, cube_id=cube_id)

    def _fetch_cellref_rows(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None,
    ) -> list[dict[str, object]]:
        cube_filter = "AND ncube.ent_id = :cube_id" if cube_id else ""
        sql = """
        SELECT
            ncube.theme_id AS theme_id,
            ncube.ent_id AS cube_id,
            ncube.labl AS cube_label,
            MIN(ncube.text) AS cube_text,
            map.cellref AS slice_label,
            MIN(ncube.text) AS slice_text,
            map.cellref AS cellref,
            CAST(NULL AS TEXT) AS dataitem_id,
            CAST(NULL AS TEXT) AS cat_id,
            CAST(NULL AS TEXT) AS view_id,
            MIN(data.end_date_decimal) AS start_year,
            MAX(data.end_date_decimal) AS end_year,
            COUNT(data.g_data) AS observation_count,
            FALSE AS has_categories,
            CAST(NULL AS TEXT) AS category_entity_id,
            CAST(NULL AS TEXT) AS category_label,
            0 AS coord_dimensions
        FROM hgis.g_data data
        JOIN hgis.g_data_map map
            ON data.cellref = map.cellref
        JOIN hgis.g_data_ent ncube
            ON ncube.ent_id = map.ncuberef
        WHERE data.g_unit = :unit_id
          AND ncube.theme_id = :theme_id
          {cube_filter}
        GROUP BY ncube.theme_id, ncube.ent_id, ncube.labl, map.cellref
        ORDER BY ncube.labl, map.cellref
        """.format(cube_filter=cube_filter)
        return self._fetch_rows(sql, unit_id=unit_id, theme_id=theme_id, cube_id=cube_id)

    def _fetch_rows(
        self,
        sql: str,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None,
    ) -> list[dict[str, object]]:
        try:
            return self.executor.fetch_all(
                statement(sql),
                {"unit_id": unit_id, "theme_id": theme_id, "cube_id": cube_id},
            )
        except SQLAlchemyError:
            logger.warning(
                "exact_slice_query_failed",
                extra={"unit_id": unit_id, "theme_id": theme_id, "cube_id": cube_id},
                exc_info=True,
            )
            return []

    def _normalize_rows(self, rows: list[dict[str, object]]) -> list[ExactSliceSummary]:
        seen: set[tuple[str | None, str | None, str | None]] = set()
        normalized: list[ExactSliceSummary] = []
        for row in rows:
            cube_id = coerce_str(row.get("cube_id"))
            dataitem_id = coerce_str(row.get("dataitem_id"))
            cell_ref = coerce_str(row.get("cellref"))
            key = (cube_id, dataitem_id, cell_ref)
            if key in seen:
                continue
            seen.add(key)

            category_entity_id = coerce_str(row.get("category_entity_id"))
            category_label = coerce_str(row.get("category_label"))
            coord_dimensions = coerce_int(row.get("coord_dimensions")) or 0
            provenance = (
                "g_data_map+g_data_coord"
                if dataitem_id or category_entity_id
                else "cellref_only"
            )

            normalized.append(
                ExactSliceSummary(
                    theme_id=coerce_str(row.get("theme_id")) or "",
                    cube_id=cube_id or "",
                    cube_label=coerce_str(row.get("cube_label")) or cube_id or "",
                    cube_text=coerce_str(row.get("cube_text")),
                    slice_label=coerce_str(row.get("slice_label")) or coerce_str(row.get("cube_label")),
                    slice_text=coerce_str(row.get("slice_text")) or coerce_str(row.get("cube_text")),
                    cell_ref=cell_ref,
                    dataitem_id=dataitem_id,
                    cat_id=coerce_str(row.get("cat_id")),
                    view_id=coerce_str(row.get("view_id")),
                    start_year=coerce_float(row.get("start_year")),
                    end_year=coerce_float(row.get("end_year")),
                    observation_count=coerce_int(row.get("observation_count")) or 0,
                    has_categories=coord_dimensions > 0,
                    category_entity=(
                        CategoryEntitySummary(
                            entity_id=category_entity_id,
                            label=category_label,
                            source="g_data_coord",
                            provenance=provenance,
                        )
                        if category_entity_id or category_label
                        else None
                    ),
                    provenance=provenance,
                )
            )
        return normalized
