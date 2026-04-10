from __future__ import annotations

from collections.abc import Sequence

from vobchat.db.models import (
    CategoryDataset,
    CategoryRow,
    TimeSeriesDataset,
    TimeSeriesRow,
)
from vobchat.db.repositories._base import (
    BaseRepository,
    PREFERRED_UNIT_NAME_CTE,
    WAY_MEASUREMENT_RE,
    coerce_float,
    coerce_int,
    coerce_str,
    parse_category,
    statement,
)


class SeriesRepository(BaseRepository):
    def fetch_series_for_units_and_cubes(
        self,
        unit_ids: Sequence[int | str],
        cube_ids: Sequence[str],
        *,
        cellrefs: Sequence[str] = (),
        dataitem_ids: Sequence[str] = (),
        start_year: float | None = None,
        end_year: float | None = None,
    ) -> TimeSeriesDataset:
        normalized_unit_ids = tuple(
            unit_id
            for unit_id in (coerce_int(value) for value in unit_ids)
            if unit_id is not None
        )
        normalized_cube_ids = tuple(
            cube_id for cube_id in (coerce_str(value) for value in cube_ids) if cube_id
        )
        normalized_cellrefs = tuple(
            cellref for cellref in (coerce_str(value) for value in cellrefs) if cellref
        )
        normalized_dataitem_ids = tuple(
            dataitem_id
            for dataitem_id in (coerce_str(value) for value in dataitem_ids)
            if dataitem_id
        )
        if not normalized_unit_ids or not normalized_cube_ids:
            return TimeSeriesDataset()

        cellref_filter = "AND d.cellref IN :cellrefs" if normalized_cellrefs else ""
        dataitem_filter = "AND m.dataitem_id IN :dataitem_ids" if normalized_dataitem_ids else ""
        expanding_params = ["unit_ids", "cube_ids"]
        if normalized_cellrefs:
            expanding_params.append("cellrefs")
        if normalized_dataitem_ids:
            expanding_params.append("dataitem_ids")
        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        , dataitem_category AS (
            SELECT
                dataitem_id,
                CASE
                    WHEN COUNT(DISTINCT cat_id) = 1 THEN MAX(cat_id)
                    ELSE NULL
                END AS cat_id
            FROM hgis.g_data_coord
            GROUP BY dataitem_id
        )
        SELECT
            d.end_date_decimal AS year,
            d.g_unit,
            u.g_unit_type,
            un.g_name AS unit_name,
            d.cellref,
            d.g_data AS value,
            m.ncuberef AS cube_id,
            m.dataitem_id AS dataitem_id,
            coord.cat_id AS cat_id,
            ncube.labl AS cube_name,
            ncube.text AS cube_text
        FROM hgis.g_data d
        JOIN hgis.g_data_map m
            ON d.cellref = m.cellref
        JOIN hgis.g_data_ent ncube
            ON m.ncuberef = ncube.ent_id
        JOIN hgis.g_unit u
            ON d.g_unit = u.g_unit
        JOIN unit_name un
            ON un.g_unit = d.g_unit
           AND un.rn = 1
        LEFT JOIN dataitem_category coord
            ON coord.dataitem_id = m.dataitem_id
        WHERE d.g_unit IN :unit_ids
          AND m.ncuberef IN :cube_ids
          {cellref_filter}
          {dataitem_filter}
          AND (:start_year IS NULL OR d.end_date_decimal >= :start_year)
          AND (:end_year IS NULL OR d.end_date_decimal <= :end_year)
        ORDER BY d.g_unit, m.ncuberef, d.end_date_decimal
        """
        rows = self.executor.fetch_all(
            statement(
                sql.format(
                    cellref_filter=cellref_filter,
                    dataitem_filter=dataitem_filter,
                ),
                expanding_params=tuple(expanding_params),
            ),
            {
                "unit_ids": normalized_unit_ids,
                "cube_ids": normalized_cube_ids,
                "cellrefs": normalized_cellrefs,
                "dataitem_ids": normalized_dataitem_ids,
                "start_year": start_year,
                "end_year": end_year,
                "user_lang": "eng",
            },
        )
        normalized_rows = tuple(self._normalize_series_row(row) for row in rows)
        return TimeSeriesDataset(
            rows=normalized_rows,
            unit_ids=normalized_unit_ids,
            cube_ids=normalized_cube_ids,
        )

    def fetch_category_breakdown(
        self,
        unit_ids: Sequence[int | str],
        cube_ids: Sequence[str],
        *,
        cellrefs: Sequence[str] = (),
        dataitem_ids: Sequence[str] = (),
        cat_ids: Sequence[str] = (),
        year: int,
    ) -> CategoryDataset:
        normalized_cat_ids = tuple(
            cat_id for cat_id in (coerce_str(value) for value in cat_ids) if cat_id
        )
        normalized_dataitem_ids = tuple(
            dataitem_id
            for dataitem_id in (coerce_str(value) for value in dataitem_ids)
            if dataitem_id
        )
        normalized_cellrefs = tuple(
            cellref for cellref in (coerce_str(value) for value in cellrefs) if cellref
        )
        if normalized_cat_ids or normalized_dataitem_ids:
            return self._fetch_category_breakdown_from_metadata(
                unit_ids=unit_ids,
                cube_ids=cube_ids,
                cellrefs=normalized_cellrefs,
                dataitem_ids=normalized_dataitem_ids,
                cat_ids=normalized_cat_ids,
                year=year,
            )

        dataset = self.fetch_series_for_units_and_cubes(
            unit_ids,
            cube_ids,
            cellrefs=normalized_cellrefs,
            dataitem_ids=normalized_dataitem_ids,
            start_year=year,
            end_year=year,
        )
        category_rows: list[CategoryRow] = []
        for row in dataset.rows:
            if not WAY_MEASUREMENT_RE.match(row.cell_ref or ""):
                continue
            category_group, category_label = parse_category(row.cell_ref)
            category_rows.append(
                CategoryRow(
                    year=int(row.year),
                    unit_id=row.unit_id,
                    unit_name=row.unit_name,
                    cube_id=row.cube_id,
                    category_group=category_group,
                    category_label=category_label or row.cell_ref or "",
                    value=row.value,
                    cell_ref=row.cell_ref,
                    dataitem_id=row.dataitem_id,
                    cat_id=row.cat_id,
                    category_source="heuristic_cellref",
                )
            )
        return CategoryDataset(
            rows=tuple(category_rows),
            year=year,
            unit_ids=dataset.unit_ids,
            cube_ids=dataset.cube_ids,
        )

    def _fetch_category_breakdown_from_metadata(
        self,
        *,
        unit_ids: Sequence[int | str],
        cube_ids: Sequence[str],
        cellrefs: Sequence[str],
        dataitem_ids: Sequence[str],
        cat_ids: Sequence[str],
        year: int,
    ) -> CategoryDataset:
        normalized_unit_ids = tuple(
            unit_id
            for unit_id in (coerce_int(value) for value in unit_ids)
            if unit_id is not None
        )
        normalized_cube_ids = tuple(
            cube_id for cube_id in (coerce_str(value) for value in cube_ids) if cube_id
        )
        normalized_cellrefs = tuple(
            cellref for cellref in (coerce_str(value) for value in cellrefs) if cellref
        )
        normalized_dataitem_ids = tuple(
            dataitem_id
            for dataitem_id in (coerce_str(value) for value in dataitem_ids)
            if dataitem_id
        )
        normalized_cat_ids = tuple(
            cat_id for cat_id in (coerce_str(value) for value in cat_ids) if cat_id
        )
        if (
            not normalized_unit_ids
            or not normalized_cube_ids
            or not normalized_dataitem_ids
            or not normalized_cat_ids
        ):
            return CategoryDataset(
                rows=(),
                year=year,
                unit_ids=normalized_unit_ids,
                cube_ids=normalized_cube_ids,
            )

        cellref_filter = "AND d.cellref IN :cellrefs" if normalized_cellrefs else ""
        expanding_params = ["unit_ids", "cube_ids", "dataitem_ids", "cat_ids"]
        if normalized_cellrefs:
            expanding_params.append("cellrefs")
        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        SELECT
            d.end_date_decimal AS year,
            d.g_unit,
            un.g_name AS unit_name,
            m.ncuberef AS cube_id,
            m.dataitem_id AS dataitem_id,
            coord.cat_id AS cat_id,
            d.cellref,
            d.g_data AS value,
            cat_ent.ent_id AS category_entity_id,
            COALESCE(cat_ent.labl, coord.cat_id, d.cellref) AS category_label
        FROM hgis.g_data d
        JOIN hgis.g_data_map m
            ON d.cellref = m.cellref
        JOIN hgis.g_data_coord coord
            ON coord.dataitem_id = m.dataitem_id
        JOIN unit_name un
            ON un.g_unit = d.g_unit
           AND un.rn = 1
        LEFT JOIN hgis.g_data_ent cat_ent
            ON cat_ent.ent_id = coord.cat_id
        WHERE d.g_unit IN :unit_ids
          AND m.ncuberef IN :cube_ids
          AND m.dataitem_id IN :dataitem_ids
          AND coord.cat_id IN :cat_ids
          {cellref_filter}
          AND d.end_date_decimal = :year
        ORDER BY d.g_unit, category_label
        """
        rows = self.executor.fetch_all(
            statement(
                sql.format(cellref_filter=cellref_filter),
                expanding_params=tuple(expanding_params),
            ),
            {
                "unit_ids": normalized_unit_ids,
                "cube_ids": normalized_cube_ids,
                "dataitem_ids": normalized_dataitem_ids,
                "cat_ids": normalized_cat_ids,
                "cellrefs": normalized_cellrefs,
                "year": float(year),
                "user_lang": "eng",
            },
        )
        category_rows = [
            CategoryRow(
                year=int(coerce_float(row.get("year")) or year),
                unit_id=coerce_int(row.get("g_unit")) or 0,
                unit_name=coerce_str(row.get("unit_name")),
                cube_id=coerce_str(row.get("cube_id")) or "",
                category_group=coerce_str(row.get("cat_id")),
                category_label=coerce_str(row.get("category_label")) or "",
                value=coerce_float(row.get("value")),
                cell_ref=coerce_str(row.get("cellref")),
                dataitem_id=coerce_str(row.get("dataitem_id")),
                cat_id=coerce_str(row.get("cat_id")),
                category_entity_id=coerce_str(row.get("category_entity_id")),
                category_source="metadata",
            )
            for row in rows
        ]
        return CategoryDataset(
            rows=tuple(category_rows),
            year=year,
            unit_ids=normalized_unit_ids,
            cube_ids=normalized_cube_ids,
        )

    @staticmethod
    def _normalize_series_row(row: dict[str, object]) -> TimeSeriesRow:
        return TimeSeriesRow(
            year=coerce_float(row.get("year")) or 0.0,
            unit_id=coerce_int(row.get("g_unit")) or 0,
            unit_name=coerce_str(row.get("unit_name")),
            unit_type=coerce_str(row.get("g_unit_type")),
            cube_id=coerce_str(row.get("cube_id")) or "",
            cube_label=coerce_str(row.get("cube_name")),
            cube_text=coerce_str(row.get("cube_text")),
            cell_ref=coerce_str(row.get("cellref")),
            value=coerce_float(row.get("value")),
            dataitem_id=coerce_str(row.get("dataitem_id")),
            cat_id=coerce_str(row.get("cat_id")),
        )
