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
        if not normalized_unit_ids or not normalized_cube_ids:
            return TimeSeriesDataset()

        sql = f"""
        {PREFERRED_UNIT_NAME_CTE}
        SELECT
            d.end_date_decimal AS year,
            d.g_unit,
            u.g_unit_type,
            un.g_name AS unit_name,
            d.cellref,
            d.g_data AS value,
            m.ncuberef AS cube_id,
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
        WHERE d.g_unit IN :unit_ids
          AND m.ncuberef IN :cube_ids
          AND (:start_year IS NULL OR d.end_date_decimal >= :start_year)
          AND (:end_year IS NULL OR d.end_date_decimal <= :end_year)
        ORDER BY d.g_unit, m.ncuberef, d.end_date_decimal
        """
        rows = self.executor.fetch_all(
            statement(sql, expanding_params=("unit_ids", "cube_ids")),
            {
                "unit_ids": normalized_unit_ids,
                "cube_ids": normalized_cube_ids,
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
        year: int,
    ) -> CategoryDataset:
        dataset = self.fetch_series_for_units_and_cubes(
            unit_ids,
            cube_ids,
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
                )
            )
        return CategoryDataset(
            rows=tuple(category_rows),
            year=year,
            unit_ids=dataset.unit_ids,
            cube_ids=dataset.cube_ids,
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
        )
