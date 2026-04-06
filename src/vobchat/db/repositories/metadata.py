from __future__ import annotations

from vobchat.db.models import (
    DataEntityInfo,
    DataEntityReference,
    DataEntityRelation,
    PlaceKeyFinding,
    PlaceProfile,
    UnitTypeInfo,
    UnitTypeRelation,
    UnitTypeStatus,
)
from vobchat.db.repositories._base import (
    BaseRepository,
    coerce_bool,
    coerce_float,
    coerce_int,
    coerce_str,
    statement,
)


class MetadataRepository(BaseRepository):
    def fetch_place_profile(self, place_id: int) -> PlaceProfile | None:
        sql = """
        SELECT
            g_place,
            g_name,
            g_container AS county,
            county_name,
            nation_name,
            state_name,
            domain_name,
            district_name,
            district_type,
            dg_text_auth,
            dg_text,
            notes,
            see_also_place
        FROM hgis.g_place
        WHERE g_place = :place_id
        LIMIT 1
        """
        row = self.executor.fetch_one(statement(sql), {"place_id": place_id})
        if row is None:
            return None

        see_also_name = None
        see_also_place_id = coerce_int(row.get("see_also_place"))
        if see_also_place_id is not None:
            see_also_row = self.executor.fetch_one(
                statement(
                    "SELECT g_name FROM hgis.g_place WHERE g_place = :place_id LIMIT 1"
                ),
                {"place_id": see_also_place_id},
            )
            if see_also_row:
                see_also_name = coerce_str(see_also_row.get("g_name"))

        return PlaceProfile(
            place_id=coerce_int(row.get("g_place")) or place_id,
            name=coerce_str(row.get("g_name")),
            county=coerce_str(row.get("county")),
            county_name=coerce_str(row.get("county_name")),
            nation_name=coerce_str(row.get("nation_name")),
            state_name=coerce_str(row.get("state_name")),
            domain_name=coerce_str(row.get("domain_name")),
            district_name=coerce_str(row.get("district_name")),
            district_type=coerce_str(row.get("district_type")),
            text_author=coerce_str(row.get("dg_text_auth")),
            text=coerce_str(row.get("dg_text")),
            notes=coerce_str(row.get("notes")),
            see_also_place_id=see_also_place_id,
            see_also_place_name=see_also_name,
        )

    def fetch_place_key_findings(self, unit_id: int) -> tuple[PlaceKeyFinding, ...]:
        sql = """
        SELECT
            g_url,
            g_label,
            g_text
        FROM hgis.g_unit_key_findings
        WHERE g_unit = :unit_id
        ORDER BY g_seq
        LIMIT 8
        """
        rows = self.executor.fetch_all(statement(sql), {"unit_id": unit_id})
        return tuple(
            PlaceKeyFinding(
                url=coerce_str(row.get("g_url")),
                label=coerce_str(row.get("g_label")),
                text=coerce_str(row.get("g_text")),
            )
            for row in rows
        )

    def fetch_unit_type_info(self, unit_type_or_label: str) -> UnitTypeInfo | None:
        if not (unit_type_or_label or "").strip():
            return None

        base_sql = """
        SELECT
            t.g_unit_type,
            t.g_type_label,
            t.g_type_level,
            ll.g_label AS level_label,
            l.g_adl_ft,
            t.g_description,
            t.g_full_description
        FROM hgis.g_unit_type t
        JOIN hgis.g_type_level l
            ON l.g_type_level = t.g_type_level
        JOIN hgis.g_type_level_label ll
            ON ll.g_type_level = l.g_type_level
        WHERE UPPER(t.g_unit_type) = UPPER(:query)
           OR LOWER(t.g_type_label) = LOWER(:query)
        ORDER BY
            CASE
                WHEN UPPER(t.g_unit_type) = UPPER(:query) THEN 0
                ELSE 1
            END,
            t.g_type_label ASC
        LIMIT 1
        """
        row = self.executor.fetch_one(statement(base_sql), {"query": unit_type_or_label})
        if row is None:
            return None

        identifier = coerce_str(row.get("g_unit_type")) or ""
        count_row = self.executor.fetch_one(
            statement(
                "SELECT COUNT(g_unit) AS unit_count FROM hgis.g_unit WHERE g_unit_type = :unit_type"
            ),
            {"unit_type": identifier},
        )

        def relation_rows(
            relation_type: str,
            *,
            relation_on_column: str,
            result_column: str,
        ) -> tuple[UnitTypeRelation, ...]:
            sql = f"""
            SELECT
                {result_column} AS unit_type,
                t.g_type_label
            FROM hgis.g_legal_rel r
            JOIN hgis.g_unit_type t
                ON t.g_unit_type = {result_column}
            WHERE r.g_rel_type = :relation_type
              AND r.{relation_on_column} = :unit_type
            ORDER BY t.g_type_label
            """
            rows = self.executor.fetch_all(
                statement(sql),
                {"relation_type": relation_type, "unit_type": identifier},
            )
            return tuple(
                UnitTypeRelation(
                    unit_type=coerce_str(item.get("unit_type")) or "",
                    label=coerce_str(item.get("g_type_label")) or "",
                )
                for item in rows
            )

        status_rows = self.executor.fetch_all(
            statement(
                """
                SELECT g_status, g_label
                FROM hgis.g_status_type
                WHERE g_unit_type = :unit_type
                ORDER BY g_status
                """
            ),
            {"unit_type": identifier},
        )

        return UnitTypeInfo(
            identifier=identifier,
            label=coerce_str(row.get("g_type_label")) or "",
            level=coerce_int(row.get("g_type_level")),
            level_label=coerce_str(row.get("level_label")),
            adl_feature_type=coerce_str(row.get("g_adl_ft")),
            description=coerce_str(row.get("g_description")),
            full_description=coerce_str(row.get("g_full_description")),
            unit_count=coerce_int(count_row.get("unit_count") if count_row else None)
            or 0,
            may_be_part_of=relation_rows(
                "IsPartOf",
                relation_on_column="g_unit_type",
                result_column="r.g_rel_unit_type",
            ),
            may_have_parts=relation_rows(
                "IsPartOf",
                relation_on_column="g_rel_unit_type",
                result_column="r.g_unit_type",
            ),
            may_have_succeeded=relation_rows(
                "SucceededBy",
                relation_on_column="g_rel_unit_type",
                result_column="r.g_unit_type",
            ),
            may_have_preceded=relation_rows(
                "SucceededBy",
                relation_on_column="g_unit_type",
                result_column="r.g_rel_unit_type",
            ),
            statuses=tuple(
                UnitTypeStatus(
                    code=coerce_str(item.get("g_status")) or "",
                    label=coerce_str(item.get("g_label")) or "",
                )
                for item in status_rows
            ),
        )

    def resolve_data_entity(self, query: str) -> DataEntityReference | None:
        if not (query or "").strip():
            return None
        sql = """
        SELECT
            ent_id,
            labl,
            ent_type
        FROM hgis.g_data_ent
        WHERE UPPER(ent_id) = UPPER(:query)
           OR LOWER(labl) = LOWER(:query)
           OR labl ILIKE :query_pattern
        ORDER BY
            CASE
                WHEN UPPER(ent_id) = UPPER(:query) THEN 0
                WHEN LOWER(labl) = LOWER(:query) THEN 1
                WHEN ent_type = 'N' THEN 2
                ELSE 3
            END,
            char_length(labl) ASC,
            labl ASC
        LIMIT 1
        """
        row = self.executor.fetch_one(
            statement(sql),
            {"query": query, "query_pattern": f"%{query}%"},
        )
        if row is None:
            return None
        return DataEntityReference(
            entity_id=coerce_str(row.get("ent_id")) or "",
            label=coerce_str(row.get("labl")) or "",
            entity_type=coerce_str(row.get("ent_type")) or "",
        )

    def fetch_data_entity_info(self, entity_id: str) -> DataEntityInfo | None:
        if not (entity_id or "").strip():
            return None

        entity_sql = """
        SELECT
            e.ent_id,
            e.ent_type,
            e.labl AS ent_name,
            e.short_labl AS ent_short_name,
            e.text AS ent_text,
            e.additivity AS ent_additivity,
            e.continuous AS rate_continuous,
            e.top_id AS rate_top,
            e.bottom_id AS rate_bottom,
            e.mult AS rate_mult,
            e.root_unit AS cube_root_unit,
            e.root_name AS cube_root_name,
            e.theme_id,
            e.rate_type,
            e.cube_display,
            e.cube_download,
            et.labl AS type_name,
            et.text AS type_text
        FROM hgis.g_data_ent e
        LEFT JOIN hgis.g_data_ent_type et
            ON et.ent_type = e.ent_type
        WHERE UPPER(e.ent_id) = UPPER(:entity_id)
        LIMIT 1
        """
        row = self.executor.fetch_one(statement(entity_sql), {"entity_id": entity_id})
        if row is None:
            return None

        higher_rows = self.executor.fetch_all(
            statement(
                """
                SELECT
                    e.ent_id AS rel_id,
                    e.labl AS rel_name,
                    e.ent_type AS rel_type
                FROM hgis.g_data_rel r
                JOIN hgis.g_data_ent e
                    ON e.ent_id = r.rel_id
                WHERE r.ent_id = :entity_id
                ORDER BY r.rel_seq
                """
            ),
            {"entity_id": entity_id},
        )
        lower_rows = self.executor.fetch_all(
            statement(
                """
                SELECT
                    e.ent_id AS rel_id,
                    e.labl AS rel_name,
                    e.ent_type AS rel_type
                FROM hgis.g_data_rel r
                JOIN hgis.g_data_ent e
                    ON e.ent_id = r.ent_id
                WHERE r.rel_id = :entity_id
                ORDER BY r.rel_seq
                """
            ),
            {"entity_id": entity_id},
        )

        return DataEntityInfo(
            entity_id=coerce_str(row.get("ent_id")) or entity_id,
            entity_type=coerce_str(row.get("ent_type")),
            name=coerce_str(row.get("ent_name")),
            short_name=coerce_str(row.get("ent_short_name")),
            text=coerce_str(row.get("ent_text")),
            additivity=coerce_str(row.get("ent_additivity")),
            continuous=coerce_bool(row.get("rate_continuous")),
            rate_top=coerce_str(row.get("rate_top")),
            rate_bottom=coerce_str(row.get("rate_bottom")),
            rate_multiplier=coerce_float(row.get("rate_mult")),
            cube_root_unit=coerce_str(row.get("cube_root_unit")),
            cube_root_name=coerce_str(row.get("cube_root_name")),
            theme_id=coerce_str(row.get("theme_id")),
            rate_type=coerce_str(row.get("rate_type")),
            cube_display=coerce_bool(row.get("cube_display")),
            cube_download=coerce_bool(row.get("cube_download")),
            type_name=coerce_str(row.get("type_name")),
            type_text=coerce_str(row.get("type_text")),
            higher_entities=tuple(self._normalize_entity_relation(item) for item in higher_rows),
            lower_entities=tuple(self._normalize_entity_relation(item) for item in lower_rows),
        )

    @staticmethod
    def _normalize_entity_relation(row: dict[str, object]) -> DataEntityRelation:
        return DataEntityRelation(
            entity_id=coerce_str(row.get("rel_id")) or "",
            label=coerce_str(row.get("rel_name")) or "",
            entity_type=coerce_str(row.get("rel_type")) or "",
        )
