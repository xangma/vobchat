from __future__ import annotations

from typing import Any

from vobchat.db.models import BoundingBox
from vobchat.db.repositories.maps import MapsRepository
from vobchat.db.repositories.metadata import MetadataRepository
from vobchat.db.repositories.places import PlacesRepository
from vobchat.db.repositories.series import SeriesRepository
from vobchat.db.repositories.themes import ThemesRepository


class ScriptedExecutor:
    def __init__(self, scripted_results: list[Any]) -> None:
        self.scripted_results = list(scripted_results)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def fetch_all(self, statement, params=None):
        self.calls.append(("all", str(statement), dict(params or {})))
        result = self.scripted_results.pop(0)
        return [dict(item) for item in result]

    def fetch_one(self, statement, params=None):
        self.calls.append(("one", str(statement), dict(params or {})))
        result = self.scripted_results.pop(0)
        if result is None:
            return None
        if isinstance(result, list):
            return dict(result[0]) if result else None
        return dict(result)


def test_places_repository_normalizes_exact_and_fuzzy_results():
    executor = ScriptedExecutor(
        [
            [
                {
                    "g_place": "101",
                    "g_name": "LONDON",
                    "g_county": "9",
                    "county_name": "Greater London",
                    "g_nation": "1",
                    "nation_name": "England",
                    "g_domain": "2",
                    "domain_name": "Britain",
                    "g_state": "3",
                    "state_name": "UK",
                    "lat": "51.5074",
                    "lon": "-0.1278",
                    "g_unit": [1001, 1002],
                    "g_unit_type": ["MOD_REG", "NONE"],
                }
            ],
            [
                {
                    "g_place": 202,
                    "g_name": "LONDONDERRY",
                    "g_county": None,
                    "county_name": "County Londonderry",
                    "g_nation": 4,
                    "nation_name": "Northern Ireland",
                    "g_domain": None,
                    "domain_name": None,
                    "g_state": None,
                    "state_name": None,
                    "lat": 54.997,
                    "lon": -7.309,
                    "g_unit": [2201],
                    "g_unit_type": ["MOD_DIST"],
                }
            ],
        ]
    )
    repo = PlacesRepository(executor)

    exact = repo.search_places_exact("London", county_id=9, unit_types=["MOD_REG"])
    fuzzy = repo.search_places_fuzzy("Lond", unit_types=["MOD_DIST", "MOD_REG"])

    assert exact[0].place_id == 101
    assert exact[0].unit_ids == (1001, 1002)
    assert exact[0].unit_types == ("MOD_REG",)
    assert exact[0].match_type == "exact"
    assert fuzzy[0].place_id == 202
    assert fuzzy[0].match_type == "fuzzy"

    exact_call = executor.calls[0]
    fuzzy_call = executor.calls[1]
    assert ":place_name" in exact_call[1]
    assert exact_call[2]["county_id"] == 9
    assert exact_call[2]["unit_types"] == ("MOD_REG",)
    assert ":place_name_pattern" in fuzzy_call[1]
    assert fuzzy_call[2]["place_name_pattern"] == "%Lond%"


def test_themes_repository_normalizes_themes_and_cubes():
    executor = ScriptedExecutor(
        [
            [{"ent_id": "T_POP", "labl": "Population", "text": "Population theme"}],
            {"ent_id": "T_WK", "labl": "Work & Poverty", "text": "Theme text"},
            [
                {
                    "theme_id": "T_POP",
                    "cube_id": "N_POP_12WAY",
                    "cube": "Population by category",
                    "cube_text": "Breakdown",
                    "start": "1801",
                    "end": "1911",
                    "count": "4",
                }
            ],
        ]
    )
    repo = ThemesRepository(executor)

    themes = repo.list_themes()
    resolved = repo.lookup_theme("work")
    cubes = repo.list_cubes_for_unit_theme(99, "T_POP")

    assert themes[0].theme_id == "T_POP"
    assert resolved and resolved.theme_id == "T_WK"
    assert cubes[0].cube_id == "N_POP_12WAY"
    assert cubes[0].has_categories is True
    assert cubes[0].observation_count == 4

    assert executor.calls[1][2]["theme_pattern"] == "%work%"
    assert executor.calls[2][2]["unit_id"] == 99


def test_series_repository_normalizes_series_and_categories():
    executor = ScriptedExecutor(
        [
            [
                {
                    "year": "1901",
                    "g_unit": "10",
                    "g_unit_type": "MOD_DIST",
                    "unit_name": "Portsmouth",
                    "cellref": "N_POP_12WAY:Male",
                    "value": "1200",
                    "cube_id": "N_POP_12WAY",
                    "cube_name": "Population",
                    "cube_text": "Population total",
                }
            ],
            [
                {
                    "year": "1901",
                    "g_unit": "10",
                    "g_unit_type": "MOD_DIST",
                    "unit_name": "Portsmouth",
                    "cellref": "N_POP_12WAY:Male",
                    "value": "1200",
                    "cube_id": "N_POP_12WAY",
                    "cube_name": "Population",
                    "cube_text": "Population total",
                }
            ],
        ]
    )
    repo = SeriesRepository(executor)

    dataset = repo.fetch_series_for_units_and_cubes([10], ["N_POP_12WAY"])
    categories = repo.fetch_category_breakdown([10], ["N_POP_12WAY"], year=1901)

    assert dataset.unit_ids == (10,)
    assert dataset.cube_ids == ("N_POP_12WAY",)
    assert dataset.rows[0].value == 1200.0
    assert categories.rows[0].category_label == "Male"
    assert categories.rows[0].year == 1901

    assert executor.calls[0][2]["unit_ids"] == (10,)
    assert executor.calls[0][2]["cube_ids"] == ("N_POP_12WAY",)


def test_maps_repository_builds_bbox_query_and_normalizes_rows():
    executor = ScriptedExecutor(
        [
            [
                {
                    "g_unit": "301",
                    "g_unit_type": "MOD_REG",
                    "unit_name": "South East",
                    "geometry_geojson": '{"type":"Polygon","coordinates":[]}',
                    "start_year": None,
                    "end_year": None,
                    "has_theme": 1,
                }
            ]
        ]
    )
    repo = MapsRepository(executor)
    bbox = BoundingBox(min_x=-1.0, min_y=50.0, max_x=1.0, max_y=52.0)

    rows = repo.fetch_map_features_by_bbox(["MOD_REG"], bbox, theme_id="T_POP")

    assert rows[0].unit_id == 301
    assert rows[0].has_theme is True
    assert ":min_x" in executor.calls[0][1]
    assert executor.calls[0][2]["unit_types"] == ("MOD_REG",)


def test_metadata_repository_normalizes_unit_type_and_entity_info():
    executor = ScriptedExecutor(
        [
            {
                "g_unit_type": "MOD_DIST",
                "g_type_label": "Modern District",
                "g_type_level": "3",
                "level_label": "District",
                "g_adl_ft": "Polygon",
                "g_description": "Short description",
                "g_full_description": "Long description",
            },
            {"unit_count": "42"},
            [{"g_status": "A", "g_label": "Active"}],
            [{"unit_type": "MOD_CNTY", "g_type_label": "Modern County"}],
            [{"unit_type": "MOD_WARD", "g_type_label": "Modern Ward"}],
            [{"unit_type": "HIST_DIST", "g_type_label": "Historic District"}],
            [{"unit_type": "FUTURE_DIST", "g_type_label": "Future District"}],
            {
                "ent_id": "N_POP_TOTAL",
                "ent_type": "N",
                "ent_name": "Population total",
                "ent_short_name": "Population",
                "ent_text": "Entity text",
                "ent_additivity": "ADD",
                "rate_continuous": "Y",
                "rate_top": "TOP",
                "rate_bottom": "BOTTOM",
                "rate_mult": "1.0",
                "cube_root_unit": "Persons",
                "cube_root_name": "Person count",
                "theme_id": "T_POP",
                "rate_type": "count",
                "cube_display": "Y",
                "cube_download": "N",
                "type_name": "Cube",
                "type_text": "Cube type",
            },
            [{"rel_id": "T_POP", "rel_name": "Population", "rel_type": "T"}],
            [{"rel_id": "N_POP_MALE", "rel_name": "Population male", "rel_type": "N"}],
        ]
    )
    repo = MetadataRepository(executor)

    unit_type = repo.fetch_unit_type_info("MOD_DIST")
    entity = repo.fetch_data_entity_info("N_POP_TOTAL")

    assert unit_type is not None
    assert unit_type.identifier == "MOD_DIST"
    assert unit_type.unit_count == 42
    assert unit_type.may_be_part_of[0].unit_type == "MOD_CNTY"
    assert unit_type.statuses[0].code == "A"

    assert entity is not None
    assert entity.entity_id == "N_POP_TOTAL"
    assert entity.continuous is True
    assert entity.cube_display is True
    assert entity.cube_download is False
    assert entity.higher_entities[0].entity_id == "T_POP"
    assert entity.lower_entities[0].entity_id == "N_POP_MALE"
