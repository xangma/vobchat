from __future__ import annotations

from datetime import datetime

from dash import dcc, html


def initial_thread_state() -> dict | None:
    return None


def initial_selection_state() -> dict:
    return {
        "current_receipt_id": None,
        "current_receipt_kind": None,
        "ui_projection": None,
        "render_projection": None,
        "selected_places": [],
        "selected_theme": None,
        "selected_cubes": [],
        "reporting_geography": None,
        "analysis_spec": None,
        "exact_slice": None,
        "time_scope": None,
        "active_output_mode": None,
        "available_themes": [],
        "available_cubes": [],
        "pending_place_candidates": [],
        "pending_clarification": None,
        "discovery_result": None,
        "defaults_used": [],
        "notices": [],
        "runtime_state": None,
        "provenance_summary": None,
    }


def initial_map_state() -> dict:
    current_year = datetime.now().year
    return {
        "ui_projection": None,
        "render_projection": None,
        "reporting_geography": None,
        "active_output_mode": None,
        "unit_type": "MOD_REG",
        "year_range": [1801, current_year],
        "bbox": None,
        "selected_ids": [],
        "with_theme_ids": [],
        "feature_count": 0,
        "last_loaded_unit_types": [],
        "notices": [],
        "provenance_summary": None,
        "status": "Choose a place or geography level to load a map.",
    }


def initial_visualization_state() -> dict:
    return {
        "current_receipt_id": None,
        "ui_projection": None,
        "render_projection": None,
        "dataset_family": None,
        "exact_slice": None,
        "time_scope": None,
        "active_output_mode": None,
        "active_tab": "line",
        "time_series": None,
        "category_breakdown": None,
        "category_year": None,
        "provenance_summary": None,
        "status": "Choose a place and a data theme to see a chart or table.",
    }


def initial_metadata_state() -> dict:
    return {
        "render_projection": None,
        "place_profile": None,
        "place_key_findings": None,
        "unit_type_info": None,
        "data_entity_resolution": None,
        "data_entity_info": None,
        "requested_unit_type": None,
        "requested_entity_id": None,
        "provenance_summary": None,
    }


def initial_request_status() -> dict:
    return {
        "mode": "stream",
        "busy": False,
        "error": None,
        "last_turn_id": None,
    }


def create_web_stores() -> html.Div:
    return html.Div(
        [
            dcc.Store(
                id="thread-state-store",
                data=initial_thread_state(),
                storage_type="session",
            ),
            dcc.Store(
                id="selection-state-store",
                data=initial_selection_state(),
                storage_type="session",
            ),
            dcc.Store(
                id="map-state-store",
                data=initial_map_state(),
                storage_type="session",
            ),
            dcc.Store(
                id="visualization-state-store",
                data=initial_visualization_state(),
                storage_type="session",
            ),
            dcc.Store(
                id="metadata-state-store",
                data=initial_metadata_state(),
                storage_type="session",
            ),
            dcc.Store(id="request-status-store", data=initial_request_status()),
        ]
    )
