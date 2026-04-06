from __future__ import annotations

from typing import Any

from vobchat.api.schemas.chat import PlannerResult


def planner_json_schema() -> dict[str, Any]:
    return PlannerResult.model_json_schema()


def planner_operation_descriptions() -> dict[str, str]:
    return {
        "search_places": "Find place candidates from a free-text place name query.",
        "lookup_postcode": "Resolve a postcode to matching reporting units.",
        "resolve_place": "Resolve a specific place id to a selected place state.",
        "remove_place": "Remove one or more selected places from thread state.",
        "list_themes": "List available themes.",
        "resolve_theme": "Resolve a theme code or label and update selected theme state.",
        "list_cubes_for_theme_and_unit": "List cubes for the current place/theme context.",
        "fetch_time_series": "Fetch comparable time-series data for selected units and theme context.",
        "fetch_category_breakdown": "Fetch category-style data for a given year.",
        "fetch_map_features": "Fetch map features for selected places or a requested unit type.",
        "fetch_place_profile": "Fetch place profile information for a selected or resolved place.",
        "fetch_unit_type_info": "Fetch unit type metadata.",
        "resolve_data_entity": "Resolve a data entity code or label.",
        "fetch_data_entity_info": "Fetch data entity metadata.",
        "reply_only": "Respond conversationally without changing structured state.",
        "clarify": "Ask a clarifying question because required inputs are missing or ambiguous.",
    }
