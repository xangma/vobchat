"""Single source of truth for the LangGraph state shape.

Moving `lg_State` out of `workflow.py` avoids circular-import headaches when
helper modules import it (e.g. state_nodes.py ⇢ workflow.py, while workflow.py
also imports those helpers).
"""

from typing import Annotated, Optional, List
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages  # adds message-append semantics
from langchain_core.messages import AnyMessage



def merge_lists(existing: Optional[List], new: Optional[List]) -> Optional[List]:
    """Helper function to merge lists, preferring new value if provided."""
    if new is not None:
        return new
    return existing


def merge_places(existing: Optional[List[dict]], new: Optional[List[dict]]) -> Optional[List[dict]]:
    """Helper function to merge places, preferring new value if provided."""
    if new is not None:
        return new
    return existing


def merge_string(existing: Optional[str], new: Optional[str]) -> Optional[str]:
    """Helper function to merge strings, preferring new value if provided."""
    if new is not None:
        return new
    return existing


def merge_int(existing: Optional[int], new: Optional[int]) -> Optional[int]:
    """Helper function to merge integers, preferring new value if provided."""
    if new is not None:
        return new
    return existing


def merge_dict(existing: Optional[dict], new: Optional[dict]) -> Optional[dict]:
    """Helper function to merge dictionaries, preferring new value if provided."""
    if new is not None:
        return new
    return existing


def merge_bool(existing: Optional[bool], new: Optional[bool]) -> Optional[bool]:
    """Helper function to merge booleans, preferring new value if provided."""
    if new is not None:
        return new
    return existing


class lg_State(TypedDict):
    # conversation
    messages: Annotated[List[AnyMessage], add_messages]

    intent_queue: Annotated[Optional[List[dict]], merge_lists]

    # user-choice plumbing
    selection_idx: Annotated[Optional[int], merge_int]

    # place + unit selections
    places: Annotated[Optional[List[dict]], merge_places]
    selected_place_g_places: Annotated[List[Optional[int]], merge_lists]
    selected_place_g_units: Annotated[List[Optional[int]], merge_lists]
    selected_place_g_unit_types: Annotated[List[Optional[str]], merge_lists]

    # theme selection
    selected_place_themes: Optional[str]
    selected_theme: Annotated[Optional[str], merge_string]

    # cube selection
    cubes : Annotated[Optional[List[str]], merge_lists]
    selected_cubes: Annotated[Optional[List[str]], merge_lists]

    # extraction results
    extracted_place_names: Annotated[List[str], merge_lists]
    extracted_counties: Annotated[List[str], merge_lists]
    extracted_unit_types: Annotated[Optional[List[str]], merge_lists]
    extracted_polygon_ids: Annotated[Optional[List[Optional[int]]], merge_lists]  # polygon IDs from map clicks
    extracted_theme: Annotated[Optional[str], merge_string]
    is_postcode: Optional[bool]
    extracted_postcode: Optional[str]

    # multi-place machinery
    multi_place_search_df: Optional[str]
    current_place_index: Annotated[Optional[int], merge_int]

    # year filters
    min_year: Optional[int]
    max_year: Optional[int]

    # map interaction
    selected_polygons: Annotated[Optional[List[str]], merge_lists]
    selected_polygons_unit_types: Annotated[Optional[List[str]], merge_lists]

    # misc / meta
    current_node: Annotated[Optional[str], merge_string]
    last_intent_payload: Annotated[Optional[dict], merge_dict]
    options: Annotated[Optional[List[dict]], merge_lists]
    message: Annotated[Optional[str], merge_string]
    continue_to_next_place: Annotated[Optional[bool], merge_bool]
    units_needing_map_selection: Annotated[Optional[List[int]], merge_lists]
    map_update_request: Annotated[Optional[dict], merge_dict]
    _theme_hint_done: Optional[bool]
    _prompted_for_place: Optional[bool]
