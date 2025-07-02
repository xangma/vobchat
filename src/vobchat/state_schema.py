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


# Helper functions to derive values from the single source of truth (places array)
def get_selected_units(state) -> List[int]:
    """Get list of selected g_unit IDs from places array."""
    places = state.get("places", []) or []
    return [p.get("g_unit") for p in places if p.get("g_unit") is not None]


def get_selected_unit_types(state) -> List[str]:
    """Get list of selected unit types from places array."""
    places = state.get("places", []) or []
    return [p.get("g_unit_type") for p in places if p.get("g_unit_type") is not None]


def get_selected_place_names(state) -> List[str]:
    """Get list of selected place names from places array."""
    places = state.get("places", []) or []
    return [p.get("name") for p in places if p.get("name")]


def get_selected_place_ids(state) -> List[int]:
    """Get list of selected g_place IDs from places array."""
    places = state.get("places", []) or []
    return [p.get("g_place") for p in places if p.get("g_place") is not None]


def add_place_to_state(state, name: str, g_unit: Optional[int] = None, g_unit_type: Optional[str] = None, g_place: Optional[int] = None) -> None:
    """Add a place to the state using single source of truth."""
    places = state.get("places", []) or []

    # Check if place already exists (by g_unit if available, otherwise by name)
    for existing_place in places:
        if g_unit and existing_place.get("g_unit") == g_unit:
            return  # Already exists
        if not g_unit and existing_place.get("name") == name:
            return  # Already exists

    # Add new place
    new_place = {
        "name": name,
        "g_unit": g_unit,
        "g_unit_type": g_unit_type,
        "g_place": g_place,
        "candidate_rows": [],
        "unit_rows": []
    }
    places.append(new_place)
    state["places"] = places


def remove_place_from_state(state, identifier) -> bool:
    """Remove a place from state by name or g_unit ID. Returns True if removed."""
    places = state.get("places", []) or []

    # Try to remove by g_unit first, then by name
    for i, place in enumerate(places):
        if (isinstance(identifier, int) and place.get("g_unit") == identifier) or \
           (isinstance(identifier, str) and place.get("name", "").lower() == identifier.lower()):
            places.pop(i)
            state["places"] = places
            return True
    return False


class lg_State(TypedDict):
    # conversation
    messages: Annotated[List[AnyMessage], add_messages]

    intent_queue: Annotated[Optional[List[dict]], merge_lists]

    # user-choice plumbing
    selection_idx: Annotated[Optional[int], merge_int]

    # place + unit selections - SINGLE SOURCE OF TRUTH
    # places array contains: {"name": str, "g_unit": int, "g_unit_type": str, "g_place": int, ...}
    places: Annotated[Optional[List[dict]], merge_places]

    # theme selection
    selected_theme: Annotated[Optional[str], merge_string]
    extracted_theme: Annotated[Optional[str], merge_string]

    # cube selection
    cubes : Annotated[Optional[List[str]], merge_lists]
    selected_cubes: Annotated[Optional[List[str]], merge_lists]

    # processing state
    current_place_index: Annotated[Optional[int], merge_int]
    is_postcode: Optional[bool]
    extracted_postcode: Optional[str]

    # year filters
    min_year: Optional[int]
    max_year: Optional[int]

    # misc / meta
    current_node: Annotated[Optional[str], merge_string]
    last_intent_payload: Annotated[Optional[dict], merge_dict]
    options: Annotated[Optional[List[dict]], merge_lists]
    message: Annotated[Optional[str], merge_string]
    continue_to_next_place: Annotated[Optional[bool], merge_bool]
    units_needing_map_selection: Annotated[Optional[List[int]], merge_lists]
    map_update_request: Annotated[Optional[dict], merge_dict]
    _prompted_for_place: Optional[bool]
    show_visualization: Annotated[Optional[bool], merge_bool]
