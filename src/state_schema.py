"""Single source of truth for the LangGraph state shape.

Moving `lg_State` out of `workflow.py` avoids circular‑import headaches when
helper modules import it (e.g. state_nodes.py ⇢ workflow.py, while workflow.py
also imports those helpers).
"""

from typing import Annotated, Optional, List
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages  # adds message‑append semantics
from langchain_core.messages import AnyMessage


class lg_State(TypedDict):
    # conversation
    messages: Annotated[List[AnyMessage], add_messages]

    # user‑choice plumbing
    selection_idx: Optional[int]

    # place + unit selections
    selected_place_g_places: List[Optional[int]]
    selected_place_g_units: List[Optional[int]]
    selected_place_g_unit_types: List[Optional[str]]

    # theme selection
    selected_place_themes: Optional[str]
    selected_theme: Optional[str]

    # extraction results
    extracted_place_names: List[str]
    extracted_counties: List[str]
    extracted_theme: Optional[str]
    is_postcode: bool
    extracted_postcode: Optional[str]

    # multi‑place machinery
    multi_place_search_df: Optional[str]
    current_place_index: int

    # year filters
    min_year: Optional[int]
    max_year: Optional[int]

    # map interaction
    selected_polygons: Optional[List[str]]
    selected_polygons_unit_types: Optional[List[str]]
    
    # misc / meta
    current_node: Optional[str]
    last_intent_payload: Optional[dict]
    options: Optional[List[dict]]
    message: Optional[str]