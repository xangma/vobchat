# app/types.py
from typing import TypedDict, Optional, List, Union
from typing_extensions import NotRequired
from langchain_core.messages import AnyMessage

class MapState(TypedDict):
    """Unified state management for map-related data"""
    unit_types: List[str]                      # Currently selected unit types
    active_unit_type: Optional[str]            # Currently active unit type filter
    year_range: Optional[tuple[int, int]]      # Selected year range [start, end]
    year_bounds: Optional[tuple[int, int]]     # Available year bounds [min, max]
    selected_polygons: List[int]               # Currently selected polygon IDs
    current_geojson: Optional[dict]            # Current GeoJSON data for the map

class PlaceState(TypedDict):
    """State management for place-related data"""
    place_id: Optional[int]                    # Selected place ID
    unit_id: Optional[int]                     # Associated unit ID
    unit_type: Optional[str]                   # Unit type (e.g., 'MOD_REG')
    themes: Optional[List[dict]]               # Available themes for the place
    selected_theme_id: Optional[int]           # Currently selected theme
    cubes: Optional[List[dict]]               # Available data cubes

class AppState(TypedDict):
    """Global application state"""
    messages: List[AnyMessage]                 # Chat messages
    map_state: MapState                        # Map-related state
    place_state: PlaceState                    # Place-related state
    selection_idx: NotRequired[Optional[int]]  # Temporary selection index for UI