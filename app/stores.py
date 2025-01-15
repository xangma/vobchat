# app/stores.py
from dash import html, dcc

def create_stores():
    """Create unified store components"""
    return html.Div([
        # Map-related stores
        dcc.Store(id="map-state", data={
            "unit_types": ["MOD_REG"],
            "active_unit_type": None,
            "year_range": None,
            "year_bounds": None,
            "selected_polygons": [],
            "current_geojson": None
        }),
        
        # Place-related stores
        dcc.Store(id="place-state", data={
            "place_id": None,
            "unit_id": None,
            "unit_type": None,
            "themes": None,
            "selected_theme_id": None,
            "cubes": None
        }),
        
        # Global app state
        dcc.Store(id="app-state", data={
            "messages": [],
            "selection_idx": None
        }),
        
        # Thread management
        dcc.Store(id="thread-id", data=0),
        # 
        dcc.Store(id='ctrl-pressed-store', data=False),
    ])