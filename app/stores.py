# app/stores.py
from dash import html, dcc


def create_stores():
    """Create unified store components"""
    return html.Div([
        # Map-related store
        dcc.Store(id="map-state", data={
            "unit_types": ["MOD_REG"],
            "active_unit_type": None,
            "year_range": None,
            "year_bounds": None,
            "selected_polygons": [],
            "current_geojson": None
        }),
        dcc.Store(id='ctrl-pressed-store', data=False),

        # Place-related store
        dcc.Store(id="place-state", data={
            "place_id": None,
            "unit_id": None,
            "unit_type": None,
            "themes": None,
            "selected_theme_id": None,
            "cubes": None,
            "cube_data": None
        }),

        # Global app state
        dcc.Store(id="app-state", data={
            "messages": [],
            "awaiting_user_selection": None,
            "button_options": [],
            "selection_idx": None,
            "retrigger_chat": False,
            "show_visualization": False,
        }),


        # Chat related stores
        dcc.Store(id="thread-id", data=0),
        dcc.Store(id="retrigger-chat", data=0),
    ])
