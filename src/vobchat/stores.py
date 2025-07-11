# app/stores.py
from dash import html, dcc

map_state_data = {
    "unit_types": ["MOD_REG"],
    "active_unit_type": None,
    "year_range": None,
    "year_bounds": None,
    "show_unselected": True,
    "zoom_to_selection": False,
    # "programmatic_unit_change_pending": None,
}

place_state_data = {
    "place_id": None,
    "unit_id": None,
    "unit_type": None,
    "themes": None,
    "selected_theme_id": None,
    "cubes": None,
    "cube_data": None,
}

app_state_data = {
    "messages": [],
    "awaiting_user_selection": None,
    "button_options": [],
    "selection_idx": None,
    "retrigger_chat": False,
    "show_visualization": False,
    "render_cursor": 0,
}

def create_stores():
    """Create unified store components"""
    return html.Div([
        # Map-related store
        dcc.Store(id="map-state", data=map_state_data),
        dcc.Store(id='ctrl-pressed-store', data=False),
        dcc.Store(id="map-moveend-trigger", data=None),
        dcc.Store(id="current_geojson", data={"type": "FeatureCollection", "features": []}),

        # Place-related store
        dcc.Store(id="place-state", data=place_state_data),

        # Global app state
        dcc.Store(id="app-state", data=app_state_data),

        # Chat related stores
        dcc.Store(id="thread-id", data=None),
        dcc.Store(id="retrigger-chat", data=None),
        dcc.Store(id="counts-store", data={}),
        dcc.Store(id='current-year-store'),
        dcc.Store(id='ctrl-listener-attached'),
        dcc.Store(id='moveend-listener-setup'),
        dcc.Store(id='map-moveend-processed'),
        dcc.Store(id='refresh-handled'),
        dcc.Store(id='zoom-handled'),
        dcc.Store(id='map-resize-debouncer'),
        dcc.Store(id='zoomend-listener-setup'),
        dcc.Store(id='map-event-listener-setup'),
        dcc.Store(id='zoom-cleanup-trigger-store'),
        dcc.Store(id='map-click-add-trigger'),
        dcc.Store(id='map-click-remove-trigger'),
        dcc.Store(id='visualization-zoom-trigger'),
        dcc.Store(id='visualization-resize-debouncer'),

        dcc.Store(id='sse-connection-status', data={}),
        dcc.Store(id='sse-event-processor', data={}),
        dcc.Store(id='sse-interrupt-store', data={}),

        dcc.Store(id='place-disambiguation-markers', data=[]),
    ])
