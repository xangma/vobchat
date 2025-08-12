"""
Map panel layout using Dash Leaflet.

This component provides the core geospatial UI:
- A set of unit-type filter buttons (ids of pattern {'type': 'unit-filter', 'unit': <key>})
- A conditional year-range slider for time-filterable unit types
- A Leaflet map with a GeoJSON overlay layer that receives polygon features
  and uses the layer "hideout.selected" array for styling selected polygons
- A placeholder Div "place-disambiguation-markers" for dynamically created
  dl.Marker components when the workflow requests place disambiguation
- Two map overlay buttons for toggling unselected polygons and resetting selection

The styling function is registered to Dash assets via dash_extensions Namespace
so it is available as a JS function referenced by the GeoJSON layer.
"""

import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import html, dcc
from dash_extensions.javascript import Namespace, assign
from datetime import datetime
import json
from vobchat.utils.constants import UNIT_TYPES

color_dict = {k: v['color'] for k, v in UNIT_TYPES.items()}

style_function = f"""
function(feature, context) {{
    // Add a fallback for context or hideout if they're undefined
    const sel = (context && context.hideout) ? context.hideout.selected || [] : [];

    // Mapping unit types to outline colors:
    const unitColors = {color_dict};

    let unitType = feature.properties.g_unit_type || 'MOD_REG';
    let outlineColor = unitColors[unitType] || 'black';

    // Ensure consistent string comparison for feature ID matching
    const featureIdStr = String(feature.id);
    const isSelected = sel.includes(featureIdStr);

    if (isSelected) {{
        return {{
            color: 'red',
            fillColor: 'red',
            fillOpacity: 0.5,
            weight: 2
        }};
    }} else {{
        return {{
            color: outlineColor,
            fillColor: 'transparent',
            fillOpacity: 0.0,
            weight: 2
        }};
    }}
}}
"""


def create_map_layout(assets_folder):
    """Create the map panel layout with Dash Leaflet.

    Parameters
    - assets_folder: path passed from app initialization so the JS style
      function can be emitted into the assets pipeline via Namespace.dump.

    Returns a vertical flex container with filter controls and a map. The
    GeoJSON overlay uses the registered style function to render selected vs
    unselected polygons based on its "hideout.selected" list. The map and
    its controls are referenced by clientside callbacks and the SSE client.
    """

    map_namespace = Namespace("map_leaflet")
    map_namespace.add(style_function, "style_function")
    map_namespace.dump(assets_folder=assets_folder)

    # Create unit-type filter buttons; their style and labels are updated
    # client-side based on selection counts and active unit types
    buttons = []

    for k, v in UNIT_TYPES.items():
        buttons.append(
            dbc.Button(
                v['long_name'],
                id={'type': 'unit-filter', 'unit': k},
                # Initial styling is minimal; the callback will update it.
                color='secondary',
                outline=True,
                className="unit-filter-button me-2 mb-2",
                n_clicks=0,
                value=k
            )
        )

    return html.Div([
        html.H3("Map (Dash Leaflet)", className="mb-3"),

        # Main content container with proper flex layout
        html.Div(
            style={"display": "flex", "flexDirection": "column", "height": "100%"},
            children=[
                # Filter controls (fixed height, non-scrollable)
                html.Div(
                    style={"flexShrink": "0", "marginBottom": "10px"},
                    children=[
                        html.H4("Filter by Unit Type", className="mb-2"),
                        html.Div(buttons, className="d-flex flex-wrap"),
                        # Year range slider
                        html.Div([
                            html.H4("Filter by Year Range", className="mt-2 mb-2"),
                            dcc.RangeSlider(
                                id='year-range-slider',
                                min=1800,
                                max=datetime.now().year,
                                value=[datetime.now().year, datetime.now().year],
                                marks={
                                    1800: '1800',
                                    datetime.now().year: str(datetime.now().year)
                                },
                            ),
                        ], id='year-range-container', style={'display': 'none'}),
                    ]
                ),

                # Map container (flexible, takes remaining space)
                html.Div(
                    style={
                        "position": "relative",
                        "flex": "1 1 auto",
                        "minHeight": "300px",
                        "border": "1px solid #dee2e6",
                        "borderRadius": "5px",
                        "overflow": "hidden", # Keeps the map contained
                    },
                    children=[
                        dl.Map(
                            [
                                dl.TileLayer(),
                                dl.GeoJSON(
                                    id="geojson-layer",
                                    data={},
                                    format="geojson",  # Changed from flatgeobuf to geojson
                                    hideout=dict(selected=[]),
                                    zoomToBounds=True,
                                    options=dict(pane="overlayPane"),
                                    style=map_namespace("style_function"),
                                ),
                                # # Layer for place disambiguation markers
                                html.Div(
                                    id="place-disambiguation-markers",
                                    children=[],  # Will be populated dynamically with dl.Marker components
                                ),
                            ],
                            center=[55.0, 9.0],
                            zoom=5,
                            style={'height': '100%', 'width': '100%'},
                            id="leaflet-map",
                        ),
                        # Controls positioned absolutely over the map
                        dbc.Button(
                            'Hide Unselected Polygons',
                            id='toggle-unselected',
                            color="secondary",
                            active=True,
                            style={
                                'position': 'absolute',
                                'top': '10px',
                                'right': '10px',
                                'zIndex': '1000',
                            },
                        ),
                        dbc.Button(
                            "Reset Selections",
                            id="reset-selections",
                            color="secondary",
                            active=True,
                            style={
                                'position': 'absolute',
                                'top': '55px',
                                'right': '10px',
                                'zIndex': '1000'
                            }
                        )
                    ]
                ),
            ]
        )
    ], style={"height": "100%", "display": "flex", "flexDirection": "column"})
