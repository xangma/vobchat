# layout.py
import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import html, dcc
from dash_extensions.javascript import Namespace, assign
from datetime import datetime
import json
from ..utils.constants import UNIT_TYPES

color_dict = {k: v['color'] for k, v in UNIT_TYPES.items()}

style_function = f"""
function(feature, context) {{
    // Add a fallback for context or hideout if they're undefined
    const sel = (context && context.hideout) ? context.hideout.selected || [] : [];
    
    // Mapping unit types to outline colors:
    const unitColors = {color_dict};
    
    let unitType = feature.properties.g_unit_type || 'MOD_REG';
    let outlineColor = unitColors[unitType] || 'black';

    if (sel.includes(feature.id)) {{
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


def create_map_layout(initial_gdf, assets_folder):
    """
    Creates the layout that uses Dash Leaflet instead of Plotly.
    """
    
    map_namespace = Namespace("map_leaflet")
    map_namespace.add(style_function, "style_function")
    map_namespace.dump(assets_folder=assets_folder)
    
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
            style={"display": "flex", "flexDirection": "column", "height": "calc(100% - 40px)"},
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
                        "overflow": "hidden" # Keeps the map contained
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
                            ],
                            center=[55.0, 10.0],
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
                
                # Debug output (fixed height, always visible)
                html.Div(
                    id='debug-output',
                    style={
                        'whiteSpace': 'pre-line',
                        'marginTop': '10px',
                        'height': '40px',
                        'overflowY': 'auto',
                        'flexShrink': '0'
                    }
                ),
            ]
        )
    ], style={"height": "100%", "display": "flex", "flexDirection": "column"})