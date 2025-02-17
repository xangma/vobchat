# layout.py
import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import html, dcc
from dash_extensions.javascript import assign
from datetime import datetime
import json
from utils.constants import UNIT_TYPES

color_dict = {k: v['color'] for k, v in UNIT_TYPES.items()}
# The clientside style function for the GeoJSON layer remains unchanged.
style_function = assign(f"""
function(feature, context) {{
    const sel = context.hideout.selected || [];
    
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
""")


def create_map_layout(initial_gdf):
    """
    Creates the layout that uses Dash Leaflet instead of Plotly.
    """

    buttons = []

    for k, v in UNIT_TYPES.items():
        buttons.append(
            dbc.Button(
                v['long_name'],
                id={'type': 'unit-filter', 'unit': k},
                # Initial styling is minimal; the callback will update it.
                color='secondary',
                outline=True,
                className="filter-button me-2 mb-2",
                n_clicks=0,
                value=k
            )
        )

    return html.Div([
        html.H3("Map (Dash Leaflet)"),

        # Include an initial map-state store so the default is set.
        dcc.Store(id="map-state",
                  data={"unit_types": ["MOD_REG"], "selected_polygons": []}),

        dbc.Card([
            dbc.CardBody([
                html.H4("Filter by Unit Type", className="mb-2"),
                html.Div(buttons, className="d-flex flex-wrap"),
                dbc.Button("Reset Selections", id="reset-selections",
                           color="secondary", className="mb-2"),
                html.Div([
                    html.H4("Filter by Year Range", className="mt-3 mb-2"),
                    dcc.RangeSlider(
                        id='year-range-slider',
                        min=1800,
                        max=datetime.now().year,
                        value=[datetime.now().year, datetime.now().year],
                        marks={1800: '1800', datetime.now(
                        ).year: str(datetime.now().year)},
                    ),
                ], id='year-range-container', style={'display': 'none'}),
            ]),
        ], className="mb-3"),

        dl.Map(
            [
                dl.TileLayer(),
                dl.GeoJSON(
                    id="geojson-layer",
                    data=json.loads(initial_gdf.to_json()),
                    hideout=dict(selected=[]),
                    zoomToBounds=True,
                    options=dict(pane="overlayPane"),
                    style=style_function,
                ),
            ],
            center=[55.0, 10.0],
            zoom=5,
            style={'height': '70vh'},
            id="leaflet-map",
        ),

        html.Div(id='debug-output', style={'whiteSpace': 'pre-line'}),
        html.Button("Reset Selections", id="reset-btn", n_clicks=0),
    ])
