import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import html, dcc
from dash_extensions.javascript import assign
from datetime import datetime
import json
from utils.constants import UNIT_TYPES


# We define a clientside style function that highlights selected polygons in red,
# while non-selected polygons remain grey. We can store the list of selected IDs
# in "hideout.selected" (a list).
style_function = assign("""
function(feature, context) {
    const sel = context.hideout.selected || [];
    // Each feature has an 'id' property in feature.properties or feature.id
    // We'll assume 'feature.id' is the unique row index.
    let color = sel.includes(feature.id) ? 'red' : 'blue';
    return {
      color: color,
      fillColor: color,
      fillOpacity: 0.5,
      weight: 1
    }
}
""")

def create_map_layout(initial_gdf):
    """
    Creates the layout that uses Dash Leaflet instead of Plotly.
    """
    
    buttons = []
    
    for k,v in UNIT_TYPES.items():
        buttons.append(
            dbc.Button(
                v,
                id={'type': 'unit-filter', 'unit': k},
                color='primary' if k == 'MOD_REG' else 'secondary',
                outline=(k != 'MOD_REG'),
                className="filter-button me-2 mb-2",
                n_clicks=0,
                value=k
            )
        )
    
    return html.Div([
        html.H3("Map (Dash Leaflet)"),

        # Filter controls
        dbc.Card([
            dbc.CardBody([
                html.H4("Filter by Unit Type", className="mb-2"),
                html.Div(
                    buttons,
                    className="d-flex flex-wrap"
                ),

                dbc.Button("Reset Filters", id="reset-filters",
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

        # The dash_leaflet Map
        dl.Map(
            [
                dl.TileLayer(),
                # The dynamic GeoJSON layer that we (re)load from the server
                dl.GeoJSON(
                    id="geojson-layer",
                    data=json.loads(initial_gdf.to_json()),
                    hideout=dict(selected=[]),
                    zoomToBounds=True,
                    options=dict(pane="overlayPane"),  # optional
                    style=style_function,  # Apply the style function
                ),
            ],
            center=[55.0, 10.0],  # Some initial center
            zoom=5,
            style={'height': '70vh'},
            id="leaflet-map",     # We can reference the map if needed
        ),

        # Debug and selection controls
        html.Div(id='debug-output', style={'whiteSpace': 'pre-line'}),
        html.Button("Reset Selections", id="reset-btn", n_clicks=0),
    ])
