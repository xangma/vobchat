# app/components/map.py
import json
import plotly.express as px
from dash import dcc, html
import dash_bootstrap_components as dbc
from utils.constants import UNIT_TYPES
from datetime import datetime

def create_initial_map_figure(initial_gdf):
    initial_geojson = json.loads(initial_gdf.to_json())
    fig = px.choropleth_mapbox(
        geojson=initial_geojson,
        locations=initial_gdf.index,
        center={"lat": initial_gdf.to_crs('+proj=cea').centroid.to_crs(initial_gdf.crs).y.mean(), 
                "lon": initial_gdf.to_crs('+proj=cea').centroid.to_crs(initial_gdf.crs).x.mean()},
        zoom=5,
        mapbox_style="open-street-map",
    )
    fig.update_traces(
        marker_line_width=0.5,
        marker_line_color='white',
        marker_opacity=0.7,
    )
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        mapbox=dict(style="carto-positron", zoom=6)
    )
    return fig

def create_map_layout(initial_gdf):
    return html.Div([
        html.H3("Map"),
        dbc.Card([
            dbc.CardBody([
                html.H4("Filter by Unit Type", className="mb-2"),
                html.Div([
                    dbc.Button(
                        unit_type,
                        id={'type': 'unit-filter', 'unit': unit_type},
                        color='primary' if unit_type == 'MOD_REG' else 'secondary',
                        outline=unit_type != 'MOD_REG',
                        className="me-2 mb-2"
                    ) for unit_type in UNIT_TYPES
                ], className="d-flex flex-wrap"),
                dbc.Button("Reset Filters", id="reset-filters", color="secondary", className="mb-2"),
                html.Div([
                    html.H4("Filter by Year Range", className="mt-3 mb-2"),
                    dcc.RangeSlider(id='year-range-slider',
                                    min=1800,
                                    max=datetime.now().year,
                                    value=[1800, datetime.now().year],
                                    marks={1800: '1800', datetime.now().year: str(datetime.now().year)}),
                ], id='year-range-container', style={'display': 'none'}),  # Hidden by default
            ]),
        ], className="mb-3"),
        dcc.Graph(id='choropleth-map', figure=create_initial_map_figure(initial_gdf), style={"height": "70vh"}),
        html.Div(id='debug-output', style={'whiteSpace': 'pre-line'}),
        html.Button("Reset Selections", id="reset-btn", n_clicks=0),
        dcc.Store(id="selected_ids"),
        dcc.Store(id="current-filter"),
        dcc.Store(id="active-filter", data=None),
        dcc.Store(id="current-gdf", storage_type='memory'),
        dcc.Store(id='filter-state', data={'unit_type': 'MOD_REG', 'year_range': None, 'year_bounds': None}),
        dcc.Store(id='unit-filter-state', data={'unit_type': 'MOD_REG'}),
        dcc.Store(id='year-range-bounds', data=None),
    ])