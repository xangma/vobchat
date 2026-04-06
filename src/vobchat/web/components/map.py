from __future__ import annotations

from datetime import datetime

import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import dcc, html
from dash_extensions.javascript import Namespace

from vobchat.utils.constants import UNIT_TYPES
from vobchat.web.state import empty_feature_collection


def create_map_layout(_assets_folder: str | None = None) -> html.Div:
    current_year = datetime.now().year
    map_namespace = Namespace("map_leaflet")
    unit_type_options = [
        {"label": value["long_name"], "value": key}
        for key, value in UNIT_TYPES.items()
    ]

    return html.Div(
        [
            html.Div(
                className="d-flex justify-content-between align-items-center mb-3",
                children=[
                    html.H3("Map", className="mb-0"),
                    html.Div(id="map-status", className="text-muted small"),
                ],
            ),
            dbc.Row(
                className="g-2 mb-3",
                children=[
                    dbc.Col(
                        dcc.Dropdown(
                            id="map-unit-type-dropdown",
                            options=unit_type_options,
                            value="MOD_REG",
                            clearable=False,
                        ),
                        md=6,
                    ),
                    dbc.Col(
                        dcc.RangeSlider(
                            id="map-year-range-slider",
                            min=1801,
                            max=current_year,
                            value=[1801, current_year],
                            marks={1801: "1801", current_year: str(current_year)},
                            tooltip={"placement": "bottom", "always_visible": False},
                        ),
                        md=6,
                    ),
                ],
            ),
            html.Div(
                style={
                    "flex": "1 1 auto",
                    "minHeight": "26rem",
                    "border": "1px solid #dee2e6",
                    "borderRadius": "0.5rem",
                    "overflow": "hidden",
                },
                children=[
                    dl.Map(
                        id="leaflet-map",
                        center=[54.5, -2.5],
                        zoom=5,
                        style={"height": "100%", "width": "100%"},
                        children=[
                            dl.TileLayer(),
                            dl.GeoJSON(
                                id="geojson-layer",
                                data=empty_feature_collection(),
                                hideout={"selected": [], "withTheme": []},
                                options={"pane": "overlayPane"},
                                style=map_namespace("style_function"),
                            ),
                        ],
                    )
                ],
            ),
        ],
        style={"height": "100%", "display": "flex", "flexDirection": "column"},
    )
