from __future__ import annotations

from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc


def create_visualization_layout() -> html.Div:
    return html.Div(
        [
            html.Div(
                className="d-flex justify-content-between align-items-center mb-3",
                children=[
                    html.H3("Visualization", className="mb-0"),
                    html.Div(id="visualization-status", className="text-muted small"),
                ],
            ),
            dbc.Row(
                className="g-2 mb-3",
                children=[
                    dbc.Col(
                        dcc.Dropdown(
                            id="theme-selector",
                            options=[],
                            value=None,
                            placeholder="Select a theme",
                            clearable=True,
                        ),
                        md=4,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="cube-selector",
                            options=[],
                            value=[],
                            placeholder="Select one or more cubes",
                            multi=True,
                        ),
                        md=5,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="category-year-selector",
                            options=[],
                            value=None,
                            placeholder="Category year",
                            clearable=False,
                        ),
                        md=3,
                    ),
                ],
            ),
            dcc.Tabs(
                id="visualization-tabs",
                value="line",
                children=[
                    dcc.Tab(
                        label="Line",
                        value="line",
                        children=[
                            dcc.Loading(
                                type="default",
                                children=dcc.Graph(
                                    id="time-series-graph",
                                    figure={},
                                    style={"height": "24rem"},
                                    config={"responsive": True},
                                ),
                            )
                        ],
                    ),
                    dcc.Tab(
                        label="Categories",
                        value="categories",
                        children=[
                            dcc.Loading(
                                type="default",
                                children=dcc.Graph(
                                    id="category-graph",
                                    figure={},
                                    style={"height": "24rem"},
                                    config={"responsive": True},
                                ),
                            )
                        ],
                    ),
                    dcc.Tab(
                        label="Data",
                        value="data",
                        children=[
                            dash_table.DataTable(
                                id="series-data-table",
                                columns=[],
                                data=[],
                                page_action="native",
                                page_size=15,
                                sort_action="native",
                                filter_action="native",
                                style_table={"height": "24rem", "overflowY": "auto"},
                                style_cell={
                                    "fontSize": "12px",
                                    "padding": "0.4rem",
                                    "textAlign": "left",
                                },
                                style_header={"fontWeight": "600"},
                            )
                        ],
                    ),
                ],
            ),
            html.Hr(className="my-4"),
            html.H4("Metadata", className="mb-3"),
            dbc.Row(
                className="g-3",
                children=[
                    dbc.Col(dbc.Card(dbc.CardBody(id="place-profile-panel")), md=6),
                    dbc.Col(dbc.Card(dbc.CardBody(id="key-findings-panel")), md=6),
                    dbc.Col(dbc.Card(dbc.CardBody(id="unit-type-panel")), md=6),
                    dbc.Col(dbc.Card(dbc.CardBody(id="data-entity-panel")), md=6),
                ],
            ),
        ],
        style={"height": "100%", "display": "flex", "flexDirection": "column"},
    )
