"""
Visualization panel layout.

This component hosts the time-series or aggregated data visualizations that
are rendered as part of the assistant’s responses. The panel defaults to
hidden; the SSE client toggles visibility and populates cube selections.

Key elements/IDs:
- "visualization-area": main container toggled by SSE state updates
- "cube-selector": Dropdown for choosing which series/cubes to plot
- "data-plot": Plotly Graph area for the selected data
- "clear-plot-button": Button to clear selections/graph

The surrounding app code and callbacks handle wiring the data into this panel.
"""

from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc


def create_visualization_layout():
    """Create the data visualization panel layout.

    Returns an initially hidden, full-height container with a controls row
    and a flexible Plotly graph area. Visibility is managed client-side
    when cube data is available or explicitly hidden by state.
    """
    return html.Div(
        [
            html.Div(
                id="visualization-area",
                style={"height": "100%", "display": "none", "flexDirection": "column"},
                # Track if the panel was previously hidden
                **{"data-was-hidden": "true"},
                children=[
                    html.H3("Data Visualization", className="mb-3"),
                    # Main content area with flex layout
                    html.Div(
                        style={
                            "flex": "1",
                            "display": "flex",
                            "flexDirection": "column",
                            "minHeight": "0",
                        },
                        children=[
                            # Controls (fixed height)
                            html.Div(
                                style={"marginBottom": "10px", "flexShrink": "0"},
                                children=[
                                    dcc.Dropdown(
                                        id="cube-selector",
                                        placeholder="Select data series to visualize",
                                        multi=True,
                                    ),
                                    html.Div(
                                        id="viz-year-slider-container",
                                        style={
                                            "display": "none",
                                            "minWidth": "240px",
                                        },
                                        children=[
                                            dcc.Slider(
                                                id="viz-year-slider",
                                                min=0,
                                                max=0,
                                                step=None,  # snap to marks
                                                value=0,
                                                marks={},
                                                tooltip={
                                                    "placement": "bottom",
                                                    "always_visible": False,
                                                },
                                                updatemode="mouseup",
                                            )
                                        ],
                                    ),
                                ],
                            ),
                            # Graphs (flexible height) with tabs
                            html.Div(
                                style={
                                    "flex": "1",
                                    "minHeight": "0",
                                    "position": "relative",
                                },
                                children=[
                                    # Absolute wrapper ensures inner content can stretch to full height
                                    html.Div(
                                        id="viz-tabs-parent",
                                        style={
                                            "position": "absolute",
                                            "inset": 0,  # top:0,right:0,bottom:0,left:0
                                            "height": "100%",
                                            "width": "100%",
                                            "display": "flex",
                                            "flexDirection": "column",
                                            "minHeight": "0",
                                        },
                                        children=[
                                            dcc.Tabs(
                                                id="viz-tabs",
                                                value="line",
                                                children=[
                                                    dcc.Tab(
                                                        label="Line",
                                                        value="line",
                                                        children=[
                                                            html.Div(
                                                                className="viz-tab-body",
                                                                children=[
                                                                    dcc.Graph(
                                                                        id="data-plot",
                                                                        style={
                                                                            "height": "100%",
                                                                            "width": "100%",
                                                                        },
                                                                        config={
                                                                            "responsive": True
                                                                        },
                                            responsive=True,
                                        )
                                    ],
                                )
                            ],
                        ),
                                                    dcc.Tab(
                                                        id="categories-tab",
                                                        label="Categories",
                                                        value="categories",
                                                        disabled=True,  # enabled when *_WAY data is present
                                                        children=[
                                                            html.Div(
                                                                className="viz-tab-body",
                                                                children=[
                                                                    dcc.Graph(
                                                                        id="category-plot",
                                                                        style={
                                                                            "height": "100%",
                                                                            "width": "100%",
                                                                        },
                                                                        config={
                                                                            "responsive": True
                                                                        },
                                                                        responsive=True,
                                                                    )
                                                                ],
                                                            )
                                                        ],
                                                    ),
                                                    dcc.Tab(
                                                        id="data-tab",
                                                        label="Data",
                                                        value="data",
                                                        children=[
                                                            html.Div(
                                                                className="viz-tab-body",
                                                                children=[
                                                                    dash_table.DataTable(
                                                                        id="viz-data-table",
                                                                        columns=[],
                                                                        data=[],
                                                                        fill_width=True,
                                                                        page_action="native",
                                                                        page_size=25,
                                                                        sort_action="native",
                                                                        filter_action="native",
                                                                        style_table={"height": "100%", "overflowY": "auto"},
                                                                        style_cell={
                                                                            "fontSize": "12px",
                                                                            "padding": "6px",
                                                                            "textAlign": "left",
                                                                            "minWidth": "80px",
                                                                        },
                                                                        style_header={"fontWeight": "600"},
                                                                    )
                                                                ],
                                                            )
                                                        ],
                                                    ),
                                                ],
                                            )
                                        ],
                                    )
                                ],
                            ),
                            # Buttons (fixed height)
                            html.Div(
                                style={
                                    "marginTop": "10px",
                                    "flexShrink": "0",
                                    "display": "flex",
                                    "justifyContent": "space-between",
                                },
                                children=[
                                    dbc.Button(
                                        "Clear Plot",
                                        id="clear-plot-button",
                                        color="secondary",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )
        ],
        style={"height": "100%", "position": "relative"},
    )
