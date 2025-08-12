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

from dash import dcc, html
import dash_bootstrap_components as dbc

def create_visualization_layout():
    """Create the data visualization panel layout.

    Returns an initially hidden, full-height container with a controls row
    and a flexible Plotly graph area. Visibility is managed client-side
    when cube data is available or explicitly hidden by state.
    """
    return html.Div([
        html.Div(
            id="visualization-area",
            style={"height": "100%", "display": "none", "flexDirection": "column"},
            # Track if the panel was previously hidden
            **{'data-was-hidden': 'true'},
            children=[
                html.H3("Data Visualization", className="mb-3"),
                
                # Main content area with flex layout
                html.Div(
                    style={"flex": "1", "display": "flex", "flexDirection": "column", "minHeight": "0"},
                    children=[
                        # Controls (fixed height)
                        html.Div(
                            style={"marginBottom": "10px", "flexShrink": "0"},
                            children=[
                                dcc.Dropdown(
                                    id="cube-selector",
                                    placeholder="Select data series to visualize",
                                    multi=True
                                )
                            ]
                        ),
                        
                        # Graph (flexible height)
                        html.Div(
                            style={"flex": "1", "minHeight": "0", "position": "relative"},
                            children=[
                                dcc.Graph(
                                    id="data-plot",
                                    style={"height": "100%"}
                                )
                            ]
                        ),
                        
                        # Buttons (fixed height)
                        html.Div(
                            style={"marginTop": "10px", "flexShrink": "0", "display": "flex", "justifyContent": "space-between"},
                            children=[
                                dbc.Button(
                                    "Clear Plot",
                                    id="clear-plot-button",
                                    color="secondary"
                                ),
                            ]
                        )
                    ]
                )
            ]
        )
    ], style={"height": "100%", "position": "relative"})
