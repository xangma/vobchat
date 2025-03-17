# app/callbacks/clientside_callbacks.py

from dash import Dash, html, Input, Output, State
from dash.dependencies import ALL
from ..utils.constants import UNIT_TYPES
import json

js_unit_types = json.dumps(UNIT_TYPES)

def register_clientside_callbacks(app: Dash):
    # Original callback
    app.clientside_callback(
    """
    function() {
        filterButtons = document.querySelectorAll('.unit-filter-button');
        filterButtons.forEach(btn => {
            if (!btn) {
                return window.dash_clientside.no_update;
            }

            btn.addEventListener("click", function(event) {
                const isCtrl = event.ctrlKey || event.metaKey;
                if (isCtrl) {
                    // We can set any Dash property using set_props.
                    // This will change the 'data' property of the dcc.Store
                    dash_clientside.set_props("ctrl-pressed-store", {data: true});
                    // console.log("Ctrl pressed");
                } else {
                    dash_clientside.set_props("ctrl-pressed-store", {data: false});
                    // console.log("Ctrl not pressed");
                }
            });
        });
        return dash_clientside.no_update;
    }
    """,
    Output('document', 'id'),
    Input('document', 'id'))

    # Add callback to handle map resize
    app.clientside_callback(
    """
    function() {
        // This callback will be triggered on window resize
        if (document.getElementById('leaflet-map')) {
            setTimeout(function() {
                // Invalidate the map size to make it adjust to its container
                window.dispatchEvent(new Event('resize'));
            }, 100);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('leaflet-map', 'id'),
    Input('map-panel', 'style'))
    
    # Add data-was-hidden attribute to visualization-area after the layout is defined
    app.clientside_callback(
        """
        function(n_clicks) {
            const visualizationArea = document.getElementById('visualization-area');
            if (visualizationArea) {
                visualizationArea.setAttribute('data-was-hidden', 'true');
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('document', 'className'),
        Input('document', 'id'),
    )
    
    app.clientside_callback(
        f"""
        function(counts, button_ids) {{
            // Define UNIT_TYPES on the client side
            const UNIT_TYPES = JSON.parse('{js_unit_types}');
            
            // Create results array for each button
            const results = [];
            
            // Process each button
            for (let i = 0; i < button_ids.length; i++) {{
                const unit = button_ids[i].unit;
                const label = UNIT_TYPES[unit] ? UNIT_TYPES[unit].long_name : unit;
                const count = counts[unit] || 0;
                
                if (count > 0) {{
                    // Create label with badge
                    results.push([
                        label,
                        {{
                            'props': {{
                                'children': count.toString(),
                                'color': 'light',
                                'text_color': 'dark', 
                                'pill': true,
                                'className': 'ms-1',
                                'style': {{'fontSize': '0.8em', 'verticalAlign': 'middle'}}
                            }},
                            'type': 'Badge',
                            'namespace': 'dash_bootstrap_components'
                        }}
                    ]);
                }} else {{
                    // Just the label
                    results.push(label);
                }}
            }}
            
            return results;
        }}
        """,
        Output({'type': 'unit-filter', 'unit': ALL}, 'children'),
        Input("counts-store", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id')
    )