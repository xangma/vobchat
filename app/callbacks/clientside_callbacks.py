# app/callbacks/clientside_callbacks.py

from dash import Dash, html, Input, Output, State
from dash.dependencies import ALL

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