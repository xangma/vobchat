# app/callbacks/clientside_callbacks.py

from dash import Dash, html, Input, Output, State
from dash.dependencies import ALL

def register_clientside_callbacks(app: Dash):
    # app.clientside_callback(
    #     """
    #     function (n_clicks, oldValue) {
    #         // We'll read from the DOM ourselves:
    #         const storeEl = document.querySelector('#ctrl-pressed-store');
    #         if (!storeEl || !storeEl.dataset) {
    #             return false;
    #         }
    #         return storeEl.dataset.ctrlpressed === 'true';
    #     }
    #     """,
    #     Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
    #     State('ctrl-pressed-store', 'data'),
    #     Output('ctrl-pressed-store', 'data'),
    #     prevent_initial_call=True
    # )

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