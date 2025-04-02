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

    # Add new callback for map moveend event
    app.clientside_callback(
        """
        function(mapState, mapTriggerId) {
            // Get the Leaflet map object
            const mapElement = document.getElementById('leaflet-map');
            if (!mapElement || !mapElement._leaflet_map) {
                return window.dash_clientside.no_update;
            }

            const map = mapElement._leaflet_map;
            
            // Set up the moveend event handler if it doesn't exist yet
            if (!window.moveendHandlerSet) {
                // Only set up the event handler when we confirm GeoJSON layer is ready
                const setupMoveendHandler = function() {
                    if (window.geojsonLayerReady) {
                        map.on('moveend', function() {
                            // Skip if the move was triggered by fitBounds
                            if (window.polygon_management && window.polygon_management.skipNextMoveend) {
                                window.polygon_management.skipNextMoveend = false;
                                return;
                            }
                            
                            // Trigger the callback by updating a store value
                            if (window.dash_clientside.set_props) {
                                window.dash_clientside.set_props("map-moveend-trigger", {data: Date.now()});
                            }
                        });
                        
                        window.moveendHandlerSet = true;
                        console.log("Moveend handler set up successfully");
                    } else {
                        // Check again in a moment
                        setTimeout(setupMoveendHandler, 100);
                    }
                };
                
                // Start the setup process
                setupMoveendHandler();
            }
            
            // Return no update as we're just setting up the handler
            return window.dash_clientside.no_update;
        }
        """,
        Output('leaflet-map', 'className'),
        Input('map-state', 'data'),
        Input('leaflet-map', 'id')
    )
    
    # Add callback to handle map moveend event
    app.clientside_callback(
        """
        function(moveendTrigger, mapState) {
            // Ignore initial load
            if (!moveendTrigger) {
                return window.dash_clientside.no_update;
            }
            
            // Get the Leaflet map object
            const mapElement = document.getElementById('leaflet-map');
            if (!mapElement || !mapElement._leaflet_map) {
                return window.dash_clientside.no_update;
            }

            const map = mapElement._leaflet_map;
            
            // Ensure GeoJSON layer is ready before proceeding
            if (!window.geojsonLayerReady) {
                console.warn("GeoJSON layer not ready, skipping moveend update");
                return window.dash_clientside.no_update;
            }
            
            // Get current bounds
            const bounds = map.getBounds();
            
            // Extract required data from map state
            const unitTypes = mapState.unit_types || ['MOD_REG'];
            const yearRange = mapState.year_range ? {
                min: mapState.year_range[0],
                max: mapState.year_range[1]
            } : null;
            
            // Update the map with the new bounds
            if (window.polygon_management) {
                window.polygon_management.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange)
                    .then(result => {
                        console.log(`Map updated on moveend with ${result.features ? result.features.length : 0} features`);
                    })
                    .catch(error => {
                        console.error('Error updating map on moveend:', error);
                    });
            }
            
            // Return no update as we handle the update directly
            return window.dash_clientside.no_update;
        }
        """,
        Output('debug-output', 'id'),
        Input('map-moveend-trigger', 'data'),
        State('map-state', 'data')
    )

    app.clientside_callback(
        """
        function(mapState, appState) {
            // Only call updateMapWithPolygons when GeoJSON layer is ready
            if (!window.geojsonLayerReady) {
                console.log("GeoJSON layer not ready, deferring updateMapWithPolygons");
                return [
                    {'display': 'none'},
                    window.dash_clientside.no_update,
                    window.dash_clientside.no_update,
                    "Waiting for map initialization...",
                    window.dash_clientside.no_update
                ];
            }
            
            // Check if we need to zoom to selection
            if (mapState && mapState.zoom_to_selection && mapState.selected_polygons && mapState.selected_polygons.length > 0) {
                // Get the map object
                const mapElement = document.getElementById('leaflet-map');
                if (mapElement && mapElement._leaflet_map) {
                    const map = mapElement._leaflet_map;
                    
                    // Use the zoomToSelectedIds function to zoom to the selected polygons
                    if (window.polygon_management) {
                        window.polygon_management.zoomToSelected(map, mapState.selected_polygons);
                    } 
                    
                    // Remove the zoom flag to prevent repeated zooming
                    let newMapState = {...mapState};
                    delete newMapState.zoom_to_selection;
                    window.dash_clientside.set_props("map-state", {data: newMapState});
                }
            }            
            
            // This function is defined in the polygon_management.js file
            return window.dash_clientside.clientside.updateMapWithPolygons(mapState, appState);
        }
        """,
        [
            Output('year-range-container', 'style', allow_duplicate=True),
            Output('geojson-layer', 'data', allow_duplicate=True),
            Output('geojson-layer', 'hideout', allow_duplicate=True),
            Output('debug-output', 'children', allow_duplicate=True),
            Output("current_geojson", "data", allow_duplicate=True),
        ],
        Input("map-state", "data"),
        State("app-state", "data"),
    )