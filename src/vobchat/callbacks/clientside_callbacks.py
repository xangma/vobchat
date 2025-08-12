# Simple Clientside Callbacks - Clean rewrite
# Single responsibility: Handle basic UI interactions without state sync complexity

import json
from dash import Dash, Input, Output, State, ALL
from vobchat.utils.constants import UNIT_TYPES

def register_simple_clientside_callbacks(app: Dash):
    """Register simplified clientside callbacks - no more state sync loops!"""

    # Constants for JavaScript
    js_unit_types = json.dumps(UNIT_TYPES)
    js_default_min_year = 1800

    # 1. Basic user interactions -> map-state updates
    app.clientside_callback(
        """
        function(unit_clicks, reset_clicks, slider_value, geojson_click, toggle_clicks,
                 ctrl_pressed, map_state, button_ids, click_data, toggle_text) {

            let add_trigger = window.dash_clientside.no_update;
            let remove_trigger = window.dash_clientside.no_update;

            const triggered = dash_clientside.callback_context.triggered;
            if (!triggered || triggered.length === 0) {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update,
                        window.dash_clientside.no_update, window.dash_clientside.no_update];
            }

            // Helper functions to work with places array (single source of truth)
            const getSelectedPolygons = (state) => {
                const places = state.places || [];
                return places
                    .filter(place => place.g_unit !== null && place.g_unit !== undefined)
                    .map(place => String(place.g_unit));
            };

            const getSelectedUnitTypes = (state) => {
                const places = state.places || [];
                return places
                    .filter(place => place.g_unit !== null && place.g_unit !== undefined)
                    .map(place => place.g_unit_type || 'MOD_REG');
            };

            const triggered_id = triggered[0].prop_id;
            let new_state = map_state ? JSON.parse(JSON.stringify(map_state)) : {};

            // Initialize defaults
            if (!new_state.unit_types) new_state.unit_types = ['MOD_REG'];
            if (!new_state.places) new_state.places = [];
            if (new_state.show_unselected === undefined) new_state.show_unselected = true;
            if (!new_state.year_range) {
                const currentYear = new Date().getFullYear();
                new_state.year_range = [currentYear, currentYear];
            }

            let state_changed = false;
            let new_toggle_text = window.dash_clientside.no_update;

            // Handle Reset
            if (triggered_id.includes('reset-selections.n_clicks') && reset_clicks) {
                new_state.places = [];
                new_toggle_text = "Hide unselected polygons";
                state_changed = true;
            }
            // Handle Unit Filters
            else if (triggered_id.includes('unit-filter')) {
                const button_id_str = triggered_id.split('.')[0];
                const button_id = JSON.parse(button_id_str);
                const clicked_type = button_id.unit;
                let current_types = new Set(new_state.unit_types || ['MOD_REG']);

                if (ctrl_pressed) {
                    if (current_types.has(clicked_type)) {
                        current_types.delete(clicked_type);
                    } else {
                        current_types.add(clicked_type);
                    }
                    if (current_types.size === 0) {
                        current_types.add('MOD_REG');
                    }
                    window.dash_clientside.set_props("ctrl-pressed-store", {data: false});
                } else {
                    current_types = new Set([clicked_type]);
                }

                new_state.unit_types = Array.from(current_types);
                state_changed = true;

                // Immediately update polygons when unit types change
                setTimeout(() => {
                    const mapElement = document.getElementById('leaflet-map');
                    const map = mapElement?._leaflet_map;
                    
                    if (map && window.polygonManagement && window.polygonManagement.updateMapWithBounds) {
                        const bounds = map.getBounds();
                        const yearRange = new_state.year_range ? { min: new_state.year_range[0], max: new_state.year_range[1] } : null;
                        
                        console.log('Unit filter: Updating polygons for unit types:', new_state.unit_types);
                        window.polygonManagement.updateMapWithBounds(map, new_state.unit_types, bounds, new_state, yearRange)
                            .then(result => {
                                console.log('Unit filter: Polygon update completed');
                            })
                            .catch(error => {
                                console.error('Unit filter: Error updating polygons:', error);
                            });
                    }
                }, 0);
            }
            // Handle Year Slider
            else if (triggered_id.includes('year-range-slider.value') && slider_value) {
                const currentYear = new Date().getFullYear();
                const y0 = Math.min(slider_value[0], currentYear);
                const y1 = Math.min(slider_value[1], currentYear);

                if (!new_state.year_range || new_state.year_range[0] !== y0 || new_state.year_range[1] !== y1) {
                    new_state.year_range = [y0, y1];
                    state_changed = true;
                }
            }
            // Handle Map Clicks
            else if (triggered_id.includes('geojson-layer.n_clicks') && click_data) {
                const fid = String(click_data.id);
                const unit_type = click_data.properties.g_unit_type;
                const unit_name = click_data.properties.unit_name || fid;

                if (fid != null) {
                    const current_places = new_state.places || [];
                    const placeIndex = current_places.findIndex(p => String(p.g_unit) === fid);
                    const isSelected = placeIndex !== -1;

                    if (isSelected) {
                        // Remove polygon
                        remove_trigger = {
                            id: fid,
                            name: unit_name,
                            type: unit_type,
                            timestamp: Date.now()
                        };

                        // Update places array
                        new_state.places = current_places.filter((_, i) => i !== placeIndex);

                        // Immediate visual feedback via pureMapState
                        if (window.pureMapState) {
                            const result = window.pureMapState.userDeselectPolygon(fid);
                            console.log('Map click: pureMapState deselect result:', result);
                        }
                    } else {
                        // Add polygon
                        add_trigger = {
                            id: fid,
                            name: unit_name,
                            type: unit_type,
                            timestamp: Date.now()
                        };

                        // Add to places array
                        const newPlace = {
                            name: unit_name,
                            g_unit: parseInt(fid),
                            g_unit_type: unit_type,
                            g_place: null,
                            candidate_rows: [],
                            unit_rows: []
                        };
                        new_state.places = [...current_places, newPlace];

                        // Immediate visual feedback via pureMapState
                        if (window.pureMapState) {
                            const result = window.pureMapState.userSelectPolygon(fid, unit_type);
                            console.log('Map click: pureMapState select result:', result);
                        }
                    }
                    state_changed = true;
                }
            }
            // Handle Toggle Unselected
            else if (triggered_id.includes('toggle-unselected.n_clicks') && toggle_clicks) {
                new_state.show_unselected = !new_state.show_unselected;
                new_toggle_text = new_state.show_unselected ? "Hide unselected polygons" : "Show unselected polygons";
                state_changed = true;
            }

            if (state_changed) {
                return [new_state, new_toggle_text, add_trigger, remove_trigger];
            } else {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update,
                        window.dash_clientside.no_update, window.dash_clientside.no_update];
            }
        }
        """,
        Output("map-state", "data"),
        Output("toggle-unselected", "children"),
        Output("map-click-add-trigger", "data"),
        Output("map-click-remove-trigger", "data"),
        Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
        Input('reset-selections', 'n_clicks'),
        Input('year-range-slider', 'value'),
        Input("geojson-layer", "n_clicks"),
        Input("toggle-unselected", "n_clicks"),
        State('ctrl-pressed-store', 'data'),
        State("map-state", "data"),
        State({'type': 'unit-filter', 'unit': ALL}, 'id'),
        State("geojson-layer", "clickData"),
        State("toggle-unselected", "children"),
        prevent_initial_call=True,
    )

    # 2. Update UI elements based on map-state
    app.clientside_callback(
        f"""
        function(map_state) {{
            // Helper functions to work with places array (single source of truth)
            const getSelectedPolygons = (state) => {{
                const places = state?.places || [];
                return places
                    .filter(place => place.g_unit !== null && place.g_unit !== undefined)
                    .map(place => String(place.g_unit));
            }};

            const getSelectedUnitTypes = (state) => {{
                const places = state?.places || [];
                return places
                    .filter(place => place.g_unit !== null && place.g_unit !== undefined)
                    .map(place => place.g_unit_type || 'MOD_REG');
            }};

            if (!map_state) {{
                console.log("No map state - using defaults");
                const currentYear = new Date().getFullYear();
                const UNIT_TYPES = JSON.parse('{js_unit_types}');
                // Debug: log the callback context structure
                console.log("Callback context outputs_list:", dash_clientside.callback_context.outputs_list);

                const button_outputs = dash_clientside.callback_context.outputs_list[5];
                console.log("Button outputs at index 5:", button_outputs);

                let button_ids = [];
                if (button_outputs && Array.isArray(button_outputs)) {{
                    button_ids = button_outputs.map(o => o.id);
                }} else {{
                    console.warn("button_outputs is not an array, fallback to empty array");
                    // Fallback: create default button styles for known unit types
                    button_ids = Object.keys(UNIT_TYPES).map(unit => ({{unit: unit}}));
                }}

                // Create proper unit type colored buttons even for initial state
                const initialButtonStyles = button_ids.map(id => {{
                    const unit = id.unit;
                    const unit_info = UNIT_TYPES[unit] || {{}};
                    const unit_color = unit_info.color || '#333';
                    return {{
                        '--unit-color': unit_color,
                        'borderColor': unit_color,
                        'backgroundColor': 'white',
                        'color': unit_color,
                        'transition': 'background-color 0.3s, color 0.3s'
                    }};
                }});

                return [
                    {{'display': 'none'}},
                    {js_default_min_year},
                    currentYear,
                    {{}},
                    [currentYear, currentYear],
                    initialButtonStyles,
                    {{}},
                    {{ selected: [] }}
                ];
            }}

            const UNIT_TYPES = JSON.parse('{js_unit_types}');
            const currentYear = new Date().getFullYear();
            const unit_types = map_state.unit_types || ['MOD_REG'];
            const selected_polygons = getSelectedPolygons(map_state);
            const year_range = map_state.year_range || [currentYear, currentYear];

            // Year slider visibility
            const timeless_types = Object.keys(UNIT_TYPES).filter(k => UNIT_TYPES[k].timeless);
            const needsYearFilter = unit_types.some(ut => !timeless_types.includes(ut));
            const container_style = needsYearFilter ? {{'display': 'block'}} : {{'display': 'none'}};

            // Year slider config
            const min_year = {js_default_min_year};
            const max_year = currentYear;
            const step = Math.max(1, Math.floor((max_year - min_year) / 10));
            const slider_marks = {{}};
            for (let y = min_year; y <= max_year; y += step) {{
                slider_marks[y.toString()] = y.toString();
            }}
            if (!slider_marks[max_year.toString()]) {{
                slider_marks[max_year.toString()] = max_year.toString();
            }}
            const slider_value = [
                Math.max(min_year, Math.min(max_year, year_range[0])),
                Math.max(min_year, Math.min(max_year, year_range[1]))
            ];

            // Button styles
            const active_set = new Set(unit_types);
            const button_outputs = dash_clientside.callback_context.outputs_list[5];
            const button_ids = (button_outputs && Array.isArray(button_outputs)) ? button_outputs.map(o => o.id) : [];
            const button_styles = button_ids.map(id => {{
                const unit = id.unit;
                const unit_info = UNIT_TYPES[unit] || {{}};
                const unit_color = unit_info.color || 'blue';
                let style = {{
                    '--unit-color': unit_color,
                    'borderColor': unit_color,
                    'backgroundColor': 'white',
                    'color': '#333',
                    'transition': 'background-color 0.3s, color 0.3s'
                }};
                if (active_set.has(unit)) {{
                    style.backgroundColor = unit_color;
                    style.color = 'white';
                }}
                return style;
            }});

            // Counts
            const counts = {{}};
            Object.keys(UNIT_TYPES).forEach(ut => {{
                counts[ut] = 0;
                counts[ut + '_g_units'] = [];
            }});
            const selected_types = getSelectedUnitTypes(map_state);
            for (let i = 0; i < selected_polygons.length; i++) {{
                const unit_type = selected_types[i];
                if (counts.hasOwnProperty(unit_type)) {{
                    counts[unit_type]++;
                    counts[unit_type + '_g_units'].push(selected_polygons[i]);
                }}
            }}

            return [
                container_style, min_year, max_year, slider_marks, slider_value,
                button_styles, counts, {{ selected: selected_polygons }}
            ];
        }}
        """,
        Output('year-range-container', 'style'),
        Output('year-range-slider', 'min'),
        Output('year-range-slider', 'max'),
        Output('year-range-slider', 'marks'),
        Output('year-range-slider', 'value', allow_duplicate=True),
        Output({'type': 'unit-filter', 'unit': ALL}, 'style'),
        Output("counts-store", "data"),
        Output('geojson-layer', 'hideout'),
        Input("map-state", "data"),
        prevent_initial_call=False
    )

    # 3. Update button labels with counts
    app.clientside_callback(
        f"""
        function(counts) {{
            if (!counts) {{
                const button_outputs = dash_clientside.callback_context.outputs_list;
                const button_ids = button_outputs ? button_outputs.map(o => o.id) : [];
                return Array(button_ids.length).fill(window.dash_clientside.no_update);
            }}

            const UNIT_TYPES = JSON.parse('{js_unit_types}');
            const button_outputs = dash_clientside.callback_context.outputs_list;
            const button_ids = button_outputs ? button_outputs.map(o => o.id) : [];

            return button_ids.map(id => {{
                const unit = id.unit;
                const label = UNIT_TYPES[unit] ? UNIT_TYPES[unit].long_name : unit;
                const count = counts[unit] || 0;

                if (count > 0) {{
                    const badge = {{
                        props: {{
                            children: count.toString(),
                            color: 'light',
                            text_color: 'dark',
                            pill: true,
                            className: 'ms-1'
                        }},
                        type: 'Badge',
                        namespace: 'dash_bootstrap_components'
                    }};
                    return [label + ' ', badge];
                }} else {{
                    return label;
                }}
            }});
        }}
        """,
        Output({'type': 'unit-filter', 'unit': ALL}, 'children'),
        Input("counts-store", "data"),
        prevent_initial_call=True
    )

    # 4. Ctrl key detection
    app.clientside_callback(
        """
        function() {
            if (!window.ctrlKeyListenerAttached) {
                document.addEventListener("click", function(event) {
                    const button = event.target.closest('.unit-filter-button');
                    if (button) {
                        const isCtrl = event.ctrlKey || event.metaKey;
                        dash_clientside.set_props("ctrl-pressed-store", {data: isCtrl});
                    }
                });
                window.ctrlKeyListenerAttached = true;
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('ctrl-listener-attached', 'data'),
        Input('document', 'id')
    )

    # 5. Auto-load polygons when map bounds change (from complex version)
    app.clientside_callback(
        """
        function(moveend_trigger, map_state) {
            const context = dash_clientside.callback_context;

            //if (!context.triggered || context.triggered.length === 0 || !map_state || !moveend_trigger) {
            //    return window.dash_clientside.no_update;
            //}

            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;

            //if (!map) {
            //    console.warn('Auto-load: Map element not found');
            //    return window.dash_clientside.no_update;
            //}

            if (!window.polygonManagement || !window.polygonManagement.updateMapWithBounds) {
                console.warn('Auto-load: polygonManagement.updateMapWithBounds not available');
                return window.dash_clientside.no_update;
            }

            //if (!window.geojsonLayerReady) {
            //    console.warn('Auto-load: GeoJSON layer not ready, skipping update');
            //    return window.dash_clientside.no_update;
            //}

            // Skip if programmatic zoom is in progress to avoid conflicts
            if (window.programmaticZoomInProgress || window.programmaticZoomAnimating) {
                console.log('Auto-load: Skipping update (programmatic zoom in progress)');
                return window.dash_clientside.no_update;
            }

            // Skip if zoom_to_selection flag is set (zoom cycle in progress)
            if (map_state.zoom_to_selection) {
                console.log('Auto-load: Skipping update (zoom_to_selection flag set)');
                return window.dash_clientside.no_update;
            }

            // Skip if in disambiguation mode (showing place markers)
            if (window._disambiguationMode) {
                console.log('Auto-load: Skipping update (disambiguation mode active)');
                return window.dash_clientside.no_update;
            }

            const bounds = map.getBounds();
            const unitTypes = map_state.unit_types || ['MOD_REG'];
            const yearRange = map_state.year_range ? { min: map_state.year_range[0], max: map_state.year_range[1] } : null;

            console.log('Auto-load: Map bounds changed, loading polygons for unit types:', unitTypes);
            window.polygonManagement.updateMapWithBounds(map, unitTypes, bounds, map_state, yearRange)
                .then(result => {
                    console.log('Auto-load: Polygon update completed');
                })
                .catch(error => {
                    console.error('Auto-load: Error updating polygons:', error);
                });

            return window.dash_clientside.no_update;
        }
        """,
        Output('map-moveend-processed', 'data'),
        Input('map-moveend-trigger', 'data'),
        State('map-state', 'data'),
        prevent_initial_call=True
    )

    # 6. SSE connection management
    app.clientside_callback(
        """
        function(thread_id, sse_status) {
            console.log("SSE callback triggered with thread ID:", thread_id, "and status:", sse_status);

            // Handle SSE connection status that includes workflow input
            if (sse_status && sse_status.connect_sse && sse_status.thread_id) {
                console.log("SSE: Connecting with workflow input:", sse_status.workflow_input);
                
                // Clear disambiguation mode if requested
                if (sse_status.clear_disambiguation_mode && window.simpleSSE?.clearDisambiguationMode) {
                    console.log("SSE: Clearing disambiguation mode");
                    window.simpleSSE.clearDisambiguationMode();
                }

                // If this is a reset, also clear map state
                if (sse_status.reset) {
                    console.log("SSE: Reset detected, clearing all state");

                    // Clear map state via pureMapState if available
                    if (window.pureMapState) {
                        window.pureMapState.executeWorkflowCommand({
                            type: 'sync_state',
                            state: {
                                places: []
                            }
                        });

                        // Reset unit types to default and trigger map update
                        window.pureMapState.userSetUnitTypes(['MOD_REG'], false);

                        // Force map refresh to show MOD_REG polygons after reset
                        setTimeout(() => {
                            if (window.polygonManagement && window.polygonManagement.updateMapWithBounds) {
                                const mapElement = document.getElementById('leaflet-map');
                                const map = mapElement?._leaflet_map;
                                if (map) {
                                    const bounds = map.getBounds();
                                    const resetMapState = {
                                        places: [],
                                        unit_types: ['MOD_REG'],
                                        show_unselected: true
                                    };
                                    console.log("SSE: Forcing map update with MOD_REG polygons after reset");
                                    window.polygonManagement.updateMapWithBounds(map, ['MOD_REG'], bounds, resetMapState, null)
                                        .then(() => {
                                            console.log("SSE: Map refreshed with MOD_REG polygons");
                                        })
                                        .catch(error => {
                                            console.error("SSE: Error refreshing map after reset:", error);
                                        });
                                }
                            }
                        }, 100); // Small delay to ensure unit type change is processed
                    }

                    // Also update Dash map-state store to trigger UI updates
                    if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                        const currentYear = new Date().getFullYear();
                        dash_clientside.set_props('map-state', {
                            data: {
                                places: [],
                                unit_types: ['MOD_REG'],
                                show_unselected: true,
                                year_range: [currentYear, currentYear]
                            }
                        });
                    }
                }

                window.simpleSSE.connect(sse_status.thread_id, sse_status.workflow_input);
                return [true, sse_status.thread_id];
            }

            // If we have a thread ID and SSE client is available, connect
            if (thread_id && window.simpleSSE && !sse_status) {
                console.log("SSE: Connecting to thread ID:", thread_id);
                window.simpleSSE.connect(thread_id);
                return [true, thread_id];
            }

            // If no thread ID but SSE client is connected, disconnect
            if (!thread_id && window.simpleSSE && window.simpleSSE.isConnected) {
                console.log("SSE: Disconnecting from SSE stream");
                window.simpleSSE.disconnect();
                return [false, null];
            }

            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }
        """,
        Output('sse-connection-status', 'data', allow_duplicate=True),
        Output('thread-id', 'data', allow_duplicate=True),
        Input('thread-id', 'data'),
        Input('sse-connection-status', 'data'),
        prevent_initial_call=False
    )
