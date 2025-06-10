# Modified: app/callbacks/clientside_callbacks.py

import json
from dash import Dash, Input, Output, State, ALL, ClientsideFunction, dcc, html
from dash.dependencies import ALL
from vobchat.utils.constants import UNIT_TYPES

# Pass constants to JavaScript
js_unit_types = json.dumps(UNIT_TYPES)
# Default/static year bounds (replace with dynamic data transfer if needed)
js_default_min_year = 1800
# js_current_year is handled by the first callback

def register_clientside_callbacks(app: Dash):
    # Get current year dynamically in JS (NO CHANGE)
    app.clientside_callback(
        """
        function() {
            const currentYear = new Date().getFullYear();
            return JSON.stringify(currentYear);
        }
        """,
        Output('current-year-store', 'data'), # Requires a dcc.Store(id='current-year-store')
        Input('document', 'id') # Trigger once on load
    )

    # 1. Callback to handle user interactions -> Update map-state (NO CHANGE)
    app.clientside_callback(
        """
        function(unit_clicks, reset_clicks, slider_value, geojson_click, toggle_clicks, ctrl_pressed,
                 map_state, button_ids, click_data, toggle_text) {

            let add_trigger_data = window.dash_clientside.no_update;
            let remove_trigger_data = window.dash_clientside.no_update;

            const triggered = dash_clientside.callback_context.triggered;
            if (!triggered || triggered.length === 0) {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update, window.dash_clientside.no_update, window.dash_clientside.no_update]; // No trigger
            }

            const triggered_id = triggered[0].prop_id;
            let new_state = map_state ? JSON.parse(JSON.stringify(map_state)) : {}; // Deep copy or initialize

            // Initialize defaults if state is empty
            if (!new_state.unit_types) new_state.unit_types = ['MOD_REG'];
            if (!new_state.selected_polygons) new_state.selected_polygons = [];
            if (!new_state.selected_polygons_unit_types) new_state.selected_polygons_unit_types = [];
            if (new_state.show_unselected === undefined) new_state.show_unselected = true;
            if (!new_state.year_range) {
                 const currentYear = new Date().getFullYear();
                 new_state.year_range = [currentYear, currentYear];
            }


            let state_changed = false;
            let new_toggle_text = window.dash_clientside.no_update;

            // --- Handle Reset ---
            if (triggered_id.includes('reset-selections.n_clicks') && reset_clicks) {
                const currentYear = new Date().getFullYear();
                new_state.unit_types = ['MOD_REG'];
                new_state.selected_polygons = [];
                new_state.selected_polygons_unit_types = [];
                new_state.year_range = [currentYear, currentYear]; // Reset year range too
                new_state.show_unselected = true; // Reset toggle
                new_toggle_text = "Hide unselected polygons";
                state_changed = true;
                console.log("Client (Cb1): Reset map state");
            }
            // --- Handle Unit Filters ---
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
                    if (current_types.size === 0) { // Ensure at least one is selected
                        current_types.add('MOD_REG');
                    }
                     window.dash_clientside.set_props("ctrl-pressed-store", {data: false});
                } else {
                    current_types = new Set([clicked_type]);
                }
                new_state.unit_types = Array.from(current_types);
                state_changed = true;
                console.log("Client (Cb1): Updated unit types", new_state.unit_types);
            }
            // --- Handle Year Slider ---
            else if (triggered_id.includes('year-range-slider.value') && slider_value) {
                 const currentYear = new Date().getFullYear();
                 const y0 = Math.min(slider_value[0], currentYear);
                 const y1 = Math.min(slider_value[1], currentYear);
                 // Only update if the value actually changed to prevent loops
                 if (!new_state.year_range || new_state.year_range[0] !== y0 || new_state.year_range[1] !== y1) {
                    new_state.year_range = [y0, y1];
                    state_changed = true;
                    console.log("Client (Cb1): Updated year range", new_state.year_range);
                 }
            }
            // --- Handle Map Clicks (Selection) ---
            else if (triggered_id.includes('geojson-layer.n_clicks') && click_data) {
                 const fid = String(click_data.id); // Convert to string for consistent comparison
                 const unit_type = click_data.properties.g_unit_type;
                 if (fid != null) { // Check for null or undefined
                    const selected_ids = new_state.selected_polygons || [];
                    const selected_units = new_state.selected_polygons_unit_types || [];
                    const index = selected_ids.indexOf(fid);

                    if (index > -1) { // Already selected, deselect
                       selected_ids.splice(index, 1);
                       selected_units.splice(index, 1);
                        // Prepare payload for RemovePlace trigger
                        remove_trigger_data = {
                            id: fid,
                            name: click_data.properties.unit_name || fid, // Use name if available, else ID
                            type: unit_type,
                            timestamp: Date.now()
                        };
                        console.log("Client (Cb1): Firing Remove trigger for:", remove_trigger_data);
                    } else { // Not selected, select
                        selected_ids.push(fid);
                        selected_units.push(unit_type);
                        // Prepare payload for AddPlace trigger
                        add_trigger_data = {
                            id: fid,
                            name: click_data.properties.unit_name || fid, // Use name if available, else ID
                            type: unit_type,
                            timestamp: Date.now()
                        };
                        console.log("Client (Cb1): Firing Add trigger for:", add_trigger_data);
                    }
                    new_state.selected_polygons = selected_ids;
                    new_state.selected_polygons_unit_types = selected_units;
                    // *** Set zoom flag when selection changes ***
                    new_state.zoom_to_selection = true; // Flag to trigger zoom in Cb8
                    state_changed = true;
                    console.log("Client (Cb1): Updated selections, set zoom flag", new_state.selected_polygons);
                 }
            }
            // --- Handle Toggle Unselected ---
            else if (triggered_id.includes('toggle-unselected.n_clicks') && toggle_clicks) {
                new_state.show_unselected = !new_state.show_unselected;
                new_toggle_text = new_state.show_unselected ? "Hide unselected polygons" : "Show unselected polygons";
                state_changed = true;
                console.log("Client (Cb1): Toggled unselected visibility", new_state.show_unselected);
            }

            // Only update state if it changed
            if (state_changed) {
                return [new_state, new_toggle_text, add_trigger_data, remove_trigger_data];
            } else {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update, window.dash_clientside.no_update, window.dash_clientside.no_update];
            }
        }
        """,
        Output("map-state", "data", allow_duplicate=True),
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

    # 2. Callback to update UI elements based on map-state (NO CHANGE)
    app.clientside_callback(
        f"""
        function(map_state, current_year_str) {{
            // --- Start Standard UI Update Logic ---
            if (!map_state) {{
                 console.log("Client (Cb2): map_state is null, returning default UI config.");
                 const initialButtonStyle = {{ backgroundColor: 'white', color: '#333' }};
                 const button_outputs_ctx = dash_clientside.callback_context.outputs_list[5];
                 const button_ids = button_outputs_ctx ? button_outputs_ctx.map(o => o.id) : []; // Safely get button IDs
                 const buttonCount = button_ids.length;
                 const currentYear = current_year_str ? parseInt(JSON.parse(current_year_str)) : new Date().getFullYear();
                 const defaultYearRange = [currentYear, currentYear];

                 const default_min_year_js = {js_default_min_year};
                 const default_max_year_js = currentYear;
                 const default_step = Math.max(1, Math.floor((default_max_year_js - default_min_year_js) / 10));
                 const default_slider_marks = {{}};
                 for (let y = default_min_year_js; y <= default_max_year_js; y += default_step) {{
                     default_slider_marks[y.toString()] = y.toString();
                 }}
                 if (!default_slider_marks[default_max_year_js.toString()]) {{
                     default_slider_marks[default_max_year_js.toString()] = default_max_year_js.toString();
                 }}

                 return [
                     {{'display': 'none'}}, // year-range-container style
                     default_min_year_js, // slider min
                     default_max_year_js, // slider max
                     default_slider_marks, // slider marks
                     defaultYearRange, // slider value
                     Array(buttonCount).fill(initialButtonStyle), // button styles
                     {{}}, // counts-store data
                     {{ selected: [] }}, // geojson-layer hideout
                     window.dash_clientside.no_update // geojson-layer data
                 ];
            }}

            // console.log("Client (Cb2): Updating UI based on map_state:", map_state); // Reduce noise

            // --- Constants & State ---
            const UNIT_TYPES = JSON.parse('{js_unit_types}');
            const default_min_year = {js_default_min_year};
            const current_year = current_year_str ? parseInt(JSON.parse(current_year_str)) : new Date().getFullYear();
            const unit_types = map_state.unit_types || ['MOD_REG'];
            const selected_polygons = map_state.selected_polygons || [];
            const year_range = map_state.year_range || [current_year, current_year];

            // --- Year Slider Visibility & Config ---
            const timeless_unit_types = Object.keys(UNIT_TYPES).filter(k => UNIT_TYPES[k].timeless);
            const needsYearFilter = unit_types.some(ut => !timeless_unit_types.includes(ut));
            const container_style = needsYearFilter ? {{'display': 'block'}} : {{'display': 'none'}};
            const min_year = default_min_year;
            const max_year = current_year;
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


            // --- Button Styles ---
            const active_set = new Set(unit_types);
            const button_styles = [];
            const button_outputs_ctx = dash_clientside.callback_context.outputs_list[5];
            const button_ids = button_outputs_ctx ? button_outputs_ctx.map(o => o.id) : [];
            button_ids.forEach(id => {{
                 const unit = id.unit;
                 const unit_info = UNIT_TYPES[unit] || {{}};
                 const unit_color = unit_info.color || 'blue';
                 let style = {{
                    '--unit-color': unit_color, 'borderColor': unit_color,
                    'backgroundColor': 'white', 'color': '#333',
                    'transition': 'background-color 0.3s, color 0.3s'
                 }};
                 if (active_set.has(unit)) {{
                    style.backgroundColor = unit_color; style.color = 'white';
                 }}
                 button_styles.push(style);
            }});

            // --- Calculate Counts ---
            const counts = {{}};
            Object.keys(UNIT_TYPES).forEach(ut => {{
                counts[ut] = 0; counts[ut + '_g_units'] = [];
            }});
            const selected_types = map_state.selected_polygons_unit_types || [];
            for (let i = 0; i < selected_polygons.length; i++) {{
                const unit_type = selected_types[i];
                if (counts.hasOwnProperty(unit_type)) {{
                   counts[unit_type]++;
                   counts[unit_type + '_g_units'].push(selected_polygons[i]);
                }}
            }}

            // --- Update Hideout object ---
            const hideout = {{ selected: selected_polygons }};

            // Return results
            return [
                container_style, min_year, max_year, slider_marks, slider_value,
                button_styles, counts, hideout,
                window.dash_clientside.no_update
            ];
        }}
        """,
        Output('year-range-container', 'style', allow_duplicate=True),
        Output('year-range-slider', 'min', allow_duplicate=True),
        Output('year-range-slider', 'max', allow_duplicate=True),
        Output('year-range-slider', 'marks', allow_duplicate=True),
        Output('year-range-slider', 'value', allow_duplicate=True),
        Output({'type': 'unit-filter', 'unit': ALL}, 'style', allow_duplicate=True),
        Output("counts-store", "data", allow_duplicate=True),
        Output('geojson-layer', 'hideout', allow_duplicate=True),
        Output('geojson-layer', 'data', allow_duplicate=True),
        Input("map-state", "data"),
        Input('current-year-store', 'data'),
        prevent_initial_call=False
    )

    # 3. Update Unit Filter Button Labels with Counts (NO CHANGE)
    app.clientside_callback(
        f"""
        function(counts) {{
            if (!counts) {{
                 const button_outputs_ctx = dash_clientside.callback_context.outputs_list;
                 const button_ids = button_outputs_ctx ? button_outputs_ctx.map(o => o.id) : [];
                 return Array(button_ids.length).fill(window.dash_clientside.no_update);
            }}
            const UNIT_TYPES = JSON.parse('{js_unit_types}');
            const button_outputs_ctx = dash_clientside.callback_context.outputs_list;
            const button_ids = button_outputs_ctx ? button_outputs_ctx.map(o => o.id) : [];

            const results = [];
            button_ids.forEach(id => {{
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
                            className: 'ms-1',
                            style: {{'fontSize': '0.8em', 'verticalAlign': 'middle'}}
                        }},
                        type: 'Badge',
                        namespace: 'dash_bootstrap_components'
                     }};
                    results.push([label + ' ', badge]);
                }} else {{
                    results.push(label);
                }}
            }});
            return results;
        }}
        """,
        Output({'type': 'unit-filter', 'unit': ALL}, 'children', allow_duplicate=True),
        Input("counts-store", "data"),
        prevent_initial_call=True
    )

    # 4. Ctrl Key Detection (NO CHANGE)
    app.clientside_callback(
        """
        function() {
            if (!window.ctrlKeyListenerAttached) {
                // Delegate to document so dynamically rendered buttons are always handled
                document.addEventListener("click", function(event) {
                    const button = event.target.closest('.unit-filter-button');
                    if (button) {
                        const isCtrl = event.ctrlKey || event.metaKey;
                        dash_clientside.set_props("ctrl-pressed-store", {data: isCtrl});
                    }
                });
                window.ctrlKeyListenerAttached = true;
                console.log("Client: Ctrl key listener attached to document delegation.");
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('ctrl-listener-attached', 'data'),
        Input('document', 'id')
    )

    # 5. Map Resize Handling with Auto-Zoom to Selected Polygons
    # This triggers when the visualization panel changes and zooms to selected polygons
    app.clientside_callback(
    """
    function(vizContainerStyle, mapState) {
        console.log("Client: Visualization container style changed:", vizContainerStyle);
        console.log("Client: mapState:", mapState);
        console.log("Client: Selected polygons:", mapState?.selected_polygons);

        // Reset hidden-refresh guard when panel becomes visible again
        if (vizContainerStyle?.display !== 'none') {
            window._vizHiddenRefreshed = false;
        }

        // When hidden, invalidate size once and refresh layer styles (e.g. after deselect), skipping resize/zoom
        if (vizContainerStyle?.display === 'none') {
            console.log("Client: Visualization panel hidden - forcing layer styles refresh, skipping resize/zoom loops");
            if (!window._vizHiddenRefreshed) {
                window._vizHiddenRefreshed = true;
                try {
                    const mapEl = document.getElementById('leaflet-map');
                    const map = mapEl?._leaflet_map;
                    if (map) {
                        console.log("Client: Visualization hidden - invalidating map size before refresh");
                        map.invalidateSize();
                    }
                    if (map && window.polygon_management?.updateMapWithBounds && window.geojsonLayerReady && mapState) {
                        const bounds = map.getBounds();
                        const unitTypes = mapState.unit_types || ['MOD_REG'];
                        const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;
                        console.log("Client: Visualization hidden - refreshing layer styles");
                        window.polygon_management.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange);
                    }
                } catch (e) {
                    console.error("Client: Error forcing layer styles refresh on hidden visualization:", e);
                }
            }
            if (window.vizResizeTimeout) {
                clearTimeout(window.vizResizeTimeout);
            }
            return window.dash_clientside.no_update;
        }

        if (window.vizResizeTimeout) {
            clearTimeout(window.vizResizeTimeout);
        }
        window.vizResizeTimeout = setTimeout(function() {
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;

            if (map) {
                console.log("Client: Visualization panel size changed, invalidating map size and checking for selected polygons");
                map.invalidateSize();

                // Check if we have selected polygons to zoom to
                if (mapState && mapState.selected_polygons && mapState.selected_polygons.length > 0) {
                    console.log("Client: Found selected polygons for auto-zoom after visualization change:", mapState.selected_polygons);

                    // CRITICAL FIX: Avoid redundant zoom operations during programmatic zoom or recent zoom completion
                    if (window.programmaticZoomInProgress || window.programmaticZoomAnimating) {
                        console.log("Client: Skipping visualization auto-zoom (programmatic zoom in progress)");
                        return window.dash_clientside.no_update;
                    }

                    // Check if we just completed a zoom operation recently (debounce)
                    const now = Date.now();
                    if (window.lastZoomEndTime_MapEvents && (now - window.lastZoomEndTime_MapEvents < 2000)) {
                        console.log("Client: Skipping visualization auto-zoom (recent zoom completion)");
                        return window.dash_clientside.no_update;
                    }

                    // CRITICAL FIX: Only use direct zoom methods to prevent map-state modifications that cause loops
                    if (window.polygon_management && window.polygon_management.zoomTo) {
                        const geojsonLayer = window.polygon_management.findGeoJSONLayer(map);
                        if (geojsonLayer) {
                            console.log("Client: Using direct zoom to selected polygons after visualization change");
                            window.polygon_management.zoomTo(map, mapState.selected_polygons, geojsonLayer);
                        } else {
                            console.log("Client: GeoJSON layer not found, skipping zoom to prevent callback loops");
                        }
                    } else {
                        console.log("Client: polygon_management not available, skipping zoom to prevent callback loops");
                    }
                } else {
                    console.log("Client: No selected polygons to zoom to after visualization change");
                }
            } else {
                console.warn("Client: Map element not found during visualization change callback");
            }
        }, 500); // Longer delay to ensure visualization panel transition is complete
        return window.dash_clientside.no_update;
    }
    """,
    Output('visualization-resize-debouncer', 'data'),
    Input('visualization-panel-container', 'style'),
    State('map-state', 'data'),
    prevent_initial_call=True
    )


    # REMOVED Callback #6 - Event setup is now done within polygon_management.js:initializeMapLayers

    # Callback #7 (Handle Trigger from Map Events) - Simplified Logging
    app.clientside_callback(
        """
        function(moveendTrigger, mapState) {
            const context = dash_clientside.callback_context;
            // console.log("Client (Cb7): map-moveend-trigger callback ENTRY."); // Reduce noise

            if (!context.triggered || context.triggered.length === 0 || !mapState || !moveendTrigger) {
                // console.log("Client (Cb7): Skipping (no trigger data or mapState)."); // Reduce noise
                return window.dash_clientside.no_update;
            }


            const mapElement_Cb7 = document.getElementById('leaflet-map');
            const map = mapElement_Cb7?._leaflet_map;
            if (!map) {
                 console.warn("Client (Cb7): Map element/object not found for processing.");
                 return window.dash_clientside.no_update;
            }
            if (!window.polygon_management || !window.polygon_management.updateMapWithBounds) {
                 console.warn("Client (Cb7): polygon_management.updateMapWithBounds not available.");
                 return window.dash_clientside.no_update;
            }
             if (!window.geojsonLayerReady) {
                console.warn("Client (Cb7): GeoJSON layer not ready, skipping moveend update.");
                return window.dash_clientside.no_update;
            }

            // Skip update if this moveend was triggered by a programmatic zoom
            if (window.programmaticZoomInProgress || window.programmaticZoomAnimating) {
                console.log("Client (Cb7): Skipping moveend update (triggered by programmatic zoom).");
                return window.dash_clientside.no_update;
            }

            // Also skip if zoom_to_selection flag is set (indicating zoom cycle in progress)
            if (mapState.zoom_to_selection) {
                console.log("Client (Cb7): Skipping moveend update (zoom_to_selection flag set).");
                return window.dash_clientside.no_update;
            }

            // Skip update if a programmatic zoom just completed (debounce mechanism)
            const now = Date.now();
            if (window.lastZoomEndTime_MapEvents && (now - window.lastZoomEndTime_MapEvents < 1000)) {
                console.log("Client (Cb7): Skipping moveend update (recent programmatic zoom completion).");
                return window.dash_clientside.no_update;
            }

            const bounds = map.getBounds();
            const unitTypes = mapState.unit_types || ['MOD_REG'];
            const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;

            console.log("Client (Cb7): Map event triggered update. Calling updateMapWithBounds.");
            window.polygon_management.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange)
                .then(result => {
                    // console.log(`Client (Cb7): Map update triggered by event completed.`); // Reduce noise
                })
                .catch(error => {
                    console.error('Client (Cb7): Error updating map triggered by event:', error);
                });

            return window.dash_clientside.no_update;
        }
        """,
        Output('map-moveend-processed', 'data'),
        Input('map-moveend-trigger', 'data'), # Still triggered by this store from JS event handlers
        State('map-state', 'data'),
        prevent_initial_call=True
    )

    # REVISED Callback #8: Initiates Fetch/Zoom & Triggers Cleanup Store
    app.clientside_callback(
        """
        function(mapState, addTriggerData, removeTriggerData) {
            const context = dash_clientside.callback_context;
            if (!context.triggered || context.triggered.length === 0 || !mapState) {
                return window.dash_clientside.no_update; // No trigger
            }

            if (!mapState.zoom_to_selection) {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update]; // Not a zoom request, return no_update for both outputs
            }

            // CRITICAL FIX: Debounce rapid-fire callback triggers to prevent cascade
            const now = Date.now();
            if (!window.lastCb8Trigger) window.lastCb8Trigger = 0;
            if (now - window.lastCb8Trigger < 500) { // 500ms debounce
                console.log("Client (Cb8 - Fetch/Zoom): Debouncing rapid trigger, skipping.");
                return [window.dash_clientside.no_update, window.dash_clientside.no_update];
            }
            window.lastCb8Trigger = now;

            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;
            const polygonManagement = window.polygon_management;
            let cleanupData = window.dash_clientside.no_update; // Default cleanup output

            if (!map || !polygonManagement || !polygonManagement.fetchPolygonsByIds || !polygonManagement.zoomTo || !window.geojsonLayerReady) {
                console.warn("Client (Cb8 - Fetch/Zoom): Prerequisites not met.");
                // Reset flags directly in map-state if prereqs fail (this part is okay)
                 try {
                    let newState = JSON.parse(JSON.stringify(mapState));
                    delete newState.zoom_to_selection;
                    //delete newState.programmatic_unit_change_pending;
                    window.dash_clientside.set_props("map-state", {data: newState}); // Update map-state directly ONLY on error
                    console.warn("Client (Cb8 - Fetch/Zoom): Reset state flags in map-state as prerequisites failed.");
                } catch(e){ console.error("Client (Cb8 - Fetch/Zoom): Error resetting state flags on prerequisite failure:", e); }
                return [window.dash_clientside.no_update, window.dash_clientside.no_update]; // Return no_update for dummy output and cleanup trigger
            }

            const idsToFetch = mapState.selected_polygons || [];
            const unitTypesForFetch = mapState.selected_polygons_unit_types || [];
            const unitType = unitTypesForFetch.length > 0 ? unitTypesForFetch[0] : mapState.unit_types[0] || null;

            console.log(`Client (Cb8 - Fetch/Zoom): mapState.selected_polygons =`, mapState.selected_polygons);
            console.log(`Client (Cb8 - Fetch/Zoom): idsToFetch =`, idsToFetch);

            if (idsToFetch.length === 0 || !unitType) {
                console.warn("Client (Cb8 - Fetch/Zoom): No IDs or unit type provided.");
                 // Reset flags directly in map-state if nothing to fetch (this is also okay)
                try {
                    let newState = JSON.parse(JSON.stringify(mapState));
                    delete newState.zoom_to_selection;
                    //delete newState.programmatic_unit_change_pending;
                    window.dash_clientside.set_props("map-state", {data: newState}); // Update map-state directly ONLY on error
                    console.warn("Client (Cb8 - Fetch/Zoom): Resetting state flags due to missing IDs/unit type.");
                } catch(e){ console.error("Client (Cb8 - Fetch/Zoom): Error resetting state flags:", e); }
                return [window.dash_clientside.no_update, window.dash_clientside.no_update];
            }

            // CRITICAL FIX: Check if polygons are already selected and visible to avoid redundant operations
            const geojsonLayer = polygonManagement.findGeoJSONLayer(map);
            if (geojsonLayer) {
                // Check if all requested polygons are already on the layer with correct styling
                const currentLayerIds = [];
                geojsonLayer.eachLayer(layer => {
                    if (layer.feature && layer.feature.properties) {
                        currentLayerIds.push(String(layer.feature.properties.g_unit_id || layer.feature.id));
                    }
                });

                const allAlreadyPresent = idsToFetch.every(id => currentLayerIds.includes(String(id)));
                const selectionUnchanged = JSON.stringify(idsToFetch.sort()) === JSON.stringify((window.lastZoomSelection || []).sort());

                if (allAlreadyPresent && selectionUnchanged) {
                    console.log("Client (Cb8 - Fetch/Zoom): All polygons already present with same selection. Skipping redundant fetch/zoom.");
                    // Trigger cleanup via store so Cb10 can clear the zoom_to_selection flag
                    return [window.dash_clientside.no_update, {triggered_by_cb8: true}];
                }
            }

            // Store current selection for next comparison
            window.lastZoomSelection = idsToFetch.slice();

            console.log(`Client (Cb8 - Fetch/Zoom): Fetching ${idsToFetch.length} polygons by ID for unit type ${unitType}.`);
            const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;

            window.programmaticZoomInProgress = true; // Set progress flag
            console.log("Client (Cb8 - Fetch/Zoom): Global programmaticZoomInProgress SET to true.");

            polygonManagement.fetchPolygonsByIds(map, mapState, unitType, idsToFetch, yearRange, mapState.selected_polygons)
                .then(fetchedGeoJson => {
                    console.log("Client (Cb8 - Fetch/Zoom): Fetch by ID completed.");
                    const geojsonLayer = polygonManagement.findGeoJSONLayer(map);
                    if (geojsonLayer) {
                        polygonManagement.zoomTo(map, idsToFetch, geojsonLayer); // This sets programmaticZoomAnimating = true
                        console.log("Client (Cb8 - Fetch/Zoom): Zoom initiated.");

                        // Cleanup is now handled by the zoomend event in polygon_management.js
                        // No need to trigger cleanup immediately - wait for zoom to complete

                    } else {
                        console.warn("Client (Cb8 - Fetch/Zoom): GeoJSON layer not found after fetch, cannot zoom.");
                        window.programmaticZoomInProgress = false;
                        window.programmaticZoomAnimating = false;
                        // Reset flags directly in map-state ONLY on error/failure
                         try {
                            let newState = JSON.parse(JSON.stringify(mapState));
                            delete newState.zoom_to_selection;
                            //delete newState.programmatic_unit_change_pending;
                            window.dash_clientside.set_props("map-state", {data: newState});
                        } catch(e){}
                    }
                })
                .catch(error => {
                    console.error("Client (Cb8 - Fetch/Zoom): Error during fetchPolygonsByIds:", error);
                    window.programmaticZoomInProgress = false;
                    window.programmaticZoomAnimating = false;
                     // Reset flags directly in map-state ONLY on error/failure
                     try {
                        let newState = JSON.parse(JSON.stringify(mapState));
                        delete newState.zoom_to_selection;
                        //delete newState.programmatic_unit_change_pending;
                        window.dash_clientside.set_props("map-state", {data: newState});
                    } catch(e){}
                });

            // This callback now outputs to the dummy 'zoom-handled' and the new trigger store
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }
        """,
        Output('zoom-handled', 'data'), # Keep dummy output
        # *** NEW: Output to the trigger store ***
        Output("zoom-cleanup-trigger-store", "data"),
        Input("map-state", "data"),
        prevent_initial_call=True
    )

    # REVISED Callback #9: Handles Refresh on user clicks and map events
    app.clientside_callback(
        """
        function(addTriggerData, removeTriggerData, moveendTriggerData, cleanupTriggerData, unitFilterClicks, mapState) {
            const context = dash_clientside.callback_context;
            if (!context.triggered || context.triggered.length === 0 || !mapState) {
                return window.dash_clientside.no_update;
            }
            console.log("Client (Cb9 - Refresh) invoked. Context triggers:", context.triggered);

            const triggeredProps = context.triggered.map(t => t.prop_id);
            const isSelection = triggeredProps.some(pid =>
                pid.startsWith('map-click-add-trigger') || pid.startsWith('map-click-remove-trigger')
            );
            const isUnitFilterChange = triggeredProps.some(pid =>
                pid.includes('unit-filter')
            );
            console.log(
                "Client (Cb9 - Refresh) debug: triggeredProps=", triggeredProps,
                ", isSelection=", isSelection,
                ", isUnitFilterChange=", isUnitFilterChange,
                ", zoom_to_selection=", mapState.zoom_to_selection,
                ", programmaticZoomAnimating=", window.programmaticZoomAnimating,
                ", programmaticZoomInProgress=", window.programmaticZoomInProgress
            );

            // --- Condition: Skip until any non-selection-driven zoom cycle completes ---
            // (Allow immediate style-refresh for a click selection or unit filter change even if zoom_to_selection is pending.)
            if (mapState.zoom_to_selection && !isSelection && !isUnitFilterChange) {
                console.log("Client (Cb9 - Refresh): Skipping update (zoom_to_selection flag still set).");
                return window.dash_clientside.no_update;
            }

            if ((window.programmaticZoomAnimating || window.programmaticZoomInProgress) && !isSelection && !isUnitFilterChange) {
                console.log(`Client (Cb9 - Refresh): Skipping update (JS zoom flags still active - animating: ${window.programmaticZoomAnimating}, inProgress: ${window.programmaticZoomInProgress}).`);
                // Add a fallback: if flags have been stuck for too long, reset them
                const now = Date.now();
                if (!window.lastZoomFlagCheck) window.lastZoomFlagCheck = now;
                if (now - window.lastZoomFlagCheck > 3000) { // 3 second timeout
                    console.warn("Client (Cb9 - Refresh): Zoom flags stuck for >3s, force clearing them.");
                    window.programmaticZoomAnimating = false;
                    window.programmaticZoomInProgress = false;
                    window.lastZoomFlagCheck = now;
                    // Don't return no_update, let it continue to refresh
                } else {
                    window.lastZoomFlagCheck = now;
                    return window.dash_clientside.no_update;
                }
            } else {
                // Reset timeout tracker when flags are clear
                window.lastZoomFlagCheck = null;
            }

            // --- Prerequisites --- (No change)
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;
            const polygonManagement = window.polygon_management;
            if (!map || !polygonManagement || !polygonManagement.updateMapWithBounds || !window.geojsonLayerReady) {
                console.warn("Client (Cb9 - Refresh): Prerequisites not met.");
                return window.dash_clientside.no_update;
            }

            // --- Perform Update --- (No change)
            console.log("Client (Cb9 - Refresh): Conditions met. Calling updateMapWithBounds.");
            const bounds = map.getBounds();
            const unitTypes = mapState.unit_types || ['MOD_REG'];
            const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;

            polygonManagement.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange)
                .then(result => {
                    console.log("Client (Cb9 - Refresh): updateMapWithBounds completed.");
                })
                .catch(error => {
                    console.error('Client (Cb9 - Refresh): Error in updateMapWithBounds:', error);
                });

            return window.dash_clientside.no_update;
        }
        """,
        Output('refresh-handled', 'data'),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),
        Input("map-moveend-trigger", "data"),
        Input("zoom-cleanup-trigger-store", "data"),
        Input({'type': 'unit-filter', 'unit': ALL}, 'n_clicks'),
        State("map-state", "data"),
        prevent_initial_call=True
    )


    # *** NEW Callback #10: Performs State Cleanup Triggered by zoom completion ***
    app.clientside_callback(
        """
        function(triggerData, currentMapState) {
            const context = dash_clientside.callback_context;
             // Only run if triggered by the store update AND state exists
            if (!context.triggered || !triggerData || !currentMapState || !triggerData.triggered_by_cb8) {
                // console.log("Client (Cb10 - Cleanup): Skipping (not triggered by Cb8 or no state).");
                return window.dash_clientside.no_update;
            }

            console.log("Client (Cb10 - Cleanup): Triggered by zoom-cleanup-trigger-store.");

            // Check if cleanup is actually needed
            if (!currentMapState.zoom_to_selection) {
                console.log("Client (Cb10 - Cleanup): Flags already cleared in map-state. No update needed.");
                return window.dash_clientside.no_update;
            }

            // Perform the state update: clear the flags
            let newState = JSON.parse(JSON.stringify(currentMapState));
            let updated = false;
            if (newState.zoom_to_selection) {
                delete newState.zoom_to_selection;
                console.log("Client (Cb10 - Cleanup): Cleared zoom_to_selection flag after zoom completion.");
                updated = true;
            }

            if (updated) {
                 console.log("Client (Cb10 - Cleanup): Returning updated map-state.");
                 // This state update will trigger Cb9 for the actual map refresh based on the cleaned state
                 return newState;
            } else {
                 console.log("Client (Cb10 - Cleanup): No flags needed clearing.");
                 return window.dash_clientside.no_update;
            }
        }
        """,
        Output("map-state", "data", allow_duplicate=True), # Output updates the main map-state
        Input("zoom-cleanup-trigger-store", "data"), # Triggered by zoomend event
        State("map-state", "data"), # Get the current map-state to modify
        prevent_initial_call=True
    )

    # Callback #11 - Place Disambiguation Auto-Zoom (clientside for performance)
    app.clientside_callback(
        """
        function(mapState) {
            console.log("Client (Cb11): Place disambiguation auto-zoom callback triggered");

            if (!mapState || !mapState.place_disambiguation || !mapState.show_place_disambiguation) {
                console.log("Client (Cb11): No place disambiguation data, no auto-zoom needed");
                return window.dash_clientside.no_update;
            }

            const places = mapState.place_disambiguation || [];
            console.log("Client (Cb11): Processing auto-zoom for", places.length, "disambiguation places");

            // Auto-zoom to show all markers
            if (places.length > 0) {
                setTimeout(() => {
                    const mapElement = document.getElementById('leaflet-map');
                    const map = mapElement?._leaflet_map;

                    if (map) {
                        console.log("Client (Cb11): Auto-zooming to show disambiguation markers");

                        // Calculate bounds for all markers
                        const lats = places.filter(p => p.lat && p.lon && !isNaN(p.lat) && !isNaN(p.lon)).map(p => p.lat);
                        const lons = places.filter(p => p.lat && p.lon && !isNaN(p.lat) && !isNaN(p.lon)).map(p => p.lon);

                        if (lats.length > 0 && lons.length > 0) {
                            const minLat = Math.min(...lats);
                            const maxLat = Math.max(...lats);
                            const minLon = Math.min(...lons);
                            const maxLon = Math.max(...lons);

                            // Add some padding
                            const latPadding = (maxLat - minLat) * 0.1 || 0.01;
                            const lonPadding = (maxLon - minLon) * 0.1 || 0.01;

                            const bounds = [
                                [minLat - latPadding, minLon - lonPadding],
                                [maxLat + latPadding, maxLon + lonPadding]
                            ];

                            console.log("Client (Cb11): Fitting map to bounds:", bounds);
                            map.fitBounds(bounds, {padding: [20, 20]});
                        }
                    }
                }, 500); // Small delay to ensure markers are rendered
            }

            return window.dash_clientside.no_update;
        }
        """,
        Output('place-disambiguation-processed', 'data'),  # Dummy output instead of map-state
        Input("map-state", "data"),
        prevent_initial_call=True
    )

    # # Add Clientside Callback for scrolling (optional but good UX)
    # app.clientside_callback(
    #     """
    #     scrollToBottom: function(children) {
    #     // Debounce mechanism
    #     if (window.scrollTimeout) {
    #         clearTimeout(window.scrollTimeout);
    #     }
    #     window.scrollTimeout = setTimeout(() => {
    #         try {
    #             const chatContainer = document.getElementById('chat-display'); // Or the ID of your scrollable container
    #             if (chatContainer) {
    #                  // Check if user is scrolled up significantly
    #                  const isScrolledUp = chatContainer.scrollHeight - chatContainer.scrollTop > chatContainer.clientHeight + 150; // 150px buffer

    #                  if (!isScrolledUp) { // Only scroll if user is near the bottom
    #                     // Using smooth scroll
    #                     // chatContainer.scrollTo({ top: chatContainer.scrollHeight, behavior: 'smooth' });
    #                     // Or instant scroll:
    #                     chatContainer.scrollTop = chatContainer.scrollHeight;
    #                  }
    #             }
    #         } catch (e) {
    #             console.error("Scroll error:", e);
    #         }
    #     }, 100); // Adjust debounce delay (ms) as needed
    #     return null; // No Dash output needed
    # }
    #     """,
    #     Output('scroll-dummy-output', 'children'),
    #     Input('chat-display', 'children'), # Triggered by chat display updates
    #     prevent_initial_call=True

    # )
