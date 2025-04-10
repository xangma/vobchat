# Modified: app/callbacks/clientside_callbacks.py

import json
from dash import Dash, Input, Output, State, ALL, ClientsideFunction, dcc, html
from dash.dependencies import ALL
from utils.constants import UNIT_TYPES

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

            const triggered = dash_clientside.callback_context.triggered;
            if (!triggered || triggered.length === 0) {
                return window.dash_clientside.no_update; // No trigger
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
                 const fid = click_data.id;
                 const unit_type = click_data.properties.g_unit_type;
                 if (fid != null) { // Check for null or undefined
                    const selected_ids = new_state.selected_polygons || [];
                    const selected_units = new_state.selected_polygons_unit_types || [];
                    const index = selected_ids.indexOf(fid);

                    if (index > -1) { // Already selected, deselect
                       selected_ids.splice(index, 1);
                       selected_units.splice(index, 1);
                    } else { // Not selected, select
                       selected_ids.push(fid);
                       selected_units.push(unit_type);
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
                return [new_state, new_toggle_text];
            } else {
                return [window.dash_clientside.no_update, window.dash_clientside.no_update];
            }
        }
        """,
        Output("map-state", "data"),
        Output("toggle-unselected", "children"),
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
                const buttonContainer = document.getElementById('unit-filter-buttons-container');
                if (buttonContainer) {
                    buttonContainer.addEventListener("click", function(event) {
                        const button = event.target.closest('.unit-filter-button');
                        if (button) {
                            const isCtrl = event.ctrlKey || event.metaKey;
                            dash_clientside.set_props("ctrl-pressed-store", {data: isCtrl});
                            // console.log(`Client: Ctrl pressed: ${isCtrl} for button`); // Reduce noise
                        }
                    });
                     window.ctrlKeyListenerAttached = true;
                     console.log("Client: Ctrl key listener attached via delegation.");
                } else {
                     console.warn("Client: Could not find '#unit-filter-buttons-container' for ctrl key listener.");
                     // Fallback might be needed if container ID changes
                }
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('ctrl-listener-attached', 'data'),
        Input('document', 'id')
    )

    # 5. Map Resize Handling (NO CHANGE - Debounce is useful here)
    app.clientside_callback(
    """
    function(style) {
        if (window.resizeTimeout) {
            clearTimeout(window.resizeTimeout);
        }
        window.resizeTimeout = setTimeout(function() {
            const mapElement = document.getElementById('leaflet-map');
            if (mapElement && mapElement._leaflet_map) {
                console.log("Client: Invalidating map size due to resize.");
                mapElement._leaflet_map.invalidateSize();
            }
        }, 150);
        return window.dash_clientside.no_update;
    }
    """,
    Output('map-resize-debouncer', 'data'),
    Input('map-panel', 'style'),
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

    # REVISED Callback #8: Handles Zoom INITIATION ONLY
    app.clientside_callback(
        """
        function(mapState) {
            const context = dash_clientside.callback_context;
            // Only run if triggered and mapState exists
            if (!context.triggered || context.triggered.length === 0 || !mapState) {
                return window.dash_clientside.no_update;
            }

            // --- Condition: Act ONLY on zoom_to_selection flag ---
            if (!mapState.zoom_to_selection) {
                return window.dash_clientside.no_update; // Not a zoom request
            }

            // --- Prerequisites ---
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;
            const polygonManagement = window.polygon_management;

            if (!map || !polygonManagement || !polygonManagement.fetchPolygonsByIds || !polygonManagement.zoomTo || !window.geojsonLayerReady) {
                console.warn("Client (Cb8 - Fetch/Zoom): Prerequisites not met (map/pm/fetchByIds/zoomTo/layerReady).");
                // Reset state flags if cannot proceed
                try {
                    let newState = JSON.parse(JSON.stringify(mapState));
                    delete newState.zoom_to_selection;
                    delete newState.programmatic_unit_change_pending;
                    window.dash_clientside.set_props("map-state", {data: newState});
                    console.warn("Client (Cb8 - Fetch/Zoom): Resetting state flags as prerequisites failed.");
                } catch(e){ console.error("Client (Cb8 - Fetch/Zoom): Error resetting state flags on prerequisite failure:", e); }
                return window.dash_clientside.no_update;
            }

            // --- Fetch Data by ID First ---
            const idsToFetch = mapState.selected_polygons || [];
            const unitTypesForFetch = mapState.selected_polygons_unit_types || [];
            const unitType = unitTypesForFetch.length > 0 ? unitTypesForFetch[0] : null; // Assuming single type for fetch by ID call, adjust if needed

            if (idsToFetch.length === 0 || !unitType) {
                console.warn("Client (Cb8 - Fetch/Zoom): No IDs or unit type provided for fetch/zoom.");
                    // Reset state flags if cannot proceed
                try {
                    let newState = JSON.parse(JSON.stringify(mapState));
                    delete newState.zoom_to_selection;
                    delete newState.programmatic_unit_change_pending;
                    window.dash_clientside.set_props("map-state", {data: newState});
                    const layer = polygonManagement.findGeoJSONLayer(map);
                    polygonManagement.refreshLayerStyles(layer, mapState.selected_polygons);
                    console.warn("Client (Cb8 - Fetch/Zoom): Resetting state flags due to missing IDs/unit type.");
                } catch(e){ console.error("Client (Cb8 - Fetch/Zoom): Error resetting state flags:", e); }
                return window.dash_clientside.no_update;
            }

            console.log(`Client (Cb8 - Fetch/Zoom): Fetching ${idsToFetch.length} polygons by ID for unit type ${unitType}.`);
            const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;
            const unitChangePending = mapState.programmatic_unit_change_pending; // Store pending change

            // Set the flag *before* starting the async fetch/zoom process
            window.programmaticZoomInProgress = true;
            console.log("Client (Cb8 - Fetch/Zoom): Global programmaticZoomInProgress SET to true.");

            polygonManagement.fetchPolygonsByIds(map, mapState, unitType, idsToFetch, yearRange)
                .then(fetchedGeoJson => {
                    console.log("Client (Cb8 - Fetch/Zoom): Fetch by ID completed.");
                        // Optional: Add fetched data directly to layer here if needed,
                        // though updateMapWithBounds should handle cache update
                        // const layer = polygonManagement.findGeoJSONLayer(map);
                        // if (layer && fetchedGeoJson.features.length > 0) {
                        //    layer.addData(fetchedGeoJson);
                        //    console.log("Client (Cb8 - Fetch/Zoom): Added fetched features to layer.");
                        //    polygonManagement.refreshLayerStyles(layer, mapState.selected_polygons);
                        // }

                    // --- Initiate Zoom AFTER fetch completes ---
                    const geojsonLayer = polygonManagement.findGeoJSONLayer(map); // Get layer ref again
                    if (geojsonLayer) {
                        polygonManagement.zoomTo(map, idsToFetch, geojsonLayer); // Call fitBounds
                        console.log("Client (Cb8 - Fetch/Zoom): Zoom initiated.");

                        // Update state: remove trigger, keep pending change
                        let newState = JSON.parse(JSON.stringify(mapState));
                        delete newState.zoom_to_selection;
                        newState.programmatic_unit_change_pending = unitChangePending; // Ensure pending change is still there
                            try {
                            window.dash_clientside.set_props("map-state", {data: newState});
                            console.log("Client (Cb8 - Fetch/Zoom): Updated state (removed zoom trigger, kept pending change).");
                            } catch(e) {
                                console.error("Client (Cb8 - Fetch/Zoom): Error updating state after zoom initiation:", e);
                                // Reset flag on error
                                window.programmaticZoomInProgress = false;
                            }
                        // DO NOT reset programmaticZoomInProgress here. zoomend handler does it.
                    } else {
                        console.warn("Client (Cb8 - Fetch/Zoom): GeoJSON layer not found after fetch, cannot zoom.");
                        window.programmaticZoomInProgress = false; // Reset flag if zoom fails
                            // Also reset state flags fully if zoom fails
                        try {
                            let newState = JSON.parse(JSON.stringify(mapState));
                            delete newState.zoom_to_selection;
                            delete newState.programmatic_unit_change_pending;
                            window.dash_clientside.set_props("map-state", {data: newState});
                        } catch(e){}
                    }
                })
                .catch(error => {
                    console.error("Client (Cb8 - Fetch/Zoom): Error during fetchPolygonsByIds:", error);
                    window.programmaticZoomInProgress = false; // Reset flag on fetch error
                    // Also reset state flags fully on error
                    try {
                        let newState = JSON.parse(JSON.stringify(mapState));
                        delete newState.zoom_to_selection;
                        delete newState.programmatic_unit_change_pending;
                        window.dash_clientside.set_props("map-state", {data: newState});
                    } catch(e){}
                });

            // Callback finishes here, async operations continue
            return window.dash_clientside.no_update;
        }
        """,
        Output('zoom-handled', 'data'), # Dummy output
        Input("map-state", "data"),
        prevent_initial_call=True
    )



    # NEW Callback #9: Handles Refresh for Non-Zoom State Changes
    app.clientside_callback(
        """
        function(mapState) {
            const context = dash_clientside.callback_context;
            // Only run if triggered and mapState exists
            if (!context.triggered || context.triggered.length === 0 || !mapState) {
                return window.dash_clientside.no_update;
            }

            // --- Condition: Act ONLY if zoom flag is NOT set AND zoom is NOT in progress ---
            // This prevents Cb9 from acting when Cb8 is supposed to act (Run 1 of zoom)
            // but *allows* it to act on Run 2 (after Cb8 reset flags) and on filter changes.
            if (mapState.zoom_to_selection || window.programmaticZoomInProgress) {
                // console.log("Client (Cb9 - Refresh): Skipping update (zoom flag set or zoom in progress).");
                return window.dash_clientside.no_update;
            }

            // --- Prerequisites for Update ---
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;
            const polygonManagement = window.polygon_management;

            if (!map || !polygonManagement || !polygonManagement.updateMapWithBounds || !window.geojsonLayerReady) {
                console.warn("Client (Cb9 - Refresh): Prerequisites not met for update (map/pm/updateFunc/layerReady).");
                return window.dash_clientside.no_update;
            }

            // --- Perform Update ---
            // This runs for filter changes, toggle changes, resets, AND
            // the second time map-state updates after Cb8 resets the zoom flag.
            console.log("Client (Cb9 - Refresh): Non-zoom state change detected (or post-zoom flag reset). Calling updateMapWithBounds.");
            const bounds = map.getBounds(); // Uses bounds *at the time Cb9 runs*
            const unitTypes = mapState.unit_types || ['MOD_REG'];
            const yearRange = mapState.year_range ? { min: mapState.year_range[0], max: mapState.year_range[1] } : null;

            polygonManagement.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange)
                .then(result => {
                    // console.log("Client (Cb9 - Refresh): updateMapWithBounds completed."); // Reduce noise
                })
                .catch(error => {
                    console.error('Client (Cb9 - Refresh): Error in updateMapWithBounds:', error);
                });

            return window.dash_clientside.no_update;
        }
        """,
        Output('refresh-handled', 'data'), # Dummy output
        Input("map-state", "data"),
        prevent_initial_call=True # Only run on changes
    )