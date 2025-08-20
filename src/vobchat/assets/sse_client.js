// Simple SSE Client - Clean rewrite
// Purpose: Connect to the SSE stream and update the UI directly without
// Dash callbacks for the high-frequency chat + map flows. This file is kept
// intentionally self-contained so it can operate with minimal coupling.
//
// Lifecycle overview:
// - connect(threadId, workflowInput?): Opens an EventSource to the /sse endpoint
//   and (optionally) POSTs an initial workflow input to kick off the turn.
// - handleStateUpdate(state): Receives normalized state deltas and updates
//   the visible chat, map, and visualization panels. Chat messages are always
//   ordered and deduped server-side to avoid flicker.
// - Streaming: During token streaming, the server emits frequent state_update
//   events with a full messages array including a growing AI bubble. The
//   llm_busy flag is cleared after the first visible token renders.
// - Interrupts: When a node requests user action (e.g., place disambiguation),
//   an 'interrupt' event arrives with options and map hints.

class SimpleSSEClient {
    constructor() {
        this.eventSource = null;
        this.threadId = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 3;
        this.lastPlacesSig = null;
        this.llmBusy = false; // server-driven busy flag for thinking indicator
        this.currentOptions = null; // cache of currently visible UI options
        this.placeStateCache = {}; // local cache to merge partial updates safely
        // Use Dash's official url base path env exposure
        this.basePath = (typeof window !== 'undefined' && window.DASH_URL_BASE_PATHNAME) ? window.DASH_URL_BASE_PATHNAME : '/';

        this.joinPath = (base, path) => {
            base = base || "";
            if (base.endsWith('/')) base = base.slice(0, -1);
            return `${base}${path}`;
        };

        console.log('Simple SSE Client initialized');

        // Robustly attach theme UI handlers; Dash may mount after DOMContentLoaded
        const attachThemeHandlers = () => {
            const panelEl = document.getElementById('theme-selection-panel');
            const statusEl = document.getElementById('theme-status');
            const closeEl = document.getElementById('theme-panel-close');

            if (statusEl && !statusEl._themeHandlersAttached) {
                statusEl._themeHandlersAttached = true;
                statusEl.addEventListener('click', () => {
                    // Toggle panel if we already have theme options
                    if (panelEl && this.currentOptions && Array.isArray(this.currentOptions) && this.currentOptions.some(o => o.option_type === 'theme_query')) {
                        panelEl.style.display = panelEl.style.display === 'none' ? 'block' : 'none';
                        return;
                    }
                    // Otherwise, ask backend to present theme options via interrupt
                    if (this.threadId) {
                        this.postWorkflowInput({
                            last_intent_payload: { intent: 'AddTheme', arguments: { source: 'theme_panel', force: true } }
                        }).catch(err => console.error('SSE: Theme status click post failed', err));
                    } else {
                        console.warn('SSE: No active thread for theme panel request');
                    }
                });
            }
            if (closeEl && !closeEl._themeHandlersAttached) {
                closeEl._themeHandlersAttached = true;
                closeEl.addEventListener('click', (e) => {
                    try { e.preventDefault(); e.stopPropagation(); } catch (_) {}
                    if (panelEl) panelEl.style.display = 'none';
                });
            }

            return Boolean(statusEl && statusEl._themeHandlersAttached && closeEl && closeEl._themeHandlersAttached);
        };

        const initThemeUI = () => {
            if (attachThemeHandlers()) return;
            if (this._themeObserver) return;
            // Observe DOM insertions and attach when elements appear
            this._themeObserver = new MutationObserver(() => {
                if (attachThemeHandlers()) {
                    try { this._themeObserver.disconnect(); } catch (_) {}
                    this._themeObserver = null;
                }
            });
            try {
                this._themeObserver.observe(document.body, { childList: true, subtree: true });
            } catch (_) {}
        };

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initThemeUI);
        } else {
            initThemeUI();
        }
    }

    connect(threadId, workflowInput = null) {
        if (this.eventSource) {
            this.disconnect();
        }

        this.threadId = threadId;
        console.log('SSE: Connecting to thread:', threadId);

        // If this is a reset, clear the chat display and hide visualization
        if (workflowInput && workflowInput.last_intent_payload && workflowInput.last_intent_payload.intent === 'Reset') {
            this.clearChatDisplay();
            this.clearButtons();
            this.hideVisualization();
            this.hideThinkingIndicator?.();
            // Also reset theme status label immediately
            try {
                const labelEl = document.getElementById('theme-status-label');
                if (labelEl) labelEl.textContent = 'Theme: (none)';
                const themePanel = document.getElementById('theme-selection-panel');
                const themeButtons = document.getElementById('theme-selection-buttons');
                const optionsContainer = document.getElementById('options-container');
                if (themePanel) themePanel.style.display = 'none';
                if (themeButtons) themeButtons.innerHTML = '';
                if (optionsContainer) optionsContainer.innerHTML = '';
                this.currentOptions = null;
                const chatInput = document.getElementById('chat-input');
                if (chatInput) chatInput.value = '';
            } catch (_) {}
        }

        let url = this.joinPath(this.basePath, `/sse/${threadId}`);

        this.eventSource = new EventSource(url);

        this.postWorkflowInput = (input) => {
          // Attach UI options (if any) so backend state is aware of visible buttons
          try {
            if (this.currentOptions && Array.isArray(this.currentOptions) && this.currentOptions.length > 0) {
                input = Object.assign({}, input, { options: this.currentOptions });
            }
          } catch (e) {
            console.warn('SSE: Failed to attach current options to workflow input', e);
          }
          if (!this.threadId) return;

          return fetch(this.joinPath(this.basePath, `/workflow/${this.threadId}`), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ workflow_input: input })
          });
        };

        this.eventSource.onopen = () => {
            console.log('SSE: Connected successfully');
            this.isConnected = true;
            this.reconnectAttempts = 0;
            // kick off the workflow if we were given a first input
            if (workflowInput) {
                this.postWorkflowInput(workflowInput)
                    .catch(err => console.error('SSE: initial POST failed', err));
            }
        };

        this.eventSource.addEventListener('message', (event) => {
            console.log('SSE: Raw message event received:', event.data);
            try {
                // Try to parse as JSON first
                const data = JSON.parse(event.data);
                console.log('SSE: Parsed JSON data:', data);
                this.handleMessage(data.content);
            } catch (e) {
                // If not JSON, treat as plain text (for "Connected", "heartbeat", etc.)
                if (event.data !== "Connected" && event.data !== "heartbeat") {
                    console.log('SSE: Received plain text message:', event.data);
                }
            }
        });

        // Debug: Log all SSE events received
        this.eventSource.addEventListener('*', (event) => {
            console.log('SSE: Any event received:', event.type, event.data);
        });

        this.eventSource.addEventListener('state_update', (event) => {
            // console.log('SSE: Raw state_update event received:', event.data);
            try {
                const data = JSON.parse(event.data);
                console.log('SSE: Parsed state_update data:', data);
                this.handleStateUpdate(data.state);
            } catch (e) {
                console.error('SSE: Error parsing state update:', e);
            }
        });

        this.eventSource.addEventListener('interrupt', (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleInterrupt(data);
            } catch (e) {
                console.error('SSE: Error parsing interrupt:', e);
            }
        });

        this.eventSource.addEventListener('error', (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleError(data.error);
            } catch (e) {
                console.error('SSE: Error parsing error event:', e);
            }
        });

        this.eventSource.onerror = () => {
            console.error('SSE: Connection error');
            this.isConnected = false;
            this.hideThinkingIndicator();
            this.attemptReconnect();
        };
    }

    disconnect() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
        this.isConnected = false;
        this.threadId = null;
        this.hideThinkingIndicator();
    }

    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('SSE: Max reconnect attempts reached');
            return;
        }

        this.reconnectAttempts++;
        setTimeout(() => {
            if (this.threadId) {
                console.log(`SSE: Reconnecting (attempt ${this.reconnectAttempts})`);
                this.connect(this.threadId);
            }
        }, 2000);
    }

    // Simple message handling - now handled through state updates
    handleMessage(content) {
        console.log('SSE: Received individual message (handled via state updates):', content);
        // Individual messages are now handled through the messages array in state updates
        // This prevents duplicates and ensures proper ordering
    }

    // Simple state update - update stores directly
    handleStateUpdate(state) {
        console.log('SSE: Received state update:', state);
        if (state.map_update_request) {
            console.log('SSE: Found map_update_request in state:', state.map_update_request);
        } else {
            console.log('SSE: No map_update_request found in state');
        }

        // Check if state is valid
        if (!state || typeof state !== 'object') {
            console.log('SSE: Invalid state update received, skipping');
            return;
        }

        // Handle explicit busy flag from server (drives thinking indicator)
        if (Object.prototype.hasOwnProperty.call(state, 'llm_busy')) {
            this.llmBusy = !!state.llm_busy;
            if (this.llmBusy) {
                this.showThinkingIndicator();
            } else {
                this.hideThinkingIndicator();
            }
        }

        // Handle messages array to update chat display (deduped)
        if (state.messages && Array.isArray(state.messages)) {
            this.updateChatDisplay(state.messages);
        }

        // Update theme status label if selected_theme provided
        if (Object.prototype.hasOwnProperty.call(state, 'selected_theme')) {
            try {
                const labelEl = document.getElementById('theme-status-label');
                let labelText = 'Theme: (none)';
                if (state.selected_theme) {
                    const df = JSON.parse(state.selected_theme);
                    if (Array.isArray(df) && df.length > 0) {
                        const lab = df[0].labl || df[0].label || null;
                        const code = df[0].ent_id || df[0].id || null;
                        if (lab && code) labelText = `Theme: ${lab}`;
                        else if (lab) labelText = `Theme: ${lab}`;
                        else if (code) labelText = `Theme: ${code}`;
                    } else if (df && typeof df === 'object') {
                        const lab = df.labl || df.label || null;
                        const code = df.ent_id || df.id || null;
                        if (lab && code) labelText = `Theme: ${lab}`;
                        else if (lab) labelText = `Theme: ${lab}`;
                        else if (code) labelText = `Theme: ${code}`;
                    }
                }
                if (labelEl) labelEl.textContent = labelText;
            } catch (e) {
                // no-op if parsing fails
            }
        }

        // Update visualization if we have cube data
        if (state.cubes || state.places) {
            this.updateVisualization(state);
        }

        // Handle map update requests
        if (state.map_update_request && state.map_update_request.action === 'update_map_selection') {
            console.log('SSE: Processing map update request:', state.map_update_request);
            this.handleMapUpdateRequest(state.map_update_request);
        } else if (state.map_update_request && state.map_update_request.action === 'show_info_marker') {
            console.log('SSE: Processing info marker request:', state.map_update_request);
            this.handleInfoMarkerRequest(state.map_update_request);
        } else if (state.places && Array.isArray(state.places)) {
            // Use places as single source of truth, but avoid redundant updates
            this.updateMapSelection(state.places);
        }

        // Handle units needing map selection
        if (state.units_needing_map_selection && Array.isArray(state.units_needing_map_selection) && state.units_needing_map_selection.length > 0) {
            console.log('SSE: Units needing map selection:', state.units_needing_map_selection);
            this.handleUnitsNeedingSelection(state.units_needing_map_selection, state.places);
            // We're now waiting for user input, not the LLM
            this.hideThinkingIndicator();
        }

        // Update stores via Dash (merge targeted updates with cached state)
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            // Only update place-state when values actually changed to avoid loops
            const updates = {};
            let hasUpdates = false;

            const deepEqual = (a, b) => {
                try {
                    if (a === b) return true;
                    if (Array.isArray(a) && Array.isArray(b) && a.length !== b.length) return false;
                    return JSON.stringify(a) === JSON.stringify(b);
                } catch (e) {
                    return false;
                }
            };

            if (state.places !== undefined && !deepEqual(this.placeStateCache.places, state.places)) {
                updates.places = state.places;
                hasUpdates = true;
            }
            if (state.cubes !== undefined && !deepEqual(this.placeStateCache.cubes, state.cubes)) {
                updates.cubes = state.cubes;
                hasUpdates = true;
            }
            if (state.selected_theme !== undefined && !deepEqual(this.placeStateCache.selected_theme, state.selected_theme)) {
                updates.selected_theme = state.selected_theme;
                hasUpdates = true;
            }

            if (hasUpdates) {
                this.placeStateCache = Object.assign({}, this.placeStateCache, updates);
                dash_clientside.set_props('place-state', { data: this.placeStateCache });
            }

            // Only update app-state if visualization needs to show/hide
            // if (state.show_visualization !== undefined) {
            //     dash_clientside.set_props('app-state', {
            //         data: { show_visualization: state.show_visualization }
            //     });
            // }
        }
    }

    // Simple interrupt handling - show buttons and handle state updates
    handleInterrupt(interruptData) {
        console.log('SSE: Received interrupt:', interruptData);
        // Interrupt means waiting for user input; hide thinking indicator
        this.hideThinkingIndicator();

        // Store current_node and full interrupt data for when buttons are clicked
        this.currentNode = interruptData.current_node || null;
        this.currentInterruptData = interruptData;

        // Store interrupt data in the sse-interrupt-store for callbacks to access
        // If this interrupt provides place_coordinates, clear any existing markers first
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            if (interruptData.place_coordinates && interruptData.place_coordinates.length > 0) {
                // Clear store to remove existing markers before setting new ones
                dash_clientside.set_props('sse-interrupt-store', { data: {} });
                // Set new data on next tick so Dash processes the clear first
                setTimeout(() => {
                    dash_clientside.set_props('sse-interrupt-store', { data: interruptData });
                }, 0);
            } else {
                dash_clientside.set_props('sse-interrupt-store', { data: interruptData });
            }
        }

        // Handle place disambiguation markers zoom
        if (interruptData.place_coordinates && interruptData.place_coordinates.length > 0) {
            // Set disambiguation mode immediately
            window._disambiguationMode = true;
            console.log('SSE: Setting disambiguation mode for place markers');
            
            // Wait a bit for markers to be created then zoom to them
            setTimeout(() => {
                this.zoomToPlaceMarkers(interruptData.place_coordinates, interruptData);
            }, 100);
        }

        // Handle cube data if provided
        if (interruptData.cube_data_ready && interruptData.cubes) {
            console.log('SSE: Received cube data, updating state');

            // Update place-state with complete state data from interrupt
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                const newData = {
                    cubes: JSON.parse(interruptData.cubes),
                    selected_cubes: JSON.parse(interruptData.selected_cubes || interruptData.cubes),
                    show_visualization: interruptData.show_visualization || true,
                    places: interruptData.places || [],
                    selected_theme: interruptData.selected_theme
                };
                this.placeStateCache = Object.assign({}, this.placeStateCache, newData);
                dash_clientside.set_props('place-state', { data: this.placeStateCache });
            }
        }

        // Handle interrupt message and update chat display
        if (interruptData.message && interruptData.messages) {
            // Get existing messages from interrupt data
            const existingMessages = interruptData.messages || [];
            
            // Create new interrupt message
            const newInterruptMessage = {
                _type: 'ai',
                content: interruptData.message,
                type: 'ai'
            };
            
            // Combine existing messages with new interrupt message
            const allMessages = [...existingMessages, newInterruptMessage];
            
            // Update chat display
            this.updateChatDisplay(allMessages);
            
            // Store only the new interrupt message to send back when workflow resumes
            this.pendingInterruptMessage = interruptData.message;
        }

        // Show buttons if provided
        if (interruptData.options && Array.isArray(interruptData.options)) {
            this.showButtons(interruptData.options);
            // Cache options so they can be sent back with subsequent messages
            this.currentOptions = interruptData.options;
        } else {
            // Clear buttons if no options
            this.clearButtons();
        }
    }

    // Zoom to place disambiguation markers and any selected polygons
    zoomToPlaceMarkers(placeCoordinates, interruptData = null) {
        console.log('SSE: Zooming to place markers:', placeCoordinates);
        
        // FIRST: Set disambiguation mode immediately to prevent polygon loading
        window._disambiguationMode = true;
        console.log('SSE: Disambiguation mode enabled in zoomToPlaceMarkers');
        
        // Find the leaflet map instance
        const mapElement = document.getElementById('leaflet-map');
        if (!mapElement || !mapElement._leaflet_map) {
            console.warn('SSE: Could not find leaflet map for zooming to place markers');
            return;
        }
        
        const map = mapElement._leaflet_map;
        
        if (placeCoordinates.length === 0) return;
        
        // Get selected polygons that should remain visible
        const places = interruptData?.places || [];
        const selectedUnits = places
            .filter(place => place.g_unit !== null && place.g_unit !== undefined)
            .map(place => String(place.g_unit));
        
        console.log('SSE: Selected units to keep visible:', selectedUnits);
        
        // Clear cache and layer
        console.log('SSE: Clearing unselected polygons for place disambiguation');
        if (window.polygonManagement && window.polygonManagement.clearCache) {
            window.polygonManagement.clearCache();
        }
        
        const geojsonLayer = window.polygonManagement?.findGeoJSONLayer?.(map);
        if (geojsonLayer && geojsonLayer.clearLayers) {
            console.log('SSE: Clearing all polygons from map');
            geojsonLayer.clearLayers();
        }
        
        // If we have selected units, fetch and display them
        if (selectedUnits.length > 0 && window.polygonManagement && window.polygonManagement.fetchPolygonsByIds) {
            console.log('SSE: Fetching selected polygons for display during disambiguation');
            
            // Group selected units by their unit types
            const unitsByType = {};
            places.forEach(place => {
                if (place.g_unit && place.g_unit_type) {
                    if (!unitsByType[place.g_unit_type]) {
                        unitsByType[place.g_unit_type] = [];
                    }
                    unitsByType[place.g_unit_type].push(String(place.g_unit));
                }
            });
            
            console.log('SSE: Units grouped by type:', unitsByType);
            
            // Fetch polygons for each unit type
            const fetchPromises = [];
            const mapState = window.mapState || {};
            
            for (const [unitType, units] of Object.entries(unitsByType)) {
                console.log(`SSE: Fetching ${units.length} polygons of type ${unitType}`);
                const promise = window.polygonManagement.fetchPolygonsByIds(map, mapState, unitType, units, null);
                fetchPromises.push(promise);
            }
            
            if (fetchPromises.length > 0) {
                Promise.all(fetchPromises)
                    .then(() => {
                        console.log('SSE: All selected polygons loaded');
                        
                        // Update the hideout to show selected polygons
                        const geojsonLayer = window.polygonManagement?.findGeoJSONLayer?.(map);
                        if (geojsonLayer && geojsonLayer.options) {
                            console.log('SSE: Updating hideout with selected units:', selectedUnits);
                            geojsonLayer.options.hideout = { selected: selectedUnits };
                            
                            // Force style refresh
                            if (window.polygonManagement && window.polygonManagement.refreshLayerStyles) {
                                window.polygonManagement.refreshLayerStyles(geojsonLayer, selectedUnits);
                            }
                        }
                        
                        this.calculateCombinedZoomBounds(map, geojsonLayer, selectedUnits, placeCoordinates);
                    })
                    .catch(error => {
                        console.error('SSE: Error fetching selected polygons:', error);
                        this.calculateCombinedZoomBounds(map, null, [], placeCoordinates);
                    });
                return;
            }
        }
        
        // No selected polygons, just zoom to markers
        this.calculateCombinedZoomBounds(map, null, [], placeCoordinates);
    }

    // Clear disambiguation mode to re-enable polygon loading
    clearDisambiguationMode() {
        console.log('SSE: Clearing disambiguation mode');
        window._disambiguationMode = false;
    }

    // Simple error handling
    handleError(error) {
        console.error('SSE: Received error:', error);
        this.handleMessage(`Error: ${error}`);
        this.hideThinkingIndicator();
    }

    // Helper: Update visualization panel
    updateVisualization(state) {
        // Show/hide visualization panel
        const container = document.getElementById('visualization-panel-container');
        const area = document.getElementById('visualization-area');

        if (state.cubes && state.cubes.length > 0) {
            // Show visualization
            if (container) container.style.display = 'flex';
            if (area) area.style.display = 'flex';
        } else if (state.show_visualization === false) {
            // Hide visualization
            if (container) container.style.display = 'none';
            if (area) area.style.display = 'none';
        }
    }

    // Helper: Update map selection (simplified)
    updateMapSelection(places) {

        // De-duplicate by skipping when places signature unchanged
        const sig = this._placesSignature(places);
        if (this.lastPlacesSig === sig) { return; }
        this.lastPlacesSig = sig;

        // Use pure map state if available, otherwise fall back to dash store
        if (window.pureMapState) {
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: {
                    places: places
                }
            });
        } else {
            // Legacy fallback: Extract selected units and update dash store
            const selectedPolygons = places
                .filter(place => place.g_unit !== null && place.g_unit !== undefined)
                .map(place => String(place.g_unit));

            console.log('SSE: Fallback - extracted polygons:', selectedPolygons);

            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                // Create minimal places array for legacy compatibility
                const placesFromPolygons = selectedPolygons.map((polygonId, idx) => ({
                    name: `Place ${idx + 1}`,
                    g_unit: parseInt(polygonId),
                    g_unit_type: 'MOD_REG'  // Default type
                }));

                dash_clientside.set_props('map-state', {
                    data: {
                        places: placesFromPolygons,
                        zoom_to_selection: selectedPolygons.length > 0
                    }
                });
            }
        }
    }

    // Internal: stable signature for places to avoid redundant updates
    _placesSignature(places) {
        try {
            const keyParts = (places || [])
                .filter(p => p && p.g_unit !== null && p.g_unit !== undefined)
                .map(p => `${String(p.g_unit)}:${p.g_unit_type || ''}`)
                .sort();
            return keyParts.join('|');
        } catch (e) {
            return String(Date.now());
        }
    }

    // Helper: Show interrupt buttons
    showButtons(options) {
        // Determine target container: theme options go to theme panel; others to options-container
        const hasThemeOptions = Array.isArray(options) && options.some(o => o.option_type === 'theme_query');
        const themePanel = document.getElementById('theme-selection-panel');
        const themeButtons = document.getElementById('theme-selection-buttons');
        const optionsContainer = document.getElementById('options-container');
        const container = hasThemeOptions && themeButtons ? themeButtons : optionsContainer;
        if (!container) return;

        // Clear appropriate container(s)
        if (themeButtons) themeButtons.innerHTML = '';
        if (!hasThemeOptions && optionsContainer) optionsContainer.innerHTML = '';

        // Show/hide theme panel
        if (themePanel) themePanel.style.display = hasThemeOptions ? 'block' : 'none';

        // Create new buttons
        options.forEach(option => {
            const button = document.createElement('button');
            button.className = 'btn btn-outline-primary me-2 mb-2';
            button.textContent = option.label;

            // Apply unit type color if provided
            if (option.color) {
                button.style.borderColor = option.color;
                button.style.color = option.color;

                // Set hover effects using CSS custom properties
                button.style.setProperty('--btn-hover-bg', option.color);
                button.style.setProperty('--btn-hover-color', 'white');

                // Add hover effect with inline styles
                button.addEventListener('mouseenter', function() {
                    this.style.backgroundColor = option.color;
                    this.style.color = 'white';
                });

                button.addEventListener('mouseleave', function() {
                    this.style.backgroundColor = 'transparent';
                    this.style.color = option.color;
                });
            }

            button.onclick = () => {
                console.log('Button clicked:', option);

                // Send selection via existing connection
                if (this.threadId) {
                    const selectionInput = {
                        selection_idx: option.value,
                        button_type: option.option_type,
                        current_node: this.currentNode  // Pass back the current_node from interrupt
                    };

                    // Include interrupt context for unit type selections to preserve state
                    if (option.option_type === 'unit' && this.currentInterruptData) {
                        // Preserve critical workflow state when resuming from unit type selection
                        if (this.currentInterruptData.current_place_index !== undefined) {
                            selectionInput.current_place_index = this.currentInterruptData.current_place_index;
                        }
                        if (this.currentInterruptData.places) {
                            selectionInput.places = this.currentInterruptData.places;
                        }
                        console.log('SSE: Including interrupt context for unit selection:', {
                            current_place_index: selectionInput.current_place_index,
                            places_count: selectionInput.places ? selectionInput.places.length : 0
                        });
                    }

                    // Send selection through existing connection instead of creating new one
                    this.sendSelection(selectionInput);
                }

                // Clear relevant buttons after selection
                if (option.option_type === 'theme_query') {
                    if (themeButtons) themeButtons.innerHTML = '';
                    if (themePanel) themePanel.style.display = 'none';
                    this.currentOptions = null;
                } else {
                    this.clearButtons();
                }
            };

            container.appendChild(button);
        });
    }

    // Send selection data to continue workflow without creating new SSE connection
    sendSelection(selectionInput) {
        if (!this.threadId) {
            console.error('SSE: No thread ID available for sending selection');
            return;
        }

        // Include pending interrupt message if available
        if (this.pendingInterruptMessage) {
            selectionInput.interrupt_message = this.pendingInterruptMessage;
            this.pendingInterruptMessage = null; // Clear after including
        }

        console.log('SSE: Sending selection via existing connection:', selectionInput);

        // Send selection data via POST request to continue workflow
    fetch(this.joinPath(this.basePath, `/workflow/${this.threadId}`), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                // Include current options so the backend can keep LLM aware
                workflow_input: Object.assign({}, selectionInput, (this.currentOptions && this.currentOptions.length > 0 ? { options: this.currentOptions } : {}))
            })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            console.log('SSE: Selection sent successfully');
        })
        .catch(error => {
            console.error('SSE: Failed to send selection:', error);
            // Fallback to creating new connection if POST fails
            console.log('SSE: Falling back to new connection');
            this.postWorkflowInput(selectionInput)
              .catch(err2 => console.error('SSE: retry failed', err2));
        });
    }

    // Helper: Clear buttons
    clearButtons() {
        const container = document.getElementById('options-container');
        const themeButtons = document.getElementById('theme-selection-buttons');
        const themePanel = document.getElementById('theme-selection-panel');
        if (container) container.innerHTML = '';
        if (themeButtons) themeButtons.innerHTML = '';
        if (themePanel) themePanel.style.display = 'none';
        // Clear cached options when buttons are cleared
        this.currentOptions = null;
    }

    // Helper: Clear chat display
    clearChatDisplay() {
        const chatDisplay = document.getElementById('chat-display');
        if (chatDisplay) {
            chatDisplay.innerHTML = '';
            console.log('SSE: Chat display cleared for reset');
        }
    }

    // Helper: Hide visualization panel
    hideVisualization() {
        const container = document.getElementById('visualization-panel-container');
        const area = document.getElementById('visualization-area');
        if (container) container.style.display = 'none';
        if (area) area.style.display = 'none';
        console.log('SSE: Visualization hidden for reset');
    }

    // Helper: Handle map update requests (replicated from complex SSE client)
    handleMapUpdateRequest(request) {
        console.log('SSE: handleMapUpdateRequest called with:', request);
        
        // Clear disambiguation mode when handling map update requests
        // This ensures proper polygon display after place/unit selection is complete
        if (window._disambiguationMode) {
            console.log('SSE: Clearing disambiguation mode due to map update request');
            this.clearDisambiguationMode();
        }
        
        if (window.pureMapState && request.places !== undefined) {
            console.log('SSE: Request places array:', request.places);
            // Extract units and unit types from places array (single source of truth)
            const units = this.getSelectedUnits({places: request.places});
            const unitTypes = this.getSelectedUnitTypes({places: request.places});
            console.log('SSE: Extracted units from map update request:', units, 'unit types:', unitTypes);

            // Sync the selected polygons using places as single source of truth
            // Only if selection actually differs from current map state
            const currentSelected = (window.pureMapState?.getSelectedPolygons?.() || []).map(String);
            const requestedSelected = (units || []).map(String);
            const sameSelection = JSON.stringify([...currentSelected].sort()) === JSON.stringify([...requestedSelected].sort());
            if (!sameSelection) {
                window.pureMapState.executeWorkflowCommand({
                    type: 'sync_state',
                    state: {
                        places: request.places
                    }
                });
            } else {
                console.log('SSE: Selection unchanged, skipping sync_state');
            }
            // Align dedupe signature with the request places
            this.lastPlacesSig = this._placesSignature(request.places);

            // Check if we have selected place coordinates to include in zoom
            const selectedPlaceCoords = request.selected_place_coordinates || [];
            console.log('SSE: Selected place coordinates for zoom:', selectedPlaceCoords);

            // Only fetch polygons if we have units to fetch
            if (units && units.length > 0) {
                console.log('SSE: Fetching polygons and zooming for map update request');
                this.fetchPolygonsAndZoom(units, unitTypes, request.places, selectedPlaceCoords);
            } else if (selectedPlaceCoords.length > 0) {
                // No polygons but we have place coordinates to zoom to
                console.log('SSE: No polygons but zooming to selected place coordinates');
                const mapElement = document.getElementById('leaflet-map');
                if (mapElement && mapElement._leaflet_map) {
                    this.calculateCombinedZoomBounds(mapElement._leaflet_map, null, [], selectedPlaceCoords);
                }
            } else {
                console.log('SSE: No units to fetch - map cleared');
            }

            console.log('SSE: Synced map state via map_update_request');
        }
    }

    // Helper: Handle info marker request  
    handleInfoMarkerRequest(request) {
        console.log('SSE: handleInfoMarkerRequest called with:', request);
        
        if (!request.info_place) {
            console.warn('SSE: No info_place in request');
            return;
        }
        
        const infoPlace = request.info_place;
        
        // Create place coordinates for the disambiguation marker system
        const placeCoordinates = [{
            index: 0,
            name: infoPlace.name,
            county: infoPlace.county_name || '',
            lat: infoPlace.coordinates.lat,
            lon: infoPlace.coordinates.lon,
            g_place: infoPlace.g_place,
            is_single: true,  // Orange marker
            is_info_marker: true
        }];
        
        // Use the existing disambiguation marker system to show the info marker
        const interruptData = {
            place_coordinates: placeCoordinates,
            current_node: 'PlaceInfo_node',
            is_info_marker: true,
            message: `Information about ${infoPlace.name}`
        };
        
        // Update the sse-interrupt-store to trigger marker display
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            // Clear any previous markers then set new info marker request
            dash_clientside.set_props('sse-interrupt-store', { data: {} });
            setTimeout(() => {
                dash_clientside.set_props('sse-interrupt-store', { data: interruptData });
                console.log('SSE: Set info marker in sse-interrupt-store');
            }, 0);
        }
        
        // Also zoom to the location or bounds
        if (infoPlace.coordinates) {
            // Find the leaflet map instance the same way as zoomToPlaceMarkers
            const mapElement = document.getElementById('leaflet-map');
            if (!mapElement || !mapElement._leaflet_map) {
                console.warn('SSE: Could not find leaflet map element for zooming');
                return;
            }
            
            const mapInstance = mapElement._leaflet_map;
            
            // If we have a g_unit, try to get its bounds
            if (infoPlace.g_unit && window.pureMapState) {
                // Try to get polygon bounds from the map state
                const polygon = window.pureMapState.getPolygonById(infoPlace.g_unit);
                if (polygon && polygon.getBounds) {
                    mapInstance.fitBounds(polygon.getBounds());
                    console.log('SSE: Zoomed to polygon bounds for info place');
                    return;
                }
            }
            
            // Otherwise, create a reasonable bounding box around the point
            // This creates approximately a 10km box around the location
            const latOffset = 0.045; // ~5km north/south  
            const lonOffset = 0.065; // ~5km east/west (adjusted for UK latitude)
            
            const bounds = [
                [infoPlace.coordinates.lat - latOffset, infoPlace.coordinates.lon - lonOffset],
                [infoPlace.coordinates.lat + latOffset, infoPlace.coordinates.lon + lonOffset]
            ];
            
            mapInstance.fitBounds(bounds, {
                padding: [50, 50] // Add some padding around the bounds
            });
            console.log('SSE: Zoomed to bounds around info place location', bounds);
        } else {
            console.warn('SSE: No coordinates in info place for zooming');
        }
    }

    // Helper: Handle units that need map selection
    handleUnitsNeedingSelection(units, places) {
        console.log('SSE: handleUnitsNeedingSelection called with units:', units, 'places:', places);

        if (window.pureMapState && places) {
            // Extract units and unit types from places array
            const allUnits = this.getSelectedUnits({places: places});
            const allUnitTypes = this.getSelectedUnitTypes({places: places});
            console.log('SSE: Handling units needing selection:', units, 'all units:', allUnits, 'all unit types:', allUnitTypes);

            // Validate extracted data
            if (!Array.isArray(allUnits)) {
                console.error('SSE: getSelectedUnits returned non-array:', allUnits);
                return;
            }
            if (!Array.isArray(allUnitTypes)) {
                console.error('SSE: getSelectedUnitTypes returned non-array:', allUnitTypes);
                return;
            }

            // Sync all selected polygons to map using places as single source of truth
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: {
                    places: places
                }
            });

            // Fetch polygons, zoom to bounds, then update unit types
            this.fetchPolygonsAndZoom(allUnits, allUnitTypes, places);
        } else {
            console.warn('SSE: Cannot handle units needing selection - pureMapState or places missing');
        }
    }

    // Helper: Calculate combined zoom bounds for polygons and place coordinates
    calculateCombinedZoomBounds(map, layer, polygonIds, placeCoordinates) {
        console.log('SSE: Calculating combined bounds for polygons and places');
        
        const bounds = window.L.latLngBounds();
        let hasContent = false;
        
        // Add polygon bounds if available
        if (layer && layer._layers && polygonIds && polygonIds.length > 0) {
            Object.values(layer._layers).forEach(layerObj => {
                if (layerObj.feature && polygonIds.includes(String(layerObj.feature.id))) {
                    if (layerObj.getBounds) {
                        bounds.extend(layerObj.getBounds());
                        hasContent = true;
                        console.log('SSE: Added polygon bounds for ID:', layerObj.feature.id);
                    }
                }
            });
        }
        
        // Add place coordinates to bounds
        if (placeCoordinates && placeCoordinates.length > 0) {
            placeCoordinates.forEach(coord => {
                bounds.extend([coord.lat, coord.lon]);
                hasContent = true;
                console.log('SSE: Added place coordinate bounds:', coord.name || 'Unknown', coord.lat, coord.lon);
            });
        }
        
        // Apply zoom if we have valid bounds
        if (hasContent && bounds.isValid()) {
            console.log('SSE: Zooming to combined bounds');
            map.fitBounds(bounds, { 
                padding: [30, 30], 
                maxZoom: 12 
            });
            return true;
        } else if (placeCoordinates && placeCoordinates.length === 1) {
            // Fallback: single place coordinate
            console.log('SSE: Fallback to single coordinate zoom');
            map.setView([placeCoordinates[0].lat, placeCoordinates[0].lon], 10);
            return true;
        }
        
        return false; // No zoom applied
    }

    // Helper: Fetch polygons, zoom to bounds, and update unit types
    fetchPolygonsAndZoom(units, unitTypes, places, includePlaceCoordinates = []) {
        // Add defensive checks
        if (!Array.isArray(units)) {
            console.error('SSE: units is not an array:', units);
            return;
        }
        if (!Array.isArray(unitTypes)) {
            console.error('SSE: unitTypes is not an array:', unitTypes);
            unitTypes = [];
        }

        if (units.length > 0 && window.polygonManagement && window.polygonManagement.fetchPolygonsByIds) {
            console.log('SSE: Fetching polygons for units:', units, 'types:', unitTypes);

            // Ensure units are valid numbers/strings
            const validUnits = units.filter(unit => unit != null && unit !== '');
            if (validUnits.length === 0) {
                console.warn('SSE: No valid units to fetch');
                return;
            }

            console.log('SSE: Valid units to fetch:', validUnits);

            // Get the map and necessary parameters
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;

            if (!map) {
                console.error('SSE: Map not found');
                return;
            }

            // Build id -> unitType map, prefer places when available
            const idToType = {};
            if (Array.isArray(places) && places.length >= validUnits.length) {
                (places || []).forEach(p => {
                    if (p && p.g_unit != null) idToType[String(p.g_unit)] = p.g_unit_type || null;
                });
            } else {
                for (let i = 0; i < validUnits.length; i++) {
                    const id = String(validUnits[i]);
                    idToType[id] = unitTypes[i] || (unitTypes.length === 1 ? unitTypes[0] : null);
                }
            }

            // Group units by type
            const byType = {};
            validUnits.map(String).forEach(id => {
                const ut = idToType[id];
                if (!ut) {
                    console.warn('SSE: No unit type for id, skipping fetch for', id);
                    return;
                }
                (byType[ut] ||= []).push(id);
            });
            console.log('SSE: Fetch groups by unit type:', byType);

            // Create a minimal map state object with places (single source of truth)
            const mapState = { places: places || [] };

            // Check layer for existing IDs and prepare per-type fetches
            const existingLayer = window.polygonManagement.findGeoJSONLayer ?
                window.polygonManagement.findGeoJSONLayer(map) : null;
            const requestedIds = validUnits.map(String);
            let allPresent = true;
            const fetchPromises = [];
            if (existingLayer && existingLayer._layers) {
                const layerIds = [];
                Object.values(existingLayer._layers).forEach(l => {
                    if (l.feature && l.feature.id) layerIds.push(String(l.feature.id));
                });
                const globalMissing = requestedIds.filter(id => !layerIds.includes(id));
                if (globalMissing.length) allPresent = false;

                Object.entries(byType).forEach(([ut, ids]) => {
                    const missing = ids.filter(id => !layerIds.includes(id));
                    if (missing.length) {
                        console.warn('SSE: Missing polygon IDs on layer for', ut, '→', missing);
                        fetchPromises.push(
                            window.polygonManagement.fetchPolygonsByIds(map, mapState, ut, missing, null)
                        );
                    } else {
                        console.log('SSE: All', ut, 'polygons present on layer; skipping fetch');
                    }
                });
            } else {
                allPresent = false;
                Object.entries(byType).forEach(([ut, ids]) => {
                    fetchPromises.push(
                        window.polygonManagement.fetchPolygonsByIds(map, mapState, ut, ids, null)
                    );
                });
            }

            const afterEnsurePolygons = (layer) => {
                if (layer && polygonManagement.zoomTo) {
                    // If a recent local zoom for the same set of IDs occurred, skip SSE zoom to avoid double-zoom
                    const recentLocalZoom = (function() {
                        try {
                            const tsOk = typeof window._lastLocalZoomTs === 'number' && (Date.now() - window._lastLocalZoomTs) < 1500;
                            const sameIds = Array.isArray(window._lastLocalZoomIds) &&
                                window._lastLocalZoomIds.slice().sort().join(',') === requestedIds.slice().sort().join(',');
                            return tsOk && sameIds;
                        } catch (e) { return false; }
                    })();

                    let zoomWasPerformed = false;
                    if (!recentLocalZoom) {
                        console.log('SSE: Considering zoom with units:', requestedIds, 'and place coords:', includePlaceCoordinates);

                        // Try combined zoom first if we have place coordinates
                        let zoomApplied = false;
                        if (includePlaceCoordinates && includePlaceCoordinates.length > 0) {
                            console.log('SSE: Creating custom zoom bounds with place coordinates');
                            zoomApplied = this.calculateCombinedZoomBounds(map, layer, requestedIds, includePlaceCoordinates);
                        }
                        if (zoomApplied) {
                            zoomWasPerformed = true;
                        } else {
                            console.log('SSE: Using standard polygon zoom');
                            try { window._zoomSource = 'sse'; } catch (e) {}
                            polygonManagement.zoomTo(map, requestedIds, layer);
                            zoomWasPerformed = true;
                        }
                    } else {
                        console.log('SSE: Skipping zoom (recent local zoom already applied)');
                    }

                    // Only update unit types if they differ from current
                    if (zoomWasPerformed) {
                        console.log('SSE: Zoom initiated for units:', requestedIds);
                    } else {
                        console.log('SSE: No zoom performed (units already centered)');
                    }
                    if (unitTypes.length > 0 && requestedIds.length > 0) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        const currentUnitTypes = (window.pureMapState?.getUnitTypes?.() || []).slice().sort().join(',');
                        const requestedUnitTypes = uniqueUnitTypes.slice().sort().join(',');
                        if (currentUnitTypes !== requestedUnitTypes) {
                            console.log('SSE: Now switching to unit types:', uniqueUnitTypes);
                            setTimeout(() => {
                                window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                            }, 500);
                        } else {
                            console.log('SSE: Unit types unchanged, skipping update');
                        }
                    } else {
                        console.log('SSE: No units to zoom to, skipping unit type update');
                    }
                } else {
                    console.log('SSE: No layer or zoom function available, just updating unit types');
                    if (unitTypes.length > 0) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        const currentUnitTypes = (window.pureMapState?.getUnitTypes?.() || []).slice().sort().join(',');
                        const requestedUnitTypes = uniqueUnitTypes.slice().sort().join(',');
                        if (currentUnitTypes !== requestedUnitTypes) {
                            window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                        }
                    }
                }
            };

            if (allPresent) {
                afterEnsurePolygons(existingLayer);
            } else {
                Promise.all(fetchPromises).then(results => {
                    try {
                        const mergedIds = [].concat(...(results || []).map(r => (r && r.features) ? r.features.map(f => f.id) : []));
                        console.log('SSE: Polygons fetched from API (grouped):', mergedIds);
                    } catch (e) {}
                    const layer = window.polygonManagement.findGeoJSONLayer ?
                        window.polygonManagement.findGeoJSONLayer(map) : null;
                    afterEnsurePolygons(layer);
                }).catch(err => {
                    console.error('SSE: Failed to fetch polygons (grouped):', err);
                    // Fallback: try to zoom anyway and update unit types
                    window.pureMapState.executeWorkflowCommand({ type: 'zoom_to_selection' });
                    if (unitTypes.length > 0) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        const currentUnitTypes = (window.pureMapState?.getUnitTypes?.() || []).slice().sort().join(',');
                        const requestedUnitTypes = uniqueUnitTypes.slice().sort().join(',');
                        if (currentUnitTypes !== requestedUnitTypes) {
                            window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                        }
                    }
                });
            }
        } else if (unitTypes.length > 0) {
            // No polygon fetching available, just update unit types
            const uniqueUnitTypes = [...new Set(unitTypes)];
            const currentUnitTypes = (window.pureMapState?.getUnitTypes?.() || []).slice().sort().join(',');
            const requestedUnitTypes = uniqueUnitTypes.slice().sort().join(',');
            if (currentUnitTypes !== requestedUnitTypes) {
                console.log('SSE: No polygon management available, just updating unit types:', uniqueUnitTypes);
                window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
            } else {
                console.log('SSE: Unit types unchanged, skipping update');
            }
        }
    }

    // Helper: Extract selected units from state
    getSelectedUnits(state) {
        const places = state.places || [];
        return places.map(place => place.g_unit).filter(unit => unit !== null && unit !== undefined);
    }

    // Helper: Extract selected unit types from state
    getSelectedUnitTypes(state) {
        const places = state.places || [];
        const result = [];
        places.forEach(place => {
            if (place.g_unit !== null && place.g_unit !== undefined) {
                result.push(place.g_unit_type);
            }
        });
        return result;
    }

    // Update chat display with all messages in proper order
    updateChatDisplay(messages) {
        console.log('SSE: Updating chat display with', messages.length, 'messages');

        const chatDisplay = document.getElementById('chat-display');
        if (!chatDisplay) {
            console.error('SSE: chat-display element not found');
            return;
        }

        // Clear existing messages
        chatDisplay.innerHTML = '';

        // Add messages in reverse order (newest first)
        for (let i = messages.length - 1; i >= 0; i--) {
            const msg = messages[i];
            if (!msg) continue;

            const messageDiv = document.createElement('div');

            // Determine message type and content
            let content = '';
            let className = 'speech-bubble';

            if (msg._type === 'human' || msg._type === 'HumanMessage') {
                // Human message
                content = msg.content;
                className += ' user-bubble';
            } else if (msg._type === 'ai' || msg._type === 'AIMessage') {
                // AI message
                content = msg.content;
                className += ' ai-bubble';
            } else if (typeof msg === 'object' && msg.content) {
                // Generic message with content
                content = msg.content;
                // Try to determine type from other fields
                if (msg.type === 'human') {
                    className += ' user-bubble';
                } else {
                    className += ' ai-bubble';
                }
            } else if (Array.isArray(msg) && msg.length >= 2) {
                // Tuple format: [role, content]
                content = msg[1];
                if (msg[0] === 'user' || msg[0] === 'human') {
                    className += ' user-bubble';
                } else {
                    className += ' ai-bubble';
                }
            }

            if (content) {
                messageDiv.className = className;
                
                // Use DOMParser to safely parse HTML content
                const parser = new DOMParser();
                const doc = parser.parseFromString(content, 'text/html');
                
                // Clear the message div and append the parsed content
                messageDiv.innerHTML = '';
                // Move all child nodes from parsed body to our message div
                while (doc.body.firstChild) {
                    messageDiv.appendChild(doc.body.firstChild);
                }
                
                chatDisplay.appendChild(messageDiv);
            }
        }

        // If the assistant is busy, append a temporary thinking bubble
        if (this.llmBusy) {
            this.appendThinkingMessage(chatDisplay);
        }

        // Re-enable send button after update
        const sendButton = document.getElementById('send-button');
        if (sendButton) {
            sendButton.disabled = false;
        }
    }

    // Inline thinking indicator helpers
    appendThinkingMessage(chatDisplay) {
        try {
            let messageDiv = document.getElementById('ai-thinking');
            if (messageDiv && messageDiv.parentNode) {
                // Remove to reinsert at the desired position
                messageDiv.parentNode.removeChild(messageDiv);
            }
            if (!messageDiv) {
                messageDiv = document.createElement('div');
                messageDiv.className = 'speech-bubble ai-bubble';
                messageDiv.id = 'ai-thinking';
                const spinner = document.createElement('span');
                spinner.className = 'spinner-border spinner-border-sm text-primary me-2';
                spinner.setAttribute('role', 'status');
                spinner.setAttribute('aria-hidden', 'true');
                const text = document.createElement('span');
                text.textContent = 'Thinking…';
                text.className = 'text-muted';
                messageDiv.appendChild(spinner);
                messageDiv.appendChild(text);
            }

            // With #chat-display using flex-direction: column-reverse,
            // inserting as the FIRST child will render it just BELOW
            // the newest message visually.
            const first = chatDisplay.firstChild;
            if (first) {
                chatDisplay.insertBefore(messageDiv, first);
            } else {
                chatDisplay.appendChild(messageDiv);
            }
            chatDisplay.scrollTop = chatDisplay.scrollHeight;
        } catch (e) { /* no-op */ }
    }

    showThinkingIndicator() {
        try {
            const chatDisplay = document.getElementById('chat-display');
            if (!chatDisplay) return;
            this.appendThinkingMessage(chatDisplay);
        } catch (e) { /* no-op */ }
    }

    hideThinkingIndicator() {
        try {
            const el = document.getElementById('ai-thinking');
            if (el && el.parentNode) {
                el.parentNode.removeChild(el);
            }
        } catch (e) { /* no-op */ }
    }
    }


// Create global instance
window.simpleSSE = new SimpleSSEClient();

// Simple function to connect SSE with thread ID
window.connectSSE = function(threadId) {
    if (threadId) {
        console.log('Simple SSE Client: Connecting with thread ID:', threadId);
        window.simpleSSE.connect(threadId);
    }
};

// Initialize when Dash is ready
document.addEventListener('DOMContentLoaded', () => {
    console.log('Simple SSE Client: DOM loaded and ready for connections');
});
