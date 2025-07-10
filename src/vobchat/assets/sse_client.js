// Simple SSE Client - Clean rewrite
// Single responsibility: Connect to SSE stream and update UI directly

class SimpleSSEClient {
    constructor() {
        this.eventSource = null;
        this.threadId = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 3;

        console.log('Simple SSE Client initialized');
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
        }

        // Build SSE URL with correct prefix
        let url = `/app/sse/${threadId}`;

        this.eventSource = new EventSource(url);

        this.postWorkflowInput = (input) => {
          if (!this.threadId) return;

          return fetch(`/app/workflow/${this.threadId}`, {
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
            console.log('SSE: Raw state_update event received:', event.data);
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
        console.log('SSE: State update keys:', Object.keys(state));
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

        // Handle messages array to update chat display
        if (state.messages && Array.isArray(state.messages)) {
            this.updateChatDisplay(state.messages);
        }

        // Update visualization if we have cube data
        if (state.cubes || state.places) {
            this.updateVisualization(state);
        }

        // Handle map update requests
        if (state.map_update_request && state.map_update_request.action === 'update_map_selection') {
            console.log('SSE: Processing map update request:', state.map_update_request);
            this.handleMapUpdateRequest(state.map_update_request);
        } else if (state.places && Array.isArray(state.places)) {
            // Use places as single source of truth
            this.updateMapSelection(state.places);
        }

        // Handle units needing map selection
        if (state.units_needing_map_selection && Array.isArray(state.units_needing_map_selection) && state.units_needing_map_selection.length > 0) {
            console.log('SSE: Units needing map selection:', state.units_needing_map_selection);
            this.handleUnitsNeedingSelection(state.units_needing_map_selection, state.places);
        }

        // Update stores via Dash (minimal, targeted updates only)
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            // Only update place-state with essential data if something actually changed
            const updates = {};
            let hasUpdates = false;

            if (state.places !== undefined) {
                updates.places = state.places;
                hasUpdates = true;
            }
            if (state.cubes !== undefined) {
                updates.cubes = state.cubes;
                hasUpdates = true;
            }
            if (state.selected_theme !== undefined) {
                updates.selected_theme = state.selected_theme;
                hasUpdates = true;
            }

            if (hasUpdates) {
                // Note: We can't access current state outside of a callback context
                // However, Dash should merge partial updates with existing state automatically
                // We only send the fields that were actually in the state update
                dash_clientside.set_props('place-state', {
                    data: updates
                });
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

        // Store current_node for when buttons are clicked
        this.currentNode = interruptData.current_node || null;

        // Handle cube data if provided
        if (interruptData.cube_data_ready && interruptData.cubes) {
            console.log('SSE: Received cube data, updating state');

            // Update place-state with complete state data from interrupt
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                dash_clientside.set_props('place-state', {
                    data: {
                        cubes: JSON.parse(interruptData.cubes),
                        selected_cubes: JSON.parse(interruptData.selected_cubes || interruptData.cubes),
                        show_visualization: interruptData.show_visualization || true,
                        places: interruptData.places || [],
                        selected_theme: interruptData.selected_theme
                    }
                });
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
            
            // Store updated messages to send back when workflow resumes
            this.currentMessages = allMessages;
        }

        // Show buttons if provided
        if (interruptData.options && Array.isArray(interruptData.options)) {
            this.showButtons(interruptData.options);
        } else {
            // Clear buttons if no options
            this.clearButtons();
        }
    }

    // Simple error handling
    handleError(error) {
        console.error('SSE: Received error:', error);
        this.handleMessage(`Error: ${error}`);
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
        console.log('SSE: Updating map selection with places:', places);

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

    // Helper: Show interrupt buttons
    showButtons(options) {
        const container = document.getElementById('options-container');
        if (!container) return;

        // Clear existing buttons
        container.innerHTML = '';

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

                    // Send selection through existing connection instead of creating new one
                    this.sendSelection(selectionInput);
                }

                // Clear buttons after selection
                this.clearButtons();
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

        // Include updated messages if available
        if (this.currentMessages) {
            selectionInput.messages = this.currentMessages;
            this.currentMessages = null; // Clear after including
        }

        console.log('SSE: Sending selection via existing connection:', selectionInput);

        // Send selection data via POST request to continue workflow
        fetch(`/app/workflow/${this.threadId}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                workflow_input: selectionInput
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
        if (container) {
            container.innerHTML = '';
        }
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
        if (window.pureMapState && request.places !== undefined) {
            console.log('SSE: Request places array:', request.places);
            // Extract units and unit types from places array (single source of truth)
            const units = this.getSelectedUnits({places: request.places});
            const unitTypes = this.getSelectedUnitTypes({places: request.places});
            console.log('SSE: Extracted units from map update request:', units, 'unit types:', unitTypes);

            // Always sync the selected polygons using places as single source of truth
            // This handles both adding polygons and clearing them (when empty)
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: {
                    places: request.places
                }
            });

            // Only fetch polygons if we have units to fetch
            if (units && units.length > 0) {
                console.log('SSE: Fetching polygons and zooming for map update request');
                this.fetchPolygonsAndZoom(units, unitTypes, request.places);
            } else {
                console.log('SSE: No units to fetch - map cleared');
            }

            console.log('SSE: Synced map state via map_update_request');
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

    // Helper: Fetch polygons, zoom to bounds, and update unit types
    fetchPolygonsAndZoom(units, unitTypes, places) {
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

            // Use the first unit type (they should all be the same for a single selection)
            const unitType = unitTypes.length > 0 ? unitTypes[0] : null;
            console.log('SSE: Unit types for fetching:', unitTypes, 'using first:', unitType);
            if (!unitType) {
                console.error('SSE: No unit type available');
                return;
            }

            // Convert units to strings as expected by fetchPolygonsByIds
            const unitStrings = validUnits.map(String);

            // Create a minimal map state object with places (single source of truth)
            const mapState = {
                places: places || []
            };

            console.log('SSE: Calling fetchPolygonsByIds with:', {map, mapState, unitType, unitStrings});

            // Fetch the polygon data first (using the correct parameter signature)
            window.polygonManagement.fetchPolygonsByIds(map, mapState, unitType, unitStrings, null).then((result) => {
                console.log('SSE: Polygons fetched from API, result:', result);
                console.log('SSE: Features returned:', result.features?.map(f => f.id));
                console.log('SSE: Now zooming to them');

                // Find the GeoJSON layer that should now contain our polygons
                const layer = window.polygonManagement.findGeoJSONLayer ?
                    window.polygonManagement.findGeoJSONLayer(map) : null;

                // Debug: Check what's actually on the layer
                if (layer && layer._layers) {
                    const layerIds = [];
                    Object.values(layer._layers).forEach(l => {
                        if (l.feature && l.feature.id) {
                            layerIds.push(String(l.feature.id));
                        }
                    });
                    console.log('SSE: Layer currently contains IDs:', layerIds);
                    console.log('SSE: We want to zoom to IDs:', unitStrings);
                    const missingIds = unitStrings.filter(id => !layerIds.includes(id));
                    if (missingIds.length > 0) {
                        console.warn('SSE: Missing polygon IDs on layer:', missingIds);
                    }
                }

                if (layer && polygonManagement.zoomTo) {
                    console.log('SSE: Using zoomTo with layer and unit strings:', unitStrings);

                    // Use the proper zoomTo signature: (map, ids, layer)
                    polygonManagement.zoomTo(map, unitStrings, layer);

                    // Only update unit types if we actually have units to zoom to
                    console.log('SSE: Zoom initiated for units:', unitStrings);
                    if (unitTypes.length > 0 && unitStrings.length > 0) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        console.log('SSE: Now switching to unit types:', uniqueUnitTypes);

                        // Add delay to ensure zoom completes before unit type change
                        setTimeout(() => {
                            window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                        }, 500);
                    } else {
                        console.log('SSE: No units to zoom to, skipping unit type update');
                    }
                } else {
                    console.log('SSE: No layer or zoom function available, just updating unit types');
                    // Fallback: just update unit types
                    if (unitTypes.length > 0) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                    }
                }
            }).catch(err => {
                console.error('SSE: Failed to fetch polygons:', err);
                // Fallback: try to zoom anyway and update unit types
                window.pureMapState.executeWorkflowCommand({
                    type: 'zoom_to_selection'
                });
                if (unitTypes.length > 0) {
                    const uniqueUnitTypes = [...new Set(unitTypes)];
                    window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
                }
            });
        } else if (unitTypes.length > 0) {
            // No polygon fetching available, just update unit types
            const uniqueUnitTypes = [...new Set(unitTypes)];
            console.log('SSE: No polygon management available, just updating unit types:', uniqueUnitTypes);
            window.pureMapState.userSetUnitTypes(uniqueUnitTypes, false);
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
                messageDiv.textContent = content;
                chatDisplay.appendChild(messageDiv);
            }
        }

        // Re-enable send button after update
        const sendButton = document.getElementById('send-button');
        if (sendButton) {
            sendButton.disabled = false;
        }
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
