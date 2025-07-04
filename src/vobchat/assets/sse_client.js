// src/vobchat/assets/sse_client.js

// Helper functions to extract data from places array (single source of truth)
function getSelectedUnits(state) {
    const places = state.places || [];
    return places.map(place => place.g_unit).filter(unit => unit !== null && unit !== undefined);
}

function getSelectedUnitTypes(state) {
    const places = state.places || [];
    return places.map(place => place.g_unit_type).filter(type => type !== null && type !== undefined);
}

function getSelectedPlaceNames(state) {
    const places = state.places || [];
    return places.map(place => place.name).filter(name => name !== null && name !== undefined);
}

class WorkflowSSEClient {
    // Store the latest interrupt so we know the current options/node for user input
    setLatestInterrupt(interruptData) {
        this.latestInterruptData = interruptData;
    }
    constructor() {
        this.eventSource = null;
        this.threadId = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // Start with 1 second

        // Event handlers
        this.onMessage = null;
        this.onInterrupt = null;
        this.onStateUpdate = null;
        this.onError = null;

        // Track streamed message UUIDs across entire connection to prevent duplicates
        this.streamedMessageIds = new Set();

        // Frontend logging
        this.logBuffer = [];
        this.setupFrontendLogging();
        this.onConnected = null;
        this.onDisconnected = null;

        // Set up periodic log saving (every 5 seconds)
        this.logSaveInterval = setInterval(() => {
            this.saveLogsToFile();
        }, 5000);
    }

    setupFrontendLogging() {
        // Override console methods to capture logs
        const originalLog = console.log;
        const originalDebug = console.debug;
        const originalError = console.error;

        console.log = (...args) => {
            this.logToFile('LOG', ...args);
            originalLog.apply(console, args);
        };

        console.debug = (...args) => {
            this.logToFile('DEBUG', ...args);
            originalDebug.apply(console, args);
        };

        console.error = (...args) => {
            this.logToFile('ERROR', ...args);
            originalError.apply(console, args);
        };
    }

    logToFile(level, ...args) {
        const timestamp = new Date().toISOString();
        const message = args.map(arg =>
            typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
        ).join(' ');

        const logEntry = `${timestamp} [${level}] ${message}\n`;
        this.logBuffer.push(logEntry);

        // Keep only last 1000 entries to prevent memory issues
        if (this.logBuffer.length > 1000) {
            this.logBuffer = this.logBuffer.slice(-1000);
        }
    }

    saveLogsToFile() {
        if (this.logBuffer.length === 0) return;

        // Send logs to backend to save to file
        try {
            const logContent = this.logBuffer.join('');
            fetch('/api/save-frontend-logs', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ logs: logContent })
            }).catch(err => {
                // Silently fail - don't use console.error to avoid recursion
                // Backend will handle logging errors
            });

            // Clear buffer after sending
            this.logBuffer = [];
        } catch (error) {
            // Silently fail to avoid recursion
        }
    }

    connect(threadId, workflowInput = null) {
        if (this.isConnected && this.threadId === threadId) {
            console.log('SSE: Already connected to thread', threadId);
            return;
        }

        this.disconnect(); // Close existing connection

        // Clear tracked message IDs for new connection
        this.streamedMessageIds.clear();
        console.log('SSE: Cleared tracked message IDs for new connection');

        this.threadId = threadId;
        console.log('SSE: Connecting to thread', threadId, 'with workflow input:', workflowInput);

        let url = `/api/sse/connect?thread_id=${encodeURIComponent(threadId)}`;

        // Add workflow input if provided
        if (workflowInput) {
            const encodedInput = encodeURIComponent(JSON.stringify(workflowInput));
            url += `&workflow_input=${encodedInput}`;
            console.log('SSE: Including workflow input in connection URL:', url);
        } else {
            console.log('SSE: No workflow input provided');
        }

        console.log('SSE: Final connection URL:', url);
        this.eventSource = new EventSource(url);

        this.eventSource.onopen = (event) => {
            console.log('SSE: Connection opened', event);
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.reconnectDelay = 1000;

            if (this.onConnected) {
                this.onConnected(threadId);
            } else {
                console.log('SSE: Connection opened for thread', threadId, '- no onConnected handler');
            }
        };

        this.eventSource.addEventListener('connected', (event) => {
            console.log('SSE: Received connected event', event.data);
        });

        this.eventSource.addEventListener('message', (event) => {
            try {
                const data = JSON.parse(event.data);
                console.log('SSE: Received message event', data);

                if (this.onMessage) {
                    this.onMessage(data.content, data.is_partial, data.message_id);
                }
            } catch (e) {
                console.error('SSE: Error parsing message event', e);
            }
        });

        this.eventSource.addEventListener('interrupt', (event) => {
            try {
                const data = JSON.parse(event.data);
                console.log('SSE: Received interrupt event', data);

                if (this.onInterrupt) {
                    this.onInterrupt(data.data);
                }
            } catch (e) {
                console.error('SSE: Error parsing interrupt event', e);
            }
        });

        this.eventSource.addEventListener('state_update', (event) => {
            try {
                const data = JSON.parse(event.data);
                console.log('SSE: Received state update event', data);

                if (this.onStateUpdate) {
                    this.onStateUpdate(data.data);
                }
            } catch (e) {
                console.error('SSE: Error parsing state update event', e);
            }
        });

        this.eventSource.addEventListener('error', (event) => {
            try {
                const data = JSON.parse(event.data);
                console.error('SSE: Received error event', data);

                if (this.onError) {
                    this.onError(data.error);
                }
            } catch (e) {
                console.error('SSE: Error parsing error event', e);
            }
        });

        this.eventSource.onerror = (event) => {
            console.error('SSE: Connection error', event);
            this.isConnected = false;

            if (this.onDisconnected) {
                this.onDisconnected();
            } else {
                console.log('SSE: Connection error - no onDisconnected handler');
            }

            // Attempt to reconnect
            this.attemptReconnect();
        };
    }

    disconnect() {
        if (this.eventSource) {
            console.log('SSE: Disconnecting');
            this.eventSource.close();
            this.eventSource = null;
        }

        this.isConnected = false;
        this.threadId = null;
        
        // Clear tracked message IDs when disconnecting
        this.streamedMessageIds.clear();
        console.log('SSE: Cleared tracked message IDs on disconnect');
    }

    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('SSE: Max reconnect attempts reached');
            return;
        }

        this.reconnectAttempts++;
        console.log(`SSE: Attempting reconnect ${this.reconnectAttempts}/${this.maxReconnectAttempts} in ${this.reconnectDelay}ms`);

        setTimeout(() => {
            if (this.threadId) {
                this.connect(this.threadId);
            }
        }, this.reconnectDelay);

        // Exponential backoff
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000); // Max 30 seconds
    }

    sendUserInput(inputData) {
        // Send user input via regular HTTP request since SSE is one-way
        // Merge in the latest interrupt context for selection_idx/button clicks
        let payload = { ...inputData };
        if (this.latestInterruptData && (inputData.selection_idx !== undefined)) {
            // Always add current_node and options from latest interrupt for button clicks
            if (this.latestInterruptData.current_node) {
                payload.current_node = this.latestInterruptData.current_node;
            }
            if (Array.isArray(this.latestInterruptData.options)) {
                payload.options = this.latestInterruptData.options;
            }
            // Add current_place_index to track which place this selection belongs to
            if (this.latestInterruptData.current_place_index !== undefined) {
                payload.current_place_index = this.latestInterruptData.current_place_index;
            }
        }
        return fetch('/api/workflow/input', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                thread_id: this.threadId,
                input_data: payload
            })
        });
    }

    startWorkflow(workflowInput) {
        // Start new workflow execution
        return fetch('/api/workflow/start', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                thread_id: this.threadId,
                workflow_input: workflowInput
            })
        });
    }
}

// Create global SSE client instance
window.workflowSSE = new WorkflowSSEClient();

// Removed polygon click tracking - handled by pure map state now

// Set up connection event handlers
window.workflowSSE.onConnected = function(threadId) {
    console.log('SSE: Successfully connected to thread', threadId);
    // Connection is established, workflow can now start
};

window.workflowSSE.onDisconnected = function() {
    console.log('SSE: Disconnected from server');
    // Try to reconnect if we have a thread ID
    if (window.workflowSSE.threadId) {
        console.log('SSE: Attempting reconnect in 2 seconds');
        setTimeout(() => {
            if (window.workflowSSE.threadId) {
                window.workflowSSE.connect(window.workflowSSE.threadId);
            }
        }, 2000);
    }
};

// Integration with Dash callbacks
window.workflowSSE.onMessage = function(content, isPartial, messageId) {
    console.log('SSE: Received message', content, 'isPartial:', isPartial, 'messageId:', messageId);

    // CRITICAL FIX: Skip duplicate messages using UUID tracking
    // This prevents the same message from being displayed multiple times across workflow executions
    if (messageId && window.workflowSSE.streamedMessageIds.has(messageId)) {
        console.log('SSE: Skipping duplicate message with ID', messageId, ':', content);
        return;
    }

    // Track this message ID to prevent future duplicates
    if (messageId) {
        window.workflowSSE.streamedMessageIds.add(messageId);
        console.log('SSE: Tracked message ID', messageId, '- total tracked:', window.workflowSSE.streamedMessageIds.size);
    }

    if (isPartial) {
        // For partial messages, show real-time streaming
        const chatDisplay = document.getElementById('chat-display');
        if (!chatDisplay) {
            console.warn('SSE: chat-display element not found');
            return;
        }

        // Find existing streaming message or create new one
        let messageDiv = chatDisplay.querySelector('.ai-bubble.streaming');

        if (!messageDiv) {
            // Create new AI bubble for partial message
            messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble streaming';
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);
        }
        messageDiv.textContent = content;
    } else {
        // For complete messages, try to add to app-state via Dash
        console.log('SSE: Complete message received, attempting to add to app-state');

        // Remove streaming message
        const chatDisplay = document.getElementById('chat-display');
        if (chatDisplay) {
            const streamingDiv = chatDisplay.querySelector('.ai-bubble.streaming');
            if (streamingDiv) {
                streamingDiv.remove();
            }
        }

        // Use direct DOM manipulation instead of trying to update app-state
        // This avoids the issue where we can't read current app-state values
        console.log('SSE: Adding AI message directly to DOM to avoid app-state conflicts');

        if (chatDisplay) {
            const messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble';
            messageDiv.textContent = content;
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);
            console.log('SSE: AI message added to chat display via DOM');
        } else {
            console.error('SSE: Chat display element not found');
        }
    }
};

window.workflowSSE.onInterrupt = function(interruptData) {
    console.log('SSE: Processing interrupt', interruptData);

    // Track interrupt and persist so button clicks can send full context
    window.workflowSSE._lastInterruptTime = Date.now();
    window.workflowSSE.setLatestInterrupt(interruptData);

    // Handle different types of interrupts
    if (interruptData.options) {
        // Multi-choice interrupt - render buttons
        console.log('SSE: Rendering buttons for interrupt with', interruptData.options.length, 'options');
        renderInterruptButtons(interruptData);
    } else {
        // No options - clear any existing buttons
        console.log('SSE: No options in interrupt - clearing buttons');
        try {
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                dash_clientside.set_props('options-container', {children: []});
                console.log('SSE: Cleared options container');
            }
        } catch (e) {
            console.error('Could not clear options container', e);
        }
    }

    // CRITICAL: Handle PureMapState synchronization for interrupts with polygon data
    // This ensures interrupts properly clear or set polygon selections just like state updates
    // Use helper function to get units from places array
    const selectedUnits = getSelectedUnits(interruptData) || [];
    console.log('SSE: Interrupt selectedUnits extracted:', selectedUnits);
    console.log('SSE: Interrupt places array:', interruptData.places);
    
    // Handle map_update_request if present
    if (interruptData.map_update_request && interruptData.map_update_request.action === 'update_map_selection') {
        console.log('SSE: Processing map update request from interrupt:', interruptData.map_update_request);
        const request = interruptData.map_update_request;
        
        if (window.pureMapState && request.places) {
            // Extract units from places array
            const units = getSelectedUnits({places: request.places});
            console.log('SSE: Extracted units from map update request:', units);
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: { selectedPolygons: units }
            });
            console.log('SSE: Synced map state via map_update_request');
        }
    } else if (selectedUnits.length > 0) {
        // Fallback to direct sync if no map_update_request
        const polygons = selectedUnits;
        if (window.pureMapState) {
            console.log('SSE: Syncing workflow polygons to pure map state (fallback):', polygons);
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: { selectedPolygons: polygons }
            });
            console.log('SSE: Sent map sync command to pure map state (fallback)');
        }
    } else {
        console.log('SSE: No map updates needed from interrupt');
    }

    // CRITICAL: Check if map_update_request was explicitly cleared in interrupt data
    if (interruptData.map_update_request === null) {
        console.log('SSE: Map update request explicitly cleared in interrupt - ensuring no stale map updates');
    }

    // CRITICAL: Always update visualization state based on interrupt data
    // This handles both data delivery (when cubes present) and removal (when units empty)
    if (interruptData.cubes || interruptData.places !== undefined) {
        try {
            updateVisualizationFromInterrupt(interruptData);
            console.log('SSE: Visualization update completed successfully');
        } catch (e) {
            console.error('SSE: Error in updateVisualizationFromInterrupt:', e);
        }
    }

    console.log('SSE: About to process interrupt message');

    // Process interrupt message for chat display
    let message = interruptData.message;
    console.log('SSE: Initial message from interrupt:', message);

    // Handle specific node cases
    if (interruptData.current_node === 'select_unit_on_map') {
        console.log('SSE: Processing select_unit_on_map node');
        // Don't override removal messages or other explicit messages
        if (!message || (!message.includes('Removed') && !message.includes('removed'))) {
            // CRITICAL: For unit type selections, don't ask user to select on map again
            // Check if this is a unit type button selection result
            const hasSelectedUnits = getSelectedUnits(interruptData).length > 0;
            const hasUnitTypes = getSelectedUnitTypes(interruptData).length > 0;

            if (hasSelectedUnits && hasUnitTypes && message && message.includes('Using')) {
                // Keep the existing message that shows unit type selection result
                console.log('SSE: Keeping unit type selection message:', message);
            } else if (hasSelectedUnits && hasUnitTypes) {
                // Show that we're proceeding with the selection
                const placeNames = getSelectedPlaceNames(interruptData);
                const unitTypes = getSelectedUnitTypes(interruptData);
                const placeName = placeNames[0] || 'the area';
                const unitType = unitTypes[0];
                // message = `Using ${unitType} data for ${placeName}.`;
            } else {
                const placeNames = getSelectedPlaceNames(interruptData);
                const placeName = placeNames[0] || 'the place';
                // message = `I found "${placeName}". Please select it on the map to continue.`;
            }
        }
        console.log('SSE: select_unit_on_map message after processing:', message);
    } else if (!message && interruptData.current_node) {
        message = `Please make a selection to continue.`;
    }

    // UNIFIED MESSAGE HANDLING: Route all messages through Dash app-state instead of direct DOM
    console.log('SSE: About to add message to chat:', message);
    if (message) {

        // UNIFIED FIX: Use direct DOM manipulation for all messages to ensure consistent ordering
        // This ensures all messages follow the same insertion order and timing
        const chatDisplay = document.getElementById('chat-display');
        if (chatDisplay) {
            const messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble';
            messageDiv.textContent = message;
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);  // Add to top in correct order
            console.log('SSE: Added interrupt message via direct DOM (unified with regular messages)');
        } else {
            console.error('SSE: Chat display element not found for interrupt message');
        }
    } else {
        console.log('SSE: No message to add to chat');
    }
};

window.workflowSSE.onStateUpdate = function(stateData) {
    console.log('SSE: Processing state update', stateData);

    // CRITICAL: Update Dash stores using proper clientside callback mechanism
    // Trigger store updates by updating the sse-event-processor store that clientside callback monitors
    if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
        dash_clientside.set_props('sse-event-processor', {
            data: {
                timestamp: Date.now(),
                hasUpdate: true,
                stateData: stateData  // Include the data directly
            }
        });
        console.log('SSE: Triggered store update via sse-event-processor');
    }

    // NEW ARCHITECTURE: SSE no longer handles map state directly
    // Instead, send commands to the pure map state manager

    // Handle workflow commands for map state - extract polygons from places array
    const polygons = getSelectedUnits(stateData) || stateData.selected_polygons || [];
    if (polygons.length >= 0) {
        if (window.pureMapState) {
            if (polygons.length > 0) {
                console.log('SSE: Syncing workflow polygons to pure map state:', polygons);
                window.pureMapState.executeWorkflowCommand({
                    type: 'sync_state',
                    state: { selectedPolygons: polygons }
                });

                // Trigger zoom if requested
                if (stateData.zoom_to_selection || polygons.length > 0) {
                    window.pureMapState.executeWorkflowCommand({
                        type: 'zoom_to_selection'
                    });
                }
            } else {
                // CRITICAL FIX: Only clear if this is an intentional clear operation
                // Check if the current user state has selections that shouldn't be cleared
                const currentUserState = window.pureMapState.getUserState();
                const hasUserSelections = currentUserState.selectedPolygons.length > 0;

                // Only clear if there's an explicit clear signal or if it's a Reset operation
                const isExplicitClear = stateData.action === 'clear' || stateData.reset === true;

                if (isExplicitClear || !hasUserSelections) {
                    console.log('SSE: Clearing selections - explicit clear or no user selections');
                    window.pureMapState.executeWorkflowCommand({
                        type: 'clear_selection'
                    });
                } else {
                    console.log('SSE: Ignoring empty state update - preserving user selections:', currentUserState.selectedPolygons);
                }
            }
        }
    }

    // Handle map update requests
    if (stateData.map_update_request && stateData.map_update_request.action === 'update_map_selection') {
        console.log('SSE: Processing map update request from state:', stateData.map_update_request);
        const request = stateData.map_update_request;

        if (window.pureMapState && request.places) {
            // Extract units from places array
            const units = getSelectedUnits({places: request.places});
            window.pureMapState.executeWorkflowCommand({
                type: 'sync_state',
                state: { selectedPolygons: units }
            });
        }
    } else if (stateData.map_update_request === null) {
        // CRITICAL: Handle explicit clearing of map_update_request (e.g., from RemovePlace)
        console.log('SSE: Map update request explicitly cleared - no additional sync needed');
    }
};

window.workflowSSE.onError = function(error) {
    console.error('SSE: Workflow error', error);

    // Show error in chat using direct DOM manipulation
    const chatDisplay = document.getElementById('chat-display');
    if (chatDisplay) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'speech-bubble ai-bubble';
        errorDiv.style.color = 'red';
        errorDiv.textContent = `Error: ${error}`;
        chatDisplay.insertBefore(errorDiv, chatDisplay.firstChild);
    }
};

// Helper functions
function renderInterruptButtons(interruptData) {
    console.log('SSE: Rendering interrupt buttons', interruptData);

    const options = interruptData.options || [];

    const buttons = options.map(opt => {
        // Create button object in the format Dash expects
        return {
            props: {
                children: opt.label,
                id: {
                    option_type: opt.option_type,
                    type: 'dynamic-button-user-choice',
                    index: opt.value
                },
                className: 'unit-filter-button me-2 mb-2',
                style: {
                    '--unit-color': opt.color || '#333',
                    borderColor: opt.color || '#333',
                    backgroundColor: 'white',
                    color: opt.color || '#333'
                }
            },
            type: 'Button',
            namespace: 'dash_bootstrap_components'
        };
    });

    try {
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            dash_clientside.set_props('options-container', {children: buttons});
            console.log('SSE: Rendered interrupt buttons:', buttons.length);
        } else {
            console.warn('SSE: dash_clientside not available for button rendering');
        }
    } catch (e) {
        console.error('Could not render interrupt buttons', e);
    }
}

function updateMapFromInterrupt(interruptData) {
    // Use helper functions to extract data from places array
    const selectedUnits = getSelectedUnits(interruptData) || [];
    const selectedUnitTypes = getSelectedUnitTypes(interruptData) || [];
    
    const mapUpdates = {
        selected_polygons: selectedUnits.map(String),
        selected_polygons_unit_types: selectedUnitTypes,
        zoom_to_selection: selectedUnits.length > 0  // Only zoom if there are selections
    };

    // CRITICAL: Also update unit_types when unit types change from workflow
    // This ensures the unit filter buttons reflect the new selection
    if (selectedUnitTypes.length > 0) {
        // Get unique unit types from the selection
        const uniqueUnitTypes = [...new Set(selectedUnitTypes)];
        mapUpdates.unit_types = uniqueUnitTypes;
        console.log('SSE: Updating unit_types from interrupt to:', uniqueUnitTypes);
    }

    console.log('SSE: Updating map from interrupt with:', mapUpdates);

    try {
        // REMOVED: Broken document.querySelector approach - Dash stores aren't DOM elements
        // Cannot read current state, so just use the updates directly
        const newMapState = {...mapUpdates};

        // CRITICAL: Check if unit types are changing to trigger immediate map refresh
        const newUnitTypes = newMapState.unit_types || [];
        // Cannot compare with old state since we can't read Dash stores, assume changed if present
        const unitTypesChanged = newUnitTypes.length > 0;

        // Update map state using Dash's set_props
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            dash_clientside.set_props('map-state', {data: newMapState});
            console.log('SSE: Updated map state from interrupt:', mapUpdates);

                /*
                 * In multi-place workflows the first place is often processed very
                 * quickly – faster than the map infrastructure (leaflet instance
                 * + polygon_management helpers) finishes initialising.  The
                 * set_props call above therefore fires the zoom-to-selection flow
                 * (Callback #8) while `window.polygon_management` or
                 * `window.geojsonLayerReady` may still be undefined, causing
                 * Callback #8 to bail out and clear the zoom flag.  As a result the
                 * polygon is never fetched / highlighted and the user doesn’t see
                 * the first place on the map.
                 *
                 * To make the workflow robust we add a lightweight safety-net:
                 *   1.  After successfully writing to map-state we check whether
                 *       the mapping helpers are ready **right now**.  If they are
                 *       we proactively fetch/zoom the polygons immediately – the
                 *       normal Callback #8 will still run but will detect the
                 *       polygons are already present and no-op.
                 *   2.  If the helpers are **not** ready we schedule a single
                 *       retry after 500 ms.  This is long enough for the map to
                 *       finish initialising on most machines but short enough that
                 *       the user still perceives the highlight as instantaneous.
                 */

                const attemptImmediateHighlight = (attempt = 0) => {
                    try {
                        const mapElement = document.getElementById('leaflet-map');
                        const map = mapElement?._leaflet_map;
                        const pm = window.polygon_management;

                        if (!map || !pm || !pm.fetchPolygonsByIds || !pm.zoomTo || !window.geojsonLayerReady) {
                            // Map infrastructure not ready – retry once after a short delay
                            if (attempt === 0) {
                                setTimeout(() => attemptImmediateHighlight(1), 500);
                            }
                            return;
                        }

                        const idsToFetch = (newMapState.selected_polygons || []).map(String);
                        if (idsToFetch.length === 0) return; // Nothing to do

                        const unitTypes = newMapState.selected_polygons_unit_types || newMapState.unit_types || [];
                        const unitType = unitTypes.length > 0 ? unitTypes[0] : null;
                        if (!unitType) return;

                        // Fetch polygons then zoom – mirror logic from Callback #8
                        pm.fetchPolygonsByIds(map, newMapState, unitType, idsToFetch, null, idsToFetch)
                            .then(() => {
                                const layer = pm.findGeoJSONLayer(map);
                                if (layer) {
                                    pm.zoomTo(map, idsToFetch, layer);
                                    pm.refreshLayerStyles(layer, idsToFetch);
                                }
                            })
                            .catch(err => console.error('SSE: Fallback polygon fetch failed:', err));
                    } catch (e) {
                        console.error('SSE: Error in immediate highlight attempt:', e);
                    }
                };

                attemptImmediateHighlight();

            // CRITICAL FIX: If unit types changed, coordinate with Callback #8 for proper polygon fetching
            if (unitTypesChanged) {
                console.log('SSE: Unit types changed from', oldUnitTypes, 'to', newUnitTypes, '- coordinating with Callback #8 for polygon refresh');

                // If there are selected polygons, let Callback #8 handle the refresh via zoom_to_selection flag
                // Callback #8 will fetch the selected polygons for the new unit type and then refresh the map
                if (newMapState.selected_polygons && newMapState.selected_polygons.length > 0 && newMapState.zoom_to_selection) {
                    console.log('SSE: Selected polygons exist - letting Callback #8 handle unit type change refresh');
                    // No immediate refresh needed - Callback #8 will handle it via fetchPolygonsByIds
                } else {
                    // No selected polygons - refresh immediately to clear old polygons
                    setTimeout(() => {
                        const mapElement = document.getElementById('leaflet-map');
                        const map = mapElement?._leaflet_map;

                        if (map && window.polygon_management && window.polygon_management.updateMapWithBounds && window.geojsonLayerReady) {
                            const bounds = map.getBounds();
                            const yearRange = newMapState.year_range ? { min: newMapState.year_range[0], max: newMapState.year_range[1] } : null;

                            console.log('SSE: No selected polygons - calling updateMapWithBounds for unit type change');
                            window.polygon_management.updateMapWithBounds(map, newUnitTypes, bounds, newMapState, yearRange)
                                .then(() => {
                                    console.log('SSE: Map refresh completed for unit type change with no selections');
                                })
                                .catch(error => {
                                    console.error('SSE: Error refreshing map for unit type change:', error);
                                });
                        } else {
                            console.warn('SSE: Cannot refresh map - prerequisites not met');
                        }
                    }, 100); // Small delay to ensure map state update has propagated
                }
            }
        } else {
            console.warn('SSE: dash_clientside not available for map state update');
        }
    } catch (e) {
        console.error('Could not update map state from interrupt', e);
    }
}

function updateVisualizationFromInterrupt(interruptData) {
    try {
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            // REMOVED: Broken document.querySelector approach
            // Now handled by proper clientside callback via sse-event-processor

            // Use interrupt data for selected units - get from places array
            // If interrupt has empty units array, it means units were removed
            const selectedUnits = getSelectedUnits(interruptData) || [];

            console.log('SSE: Updating visualization with interrupt data - selected units:', selectedUnits);
            console.log('SSE: Interrupt has cubes:', !!interruptData.cubes);

            // Determine the new place state data based on interrupt
            let newPlaceState;
            if (selectedUnits.length === 0) {
                // Complete removal case - clear all visualization data
                console.log('SSE: Clearing visualization data (complete removal detected)');
                newPlaceState = {
                    cubes: null,
                    cube_data: null,
                    places: []  // Clear places array as single source of truth
                };
            } else {
                // Partial removal or addition case - use cube data from interrupt
                console.log('SSE: Updating visualization data (partial removal or addition)');
                newPlaceState = {
                    cubes: interruptData.cubes,
                    cube_data: interruptData.cube_data,
                    places: interruptData.places || []  // Store places array as single source of truth
                };
            }

            // Update place state
            dash_clientside.set_props('place-state', {
                data: newPlaceState
            });

            // Get current app state and update visualization flag
        }
    } catch (e) {
        console.error('Could not update visualization from interrupt', e);
    }
}

function updateMapState(stateData) {
    try {
        // REMOVED: Broken document.querySelector approach for Dash stores
        // Dash stores cannot be accessed via DOM - this was always failing silently

        const updates = {};
        // Extract data from places array using helper functions
        const selectedUnits = getSelectedUnits(stateData) || [];
        const selectedUnitTypes = getSelectedUnitTypes(stateData) || [];
        
        if (selectedUnits.length > 0) {
            updates.selected_polygons = selectedUnits.map(String);
        }
        if (selectedUnitTypes.length > 0) {
            updates.selected_polygons_unit_types = selectedUnitTypes;

            // CRITICAL: Also update unit_types when unit types change from state updates
            // This ensures consistency between workflow state and map filter state
            const uniqueUnitTypes = [...new Set(selectedUnitTypes)];
            updates.unit_types = uniqueUnitTypes;
            console.log('SSE: Updating unit_types from state update to:', uniqueUnitTypes);
        }

        // CRITICAL: Add zoom flag if present in state data
        if (stateData.zoom_to_selection !== undefined) {
            updates.zoom_to_selection = stateData.zoom_to_selection;
            console.log('SSE: Setting zoom_to_selection from state update:', stateData.zoom_to_selection);
        }

        if (Object.keys(updates).length > 0) {
            const newMapState = {...updates};

            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                dash_clientside.set_props('map-state', {data: newMapState});
                console.log('SSE: Updated map state from state update:', updates);
            } else {
                console.warn('SSE: dash_clientside not available for map state update');
            }
        }
    } catch (e) {
        console.error('Could not update map state', e);
    }
}

console.log('SSE Client loaded and ready');

// Add debugging to window object
window.SSE_DEBUG = true;
console.log('SSE: workflowSSE client created:', !!window.workflowSSE);

// REMOVED: trySSEAutoConnect() function because document.querySelector() 
// cannot access Dash stores. SSE connection is now handled via proper 
// clientside callbacks that have access to store data.

// REMOVED: setupSSEMonitoring() function because document.querySelector() 
// cannot access Dash stores. Store monitoring is now handled via proper 
// clientside callbacks that have access to store data.

// REMOVED: Auto-connection logic because trySSEAutoConnect() and setupSSEMonitoring() 
// used broken document.querySelector() approach. SSE connection is now handled 
// via proper clientside callbacks.
