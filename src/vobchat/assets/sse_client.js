// src/vobchat/assets/sse_client.js

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
        this.onConnected = null;
        this.onDisconnected = null;
    }
    
    connect(threadId) {
        if (this.isConnected && this.threadId === threadId) {
            console.log('SSE: Already connected to thread', threadId);
            return;
        }
        
        this.disconnect(); // Close existing connection
        
        this.threadId = threadId;
        console.log('SSE: Connecting to thread', threadId);
        
        const url = `/api/sse/connect?thread_id=${encodeURIComponent(threadId)}`;
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
                    this.onMessage(data.content, data.is_partial);
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
window.workflowSSE.onMessage = function(content, isPartial) {
    console.log('SSE: Received message', content, 'isPartial:', isPartial);
    
    // Update chat display directly
    const chatDisplay = document.getElementById('chat-display');
    if (!chatDisplay) {
        console.warn('SSE: chat-display element not found');
        return;
    }
    
    // Create or update AI message bubble for streaming
    // Use a specific class to identify streaming messages vs interrupt messages
    let messageDiv = chatDisplay.querySelector('.ai-bubble.streaming:first-child');
    
    if (isPartial) {
        if (!messageDiv) {
            // Create new AI bubble for partial message
            messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble streaming';
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);
        }
        messageDiv.textContent = content;
    } else {
        // Complete message
        if (messageDiv) {
            messageDiv.textContent = content;
            // Remove streaming class since message is complete
            messageDiv.classList.remove('streaming');
        } else {
            // Create new complete message
            messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble';
            messageDiv.textContent = content;
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);
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
    
    if (interruptData.current_node === 'select_unit_on_map') {
        // Map selection interrupt - update map state
        try {
            updateMapFromInterrupt(interruptData);
            console.log('SSE: Map update completed successfully');
        } catch (e) {
            console.error('SSE: Error in updateMapFromInterrupt:', e);
        }
    }
    
    // CRITICAL: Always update visualization state based on interrupt data
    // This handles both data delivery (when cubes present) and removal (when units empty)
    if (interruptData.cubes || interruptData.selected_place_g_units !== undefined) {
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
            const placeName = interruptData.extracted_place_names?.[0] || 'the place';
            message = `I found "${placeName}". Please select it on the map to continue.`;
        }
        console.log('SSE: select_unit_on_map message after processing:', message);
    } else if (!message && interruptData.current_node) {
        message = `Please make a selection to continue.`;
    }
    
    // Add interrupt message to chat using direct DOM manipulation
    console.log('SSE: About to add message to chat:', message);
    if (message) {
        const chatDisplay = document.getElementById('chat-display');
        if (chatDisplay) {
            const messageDiv = document.createElement('div');
            messageDiv.className = 'speech-bubble ai-bubble';
            messageDiv.textContent = message;
            // Insert at the top but don't overwrite - let messages stack
            chatDisplay.insertBefore(messageDiv, chatDisplay.firstChild);
            console.log('SSE: Added interrupt message to chat:', message);
        } else {
            console.warn('SSE: chat-display element not found for interrupt message');
        }
    } else {
        console.log('SSE: No message to add to chat');
    }
};

window.workflowSSE.onStateUpdate = function(stateData) {
    console.log('SSE: Processing state update', stateData);
    
    // CRITICAL: Check if this state update contradicts recent interrupt data
    // If interrupt data indicates removal (empty arrays) but state update has data,
    // prioritize the interrupt data as it represents the most recent workflow action
    if (window.workflowSSE.latestInterruptData) {
        const interruptUnits = window.workflowSSE.latestInterruptData.selected_place_g_units || [];
        const stateUnits = stateData.selected_place_g_units || [];
        
        // If interrupt shows empty selection but state update has units, it's likely stale
        if (interruptUnits.length === 0 && stateUnits.length > 0) {
            console.log('SSE: Ignoring stale state update - interrupt shows removal but state has units');
            console.log('SSE: Interrupt units:', interruptUnits, 'State units:', stateUnits);
            return;
        }
        
        // If interrupt shows different units than state, prefer interrupt (more recent workflow action)
        if (interruptUnits.length > 0 && stateUnits.length > 0 && 
            JSON.stringify(interruptUnits.sort()) !== JSON.stringify(stateUnits.sort())) {
            console.log('SSE: Ignoring conflicting state update - interrupt and state show different units');
            console.log('SSE: Interrupt units:', interruptUnits, 'State units:', stateUnits);
            return;
        }
    }
    
    // Update relevant Dash components based on state changes
    if (stateData.selected_place_g_units !== undefined || stateData.selected_polygons !== undefined) {
        updateMapState(stateData);
    }
    
    if (stateData.show_visualization !== undefined) {
        try {
            // Get current app state from the store element directly
            const appStateStore = document.querySelector('#app-state');
            const currentAppState = (appStateStore && appStateStore._dash_value && appStateStore._dash_value.data) || {};
            
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                dash_clientside.set_props('app-state', {
                    data: {
                        ...currentAppState,
                        show_visualization: stateData.show_visualization
                    }
                });
                console.log('SSE: Updated app state show_visualization:', stateData.show_visualization);
            }
        } catch (e) {
            console.error('Could not update app state from state update', e);
        }
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
    const mapUpdates = {
        selected_polygons: interruptData.selected_place_g_units?.map(String) || [],
        selected_polygons_unit_types: interruptData.selected_place_g_unit_types || [],
        zoom_to_selection: interruptData.selected_place_g_units?.length > 0  // Only zoom if there are selections
    };
    
    console.log('SSE: Updating map from interrupt with:', mapUpdates);
    
    try {
        // Get current map state from the store element directly
        const mapStateStore = document.querySelector('#map-state');
        const currentMapState = (mapStateStore && mapStateStore._dash_value && mapStateStore._dash_value.data) || {};
        const newMapState = {...currentMapState, ...mapUpdates};
        
        // Update map state using Dash's set_props
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            dash_clientside.set_props('map-state', {data: newMapState});
            console.log('SSE: Updated map state from interrupt:', mapUpdates);
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
            // Get current place state
            const placeStateStore = document.querySelector('#place-state');
            const currentPlaceState = (placeStateStore && placeStateStore._dash_value && placeStateStore._dash_value.data) || {};
            
            // Use interrupt data for selected units - this is authoritative
            // If interrupt has empty units array, it means units were removed
            const selectedUnits = interruptData.selected_place_g_units || [];
            
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
                    selected_place_g_units: []
                };
            } else {
                // Partial removal or addition case - use filtered cube data from interrupt
                console.log('SSE: Updating visualization data (partial removal or addition)');
                newPlaceState = {
                    cubes: interruptData.cubes || currentPlaceState.cubes,
                    cube_data: interruptData.cubes || currentPlaceState.cube_data,
                    selected_place_g_units: selectedUnits
                };
            }
            
            // Update place state
            dash_clientside.set_props('place-state', {
                data: newPlaceState
            });
            
            // Get current app state and update visualization flag
            const appStateStore = document.querySelector('#app-state');
            const currentAppState = (appStateStore && appStateStore._dash_value && appStateStore._dash_value.data) || {};
            
            // Use interrupt's show_visualization flag if provided, otherwise determine based on data
            const shouldShowVisualization = interruptData.show_visualization !== undefined 
                ? interruptData.show_visualization 
                : !!(newPlaceState.cubes && selectedUnits.length > 0);
            
            dash_clientside.set_props('app-state', {
                data: {
                    ...currentAppState,
                    show_visualization: shouldShowVisualization
                }
            });
            
            console.log('SSE: Updated visualization from interrupt - show_visualization:', shouldShowVisualization);
        } else {
            console.warn('SSE: dash_clientside not available for visualization update');
        }
    } catch (e) {
        console.error('Could not update visualization from interrupt', e);
    }
}

function updateMapState(stateData) {
    try {
        // Get current map state from the store element directly
        const mapStateStore = document.querySelector('#map-state');
        const currentMapState = (mapStateStore && mapStateStore._dash_value && mapStateStore._dash_value.data) || {};
        
        const updates = {};
        if (stateData.selected_place_g_units) {
            updates.selected_polygons = stateData.selected_place_g_units.map(String);
        }
        if (stateData.selected_place_g_unit_types) {
            updates.selected_polygons_unit_types = stateData.selected_place_g_unit_types;
        }
        
        if (Object.keys(updates).length > 0) {
            const newMapState = {...currentMapState, ...updates};
            
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

// Auto-connect when a thread ID becomes available or SSE connection is triggered
function trySSEAutoConnect() {
    // Check for connect_sse signal first
    try {
        const sseConnectionStatus = document.querySelector('#sse-connection-status');
        if (sseConnectionStatus && sseConnectionStatus._dash_value && sseConnectionStatus._dash_value.data) {
            const status = sseConnectionStatus._dash_value.data;
            if (status.connect_sse && status.thread_id && !window.workflowSSE.isConnected) {
                console.log('SSE: Connect signal received for thread:', status.thread_id);
                window.workflowSSE.connect(status.thread_id);
                return true;
            }
        }
    } catch (e) {
        console.log('SSE: No connect signal available yet');
    }
    
    // Check if we have a thread ID from any callbacks or stores
    try {
        // Look for thread-id store data
        const threadIdStore = document.querySelector('#thread-id');
        if (threadIdStore && threadIdStore._dash_value && threadIdStore._dash_value.data) {
            const threadId = threadIdStore._dash_value.data;
            if (threadId && !window.workflowSSE.isConnected) {
                console.log('SSE: Auto-connecting with thread ID:', threadId);
                window.workflowSSE.connect(threadId);
                return true;
            }
        }
    } catch (e) {
        console.log('SSE: No thread ID available yet');
    }
    return false;
}

// Monitor for thread ID changes and SSE connection signals using MutationObserver
function setupSSEMonitoring() {
    // Monitor thread-id store
    const threadIdStore = document.querySelector('#thread-id');
    if (threadIdStore) {
        const threadObserver = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'attributes' || mutation.type === 'childList') {
                    console.log('SSE: Thread ID store changed, checking for connection');
                    trySSEAutoConnect();
                }
            });
        });
        
        threadObserver.observe(threadIdStore, {
            attributes: true,
            childList: true,
            subtree: true,
            attributeOldValue: true
        });
        
        console.log('SSE: Monitoring thread-id store for changes');
    }
    
    // Monitor sse-connection-status store for connect signals
    const sseConnectionStore = document.querySelector('#sse-connection-status');
    if (sseConnectionStore) {
        const sseObserver = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'attributes' || mutation.type === 'childList') {
                    console.log('SSE: Connection status changed, checking for connect signal');
                    trySSEAutoConnect();
                }
            });
        });
        
        sseObserver.observe(sseConnectionStore, {
            attributes: true,
            childList: true,
            subtree: true,
            attributeOldValue: true
        });
        
        console.log('SSE: Monitoring sse-connection-status store for changes');
    }
}

// Try to connect immediately and then set up monitoring
if (!trySSEAutoConnect()) {
    // Set up monitoring for changes
    setupSSEMonitoring();
    
    // Also try periodically as fallback
    const connectInterval = setInterval(() => {
        if (trySSEAutoConnect()) {
            clearInterval(connectInterval);
        }
    }, 1000); // Check every second
    
    // Stop trying after 60 seconds
    setTimeout(() => clearInterval(connectInterval), 60000);
}