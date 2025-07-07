// Pure client-side map state management
// This handles immediate user feedback separate from workflow state

class PureMapState {
    constructor() {
        this.userState = {
            selectedPolygons: [],
            selectedPolygonTypes: [], // Track unit type for each selected polygon
            highlightedPolygons: [],
            unitTypes: ['MOD_REG'],
            showUnselected: true,
            yearRange: [new Date().getFullYear(), new Date().getFullYear()]
        };
        
        this.workflowState = {
            selectedPolygons: [],
            unitTypes: [],
            pendingUpdates: []
        };
        
        this.commandQueue = [];
        this.isProcessingCommands = false;
        
        console.log('PureMapState: Initialized with user state:', this.userState);
    }
    
    // === USER INTERACTION LAYER ===
    // These methods provide immediate feedback
    
    userSelectPolygon(polygonId, unitType = 'MOD_REG') {
        const polygonIdStr = String(polygonId);
        
        if (!this.userState.selectedPolygons.includes(polygonIdStr)) {
            this.userState.selectedPolygons.push(polygonIdStr);
            this.userState.selectedPolygonTypes.push(unitType);
            this.userState.highlightedPolygons.push(polygonIdStr);
            
            console.log('PureMapState: User selected polygon:', polygonIdStr, 'type:', unitType);
            this._updateMapDisplay();
            this._zoomToPolygons([polygonIdStr]); // Zoom to the selected polygon
            this._notifyWorkflow('polygon_selected', { polygonId: polygonIdStr, unitType });
            
            return true; // Selection added
        }
        return false; // Already selected
    }
    
    userDeselectPolygon(polygonId) {
        const polygonIdStr = String(polygonId);
        const index = this.userState.selectedPolygons.indexOf(polygonIdStr);
        
        if (index !== -1) {
            this.userState.selectedPolygons.splice(index, 1);
            this.userState.selectedPolygonTypes.splice(index, 1);
            this.userState.highlightedPolygons = this.userState.highlightedPolygons.filter(id => id !== polygonIdStr);
            
            console.log('PureMapState: User deselected polygon:', polygonIdStr);
            this._updateMapDisplay();
            this._notifyWorkflow('polygon_deselected', { polygonId: polygonIdStr });
            
            return true; // Selection removed
        }
        return false; // Not selected
    }
    
    userTogglePolygon(polygonId, unitType = null) {
        const polygonIdStr = String(polygonId);
        
        if (this.userState.selectedPolygons.includes(polygonIdStr)) {
            return this.userDeselectPolygon(polygonIdStr);
        } else {
            return this.userSelectPolygon(polygonIdStr, unitType);
        }
    }
    
    userSetUnitTypes(unitTypes) {
        this.userState.unitTypes = [...unitTypes];
        console.log('PureMapState: User set unit types:', this.userState.unitTypes);
        this._updateMapDisplay();
        this._notifyWorkflow('unit_types_changed', { unitTypes: this.userState.unitTypes });
    }
    
    userSetYearRange(yearRange, notifyWorkflow = false) {
        this.userState.yearRange = [...yearRange];
        console.log('PureMapState: User set year range:', this.userState.yearRange);
        
        // Only notify workflow if explicitly requested (for true user interactions)
        if (notifyWorkflow) {
            this._notifyWorkflow('year_range_changed', { yearRange: this.userState.yearRange });
        }
    }
    
    userReset() {
        const currentYear = new Date().getFullYear();
        this.userState = {
            selectedPolygons: [],
            selectedPolygonTypes: [],
            highlightedPolygons: [],
            unitTypes: ['MOD_REG'],
            showUnselected: true,
            yearRange: [currentYear, currentYear]
        };
        
        console.log('PureMapState: User reset state');
        this._updateMapDisplay();
        this._notifyWorkflow('state_reset', {});
    }
    
    // === WORKFLOW COMMAND INTERFACE ===
    // These methods handle commands from the workflow
    
    executeWorkflowCommand(command) {
        this.commandQueue.push(command);
        this._processCommandQueue();
    }
    
    async _processCommandQueue() {
        if (this.isProcessingCommands) return;
        
        this.isProcessingCommands = true;
        
        while (this.commandQueue.length > 0) {
            const command = this.commandQueue.shift();
            await this._executeCommand(command);
        }
        
        this.isProcessingCommands = false;
    }
    
    async _executeCommand(command) {
        console.log('PureMapState: Executing workflow command:', command);
        
        switch (command.type) {
            case 'select_polygons':
                this._workflowSelectPolygons(command.polygonIds);
                break;
                
            case 'deselect_polygons':
                this._workflowDeselectPolygons(command.polygonIds);
                break;
                
            case 'clear_selection':
                this._workflowClearSelection();
                break;
                
            case 'sync_state':
                this._workflowSyncState(command.state);
                break;
                
            case 'zoom_to_selection':
                this._workflowZoomToSelection();
                break;
                
            default:
                console.warn('PureMapState: Unknown workflow command:', command.type);
        }
    }
    
    _workflowSelectPolygons(polygonIds) {
        const idsToAdd = polygonIds.map(String).filter(id => !this.userState.selectedPolygons.includes(id));
        
        if (idsToAdd.length > 0) {
            this.userState.selectedPolygons.push(...idsToAdd);
            this.userState.highlightedPolygons.push(...idsToAdd);
            
            console.log('PureMapState: Workflow selected polygons:', idsToAdd);
            this._updateMapDisplay();
        }
    }
    
    _workflowDeselectPolygons(polygonIds) {
        const idsToRemove = polygonIds.map(String);
        
        this.userState.selectedPolygons = this.userState.selectedPolygons.filter(id => !idsToRemove.includes(id));
        this.userState.highlightedPolygons = this.userState.highlightedPolygons.filter(id => !idsToRemove.includes(id));
        
        console.log('PureMapState: Workflow deselected polygons:', idsToRemove);
        this._updateMapDisplay();
    }
    
    _workflowClearSelection() {
        this.userState.selectedPolygons = [];
        this.userState.highlightedPolygons = [];
        
        console.log('PureMapState: Workflow cleared selection');
        this._updateMapDisplay();
    }
    
    _workflowSyncState(state) {
        // Merge workflow state with user state, preserving user selections
        const newPolygons = (state.selectedPolygons || []).map(String);
        const newPolygonTypes = state.selectedPolygonTypes || [];
        
        // Add any new workflow polygons to user state
        const toAdd = newPolygons.filter(id => !this.userState.selectedPolygons.includes(id));
        if (toAdd.length > 0) {
            this.userState.selectedPolygons.push(...toAdd);
            this.userState.highlightedPolygons.push(...toAdd);
            
            // Add corresponding unit types
            toAdd.forEach((polygonId, index) => {
                const polygonIndex = newPolygons.indexOf(polygonId);
                if (polygonIndex !== -1 && polygonIndex < newPolygonTypes.length) {
                    this.userState.selectedPolygonTypes.push(newPolygonTypes[polygonIndex]);
                } else {
                    this.userState.selectedPolygonTypes.push(null); // Default if no type provided
                }
            });
        }
        
        // Remove polygons not in workflow state (if they weren't user-selected recently)
        const polygonsToKeep = [];
        const typesToKeep = [];
        this.userState.selectedPolygons.forEach((id, index) => {
            if (newPolygons.includes(id) || this._isRecentUserSelection(id)) {
                polygonsToKeep.push(id);
                typesToKeep.push(this.userState.selectedPolygonTypes[index] || null);
            }
        });
        this.userState.selectedPolygons = polygonsToKeep;
        this.userState.selectedPolygonTypes = typesToKeep;
        this.userState.highlightedPolygons = this.userState.highlightedPolygons.filter(id => 
            this.userState.selectedPolygons.includes(id)
        );
        
        console.log('PureMapState: Workflow synced state:', state);
        console.log('PureMapState: Updated polygon types:', this.userState.selectedPolygonTypes);
        this._updateMapDisplay();
    }
    
    _workflowZoomToSelection() {
        if (this.userState.selectedPolygons.length > 0) {
            this._zoomToPolygons(this.userState.selectedPolygons);
        }
    }
    
    // === INTERNAL HELPERS ===
    
    _updateMapDisplay() {
        // Update the actual Leaflet map to reflect current state
        try {
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                // DIRECT UPDATE: Bypass the map-state store and update the hideout directly
                // This prevents conflicts with traditional callbacks
                const hideout = { selected: this.userState.selectedPolygons };
                
                console.log('PureMapState: Directly updating geojson hideout:', hideout);
                dash_clientside.set_props('geojson-layer', { hideout: hideout });
                
                // Also update the map-state store for consistency with other UI components
                const mapStateStore = document.querySelector('#map-state');
                const currentMapState = (mapStateStore && mapStateStore._dash_value) || {};
                
                const newMapState = {
                    ...currentMapState,
                    selected_polygons: this.userState.selectedPolygons,
                    selected_polygons_unit_types: this.userState.selectedPolygonTypes,
                    unit_types: this.userState.unitTypes,
                    show_unselected: this.userState.showUnselected,
                    year_range: this.userState.yearRange
                };
                
                // Update map state store (this might trigger other callbacks, but hideout is already set)
                dash_clientside.set_props('map-state', { data: newMapState });
                
                console.log('PureMapState: Updated map display with state:', newMapState);
            }
        } catch (error) {
            console.error('PureMapState: Failed to update map display:', error);
        }
    }
    
    _zoomToPolygons(polygonIds) {
        // Trigger zoom to specified polygons using polygon_management
        try {
            const mapElement = document.getElementById('leaflet-map');
            const map = mapElement?._leaflet_map;
            
            if (map && window.polygon_management && window.polygon_management.zoomTo) {
                console.log('PureMapState: Zooming to polygons:', polygonIds);
                window.polygon_management.zoomTo(map, polygonIds);
            } else {
                console.error('PureMapState: Map or polygon_management.zoomTo not available');
            }
        } catch (error) {
            console.error('PureMapState: Failed to trigger zoom:', error);
        }
    }
    
    _notifyWorkflow(action, data) {
        // Send notification to workflow about user action (async, no blocking)
        try {
            fetch('/api/map/user-action', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    action: action,
                    data: data,
                    timestamp: Date.now()
                })
            }).catch(error => {
                console.error('PureMapState: Failed to notify workflow:', error);
            });
        } catch (error) {
            console.error('PureMapState: Failed to create workflow notification:', error);
        }
    }
    
    _isRecentUserSelection() {
        // Check if this polygon was recently selected by user (within last 5 seconds)
        // This prevents workflow sync from removing fresh user selections
        
        // For now, just return false - this can be enhanced with timestamp tracking
        return false;
    }
    
    // === PUBLIC GETTERS ===
    
    getUserState() {
        return { ...this.userState };
    }
    
    getSelectedPolygons() {
        return [...this.userState.selectedPolygons];
    }
    
    getUnitTypes() {
        return [...this.userState.unitTypes];
    }
    
    getYearRange() {
        return [...this.userState.yearRange];
    }
}

// Create global instance
window.pureMapState = new PureMapState();

console.log('PureMapState: Initialized pure client-side map state management');