// app/assets/polygon_management.js

// Add init hook to make map accessible from the container
L.Map.addInitHook(function () {
    // Store a reference of the Leaflet map object on the map container,
    // so that it could be retrieved from DOM selection.
    this.getContainer()._leaflet_map = this;

    // Set up an event listener for when the map is ready
    this.once('load', function () {
        console.log("Map loaded, initializing polygon management");

        // Create a flag to track when GeoJSON layer is ready
        window.geojsonLayerReady = false;

        // Increase timeout to allow all components to initialize
        setTimeout(() => {
            initializeMapLayers(this);
        }, 500);
    });
});

/**
 * New function to initialize map layers and ensure GeoJSON layer is found
 * before attempting to update the map
 * @param {Object} map - Leaflet map object
 */
function initializeMapLayers(map) {
    if (!window.polygon_management) {
        console.warn("polygon_management object not initialized");
        return;
    }

    // First, try to find the GeoJSON layer
    const geojsonLayer = window.polygon_management.findGeoJSONLayer(map);
    
    // If GeoJSON layer not found, retry after a short delay
    if (!geojsonLayer) {
        console.log("GeoJSON layer not found, will retry in 200ms");
        setTimeout(() => {
            initializeMapLayers(map);
        }, 200);
        return;
    }
    
    // Mark the GeoJSON layer as ready
    window.geojsonLayerReady = true;
    console.log("GeoJSON layer found and ready for updates");
    
    // Initialize with default unit type if needed
    const initialState = window.dash_clientside.store &&
        window.dash_clientside.store.getState &&
        window.dash_clientside.store.getState()["map-state"] ||
        { unit_types: ['MOD_REG'] };

    if (initialState && initialState.unit_types && initialState.unit_types.length > 0) {
        // Initial load with current bounds
        const bounds = map.getBounds();
        window.polygon_management.updateMapWithBounds(map, initialState.unit_types, bounds);
    }
}

// Global polygon cache to store loaded polygons by unit type and feature ID
window.polygonCache = {
    featureById: {},        // Stores features by ID
    featuresByUnitType: {},  // Stores feature IDs by unit type
    pendingRequests: {},    // Tracks in-flight requests 
    geojsonLayer: null      // Reference to the main GeoJSON layer
};

window.polygon_management = {
    // Flag to prevent moveend events when using fitBounds
    skipNextMoveend: false,

    /**
     * Get a simple string representation of bounds for request tracking
     * @param {L.LatLngBounds} bounds - Leaflet bounds object
     * @returns {string} - String representation of bounds (rounded to 4 decimals)
     */
    getBoundsKey: function (bounds) {
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();
        return [
            Math.round(sw.lng * 10000) / 10000,
            Math.round(sw.lat * 10000) / 10000,
            Math.round(ne.lng * 10000) / 10000,
            Math.round(ne.lat * 10000) / 10000
        ].join('_');
    },

    /**
     * Get list of cached feature IDs for specific unit types
     * @param {Array} unitTypes - Array of unit types
     * @returns {Array} - Array of feature IDs that are already cached
     */
    getCachedFeatureIds: function(unitTypes) {
        const cachedIds = new Set();
        
        // Collect IDs from each unit type
        unitTypes.forEach(unitType => {
            const idsForType = window.polygonCache.featuresByUnitType[unitType] || [];
            idsForType.forEach(id => cachedIds.add(id));
        });
        
        return Array.from(cachedIds);
    },

    /**
     * Refresh styles on all layers to ensure selected polygons are highlighted
     * @param {Object} geojsonLayer - The GeoJSON layer containing the features
     * @param {Array} selectedPolygons - Array of selected polygon IDs
     */
    refreshLayerStyles: function (geojsonLayer, selectedPolygons) {
        if (!geojsonLayer || !geojsonLayer._layers) {
            return;
        }

        // Get the style function
        let styleFunction = null;
        if (window.map_leaflet && window.map_leaflet.style_function) {
            styleFunction = window.map_leaflet.style_function;
        } else if (geojsonLayer.options && geojsonLayer.options.style) {
            styleFunction = geojsonLayer.options.style;
        }

        if (!styleFunction) {
            console.warn("No style function found for refreshing layer styles");
            return;
        }

        // Create the context object that will be passed to the style function
        const context = {
            hideout: { selected: selectedPolygons || [] }
        };

        // Apply the style to each layer
        Object.values(geojsonLayer._layers).forEach(layer => {
            if (layer.feature) {
                try {
                    // Calculate the style for this feature
                    const style = styleFunction(layer.feature, context);
                    // Apply the style to the layer
                    if (style) {
                        layer.setStyle(style);
                    }
                } catch (error) {
                    console.error("Error applying style:", error, layer.feature);
                }
            }
        });
    },

    /**
     * Fetch polygons within a bounding box for specified unit types, excluding already cached IDs
     * @param {Array} unitTypes - Array of unit types to fetch
     * @param {L.LatLngBounds} bounds - Leaflet bounds object
     * @param {Array} cachedIds - Array of feature IDs to exclude from the request
     * @param {Object} yearRange - Optional year range for time-dependent units
     * @returns {Promise} - Promise that resolves to the fetched GeoJSON
     */
    fetchPolygonsByBounds: function (unitTypes, bounds, cachedIds, yearRange) {
        if (!unitTypes || !unitTypes.length || !bounds) {
            return Promise.reject('Missing parameters');
        }

        // Extract bounds coordinates
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();

        // Generate a unique request ID for tracking
        const requestId = Date.now().toString(36) + Math.random().toString(36).substring(2, 7);

        // Make sure cachedIds is an array
        const excludeIds = Array.isArray(cachedIds) ? cachedIds : [];

        // Cache key for tracking this request
        const boundsKey = this.getBoundsKey(bounds);
        const requestKey = `${unitTypes.join('_')}_${boundsKey}`;
        console.log(`CLIENT: ${requestId}: Request key: ${requestKey}`);

        // Check if this request is already in progress
        if (window.polygonCache.pendingRequests[requestKey]) {
            console.log(`CLIENT: ${requestId}: Request for bounds already in progress, reusing promise`);
            return window.polygonCache.pendingRequests[requestKey];
        }

        // Build the base params for both GET and POST requests
        const params = {
            minX: sw.lng,
            minY: sw.lat,
            maxX: ne.lng,
            maxY: ne.lat,
            request_id: requestId
        };

        // Add year range if specified
        if (yearRange && yearRange.min && yearRange.max) {
            params.start_year = yearRange.min;
            params.end_year = yearRange.max;
        }

        let fetchPromise;

        // Decide between GET and POST based on number of cached IDs
        if (excludeIds.length > 50) {
            // Use POST for large number of IDs
            console.log(`CLIENT: ${requestId}: Using POST to send ${excludeIds.length} cached IDs`);
            
            // Build the POST request payload
            const payload = {
                unit_types: unitTypes,
                bounds: {
                    minX: sw.lng,
                    minY: sw.lat,
                    maxX: ne.lng,
                    maxY: ne.lat
                },
                exclude_ids: excludeIds,
                request_id: requestId
            };
            
            // Add year range if specified
            if (yearRange && yearRange.min && yearRange.max) {
                payload.start_year = yearRange.min;
                payload.end_year = yearRange.max;
            }

            // Make the POST request
            fetchPromise = fetch('/api/polygons/bbox', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to fetch polygons: ${response.statusText}`);
                }
                return response.json();
            });
        } else {
            // Use GET for smaller number of IDs
            // Build URL with parameters
            let url = `/api/polygons/bbox?unit_types=${unitTypes.join(',')}&minX=${sw.lng}&minY=${sw.lat}&maxX=${ne.lng}&maxY=${ne.lat}&request_id=${requestId}`;

            // Add year range if specified
            if (yearRange && yearRange.min && yearRange.max) {
                url += `&start_year=${yearRange.min}&end_year=${yearRange.max}`;
            }

            // Add the cached IDs parameter if we have any
            if (excludeIds.length > 0) {
                url += `&exclude_ids=${excludeIds.join(',')}`;
                console.log(`CLIENT: ${requestId}: Excluding ${excludeIds.length} cached IDs from request`);
            }

            console.log(`CLIENT: ${requestId}: Fetching polygons for bounds: ${sw.lng},${sw.lat} to ${ne.lng},${ne.lat}`);

            // Make the GET request
            fetchPromise = fetch(url)
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`Failed to fetch polygons: ${response.statusText}`);
                    }
                    return response.json();
                });
        }
        fetchPromise
        .then(data => {
            // Handle special case where server tells us to use cached data
            if (data.useCachedFeatures) {
                console.log(`CLIENT: ${requestId}: Server confirmed we can use our cached data`);

                // Create a filtered version of our cached features for this view
                const filteredFeatures = [];
                unitTypes.forEach(unitType => {
                    const featureIds = window.polygonCache.featuresByUnitType[unitType] || [];
                    featureIds.forEach(id => {
                        const feature = window.polygonCache.featureById[id];
                        if (feature) {
                            filteredFeatures.push(feature);
                        }
                    });
                });

                console.log(`CLIENT: ${requestId}: Using ${filteredFeatures.length} cached features for the current view`);

                // Return filtered features as a GeoJSON object
                return {
                    type: "FeatureCollection",
                    features: filteredFeatures,
                    fromCache: true
                };
            }

            console.log(`CLIENT: ${requestId}: Received ${data.features ? data.features.length : 0} new polygons from server`);

            // Store new features in the cache
            if (data.features && data.features.length > 0) {
                data.features.forEach(feature => {
                    if (feature.id) {
                        // Store the feature by ID
                        window.polygonCache.featureById[feature.id] = feature;
                        
                        // Store the ID in the unit type index
                        if (feature.properties && feature.properties.g_unit_type) {
                            const unitType = feature.properties.g_unit_type;
                            if (!window.polygonCache.featuresByUnitType[unitType]) {
                                window.polygonCache.featuresByUnitType[unitType] = [];
                            }
                            if (!window.polygonCache.featuresByUnitType[unitType].includes(feature.id)) {
                                window.polygonCache.featuresByUnitType[unitType].push(feature.id);
                            }
                        }
                    }
                });
            }

            // Remove from pending requests
            delete window.polygonCache.pendingRequests[requestKey];
            return data;
        })
        .catch(error => {
            console.error(`CLIENT: ${requestId}: Error fetching polygons: ${error.message}`);
            // Remove from pending requests on error
            delete window.polygonCache.pendingRequests[requestKey];
            throw error;
        });

        // Store the promise for potential reuse
        window.polygonCache.pendingRequests[requestKey] = fetchPromise;
        return fetchPromise;
    },
    
    /**
     * Find the GeoJSON layer in the map
     * @param {Object} map - Leaflet map object
     * @returns {Object} - The GeoJSON layer, or null if not found
     */
    findGeoJSONLayer: function (map) {
        // If we already have a reference, use it
        if (window.polygonCache.geojsonLayer) {
            return window.polygonCache.geojsonLayer;
        }

        // Try to find the layer by its ID
        const geoJSONElement = document.getElementById('geojson-layer');
        if (geoJSONElement && geoJSONElement._leaflet_id && map._layers[geoJSONElement._leaflet_id]) {
            window.polygonCache.geojsonLayer = map._layers[geoJSONElement._leaflet_id];
            console.log("Found GeoJSON layer by ID", window.polygonCache.geojsonLayer);
            return window.polygonCache.geojsonLayer;
        }

        // Otherwise search through all layers
        let geojsonLayer = null;
        Object.values(map._layers).forEach(layer => {
            // Look for a layer that has _layers and no _url (typical of a FeatureGroup or GeoJSON layer)
            if (layer._layers && !layer._url) {
                // Debug info to see what the layer options are
                console.log("Found potential GeoJSON layer", layer);
                if (layer.options && layer.options.style) {
                    console.log("Layer has style function", layer.options.style);
                }
                geojsonLayer = layer;
            }
        });

        // Store for future reference
        if (geojsonLayer) {
            console.log("Found GeoJSON layer in map", geojsonLayer);
            window.polygonCache.geojsonLayer = geojsonLayer;
        } 
        
        return geojsonLayer;
    },

    /**
     * Calculate bounds for selected features
     * @param {Array} features - Array of GeoJSON features
     * @returns {L.LatLngBounds|null} - Leaflet bounds object or null if no features
     */
    calculateBounds: function (features) {
        if (!features || features.length === 0) {
            return null;
        }

        let bounds = null;

        features.forEach(feature => {
            if (!feature.geometry) return;

            // Create a temporary GeoJSON layer to get the bounds
            const tempLayer = dash_leaflet.geoJSON(feature);
            const featureBounds = tempLayer.getBounds();

            if (!bounds) {
                bounds = featureBounds;
            } else {
                bounds.extend(featureBounds);
            }
        });

        return bounds;
    },

    /**
     * Zoom map to selected features
     * @param {Object} map - Leaflet map object
     * @param {Array} selectedFeatures - Array of selected GeoJSON features 
     */
    zoomToSelected: function (map, selectedIds) {
        if (!map || !selectedIds || selectedIds.length === 0) return;

        // Find features with the selected IDs
        const geojsonLayer = window.polygon_management.findGeoJSONLayer(map);
        if (!geojsonLayer) return;

        // Get bounds of selected features
        const bounds = L.latLngBounds();
        let foundFeature = false;

        Object.values(geojsonLayer._layers).forEach(layer => {
            if (layer.feature && selectedIds.includes(layer.feature.id)) {
                bounds.extend(layer.getBounds());
                foundFeature = true;
            }
        });

        // If we found at least one feature, zoom to it
        if (foundFeature) {
            window.polygon_management.skipNextMoveend = true;
            map.fitBounds(bounds, {
                padding: [50, 50],
                maxZoom: 12,
                animate: true,
                duration: 0.5
            });
            console.log("Zoomed to selected features");
        }
    },

    /**
     * Update the map with polygons based on current bounds
     * @param {Object} map - Leaflet map object
     * @param {Array} unitTypes - Array of unit types to fetch
     * @param {L.LatLngBounds} bounds - Current map bounds
     * @param {Object} yearRange - Optional year range
     * @returns {Promise} - Promise that resolves when update is complete
     */
    updateMapWithBounds: function (map, unitTypes, bounds, mapState, yearRange) {
        if (!map || !unitTypes || !unitTypes.length || !bounds) {
            console.error('Missing required parameters for updateMapWithBounds');
            return Promise.reject('Missing parameters');
        }

        // Check if GeoJSON layer is ready
        if (!window.geojsonLayerReady) {
            console.warn('GeoJSON layer not ready yet, deferring update');
            return Promise.resolve({
                type: "FeatureCollection",
                features: []
            });
        }

        console.log(`Updating map with bounds for unit types: ${unitTypes.join(', ')}`);

        // Find the GeoJSON layer
        let geojsonLayer = this.findGeoJSONLayer(map);
        if (!geojsonLayer) {
            console.error('GeoJSON layer not found in map');
            return Promise.reject('No GeoJSON layer found');
        }
        
        let showUnselected = true; // Default to true
        if (mapState && mapState.show_unselected) {
                showUnselected = mapState.show_unselected;
        }

        let selectedPolygons = [];
        // // Extract map state data
        if (mapState && mapState.selected_polygons) {
            selectedPolygons = mapState.selected_polygons;
        }
        // Generate a unique request ID for tracking
        const requestId = Date.now().toString(36) + Math.random().toString(36).substring(2, 7);
        console.log(`CLIENT: ${requestId}: Selected polygons: ${selectedPolygons.length}`);
        // Track the current zoom level
        const currentZoom = map.getZoom();
        console.log(`CLIENT: ${requestId}: Current zoom level: ${currentZoom}`);

        // Get list of cached feature IDs for these unit types
        const cachedIds = this.getCachedFeatureIds(unitTypes);
        console.log(`CLIENT: ${requestId}: Found ${cachedIds.length} cached features for the requested unit types`);

        // Fetch new polygons, excluding cached ones
        return this.fetchPolygonsByBounds(unitTypes, bounds, cachedIds, yearRange)
            .then(geodata => {
                console.log(`CLIENT: ${requestId}: Received ${geodata.features ? geodata.features.length : 0} polygons from server`);

                // Get all features (both newly fetched and previously cached)
                const displayFeatures = [];

                // Collect features from each requested unit type
                unitTypes.forEach(unitType => {
                    const featureIds = window.polygonCache.featuresByUnitType[unitType] || [];
                    featureIds.forEach(id => {
                        const feature = window.polygonCache.featureById[id];
                        if (feature && (!geodata.fromCache || isFeatureInView(feature, bounds))) {
                            // Filter features based on selected state if needed
                            if (showUnselected || (feature.id && selectedPolygons.includes(feature.id))) {
                                displayFeatures.push(feature);
                            }
                        }
                    });
                });

                // Add any new features from the server response
                if (geodata.features && !geodata.fromCache) {
                    geodata.features.forEach(feature => {
                        // Filter features based on selected state if needed
                        if (showUnselected || (feature.id && selectedPolygons.includes(feature.id))) {
                            displayFeatures.push(feature);
                        }
                    });
                }

                // Clear existing layers
                geojsonLayer.clearLayers();

                // Create a GeoJSON object with all features
                const completeGeodata = {
                    type: "FeatureCollection",
                    features: displayFeatures
                };

                // Create options object with hideout
                const geoJsonOptions = {
                    style: geojsonLayer.options && geojsonLayer.options.style,
                    pointToLayer: geojsonLayer.options && geojsonLayer.options.pointToLayer,
                    onEachFeature: geojsonLayer.options && geojsonLayer.options.onEachFeature,
                    hideout: { selected: selectedPolygons }
                };

                // Add all features to the map
                geojsonLayer.addData(completeGeodata, geoJsonOptions);

                // Update the hideout property
                if (geojsonLayer.options) {
                    geojsonLayer.options.hideout = { selected: selectedPolygons };
                } else {
                    geojsonLayer.options = { hideout: { selected: selectedPolygons } };
                }

                // Force a style refresh
                this.refreshLayerStyles(geojsonLayer, selectedPolygons);

                console.log(`CLIENT: ${requestId}: Map updated with combined data (${displayFeatures.length} features displayed)`);
                return completeGeodata;
            })
            .catch(error => {
                console.error(`CLIENT: ${requestId}: Error updating map:`, error);
                return Promise.reject(error);
            });

        // Helper function to check if a feature is in the current view
        function isFeatureInView(feature, bounds) {
            if (!feature.geometry) return false;
            
            // For points, simple check if coordinates are within bounds
            if (feature.geometry.type === 'Point') {
                const coords = feature.geometry.coordinates;
                return coords[0] >= bounds.getWest() && 
                       coords[0] <= bounds.getEast() &&
                       coords[1] >= bounds.getSouth() && 
                       coords[1] <= bounds.getNorth();
            }
            
            // For other geometries, assume they might be visible
            // A more accurate check would use a spatial library but this is lighter
            return true;
        }
    },

    /**
     * Update the map based on selected unit types
     * @param {Object} map - Leaflet map object
     * @param {Array} unitTypes - Array of selected unit types
     * @param {Object} yearRange - Optional year range for time-dependent unit types
     * @param {Array} selectedPolygons - IDs of selected polygons
     * @param {boolean} showUnselected - Whether to show unselected polygons
     * @returns {Promise} - Promise that resolves when map is updated
     */
    updateMap: function (map, unitTypes, yearRange, selectedPolygons, showUnselected) {
        if (!map || !unitTypes || !unitTypes.length) {
            console.error('Missing required parameters for updateMap');
            return Promise.reject('Missing parameters');
        }

        console.log(`Updating map with unit types: ${unitTypes.join(', ')}`);
        console.log(`Selected polygons: ${selectedPolygons.length}, Show unselected: ${showUnselected}`);

        // Get the current bounds
        const bounds = map.getBounds();

        // Update the map using the current bounds approach
        return this.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange);
    }
};

// Register client-side callback to update map when map-state changes
window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        /**
         * Client-side callback to update the map when unit filters change
         * @param {Object} mapState - The current map state
         * @param {Object} appState - The current app state
         * @returns {Array} - Returns updates for multiple outputs
         */
        updateMapWithPolygons: function (mapState, appState) {
            // Ensure we have a valid map state
            if (!mapState || !mapState.unit_types || !mapState.unit_types.length) {
                return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update,
                dash_clientside.no_update, dash_clientside.no_update];
            }

            // Get the Leaflet map object
            const mapElement = document.getElementById('leaflet-map');
            if (!mapElement || !mapElement._leaflet_map) {
                console.warn('Map element or Leaflet map instance not found');
                return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update,
                dash_clientside.no_update, dash_clientside.no_update];
            }

            const map = mapElement._leaflet_map;

            // Check if the GeoJSON layer is ready before proceeding
            if (!window.geojsonLayerReady) {
                console.warn('GeoJSON layer not ready yet, skipping callback');
                return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update,
                dash_clientside.no_update, dash_clientside.no_update];
            }

            // Determine if we need to show/hide the year range container
            let containerStyle = { 'display': 'none' };

            // Check if any of the selected unit types are not timeless
            const timelessUnitTypes = ["MOD_CNTY", "MOD_DIST", "MOD_REG"]; // Unit types that don't require a year filter
            const needsYearFilter = mapState.unit_types.some(ut => !timelessUnitTypes.includes(ut));

            if (needsYearFilter) {
                containerStyle = { 'display': 'block' };
            }

            // Extract required data from map state
            const unitTypes = mapState.unit_types || ['MOD_REG'];
            const yearRange = mapState.year_range ? {
                min: mapState.year_range[0],
                max: mapState.year_range[1]
            } : null;
            const selectedPolygons = mapState.selected_polygons || [];
            const showUnselected = mapState.show_unselected !== false; // Default to true

            // Get the current bounds
            const bounds = map.getBounds();

            // Call the updateMapWithBounds function
            window.polygon_management.updateMapWithBounds(map, unitTypes, bounds, mapState, yearRange)
                .then(filteredGeoJSON => {
                    // Success - the map has been updated directly
                    console.log(`Map updated successfully with ${filteredGeoJSON.features.length} features`);
                })
                .catch(error => {
                    console.error('Error updating map:', error);
                });

            // Create a debug message
            const debugMsg = `Showing polygons for ${unitTypes.join(', ')} in current view`;

            // Since we're handling the updates directly on the map object,
            // we only need to update the year range container and debug message
            return [
                containerStyle,
                dash_clientside.no_update,
                dash_clientside.no_update,
                debugMsg,
                dash_clientside.no_update
            ];
        }
    }
});