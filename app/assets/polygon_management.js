// app/assets/polygon_management.js (updated with bounding box support)

// Add init hook to make map accessible from the container
L.Map.addInitHook(function () {
    // Store a reference of the Leaflet map object on the map container,
    // so that it could be retrieved from DOM selection.
    this.getContainer()._leaflet_map = this;

    // Set up an event listener for when the map is ready
    this.once('load', function () {
        console.log("Map loaded, initializing polygon management");

        // Wait a moment for all layers to initialize
        setTimeout(() => {
            // Find the GeoJSON layer and store it
            if (window.polygon_management) {
                window.polygon_management.findGeoJSONLayer(this);

                // Initialize with default unit type if needed
                const initialState = window.dash_clientside.store &&
                    window.dash_clientside.store.getState &&
                    window.dash_clientside.store.getState()["map-state"] ||
                    { unit_types: ['MOD_REG'] };

                if (initialState && initialState.unit_types && initialState.unit_types.length > 0) {
                    // Note: moveend event is now handled in the clientside callback

                    // Initial load with current bounds
                    const bounds = this.getBounds();
                    window.polygon_management.updateMapWithBounds(this, initialState.unit_types, bounds);
                }
            }
        }, 500);
    });
});

// Global polygon cache to store loaded polygons by unit type
window.polygonCache = {
    loadedUnitTypes: {},      // Tracks which unit types have been loaded
    pendingRequests: {},      // Tracks in-flight requests by unit type
    bboxCache: {},            // Stores bounding boxes by unit type that have been loaded
    allFeatures: {},          // Stores all features by ID
    geojsonLayer: null        // Reference to the main GeoJSON layer
};

window.polygon_management = {
    // Flag to prevent moveend events when using fitBounds
    skipNextMoveend: false,

    /**
     * Get a simple string representation of bounds for cache tracking
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
     * Check if a bounding box is already cached
     * @param {string} unitType - The unit type
     * @param {L.LatLngBounds} bounds - Leaflet bounds object
     * @returns {boolean} - True if this bounding box is already cached
     */
    isBoundsCached: function (unitType, bounds) {
        const boundsKey = this.getBoundsKey(bounds);

        // Check if this unit type has any cached bounds
        if (!window.polygonCache.bboxCache[unitType]) {
            window.polygonCache.bboxCache[unitType] = {};
            return false;
        }

        // Check if this specific bounds is cached
        return !!window.polygonCache.bboxCache[unitType][boundsKey];
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
         * Fetch polygons within a bounding box for specified unit types
         * @param {Array} unitTypes - Array of unit types to fetch
         * @param {L.LatLngBounds} bounds - Leaflet bounds object
         * @param {Object} yearRange - Optional year range for time-dependent units
         * @returns {Promise} - Promise that resolves to the fetched GeoJSON
         */
    fetchPolygonsByBounds: function (unitTypes, bounds, yearRange) {
        if (!unitTypes || !unitTypes.length || !bounds) {
            return Promise.reject('Missing parameters');
        }

        // Extract bounds coordinates
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();

        // Generate a unique request ID for tracking
        const requestId = Date.now().toString(36) + Math.random().toString(36).substring(2, 7);

        // Check if we already have cached all the data for this view
        const boundsKey = this.getBoundsKey(bounds);
        let allDataCached = true;
        let cachedFeaturesForView = 0;

        // For each unit type, check if we have this bounds in cache
        for (const unitType of unitTypes) {
            if (!window.polygonCache.bboxCache[unitType] ||
                !window.polygonCache.bboxCache[unitType][boundsKey]) {
                allDataCached = false;
                break;
            }

            // Check how many cached features we have for this unit type
            cachedFeaturesForView += Object.values(window.polygonCache.allFeatures)
                .filter(f => f.properties && f.properties.g_unit_type === unitType)
                .length;
        }

        // Build URL with parameters
        let url = `/api/polygons/bbox?unit_types=${unitTypes.join(',')}&minX=${sw.lng}&minY=${sw.lat}&maxX=${ne.lng}&maxY=${ne.lat}&request_id=${requestId}`;

        // Add year range if specified
        if (yearRange && yearRange.min && yearRange.max) {
            url += `&start_year=${yearRange.min}&end_year=${yearRange.max}`;
        }

        // If we have all data cached, tell the server
        if (allDataCached && cachedFeaturesForView > 0) {
            url += `&client_has_data=true`;
            console.log(`CLIENT: ${requestId}: All data already cached locally (${cachedFeaturesForView} features), informing server`);
        } else {
            console.log(`CLIENT: ${requestId}: Not all data cached (cached=${allDataCached}, features=${cachedFeaturesForView}), requesting from server`);
        }

        console.log(`CLIENT: ${requestId}: Fetching polygons for bounds: ${sw.lng},${sw.lat} to ${ne.lng},${ne.lat}`);

        // Cache key for tracking this request
        const requestKey = `bbox_${unitTypes.join('_')}_${boundsKey}`;
        console.log(`CLIENT: ${requestId}: Request key: ${requestKey}`);

        // Log current cache state
        console.log(`CLIENT: ${requestId}: Current client-side cache state:`);
        for (const unitType in window.polygonCache.bboxCache) {
            const cachedBounds = Object.keys(window.polygonCache.bboxCache[unitType] || {});
            console.log(`CLIENT: ${requestId}: Unit type ${unitType}: ${cachedBounds.length} cached bounds: ${cachedBounds.join(', ')}`);
        }

        // Check if this request is already in progress
        if (window.polygonCache.pendingRequests[requestKey]) {
            console.log(`CLIENT: ${requestId}: Request for bounds already in progress, reusing promise`);
            return window.polygonCache.pendingRequests[requestKey];
        }

        // Make the fetch request
        const fetchPromise = fetch(url)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to fetch polygons: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                // Handle special case where server tells us to use cached data
                if (data.useCachedFeatures) {
                    console.log(`CLIENT: ${requestId}: Server confirmed we can use our cached data`);

                    // Create a filtered version of our cached features for this view
                    const filteredFeatures = Object.values(window.polygonCache.allFeatures)
                        .filter(f => unitTypes.includes(f.properties && f.properties.g_unit_type))
                        .filter(f => {
                            // Check if the feature is within the current bounds
                            // This is a simplified check that works for points
                            if (f.geometry && f.geometry.type === 'Point') {
                                const coords = f.geometry.coordinates;
                                return coords[0] >= sw.lng && coords[0] <= ne.lng &&
                                    coords[1] >= sw.lat && coords[1] <= ne.lat;
                            }
                            // For polygons and other geometries, assume they're in the view
                            // A more accurate check would use turf.js to check spatial relationships
                            return true;
                        });

                    console.log(`CLIENT: ${requestId}: Using ${filteredFeatures.length} cached features for the current view`);

                    // Return filtered features as a GeoJSON object
                    return {
                        type: "FeatureCollection",
                        features: filteredFeatures,
                        fromCache: true
                    };
                }

                console.log(`CLIENT: ${requestId}: Received ${data.features ? data.features.length : 0} polygons from server`);

                // Check if we have duplicates with what's already in cache
                const newFeatureIds = new Set();
                if (data.features) {
                    data.features.forEach(f => {
                        if (f.id) newFeatureIds.add(f.id);
                    });
                }

                const existingIds = new Set();
                Object.values(window.polygonCache.allFeatures).forEach(f => {
                    if (f.id) existingIds.add(f.id);
                });

                const intersection = [...newFeatureIds].filter(id => existingIds.has(id));
                console.log(`CLIENT: ${requestId}: ${intersection.length} features already in cache out of ${newFeatureIds.size} received`);

                // Mark these bounds as cached for each unit type
                unitTypes.forEach(unitType => {
                    if (!window.polygonCache.bboxCache[unitType]) {
                        window.polygonCache.bboxCache[unitType] = {};
                    }
                    window.polygonCache.bboxCache[unitType][boundsKey] = true;
                    console.log(`CLIENT: ${requestId}: Cached bounds ${boundsKey} for unit type ${unitType}`);
                });

                // Store individual features for filtering
                let newFeaturesAdded = 0;
                if (data && data.features) {
                    data.features.forEach(feature => {
                        // Use feature ID as key if it exists
                        if (feature.id) {
                            if (!window.polygonCache.allFeatures[feature.id]) {
                                newFeaturesAdded++;
                            }
                            window.polygonCache.allFeatures[feature.id] = feature;
                        }
                    });
                }
                console.log(`CLIENT: ${requestId}: Added ${newFeaturesAdded} new features to the client-side feature cache`);

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
            window.polygonCache.geojsonLayer = geojsonLayer;
        } else {
            console.warn("No GeoJSON layer found, will create one");
            // If no layer is found, we could create one with the correct style
            try {
                // Try to get the style function from the global namespace
                let styleFunction = null;
                if (window.map_leaflet && window.map_leaflet.style_function) {
                    styleFunction = window.map_leaflet.style_function;
                    console.log("Using style function from window.map_leaflet");
                }

                geojsonLayer = dash_leaflet.GeoJSON([], {
                    style: styleFunction
                })
                // Add the layer to the map
                geojsonLayer.addTo(map);
                window.polygonCache.geojsonLayer = geojsonLayer;
            } catch (error) {
                console.error("Error creating new GeoJSON layer", error);
            }
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
    zoomToSelectedFeatures: function (map, selectedFeatures) {
        const bounds = this.calculateBounds(selectedFeatures);

        if (bounds && bounds.isValid()) {
            // Set flag to ignore the moveend event triggered by this fit bounds
            this.skipNextMoveend = true;

            // Add some padding around the bounds
            map.fitBounds(bounds, {
                padding: [50, 50],
                maxZoom: 12,
                animate: true,
                duration: 0.5
            });
            console.log("Zoomed to selected features");
        } else {
            console.log("No valid bounds for selected features");
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
    updateMapWithBounds: function (map, unitTypes, bounds, yearRange) {
        if (!map || !unitTypes || !unitTypes.length || !bounds) {
            console.error('Missing required parameters for updateMapWithBounds');
            return Promise.reject('Missing parameters');
        }

        console.log(`Updating map with bounds for unit types: ${unitTypes.join(', ')}`);

        // Find the GeoJSON layer
        let geojsonLayer = this.findGeoJSONLayer(map);
        if (!geojsonLayer) {
            console.error('GeoJSON layer not found in map');
            return Promise.reject('No GeoJSON layer found');
        }

        // Get the map state to determine selected polygons and other settings
        const mapState = window.dash_clientside.store &&
            window.dash_clientside.store.getState &&
            window.dash_clientside.store.getState()["map-state"] || {};

        // Extract map state data
        const selectedPolygons = mapState.selected_polygons || [];
        const showUnselected = mapState.show_unselected !== false; // Default to true

        // Generate a unique request ID for tracking
        const requestId = Date.now().toString(36) + Math.random().toString(36).substring(2, 7);

        // Extract bounds coordinates
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();
        const boundsKey = this.getBoundsKey(bounds);

        // Track the current zoom level
        const currentZoom = map.getZoom();
        console.log(`CLIENT: ${requestId}: Current zoom level: ${currentZoom}`);

        // Check if we need to fetch new data by comparing with our cached regions
        let needFetchFromServer = false;

        // Check each unit type to see if we have coverage for the current bounds
        for (const unitType of unitTypes) {
            // Initialize unit type cache if it doesn't exist
            if (!window.polygonCache.bboxCache[unitType]) {
                window.polygonCache.bboxCache[unitType] = {};
                needFetchFromServer = true;
                console.log(`CLIENT: ${requestId}: No cache for unit type ${unitType}, will fetch from server`);
                continue;
            }

            // Check if current bounds are fully contained within our cached bounds
            const cachedBoundsKeys = Object.keys(window.polygonCache.bboxCache[unitType]);
            if (cachedBoundsKeys.length === 0) {
                needFetchFromServer = true;
                console.log(`CLIENT: ${requestId}: No cached bounds for unit type ${unitType}, will fetch from server`);
                continue;
            }

            // Check if current view is covered by cached areas
            let isCovered = false;

            // For each cached bound, check if it contains or mostly overlaps the current bounds
            // This is a simplified approach - in production you'd use a proper spatial index
            for (const cachedKey of cachedBoundsKeys) {
                // Extract the coordinates from the cached key
                const coordParts = cachedKey.split('_');
                if (coordParts.length < 5) continue; // Skip invalid keys

                const cachedSW = {
                    lng: parseFloat(coordParts[1]),
                    lat: parseFloat(coordParts[2])
                };
                const cachedNE = {
                    lng: parseFloat(coordParts[3]),
                    lat: parseFloat(coordParts[4])
                };

                // Calculate overlap percentage between current bounds and cached bounds
                const overlapX = Math.max(0, Math.min(ne.lng, cachedNE.lng) - Math.max(sw.lng, cachedSW.lng));
                const overlapY = Math.max(0, Math.min(ne.lat, cachedNE.lat) - Math.max(sw.lat, cachedSW.lat));

                // Calculate areas
                const currentArea = (ne.lng - sw.lng) * (ne.lat - sw.lat);
                const cachedArea = (cachedNE.lng - cachedSW.lng) * (cachedNE.lat - cachedSW.lat);
                const overlapArea = overlapX * overlapY;

                // If current view is at least 90% covered by cached area, consider it covered
                if (overlapArea > 0 && (overlapArea / currentArea) > 0.9) {
                    isCovered = true;
                    console.log(`CLIENT: ${requestId}: Unit type ${unitType} is covered by cached bounds ${cachedKey} (${Math.round(overlapArea / currentArea * 100)}%)`);
                    break;
                }
            }

            if (!isCovered) {
                needFetchFromServer = true;
                console.log(`CLIENT: ${requestId}: Unit type ${unitType} not fully covered by cached bounds, will fetch from server`);
            }
        }

        // Get features that match the current unit types
        const cachedFeatures = Object.values(window.polygonCache.allFeatures).filter(f =>
            f.properties && unitTypes.includes(f.properties.g_unit_type)
        );

        // If we have features but need to fetch more, we'll do both:
        // 1. Render what we have now
        // 2. Fetch additional data and update the map when it arrives
        if (cachedFeatures.length > 0) {
            console.log(`CLIENT: ${requestId}: Using ${cachedFeatures.length} cached features while fetching additional data`);

            // Filter features based on selected state if needed
            let displayFeatures = cachedFeatures;
            if (!showUnselected) {
                displayFeatures = displayFeatures.filter(feature =>
                    feature.id && selectedPolygons.includes(feature.id));
            }

            // Clear existing layers and add the cached ones immediately
            geojsonLayer.clearLayers();

            // Create a temporary GeoJSON object with the cached features
            const cachedGeodata = {
                type: "FeatureCollection",
                features: displayFeatures
            };

            // Create options object with hideout to ensure style function works correctly
            const geoJsonOptions = {
                style: geojsonLayer.options && geojsonLayer.options.style,
                pointToLayer: geojsonLayer.options && geojsonLayer.options.pointToLayer,
                onEachFeature: geojsonLayer.options && geojsonLayer.options.onEachFeature,
                hideout: { selected: selectedPolygons }
            };


            // Add each layer to the main GeoJSON layer
            geojsonLayer.addData(cachedGeodata, geoJsonOptions);

            // Update the hideout property with selected IDs
            if (geojsonLayer.options) {
                geojsonLayer.options.hideout = { selected: selectedPolygons };
            } else {
                geojsonLayer.options = { hideout: { selected: selectedPolygons } };
            }

            // Force a style refresh
            this.refreshLayerStyles(geojsonLayer, selectedPolygons);
        }

        // If we don't need to fetch from the server, return the cached data
        if (!needFetchFromServer) {
            console.log(`CLIENT: ${requestId}: Using only cached data, no server fetch needed`);

            // Create a GeoJSON object with the cached features
            const geodata = {
                type: "FeatureCollection",
                features: cachedFeatures
            };

            return Promise.resolve(geodata);
        }

        // Otherwise, fetch from the server
        console.log(`CLIENT: ${requestId}: Fetching additional data from server for bounds ${sw.lng},${sw.lat} to ${ne.lng},${ne.lat}`);

        // Expand bounds slightly to ensure we fetch polygons that might be partially visible
        const expandedBounds = bounds.pad(0.1); // Add 10% padding around the visible area

        // Fetch polygons based on the expanded bounds
        return this.fetchPolygonsByBounds(unitTypes, expandedBounds, yearRange)
            .then(geodata => {
                console.log(`CLIENT: ${requestId}: Received ${geodata.features ? geodata.features.length : 0} polygons from server`);

                // Get all features (both newly fetched and previously cached)
                let allFeatures = [...cachedFeatures];

                // Add any new features from the server response
                if (geodata.features && geodata.features.length > 0) {
                    // Add only features that aren't already in our cache
                    const existingIds = new Set(cachedFeatures.map(f => f.id));
                    const newFeatures = geodata.features.filter(f => !existingIds.has(f.id));

                    console.log(`CLIENT: ${requestId}: Adding ${newFeatures.length} new features to the display`);

                    // Update the all features array
                    allFeatures = [...cachedFeatures, ...newFeatures];
                }

                // Filter features based on selected state if needed
                let displayFeatures = allFeatures;
                if (!showUnselected) {
                    displayFeatures = displayFeatures.filter(feature =>
                        feature.id && selectedPolygons.includes(feature.id));
                }

                // Clear existing layers and add all features
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

                console.log(`CLIENT: ${requestId}: Map updated with combined data (${displayFeatures.length} features)`);
                return completeGeodata;
            })
            .catch(error => {
                console.error(`CLIENT: ${requestId}: Error updating map:`, error);
                return Promise.reject(error);
            });
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
        return this.updateMapWithBounds(map, unitTypes, bounds, yearRange);
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
            window.polygon_management.updateMapWithBounds(map, unitTypes, bounds, yearRange)
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