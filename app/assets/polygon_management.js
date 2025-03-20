// app/assets/polygon_management.js

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
                    const unitType = initialState.unit_types[0];
                    window.polygon_management.fetchPolygons(unitType).then(data => {
                        console.log(`Initial polygons loaded for ${unitType}`);
                    }).catch(error => {
                        console.error("Error loading initial polygons:", error);
                    });
                }
            }
        }, 500);
    });
});

// Global polygon cache to store loaded polygons by unit type
window.polygonCache = {
    loadedUnitTypes: {},      // Tracks which unit types have been loaded
    pendingRequests: {},      // Tracks in-flight requests by unit type
    allFeatures: {},          // Stores all features by ID
    geojsonLayer: null        // Reference to the main GeoJSON layer
};

window.polygon_management = {
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
        if (geojsonLayer.options && geojsonLayer.options.style) {
            styleFunction = geojsonLayer.options.style;
        } else if (window.map_leaflet && window.map_leaflet.style_function) {
            styleFunction = window.map_leaflet.style_function;
        }

        if (!styleFunction) {
            console.warn("No style function found for refreshing layer styles");
            return;
        }

        // Create the context object that will be passed to the style function
        const context = {
            hideout: { selected: selectedPolygons }
        };

        // Apply the style to each layer
        Object.values(geojsonLayer._layers).forEach(layer => {
            if (layer.feature) {
                // Calculate the style for this feature
                const style = styleFunction(layer.feature, context);
                // Apply the style to the layer
                if (style) {
                    layer.setStyle(style);
                }
            }
        });
    },
    /**
     * Fetch polygons for a unit type and optional year range
     * @param {string} unitType - The unit type to fetch (e.g., 'MOD_REG', 'MOD_DIST')
     * @param {Object} yearRange - Optional year range for time-dependent units
     * @returns {Promise} - Promise that resolves to the fetched GeoJSON
     */
    fetchPolygons: function (unitType, yearRange) {
        // Check if this unit type is already loaded in the cache
        if (window.polygonCache.loadedUnitTypes[unitType]) {
            console.log(`Using cached polygons for ${unitType}`);
            return Promise.resolve({
                type: "FeatureCollection",
                features: Object.values(window.polygonCache.allFeatures).filter(
                    feature => feature.properties && feature.properties.g_unit_type === unitType
                )
            });
        }

        // Check if there's already a pending request for this unit type
        if (window.polygonCache.pendingRequests[unitType]) {
            console.log(`Request for ${unitType} already in progress, reusing promise`);
            return window.polygonCache.pendingRequests[unitType];
        }

        // Build the URL with parameters
        let url = `/api/polygons/${unitType}`;

        // Add year range parameters if provided
        if (yearRange && yearRange.min && yearRange.max) {
            url += `?start_year=${yearRange.min}&end_year=${yearRange.max}`;
        }

        console.log(`Fetching polygons for ${unitType}`);

        // Create the fetch Promise and store it
        const fetchPromise = fetch(url)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to fetch polygons: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Received ${data.features ? data.features.length : 0} polygons for ${unitType}`);

                // Store the fetched polygons in the cache
                window.polygonCache.loadedUnitTypes[unitType] = true;

                // Store individual features for filtering
                if (data && data.features) {
                    data.features.forEach(feature => {
                        // Ensure the feature has the unit type property
                        if (feature.properties) {
                            feature.properties.g_unit_type = unitType;
                        }

                        // Use feature ID as key
                        if (feature.id) {
                            window.polygonCache.allFeatures[feature.id] = feature;
                        }
                    });
                }

                // Remove from pending requests once completed
                delete window.polygonCache.pendingRequests[unitType];
                return data;
            })
            .catch(error => {
                // Also remove from pending on error
                delete window.polygonCache.pendingRequests[unitType];
                throw error;
            });

        // Store the promise for potential reuse
        window.polygonCache.pendingRequests[unitType] = fetchPromise;
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

                geojsonLayer = L.geoJSON([], {
                    style: styleFunction
                }).addTo(map);
                window.polygonCache.geojsonLayer = geojsonLayer;
            } catch (error) {
                console.error("Error creating new GeoJSON layer", error);
            }
        }

        return geojsonLayer;
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

        // Track which promises we need to wait for
        let fetchPromises = [];

        // Check if we need to fetch any new unit types
        unitTypes.forEach(unitType => {
            // Only start a new fetch if it's not already loaded or pending
            if (!window.polygonCache.loadedUnitTypes[unitType] &&
                !window.polygonCache.pendingRequests[unitType]) {
                // Need to fetch this unit type
                fetchPromises.push(this.fetchPolygons(unitType, yearRange));
            } else if (window.polygonCache.pendingRequests[unitType]) {
                // There's a pending request, so we need to wait for it
                fetchPromises.push(window.polygonCache.pendingRequests[unitType]);
            }
            // No need to push anything if it's already loaded
        });

        // Once all fetches are complete, update the map
        return Promise.all(fetchPromises)
            .then(() => {
                // Find the GeoJSON layer
                let geojsonLayer = this.findGeoJSONLayer(map);

                if (!geojsonLayer) {
                    console.error('GeoJSON layer not found in map');
                    return;
                }

                // Clear all existing layers first
                geojsonLayer.clearLayers();

                // Create a filter function to determine which features to display
                const filterFunction = function (feature) {
                    // First check if the feature belongs to one of the selected unit types
                    const hasSelectedUnitType = feature.properties &&
                        feature.properties.g_unit_type &&
                        unitTypes.includes(feature.properties.g_unit_type);

                    if (!hasSelectedUnitType) {
                        return false;
                    }

                    // Then check if we should display this feature based on selection status
                    const isSelected = feature.id && selectedPolygons.includes(feature.id);
                    return isSelected || showUnselected;
                };

                // Create a GeoJSON object with all features
                const allFeatures = Object.values(window.polygonCache.allFeatures);

                // Create a filtered GeoJSON object
                const filteredGeoJSON = {
                    type: "FeatureCollection",
                    features: allFeatures.filter(filterFunction)
                };

                console.log(`Adding ${filteredGeoJSON.features.length} filtered features to map`);

                // Instead of using geojsonLayer.addData directly,
                // we'll create a new GeoJSON layer with the style function applied
                // and then add its layers to our main layer

                // First check if the original geojsonLayer has a style function in its options
                const styleFunction = geojsonLayer.options && geojsonLayer.options.style;

                // Create a new temporary GeoJSON layer with proper styling
                const tempLayer = L.geoJSON(filteredGeoJSON, {
                    style: styleFunction,
                    // Copy other options that might be important
                    pointToLayer: geojsonLayer.options && geojsonLayer.options.pointToLayer,
                    onEachFeature: geojsonLayer.options && geojsonLayer.options.onEachFeature
                });

                // Add each of the styled layers to our main layer
                tempLayer.eachLayer(layer => {
                    geojsonLayer.addLayer(layer);
                });

                // Update the hideout property with selected IDs
                if (geojsonLayer.options && geojsonLayer.options.hideout) {
                    geojsonLayer.options.hideout.selected = selectedPolygons;
                } else {
                    // If hideout doesn't exist, create it
                    if (!geojsonLayer.options) {
                        geojsonLayer.options = {};
                    }
                    geojsonLayer.options.hideout = { selected: selectedPolygons };
                }

                // Force a style refresh on all layers to ensure proper highlighting
                this.refreshLayerStyles(geojsonLayer, selectedPolygons);

                return filteredGeoJSON;
            });
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

            // Call the updateMap function
            window.polygon_management.updateMap(map, unitTypes, yearRange, selectedPolygons, showUnselected)
                .then(filteredGeoJSON => {
                    // Success - the map has been updated directly
                    console.log(`Map updated successfully with ${filteredGeoJSON.features.length} features`);
                })
                .catch(error => {
                    console.error('Error updating map:', error);
                });

            // Create a debug message
            const debugMsg = `Showing polygons for ${unitTypes.join(', ')}`;

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