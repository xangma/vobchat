// app/assets/polygon_management.js

window.mapEventListenersAttached = false;
window.lastZoomEndTime_MapEvents = 0;
window.attachedMapId_Dbg = null;
window.programmaticZoomInProgress = false; // Global flag for zoom state
window.geojsonLayerReady = false; // Flag for layer readiness

// Add init hook to make map accessible from the container
L.Map.addInitHook(function () {
    this.getContainer()._leaflet_map = this;
    // Use 'whenReady' which fires once map is initialized (container size known, etc.)
    // 'load' fires after initial tiles load, which might be later than needed.
    this.whenReady(function() { // Changed from 'once('load', ...)'
        console.log("JS: Map is ready (whenReady event), initializing polygon management.");
        // REMOVED: setTimeout(() => { ... }, 500);
        initializeMapLayers(this); // Initialize directly
    });
});

// REMOVED waitForFeatures function - Zoom will be attempted directly after initial load

function setupMapEventListeners(map) {
    if (window.mapEventListenersAttached) {
        console.log("JS (setupMapEventListeners): Listeners already attached.");
        return;
    }
    if (!map || typeof map.on !== 'function') {
        console.error("JS setupMapEventListeners: Invalid map object provided.");
        return;
    }

    console.log("JS (setupMapEventListeners): Attaching listeners...");
    window.attachedMapId_Dbg = map._leaflet_id; // Store ID for debugging

    // Shared function to trigger Cb7 (data refresh callback)
    const triggerCb7Update = function (eventName) {
        // Check if dash_clientside is available before using it
        if (window.dash_clientside && window.dash_clientside.set_props) {
            console.log(`JS (Map Event - ${eventName}): Event detected. Triggering Cb7 via store.`);
            try {
                // Update store which acts as input for Cb7
                window.dash_clientside.set_props("map-moveend-trigger", { data: Date.now() });
            } catch (err) {
                console.error(`JS (Map Event - ${eventName}): Error in set_props for trigger:`, err);
            }
        } else {
            console.error(`JS (Map Event - ${eventName}): dash_clientside.set_props not available!`);
        }
    };

    // Attach zoomend listener
    map.on('zoomend', function (e) {
        // console.log("JS: >>> map.on('zoomend') event FIRED! <<<"); // Reduce noise
        // *** Check global flag: Ignore if programmatic zoom was *just initiated* ***
        if (window.programmaticZoomInProgress) {
            console.log("JS: map.on('zoomend') event IGNORED (programmatic zoom flag is true).");
            return; // Skip if Cb8 just called fitBounds and hasn't reset the flag yet
        }
        // If the flag is false, it means zoom was manual OR programmatic zoom finished previously
        console.log("JS: map.on('zoomend') event processed (flag is false).");
        window.lastZoomEndTime_MapEvents = Date.now(); // For moveend debounce
        triggerCb7Update('zoomend');
    });
    console.log("JS (setupMapEventListeners): zoomend listener attached.");

    // Attach moveend listener
    map.on('moveend', function (e) {
        // console.log("JS: >>> map.on('moveend') event FIRED! <<<"); // Reduce noise
        // *** Check global flag: Ignore if programmatic zoom was *just initiated* ***
        if (window.programmaticZoomInProgress) {
            console.log("JS: map.on('moveend') event IGNORED (programmatic zoom flag is true).");
            return; // Skip if Cb8 just called fitBounds and hasn't reset the flag yet
        }
        console.log("JS: map.on('moveend') event processed (flag is false).");

        // Debounce check (don't fire moveend if zoomend just fired recently)
        const now = Date.now();
        if (typeof window.lastZoomEndTime_MapEvents !== 'number') window.lastZoomEndTime_MapEvents = 0;
        if (window.lastZoomEndTime_MapEvents && (now - window.lastZoomEndTime_MapEvents < 200)) { // 200ms debounce window
            console.log("JS: map.on('moveend') event SKIPPED (debounce after recent zoomend).");
            return; // Don't trigger Cb7 again if zoomend just did
        }
        triggerCb7Update('moveend (not debounced)');
    });
    console.log("JS (setupMapEventListeners): moveend listener attached.");

    window.mapEventListenersAttached = true;
    console.log("JS (setupMapEventListeners): All listeners attached successfully.");
}


function initializeMapLayers(map) {
    if (!window.polygon_management) {
        console.warn("JS: polygon_management object not initialized yet, retrying init...");
        // Keep retry logic, potentially shorten interval
        setTimeout(() => initializeMapLayers(map), 150); // Shortened interval
        return;
    }

    const geojsonLayer = window.polygon_management.findGeoJSONLayer(map);
    if (!geojsonLayer) {
        console.log("JS: GeoJSON layer not found during init, will retry...");
         // Keep retry logic, potentially shorten interval
        setTimeout(() => initializeMapLayers(map), 150); // Shortened interval
        return;
    }

    // Only proceed if layer is found
    window.geojsonLayerReady = true;
    console.log("JS: GeoJSON layer found and ready.");

    // *** Setup map event listeners now that map and layer seem ready ***
    setupMapEventListeners(map); // Pass the map instance

    // --- Perform Initial Data Load ---
    let initialState = null;
    try {
        // Attempt to get state synchronously if available
        initialState = window.dash_clientside.store.getState()["map-state"];
    } catch (e) {
        console.warn("JS: Could not get initial map-state from Dash store during init.", e);
    }

    // Define default initial state if store is empty or lacks data
    const defaultUnitTypes = ['MOD_REG'];
    const currentYear = new Date().getFullYear();
    const defaultYearRange = { min: currentYear, max: currentYear };
    const defaultShowUnselected = true;
    const defaultSelected = [];

    const initialUnitTypes = initialState?.unit_types?.length ? initialState.unit_types : defaultUnitTypes;
    const initialYearRange = initialState?.year_range ? { min: initialState.year_range[0], max: initialState.year_range[1] } : defaultYearRange;
    const initialMapStateForLoad = {
        unit_types: initialUnitTypes,
        year_range: initialState?.year_range || [defaultYearRange.min, defaultYearRange.max],
        selected_polygons: initialState?.selected_polygons || defaultSelected,
        show_unselected: initialState?.show_unselected ?? defaultShowUnselected
    };

    console.log("JS: Performing initial data load with state:", initialMapStateForLoad);

    const bounds = map.getBounds(); // Use current map bounds for initial load

    // Fetch initial data
    window.polygon_management.updateMapWithBounds(map, initialUnitTypes, bounds, initialMapStateForLoad, initialYearRange)
        .then(() => {
            console.log("JS: Initial polygon load complete.");
            // *** REMOVED: waitForFeatures call ***
            // Try zooming immediately after the promise resolves
            const layer = window.polygon_management.findGeoJSONLayer(map); // Get layer ref again just in case
             // Check if layer has features before zooming
             if (layer && layer._layers && Object.keys(layer._layers).length > 0) {
                 console.log("JS: Zooming to initial features after load.");
                 window.polygon_management.zoomTo(map, null, layer); // Zoom to all loaded features
             } else {
                  console.log("JS: No initial features loaded or layer empty, skipping initial zoom.");
             }
        })
        .catch(err => {
            console.error("JS: Error during initial polygon load:", err);
        });
}


// Global polygon cache (NO CHANGE)
window.polygonCache = {
    featureById: {},
    featuresByUnitType: {},
    pendingRequests: {},
    geojsonLayer: null
};

window.polygon_management = {
    // getBoundsKey (NO CHANGE)
    getBoundsKey: function (bounds) {
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();
        return [
            sw.lng.toFixed(4), sw.lat.toFixed(4),
            ne.lng.toFixed(4), ne.lat.toFixed(4)
        ].join('_');
    },

    // getCachedFeatureIds (NO CHANGE)
    getCachedFeatureIds: function (unitTypes) {
        const cachedIds = new Set();
        unitTypes.forEach(unitType => {
            const idSet = window.polygonCache.featuresByUnitType[unitType]; // Should be a Set
            if (idSet instanceof Set) {
                 idSet.forEach(id => cachedIds.add(id));
            } else if (Array.isArray(idSet)) { // Fallback if somehow it's an array
                 idSet.forEach(id => cachedIds.add(id));
            }
        });
        return Array.from(cachedIds);
    },


    // refreshLayerStyles (NO CHANGE)
    refreshLayerStyles: function (geojsonLayer, selectedPolygons) {
        if (!geojsonLayer || !geojsonLayer._layers) {
            console.warn("JS: refreshLayerStyles: GeoJSON layer or internal _layers not found.");
            return;
        }
        const currentSelection = Array.isArray(selectedPolygons) ? selectedPolygons : [];
        let styleFunction = null;
        if (window.map_leaflet && typeof window.map_leaflet.style_function === 'function') {
            styleFunction = window.map_leaflet.style_function;
        } else if (geojsonLayer.options && typeof geojsonLayer.options.style === 'function') {
            styleFunction = geojsonLayer.options.style;
        } else {
            console.error("JS: refreshLayerStyles: No style function found.");
            return;
        }
        const context = { hideout: { selected: currentSelection } };
        let appliedCount = 0;
        Object.values(geojsonLayer._layers).forEach(layer => {
            if (layer.feature && typeof layer.setStyle === 'function') {
                try {
                    const style = styleFunction(layer.feature, context);
                    if (style) {
                        layer.setStyle(style);
                        appliedCount++;
                    } else {
                         // console.warn("JS: Style function returned undefined for feature:", layer.feature.id); // Reduce noise
                    }
                } catch (error) {
                    console.error("JS: refreshLayerStyles: Error applying style to feature:", layer.feature.id, error);
                }
            }
        });
        // console.log(`JS: refreshLayerStyles: Applied styles to ${appliedCount} layers.`); // Reduce noise
    },

    // fetchPolygonsByBounds (NO CHANGE - Fetch logic remains the same)
    fetchPolygonsByBounds: function (unitTypes, bounds, cachedIds, yearRange) {
        if (!unitTypes || !unitTypes.length || !bounds) {
            return Promise.reject('JS fetchPolygonsByBounds: Missing parameters');
        }
        const sw = bounds.getSouthWest();
        const ne = bounds.getNorthEast();
        const requestId = `req-${Date.now().toString(36)}-${Math.random().toString(36).substring(2, 5)}`;
        const excludeIds = Array.isArray(cachedIds) ? cachedIds : [];
        const boundsKey = this.getBoundsKey(bounds);
        const requestKey = `${unitTypes.join(',')}|${boundsKey}|${yearRange ? `${yearRange.min}-${yearRange.max}`: 'any'}`; // Add year to key
        // console.log(`JS (${requestId}): Fetch check for key: ${requestKey}`); // Reduce noise

        if (window.polygonCache.pendingRequests[requestKey]) {
            // console.log(`JS (${requestId}): Request already pending for ${requestKey}.`); // Reduce noise
            return window.polygonCache.pendingRequests[requestKey];
        }

        const baseParams = {
            minX: sw.lng, minY: sw.lat, maxX: ne.lng, maxY: ne.lat,
            request_id: requestId
        };
        if (yearRange && yearRange.min != null && yearRange.max != null) {
            baseParams.start_year = yearRange.min;
            baseParams.end_year = yearRange.max;
        }

        let fetchPromise;
        const usePost = excludeIds.length > 100;

        // console.log(`JS (${requestId}): Initiating fetch. Method: ${usePost ? 'POST' : 'GET'}. Excluding ${excludeIds.length} IDs.`); // Reduce noise

        if (usePost) {
            const payload = {
                unit_types: unitTypes,
                bounds: { minX: baseParams.minX, minY: baseParams.minY, maxX: baseParams.maxX, maxY: baseParams.maxY },
                exclude_ids: excludeIds,
                request_id: requestId
            };
            if (baseParams.start_year != null) payload.start_year = baseParams.start_year;
            if (baseParams.end_year != null) payload.end_year = baseParams.end_year;

            fetchPromise = fetch('/api/polygons/bbox', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
                body: JSON.stringify(payload)
            });
        } else {
            const urlParams = new URLSearchParams();
            urlParams.set('unit_types', unitTypes.join(','));
            Object.entries(baseParams).forEach(([key, value]) => urlParams.set(key, value));
            if (excludeIds.length > 0) {
                urlParams.set('exclude_ids', excludeIds.join(','));
            }
            const url = `/api/polygons/bbox?${urlParams.toString()}`;
            fetchPromise = fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            });
        }

        const processingPromise = fetchPromise.then(response => {
            if (!response.ok) {
                return response.text().then(text => {
                    throw new Error(`JS (${requestId}): Fetch failed: ${response.status} ${response.statusText}. Body: ${text}`);
                });
            }
            return response.json();
        })
            .then(data => {
                delete window.polygonCache.pendingRequests[requestKey];

                if (data.useCachedFeatures) {
                    // console.log(`JS (${requestId}): Server indicated use cached features.`); // Reduce noise
                    return { type: "FeatureCollection", features: [], fromCache: true };
                }

                if (!data || !Array.isArray(data.features)) {
                    console.warn(`JS (${requestId}): Received invalid data structure.`);
                    return { type: "FeatureCollection", features: [], fromCache: false };
                }

                // console.log(`JS (${requestId}): Received ${data.features.length} new polygons.`); // Reduce noise

                let addedToCache = 0;
                data.features.forEach(feature => {
                    if (feature && feature.id != null && feature.properties?.g_unit_type) {
                        const featureId = feature.id;
                        const unitType = feature.properties.g_unit_type;

                        if (!window.polygonCache.featureById[featureId]) {
                            window.polygonCache.featureById[featureId] = feature;
                            addedToCache++;
                        } else {
                            // Optional: Overwrite if needed, e.g., if fetched data is always newer
                            // window.polygonCache.featureById[featureId] = feature;
                        }

                        if (!window.polygonCache.featuresByUnitType[unitType]) {
                            window.polygonCache.featuresByUnitType[unitType] = new Set();
                        }
                        window.polygonCache.featuresByUnitType[unitType].add(featureId);
                    } else {
                        // console.warn(`JS (${requestId}): Skipping feature due to missing id/type:`, feature); // Reduce noise
                    }
                });
                 if (addedToCache > 0) {
                     // console.log(`JS (${requestId}): Added ${addedToCache} new features to cache.`); // Reduce noise
                 }

                return { ...data, fromCache: false };

            })
            .catch(error => {
                console.error(`JS (${requestId}): Error in fetchPolygonsByBounds chain:`, error);
                delete window.polygonCache.pendingRequests[requestKey];
                return Promise.reject(error);
            });

        window.polygonCache.pendingRequests[requestKey] = processingPromise;
        return processingPromise;
    },

    // findGeoJSONLayer (NO CHANGE)
    findGeoJSONLayer: function (map) {
        if (window.polygonCache.geojsonLayer && map.hasLayer(window.polygonCache.geojsonLayer)) {
            return window.polygonCache.geojsonLayer;
        }
        // console.log("JS: Searching for GeoJSON layer..."); // Reduce noise
        const geoJSONElement = document.getElementById('geojson-layer');
        if (geoJSONElement && geoJSONElement._leaflet_id && map._layers[geoJSONElement._leaflet_id]) {
            window.polygonCache.geojsonLayer = map._layers[geoJSONElement._leaflet_id];
            // console.log("JS: Found GeoJSON layer by ID:", window.polygonCache.geojsonLayer._leaflet_id); // Reduce noise
            return window.polygonCache.geojsonLayer;
        }

        // console.log("JS: GeoJSON layer ID not found, searching all map layers..."); // Reduce noise
        let foundLayer = null;
        map.eachLayer(layer => {
            if (layer instanceof L.FeatureGroup && layer.options && (layer.options.style || layer.options.onEachFeature)) {
                // console.log("JS: Found potential GeoJSON layer (FeatureGroup) by options:", layer); // Reduce noise
                foundLayer = layer;
                return;
            }
        });

        if (foundLayer) {
            // console.log("JS: Found GeoJSON layer via layer iteration."); // Reduce noise
            window.polygonCache.geojsonLayer = foundLayer;
        } else {
            console.warn("JS: Could not find GeoJSON layer on the map!");
            window.polygonCache.geojsonLayer = null;
        }
        return window.polygonCache.geojsonLayer;
    },

    // calculateBounds (NO CHANGE)
    calculateBounds: function (features) {
        if (!features || features.length === 0) return null;
        if (typeof dash_leaflet === 'undefined' || typeof dash_leaflet.geoJSON !== 'function') {
            console.error("JS: calculateBounds requires 'dash_leaflet'.");
            return null;
        }
        let bounds = null;
        features.forEach(feature => {
            if (feature?.geometry) {
                try {
                    const tempLayer = dash_leaflet.geoJSON(feature);
                    const featureBounds = tempLayer.getBounds();
                    if (bounds) {
                        bounds.extend(featureBounds);
                    } else {
                        bounds = featureBounds;
                    }
                } catch (e) {
                    console.error("JS: calculateBounds - Error processing feature:", feature.id, e);
                }
            }
        });
        return bounds;
    },

    // zoomTo (NO CHANGE - Logic is fine, relies on fitBounds)
    zoomTo: function (map, selectedIds = null, geojsonLayer = null) {
        const layerToUse = geojsonLayer || this.findGeoJSONLayer(map);

        if (!layerToUse || !layerToUse._layers) {
            console.warn("JS: zoomTo: GeoJSON layer not found or has no features.");
            return;
        }

        const targetBounds = L.latLngBounds();
        let featuresFound = 0;
        const useSelection = Array.isArray(selectedIds) && selectedIds.length > 0;

        Object.values(layerToUse._layers).forEach(layer => {
            if (layer.feature && typeof layer.getBounds === 'function') {
                 const includeFeature = !useSelection || (layer.feature.id != null && selectedIds.includes(layer.feature.id));
                 if (includeFeature) {
                    try {
                         targetBounds.extend(layer.getBounds());
                         featuresFound++;
                    } catch (e) {
                         console.warn("JS: zoomTo: Error getting bounds for layer/feature:", layer.feature?.id, e);
                    }
                 }
            }
        });

        if (featuresFound > 0 && targetBounds.isValid()) {
            console.log(`JS: zoomTo: Zooming to bounds of ${featuresFound} features.`);
            map.fitBounds(targetBounds, {
                padding: [20, 20], // Small padding
                maxZoom: 14,      // Limit max zoom level
                animate: true,    // Use animation
                duration: 0.5     // Animation duration (seconds)
            });
             // *** REMOVED setting skipNextMoveend flag here ***
             // The programmaticZoomInProgress flag handles event skipping now.
        } else {
            console.warn("JS: zoomTo: No valid features found to zoom to.");
        }
    },


    // updateMapWithBounds (NO CHANGE - Logic is the core update mechanism)
    updateMapWithBounds: function (map, unitTypes, bounds, mapState, yearRange) {
        const requestId = `update-${Date.now().toString(36)}`;
        // console.log(`JS (${requestId}): updateMapWithBounds called. UnitTypes: ${unitTypes.join(',')}`); // Reduce noise

        if (!map || !unitTypes || !unitTypes.length || !bounds || !mapState) {
            console.error(`JS (${requestId}): Missing required parameters for updateMapWithBounds.`);
            return Promise.reject('Missing parameters');
        }
        if (!window.geojsonLayerReady) {
            console.warn(`JS (${requestId}): GeoJSON layer not ready, aborting update.`);
            return Promise.resolve({ type: "FeatureCollection", features: [] }); // Resolve empty
        }

        const geojsonLayer = this.findGeoJSONLayer(map);
        if (!geojsonLayer) {
            console.error(`JS (${requestId}): GeoJSON layer not found in map.`);
            return Promise.reject('No GeoJSON layer found');
        }

        const selectedPolygons = mapState.selected_polygons || [];
        const showUnselected = mapState.show_unselected ?? true;
        // console.log(`JS (${requestId}): Selection count: ${selectedPolygons.length}, Show unselected: ${showUnselected}`); // Reduce noise

        const cachedIds = this.getCachedFeatureIds(unitTypes);
        // console.log(`JS (${requestId}): Found ${cachedIds.length} cached features for requested types.`); // Reduce noise

        return this.fetchPolygonsByBounds(unitTypes, bounds, cachedIds, yearRange)
            .then(fetchedData => {
                // console.log(`JS (${requestId}): Fetch completed. New features: ${fetchedData.features?.length ?? 0}.`); // Reduce noise

                const displayFeaturesMap = new Map();

                // 1. Add relevant features from cache
                unitTypes.forEach(unitType => {
                    const featureIdSet = window.polygonCache.featuresByUnitType[unitType];
                     if (featureIdSet instanceof Set) {
                         featureIdSet.forEach(id => {
                             const feature = window.polygonCache.featureById[id];
                             if (feature && !displayFeaturesMap.has(id)) {
                                 displayFeaturesMap.set(id, feature);
                             }
                         });
                     }
                });
                // console.log(`JS (${requestId}): Added ${displayFeaturesMap.size} features from cache.`); // Reduce noise

                // 2. Add newly fetched features
                if (fetchedData.features && fetchedData.features.length > 0) {
                    let newFeaturesAdded = 0;
                    fetchedData.features.forEach(feature => {
                        if (feature && feature.id != null) {
                            if (!displayFeaturesMap.has(feature.id)) newFeaturesAdded++;
                            displayFeaturesMap.set(feature.id, feature);
                        }
                    });
                     // console.log(`JS (${requestId}): Added/updated ${newFeaturesAdded} features from fetch.`); // Reduce noise
                }

                // 3. Filter based on selection display preference
                let finalFeatures = Array.from(displayFeaturesMap.values());
                const initialCount = finalFeatures.length;

                if (!showUnselected) {
                    finalFeatures = finalFeatures.filter(feature =>
                        feature.id != null && selectedPolygons.includes(feature.id)
                    );
                     // console.log(`JS (${requestId}): Filtered to show only selected. Kept ${finalFeatures.length} / ${initialCount}.`); // Reduce noise
                } else {
                    // console.log(`JS (${requestId}): Showing all ${initialCount} features.`); // Reduce noise
                }

                // --- Update Map Layer ---
                // console.log(`JS (${requestId}): Clearing existing GeoJSON layer.`); // Reduce noise
                geojsonLayer.clearLayers();

                if (finalFeatures.length > 0) {
                    const completeGeodata = {
                        type: "FeatureCollection",
                        features: finalFeatures
                    };
                    const geoJsonOptions = {
                        ...(geojsonLayer.options || {}),
                        hideout: { selected: selectedPolygons } // Ensure hideout is current
                    };
                    geojsonLayer.options = geoJsonOptions; // Set options *before* adding data? Or after? Let's try before.

                    // console.log(`JS (${requestId}): Adding ${finalFeatures.length} features.`); // Reduce noise
                    geojsonLayer.addData(completeGeodata);

                    // Force style refresh AFTER data is added, using the current selection context
                    // console.log(`JS (${requestId}): Forcing style refresh.`); // Reduce noise
                    this.refreshLayerStyles(geojsonLayer, selectedPolygons);

                } else {
                     // console.log(`JS (${requestId}): No features to display after filtering.`); // Reduce noise
                    // Update hideout even if empty
                    geojsonLayer.options = { ...(geojsonLayer.options || {}), hideout: { selected: selectedPolygons } };
                }

                 // console.log(`JS (${requestId}): updateMapWithBounds complete.`); // Reduce noise
                return { type: "FeatureCollection", features: finalFeatures };

            })
            .catch(error => {
                console.error(`JS (${requestId}): Error during updateMapWithBounds process:`, error);
                return Promise.reject(error);
            });
    },

}; // End of window.polygon_management