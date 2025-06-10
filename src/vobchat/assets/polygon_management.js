// app/assets/polygon_management.js

window.mapEventListenersAttached = false;
window.lastZoomEndTime_MapEvents = 0;
window.attachedMapId_Dbg = null;
window.programmaticZoomInProgress = false; // Global flag for zoom state
window.geojsonLayerReady = false; // Flag for layer readiness
window.programmaticZoomAnimating = false;

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

function setupMapEventListeners(map) {
    if (window.mapEventListenersAttached && window.attachedMapId_Dbg === map._leaflet_id) {
        // console.log("JS (setupMapEventListeners): Listeners already attached for this map."); // Less verbose
        return;
    }
    if (!map || typeof map.on !== 'function') {
        console.error("JS setupMapEventListeners: Invalid map object provided.");
        return;
    }

    console.log("JS (setupMapEventListeners): Attaching listeners...");
    window.attachedMapId_Dbg = map._leaflet_id;

    // --- Detach existing listeners first (robustness) ---
    map.off('zoomend');
    map.off('moveend');
    console.log("JS (setupMapEventListeners): Detached any previous listeners.");

    // Shared function to trigger Cb7 (data refresh callback)
    const triggerCb7Update = function (eventName) {
        if (window.dash_clientside && window.dash_clientside.set_props) {
            console.log(`JS (Map Event - ${eventName}): Triggering Cb7 via store.`);
            try {
                window.dash_clientside.set_props("map-moveend-trigger", { data: Date.now() });
            } catch (err) {
                console.error(`JS (Map Event - ${eventName}): Error in set_props for trigger:`, err);
            }
        } else {
            console.error(`JS (Map Event - ${eventName}): dash_clientside.set_props not available!`);
        }
    };

    map.on('zoomend', function (e) {
        console.log("JS: map.on('zoomend') event FIRED!");

        // Check if this zoomend corresponds to the *end* of our programmatic zoom animation
        if (window.programmaticZoomAnimating) {
            console.log("JS: map.on('zoomend') - Detected COMPLETION of programmatic zoom animation.");

            // Reset the JS flags ONLY
            window.programmaticZoomAnimating = false;
            window.programmaticZoomInProgress = false; // Reset general flag too
            console.log("JS: map.on('zoomend') - Reset JS programmatic zoom flags.");

            // CRITICAL: Trigger cleanup callback now that zoom is truly complete
            // This ensures Cb10 runs at the right time to clear zoom_to_selection flag
            if (window.dash_clientside && window.dash_clientside.set_props) {
                try {
                    window._zoomCleanupCount = (window._zoomCleanupCount || 0) + 1;
                    console.log(
                        "JS: map.on('zoomend') - Triggering delayed cleanup via store – count:",
                        window._zoomCleanupCount
                    );
                    window.dash_clientside.set_props("zoom-cleanup-trigger-store", {
                        data: {
                            timestamp: Date.now(),
                            triggered_by_cb8: true,
                            zoom_completed: true
                        }
                    });
                } catch (err) {
                    console.error("JS: map.on('zoomend') - Error triggering cleanup:", err);
                }
            }

            // Prevent default Cb7 trigger after programmatic zoom
            return;
        }

        // --- Handle Manual Zooms ---
        if (!window.programmaticZoomInProgress && !window.programmaticZoomAnimating) {
            console.log("JS: map.on('zoomend') - Processing as manual zoom.");
            window.lastZoomEndTime_MapEvents = Date.now();
            triggerCb7Update('zoomend (manual)'); // Trigger Cb7 for manual zooms
        } else {
            console.log("JS: map.on('zoomend') - Event ignored (flags indicate programmatic zoom still in progress?).");
        }
    });
    console.log("JS (setupMapEventListeners): SIMPLIFIED zoomend listener attached.");

    // --- REVISED moveend listener ---
    map.on('moveend', function (e) {
        console.log("JS: map.on('moveend') event FIRED!");

        // STRONGER CHECK: Ignore moveend if a programmatic zoom was *just* animating or is marked as in progress
        if (window.programmaticZoomAnimating || window.programmaticZoomInProgress) {
            console.log("JS: map.on('moveend') event IGNORED (programmatic zoom flags are true).");
            return;
        }

        // Debounce check (keep existing logic, but might be less critical now)
        const now = Date.now();
        if (typeof window.lastZoomEndTime_MapEvents !== 'number') window.lastZoomEndTime_MapEvents = 0;
        if (window.lastZoomEndTime_MapEvents && (now - window.lastZoomEndTime_MapEvents < 250)) { // Slightly increased debounce
            console.log("JS: map.on('moveend') event SKIPPED (debounce after recent zoomend).");
            return;
        }

        console.log("JS: map.on('moveend') - Processing event.");
        triggerCb7Update('moveend (debounced/allowed)');
    });
    console.log("JS (setupMapEventListeners): REVISED moveend listener attached.");

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
        let selectedCount = 0;

        // Debug: Add stack trace to see which function called this
        const stack = new Error().stack;
        const caller = stack.split('\n')[2] ? stack.split('\n')[2].trim() : 'unknown';
        console.log(`JS: refreshLayerStyles: Processing ${Object.keys(geojsonLayer._layers).length} layers with selection:`, currentSelection, `Called from: ${caller}`);

        Object.values(geojsonLayer._layers).forEach(layer => {
            if (layer.feature && typeof layer.setStyle === 'function') {
                const featureId = layer.feature.id;
                const featureIdStr = String(featureId);
                const featureName = layer.feature.properties?.g_unit_name || 'unnamed';
                const isSelected = currentSelection.includes(featureIdStr);
                if (isSelected) selectedCount++;

                try {
                    const style = styleFunction(layer.feature, context);
                    if (style) {
                        layer.setStyle(style);
                        appliedCount++;
                    } else {
                        console.warn("JS: Style function returned undefined for feature:", layer.feature.id);
                    }
                } catch (error) {
                    console.error("JS: refreshLayerStyles: Error applying style to feature:", layer.feature.id, error);
                }
            }
        });
        console.log(`JS: refreshLayerStyles: Applied styles to ${appliedCount} layers, ${selectedCount} were selected.`);
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

    // Debug function to inspect cache contents
    debugCacheContents: function() {
        console.log("JS: DEBUG - Cache contents:");
        console.log(`JS: DEBUG - featureById keys: [${Object.keys(window.polygonCache.featureById).join(', ')}]`);
        Object.entries(window.polygonCache.featuresByUnitType).forEach(([unitType, idSet]) => {
            const ids = idSet instanceof Set ? Array.from(idSet) : (Array.isArray(idSet) ? idSet : []);
            console.log(`JS: DEBUG - ${unitType}: [${ids.join(', ')}]`);
        });
    },

    fetchPolygonsByIds: function (map, mapState, unitType, ids, yearRange = null, selectedPolygons = null) { // Added selectedPolygons parameter
        const requestId = `req-id-${Date.now().toString(36)}`;
        console.log(`JS (${requestId}): Fetching polygons by ID. UnitType: ${unitType}, Count: ${ids.length}`);

        // Debug cache contents before processing
        this.debugCacheContents();

        if (!map) {
            console.error(`JS (${requestId}): Invalid map object provided to fetchPolygonsByIds.`);
            return Promise.reject('Invalid map object');
        }
        if (!unitType || !ids || ids.length === 0) {
            console.warn(`JS (${requestId}): Missing unitType or ids for fetchPolygonsByIds.`);
            return Promise.resolve({ type: "FeatureCollection", features: [] });
        }

        const featuresToAdd = [];
        const idsToActuallyFetch = [];

        // Check cache first and prepare list of IDs that *really* need fetching
        ids.forEach(id => {
            const cachedFeature = window.polygonCache.featureById[id];
            console.log(`JS (${requestId}): DEBUG - Checking cache for ID ${id}: ${cachedFeature ? 'FOUND' : 'NOT FOUND'}`);
            if (cachedFeature) {
                console.log(`JS (${requestId}): DEBUG - Adding cached feature ${id} (${cachedFeature.properties?.g_unit_name || 'unnamed'}) to featuresToAdd`);
                featuresToAdd.push(cachedFeature); // Add from cache
            } else {
                console.log(`JS (${requestId}): DEBUG - ID ${id} needs fetching from API`);
                idsToActuallyFetch.push(id); // Needs fetching
            }
        });

        let fetchPromise;
        if (idsToActuallyFetch.length === 0) {
            console.log(`JS (${requestId}): All requested IDs found in cache.`);
            fetchPromise = Promise.resolve({ type: "FeatureCollection", features: [], fromCache: true }); // Nothing new to fetch from API
        } else {
            // --- Prepare API Call ---
            const urlParams = new URLSearchParams();
            urlParams.set('ids', idsToActuallyFetch.join(','));
            urlParams.set('unit_type', unitType);
            if (yearRange && yearRange.min != null && yearRange.max != null) {
                urlParams.set('start_year', yearRange.min);
                urlParams.set('end_year', yearRange.max);
            }
            const url = `/api/polygons/ids?${urlParams.toString()}`;

            console.log(`JS (${requestId}): Calling API for ${idsToActuallyFetch.length} IDs: ${url}`);
            fetchPromise = fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            })
                .then(response => {
                    if (!response.ok) {
                        return response.text().then(text => {
                            throw new Error(`JS (${requestId}): Fetch by ID failed: ${response.status} ${response.statusText}. Body: ${text}`);
                        });
                    }
                    return response.json();
                })
                .then(data => {
                    if (!data || !Array.isArray(data.features)) {
                        console.warn(`JS (${requestId}): Received invalid data structure from ID fetch.`);
                        return { type: "FeatureCollection", features: [], fromCache: false };
                    }
                    console.log(`JS (${requestId}): Received ${data.features.length} polygons from ID fetch API call.`);
                    return { ...data, fromCache: false }; // Pass fetched data downstream
                });
        }

        // Chain processing after fetch (or immediately if all cached)
        return fetchPromise.then(fetchedData => {
            // --- Update Cache with newly fetched data ---
            if (!fetchedData.fromCache && fetchedData.features.length > 0) {
                let addedToCacheCount = 0;
                fetchedData.features.forEach(feature => {
                    if (feature && feature.id != null && feature.properties?.g_unit_type) {
                        const featureId = feature.id;
                        const featUnitType = feature.properties.g_unit_type;
                        if (!window.polygonCache.featureById[featureId]) {
                            window.polygonCache.featureById[featureId] = feature;
                            addedToCacheCount++;
                        }
                        if (!window.polygonCache.featuresByUnitType[featUnitType]) {
                            window.polygonCache.featuresByUnitType[featUnitType] = new Set();
                        }
                        window.polygonCache.featuresByUnitType[featUnitType].add(featureId);
                        featuresToAdd.push(feature); // Add newly fetched to our list
                    }
                });
                if (addedToCacheCount > 0) {
                    console.log(`JS (${requestId}): Added ${addedToCacheCount} new features to cache from ID fetch.`);
                }
            }

            // --- Add Features to Map Layer ---
            const geojsonLayer = this.findGeoJSONLayer(map);
            if (!geojsonLayer) {
                console.error(`JS (${requestId}): GeoJSON layer not found, cannot add fetched features.`);
                return Promise.reject('GeoJSON layer not found');
            }

            const featuresActuallyAddedToLayer = [];
            if (featuresToAdd.length > 0) {
                // Filter out features already on the layer to avoid duplicates
                const currentLayerIds = new Set(Object.values(geojsonLayer._layers).map(l => l.feature?.id).filter(id => id != null).map(id => String(id)));
                console.log(`JS (${requestId}): DEBUG - Current layer IDs: [${Array.from(currentLayerIds).join(', ')}]`);
                console.log(`JS (${requestId}): DEBUG - Features to add IDs: [${featuresToAdd.map(f => f.id).join(', ')}]`);

                const featuresToAddFiltered = featuresToAdd.filter(f => {
                    const isAlreadyOnLayer = f.id != null && currentLayerIds.has(String(f.id));
                    console.log(`JS (${requestId}): DEBUG - Feature ${f.id}: already on layer = ${isAlreadyOnLayer}`);
                    return f.id != null && !isAlreadyOnLayer;
                });


                if (featuresToAddFiltered.length > 0) {
                    console.log(`JS (${requestId}): Adding ${featuresToAddFiltered.length} features (cached + fetched) to map layer.`);
                    const geoJsonData = { type: "FeatureCollection", features: featuresToAddFiltered };
                    geojsonLayer.addData(geoJsonData);
                    featuresActuallyAddedToLayer.push(...featuresToAddFiltered);

                } else {
                    console.log(`JS (${requestId}): All requested/fetched features were already on the layer.`);
                }
            } else {
                console.log(`JS (${requestId}): No features (cached or fetched) to add to layer.`);
            }

            // Return only the features that were actually added in this run
            return { type: "FeatureCollection", features: featuresActuallyAddedToLayer };
        })
            .catch(error => {
                console.error(`JS (${requestId}): Error in fetchPolygonsByIds processing:`, error);
                return Promise.resolve({ type: "FeatureCollection", features: [], error: true }); // Resolve empty on error
            });
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

    zoomTo: function (map, selectedIds = null, geojsonLayer = null) {
        const layerToUse = geojsonLayer || this.findGeoJSONLayer(map);

        if (!layerToUse || !layerToUse._layers) {
            console.warn("JS: zoomTo: GeoJSON layer not found or has no features.");
            // Reset flags if zoom cannot proceed
            window.programmaticZoomInProgress = false;
            window.programmaticZoomAnimating = false;
            return;
        }

        const targetBounds = L.latLngBounds();
        let featuresFound = 0;
        const useSelection = Array.isArray(selectedIds) && selectedIds.length > 0;

        console.log(`JS: zoomTo: DEBUG - Selected IDs for zoom: [${selectedIds ? selectedIds.join(', ') : 'ALL'}]`);
        console.log(`JS: zoomTo: DEBUG - Layer has ${Object.keys(layerToUse._layers).length} total features`);

        Object.values(layerToUse._layers).forEach(layer => {
            if (layer.feature && typeof layer.getBounds === 'function') {
                const featureId = layer.feature.id;
                const featureName = layer.feature.properties?.g_unit_name || 'unnamed';
                const includeFeature = !useSelection || (featureId != null && selectedIds.includes(String(featureId)));

                console.log(`JS: zoomTo: DEBUG - Feature ${featureId} (${featureName}): included = ${includeFeature}`);

                if (includeFeature) {
                    try {
                        targetBounds.extend(layer.getBounds());
                        featuresFound++;
                        console.log(`JS: zoomTo: DEBUG - Added feature ${featureId} to bounds (total: ${featuresFound})`);
                    } catch (e) {
                        console.warn("JS: zoomTo: Error getting bounds for layer/feature:", featureId, e);
                    }
                }
            } else {
                console.log(`JS: zoomTo: DEBUG - Skipping layer without feature or getBounds method`);
            }
        });


        if (featuresFound > 0 && targetBounds.isValid()) {
            console.log(`JS: zoomTo: Zooming to bounds of ${featuresFound} features.`);

            // *** Set animation flag JUST before calling fitBounds ***
            window.programmaticZoomAnimating = true;
            console.log("JS: zoomTo: SET programmaticZoomAnimating = true");

            map.fitBounds(targetBounds, {
                padding: [20, 20],
                maxZoom: 14,
                animate: true,
                duration: 0.5
            });
            // The zoomend handler will now reset programmaticZoomAnimating = false

        } else {
            console.warn("JS: zoomTo: No valid features found to zoom to.");
            // Reset flags if zoom doesn't happen
            window.programmaticZoomInProgress = false;
            window.programmaticZoomAnimating = false;
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
                        feature.id != null && selectedPolygons.includes(String(feature.id))
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
