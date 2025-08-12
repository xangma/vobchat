/**
 * polygon_management.js – cleaned and modularised version
 * -------------------------------------------------------
 * Dash‑Leaflet helper that keeps a client‑side cache of polygon features
 * and synchronises the layer with the map viewport / selection state.
 *
 * All module internals live in an IIFE to avoid leaking globals; only the
 * public API is attached to window.polygonManagement so Dash callbacks or
 * other assets can use it.
 *
 * Notable improvements vs. the original:
 *   • ES‑2020 syntax (const/let, arrow functions, optional‑chaining)
 *   • Single "flags" object instead of many scattered window.* vars
 *   • Centralised helper to read the <dcc.Store id="map-state"> element
 *   • Far less console noise – log levels are grouped behind LOG() helper
 *   • Debounce helper + utility functions extracted
 *   • Clear separation between *internal* helpers and *exported* API
 *
 * © 2025  – feel free to adapt as needed.
 */

(() => {
  'use strict';

  /* ──────────────────────────────────────────────────────────
     Constants + simple utilities
     ─────────────────────────────────────────────────────── */
  const LOG_PREFIX = '%c[polygon‑mgmt]';
  const LOG_STYLE = 'color:#4E9F3D;font-weight:600';
  const DEBOUNCE_MS = 250;
  const ZOOM_PADDING = [20, 20];
  const MAX_ZOOM = 14;

  const LOG = (...args) => console.log(LOG_PREFIX, LOG_STYLE, ...args);
  const WARN = (...args) => console.warn(LOG_PREFIX, LOG_STYLE, ...args);
  const ERR = (...args) => console.error(LOG_PREFIX, LOG_STYLE, ...args);

  const debounce = (fn, ms = DEBOUNCE_MS) => {
    let t;
    return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
  };

  /**
   * Extract selected polygon IDs from places array (single source of truth).
   * Mirrors the backend helper function get_selected_units().
   */
  const getSelectedPolygonsFromPlaces = (mapState) => {
    const places = mapState?.places || [];
    return places
      .filter(place => place.g_unit !== null && place.g_unit !== undefined)
      .map(place => String(place.g_unit));
  };

  /**
   * Grab the JSON payload of a `dcc.Store(id="map-state")` component.
   * Dash renders the store as a <script type="application/json"> element
   * whose textContent holds the serialised JSON.
   */
  const getMapState = () => {
    try {
      const el = document.getElementById('map-state');
      if (!el) return null;
      const txt = (el.textContent || '').trim();
      return txt ? JSON.parse(txt) : null;
    } catch (e) {
      WARN('Could not parse map‑state store', e);
      return null;
    }
  };

  /* ──────────────────────────────────────────────────────────
     Runtime flags & in‑memory cache
     ─────────────────────────────────────────────────────── */
  const flags = {
    listenersAttached: false,
    attachedMapId: null,
    lastZoomEnd: 0,
    programmaticZoomInProgress: false,
    programmaticZoomAnimating: false,
    geojsonLayerReady: false,
    suppressedMoveendCount: 0,
    suppressedThisZoomCount: 0,
    zoomSource: null,
    // instrumentation
    zoomSessionId: null,
    zoomSessionsStarted: 0,
    zoomSessionsCompleted: 0,
    zoomendProgrammaticCount: 0,
    zoomendUserCount: 0,
    moveendProcessedCount: 0,
    fetchByIdsCalls: 0,
    updateByBoundsCalls: 0,
    refreshStylesCalls: 0
  };

  /**
   * Cache structure:
   *   featureById:        { [id]: GeoJSONFeature }
   *   featuresByUnitType: { [unitType]: Set<id> }
   *   pendingRequests:    { [cacheKey]: Promise }
   *   geojsonLayer:       L.GeoJSON | null
   */
  const cache = {
    featureById: Object.create(null),
    featuresByUnitType: Object.create(null),
    pendingRequests: Object.create(null),
    geojsonLayer: null
  };

  /* ──────────────────────────────────────────────────────────
     Internal helpers – mostly pure functions
     ─────────────────────────────────────────────────────── */

  const getBoundsKey = bounds => {
    const sw = bounds.getSouthWest();
    const ne = bounds.getNorthEast();
    return [sw.lng, sw.lat, ne.lng, ne.lat].map(v => v.toFixed(4)).join('_');
  };

  const getCachedFeatureIds = unitTypes => {
    const ids = new Set();
    unitTypes.forEach(t => {
      const set = cache.featuresByUnitType[t];
      if (set) set.forEach(id => ids.add(id));
    });
    return [...ids];
  };

  const findGeoJSONLayer = map => {
    if (cache.geojsonLayer && map.hasLayer(cache.geojsonLayer)) return cache.geojsonLayer;

    // Dash usually injects the GeoJSON layer with id="geojson-layer"
    const el = document.getElementById('geojson-layer');
    console.log('findGeoJSONLayer:', el, map._layers);
    if (el && map._layers[el._leaflet_id]) {
      cache.geojsonLayer = map._layers[el._leaflet_id];
      return cache.geojsonLayer;
    }

    // Fallback: iterate layers to find a FeatureGroup with style / onEachFeature
    let found = null;
    map.eachLayer(l => {
      if (l instanceof L.FeatureGroup && (l.options?.style || l.options?.onEachFeature)) found = l;
    });
    cache.geojsonLayer = found;
    console.log('findGeoJSONLayer: found', found);
    return found;
  };

  /*
   * Style refresh helper – applies style function with current selection.
   */
  const refreshLayerStyles = (layer, selected) => {
    if (!layer || !layer._layers) return;
    const sel = Array.isArray(selected) ? selected : [];
    const ctx = { hideout: { selected: sel } };
    const styleFn = layer.options?.style || window.map_leaflet?.style_function;
    if (!styleFn) { ERR('No style function found to refresh layer styles'); return; }
    flags.refreshStylesCalls += 1;
    LOG(`refreshLayerStyles (#${flags.refreshStylesCalls}) — selected=${sel.length}`);
    Object.values(layer._layers).forEach(l => l.feature && l.setStyle(styleFn(l.feature, ctx)));
  };

  /* ──────────────────────────────────────────────────────────
     Map event listeners
     ─────────────────────────────────────────────────────── */

  const attachMapEventListeners = map => {
    if (flags.listenersAttached && flags.attachedMapId === map._leaflet_id) return;
    if (!map?.on) { ERR('Invalid map object passed to attachMapEventListeners'); return; }

    LOG('Attaching map listeners');
    flags.attachedMapId = map._leaflet_id;

    map.off('zoomend').off('moveend'); // remove any previous handlers

    /* zoomend */
    map.on('zoomend', () => {
      LOG('zoomend');
      if (flags.programmaticZoomAnimating || flags.programmaticZoomInProgress) {
        // End of animation: keep InProgress true until the following moveend,
        // so that moveend gets suppressed once.
        flags.programmaticZoomAnimating = false;
        flags.zoomendProgrammaticCount += 1;
        LOG(`zoomend (programmatic) — session=${flags.zoomSessionId} source=${flags.zoomSource} count=${flags.zoomendProgrammaticCount}; awaiting moveend to clear suppression`);
        // Notify Dash cleanup callback via hidden store
        window.dash_clientside?.set_props?.('zoom-cleanup-trigger-store', {
          data: { ts: Date.now(), zoom_completed: true }
        });
        return; // swallow event
      }
      // User-driven zoom end: trigger a moveend processing tick
      flags.zoomendUserCount += 1;
      flags.lastZoomEnd = Date.now();
      window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
    });

    /* moveend (debounced) */
    map.on('moveend', debounce(() => {
      // Suppress the first moveend after a programmatic zoom, then clear flag
      if (flags.programmaticZoomAnimating || flags.programmaticZoomInProgress) {
        flags.suppressedMoveendCount += 1;
        flags.suppressedThisZoomCount += 1;
        LOG(`moveend (suppressed during programmatic zoom) — session=${flags.zoomSessionId} source=${flags.zoomSource} thisZoom=${flags.suppressedThisZoomCount} total=${flags.suppressedMoveendCount}`);
        flags.programmaticZoomInProgress = false;
        flags.programmaticZoomAnimating = false;
        flags.zoomSessionsCompleted += 1;
        LOG(`programmatic zoom suppression cleared — session=${flags.zoomSessionId} source=${flags.zoomSource} suppressedThisZoom=${flags.suppressedThisZoomCount} sessions started=${flags.zoomSessionsStarted} completed=${flags.zoomSessionsCompleted}`);
        flags.zoomSource = null;
        flags.suppressedThisZoomCount = 0;
        flags.zoomSessionId = null;
        return;
      }
      LOG('moveend');
      flags.moveendProcessedCount += 1;
      LOG(`moveend processed (#${flags.moveendProcessedCount})`);
      window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
    }));

    flags.listenersAttached = true;
  };

  /* ──────────────────────────────────────────────────────────
     Feature fetching (by bounds and by IDs)
     ─────────────────────────────────────────────────────── */

  const fetchPolygonsByBounds = (unitTypes, bounds, cachedIds, yearRange) => {
    LOG(`fetchPolygonsByBounds — unitTypes=${unitTypes.join(',')} cachedIds=${cachedIds.length} yearRange=${yearRange ? yearRange.min+'-'+yearRange.max : 'none'}`);
    if (!unitTypes.length) return Promise.reject('No unitTypes supplied');
    const [sw, ne] = [bounds.getSouthWest(), bounds.getNorthEast()];
    const boundsObj = { minX: sw.lng, minY: sw.lat, maxX: ne.lng, maxY: ne.lat };

    const cacheKey = `${unitTypes.join(',')}|${getBoundsKey(bounds)}|${yearRange?.min ?? ''}-${yearRange?.max ?? ''}`;
    if (cache.pendingRequests[cacheKey]) return cache.pendingRequests[cacheKey];

    // Use POST if we have many cached IDs to avoid URL length limits
    // Estimate URL length: base URL + params + exclude_ids
    const excludeIdsParam = cachedIds.length > 0 ? cachedIds.join(',') : '';
    const estimatedUrlLength = 100 + excludeIdsParam.length; // rough estimate
    const usePost = estimatedUrlLength > 3000; // Leave some margin under the 4094 limit

    let fetchPromise;
    const url = new URL('/api/polygons/bbox', window.location.origin);

    if (usePost) {
      // Use POST with JSON body for large exclude_ids lists
      const postData = {
        unit_types: unitTypes,
        bounds: boundsObj,
        exclude_ids: cachedIds,
        ...(yearRange && { start_year: yearRange.min, end_year: yearRange.max })
      };

      fetchPromise = fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(postData)
      });
    } else {
      // Use GET for smaller requests
      Object.entries(boundsObj).forEach(([k, v]) => url.searchParams.set(k, v));
      url.searchParams.set('unit_types', unitTypes.join(','));
      if (cachedIds.length) url.searchParams.set('exclude_ids', excludeIdsParam);
      if (yearRange) {
        url.searchParams.set('start_year', yearRange.min);
        url.searchParams.set('end_year', yearRange.max);
      }

      fetchPromise = fetch(url, { headers: { Accept: 'application/json' } });
    }

    const startedAt = Date.now();
    const p = fetchPromise
      .then(r => r.ok ? r.json() : r.text().then(t => Promise.reject(`${r.status} ${t}`)))
      .then(data => {
        if (!Array.isArray(data?.features)) return { type: 'FeatureCollection', features: [] };
        data.features.forEach(f => {
          cache.featureById[f.id] = f;
          const t = f.properties?.g_unit_type;
          if (t) (cache.featuresByUnitType[t] ||= new Set()).add(f.id);
        });
        LOG(`fetchPolygonsByBounds: received ${data.features.length} features in ${Date.now()-startedAt}ms`);
        return data;
      })
      .finally(() => delete cache.pendingRequests[cacheKey]);

    cache.pendingRequests[cacheKey] = p;
    return p;
  };

  const fetchPolygonsByIds = (map, mapState, unitType, ids, yearRange) => {
    flags.fetchByIdsCalls += 1;
    LOG(`fetchPolygonsByIds call #${flags.fetchByIdsCalls} — unitType=${unitType} ids=${ids?.length}`);
    if (!ids.length) return Promise.resolve({ type: 'FeatureCollection', features: [] });

    const missing = ids.filter(id => !cache.featureById[id]);
    const existing = ids.filter(id => cache.featureById[id]).map(id => cache.featureById[id]);
    LOG(`fetchPolygonsByIds: have=${ids.length - missing.length} missing=${missing.length}`);

    const fetchPromise = !missing.length ? Promise.resolve({ features: [] }) : (() => {
      const url = new URL('/api/polygons/ids', window.location.origin);
      url.searchParams.set('ids', missing.join(','));
      url.searchParams.set('unit_type', unitType);
      if (yearRange) { url.searchParams.set('start_year', yearRange.min); url.searchParams.set('end_year', yearRange.max); }
      return fetch(url, { headers: { Accept: 'application/json' } })
        .then(r => r.ok ? r.json() : r.text().then(t => Promise.reject(`${r.status} ${t}`)));
    })();

    return fetchPromise.then(({ features = [] }) => {
      // merge newly fetched into cache
      features.forEach(f => {
        cache.featureById[f.id] = f;
        (cache.featuresByUnitType[f.properties?.g_unit_type] ||= new Set()).add(f.id);
      });
      // Combine existing-cached and newly fetched, de-duplicate by ID
      const combined = [...existing, ...features];
      const byId = Object.create(null);
      combined.forEach(f => { if (f && f.id != null) byId[String(f.id)] = f; });
      const uniqueFeatures = Object.values(byId);

      const layer = findGeoJSONLayer(map);
      if (layer) {
        // Remove any existing layer entries for the IDs we are about to add
        const idsToReplace = new Set(uniqueFeatures.map(f => String(f.id)));
        if (layer._layers) {
          Object.values(layer._layers).forEach(l => {
            if (l?.feature?.id != null && idsToReplace.has(String(l.feature.id))) {
              layer.removeLayer(l);
            }
          });
        }

        // Add unique features
        if (uniqueFeatures.length) {
          layer.addData({ type: 'FeatureCollection', features: uniqueFeatures });
        }
      }
      const selectedPolygons = getSelectedPolygonsFromPlaces(mapState);
      refreshLayerStyles(layer, selectedPolygons);
      LOG(`fetchPolygonsByIds: added=${uniqueFeatures.length} layerSize=${Object.keys(layer?._layers || {}).length}`);
      return { type: 'FeatureCollection', features: uniqueFeatures };
    });
  };

  /* ──────────────────────────────────────────────────────────
     Map update orchestrator
     ─────────────────────────────────────────────────────── */

  const updateMapWithBounds = (map, unitTypes, bounds, mapState, yearRange) => {
    flags.updateByBoundsCalls += 1;
    const key = getBoundsKey(bounds);
    LOG(`updateMapWithBounds call #${flags.updateByBoundsCalls} — unitTypes=${unitTypes.join(',')} bounds=${key}`);
    const layer = findGeoJSONLayer(map);
    if (!layer) return Promise.reject('GeoJSON layer not ready');

    const cachedIds = getCachedFeatureIds(unitTypes);
    return fetchPolygonsByBounds(unitTypes, bounds, cachedIds, yearRange)
      .then(({ features }) => {
        const selected = getSelectedPolygonsFromPlaces(mapState);
        const showUnselected = mapState.show_unselected ?? true;
        const merged = [
          ...features,
          ...cachedIds.map(id => cache.featureById[id]).filter(Boolean)
        ];
        const toDisplay = showUnselected ? merged : merged.filter(f => selected.includes(String(f.id)));

        layer.clearLayers();
        if (toDisplay.length) layer.addData({ type: 'FeatureCollection', features: toDisplay });
        refreshLayerStyles(layer, selected);
        LOG(`updateMapWithBounds: displayed=${toDisplay.length} selected=${selected.length} show_unselected=${showUnselected}`);
        return { type: 'FeatureCollection', features: toDisplay };
      });
  };

  /* ──────────────────────────────────────────────────────────
     Zoom helper
     ─────────────────────────────────────────────────────── */

  const zoomTo = (map, selectedIds = null, layer = null) => {
    const lyr = layer || findGeoJSONLayer(map);
    if (!lyr?._layers) return;

    const ids = selectedIds?.map(String);
    const bounds = L.latLngBounds();
    let count = 0;
    Object.values(lyr._layers).forEach(l => {
      if (!l.feature || !l.getBounds) return;
      if (!ids || ids.includes(String(l.feature.id))) {
        bounds.extend(l.getBounds());
        count += 1;
      }
    });
    if (!count || !bounds.isValid()) return;

    // Mark programmatic zoom to suppress moveend-triggered auto-loads
    const newSessionId = `${Date.now()}-${Math.floor(Math.random()*10000)}`;
    flags.zoomSessionId = newSessionId;
    flags.zoomSessionsStarted += 1;
    // Capture and clear an external source tag if set
    try {
      if (typeof window !== 'undefined' && window._zoomSource) {
        flags.zoomSource = window._zoomSource;
        window._zoomSource = null;
      } else {
        flags.zoomSource = flags.zoomSource || 'unknown';
      }
    } catch (e) {
      flags.zoomSource = 'unknown';
    }
    flags.suppressedThisZoomCount = 0;
    LOG(`zoomTo: starting programmatic zoom — session=${newSessionId} source=${flags.zoomSource} ids=${ids?.length ?? 'all'}`);
    flags.programmaticZoomInProgress = true;
    flags.programmaticZoomAnimating = true;
    map.fitBounds(bounds, { padding: ZOOM_PADDING, maxZoom: MAX_ZOOM, animate: true, duration: 0.5 });
  };

  /* ──────────────────────────────────────────────────────────
     Module initialisation hook – runs for every map instance
     ─────────────────────────────────────────────────────── */

  L.Map.addInitHook(function () {
    this.getContainer()._leaflet_map = this; // expose map for Dash tests/debug
    this.whenReady(() => {
      LOG('Map ready – initialising polygon management');
      initialise(this);
    });
  });

  const initialise = map => {
    // Wait until the GeoJSON layer appears, then finish setup
    const waitLayer = () => {
      const layer = findGeoJSONLayer(map);
      if (!layer) return setTimeout(waitLayer, 120);
      flags.geojsonLayerReady = true;
      attachMapEventListeners(map);

      /* initial data load */
      const now = new Date().getFullYear();
      const store = getMapState() || {};
      const unitTypes = store.unit_types?.length ? store.unit_types : ['MOD_REG'];
      const yrRange = store.year_range ? { min: store.year_range[0], max: store.year_range[1] } : { min: now, max: now };
      const initState = {
        ...store,
        unit_types: unitTypes,
        year_range: [yrRange.min, yrRange.max]
      };
      updateMapWithBounds(map, unitTypes, map.getBounds(), initState, yrRange)
        .then(() => {
          // auto‑zoom to loaded features
          if (layer && Object.keys(layer._layers).length) zoomTo(map, null, layer);
        });
    };
    waitLayer();
  };

  /* ──────────────────────────────────────────────────────────
     Public API (exported on window)
     ─────────────────────────────────────────────────────── */

  window.polygonManagement = {
    // flags/cache – exposed for debugging only
    _flags: flags,
    _cache: cache,

    // helpers consumed by Dash clientside callbacks
    findGeoJSONLayer,
    refreshLayerStyles,
    updateMapWithBounds,
    fetchPolygonsByIds,
    zoomTo
  };
})();
