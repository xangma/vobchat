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
    geojsonLayerReady: false
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
      if (flags.programmaticZoomAnimating) {
        flags.programmaticZoomAnimating = false;
        flags.programmaticZoomInProgress = false;
        // Notify Dash cleanup callback via hidden store
        window.dash_clientside?.set_props?.('zoom-cleanup-trigger-store', {
          data: { ts: Date.now(), zoom_completed: true }
        });
        return; // swallow event
      }
      if (!flags.programmaticZoomInProgress) {
        flags.lastZoomEnd = Date.now();
        window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
      }
    });

    /* moveend (debounced) */
    map.on('moveend', debounce(() => {
      LOG('moveend');
      // if (flags.programmaticZoomAnimating || flags.programmaticZoomInProgress) return;
      window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
    }));

    flags.listenersAttached = true;
  };

  /* ──────────────────────────────────────────────────────────
     Feature fetching (by bounds and by IDs)
     ─────────────────────────────────────────────────────── */

  const fetchPolygonsByBounds = (unitTypes, bounds, cachedIds, yearRange) => {
    if (!unitTypes.length) return Promise.reject('No unitTypes supplied');
    const [sw, ne] = [bounds.getSouthWest(), bounds.getNorthEast()];
    const base = { minX: sw.lng, minY: sw.lat, maxX: ne.lng, maxY: ne.lat };
    if (yearRange) { base.start_year = yearRange.min; base.end_year = yearRange.max; }

    const cacheKey = `${unitTypes.join(',')}|${getBoundsKey(bounds)}|${yearRange?.min ?? ''}-${yearRange?.max ?? ''}`;
    if (cache.pendingRequests[cacheKey]) return cache.pendingRequests[cacheKey];

    const url = new URL('/api/polygons/bbox', window.location.origin);
    Object.entries(base).forEach(([k, v]) => url.searchParams.set(k, v));
    url.searchParams.set('unit_types', unitTypes.join(','));
    if (cachedIds.length) url.searchParams.set('exclude_ids', cachedIds.join(','));

    const p = fetch(url, { headers: { Accept: 'application/json' } })
      .then(r => r.ok ? r.json() : r.text().then(t => Promise.reject(`${r.status} ${t}`)))
      .then(data => {
        if (!Array.isArray(data?.features)) return { type: 'FeatureCollection', features: [] };
        data.features.forEach(f => {
          cache.featureById[f.id] = f;
          const t = f.properties?.g_unit_type;
          if (t) (cache.featuresByUnitType[t] ||= new Set()).add(f.id);
        });
        return data;
      })
      .finally(() => delete cache.pendingRequests[cacheKey]);

    cache.pendingRequests[cacheKey] = p;
    return p;
  };

  const fetchPolygonsByIds = (map, mapState, unitType, ids, yearRange) => {
    if (!ids.length) return Promise.resolve({ type: 'FeatureCollection', features: [] });

    const missing = ids.filter(id => !cache.featureById[id]);
    const existing = ids.filter(id => cache.featureById[id]).map(id => cache.featureById[id]);

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
      const all = [...existing, ...features];
      const layer = findGeoJSONLayer(map);
      if (layer) layer.addData({ type: 'FeatureCollection', features: all });
      const selectedPolygons = getSelectedPolygonsFromPlaces(mapState);
      refreshLayerStyles(layer, selectedPolygons);
      return { type: 'FeatureCollection', features: all };
    });
  };

  /* ──────────────────────────────────────────────────────────
     Map update orchestrator
     ─────────────────────────────────────────────────────── */

  const updateMapWithBounds = (map, unitTypes, bounds, mapState, yearRange) => {
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
