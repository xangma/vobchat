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

  const LOG_PREFIX = '%c[polygon‑mgmt]';
  const LOG_STYLE = 'color:#4E9F3D;font-weight:600';
  const DEBOUNCE_MS = 250;
  const ZOOM_PADDING = [20, 20];
  const MAX_ZOOM = 14;
  const isDebug = () => (typeof window !== 'undefined' && !!window.VOB_DEBUG);
  const LOG = (...args) => { if (isDebug()) console.log(LOG_PREFIX, LOG_STYLE, ...args); };
  const TRACE = (label, payload = undefined) => {
    // Keep TRACE but do not print stack to reduce noise
    if (!isDebug()) return;
    if (payload !== undefined) {
      console.log(LOG_PREFIX, LOG_STYLE, `${label}`, payload);
    } else {
      console.log(LOG_PREFIX, LOG_STYLE, `${label}`);
    }
  };
  const ERR = (...args) => console.error(LOG_PREFIX, LOG_STYLE, ...args);
  const debounce = (fn, ms = DEBOUNCE_MS) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };

  const getSelectedPolygonsFromPlaces = (mapState) =>
    (mapState?.places || [])
      .filter(p => p.g_unit !== null && p.g_unit !== undefined)
      .map(p => String(p.g_unit));

  const getSelectedThemeId = () => window.vobUtils?.getSelectedThemeId?.() || null;

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
    zoomSessionId: null,
    zoomSessionsStarted: 0,
    zoomSessionsCompleted: 0,
    zoomendProgrammaticCount: 0,
    zoomendUserCount: 0,
    moveendProcessedCount: 0,
    fetchByIdsCalls: 0,
    updateByBoundsCalls: 0,
    refreshStylesCalls: 0,
    forceNextHydration: false
  };

  const cache = {
    featureById: Object.create(null),
    featuresByUnitType: Object.create(null),
    pendingRequests: Object.create(null),
    geojsonLayer: null
  };

  const getBoundsKey = bounds => {
    const sw = bounds.getSouthWest(); const ne = bounds.getNorthEast();
    return [sw.lng, sw.lat, ne.lng, ne.lat].map(v => v.toFixed(4)).join('_');
  };
  const getCachedFeatureIds = unitTypes => {
    const ids = new Set();
    unitTypes.forEach(t => { const set = cache.featuresByUnitType[t]; if (set) set.forEach(id => ids.add(id)); });
    return [...ids];
  };

  const findGeoJSONLayer = map => {
    TRACE('findGeoJSONLayer(): enter', { mapId: map?._leaflet_id, haveCached: !!cache.geojsonLayer });
    if (cache.geojsonLayer && map?.hasLayer(cache.geojsonLayer)) return cache.geojsonLayer;
    const el = document.getElementById('geojson-layer');
    if (el && map?._layers?.[el._leaflet_id]) { cache.geojsonLayer = map._layers[el._leaflet_id]; return cache.geojsonLayer; }
    let found = null;
    map?.eachLayer(l => { if (l instanceof L.FeatureGroup && (l.options?.style || l.options?.onEachFeature)) found = l; });
    cache.geojsonLayer = found;
    TRACE('findGeoJSONLayer(): resolved', { mapId: map?._leaflet_id, found: !!found, layerId: found?._leaflet_id });
    return found;
  };

  const refreshLayerStyles = (layer) => {
    if (window.VOB_TRACE?.layer) {
      const ids = (layer?.options?.hideout?.selected || []).map(String).slice(0, 5);
      console.log('[layer] refresh', { ok: !!layer, children: layer?._layers ? Object.keys(layer._layers).length : 0, sel: (layer?.options?.hideout?.selected || []).length, ids });
    }
    if (!layer || !layer._layers) return;
    // No local hydration of selection. Hideout is driven solely by the Dash callback.
    const existingHideout = (layer.options && layer.options.hideout) ? layer.options.hideout : {};
    const ctx = { hideout: existingHideout };
    const styleFn = layer.options?.style || window.map_leaflet?.style_function;
    if (!styleFn) { ERR('No style function found to refresh layer styles'); return; }
    flags.refreshStylesCalls += 1;
    Object.values(layer._layers).forEach(l => {
      if (l.feature) {
        try { l.setStyle(styleFn(l.feature, ctx)); } catch (e) { ERR('setStyle failed', e); }
      }
    });
    if (window.VOB_TRACE?.layer) console.log('[layer] refresh.done', { calls: flags.refreshStylesCalls, sel: (layer.options?.hideout?.selected || []).length });
  };

  // Selection is managed exclusively by Dash hideout updates; no local mirroring here.

  const attachMapEventListeners = map => {
    if (flags.listenersAttached && flags.attachedMapId === map._leaflet_id) return;
    if (!map?.on) { ERR('Invalid map object passed to attachMapEventListeners'); return; }

    flags.attachedMapId = map._leaflet_id;
    LOG('attachMapEventListeners(): attaching', { mapId: flags.attachedMapId });
    map.off('zoomend').off('moveend');

    map.on('zoomend', () => {
      TRACE('event: zoomend', { programmaticZoomAnimating: flags.programmaticZoomAnimating, programmaticZoomInProgress: flags.programmaticZoomInProgress, zoom: map.getZoom?.() });
      if (flags.programmaticZoomAnimating || flags.programmaticZoomInProgress) {
        flags.programmaticZoomAnimating = false;
        window.dash_clientside?.set_props?.('zoom-cleanup-trigger-store', { data: { ts: Date.now(), zoom_completed: true } });
        LOG('zoomend: programmatic cleanup');
        return;
      }
      flags.lastZoomEnd = Date.now();
      window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
      LOG('zoomend: user', { zoom: map.getZoom?.() });
    });

    map.on('moveend', debounce(() => {
      TRACE('event: moveend', { programmaticZoomAnimating: flags.programmaticZoomAnimating, programmaticZoomInProgress: flags.programmaticZoomInProgress, center: map.getCenter?.() });
      if (flags.programmaticZoomAnimating || flags.programmaticZoomInProgress) {
        flags.programmaticZoomInProgress = false;
        flags.programmaticZoomAnimating = false;
        flags.zoomSource = null;
        LOG('moveend: suppressed (programmatic)');
        return;
      }
      window.dash_clientside?.set_props?.('map-moveend-trigger', { data: Date.now() });
      LOG('moveend: user');
    }));
    flags.listenersAttached = true;
  };

  const hydratedBboxes = new Set();

  const fetchPolygonsByBounds = (unitTypes, bounds, cachedIds, yearRange) => {
    if (window.VOB_TRACE?.layer) console.log('[fetch] bbox.enter', { types: unitTypes, cached: (cachedIds || []).length, yr: yearRange || null });
    if (!unitTypes.length) return Promise.reject('No unitTypes supplied');
    const [sw, ne] = [bounds.getSouthWest(), bounds.getNorthEast()];
    const boundsObj = { minX: sw.lng, minY: sw.lat, maxX: ne.lng, maxY: ne.lat };
    const cacheKey = `${unitTypes.join(',')}|${getBoundsKey(bounds)}|${yearRange?.min ?? ''}-${yearRange?.max ?? ''}`;
    if (cache.pendingRequests[cacheKey]) return cache.pendingRequests[cacheKey];

    const themeId = getSelectedThemeId();
    const hydKey = `${unitTypes.join(',')}|${getBoundsKey(bounds)}|${yearRange?.min ?? ''}-${yearRange?.max ?? ''}|${themeId || 'none'}`;
    const forced = !!flags.forceNextHydration;
    flags.forceNextHydration = false;
    const needsHydration = Boolean(themeId) && (!hydratedBboxes.has(hydKey) || forced);
    const excludeIdsForRequest = needsHydration ? [] : cachedIds;
    const excludeIdsParam = excludeIdsForRequest.length > 0 ? excludeIdsForRequest.join(',') : '';
    const estimatedUrlLength = 100 + excludeIdsParam.length;
    const usePost = estimatedUrlLength > 3000 || Boolean(themeId);
    if (window.VOB_TRACE?.layer) console.log('[fetch] bbox.plan', { post: usePost ? 1 : 0, hydrate: needsHydration ? 1 : 0, ex: excludeIdsForRequest.length });

    let fetchPromise;
    const url = new URL('/api/polygons/bbox', window.location.origin);

    if (usePost) {
      const postData = {
        unit_types: unitTypes,
        bounds: boundsObj,
        exclude_ids: excludeIdsForRequest,
        ...(yearRange && { start_year: yearRange.min, end_year: yearRange.max }),
        ...(themeId && { theme_id: themeId })
      };
      fetchPromise = fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, body: JSON.stringify(postData) });
    } else {
      Object.entries(boundsObj).forEach(([k, v]) => url.searchParams.set(k, v));
      url.searchParams.set('unit_types', unitTypes.join(','));
      if (excludeIdsForRequest.length) url.searchParams.set('exclude_ids', excludeIdsParam);
      if (yearRange) { url.searchParams.set('start_year', yearRange.min); url.searchParams.set('end_year', yearRange.max); }
      if (themeId) url.searchParams.set('theme_id', themeId);
      fetchPromise = fetch(url, { headers: { Accept: 'application/json' } });
    }

    const p = fetchPromise
      .then(r => r.ok ? r.json() : r.text().then(t => Promise.reject(`${r.status} ${t}`)))
      .then(data => {
        LOG('fetchPolygonsByBounds(): response', { features: Array.isArray(data?.features) ? data.features.length : 0, withThemeUnits: Array.isArray(data?.withThemeUnits) ? data.withThemeUnits.length : 0 });
        if (!Array.isArray(data?.features)) return { type: 'FeatureCollection', features: [] };
        data.features.forEach(f => {
          cache.featureById[f.id] = f;
          const t = f.properties?.g_unit_type; if (t) (cache.featuresByUnitType[t] ||= new Set()).add(f.id);
        });
        try {
          const mapElement = document.getElementById('leaflet-map');
          const map = mapElement?._leaflet_map;
          const layer = findGeoJSONLayer(map);
          if (layer && layer.options) {
            const prev = layer.options.hideout || {};
            const wt = Array.isArray(data.withThemeUnits) ? data.withThemeUnits.map(String) : [];
            if (wt.length) {
              const merged = Array.from(new Set([...(prev.withTheme || []).map(String), ...wt]));
              layer.options.hideout = Object.assign({}, prev, { withTheme: merged });
              if (needsHydration) hydratedBboxes.add(hydKey);
              if (window.VOB_TRACE?.layer) console.log('[fetch] bbox.withTheme', { prev: (prev.withTheme || []).length, add: wt.length, merged: merged.length });
            }
          }
        } catch (_) { }
        return data;
      })
      .finally(() => delete cache.pendingRequests[cacheKey]);

    cache.pendingRequests[cacheKey] = p;
    return p;
  };

  const fetchPolygonsByIds = (map, mapState, unitType, ids, yearRange) => {
    if (window.VOB_TRACE?.layer) console.log('[fetch] ids.enter', { type: unitType, ids: (ids || []).length, yr: yearRange || null });
    if (!ids.length) return Promise.resolve({ type: 'FeatureCollection', features: [] });
    const missing = ids.filter(id => !cache.featureById[id]);
    const existing = ids.filter(id => cache.featureById[id]).map(id => cache.featureById[id]);
    if (window.VOB_TRACE?.layer) console.log('[fetch] ids.split', { miss: missing.length, have: existing.length });

    const fetchPromise = !missing.length ? Promise.resolve({ features: [] }) : (() => {
      const url = new URL('/api/polygons/ids', window.location.origin);
      url.searchParams.set('ids', missing.join(','));
      url.searchParams.set('unit_type', unitType);
      if (yearRange) { url.searchParams.set('start_year', yearRange.min); url.searchParams.set('end_year', yearRange.max); }
      return fetch(url, { headers: { Accept: 'application/json' } })
        .then(r => r.ok ? r.json() : r.text().then(t => Promise.reject(`${r.status} ${t}`)));
    })();

    return fetchPromise.then(({ features = [] }) => {
      if (window.VOB_TRACE?.layer) console.log('[fetch] ids.fetched', { features: features.length });
      features.forEach(f => {
        cache.featureById[f.id] = f;
        (cache.featuresByUnitType[f.properties?.g_unit_type] ||= new Set()).add(f.id);
      });
      const combined = [...existing, ...features];
      const byId = Object.create(null);
      combined.forEach(f => { if (f && f.id != null) byId[String(f.id)] = f; });
      const uniqueFeatures = Object.values(byId);

      const layer = findGeoJSONLayer(map);
      if (layer) {
        const idsToReplace = new Set(uniqueFeatures.map(f => String(f.id)));
        if (layer._layers) {
          Object.values(layer._layers).forEach(l => {
            if (l?.feature?.id != null && idsToReplace.has(String(l.feature.id))) layer.removeLayer(l);
          });
        }
        if (uniqueFeatures.length) layer.addData({ type: 'FeatureCollection', features: uniqueFeatures });
        if (window.VOB_TRACE?.layer) console.log('[layer] addData', { replaced: idsToReplace.size, children: Object.keys(layer._layers || {}).length });
      }

      // Selection is now driven solely by Dash hideout callback; just refresh styles
      refreshLayerStyles(layer);
      return { type: 'FeatureCollection', features: uniqueFeatures };
    });
  };

  const updateMapWithBounds = (map, unitTypes, bounds, mapState, yearRange) => {
    if (window.VOB_TRACE?.layer) console.log('[layer] update.enter', { types: unitTypes, yr: yearRange || null });
    const layer = findGeoJSONLayer(map);
    if (!layer) return Promise.reject('GeoJSON layer not ready');

    const cachedIds = getCachedFeatureIds(unitTypes);
    return fetchPolygonsByBounds(unitTypes, bounds, cachedIds, yearRange)
      .then(({ features }) => {
        let selected = getSelectedPolygonsFromPlaces(mapState);
        const showUnselected = mapState.show_unselected ?? true;
        const merged = [...features, ...cachedIds.map(id => cache.featureById[id]).filter(Boolean)];
        const byId = Object.create(null);
        merged.forEach(f => { if (f && f.id != null) byId[String(f.id)] = f; });
        const mergedUnique = Object.values(byId);
        const toDisplay = showUnselected ? mergedUnique : mergedUnique.filter(f => selected.includes(String(f.id)));

        layer.clearLayers();
        if (toDisplay.length) layer.addData({ type: 'FeatureCollection', features: toDisplay });
        if (window.VOB_TRACE?.layer) console.log('[layer] update.refreshed', { fetched: features.length, cached: cachedIds.length, shown: toDisplay.length, showUnselected });

        // Selection is now driven solely by Dash hideout callback; just refresh styles
        refreshLayerStyles(layer);
        return { type: 'FeatureCollection', features: toDisplay };
      });
  };

  const zoomTo = (map, selectedIds = null, layer = null) => {
    const lyr = layer || findGeoJSONLayer(map);
    if (!lyr?._layers) return;
    const ids = selectedIds?.map(String);
    const bounds = L.latLngBounds();
    let count = 0;
    Object.values(lyr._layers).forEach(l => {
      if (!l.feature || !l.getBounds) return;
      if (!ids || ids.includes(String(l.feature.id))) { bounds.extend(l.getBounds()); count += 1; }
    });
    if (!count || !bounds.isValid()) return;
    LOG('zoomTo(): fitting', { idsCount: ids ? ids.length : 'ALL', featuresUsed: count });
    // Mark this zoom as programmatic to suppress auto-load moveend refresh
    try { flags.programmaticZoomInProgress = true; flags.programmaticZoomAnimating = true; flags.zoomSource = 'zoomTo'; } catch (e) { }
    map.fitBounds(bounds, { padding: ZOOM_PADDING, maxZoom: MAX_ZOOM, animate: true, duration: 0.5 });
  };

  L.Map.addInitHook(function () {
    this.getContainer()._leaflet_map = this;
    this.whenReady(() => {
      TRACE('Leaflet.whenReady(): enter', { mapId: this._leaflet_id, zoom: this.getZoom?.(), center: this.getCenter?.() });
      const waitLayer = () => {
        const layer = findGeoJSONLayer(this);
        if (!layer) return setTimeout(waitLayer, 120);
        attachMapEventListeners(this);

        const now = new Date().getFullYear();
        const store = (window.vobUtils?.getMapState?.() || {});
        const unitTypes = store.unit_types?.length ? store.unit_types : ['MOD_REG'];
        const yrRange = store.year_range ? { min: store.year_range[0], max: store.year_range[1] } : { min: now, max: now };
        const initState = { ...store, unit_types: unitTypes, year_range: [yrRange.min, yrRange.max] };

        updateMapWithBounds(this, unitTypes, this.getBounds(), initState, yrRange)
          .then(() => {
            LOG('Leaflet.whenReady(): initial update complete', { layerChildren: Object.keys(layer._layers || {}).length });
            if (layer && Object.keys(layer._layers).length) zoomTo(this, null, layer);
          })
          .catch(err => ERR('Leaflet.whenReady(): initial update failed', err));
      };
      waitLayer();
    });
  });

  window.polygonManagement = {
    _flags: flags,
    _cache: cache,
    findGeoJSONLayer,
    refreshLayerStyles,
    updateMapWithBounds,
    fetchPolygonsByIds,
    zoomTo,
    // optionally expose for debugging
    debugDump() { if (!isDebug()) return; console.log(LOG_PREFIX, LOG_STYLE, 'debugDump()', { flags, cache: { featureCount: Object.keys(cache.featureById).length, types: Object.fromEntries(Object.entries(cache.featuresByUnitType).map(([k,v]) => [k, v.size])) } }); console.trace(); },
    clearCache() { TRACE('clearCache(): enter'); cache.featureById = Object.create(null); cache.featuresByUnitType = Object.create(null); cache.pendingRequests = Object.create(null); LOG('clearCache(): done'); }
  };
})();
