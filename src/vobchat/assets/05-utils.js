// Lightweight DOM + store helpers shared by assets
// Exposes window.vobUtils with safe helpers to read Dash stores and theme ID.

(function(){
  'use strict';
  // Lightweight category flags for debug logging; can be toggled from console
  // Example: window.VOB_TRACE = { select: true, layer: true, style: false }
  if (typeof window !== 'undefined') {
    // Default to quiet; enable categories explicitly in console if needed
    window.VOB_TRACE = Object.assign({ select: false, layer: false, style: false }, window.VOB_TRACE || {});
  }
  const isDebug = () => (typeof window !== 'undefined' && !!window.VOB_DEBUG);
  const LOG = (...args) => { if (isDebug()) console.log('%c[vob-utils]', 'color:#6A4C93;font-weight:600', ...args); };
  const TRACE = (label, payload) => {
    if (!isDebug()) return;
    try {
      const stack = new Error('trace').stack.split('\n').slice(2, 8).join('\n');
      console.log('%c[vob-utils]', 'color:#6A4C93;font-weight:600', label, payload || '');
      console.log(stack);
    } catch (_) { console.log('%c[vob-utils]', 'color:#6A4C93;font-weight:600', label, payload || ''); }
  };

  const safeParse = (txt) => {
    try { return JSON.parse(txt); } catch (_) { return null; }
  };

  const readStore = (id) => {
    TRACE('readStore(): enter', { id });
    try {
      const el = document.getElementById(id);
      if (!el) return null;
      // Prefer the live Dash value when available
      if (el._dash_value !== undefined) { LOG('readStore(): dash value', { id }); return el._dash_value; }
      const txt = (el.textContent || '').trim();
      const parsed = txt ? safeParse(txt) : null;
      LOG('readStore(): parsed', { id, hasValue: !!parsed });
      return parsed;
    } catch (_) {
      return null;
    }
  };

  const getMapState = () => readStore('map-state');
  const getPlaceState = () => readStore('place-state');

  const parseSelectedTheme = (val) => {
    if (!val) return null;
    let obj = val;
    if (typeof obj === 'string') obj = safeParse(obj);
    if (!obj) return null;
    if (Array.isArray(obj) && obj.length > 0) return obj[0]?.ent_id || obj[0]?.id || null;
    if (obj && typeof obj === 'object') return obj.ent_id || obj.id || null;
    return null;
  };

  const getSelectedThemeId = () => {
    // Prefer SSE cache for immediacy
    try {
      const sse = window.simpleSSE;
      const st = sse?.placeStateCache?.selected_theme;
      const fromCache = parseSelectedTheme(st);
      if (fromCache) { LOG('getSelectedThemeId(): from SSE cache', { id: fromCache }); return fromCache; }
    } catch (_) {}
    // Fallback to store
    const ps = getPlaceState();
    const id = parseSelectedTheme(ps?.selected_theme);
    LOG('getSelectedThemeId(): from store', { id: id || null });
    return id;
  };

  // Merge targeted updates into the map-state store (dedup to avoid loops)
  const syncMapState = (updates) => {
    TRACE('syncMapState(): enter', { updates });
    try {
      if (typeof dash_clientside === 'undefined' || !dash_clientside.set_props) return null;
      const cur = getMapState() || {};
      const next = Object.assign({}, cur, updates || {});
      // Shallow signature compare of keys we are updating
      const keys = Object.keys(updates || {});
      const same = keys.every(k => {
        try {
          const a = cur[k];
          const b = next[k];
          if (a === b) return true;
          if (Array.isArray(a) && Array.isArray(b)) return JSON.stringify(a) === JSON.stringify(b);
          if (typeof a === 'object' && typeof b === 'object') return JSON.stringify(a) === JSON.stringify(b);
          return a === b;
        } catch (_) { return false; }
      });
      if (!same) {
        dash_clientside.set_props('map-state', { data: next });
        LOG('syncMapState(): applied', { keys: Object.keys(updates || {}) });
      }
      return next;
    } catch (_) {
      return null;
    }
  };

  // Robust unit type derivation with fallbacks
  const getUnitTypes = (map, placeState, mapStore) => {
    TRACE('getUnitTypes(): enter');
    // 1) From placeState.places (single source of truth)
    try {
      const places = placeState?.places || [];
      const arr = places
        .filter(p => p && p.g_unit != null)
        .map(p => p.g_unit_type)
        .filter(Boolean);
      if (arr.length) { LOG('getUnitTypes(): from places', { count: arr.length }); return Array.from(new Set(arr)); }
    } catch (_) {}
    // 2) From visible layer features
    try {
      const layer = window.polygonManagement?.findGeoJSONLayer?.(map);
      const types = [];
      if (layer && layer._layers) {
        Object.values(layer._layers).forEach(l => {
          const ut = l?.feature?.properties?.g_unit_type;
          if (ut) types.push(ut);
        });
      }
      if (types.length) { LOG('getUnitTypes(): from layer', { count: types.length }); return Array.from(new Set(types)); }
    } catch (_) {}
    // 3) From mapStore.unit_types
    try {
      const uts = Array.isArray(mapStore?.unit_types) ? mapStore.unit_types : [];
      if (uts.length) { LOG('getUnitTypes(): from store', { count: uts.length }); return Array.from(new Set(uts)); }
    } catch (_) {}
    // 4) Default
    LOG('getUnitTypes(): default MOD_REG');
    return ['MOD_REG'];
  };

  // Robust selected ID derivation with fallbacks (strings)
  const getSelectedIds = (map, placeState, mapStore, layer) => {
    TRACE('getSelectedIds(): enter');
    // 1) From mapStore.places (authoritative local state)
    try {
      const places = mapStore?.places || [];
      const ids = places
        .filter(p => p && p.g_unit != null)
        .map(p => String(p.g_unit));
      if (ids.length) { LOG('getSelectedIds(): from mapStore', { count: ids.length }); return Array.from(new Set(ids)); }
    } catch (_) {}
    // 2) From placeState.places (server-driven state; may lag)
    try {
      const places = placeState?.places || [];
      const ids = places
        .filter(p => p && p.g_unit != null)
        .map(p => String(p.g_unit));
      if (ids.length) { LOG('getSelectedIds(): from placeState', { count: ids.length }); return Array.from(new Set(ids)); }
    } catch (_) {}
    // 3) From layer hideout.selected
    try {
      const lyr = layer || window.polygonManagement?.findGeoJSONLayer?.(map);
      const ids = (lyr?.options?.hideout?.selected || []).map(String);
      if (ids.length) { LOG('getSelectedIds(): from layer.hideout', { count: ids.length }); return Array.from(new Set(ids)); }
    } catch (_) {}
    // 4) From PureMapState (deprecated)
    LOG('getSelectedIds(): none found');
    return [];
  };

  window.vobUtils = Object.freeze({
    readStore,
    getMapState,
    getPlaceState,
    getSelectedThemeId,
    syncMapState,
    getUnitTypes,
    getSelectedIds,
  });
})();
