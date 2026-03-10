// Simple SSE Client - Clean rewrite
// Purpose: Connect to the SSE stream and update the UI directly without
// Dash callbacks for the high-frequency chat + map flows. This file is kept
// intentionally self-contained so it can operate with minimal coupling.
//
// Lifecycle overview:
// - connect(threadId, workflowInput?): Opens an EventSource to the /sse endpoint
//   and (optionally) POSTs an initial workflow input to kick off the turn.
// - handleStateUpdate(state): Receives normalized state deltas and updates
//   the visible chat, map, and visualization panels. Chat messages are always
//   ordered and deduped server-side to avoid flicker.
// - Streaming: During token streaming, the server emits frequent state_update
//   events with a full messages array including a growing AI bubble. The
//   llm_busy flag is cleared after the first visible token renders.
// - Interrupts: When a node requests user action (e.g., place disambiguation),
//   an 'interrupt' event arrives with options and map hints.
class SimpleSSEClient {
    constructor() {
        this.eventSource = null;
        this.threadId = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 3;
        this.lastPlacesSig = null;
        this.llmBusy = false;
        this.currentOptions = null;
        this.placeStateCache = {};
        this.recentAdds = new Map();
        this.recentRemovals = new Map();
        this.basePath = (typeof window !== 'undefined' && window.DASH_URL_BASE_PATHNAME) ? window.DASH_URL_BASE_PATHNAME : '/';

        // Debug logging helpers (guarded by window.VOB_DEBUG)
        this._isDebug = () => (typeof window !== 'undefined' && !!window.VOB_DEBUG);
        this._LOG = (...args) => { if (this._isDebug()) console.log('%c[sse-client]', 'color:#FF7F50;font-weight:600', ...args); };
        this._TRACE = (label, payload = undefined) => {
            if (!this._isDebug()) return;
            try {
                const stack = new Error('trace').stack.split('\n').slice(2, 8).join('\n');
                console.log('%c[sse-client]', 'color:#FF7F50;font-weight:600', label, payload || '');
                console.log(stack);
            } catch (_) { console.log('%c[sse-client]', 'color:#FF7F50;font-weight:600', label, payload || ''); }
        };

        this.joinPath = (base, path) => {
            base = base || "";
            if (base.endsWith('/')) base = base.slice(0, -1);
            return `${base}${path}`;
        };

        // Theme handlers (unchanged)
        const attachThemeHandlers = () => {
            const panelEl = document.getElementById('theme-selection-panel');
            const statusEl = document.getElementById('theme-status');
            const closeEl = document.getElementById('theme-panel-close');
            const clearEl = document.getElementById('theme-panel-clear');

            if (statusEl && !statusEl._themeHandlersAttached) {
                statusEl._themeHandlersAttached = true;
                statusEl.addEventListener('click', (e) => {
                    if (panelEl && this.currentOptions && Array.isArray(this.currentOptions) && this.currentOptions.some(o => o.option_type === 'theme_query')) {
                        let isOpen = false;
                        try {
                            const cs = window.getComputedStyle(panelEl);
                            isOpen = cs && cs.display && cs.display !== 'none';
                        } catch (_) {
                            isOpen = panelEl.style.display && panelEl.style.display !== 'none';
                        }
                        panelEl.style.display = isOpen ? 'none' : 'block';
                        try { e.preventDefault(); e.stopPropagation(); } catch (_) { }
                        return;
                    }
                });
            }
            if (closeEl && !closeEl._themeHandlersAttached) {
                closeEl._themeHandlersAttached = true;
                closeEl.addEventListener('click', (e) => {
                    try { e.preventDefault(); e.stopPropagation(); } catch (_) { }
                    if (panelEl) panelEl.style.display = 'none';
                });
            }
            if (clearEl && !clearEl._themeHandlersAttached) {
                clearEl._themeHandlersAttached = true;
                clearEl.addEventListener('click', (e) => {
                    try { e.preventDefault(); e.stopPropagation(); } catch (_) { }
                    const labelEl = document.getElementById('theme-status-label');
                    if (labelEl) labelEl.textContent = 'Theme: (none)';
                    if (panelEl) panelEl.style.display = 'none';
                    this.currentOptions = null;
                    if (this.threadId) {
                        this.postWorkflowInput({
                            last_intent_payload: { intent: 'RemoveTheme', arguments: { source: 'theme_panel' } }
                        }).catch(err => console.error('SSE: Theme clear post failed', err));
                    }
                });
            }
            return Boolean(statusEl && statusEl._themeHandlersAttached && closeEl && closeEl._themeHandlersAttached && clearEl && clearEl._themeHandlersAttached);
        };
        const initThemeUI = () => {
            if (attachThemeHandlers()) return;
            if (this._themeObserver) return;
            this._themeObserver = new MutationObserver(() => {
                if (attachThemeHandlers()) {
                    try { this._themeObserver.disconnect(); } catch (_) { }
                    this._themeObserver = null;
                }
            });
            try { this._themeObserver.observe(document.body, { childList: true, subtree: true }); } catch (_) { }
        };
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initThemeUI);
        } else {
            initThemeUI();
        }
    }

    /* ──────────────────────────────────────────────────────────
     Helpers
    ─────────────────────────────────────────────────────────── */

    _getMapAndLayer() {
        const map = document.getElementById('leaflet-map')?._leaflet_map || null;
        const layer = window.polygonManagement?.findGeoJSONLayer?.(map) || null;
        this._TRACE('_getMapAndLayer()', { mapId: map?._leaflet_id, haveLayer: !!layer });
        return { map, layer };
    }

    _placesToSelectedIds(places) {
        return (places || [])
            .filter(p => p && p.g_unit != null)
            .map(p => String(p.g_unit));
    }
 
    // Drive selection via map-state only; hideout is derived by a dedicated callback
    setLayerSelectionFromPlaces(places) {
        if (window.VOB_TRACE?.select) this._LOG('[select] setLayerSelectionFromPlaces', { places: (places || []).length });
        this.updateMapSelection(places);
    }

    noteClientAdd(_) { /* deprecated */ }
    noteClientRemove(_) { /* deprecated */ }
    pruneRecent(_) { /* deprecated */ }

    async connect(threadId, workflowInput = null) {
        if (this.eventSource) this.disconnect();
        this.threadId = threadId;

        // If no threadId provided, mint one from the server and bind to session
        if (!this.threadId) {
            try {
                const resp = await fetch(this.joinPath(this.basePath, `/threads/new`), { method: 'POST', headers: { 'Accept': 'application/json' } });
                if (resp.ok) {
                    const data = await resp.json();
                    if (data && data.thread_id) {
                        this.threadId = data.thread_id;
                    }
                }
            } catch (e) {
                console.error('SSE: failed to mint thread id', e);
            }
            if (!this.threadId) {
                console.error('SSE: no thread id available to connect');
                return;
            }
        }

        // Reset UX bits on Reset intent
        if (workflowInput && workflowInput.last_intent_payload?.intent === 'Reset') {
            this.clearChatDisplay();
            this.clearButtons();
            this.hideVisualization();
            this.hideThinkingIndicator?.();
            try {
                const labelEl = document.getElementById('theme-status-label');
                if (labelEl) labelEl.textContent = 'Theme: (none)';
                const themePanel = document.getElementById('theme-selection-panel');
                const themeButtons = document.getElementById('theme-selection-buttons');
                const optionsContainer = document.getElementById('options-container');
                if (themePanel) themePanel.style.display = 'none';
                if (themeButtons) themeButtons.innerHTML = '';
                if (optionsContainer) optionsContainer.innerHTML = '';
                this.currentOptions = null;
                const chatInput = document.getElementById('chat-input');
                if (chatInput) chatInput.value = '';
                // Clear stores
                try { dash_clientside?.set_props?.('place-state', { data: {} }); } catch (_) { }
                // Clear selection via map-state; hideout will follow
                try { window.vobUtils?.syncMapState?.({ places: [] }); } catch (_) { }
            } catch (_) { }
        }

        const url = this.joinPath(this.basePath, `/sse/${this.threadId}`);
        this.eventSource = new EventSource(url);

        this.postWorkflowInput = (input) => {
            try {
                if (this.currentOptions && Array.isArray(this.currentOptions) && this.currentOptions.length > 0) {
                    input = Object.assign({}, input, { options: this.currentOptions });
                }
            } catch (e) { }
            if (!this.threadId) return;
            return fetch(this.joinPath(this.basePath, `/workflow/${this.threadId}`), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ workflow_input: input })
            });
        };

        this.eventSource.onopen = () => {
            this.isConnected = true;
            this.reconnectAttempts = 0;
            if (workflowInput) this.postWorkflowInput(workflowInput).catch(err => console.error('SSE: initial POST failed', err));
        };

        this.eventSource.addEventListener('state_update', (event) => {
            try { this.handleStateUpdate(JSON.parse(event.data).state); }
            catch (e) { console.error('SSE: state_update parse error', e); }
        });

        this.eventSource.addEventListener('interrupt', (event) => {
            try { this.handleInterrupt(JSON.parse(event.data)); }
            catch (e) { console.error('SSE: interrupt parse error', e); }
        });

        this.eventSource.addEventListener('error', (event) => {
            try { this.handleError(JSON.parse(event.data).error); }
            catch (e) { console.error('SSE: error event parse error', e); }
        });

        this.eventSource.onerror = () => {
            this.isConnected = false;
            this.hideThinkingIndicator();
            this.attemptReconnect();
        };
    }

    disconnect() {
        if (this.eventSource) this.eventSource.close();
        this.eventSource = null;
        this.isConnected = false;
        this.threadId = null;
        this.hideThinkingIndicator();
    }

    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) return;
        this.reconnectAttempts++;
        setTimeout(() => { if (this.threadId) this.connect(this.threadId); }, 2000);
    }

    handleMessage(_) { }

    handleStateUpdate(state) {
        this._TRACE('handleStateUpdate(): enter', { keys: Object.keys(state || {}) });
        if (!state || typeof state !== 'object') return;

        if (Object.prototype.hasOwnProperty.call(state, 'llm_busy')) {
            this.llmBusy = !!state.llm_busy;
            this.llmBusy ? this.showThinkingIndicator() : this.hideThinkingIndicator();
        }

        if (state.messages && Array.isArray(state.messages)) {
            this.updateChatDisplay(state.messages);
        }

        // Theme label
        if (Object.prototype.hasOwnProperty.call(state, 'selected_theme')) {
            try {
                const labelEl = document.getElementById('theme-status-label');
                let labelText = 'Theme: (none)';
                if (state.selected_theme) {
                    const df = JSON.parse(state.selected_theme);
                    if (Array.isArray(df) && df.length > 0) {
                        const lab = df[0].labl || df[0].label || null;
                        labelText = lab ? `Theme: ${lab}` : labelText;
                    } else if (df && typeof df === 'object') {
                        const lab = df.labl || df.label || null;
                        labelText = lab ? `Theme: ${lab}` : labelText;
                    }
                }
                if (labelEl) labelEl.textContent = labelText;
            } catch (_) { }
        }

        if (state.cubes || state.places) {
            this.updateVisualization(state);
        }

        if (state.map_update_request?.action === 'update_map_selection') {
            this.handleMapUpdateRequest(state.map_update_request);
        } else if (state.map_update_request?.action === 'show_info_marker') {
            this.handleInfoMarkerRequest(state.map_update_request);
        }

        // Store cache + trigger bbox refresh on theme changes so shading updates
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            const updates = {};
            const deepEqual = (a, b) => {
                try { if (a === b) return true; if (Array.isArray(a) && Array.isArray(b) && a.length !== b.length) return false; return JSON.stringify(a) === JSON.stringify(b); }
                catch (e) { return false; }
            };
            if (state.places !== undefined && !deepEqual(this.placeStateCache.places, state.places)) {
                updates.places = state.places;
            }
            if (state.cubes !== undefined && !deepEqual(this.placeStateCache.cubes, state.cubes)) {
                updates.cubes = state.cubes;
            }
            if (state.selected_theme !== undefined && !deepEqual(this.placeStateCache.selected_theme, state.selected_theme)) {
                updates.selected_theme = state.selected_theme;
            }
            if (Object.keys(updates).length) {
                this.placeStateCache = Object.assign({}, this.placeStateCache, updates);
                dash_clientside.set_props('place-state', { data: this.placeStateCache });
                // If selected_theme changed, force a bounds refresh to hydrate withTheme and restyle
                if (Object.prototype.hasOwnProperty.call(updates, 'selected_theme')) {
                    try {
                        const mapEl = document.getElementById('leaflet-map');
                        const map = mapEl?._leaflet_map;
                        if (map && window.polygonManagement && window.polygonManagement.updateMapWithBounds) {
                            // Immediately clear withTheme so polygons revert to grey while new theme hydrates
                            try {
                                const layer = window.polygonManagement.findGeoJSONLayer ? window.polygonManagement.findGeoJSONLayer(map) : null;
                                if (layer && layer.options) {
                                    const prev = layer.options.hideout || {};
                                    const selected = Array.isArray(prev.selected) ? prev.selected.map(String) : [];
                                    layer.options.hideout = Object.assign({}, prev, { withTheme: [], selected });
                                    window.polygonManagement.refreshLayerStyles?.(layer);
                                }
                            } catch (_) { }
                            const storeNow = window.vobUtils?.getMapState?.() || {};
                            // Respect the currently active unit_types in map-state; fallback to last resolved place
                            let unitTypes = Array.isArray(storeNow.unit_types) && storeNow.unit_types.length ? storeNow.unit_types : null;
                            if (!unitTypes) {
                                const allUTs = window.vobUtils.getUnitTypes(map, this.placeStateCache, storeNow);
                                const uniqueAll = Array.isArray(allUTs) ? [...new Set(allUTs)] : [];
                                let lastUT = null;
                                try {
                                    const places = Array.isArray(this.placeStateCache.places) ? this.placeStateCache.places : [];
                                    for (let i = places.length - 1; i >= 0; i--) {
                                        const p = places[i];
                                        if (p && p.g_unit != null && p.g_unit_type) { lastUT = p.g_unit_type; break; }
                                    }
                                } catch (_) {}
                                unitTypes = lastUT ? [lastUT] : uniqueAll;
                            }
                            const yrRange = storeNow.year_range ? { min: storeNow.year_range[0], max: storeNow.year_range[1] } : null;
                            // Ensure next bbox fetch hydrates withTheme
                            try { if (window.polygonManagement._flags) window.polygonManagement._flags.forceNextHydration = true; } catch (_) {}
                            window.polygonManagement.updateMapWithBounds(map, unitTypes, map.getBounds(), Object.assign({}, storeNow, { unit_types: unitTypes }), yrRange)
                                .catch(() => { /* silent */ });
                        }
                    } catch (_) { }
                }
            }
        }
    }

    handleInterrupt(interruptData) {
        this.hideThinkingIndicator();
        this.currentNode = interruptData.current_node || null;
        this.currentInterruptData = interruptData;

        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            if (interruptData.place_coordinates && interruptData.place_coordinates.length > 0) {
                dash_clientside.set_props('sse-interrupt-store', { data: {} });
                setTimeout(() => dash_clientside.set_props('sse-interrupt-store', { data: interruptData }), 0);
            } else {
                dash_clientside.set_props('sse-interrupt-store', { data: interruptData });
            }
        }

        if (interruptData.place_coordinates?.length) {
            window._disambiguationMode = true;
            setTimeout(() => { this.zoomToPlaceMarkers(interruptData.place_coordinates, interruptData); }, 100);
        }

        if (interruptData.cube_data_ready && interruptData.cubes) {
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                const newData = {
                    cubes: JSON.parse(interruptData.cubes),
                    selected_cubes: JSON.parse(interruptData.selected_cubes || interruptData.cubes),
                    show_visualization: interruptData.show_visualization || true,
                    places: interruptData.places || [],
                    selected_theme: interruptData.selected_theme
                };
                this.placeStateCache = Object.assign({}, this.placeStateCache, newData);
                dash_clientside.set_props('place-state', { data: this.placeStateCache });
            }
            // Selection highlight remains driven by map-state -> hideout
        }

        if (interruptData.message && interruptData.messages) {
            const allMessages = [...interruptData.messages, { _type: 'ai', content: interruptData.message, type: 'ai' }];
            this.updateChatDisplay(allMessages);
            this.pendingInterruptMessage = interruptData.message;
        }

        if (interruptData.options && Array.isArray(interruptData.options)) {
            this.showButtons(interruptData.options);
            this.currentOptions = interruptData.options;
        } else {
            this.clearButtons();
        }
    }



    zoomToPlaceMarkers(placeCoordinates, interruptData = null) {
        window._disambiguationMode = true;
        const mapElement = document.getElementById('leaflet-map');
        if (!mapElement || !mapElement._leaflet_map) return;
        const map = mapElement._leaflet_map;

        if (!placeCoordinates.length) return;

        const places = interruptData?.places || [];
        const selectedUnits = this._placesToSelectedIds(places);

        // Clear layer & cache for clarity during disambiguation
        window.polygonManagement?.clearCache?.(); // tolerant if missing
        const geojsonLayer = window.polygonManagement?.findGeoJSONLayer?.(map);
        if (geojsonLayer?.clearLayers) geojsonLayer.clearLayers();

        // If any polygons are selected, fetch them (grouped by type), then zoom to both
        if (selectedUnits.length && window.polygonManagement?.fetchPolygonsByIds) {
            const unitsByType = {};
            places.forEach(p => { if (p?.g_unit && p?.g_unit_type) (unitsByType[p.g_unit_type] ||= []).push(String(p.g_unit)); });
            const mapState = { places };
            const promises = Object.entries(unitsByType).map(([ut, ids]) =>
                window.polygonManagement.fetchPolygonsByIds(map, mapState, ut, ids, null)
            );
            Promise.all(promises)
                .then(() => {
                    // ensure highlight reflects selected units
                    this.setLayerSelectionFromPlaces(places);
                    const lyr = window.polygonManagement?.findGeoJSONLayer?.(map);
                    this.calculateCombinedZoomBounds(map, lyr, selectedUnits, placeCoordinates);
                })
                .catch(() => this.calculateCombinedZoomBounds(map, null, [], placeCoordinates));
            return;
        }
        this.calculateCombinedZoomBounds(map, null, [], placeCoordinates);
    }

    clearDisambiguationMode() { window._disambiguationMode = false; }

    handleError(error) {
        console.error('SSE error:', error);
        this.hideThinkingIndicator();
    }

    updateVisualization(state) {
        const container = document.getElementById('visualization-panel-container');
        const area = document.getElementById('visualization-area');
        const hasTheme = !!state.selected_theme;
        const hasCubes = Array.isArray(state.cubes) && state.cubes.length > 0;
        if (!hasTheme) {
            if (container) container.style.display = 'none';
            if (area) area.style.display = 'none';
            return;
        }
        if (hasCubes) {
            if (container) container.style.display = 'flex';
            if (area) area.style.display = 'flex';
        } else if (state.show_visualization === false) {
            if (container) container.style.display = 'none';
            if (area) area.style.display = 'none';
        }
    }

    updateMapSelection(places) {
        this._TRACE('updateMapSelection(): enter', { places: (places || []).length });
        const sig = this._placesSignature(places);
        if (this.lastPlacesSig === sig) return;
        this.lastPlacesSig = sig;
        try { window.vobUtils?.syncMapState?.({ places }); }
        catch (_) {
            if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
                const current = (window.vobUtils?.getMapState?.() || {});
                const next = Object.assign({}, current, { places });
                dash_clientside.set_props('map-state', { data: next });
            }
        }
        this._LOG('updateMapSelection(): synced to store');
    }

    _placesSignature(places) {
        try {
            const keyParts = (places || [])
                .filter(p => p && p.g_unit !== null && p.g_unit !== undefined)
                .map(p => `${String(p.g_unit)}:${p.g_unit_type || ''}`)
                .sort();
            return keyParts.join('|');
        } catch (e) { return String(Date.now()); }
    }

    showButtons(options) {
        const hasThemeOptions = Array.isArray(options) && options.some(o => o.option_type === 'theme_query');
        const themePanel = document.getElementById('theme-selection-panel');
        const themeButtons = document.getElementById('theme-selection-buttons');
        const optionsContainer = document.getElementById('options-container');
        const container = hasThemeOptions && themeButtons ? themeButtons : optionsContainer;
        if (!container) return;
        if (themeButtons) themeButtons.innerHTML = '';
        if (!hasThemeOptions && optionsContainer) optionsContainer.innerHTML = '';
        if (themePanel) themePanel.style.display = hasThemeOptions ? 'block' : 'none';

        const getSelectedThemeId = () => {
            try {
                const st = this.placeStateCache?.selected_theme;
                if (!st) return null;
                const parsed = JSON.parse(st);
                if (Array.isArray(parsed) && parsed.length > 0) return String(parsed[0].ent_id || parsed[0].id || '');
                if (parsed && typeof parsed === 'object') return String(parsed.ent_id || parsed.id || '');
            } catch (_) { }
            return null;
        };
        const selectedThemeId = getSelectedThemeId();

        options.forEach(option => {
            const button = document.createElement('button');
            const isTheme = option.option_type === 'theme_query';
            const isSelectedTheme = isTheme && selectedThemeId && String(option.value) === String(selectedThemeId);
            button.className = `btn ${isSelectedTheme ? 'btn-primary theme-option-btn selected' : 'btn-outline-primary theme-option-btn'} me-2 mb-2`;
            button.textContent = option.label;

            if (option.color) {
                button.style.borderColor = option.color;
                button.style.color = option.color;
                button.addEventListener('mouseenter', function () { this.style.backgroundColor = option.color; this.style.color = 'white'; });
                button.addEventListener('mouseleave', function () { this.style.backgroundColor = 'transparent'; this.style.color = option.color; });
            }

            button.onclick = () => {
                if (this.threadId) {
                    const selectionInput = {
                        selection_idx: option.value,
                        button_type: option.option_type,
                        current_node: this.currentNode
                    };
                    if (option.option_type === 'unit' && this.currentInterruptData) {
                        if (this.currentInterruptData.current_place_index !== undefined) {
                            selectionInput.current_place_index = this.currentInterruptData.current_place_index;
                        }
                        if (this.currentInterruptData.places) {
                            selectionInput.places = this.currentInterruptData.places;
                        }
                    }
                    this.sendSelection(selectionInput);
                }
                if (option.option_type === 'theme_query') {
                    if (themeButtons) themeButtons.innerHTML = '';
                    if (themePanel) themePanel.style.display = 'none';
                    this.currentOptions = null;
                } else {
                    this.clearButtons();
                }
            };
            container.appendChild(button);
        });
    }

    sendSelection(selectionInput) {
        if (!this.threadId) return;
        if (this.pendingInterruptMessage) {
            selectionInput.interrupt_message = this.pendingInterruptMessage;
            this.pendingInterruptMessage = null;
        }
        fetch(this.joinPath(this.basePath, `/workflow/${this.threadId}`), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                workflow_input: Object.assign({}, selectionInput, (this.currentOptions && this.currentOptions.length > 0 ? { options: this.currentOptions } : {}))
            })
        }).catch(error => {
            console.error('SSE: selection POST failed', error);
            this.postWorkflowInput(selectionInput).catch(() => { });
        });
    }

    clearButtons() {
        const container = document.getElementById('options-container');
        const themeButtons = document.getElementById('theme-selection-buttons');
        const themePanel = document.getElementById('theme-selection-panel');
        if (container) container.innerHTML = '';
        if (themeButtons) themeButtons.innerHTML = '';
        if (themePanel) themePanel.style.display = 'none';
        this.currentOptions = null;
    }

    clearChatDisplay() {
        const chatDisplay = document.getElementById('chat-display');
        if (chatDisplay) chatDisplay.innerHTML = '';
    }

    hideVisualization() {
        const container = document.getElementById('visualization-panel-container');
        const area = document.getElementById('visualization-area');
        if (container) container.style.display = 'none';
        if (area) area.style.display = 'none';
    }

    handleMapUpdateRequest(request) {
        if (window._disambiguationMode) this.clearDisambiguationMode();
 
        if (request.places !== undefined) {
            const storeNow = (window.vobUtils?.getMapState?.() || {});
            const effectivePlaces = request.places || [];
            // Guard against stale server echoes: if server's signature equals the last one we applied,
            // and the client has since diverged, skip applying this update.
            try {
                const clientSig = this._placesSignature(Array.isArray(storeNow.places) ? storeNow.places : []);
                const requestedSig = this._placesSignature(effectivePlaces);
                // If nothing changes, avoid redundant store writes/zoom
                if (requestedSig === clientSig) {
                    this.lastPlacesSig = requestedSig;
                    this._LOG && this._LOG('handleMapUpdateRequest(): no-op (same selection)', { sig: requestedSig });
                    return;
                }
                if (requestedSig === this.lastPlacesSig && clientSig !== requestedSig) {
                    this._LOG && this._LOG('handleMapUpdateRequest(): skip stale echo', { requestedSig, lastSig: this.lastPlacesSig, clientSig });
                    return;
                }
                // Additional deterministic guard:
                // If client currently has a non-empty selection and the server request would strictly reduce it
                // (e.g., empty), but we haven't yet accepted any server signature for this client state,
                // skip once to avoid flicker right after a local click-add.
                const toSet = (placesArr) => {
                    const s = new Set();
                    (placesArr || []).forEach(p => { if (p && p.g_unit != null) s.add(`${String(p.g_unit)}:${p.g_unit_type || ''}`); });
                    return s;
                };
                const clientSet = toSet(storeNow.places || []);
                const reqSet = toSet(effectivePlaces || []);
                if (clientSet.size > 0 && reqSet.size < clientSet.size && (!this.lastPlacesSig || this.lastPlacesSig !== clientSig)) {
                    this._LOG && this._LOG('handleMapUpdateRequest(): skip reducing set (likely pre-ack echo)', { clientSize: clientSet.size, reqSize: reqSet.size, lastSig: this.lastPlacesSig, clientSig });
                    return;
                }
            } catch (_) { }
            // Sync places + unit_types to map-state; hideout selection is derived elsewhere
            try {
                const mapRef = document.getElementById('leaflet-map')?._leaflet_map;
                const unitTypesAll = window.vobUtils.getUnitTypes(mapRef, { places: effectivePlaces }, storeNow);
                const uniqueAll = Array.isArray(unitTypesAll) ? [...new Set(unitTypesAll)] : [];
                // Prefer last resolved place's unit type for the active button state
                let lastUT = null;
                try {
                    for (let i = (effectivePlaces || []).length - 1; i >= 0; i--) {
                        const p = effectivePlaces[i];
                        if (p && p.g_unit != null && p.g_unit_type) { lastUT = p.g_unit_type; break; }
                    }
                } catch (_) {}
                const unit_types = lastUT ? [lastUT] : uniqueAll;
                window.vobUtils.syncMapState({ unit_types, places: effectivePlaces });
                // Refresh the layer now to reflect active unit type (clears other types from view)
                try {
                    if (mapRef && window.polygonManagement && window.polygonManagement.updateMapWithBounds) {
                        const storeRef = window.vobUtils?.getMapState?.() || {};
                        const yr = storeRef.year_range ? { min: storeRef.year_range[0], max: storeRef.year_range[1] } : null;
                        window.polygonManagement.updateMapWithBounds(mapRef, unit_types, mapRef.getBounds(), Object.assign({}, storeRef, { unit_types }), yr)
                            .catch(() => { /* silent */ });
                    }
                } catch (_) {}
            } catch (_) { }
 
            this.lastPlacesSig = this._placesSignature(effectivePlaces);
 
            // Ensure polygons are present, then zoom if needed
            const units = this.getSelectedUnits({ places: effectivePlaces });
            const selectedPlaceCoords = request.selected_place_coordinates || [];
            if (units && units.length > 0) {
                this.fetchPolygonsAndZoom(units, null, effectivePlaces, selectedPlaceCoords);
            } else if (selectedPlaceCoords.length > 0) {
                const mapElement = document.getElementById('leaflet-map');
                if (mapElement && mapElement._leaflet_map) {
                    this.calculateCombinedZoomBounds(mapElement._leaflet_map, null, [], selectedPlaceCoords);
                }
            }
        }
    }

    handleInfoMarkerRequest(request) {
        // unchanged
        if (!request.info_place) return;
        const infoPlace = request.info_place;
        const placeCoordinates = [{
            index: 0, name: infoPlace.name, county: infoPlace.county_name || '',
            lat: infoPlace.coordinates.lat, lon: infoPlace.coordinates.lon,
            g_place: infoPlace.g_place, is_single: true, is_info_marker: true
        }];

        const interruptData = {
            place_coordinates: placeCoordinates,
            current_node: 'PlaceInfo_node',
            is_info_marker: true,
            message: `Information about ${infoPlace.name}`
        };
        if (typeof dash_clientside !== 'undefined' && dash_clientside.set_props) {
            dash_clientside.set_props('sse-interrupt-store', { data: {} });
            setTimeout(() => dash_clientside.set_props('sse-interrupt-store', { data: interruptData }), 0);
        }
        const mapElement = document.getElementById('leaflet-map');
        if (!mapElement || !mapElement._leaflet_map) return;
        const mapInstance = mapElement._leaflet_map;

        try {
            if (infoPlace.g_unit) {
                const layer = window.polygonManagement?.findGeoJSONLayer?.(mapInstance);
                if (layer && layer._layers) {
                    let found = null;
                    Object.values(layer._layers).forEach(l => { if (l?.feature?.id && String(l.feature.id) === String(infoPlace.g_unit)) found = l; });
                    if (found?.getBounds) { mapInstance.fitBounds(found.getBounds()); return; }
                }
            }
        } catch (_) { }

        const latOffset = 0.045, lonOffset = 0.065;
        const bounds = [
            [infoPlace.coordinates.lat - latOffset, infoPlace.coordinates.lon - lonOffset],
            [infoPlace.coordinates.lat + latOffset, infoPlace.coordinates.lon + lonOffset]
        ];
        mapInstance.fitBounds(bounds, { padding: [50, 50] });
    }

    handleUnitsNeedingSelection(units, places) {
        if (!places) return;
        const allUnits = this.getSelectedUnits({ places });
        const allUnitTypes = this.getSelectedUnitTypes({ places });
        try { window.vobUtils?.syncMapState?.({ places }); } catch (_) { }
        // Mirror to layer.hideout immediately
        this.setLayerSelectionFromPlaces(places);
        this.fetchPolygonsAndZoom(allUnits, allUnitTypes, places);
    }

    calculateCombinedZoomBounds(map, layer, polygonIds, placeCoordinates) {
        const bounds = window.L.latLngBounds();
        let hasContent = false;
        if (layer && layer._layers && polygonIds && polygonIds.length > 0) {
            Object.values(layer._layers).forEach(layerObj => {
                if (layerObj.feature && polygonIds.includes(String(layerObj.feature.id))) {
                    if (layerObj.getBounds) { bounds.extend(layerObj.getBounds()); hasContent = true; }
                }
            });
        }
        if (placeCoordinates?.length) {
            placeCoordinates.forEach(coord => { bounds.extend([coord.lat, coord.lon]); hasContent = true; });
        }
        if (hasContent && bounds.isValid()) {
            map.fitBounds(bounds, { padding: [30, 30], maxZoom: 12 });
            return true;
        } else if (placeCoordinates && placeCoordinates.length === 1) {
            map.setView([placeCoordinates[0].lat, placeCoordinates[0].lon], 10);
            return true;
        }
        return false;
    }

    fetchPolygonsAndZoom(units, unitTypes, places, includePlaceCoordinates = []) {
        if (!Array.isArray(units)) return;
        if (!Array.isArray(unitTypes)) unitTypes = [];

        if (units.length && window.polygonManagement?.fetchPolygonsByIds) {
            const validUnits = units.filter(u => u != null && u !== '');
            if (!validUnits.length) return;

            const map = document.getElementById('leaflet-map')?._leaflet_map;
            if (!map) return;

            const idToType = {};
            if (Array.isArray(places) && places.length >= validUnits.length) {
                (places || []).forEach(p => { if (p && p.g_unit != null) idToType[String(p.g_unit)] = p.g_unit_type || null; });
            } else {
                for (let i = 0; i < validUnits.length; i++) {
                    const id = String(validUnits[i]);
                    idToType[id] = unitTypes[i] || (unitTypes.length === 1 ? unitTypes[0] : null);
                }
            }
            const byType = {};
            validUnits.map(String).forEach(id => { const ut = idToType[id]; if (ut) (byType[ut] ||= []).push(id); });

            const mapState = { places: places || [] };
            const existingLayer = window.polygonManagement.findGeoJSONLayer ? window.polygonManagement.findGeoJSONLayer(map) : null;
            const requestedIds = validUnits.map(String);
            let allPresent = true;
            const fetchPromises = [];

            if (existingLayer && existingLayer._layers) {
                const layerIds = [];
                Object.values(existingLayer._layers).forEach(l => { if (l.feature?.id) layerIds.push(String(l.feature.id)); });
                const globalMissing = requestedIds.filter(id => !layerIds.includes(id));
                if (globalMissing.length) allPresent = false;

                Object.entries(byType).forEach(([ut, ids]) => {
                    const missing = ids.filter(id => !layerIds.includes(id));
                    if (missing.length) {
                        fetchPromises.push(window.polygonManagement.fetchPolygonsByIds(map, mapState, ut, missing, null));
                    }
                });
            } else {
                allPresent = false;
                Object.entries(byType).forEach(([ut, ids]) => {
                    fetchPromises.push(window.polygonManagement.fetchPolygonsByIds(map, mapState, ut, ids, null));
                });
            }

            const afterEnsurePolygons = (layer) => {
                // Selection highlight remains driven by map-state -> hideout

                const recentLocalZoom = (function () {
                    try {
                        const tsOk = typeof window._lastLocalZoomTs === 'number' && (Date.now() - window._lastLocalZoomTs) < 1500;
                        const sameIds = Array.isArray(window._lastLocalZoomIds) &&
                            window._lastLocalZoomIds.slice().sort().join(',') === requestedIds.slice().sort().join(',');
                        return tsOk && sameIds;
                    } catch (e) { return false; }
                })();

                let zoomWasPerformed = false;
                if (!recentLocalZoom) {
                    let zoomApplied = false;
                    if (includePlaceCoordinates?.length) {
                        zoomApplied = this.calculateCombinedZoomBounds(map, layer, requestedIds, includePlaceCoordinates);
                    }
                    if (!zoomApplied && window.polygonManagement?.zoomTo) {
                        try { window._zoomSource = 'sse'; } catch (e) { }
                        window.polygonManagement.zoomTo(map, requestedIds, layer);
                        zoomWasPerformed = true;
                    }
                }
                // keep unit_types in sync if needed
                if (unitTypes.length && requestedIds.length) {
                    const uniqueUnitTypes = [...new Set(unitTypes)];
                    const storeUT = ((window.vobUtils?.getMapState?.() || {}).unit_types || []).slice().sort().join(',');
                    const requestedUnitTypes = uniqueUnitTypes.slice().sort().join(',');
                    if (storeUT !== requestedUnitTypes) {
                        setTimeout(() => { try { window.vobUtils?.syncMapState?.({ unit_types: uniqueUnitTypes }); } catch (_) { } }, 300);
                    }
                }
            };

            if (allPresent) {
                afterEnsurePolygons(existingLayer);
            } else {
                Promise.all(fetchPromises).then(() => {
                    const layer = window.polygonManagement.findGeoJSONLayer ? window.polygonManagement.findGeoJSONLayer(map) : null;
                    afterEnsurePolygons(layer);
                }).catch(() => {
                    if (unitTypes.length) {
                        const uniqueUnitTypes = [...new Set(unitTypes)];
                        try { window.vobUtils?.syncMapState?.({ unit_types: uniqueUnitTypes }); } catch (_) { }
                    }
                });
            }
        } else if (unitTypes.length) {
            const uniqueUnitTypes = [...new Set(unitTypes)];
            try { window.vobUtils?.syncMapState?.({ unit_types: uniqueUnitTypes }); } catch (_) { }
        }
    }

    getSelectedUnits(state) {
        const places = state.places || [];
        return places.map(place => place.g_unit).filter(unit => unit !== null && unit !== undefined);
    }
    getSelectedUnitTypes(state) {
        const places = state.places || [];
        const result = [];
        places.forEach(place => { if (place.g_unit !== null && place.g_unit !== undefined) result.push(place.g_unit_type); });
        return result;
    }

    updateChatDisplay(messages) {
        const chatDisplay = document.getElementById('chat-display');
        if (!chatDisplay) return;
        chatDisplay.innerHTML = '';
        for (let i = messages.length - 1; i >= 0; i--) {
            const msg = messages[i];
            if (!msg) continue;
            const messageDiv = document.createElement('div');
            let content = '';
            let className = 'speech-bubble';
            if (msg._type === 'human' || msg._type === 'HumanMessage') { content = msg.content; className += ' user-bubble'; }
            else if (msg._type === 'ai' || msg._type === 'AIMessage') { content = msg.content; className += ' ai-bubble'; }
            else if (typeof msg === 'object' && msg.content) { content = msg.content; className += (msg.type === 'human' ? ' user-bubble' : ' ai-bubble'); }
            else if (Array.isArray(msg) && msg.length >= 2) { content = msg[1]; className += ((msg[0] === 'user' || msg[0] === 'human') ? ' user-bubble' : ' ai-bubble'); }
            if (content) {
                messageDiv.className = className;
                const parser = new DOMParser();
                const doc = parser.parseFromString(content, 'text/html');
                messageDiv.innerHTML = '';
                while (doc.body.firstChild) messageDiv.appendChild(doc.body.firstChild);
                chatDisplay.appendChild(messageDiv);
            }
        }
        if (this.llmBusy) this.appendThinkingMessage(chatDisplay);
        const sendButton = document.getElementById('send-button');
        if (sendButton) sendButton.disabled = false;
    }

    appendThinkingMessage(chatDisplay) {
        try {
            let messageDiv = document.getElementById('ai-thinking');
            if (messageDiv && messageDiv.parentNode) messageDiv.parentNode.removeChild(messageDiv);
            if (!messageDiv) {
                messageDiv = document.createElement('div');
                messageDiv.className = 'speech-bubble ai-bubble';
                messageDiv.id = 'ai-thinking';
                const spinner = document.createElement('span');
                spinner.className = 'spinner-border spinner-border-sm text-primary me-2';
                spinner.setAttribute('role', 'status'); spinner.setAttribute('aria-hidden', 'true');
                const text = document.createElement('span');
                text.textContent = 'Thinking…'; text.className = 'text-muted';
                messageDiv.appendChild(spinner); messageDiv.appendChild(text);
            }
            const first = chatDisplay.firstChild;
            if (first) chatDisplay.insertBefore(messageDiv, first); else chatDisplay.appendChild(messageDiv);
            chatDisplay.scrollTop = chatDisplay.scrollHeight;
        } catch (e) { }
    }
    showThinkingIndicator() { try { const chatDisplay = document.getElementById('chat-display'); if (chatDisplay) this.appendThinkingMessage(chatDisplay); } catch (e) { } }
    hideThinkingIndicator() { try { const el = document.getElementById('ai-thinking'); if (el?.parentNode) el.parentNode.removeChild(el); } catch (e) { } }
}

window.simpleSSE = new SimpleSSEClient();
window.connectSSE = function (threadId) { if (threadId) window.simpleSSE.connect(threadId); };
document.addEventListener('DOMContentLoaded', () => { });
