// app/assets/resizable.js

let userHasResized = false; // Flag to track manual resizing
let isVizVisible = false; // Track visualization visibility
let initializationComplete = false; // Flag to prevent multiple initializations
let retryCount = 0; // Counter for retries
const MAX_RETRIES = 50; // Limit retries (e.g., 50 * 100ms = 5 seconds)
let mapRefitTimeout = null; // Debounce refit calls
const MAP_REFIT_DELAY_MS = 150; // Small delay to let layout settle

// --- Initialization Trigger ---
// Use DOMContentLoaded as the starting point, but delegate actual init to a check function
document.addEventListener('DOMContentLoaded', function () {
    console.log("JS: DOMContentLoaded. Starting initialization checks...");
    checkAndInitializeLayout();
});

// --- Check and Initialize Function ---
function checkAndInitializeLayout() {
    if (initializationComplete) return; // Don't run init multiple times

    // List of essential element IDs
    const requiredElementIds = [
        'chat-panel',
        'visualization-panel-container',
        'map-panel',
        'resize-handle-1',
        'resize-handle-2',
        'visualization-area' // Also wait for the inner viz area for the observer
    ];

    // Check if all required elements exist
    const allElementsFound = requiredElementIds.every(id => document.getElementById(id));

    if (allElementsFound) {
        console.log("JS: checkAndInitializeLayout - All required elements found. Initializing now.");
        initializationComplete = true; // Set flag
        // Proceed with the actual initialization logic
        updateVizVisibilityState();
        initResizable();
        setInitialPanelSizes();
        setupVisibilityObserver();
        setupMapPanelResizeObserver();
    } else {
        retryCount++;
        if (retryCount > MAX_RETRIES) {
            console.error("JS: checkAndInitializeLayout - Max retries exceeded. Failed to find all required elements. Layout initialization aborted.");
            // Log which elements are missing
            requiredElementIds.forEach(id => {
                if (!document.getElementById(id)) {
                    console.error(`JS: Missing element: #${id}`);
                }
            });
        } else {
            // console.log(`JS: checkAndInitializeLayout - Elements not ready yet. Retrying (${retryCount}/${MAX_RETRIES})...`);
            // Retry after a short delay
            setTimeout(checkAndInitializeLayout, 100); // Retry every 100ms
        }
    }
}


// --- Rest of the functions (Keep them as they were in the previous correct version) ---

function updateVizVisibilityState() {
    const vizPanelContainer = document.getElementById('visualization-panel-container');
    if (vizPanelContainer) {
        const currentDisplay = window.getComputedStyle(vizPanelContainer).display;
        isVizVisible = currentDisplay !== 'none';
        // console.log(`JS: updateVizVisibilityState - Viz display='${currentDisplay}', isVizVisible=${isVizVisible}`);
    } else {
        // This should ideally not happen if checkAndInitializeLayout worked
        console.warn("JS: updateVizVisibilityState - Visualization panel container not found (unexpected).");
        isVizVisible = false;
    }
}

function initResizable() {
    console.log("JS: initResizable - Attaching listeners...");
    const resizer1 = document.getElementById('resize-handle-1');
    const resizer2 = document.getElementById('resize-handle-2');

    const attachListener = (resizer, handleIndex) => {
        if (resizer) {
            // console.log(`JS: Attaching mousedown listener to #resize-handle-${handleIndex}`);
            const mouseDownHandler = (e) => {
                // console.log(`JS: Mousedown event registered on handle ${handleIndex}`);
                initHorizontalDrag(e, handleIndex);
            };
            resizer.removeEventListener('mousedown', mouseDownHandler); // Clean up just in case
            resizer.addEventListener('mousedown', mouseDownHandler);
        } else {
            console.warn(`JS: #resize-handle-${handleIndex} not found during listener attachment.`);
        }
    };

    attachListener(resizer1, 1);
    attachListener(resizer2, 2);
}


function initHorizontalDrag(e, handleIndex) {
    e.preventDefault();
    // console.log(`JS: initHorizontalDrag - Started for handle ${handleIndex}`);
    userHasResized = true;

    const resizer = e.target;
    if (!resizer || !resizer.classList.contains('resize-handle-horizontal')) {
        console.warn("JS: initHorizontalDrag - Event target is not the resize handle. Aborting drag.");
        return;
    }
    resizer.classList.add('active');

    const chatPanel = document.getElementById('chat-panel');
    const vizPanel = document.getElementById('visualization-panel-container');
    const mapPanel = document.getElementById('map-panel');

    if (!chatPanel || !vizPanel || !mapPanel) {
        console.error("JS: initHorizontalDrag - One or more panels not found!");
        resizer.classList.remove('active');
        return;
    }

    const startX = e.clientX;
    const chatStartWidth = chatPanel.getBoundingClientRect().width;
    const vizStartWidth = isVizVisible ? vizPanel.getBoundingClientRect().width : 0;
    const mapStartWidth = mapPanel.getBoundingClientRect().width;
    const totalWidth = chatStartWidth + vizStartWidth + mapStartWidth;

    function doDragHorizontal(e) {
        const deltaX = e.clientX - startX;
        const absoluteMinWidth = 150;
        let newChatWidth, newVizWidth, newMapWidth;
        let currentTotalWidth = totalWidth;

        // --- Calculations (identical to previous version) ---
        if (handleIndex === 1) {
            newChatWidth = chatStartWidth + deltaX;
            if (isVizVisible) { newVizWidth = vizStartWidth - deltaX; newMapWidth = mapStartWidth; }
            else { newVizWidth = 0; newMapWidth = mapStartWidth - deltaX; }
        } else {
            if (!isVizVisible) return;
            newChatWidth = chatStartWidth; newVizWidth = vizStartWidth + deltaX; newMapWidth = mapStartWidth - deltaX;
        }

        // --- Constraints and Redistribution (identical to previous version) ---
        if (isVizVisible) {
            if (newChatWidth < absoluteMinWidth) { const needed = absoluteMinWidth - newChatWidth; newChatWidth = absoluteMinWidth; if (handleIndex === 1) newVizWidth -= needed; }
            if (newMapWidth < absoluteMinWidth) { const needed = absoluteMinWidth - newMapWidth; newMapWidth = absoluteMinWidth; if (handleIndex === 2) newVizWidth -= needed; }
            if (newVizWidth < absoluteMinWidth) { const needed = absoluteMinWidth - newVizWidth; newVizWidth = absoluteMinWidth; if (handleIndex === 1) newChatWidth -= needed; else if (handleIndex === 2) newMapWidth -= needed; }
            newChatWidth = Math.max(newChatWidth, absoluteMinWidth); newVizWidth = Math.max(newVizWidth, absoluteMinWidth); newMapWidth = Math.max(newMapWidth, absoluteMinWidth);
            const constrainedSum = newChatWidth + newVizWidth; newMapWidth = Math.max(absoluteMinWidth, currentTotalWidth - constrainedSum);
            if (currentTotalWidth - newMapWidth < newChatWidth + absoluteMinWidth && handleIndex === 2) { newVizWidth = Math.max(absoluteMinWidth, currentTotalWidth - newMapWidth - newChatWidth); }
            if (currentTotalWidth - newMapWidth < newVizWidth + absoluteMinWidth && handleIndex === 1) { newChatWidth = Math.max(absoluteMinWidth, currentTotalWidth - newMapWidth - newVizWidth); }
        } else {
            if (newChatWidth < absoluteMinWidth) { const needed = absoluteMinWidth - newChatWidth; newChatWidth = absoluteMinWidth; newMapWidth -= needed; }
            if (newMapWidth < absoluteMinWidth) { const needed = absoluteMinWidth - newMapWidth; newMapWidth = absoluteMinWidth; newChatWidth -= needed; }
            newChatWidth = Math.max(newChatWidth, absoluteMinWidth); newMapWidth = Math.max(absoluteMinWidth, currentTotalWidth - newChatWidth); newChatWidth = Math.max(absoluteMinWidth, currentTotalWidth - newMapWidth);
            newVizWidth = 0;
        }

        // --- Apply Styles ---
        chatPanel.style.flex = `0 0 ${newChatWidth}px`;
        vizPanel.style.flex = `0 0 ${newVizWidth}px`;
        mapPanel.style.flex = `1 1 ${newMapWidth}px`;

        window.dispatchEvent(new Event('resize'));
    }

    function stopDragHorizontal(e) {
        // console.log(`JS: stopDragHorizontal - Mouseup detected for handle ${handleIndex}`);
        if (resizer) { resizer.classList.remove('active'); }
        document.removeEventListener('mousemove', doDragHorizontal);
        document.removeEventListener('mouseup', stopDragHorizontal);
        // resetFlexBasisToPercentage(); // Optional
        // After user completes drag, refit the map to selection or all
        scheduleMapRefit('drag_end');
    }

    document.addEventListener('mousemove', doDragHorizontal);
    document.addEventListener('mouseup', stopDragHorizontal);
}


function setInitialPanelSizes() {
    if (userHasResized) { return; }

    const chatPanel = document.getElementById('chat-panel');
    const vizPanel = document.getElementById('visualization-panel-container');
    const mapPanel = document.getElementById('map-panel');
    const resizer1 = document.getElementById('resize-handle-1');
    const resizer2 = document.getElementById('resize-handle-2');

    // This check should ideally not be needed now, but keep as safeguard
    if (!chatPanel || !vizPanel || !mapPanel || !resizer1 || !resizer2) {
        console.warn("JS: setInitialPanelSizes - Panels or resizers not found (unexpected after init check).");
        return;
    }

    updateVizVisibilityState(); // Ensure isVizVisible is current

    // console.log(`JS: setInitialPanelSizes - Setting initial sizes. Viz Visible: ${isVizVisible}`);

    if (isVizVisible) {
        chatPanel.style.flex = '0 0 30%';
        vizPanel.style.flex = '0 0 40%';
        mapPanel.style.flex = '1 1 30%';
        vizPanel.style.display = 'flex';
        resizer2.style.display = 'flex';
    } else {
        chatPanel.style.flex = '0 0 30%';
        vizPanel.style.flex = '0 0 0%';
        mapPanel.style.flex = '1 1 70%';
        vizPanel.style.display = 'none';
        resizer2.style.display = 'none';
    }
    // No resize event dispatch needed here anymore
}


function setupVisibilityObserver() {
    const visualizationArea = document.getElementById('visualization-area');
    const vizPanelContainer = document.getElementById('visualization-panel-container');
    const resizer2 = document.getElementById('resize-handle-2');

    // This check should also not be needed if checkAndInitializeLayout worked
    if (!visualizationArea || !vizPanelContainer || !resizer2) {
        console.warn("JS: setupVisibilityObserver - Required elements not found (unexpected after init check).");
        return;
    }

    const observer = new MutationObserver(mutations => {
        let visibilityChanged = false;
        mutations.forEach(mutation => {
            if (mutation.attributeName === 'style') {
                const newDisplay = window.getComputedStyle(visualizationArea).display;
                const currentlyVisible = newDisplay !== 'none';
                if (currentlyVisible !== isVizVisible) {
                    isVizVisible = currentlyVisible;
                    visibilityChanged = true;
                }
            }
        });

        if (visibilityChanged) {
            // console.log(`JS Observer: Visibility changed to ${isVizVisible}. Resetting sizes and handle visibility.`);
            vizPanelContainer.style.display = isVizVisible ? 'flex' : 'none';
            resizer2.style.display = isVizVisible ? 'flex' : 'none';
            userHasResized = false; // Reset resize flag on visibility change
            setInitialPanelSizes(); // Re-apply layout
            // After layout toggles, refit the map to selection (or all)
            scheduleMapRefit('viz_visibility_change');
        }
    });

    observer.observe(visualizationArea, { attributes: true, attributeFilter: ['style'] });
    console.log("JS: Visibility observer attached to #visualization-area.");
}

// Debounced window resize handler (Keep as is)
let resizeTimeout;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
        if (!userHasResized) { setInitialPanelSizes(); }
        // Always try to refit map after window resize settles
        scheduleMapRefit('window_resize');
    }, 200);
});
 
// --- Map refit helpers ---
function scheduleMapRefit(reason) {
    try {
        if (mapRefitTimeout) clearTimeout(mapRefitTimeout);
        mapRefitTimeout = setTimeout(() => {
            try { refitLeafletMap(reason); } catch (e) { /* no-op */ }
        }, MAP_REFIT_DELAY_MS);
    } catch (_) { /* swallow */ }
}
 
function refitLeafletMap(reason) {
    const mapEl = document.getElementById('leaflet-map');
    const map = mapEl?._leaflet_map;
    if (!map) {
        // Map not ready yet; try once more shortly
        setTimeout(() => { try { refitLeafletMap(reason + ':retry'); } catch (_) {} }, 120);
        return;
    }
    // Invalidate Leaflet's cached size so it reacts to container width changes
    try { map.invalidateSize(true); } catch (_) {}
 
    const doZoom = () => {
        // Find the GeoJSON layer and selected ids
        const layer = (window.polygonManagement && window.polygonManagement.findGeoJSONLayer)
            ? window.polygonManagement.findGeoJSONLayer(map)
            : null;
        if (!layer || !layer._layers) return;
 
        let selectedIds = [];
        try {
            // Prefer robust util that checks map-state, place-state, then layer.hideout
            selectedIds = (window.vobUtils && window.vobUtils.getSelectedIds)
                ? window.vobUtils.getSelectedIds(map, (window.vobUtils.getPlaceState && window.vobUtils.getPlaceState()) || {}, (window.vobUtils.getMapState && window.vobUtils.getMapState()) || {}, layer)
                : ((layer.options?.hideout?.selected) || []).map(String);
        } catch (_) {
            selectedIds = ((layer.options && layer.options.hideout && Array.isArray(layer.options.hideout.selected))
                ? layer.options.hideout.selected
                : []).map(String);
        }
 
        // If any selected, fit to them; otherwise fit to all currently visible polygons (unselected)
        try {
            if (window.polygonManagement && typeof window.polygonManagement.zoomTo === 'function') {
                if (selectedIds && selectedIds.length > 0) {
                    try { window._zoomSource = 'resize_selected'; } catch (_) {}
                    window.polygonManagement.zoomTo(map, selectedIds, layer);
                } else {
                    // For dense unit types (non-region/county), prefer a fixed fallback zoom instead of fitting all
                    let uts = [];
                    try {
                        uts = (window.vobUtils && window.vobUtils.getUnitTypes)
                            ? window.vobUtils.getUnitTypes(map, window.vobUtils.getPlaceState?.(), window.vobUtils.getMapState?.())
                            : [];
                    } catch (_) {}
                    const DENSE_UNITS = new Set(['CONSTITUENCY','LG_DIST','MOD_DIST','MOD_WARD']);
                    const hasDense = Array.isArray(uts) ? uts.some(u => DENSE_UNITS.has(u)) : false;
                    if (hasDense) {
                        // Snap to fallback zoom level for better context when many polygons exist
                        const target = window.VOB_FALLBACK_ZOOM_DENSE || 8;
                        try { window._zoomSource = 'resize_dense_fallback'; } catch (_) {}
                        map.setZoom(target, { animate: true });
                    } else {
                        try { window._zoomSource = 'resize_all'; } catch (_) {}
                        window.polygonManagement.zoomTo(map, null, layer);
                    }
                }
            }
        } catch (_) { /* ignore */ }
    };
 
    // Wait until the map panel width stabilizes before zooming to avoid race conditions
    waitForStableWidth(mapEl, doZoom, 600, 2, 0.5);
}
 
// Optional: observe the map panel for size changes independent of window resize
function setupMapPanelResizeObserver() {
    const mapPanel = document.getElementById('map-panel');
    if (!mapPanel || typeof ResizeObserver === 'undefined') return;
    try {
        let lastWidth = -1;
        const ro = new ResizeObserver((entries) => {
            for (const entry of entries) {
                if (!entry || !entry.contentRect) continue;
                const w = entry.contentRect.width;
                if (lastWidth >= 0 && Math.abs(w - lastWidth) < 0.5) {
                    continue;
                }
                lastWidth = w;
                scheduleMapRefit('map_panel_resize');
            }
        });
        ro.observe(mapPanel);
    } catch (e) {
        // Fallback to window resize only
    }
}
 
// Utility: wait for element width to be stable for N consecutive frames (or timeout)
function waitForStableWidth(el, callback, timeoutMs = 600, consecutiveFrames = 2, thresholdPx = 1) {
    if (!el || typeof requestAnimationFrame === 'undefined') {
        // Best effort fallback
        setTimeout(callback, MAP_REFIT_DELAY_MS);
        return;
    }
    let last = -1;
    let stable = 0;
    const started = performance.now();
    const step = () => {
        const rect = el.getBoundingClientRect();
        const w = rect ? rect.width : 0;
        if (last >= 0 && Math.abs(w - last) <= thresholdPx) {
            stable += 1;
        } else {
            stable = 0;
        }
        last = w;
        const timedOut = (performance.now() - started) > timeoutMs;
        if (stable >= consecutiveFrames || timedOut) {
            // One extra frame to allow layout to settle after stability detected
            requestAnimationFrame(() => callback());
        } else {
            requestAnimationFrame(step);
        }
    };
    requestAnimationFrame(step);
}
