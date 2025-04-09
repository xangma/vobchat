// app/assets/resizable.js

let userHasResized = false; // Flag to track manual resizing
let isVizVisible = false; // Track visualization visibility
let initializationComplete = false; // Flag to prevent multiple initializations
let retryCount = 0; // Counter for retries
const MAX_RETRIES = 50; // Limit retries (e.g., 50 * 100ms = 5 seconds)

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
    }, 200);
});