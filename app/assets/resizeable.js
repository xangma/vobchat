// app/assets/resizable.js

// Global flag to track if panels have been manually resized
let userHasResized = false;

// Wait for the DOM to be fully loaded
document.addEventListener('DOMContentLoaded', function () {
    initResizable();
    // Only set initial sizes if the user hasn't resized yet
    if (!userHasResized) {
        setInitialPanelSizes();
    }
    setupVisibilityObserver();
});

function initResizable() {
    // Initialize horizontal resizers
    const horizontalResizers = document.querySelectorAll('.resize-handle-horizontal');
    horizontalResizers.forEach(resizer => {
        resizer.addEventListener('mousedown', initHorizontalDrag);
    });

    // Initialize vertical resizers
    const verticalResizers = document.querySelectorAll('.resize-handle-vertical');
    verticalResizers.forEach(resizer => {
        resizer.addEventListener('mousedown', initVerticalDrag);
    });
}

function initHorizontalDrag(e) {
    e.preventDefault();

    const resizer = e.target;
    resizer.classList.add('active');

    // Get the panels to resize
    const prevPanel = resizer.previousElementSibling;
    const nextPanel = resizer.nextElementSibling;

    // Get the initial mouse position and panel sizes
    const startX = e.clientX;
    const prevPanelStartWidth = prevPanel.getBoundingClientRect().width;
    const nextPanelStartWidth = nextPanel.getBoundingClientRect().width;

    // Add mousemove and mouseup event listeners
    document.addEventListener('mousemove', doDragHorizontal);
    document.addEventListener('mouseup', stopDragHorizontal);

    function doDragHorizontal(e) {
        // Mark that the user has manually resized panels
        userHasResized = true;

        // Calculate the distance moved
        const deltaX = e.clientX - startX;

        // Calculate new widths
        let newPrevPanelWidth = prevPanelStartWidth + deltaX;
        let newNextPanelWidth = nextPanelStartWidth - deltaX;

        // Calculate total width
        const totalWidth = prevPanelStartWidth + nextPanelStartWidth;

        // Set absolute minimum width for each panel in pixels
        const absoluteMinWidth = 250;

        // Set relative minimum width (percentage of total)
        const relativeMinWidth = totalWidth * 0.2; // 20% of total width

        // Use the larger of the two minimums
        const minWidth = Math.max(absoluteMinWidth, relativeMinWidth);

        // Set maximum width (80% of total)
        const maxWidth = totalWidth * 0.8;

        // Apply constraints
        if (newPrevPanelWidth < minWidth) {
            newPrevPanelWidth = minWidth;
            newNextPanelWidth = totalWidth - minWidth;
        } else if (newNextPanelWidth < minWidth) {
            newNextPanelWidth = minWidth;
            newPrevPanelWidth = totalWidth - minWidth;
        } else if (newPrevPanelWidth > maxWidth) {
            newPrevPanelWidth = maxWidth;
            newNextPanelWidth = totalWidth - maxWidth;
        } else if (newNextPanelWidth > maxWidth) {
            newNextPanelWidth = maxWidth;
            newPrevPanelWidth = totalWidth - maxWidth;
        }

        // Update widths
        prevPanel.style.flex = `0 0 ${newPrevPanelWidth}px`;
        nextPanel.style.flex = `0 0 ${newNextPanelWidth}px`;

        // Trigger window resize event to refresh any internal components
        window.dispatchEvent(new Event('resize'));
    }

    function stopDragHorizontal() {
        resizer.classList.remove('active');
        document.removeEventListener('mousemove', doDragHorizontal);
        document.removeEventListener('mouseup', stopDragHorizontal);
    }
}

function initVerticalDrag(e) {
    e.preventDefault();

    const resizer = e.target;
    resizer.classList.add('active');

    // Get the panels to resize
    const prevPanel = resizer.previousElementSibling;
    const nextPanel = resizer.nextElementSibling;

    // Get the initial mouse position and panel sizes
    const startY = e.clientY;
    const prevPanelStartHeight = prevPanel.getBoundingClientRect().height;
    const nextPanelStartHeight = nextPanel.getBoundingClientRect().height;

    // Add mousemove and mouseup event listeners
    document.addEventListener('mousemove', doDragVertical);
    document.addEventListener('mouseup', stopDragVertical);

    function doDragVertical(e) {
        // Mark that the user has manually resized panels
        userHasResized = true;

        // Calculate the distance moved
        const deltaY = e.clientY - startY;

        // Calculate new heights
        let newPrevPanelHeight = prevPanelStartHeight + deltaY;
        let newNextPanelHeight = nextPanelStartHeight - deltaY;

        // Calculate total height
        const totalHeight = prevPanelStartHeight + nextPanelStartHeight;

        // Set absolute minimum height for each panel in pixels
        const absoluteMinHeight = 200;

        // Set relative minimum height (percentage of total)
        const relativeMinHeight = totalHeight * 0.2; // 20% of total height

        // Use the larger of the two minimums
        const minHeight = Math.max(absoluteMinHeight, relativeMinHeight);

        // Set maximum height (75% of total) - allow visualization to be larger than chat+map if needed
        const maxHeight = totalHeight * 0.75;

        // Apply constraints
        if (newPrevPanelHeight < minHeight) {
            newPrevPanelHeight = minHeight;
            newNextPanelHeight = totalHeight - minHeight;
        } else if (newNextPanelHeight < minHeight) {
            newNextPanelHeight = minHeight;
            newPrevPanelHeight = totalHeight - minHeight;
        } else if (newPrevPanelHeight > maxHeight) {
            newPrevPanelHeight = maxHeight;
            newNextPanelHeight = totalHeight - maxHeight;
        } else if (newNextPanelHeight > maxHeight) {
            newNextPanelHeight = maxHeight;
            newPrevPanelHeight = totalHeight - maxHeight;
        }

        // Update heights
        prevPanel.style.flex = `0 0 ${newPrevPanelHeight}px`;
        nextPanel.style.flex = `0 0 ${newNextPanelHeight}px`;

        // Trigger window resize event to refresh any internal components
        window.dispatchEvent(new Event('resize'));
    }

    function stopDragVertical() {
        resizer.classList.remove('active');
        document.removeEventListener('mousemove', doDragVertical);
        document.removeEventListener('mouseup', stopDragVertical);
    }
}

// Re-initialize resizable elements when the layout changes
// (using MutationObserver to detect when components are added/removed)
const observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
        if (mutation.addedNodes.length > 0) {
            initResizable();
        }
    });
});

// Set initial panel sizes for better default appearance
function setInitialPanelSizes() {
    // Only set initial sizes if the user hasn't manually resized
    if (userHasResized) {
        return;
    }

    // Set initial horizontal split (50% for chat, 50% for map)
    const horizontalContainer = document.querySelector('.resizable-horizontal');
    if (horizontalContainer) {
        const panels = horizontalContainer.querySelectorAll('.resizable-panel');
        if (panels.length >= 2) {
            const totalWidth = horizontalContainer.getBoundingClientRect().width;
            panels[0].style.flex = `0 0 ${totalWidth * 0.5}px`; // First panel (chat) - 50%
            panels[1].style.flex = `0 0 ${totalWidth * 0.5}px`; // Second panel (map) - 50%
        }
    }

    // Set initial vertical split (85% for top row, 15% for visualization)
    // The visualization panel will start hidden but this ensures proper proportions when shown
    const verticalContainer = document.querySelector('.resizable-vertical');
    if (verticalContainer) {
        const children = verticalContainer.children;
        if (children.length >= 3) { // First child is top row, second is resize handle, third is visualization
            const visualizationArea = document.getElementById('visualization-area');
            const isVisualizationHidden = visualizationArea &&
                window.getComputedStyle(visualizationArea).display === 'none';

            if (isVisualizationHidden) {
                // If visualization is hidden, give top row full height
                children[0].style.flex = `1 1 auto`; // Top row takes all available space
                children[2].style.flex = `0 0 0`; // Visualization gets no space
            } else {
                // Normal split if visualization is visible
                const totalHeight = verticalContainer.getBoundingClientRect().height;
                children[0].style.flex = `0 0 ${totalHeight * 0.85}px`; // Top row - 85%
                children[2].style.flex = `0 0 ${totalHeight * 0.15}px`; // Visualization - 15%
            }
        }
    }

    // Trigger resize event to update internal components
    window.dispatchEvent(new Event('resize'));
}

// Add a mutation observer to watch for changes in visualization visibility
function setupVisibilityObserver() {
    const visualizationArea = document.getElementById('visualization-area');
    if (!visualizationArea) return;

    // Create a new observer
    const observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.attributeName === 'style') {
                const newDisplayValue = window.getComputedStyle(visualizationArea).display;
                const wasHidden = visualizationArea.dataset.wasHidden === 'true';
                const isNowHidden = newDisplayValue === 'none';

                // Only adjust layout when visibility actually changes
                if (wasHidden !== isNowHidden) {
                    visualizationArea.dataset.wasHidden = isNowHidden;

                    // Reset userHasResized flag when visualization appears/disappears
                    if (!isNowHidden) {
                        userHasResized = false;
                        setInitialPanelSizes();
                    }
                }
            }
        });
    });

    // Start observing
    observer.observe(visualizationArea, { attributes: true });

    // Set initial state
    visualizationArea.dataset.wasHidden = (window.getComputedStyle(visualizationArea).display === 'none');
}

// Start observing the document body for DOM changes
observer.observe(document.body, { childList: true, subtree: true });

// Update window resize handler
window.addEventListener('resize', function () {
    // Use a debounce to avoid calling this too frequently
    if (this.resizeTimeout) clearTimeout(this.resizeTimeout);
    this.resizeTimeout = setTimeout(function () {
        // Don't reset the layout on window resize if user has manually adjusted it
        if (!userHasResized) {
            setInitialPanelSizes();
        }
    }, 200);
});

// When visibility changes for the visualization area, we may need to readjust
function updateOnVisibilityChange() {
    const visualizationArea = document.getElementById('visualization-area');
    if (visualizationArea) {
        const isHidden = window.getComputedStyle(visualizationArea).display === 'none';

        // Reset the flag to allow repositioning when visualization appears/disappears
        if (visualizationArea.dataset.wasHidden !== String(isHidden)) {
            userHasResized = false;
            setInitialPanelSizes();
            visualizationArea.dataset.wasHidden = isHidden;
        }
    }
}