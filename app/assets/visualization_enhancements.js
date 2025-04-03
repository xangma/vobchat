// app/assets/visualization_enhancements.js

// Run initialization once on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log("DOM loaded, initializing visualization enhancements");
    initializeVisualizationEnhancements();
    
    // Add click handler for the float toggle button
    const floatToggleButton = document.getElementById('float-toggle-button');
    if (floatToggleButton) {
        floatToggleButton.addEventListener('click', toggleFloatingMode);
    }
    
    // Set up a mutation observer to watch for visualization area being added to the DOM
    const bodyObserver = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.addedNodes && mutation.addedNodes.length) {
                for (let i = 0; i < mutation.addedNodes.length; i++) {
                    const node = mutation.addedNodes[i];
                    if (node.id === 'visualization-area' || 
                        (node.querySelector && node.querySelector('#visualization-area'))) {
                        console.log("Visualization area detected in DOM changes");
                        setTimeout(initializeVisualizationEnhancements, 100);
                        return;
                    }
                }
            }
        });
    });
    
    // Start observing the document with the configured parameters
    bodyObserver.observe(document.body, { childList: true, subtree: true });
});

// Also check periodically for the visualization area to appear
setInterval(function() {
    const visualizationArea = document.getElementById('visualization-area');
    if (visualizationArea) {
        const computedStyle = window.getComputedStyle(visualizationArea);
        if (computedStyle.display !== 'none' && !visualizationArea.querySelector('.viz-control-panel')) {
            console.log("Visualization area is visible but missing controls - reinitializing");
            initializeVisualizationEnhancements();
        }
    }
}, 1000);

function initializeVisualizationEnhancements() {
    // Wait for the visualization area to be available
    const visualizationArea = document.getElementById('visualization-area');
    if (!visualizationArea) {
        setTimeout(initializeVisualizationEnhancements, 100);
        return;
    }

    // Create the visualization control panel
    createVisualizationControls();

    // Make the visualization area draggable and resizable
    makeVisualizationDraggable();
    makeVisualizationResizable();

    // Listen for visibility changes
    observeVisualizationVisibility();
    
    // Force a check of current visibility status
    updateVisualizationControls(visualizationArea.style.display !== 'none');
}

function createVisualizationControls() {
    const visualizationArea = document.getElementById('visualization-area');
    if (!visualizationArea) return;
    
    // Check if control panel already exists
    if (visualizationArea.querySelector('.viz-control-panel')) {
        console.log("Control panel already exists, skipping creation");
        return;
    }
    
    console.log("Creating visualization control panel");
    
    // Create a control panel for the visualization area
    const controlPanel = document.createElement('div');
    controlPanel.className = 'viz-control-panel';
    controlPanel.innerHTML = `
        <div class="viz-control-handle" title="Drag to move">
            <i class="fa fa-arrows"></i>
        </div>
        <div class="viz-control-buttons">
            <button class="viz-control-button viz-popout" title="Pop out to separate window">
                <i class="fa fa-external-link"></i>
            </button>
            <button class="viz-control-button viz-minimize" title="Minimize visualization">
                <i class="fa fa-minus"></i>
            </button>
            <button class="viz-control-button viz-maximize" title="Maximize visualization">
                <i class="fa fa-expand"></i>
            </button>
            <button class="viz-control-button viz-close" title="Close visualization">
                <i class="fa fa-times"></i>
            </button>
        </div>
    `;
    
    // Save original position and size before any transforms (if not already saved)
    if (!visualizationArea.dataset.originalPosition) {
        visualizationArea.dataset.originalPosition = JSON.stringify({
            position: 'relative',
            top: '0',
            left: '0',
            width: '100%',
            height: '100%'
        });
    }
    
    // Add the control panel to the visualization area
    visualizationArea.insertBefore(controlPanel, visualizationArea.firstChild);
    
    // Add event listeners for the control buttons
    const popoutButton = controlPanel.querySelector('.viz-popout');
    const minimizeButton = controlPanel.querySelector('.viz-minimize');
    const maximizeButton = controlPanel.querySelector('.viz-maximize');
    const closeButton = controlPanel.querySelector('.viz-close');
    
    if (popoutButton) popoutButton.addEventListener('click', openVisualizationInNewWindow);
    if (minimizeButton) minimizeButton.addEventListener('click', minimizeVisualization);
    if (maximizeButton) maximizeButton.addEventListener('click', maximizeVisualization);
    if (closeButton) closeButton.addEventListener('click', closeVisualization);
    
    // Add a listener for the float toggle button if it exists
    const floatToggleButton = document.getElementById('float-toggle-button');
    if (floatToggleButton) {
        // Remove existing event listeners to prevent duplicates
        floatToggleButton.removeEventListener('click', toggleFloatingMode);
        // Add the event listener
        floatToggleButton.addEventListener('click', toggleFloatingMode);
        console.log("Float toggle button handler attached");
    }
}

function makeVisualizationDraggable() {
    const visualizationArea = document.getElementById('visualization-area');
    const handle = visualizationArea.querySelector('.viz-control-handle');
    
    let isDragging = false;
    let offsetX, offsetY;
    
    handle.addEventListener('mousedown', function(e) {
        // Only make draggable when in floating mode
        if (visualizationArea.classList.contains('viz-floating')) {
            isDragging = true;
            
            // Calculate the offset from the mouse position to the visualization area's top-left corner
            const rect = visualizationArea.getBoundingClientRect();
            offsetX = e.clientX - rect.left;
            offsetY = e.clientY - rect.top;
            
            // Set initial position if not already positioned
            if (!visualizationArea.style.position || visualizationArea.style.position === 'relative') {
                visualizationArea.style.position = 'fixed';
                visualizationArea.style.top = rect.top + 'px';
                visualizationArea.style.left = rect.left + 'px';
                visualizationArea.style.width = rect.width + 'px';
                visualizationArea.style.height = rect.height + 'px';
            }
            
            // Add a temporary class for styling during drag
            visualizationArea.classList.add('viz-dragging');
            
            // Prevent text selection during drag
            e.preventDefault();
        }
    });
    
    document.addEventListener('mousemove', function(e) {
        if (!isDragging) return;
        
        // Calculate new position
        const newLeft = e.clientX - offsetX;
        const newTop = e.clientY - offsetY;
        
        // Update position
        visualizationArea.style.left = newLeft + 'px';
        visualizationArea.style.top = newTop + 'px';
    });
    
    document.addEventListener('mouseup', function() {
        if (isDragging) {
            isDragging = false;
            visualizationArea.classList.remove('viz-dragging');
        }
    });
}

function makeVisualizationResizable() {
    const visualizationArea = document.getElementById('visualization-area');
    if (!visualizationArea) return;
    
    // Check if resize handles already exist
    if (visualizationArea.querySelector('.viz-resize-handles')) {
        console.log("Resize handles already exist, skipping creation");
        return;
    }
    
    console.log("Creating visualization resize handles");
    
    // Add resize handles
    const resizeHandles = document.createElement('div');
    resizeHandles.className = 'viz-resize-handles';
    
    // Create resize handles for different directions
    const directions = ['n', 'e', 's', 'w', 'ne', 'se', 'sw', 'nw'];
    directions.forEach(direction => {
        const handle = document.createElement('div');
        handle.className = `viz-resize-handle viz-resize-${direction}`;
        resizeHandles.appendChild(handle);
    });
    
    visualizationArea.appendChild(resizeHandles);
    
    // Set up resize functionality
    let isResizing = false;
    let currentHandle = null;
    let startX, startY, startWidth, startHeight, startTop, startLeft;
    
    // Add event listeners to all resize handles
    const handles = visualizationArea.querySelectorAll('.viz-resize-handle');
    handles.forEach(handle => {
        handle.addEventListener('mousedown', function(e) {
            // Only resize when in floating mode
            if (visualizationArea.classList.contains('viz-floating')) {
                isResizing = true;
                currentHandle = this;
                
                const rect = visualizationArea.getBoundingClientRect();
                startX = e.clientX;
                startY = e.clientY;
                startWidth = rect.width;
                startHeight = rect.height;
                startTop = rect.top;
                startLeft = rect.left;
                
                // Add a class for styling during resize
                visualizationArea.classList.add('viz-resizing');
                
                // Prevent text selection during resize
                e.preventDefault();
            }
        });
    });
    
    document.addEventListener('mousemove', function(e) {
        if (!isResizing) return;
        
        // Calculate changes in position and size
        const deltaX = e.clientX - startX;
        const deltaY = e.clientY - startY;
        
        // Determine which direction to resize based on the handle class
        if (currentHandle.classList.contains('viz-resize-e') || 
            currentHandle.classList.contains('viz-resize-ne') || 
            currentHandle.classList.contains('viz-resize-se')) {
            // East (right) resize
            visualizationArea.style.width = (startWidth + deltaX) + 'px';
        }
        
        if (currentHandle.classList.contains('viz-resize-w') || 
            currentHandle.classList.contains('viz-resize-nw') || 
            currentHandle.classList.contains('viz-resize-sw')) {
            // West (left) resize
            visualizationArea.style.width = (startWidth - deltaX) + 'px';
            visualizationArea.style.left = (startLeft + deltaX) + 'px';
        }
        
        if (currentHandle.classList.contains('viz-resize-n') || 
            currentHandle.classList.contains('viz-resize-ne') || 
            currentHandle.classList.contains('viz-resize-nw')) {
            // North (top) resize
            visualizationArea.style.height = (startHeight - deltaY) + 'px';
            visualizationArea.style.top = (startTop + deltaY) + 'px';
        }
        
        if (currentHandle.classList.contains('viz-resize-s') || 
            currentHandle.classList.contains('viz-resize-se') || 
            currentHandle.classList.contains('viz-resize-sw')) {
            // South (bottom) resize
            visualizationArea.style.height = (startHeight + deltaY) + 'px';
        }
    });
    
    document.addEventListener('mouseup', function() {
        if (isResizing) {
            isResizing = false;
            currentHandle = null;
            visualizationArea.classList.remove('viz-resizing');
            
            // Trigger a window resize event to redraw the plot
            window.dispatchEvent(new Event('resize'));
        }
    });
}

function observeVisualizationVisibility() {
    const visualizationArea = document.getElementById('visualization-area');
    
    // Create a MutationObserver to watch for style changes
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.attributeName === 'style') {
                const computedStyle = window.getComputedStyle(visualizationArea);
                const isVisible = computedStyle.display !== 'none';
                console.log("Visualization visibility changed:", isVisible);
                updateVisualizationControls(isVisible);
                
                // If becoming visible and doesn't have control panel, recreate it
                if (isVisible && !visualizationArea.querySelector('.viz-control-panel')) {
                    console.log("Recreating visualization controls");
                    createVisualizationControls();
                    makeVisualizationDraggable();
                    makeVisualizationResizable();
                }
            }
        });
    });
    
    // Start observing with more detailed configuration
    observer.observe(visualizationArea, { 
        attributes: true,
        attributeFilter: ['style', 'class', 'data-was-hidden'],
        attributeOldValue: true
    });
    
    // Initial update
    const computedStyle = window.getComputedStyle(visualizationArea);
    const isVisible = computedStyle.display !== 'none';
    updateVisualizationControls(isVisible);
}

function updateVisualizationControls(isVisible) {
    const visualizationArea = document.getElementById('visualization-area');
    if (!visualizationArea) return;
    
    const resizeHandles = visualizationArea.querySelector('.viz-resize-handles');
    const controlPanel = visualizationArea.querySelector('.viz-control-panel');
    
    console.log("Updating visualization controls:", isVisible);
    console.log("Control panel exists:", !!controlPanel);
    console.log("Resize handles exist:", !!resizeHandles);
    
    if (isVisible) {
        // If becoming visible but missing controls, recreate them
        if (!controlPanel) {
            console.log("Creating missing control panel");
            createVisualizationControls();
        } else {
            controlPanel.style.display = '';
        }
        
        if (!resizeHandles) {
            console.log("Creating missing resize handles");
            makeVisualizationResizable();
        } else if (resizeHandles) {
            resizeHandles.style.display = visualizationArea.classList.contains('viz-floating') ? '' : 'none';
        }
    } else {
        // Hide controls when not visible
        if (resizeHandles) resizeHandles.style.display = 'none';
        if (controlPanel) controlPanel.style.display = 'none';
    }
}

function openVisualizationInNewWindow() {
    // Get the current visualization content
    const visualizationArea = document.getElementById('visualization-area');
    const plotElement = document.getElementById('data-plot');
    
    if (!plotElement) {
        console.warn('No plot element found to pop out');
        return;
    }
    
    // Create a new window
    const newWindow = window.open('', 'DDME Visualization', 'width=800,height=600');
    
    // Create content for the new window
    const html = `
        <!DOCTYPE html>
        <html>
        <head>
            <title>DDME Visualization</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css">
            <style>
                body {
                    margin: 0;
                    padding: 20px;
                    font-family: Arial, sans-serif;
                }
                .visualization-container {
                    width: 100%;
                    height: calc(100vh - 40px);
                    display: flex;
                    flex-direction: column;
                }
                .controls {
                    display: flex;
                    margin-bottom: 10px;
                    justify-content: space-between;
                }
                #visualization-content {
                    flex: 1;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    overflow: hidden;
                }
            </style>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div class="visualization-container">
                <div class="controls">
                    <h3>DDME Visualization</h3>
                    <button id="closeButton" class="btn btn-danger">Close</button>
                </div>
                <div id="visualization-content"></div>
            </div>
            <script>
                // Add close button functionality
                document.getElementById('closeButton').addEventListener('click', function() {
                    window.close();
                });
                
                // Connect to parent window for data updates
                window.addEventListener('message', function(event) {
                    if (event.data.type === 'plotlyData') {
                        Plotly.newPlot(
                            'visualization-content', 
                            event.data.data, 
                            event.data.layout, 
                            event.data.config
                        );
                    }
                });
                
                // Let parent know we're ready
                window.opener.postMessage({ type: 'vizWindowReady' }, '*');
            </script>
        </body>
        </html>
    `;
    
    // Write the HTML to the new window
    newWindow.document.write(html);
    newWindow.document.close();
    
    // Setup message handler to send plot data to the new window
    window.addEventListener('message', function(event) {
        if (event.data.type === 'vizWindowReady') {
            // Extract the plot data from the current plot
            try {
                const plotlyDiv = document.getElementById('data-plot')._fullLayout._container;
                const data = JSON.parse(JSON.stringify(plotlyDiv.data));
                const layout = JSON.parse(JSON.stringify(plotlyDiv.layout));
                
                // Add responsive layout settings
                layout.autosize = true;
                
                // Send the data to the new window
                newWindow.postMessage({
                    type: 'plotlyData',
                    data: data,
                    layout: layout,
                    config: { responsive: true }
                }, '*');
            } catch (error) {
                console.error('Error extracting plot data:', error);
                newWindow.postMessage({
                    type: 'error',
                    message: 'Could not extract plot data'
                }, '*');
            }
        }
    });
    
    // Focus the new window
    newWindow.focus();
}

function minimizeVisualization() {
    const visualizationArea = document.getElementById('visualization-area');
    const plotArea = document.getElementById('data-plot').parentNode;
    
    if (!visualizationArea.classList.contains('viz-minimized')) {
        // Save current size before minimizing
        visualizationArea.dataset.savedStyle = JSON.stringify({
            width: visualizationArea.style.width,
            height: visualizationArea.style.height
        });
        
        // Minimize by reducing height and keeping header visible
        plotArea.style.display = 'none';
        visualizationArea.style.height = '60px';
        visualizationArea.classList.add('viz-minimized');
    } else {
        // Restore previous size
        if (visualizationArea.dataset.savedStyle) {
            const savedStyle = JSON.parse(visualizationArea.dataset.savedStyle);
            visualizationArea.style.width = savedStyle.width;
            visualizationArea.style.height = savedStyle.height;
        }
        
        plotArea.style.display = '';
        visualizationArea.classList.remove('viz-minimized');
    }
    
    // Trigger resize event to redraw the plot
    window.dispatchEvent(new Event('resize'));
}

function maximizeVisualization() {
    const visualizationArea = document.getElementById('visualization-area');
    
    if (!visualizationArea.classList.contains('viz-maximized')) {
        // Save current state before maximizing
        visualizationArea.dataset.savedStyle = JSON.stringify({
            position: visualizationArea.style.position,
            top: visualizationArea.style.top,
            left: visualizationArea.style.left,
            width: visualizationArea.style.width,
            height: visualizationArea.style.height
        });
        
        // Maximize to fill the entire screen
        visualizationArea.style.position = 'fixed';
        visualizationArea.style.top = '0';
        visualizationArea.style.left = '0';
        visualizationArea.style.width = '100%';
        visualizationArea.style.height = '100%';
        visualizationArea.style.zIndex = '10000';
        visualizationArea.classList.add('viz-maximized');
        visualizationArea.classList.add('viz-floating');
    } else {
        // Restore previous state
        if (visualizationArea.dataset.savedStyle) {
            const savedStyle = JSON.parse(visualizationArea.dataset.savedStyle);
            visualizationArea.style.position = savedStyle.position;
            visualizationArea.style.top = savedStyle.top;
            visualizationArea.style.left = savedStyle.left;
            visualizationArea.style.width = savedStyle.width;
            visualizationArea.style.height = savedStyle.height;
        } else {
            // Restore to original position if no saved style
            const originalPosition = JSON.parse(visualizationArea.dataset.originalPosition);
            visualizationArea.style.position = originalPosition.position;
            visualizationArea.style.top = originalPosition.top;
            visualizationArea.style.left = originalPosition.left;
            visualizationArea.style.width = originalPosition.width;
            visualizationArea.style.height = originalPosition.height;
            visualizationArea.classList.remove('viz-floating');
        }
        
        visualizationArea.style.zIndex = '';
        visualizationArea.classList.remove('viz-maximized');
    }
    
    // Trigger resize event to redraw the plot
    window.dispatchEvent(new Event('resize'));
}

function closeVisualization() {
    const visualizationArea = document.getElementById('visualization-area');
    
    // Hide the visualization area
    visualizationArea.style.display = 'none';
    
    // Reset to original position
    if (visualizationArea.dataset.originalPosition) {
        const originalPosition = JSON.parse(visualizationArea.dataset.originalPosition);
        visualizationArea.style.position = originalPosition.position;
        visualizationArea.style.top = originalPosition.top;
        visualizationArea.style.left = originalPosition.left;
        visualizationArea.style.width = originalPosition.width;
        visualizationArea.style.height = originalPosition.height;
    }
    
    // Remove all transformation classes
    visualizationArea.classList.remove('viz-floating');
    visualizationArea.classList.remove('viz-maximized');
    visualizationArea.classList.remove('viz-minimized');
    
    // Clear cube selection
    const cubeSelector = document.getElementById('cube-selector');
    if (cubeSelector) cubeSelector.value = null;
    
    // Trigger the clear visualization event 
    const clearButton = document.getElementById('clear-plot-button');
    if (clearButton) clearButton.click();
}

// Helper function to convert visualization to floating mode
function toggleFloatingMode() {
    const visualizationArea = document.getElementById('visualization-area');
    const floatToggleButton = document.getElementById('float-toggle-button');
    
    if (!visualizationArea.classList.contains('viz-floating')) {
        // Save current state before floating
        visualizationArea.dataset.savedStyle = JSON.stringify({
            position: visualizationArea.style.position,
            top: visualizationArea.style.top,
            left: visualizationArea.style.left,
            width: visualizationArea.style.width,
            height: visualizationArea.style.height
        });
        
        // Get current position and size
        const rect = visualizationArea.getBoundingClientRect();
        
        // Convert to floating mode
        visualizationArea.style.position = 'fixed';
        visualizationArea.style.top = rect.top + 'px';
        visualizationArea.style.left = rect.left + 'px';
        visualizationArea.style.width = rect.width + 'px';
        visualizationArea.style.height = rect.height + 'px';
        visualizationArea.style.zIndex = '1000';
        visualizationArea.classList.add('viz-floating');
        
        // Update button text
        if (floatToggleButton) {
            floatToggleButton.textContent = "Dock Visualization";
        }
        
        // Show resize handles
        const resizeHandles = visualizationArea.querySelector('.viz-resize-handles');
        if (resizeHandles) resizeHandles.style.display = '';
    } else {
        // Restore previous state
        if (visualizationArea.dataset.savedStyle) {
            const savedStyle = JSON.parse(visualizationArea.dataset.savedStyle);
            visualizationArea.style.position = savedStyle.position;
            visualizationArea.style.top = savedStyle.top;
            visualizationArea.style.left = savedStyle.left;
            visualizationArea.style.width = savedStyle.width;
            visualizationArea.style.height = savedStyle.height;
        } else {
            // Restore to original position if no saved style
            const originalPosition = JSON.parse(visualizationArea.dataset.originalPosition);
            visualizationArea.style.position = originalPosition.position;
            visualizationArea.style.top = originalPosition.top;
            visualizationArea.style.left = originalPosition.left;
            visualizationArea.style.width = originalPosition.width;
            visualizationArea.style.height = originalPosition.height;
        }
        
        visualizationArea.style.zIndex = '';
        visualizationArea.classList.remove('viz-floating');
        
        // Update button text
        if (floatToggleButton) {
            floatToggleButton.textContent = "Make Floating";
        }
    }
    
    // Trigger a window resize event to ensure plots are properly sized
    window.dispatchEvent(new Event('resize'));
}