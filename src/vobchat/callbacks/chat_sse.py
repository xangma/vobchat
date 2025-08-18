# Simple Chat Callback - Clean rewrite
# Single responsibility: Handle user input and trigger workflows

import logging
import time
from uuid import uuid4
from typing import Dict, Any

import dash
from dash import Input, Output, State, no_update, ALL
from dash.exceptions import PreventUpdate
import dash_leaflet as dl

from vobchat.intent_handling import AssistantIntent
from vobchat.sse_manager import get_sse_manager

logger = logging.getLogger(__name__)

simple_sse_manager = get_sse_manager()


def register_simple_chat_callbacks(app, compiled_workflow):
    """Register simplified chat callbacks that work with clean SSE architecture"""

    logger.info("Registering simplified chat callbacks")

    @app.callback(
        Output("chat-input", "value"),
        Output("thread-id", "data", allow_duplicate=True),
        Output("send-button", "disabled"),
        Output("chat-display", "children", allow_duplicate=True),
        Output("sse-connection-status", "data", allow_duplicate=True),
        Output("place-disambiguation-markers", "children", allow_duplicate=True),
        Output("sse-interrupt-store", "data", allow_duplicate=True),
        # Inputs
        Input("send-button", "n_clicks"),
        Input("chat-input", "n_submit"),
        Input("reset-button", "n_clicks"),
        Input("map-click-add-trigger", "data"),
        Input("map-click-remove-trigger", "data"),
        # States
        State("thread-id", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),
        State("map-state", "data"),
        prevent_initial_call=True,
    )
    def handle_user_input(
        n_clicks,
        n_submit,
        reset_clicks,
        map_add_payload,
        map_remove_payload,
        thread_id,
        user_input,
        chat_display,
        map_state,
    ):
        """Simple chat handler - determines what triggered and starts appropriate workflow"""

        ctx = dash.callback_context
        if not ctx.triggered:
            raise PreventUpdate

        trigger = ctx.triggered[0]["prop_id"]
        logger.info(f"Chat triggered by: {trigger}")

        # Initialize thread ID if needed
        if not thread_id:
            thread_id = str(uuid4())
            logger.info(f"Generated new thread ID: {thread_id}")

        # Handle reset
        if "reset-button" in trigger:
            logger.info(
                "Reset triggered - generating new thread ID and triggering reset workflow"
            )
            new_thread_id = str(uuid4())

            # Remove all SSE clients for old threads across all workers

            # Get current active threads for logging
            active_threads = simple_sse_manager.get_all_active_threads()
            if active_threads:
                logger.info(f"Active SSE threads before cleanup: {active_threads}")

            # Broadcast cleanup signal for all threads except the new one
            logger.info(
                f"Broadcasting cleanup signal for all threads except {new_thread_id}"
            )
            simple_sse_manager.broadcast_cleanup_all_except(new_thread_id)

            # Also cleanup all threads except the new one locally
            cleaned_count = simple_sse_manager.cleanup_all_threads_except(new_thread_id)
            if cleaned_count > 0:
                logger.info(
                    f"Cleaned up {cleaned_count} local SSE clients during reset"
                )

            # Create workflow input for reset
            reset_workflow_input = {
                "last_intent_payload": {"intent": "Reset", "arguments": {}}
            }

            # Create SSE connection status that tells client to connect with reset workflow input
            sse_status = {
                "connect_sse": True,
                "thread_id": new_thread_id,
                "workflow_input": reset_workflow_input,
                "reset": True,  # Flag to tell SSE client this is a reset
                "timestamp": time.time(),
            }

            # Clear chat display and return new thread with reset trigger
            # Also clear place disambiguation markers and interrupt store
            return "", new_thread_id, False, [], sse_status, [], {}

        # Prepare workflow input based on trigger type
        workflow_input = None

        # Handle text input
        if (
            user_input
            and user_input.strip()
            and ("send-button" in trigger or "chat-input" in trigger)
        ):
            logger.info(f"Text input: {user_input}")

            workflow_input = {"messages": [("user", user_input)]}

        # Handle map clicks
        elif "map-click-add-trigger" in trigger and map_add_payload:
            logger.info(f"Map add click: {map_add_payload}")
            places_from_client = []
            try:
                if isinstance(map_state, dict):
                    places_from_client = map_state.get("places", []) or []
            except Exception:
                places_from_client = []
            workflow_input = {
                "places": places_from_client,
                "last_intent_payload": {
                    "intent": AssistantIntent.ADD_PLACE.value,
                    "arguments": {
                        "place": map_add_payload.get("name", "Unknown Place"),
                        "unit_type": map_add_payload.get("type"),
                        "polygon_id": int(map_add_payload["id"])
                        if str(map_add_payload["id"]).isdigit()
                        else None,
                        "source": "map_click",
                    },
                }
            }

        elif "map-click-remove-trigger" in trigger and map_remove_payload:
            logger.info(f"Map remove click: {map_remove_payload}")
            places_from_client = []
            try:
                if isinstance(map_state, dict):
                    places_from_client = map_state.get("places", []) or []
            except Exception:
                places_from_client = []
            workflow_input = {
                "places": places_from_client,
                "last_intent_payload": {
                    "intent": AssistantIntent.REMOVE_PLACE.value,
                    "arguments": {
                        "place": map_remove_payload.get("name", "Unknown Place"),
                        "unit_type": map_remove_payload.get("type"),
                        "polygon_id": int(map_remove_payload["id"])
                        if str(map_remove_payload["id"]).isdigit()
                        else None,
                        "source": "map_click",
                    },
                }
            }

        # If we have workflow input, signal SSE client to connect with workflow input
        if workflow_input:
            logger.info(
                f"Signaling SSE client to connect with workflow input for thread {thread_id}"
            )

            # Create SSE connection status that tells client to connect with workflow input
            sse_status = {
                "connect_sse": True,
                "thread_id": thread_id,
                "workflow_input": workflow_input,
                "timestamp": time.time(),
            }

            # Return updates: clear input, keep thread_id, disable button, no chat update, trigger SSE
            return "", thread_id, True, no_update, sse_status, no_update, no_update

        raise PreventUpdate

    # Add callback for place disambiguation markers
    @app.callback(
        Output("place-disambiguation-markers", "children", allow_duplicate=True),
        Input("sse-interrupt-store", "data"),
        prevent_initial_call=True,
    )
    def update_place_disambiguation_markers(interrupt_data):
        """Update map markers when place disambiguation is needed"""
        if not interrupt_data:
            return []

        # Check if this is a place disambiguation interrupt
        place_coordinates = interrupt_data.get("place_coordinates", [])
        if not place_coordinates:
            return []

        # Create markers for each candidate place
        markers = []

        # Get current place being processed and places that have been selected
        current_place_index = interrupt_data.get("current_place_index", 0)
        places_state = interrupt_data.get("places", [])

        logger.info(
            f"Disambiguation markers - current_place_index: {current_place_index}"
        )
        logger.info(f"Places state: {places_state}")
        logger.info(f"Place coordinates: {place_coordinates}")

        for i, place in enumerate(place_coordinates):
            # IMPORTANT: place_coordinates contains disambiguation options for the CURRENT place being processed
            # The current_place_index tells us which place in places_state we're disambiguating
            # So we should check places_state[current_place_index], not place.get("index")

            # Check if the current place being disambiguated has been selected
            is_selected = False
            place_data = None
            if current_place_index < len(places_state):
                place_data = places_state[current_place_index]
                # Only consider selected if g_unit is not None and not empty
                g_unit = place_data.get("g_unit")
                is_selected = (
                    g_unit is not None
                    and str(g_unit).strip() != ""
                    and g_unit != "null"
                )

            logger.info(
                f"Place {place['name']} (disambiguation option {i} for places_state[{current_place_index}]): is_selected={is_selected}, g_unit={place_data.get('g_unit') if place_data else 'N/A'}"
            )

            # Check if this is a single place (should be highlighted)
            is_single = place.get("is_single", False)

            # Choose icon color: green for selected, yellow/orange for single (highlighted), blue for multiple options
            if is_selected:
                icon_url = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png"
            elif is_single:
                # Use orange/yellow for single places that need attention
                icon_url = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-orange.png"
            else:
                icon_url = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png"

            # Create marker with appropriate color and permanent tooltip
            marker = dl.Marker(
                position=[place["lat"], place["lon"]],
                children=[
                    dl.Tooltip(
                        f"{place['name']}, {place['county']}"
                        + (" ✓" if is_selected else ""),
                        permanent=True,
                        direction="right",
                        offset=[10, 0],
                        className="place-label-tooltip",
                    )
                ],
                id={"type": "place-candidate-marker", "index": place["index"]},
                # Use different icon colors based on selection status
                icon={
                    "iconUrl": icon_url,
                    "shadowUrl": "https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png",
                    "iconSize": [25, 41],
                    "iconAnchor": [12, 41],
                    "popupAnchor": [1, -34],
                    "shadowSize": [41, 41],
                },
            )
            markers.append(marker)

        logger.info(f"Created {len(markers)} place disambiguation markers")
        return markers

    # Add callback to handle place marker clicks
    @app.callback(
        Output("thread-id", "data", allow_duplicate=True),
        Output("sse-connection-status", "data", allow_duplicate=True),
        Output("place-disambiguation-markers", "children", allow_duplicate=True),
        Output("sse-interrupt-store", "data", allow_duplicate=True),
        Input({"type": "place-candidate-marker", "index": ALL}, "n_clicks"),
        State("thread-id", "data"),
        State("sse-interrupt-store", "data"),
        prevent_initial_call=True,
    )
    def handle_place_marker_click(n_clicks_list, thread_id, interrupt_data):
        """Handle clicks on place disambiguation markers"""
        if not any(n_clicks_list):
            raise PreventUpdate

        # Find which marker was clicked
        ctx = dash.callback_context
        if not ctx.triggered:
            raise PreventUpdate

        # Extract the index from the triggered prop id
        triggered = ctx.triggered[0]
        prop_id = triggered["prop_id"]
        import json

        marker_id = json.loads(prop_id.split(".")[0])
        selected_index = marker_id["index"]

        logger.info(f"Place marker clicked: index {selected_index}")

        # Check if this is a single place marker (auto-selected)
        # If so, we should keep it visible for potential unit type selection
        is_single_place = False
        if interrupt_data and interrupt_data.get("place_coordinates"):
            coords = interrupt_data.get("place_coordinates", [])
            if len(coords) == 1 and coords[0].get("is_single"):
                is_single_place = True

        if is_single_place:
            # Keep markers visible for unit type selection
            markers_cleared = no_update
            interrupt_cleared = no_update
        else:
            # Clear markers for multi-place disambiguation
            markers_cleared = []
            interrupt_cleared = {}

        # Prepare workflow input with the selection
        workflow_input = {
            "selection_idx": selected_index,
            "current_node": interrupt_data.get("current_node"),
            "current_place_index": interrupt_data.get("current_place_index"),
            "places": interrupt_data.get("places", []),
        }

        # Start workflow with selection
        logger.info(f"Resuming workflow with place selection: {selected_index}")
        start_workflow_background(compiled_workflow, thread_id, workflow_input)

        # Trigger SSE connection and clear disambiguation mode
        sse_status = {
            "connect_sse": True,
            "thread_id": thread_id,
            "workflow_input": workflow_input,
            "clear_disambiguation_mode": True,
            "timestamp": time.time(),
        }

        return thread_id, sse_status, markers_cleared, interrupt_cleared


def start_workflow_background(
    compiled_workflow, thread_id: str, workflow_input: Dict[str, Any]
):
    """Start workflow execution in background using async methods"""

    async def run_workflow_async():
        try:
            logger.info(f"Background workflow starting for thread {thread_id}")

            # Create config for this thread
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": None,
                }
            }

            # Use the simplified workflow adapter with async execution
            from vobchat.workflow_sse_adapter import create_simple_workflow_adapter

            adapter = create_simple_workflow_adapter(compiled_workflow)

            # Execute workflow via adapter - this handles SSE streaming
            await adapter.run(thread_id, workflow_input)
            logger.info(f"Workflow completed for thread {thread_id}")

        except Exception as e:
            logger.error(f"Workflow error for thread {thread_id}: {e}", exc_info=True)
            # Send error via SSE
            await simple_sse_manager.error(thread_id, str(e))

    # Submit async task to the async manager
    from vobchat.utils.async_manager import async_manager

    async_manager.submit_task(run_workflow_async())
    logger.info(f"Background workflow task submitted for {thread_id}")
