# app/api/map_state_routes.py

from flask import jsonify, request
import logging
import json
from typing import Dict, List, Any

# Import Redis for session state management
from vobchat.utils.redis_pool import redis_pool_manager
from vobchat.models import login_required
from vobchat.sse_manager import make_json_safe

logger = logging.getLogger(__name__)

def register_map_state_routes(server):
    """Register API routes for simple map state management."""
    
    @login_required
    @server.route('/api/map/toggle-polygon', methods=['POST'])
    def toggle_polygon():
        """
        Simple API to add or remove a polygon from the map selection.
        This bypasses the complex workflow for direct map interactions.
        
        Request Body:
        {
            "thread_id": "uuid",
            "polygon_id": "12345",
            "unit_type": "MOD_REG", 
            "name": "SOUTH EAST",
            "action": "add" or "remove"  # optional, will auto-detect if not provided
        }
        
        Returns:
            JSON: Updated selection state
        """
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No JSON data provided"}), 400
                
            thread_id = data.get('thread_id')
            polygon_id = str(data.get('polygon_id'))
            unit_type = data.get('unit_type')
            name = data.get('name', '')
            action = data.get('action')  # 'add' or 'remove', optional
            
            if not all([thread_id, polygon_id, unit_type]):
                return jsonify({"error": "Missing required fields: thread_id, polygon_id, unit_type"}), 400
            
            # Get current state from Redis
            r = redis_pool_manager.get_sync_client()
            # Get the current workflow state
            state_key = f"workflow_state:{thread_id}"
            state_data = r.get(state_key)
            
            if state_data:
                state = json.loads(state_data)
            else:
                # Initialize empty state if none exists
                state = {
                    "places": [],  # Single source of truth
                    "selected_polygons": [],
                    # extracted_place_names removed - using places array as single source of truth
                }
            
            # Get current selections using helper functions
            from vobchat.state_schema import get_selected_units, get_selected_unit_types
            current_units = get_selected_units(state)
            current_unit_types = get_selected_unit_types(state)
            current_places = [place.get("g_place") for place in state.get("places", []) if place.get("g_place")]
            current_polygons = state.get("selected_polygons", [])
            places = state.get("places", []) or []
            current_names = [p.get("name", f"Place {i}") for i, p in enumerate(places)]
            current_place_objects = state.get("places", [])
            
            # Convert polygon_id to int for consistency
            polygon_id_int = int(polygon_id)
            
            # Auto-detect action if not provided
            if action is None:
                action = "remove" if polygon_id_int in current_units else "add"
            
            # Perform the action
            if action == "add":
                # Add polygon if not already present
                if polygon_id_int not in current_units:
                    current_units.append(polygon_id_int)
                    current_places.append(polygon_id_int)
                    current_polygons.append(polygon_id_int)
                    current_names.append(name)
                    
                    # Add unit type if not already present
                    if unit_type not in current_unit_types:
                        current_unit_types.append(unit_type)
                    
                    # Add to places array for consistency with workflow
                    place_obj = {
                        "name": name,
                        "candidate_rows": [],
                        "g_place": None,
                        "unit_rows": [],
                        "g_unit": polygon_id_int,
                        "g_unit_type": unit_type
                    }
                    current_place_objects.append(place_obj)
                    
                    logger.info(f"Added polygon {polygon_id} ({name}) to selection for thread {thread_id}")
                else:
                    logger.info(f"Polygon {polygon_id} already in selection for thread {thread_id}")
                    
            elif action == "remove":
                # Remove polygon if present
                if polygon_id_int in current_units:
                    # Remove from all arrays
                    while polygon_id_int in current_units:
                        idx = current_units.index(polygon_id_int)
                        current_units.pop(idx)
                        if idx < len(current_places):
                            current_places.pop(idx)
                        if idx < len(current_names):
                            current_names.pop(idx)
                    
                    # Remove from polygons array
                    while polygon_id_int in current_polygons:
                        current_polygons.remove(polygon_id_int)
                    
                    # Remove from places objects
                    current_place_objects = [p for p in current_place_objects if p.get("g_unit") != polygon_id_int]
                    
                    # If no more units of this type, remove the unit type
                    remaining_types = set()
                    for place_obj in current_place_objects:
                        if place_obj.get("g_unit_type"):
                            remaining_types.add(place_obj["g_unit_type"])
                    current_unit_types = list(remaining_types)
                    
                    logger.info(f"Removed polygon {polygon_id} ({name}) from selection for thread {thread_id}")
                else:
                    logger.info(f"Polygon {polygon_id} not in selection for thread {thread_id}")
            
            # Update state - only store the single source of truth
            state.update({
                "selected_polygons": current_polygons,
                # extracted_place_names removed - using places array as single source of truth
                "places": current_place_objects,  # Single source of truth
                "current_place_index": len(current_place_objects)
            })
            
            # Save updated state back to Redis
            r.set(state_key, json.dumps(make_json_safe(state)), ex=3600)  # 1 hour expiry
            
            # Return the updated selection state
            response_data = {
                "success": True,
                "action_performed": action,
                "polygon_id": polygon_id,
                "name": name,
                "selected_polygons": current_polygons,
                "selected_unit_types": current_unit_types,
                "total_selected": len(current_units)
            }
            
            logger.info(f"Map state updated for thread {thread_id}: {action} {name} -> {len(current_units)} total polygons")
            return jsonify(response_data)
                
        except Exception as e:
            logger.error(f"Error in toggle_polygon: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @login_required  
    @server.route('/api/map/state/<string:thread_id>', methods=['GET'])
    def get_map_state(thread_id: str):
        """
        Get the current map selection state for a thread.
        
        Returns:
            JSON: Current selection state
        """
        try:
            r = redis_pool_manager.get_sync_client()
            state_key = f"workflow_state:{thread_id}"
            state_data = r.get(state_key)
            
            if state_data:
                state = json.loads(state_data)
                response_data = {
                    "selected_polygons": state.get("selected_polygons", []),
                    "selected_unit_types": get_selected_unit_types(state),
                    "selected_place_names": [p.get("name", f"Place {i}") for i, p in enumerate(state.get("places", []) or [])],
                    "total_selected": len(get_selected_units(state))
                }
            else:
                response_data = {
                    "selected_polygons": [],
                    "selected_unit_types": [],
                    "selected_place_names": [],
                    "total_selected": 0
                }
            
            return jsonify(response_data)
                
        except Exception as e:
            logger.error(f"Error getting map state for thread {thread_id}: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @login_required
    @server.route('/api/map/clear/<string:thread_id>', methods=['POST'])
    def clear_map_selection(thread_id: str):
        """
        Clear all polygon selections for a thread.
        
        Returns:
            JSON: Confirmation of clearing
        """
        try:
            r = redis_pool_manager.get_sync_client()
            state_key = f"workflow_state:{thread_id}"
            state_data = r.get(state_key)
            
            if state_data:
                state = json.loads(state_data)
                
                # Clear all selection-related fields
                state.update({
                    "selected_polygons": [],
                    # extracted_place_names removed - using places array as single source of truth,
                    "places": [],  # Single source of truth
                    "current_place_index": 0,
                    "selected_theme": None,
                    "extracted_theme": None,
                    "selected_cubes": None
                })
                
                # Save updated state
                r.set(state_key, json.dumps(make_json_safe(state)), ex=3600)
                
                logger.info(f"Cleared all map selections for thread {thread_id}")
                
            return jsonify({
                "success": True,
                "message": "Map selection cleared",
                "selected_polygons": [],
                "total_selected": 0
            })
                
        except Exception as e:
            logger.error(f"Error clearing map state for thread {thread_id}: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @login_required
    @server.route('/api/map/user-action', methods=['POST'])
    def handle_user_action():
        """
        Handle user actions from the pure map state manager.
        This endpoint receives notifications about user interactions
        and can trigger workflow responses if needed.
        
        Request Body:
        {
            "action": "polygon_selected" | "polygon_deselected" | "unit_types_changed" | "year_range_changed" | "state_reset",
            "data": { action-specific data },
            "timestamp": number
        }
        
        Returns:
            JSON: Acknowledgment
        """
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No JSON data provided"}), 400
                
            action = data.get('action')
            action_data = data.get('data', {})
            timestamp = data.get('timestamp')
            
            if not action:
                return jsonify({"error": "Missing required field: action"}), 400
            
            logger.info(f"User action received: {action} with data: {action_data}")
            
            # For now, just acknowledge the action
            # In the future, this could trigger workflow responses or analytics
            
            response_data = {
                "success": True,
                "action": action,
                "timestamp": timestamp,
                "acknowledged": True
            }
            
            return jsonify(response_data)
                
        except Exception as e:
            logger.error(f"Error handling user action: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500