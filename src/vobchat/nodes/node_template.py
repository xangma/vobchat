"""
Template for creating new LangGraph nodes with proper interrupt and message handling.

IMPORTANT CONCEPTS:
==================

1. State Changes Before Interrupts Are Lost
   - Any state modifications (including _append_ai calls) made before an interrupt() are NOT saved
   - This is core LangGraph behavior - interrupts return from the node immediately
   - Solution: Pass messages through the interrupt payload instead

2. Message Handling Patterns
   - For messages that need to persist: Include in interrupt payload with "message" and "messages" fields
   - For immediate state updates: Use _append_ai and return state without interrupt
   - For error messages: Use _append_ai and return state (no interrupt needed)

3. Frontend Integration
   - Frontend sse_client.js handles "message" field in interrupt data
   - Messages are combined with existing conversation and displayed
   - Messages are added to state when workflow resumes via start_node

4. State Schema
   - Always check state_schema.py for required fields before adding new ones
   - Use serialize_messages() utility for LangChain message serialization
   - Include interrupt_message field for message persistence across interrupts

TEMPLATE PATTERNS:
==================
"""

from __future__ import annotations

import logging
from typing import Dict, List, Union
from langgraph.types import Command, interrupt

from vobchat.state_schema import lg_State
from .utils import _append_ai, serialize_messages

logger = logging.getLogger(__name__)


def simple_node_template(state: lg_State) -> Dict[str, Union[str, list, dict]]:
    """
    Template for a simple node that processes data and returns state.
    Use this pattern when NO interrupt is needed.
    """
    
    # 1. Validate prerequisites
    if not state.get("some_required_field"):
        msg = _append_ai(state, "Error: Missing required data.")
        return {"messages": [msg]}
    
    # 2. Process data
    try:
        # Your processing logic here
        result = "Processing completed successfully"
        
        # 3. Add success message and return state
        msg = _append_ai(state, result)
        return {
            "messages": [msg],
            "some_result_field": result
        }
        
    except Exception as e:
        logger.error(f"Error in simple_node_template: {e}", exc_info=True)
        msg = _append_ai(state, f"Error during processing: {str(e)}")
        return {"messages": [msg]}


def interrupt_node_template(state: lg_State) -> None:
    """
    Template for a node that needs to interrupt for user interaction.
    Use this pattern when you need to pause workflow for user input.
    
    CRITICAL: Messages sent via _append_ai before interrupt() are LOST!
    Instead, pass messages through the interrupt payload.
    """
    
    # 1. Validate prerequisites  
    if not state.get("some_required_field"):
        # For error messages, DON'T interrupt - just return state
        msg = _append_ai(state, "Error: Missing required data.")
        return {"messages": [msg]}
    
    # 2. Process data that needs user interaction
    options = [
        {"label": "Option 1", "value": "opt1"},
        {"label": "Option 2", "value": "opt2"}
    ]
    
    # 3. Create message but DON'T append to state (will be lost)
    user_prompt = "Please select an option from the choices below:"
    
    # 4. Interrupt with message and options
    # IMPORTANT: Include both "message" and "messages" for proper frontend handling
    interrupt({
        "message": user_prompt,  # New message to display
        "messages": serialize_messages(state.get("messages", [])),  # Existing conversation
        "options": options,  # UI options for user selection
        "current_node": "interrupt_node_template",  # For workflow resumption
        # Include any other state data needed by frontend or for resumption
        "some_context_data": state.get("some_field")
    })
    
    # Note: Code after interrupt() will NOT execute


def data_processing_with_interrupt_template(state: lg_State) -> None:
    """
    Template for a node that processes data AND needs to interrupt.
    This is the most complex pattern - data processing followed by interrupt.
    
    CRITICAL: Any _append_ai calls before interrupt are LOST!
    Pass the success message through interrupt payload instead.
    """
    
    # 1. Validate prerequisites
    if not state.get("required_data"):
        # Error case - no interrupt needed, just return state
        msg = _append_ai(state, "Error: Missing required data.")
        return {"messages": [msg]}
    
    # 2. Process data
    try:
        # Your data processing logic here
        processed_data = {"result": "some processed data"}
        
        # 3. Create success message but DON'T append (will be lost due to interrupt)
        success_message = f"Successfully processed {len(processed_data)} items."
        
        # 4. Interrupt with both the success message and processed data
        interrupt({
            "message": success_message,  # Success message to display
            "messages": serialize_messages(state.get("messages", [])),  # Existing conversation
            "processed_data": processed_data,  # Data for frontend/visualization
            "data_ready": True,  # Signal for frontend
            "current_node": "data_processing_with_interrupt_template"
        })
        
    except Exception as e:
        logger.error(f"Error in processing: {e}", exc_info=True)
        # Error case - no interrupt, return state with error message
        msg = _append_ai(state, f"Error during processing: {str(e)}")
        return {"messages": [msg]}


def command_routing_template(state: lg_State) -> Command:
    """
    Template for a node that uses Command for routing decisions.
    Use this pattern when you need conditional routing to different nodes.
    """
    
    # 1. Analyze state to determine routing
    condition = state.get("some_condition")
    
    if condition == "route_a":
        # Update state and route to node A
        msg = _append_ai(state, "Routing to process A.")
        return Command(
            goto="node_a",
            update={
                "messages": [msg],
                "routing_decision": "a"
            }
        )
    elif condition == "route_b":
        # Route to node B without message
        return Command(goto="node_b")
    else:
        # Default routing
        msg = _append_ai(state, "Using default routing.")
        return Command(
            goto="default_node",
            update={"messages": [msg]}
        )


# COMMON UTILITIES AND PATTERNS
# =============================

def validate_state_template(state: lg_State, required_fields: List[str]) -> tuple[bool, str]:
    """
    Utility template for state validation.
    Returns (is_valid, error_message)
    """
    for field in required_fields:
        if not state.get(field):
            return False, f"Missing required field: {field}"
    return True, ""


def handle_error_template(state: lg_State, error: Exception, context: str) -> Dict[str, list]:
    """
    Utility template for consistent error handling.
    Always use this pattern for errors that don't need interrupts.
    """
    logger.error(f"Error in {context}: {error}", exc_info=True)
    msg = _append_ai(state, f"Error: {str(error)}")
    return {"messages": [msg]}


# EXAMPLES OF REAL PATTERNS FROM CODEBASE
# =======================================

def real_world_example_resolve_place(state: lg_State) -> None:
    """
    Real example showing place resolution with interrupt for disambiguation.
    Based on actual resolve_place_and_unit implementation.
    """
    places = state.get("places", [])
    
    # Process each place that needs resolution
    for idx, place in enumerate(places):
        if place.get("needs_resolution"):
            # Look up place in database (your lookup logic here)
            candidates = []  # Your database lookup results
            
            if len(candidates) > 1:
                # Multiple matches - need user disambiguation
                disambiguation_message = f"More than one **{place['name']}** - which do you mean?"
                
                interrupt({
                    "message": disambiguation_message,
                    "messages": serialize_messages(state.get("messages", [])),
                    "options": candidates,  # List of candidate places
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": idx,
                    "places": places
                })
                # Execution stops here due to interrupt
                
            elif len(candidates) == 1:
                # Single match - auto-resolve
                place.update(candidates[0])
                _append_ai(state, f"Found {place['name']}.")
                
            else:
                # No matches - error
                _append_ai(state, f"Could not find {place['name']}.")


def real_world_example_data_processing(state: lg_State) -> None:
    """
    Real example showing data processing with interrupt.
    Based on actual find_cubes_node implementation.
    """
    units = state.get("selected_units", [])
    theme = state.get("selected_theme")
    
    if not units or not theme:
        msg = _append_ai(state, "Missing required data for processing.")
        return {"messages": [msg]}
    
    # Process data (your processing logic here)
    processed_data = {"cubes": [], "count": 0}  # Your results
    
    # Create success message but don't append (will be lost)
    success_message = f"Loaded {processed_data['count']} data rows for visualization."
    
    # Interrupt with data and message
    interrupt({
        "message": success_message,
        "messages": serialize_messages(state.get("messages", [])),
        "cube_data_ready": True,
        "cubes": processed_data["cubes"],
        "show_visualization": True
    })


# CHECKLIST FOR NEW NODES
# =======================
"""
Before implementing a new node, ask:

1. Does this node need to interrupt for user input?
   - Yes: Use interrupt_node_template or data_processing_with_interrupt_template
   - No: Use simple_node_template or command_routing_template

2. Does this node send messages to the user?
   - If interrupting: Include message in interrupt payload, NOT _append_ai before interrupt
   - If not interrupting: Use _append_ai and return state

3. Does this node need conditional routing?
   - Yes: Use Command pattern with goto and optional update
   - No: Return state dict or use interrupt

4. What state fields does this node update?
   - Check state_schema.py for existing fields
   - Add new fields to state_schema.py if needed
   - Use proper merge functions for complex fields

5. How should errors be handled?
   - Use handle_error_template pattern
   - Log errors with context
   - Return state with error message (no interrupt for errors)

6. Frontend integration needed?
   - Check if sse_client.js needs updates for new interrupt data
   - Ensure message handling works with existing patterns
   - Test interrupt → resume → state persistence flow
"""
