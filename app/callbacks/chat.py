import json
import dash
from dash import html
from dash.exceptions import PreventUpdate
from uuid import uuid4

from stores import app_state_data, map_state_data, place_state_data

from dash import Input, Output, State, ALL

import dash_bootstrap_components as dbc
import logging

from dash_extensions.enrich import CycleBreakerInput

from langgraph.types import interrupt, Command

logger = logging.getLogger(__name__)


def register_chat_callbacks(app, compiled_workflow):


    @app.callback(
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value", allow_duplicate=True),
        Output("app-state", "data", allow_duplicate=True),
        Output("map-state", "data", allow_duplicate=True),
        Output("place-state", "data", allow_duplicate=True),
        Output("retrigger-chat", "data", allow_duplicate=True),
        Output("options-container", "children"),
        Output("counts-store", "data"),
        Output("thread-id", "data"),
        Input("send-button", "n_clicks"),
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        CycleBreakerInput("retrigger-chat", "data"),
        Input("reset-button", "n_clicks"),
        State("thread-id", "data"),
        State("app-state", "data"),
        State("map-state", "data"),
        State("place-state", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),
        State("options-container", "children"),
        prevent_initial_call=True
    )
    def update_chat(
        n_clicks,
        button_clicks,
        retrigger_chat,
        reset__n_clicks,
        thread_id,
        app_state,
        map_state,
        place_state,
        user_input,
        chat_history,
        buttons,
    ):

        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
        if "retrigger-chat_data_breaker.dst" in ctx_trigger:
            logger.debug("retrigger-chat_data_breaker.dst triggered")
        workflow_res = None
        logger.debug({
            "event": "update_chat_start",
            "params": {
                "n_clicks": n_clicks,
                "button_clicks": button_clicks,
                "user_input": user_input,
                "chat_history_length": len(chat_history) if chat_history else 0,
                "ctx.triggered": ctx.triggered,
                "retrigger_chat": retrigger_chat
            }
        })
        
        if not app_state['retrigger_chat'] and "reset-button" not in ctx_trigger and "dynamic-button-user-choice" not in ctx_trigger and (not user_input or user_input.strip() == ""):
            # No meaningful user interaction
            logger.debug("No meaningful user interaction detected")
            raise PreventUpdate
        
        if retrigger_chat:
            retrigger_chat = None
            app_state.update({"retrigger_chat": False})
        
        # 0) Check if we need to reset the entire application
        if "reset-button" in ctx_trigger:
            app_state = app_state_data
            map_state = map_state_data
            place_state = place_state_data
            chat_history = []
            buttons = []
            thread_id = None
            counts_store = {}
            return (
                chat_history,
                "",
                app_state,
                map_state,
                place_state,
                retrigger_chat,
                buttons,
                counts_store,
                thread_id
            )
            
        # 1) Initialize chat history
            
        if chat_history is None:
            chat_history = []
            
        # Prepare config, etc.
        if not thread_id:
            thread_id = str(uuid4())
        config = {"configurable": {"thread_id": thread_id}}

        logger.debug({"event": "workflow_config", "config": config})

        before_state = compiled_workflow.get_state(config)

        if map_state.get('selected_polygons'):
            before_state.values['selected_polygons'] = map_state['selected_polygons']
            before_state.values['selected_polygons_unit_types'] = map_state['selected_polygons_unit_types']
            compiled_workflow.update_state(config=config, values=before_state.values)

        # 2) Check triggers: button vs. send
        selection_idx = None
        # compiled_workflow.update_state(config=config, values=values)
        if 'dynamic-button-user-choice' in ctx_trigger:
            # Identify which button was clicked
            selection_data = json.loads(ctx_trigger.split(".")[0])
            selection_idx = selection_data["index"]
            logger.debug({"event": "button_click",
                        "selection_idx": selection_idx})
            # Update the node's state with selection_idx
            # values = {"selection_idx": selection_idx}
            # compiled_workflow.update_state(config=config, values=values)
            buttons = []


        # 3) If user typed text, show it in the chat
        if user_input and user_input.strip():
            user_message = html.Div(f"You: {user_input}", className="mb-2")
            chat_history.append(user_message)

        inputs = None
        if user_input and user_input.strip():
            inputs = {"messages": [("user", user_input)]}
        

            
        if 'dynamic-button-user-choice' in ctx_trigger and before_state.values.get('interrupt_state'):
            before_state.values.update({"selection_idx": selection_idx})
            workflow_res = compiled_workflow.invoke(
                Command(goto=before_state.values['current_node'], update=before_state.values), config=config)
        else:
            workflow_res = compiled_workflow.invoke(
                inputs, config=config)

        # 7) Now handle any interrupts

        logger.debug({"event": "handle_interrupts_start",
                    "input_params": {"user_input": user_input}})

        # Default return values
        buttons = dash.no_update

        # Get current workflow state
        state = compiled_workflow.get_state(config)
        
        if state.tasks:
            # Check for interrupts on the first pending task
            interrupt_task = state.tasks[0]
            if interrupt_task.interrupts or state.values.get("interrupt_state") == True or state.values.get("interrupt_data"):
                new_history = None
                logger.debug(
                    "First task has interrupts.")

                # We have at least one interrupt
                if len(interrupt_task.interrupts) > 0:
                    # interrupt = interrupt_task.interrupts[0].value
                    interrupt_value = interrupt_task.interrupts[0].value
                else:
                    interrupt_value = state.values.get("interrupt_data")
                logger.debug({"event": "processing_interrupt",
                            "interrupt_value": interrupt_value})
                
                # update the workflow state with the interrupt data
                compiled_workflow.update_state(
                    config=config, values=interrupt_value)

                # Multiple choice "options" interrupt
                if interrupt_value.get("options", []):
                    logger.debug("Interrupt with multiple button options")
                    options = interrupt_value.get("options", [])

                    # The node wants the user to pick from a list of options
                    buttons = [
                        dbc.Button(
                            opt["label"],
                            id={
                                "option_type": opt["option_type"],
                                "type": "dynamic-button-user-choice",
                                "index": opt["value"]
                            },
                            color="secondary",
                            className="unit-filter-button me-2 mb-2",
                            outline=True,
                            value=opt["value"],
                            style={
                                '--unit-color': opt["color"],
                                'borderColor': opt["color"],
                                'backgroundColor': 'white',
                                # Use a dark/grey color for unselected text.
                                'color': '#333',
                                'transition': 'background-color 0.3s, color 0.3s'
                            }
                        )
                        for opt in options
                    ]
                    
                    prompt_text = interrupt_value.get(
                        "message", "Please choose:")
                    interrupt_message = html.Div(
                        f"AI: {prompt_text}", className="mb-2 text-primary")

                    # Mark that we are waiting for user selection
                    app_state.update({
                        "button_options": options,
                    })
                    new_history = chat_history[:] + [interrupt_message]
                    


                # Map selection
                if interrupt_value.get("current_node") == "select_unit_on_map":
                    logger.debug("Map selection interrupt")
                    selected_place_g_units = interrupt_value["selected_place_g_units"]
                    selected_place_g_unit_types = interrupt_value["selected_place_g_unit_types"]
                    for i, g_unit in enumerate(selected_place_g_units):
                        if g_unit not in map_state["selected_polygons"]:
                            map_state["selected_polygons"].append(str(g_unit))
                            map_state["selected_polygons_unit_types"].append(selected_place_g_unit_types[i])

                    app_state.update({
                        "button_options": [],
                        "retrigger_chat": True
                    })
                    retrigger_chat = None
                    buttons = []

                elif interrupt_value.get("cubes"):
                    logger.debug("Cube selection interrupt")
                    cubes = interrupt_value.get("cubes", [])
                    place_state.update({"cubes": cubes})
                    compiled_workflow.update_state(
                        config=config, values={"selected_cubes": cubes})

                    prompt_text = interrupt_value.get(
                        "message")
                    interrupt_message = html.Div(
                        f"AI: {prompt_text}", className="mb-2 text-primary")
                    new_history = chat_history[:] + [interrupt_message]
                    app_state.update({"show_visualization": True})

                elif interrupt_value.get("assistant_message"):
                    logger.debug("Assistant message interrupt")
                    assistant_message = interrupt_value.get(
                        "message")
                    interrupt_message = html.Div(
                        f"AI: {assistant_message}", className="mb-2 text-primary")
                    new_history = chat_history[:] + [interrupt_message]
                
                # Otherwise it's a "text input" interrupt
                else:
                    logger.debug("Text input interrupt")
                    prompt_text = interrupt_value.get(
                        "message", "Please provide input.")
                    interrupt_message = html.Div(
                        f"AI: {prompt_text}", className="mb-2 text-primary")

                    if user_input:
                        # We already have a user input, so we update the node
                        compiled_workflow.update_state(
                            config=config, values={"messages": [("user", user_input)]})
                    else:
                        # We need to prompt the user
                        new_history = chat_history[:] + [interrupt_message]

                if interrupt_value.get("current_place_index") is not None:
                    compiled_workflow.update_state(
                        config=config, values={"current_node": interrupt_value['current_node'], "selection_idx": None, "current_place_index": interrupt_value["current_place_index"], "selected_place_g_places": interrupt_value.get("selected_place_g_places"), "selected_place_g_units": interrupt_value.get("selected_place_g_units"), "selected_place_g_unit_types": interrupt_value.get("selected_place_g_unit_types")})
            
                if new_history:
                    chat_history = new_history


                
        # 8) If no interrupt, we present the final AI output from workflow_res
        #    (only if there's an AI message in the updated state)
        if workflow_res:
            messages = workflow_res["messages"] if "messages" in workflow_res else []
            if messages and messages[-1].type == "ai":
                ai_text = messages[-1].content
                chat_history.append(
                    html.Div(f"AI: {ai_text}", className="mb-2 text-primary"))

        logger.debug({
            "event": "chat_update_complete",
            "new_chat_history_length": len(chat_history),
            "new_app_state": app_state,
            "new_map_state": map_state
        })

        return (
            chat_history,
            "",
            app_state,
            map_state,
            place_state,
            retrigger_chat,
            buttons,
            thread_id
        )
    
    @app.callback(
        Output("retrigger-chat", "data", allow_duplicate=True),
        Input("app-state", "data"),
        Input("map-state", "data"),
        State("retrigger-chat", "data"),
        prevent_initial_call=True
    )
    def retrigger_chat_callback(app_state, map_state, retrigger_chat):
        if app_state.get("retrigger_chat"):
            logger.debug("Retriggering chat")
            return True
        else:
            return dash.no_update
