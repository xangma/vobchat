import json
import dash
from dash import html

from dash import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from langgraph.errors import NodeInterrupt

import dash_bootstrap_components as dbc
import logging
from typing import Dict, Any

from dash_extensions.enrich import CycleBreakerInput
logger = logging.getLogger(__name__)


def register_chat_callbacks(app, compiled_workflow):
                
    @app.callback(
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value"),
        Output("app-state", "data", allow_duplicate=True),
        Input("clear-button", "n_clicks"),
        State("app-state", "data"),
        prevent_initial_call=True
    )
    def clear_chat(n_clicks, app_state):
        logger.debug(
            {"event": "clear_chat", "n_clicks": n_clicks, "app_state": app_state})
        if not n_clicks:
            raise PreventUpdate

        new_app_state = (app_state or {}).copy()
        new_app_state["messages"] = []
        new_app_state["thread_id"] = (
            new_app_state.get("thread_id", 0) or 0) + 1
        logger.debug({"event": "clearing_chat",
                    "new_app_state": new_app_state})

        return [], "", new_app_state

    @app.callback(
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value", allow_duplicate=True),
        Output("app-state", "data", allow_duplicate=True),
        Output("map-state", "data", allow_duplicate=True),
        Output("place-state", "data", allow_duplicate=True),
        Output("options-container", "children"),
        Input("send-button", "n_clicks"),
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        CycleBreakerInput("retrigger-chat", "data"),
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
        app_state,
        map_state,
        place_state,
        user_input,
        chat_history,
        buttons,
    ):
        app_state['retrigger_chat'] = False

        ctx = dash.callback_context
        ctx_trigger = ctx.triggered[0]["prop_id"]
        
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

        # 1) Initialize chat history
            
        if chat_history is None:
            chat_history = []
            
        # Prepare config, etc.
        thread_id = (app_state or {}).get("thread_id", 0)
        config = {"configurable": {"thread_id": thread_id}}

        logger.debug({"event": "workflow_config", "config": config})

        if map_state.get('selected_polygons'):
            compiled_workflow.update_state(config=config, values={
                                           'selected_polygons': map_state['selected_polygons'], 'selected_place_g_unit_type': map_state['unit_types']})

        # 2) Check triggers: button vs. send
        selection_idx = None
        if 'dynamic-button-user-choice' in ctx_trigger:
            # Identify which button was clicked
            selection_data = json.loads(ctx_trigger.split(".")[0])
            selection_idx = selection_data["index"]
            logger.debug({"event": "button_click",
                        "selection_idx": selection_idx})
            # Update the node's state with selection_idx
            values = {"selection_idx": selection_idx}
            compiled_workflow.update_state(config=config, values=values)
            buttons = []
        elif n_clicks is None and (not user_input or user_input.strip() == ""):
            # No meaningful user interaction
            raise PreventUpdate

        # 3) If user typed text, show it in the chat
        if user_input and user_input.strip():
            user_message = html.Div(f"You: {user_input}", className="mb-2")
            chat_history.append(user_message)

        inputs = None
        if user_input and user_input.strip():
            inputs = {"messages": [("user", user_input)]}
            
        db_res = compiled_workflow.invoke(
            inputs , config=config)

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
            if interrupt_task.interrupts or state.values.get("interrupt_state") == True:
                new_history = None
                logger.debug(
                    "First task has interrupts.")

                # We have at least one interrupt
                interrupt = interrupt_task.interrupts[0]
                interrupt_value = interrupt.value
                logger.debug({"event": "processing_interrupt",
                            "interrupt_value": interrupt_value})

                # Multiple choice "options" interrupt
                if "options" in interrupt_value:
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
                            className="mb-2"
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
                        "awaiting_user_selection": True
                    })
                    new_history = chat_history[:] + [interrupt_message]


                # Map selection
                if interrupt_value.get("message") == "map_selection":
                    logger.debug("Map selection interrupt")
                    map_state.update({
                        "selected_polygons": [interrupt_value["g_unit"]],
                        "unit_types": [interrupt_value["g_unit_type"]]
                    })

                    app_state.update({
                        "button_options": [],
                        "awaiting_user_selection": False,
                        "retrigger_chat": True
                    })
                    compiled_workflow.update_state(
                        config=config, values={"interrupt_state": False, "selection_idx": None, "selected_place_g_unit": interrupt_value["g_unit"], "selected_place_g_unit_type": interrupt_value["g_unit_type"]})
                        
                    buttons = []

                # Another example: "selected_polygons"
                elif "selected_polygons" in interrupt_value.get("message", {}):
                    logger.debug("Polygon selection interrupt")
                    map_state.update({
                        "selected_polygons": interrupt_value["value"]
                        })

                elif interrupt_value.get("cubes"):
                    logger.debug("Cube selection interrupt")
                    cubes = interrupt_value.get("cubes", [])
                    place_state.update({"cubes": cubes})
                    prompt_text = interrupt_value.get(
                        "message")
                    interrupt_message = html.Div(
                        f"AI: {prompt_text}", className="mb-2 text-primary")
                    new_history = chat_history[:] + [interrupt_message]
                    app_state.update({"show_visualization": True})

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
                        
                if new_history:
                    chat_history = new_history

        # 8) If no interrupt, we present the final AI output from db_res
        #    (only if there's an AI message in the updated state)
        messages = db_res["messages"] if "messages" in db_res else []
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
            buttons,
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
            return retrigger_chat + 1
        else:
            return dash.no_update
