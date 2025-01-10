# app/callbacks/chat.py
import json
import dash
from dash import html, callback_context
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from langchain_core.messages import AIMessage
from langgraph.errors import NodeInterrupt
import dash_bootstrap_components as dbc
import logging

logger = logging.getLogger(__name__)

def handle_interrupts(chat_history, compiled_workflow, config, user_input=None, selection_idx=None):
    """
    Handles any interrupt the workflow might have.
    """
    logger.debug({
        "event": "handle_interrupts_start",
        "input_params": {
            "user_input": user_input,
            "selection_idx": selection_idx
        }
    })

    state = compiled_workflow.get_state(config)
    logger.debug({
        "event": "workflow_state",
        "state": state
    })

    if not state.tasks:
        logger.debug("No tasks found in state - returning without interrupts")
        return chat_history, dash.no_update, dash.no_update, False

    interrupt_task = state.tasks[0]
    logger.debug({
        "event": "task_interrupts",
        "interrupts": interrupt_task.interrupts
    })

    if not interrupt_task.interrupts:
        logger.debug("No interrupts on first task - returning")
        return chat_history, dash.no_update, dash.no_update, False

    interrupt = interrupt_task.interrupts[0]
    interrupt_value = interrupt.value
    logger.debug({
        "event": "processing_interrupt",
        "interrupt_value": interrupt_value
    })

    if interrupt_value.get("message") == "map_selection":
        logger.debug({
            "event": "map_selection_interrupt",
            "g_unit": interrupt_value["g_unit"],
            "g_unit_type": interrupt_value["g_unit_type"]
        })
        return (chat_history, dash.no_update, interrupt_value["g_unit"], False)

    if "options" in interrupt_value:
        logger.debug("Handling options interrupt")
        if selection_idx is not None:
            logger.debug({
                "event": "processing_selection",
                "selection_idx": selection_idx
            })
            compiled_workflow.update_state(config=config, values={"selection_idx": selection_idx})
            return chat_history, dash.no_update, dash.no_update, True
        else:
            options = interrupt_value.get("options", [])
            logger.debug({
                "event": "creating_buttons",
                "num_options": len(options),
                "options": options
            })
            buttons = [
                dbc.Button(
                    opt["label"],
                    id={
                        "option_type": opt['option_type'],
                        "type": "dynamic-button-user-choice",
                        "index": opt["value"]
                    },
                    color="secondary",
                    className="mb-2"
                )
                for opt in options
            ]
            interrupt_message = html.Div(
                f"AI: {interrupt_value.get('message', 'Action required.')}",
                className="mb-2 text-primary"
            )
            new_history = chat_history[:] + [interrupt_message]
            return new_history, buttons, dash.no_update, True
    elif "selected_polygons" in interrupt_value.get('message', {}):
        logger.debug({
            "event": "polygon_selection",
            "polygons": interrupt_value['value']
        })
        return chat_history, dash.no_update, interrupt_value['value'], False
    else:
        logger.debug("Handling text input interrupt")
        if user_input:
            logger.debug({
                "event": "updating_state",
                "user_input": user_input
            })
            compiled_workflow.update_state(config=config, values={"messages": [("user", user_input)]})
            return chat_history, dash.no_update, dash.no_update, True
        else:
            logger.debug("Creating prompt for user text input")
            interrupt_message = html.Div(
                f"AI: {interrupt_value.get('message', 'Please provide input.')}",
                className="mb-2 text-primary"
            )
            new_history = chat_history[:] + [interrupt_message]
            return new_history, dash.no_update, dash.no_update, True

def register_chat_callbacks(app, compiled_workflow):
    @app.callback(
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value"),
        Output("thread_id", "data", allow_duplicate=True),
        Input("clear-button", "n_clicks"),
        Input("thread_id", "data"),
        prevent_initial_call=True
    )
    def clear_chat(n_clicks, thread_id):
        logger.debug({
            "event": "clear_chat",
            "n_clicks": n_clicks,
            "thread_id": thread_id
        })
        
        if n_clicks is None:
            logger.debug("No clicks - preventing update")
            raise PreventUpdate
            
        logger.debug({
            "event": "clearing_chat",
            "new_thread_id": thread_id + 1
        })
        return [], "", thread_id + 1

    @app.callback(
        Output("chat-display", "children", allow_duplicate=True),
        Output("chat-input", "value", allow_duplicate=True),
        Output("selected_ids", "data", allow_duplicate=True),
        Output("options-container", "children"),
        Input("send-button", "n_clicks"),
        Input({"option_type": ALL, "type": "dynamic-button-user-choice", "index": ALL}, "n_clicks"),
        State("thread_id", "data"),
        State("chat-input", "value"),
        State("chat-display", "children"),
        State("options-container", "children"),
        prevent_initial_call=True
    )
    def update_chat(n_clicks, button_clicks, thread_id, user_input, chat_history, buttons):
        logger.debug({
            "event": "update_chat_start",
            "params": {
                "n_clicks": n_clicks,
                "button_clicks": button_clicks,
                "thread_id": thread_id,
                "user_input": user_input,
                "chat_history_length": len(chat_history) if chat_history else 0,
                "buttons_present": bool(buttons)
            }
        })

        if chat_history is None:
            logger.debug("Initializing empty chat history")
            chat_history = []

        selection_idx = None
        if any(button_clicks):
            ctx = dash.callback_context.triggered[0]
            logger.debug({
                "event": "button_click",
                "context": ctx
            })
            selectiontext = json.loads(ctx["prop_id"].split(".")[0])
            selection_idx = int(selectiontext["index"])
            selection_option_type = selectiontext["option_type"]
            logger.debug({
                "event": "selection_processed",
                "selection": {
                    "index": selection_idx,
                    "type": selection_option_type
                }
            })
        elif n_clicks is None and (not user_input or user_input.strip() == ""):
            logger.debug("No interaction detected - returning without updates")
            return chat_history, "", dash.no_update, dash.no_update

        user_message = None
        if user_input and user_input.strip():
            logger.debug({
                "event": "processing_user_input",
                "input": user_input
            })
            user_message = html.Div(f"You: {user_input}", className="mb-2")

        config = {"configurable": {"thread_id": thread_id}}
        logger.debug({
            "event": "workflow_config",
            "config": config
        })
        
        state = compiled_workflow.get_state(config)
        logger.debug({
            "event": "workflow_state",
            "state": state
        })

        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config, 
            user_input=user_input, 
            selection_idx=selection_idx
        )
        
        logger.debug({
            "event": "handle_interrupts_result",
            "result": {
                "still_interrupting": still_interrupting,
                "selected_ids_updated": selected_ids != dash.no_update,
                "buttons_updated": buttons != dash.no_update,
                "chat_history_length": len(chat_history)
            }
        })

        if still_interrupting:
            logger.debug("Interrupt ongoing - returning intermediate state")
            return chat_history, "", selected_ids or dash.no_update, buttons

        inputs = {"messages": [("user", user_input)]} if user_input else None
        logger.debug({
            "event": "workflow_invoke",
            "inputs": inputs
        })

        db_res = compiled_workflow.invoke(inputs, config=config)
        logger.debug({
            "event": "workflow_response",
            "response": db_res
        })

        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config
        )
        
        if still_interrupting:
            logger.debug("New interrupt detected - returning intermediate state")
            return chat_history, "", dash.no_update, buttons
        
        ai_out = db_res['messages'][-1].content
        logger.debug({
            "event": "ai_response",
            "content": ai_out
        })
        ai_message = html.Div(f"AI: {ai_out}", className="mb-2 text-primary")

        new_chat_history = chat_history[:]
        if user_message:
            new_chat_history.append(user_message)
        new_chat_history.append(ai_message)
        logger.debug({
            "event": "chat_history_updated",
            "new_length": len(new_chat_history)
        })

        new_polygons = db_res.get('selected_polygons', None)
        logger.debug({
            "event": "polygons_status",
            "polygons_present": bool(new_polygons)
        })

        logger.debug("Completing update_chat")
        return new_chat_history, "", new_polygons, dash.no_update