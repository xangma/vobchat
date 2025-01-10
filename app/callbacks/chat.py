import json
import dash
from dash import html, callback_context
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from langchain_core.messages import AIMessage
from langgraph.errors import NodeInterrupt
import dash_bootstrap_components as dbc
import logging
from configure_logging import format_state_for_logging, format_complex_object

logger = logging.getLogger(__name__)

def handle_interrupts(chat_history, compiled_workflow, config, user_input=None, selection_idx=None):
    """
    Handles any interrupt the workflow might have.
    """
    logger.debug("\n" + "="*50 + "\nStarting handle_interrupts")
    logger.debug("Input params:\n%s", format_complex_object({
        "user_input": user_input,
        "selection_idx": selection_idx
    }))

    state = compiled_workflow.get_state(config)
    logger.debug("Current state:\n%s", format_state_for_logging(state))

    if not state.tasks:
        logger.debug("No tasks found in state - returning without interrupts")
        return chat_history, dash.no_update, dash.no_update, False

    interrupt_task = state.tasks[0]
    logger.debug("First task interrupts:\n%s", format_complex_object(interrupt_task.interrupts))

    if not interrupt_task.interrupts:
        logger.debug("No interrupts on first task - returning")
        return chat_history, dash.no_update, dash.no_update, False

    interrupt = interrupt_task.interrupts[0]
    interrupt_value = interrupt.value
    logger.debug("Processing interrupt value:\n%s", format_complex_object(interrupt_value))

    if interrupt_value.get("message") == "map_selection":
        logger.debug("Handling map_selection interrupt")
        g_unit = interrupt_value["g_unit"]
        g_unit_type = interrupt_value["g_unit_type"]
        logger.debug("Map selection params: g_unit=%s, g_unit_type=%s", g_unit, g_unit_type)
        return (chat_history, dash.no_update, g_unit, False)

    if "options" in interrupt_value:
        logger.debug("Handling options interrupt")
        if selection_idx is not None:
            logger.debug("Processing user selection: %s", selection_idx)
            compiled_workflow.update_state(config=config, values={"selection_idx": selection_idx})
            return chat_history, dash.no_update, dash.no_update, True
        else:
            options = interrupt_value.get("options", [])
            logger.debug("Creating buttons for %d options", len(options))
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
        logger.debug("Handling selected_polygons interrupt")
        new_polygons = interrupt_value['value']
        logger.debug("New polygons data:\n%s", format_complex_object(new_polygons))
        return chat_history, dash.no_update, new_polygons, False
    else:
        logger.debug("Handling text input interrupt")
        if user_input:
            logger.debug("Updating state with user input: %s", user_input)
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
        logger.debug("\n" + "="*50 + "\nClear chat called: n_clicks=%s, thread_id=%s", 
                    n_clicks, thread_id)
        if n_clicks is None:
            logger.debug("No clicks - preventing update")
            raise PreventUpdate
        logger.debug("Clearing chat. New thread_id will be: %d", thread_id + 1)
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
        logger.debug("\n" + "="*50 + "\nUpdate chat called")
        logger.debug("Callback parameters:\n%s", format_complex_object({
            "n_clicks": n_clicks,
            "button_clicks": button_clicks,
            "thread_id": thread_id,
            "user_input": user_input,
            "chat_history_length": len(chat_history) if chat_history else 0,
            "buttons_present": bool(buttons)
        }))

        if chat_history is None:
            logger.debug("Initializing empty chat history")
            chat_history = []

        selection_idx = None
        if any(button_clicks):
            ctx = dash.callback_context.triggered[0]
            logger.debug("Button click detected. Context:\n%s", format_complex_object(ctx))
            selectiontext = json.loads(ctx["prop_id"].split(".")[0])
            selection_idx = int(selectiontext["index"])
            selection_option_type = selectiontext["option_type"]
            logger.debug("Selected option: index=%d, type=%s", 
                        selection_idx, selection_option_type)
        elif n_clicks is None and (not user_input or user_input.strip() == ""):
            logger.debug("No interaction detected - returning without updates")
            return chat_history, "", dash.no_update, dash.no_update

        user_message = None
        if user_input and user_input.strip():
            logger.debug("Processing user input: %s", user_input)
            user_message = html.Div(f"You: {user_input}", className="mb-2")

        config = {"configurable": {"thread_id": thread_id}}
        logger.debug("Created workflow config: %s", format_complex_object(config))
        
        state = compiled_workflow.get_state(config)
        logger.debug("Current workflow state:\n%s", format_state_for_logging(state))

        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config, 
            user_input=user_input, 
            selection_idx=selection_idx
        )
        
        logger.debug("handle_interrupts result:\n%s", format_complex_object({
            "still_interrupting": still_interrupting,
            "selected_ids_updated": selected_ids != dash.no_update,
            "buttons_updated": buttons != dash.no_update,
            "chat_history_length": len(chat_history)
        }))

        if still_interrupting:
            logger.debug("Interrupt ongoing - returning intermediate state")
            return chat_history, "", selected_ids or dash.no_update, buttons

        inputs = {"messages": [("user", user_input)]} if user_input else None
        logger.debug("Invoking workflow with inputs:\n%s", format_complex_object(inputs))

        db_res = compiled_workflow.invoke(inputs, config=config)
        logger.debug("Workflow response:\n%s", format_complex_object(db_res))

        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config
        )
        
        if still_interrupting:
            logger.debug("New interrupt detected - returning intermediate state")
            return chat_history, "", dash.no_update, buttons
        
        ai_out = db_res['messages'][-1].content
        logger.debug("Processing AI response:\n%s", ai_out)
        ai_message = html.Div(f"AI: {ai_out}", className="mb-2 text-primary")

        new_chat_history = chat_history[:]
        if user_message:
            new_chat_history.append(user_message)
        new_chat_history.append(ai_message)
        logger.debug("Updated chat history length: %d", len(new_chat_history))

        new_polygons = db_res.get('selected_polygons', None)
        logger.debug("New polygons present: %s", bool(new_polygons))

        logger.debug("Completing update_chat")
        return new_chat_history, "", new_polygons, dash.no_update