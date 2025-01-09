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
    Returns:
      - updated chat history
      - updated UI 'options-container' or other UI elements
      - updated data store(s)
      - boolean: is interrupt still ongoing or not?
    """
    state = compiled_workflow.get_state(config)
    logger.debug(f"Interrupt: Current state: {state}")
    if not state.tasks:
        # No tasks => no interrupts.
        return chat_history, dash.no_update, dash.no_update, False

    interrupt_task = state.tasks[0]
    if not interrupt_task.interrupts:
        # No interrupts on the first task
        return chat_history, dash.no_update, dash.no_update, False

    # If we do have an interrupt:
    interrupt = interrupt_task.interrupts[0]
    interrupt_value = interrupt.value
    
    # Suppose the interrupt might be resolved by user text *or* a user selection *or* something else.
    # Example logic:
    if interrupt_value.get("message") == "map_selection":
        # The backend wants the UI to set the filter to the chosen g_unit_type,
        # fetch polygons for that type, and select the given g_unit.

        g_unit = interrupt_value["g_unit"]
        g_unit_type = interrupt_value["g_unit_type"]

        # Option A: directly update the `unit-filter-state` and `map-state`
        # so your map callbacks fetch the correct polygons and select the given ID.
        # We'll just assume we pass them out as "updates".
        return (
            chat_history,
            dash.no_update,  # No new "options-container"
            g_unit,
            False
        )
    if "options" in interrupt_value:
        # Possibly a "choose 1 out of N" scenario
        if selection_idx is not None:
            # The user clicked or selected an option
            compiled_workflow.update_state(config=config, values={"selection_idx": selection_idx})
            # return now, so the main function can re-check the new state
            return chat_history, dash.no_update, dash.no_update, True
        else:
            # They haven't chosen yet => present the options
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
                for opt in interrupt_value.get("options", [])
            ]
            interrupt_message = html.Div(
                f"AI: {interrupt_value.get('message', 'Action required.')}",
                className="mb-2 text-primary"
            )
            # Append interrupt message to chat (if not already appended)
            new_history = chat_history[:] + [interrupt_message]
            return new_history, buttons, dash.no_update, True
    elif "selected_polygons" in interrupt_value.get('message', {}):
        # Possibly a "select polygon" scenario
        new_polygons = interrupt_value['value']
        return chat_history, dash.no_update, new_polygons, False
    else:
        # Possibly a "user text needed" scenario or something else
        # If the user typed something, update the workflow
        if user_input:
            compiled_workflow.update_state(config=config, values={"messages": [("user", user_input)]})
            # Let the main function re-check the new state
            return chat_history, dash.no_update, dash.no_update, True
        else:
            # Prompt user for text
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
        """
        Clears the conversation when the user clicks the clear button.
        """
        if n_clicks is None:
            raise PreventUpdate
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
    def update_chat(n_clicks, button_clicks, thread_id,
                    user_input, chat_history, buttons):
        """
        Either user typed something or clicked an interrupt button. Update workflow accordingly.
        """
        if chat_history is None:
            chat_history = []
        if any(button_clicks):
            ctx = dash.callback_context.triggered[0]
            selectiontext = json.loads(ctx["prop_id"].split(".")[0])
            selection_idx = int(selectiontext["index"])
            selection_option_type = selectiontext["option_type"]
        elif n_clicks is None and (not user_input or user_input.strip() == ""):
            # Nothing happened
            # raise PreventUpdate
            return chat_history, "", dash.no_update, dash.no_update

        # Prepare user message (if typed)
        user_message = None
        if user_input and user_input.strip():
            user_message = html.Div(f"You: {user_input}", className="mb-2")

        # If the graph was interrupted previously, handle that first
        config = {"configurable": {"thread_id": thread_id}}
        state = compiled_workflow.get_state(config)

        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config, 
            user_input=user_input, 
            selection_idx=selection_idx
        )

        if still_interrupting:
            # The interrupt logic is not resolved yet => return with the new UI
            return chat_history, "", selected_ids or dash.no_update, buttons
        inputs = {"messages": [("user", user_input)]} if user_input else None

        # Invoke the graph
        db_res = compiled_workflow.invoke(inputs, config=config)

        # 3) Check again if there's a *new* interrupt
        chat_history, buttons, selected_ids, still_interrupting = handle_interrupts(
            chat_history, 
            compiled_workflow, 
            config
        )
        if still_interrupting:
            return chat_history, "", dash.no_update, buttons
        
        # Otherwise, the workflow completed or continued normally
        ai_out = db_res['messages'][-1].content
        ai_message = html.Div(f"AI: {ai_out}", className="mb-2 text-primary")

        # Update chat history
        new_chat_history = chat_history[:]
        if user_message:
            new_chat_history.append(user_message)
        new_chat_history.append(ai_message)

        # If a new polygon selection was set in the workflow
        new_polygons = db_res.get('selected_polygons', None)

        return new_chat_history, "", new_polygons, dash.no_update