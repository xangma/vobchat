# app/callbacks/chat.py
import json
import dash
from dash import html, no_update, callback_context
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from langchain_core.messages import AIMessage
from langgraph.errors import NodeInterrupt
import pandas as pd

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
    def update_chat(n_clicks, button_clicks, thread_id, user_input, chat_history, buttons):
        # Determine if the user clicked a button or submitted input
        selection_idx = None
        if chat_history is None:
            chat_history = []
        if any(button_clicks):
            ctx = dash.callback_context.triggered[0]
            selectiontext = json.loads(ctx["prop_id"].split(".")[0])
            selection_idx = int(selectiontext["index"])
            selection_option_type = selectiontext["option_type"]

        elif n_clicks is None and (user_input is None or user_input.strip() == ""):
            return chat_history, "", no_update, no_update

        # Initialize user message for chat display
        user_message = html.Div(f"You: {user_input}", className="mb-2") if user_input else None

        # Configure graph execution
        config = {"configurable": {"thread_id": thread_id}}

        # Check the current state of the graph
        state = compiled_workflow.get_state(config)

        if state.tasks:  # If the graph is already interrupted
            interrupt_task = state.tasks[0]  # Take the first task if multiple
            if interrupt_task.interrupts:
                interrupt = interrupt_task.interrupts[0]
                interrupt_value = interrupt.value

                if selection_idx is not None and selection_option_type == interrupt_value['options'][0]['option_type']:  # Resume graph with user selection_idx
                    compiled_workflow.update_state(config=config, values={"selection_idx": selection_idx})
                elif user_input and user_input.strip():  # Resume graph with user input
                    compiled_workflow.update_state(config=config, values={"messages": [("user", user_input)]})
                else:
                    # Display interrupt message and options
                    buttons = [
                        html.Button(
                            opt["label"],
                            id={"option_type": opt['option_type'], "type": "dynamic-button-user-choice", "index": opt["value"]}
                        )
                        for opt in interrupt_value.get("options", [])
                    ]
                    interrupt_message = html.Div(f"AI: {interrupt_value.get('message', 'Action required.')}",
                                                className="mb-2 text-primary")
                    return chat_history + [user_message, interrupt_message], "", no_update, buttons

        # Handle new user input if there is no ongoing interrupt
        inputs = {"messages": [("user", user_input)]} if user_input else None

        # Invoke the graph
        db_res = compiled_workflow.invoke(inputs, config=config)

        # Check if the graph exited with a new interrupt
        state = compiled_workflow.get_state(config)
        if state.tasks:
            interrupt_task = state.tasks[0]
            if interrupt_task.interrupts:
                interrupt = interrupt_task.interrupts[0]
                interrupt_value = interrupt.value
                buttons = [
                    html.Button(
                        opt["label"],
                        id={"option_type": opt['option_type'], "type": "dynamic-button-user-choice", "index": opt["value"]}
                    )
                    for opt in interrupt_value.get("options", [])
                ]
                interrupt_message = html.Div(f"AI: {interrupt_value.get('message', 'Action required.')}",
                                            className="mb-2 text-primary")
                return chat_history + [user_message, interrupt_message], "", no_update, buttons

        # Process graph output if no interrupt occurred
        ai_out = db_res['messages'][-1].content
        gdf_id = db_res.get('selected_place_gdf_id')
        ai_message = html.Div(f"AI: {ai_out}", className="mb-2 text-primary")

        # Update chat history
        if chat_history is None:
            chat_history = []
        if user_message:
            chat_history.append(user_message)
        chat_history.append(ai_message)