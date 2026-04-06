from __future__ import annotations

import json

from vobchat.api.schemas.chat import ChatThreadState, PlannerResult
from vobchat.core.llm.tool_schemas import planner_operation_descriptions


PLANNER_SYSTEM_PROMPT = """You are the VobChat planner.
Choose exactly one explicit operation for the user's latest message.
You may only choose deterministic operations from the allowed set.
Do not write SQL, code, Plotly, or free-form tool instructions.
Prefer the current thread state when it already contains the needed place, theme, or cube context.
If the request is ambiguous, choose clarify.
Return JSON only.
"""


ASSISTANT_SYSTEM_PROMPT = """You are the VobChat assistant.
Write a concise, factual reply based only on the supplied structured result.
Do not invent data, SQL, or charts.
Keep the answer brief and direct. If clarification is needed, ask for it plainly.
"""


def _compact_thread_summary(state: ChatThreadState) -> dict[str, object]:
    recent_messages = [
        {"role": message.role, "content": message.content}
        for message in state.messages[-6:]
    ]
    selected_places = [
        {"place_id": place.place.place_id, "name": place.place.name}
        for place in state.selected_places
    ]
    selected_theme = None
    if state.selected_theme is not None:
        selected_theme = {
            "theme_id": state.selected_theme.theme_id,
            "label": state.selected_theme.label,
        }
    selected_cubes = [
        {"cube_id": cube.cube_id, "label": cube.label}
        for cube in state.selected_cubes
    ]
    return {
        "thread_id": state.thread_id,
        "selected_places": selected_places,
        "selected_theme": selected_theme,
        "selected_cubes": selected_cubes,
        "recent_messages": recent_messages,
    }


def build_planner_messages(
    state: ChatThreadState,
    user_message: str,
) -> list[dict[str, str]]:
    operation_help = json.dumps(planner_operation_descriptions(), indent=2)
    thread_summary = json.dumps(_compact_thread_summary(state), indent=2)
    return [
        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
        {
            "role": "system",
            "content": f"Allowed operations:\n{operation_help}",
        },
        {
            "role": "user",
            "content": (
                "Thread summary:\n"
                f"{thread_summary}\n\n"
                f"Latest user message:\n{user_message}"
            ),
        },
    ]


def build_assistant_messages(
    state: ChatThreadState,
    planner_result: PlannerResult,
    execution_summary: dict[str, object],
) -> list[dict[str, str]]:
    thread_summary = json.dumps(_compact_thread_summary(state), indent=2)
    summary = json.dumps(
        {
            "planner_result": planner_result.model_dump(mode="json"),
            "execution_summary": execution_summary,
        },
        indent=2,
    )
    return [
        {"role": "system", "content": ASSISTANT_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "Current thread summary:\n"
                f"{thread_summary}\n\n"
                "Structured result:\n"
                f"{summary}"
            ),
        },
    ]
