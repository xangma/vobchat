from __future__ import annotations

import importlib

import pytest


def test_active_entrypoints_still_import() -> None:
    web_app = importlib.import_module("vobchat.web.app")
    api_main = importlib.import_module("vobchat.api.main")
    legacy_wrapper = importlib.import_module("vobchat.app")

    assert web_app.app is not None
    assert web_app.server is not None
    assert api_main.app is not None
    assert legacy_wrapper.server is web_app.server


@pytest.mark.parametrize(
    "module_name",
    [
        "vobchat.workflow",
        "vobchat.workflow_sse_adapter",
        "vobchat.conversational_agent",
        "vobchat.intent_handling",
        "vobchat.intent_subagents",
        "vobchat.tools",
        "vobchat.sse_manager",
        "vobchat.config",
        "vobchat.configure_logging",
        "vobchat.callbacks",
        "vobchat.components",
    ],
)
def test_legacy_modules_are_removed(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)
