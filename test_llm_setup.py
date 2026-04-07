from __future__ import annotations

import asyncio
from pathlib import Path

from vobchat.api.schemas.chat import ChatOperation, ChatTurnRequest, PlannerAction, PlannerResult
from vobchat.api.services.chat_orchestrator import ChatOrchestrator
from vobchat.core.llm.client import (
    LLMConfigurationError,
    LLMConnectivityStatus,
)
from vobchat.core.llm.setup import build_llm_env, read_env_file, update_env_file
from vobchat.core.settings import build_settings
from vobchat.cli import main as cli_main


def test_provider_agnostic_llm_settings_parse():
    settings = build_settings(
        {
            "LLM_PROVIDER": "vllm",
            "LLM_OPENAI_BASE_URL": "http://127.0.0.1:8001",
            "LLM_MODEL": "Qwen/Qwen2.5-7B-Instruct",
            "LLM_API_KEY": "token",
            "LLM_TEMPERATURE": "0.2",
            "LLM_TIMEOUT_SECONDS": "45",
            "LLM_VERIFY_SSL": "false",
        }
    )

    assert settings.llm.provider == "vllm"
    assert settings.llm.openai_base_url == "http://127.0.0.1:8001/v1"
    assert settings.llm.model == "Qwen/Qwen2.5-7B-Instruct"
    assert settings.llm.api_key == "token"
    assert settings.llm.temperature == 0.2
    assert settings.llm.timeout_seconds == 45
    assert settings.llm.verify_ssl is False


def test_update_env_file_replaces_legacy_llm_keys(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "VOBCHAT_LLM_MODEL=old-model\n"
        "OLLAMA_HOST=127.0.0.1\n"
        "SECRET_KEY=keep-me\n",
        encoding="utf-8",
    )

    update_env_file(
        env_file,
        build_llm_env(
            provider="openai-compatible",
            base_url="http://127.0.0.1:11434/v1",
            model="qwen2.5:7b-instruct",
            api_key="",
            verify_ssl=False,
        ),
    )

    updated = env_file.read_text(encoding="utf-8")
    assert "SECRET_KEY=keep-me" in updated
    assert "LLM_OPENAI_BASE_URL=http://127.0.0.1:11434/v1" in updated
    assert "LLM_MODEL=qwen2.5:7b-instruct" in updated
    assert "VOBCHAT_LLM_MODEL" not in updated
    assert "OLLAMA_HOST" not in updated


def test_setup_llm_existing_endpoint_writes_env_and_reports_success(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    env_file = tmp_path / ".env"

    monkeypatch.setattr(
        "vobchat.cli.probe_settings_from_env_file",
        lambda path: LLMConnectivityStatus(
            provider="openai-compatible",
            base_url="http://127.0.0.1:9999/v1",
            model="demo-model",
            configured=True,
            reachable=True,
            model_available=True,
            detail="ok",
        ),
    )

    exit_code = cli_main(
        [
            "setup-llm",
            "--env-file",
            str(env_file),
            "--mode",
            "existing",
            "--base-url",
            "http://127.0.0.1:9999/v1",
            "--model",
            "demo-model",
            "--api-key",
            "secret",
            "--no-verify-ssl",
        ]
    )

    output = capsys.readouterr().out
    config = read_env_file(env_file)

    assert exit_code == 0
    assert "LLM setup succeeded." in output
    assert config["LLM_PROVIDER"] == "openai-compatible"
    assert config["LLM_OPENAI_BASE_URL"] == "http://127.0.0.1:9999/v1"
    assert config["LLM_MODEL"] == "demo-model"
    assert config["LLM_API_KEY"] == "secret"
    assert config["LLM_VERIFY_SSL"] == "false"


def test_setup_llm_vllm_bootstrap_uses_optional_helper(monkeypatch, tmp_path: Path, capsys):
    env_file = tmp_path / ".env"
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        "vobchat.cli.detect_command",
        lambda name: type("Availability", (), {"available": True, "path": f"/usr/bin/{name}"})(),
    )
    monkeypatch.setattr(
        "vobchat.cli.run_command",
        lambda command, **kwargs: calls.append(tuple(command))
        or type("Result", (), {"returncode": 0, "stdout": "started", "stderr": ""})(),
    )
    monkeypatch.setattr(
        "vobchat.cli.probe_settings_from_env_file",
        lambda path: LLMConnectivityStatus(
            provider="vllm",
            base_url="http://127.0.0.1:8001/v1",
            model="Qwen/Qwen2.5-7B-Instruct",
            configured=True,
            reachable=True,
            model_available=True,
            detail="ok",
        ),
    )

    exit_code = cli_main(
        [
            "setup-llm",
            "--env-file",
            str(env_file),
            "--mode",
            "vllm",
            "--model",
            "Qwen/Qwen2.5-7B-Instruct",
            "--bootstrap",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert ("docker", "compose", "-f", "docker-compose.llm-vllm.yml", "up", "-d") in calls
    assert "Started the optional vLLM helper compose stack." in output or "started" in output


def test_doctor_llm_reports_unconfigured_endpoint(monkeypatch, tmp_path: Path, capsys):
    env_file = tmp_path / ".env"
    env_file.write_text("SECRET_KEY=dev-secret-key\n", encoding="utf-8")
    for key in (
        "LLM_PROVIDER",
        "LLM_OPENAI_BASE_URL",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_VERIFY_SSL",
    ):
        monkeypatch.delenv(key, raising=False)

    exit_code = cli_main(["doctor", "llm", "--env-file", str(env_file)])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Configured: no" in output
    assert "setup-llm" in output


class UnavailableLLMClient:
    async def complete_text(self, messages, *, temperature=None, max_tokens=None):
        raise LLMConfigurationError("missing llm config")

    async def stream_text(self, messages, *, temperature=None, max_tokens=None):
        if False:
            yield ""


class ReplyOnlyPlanner:
    async def plan(self, state, user_message):
        return PlannerResult(
            source="fallback",
            action=PlannerAction(
                operation=ChatOperation.REPLY_ONLY,
                confidence=1.0,
                assistant_task="Reply briefly.",
            ),
        )


def test_chat_orchestrator_returns_actionable_llm_setup_message():
    orchestrator = ChatOrchestrator(
        planner=ReplyOnlyPlanner(),
        llm_client=UnavailableLLMClient(),
    )

    created = asyncio.run(orchestrator.create_thread())
    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(thread_id=created.thread_id, message="hello", stream=False),
            emit_events=False,
        )
    )

    assert "setup-llm" in response.assistant_message.content
    assert any("setup-llm" in notice for notice in response.ui_delta.notices)
