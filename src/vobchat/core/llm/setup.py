from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from vobchat.core.llm.client import LLMConnectivityStatus, probe_llm_endpoint
from vobchat.core.settings import AppSettings, build_settings


DEFAULT_OLLAMA_BASE_URL = "http://127.0.0.1:11434/v1"
DEFAULT_OLLAMA_MODEL = "qwen2.5:7b-instruct"
DEFAULT_VLLM_BASE_URL = "http://127.0.0.1:8001/v1"
DEFAULT_VLLM_MODEL = "Qwen/Qwen2.5-7B-Instruct"

LEGACY_LLM_ENV_KEYS = (
    "VOBCHAT_LLM_MODEL",
    "VOBCHAT_LLM_TEMP",
    "VOBCHAT_OLLAMA_REASONING",
    "VOBCHAT_OPENAI_API_KEY",
    "VOBCHAT_LLM_TIMEOUT_SECONDS",
    "OLLAMA_HOST",
    "OLLAMA_PORT",
    "OLLAMA_SUBPATH",
    "OLLAMA_USE_SSL",
)


@dataclass(frozen=True)
class CommandAvailability:
    name: str
    available: bool
    path: str | None


@dataclass(frozen=True)
class CommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


def read_env_file(path: str | Path) -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            try:
                value = json.loads(value) if value[0] == '"' else value[1:-1]
            except Exception:
                value = value[1:-1]
        values[key.strip()] = value
    return values


def build_effective_settings(
    path: str | Path,
    *,
    overrides: Mapping[str, str] | None = None,
) -> AppSettings:
    env = dict(os.environ)
    env.update(read_env_file(path))
    if overrides:
        env.update({key: value for key, value in overrides.items() if value is not None})
    return build_settings(env)


def build_llm_env(
    *,
    provider: str,
    base_url: str,
    model: str,
    api_key: str = "",
    temperature: float = 0.7,
    timeout_seconds: int = 60,
    verify_ssl: bool = True,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str | None]:
    updates: dict[str, str | None] = {
        "LLM_PROVIDER": provider,
        "LLM_OPENAI_BASE_URL": base_url,
        "LLM_MODEL": model,
        "LLM_API_KEY": api_key,
        "LLM_TEMPERATURE": str(temperature),
        "LLM_TIMEOUT_SECONDS": str(timeout_seconds),
        "LLM_VERIFY_SSL": "true" if verify_ssl else "false",
    }
    for key in LEGACY_LLM_ENV_KEYS:
        updates[key] = None
    if extra:
        updates.update(extra)
    return updates


def update_env_file(path: str | Path, updates: Mapping[str, str | None]) -> None:
    env_path = Path(path)
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    rendered: list[str] = []
    remaining = dict(updates)

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            rendered.append(line)
            continue

        key, _ = line.split("=", 1)
        normalized_key = key.strip()
        if normalized_key not in remaining:
            rendered.append(line)
            continue

        value = remaining.pop(normalized_key)
        if value is None:
            continue
        rendered.append(f"{normalized_key}={_format_env_value(value)}")

    for key, value in remaining.items():
        if value is None:
            continue
        rendered.append(f"{key}={_format_env_value(value)}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(rendered).rstrip() + "\n", encoding="utf-8")


def detect_command(name: str) -> CommandAvailability:
    resolved = shutil.which(name)
    return CommandAvailability(name=name, available=resolved is not None, path=resolved)


def run_command(
    command: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> CommandResult:
    completed = subprocess.run(
        list(command),
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(
        command=tuple(command),
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def probe_settings_from_env_file(
    path: str | Path,
    *,
    overrides: Mapping[str, str] | None = None,
) -> LLMConnectivityStatus:
    settings = build_effective_settings(path, overrides=overrides)
    return probe_llm_endpoint(settings.llm)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _format_env_value(value: str) -> str:
    if value == "":
        return ""
    if re.search(r"\s|#|['\"]", value):
        return json.dumps(value)
    return value
