from __future__ import annotations

import argparse
from getpass import getpass
import os
from pathlib import Path
from typing import Sequence

from vobchat.core.llm.client import llm_setup_guidance
from vobchat.core.llm.setup import (
    DEFAULT_OLLAMA_BASE_URL,
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_VLLM_BASE_URL,
    DEFAULT_VLLM_MODEL,
    build_llm_env,
    detect_command,
    probe_settings_from_env_file,
    read_env_file,
    repo_root,
    run_command,
    update_env_file,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    return int(handler(args) or 0)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vobchat")
    subparsers = parser.add_subparsers(dest="command")

    setup_parser = subparsers.add_parser(
        "setup-llm",
        help="Guide LLM endpoint setup for the active VobChat runtime.",
    )
    setup_parser.add_argument("--env-file", default=".env", help="Env file to read and update.")
    setup_parser.add_argument(
        "--mode",
        choices=("existing", "ollama", "vllm"),
        help="Setup mode. If omitted, the command prompts interactively.",
    )
    setup_parser.add_argument("--base-url", help="OpenAI-compatible base URL to use.")
    setup_parser.add_argument("--model", help="Model name or repository to use.")
    setup_parser.add_argument("--api-key", help="API key to send to the endpoint.")
    setup_parser.add_argument("--provider", help="Provider label to record in config.")
    setup_parser.add_argument(
        "--verify-ssl",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Verify TLS certificates when contacting the LLM endpoint.",
    )
    setup_parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="For vLLM, start the optional helper compose file after writing config.",
    )
    setup_parser.add_argument(
        "--pull",
        action="store_true",
        help="For Ollama, pull the selected model after writing config.",
    )
    setup_parser.add_argument(
        "--skip-test",
        action="store_true",
        help="Skip the connectivity test after updating config.",
    )
    setup_parser.set_defaults(handler=_handle_setup_llm)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run targeted diagnostics for the active VobChat runtime.",
    )
    doctor_subparsers = doctor_parser.add_subparsers(dest="doctor_command")

    doctor_llm_parser = doctor_subparsers.add_parser(
        "llm",
        help="Check the configured OpenAI-compatible LLM endpoint.",
    )
    doctor_llm_parser.add_argument(
        "--env-file",
        default=".env",
        help="Env file to inspect before probing the endpoint.",
    )
    doctor_llm_parser.set_defaults(handler=_handle_doctor_llm)

    return parser


def _handle_setup_llm(args: argparse.Namespace) -> int:
    env_file = Path(args.env_file).expanduser()
    current = read_env_file(env_file)
    interactive = args.mode is None
    mode = args.mode or _prompt_choice(
        "Choose LLM setup mode",
        choices=(
            ("existing", "Use an existing OpenAI-compatible endpoint"),
            ("ollama", "Set up against a local Ollama server"),
            ("vllm", "Set up against an optional vLLM service"),
        ),
        default="existing",
    )

    if mode == "existing":
        updates = _setup_existing_endpoint(args, current, interactive=interactive)
    elif mode == "ollama":
        updates = _setup_ollama(args, current, interactive=interactive)
    else:
        updates = _setup_vllm(args, current, interactive=interactive)

    update_env_file(env_file, updates)
    print(f"Updated {env_file} with provider-neutral LLM settings.")

    if mode == "ollama":
        _maybe_pull_ollama_model(args, updates, interactive=interactive)
    elif mode == "vllm":
        _maybe_bootstrap_vllm(args, current, updates, interactive=interactive)

    if args.skip_test:
        print("Skipped connectivity test.")
        return 0

    status = probe_settings_from_env_file(env_file)
    _print_doctor_status(status)
    if status.reachable and status.model_available is not False:
        print("LLM setup succeeded.")
        return 0

    print("LLM setup finished, but the endpoint test failed. Fix the endpoint and rerun `vobchat doctor llm`.")
    return 1


def _handle_doctor_llm(args: argparse.Namespace) -> int:
    env_file = Path(args.env_file).expanduser()
    status = probe_settings_from_env_file(env_file)
    _print_doctor_status(status)
    if status.reachable and status.model_available is not False:
        return 0
    print(f"Next step: {llm_setup_guidance()}")
    return 1


def _setup_existing_endpoint(
    args: argparse.Namespace,
    current: dict[str, str],
    *,
    interactive: bool,
) -> dict[str, str | None]:
    provider = (args.provider or current.get("LLM_PROVIDER") or "openai-compatible").strip()
    base_url = _prompt_or_value(
        args.base_url,
        "OpenAI-compatible base URL",
        current.get("LLM_OPENAI_BASE_URL") or "",
        interactive=interactive,
    )
    model = _prompt_or_value(
        args.model,
        "Model name",
        current.get("LLM_MODEL") or "",
        interactive=interactive,
    )
    api_key = (
        args.api_key
        if args.api_key is not None
        else (
            _prompt_secret(
                "API key (leave blank if the endpoint does not need one)",
                current.get("LLM_API_KEY", ""),
            )
            if interactive
            else current.get("LLM_API_KEY", "")
        )
    )
    verify_ssl = _resolve_verify_ssl(
        args.verify_ssl,
        current=current.get("LLM_VERIFY_SSL"),
        default=not base_url.startswith("http://"),
        interactive=interactive,
    )
    return build_llm_env(
        provider=provider,
        base_url=base_url,
        model=model,
        api_key=api_key,
        temperature=float(current.get("LLM_TEMPERATURE", "0.7") or 0.7),
        timeout_seconds=int(current.get("LLM_TIMEOUT_SECONDS", "60") or 60),
        verify_ssl=verify_ssl,
    )


def _setup_ollama(
    args: argparse.Namespace,
    current: dict[str, str],
    *,
    interactive: bool,
) -> dict[str, str | None]:
    ollama = detect_command("ollama")
    if ollama.available:
        print(f"Detected Ollama CLI at {ollama.path}.")
    else:
        print("Ollama CLI is not installed. Install it from https://ollama.com/download and rerun this command.")

    base_url = _prompt_or_value(
        args.base_url,
        "Ollama OpenAI-compatible base URL",
        current.get("LLM_OPENAI_BASE_URL") or DEFAULT_OLLAMA_BASE_URL,
        interactive=interactive,
    )
    model = _prompt_or_value(
        args.model,
        "Ollama model name",
        current.get("LLM_MODEL") or DEFAULT_OLLAMA_MODEL,
        interactive=interactive,
    )
    if ollama.available:
        print("If Ollama is installed but not running, start it with `ollama serve` before rerunning `vobchat doctor llm`.")
    return build_llm_env(
        provider="ollama",
        base_url=base_url,
        model=model,
        api_key=args.api_key or current.get("LLM_API_KEY", ""),
        temperature=float(current.get("LLM_TEMPERATURE", "0.7") or 0.7),
        timeout_seconds=int(current.get("LLM_TIMEOUT_SECONDS", "60") or 60),
        verify_ssl=_resolve_verify_ssl(
            args.verify_ssl,
            current=current.get("LLM_VERIFY_SSL"),
            default=False,
            interactive=interactive,
        ),
    )


def _setup_vllm(
    args: argparse.Namespace,
    current: dict[str, str],
    *,
    interactive: bool,
) -> dict[str, str | None]:
    docker = detect_command("docker")
    if docker.available:
        print(f"Detected Docker at {docker.path}.")
    else:
        print("Docker is not installed or not on PATH. The optional vLLM bootstrap helper will not be available.")

    base_url = _prompt_or_value(
        args.base_url,
        "vLLM OpenAI-compatible base URL",
        current.get("LLM_OPENAI_BASE_URL") or DEFAULT_VLLM_BASE_URL,
        interactive=interactive,
    )
    model = _prompt_or_value(
        args.model,
        "vLLM model repository",
        current.get("VLLM_MODEL_REPOSITORY") or current.get("LLM_MODEL") or DEFAULT_VLLM_MODEL,
        interactive=interactive,
    )
    api_key = (
        args.api_key
        if args.api_key is not None
        else (
            _prompt_secret(
                "API key (leave blank if the vLLM endpoint does not require one)",
                current.get("LLM_API_KEY", ""),
            )
            if interactive
            else current.get("LLM_API_KEY", "")
        )
    )
    extra_updates = {
        "VLLM_MODEL_REPOSITORY": model,
        "VLLM_PORT": str(_port_from_base_url(base_url, default=8001)),
    }
    if "HF_TOKEN" in current:
        extra_updates["HF_TOKEN"] = current["HF_TOKEN"]
    return build_llm_env(
        provider="vllm",
        base_url=base_url,
        model=model,
        api_key=api_key,
        temperature=float(current.get("LLM_TEMPERATURE", "0.7") or 0.7),
        timeout_seconds=int(current.get("LLM_TIMEOUT_SECONDS", "60") or 60),
        verify_ssl=_resolve_verify_ssl(
            args.verify_ssl,
            current=current.get("LLM_VERIFY_SSL"),
            default=False,
            interactive=interactive,
        ),
        extra=extra_updates,
    )


def _maybe_pull_ollama_model(
    args: argparse.Namespace,
    updates: dict[str, str | None],
    *,
    interactive: bool,
) -> None:
    model = updates.get("LLM_MODEL")
    if not model:
        return
    ollama = detect_command("ollama")
    if not ollama.available:
        return
    if not args.pull and not interactive:
        return
    if not args.pull and not _prompt_confirm(f"Pull Ollama model '{model}' now?", default=False):
        return
    result = run_command(("ollama", "pull", model))
    if result.returncode == 0:
        print(result.stdout.strip() or f"Pulled Ollama model '{model}'.")
        return
    print(result.stderr.strip() or result.stdout.strip() or f"`ollama pull {model}` failed.")


def _maybe_bootstrap_vllm(
    args: argparse.Namespace,
    current: dict[str, str],
    updates: dict[str, str | None],
    *,
    interactive: bool,
) -> None:
    docker = detect_command("docker")
    if not docker.available:
        return
    should_bootstrap = args.bootstrap
    if interactive and not should_bootstrap:
        should_bootstrap = _prompt_confirm(
            "Start the optional vLLM helper compose stack now?",
            default=False,
        )
    if not should_bootstrap:
        print(
            "Optional vLLM helper left stopped. Start it later with "
            "`docker compose -f docker-compose.llm-vllm.yml up -d`."
        )
        return

    command = ("docker", "compose", "-f", "docker-compose.llm-vllm.yml", "up", "-d")
    env = dict(os.environ)
    env.update(current)
    env.update({key: value for key, value in updates.items() if value is not None})
    result = run_command(command, cwd=repo_root(), env=env)
    if result.returncode == 0:
        print(result.stdout.strip() or "Started the optional vLLM helper compose stack.")
        return
    print(result.stderr.strip() or result.stdout.strip() or "Failed to start the optional vLLM helper compose stack.")


def _print_doctor_status(status) -> None:
    print(f"Provider: {status.provider or '<unset>'}")
    print(f"Base URL: {status.base_url or '<unset>'}")
    print(f"Model: {status.model or '<unset>'}")
    print(f"Configured: {'yes' if status.configured else 'no'}")
    print(f"Reachable: {'yes' if status.reachable else 'no'}")
    if status.model_available is not None:
        print(f"Model available: {'yes' if status.model_available else 'no'}")
    print(f"Detail: {status.detail}")


def _prompt_or_value(
    value: str | None,
    label: str,
    default: str,
    *,
    interactive: bool,
) -> str:
    if value is not None and value.strip():
        return value.strip()
    if not interactive:
        return default
    return _prompt_text(label, default=default)


def _prompt_text(label: str, *, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    response = input(f"{label}{suffix}: ").strip()
    return response or default


def _prompt_secret(label: str, default: str = "") -> str:
    suffix = " [stored value hidden]" if default else ""
    response = getpass(f"{label}{suffix}: ").strip()
    return response or default


def _prompt_confirm(label: str, *, default: bool) -> bool:
    suffix = " [Y/n]" if default else " [y/N]"
    response = input(f"{label}{suffix}: ").strip().lower()
    if not response:
        return default
    return response in {"y", "yes"}


def _prompt_choice(
    label: str,
    *,
    choices: Sequence[tuple[str, str]],
    default: str,
) -> str:
    print(label + ":")
    for key, description in choices:
        default_tag = " (default)" if key == default else ""
        print(f"  {key}: {description}{default_tag}")
    response = input("> ").strip().lower()
    if not response:
        return default
    allowed = {key for key, _ in choices}
    if response in allowed:
        return response
    print(f"Unrecognised choice '{response}', using {default}.")
    return default


def _resolve_verify_ssl(
    explicit: bool | None,
    *,
    current: str | None,
    default: bool,
    interactive: bool,
) -> bool:
    if explicit is not None:
        return explicit
    if current is not None:
        return current.strip().lower() in {"1", "true", "yes", "on"}
    if not interactive:
        return default
    return _prompt_confirm("Verify TLS certificates?", default=default)


def _port_from_base_url(base_url: str, *, default: int) -> int:
    tail = base_url.rsplit(":", 1)
    if len(tail) != 2:
        return default
    port_text = tail[1].split("/", 1)[0]
    try:
        return int(port_text)
    except ValueError:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
