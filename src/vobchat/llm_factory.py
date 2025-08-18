"""Centralized ChatOllama factory.

Provides shared, memoized constructors for ChatOllama with consistent
configuration, logging callbacks, and optional JSON-mode.

Use get_llm(json_mode=True) when you expect JSON output (e.g., with
LangChain structured outputs or JSON-only prompts).
"""

from __future__ import annotations

from typing import Dict, Tuple, Optional
import os

from langchain_ollama import ChatOllama

from .configure_logging import get_llm_callback


def _build_base_url() -> str:
    host = os.getenv("OLLAMA_HOST", "localhost")
    port = os.getenv("OLLAMA_PORT", "11434")
    subpath = os.getenv("OLLAMA_SUBPATH", "").strip("/")
    use_ssl = os.getenv("OLLAMA_USE_SSL", "true").lower() == "true"
    protocol = "https" if use_ssl else "http"
    if subpath:
        return f"{protocol}://{host}:{port}/{subpath}"
    return f"{protocol}://{host}:{port}"


_MODEL_NAME = os.getenv("VOBCHAT_LLM_MODEL", "deepseek-r1-wt:latest")
_MODEL_TEMP_RAW = os.getenv("VOBCHAT_LLM_TEMP", "0.7")
try:
    _MODEL_TEMP = float(_MODEL_TEMP_RAW)  # env might be str
except Exception:
    _MODEL_TEMP = 0.7

_REASONING_ENV = os.getenv("VOBCHAT_OLLAMA_REASONING", "true").lower() == "true"

# Memoized instances keyed by (json_mode, reasoning)
_CACHE: Dict[Tuple[bool, bool], ChatOllama] = {}


def get_llm(*, json_mode: bool = False, reasoning: Optional[bool] = None) -> ChatOllama:
    """Return a shared ChatOllama configured from environment.

    Args:
        json_mode: If True, pass format="json" to ChatOllama for JSON outputs.
        reasoning: Override reasoning flag; defaults to VOBCHAT_OLLAMA_REASONING.
    """
    use_reasoning = _REASONING_ENV if reasoning is None else bool(reasoning)
    key = (bool(json_mode), use_reasoning)
    if key in _CACHE:
        return _CACHE[key]

    base_url = _build_base_url()
    kwargs = {
        "model": _MODEL_NAME,
        "base_url": base_url,
        "temperature": _MODEL_TEMP,
        "client_kwargs": {"verify": False},
        "callbacks": [get_llm_callback()],
    }
    if json_mode:
        kwargs["format"] = "json"
    # Only set reasoning if supported and desired
    try:
        kwargs["reasoning"] = use_reasoning
    except Exception:
        pass

    llm = ChatOllama(**kwargs)  # type: ignore[arg-type]
    _CACHE[key] = llm
    return llm

