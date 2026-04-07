from __future__ import annotations

import io
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from vobchat.core.settings import get_settings

# Optional imports (kept lazy-safe in code below)
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore


# -----------------------------------------------------------------------------
# Formatters
# -----------------------------------------------------------------------------


class PrettyFormatter(logging.Formatter):
    """Human-friendly formatter that handles rich objects without noisy output."""

    PLAIN_FMT = "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s"

    def __init__(self, datefmt: str = "%Y-%m-%d %H:%M:%S") -> None:
        # Use UTC consistently
        super().__init__(datefmt=datefmt)
        self.converter = time.gmtime  # type: ignore[attr-defined]
        self._plain = logging.Formatter(self.PLAIN_FMT, datefmt=datefmt)
        self._plain.converter = time.gmtime  # type: ignore[attr-defined]

    # ---- helpers ---------------------------------------------------------

    def _format_dataframe(self, df: "pd.DataFrame") -> str:
        try:
            return "\n" + df.to_string(index=True, max_cols=None, max_rows=50)
        except Exception:
            return f"\n{repr(df)}"

    def _format_complex(self, value: Any) -> str:
        try:
            # pandas DataFrame
            if pd is not None and isinstance(value, pd.DataFrame):  # type: ignore[arg-type]
                return self._format_dataframe(value)

            # dict / list -> pretty JSON
            # if isinstance(value, (dict, list)):
            #     return "\n" + json.dumps(value, indent=2, default=str)

            # Long strings on a new line
            if isinstance(value, str) and len(value) > 100:
                return "\n" + value

            return str(value)
        except Exception:
            return repr(value)

    # ---- main ------------------------------------------------------------

    def format(self, record: logging.LogRecord) -> str:
        # Start with the plain line (time, level, logger, file:line)
        base = self._plain.format(record)

        return base


class CompactJSONFormatter(logging.Formatter):
    """
    Minimal JSON line formatter:
    - Single line per record
    - ISO8601 UTC timestamp with 'Z'
    - If message is dict, minified JSON payload; else plain text
    """

    def __init__(self) -> None:
        super().__init__(datefmt="%Y-%m-%dT%H:%M:%S")
        self.converter = time.gmtime  # UTC
        self._plain = logging.Formatter(
            "%(asctime)sZ [%(levelname)s] %(name)s - %(message)s"
        )
        self._plain.converter = time.gmtime  # type: ignore[attr-defined]

    def format(self, record: logging.LogRecord) -> str:
        try:
            if isinstance(record.msg, dict):
                ts = self.formatTime(record, self.datefmt)
                head = f"{ts}Z [{record.levelname}] {record.name} - "
                return head + json.dumps(record.msg, separators=(",", ":"), default=str)
            return self._plain.format(record)
        except Exception:
            return self._plain.format(record)


# -----------------------------------------------------------------------------
# Handlers
# -----------------------------------------------------------------------------


class EnhancedLogHandler(logging.Handler):
    """In-memory buffer to read logs programmatically; rotates to cap memory."""

    def __init__(
        self, level: int = logging.NOTSET, max_buffer_size: int = 1_000_000
    ) -> None:
        super().__init__(level)
        self.log_buffer = io.StringIO()
        self.setFormatter(PrettyFormatter())
        self.max_buffer_size = max_buffer_size

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            # Rotate *before* writing if we’re at/over cap
            if self.log_buffer.tell() >= self.max_buffer_size:
                self.rotate_buffer()
            self.log_buffer.write(msg + "\n")
        except Exception as e:
            try:
                self.log_buffer.write(f"Error formatting log: {e}\n")
            except Exception:
                pass

    def rotate_buffer(self) -> None:
        """Keep last 75% to preserve most recent context."""
        content = self.log_buffer.getvalue()
        keep_from = len(content) // 4
        self.log_buffer.seek(0)
        self.log_buffer.truncate(0)
        self.log_buffer.write(content[keep_from:])

    def get_logs(self) -> str:
        return self.log_buffer.getvalue()

    def clear_logs(self) -> None:
        self.log_buffer.seek(0)
        self.log_buffer.truncate(0)


# -----------------------------------------------------------------------------
# Public configuration
# -----------------------------------------------------------------------------

_configured_once = False  # simple guard to prevent accidental double-config


def configure_enhanced_logging() -> logging.Logger:
    """Configure root + dedicated LLM logger with sane defaults."""
    global _configured_once
    logger = logging.getLogger()
    settings = get_settings()

    # Respect centralized settings; default INFO
    level_name = settings.logging.level
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    # Clear existing only on first configure to avoid duplicate streams in hot-reloads
    if not _configured_once:
        for h in logger.handlers[:]:
            logger.removeHandler(h)

    # Buffer handler for programmatic access
    buffer_handler = EnhancedLogHandler()
    logger.addHandler(buffer_handler)

    # Console (Pretty)
    console = logging.StreamHandler()
    console.setFormatter(PrettyFormatter())
    logger.addHandler(console)

    # File (backend)
    backend_log_path = Path(settings.logging.log_dir) / "backend.log"
    try:
        backend_log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            backend_log_path, mode="w", encoding="utf-8"
        )
        file_handler.setFormatter(PrettyFormatter())
        logger.addHandler(file_handler)
        print(f"Backend logging configured to: {backend_log_path}")
    except Exception as e:
        print(f"Failed to setup backend log file: {e}")

    # Dedicated LLM logger (JSONL)
    try:
        llm_logger = logging.getLogger("vobchat.llm")
        llm_level_name = settings.logging.llm_level
        llm_level = getattr(logging, llm_level_name, logging.INFO)
        llm_logger.setLevel(llm_level)
        llm_logger.propagate = False  # keep it out of root

        llm_log_path = Path(settings.logging.log_dir) / "llm_debug.log"
        llm_log_path.parent.mkdir(parents=True, exist_ok=True)

        llm_file = logging.FileHandler(llm_log_path, mode="w", encoding="utf-8")
        llm_file.setFormatter(CompactJSONFormatter())

        # refresh handlers to avoid duplication on re-config
        for h in llm_logger.handlers[:]:
            llm_logger.removeHandler(h)
        llm_logger.addHandler(llm_file)

        print(f"LLM debug logging configured to: {llm_log_path}")
        llm_logger.debug({"event": "llm_logger_initialized", "path": llm_log_path})

        # One-shot session context
        llm_logger.debug(
            {
                "event": "llm_session_start",
                "provider": settings.llm.provider,
                "model": settings.llm.model,
                "temperature": settings.llm.temperature,
                "endpoint": {
                    "base_url": settings.llm.openai_base_url,
                    "verify_ssl": settings.llm.verify_ssl,
                },
            }
        )
    except Exception as e:
        print(f"Failed to setup LLM log file: {e}")

    # Tame noisy libs
    for name in [
        "asyncio",  # we’ll set to ERROR below
        "httpcore",
        "httpcore.http11",
        "httpcore._trace",
        "httpx",
        "urllib3",
        "hpack",
        "h11",
        "anyio",
        "websockets",
        "werkzeug",
    ]:
        try:
            logging.getLogger(name).setLevel(logging.ERROR)
        except Exception:
            pass

    # asyncio warnings & level
    logging.getLogger("asyncio").setLevel(logging.ERROR)

    # Dedicated model logger verbosity (env override)
    llm_level_name = settings.logging.llm_level
    llm_level = getattr(logging, llm_level_name, logging.INFO)
    logging.getLogger("vobchat.llm").setLevel(llm_level)

    # Suppress specific resource cleanup warnings
    import warnings

    warnings.filterwarnings("ignore", message=".*I/O operation on closed.*")
    warnings.filterwarnings("ignore", category=ResourceWarning, module="asyncio")

    _configured_once = True
    return logger


# -----------------------------------------------------------------------------
# LLM logging helpers
# -----------------------------------------------------------------------------


def get_llm_logger() -> logging.Logger:
    return logging.getLogger("vobchat.llm")


def serialize_chat_messages(messages: List[Any]) -> List[Dict[str, Any]]:
    """Best-effort serialization of chat messages (role/type + content only)."""
    out: List[Dict[str, Any]] = []
    if not messages:
        return out

    if (
        isinstance(messages, (list, tuple))
        and messages
        and isinstance(messages[0], (list, tuple))
    ):
        flat: List[Any] = []
        for sub in messages:
            flat.extend(list(sub))
    else:
        flat = list(messages)

    for m in flat:
        try:
            role = (
                getattr(m, "type", None)
                or getattr(m, "role", None)
                or m.__class__.__name__
            )
            content = getattr(m, "content", None)
            if content is None and isinstance(m, dict):
                role = m.get("role") or role
                content = m.get("content")
            out.append({"role": role, "content": content})
        except Exception:
            out.append({"item": str(m)})
    return out


def log_llm_interaction(
    *,
    name: str,
    prompt_vars: Optional[Dict[str, Any]] = None,
    formatted_messages: Optional[List[Any]] = None,
    output: Optional[Any] = None,
    reasoning: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Structured single-event LLM interaction log (compact + JSONL-friendly)."""
    logger = get_llm_logger()
    try:
        payload: Dict[str, Any] = {"event": "llm_interaction", "name": name}
        if prompt_vars is not None:
            payload["prompt_vars"] = prompt_vars
        if formatted_messages is not None:
            payload["messages"] = serialize_chat_messages(formatted_messages)
        if output is not None:
            try:
                payload["output"] = (
                    output.model_dump() if hasattr(output, "model_dump") else output
                )
            except Exception:
                payload["output"] = str(output)
        if reasoning is not None:
            payload["reasoning"] = reasoning
        if extra:
            payload["extra"] = extra
        logger.debug(payload)
    except Exception:
        try:
            logger.debug(
                {"event": "llm_interaction", "name": name, "error": "failed to log"}
            )
        except Exception:
            pass
