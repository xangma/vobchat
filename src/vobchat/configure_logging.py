# src/vobchat/configure_logging.py
from __future__ import annotations

import io
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

# Optional imports (kept lazy-safe in code below)
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    from langchain_core.messages.base import BaseMessage  # type: ignore
except Exception:  # pragma: no cover
    BaseMessage = None  # type: ignore


# -----------------------------------------------------------------------------
# Formatters
# -----------------------------------------------------------------------------


class PrettyFormatter(logging.Formatter):
    """Human-friendly formatter that handles dicts/lists/DataFrames/LangChain messages."""

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

    def _format_langchain_message(self, msg: "BaseMessage") -> Dict[str, Any]:
        out = {"type": msg.__class__.__name__}
        # Best-effort to keep it small
        try:
            out["content"] = getattr(msg, "content", None)
            out["id"] = getattr(msg, "id", None)
        except Exception:
            pass
        return out

    def _format_complex(self, value: Any) -> str:
        try:
            # pandas DataFrame
            if pd is not None and isinstance(value, pd.DataFrame):  # type: ignore[arg-type]
                return self._format_dataframe(value)

            # LangChain BaseMessage
            if BaseMessage is not None and isinstance(value, BaseMessage):  # type: ignore[arg-type]
                return "\n" + json.dumps(
                    self._format_langchain_message(value), indent=2, default=str
                )

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

        # If message itself is complex, append a pretty rendering
        # msg = record.msg
        # if (
        #     isinstance(msg, (dict, list))
        #     or (pd is not None and isinstance(msg, getattr(pd, "DataFrame", tuple())))
        #     or (
        #         BaseMessage is not None and isinstance(msg, BaseMessage)  # type: ignore[arg-type]
        #     )
        # ):
        #     return base + self._format_complex(msg)

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

    # Respect env override; default INFO
    level_name = (os.getenv("VOBCHAT_LOG_LEVEL") or "INFO").upper()
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
    backend_log_path = "logs/backend.log"
    try:
        os.makedirs(os.path.dirname(backend_log_path), exist_ok=True)
        file_handler = logging.FileHandler(backend_log_path, mode="w", encoding="utf-8")
        file_handler.setFormatter(PrettyFormatter())
        logger.addHandler(file_handler)
        print(f"Backend logging configured to: {backend_log_path}")
    except Exception as e:
        print(f"Failed to setup backend log file: {e}")

    # Dedicated LLM logger (JSONL)
    try:
        llm_logger = logging.getLogger("vobchat.llm")
        lc_level_name = (os.getenv("VOBCHAT_LANGCHAIN_LOG_LEVEL") or "INFO").upper()
        lc_level = getattr(logging, lc_level_name, logging.INFO)
        llm_logger.setLevel(lc_level)
        llm_logger.propagate = False  # keep it out of root

        llm_log_path = "logs/llm_debug.log"
        os.makedirs(os.path.dirname(llm_log_path), exist_ok=True)

        llm_file = logging.FileHandler(llm_log_path, mode="w", encoding="utf-8")
        llm_file.setFormatter(CompactJSONFormatter())

        # refresh handlers to avoid duplication on re-config
        for h in llm_logger.handlers[:]:
            llm_logger.removeHandler(h)
        llm_logger.addHandler(llm_file)

        print(f"LLM debug logging configured to: {llm_log_path}")
        llm_logger.debug({"event": "llm_logger_initialized", "path": llm_log_path})

        # One-shot session context
        host = os.getenv("OLLAMA_HOST", "localhost")
        port = os.getenv("OLLAMA_PORT", "11434")
        sub = os.getenv("OLLAMA_SUBPATH", "")
        use_ssl = os.getenv("OLLAMA_USE_SSL", "true")
        model = os.getenv("VOBCHAT_LLM_MODEL", "deepseek-r1-wt:latest")
        temp = os.getenv("VOBCHAT_LLM_TEMP", "0.7")
        reasoning = os.getenv("VOBCHAT_OLLAMA_REASONING", "true")
        llm_logger.debug(
            {
                "event": "llm_session_start",
                "model": model,
                "temperature": temp,
                "reasoning": reasoning,
                "endpoint": {
                    "host": host,
                    "port": port,
                    "subpath": sub,
                    "ssl": use_ssl,
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

    # LangChain verbosity (env override)
    lc_level_name = (os.getenv("VOBCHAT_LANGCHAIN_LOG_LEVEL") or "INFO").upper()
    lc_level = getattr(logging, lc_level_name, logging.INFO)
    logging.getLogger("langchain").setLevel(lc_level)
    logging.getLogger("langchain_core").setLevel(lc_level)

    # Suppress specific resource cleanup warnings
    import warnings

    warnings.filterwarnings("ignore", message=".*I/O operation on closed.*")
    warnings.filterwarnings("ignore", category=ResourceWarning, module="asyncio")

    _configured_once = True
    return logger


# -----------------------------------------------------------------------------
# High-level helpers
# -----------------------------------------------------------------------------


def log_workflow_response(logger: logging.Logger, response: Dict[str, Any]) -> None:
    """Emit a compact, structured workflow response."""
    logger.debug(
        {
            "workflow_status": "Response received",
            "messages": response.get("messages", []),
            "extracted_data": {
                k: v
                for k, v in response.items()
                if k != "messages" and not str(k).startswith("_")
            },
        }
    )


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

    # LangChain sometimes gives list[list[message]]
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


# -----------------------------------------------------------------------------
# LangChain callback (compact)
# -----------------------------------------------------------------------------

try:
    from langchain_core.callbacks import BaseCallbackHandler  # type: ignore
except Exception:  # pragma: no cover
    BaseCallbackHandler = object  # type: ignore


class LLMLogCallback(BaseCallbackHandler):  # type: ignore
    """Logs prompts and outputs compactly; avoids per-token spam."""

    def __init__(self) -> None:
        self._logger = get_llm_logger()

    @staticmethod
    def _compact_serialized(
        serialized: Any, meta: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        safe: Dict[str, Any] = {}
        try:
            meta = meta or {}
            for k_src, k_dst in [
                ("ls_provider", "provider"),
                ("ls_model_name", "model"),
                ("ls_model_type", "model_type"),
                ("ls_temperature", "temperature"),
            ]:
                if meta.get(k_src) is not None:
                    safe[k_dst] = meta.get(k_src)
            if isinstance(serialized, dict):
                if serialized.get("name"):
                    safe["name"] = serialized["name"]
                if serialized.get("id"):
                    safe["id"] = serialized["id"]
        except Exception:
            pass
        return safe

    @staticmethod
    def _extract_text(response: Any) -> Optional[str]:
        try:
            if hasattr(response, "content") and isinstance(response.content, str):
                return json.dumps(response.content)
            if hasattr(response, "message") and getattr(
                response.message, "content", None
            ):
                return json.dumps(response.message.content)  # type: ignore[attr-defined]
            gens = getattr(response, "generations", None)
            if gens and isinstance(gens, (list, tuple)) and gens:
                first = gens[0][0] if isinstance(gens[0], (list, tuple)) else gens[0]
                if hasattr(first, "text") and isinstance(first.text, str):
                    return first.text
                msg = getattr(first, "message", None)
                if msg is not None and getattr(msg, "content", None):
                    return json.dumps(msg.content)
        except Exception:
            return None
        return None

    @staticmethod
    def _truncate(text: str, max_len: int = 2000) -> str:
        return text if len(text) <= max_len else text[: max_len - 3] + "..."

    # --- Chat model lifecycle -------------------------------------------

    def on_chat_model_start(self, serialized, messages, **kwargs):  # type: ignore
        try:
            meta = kwargs.get("metadata")
            payload = {
                "event": "on_chat_model_start",
                "model": self._compact_serialized(serialized, meta),
                "messages": serialize_chat_messages(messages),
                "tags": kwargs.get("tags"),
            }
            if isinstance(meta, dict):
                compact_meta = {
                    k: v
                    for k, v in meta.items()
                    if str(k).startswith("ls_")
                    or k in {"thread_id", "langgraph_node", "langgraph_step"}
                }
                if compact_meta:
                    payload["metadata"] = compact_meta
            self._logger.debug(payload)
        except Exception:
            pass

    def on_chat_model_stream(self, chunk, **kwargs):  # type: ignore
        return  # keep logs tidy

    def on_chat_model_end(self, response, **kwargs):  # type: ignore
        try:
            out_text = self._extract_text(response) or str(response)
            payload = {
                "event": "on_chat_model_end",
                "output": self._truncate(out_text),
                "tags": kwargs.get("tags"),
            }
            meta = kwargs.get("metadata")
            if isinstance(meta, dict):
                compact_meta = {
                    k: v
                    for k, v in meta.items()
                    if str(k).startswith("ls_")
                    or k in {"thread_id", "langgraph_node", "langgraph_step"}
                }
                if compact_meta:
                    payload["metadata"] = compact_meta
            self._logger.debug(payload)
        except Exception:
            pass

    # Fallback for LLM-level callbacks
    def on_llm_start(self, serialized, prompts, **kwargs):  # type: ignore
        try:
            meta = kwargs.get("metadata")
            payload = {
                "event": "on_llm_start",
                "model": self._compact_serialized(serialized, meta),
                "prompt_count": len(prompts) if hasattr(prompts, "__len__") else None,
                "tags": kwargs.get("tags"),
            }
            if isinstance(meta, dict):
                compact_meta = {
                    k: v
                    for k, v in meta.items()
                    if str(k).startswith("ls_")
                    or k in {"thread_id", "langgraph_node", "langgraph_step"}
                }
                if compact_meta:
                    payload["metadata"] = compact_meta
            self._logger.debug(payload)
        except Exception:
            pass

    def on_llm_new_token(self, token: str, **kwargs):  # type: ignore
        return  # no per-token spam

    def on_llm_end(self, response, **kwargs):  # type: ignore
        try:
            out_text = self._extract_text(response) or str(response)
            payload = {
                "event": "on_llm_end",
                "output": self._truncate(out_text),
                "tags": kwargs.get("tags"),
            }
            meta = kwargs.get("metadata")
            if isinstance(meta, dict):
                compact_meta = {
                    k: v
                    for k, v in meta.items()
                    if str(k).startswith("ls_")
                    or k in {"thread_id", "langgraph_node", "langgraph_step"}
                }
                if compact_meta:
                    payload["metadata"] = compact_meta
            self._logger.debug(payload)
        except Exception:
            pass


_LLM_CALLBACK_SINGLETON: Optional[LLMLogCallback] = None


def get_llm_callback() -> LLMLogCallback:
    """Return a singleton instance of the logging callback."""
    global _LLM_CALLBACK_SINGLETON
    if _LLM_CALLBACK_SINGLETON is None:
        _LLM_CALLBACK_SINGLETON = LLMLogCallback()
    return _LLM_CALLBACK_SINGLETON
