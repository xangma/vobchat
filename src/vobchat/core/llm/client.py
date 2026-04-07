from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import logging
import re
from typing import Any, AsyncIterator, TypeVar

import httpx

from vobchat.core.logging import get_llm_logger
from vobchat.core.settings import LLMSettings, get_settings


logger = logging.getLogger(__name__)
llm_logger = get_llm_logger()

T = TypeVar("T")
LLMMessage = dict[str, str]

_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", re.DOTALL)
_SETUP_GUIDANCE = (
    "Configure an OpenAI-compatible endpoint with `vobchat setup-llm` "
    "or set LLM_OPENAI_BASE_URL and LLM_MODEL."
)


class LLMClientError(RuntimeError):
    pass


class LLMConfigurationError(LLMClientError):
    pass


class LLMServiceUnavailableError(LLMClientError):
    pass


@dataclass(frozen=True)
class LLMConnectivityStatus:
    provider: str
    base_url: str
    model: str
    configured: bool
    reachable: bool
    model_available: bool | None
    detail: str


def llm_setup_guidance() -> str:
    return _SETUP_GUIDANCE


def _build_headers(settings: LLMSettings) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if settings.api_key:
        headers["Authorization"] = f"Bearer {settings.api_key}"
    return headers


def probe_llm_endpoint(
    settings: LLMSettings | None = None,
    *,
    timeout_seconds: float | None = None,
) -> LLMConnectivityStatus:
    llm_settings = settings or get_settings().llm
    base_url = llm_settings.openai_base_url.rstrip("/")

    if not llm_settings.configured:
        return LLMConnectivityStatus(
            provider=llm_settings.provider,
            base_url=base_url,
            model=llm_settings.model,
            configured=False,
            reachable=False,
            model_available=None,
            detail=f"LLM configuration is incomplete. {llm_setup_guidance()}",
        )

    timeout = timeout_seconds or min(float(llm_settings.timeout_seconds), 5.0)
    try:
        with httpx.Client(
            base_url=base_url,
            headers=_build_headers(llm_settings),
            timeout=timeout,
            verify=llm_settings.verify_ssl,
        ) as client:
            response = client.get("/models")
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        detail = f"LLM endpoint responded with HTTP {status_code}. {llm_setup_guidance()}"
        if status_code in {401, 403}:
            detail = (
                f"LLM endpoint rejected authentication for {base_url}. "
                "Check LLM_API_KEY and endpoint permissions."
            )
        return LLMConnectivityStatus(
            provider=llm_settings.provider,
            base_url=base_url,
            model=llm_settings.model,
            configured=True,
            reachable=False,
            model_available=None,
            detail=detail,
        )
    except httpx.HTTPError as exc:
        return LLMConnectivityStatus(
            provider=llm_settings.provider,
            base_url=base_url,
            model=llm_settings.model,
            configured=True,
            reachable=False,
            model_available=None,
            detail=(
                f"Could not reach the configured LLM endpoint at {base_url}: {exc}. "
                f"{llm_setup_guidance()}"
            ),
        )
    except ValueError as exc:
        return LLMConnectivityStatus(
            provider=llm_settings.provider,
            base_url=base_url,
            model=llm_settings.model,
            configured=True,
            reachable=False,
            model_available=None,
            detail=f"LLM endpoint returned invalid JSON from /models: {exc}",
        )

    models = payload.get("data")
    if isinstance(models, list):
        model_ids = {
            str(item.get("id"))
            for item in models
            if isinstance(item, dict) and item.get("id")
        }
        if model_ids and llm_settings.model not in model_ids:
            return LLMConnectivityStatus(
                provider=llm_settings.provider,
                base_url=base_url,
                model=llm_settings.model,
                configured=True,
                reachable=True,
                model_available=False,
                detail=(
                    f"Endpoint is reachable but model '{llm_settings.model}' was not listed "
                    f"by /models at {base_url}."
                ),
            )

    return LLMConnectivityStatus(
        provider=llm_settings.provider,
        base_url=base_url,
        model=llm_settings.model,
        configured=True,
        reachable=True,
        model_available=True,
        detail=f"Connected to {base_url} and found model '{llm_settings.model}'.",
    )


def log_llm_startup_status(*, component: str) -> LLMConnectivityStatus:
    status = probe_llm_endpoint(timeout_seconds=2.0)
    if not status.configured:
        logger.warning("%s LLM is not configured: %s", component, status.detail)
    elif not status.reachable:
        logger.warning("%s LLM is unavailable: %s", component, status.detail)
    elif status.model_available is False:
        logger.warning("%s LLM model is unavailable: %s", component, status.detail)
    else:
        logger.info(
            "%s LLM ready: provider=%s base_url=%s model=%s",
            component,
            status.provider,
            status.base_url,
            status.model,
        )
    return status


class OpenAICompatibleLLMClient:
    """Thin OpenAI-compatible client for external or local model endpoints."""

    def __init__(
        self,
        *,
        provider: str,
        base_url: str,
        model: str,
        api_key: str,
        default_temperature: float,
        timeout_seconds: int,
        verify_ssl: bool,
    ) -> None:
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.model = model.strip()
        self.api_key = api_key.strip()
        self.default_temperature = default_temperature
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl
        self._client: httpx.AsyncClient | None = None

    def _ensure_configured(self) -> None:
        if self.base_url and self.model:
            return
        raise LLMConfigurationError(
            f"LLM endpoint is not configured. {llm_setup_guidance()}"
        )

    def _get_client(self) -> httpx.AsyncClient:
        self._ensure_configured()
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=_build_headers(
                    LLMSettings(
                        provider=self.provider,
                        openai_base_url=self.base_url,
                        model=self.model,
                        api_key=self.api_key,
                        temperature=self.default_temperature,
                        timeout_seconds=self.timeout_seconds,
                        verify_ssl=self.verify_ssl,
                    )
                ),
                timeout=httpx.Timeout(self.timeout_seconds, read=self.timeout_seconds),
                verify=self.verify_ssl,
            )
        return self._client

    async def complete_text(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        payload = self._build_payload(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )
        try:
            response = await self._get_client().post("/chat/completions", json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise LLMServiceUnavailableError(
                f"Could not reach the configured LLM endpoint at {self.base_url}: {exc}. "
                f"{llm_setup_guidance()}"
            ) from exc
        data = response.json()
        llm_logger.debug({"event": "chat_completion", "payload": payload, "response": data})
        return self._extract_message_text(data)

    async def complete_json(
        self,
        messages: list[LLMMessage],
        response_model: type[T],
        *,
        temperature: float = 0.0,
    ) -> T:
        schema_text = json.dumps(response_model.model_json_schema(), indent=2)
        json_messages = [
            {
                "role": "system",
                "content": (
                    "Return only valid JSON matching this schema. "
                    "Do not wrap the JSON in markdown.\n"
                    f"{schema_text}"
                ),
            },
            *messages,
        ]
        content = await self.complete_text(json_messages, temperature=temperature)
        payload = self._extract_json_payload(content)
        return response_model.model_validate(payload)

    async def stream_text(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> AsyncIterator[str]:
        payload = self._build_payload(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        try:
            async with self._get_client().stream(
                "POST",
                "/chat/completions",
                json=payload,
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data_text = line.removeprefix("data:").strip()
                    if data_text == "[DONE]":
                        break
                    try:
                        payload_json = json.loads(data_text)
                    except json.JSONDecodeError:
                        logger.debug(
                            "Skipping non-JSON SSE line from LLM endpoint: %s",
                            data_text,
                        )
                        continue
                    delta = self._extract_stream_delta(payload_json)
                    if delta:
                        yield delta
        except httpx.HTTPError as exc:
            raise LLMServiceUnavailableError(
                f"Could not reach the configured LLM endpoint at {self.base_url}: {exc}. "
                f"{llm_setup_guidance()}"
            ) from exc

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _build_payload(
        self,
        *,
        messages: list[LLMMessage],
        temperature: float | None,
        max_tokens: int | None,
        stream: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.default_temperature if temperature is None else temperature,
            "stream": stream,
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        return payload

    @staticmethod
    def _extract_message_text(data: dict[str, Any]) -> str:
        choices = data.get("choices") or []
        if not choices:
            return ""
        message = choices[0].get("message") or {}
        content = message.get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                item.get("text", "")
                for item in content
                if isinstance(item, dict)
            )
        return str(content)

    @staticmethod
    def _extract_stream_delta(data: dict[str, Any]) -> str:
        choices = data.get("choices") or []
        if not choices:
            return ""
        delta = choices[0].get("delta") or {}
        content = delta.get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                item.get("text", "")
                for item in content
                if isinstance(item, dict)
            )
        return ""

    @staticmethod
    def _extract_json_payload(content: str) -> Any:
        stripped = content.strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

        block = _JSON_BLOCK_RE.search(content)
        if block:
            return json.loads(block.group(1))

        start_object = stripped.find("{")
        end_object = stripped.rfind("}")
        start_array = stripped.find("[")
        end_array = stripped.rfind("]")
        if start_object != -1 and end_object != -1 and end_object > start_object:
            return json.loads(stripped[start_object : end_object + 1])
        if start_array != -1 and end_array != -1 and end_array > start_array:
            return json.loads(stripped[start_array : end_array + 1])
        raise ValueError("LLM response did not contain valid JSON")


@lru_cache(maxsize=1)
def get_llm_client() -> OpenAICompatibleLLMClient:
    settings = get_settings()
    return OpenAICompatibleLLMClient(
        provider=settings.llm.provider,
        base_url=settings.llm.openai_base_url,
        model=settings.llm.model,
        api_key=settings.llm.api_key,
        default_temperature=settings.llm.temperature,
        timeout_seconds=settings.llm.timeout_seconds,
        verify_ssl=settings.llm.verify_ssl,
    )
