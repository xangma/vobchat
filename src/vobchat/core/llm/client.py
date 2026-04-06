from __future__ import annotations

from functools import lru_cache
import json
import logging
import re
from typing import Any, AsyncIterator, TypeVar

import httpx

from vobchat.core.logging import get_llm_logger
from vobchat.core.settings import get_settings


logger = logging.getLogger(__name__)
llm_logger = get_llm_logger()

T = TypeVar("T")
LLMMessage = dict[str, str]

_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", re.DOTALL)


class OpenAICompatibleLocalModelClient:
    """Thin OpenAI-compatible client for local model endpoints."""

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str,
        default_temperature: float,
        timeout_seconds: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.default_temperature = default_temperature
        self.timeout_seconds = timeout_seconds
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(timeout_seconds, read=timeout_seconds),
            verify=False,
        )

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
        response = await self._client.post("/chat/completions", json=payload)
        response.raise_for_status()
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
        async with self._client.stream("POST", "/chat/completions", json=payload) as response:
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
                    logger.debug("Skipping non-JSON SSE line from local model: %s", data_text)
                    continue
                delta = self._extract_stream_delta(payload_json)
                if delta:
                    yield delta

    async def aclose(self) -> None:
        await self._client.aclose()

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
        raise ValueError("Local model response did not contain valid JSON")


@lru_cache(maxsize=1)
def get_local_model_client() -> OpenAICompatibleLocalModelClient:
    settings = get_settings()
    return OpenAICompatibleLocalModelClient(
        base_url=settings.ollama.openai_base_url,
        model=settings.ollama.model,
        api_key=settings.ollama.api_key,
        default_temperature=settings.ollama.temperature,
        timeout_seconds=settings.ollama.request_timeout_seconds,
    )
