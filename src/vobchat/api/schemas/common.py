from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class APIModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class HealthResponse(APIModel):
    status: str = Field(examples=["ok"])
    service: str = Field(examples=["api"])
    details: dict[str, str] = Field(default_factory=dict)


def parse_multi_value(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, dict)):
        out: list[Any] = []
        for item in value:
            out.extend(parse_multi_value(item))
        return out
    return [value]
