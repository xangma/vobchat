from __future__ import annotations

from collections.abc import Iterable, Sequence
import re
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.sql.elements import TextClause

from vobchat.db.session import DatabaseExecutor, QueryExecutor
from vobchat.utils.constants import UNIT_TYPES


PREFERRED_UNIT_NAME_CTE = """
WITH unit_name AS (
    SELECT
        g_unit,
        g_name,
        ROW_NUMBER() OVER (
            PARTITION BY g_unit
            ORDER BY
                CASE
                    WHEN g_language IS NOT NULL AND g_language = :user_lang THEN 0
                    WHEN g_language = 'eng' THEN 1
                    ELSE 2
                END
        ) AS rn
    FROM hgis.g_name
    WHERE g_name_status = 'P'
)
"""

WAY_MEASUREMENT_RE = re.compile(r".*?_\d*WAY.*$", re.IGNORECASE)


class BaseRepository:
    def __init__(self, executor: QueryExecutor | None = None) -> None:
        self.executor = executor or DatabaseExecutor()


def statement(sql: str, *, expanding_params: Sequence[str] = ()) -> TextClause:
    stmt = text(sql)
    if expanding_params:
        stmt = stmt.bindparams(
            *(bindparam(param_name, expanding=True) for param_name in expanding_params)
        )
    return stmt


def coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text_value = str(value).strip().lower()
    if text_value in {"y", "yes", "true", "1"}:
        return True
    if text_value in {"n", "no", "false", "0"}:
        return False
    return None


def coerce_int_tuple(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, Iterable):
        candidates = list(value)
    else:
        candidates = [value]

    out: list[int] = []
    for candidate in candidates:
        coerced = coerce_int(candidate)
        if coerced is not None and coerced not in out:
            out.append(coerced)
    return tuple(out)


def coerce_str_tuple(
    value: Any,
    *,
    skip_values: set[str] | None = None,
) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, Iterable):
        candidates = list(value)
    else:
        candidates = [value]

    skip = {item.upper() for item in (skip_values or set())}
    out: list[str] = []
    for candidate in candidates:
        coerced = coerce_str(candidate)
        if not coerced:
            continue
        if coerced.upper() in skip:
            continue
        if coerced not in out:
            out.append(coerced)
    return tuple(out)


def normalize_unit_types(unit_types: Sequence[str] | None) -> tuple[str, ...]:
    requested = tuple(unit_types or tuple(UNIT_TYPES.keys()))
    invalid = [unit_type for unit_type in requested if unit_type not in UNIT_TYPES]
    if invalid:
        raise ValueError(f"Unsupported unit type(s): {', '.join(sorted(invalid))}")
    seen: list[str] = []
    for unit_type in requested:
        if unit_type not in seen:
            seen.append(unit_type)
    return tuple(seen)


def timeless_unit_types(unit_types: Sequence[str] | None = None) -> tuple[str, ...]:
    requested = normalize_unit_types(unit_types)
    return tuple(
        unit_type
        for unit_type in requested
        if UNIT_TYPES.get(unit_type, {}).get("timeless")
    )


def parse_category(cell_ref: str | None) -> tuple[str | None, str]:
    if not cell_ref:
        return None, ""
    parts = str(cell_ref).split(":", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, str(cell_ref)
