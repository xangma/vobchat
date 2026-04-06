from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, Iterator, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from vobchat.db.engine import get_db_engine


class QueryExecutor(Protocol):
    def fetch_all(
        self,
        statement: Any,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...

    def fetch_one(
        self,
        statement: Any,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None: ...


@contextmanager
def get_connection(engine: Engine | None = None) -> Iterator[Connection]:
    active_engine = engine or get_db_engine()
    with active_engine.connect() as connection:
        yield connection


@contextmanager
def get_readonly_connection(engine: Engine | None = None) -> Iterator[Connection]:
    active_engine = engine or get_db_engine()
    with active_engine.connect() as connection:
        with connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            yield connection


class DatabaseExecutor:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine or get_db_engine()

    def fetch_all(
        self,
        statement: Any,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with get_readonly_connection(self.engine) as connection:
            result = connection.execute(statement, dict(params or {}))
            return [dict(row) for row in result.mappings().all()]

    def fetch_one(
        self,
        statement: Any,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        rows = self.fetch_all(statement, params)
        return rows[0] if rows else None
