from __future__ import annotations

from functools import lru_cache
import logging
from typing import Any

from sqlalchemy import URL, create_engine, event
from sqlalchemy.engine import Engine

from vobchat.core.settings import AppSettings, DatabaseSettings, get_settings


logger = logging.getLogger(__name__)


def build_database_url(
    database_settings: DatabaseSettings | None = None,
) -> URL:
    settings = database_settings or get_settings().database
    return URL.create(
        drivername=f"postgresql+{settings.driver}",
        username=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
        database=settings.name,
        query={
            "application_name": settings.application_name,
            "connect_timeout": str(settings.connect_timeout),
        },
    )


def _apply_session_settings(
    dbapi_connection: Any,
    database_settings: DatabaseSettings,
) -> None:
    statements = []
    if database_settings.read_only:
        statements.append("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
    statements.extend(
        [
            f"SET statement_timeout = '{database_settings.statement_timeout_ms}ms'",
            (
                "SET idle_in_transaction_session_timeout = "
                f"'{database_settings.idle_in_transaction_session_timeout_ms}ms'"
            ),
        ]
    )

    try:
        previous_autocommit = getattr(dbapi_connection, "autocommit", None)
        if previous_autocommit is not None:
            dbapi_connection.autocommit = True
        with dbapi_connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        if previous_autocommit is not None:
            dbapi_connection.autocommit = previous_autocommit
    except Exception as exc:
        logger.warning("Failed to apply DB session settings: %s", exc)


def create_db_engine(settings: AppSettings | None = None) -> Engine:
    app_settings = settings or get_settings()
    engine = create_engine(
        build_database_url(app_settings.database),
        pool_pre_ping=app_settings.database.pool_pre_ping,
        future=True,
        echo=app_settings.debug,
    )

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, _connection_record):
        _apply_session_settings(dbapi_connection, app_settings.database)

    return engine


@lru_cache(maxsize=1)
def get_db_engine() -> Engine:
    return create_db_engine()


def reset_db_engine_cache() -> None:
    if get_db_engine.cache_info().currsize:
        try:
            get_db_engine().dispose()
        except Exception:
            logger.debug("Failed to dispose cached DB engine", exc_info=True)
    get_db_engine.cache_clear()
