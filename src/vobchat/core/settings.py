from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path


EnvMapping = Mapping[str, str]


def _get_str(env: EnvMapping, name: str, default: str = "") -> str:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip()


def _get_bool(env: EnvMapping, name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(env: EnvMapping, name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(env: EnvMapping, name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _normalize_dash_base(raw: str | None) -> str:
    base = (raw or "/").strip()
    if base in {"", "/"}:
        return "/"
    if not base.startswith("/"):
        base = f"/{base}"
    if not base.endswith("/"):
        base = f"{base}/"
    return base


def _normalize_openai_base_url(raw: str) -> str:
    base = raw.strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/v1"):
        return base
    return f"{base}/v1"


def _default_auth_database_url() -> str:
    if Path("/app/data").exists():
        return "sqlite:////app/data/users.db"

    instance_dir = Path.cwd() / "instance"
    instance_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{instance_dir / 'users.db'}"


@dataclass(frozen=True)
class DashSettings:
    url_base_pathname: str
    route_prefix: str
    debug: bool
    host: str
    port: int


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    user: str
    password: str
    name: str
    port: int
    schema: str
    driver: str
    read_only: bool
    connect_timeout: int
    application_name: str
    statement_timeout_ms: int
    idle_in_transaction_session_timeout_ms: int
    pool_pre_ping: bool

    @property
    def uri(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
            f"?connect_timeout={self.connect_timeout}"
            f"&application_name={self.application_name}"
        )

    @property
    def sqlalchemy_uri(self) -> str:
        return (
            f"postgresql+{self.driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
            f"?connect_timeout={self.connect_timeout}"
            f"&application_name={self.application_name}"
        )


@dataclass(frozen=True)
class AuthSettings:
    secret_key: str
    database_url: str
    session_cookie_secure: bool
    session_cookie_httponly: bool
    session_cookie_samesite: str
    wtf_csrf_enabled: bool


@dataclass(frozen=True)
class LLMSettings:
    provider: str
    openai_base_url: str
    model: str
    api_key: str
    temperature: float
    timeout_seconds: int
    verify_ssl: bool

    @property
    def configured(self) -> bool:
        return bool(self.openai_base_url and self.model)


@dataclass(frozen=True)
class LoggingSettings:
    level: str
    llm_level: str
    log_dir: Path


@dataclass(frozen=True)
class APISettings:
    internal_base_url: str


@dataclass(frozen=True)
class AppSettings:
    dash: DashSettings
    database: DatabaseSettings
    auth: AuthSettings
    llm: LLMSettings
    logging: LoggingSettings
    api: APISettings
    debug: bool


def build_settings(env: EnvMapping | None = None) -> AppSettings:
    source = env or os.environ

    dash_base = _normalize_dash_base(source.get("DASH_URL_BASE_PATHNAME") or "/")
    route_prefix = "" if dash_base == "/" else dash_base.rstrip("/")

    auth_db_url = (
        source.get("AUTH_DATABASE_URL")
        or source.get("DATABASE_URL")
        or _default_auth_database_url()
    )

    log_dir = Path(_get_str(source, "VOBCHAT_LOG_DIR", "logs"))

    dash = DashSettings(
        url_base_pathname=dash_base,
        route_prefix=route_prefix,
        debug=_get_bool(source, "DASH_DEBUG", False),
        host=_get_str(source, "DASH_HOST", "0.0.0.0"),
        port=_get_int(source, "DASH_PORT", 8050),
    )
    database = DatabaseSettings(
        host=_get_str(source, "DB_HOST", "localhost"),
        user=_get_str(source, "DB_USER", "postgres"),
        password=(
            source.get("DB_PASSWORD")
            or source.get("POSTGRES_PASS")
            or source.get("VOB_PASS")
            or ""
        ),
        name=_get_str(source, "DB_NAME", "vobchat"),
        port=_get_int(source, "DB_PORT", 5432),
        schema=_get_str(source, "DB_SCHEMA", "hgis"),
        driver=_get_str(source, "DB_DRIVER", "psycopg"),
        read_only=_get_bool(source, "DB_READ_ONLY", True),
        connect_timeout=_get_int(source, "DB_CONNECT_TIMEOUT", 10),
        application_name=_get_str(source, "DB_APPLICATION_NAME", "vobchat"),
        statement_timeout_ms=_get_int(source, "DB_STATEMENT_TIMEOUT_MS", 30000),
        idle_in_transaction_session_timeout_ms=_get_int(
            source,
            "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
            60000,
        ),
        pool_pre_ping=_get_bool(source, "DB_POOL_PRE_PING", True),
    )
    auth = AuthSettings(
        secret_key=_get_str(source, "SECRET_KEY", "dev-secret-key"),
        database_url=auth_db_url,
        session_cookie_secure=_get_bool(source, "SESSION_COOKIE_SECURE", True),
        session_cookie_httponly=_get_bool(source, "SESSION_COOKIE_HTTPONLY", True),
        session_cookie_samesite=_get_str(source, "SESSION_COOKIE_SAMESITE", "Lax"),
        wtf_csrf_enabled=_get_bool(source, "WTF_CSRF_ENABLED", True),
    )
    llm = LLMSettings(
        provider=_get_str(source, "LLM_PROVIDER", "openai-compatible") or "openai-compatible",
        openai_base_url=_normalize_openai_base_url(
            _get_str(source, "LLM_OPENAI_BASE_URL", "")
        ),
        model=_get_str(source, "LLM_MODEL", ""),
        api_key=_get_str(source, "LLM_API_KEY", ""),
        temperature=_get_float(source, "LLM_TEMPERATURE", 0.7),
        timeout_seconds=_get_int(source, "LLM_TIMEOUT_SECONDS", 60),
        verify_ssl=_get_bool(source, "LLM_VERIFY_SSL", True),
    )
    logging = LoggingSettings(
        level=_get_str(source, "VOBCHAT_LOG_LEVEL", "INFO").upper(),
        llm_level=_get_str(source, "VOBCHAT_LLM_LOG_LEVEL", "INFO").upper(),
        log_dir=log_dir,
    )
    api = APISettings(
        internal_base_url=_get_str(source, "VOBCHAT_API_BASE_URL", "http://api:8000")
    )

    return AppSettings(
        dash=dash,
        database=database,
        auth=auth,
        llm=llm,
        logging=logging,
        api=api,
        debug=dash.debug,
    )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return build_settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
