from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
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

    def as_legacy_config(self) -> dict[str, str | int]:
        return {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "dbname": self.name,
            "port": self.port,
            "schema": self.schema,
        }

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
class OllamaSettings:
    host: str
    port: int
    subpath: str
    use_ssl: bool
    model: str
    api_key: str
    temperature: float
    reasoning: bool
    request_timeout_seconds: int

    @property
    def base_url(self) -> str:
        protocol = "https" if self.use_ssl else "http"
        subpath = self.subpath.strip("/")
        if subpath:
            return f"{protocol}://{self.host}:{self.port}/{subpath}"
        return f"{protocol}://{self.host}:{self.port}"

    @property
    def openai_base_url(self) -> str:
        base = self.base_url.rstrip("/")
        if base.endswith("/v1"):
            return base
        return f"{base}/v1"


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
    ollama: OllamaSettings
    logging: LoggingSettings
    api: APISettings
    debug: bool


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    dash_base = _normalize_dash_base(os.getenv("DASH_URL_BASE_PATHNAME") or "/")
    route_prefix = "" if dash_base == "/" else dash_base.rstrip("/")

    temperature_raw = os.getenv("VOBCHAT_LLM_TEMP", "0.7")
    try:
        temperature = float(temperature_raw)
    except ValueError:
        temperature = 0.7

    auth_db_url = (
        os.getenv("AUTH_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or _default_auth_database_url()
    )

    log_dir = Path(os.getenv("VOBCHAT_LOG_DIR", "logs"))

    dash = DashSettings(
        url_base_pathname=dash_base,
        route_prefix=route_prefix,
        debug=_get_bool("DASH_DEBUG", False),
        host=os.getenv("DASH_HOST", "0.0.0.0"),
        port=_get_int("DASH_PORT", 8050),
    )
    database = DatabaseSettings(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "postgres"),
        password=(
            os.getenv("DB_PASSWORD")
            or os.getenv("POSTGRES_PASS")
            or os.getenv("VOB_PASS")
            or ""
        ),
        name=os.getenv("DB_NAME", "vobchat"),
        port=_get_int("DB_PORT", 5432),
        schema=os.getenv("DB_SCHEMA", "hgis"),
        driver=os.getenv("DB_DRIVER", "psycopg"),
        read_only=_get_bool("DB_READ_ONLY", True),
        connect_timeout=_get_int("DB_CONNECT_TIMEOUT", 10),
        application_name=os.getenv("DB_APPLICATION_NAME", "vobchat"),
        statement_timeout_ms=_get_int("DB_STATEMENT_TIMEOUT_MS", 30000),
        idle_in_transaction_session_timeout_ms=_get_int(
            "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", 60000
        ),
        pool_pre_ping=_get_bool("DB_POOL_PRE_PING", True),
    )
    auth = AuthSettings(
        secret_key=os.getenv("SECRET_KEY", "dev-secret-key"),
        database_url=auth_db_url,
        session_cookie_secure=_get_bool("SESSION_COOKIE_SECURE", True),
        session_cookie_httponly=_get_bool("SESSION_COOKIE_HTTPONLY", True),
        session_cookie_samesite=os.getenv("SESSION_COOKIE_SAMESITE", "Lax"),
        wtf_csrf_enabled=_get_bool("WTF_CSRF_ENABLED", True),
    )
    ollama = OllamaSettings(
        host=os.getenv("OLLAMA_HOST", "localhost"),
        port=_get_int("OLLAMA_PORT", 11434),
        subpath=os.getenv("OLLAMA_SUBPATH", ""),
        use_ssl=_get_bool("OLLAMA_USE_SSL", True),
        model=os.getenv("VOBCHAT_LLM_MODEL", "deepseek-r1-wt:latest"),
        api_key=(
            os.getenv("VOBCHAT_OPENAI_API_KEY")
            or os.getenv("OLLAMA_API_KEY")
            or "ollama"
        ),
        temperature=temperature,
        reasoning=_get_bool("VOBCHAT_OLLAMA_REASONING", True),
        request_timeout_seconds=_get_int("VOBCHAT_LLM_TIMEOUT_SECONDS", 60),
    )
    logging = LoggingSettings(
        level=(os.getenv("VOBCHAT_LOG_LEVEL") or "INFO").upper(),
        llm_level=(
            os.getenv("VOBCHAT_LLM_LOG_LEVEL")
            or os.getenv("VOBCHAT_LANGCHAIN_LOG_LEVEL")
            or "INFO"
        ).upper(),
        log_dir=log_dir,
    )
    api = APISettings(
        internal_base_url=os.getenv("VOBCHAT_API_BASE_URL", "http://api:8000")
    )

    return AppSettings(
        dash=dash,
        database=database,
        auth=auth,
        ollama=ollama,
        logging=logging,
        api=api,
        debug=dash.debug,
    )


def reset_settings_cache() -> None:
    get_settings.cache_clear()
