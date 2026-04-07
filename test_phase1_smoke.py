import importlib
import sys

from fastapi.testclient import TestClient

from vobchat.core.settings import get_settings, reset_settings_cache


def test_settings_load_defaults(monkeypatch):
    for key in [
        "DASH_URL_BASE_PATHNAME",
        "DASH_DEBUG",
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "AUTH_DATABASE_URL",
        "DATABASE_URL",
        "SECRET_KEY",
        "LLM_PROVIDER",
        "LLM_OPENAI_BASE_URL",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_VERIFY_SSL",
    ]:
        monkeypatch.delenv(key, raising=False)

    reset_settings_cache()
    settings = get_settings()

    assert settings.dash.url_base_pathname == "/"
    assert settings.dash.route_prefix == ""
    assert settings.database.port == 5432
    assert settings.auth.secret_key == "dev-secret-key"
    assert settings.llm.provider == "openai-compatible"
    assert settings.llm.openai_base_url == ""
    assert settings.llm.model == ""
    assert settings.llm.verify_ssl is True


def test_fastapi_health_endpoints():
    from vobchat.api.main import create_app

    client = TestClient(create_app())

    health = client.get("/healthz")
    ready = client.get("/readyz")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


def test_web_app_imports_with_workflow_skip(monkeypatch, tmp_path):
    auth_db_path = tmp_path / "users.db"
    monkeypatch.setenv("AUTH_DATABASE_URL", f"sqlite:///{auth_db_path}")
    monkeypatch.setenv("VOBCHAT_SKIP_WORKFLOW_STARTUP", "true")

    reset_settings_cache()
    for module_name in [
        "vobchat.web.app",
        "vobchat.web",
    ]:
        sys.modules.pop(module_name, None)

    module = importlib.import_module("vobchat.web.app")

    assert callable(module.create_app)
    assert module.app is not None
    assert module.server is module.app.server
