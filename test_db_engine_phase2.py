from vobchat.core.settings import get_settings, reset_settings_cache
from vobchat.db.engine import build_database_url, create_db_engine, reset_db_engine_cache


def test_build_database_url_and_engine_use_psycopg(monkeypatch):
    monkeypatch.setenv("DB_HOST", "db.example.test")
    monkeypatch.setenv("DB_PORT", "5544")
    monkeypatch.setenv("DB_NAME", "vision_of_britain")
    monkeypatch.setenv("DB_USER", "readonly_user")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("DB_DRIVER", "psycopg")
    monkeypatch.setenv("DB_APPLICATION_NAME", "vobchat-tests")

    reset_settings_cache()
    reset_db_engine_cache()

    settings = get_settings()
    url = build_database_url(settings.database)

    assert url.drivername == "postgresql+psycopg"
    assert url.host == "db.example.test"
    assert url.port == 5544
    assert url.database == "vision_of_britain"

    engine = create_db_engine()
    try:
        assert engine.url.drivername == "postgresql+psycopg"
        assert engine.url.host == "db.example.test"
        assert engine.url.database == "vision_of_britain"
    finally:
        engine.dispose()
        reset_db_engine_cache()
        reset_settings_cache()
