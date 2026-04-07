from __future__ import annotations

from fastapi import FastAPI

from vobchat.api.routers import (
    chat_router,
    health_router,
    maps_router,
    metadata_router,
    places_router,
    series_router,
    themes_router,
)
from vobchat.core.llm import log_llm_startup_status
from vobchat.core.logging import configure_enhanced_logging
from vobchat.core.settings import get_settings


def create_app() -> FastAPI:
    configure_enhanced_logging()
    settings = get_settings()
    log_llm_startup_status(component="api")
    app = FastAPI(
        title="VobChat API",
        version="0.4.0",
        debug=settings.debug,
        description="Typed FastAPI surface for the VobChat rewrite path.",
    )
    app.include_router(health_router)
    app.include_router(chat_router)
    app.include_router(places_router)
    app.include_router(themes_router)
    app.include_router(series_router)
    app.include_router(maps_router)
    app.include_router(metadata_router)
    return app


app = create_app()
