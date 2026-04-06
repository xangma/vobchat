from vobchat.api.schemas.common import HealthResponse
from vobchat.core.settings import get_settings


def get_health_response() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        service="api",
        details={"dash_base_path": settings.dash.url_base_pathname},
    )


def get_readiness_response() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ready",
        service="api",
        details={
            "config": "loaded",
            "auth_db_url": settings.auth.database_url,
        },
    )
