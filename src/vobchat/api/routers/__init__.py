from vobchat.api.routers.chat import router as chat_router
from vobchat.api.routers.health import router as health_router
from vobchat.api.routers.maps import router as maps_router
from vobchat.api.routers.metadata import router as metadata_router
from vobchat.api.routers.places import router as places_router
from vobchat.api.routers.series import router as series_router
from vobchat.api.routers.themes import router as themes_router

__all__ = [
    "chat_router",
    "health_router",
    "maps_router",
    "metadata_router",
    "places_router",
    "series_router",
    "themes_router",
]
