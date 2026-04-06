from fastapi import APIRouter

from vobchat.api.schemas.common import HealthResponse
from vobchat.api.services.health import get_health_response, get_readiness_response


router = APIRouter()


@router.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    return get_health_response()


@router.get("/readyz", response_model=HealthResponse)
def readyz() -> HealthResponse:
    return get_readiness_response()
