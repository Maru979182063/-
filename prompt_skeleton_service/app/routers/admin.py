from fastapi import APIRouter, Depends

from app.core.dependencies import get_registry
from app.schemas.api import ReloadConfigResponse
from app.services.config_registry import ConfigRegistry

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.post("/reload-config", response_model=ReloadConfigResponse)
def reload_config(registry: ConfigRegistry = Depends(get_registry)) -> ReloadConfigResponse:
    return ReloadConfigResponse.model_validate(registry.reload())
