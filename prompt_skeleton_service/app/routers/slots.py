from fastapi import APIRouter, Depends

from app.core.dependencies import get_registry
from app.schemas.api import ResolveResult, SlotResolveRequest
from app.services.config_registry import ConfigRegistry
from app.services.prompt_orchestrator import PromptOrchestratorService

router = APIRouter(prefix="/api/v1/slots", tags=["slots"])


@router.post("/resolve", response_model=ResolveResult)
def resolve_slots(
    request: SlotResolveRequest,
    registry: ConfigRegistry = Depends(get_registry),
) -> ResolveResult:
    service = PromptOrchestratorService(registry)
    return ResolveResult.model_validate(service.resolve_slots(request))
