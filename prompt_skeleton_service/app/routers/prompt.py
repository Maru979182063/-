from fastapi import APIRouter, Depends

from app.core.dependencies import get_registry
from app.schemas.api import PromptBuildRequest
from app.schemas.decoder import DecodedPromptBuildEnvelope, DifyFormInput
from app.schemas.item import PromptBuildResponse
from app.services.config_registry import ConfigRegistry
from app.services.prompt_orchestrator import PromptOrchestratorService

router = APIRouter(prefix="/api/v1/prompt", tags=["prompt"])


@router.post("/decode-input", response_model=DecodedPromptBuildEnvelope)
def decode_input(
    request: DifyFormInput,
    registry: ConfigRegistry = Depends(get_registry),
) -> DecodedPromptBuildEnvelope:
    service = PromptOrchestratorService(registry)
    return DecodedPromptBuildEnvelope.model_validate(service.decode_input(request))


@router.post("/build", response_model=PromptBuildResponse)
def build_prompt(
    request: PromptBuildRequest,
    registry: ConfigRegistry = Depends(get_registry),
) -> PromptBuildResponse:
    service = PromptOrchestratorService(registry)
    return PromptBuildResponse.model_validate(service.build_prompt(request))
