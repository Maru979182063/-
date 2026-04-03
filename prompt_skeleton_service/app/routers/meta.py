from fastapi import APIRouter, Depends

from app.core.dependencies import get_prompt_template_registry, get_registry
from app.schemas.meta import ControlMetadataResponse, MetaQuestionTypeListResponse
from app.schemas.prompt_registry import PromptTemplateGroupResponse, PromptTemplateListResponse
from app.services.config_registry import ConfigRegistry
from app.services.meta_service import MetaService
from app.services.prompt_template_registry import PromptTemplateRegistry

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])


@router.get("/prompt-templates", response_model=PromptTemplateListResponse)
def list_prompt_templates(
    registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
) -> PromptTemplateListResponse:
    items = registry.list_templates()
    return PromptTemplateListResponse(count=len(items), items=items)


@router.get("/prompt-templates/{template_name}", response_model=PromptTemplateGroupResponse)
def get_prompt_template(
    template_name: str,
    registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
) -> PromptTemplateGroupResponse:
    items = registry.get_by_name(template_name)
    return PromptTemplateGroupResponse(template_name=template_name, items=items)


@router.get("/question-types", response_model=MetaQuestionTypeListResponse)
def list_question_type_meta(
    registry: ConfigRegistry = Depends(get_registry),
) -> MetaQuestionTypeListResponse:
    return MetaQuestionTypeListResponse.model_validate(MetaService(registry).list_question_types())


@router.get("/question-types/{question_type}/controls", response_model=ControlMetadataResponse)
def get_question_type_controls(
    question_type: str,
    registry: ConfigRegistry = Depends(get_registry),
) -> ControlMetadataResponse:
    return ControlMetadataResponse.model_validate(MetaService(registry).get_controls(question_type))
