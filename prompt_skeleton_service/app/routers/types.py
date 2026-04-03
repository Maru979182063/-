from fastapi import APIRouter, Depends

from app.core.dependencies import get_registry
from app.schemas.api import BusinessSubtypeSummary, PatternSummary, TypeListItem, TypeSchemaResponse
from app.services.config_registry import ConfigRegistry

router = APIRouter(prefix="/api/v1/types", tags=["types"])


@router.get("", response_model=list[TypeListItem])
def list_types(registry: ConfigRegistry = Depends(get_registry)) -> list[TypeListItem]:
    configs = registry.list_types()
    return [
        TypeListItem(
            type_id=config.type_id,
            display_name=config.display_name,
            aliases=config.aliases,
            enabled_patterns=[pattern.pattern_id for pattern in config.patterns if pattern.enabled],
        )
        for config in configs
    ]


@router.get("/{question_type}/schema", response_model=TypeSchemaResponse)
def get_type_schema(question_type: str, registry: ConfigRegistry = Depends(get_registry)) -> TypeSchemaResponse:
    config = registry.get_type(question_type)
    return TypeSchemaResponse(
        type_id=config.type_id,
        display_name=config.display_name,
        task_definition=config.task_definition,
        aliases=config.aliases,
        skeleton=config.skeleton,
        slot_schema={name: slot.model_dump() for name, slot in config.slot_schema.items()},
        default_slots=config.default_slots,
        default_pattern_id=config.default_pattern_id,
        available_patterns=[
            PatternSummary(
                pattern_id=pattern.pattern_id,
                pattern_name=pattern.pattern_name,
                enabled=pattern.enabled,
                match_rules=pattern.match_rules,
            )
            for pattern in config.patterns
        ],
        business_subtypes=[
            BusinessSubtypeSummary(
                subtype_id=subtype.subtype_id,
                display_name=subtype.display_name,
                description=subtype.description,
                preferred_patterns=subtype.preferred_patterns,
            )
            for subtype in config.business_subtypes
        ],
        fewshot_policy=config.fewshot_policy.model_dump(),
    )
