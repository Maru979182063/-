from __future__ import annotations

from uuid import uuid4

from app.core.exceptions import DomainError
from app.schemas.api import PromptBuildRequest, SlotResolveRequest
from app.schemas.decoder import DifyFormInput
from app.schemas.config import BusinessSubtypeConfig, PatternConfig
from app.services.config_registry import ConfigRegistry
from app.services.input_decoder import InputDecoderService
from app.services.prompt_builder import PromptBuilderService
from app.services.slot_resolver import SlotResolverService


class PromptOrchestratorService:
    def __init__(self, registry: ConfigRegistry) -> None:
        self.registry = registry
        self.input_decoder = InputDecoderService()
        self.slot_resolver = SlotResolverService()
        self.prompt_builder = PromptBuilderService()

    def decode_input(self, request: DifyFormInput) -> dict:
        return self.input_decoder.decode(request)

    def resolve_slots(self, request: SlotResolveRequest) -> dict:
        type_config = self.registry.get_type(request.question_type)
        return self.slot_resolver.resolve(
            type_config,
            difficulty_target=request.difficulty_target,
            type_slots=request.type_slots,
            business_subtype=request.business_subtype,
            pattern_id=request.pattern_id,
        )

    def build_prompt(self, request: PromptBuildRequest) -> dict:
        if request.use_fewshot and request.fewshot_mode != "structure_only":
            raise DomainError(
                "Unsupported fewshot_mode.",
                status_code=422,
                details={"fewshot_mode": request.fewshot_mode, "supported": ["structure_only"]},
            )

        type_config = self.registry.get_type(request.question_type)
        subtype_config = self._get_business_subtype(type_config, request.business_subtype)
        resolve_result = self.slot_resolver.resolve(
            type_config,
            difficulty_target=request.difficulty_target,
            type_slots=request.type_slots,
            business_subtype=request.business_subtype,
            pattern_id=request.pattern_id,
        )
        pattern = self._get_pattern(type_config, resolve_result["selected_pattern"])
        prompt_package = self.prompt_builder.build(
            question_type_config=type_config,
            business_subtype_config=subtype_config,
            pattern=pattern,
            difficulty_target=request.difficulty_target,
            resolved_slots=resolve_result["resolved_slots"],
            skeleton=resolve_result["skeleton"],
            control_logic=resolve_result["control_logic"],
            generation_logic=resolve_result["generation_logic"],
            topic=request.topic,
            count=request.count,
            passage_style=request.passage_style,
            use_fewshot=request.use_fewshot,
            fewshot_mode=request.fewshot_mode,
            extra_constraints=request.extra_constraints,
        )
        notes = [
            "This service only assembles prompt skeletons and does not call any model.",
            "Few-shot priority is business_subtype > pattern > type default, and does not control difficulty.",
        ]
        return self._build_question_item(
            request=request,
            resolve_result=resolve_result,
            prompt_package=prompt_package,
            notes=notes,
        )

    def _get_pattern(self, type_config, pattern_id: str) -> PatternConfig:
        for pattern in type_config.patterns:
            if pattern.pattern_id == pattern_id:
                return pattern
        raise RuntimeError(f"Pattern not found after resolution: {pattern_id}")

    def _get_business_subtype(self, type_config, business_subtype: str | None) -> BusinessSubtypeConfig | None:
        if not business_subtype:
            return None
        for subtype in type_config.business_subtypes:
            if subtype.subtype_id == business_subtype:
                return subtype
        raise DomainError(
            "Unknown business_subtype for this question_type.",
            status_code=404,
            details={"question_type": type_config.type_id, "business_subtype": business_subtype},
        )

    def _build_question_item(
        self,
        *,
        request: PromptBuildRequest,
        resolve_result: dict,
        prompt_package: dict,
        notes: list[str],
    ) -> dict:
        return {
            "item_id": str(uuid4()),
            "question_type": resolve_result["question_type"],
            "business_subtype": resolve_result["business_subtype"],
            "pattern_id": resolve_result["selected_pattern"],
            "selected_pattern": resolve_result["selected_pattern"],
            "pattern_selection_reason": resolve_result["pattern_selection_reason"],
            "resolved_slots": resolve_result["resolved_slots"],
            "skeleton": resolve_result["skeleton"],
            "difficulty_target": request.difficulty_target,
            "difficulty_target_profile": resolve_result["difficulty_target_profile"],
            "difficulty_projection": resolve_result["difficulty_projection"],
            "difficulty_fit": resolve_result["difficulty_fit"],
            "control_logic": resolve_result["control_logic"],
            "generation_logic": resolve_result["generation_logic"],
            "prompt_package": prompt_package,
            "generated_question": None,
            "validation_result": None,
            "statuses": {
                "build_status": "success",
                "generation_status": "not_started",
                "validation_status": "not_started",
                "review_status": "draft",
            },
            "warnings": resolve_result["warnings"],
            "notes": notes,
        }
