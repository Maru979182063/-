from __future__ import annotations

from typing import Any

from app.schemas.question import MaterialSelectionResult
from app.schemas.runtime import OperationRouteConfig, QuestionRuntimeConfig


class QuestionSnapshotBuilder:
    def __init__(self, runtime_config: QuestionRuntimeConfig) -> None:
        self.runtime_config = runtime_config

    def build(
        self,
        *,
        request_id: str,
        raw_input: dict[str, Any],
        standard_request: dict[str, Any],
        built_item: dict[str, Any],
        material: MaterialSelectionResult,
        route: OperationRouteConfig,
        raw_model_output: dict[str, Any] | None,
        parsed_structured_output: dict[str, Any] | None,
        parse_error: str | None,
        validation_result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        provider = self.runtime_config.llm.providers[route.provider]
        model_name = getattr(provider.models, route.model_key)
        base_url = provider.default_base_url
        if provider.base_url_env:
            import os

            base_url = os.getenv(provider.base_url_env, provider.default_base_url)

        return {
            "request_id": request_id,
            "input_snapshot": {
                "raw_business_input": raw_input,
                "decoded_standard_request": standard_request,
            },
            "prompt_snapshot": {
                "question_type": built_item.get("question_type"),
                "business_subtype": built_item.get("business_subtype"),
                "pattern_id": built_item.get("pattern_id"),
                "selected_pattern": built_item.get("selected_pattern"),
                "prompt_template_name": built_item.get("prompt_template_name"),
                "prompt_template_version": built_item.get("prompt_template_version"),
                "pattern_selection_reason": built_item.get("pattern_selection_reason"),
                "prompt_package": built_item.get("prompt_package"),
            },
            "runtime_snapshot": {
                "provider": route.provider,
                "model": model_name,
                "base_url": base_url,
                "temperature": provider.params.temperature,
                "max_output_tokens": provider.params.max_output_tokens,
                "timeout_seconds": provider.params.timeout_seconds,
                "route_model_key": route.model_key,
                "runtime_config_version": "question_runtime.v1",
            },
            "material_snapshot": {
                "material_id": material.material_id,
                "article_id": material.article_id,
                "document_genre": material.document_genre,
                "material_structure_label": material.material_structure_label,
                "material_structure_reason": material.material_structure_reason,
                "standalone_readability": material.standalone_readability,
                "source": material.source,
                "source_tail": material.source_tail,
                "original_text": material.original_text,
                "text_refined": material.text_refined,
                "refinement_reason": material.refinement_reason,
                "preview": self._preview(material.text),
                "tags": material.knowledge_tags,
                "quality_score": material.quality_score,
                "fit_scores": material.fit_scores,
            },
            "model_output_snapshot": {
                "raw_model_output": raw_model_output,
                "parsed_structured_output": parsed_structured_output,
                "parse_error": parse_error,
            },
            "validation_snapshot": validation_result or {},
        }

    def _preview(self, text: str, limit: int = 120) -> str:
        clean = (text or "").replace("\n", " ").strip()
        return clean if len(clean) <= limit else clean[: limit - 3] + "..."
