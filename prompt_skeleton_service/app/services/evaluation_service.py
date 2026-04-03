from __future__ import annotations

import json

from app.schemas.evaluation import JudgeResult
from app.schemas.item import GeneratedQuestion, ValidationResult
from app.schemas.runtime import OperationRouteConfig, QuestionRuntimeConfig
from app.services.llm_gateway import LLMGatewayService
from app.services.prompt_template_registry import PromptTemplateRegistry


class EvaluationService:
    def __init__(
        self,
        runtime_config: QuestionRuntimeConfig,
        prompt_template_registry: PromptTemplateRegistry | None = None,
    ) -> None:
        self.runtime_config = runtime_config
        self.prompt_template_registry = prompt_template_registry
        self.llm_gateway = LLMGatewayService(runtime_config)

    def evaluate(
        self,
        *,
        question_type: str,
        business_subtype: str | None = None,
        generated_question: GeneratedQuestion | None,
        validation_result: ValidationResult,
        material_text: str,
        difficulty_fit: dict | None,
    ) -> dict:
        if hasattr(difficulty_fit, "model_dump"):
            difficulty_fit = difficulty_fit.model_dump()

        judge_config = self.runtime_config.evaluation.judge
        if not judge_config.enabled:
            raise RuntimeError("LLM judge is mandatory in the current runtime configuration and cannot be disabled.")

        judge_prompt = self._build_judge_prompt(
            question_type=question_type,
            business_subtype=business_subtype,
            generated_question=generated_question,
            validation_result=validation_result,
            material_text=material_text,
            difficulty_fit=difficulty_fit or {},
        )
        route = OperationRouteConfig(provider=judge_config.provider, model_key=judge_config.model_key)
        result = self.llm_gateway.generate_json(
            route=route,
            system_prompt=(
                "You are a strict exam-quality reviewer. "
                "Evaluate the generated question and return only the required structured JSON."
            ),
            user_prompt=judge_prompt,
            schema_name="judge_result",
            schema=JudgeResult.model_json_schema(),
        )
        normalized = JudgeResult.model_validate(result).model_dump()
        normalized["raw"] = {**(normalized.get("raw") or {}), "judge_prompt": judge_prompt}
        return normalized

    def _build_judge_prompt(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        generated_question: GeneratedQuestion | None,
        validation_result: ValidationResult,
        material_text: str,
        difficulty_fit: dict,
    ) -> str:
        template_text = self._resolve_judge_template(question_type, business_subtype)
        question_payload = generated_question.model_dump() if generated_question is not None else {}
        context = {
            "question_type": question_type,
            "business_subtype": business_subtype or "",
            "generated_question": json.dumps(question_payload, ensure_ascii=False),
            "validation_result": json.dumps(validation_result.model_dump(), ensure_ascii=False),
            "difficulty_fit": json.dumps(difficulty_fit, ensure_ascii=False),
            "material_preview": self._preview(material_text),
        }
        return template_text.format_map(context)

    def _resolve_judge_template(self, question_type: str, business_subtype: str | None) -> str:
        if self.prompt_template_registry is None:
            return (
                "你是一名公考题审核专家。请结合题目、材料、答案、解析、校验结果和难度适配结果，"
                "从题型契合度、难度契合度、材料贴合度、干扰项质量、答案解析一致性五个维度给出审查结论。"
            )
        record = self.prompt_template_registry.resolve_default(
            question_type=question_type,
            business_subtype=business_subtype,
            action_type="judge_review",
        )
        return record.content

    def _preview(self, text: str, limit: int = 200) -> str:
        clean = (text or "").replace("\n", " ").strip()
        return clean if len(clean) <= limit else clean[: limit - 3] + "..."
