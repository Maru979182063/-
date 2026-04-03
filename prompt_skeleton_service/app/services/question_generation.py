from __future__ import annotations

import logging
import random
import re
from copy import deepcopy
from difflib import SequenceMatcher
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, TypeAdapter

from app.core.exceptions import DomainError
from app.schemas.api import PromptBuildRequest
from app.schemas.decoder import BatchMeta, DifyFormInput
from app.schemas.item import GeneratedQuestion
from app.schemas.prompt_registry import PromptTemplateRecord
from app.schemas.question import (
    MaterialPolicy,
    MaterialSelectionResult,
    QuestionGenerateRequest,
    QuestionGenerationBatchResponse,
)
from app.schemas.runtime import QuestionRuntimeConfig
from app.services.evaluation_service import EvaluationService
from app.services.llm_gateway import LLMGatewayService
from app.services.material_bridge import MaterialBridgeService
from app.services.prompt_orchestrator import PromptOrchestratorService
from app.services.question_repository import QuestionRepository
from app.services.question_snapshot_builder import QuestionSnapshotBuilder
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.question_validator import QuestionValidatorService
from app.services.source_question_analyzer import SourceQuestionAnalyzer
from app.services.source_question_parser import SourceQuestionParserService


logger = logging.getLogger(__name__)


class GeneratedQuestionDraft(BaseModel):
    class GeneratedQuestionOptionsDraft(BaseModel):
        A: str
        B: str
        C: str
        D: str

    stem: str
    options: GeneratedQuestionOptionsDraft
    answer: str
    analysis: str


class MaterialRefinementDraft(BaseModel):
    refined_text: str
    changed: bool = False
    reason: str | None = None


class QuestionGenerationService:
    JUDGE_OVERALL_PASS_THRESHOLD = 80
    JUDGE_MATERIAL_ALIGNMENT_THRESHOLD = 70
    JUDGE_ANSWER_ANALYSIS_THRESHOLD = 70
    JUDGE_HARD_DIFFICULTY_THRESHOLD = 68
    QUALITY_REPAIR_RETRY_THRESHOLD = 84
    MAX_ALIGNMENT_RETRIES = 3
    MAX_QUALITY_REPAIR_RETRIES = 3
    RACE_CANDIDATE_COUNT = 3

    def __init__(
        self,
        *,
        orchestrator: PromptOrchestratorService,
        runtime_config: QuestionRuntimeConfig,
        repository: QuestionRepository,
        prompt_template_registry: PromptTemplateRegistry,
    ) -> None:
        self.orchestrator = orchestrator
        self.runtime_config = runtime_config
        self.repository = repository
        self.prompt_template_registry = prompt_template_registry
        self.material_bridge = MaterialBridgeService(runtime_config.materials)
        self.llm_gateway = LLMGatewayService(runtime_config)
        self.generated_question_adapter = TypeAdapter(GeneratedQuestionDraft)
        self.material_refinement_adapter = TypeAdapter(MaterialRefinementDraft)
        self.validator = QuestionValidatorService()
        self.snapshot_builder = QuestionSnapshotBuilder(runtime_config)
        self.evaluator = EvaluationService(runtime_config, prompt_template_registry)
        self.source_question_analyzer = SourceQuestionAnalyzer()
        self.source_question_parser = SourceQuestionParserService(runtime_config)

    def _question_generation_route(self):
        return self.runtime_config.llm.routing.question_generation or self.runtime_config.llm.routing.generate_question

    def _question_repair_route(self):
        return self.runtime_config.llm.routing.question_repair or self._question_generation_route()

    def _material_refinement_route(self):
        return self.runtime_config.llm.routing.material_refinement or self.runtime_config.llm.routing.review_actions.minor_edit

    def generate(self, request: QuestionGenerateRequest) -> dict:
        prepared_request = self._prepare_request(request)
        decode_request, target_override_warning = self._build_decode_request(prepared_request)
        decoded = self.orchestrator.decode_input(decode_request)
        standard_request = decoded["standard_request"]
        effective_difficulty_target = self._effective_difficulty_target(
            standard_request["difficulty_target"],
            use_reference_question=bool(request.source_question),
        )
        standard_request["difficulty_target"] = effective_difficulty_target
        batch_meta = BatchMeta.model_validate(decoded["batch_meta"])
        batch_meta.difficulty_target = effective_difficulty_target
        effective_count = batch_meta.effective_count
        batch_id = str(uuid4())
        request_id = str(uuid4())
        source_question_analysis = self.source_question_analyzer.analyze(
            source_question=prepared_request.source_question,
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
        )
        request_snapshot = self._build_request_snapshot(
            prepared_request,
            standard_request,
            decoded,
            request_id=request_id,
            source_question_analysis=source_question_analysis,
        )

        requested_material_count = max(effective_count * 4, effective_count + 2)
        materials, material_warnings = self.material_bridge.select_materials(
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
            difficulty_target=standard_request["difficulty_target"],
            topic=prepared_request.topic,
            text_direction=prepared_request.text_direction,
            document_genre=(prepared_request.material_policy.preferred_document_genres[0] if prepared_request.material_policy and prepared_request.material_policy.preferred_document_genres else None),
            material_structure_label=prepared_request.material_structure,
            material_policy=prepared_request.material_policy,
            count=requested_material_count,
            business_card_ids=source_question_analysis.get("business_card_ids") or [],
            query_terms=source_question_analysis.get("query_terms") or [],
            target_length=source_question_analysis.get("target_length"),
            length_tolerance=source_question_analysis.get("length_tolerance", 120),
            structure_constraints=source_question_analysis.get("structure_constraints") or {},
            enable_anchor_adaptation=bool(source_question_analysis),
            usage_stats_lookup=self.repository.get_material_usage_stats,
        )

        if not materials:
            raise DomainError(
                "No eligible materials were returned by passage_service.",
                status_code=502,
                details={"question_type": standard_request["question_type"]},
            )
        materials = self._prioritize_material_candidates(
            materials,
            question_type=standard_request["question_type"],
            source_question_analysis=source_question_analysis,
        )

        items: list[dict] = []
        accepted_candidates: list[dict] = []
        rejected_attempts: list[dict] = []
        rejected_candidates: list[dict] = []
        race_materials = materials[: max(self.RACE_CANDIDATE_COUNT, effective_count)]
        for index in range(len(race_materials)):
            material = self._annotate_material_usage(race_materials[index])
            if standard_request["question_type"] == "sentence_order":
                adapted_material = self._coerce_sentence_order_material(
                    material=material,
                    source_question_analysis=source_question_analysis,
                )
                if adapted_material is None:
                    rejected_attempts.append(
                        {
                            "material_id": material.material_id,
                            "selection_reason": material.selection_reason,
                            "document_genre": material.document_genre,
                            "validation_errors": ["sentence_order_material_unit_count_mismatch"],
                            "warnings": [],
                            "judge_score": 0.0,
                            "validator_score": 0.0,
                            "material_alignment": 0.0,
                            "answer_analysis_consistency": 0.0,
                            "current_status": "auto_failed",
                        }
                    )
                    continue
                material = adapted_material
            material = self._refine_material_if_needed(material)
            build_request = self._build_prompt_request_from_snapshot(request_snapshot)
            built_item = self._build_generated_item(
                build_request=build_request,
                material=material,
                batch_id=batch_id,
                item_id=None,
                request_snapshot=request_snapshot,
                revision_count=0,
                route=self._question_generation_route(),
                source_action="generate",
                review_note=None,
                request_id=request_id,
                previous_item=None,
            )

            if not (built_item.get("validation_result") or {}).get("passed"):
                rejected_summary = self._summarize_rejected_attempt(built_item, material)
                rejected_attempts.append(rejected_summary)
                rejected_candidates.append(
                    {
                        "item": built_item,
                        "material": material,
                        "summary": rejected_summary,
                        "rank_score": self._rejected_attempt_rank_score(built_item),
                    }
                )
                logger.info(
                    "question_rejected batch_id=%s attempt=%s material_id=%s status=%s errors=%s",
                    batch_id,
                    index + 1,
                    material.material_id,
                    built_item.get("current_status"),
                    (built_item.get("validation_result") or {}).get("errors", [])[:3],
                )
                continue

            logger.info(
                "question_candidate_accepted batch_id=%s item_id=%s version_no=%s request_id=%s rank_score=%s",
                batch_id,
                built_item["item_id"],
                built_item.get("current_version_no"),
                request_id,
                self._accepted_attempt_rank_score(built_item),
            )
            accepted_candidates.append(
                {
                    "item": built_item,
                    "material": material,
                    "rank_score": self._accepted_attempt_rank_score(built_item),
                }
            )

        if not items and standard_request["question_type"] == "sentence_order" and prepared_request.source_question:
            fallback_material = self._build_reference_source_material(prepared_request.source_question)
            built_item = self._build_generated_item(
                build_request=self._build_prompt_request_from_snapshot(request_snapshot),
                material=fallback_material,
                batch_id=batch_id,
                item_id=None,
                request_snapshot=request_snapshot,
                revision_count=0,
                route=self._question_generation_route(),
                source_action="generate",
                review_note="reference_source_fallback_used",
                request_id=request_id,
                previous_item=None,
            )
            if (built_item.get("validation_result") or {}).get("passed"):
                accepted_candidates.append(
                    {
                        "item": built_item,
                        "material": fallback_material,
                        "rank_score": self._accepted_attempt_rank_score(built_item),
                    }
                )
                material_warnings.append(
                    "Used the reference source passage as a fallback because no retrieved sentence-order materials passed validation."
                )
            else:
                rejected_summary = self._summarize_rejected_attempt(built_item, fallback_material)
                rejected_attempts.append(rejected_summary)
                rejected_candidates.append(
                    {
                        "item": built_item,
                        "material": fallback_material,
                        "summary": rejected_summary,
                        "rank_score": self._rejected_attempt_rank_score(built_item),
                    }
                )

        if accepted_candidates:
            accepted_candidates.sort(key=lambda entry: entry["rank_score"], reverse=True)
            winners = accepted_candidates[:effective_count]
            for winner in winners:
                built_item = winner["item"]
                self.repository.save_version(built_item.pop("_version_record"))
                self.repository.save_item(built_item)
                items.append(built_item)
            if len(accepted_candidates) > 1:
                material_warnings.append("Race mode enabled: returned the highest-scoring acceptable candidate from this round.")

        if not items:
            fallback_record_path = self._write_failure_markdown_record(
                batch_id=batch_id,
                request_id=request_id,
                question_type=standard_request["question_type"],
                difficulty_target=standard_request["difficulty_target"],
                rejected_attempts=rejected_attempts,
            )
            if rejected_candidates:
                best_rejected = max(rejected_candidates, key=lambda entry: entry["rank_score"])
                best_item = best_rejected["item"]
                best_item["notes"] = best_item.get("notes", []) + [
                    "best_rejected_attempt_returned_for_review",
                    f"failure_record_md={fallback_record_path}",
                ]
                self.repository.save_version(best_item.pop("_version_record"))
                self.repository.save_item(best_item)
                items.append(best_item)
                material_warnings.append(
                    "No question fully passed validation after retries; returned the highest-scoring blocked attempt for manual review."
                )
                material_warnings.append(f"Failure reasons were recorded to: {fallback_record_path}")
            else:
                raise DomainError(
                    "No acceptable questions passed validation after retries.",
                    status_code=422,
                    details={
                        "question_type": standard_request["question_type"],
                        "difficulty_target": standard_request["difficulty_target"],
                        "rejected_attempts": rejected_attempts[:5],
                        "failure_record_md": fallback_record_path,
                    },
                )

        generation_warnings = decoded.get("warnings", []) + ([target_override_warning] if target_override_warning else []) + material_warnings
        if rejected_attempts:
            generation_warnings.append(
                f"Filtered out {len(rejected_attempts)} low-quality generation attempts before returning results."
            )
        if len(items) < effective_count:
            generation_warnings.append(
                f"Only {len(items)} acceptable question(s) passed validation, below the requested {effective_count}."
            )

        response = {
            "batch_id": batch_id,
            "batch_meta": batch_meta.model_dump(),
            "items": items,
            "warnings": generation_warnings,
            "notes": [
                "Materials are fetched from the passage_service V2 material pool at generation time.",
                "If a reference question is provided, retrieval and generation both reuse its business-card and length signals.",
                "Reference-question runs raise generation difficulty by one level and treat the reference question as a style template.",
                "Only questions that pass validator and LLM quality gate are returned to the UI.",
                "Batch review actions are not wired yet; this slice persists generated items for later review work.",
            ],
        }
        self.repository.save_batch(batch_id, response)
        return QuestionGenerationBatchResponse.model_validate(response).model_dump()

    def _prepare_request(self, request: QuestionGenerateRequest) -> QuestionGenerateRequest:
        source_question = request.source_question
        if source_question is None:
            return request
        passage = (source_question.passage or "").strip()
        stem = (source_question.stem or "").strip()
        options = source_question.options or {}
        has_structured_options = any(str(value or "").strip() for value in options.values())
        looks_like_raw_question = bool(
            passage
            and not has_structured_options
            and any(token in passage for token in ("A.", "B.", "C.", "D.", "A、", "B、", "C、", "D、", "正确答案", "答案：", "解析：", "重新排列", "语序正确", "横线"))
        )
        generic_stem = stem in {
            "",
            "\u8fd9\u6bb5\u6587\u5b57\u610f\u5728\u8bf4\u660e\uff08 \uff09\u3002",
            "\u8fd9\u6bb5\u6587\u5b57\u610f\u5728\u5f3a\u8c03\uff08 \uff09\u3002",
            "\u8fd9\u6bb5\u6587\u5b57\u4e3b\u8981\u8bf4\u660e\uff08 \uff09\u3002",
        }
        if not looks_like_raw_question or not generic_stem:
            return request
        parsed = self.source_question_parser.parse(passage)
        return request.model_copy(update={"source_question": parsed})

    def _build_decode_request(self, request: QuestionGenerateRequest) -> tuple[DifyFormInput, str | None]:
        inferred = self.source_question_analyzer.infer_request_target(request.source_question)
        current_focus = str(request.question_focus or "").strip()
        if current_focus.lower() in {"select", "auto"} or current_focus in {"不指定", "不指定（自动匹配）", "请选择"}:
            current_focus = ""
        if not inferred:
            return request.to_dify_form_input(), None

        target_focus = self._focus_value_for_target(
            question_type=str(inferred.get("question_type") or ""),
            business_subtype=inferred.get("business_subtype"),
        )
        if not target_focus:
            return request.to_dify_form_input(), None

        should_override = (not current_focus) or (current_focus != target_focus)
        if not should_override:
            return request.to_dify_form_input(), None

        return (
            DifyFormInput(
                question_focus=target_focus,
                difficulty_level=request.difficulty_level,
                text_direction=request.text_direction,
                special_question_types=[],
                count=request.count,
            ),
            f"Reference question auto-overrode form selection to {target_focus}.",
        )

    @staticmethod
    def _focus_value_for_target(*, question_type: str, business_subtype: str | None) -> str | None:
        if question_type == "sentence_order":
            return "sentence_order"
        if question_type == "sentence_fill":
            return "sentence_fill"
        if question_type == "continuation":
            return "continuation"
        if question_type == "main_idea" and business_subtype == "title_selection":
            return "title_selection"
        if question_type == "main_idea" and business_subtype == "center_understanding":
            return "center_understanding"
        return None

    def _build_prompt_request_from_snapshot(self, request_snapshot: dict) -> PromptBuildRequest:
        return PromptBuildRequest(
            question_type=request_snapshot["question_type"],
            business_subtype=request_snapshot.get("business_subtype"),
            pattern_id=request_snapshot.get("pattern_id"),
            difficulty_target=request_snapshot["difficulty_target"],
            topic=request_snapshot.get("topic"),
            count=1,
            passage_style=request_snapshot.get("passage_style"),
            use_fewshot=request_snapshot.get("use_fewshot", True),
            fewshot_mode=request_snapshot.get("fewshot_mode", "structure_only"),
            type_slots=deepcopy(request_snapshot.get("type_slots") or {}),
            extra_constraints=deepcopy(request_snapshot.get("extra_constraints") or {}),
        )

    def _generate_question(
        self,
        built_item: dict,
        material: MaterialSelectionResult,
        route,
        prompt_template: PromptTemplateRecord,
        feedback_notes: list[str] | None = None,
    ) -> tuple[GeneratedQuestion, dict]:
        prompt_package = built_item["prompt_package"]
        system_prompt = "\n\n".join(
            [
                prompt_template.content,
                prompt_package["system_prompt"],
            ]
        )
        final_user_prompt = "\n\n".join(
            self._build_generation_prompt_sections(
                built_item=built_item,
                material=material,
                prompt_package=prompt_package,
                feedback_notes=feedback_notes or [],
            )
        )
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=system_prompt,
            user_prompt=final_user_prompt,
            schema_name="generated_question",
            schema=GeneratedQuestionDraft.model_json_schema(),
        )
        try:
            generated = self.generated_question_adapter.validate_python(response)
        except Exception as exc:  # noqa: BLE001
            raise DomainError(
                "Structured model output could not be parsed into GeneratedQuestion.",
                status_code=502,
                details={"reason": str(exc)},
            ) from exc
        metadata = {
            "material_id": material.material_id,
            "article_id": material.article_id,
            "batch_prompt_pattern": built_item["selected_pattern"],
        }
        generated_question = GeneratedQuestion(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            pattern_id=built_item.get("pattern_id"),
            stem=generated.stem,
            options=generated.options.model_dump(),
            answer=generated.answer,
            analysis=generated.analysis,
            metadata=metadata,
        )
        if built_item["question_type"] == "sentence_order":
            generated_question = self._enforce_sentence_order_six_unit_output(generated_question)
        return self._remap_answer_position(generated_question), response

    def revise_minor_edit(self, item: dict, instruction: str | None) -> dict:
        request_id = str(uuid4())
        generated_question = item.get("generated_question")
        if not generated_question:
            raise DomainError(
                "minor_edit requires an existing generated_question.",
                status_code=422,
                details={"item_id": item.get("item_id")},
            )
        material = MaterialSelectionResult.model_validate(item["material_selection"])
        material = self._annotate_material_usage(material)
        material = self._refine_material_if_needed(material)
        route = self.runtime_config.llm.routing.review_actions.minor_edit
        prompt_template = self._resolve_template(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            action_type="minor_edit",
        )
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=prompt_template.content,
            user_prompt="\n\n".join(
                [
                    "Current generated question JSON:",
                    str(generated_question),
                    "Original source material:",
                    material.text,
                    f"Revision instruction: {instruction or 'Polish the wording while preserving intent.'}",
                    "Return a full updated GeneratedQuestion object.",
                ]
            ),
            schema_name="generated_question_revision",
            schema=GeneratedQuestionDraft.model_json_schema(),
        )
        revised = self.generated_question_adapter.validate_python(response)
        metadata = {
            "material_id": material.material_id,
            "article_id": material.article_id,
            "revision_mode": "minor_edit",
        }
        item["generated_question"] = self._remap_answer_position(GeneratedQuestion(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            pattern_id=item.get("pattern_id"),
            stem=revised.stem,
            options=revised.options.model_dump(),
            answer=revised.answer,
            analysis=revised.analysis,
            metadata=metadata,
        )).model_dump()
        validation_result = self.validator.validate(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            generated_question=GeneratedQuestion.model_validate(item["generated_question"]),
            material_text=material.text,
            original_material_text=material.original_text,
            material_source=material.source,
            difficulty_fit=item.get("difficulty_fit"),
            source_question=(item.get("request_snapshot") or {}).get("source_question"),
            source_question_analysis=(item.get("request_snapshot") or {}).get("source_question_analysis"),
        )
        version_no = int(item.get("current_version_no", 1)) + 1
        item["current_version_no"] = version_no
        item["current_status"] = "pending_review" if validation_result.passed else "auto_failed"
        item["validation_result"] = validation_result.model_dump()
        item["evaluation_result"] = self.evaluator.evaluate(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            generated_question=GeneratedQuestion.model_validate(item["generated_question"]),
            validation_result=validation_result,
            material_text=material.text,
            difficulty_fit=item.get("difficulty_fit"),
        )
        self._apply_evaluation_gate(
            validation_result=validation_result,
            evaluation_result=item["evaluation_result"],
            difficulty_target=str(item.get("difficulty_target") or ""),
        )
        item["validation_result"] = validation_result.model_dump()
        item["revision_count"] = int(item.get("revision_count", 0)) + 1
        item["statuses"]["generation_status"] = "success"
        item["statuses"]["validation_status"] = validation_result.validation_status
        item["statuses"]["review_status"] = "waiting_review" if validation_result.passed else "needs_revision"
        item["latest_action"] = "minor_edit"
        item["latest_action_at"] = self.repository._utc_now()
        item["notes"] = item.get("notes", []) + [f"minor_edit applied: {instruction or 'no instruction'}"]
        item["_version_record"] = self._build_version_record(
            item=item,
            source_action="minor_edit",
            parent_version_no=version_no - 1,
            version_no=version_no,
            target_difficulty=item.get("difficulty_target"),
            material=material,
            prompt_template=prompt_template,
            raw_model_output=response,
            parsed_structured_output=item["generated_question"],
            parse_error=None,
            validation_result=item["validation_result"],
            evaluation_result=item["evaluation_result"],
            runtime_snapshot=self.snapshot_builder.build(
                request_id=request_id,
                raw_input=item.get("request_snapshot", {}).get("source_form", {}),
                standard_request=self._build_prompt_request_from_snapshot(item["request_snapshot"]).model_dump(),
                built_item=item,
                material=material,
                route=route,
                raw_model_output=response,
                parsed_structured_output=item["generated_question"],
                parse_error=None,
                validation_result=item["validation_result"],
            ),
        )
        return item

    def revise_question_modify(self, item: dict, instruction: str | None, control_overrides: dict) -> dict:
        request_id = str(uuid4())
        request_snapshot = self._apply_control_overrides(item.get("request_snapshot") or {}, control_overrides, instruction)
        material = MaterialSelectionResult.model_validate(item["material_selection"])
        material = self._annotate_material_usage(material)
        material = self._refine_material_if_needed(material)
        return self._build_generated_item(
            build_request=self._build_prompt_request_from_snapshot(request_snapshot),
            material=material,
            batch_id=item["batch_id"],
            item_id=item["item_id"],
            request_snapshot=request_snapshot,
            revision_count=int(item.get("revision_count", 0)) + 1,
            route=self.runtime_config.llm.routing.review_actions.question_modify,
            source_action="question_modify",
            review_note=f"question_modify: {instruction or 'control override only'}",
            request_id=request_id,
            previous_item=item,
        )

    def revise_text_modify(self, item: dict, instruction: str | None, control_overrides: dict) -> dict:
        request_id = str(uuid4())
        request_snapshot = self._apply_control_overrides(item.get("request_snapshot") or {}, control_overrides, instruction)
        previous_material_id = (item.get("material_selection") or {}).get("material_id")
        requested_material_id = control_overrides.get("material_id")
        material_policy = self._material_policy_from_snapshot(request_snapshot)
        manual_material_text = self._clean_material_text(str(control_overrides.get("material_text") or "").strip())
        if manual_material_text:
            materials = [self._build_manual_material_selection(item=item, text=manual_material_text)]
            warnings = ["Manual material override was used for text_modify."]
        elif requested_material_id:
            replacement_candidates = self.material_bridge.list_material_options(
                question_type=request_snapshot["question_type"],
                business_subtype=request_snapshot.get("business_subtype"),
                document_genre=(material_policy.preferred_document_genres[0] if material_policy and material_policy.preferred_document_genres else None),
                material_structure_label=request_snapshot.get("material_structure"),
                exclude_material_ids=None,
                limit=24,
                difficulty_target=request_snapshot.get("difficulty_target", "medium"),
                usage_stats_lookup=self.repository.get_material_usage_stats,
            )
            materials = [candidate for candidate in replacement_candidates if candidate.material_id == requested_material_id]
            warnings = []
        else:
            materials, warnings = self.material_bridge.select_materials(
                question_type=request_snapshot["question_type"],
                business_subtype=request_snapshot.get("business_subtype"),
                difficulty_target=request_snapshot["difficulty_target"],
                topic=request_snapshot.get("topic"),
                text_direction=((request_snapshot.get("extra_constraints") or {}).get("text_direction")),
                document_genre=(material_policy.preferred_document_genres[0] if material_policy and material_policy.preferred_document_genres else None),
                material_policy=material_policy,
                count=1,
                business_card_ids=((request_snapshot.get("source_question_analysis") or {}).get("business_card_ids") or []),
                query_terms=((request_snapshot.get("source_question_analysis") or {}).get("query_terms") or []),
                target_length=((request_snapshot.get("source_question_analysis") or {}).get("target_length")),
                length_tolerance=((request_snapshot.get("source_question_analysis") or {}).get("length_tolerance") or 120),
                structure_constraints=((request_snapshot.get("source_question_analysis") or {}).get("structure_constraints") or {}),
                enable_anchor_adaptation=bool(request_snapshot.get("source_question_analysis")),
                exclude_material_ids=None if requested_material_id else ({previous_material_id} if previous_material_id else None),
                usage_stats_lookup=self.repository.get_material_usage_stats,
            )
        if not materials:
            raise DomainError(
                "text_modify could not find a replacement material.",
                status_code=422,
                details={"item_id": item["item_id"], "previous_material_id": previous_material_id, "requested_material_id": requested_material_id},
            )
        rebuilt = self._build_generated_item(
            build_request=self._build_prompt_request_from_snapshot(request_snapshot),
            material=self._refine_material_if_needed(self._annotate_material_usage(materials[0])),
            batch_id=item["batch_id"],
            item_id=item["item_id"],
            request_snapshot=request_snapshot,
            revision_count=int(item.get("revision_count", 0)) + 1,
            route=self.runtime_config.llm.routing.review_actions.text_modify,
            source_action="text_modify",
            review_note=f"text_modify: {instruction or 'new material requested'}",
            request_id=request_id,
            previous_item=item,
        )
        rebuilt["warnings"] = rebuilt.get("warnings", []) + warnings
        return rebuilt

    def apply_manual_edit(self, item: dict, instruction: str | None, control_overrides: dict) -> dict:
        request_id = str(uuid4())
        patch = deepcopy(control_overrides.get("manual_patch") or {})
        current_question = GeneratedQuestion.model_validate(item.get("generated_question") or {})
        current_material = MaterialSelectionResult.model_validate(item["material_selection"])

        normalized_options = {}
        option_patch = patch.get("options") or {}
        for letter in ("A", "B", "C", "D"):
            normalized_options[letter] = str(option_patch.get(letter, current_question.options.get(letter, ""))).strip()

        normalized_answer = str(patch.get("answer", current_question.answer or "")).strip().upper()
        if normalized_answer not in {"A", "B", "C", "D"}:
            raise DomainError(
                "manual_edit requires answer to be one of A/B/C/D.",
                status_code=422,
                details={"answer": normalized_answer},
            )

        edited_question = current_question.model_copy(
            update={
                "stem": str(patch.get("stem", current_question.stem or "")).strip(),
                "options": normalized_options,
                "answer": normalized_answer,
                "analysis": str(patch.get("analysis", current_question.analysis or "")).strip(),
                "metadata": {
                    **(current_question.metadata or {}),
                    "manual_edit": True,
                },
            }
        )

        updated_material_text = self._clean_material_text(str(patch.get("material_text", current_material.text or "")).strip())
        edited_material = current_material.model_copy(
            update={
                "original_text": current_material.original_text or current_material.text,
                "text": updated_material_text or current_material.text,
                "text_refined": True if updated_material_text and updated_material_text != current_material.text else current_material.text_refined,
                "refinement_reason": (
                    "manual_edit"
                    if updated_material_text and updated_material_text != current_material.text
                    else current_material.refinement_reason
                ),
            }
        )

        revised_item = deepcopy(item)
        revised_item["generated_question"] = edited_question.model_dump()
        revised_item["stem_text"] = edited_question.stem
        revised_item["material_selection"] = edited_material.model_dump()
        revised_item["material_text"] = edited_material.text
        revised_item["material_source"] = edited_material.source
        revised_item["material_usage_count_before"] = edited_material.usage_count_before
        revised_item["material_previously_used"] = edited_material.previously_used
        revised_item["material_last_used_at"] = edited_material.last_used_at

        validation_result = self.validator.validate(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            generated_question=edited_question,
            material_text=edited_material.text,
            original_material_text=edited_material.original_text,
            material_source=edited_material.source,
            difficulty_fit=revised_item.get("difficulty_fit"),
            source_question=(revised_item.get("request_snapshot") or {}).get("source_question"),
            source_question_analysis=(revised_item.get("request_snapshot") or {}).get("source_question_analysis"),
        )
        revised_item["validation_result"] = validation_result.model_dump()
        revised_item["evaluation_result"] = self.evaluator.evaluate(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            generated_question=edited_question,
            validation_result=validation_result,
            material_text=edited_material.text,
            difficulty_fit=revised_item.get("difficulty_fit"),
        )
        self._apply_evaluation_gate(
            validation_result=validation_result,
            evaluation_result=revised_item["evaluation_result"],
            difficulty_target=str(revised_item.get("difficulty_target") or ""),
        )
        revised_item["validation_result"] = validation_result.model_dump()

        version_no = int(item.get("current_version_no", 1)) + 1
        revised_item["current_version_no"] = version_no
        revised_item["revision_count"] = int(item.get("revision_count", 0)) + 1
        revised_item["current_status"] = "pending_review" if validation_result.passed else "auto_failed"
        revised_item["statuses"]["generation_status"] = "success"
        revised_item["statuses"]["validation_status"] = validation_result.validation_status
        revised_item["statuses"]["review_status"] = "waiting_review" if validation_result.passed else "needs_revision"
        revised_item["latest_action"] = "manual_edit"
        revised_item["latest_action_at"] = self.repository._utc_now()
        revised_item["notes"] = revised_item.get("notes", []) + [f"manual_edit: {instruction or 'saved from UI'}"]

        template_record = self._resolve_template(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            action_type="question_modify",
        )
        runtime_snapshot = self.snapshot_builder.build(
            request_id=request_id,
            raw_input=(revised_item.get("request_snapshot") or {}).get("source_form", {}),
            standard_request=self._build_prompt_request_from_snapshot(revised_item["request_snapshot"]).model_dump(),
            built_item=revised_item,
            material=edited_material,
            route=self.runtime_config.llm.routing.review_actions.question_modify,
            raw_model_output=None,
            parsed_structured_output=revised_item["generated_question"],
            parse_error=None,
            validation_result=revised_item["validation_result"],
        )
        revised_item["_version_record"] = self._build_version_record(
            item=revised_item,
            source_action="manual_edit",
            parent_version_no=version_no - 1,
            version_no=version_no,
            target_difficulty=revised_item.get("difficulty_target"),
            material=edited_material,
            prompt_template=template_record,
            raw_model_output=None,
            parsed_structured_output=revised_item["generated_question"],
            parse_error=None,
            validation_result=revised_item["validation_result"],
            evaluation_result=revised_item["evaluation_result"],
            runtime_snapshot=runtime_snapshot,
        )
        return revised_item

    def _build_generated_item(
        self,
        *,
        build_request: PromptBuildRequest,
        material: MaterialSelectionResult,
        batch_id: str,
        item_id: str | None,
        request_snapshot: dict,
        revision_count: int,
        route,
        source_action: str,
        review_note: str | None,
        request_id: str,
        previous_item: dict | None = None,
    ) -> dict:
        built_item = self.orchestrator.build_prompt(build_request)
        if item_id:
            built_item["item_id"] = item_id
        built_item["batch_id"] = batch_id
        built_item["request_snapshot"] = deepcopy(request_snapshot)
        built_item["material_selection"] = material.model_dump()
        built_item["material_text"] = material.text
        built_item["material_source"] = material.source
        built_item["material_usage_count_before"] = material.usage_count_before
        built_item["material_previously_used"] = material.previously_used
        built_item["material_last_used_at"] = material.last_used_at
        built_item["revision_count"] = revision_count
        template_record = self._resolve_template(
            question_type=build_request.question_type,
            business_subtype=build_request.business_subtype,
            action_type=source_action,
        )
        built_item["prompt_template_name"] = template_record.template_name
        built_item["prompt_template_version"] = template_record.template_version
        version_no = 1 if previous_item is None else int(previous_item.get("current_version_no", 1)) + 1
        parent_version_no = None if previous_item is None else int(previous_item.get("current_version_no", 1))
        built_item["current_version_no"] = version_no
        built_item["latest_action"] = source_action
        built_item["latest_action_at"] = self.repository._utc_now()

        raw_model_output: dict | None = None
        parsed_structured_output: dict | None = None
        parse_error: str | None = None
        generated_question: GeneratedQuestion | None = None
        request_source_question = request_snapshot.get("source_question")
        request_source_analysis = request_snapshot.get("source_question_analysis")

        try:
            generated_question, raw_model_output = self._generate_question(
                built_item,
                material,
                route,
                template_record,
                feedback_notes=[],
            )
            built_item["generated_question"] = generated_question.model_dump()
            built_item["stem_text"] = generated_question.stem
            parsed_structured_output = built_item["generated_question"]
            built_item["statuses"]["generation_status"] = "success"
        except DomainError as exc:
            parse_error = exc.message
            built_item["warnings"] = built_item.get("warnings", []) + [exc.message]
            built_item["notes"] = built_item.get("notes", []) + [f"generation_error: {exc.details}"]
            built_item["statuses"]["generation_status"] = "failed"
            if previous_item is not None:
                built_item["generated_question"] = previous_item.get("generated_question")
                built_item["validation_result"] = previous_item.get("validation_result")

        validation_result = self.validator.validate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=generated_question,
            material_text=material.text,
            original_material_text=material.original_text,
            material_source=material.source,
            difficulty_fit=built_item.get("difficulty_fit"),
            source_question=request_source_question,
            source_question_analysis=request_source_analysis,
        )

        alignment_retry_count = 0
        while generated_question and self._should_retry_alignment(validation_result, request_source_analysis):
            if alignment_retry_count >= self.MAX_ALIGNMENT_RETRIES:
                break
            feedback_notes = self._build_alignment_feedback_notes(validation_result, request_source_analysis)
            try:
                regenerated_question, retry_raw_output = self._generate_question(
                    built_item,
                    material,
                    self._question_repair_route(),
                    template_record,
                    feedback_notes=feedback_notes,
                )
                retry_validation_result = self.validator.validate(
                    question_type=built_item["question_type"],
                    business_subtype=built_item.get("business_subtype"),
                    generated_question=regenerated_question,
                    material_text=material.text,
                    original_material_text=material.original_text,
                    material_source=material.source,
                    difficulty_fit=built_item.get("difficulty_fit"),
                    source_question=request_source_question,
                    source_question_analysis=request_source_analysis,
                )
                alignment_retry_count += 1
                if retry_validation_result.passed or retry_validation_result.score > validation_result.score:
                    generated_question = regenerated_question
                    raw_model_output = retry_raw_output
                    built_item["generated_question"] = generated_question.model_dump()
                    built_item["stem_text"] = generated_question.stem
                    parsed_structured_output = built_item["generated_question"]
                    validation_result = retry_validation_result
                    built_item["notes"] = built_item.get("notes", []) + [f"alignment_retry_applied_{alignment_retry_count}"]
                else:
                    break
            except DomainError as exc:
                built_item["warnings"] = built_item.get("warnings", []) + [f"alignment retry skipped: {exc.message}"]
                break

        built_item["validation_result"] = validation_result.model_dump()
        built_item["evaluation_result"] = self.evaluator.evaluate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=generated_question,
            validation_result=validation_result,
            material_text=material.text,
            difficulty_fit=built_item.get("difficulty_fit"),
        )
        quality_gate_errors = self._apply_evaluation_gate(
            validation_result=validation_result,
            evaluation_result=built_item["evaluation_result"],
            difficulty_target=str(built_item.get("difficulty_target") or ""),
        )
        quality_retry_count = 0
        while generated_question and self._should_retry_quality_repair(
            validation_result=validation_result,
            quality_gate_errors=quality_gate_errors,
            evaluation_result=built_item["evaluation_result"],
            source_question_analysis=request_source_analysis,
        ):
            if quality_retry_count >= self.MAX_QUALITY_REPAIR_RETRIES:
                break
            feedback_notes = self._build_quality_repair_feedback_notes(
                validation_result=validation_result,
                evaluation_result=built_item["evaluation_result"],
                quality_gate_errors=quality_gate_errors,
                source_question_analysis=request_source_analysis,
            )
            try:
                repaired_question, repaired_raw_output = self._generate_question(
                    built_item,
                    material,
                    self._question_repair_route(),
                    template_record,
                    feedback_notes=feedback_notes,
                )
                repaired_validation_result = self.validator.validate(
                    question_type=built_item["question_type"],
                    business_subtype=built_item.get("business_subtype"),
                    generated_question=repaired_question,
                    material_text=material.text,
                    original_material_text=material.original_text,
                    material_source=material.source,
                    difficulty_fit=built_item.get("difficulty_fit"),
                    source_question=request_source_question,
                    source_question_analysis=request_source_analysis,
                )
                repaired_evaluation_result = self.evaluator.evaluate(
                    question_type=built_item["question_type"],
                    business_subtype=built_item.get("business_subtype"),
                    generated_question=repaired_question,
                    validation_result=repaired_validation_result,
                    material_text=material.text,
                    difficulty_fit=built_item.get("difficulty_fit"),
                )
                repaired_quality_gate_errors = self._apply_evaluation_gate(
                    validation_result=repaired_validation_result,
                    evaluation_result=repaired_evaluation_result,
                    difficulty_target=str(built_item.get("difficulty_target") or ""),
                )
                quality_retry_count += 1
                if self._should_accept_quality_retry(
                    current_validation_result=validation_result,
                    current_evaluation_result=built_item["evaluation_result"],
                    repaired_validation_result=repaired_validation_result,
                    repaired_evaluation_result=repaired_evaluation_result,
                    repaired_quality_gate_errors=repaired_quality_gate_errors,
                ):
                    generated_question = repaired_question
                    raw_model_output = repaired_raw_output
                    built_item["generated_question"] = generated_question.model_dump()
                    built_item["stem_text"] = generated_question.stem
                    parsed_structured_output = built_item["generated_question"]
                    validation_result = repaired_validation_result
                    built_item["evaluation_result"] = repaired_evaluation_result
                    quality_gate_errors = repaired_quality_gate_errors
                    built_item["notes"] = built_item.get("notes", []) + [f"quality_repair_retry_applied_{quality_retry_count}"]
                else:
                    break
            except DomainError as exc:
                built_item["warnings"] = built_item.get("warnings", []) + [f"quality repair retry skipped: {exc.message}"]
                break

        built_item["validation_result"] = validation_result.model_dump()
        built_item["statuses"]["validation_status"] = validation_result.validation_status
        built_item["current_status"] = "pending_review" if validation_result.passed else "auto_failed"
        built_item["statuses"]["review_status"] = "waiting_review" if validation_result.passed else "needs_revision"

        if review_note:
            built_item["notes"] = built_item.get("notes", []) + [review_note]
        if quality_gate_errors:
            built_item["notes"] = built_item.get("notes", []) + ["evaluation_gate_applied"]
        runtime_snapshot = self.snapshot_builder.build(
            request_id=request_id,
            raw_input=request_snapshot.get("source_form", {}),
            standard_request=build_request.model_dump(),
            built_item=built_item,
            material=material,
            route=route,
            raw_model_output=raw_model_output,
            parsed_structured_output=parsed_structured_output,
            parse_error=parse_error,
            validation_result=built_item["validation_result"],
        )
        built_item["_version_record"] = self._build_version_record(
            item=built_item,
            source_action=source_action,
            parent_version_no=parent_version_no,
            version_no=version_no,
            target_difficulty=build_request.difficulty_target,
            material=material,
            prompt_template=template_record,
            raw_model_output=raw_model_output,
            parsed_structured_output=parsed_structured_output,
            parse_error=parse_error,
            validation_result=built_item["validation_result"],
            evaluation_result=built_item["evaluation_result"],
            runtime_snapshot=runtime_snapshot,
        )
        return built_item

    def _build_request_snapshot(
        self,
        request: QuestionGenerateRequest,
        standard_request: dict,
        decoded: dict,
        *,
        request_id: str,
        source_question_analysis: dict,
    ) -> dict:
        merged_extra_constraints = deepcopy(standard_request.get("extra_constraints") or {})
        if request.extra_constraints:
            merged_extra_constraints.update(request.extra_constraints)
        if source_question_analysis:
            merged_extra_constraints.setdefault("source_question_style_summary", deepcopy(source_question_analysis.get("style_summary") or {}))
            merged_extra_constraints.setdefault("reference_business_cards", deepcopy(source_question_analysis.get("business_card_ids") or []))
            merged_extra_constraints.setdefault("reference_query_terms", deepcopy(source_question_analysis.get("query_terms") or []))
        resolved_pattern_id = standard_request.get("pattern_id") or self._resolve_reference_pattern_id(
            question_type=standard_request["question_type"],
            source_question_analysis=source_question_analysis,
        )
        return {
            "request_id": request_id,
            "question_type": standard_request["question_type"],
            "business_subtype": standard_request.get("business_subtype"),
            "pattern_id": resolved_pattern_id,
            "difficulty_target": standard_request["difficulty_target"],
            "topic": request.topic or source_question_analysis.get("topic"),
            "material_structure": request.material_structure,
            "passage_style": request.passage_style,
            "use_fewshot": request.use_fewshot,
            "fewshot_mode": request.fewshot_mode,
            "type_slots": deepcopy(request.type_slots),
            "extra_constraints": merged_extra_constraints,
            "material_policy": request.material_policy.model_dump() if request.material_policy else None,
            "source_question": request.source_question.model_dump() if request.source_question else None,
            "source_question_analysis": deepcopy(source_question_analysis),
            "source_form": {
                "question_focus": request.question_focus,
                "difficulty_level": request.difficulty_level,
                "effective_difficulty_target": standard_request["difficulty_target"],
                "text_direction": request.text_direction,
                "special_question_types": deepcopy(request.special_question_types),
                "mapping_source": decoded.get("mapping_source"),
                "selected_special_type": decoded.get("selected_special_type"),
            },
        }

    @staticmethod
    def _summarize_rejected_attempt(built_item: dict, material: MaterialSelectionResult) -> dict:
        validation_result = built_item.get("validation_result") or {}
        evaluation_result = built_item.get("evaluation_result") or {}
        return {
            "material_id": material.material_id,
            "pattern_id": built_item.get("pattern_id"),
            "current_status": built_item.get("current_status"),
            "validation_errors": validation_result.get("errors", [])[:5],
            "judge_score": evaluation_result.get("overall_score"),
            "judge_reason": evaluation_result.get("judge_reason"),
        }

    def _rejected_attempt_rank_score(self, built_item: dict) -> float:
        validation_result = built_item.get("validation_result") or {}
        evaluation_result = built_item.get("evaluation_result") or {}
        validator_score = float(validation_result.get("score") or 0.0)
        judge_score = self._normalize_judge_score(evaluation_result.get("overall_score"))
        material_alignment = self._normalize_judge_score(evaluation_result.get("material_alignment"))
        answer_consistency = self._normalize_judge_score(evaluation_result.get("answer_analysis_consistency"))
        error_penalty = len(validation_result.get("errors") or []) * 6
        warning_penalty = len(validation_result.get("warnings") or []) * 2
        return round(0.45 * judge_score + 0.30 * validator_score + 0.15 * material_alignment + 0.10 * answer_consistency - error_penalty - warning_penalty, 4)

    def _accepted_attempt_rank_score(self, built_item: dict) -> float:
        base = self._rejected_attempt_rank_score(built_item)
        evaluation_result = built_item.get("evaluation_result") or {}
        difficulty_fit = self._normalize_judge_score(evaluation_result.get("difficulty_fit"))
        return round(base + 8 + 0.08 * difficulty_fit, 4)

    def _write_failure_markdown_record(
        self,
        *,
        batch_id: str,
        request_id: str,
        question_type: str,
        difficulty_target: str,
        rejected_attempts: list[dict],
    ) -> str:
        reports_dir = Path(__file__).resolve().parents[3] / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        safe_batch = batch_id.replace("-", "")
        path = reports_dir / f"generation_failure_{safe_batch}.md"
        lines = [
            f"# Generation Failure Record {batch_id}",
            "",
            f"- request_id: `{request_id}`",
            f"- question_type: `{question_type}`",
            f"- difficulty_target: `{difficulty_target}`",
            f"- rejected_attempt_count: `{len(rejected_attempts)}`",
            "",
            "## Rejected Attempts",
            "",
        ]
        for index, attempt in enumerate(rejected_attempts[:10], start=1):
            lines.extend(
                [
                    f"### Attempt {index}",
                    f"- material_id: `{attempt.get('material_id')}`",
                    f"- pattern_id: `{attempt.get('pattern_id')}`",
                    f"- current_status: `{attempt.get('current_status')}`",
                    f"- judge_score: `{attempt.get('judge_score')}`",
                    "- validation_errors:",
                ]
            )
            errors = attempt.get("validation_errors") or []
            if errors:
                lines.extend([f"  - {error}" for error in errors])
            else:
                lines.append("  - none")
            reason = str(attempt.get("judge_reason") or "").strip()
            if reason:
                lines.extend(["- judge_reason:", reason, ""])
            else:
                lines.append("")
        path.write_text("\n".join(lines), encoding="utf-8")
        return str(path)

    def _resolve_reference_pattern_id(self, *, question_type: str, source_question_analysis: dict) -> str | None:
        if not source_question_analysis:
            return None
        structure_constraints = source_question_analysis.get("structure_constraints") or {}
        business_card_ids = source_question_analysis.get("business_card_ids") or []
        if question_type == "sentence_fill":
            blank_position = str(structure_constraints.get("blank_position") or "")
            function_type = str(structure_constraints.get("function_type") or "")
            if blank_position == "opening":
                return "opening_summary"
            if blank_position == "ending":
                return "ending_summary"
            if function_type in {"carry_previous", "lead_next", "bridge_both_sides"}:
                return "bridge_transition"
            return "comprehensive_multi_match"
        if question_type == "sentence_order":
            logic_modes = set(structure_constraints.get("logic_modes") or [])
            if any(
                card_id in business_card_ids
                for card_id in (
                    "sentence_order__head_tail_lock__abstract",
                    "sentence_order__head_tail_logic__abstract",
                )
            ):
                return "dual_anchor_lock"
            if "timeline_sequence" in logic_modes or "action_sequence" in logic_modes:
                return "carry_parallel_expand"
            if "problem_solution" in logic_modes:
                return "problem_solution_case_blocks"
            if "viewpoint_explanation" in logic_modes:
                return "viewpoint_reason_action"
        return None

    def _apply_control_overrides(self, request_snapshot: dict, control_overrides: dict, instruction: str | None) -> dict:
        snapshot = deepcopy(request_snapshot)
        snapshot.setdefault("type_slots", {})
        snapshot.setdefault("extra_constraints", {})
        for transient_key in ("material_id", "material_text", "manual_patch"):
            snapshot["type_slots"].pop(transient_key, None)

        reserved_keys = {
            "question_type",
            "business_subtype",
            "pattern_id",
            "difficulty_target",
            "difficulty_raise_factor",
            "material_id",
            "material_text",
            "manual_patch",
            "topic",
            "passage_style",
            "extra_constraints",
            "type_slots",
            "use_fewshot",
            "fewshot_mode",
            "material_policy",
        }
        for key in ("question_type", "business_subtype", "pattern_id", "difficulty_target", "topic", "passage_style", "use_fewshot", "fewshot_mode", "material_policy"):
            if key in control_overrides:
                snapshot[key] = control_overrides[key]

        if "extra_constraints" in control_overrides and isinstance(control_overrides["extra_constraints"], dict):
            snapshot["extra_constraints"].update(control_overrides["extra_constraints"])
        if "type_slots" in control_overrides and isinstance(control_overrides["type_slots"], dict):
            snapshot["type_slots"].update(control_overrides["type_slots"])

        direct_slot_overrides = {key: value for key, value in control_overrides.items() if key not in reserved_keys}
        snapshot["type_slots"].update(direct_slot_overrides)

        if control_overrides:
            snapshot["extra_constraints"]["required_review_overrides"] = {
                key: value
                for key, value in control_overrides.items()
                if key not in {"material_id"}
            }
        if instruction:
            snapshot["extra_constraints"]["review_instruction"] = instruction
        return snapshot

    def _material_policy_from_snapshot(self, request_snapshot: dict) -> MaterialPolicy | None:
        raw_policy = request_snapshot.get("material_policy")
        if raw_policy is None or isinstance(raw_policy, MaterialPolicy):
            return raw_policy
        if isinstance(raw_policy, dict):
            return MaterialPolicy.model_validate(raw_policy)
        raise DomainError(
            "request_snapshot.material_policy is invalid.",
            status_code=422,
            details={"material_policy_type": type(raw_policy).__name__},
        )

    def _build_manual_material_selection(self, *, item: dict, text: str) -> MaterialSelectionResult:
        current_material = MaterialSelectionResult.model_validate(item["material_selection"])
        source = dict(current_material.source or {})
        source["manual_material_input"] = True
        source.setdefault("source_name", "manual_input")
        return current_material.model_copy(
            update={
                "material_id": f"manual::{uuid4()}",
                "text": text,
                "original_text": text,
                "source": source,
                "source_tail": current_material.source_tail,
                "selection_reason": "manual_material_override",
                "text_refined": False,
                "refinement_reason": None,
                "anchor_adapted": False,
                "anchor_adaptation_reason": "manual_material_override",
                "anchor_span": {},
                "usage_count_before": 0,
                "previously_used": False,
                "last_used_at": None,
            }
        )

    def _remap_answer_position(self, question: GeneratedQuestion) -> GeneratedQuestion:
        letters = ["A", "B", "C", "D"]
        answer = (question.answer or "").strip().upper()
        if answer not in letters:
            return question

        target_answer = random.SystemRandom().choice(letters)
        if target_answer == answer:
            metadata = dict(question.metadata or {})
            metadata["answer_position"] = target_answer
            return question.model_copy(update={"metadata": metadata})

        old_options = dict(question.options)
        wrong_letters = [letter for letter in letters if letter != answer]
        target_wrong_letters = [letter for letter in letters if letter != target_answer]

        shuffled_wrong_letters = wrong_letters[:]
        random.SystemRandom().shuffle(shuffled_wrong_letters)

        mapping = {answer: target_answer}
        remapped_options = {target_answer: old_options[answer]}
        for source_letter, target_letter in zip(shuffled_wrong_letters, target_wrong_letters, strict=False):
            mapping[source_letter] = target_letter
            remapped_options[target_letter] = old_options[source_letter]

        ordered_options = {letter: remapped_options[letter] for letter in letters if letter in remapped_options}
        remapped_analysis = self._remap_option_references(question.analysis, mapping)
        metadata = dict(question.metadata or {})
        metadata["answer_position_before_shuffle"] = answer
        metadata["answer_position"] = target_answer
        metadata["option_letter_mapping"] = mapping
        return question.model_copy(
            update={
                "options": ordered_options,
                "answer": target_answer,
                "analysis": remapped_analysis,
                "metadata": metadata,
            }
        )

    def _remap_option_references(self, text: str, mapping: dict[str, str]) -> str:
        if not text:
            return text
        placeholders = {letter: f"__OPTION_{index}__" for index, letter in enumerate(mapping.keys(), start=1)}
        remapped = text
        for source, placeholder in placeholders.items():
            remapped = re.sub(
                rf"(?<![A-Z0-9]){re.escape(source)}(?![A-Z0-9])",
                placeholder,
                remapped,
            )
        for source, placeholder in placeholders.items():
            remapped = remapped.replace(placeholder, mapping[source])
        return remapped
        remapped = text
        placeholders = {letter: f"__OPTION_{index}__" for index, letter in enumerate(mapping.keys(), start=1)}
        for source, placeholder in placeholders.items():
            remapped = re.sub(rf"(?<![A-Z]){source}(?=\s*项)", placeholder, remapped)
            remapped = re.sub(rf"(?<![A-Z])选项\s*{source}(?![A-Z])", f"选项 {placeholder}", remapped)
            remapped = re.sub(rf"(?<![A-Z]){source}(?=\s*选项)", placeholder, remapped)
            remapped = re.sub(rf"(?<![A-Z])\({source}\)", f"({placeholder})", remapped)
            remapped = re.sub(rf"(?<![A-Z])（{source}）", f"（{placeholder}）", remapped)
            remapped = re.sub(rf"(?<![A-Z]){source}(?=\s*[（(])", placeholder, remapped)
            remapped = re.sub(
                rf"(正确答案(?:为|是)?\s*){source}(?![A-Z])",
                rf"\1{placeholder}",
                remapped,
            )
            remapped = re.sub(
                rf"(答案(?:为|是)?\s*){source}(?![A-Z])",
                rf"\1{placeholder}",
                remapped,
            )
            remapped = re.sub(rf"(故选\s*){source}(?![A-Z])", rf"\1{placeholder}", remapped)
            remapped = re.sub(rf"(应选\s*){source}(?![A-Z])", rf"\1{placeholder}", remapped)
        for source, placeholder in placeholders.items():
            remapped = remapped.replace(placeholder, mapping[source])
        return remapped

    def _build_version_record(
        self,
        *,
        item: dict,
        source_action: str,
        parent_version_no: int | None,
        version_no: int,
        target_difficulty: str | None,
        material: MaterialSelectionResult,
        prompt_template: PromptTemplateRecord,
        raw_model_output: dict | None,
        parsed_structured_output: dict | None,
        parse_error: str | None,
        validation_result: dict,
        evaluation_result: dict,
        runtime_snapshot: dict,
    ) -> dict:
        generated_question = item.get("generated_question") or {}
        return {
            "version_id": str(uuid4()),
            "item_id": item["item_id"],
            "version_no": version_no,
            "parent_version_no": parent_version_no,
            "source_action": source_action,
            "target_difficulty": target_difficulty,
            "material_id": material.material_id,
            "prompt_template_name": prompt_template.template_name,
            "prompt_template_version": prompt_template.template_version,
            "stem": generated_question.get("stem"),
            "options": generated_question.get("options", {}),
            "answer": generated_question.get("answer"),
            "analysis": generated_question.get("analysis"),
            "prompt_package": item.get("prompt_package", {}),
            "prompt_render_snapshot": runtime_snapshot.get("prompt_snapshot", {}),
            "raw_model_output": raw_model_output,
            "parsed_structured_output": parsed_structured_output,
            "parse_error": parse_error,
            "validation_result": validation_result,
            "evaluation_result": evaluation_result,
            "runtime_snapshot": runtime_snapshot,
            "created_at": self.repository._utc_now(),
        }

    def _annotate_material_usage(self, material: MaterialSelectionResult) -> MaterialSelectionResult:
        usage = self.repository.get_material_usage_stats(material.material_id)
        usage_note = None
        if usage["previously_used"]:
            usage_note = f"This material has been used {usage['usage_count_before']} time(s) before in this system."
        cleaned_text = self._clean_material_text(material.text)
        return material.model_copy(
            update={
                "original_text": material.original_text or material.text,
                "text": cleaned_text,
                "usage_count_before": usage["usage_count_before"],
                "previously_used": usage["previously_used"],
                "last_used_at": usage["last_used_at"],
                "usage_note": usage_note,
            }
        )

    def _refine_material_if_needed(self, material: MaterialSelectionResult) -> MaterialSelectionResult:
        if not self._needs_material_refinement(material.text):
            return material

        try:
            response = self.llm_gateway.generate_json(
                route=self._material_refinement_route(),
                system_prompt=(
                    "你是一名材料轻修助手。请对输入文段做最小必要的语言修整，使其可以作为独立可读的出题材料。"
                    "只能修复明显的断裂、重复、代词悬空、开头突兀、标签残留和拼接痕迹。"
                    "不得增加原文没有的事实、背景、判断和立场，不得扩写。"
                    "如果信息不足以完整补齐背景，也只能做最小平滑处理。"
                ),
                user_prompt="\n\n".join(
                    [
                        "请对下面文段做最小必要精修，使其更像可独立阅读的一段材料。",
                        "请顺手去掉【关键词】【事件】【点评】这类不适合直接给人阅读的模板标签。",
                        "只修复断裂、重复、开头突兀和明显拼接问题，不要新增信息。",
                        "[原始文段]",
                        material.text,
                    ]
                ),
                schema_name="material_refinement",
                schema=MaterialRefinementDraft.model_json_schema(),
            )
            refined = self.material_refinement_adapter.validate_python(response)
            refined_text = self._clean_material_text(refined.refined_text)
            if refined_text and (refined.changed or refined_text != material.text):
                return material.model_copy(
                    update={
                        "text": refined_text,
                        "text_refined": True,
                        "refinement_reason": refined.reason or "llm_light_refinement",
                    }
                )
        except Exception:  # noqa: BLE001
            return material
        return material

    def _clean_material_text(self, text: str) -> str:
        normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        if not normalized:
            return ""

        normalized = self._strip_material_template_labels(normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()

        if normalized.count("【关键词】") > 1:
            blocks = [part.strip() for part in re.split(r"(?=【关键词】)", normalized) if part.strip()]
            unique_blocks: list[str] = []
            block_signatures: list[str] = []
            for block in blocks:
                signature = re.sub(r"\s+", "", block)
                if not signature:
                    continue
                if any(
                    signature in existing
                    or existing in signature
                    or SequenceMatcher(None, signature, existing).ratio() >= 0.88
                    for existing in block_signatures
                ):
                    continue
                block_signatures.append(signature)
                unique_blocks.append(block)
            normalized = "\n\n".join(unique_blocks).strip()

        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
        deduped: list[str] = []
        seen_signatures: list[str] = []

        for paragraph in paragraphs:
            signature = re.sub(r"\s+", "", paragraph)
            if not signature:
                continue
            if signature in seen_signatures:
                continue
            if any(
                signature in existing or existing in signature or SequenceMatcher(None, signature, existing).ratio() >= 0.94
                for existing in seen_signatures
            ):
                continue
            seen_signatures.append(signature)
            deduped.append(paragraph)

        cleaned = "\n\n".join(deduped).strip()
        return cleaned or normalized

    def _strip_material_template_labels(self, text: str) -> str:
        lines = [line.strip() for line in text.split("\n")]
        cleaned_lines: list[str] = []
        pending_event_label = False

        for line in lines:
            if not line:
                cleaned_lines.append("")
                pending_event_label = False
                continue

            if re.fullmatch(r"【(?:关键词|点评|延伸阅读|案例|来源)】.*", line):
                if line.startswith("【事件】") and len(line) > len("【事件】"):
                    cleaned_lines.append(line.replace("【事件】", "", 1).strip())
                pending_event_label = False
                continue

            if line == "【事件】":
                pending_event_label = True
                continue

            if pending_event_label:
                cleaned_lines.append(line)
                pending_event_label = False
                continue

            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"(?:\n\s*){3,}", "\n\n", cleaned).strip()
        return cleaned

    def _needs_material_refinement(self, text: str) -> bool:
        clean = (text or "").strip()
        if not clean:
            return False
        if "[BLANK]" in clean or "____" in clean or "___" in clean:
            return False
        opening = clean[:120]
        suspicious_openings = (
            "大会取得丰硕成果",
            "我们对大会的成功表示热烈祝贺",
            "预祝大会圆满成功",
            "这次大会",
            "本次会议",
            "会议高度评价",
        )
        if any(opening.startswith(marker) for marker in suspicious_openings):
            return True
        if "【关键词】" in clean or "【事件】" in clean:
            return True
        if "【点评】" in clean or "【案例】" in clean or "【延伸阅读】" in clean:
            return True
        if clean.count("预祝大会圆满成功") > 1:
            return True
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", clean) if part.strip()]
        if len(paragraphs) >= 2:
            first = re.sub(r"\s+", "", paragraphs[0])
            second = re.sub(r"\s+", "", paragraphs[1])
            if first and second and (first in second or second in first or SequenceMatcher(None, first, second).ratio() >= 0.9):
                return True
        if len(clean) >= 120 and not re.search(r"[。！？!?]$", clean):
            return True
        return False

    def _resolve_template(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        action_type: str,
    ) -> PromptTemplateRecord:
        return self.prompt_template_registry.resolve_default(
            question_type=question_type,
            business_subtype=business_subtype,
            action_type=action_type,
        )

    def list_replacement_materials(self, item: dict, *, limit: int = 8) -> list[dict]:
        material_selection = item.get("material_selection") or {}
        request_snapshot = item.get("request_snapshot") or {}
        preferred_genre = (
            ((request_snapshot.get("material_policy") or {}).get("preferred_document_genres") or [None])[0]
            or material_selection.get("document_genre")
        )
        source_question_analysis = request_snapshot.get("source_question_analysis") or {}
        candidates = self.material_bridge.list_material_options(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            document_genre=preferred_genre,
            material_structure_label=(material_selection.get("material_structure_label") or None),
            business_card_ids=source_question_analysis.get("business_card_ids") or [],
            query_terms=source_question_analysis.get("query_terms") or [],
            target_length=source_question_analysis.get("target_length"),
            length_tolerance=source_question_analysis.get("length_tolerance", 120),
            structure_constraints=source_question_analysis.get("structure_constraints") or {},
            enable_anchor_adaptation=bool(source_question_analysis),
            exclude_material_ids={material_selection.get("material_id")} if material_selection.get("material_id") else None,
            limit=limit,
            difficulty_target=item.get("difficulty_target", "medium"),
            usage_stats_lookup=self.repository.get_material_usage_stats,
        )
        return [
                {
                    "material_id": candidate.material_id,
                    "label": f"{(candidate.source.get('article_title') or candidate.source.get('source_name') or candidate.material_id)}",
                    "article_title": candidate.source.get("article_title"),
                    "source_name": candidate.source.get("source_name") or candidate.source.get("source_id"),
                    "document_genre": candidate.document_genre,
                    "text_preview": candidate.text[:120],
                    "material_text": candidate.text,
                    "usage_count_before": candidate.usage_count_before,
                }
            for candidate in candidates
        ]

    def _build_generation_prompt_sections(
        self,
        *,
        built_item: dict,
        material: MaterialSelectionResult,
        prompt_package: dict,
        feedback_notes: list[str] | None = None,
    ) -> list[str]:
        sections = [
            prompt_package["user_prompt"],
            "[Selected Material]",
            material.text,
            "[Original Material Evidence]",
            material.original_text or material.text,
            "[Material Meta]",
            f"material_id={material.material_id}; article_id={material.article_id}; reason={material.selection_reason}",
        ]
        sections.extend(
            [
                "[Material Readability Contract]",
                *self._material_readability_contract_lines(),
            ]
        )
        material_prompt_extras = ((material.source or {}).get("prompt_extras") or {}) if isinstance(material.source, dict) else {}
        if material_prompt_extras:
            sections.extend(
                [
                    "[Material Prompt Extras]",
                    str(material_prompt_extras),
                ]
            )
        request_snapshot = built_item.get("request_snapshot") or {}
        source_question = request_snapshot.get("source_question") or {}
        source_question_analysis = request_snapshot.get("source_question_analysis") or {}
        if source_question:
            reference_payload = deepcopy(source_question)
            if isinstance(reference_payload.get("passage"), str) and len(reference_payload["passage"]) > 600:
                reference_payload["passage"] = f"{reference_payload['passage'][:600]}...(truncated)"
            if "analysis" in reference_payload:
                reference_payload["analysis"] = "[omitted_for_structure_only_fewshot]"
            sections.extend(
                [
                    "[Reference Question Template]",
                    str(reference_payload),
                    "[Reference Question Analysis]",
                    str(source_question_analysis),
                    "Treat the reference question as a structure-only few-shot template. Align with its stem style, option granularity, reasoning path, and written tone, but do not copy wording.",
                    "Do not copy or paraphrase the reference question's topical content, examples, terminology, or explanation text into the new analysis.",
                    "If the reference question contains an explanation, reuse only its explanation structure and elimination order, not its content wording or topic-specific claims.",
                    "Prioritize discourse structure and reasoning shape over topical overlap. Topic may change, but the question-making logic should stay parallel.",
                ]
            )
            structure_constraints = source_question_analysis.get("structure_constraints") or {}
            hard_constraints = self._build_reference_hard_constraints(
                question_type=built_item.get("question_type"),
                structure_constraints=structure_constraints,
            )
            if hard_constraints:
                sections.extend(
                    [
                        "[Hard Constraints]",
                        *hard_constraints,
                    ]
                )
            grounding_rules = self._build_answer_grounding_rules(
                question_type=built_item.get("question_type"),
                source_question=source_question,
            )
            if grounding_rules:
                sections.extend(
                    [
                        "[Answer Grounding Contract]",
                        *grounding_rules,
                    ]
                )
        answer_anchor_text = str(material_prompt_extras.get("answer_anchor_text") or "").strip()
        if answer_anchor_text:
            sections.extend(
                [
                    "[Material Answer Anchor]",
                    answer_anchor_text,
                    "For this item, the correct answer must stay semantically equivalent to the removed source span above. Do not invent a new answer basis.",
                ]
            )
        if feedback_notes:
            sections.extend(
                [
                    "[Repair Requirements]",
                    *feedback_notes,
                ]
            )
        sections.append("Use the provided material as the source passage and generate exactly one question.")
        return sections

    def _material_readability_contract_lines(self) -> list[str]:
        return [
            "The final displayed material must read like a natural, formal Chinese passage excerpt that a human can read directly.",
            "Do not output bizarre symbols, duplicated fragments, machine-stitched paragraphs, broken quotations, or half-finished enumerations.",
            "Prefer preserving the original sentence order and wording. Only make the smallest necessary repairs for readability and coherence.",
            "If the selected material cannot be safely polished without changing its evidence basis, keep the material conservative rather than aggressively rewriting it.",
        ]

    def _build_answer_grounding_rules(self, *, question_type: str | None, source_question: dict[str, object]) -> list[str]:
        if not source_question:
            return []
        rules = [
            "The answer basis must be traceable to the original material evidence above, not invented by the model.",
            "You may compress or paraphrase, but do not add any new stance, causal chain, countermeasure, conclusion, or evaluative claim not supported by the original material.",
            "The correct option must be defensible from the original material alone. Distractors may only come from non-key details, scope shifts, partial readings, or unsupported extensions.",
            "The analysis must explain the answer using evidence from the original material, and should mirror the reference question's elimination style when possible.",
        ]
        if question_type == "sentence_order":
            rules.extend(
                [
                    "Do not rewrite the sortable units into a different set of sentences. Keep the same sortable units and only ask about their order.",
                    "Use the mother-question style for ordering items: preserve sentence-level units and reason about first sentence, tail sentence, bindings, and sequence clues.",
                    "If the material is slightly weak, you may lightly sharpen anchor words or uniqueness cues inside the existing units, but do not change unit count, unit order, factual meaning, or discourse skeleton.",
                    "Any repair to ordering clues must be minimal and local: strengthen opener/tail/binding markers only when they are already latent in the original material evidence.",
                ]
            )
        elif question_type == "sentence_fill":
            rules.extend(
                [
                    "Do not invent a blank sentence whose core meaning is absent from the original material. The correct option must fit the blank position and be supported by the original context.",
                    "Keep the blanked passage readable and in mother-question style. The blank should replace only the target sentence or clause, not the surrounding evidence chain.",
                ]
            )
        else:
            rules.extend(
                [
                    "For main-idea or title-style items, the correct option must stay within the central meaning already present in the original material.",
                    "Do not turn a detail into the correct answer, and do not create a stronger conclusion than the passage itself supports.",
                ]
            )
        return rules

    def _apply_evaluation_gate(
        self,
        *,
        validation_result,
        evaluation_result: dict[str, object] | None,
        difficulty_target: str,
    ) -> list[str]:
        if not evaluation_result:
            return []

        overall_score = self._normalize_judge_score(evaluation_result.get("overall_score"))
        material_alignment = self._normalize_judge_score(evaluation_result.get("material_alignment"))
        answer_analysis_consistency = self._normalize_judge_score(evaluation_result.get("answer_analysis_consistency"))
        difficulty_fit_score = self._normalize_judge_score(evaluation_result.get("difficulty_fit"))

        errors: list[str] = []
        if overall_score and overall_score < self.JUDGE_OVERALL_PASS_THRESHOLD:
            errors.append("llm_judge_overall_score_too_low")
        if material_alignment and material_alignment < self.JUDGE_MATERIAL_ALIGNMENT_THRESHOLD:
            errors.append("llm_judge_material_alignment_too_low")
        if answer_analysis_consistency and answer_analysis_consistency < self.JUDGE_ANSWER_ANALYSIS_THRESHOLD:
            errors.append("llm_judge_answer_analysis_consistency_too_low")
        if difficulty_target == "hard" and difficulty_fit_score and difficulty_fit_score < self.JUDGE_HARD_DIFFICULTY_THRESHOLD:
            errors.append("llm_judge_difficulty_fit_too_low_for_hard_target")

        if not errors:
            return []

        existing_errors = list(validation_result.errors or [])
        for error in errors:
            if error not in existing_errors:
                existing_errors.append(error)
        validation_result.errors = existing_errors
        validation_result.passed = False
        validation_result.validation_status = "failed"
        validation_result.next_review_status = "needs_revision"
        validation_result.score = max(0, min(int(validation_result.score or 100), int(overall_score) if overall_score else 60))
        return errors

    def _normalize_judge_score(self, value: object) -> float:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if 0.0 <= numeric <= 1.0:
            return round(numeric * 100, 2)
        return numeric

    def _effective_difficulty_target(self, difficulty_target: str, *, use_reference_question: bool) -> str:
        if not use_reference_question:
            return difficulty_target
        return {
            "easy": "medium",
            "medium": "hard",
            "hard": "hard",
        }.get(difficulty_target, difficulty_target)

    def _build_reference_hard_constraints(self, *, question_type: str | None, structure_constraints: dict[str, object]) -> list[str]:
        if not question_type or not structure_constraints:
            return []
        if question_type == "sentence_order":
            unit_count = int(structure_constraints.get("sortable_unit_count") or 0) or 6
            logic_modes = list(structure_constraints.get("logic_modes") or [])
            binding_types = list(structure_constraints.get("binding_types") or [])
            lines = [
                "Keep the generated question as a sentence-ordering item, not another question type.",
                "For sentence-order repair, you may do minimal cue sharpening inside existing units, but you must preserve unit order, unit count, and the original evidence meaning.",
                "Render the ordering material as exactly six sortable sentence units whenever the reference skeleton is a standard six-sentence ordering item.",
            ]
            if unit_count:
                lines.append(f"Preserve the sortable unit count from the reference question: exactly {unit_count} units. Do not shrink 6 sentences into 3.")
            if logic_modes:
                lines.append(f"Preserve the main ordering logic from the reference question: {', '.join(logic_modes)}.")
            if binding_types:
                lines.append(f"Analysis should explicitly explain these ordering clues when relevant: {', '.join(binding_types)}.")
            return lines
        if question_type == "sentence_fill":
            blank_position = str(structure_constraints.get("blank_position") or "")
            function_type = str(structure_constraints.get("function_type") or "")
            unit_type = str(structure_constraints.get("unit_type") or "")
            lines = [
                "Keep the generated question as a sentence-fill item, not another question type.",
            ]
            if blank_position:
                lines.append(f"Preserve the blank position from the reference question: {blank_position}.")
            if function_type:
                lines.append(f"Preserve the blank-function direction from the reference question: {function_type}.")
            if unit_type:
                lines.append(f"Prefer the same blank unit granularity as the reference question: {unit_type}.")
            return lines
        return []

    def _should_retry_alignment(self, validation_result, source_question_analysis: dict | None) -> bool:
        if not source_question_analysis:
            return False
        if validation_result is None:
            return False
        checks = validation_result.checks or {}
        if not validation_result.passed:
            return True
        for check_name in (
            "sentence_order_reference_unit_alignment",
            "sentence_fill_blank_position_alignment",
            "analysis_answer_consistency",
            "reference_answer_grounding",
            "analysis_material_grounding",
        ):
            check = checks.get(check_name) or {}
            if check and not check.get("passed", True):
                return True
        return False

    def _build_alignment_feedback_notes(self, validation_result, source_question_analysis: dict | None) -> list[str]:
        notes: list[str] = [
            "Regenerate the question and strictly fix the alignment issues below.",
            "Do not change the question family. Keep the discourse logic parallel to the reference question.",
        ]
        if source_question_analysis:
            structure_constraints = source_question_analysis.get("structure_constraints") or {}
            notes.extend(
                self._build_reference_hard_constraints(
                    question_type=str((source_question_analysis.get("style_summary") or {}).get("question_type") or ""),
                    structure_constraints=structure_constraints,
                )
            )
        notes.extend(validation_result.errors[:4])
        if not validation_result.errors:
            notes.extend(validation_result.warnings[:3])
        return list(dict.fromkeys(note for note in notes if note))

    def _should_retry_quality_repair(
        self,
        *,
        validation_result,
        quality_gate_errors: list[str],
        evaluation_result: dict | None,
        source_question_analysis: dict | None,
    ) -> bool:
        if not source_question_analysis:
            return False
        if quality_gate_errors:
            return True
        if validation_result is not None and not validation_result.passed:
            return True
        overall_score = self._normalize_judge_score((evaluation_result or {}).get("overall_score"))
        return bool(overall_score and overall_score < self.QUALITY_REPAIR_RETRY_THRESHOLD)

    def _build_quality_repair_feedback_notes(
        self,
        *,
        validation_result,
        evaluation_result: dict | None,
        quality_gate_errors: list[str],
        source_question_analysis: dict | None,
    ) -> list[str]:
        notes: list[str] = [
            "The previous attempt was rejected. Regenerate once and repair the exact issues below.",
            "Do not improvise a new discourse shape. Stay close to the reference question's unit count, logic skeleton, and elimination style.",
        ]
        style_question_type = str((source_question_analysis or {}).get("style_summary", {}).get("question_type") or "")
        if style_question_type == "sentence_order":
            notes.extend(
                [
                    "For sentence-order repair only: if needed, minimally strengthen anchor words, opener cues, tail cues, or binding cues inside the existing sortable units.",
                    "Do not add or remove units. Do not reorder the source units. Do not invent new facts or a new discourse path. Only make the uniqueness clues easier to read.",
                ]
            )
        if source_question_analysis:
            structure_constraints = source_question_analysis.get("structure_constraints") or {}
            notes.extend(
                self._build_reference_hard_constraints(
                    question_type=style_question_type,
                    structure_constraints=structure_constraints,
                )
            )
        notes.extend(validation_result.errors[:4])
        notes.extend(quality_gate_errors[:4])
        judge_reason = str((evaluation_result or {}).get("judge_reason") or "").strip()
        if judge_reason:
            notes.append(f"Reviewer feedback: {judge_reason}")
        if not validation_result.errors:
            notes.extend(validation_result.warnings[:3])
        return list(dict.fromkeys(note for note in notes if note))

    def _should_accept_quality_retry(
        self,
        *,
        current_validation_result,
        current_evaluation_result: dict | None,
        repaired_validation_result,
        repaired_evaluation_result: dict | None,
        repaired_quality_gate_errors: list[str],
    ) -> bool:
        if repaired_validation_result.passed and not repaired_quality_gate_errors:
            return True
        current_error_count = len((current_validation_result.errors or [])) + len(current_validation_result.warnings or [])
        repaired_error_count = len((repaired_validation_result.errors or [])) + len(repaired_validation_result.warnings or [])
        current_score = self._normalize_judge_score((current_evaluation_result or {}).get("overall_score"))
        repaired_score = self._normalize_judge_score((repaired_evaluation_result or {}).get("overall_score"))
        return repaired_error_count < current_error_count or repaired_score > current_score + 6

    def _prioritize_material_candidates(
        self,
        materials: list[MaterialSelectionResult],
        *,
        question_type: str,
        source_question_analysis: dict | None,
    ) -> list[MaterialSelectionResult]:
        if question_type != "sentence_order":
            return materials
        structure_constraints = (source_question_analysis or {}).get("structure_constraints") or {}
        reference_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
        if reference_unit_count <= 0:
            return materials

        exact_matches: list[MaterialSelectionResult] = []
        sufficient_matches: list[MaterialSelectionResult] = []
        near_matches: list[MaterialSelectionResult] = []
        others: list[MaterialSelectionResult] = []
        for material in materials:
            unit_count = self._best_sortable_unit_count(material)
            if unit_count == reference_unit_count:
                exact_matches.append(material)
            elif unit_count > reference_unit_count:
                sufficient_matches.append(material)
            elif abs(unit_count - reference_unit_count) == 1:
                near_matches.append(material)
            else:
                others.append(material)
        if exact_matches:
            return exact_matches + sufficient_matches + near_matches + others
        if sufficient_matches:
            return sufficient_matches + near_matches + others
        return near_matches + others

    def _sentence_order_target_unit_count(self, source_question_analysis: dict | None) -> int:
        return 6

    def _extract_sortable_units_from_text(self, text: str) -> list[str]:
        raw = (text or "").strip()
        if not raw:
            return []
        normalized = raw.replace("\r\n", "\n").strip()
        enumerated = re.split(r"(?=[①②③④⑤⑥⑦⑧⑨⑩])", normalized)
        enumerated = [part.strip() for part in enumerated if part.strip()]
        if len(enumerated) >= 2:
            cleaned: list[str] = []
            for part in enumerated:
                cleaned.append(re.sub(r"^[①②③④⑤⑥⑦⑧⑨⑩]\s*", "", part).strip())
            return [part for part in cleaned if part]
        return [item.strip() for item in re.split(r"(?<=[。！？!?；;])\s*|\n+", normalized) if item.strip()]

    def _format_sortable_units(self, units: list[str]) -> str:
        circled = "①②③④⑤⑥⑦⑧⑨⑩"
        return "\n".join(f"{circled[index] if index < len(circled) else f'{index + 1}.'} {unit}" for index, unit in enumerate(units))

    def _best_sortable_unit_count(self, material: MaterialSelectionResult) -> int:
        counts = [
            self._count_sortable_units_from_material(material.text),
            self._count_sortable_units_from_material(material.original_text or ""),
        ]
        return max(counts)

    def _coerce_sentence_order_material(
        self,
        *,
        material: MaterialSelectionResult,
        source_question_analysis: dict | None,
    ) -> MaterialSelectionResult | None:
        target_count = self._sentence_order_target_unit_count(source_question_analysis)
        candidate_sources = [material.original_text or "", material.text]
        best_units: list[str] = []
        for source_text in candidate_sources:
            units = self._extract_sortable_units_from_text(source_text)
            if len(units) >= target_count:
                best_units = units[:target_count]
                break
            if len(units) > len(best_units):
                best_units = units
        if len(best_units) < target_count:
            return None
        coerced_text = self._format_sortable_units(best_units)
        if coerced_text == material.text:
            return material
        return material.model_copy(
            update={
                "text": coerced_text,
                "text_refined": True,
                "refinement_reason": f"sentence_order_unit_coercion::{target_count}",
            }
        )

    def _enforce_sentence_order_six_unit_output(self, generated_question: GeneratedQuestion) -> GeneratedQuestion:
        expected_markers = ["①", "②", "③", "④", "⑤", "⑥"]
        stem = (generated_question.stem or "").strip()
        if stem:
            stem = re.sub(r"将[以上下列些]*\d+个句子重新排列[，,]?\s*语序正确的一项是[:：]?", "将以下6个句子重新排列，语序正确的一项是：", stem)
            stem = re.sub(r"将[以上下列些]*[一二三四五六七八九十]+个句子重新排列[，,]?\s*语序正确的一项是[:：]?", "将以下6个句子重新排列，语序正确的一项是：", stem)
            if "重新排列" in stem and "6个句子" not in stem:
                stem = "将以下6个句子重新排列，语序正确的一项是："

        normalized_options: dict[str, str] = {}
        for key, value in (generated_question.options or {}).items():
            text = str(value or "").strip()
            circled = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", text)
            if circled:
                cleaned: list[str] = []
                for marker in circled:
                    if marker in expected_markers and marker not in cleaned:
                        cleaned.append(marker)
                normalized_options[key] = "".join(cleaned[:6]) if cleaned else text
            else:
                normalized_options[key] = text

        analysis = str(generated_question.analysis or "")
        analysis = re.sub(r"(\d+)个句子", lambda m: "6个句子" if m.group(1) != "6" else m.group(0), analysis)
        analysis = re.sub(r"([七八九十7-9])句", "6句", analysis)
        analysis = re.sub(r"[⑦⑧⑨⑩]", "", analysis)

        return generated_question.model_copy(
            update={
                "stem": stem,
                "options": normalized_options,
                "analysis": analysis.strip(),
            }
        )

    def _count_sortable_units_from_material(self, material_text: str) -> int:
        text = (material_text or "").strip()
        if not text:
            return 0
        sortable_block = text.split("\n\n")[-1].strip()
        enumerated = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", sortable_block)
        if enumerated:
            return len(set(enumerated))
        sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", sortable_block) if item.strip()]
        return len(sentences)

    def _build_reference_source_material(self, source_question) -> MaterialSelectionResult:
        passage = (source_question.passage or "").strip()
        stem = (source_question.stem or "").strip()
        return MaterialSelectionResult(
            material_id=f"reference_source::{uuid4().hex}",
            article_id="reference_source_question",
            text=passage,
            original_text=passage,
            source={
                "source_name": "reference_source_question",
                "article_title": stem or "参考母题原文",
                "source_id": "reference_source_question",
            },
            document_genre="reference",
            material_structure_label="reference_source",
            standalone_readability=0.95,
            quality_score=0.95,
            selection_reason="reference_source_fallback",
        )

    # Clean override implementations for material cleanup and refinement.
    # These are intentionally placed after the older methods so Python uses these
    # definitions at runtime even if earlier legacy text blocks remain in file history.
    def _refine_material_if_needed(self, material: MaterialSelectionResult) -> MaterialSelectionResult:
        cleaned_seed = self._clean_material_text(material.text)
        if not cleaned_seed:
            return material

        cleaned_material = material
        if cleaned_seed != material.text:
            cleaned_material = material.model_copy(
                update={
                    "text": cleaned_seed,
                    "text_refined": True,
                    "refinement_reason": "rule_cleanup",
                }
            )

        if not self._needs_material_refinement(cleaned_seed):
            return cleaned_material

        try:
            response = self.llm_gateway.generate_json(
                route=self._material_refinement_route(),
                system_prompt=(
                    "你是一名正式文稿润色助手，只负责对材料做保守、最小限度的顺滑修整。"
                    "目标是让文段像正式公开发表的中文文稿摘录，语句完整、自然、可供人类直接阅读。"
                    "只能处理明显的重复、拼接痕迹、模板标签、代词悬空、半截枚举和断裂衔接。"
                    "必须尽量保留原句、原顺序、原信息重心，不得扩写，不得换论点，不得补充原文没有的事实、背景、判断和立场。"
                    "如果无法在不改动证据基础的前提下可靠修复，就返回清理后的原文，不要硬改。"
                ),
                user_prompt="\n\n".join(
                    [
                        "请对下面文段做最小必要精修。",
                        "要求：修完后必须像正式文稿中的自然段落，不能出现奇怪字符、重复句、半截句、拼接腔或机器改写痕迹。",
                        "优先保留原句和原顺序；只做去重、去模板标签、补足最小衔接、修正明显断裂。",
                        "如果原文本身已经基本可读，请尽量保持不变；如果无法可靠修复，请返回“清理后基底文段”的原貌。",
                        "[清理后基底文段]",
                        cleaned_seed,
                        "[原始文段]",
                        material.text,
                    ]
                ),
                schema_name="material_refinement",
                schema=MaterialRefinementDraft.model_json_schema(),
            )
            refined = self.material_refinement_adapter.validate_python(response)
            refined_text = self._clean_material_text(refined.refined_text)
            if refined_text and self._is_safe_material_refinement(cleaned_seed, refined_text):
                if refined.changed or refined_text != cleaned_seed:
                    return material.model_copy(
                        update={
                            "text": refined_text,
                            "text_refined": True,
                            "refinement_reason": refined.reason or "llm_light_refinement",
                        }
                    )
                return cleaned_material
        except Exception:  # noqa: BLE001
            return cleaned_material
        return cleaned_material

    def _clean_material_text(self, text: str) -> str:
        normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        if not normalized:
            return ""

        normalized = self._strip_material_template_labels(normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()

        if normalized.count("【关键词】") > 1:
            blocks = [part.strip() for part in re.split(r"(?=【关键词】)", normalized) if part.strip()]
            unique_blocks: list[str] = []
            block_signatures: list[str] = []
            for block in blocks:
                signature = re.sub(r"\s+", "", block)
                if not signature:
                    continue
                if any(
                    signature in existing
                    or existing in signature
                    or SequenceMatcher(None, signature, existing).ratio() >= 0.88
                    for existing in block_signatures
                ):
                    continue
                block_signatures.append(signature)
                unique_blocks.append(block)
            normalized = "\n\n".join(unique_blocks).strip()

        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
        deduped: list[str] = []
        seen_signatures: list[str] = []
        for paragraph in paragraphs:
            signature = re.sub(r"\s+", "", paragraph)
            if not signature:
                continue
            if signature in seen_signatures:
                continue
            if any(
                signature in existing or existing in signature or SequenceMatcher(None, signature, existing).ratio() >= 0.94
                for existing in seen_signatures
            ):
                continue
            seen_signatures.append(signature)
            deduped.append(paragraph)

        cleaned = "\n\n".join(deduped).strip() or normalized
        cleaned = self._dedupe_repeated_sentences(cleaned)
        cleaned = re.sub(r"(提供了一个重要观察维度——)\s*一个是", r"\1", cleaned)
        cleaned = re.sub(r"[，、；：]\s*$", "", cleaned)
        return cleaned

    def _dedupe_repeated_sentences(self, text: str) -> str:
        units = [part.strip() for part in re.split(r"(?<=[。！？!?；;])\s*|\n+", text or "") if part.strip()]
        if len(units) <= 1:
            return (text or "").strip()

        kept: list[str] = []
        seen_signatures: list[str] = []
        for unit in units:
            signature = re.sub(r"\s+", "", unit)
            if len(signature) < 10:
                kept.append(unit)
                continue
            if any(signature == existing or signature in existing or existing in signature for existing in seen_signatures):
                continue
            seen_signatures.append(signature)
            kept.append(unit)
        return "\n".join(kept).strip()

    def _is_safe_material_refinement(self, original_text: str, refined_text: str) -> bool:
        original = (original_text or "").strip()
        refined = (refined_text or "").strip()
        if not original or not refined:
            return False
        if any(marker in refined for marker in ("【关键词】", "【事件】", "【点评】", "【案例】", "【延伸阅读】")):
            return False
        if len(refined) < max(40, int(len(original) * 0.7)):
            return False
        if len(refined) > int(len(original) * 1.2):
            return False
        if self._has_dense_duplicate_units(refined):
            return False
        if len(refined) >= 80 and not re.search(r"[。！？!?]$", refined):
            return False
        return True

    def _has_dense_duplicate_units(self, text: str) -> bool:
        units = [part.strip() for part in re.split(r"(?<=[。！？!?；;])\s*|\n+", text or "") if part.strip()]
        signatures: list[str] = []
        for unit in units:
            signature = re.sub(r"\s+", "", unit)
            if len(signature) < 12:
                continue
            if any(
                signature == existing
                or signature in existing
                or existing in signature
                or SequenceMatcher(None, signature, existing).ratio() >= 0.94
                for existing in signatures
            ):
                return True
            signatures.append(signature)
        return False

    def _strip_material_template_labels(self, text: str) -> str:
        lines = [line.strip() for line in text.split("\n")]
        cleaned_lines: list[str] = []
        pending_event_label = False

        for line in lines:
            if not line:
                cleaned_lines.append("")
                pending_event_label = False
                continue

            if re.fullmatch(r"【(?:关键词|点评|延伸阅读|案例|来源)】.*", line):
                if line.startswith("【事件】") and len(line) > len("【事件】"):
                    cleaned_lines.append(line.replace("【事件】", "", 1).strip())
                pending_event_label = False
                continue

            if line == "【事件】":
                pending_event_label = True
                continue

            if pending_event_label:
                cleaned_lines.append(line)
                pending_event_label = False
                continue

            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        return re.sub(r"(?:\n\s*){3,}", "\n\n", cleaned).strip()

    def _needs_material_refinement(self, text: str) -> bool:
        clean = (text or "").strip()
        if not clean:
            return False
        if "[BLANK]" in clean or "____" in clean or "___" in clean:
            return False

        opening = clean[:120]
        suspicious_openings = (
            "大会取得丰硕成果",
            "我们对大会的成功表示热烈祝贺",
            "预祝大会圆满成功",
            "这次大会",
            "本次会议",
            "会议高度评价",
        )
        if any(opening.startswith(marker) for marker in suspicious_openings):
            return True
        if "【关键词】" in clean or "【事件】" in clean or "【点评】" in clean or "【案例】" in clean or "【延伸阅读】" in clean:
            return True
        if clean.count("预祝大会圆满成功") > 1:
            return True
        if self._has_dense_duplicate_units(clean):
            return True

        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", clean) if part.strip()]
        if len(paragraphs) >= 2:
            first = re.sub(r"\s+", "", paragraphs[0])
            second = re.sub(r"\s+", "", paragraphs[1])
            if first and second and (first in second or second in first or SequenceMatcher(None, first, second).ratio() >= 0.9):
                return True

        if "一个是" in clean and "另一个是" not in clean:
            return True
        if "一方面" in clean and "另一方面" not in clean:
            return True
        if "首先" in clean and "其次" not in clean and "第二" not in clean:
            return True
        if "其一" in clean and "其二" not in clean:
            return True
        if "第一，" in clean and "第二" not in clean:
            return True
        if "提供了一个重要观察维度" in clean and "一个是" in clean and "另一个是" not in clean:
            return True
        if re.search(r"[“\"']\s*$", clean):
            return True
        return False
