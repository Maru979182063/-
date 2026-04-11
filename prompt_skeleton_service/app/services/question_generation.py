from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import random
import re
from copy import deepcopy
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
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
    UserMaterialPayload,
)
from app.schemas.runtime import QuestionRuntimeConfig
from app.services.evaluation_service import EvaluationService
from app.services.input_decoder import DIFFICULTY_MAPPING
from app.services.llm_gateway import LLMGatewayService
from app.services.material_bridge import MaterialBridgeService
from app.services.prompt_orchestrator import PromptOrchestratorService
from app.services.question_card_binding import QuestionCardBindingService
from app.services.question_generation_prompt_assets import load_question_generation_prompt_assets
from app.services.question_repository import QuestionRepository
from app.services.question_snapshot_builder import QuestionSnapshotBuilder
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.sentence_fill_protocol import normalize_sentence_fill_function_type
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
    original_sentences: list[str] = []
    correct_order: list[int] = []
    options: GeneratedQuestionOptionsDraft
    answer: str
    analysis: str


class MaterialRefinementDraft(BaseModel):
    refined_text: str
    changed: bool = False
    reason: str | None = None


class QuestionGenerationService:
    MATERIAL_REFINEMENT_READABILITY_ASSIST = "readability_assist"
    MATERIAL_REFINEMENT_THEME_PRESERVING = "theme_preserving_rewrite"
    JUDGE_OVERALL_PASS_THRESHOLD = 80
    JUDGE_MATERIAL_ALIGNMENT_THRESHOLD = 70
    JUDGE_ANSWER_ANALYSIS_THRESHOLD = 70
    JUDGE_HARD_DIFFICULTY_THRESHOLD = 68
    QUALITY_REPAIR_RETRY_THRESHOLD = 84
    MAX_ALIGNMENT_RETRIES = 2
    MAX_QUALITY_REPAIR_RETRIES = 2
    RACE_CANDIDATE_COUNT = 2
    INTERNAL_EXTRA_CONSTRAINT_FIELDS = {
        "reference_business_cards",
        "reference_query_terms",
        "required_review_overrides",
        "review_instruction",
        "source_question_style_summary",
    }
    PATTERN_BRIDGE_HINTS = {
        ("sentence_fill", "opening_summary"): {
            "preferred_business_card_ids": ["sentence_fill__opening_summary__abstract"],
            "structure_constraints": {
                "blank_position": "opening",
                "function_type": "summary",
                "logic_relation": "summary",
            },
        },
        ("sentence_fill", "bridge_transition"): {
            "preferred_business_card_ids": ["sentence_fill__middle_bridge_both_sides__abstract"],
            "structure_constraints": {
                "blank_position": "middle",
                "function_type": "bridge",
                "logic_relation": "continuation",
            },
        },
        ("sentence_fill", "middle_focus_shift"): {
            "preferred_business_card_ids": ["sentence_fill__middle_lead_next__abstract"],
            "structure_constraints": {
                "blank_position": "middle",
                "function_type": "lead_next",
                "logic_relation": "focus_shift",
            },
        },
        ("sentence_fill", "middle_explanation"): {
            "preferred_business_card_ids": ["sentence_fill__middle_carry_previous__abstract"],
            "structure_constraints": {
                "blank_position": "middle",
                "function_type": "carry_previous",
                "logic_relation": "explanation",
            },
        },
        ("sentence_fill", "ending_summary"): {
            "preferred_business_card_ids": ["sentence_fill__ending_summary__abstract"],
            "structure_constraints": {
                "blank_position": "ending",
                "function_type": "conclusion",
                "logic_relation": "summary",
            },
        },
        ("sentence_fill", "ending_elevation"): {
            "preferred_business_card_ids": [],
            "structure_constraints": {
                "blank_position": "ending",
                "function_type": "conclusion",
                "logic_relation": "elevation",
            },
        },
        ("sentence_fill", "inserted_reference_match"): {
            "preferred_business_card_ids": [],
            "structure_constraints": {
                "blank_position": "inserted",
                "function_type": "reference_summary",
                "logic_relation": "reference_match",
                "reference_anchor": "required",
            },
        },
        ("sentence_fill", "comprehensive_multi_match"): {
            "preferred_business_card_ids": [],
            "structure_constraints": {
                "blank_position": "mixed",
                "function_type": "bridge",
                "logic_relation": "multi_constraint",
            },
        },
    }
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
        self.question_card_binding = QuestionCardBindingService()
        self.source_question_analyzer = SourceQuestionAnalyzer(runtime_config)
        self.source_question_parser = SourceQuestionParserService(runtime_config)
        self.prompt_assets = load_question_generation_prompt_assets()

    def _question_generation_route(self):
        return self.runtime_config.llm.routing.question_generation or self.runtime_config.llm.routing.generate_question

    def _question_repair_route(self):
        return self.runtime_config.llm.routing.question_repair or self._question_generation_route()

    def _material_refinement_route(self):
        return self.runtime_config.llm.routing.material_refinement or self.runtime_config.llm.routing.review_actions.minor_edit

    @classmethod
    def _resolve_material_refinement_mode(
        cls,
        *,
        request_snapshot: dict[str, Any] | None,
        material: MaterialSelectionResult,
    ) -> str:
        snapshot = request_snapshot or {}
        question_type = str(snapshot.get("question_type") or "").strip()
        business_subtype = str(snapshot.get("business_subtype") or "").strip()
        if question_type == "main_idea" and business_subtype == "center_understanding":
            return cls.MATERIAL_REFINEMENT_THEME_PRESERVING
        return cls.MATERIAL_REFINEMENT_READABILITY_ASSIST

    def _build_material_refinement_prompts(
        self,
        *,
        refinement_mode: str,
        cleaned_seed: str,
        original_text: str,
    ) -> tuple[str, str]:
        if refinement_mode == self.MATERIAL_REFINEMENT_THEME_PRESERVING:
            system_prompt = (
                "你是一名主旨保持型材料改写助手。请把输入材料改写成更适合独立阅读和出题使用的自然文段。"
                "你必须严格保留原材料的主旨、核心对象、结论方向和关键考点，不能改主旨，不能换论证终点。"
                "在此基础上，你可以重写句子、合并条目、去掉小标题与编号、补足自然衔接，让文段更顺、更像正式材料。"
                "不得新增原文没有的事实、数据、背景和立场，不得把局部信息改成新的中心。"
            )
            user_prompt = "\n\n".join(
                [
                    "请把下面材料改写成主旨保持型自然文段。",
                    "要求：必须保留原材料主旨、核心对象、结论方向和关键考点。",
                    "允许：去掉条目编号、小标题、模板标签，重写句子，合并整理成连续文段。",
                    "禁止：新增事实、改变主旨、改变论证方向、拔高或缩窄中心。",
                    "[清理后基底文段]",
                    cleaned_seed,
                    "[原始文段]",
                    original_text,
                ]
            )
            return system_prompt, user_prompt

        system_prompt = (
            "你是一名材料阅读辅助助手。请对输入文段做最小必要的语言修整，使其可以作为独立可读的出题材料。"
            "只能做阅读辅助，不得改写主干表达，不得重组论证结构，不得新增原文没有的事实、背景、判断和立场。"
            "你可以去掉模板标签、小标题和条目编号，修复明显断裂、重复、代词悬空、开头突兀和拼接痕迹。"
            "如果材料本身基本可读，请尽量少改。"
        )
        user_prompt = "\n\n".join(
            [
                "请对下面文段做阅读辅助式轻修。",
                "要求：保留原文事实、表达顺序和论证关系，只做去标签、去小标题、去编号、去重复和最小衔接修复。",
                "禁止：改写主干观点、重组结构、扩写背景、替换论据。",
                "[清理后基底文段]",
                cleaned_seed,
                "[原始文段]",
                original_text,
            ]
        )
        return system_prompt, user_prompt

    def generate(self, request: QuestionGenerateRequest) -> dict:
        prepared_request = self._prepare_request(request)
        decoded, target_override_warning = self._decode_generation_target(prepared_request)
        standard_request = dict(decoded["standard_request"])
        question_card_binding = self._resolve_question_card_binding(
            question_card_id=prepared_request.question_card_id,
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
            pattern_id=standard_request.get("pattern_id"),
        )
        standard_request = self._apply_question_card_binding(
            standard_request=standard_request,
            question_card_binding=question_card_binding,
        )
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
            question_card_binding=question_card_binding,
        )
        self._persist_source_question_asset(
            request=prepared_request,
            request_snapshot=request_snapshot,
            source_question_analysis=source_question_analysis,
            question_card_binding=question_card_binding,
        )
        materials, material_warnings = self._resolve_generation_materials(
            request=prepared_request,
            standard_request=standard_request,
            source_question_analysis=source_question_analysis,
            question_card_binding=question_card_binding,
            request_snapshot=request_snapshot,
            effective_count=effective_count,
        )
        if question_card_binding.get("warning"):
            material_warnings.insert(0, question_card_binding["warning"])

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
        race_material_limit = max(self.RACE_CANDIDATE_COUNT, effective_count)
        if self._should_use_compact_main_idea_path(source_question_analysis):
            race_material_limit = effective_count
        race_materials = materials[: race_material_limit]
        race_results: list[dict] = []
        with ThreadPoolExecutor(max_workers=max(1, min(len(race_materials), self.RACE_CANDIDATE_COUNT))) as executor:
            future_map = {
                executor.submit(
                    self._run_race_candidate,
                    material=race_materials[index],
                    index=index,
                    standard_request=standard_request,
                    source_question_analysis=source_question_analysis,
                    request_snapshot=request_snapshot,
                    batch_id=batch_id,
                    request_id=request_id,
                ): index
                for index in range(len(race_materials))
            }
            for future in as_completed(future_map):
                race_results.append(future.result())

        race_results.sort(key=lambda entry: entry["index"])
        for result in race_results:
            if result["accepted"]:
                accepted_candidates.append(result["candidate"])
                continue
            rejected_attempts.append(result["summary"])
            rejected_candidates.append(result["candidate"])

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

        fallback_record_path: str | None = None
        needs_review_fallback = len(items) < effective_count and bool(rejected_candidates)
        if not items and standard_request["question_type"] == "sentence_order" and not rejected_candidates:
            raise DomainError(
                "No eligible sentence_order materials passed validation from passage_service.",
                status_code=422,
                details={
                    "question_type": standard_request["question_type"],
                    "reason": "empty_effective_material_pool",
                },
            )

        if rejected_attempts and (not items or needs_review_fallback):
            fallback_record_path = self._write_failure_markdown_record(
                batch_id=batch_id,
                request_id=request_id,
                question_type=standard_request["question_type"],
                difficulty_target=standard_request["difficulty_target"],
                rejected_attempts=rejected_attempts,
            )

        if not items and not rejected_candidates:
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

        if needs_review_fallback:
            added_fallback_count = self._append_review_fallback_items(
                items=items,
                rejected_candidates=rejected_candidates,
                limit=effective_count,
                failure_record_path=fallback_record_path,
            )
            if added_fallback_count:
                if not accepted_candidates:
                    material_warnings.append(
                        "No question fully passed validation after retries; returned the highest-scoring blocked attempt for manual review."
                    )
                else:
                    material_warnings.append(
                        "Returned additional blocked attempts for frontend display because the fully accepted result count was below the requested count."
                    )
                if fallback_record_path:
                    material_warnings.append(f"Failure reasons were recorded to: {fallback_record_path}")

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
                "When fully accepted items are insufficient, the highest-scoring blocked attempts may still be returned for frontend display and manual review.",
                "Batch review actions are not wired yet; this slice persists generated items for later review work.",
            ],
        }
        if request_snapshot.get("generation_mode") == "forced_user_material":
            response["notes"].append(
                "Forced user-material mode: skipped passage retrieval and returned generated output even when only blocked attempts were available."
            )
        self.repository.save_batch(batch_id, response)
        return QuestionGenerationBatchResponse.model_validate(response).model_dump()

    def _resolve_generation_materials(
        self,
        *,
        request: QuestionGenerateRequest,
        standard_request: dict[str, Any],
        source_question_analysis: dict[str, Any],
        question_card_binding: dict[str, Any],
        request_snapshot: dict[str, Any],
        effective_count: int,
    ) -> tuple[list[MaterialSelectionResult], list[str]]:
        if request.user_material is not None and request_snapshot.get("generation_mode") == "forced_user_material":
            forced_count = max(effective_count, self.RACE_CANDIDATE_COUNT)
            materials = self._build_forced_user_material_candidates(
                user_material=request.user_material,
                question_card_binding=question_card_binding,
                request_snapshot=request_snapshot,
                count=forced_count,
            )
            return materials, [
                "Forced user-material mode enabled: using the user-supplied passage directly and bypassing passage_service retrieval.",
                "This result is tagged as cautionary for later adaptive analysis.",
            ]

        bridge_hints = self._merge_material_bridge_hints(
            self._material_bridge_hints(source_question_analysis),
            self._requested_pattern_bridge_hints(
                question_type=standard_request["question_type"],
                pattern_id=standard_request.get("pattern_id"),
            ),
        )
        requested_material_count = max(effective_count * 4, effective_count + 2)
        materials, material_warnings = self.material_bridge.select_materials(
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
            question_card_id=question_card_binding.get("question_card_id"),
            difficulty_target=standard_request["difficulty_target"],
            topic=request.topic,
            text_direction=request.text_direction,
            document_genre=(
                request.material_policy.preferred_document_genres[0]
                if request.material_policy and request.material_policy.preferred_document_genres
                else None
            ),
            material_structure_label=request.material_structure,
            material_policy=request.material_policy,
            count=requested_material_count,
            business_card_ids=bridge_hints["business_card_ids"],
            preferred_business_card_ids=bridge_hints["preferred_business_card_ids"],
            query_terms=bridge_hints["query_terms"],
            target_length=source_question_analysis.get("target_length"),
            length_tolerance=source_question_analysis.get("length_tolerance", 120),
            structure_constraints=bridge_hints["structure_constraints"],
            enable_anchor_adaptation=bool(source_question_analysis),
            preference_profile=request_snapshot.get("preference_profile"),
            usage_stats_lookup=self.repository.get_material_usage_stats,
        )
        return materials, material_warnings

    def _persist_source_question_asset(
        self,
        *,
        request: QuestionGenerateRequest,
        request_snapshot: dict[str, Any],
        source_question_analysis: dict[str, Any],
        question_card_binding: dict[str, Any],
    ) -> None:
        if request.source_question is None:
            return
        try:
            payload = request.source_question.model_dump()
            metadata = {
                "ingest_source": "generate_request.source_question",
                "request_id": request_snapshot.get("request_id"),
                "question_card_binding": {
                    "question_card_id": question_card_binding.get("question_card_id"),
                    "binding_source": question_card_binding.get("binding_source"),
                    "binding_reason": question_card_binding.get("binding_reason"),
                },
                "source_question_analysis": deepcopy(source_question_analysis),
                "use_fewshot": request_snapshot.get("use_fewshot", True),
                "fewshot_mode": request_snapshot.get("fewshot_mode", "structure_only"),
                "usage_count": 1,
            }
            self.repository.upsert_source_question_asset(
                asset_id=str(uuid4()),
                source_type="user_uploaded_reference",
                payload=payload,
                metadata=metadata,
                question_card_id=question_card_binding.get("question_card_id"),
                question_type=request_snapshot.get("question_type"),
                business_subtype=request_snapshot.get("business_subtype"),
                pattern_id=request_snapshot.get("pattern_id"),
                difficulty_target=request_snapshot.get("difficulty_target"),
                topic=request_snapshot.get("topic"),
            )
        except Exception:  # noqa: BLE001
            logger.exception("source_question_asset_persist_failed")

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

    def _decode_generation_target(self, request: QuestionGenerateRequest) -> tuple[dict, str | None]:
        if request.question_card_id:
            return self._build_explicit_question_card_decode_result(request), None

        decode_request, target_override_warning = self._build_decode_request(request)
        return self.orchestrator.decode_input(decode_request), target_override_warning

    def _build_explicit_question_card_decode_result(self, request: QuestionGenerateRequest) -> dict:
        binding = self.question_card_binding.resolve(
            question_card_id=request.question_card_id,
            require_match=True,
        )
        runtime_binding = binding["runtime_binding"]
        difficulty_target = self._normalize_difficulty_level(request.difficulty_level)
        requested_count = request.count or 1
        effective_count, count_warnings = self._normalize_requested_count(requested_count)

        standard_request = {
            "question_type": runtime_binding["question_type"],
            "business_subtype": runtime_binding.get("business_subtype"),
            "pattern_id": None,
            "difficulty_target": difficulty_target,
            "topic": request.topic,
            "count": effective_count,
            "passage_style": request.passage_style,
            "use_fewshot": request.use_fewshot,
            "fewshot_mode": request.fewshot_mode,
            "type_slots": deepcopy(request.type_slots),
            "extra_constraints": self._merge_request_extra_constraints(request),
        }
        batch_meta = BatchMeta(
            requested_count=requested_count,
            effective_count=effective_count,
            question_type=runtime_binding["question_type"],
            business_subtype=runtime_binding.get("business_subtype"),
            pattern_id=None,
            difficulty_target=difficulty_target,
        )
        return {
            "mapping_source": "question_card_id",
            "selected_special_type": None,
            "standard_request": standard_request,
            "batch_meta": batch_meta.model_dump(),
            "warnings": list(count_warnings),
        }

    def _build_decode_request(self, request: QuestionGenerateRequest) -> tuple[DifyFormInput, str | None]:
        current_focus = str(request.question_focus or "").strip()
        selected_special_types = [item for item in (request.special_question_types or []) if str(item).strip()]
        if current_focus.lower() in {"select", "auto"} or current_focus in {"不指定", "不指定（自动匹配）", "请选择"}:
            current_focus = ""
        return request.to_dify_form_input(), None

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

    @staticmethod
    def _normalize_difficulty_level(difficulty_level: str) -> str:
        mapping = {
            "绠€鍗?": "easy",
            "涓瓑": "medium",
            "鍥伴毦": "hard",
            "easy": "easy",
            "medium": "medium",
            "hard": "hard",
        }
        difficulty_target = mapping.get(difficulty_level)
        if not difficulty_target:
            raise DomainError(
                "Unsupported difficulty_level.",
                status_code=422,
                details={
                    "difficulty_level": difficulty_level,
                    "supported": sorted(set(mapping.keys())),
                },
            )
        return difficulty_target

    @staticmethod
    def _normalize_requested_count(requested_count: int) -> tuple[int, list[str]]:
        warnings: list[str] = []
        effective_count = requested_count
        if requested_count < 1:
            effective_count = 1
            warnings.append("count was below 1 and has been corrected to 1.")
        elif requested_count > 5:
            effective_count = 5
            warnings.append("count was above 5 and has been truncated to 5 for the current demo.")
        return effective_count, warnings

    @staticmethod
    def _merge_request_extra_constraints(request: QuestionGenerateRequest) -> dict:
        merged = deepcopy(request.extra_constraints or {})
        if request.text_direction:
            merged.setdefault("text_direction", request.text_direction)
        return merged

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
            original_sentences=list(generated.original_sentences or []),
            correct_order=list(generated.correct_order or []),
            options=generated.options.model_dump(),
            answer=generated.answer,
            analysis=generated.analysis,
            metadata=metadata,
        )
        if built_item["question_type"] == "sentence_order":
            generated_question = self.build_sentence_order_question(generated_question, material_text=material.text)
            generated_question = self._enforce_sentence_order_six_unit_output(generated_question)
        return self._remap_answer_position(generated_question), response

    def _run_targeted_question_repair(
        self,
        *,
        built_item: dict,
        material: MaterialSelectionResult,
        current_question: GeneratedQuestion,
        route,
        repair_plan: dict[str, Any],
        feedback_notes: list[str],
    ) -> tuple[GeneratedQuestion, dict]:
        system_prompt = (
            "You are a strict targeted-repair assistant for exam-question generation. "
            "Do not regenerate the whole question from scratch. "
            "Only edit the explicitly allowed fields, preserve locked fields exactly, "
            "do not cross the material boundary, and do not invent facts beyond the source material. "
            "If the requested repair cannot be completed safely, keep the object as close to the current version as possible."
        )
        current_payload = current_question.model_dump()
        user_prompt = "\n\n".join(
            [
                f"question_type={built_item['question_type']}",
                f"business_subtype={built_item.get('business_subtype') or ''}",
                f"repair_mode={repair_plan.get('mode') or 'targeted_repair'}",
                f"allowed_fields={', '.join(repair_plan.get('allowed_fields') or [])}",
                f"locked_fields={', '.join(repair_plan.get('locked_fields') or [])}",
                f"target_errors={', '.join(repair_plan.get('target_errors') or [])}",
                f"target_checks={', '.join(repair_plan.get('target_checks') or [])}",
                "Current generated question JSON:",
                str(current_payload),
                "Original source material:",
                material.text,
                "Repair requirements:",
                "\n".join(feedback_notes or []),
                "Return a full updated GeneratedQuestion object. "
                "Only change the allowed fields. Keep all locked fields semantically unchanged.",
            ]
        )
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="generated_question_targeted_repair",
            schema=GeneratedQuestionDraft.model_json_schema(),
        )
        revised = self.generated_question_adapter.validate_python(response)
        revised_question = GeneratedQuestion(
            question_type=current_question.question_type,
            business_subtype=current_question.business_subtype,
            pattern_id=current_question.pattern_id,
            stem=revised.stem,
            original_sentences=list(revised.original_sentences or []),
            correct_order=list(revised.correct_order or []),
            options=revised.options.model_dump(),
            answer=revised.answer,
            analysis=revised.analysis,
            metadata={
                **(current_question.metadata or {}),
                "repair_mode": repair_plan.get("mode"),
            },
        )
        merged_question = self._merge_repaired_question_with_scope(
            current_question=current_question,
            repaired_question=revised_question,
            repair_plan=repair_plan,
        )
        if built_item["question_type"] == "sentence_order":
            merged_question = self._enforce_sentence_order_six_unit_output(merged_question)
        return self._remap_answer_position(merged_question), response

    @staticmethod
    def _merge_repaired_question_with_scope(
        *,
        current_question: GeneratedQuestion,
        repaired_question: GeneratedQuestion,
        repair_plan: dict[str, Any],
    ) -> GeneratedQuestion:
        allowed_fields = set(repair_plan.get("allowed_fields") or [])
        if not allowed_fields:
            return current_question
        update: dict[str, Any] = {}
        if "stem" in allowed_fields:
            update["stem"] = repaired_question.stem
        if "original_sentences" in allowed_fields:
            update["original_sentences"] = list(repaired_question.original_sentences or [])
        if "correct_order" in allowed_fields:
            update["correct_order"] = list(repaired_question.correct_order or [])
        if "options" in allowed_fields:
            update["options"] = dict(repaired_question.options or {})
        if "answer" in allowed_fields:
            update["answer"] = repaired_question.answer
        if "analysis" in allowed_fields:
            update["analysis"] = repaired_question.analysis
        metadata = dict(current_question.metadata or {})
        metadata.update({"repair_mode": repair_plan.get("mode")})
        update["metadata"] = metadata
        return current_question.model_copy(update=update)

    @staticmethod
    def _collect_validation_issue_keys(validation_result, quality_gate_errors: list[str] | None = None) -> set[str]:
        keys = {str(error).strip() for error in (getattr(validation_result, "errors", None) or []) if str(error).strip()}
        keys.update(str(error).strip() for error in (quality_gate_errors or []) if str(error).strip())
        checks = getattr(validation_result, "checks", None) or {}
        for check_name, payload in checks.items():
            if isinstance(payload, dict) and payload.get("passed") is False:
                keys.add(str(check_name))
        return keys

    def _build_targeted_repair_plan(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        validation_result,
        quality_gate_errors: list[str],
        source_question_analysis: dict | None,
    ) -> dict[str, Any] | None:
        issue_keys = self._collect_validation_issue_keys(validation_result, quality_gate_errors)

        if question_type == "main_idea":
            if {"main_axis_mismatch", "abstraction_level_mismatch"} & issue_keys:
                return {
                    "mode": "main_idea_axis_repair",
                    "allowed_fields": ["options", "answer", "analysis"],
                    "locked_fields": ["stem", "original_sentences", "correct_order"],
                    "target_errors": ["main_axis_mismatch", "abstraction_level_mismatch"],
                    "target_checks": ["analysis_mentions_correct_option_text"],
                    "notes": [
                        "Do not change the material scope or invent new facts.",
                        "Repair the correct option so it matches the passage main axis and target abstraction level.",
                        "Keep distractors on-topic but clearly below the correct option in coverage.",
                        "Rewrite the analysis so it explicitly explains why the correct option text fits best.",
                    ],
                }
            if "analysis_mentions_correct_option_text" in issue_keys or (
                not issue_keys and quality_gate_errors and business_subtype == "center_understanding"
            ):
                return {
                    "mode": "analysis_only_repair",
                    "allowed_fields": ["analysis"],
                    "locked_fields": ["stem", "original_sentences", "correct_order", "options", "answer"],
                    "target_errors": [],
                    "target_checks": ["analysis_mentions_correct_option_text"],
                    "notes": [
                        "Only improve the analysis.",
                        "Do not change stem, options, answer, or material-facing semantics.",
                        "The analysis must explicitly explain why the declared correct option text best matches the passage.",
                    ],
                }

        if question_type == "sentence_order":
            unrecoverable = {
                "sentence_order should preserve the reference sortable-unit count (7), but generated result drifted.",
                "sentence_order material does not show a strong enough unique opener candidate.",
                "sentence_order material lacks enough deterministic binding pairs to support a unique-best sequence.",
                "sentence_order material remains too readable after key-unit exchange and is not uniquely orderable enough.",
                "sentence_order material admits too many near-plausible ordering paths.",
                "sentence_order material does not reach the reference question's unique-answer strength.",
                "sentence_order_material_unit_count_mismatch",
            }
            if issue_keys & unrecoverable:
                return None
            if {
                "sentence_order_single_truth_option",
                "sentence_order_answer_binding",
                "sentence_order_analysis_binding",
                "analysis_mentions_correct_option_text",
            } & issue_keys:
                return {
                    "mode": "sentence_order_answer_explanation_repair",
                    "allowed_fields": ["options", "answer", "analysis"],
                    "locked_fields": ["stem", "original_sentences", "correct_order"],
                    "target_errors": ["sentence_order_single_truth_option", "sentence_order_answer_binding", "sentence_order_analysis_binding"],
                    "target_checks": ["analysis_mentions_correct_option_text"],
                    "notes": [
                        "Do not change the original sentences or correct-order truth.",
                        "Only repair option mapping, declared answer, and explanation.",
                        "The final explanation must describe head/tail or binding clues using the actual sentence numbering in the current question.",
                    ],
                }

        return None

    @staticmethod
    def _count_target_failures(validation_result, repair_plan: dict[str, Any]) -> int:
        target_errors = set(repair_plan.get("target_errors") or [])
        target_checks = set(repair_plan.get("target_checks") or [])
        count = 0
        for error in (getattr(validation_result, "errors", None) or []):
            if str(error).strip() in target_errors:
                count += 1
        checks = getattr(validation_result, "checks", None) or {}
        for check_name, payload in checks.items():
            if check_name in target_checks and isinstance(payload, dict) and payload.get("passed") is False:
                count += 1
        return count

    def _should_accept_targeted_repair(
        self,
        *,
        repair_plan: dict[str, Any],
        current_validation_result,
        current_evaluation_result: dict | None,
        repaired_validation_result,
        repaired_evaluation_result: dict | None,
        repaired_quality_gate_errors: list[str],
    ) -> bool:
        current_target_failures = self._count_target_failures(current_validation_result, repair_plan)
        repaired_target_failures = self._count_target_failures(repaired_validation_result, repair_plan)
        current_error_count = len((current_validation_result.errors or [])) + len((current_validation_result.warnings or []))
        repaired_error_count = len((repaired_validation_result.errors or [])) + len((repaired_validation_result.warnings or []))
        current_score = self._normalize_judge_score((current_evaluation_result or {}).get("overall_score"))
        repaired_score = self._normalize_judge_score((repaired_evaluation_result or {}).get("overall_score"))

        if repaired_validation_result.passed and not repaired_quality_gate_errors:
            return True
        if repaired_target_failures > current_target_failures:
            return False
        if repaired_target_failures < current_target_failures and repaired_error_count <= current_error_count and repaired_score >= current_score - 2:
            return True
        return self._should_accept_quality_retry(
            current_validation_result=current_validation_result,
            current_evaluation_result=current_evaluation_result,
            repaired_validation_result=repaired_validation_result,
            repaired_evaluation_result=repaired_evaluation_result,
            repaired_quality_gate_errors=repaired_quality_gate_errors,
        )

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
        material = self._refine_material_if_needed(material, request_snapshot=item.get("request_snapshot"))
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
        revised_question = GeneratedQuestion(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            pattern_id=item.get("pattern_id"),
            stem=revised.stem,
            original_sentences=list(revised.original_sentences or []),
            correct_order=list(revised.correct_order or []),
            options=revised.options.model_dump(),
            answer=revised.answer,
            analysis=revised.analysis,
            metadata=metadata,
        )
        if item["question_type"] == "sentence_order":
            revised_question = self.build_sentence_order_question(revised_question, material_text=material.text)
            revised_question = self._enforce_sentence_order_six_unit_output(revised_question)
        item["generated_question"] = self._remap_answer_position(revised_question).model_dump()
        validation_result = self.validator.validate(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            generated_question=GeneratedQuestion.model_validate(item["generated_question"]),
            material_text=material.text,
            original_material_text=material.original_text,
            material_source=material.source,
            validator_contract=material.validator_contract,
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
        material = self._refine_material_if_needed(material, request_snapshot=request_snapshot)
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
                question_card_id=request_snapshot.get("question_card_id"),
                document_genre=(material_policy.preferred_document_genres[0] if material_policy and material_policy.preferred_document_genres else None),
                material_structure_label=request_snapshot.get("material_structure"),
                exclude_material_ids=None,
                limit=24,
                difficulty_target=request_snapshot.get("difficulty_target", "medium"),
                preference_profile=request_snapshot.get("preference_profile"),
                usage_stats_lookup=self.repository.get_material_usage_stats,
            )
            materials = [candidate for candidate in replacement_candidates if candidate.material_id == requested_material_id]
            warnings = []
        else:
            source_question_analysis = request_snapshot.get("source_question_analysis") or {}
            bridge_hints = self._merge_material_bridge_hints(
                self._material_bridge_hints(source_question_analysis),
                self._requested_pattern_bridge_hints(
                    question_type=request_snapshot["question_type"],
                    pattern_id=request_snapshot.get("pattern_id"),
                ),
            )
            materials, warnings = self.material_bridge.select_materials(
                question_type=request_snapshot["question_type"],
                business_subtype=request_snapshot.get("business_subtype"),
                question_card_id=request_snapshot.get("question_card_id"),
                difficulty_target=request_snapshot["difficulty_target"],
                topic=request_snapshot.get("topic"),
                text_direction=((request_snapshot.get("extra_constraints") or {}).get("text_direction")),
                document_genre=(material_policy.preferred_document_genres[0] if material_policy and material_policy.preferred_document_genres else None),
                material_policy=material_policy,
                count=1,
                business_card_ids=bridge_hints["business_card_ids"],
                preferred_business_card_ids=bridge_hints["preferred_business_card_ids"],
                query_terms=bridge_hints["query_terms"],
                target_length=source_question_analysis.get("target_length"),
                length_tolerance=(source_question_analysis.get("length_tolerance") or 120),
                structure_constraints=bridge_hints["structure_constraints"],
                enable_anchor_adaptation=bool(source_question_analysis),
                exclude_material_ids=None if requested_material_id else ({previous_material_id} if previous_material_id else None),
                preference_profile=request_snapshot.get("preference_profile"),
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
            material=self._refine_material_if_needed(
                self._annotate_material_usage(materials[0]),
                request_snapshot=request_snapshot,
            ),
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
        if item.get("question_type") == "sentence_order":
            edited_question = self.build_sentence_order_question(edited_question, material_text=edited_material.text)
            edited_question = self._enforce_sentence_order_six_unit_output(edited_question)

        revised_item = deepcopy(item)
        revised_item["generated_question"] = edited_question.model_dump()
        revised_item["stem_text"] = edited_question.stem
        revised_item["material_selection"] = edited_material.model_dump()
        revised_item["material_text"] = edited_material.text
        revised_item["material_source"] = edited_material.source
        revised_item["material_usage_count_before"] = edited_material.usage_count_before
        revised_item["material_previously_used"] = edited_material.previously_used
        revised_item["material_last_used_at"] = edited_material.last_used_at
        revised_item["preference_profile"] = self._preference_profile_from_snapshot(revised_item.get("request_snapshot") or {})
        revised_item["feedback_snapshot"] = self._feedback_snapshot_from_material(edited_material)

        validation_result = self.validator.validate(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            generated_question=edited_question,
            material_text=edited_material.text,
            original_material_text=edited_material.original_text,
            material_source=edited_material.source,
            validator_contract=edited_material.validator_contract,
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
        runtime_snapshot = self._attach_feedback_runtime_context(
            built_item=revised_item,
            material=edited_material,
            request_snapshot=revised_item.get("request_snapshot") or {},
            runtime_snapshot=runtime_snapshot,
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
        built_item["generation_mode"] = str(request_snapshot.get("generation_mode") or "standard")
        built_item["material_source_type"] = str((material.source or {}).get("material_source_type") or "platform_selected")
        built_item["forced_generation"] = built_item["generation_mode"] == "forced_user_material"
        built_item["material_text"] = material.text
        built_item["material_source"] = material.source
        built_item["material_usage_count_before"] = material.usage_count_before
        built_item["material_previously_used"] = material.previously_used
        built_item["material_last_used_at"] = material.last_used_at
        built_item["preference_profile"] = self._preference_profile_from_snapshot(request_snapshot)
        built_item["feedback_snapshot"] = self._feedback_snapshot_from_material(material)
        built_item["revision_count"] = revision_count
        if built_item["forced_generation"]:
            built_item["notes"] = built_item.get("notes", []) + [
                "forced_user_material_generation",
                "caution:user_uploaded_material_unvalidated",
            ]
        refinement_mode = str((material.source or {}).get("material_refinement_mode") or "").strip()
        if refinement_mode:
            built_item["notes"] = built_item.get("notes", []) + [f"material_refinement_mode:{refinement_mode}"]
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
            validator_contract=material.validator_contract,
            difficulty_fit=built_item.get("difficulty_fit"),
            source_question=request_source_question,
            source_question_analysis=request_source_analysis,
        )

        alignment_retry_count = 0
        while generated_question and self._should_retry_alignment(validation_result, request_source_analysis):
            if alignment_retry_count >= self.MAX_ALIGNMENT_RETRIES:
                break
            repair_plan = self._build_targeted_repair_plan(
                question_type=built_item["question_type"],
                business_subtype=built_item.get("business_subtype"),
                validation_result=validation_result,
                quality_gate_errors=[],
                source_question_analysis=request_source_analysis,
            )
            if not repair_plan:
                break
            feedback_notes = self._build_alignment_feedback_notes(validation_result, request_source_analysis)
            try:
                regenerated_question, retry_raw_output = self._run_targeted_question_repair(
                    built_item=built_item,
                    material=material,
                    current_question=generated_question,
                    route=self._question_repair_route(),
                    repair_plan=repair_plan,
                    feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                )
                retry_validation_result = self.validator.validate(
                    question_type=built_item["question_type"],
                    business_subtype=built_item.get("business_subtype"),
                    generated_question=regenerated_question,
                    material_text=material.text,
                    original_material_text=material.original_text,
                    material_source=material.source,
                    validator_contract=material.validator_contract,
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
                    built_item["notes"] = built_item.get("notes", []) + [f"alignment_retry_applied_{alignment_retry_count}::{repair_plan.get('mode')}"]
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
            repair_plan = self._build_targeted_repair_plan(
                question_type=built_item["question_type"],
                business_subtype=built_item.get("business_subtype"),
                validation_result=validation_result,
                quality_gate_errors=quality_gate_errors,
                source_question_analysis=request_source_analysis,
            )
            if not repair_plan:
                break
            feedback_notes = self._build_quality_repair_feedback_notes(
                validation_result=validation_result,
                evaluation_result=built_item["evaluation_result"],
                quality_gate_errors=quality_gate_errors,
                source_question_analysis=request_source_analysis,
            )
            try:
                repaired_question, repaired_raw_output = self._run_targeted_question_repair(
                    built_item=built_item,
                    material=material,
                    current_question=generated_question,
                    route=self._question_repair_route(),
                    repair_plan=repair_plan,
                    feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                )
                repaired_validation_result = self.validator.validate(
                    question_type=built_item["question_type"],
                    business_subtype=built_item.get("business_subtype"),
                    generated_question=repaired_question,
                    material_text=material.text,
                    original_material_text=material.original_text,
                    material_source=material.source,
                    validator_contract=material.validator_contract,
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
                if self._should_accept_targeted_repair(
                    repair_plan=repair_plan,
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
                    built_item["notes"] = built_item.get("notes", []) + [f"quality_repair_retry_applied_{quality_retry_count}::{repair_plan.get('mode')}"]
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
        runtime_snapshot = self._attach_feedback_runtime_context(
            built_item=built_item,
            material=material,
            request_snapshot=request_snapshot,
            runtime_snapshot=runtime_snapshot,
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

    def _run_race_candidate(
        self,
        *,
        material: MaterialSelectionResult,
        index: int,
        standard_request: dict,
        source_question_analysis: dict | None,
        request_snapshot: dict,
        batch_id: str,
        request_id: str,
    ) -> dict:
        material = self._annotate_material_usage(material)
        if standard_request["question_type"] == "sentence_order":
            adapted_material = self._coerce_sentence_order_material(
                material=material,
                source_question_analysis=source_question_analysis,
            )
            if adapted_material is None:
                rejected_summary = {
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
                return {
                    "index": index,
                    "accepted": False,
                    "summary": rejected_summary,
                    "candidate": {
                        "item": {"current_status": "auto_failed", "validation_result": {"errors": ["sentence_order_material_unit_count_mismatch"]}},
                        "material": material,
                        "summary": rejected_summary,
                        "rank_score": -999.0,
                    },
                }
            material = adapted_material

        material = self._refine_material_if_needed(material, request_snapshot=request_snapshot)
        built_item = self._build_generated_item(
            build_request=self._build_prompt_request_from_snapshot(request_snapshot),
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
            logger.info(
                "question_rejected batch_id=%s attempt=%s material_id=%s status=%s errors=%s",
                batch_id,
                index + 1,
                material.material_id,
                built_item.get("current_status"),
                (built_item.get("validation_result") or {}).get("errors", [])[:3],
            )
            return {
                "index": index,
                "accepted": False,
                "summary": rejected_summary,
                "candidate": {
                    "item": built_item,
                    "material": material,
                    "summary": rejected_summary,
                    "rank_score": self._rejected_attempt_rank_score(built_item),
                },
            }

        logger.info(
            "question_candidate_accepted batch_id=%s item_id=%s version_no=%s request_id=%s rank_score=%s",
            batch_id,
            built_item["item_id"],
            built_item.get("current_version_no"),
            request_id,
            self._accepted_attempt_rank_score(built_item),
        )
        return {
            "index": index,
            "accepted": True,
            "summary": None,
            "candidate": {
                "item": built_item,
                "material": material,
                "rank_score": self._accepted_attempt_rank_score(built_item),
            },
        }

    def _build_request_snapshot(
        self,
        request: QuestionGenerateRequest,
        standard_request: dict,
        decoded: dict,
        *,
        request_id: str,
        source_question_analysis: dict,
        question_card_binding: dict,
    ) -> dict:
        merged_extra_constraints = deepcopy(standard_request.get("extra_constraints") or {})
        if request.extra_constraints:
            merged_extra_constraints.update(request.extra_constraints)
        normalized_preference_profile = self.material_bridge._normalize_preference_profile(
            (merged_extra_constraints or {}).get("preference_profile")
        )
        if not self.material_bridge._has_active_preference(normalized_preference_profile):
            normalized_preference_profile = self._default_preference_profile(
                difficulty_target=standard_request["difficulty_target"],
                question_type=standard_request["question_type"],
                business_subtype=standard_request.get("business_subtype"),
            )
        merged_extra_constraints["preference_profile"] = normalized_preference_profile
        resolved_pattern_id = standard_request.get("pattern_id")
        return {
            "request_id": request_id,
            "generation_mode": request.generation_mode,
            "question_type": standard_request["question_type"],
            "business_subtype": standard_request.get("business_subtype"),
            "pattern_id": resolved_pattern_id,
            "question_card_id": question_card_binding.get("question_card_id"),
            "question_card_binding": deepcopy(question_card_binding),
            "difficulty_target": standard_request["difficulty_target"],
            "topic": request.topic or source_question_analysis.get("topic"),
            "material_structure": request.material_structure,
            "passage_style": request.passage_style,
            "use_fewshot": request.use_fewshot,
            "fewshot_mode": request.fewshot_mode,
            "type_slots": deepcopy(request.type_slots),
            "extra_constraints": merged_extra_constraints,
            "preference_profile": normalized_preference_profile,
            "material_policy": request.material_policy.model_dump() if request.material_policy else None,
            "source_question": request.source_question.model_dump() if request.source_question else None,
            "user_material": request.user_material.model_dump() if request.user_material else None,
            "source_question_analysis": deepcopy(source_question_analysis),
            "source_form": {
                "question_card_id": request.question_card_id,
                "generation_mode": request.generation_mode,
                "question_focus": request.question_focus,
                "difficulty_level": request.difficulty_level,
                "effective_difficulty_target": standard_request["difficulty_target"],
                "text_direction": request.text_direction,
                "special_question_types": deepcopy(request.special_question_types),
                "mapping_source": decoded.get("mapping_source"),
                "selected_special_type": decoded.get("selected_special_type"),
            },
        }

    def _build_forced_user_material_candidates(
        self,
        *,
        user_material: UserMaterialPayload,
        question_card_binding: dict[str, Any],
        request_snapshot: dict[str, Any],
        count: int,
    ) -> list[MaterialSelectionResult]:
        question_card = question_card_binding.get("question_card") or {}
        validator_contract = question_card.get("validator_contract") if isinstance(question_card, dict) else None
        runtime_binding = question_card_binding.get("runtime_binding") if isinstance(question_card_binding, dict) else None
        document_genre = (
            user_material.document_genre
            or ((request_snapshot.get("material_policy") or {}).get("preferred_document_genres") or [None])[0]
            or "user_uploaded"
        )
        source_label = user_material.source_label or "user_uploaded_material"
        article_title = user_material.title or request_snapshot.get("topic") or "用户自带材料"
        topic = user_material.topic or request_snapshot.get("topic")
        base_source = {
            "source_name": source_label,
            "article_title": article_title,
            "source_id": "user_uploaded_material",
            "source_type": "user_uploaded",
            "forced_user_material": True,
            "forced_generation": True,
            "material_source_type": "user_uploaded",
            "generation_mode": "forced_user_material",
            "caution_tag": "user_uploaded_material_unvalidated",
        }
        if topic:
            base_source["topic"] = topic

        materials: list[MaterialSelectionResult] = []
        for index in range(max(1, count)):
            materials.append(
                MaterialSelectionResult(
                    material_id=f"user_uploaded::{uuid4().hex}",
                    article_id=f"user_uploaded_material::{index + 1}",
                    question_card_id=question_card_binding.get("question_card_id"),
                    runtime_binding=deepcopy(runtime_binding) if isinstance(runtime_binding, dict) else None,
                    validator_contract=deepcopy(validator_contract) if isinstance(validator_contract, dict) else None,
                    text=user_material.text,
                    original_text=user_material.text,
                    source=deepcopy(base_source),
                    primary_label=topic,
                    document_genre=document_genre,
                    material_structure_label=request_snapshot.get("material_structure"),
                    standalone_readability=1.0,
                    quality_score=1.0,
                    knowledge_tags=[],
                    selection_reason="forced_user_material_input",
                )
            )
        return materials

    def _preference_profile_from_snapshot(self, request_snapshot: dict[str, Any]) -> dict[str, float]:
        extra_constraints = request_snapshot.get("extra_constraints") or {}
        return self.material_bridge._normalize_preference_profile(
            request_snapshot.get("preference_profile")
            or (extra_constraints.get("preference_profile") if isinstance(extra_constraints, dict) else None)
        )

    def _default_preference_profile(
        self,
        *,
        difficulty_target: str,
        question_type: str,
        business_subtype: str | None,
    ) -> dict[str, float]:
        base_profile = {
            "prefer_higher_reasoning_depth": 0.18,
            "prefer_lower_ambiguity": 0.08,
            "prefer_higher_constraint_intensity": 0.12,
            "penalty_tolerance": 0.02,
            "repair_tolerance": 0.0,
        }
        if difficulty_target == "hard":
            base_profile.update(
                {
                    "prefer_higher_reasoning_depth": 0.28,
                    "prefer_lower_ambiguity": 0.1,
                    "prefer_higher_constraint_intensity": 0.18,
                }
            )
        elif difficulty_target == "medium":
            base_profile.update(
                {
                    "prefer_higher_reasoning_depth": 0.24,
                    "prefer_lower_ambiguity": 0.09,
                    "prefer_higher_constraint_intensity": 0.16,
                }
            )

        if question_type == "main_idea" and business_subtype == "center_understanding":
            base_profile["prefer_higher_reasoning_depth"] = round(
                base_profile["prefer_higher_reasoning_depth"] + 0.06, 4
            )
            base_profile["prefer_higher_constraint_intensity"] = round(
                base_profile["prefer_higher_constraint_intensity"] + 0.04, 4
            )
        return self.material_bridge._normalize_preference_profile(base_profile)

    @staticmethod
    def _feedback_snapshot_from_material(material: MaterialSelectionResult) -> dict[str, Any]:
        source = material.source or {}
        snapshot = source.get("feedback_snapshot") if isinstance(source.get("feedback_snapshot"), dict) else {}
        return deepcopy(snapshot)

    def _attach_feedback_runtime_context(
        self,
        *,
        built_item: dict[str, Any],
        material: MaterialSelectionResult,
        request_snapshot: dict[str, Any],
        runtime_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        updated_snapshot = dict(runtime_snapshot or {})
        updated_snapshot["feedback_snapshot"] = deepcopy(
            built_item.get("feedback_snapshot") or self._feedback_snapshot_from_material(material)
        )
        updated_snapshot["preference_profile"] = deepcopy(
            built_item.get("preference_profile") or self._preference_profile_from_snapshot(request_snapshot)
        )
        return updated_snapshot

    def _resolve_question_card_binding(
        self,
        *,
        question_card_id: str | None,
        question_type: str,
        business_subtype: str | None,
        pattern_id: str | None,
    ) -> dict:
        binding = self.question_card_binding.resolve(
            question_card_id=question_card_id,
            question_type=question_type,
            business_subtype=business_subtype,
            require_match=True,
        )
        mapping_inputs = {
            "question_card_id": question_card_id,
            "question_type": question_type,
            "business_subtype": business_subtype,
            "pattern_id": pattern_id,
        }
        return {
            **binding,
            "mapping_status": "mapped",
            "mapping_reason": binding.get("binding_reason"),
            "mapping_inputs": mapping_inputs,
        }

    @staticmethod
    def _apply_question_card_binding(*, standard_request: dict, question_card_binding: dict) -> dict:
        runtime_binding = question_card_binding.get("runtime_binding") or {}
        bound_question_type = runtime_binding.get("question_type")
        if not bound_question_type:
            return standard_request

        updated_request = dict(standard_request)
        updated_request["question_type"] = bound_question_type
        updated_request["business_subtype"] = runtime_binding.get("business_subtype")
        return updated_request

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

    def _append_review_fallback_items(
        self,
        *,
        items: list[dict],
        rejected_candidates: list[dict],
        limit: int,
        failure_record_path: str | None,
    ) -> int:
        selected_item_ids = {str(item.get("item_id") or "") for item in items if item.get("item_id")}
        remaining_slots = max(0, limit - len(items))
        if remaining_slots <= 0:
            return 0

        added_count = 0
        ranked_rejected = sorted(rejected_candidates, key=lambda entry: entry["rank_score"], reverse=True)
        for rejected in ranked_rejected:
            if added_count >= remaining_slots:
                break
            fallback_item = rejected["item"]
            fallback_item_id = str(fallback_item.get("item_id") or "")
            if fallback_item_id and fallback_item_id in selected_item_ids:
                continue
            fallback_notes = list(fallback_item.get("notes") or [])
            fallback_notes.extend(
                [
                    "frontend_display_fallback",
                    "blocked_attempt_returned_for_review",
                ]
            )
            if failure_record_path:
                fallback_notes.append(f"failure_record_md={failure_record_path}")
            fallback_item["notes"] = fallback_notes
            self.repository.save_version(fallback_item.pop("_version_record"))
            self.repository.save_item(fallback_item)
            items.append(fallback_item)
            if fallback_item_id:
                selected_item_ids.add(fallback_item_id)
            added_count += 1
        return added_count

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
        return None

    def _apply_control_overrides(self, request_snapshot: dict, control_overrides: dict, instruction: str | None) -> dict:
        snapshot = deepcopy(request_snapshot)
        snapshot.setdefault("type_slots", {})
        snapshot.setdefault("extra_constraints", {})
        for transient_key in ("material_id", "material_text", "manual_patch"):
            snapshot["type_slots"].pop(transient_key, None)

        allowed_top_level_keys = {
            "question_type",
            "business_subtype",
            "pattern_id",
            "difficulty_target",
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
        unexpected_top_level_keys = sorted(set(control_overrides.keys()) - allowed_top_level_keys)
        if unexpected_top_level_keys:
            raise DomainError(
                "Review control overrides contain unsupported fields.",
                status_code=422,
                details={"disallowed_input_fields": unexpected_top_level_keys},
            )
        for key in ("question_type", "business_subtype", "pattern_id", "difficulty_target", "topic", "passage_style", "use_fewshot", "fewshot_mode", "material_policy"):
            if key in control_overrides:
                snapshot[key] = control_overrides[key]

        if "extra_constraints" in control_overrides:
            if not isinstance(control_overrides["extra_constraints"], dict):
                raise DomainError(
                    "Review extra_constraints overrides must be an object.",
                    status_code=422,
                    details={"field": "extra_constraints"},
                )
            allowed_extra_keys = {
                str(key)
                for key in snapshot["extra_constraints"].keys()
                if str(key) not in self.INTERNAL_EXTRA_CONSTRAINT_FIELDS
            }
            allowed_extra_keys.update(
                {
                    "review_distractor_strategy",
                    "review_distractor_intensity",
                    "review_difficulty_target",
                    "review_adjustment_scope",
                    "review_keep_correct_answer_fixed",
                }
            )
            unknown_extra_keys = sorted(set(control_overrides["extra_constraints"].keys()) - allowed_extra_keys)
            if unknown_extra_keys:
                raise DomainError(
                    "Review extra_constraints overrides contain unsupported nested fields.",
                    status_code=422,
                    details={"disallowed_input_fields": [f"extra_constraints.{key}" for key in unknown_extra_keys]},
                )
            snapshot["extra_constraints"].update(control_overrides["extra_constraints"])
        if "type_slots" in control_overrides:
            if not isinstance(control_overrides["type_slots"], dict):
                raise DomainError(
                    "Review type_slots overrides must be an object.",
                    status_code=422,
                    details={"field": "type_slots"},
                )
            allowed_type_slot_keys = {str(key) for key in snapshot["type_slots"].keys()}
            unknown_type_slot_keys = sorted(set(control_overrides["type_slots"].keys()) - allowed_type_slot_keys)
            if unknown_type_slot_keys:
                raise DomainError(
                    "Review type_slots overrides contain unsupported nested fields.",
                    status_code=422,
                    details={"disallowed_input_fields": [f"type_slots.{key}" for key in unknown_type_slot_keys]},
                )
            snapshot["type_slots"].update(control_overrides["type_slots"])
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
        for key in ("scoring", "selected_task_scoring", "task_scoring", "decision_meta", "feedback_snapshot", "ranking_meta"):
            source.pop(key, None)
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

    def _extract_order_sequence(self, text: str) -> list[int]:
        raw = str(text or "").strip()
        if not raw:
            return []
        circled_map = {
            "①": 1,
            "②": 2,
            "③": 3,
            "④": 4,
            "⑤": 5,
            "⑥": 6,
            "⑦": 7,
            "⑧": 8,
            "⑨": 9,
            "⑩": 10,
        }
        circled = [circled_map[ch] for ch in raw if ch in circled_map]
        if circled:
            return circled
        return [int(match) for match in re.findall(r"\d+", raw)]

    def _format_order_sequence(self, order: list[int]) -> str:
        circled = {1: "①", 2: "②", 3: "③", 4: "④", 5: "⑤", 6: "⑥", 7: "⑦", 8: "⑧", 9: "⑨", 10: "⑩"}
        return "".join(circled.get(value, str(value)) for value in order)

    def _derive_sentence_order_options(self, correct_order: list[int], existing_options: dict[str, str]) -> dict[str, str]:
        sequences: list[list[int]] = []
        seen: set[tuple[int, ...]] = set()

        def add_sequence(sequence: list[int]) -> None:
            key = tuple(sequence)
            if len(sequence) != 6 or sorted(sequence) != [1, 2, 3, 4, 5, 6] or key in seen:
                return
            seen.add(key)
            sequences.append(sequence)

        add_sequence(correct_order)
        for value in existing_options.values():
            add_sequence(self._extract_order_sequence(value))

        fallback_variants = [
            correct_order[:1] + correct_order[2:3] + correct_order[1:2] + correct_order[3:],
            correct_order[:2] + correct_order[3:4] + correct_order[2:3] + correct_order[4:],
            correct_order[:3] + correct_order[4:5] + correct_order[3:4] + correct_order[5:],
            correct_order[1:2] + correct_order[:1] + correct_order[2:],
            correct_order[:4] + correct_order[5:6] + correct_order[4:5],
        ]
        for variant in fallback_variants:
            add_sequence(variant)
            if len(sequences) >= 4:
                break

        while len(sequences) < 4:
            pivot = len(sequences)
            variant = correct_order[:]
            left = pivot % 5
            right = left + 1
            variant[left], variant[right] = variant[right], variant[left]
            add_sequence(variant)
            if len(sequences) >= 4:
                break

        letters = ("A", "B", "C", "D")
        return {letter: self._format_order_sequence(sequence) for letter, sequence in zip(letters, sequences[:4], strict=False)}

    def _build_sentence_order_analysis(
        self,
        correct_order: list[int],
        original_sentences: list[str],
        options: dict[str, str],
        answer: str,
    ) -> str:
        order_text = self._format_order_sequence(correct_order)
        ordered_sentences = [
            original_sentences[index - 1].strip()
            for index in correct_order
            if 0 < index <= len(original_sentences)
        ]
        first_sentence = ordered_sentences[0] if ordered_sentences else ""
        last_sentence = ordered_sentences[-1] if ordered_sentences else ""
        first_hint = re.split(r"[，。；：]", first_sentence)[0].strip("“”\" ") if first_sentence else ""
        last_hint = re.split(r"[，。；：]", last_sentence)[0].strip("“”\" ") if last_sentence else ""
        first_mismatch_letters = []
        tail_mismatch_letters = []
        for letter, value in (options or {}).items():
            seq = self._extract_order_sequence(value)
            if len(seq) != 6 or seq == correct_order:
                continue
            if seq and seq[0] != correct_order[0]:
                first_mismatch_letters.append(letter)
            if seq and seq[-1] != correct_order[-1]:
                tail_mismatch_letters.append(letter)
        pieces = []
        if first_hint:
            pieces.append(f"先看首句，{self._format_order_sequence([correct_order[0]])}句以“{first_hint}”起笔，更适合作为全段起点。")
            if first_mismatch_letters:
                pieces.append(f"据此可先排除{ '、'.join(first_mismatch_letters) }项中首句放置不当的组合。")
        if len(correct_order) >= 4:
            middle_text = self._format_order_sequence(correct_order[1:-1])
            pieces.append(f"中间部分按 {middle_text} 依次展开，重点核对承接、递进和局部捆绑是否顺畅。")
        if last_hint:
            pieces.append(f"再看尾句，{self._format_order_sequence([correct_order[-1]])}句以“{last_hint}”形成收束，更符合完整行文。")
            if tail_mismatch_letters:
                pieces.append(f"由此还能进一步排除{ '、'.join(tail_mismatch_letters) }项中尾句收束不当的排序。")
        pieces.append(f"综合来看，只有{answer}项与正确顺序 {order_text} 完全一致，因此答案为{answer}。")
        return "".join(pieces)

    def build_sentence_order_question(self, question: GeneratedQuestion, *, material_text: str) -> GeneratedQuestion:
        original_sentences = self._extract_sortable_units_from_text(material_text)[:6]
        if len(original_sentences) < 6:
            return question

        existing_correct_order = list(question.correct_order or [])
        answer = str(question.answer or "").strip().upper()
        answer_sequence = self._extract_order_sequence((question.options or {}).get(answer, ""))
        if len(answer_sequence) == 6 and sorted(answer_sequence) == [1, 2, 3, 4, 5, 6]:
            correct_order = answer_sequence
        elif len(existing_correct_order) == 6 and sorted(existing_correct_order) == [1, 2, 3, 4, 5, 6]:
            correct_order = existing_correct_order
        else:
            fallback_sequences = [
                self._extract_order_sequence(value)
                for value in (question.options or {}).values()
            ]
            fallback_sequences = [
                sequence for sequence in fallback_sequences if len(sequence) == 6 and sorted(sequence) == [1, 2, 3, 4, 5, 6]
            ]
            correct_order = fallback_sequences[0] if fallback_sequences else [1, 2, 3, 4, 5, 6]
        if correct_order == [1, 2, 3, 4, 5, 6]:
            fallback_sequences = [
                self._extract_order_sequence(value)
                for value in (question.options or {}).values()
            ]
            non_trivial = [
                sequence
                for sequence in fallback_sequences
                if len(sequence) == 6 and sorted(sequence) == [1, 2, 3, 4, 5, 6] and sequence != [1, 2, 3, 4, 5, 6]
            ]
            if non_trivial:
                correct_order = non_trivial[0]
            else:
                correct_order = [2, 1, 3, 4, 6, 5]

        rebuilt_options = self._derive_sentence_order_options(correct_order, question.options or {})
        rebuilt_answer = next(
            (letter for letter, value in rebuilt_options.items() if self._extract_order_sequence(value) == correct_order),
            "A",
        )
        metadata = dict(question.metadata or {})
        metadata["sentence_order_recomputed"] = True
        metadata["sentence_order_truth_source"] = "correct_order"
        return question.model_copy(
            update={
                "stem": "将以下6个句子重新排列，语序正确的一项是：",
                "original_sentences": original_sentences,
                "correct_order": correct_order,
                "options": rebuilt_options,
                "answer": rebuilt_answer,
                "analysis": self._build_sentence_order_analysis(correct_order, original_sentences, rebuilt_options, rebuilt_answer),
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

    def _refine_material_if_needed(
        self,
        material: MaterialSelectionResult,
        *,
        request_snapshot: dict[str, Any] | None = None,
    ) -> MaterialSelectionResult:
        if bool((material.source or {}).get("forced_user_material")):
            return material

        refinement_mode = self._resolve_material_refinement_mode(
            request_snapshot=request_snapshot,
            material=material,
        )
        cleaned_seed = self._clean_material_text(material.text)
        refined_material = material
        if cleaned_seed and cleaned_seed != material.text:
            refined_material = material.model_copy(
                update={
                    "text": cleaned_seed,
                    "text_refined": True,
                    "refinement_reason": "rule_cleanup",
                    "source": {
                        **(material.source or {}),
                        "material_refinement_mode": refinement_mode,
                    },
                }
            )
        elif refinement_mode != str((material.source or {}).get("material_refinement_mode") or "").strip():
            refined_material = material.model_copy(
                update={
                    "source": {
                        **(material.source or {}),
                        "material_refinement_mode": refinement_mode,
                    },
                }
            )

        base_text = refined_material.text
        if not self._needs_material_refinement(base_text):
            return refined_material

        try:
            system_prompt, user_prompt = self._build_material_refinement_prompts(
                refinement_mode=refinement_mode,
                cleaned_seed=base_text,
                original_text=material.text,
            )
            response = self.llm_gateway.generate_json(
                route=self._material_refinement_route(),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema_name="material_refinement",
                schema=MaterialRefinementDraft.model_json_schema(),
            )
            refined = self.material_refinement_adapter.validate_python(response)
            refined_text = self._clean_material_text(refined.refined_text)
            if refined_text and self._is_safe_material_refinement(
                original_text=base_text,
                refined_text=refined_text,
                refinement_mode=refinement_mode,
            ) and (refined.changed or refined_text != base_text):
                return refined_material.model_copy(
                    update={
                        "text": refined_text,
                        "text_refined": True,
                        "refinement_reason": refined.reason or refinement_mode,
                        "source": {
                            **(refined_material.source or {}),
                            "material_refinement_mode": refinement_mode,
                        },
                    }
                )
        except Exception:  # noqa: BLE001
            return refined_material
        return refined_material

    def _clean_material_text(self, text: str) -> str:
        normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        if not normalized:
            return ""

        normalized = self._strip_material_template_labels(normalized)
        normalized = self._strip_outline_section_headings(normalized)
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

    def _strip_outline_section_headings(self, text: str) -> str:
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text or "") if part.strip()]
        if not paragraphs:
            return (text or "").strip()

        cleaned_paragraphs: list[str] = []
        heading_pattern = re.compile(
            r"^(?:[(（]?\d+[)）]|[一二三四五六七八九十百千万]+[、.．])\s*"
            r"([^\n。；：:]{2,16})[。；：:]\s*"
        )
        standalone_heading_pattern = re.compile(
            r"^(?:[(（]?\d+[)）]?|[一二三四五六七八九十百千万]+[、.．])\s*([^\n。！？!?；;：:]{2,24})$"
        )

        for index, paragraph in enumerate(paragraphs):
            candidate = paragraph.strip()
            match = heading_pattern.match(candidate)
            if match:
                title = match.group(1).strip()
                body = candidate[match.end():].strip()
                if body and not re.match(r"^[A-DＡ-Ｄ][.．、:：)]", body):
                    candidate = body
                elif len(title) <= 12:
                    candidate = body or title
            else:
                standalone_match = standalone_heading_pattern.match(candidate)
                if standalone_match:
                    title = standalone_match.group(1).strip()
                    next_paragraph = paragraphs[index + 1].strip() if index + 1 < len(paragraphs) else ""
                    if title and len(title) <= 20 and next_paragraph:
                        continue

            cleaned_paragraphs.append(candidate.strip())

        compacted = [paragraph for paragraph in cleaned_paragraphs if paragraph]
        return "\n\n".join(compacted).strip()

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

    def _build_reference_hard_constraints(
        self,
        *,
        question_type: str | None,
        structure_constraints: dict[str, object],
        question_card_binding: dict[str, object] | None = None,
    ) -> list[str]:
        if not question_type or not structure_constraints:
            return []

        formal_facts = self._collect_reference_hard_constraint_facts(
            question_type=question_type,
            structure_constraints=structure_constraints,
            question_card_binding=question_card_binding,
        )
        lines = self._format_reference_hard_constraint_lines(
            question_type=question_type,
            formal_facts=formal_facts,
        )
        lines.extend(
            self._legacy_reference_hard_constraint_residuals(
                question_type=question_type,
                structure_constraints=structure_constraints,
            )
        )
        return lines

    def _collect_reference_hard_constraint_facts(
        self,
        *,
        question_type: str,
        structure_constraints: dict[str, object],
        question_card_binding: dict[str, object] | None = None,
    ) -> dict[str, object]:
        question_card = ((question_card_binding or {}).get("question_card") or {}) if isinstance(question_card_binding, dict) else {}
        validator_contract = question_card.get("validator_contract") or {}
        slot_extensions = question_card.get("slot_extensions") or {}
        reference_hard_constraints = question_card.get("reference_hard_constraints") or {}
        facts: dict[str, object] = {"preserve_question_type": True}

        if question_type == "sentence_order":
            card_unit_count = 0
            if isinstance(validator_contract, dict):
                sentence_order_contract = validator_contract.get("sentence_order") or {}
                if isinstance(sentence_order_contract, dict):
                    try:
                        card_unit_count = int(sentence_order_contract.get("sortable_unit_count") or 0)
                    except (TypeError, ValueError):
                        card_unit_count = 0
            try:
                structure_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
            except (TypeError, ValueError):
                structure_unit_count = 0

            facts["preserve_unit_count"] = bool(
                structure_constraints.get("preserve_unit_count")
                or (slot_extensions.get("preserve_unit_count") if isinstance(slot_extensions, dict) else False)
            )
            facts["sortable_unit_count"] = structure_unit_count or card_unit_count or None
            facts["logic_modes"] = [str(item) for item in (structure_constraints.get("logic_modes") or []) if str(item).strip()]
            facts["binding_types"] = [str(item) for item in (structure_constraints.get("binding_types") or []) if str(item).strip()]
            facts["allow_minimal_cue_sharpening"] = bool(reference_hard_constraints.get("allow_minimal_cue_sharpening"))
            facts["avoid_trivial_correct_order"] = bool(reference_hard_constraints.get("avoid_trivial_correct_order"))
            facts["require_unique_defensible_order"] = bool(reference_hard_constraints.get("require_unique_defensible_order"))
            return facts

        if question_type == "sentence_fill":
            facts["blank_position"] = str(structure_constraints.get("blank_position") or "").strip()
            facts["function_type"] = normalize_sentence_fill_function_type(structure_constraints.get("function_type"))
            facts["unit_type"] = str(structure_constraints.get("unit_type") or "").strip()
            return facts

        return facts

    def _format_reference_hard_constraint_lines(
        self,
        *,
        question_type: str,
        formal_facts: dict[str, object],
    ) -> list[str]:
        lines: list[str] = []
        if formal_facts.get("preserve_question_type"):
            if question_type == "sentence_order":
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "preserve_question_type_template",
                    )
                )
            elif question_type == "sentence_fill":
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_fill",
                        "preserve_question_type_template",
                    )
                )

        if question_type == "sentence_order":
            if formal_facts.get("preserve_unit_count") and formal_facts.get("sortable_unit_count"):
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "preserve_unit_count_template",
                    ).format(unit_count=formal_facts["sortable_unit_count"])
                )
            logic_modes = [str(item) for item in (formal_facts.get("logic_modes") or []) if str(item).strip()]
            if logic_modes:
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "preserve_logic_modes_template",
                    ).format(logic_modes=", ".join(logic_modes))
                )
            binding_types = [str(item) for item in (formal_facts.get("binding_types") or []) if str(item).strip()]
            if binding_types:
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "preserve_binding_types_template",
                    ).format(binding_types=", ".join(binding_types))
                )
            if formal_facts.get("allow_minimal_cue_sharpening"):
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "allow_minimal_cue_sharpening_template",
                    )
                )
            if formal_facts.get("avoid_trivial_correct_order"):
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "avoid_trivial_correct_order_template",
                    )
                )
            if formal_facts.get("require_unique_defensible_order"):
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_order",
                        "require_unique_defensible_order_template",
                    )
                )
            return lines

        if question_type == "sentence_fill":
            blank_position = str(formal_facts.get("blank_position") or "").strip()
            function_type = normalize_sentence_fill_function_type(formal_facts.get("function_type"))
            unit_type = str(formal_facts.get("unit_type") or "").strip()
            if blank_position:
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_fill",
                        "blank_position_template",
                    ).format(blank_position=blank_position)
                )
            if function_type:
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_fill",
                        "function_type_template",
                    ).format(function_type=function_type)
                )
            if unit_type:
                lines.append(
                    self._prompt_asset_text(
                        "reference_hard_constraints",
                        "sentence_fill",
                        "unit_type_template",
                    ).format(unit_type=unit_type)
                )
            return lines

        return lines

    def _deprecated_reference_hard_constraint_residuals(
        self,
        *,
        question_type: str,
        structure_constraints: dict[str, object],
    ) -> list[str]:
        # Legacy residual: these sentence-order repair rules still lack a formal structured source.
        if question_type != "sentence_order" or not structure_constraints:
            return []

        return [
            "For sentence-order repair, you may do minimal cue sharpening inside existing units, but you must preserve unit order and the original evidence meaning.",
            "When a reference question is provided, do not output the trivial order 鈶犫憽鈶⑩懀鈶も懃 as the final correct order unless the source material itself uniquely requires that exact arrangement.",
            "Actively shuffle the correct order away from 鈶犫憽鈶⑩懀鈶も懃 while preserving a single uniquely defensible answer.",
        ]

    def _needs_material_refinement(self, text: str) -> bool:
        clean = (text or "").strip()
        if not clean:
            return False
        if "[BLANK]" in clean or "____" in clean or "___" in clean:
            return False
        if self._strip_outline_section_headings(clean) != clean:
            return True
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
        bridge_hints = self._merge_material_bridge_hints(
            self._material_bridge_hints(source_question_analysis),
            self._requested_pattern_bridge_hints(
                question_type=item["question_type"],
                pattern_id=request_snapshot.get("pattern_id") or item.get("pattern_id"),
            ),
        )
        candidates = self.material_bridge.list_material_options(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            question_card_id=(request_snapshot.get("question_card_id") or (material_selection.get("question_card_id"))),
            document_genre=preferred_genre,
            material_structure_label=(material_selection.get("material_structure_label") or None),
            business_card_ids=bridge_hints["business_card_ids"],
            preferred_business_card_ids=bridge_hints["preferred_business_card_ids"],
            query_terms=bridge_hints["query_terms"],
            target_length=source_question_analysis.get("target_length"),
            length_tolerance=source_question_analysis.get("length_tolerance", 120),
            structure_constraints=bridge_hints["structure_constraints"],
            enable_anchor_adaptation=bool(source_question_analysis),
            exclude_material_ids={material_selection.get("material_id")} if material_selection.get("material_id") else None,
            limit=limit,
            difficulty_target=item.get("difficulty_target", "medium"),
            preference_profile=request_snapshot.get("preference_profile"),
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
        section_context = self._build_generation_section_context(
            built_item=built_item,
            material=material,
        )
        sections = [prompt_package["user_prompt"]]
        sections.extend(
            self._build_material_context_sections(
                material=material,
                material_prompt_extras=section_context["material_prompt_extras"],
            )
        )
        grounding_rules = self._build_answer_grounding_rules(
            question_type=built_item.get("question_type"),
            business_subtype=built_item.get("business_subtype"),
            source_question=section_context["source_question"],
            question_card_binding=section_context["effective_question_card_binding"],
        )
        if grounding_rules:
            sections.extend(
                self._build_reference_answer_grounding_sections(grounding_rules)
            )
        review_override_lines = self._build_review_override_lines(
            request_snapshot=section_context["request_snapshot"],
            question_type=built_item.get("question_type"),
        )
        if review_override_lines:
            sections.extend(self._build_repair_requirement_sections(review_override_lines))
        if section_context["source_question"]:
            sections.extend(
                self._build_reference_generation_sections(
                    question_type=built_item.get("question_type"),
                    source_question=section_context["source_question"],
                    source_question_analysis=section_context["source_question_analysis"],
                    question_card_binding=section_context["effective_question_card_binding"],
                )
            )
        if section_context["answer_anchor_text"]:
            sections.extend(
                self._build_material_answer_anchor_sections(
                    answer_anchor_text=section_context["answer_anchor_text"],
                )
            )
        if feedback_notes:
            sections.extend(self._build_repair_requirement_sections(feedback_notes))
        sections.append(self._prompt_asset_text("final_generation_instruction"))
        return sections

    def _material_readability_contract_lines(self) -> list[str]:
        return self._prompt_asset_lines("material_readability_contract")

    def _build_generation_section_context(
        self,
        *,
        built_item: dict,
        material: MaterialSelectionResult,
    ) -> dict[str, object]:
        request_snapshot = built_item.get("request_snapshot") or {}
        material_prompt_extras = ((material.source or {}).get("prompt_extras") or {}) if isinstance(material.source, dict) else {}
        return {
            "request_snapshot": request_snapshot,
            "effective_question_card_binding": self._resolve_section_question_card_binding(
                question_type=built_item.get("question_type"),
                request_snapshot=request_snapshot,
            ),
            "source_question": request_snapshot.get("source_question") or {},
            "source_question_analysis": request_snapshot.get("source_question_analysis") or {},
            "material_prompt_extras": material_prompt_extras,
            "answer_anchor_text": str(material_prompt_extras.get("answer_anchor_text") or "").strip(),
        }

    def _build_material_context_sections(
        self,
        *,
        material: MaterialSelectionResult,
        material_prompt_extras: dict[str, object],
    ) -> list[str]:
        sections = [*self._make_prompt_section("selected_material", [material.text])]
        sections.extend(
            self._make_prompt_section(
                "original_material_evidence",
                [material.original_text or material.text],
            )
        )
        sections.extend(self._make_prompt_section("material_meta", self._material_meta_lines(material)))
        sections.extend(
            self._make_prompt_section(
                "material_readability_contract",
                self._material_readability_contract_lines(),
            )
        )
        if material_prompt_extras:
            sections.extend(
                self._make_prompt_section(
                    "material_prompt_extras",
                    self._material_prompt_extra_lines(material_prompt_extras),
                )
            )
        return sections

    def _material_meta_lines(self, material: MaterialSelectionResult) -> list[str]:
        return [
            f"material_id={material.material_id}; article_id={material.article_id}; reason={material.selection_reason}"
        ]

    def _material_prompt_extra_lines(self, material_prompt_extras: dict[str, object]) -> list[str]:
        return [str(material_prompt_extras)]

    def _build_reference_generation_sections(
        self,
        *,
        question_type: str | None,
        source_question: dict[str, object],
        source_question_analysis: dict[str, object],
        question_card_binding: dict[str, object] | None = None,
    ) -> list[str]:
        reference_context = self._build_reference_generation_context(
            question_type=question_type,
            source_question=source_question,
            source_question_analysis=source_question_analysis,
            question_card_binding=question_card_binding,
        )
        sections = self._build_reference_prompt_sections(
            reference_payload=reference_context["reference_payload"],
            source_question_analysis=reference_context["source_question_analysis"],
        )
        if reference_context["hard_constraints"]:
            sections.extend(
                self._build_reference_hard_constraint_sections(reference_context["hard_constraints"])
            )
        return sections

    def _build_reference_generation_context(
        self,
        *,
        question_type: str | None,
        source_question: dict[str, object],
        source_question_analysis: dict[str, object],
        question_card_binding: dict[str, object] | None = None,
    ) -> dict[str, object]:
        structure_constraints = source_question_analysis.get("structure_constraints") or {}
        return {
            "reference_payload": self._prepare_reference_prompt_payload(source_question),
            "source_question_analysis": source_question_analysis,
            "hard_constraints": self._build_reference_hard_constraints(
                question_type=question_type,
                structure_constraints=structure_constraints,
                question_card_binding=question_card_binding,
            ),
        }

    def _prepare_reference_prompt_payload(self, source_question: dict[str, object]) -> dict[str, object]:
        reference_payload = deepcopy(source_question)
        if isinstance(reference_payload.get("passage"), str) and len(reference_payload["passage"]) > 600:
            reference_payload["passage"] = f"{reference_payload['passage'][:600]}...(truncated)"
        if "analysis" in reference_payload:
            reference_payload["analysis"] = "[omitted_for_structure_only_fewshot]"
        return reference_payload

    def _build_reference_hard_constraint_sections(self, hard_constraints: list[str]) -> list[str]:
        return self._make_prompt_section("hard_constraints", hard_constraints)

    def _build_reference_answer_grounding_sections(self, grounding_rules: list[str]) -> list[str]:
        return self._make_prompt_section("answer_grounding_contract", grounding_rules)

    def _build_material_answer_anchor_sections(self, *, answer_anchor_text: str) -> list[str]:
        return self._make_prompt_section(
            "material_answer_anchor",
            [
                answer_anchor_text,
                self._prompt_asset_text("material_answer_anchor", "explanation"),
            ],
        )

    def _build_repair_requirement_sections(self, feedback_notes: list[str]) -> list[str]:
        return self._make_prompt_section("repair_requirements", feedback_notes)

    @staticmethod
    def _build_review_override_lines(
        *,
        request_snapshot: dict[str, Any],
        question_type: str | None,
    ) -> list[str]:
        extra_constraints = request_snapshot.get("extra_constraints") or {}
        if not isinstance(extra_constraints, dict):
            return []
        strategy = str(extra_constraints.get("review_distractor_strategy") or "").strip()
        intensity = str(extra_constraints.get("review_distractor_intensity") or "").strip()
        difficulty_target = str(extra_constraints.get("review_difficulty_target") or "").strip()
        adjustment_scope = str(extra_constraints.get("review_adjustment_scope") or "").strip()
        keep_correct_answer_fixed = extra_constraints.get("review_keep_correct_answer_fixed")
        if not any([strategy, intensity, difficulty_target, adjustment_scope, keep_correct_answer_fixed is not None]):
            return []
        lines = ["Apply the reviewer-side adjustment requirements below while keeping the material boundary unchanged."]
        if question_type:
            lines.append(f"Keep the question family fixed as {question_type}.")
        if strategy:
            lines.append(f"Distractor error mode target: {strategy}.")
        if intensity:
            lines.append(f"Distractor intensity target: {intensity}.")
        if difficulty_target:
            lines.append(f"Target question difficulty after revision: {difficulty_target}.")
        if adjustment_scope:
            lines.append(f"Revision scope target: {adjustment_scope}.")
        if keep_correct_answer_fixed is True:
            lines.append("Keep the current correct answer unchanged unless it is materially indefensible.")
        elif keep_correct_answer_fixed is False:
            lines.append("You may adjust the current correct answer if needed to satisfy the reviewer-side control target.")
        return lines

    def _resolve_section_question_card_binding(
        self,
        *,
        question_type: str | None,
        request_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        snapshot = request_snapshot or {}
        question_card_binding = snapshot.get("question_card_binding")
        if isinstance(question_card_binding, dict) and isinstance(question_card_binding.get("question_card"), dict):
            return question_card_binding

        question_card_id = str(snapshot.get("question_card_id") or "").strip()
        resolved_question_type = str(snapshot.get("question_type") or question_type or "").strip()
        if not question_card_id or not resolved_question_type:
            return question_card_binding if isinstance(question_card_binding, dict) else None

        try:
            return self._resolve_question_card_binding(
                question_card_id=question_card_id,
                question_type=resolved_question_type,
                business_subtype=snapshot.get("business_subtype"),
                pattern_id=snapshot.get("pattern_id"),
            )
        except DomainError:
            return question_card_binding if isinstance(question_card_binding, dict) else None

    def _build_answer_grounding_rules(
        self,
        *,
        question_type: str | None,
        business_subtype: str | None = None,
        source_question: dict[str, object],
        question_card_binding: dict[str, object] | None = None,
    ) -> list[str]:
        formal_facts = self._collect_answer_grounding_facts(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card_binding=question_card_binding,
        )
        rules = self._format_answer_grounding_lines(
            question_type=question_type,
            business_subtype=business_subtype,
            formal_facts=formal_facts,
        )
        return rules

    def _collect_answer_grounding_facts(
        self,
        *,
        question_type: str | None,
        business_subtype: str | None = None,
        question_card_binding: dict[str, object] | None = None,
    ) -> dict[str, object]:
        question_card = ((question_card_binding or {}).get("question_card") or {}) if isinstance(question_card_binding, dict) else {}
        answer_grounding = question_card.get("answer_grounding") or {}
        if not isinstance(answer_grounding, dict):
            return {}

        facts: dict[str, object] = {}
        for key in (
            "require_material_traceability",
            "disallow_unsupported_extensions",
            "require_correct_option_material_defensibility",
            "require_analysis_material_evidence",
            "align_with_reference_elimination_style",
        ):
            if key in answer_grounding:
                facts[key] = bool(answer_grounding.get(key))

        facts["unsupported_extension_types"] = [
            str(item)
            for item in (answer_grounding.get("unsupported_extension_types") or [])
            if str(item).strip()
        ]
        facts["distractor_sources"] = [
            str(item)
            for item in (answer_grounding.get("distractor_sources") or [])
            if str(item).strip()
        ]

        main_idea_subtype = self._resolve_answer_grounding_main_idea_subtype(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card_binding=question_card_binding,
        )
        if main_idea_subtype in {"title_selection", "center_understanding"}:
            facts["main_idea_subtype"] = main_idea_subtype
            for key in (
                "require_central_meaning_alignment",
                "disallow_detail_as_correct_answer",
                "disallow_stronger_conclusion",
            ):
                if key in answer_grounding:
                    facts[key] = bool(answer_grounding.get(key))
            expression_fidelity_mode = str(answer_grounding.get("expression_fidelity_mode") or "").strip()
            if expression_fidelity_mode:
                facts["expression_fidelity_mode"] = expression_fidelity_mode
            for key in (
                "allow_meaning_preserving_creation",
                "allow_cross_sentence_abstraction",
                "allow_exam_style_rephrasing",
            ):
                if key in answer_grounding:
                    facts[key] = bool(answer_grounding.get(key))
            return facts

        if question_type == "sentence_order":
            for key in (
                "preserve_sortable_units",
                "preserve_sentence_level_units",
                "allow_minimal_local_cue_repair",
                "cue_repair_requires_latent_clues",
            ):
                if key in answer_grounding:
                    facts[key] = bool(answer_grounding.get(key))
            return facts

        if question_type == "sentence_fill":
            if "require_blank_support_from_original_context" in answer_grounding:
                facts["require_blank_support_from_original_context"] = bool(
                    answer_grounding.get("require_blank_support_from_original_context")
                )
            facts["blank_replacement_scope"] = str(answer_grounding.get("blank_replacement_scope") or "").strip()
            return facts

        return facts

    def _format_answer_grounding_lines(
        self,
        *,
        question_type: str | None,
        business_subtype: str | None = None,
        formal_facts: dict[str, object],
    ) -> list[str]:
        lines: list[str] = []
        if formal_facts.get("require_material_traceability"):
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "require_material_traceability_template",
                )
            )
        extension_types = [
            str(item) for item in (formal_facts.get("unsupported_extension_types") or []) if str(item).strip()
        ]
        if formal_facts.get("disallow_unsupported_extensions") and extension_types:
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "disallow_unsupported_extensions_template",
                ).format(extension_types=", ".join(extension_types))
            )
        if formal_facts.get("require_correct_option_material_defensibility"):
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "require_correct_option_material_defensibility_template",
                )
            )
        distractor_sources = [str(item) for item in (formal_facts.get("distractor_sources") or []) if str(item).strip()]
        if distractor_sources:
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "distractor_sources_template",
                ).format(distractor_sources=", ".join(distractor_sources))
            )
        if formal_facts.get("require_analysis_material_evidence"):
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "require_analysis_material_evidence_template",
                )
            )
        if formal_facts.get("align_with_reference_elimination_style"):
            lines.append(
                self._prompt_asset_text(
                    "answer_grounding",
                    "base",
                    "align_with_reference_elimination_style_template",
                )
            )

        if question_type == "sentence_order":
            if formal_facts.get("preserve_sortable_units"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_order",
                        "preserve_sortable_units_template",
                    )
                )
            if formal_facts.get("preserve_sentence_level_units"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_order",
                        "preserve_sentence_level_units_template",
                    )
                )
            if formal_facts.get("allow_minimal_local_cue_repair"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_order",
                        "allow_minimal_local_cue_repair_template",
                    )
                )
            if formal_facts.get("cue_repair_requires_latent_clues"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_order",
                        "cue_repair_requires_latent_clues_template",
                    )
                )
            return lines

        if question_type == "sentence_fill":
            if formal_facts.get("require_blank_support_from_original_context"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_fill",
                        "require_blank_support_from_original_context_template",
                    )
                )
            blank_replacement_scope = str(formal_facts.get("blank_replacement_scope") or "").strip()
            if blank_replacement_scope:
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "sentence_fill",
                        "blank_replacement_scope_template",
                    ).format(blank_replacement_scope=blank_replacement_scope)
                )
            return lines

        main_idea_subtype = (
            str(formal_facts.get("main_idea_subtype") or "").strip()
            or (business_subtype if question_type == "main_idea" else None)
            or ""
        )
        if question_type == "main_idea" and main_idea_subtype in {"title_selection", "center_understanding"}:
            if formal_facts.get("require_central_meaning_alignment"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "title_selection",
                        "require_central_meaning_alignment_template",
                    )
                )
            if formal_facts.get("disallow_detail_as_correct_answer"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "title_selection",
                        "disallow_detail_as_correct_answer_template",
                    )
                )
            if formal_facts.get("disallow_stronger_conclusion"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "title_selection",
                        "disallow_stronger_conclusion_template",
                    )
                )
            expression_fidelity_mode = str(formal_facts.get("expression_fidelity_mode") or "").strip()
            if expression_fidelity_mode == "source_strict":
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "main_idea",
                        "source_strict_template",
                    )
                )
            elif expression_fidelity_mode == "meaning_preserving" and main_idea_subtype == "center_understanding":
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        "main_idea",
                        "meaning_preserving_template",
                    )
                )
                if formal_facts.get("allow_cross_sentence_abstraction"):
                    lines.append(
                        self._prompt_asset_text(
                            "answer_grounding",
                            "main_idea",
                            "allow_cross_sentence_abstraction_template",
                        )
                    )
                if formal_facts.get("allow_exam_style_rephrasing"):
                    lines.append(
                        self._prompt_asset_text(
                            "answer_grounding",
                            "main_idea",
                            "allow_exam_style_rephrasing_template",
                        )
                    )
                if formal_facts.get("allow_meaning_preserving_creation"):
                    lines.append(
                        self._prompt_asset_text(
                            "answer_grounding",
                            "main_idea",
                            "allow_meaning_preserving_creation_template",
                        )
                    )
            return lines

        return lines

    @staticmethod
    def _resolve_answer_grounding_main_idea_subtype(
        *,
        question_type: str | None,
        business_subtype: str | None = None,
        question_card_binding: dict[str, object] | None = None,
    ) -> str | None:
        if question_type != "main_idea":
            return None
        normalized_business_subtype = str(business_subtype or "").strip() or None
        if normalized_business_subtype in {"title_selection", "center_understanding"}:
            return normalized_business_subtype
        runtime_binding = (question_card_binding or {}).get("runtime_binding") or {}
        runtime_subtype = str(runtime_binding.get("business_subtype") or "").strip() or None
        if runtime_subtype in {"title_selection", "center_understanding"}:
            return runtime_subtype
        question_card = (question_card_binding or {}).get("question_card") or {}
        card_subtype = str(question_card.get("business_subtype_id") or "").strip() or None
        if card_subtype in {"title_selection", "center_understanding"}:
            return card_subtype
        return None

    def _answer_grounding_residuals(
        self,
        *,
        question_type: str | None,
        question_card_binding: dict[str, object] | None = None,
        formal_facts: dict[str, object] | None = None,
    ) -> list[str]:
        return []

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
        if use_reference_question:
            return difficulty_target
        if difficulty_target == "easy":
            return "medium"
        if difficulty_target == "medium":
            return "hard"
        return difficulty_target

    def _deprecated_build_reference_hard_constraints_override(self, *, question_type: str | None, structure_constraints: dict[str, object], question_card_binding: dict[str, object] | None = None) -> list[str]:
        if not question_type or not structure_constraints:
            return []
        formal_facts = self._collect_reference_hard_constraint_facts(
            question_type=question_type,
            structure_constraints=structure_constraints,
            question_card_binding=question_card_binding,
        )
        lines = self._format_reference_hard_constraint_lines(
            question_type=question_type,
            formal_facts=formal_facts,
        )
        lines.extend(
            self._legacy_reference_hard_constraint_residuals(
                question_type=question_type,
                structure_constraints=structure_constraints,
            )
        )
        return lines
        if question_type == "sentence_order":
            unit_count = int(structure_constraints.get("sortable_unit_count") or 0) or 6
            logic_modes = list(structure_constraints.get("logic_modes") or [])
            binding_types = list(structure_constraints.get("binding_types") or [])
            lines = [
                "Keep the generated question as a sentence-ordering item, not another question type.",
                "For sentence-order repair, you may do minimal cue sharpening inside existing units, but you must preserve unit order, unit count, and the original evidence meaning.",
                "Render the ordering material as exactly six sortable sentence units whenever the reference skeleton is a standard six-sentence ordering item.",
                "When a reference question is provided, do not output the trivial order ①②③④⑤⑥ as the final correct order unless the source material itself uniquely requires that exact arrangement.",
                "Actively shuffle the correct order away from ①②③④⑤⑥ while preserving a single uniquely defensible answer.",
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
            function_type = normalize_sentence_fill_function_type(structure_constraints.get("function_type"))
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

    def _legacy_reference_hard_constraint_residuals(
        self,
        *,
        question_type: str,
        structure_constraints: dict[str, object],
    ) -> list[str]:
        return []

    def _should_retry_alignment(self, validation_result, source_question_analysis: dict | None) -> bool:
        if not source_question_analysis:
            return False
        if self._should_use_compact_main_idea_path(source_question_analysis) and not self._has_targeted_repair_candidate(
            question_type=str((source_question_analysis.get("style_summary") or {}).get("question_type") or ""),
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis=source_question_analysis,
        ):
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
        notes = self._prompt_asset_lines("repair_feedback", "alignment_intro")
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
        if self._should_use_compact_main_idea_path(source_question_analysis) and not self._has_targeted_repair_candidate(
            question_type=str((source_question_analysis.get("style_summary") or {}).get("question_type") or ""),
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=quality_gate_errors,
            source_question_analysis=source_question_analysis,
        ):
            return False
        if quality_gate_errors:
            return True
        if validation_result is not None and not validation_result.passed:
            return True
        overall_score = self._normalize_judge_score((evaluation_result or {}).get("overall_score"))
        return bool(overall_score and overall_score < self.QUALITY_REPAIR_RETRY_THRESHOLD)

    def _has_targeted_repair_candidate(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        validation_result,
        quality_gate_errors: list[str],
        source_question_analysis: dict | None,
    ) -> bool:
        return bool(
            self._build_targeted_repair_plan(
                question_type=question_type,
                business_subtype=business_subtype,
                validation_result=validation_result,
                quality_gate_errors=quality_gate_errors,
                source_question_analysis=source_question_analysis,
            )
        )

    @staticmethod
    def _material_bridge_hints(source_question_analysis: dict | None) -> dict[str, Any]:
        if not isinstance(source_question_analysis, dict):
            return {
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        retrieval_preferred = (
            source_question_analysis["retrieval_preferred_business_card_ids"]
            if "retrieval_preferred_business_card_ids" in source_question_analysis
            else source_question_analysis.get("business_card_ids") or []
        )
        retrieval_query_terms = (
            source_question_analysis["retrieval_query_terms"]
            if "retrieval_query_terms" in source_question_analysis
            else source_question_analysis.get("query_terms") or []
        )
        retrieval_structure_constraints = (
            source_question_analysis["retrieval_structure_constraints"]
            if "retrieval_structure_constraints" in source_question_analysis
            else source_question_analysis.get("structure_constraints") or {}
        )
        return {
            "business_card_ids": [str(card_id) for card_id in (source_question_analysis.get("retrieval_business_card_ids") or []) if str(card_id).strip()],
            "preferred_business_card_ids": [str(card_id) for card_id in retrieval_preferred if str(card_id).strip()],
            "query_terms": [str(term) for term in retrieval_query_terms if str(term).strip()],
            "structure_constraints": dict(retrieval_structure_constraints or {}),
        }

    @classmethod
    def _requested_pattern_bridge_hints(cls, *, question_type: str, pattern_id: str | None) -> dict[str, Any]:
        pattern_key = str(pattern_id or "").strip()
        hint = cls.PATTERN_BRIDGE_HINTS.get((str(question_type or "").strip(), pattern_key))
        if not hint:
            return {
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        return {
            "business_card_ids": [str(card_id) for card_id in (hint.get("business_card_ids") or []) if str(card_id).strip()],
            "preferred_business_card_ids": [str(card_id) for card_id in (hint.get("preferred_business_card_ids") or []) if str(card_id).strip()],
            "query_terms": [str(term) for term in (hint.get("query_terms") or []) if str(term).strip()],
            "structure_constraints": dict(hint.get("structure_constraints") or {}),
        }

    @staticmethod
    def _merge_material_bridge_hints(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        def _merge_list(*values: list[str]) -> list[str]:
            merged: list[str] = []
            seen: set[str] = set()
            for value_list in values:
                for value in value_list or []:
                    text = str(value).strip()
                    if not text or text in seen:
                        continue
                    seen.add(text)
                    merged.append(text)
            return merged

        base = base or {}
        override = override or {}
        return {
            "business_card_ids": _merge_list(base.get("business_card_ids") or [], override.get("business_card_ids") or []),
            "preferred_business_card_ids": _merge_list(
                override.get("preferred_business_card_ids") or [],
                base.get("preferred_business_card_ids") or [],
            ),
            "query_terms": _merge_list(base.get("query_terms") or [], override.get("query_terms") or []),
            "structure_constraints": {
                **dict(base.get("structure_constraints") or {}),
                **dict(override.get("structure_constraints") or {}),
            },
        }

    @staticmethod
    def _is_weak_main_idea_reference_signal(source_question_analysis: dict | None) -> bool:
        if not isinstance(source_question_analysis, dict):
            return False
        style_summary = source_question_analysis.get("style_summary") or {}
        if str(style_summary.get("question_type") or "") != "main_idea":
            return False
        query_terms = [term for term in (source_question_analysis.get("query_terms") or []) if str(term).strip()]
        if query_terms:
            return False
        business_card_ids = [str(card_id).strip() for card_id in (source_question_analysis.get("business_card_ids") or []) if str(card_id).strip()]
        if business_card_ids == ["theme_word_focus__main_idea"]:
            return True
        business_card_scores = source_question_analysis.get("business_card_scores") or []
        strong_scores = 0
        for entry in business_card_scores:
            if not isinstance(entry, dict):
                continue
            try:
                score = float(entry.get("score") or 0.0)
            except (TypeError, ValueError):
                score = 0.0
            if score >= 0.55:
                strong_scores += 1
        return strong_scores == 0 and len(business_card_ids) <= 1

    @classmethod
    def _should_use_compact_main_idea_path(cls, source_question_analysis: dict | None) -> bool:
        if not isinstance(source_question_analysis, dict):
            return False
        style_summary = source_question_analysis.get("style_summary") or {}
        if str(style_summary.get("question_type") or "") != "main_idea":
            return False
        return True

    def _build_quality_repair_feedback_notes(
        self,
        *,
        validation_result,
        evaluation_result: dict | None,
        quality_gate_errors: list[str],
        source_question_analysis: dict | None,
    ) -> list[str]:
        notes = self._prompt_asset_lines("repair_feedback", "quality_intro")
        style_question_type = str((source_question_analysis or {}).get("style_summary", {}).get("question_type") or "")
        if style_question_type == "sentence_order":
            notes.extend(self._prompt_asset_lines("repair_feedback", "sentence_order_quality_extra"))
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
            notes.append(self._prompt_asset_text("repair_feedback", "reviewer_feedback_template").format(judge_reason=judge_reason))
        if not validation_result.errors:
            notes.extend(validation_result.warnings[:3])
        return list(dict.fromkeys(note for note in notes if note))

    def _make_prompt_section(self, section_key: str, lines: list[str]) -> list[str]:
        return [self._section_label(section_key), *lines]

    def _build_reference_prompt_sections(
        self,
        *,
        reference_payload: dict,
        source_question_analysis: dict,
    ) -> list[str]:
        sections: list[str] = []
        sections.extend(
            self._build_reference_question_template_sections(
                reference_payload=reference_payload,
            )
        )
        sections.extend(
            self._build_reference_question_analysis_sections(
                source_question_analysis=source_question_analysis,
            )
        )
        sections.extend(self._reference_guidance_lines())
        return sections

    def _build_reference_question_template_sections(self, *, reference_payload: dict) -> list[str]:
        return self._make_prompt_section(
            "reference_question_template",
            self._reference_question_template_lines(reference_payload),
        )

    def _reference_question_template_lines(self, reference_payload: dict) -> list[str]:
        return [str(reference_payload)]

    def _build_reference_question_analysis_sections(self, *, source_question_analysis: dict) -> list[str]:
        return self._make_prompt_section(
            "reference_question_analysis",
            self._reference_question_analysis_lines(source_question_analysis),
        )

    def _reference_question_analysis_lines(self, source_question_analysis: dict) -> list[str]:
        return [str(source_question_analysis)]

    def _reference_guidance_lines(self) -> list[str]:
        return self._prompt_asset_lines("reference_guidance")

    def _section_label(self, section_key: str) -> str:
        return self._prompt_asset_text("section_labels", section_key)

    def _prompt_asset_lines(self, *path: str) -> list[str]:
        node = self._prompt_asset_node(*path)
        if not isinstance(node, list):
            raise DomainError(
                "Prompt asset path does not resolve to a list.",
                status_code=500,
                details={"path": ".".join(path)},
            )
        return [str(item) for item in node]

    def _prompt_asset_text(self, *path: str) -> str:
        node = self._prompt_asset_node(*path)
        if isinstance(node, list):
            raise DomainError(
                "Prompt asset path does not resolve to a text value.",
                status_code=500,
                details={"path": ".".join(path)},
            )
        return str(node)

    def _prompt_asset_node(self, *path: str):
        node = self.prompt_assets
        for key in path:
            if not isinstance(node, dict) or key not in node:
                raise DomainError(
                    "Prompt asset path is missing.",
                    status_code=500,
                    details={"path": ".".join(path)},
                )
            node = node[key]
        return node

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

    # Legacy helper copies kept only for audit traceability.
    # They are not used by the active generation path.
    def _legacy_refine_material_if_needed(self, material: MaterialSelectionResult) -> MaterialSelectionResult:
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

    def _legacy_clean_material_text(self, text: str) -> str:
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

    def _is_safe_material_refinement(
        self,
        original_text: str,
        refined_text: str,
        refinement_mode: str = "readability_assist",
    ) -> bool:
        original = (original_text or "").strip()
        refined = (refined_text or "").strip()
        if not original or not refined:
            return False
        if any(marker in refined for marker in ("【关键词】", "【事件】", "【点评】", "【案例】", "【延伸阅读】")):
            return False
        if self._has_dense_duplicate_units(refined):
            return False
        if len(refined) >= 80 and not re.search(r"[。！？!?]$", refined):
            return False
        if refinement_mode == self.MATERIAL_REFINEMENT_THEME_PRESERVING:
            if len(refined) < max(40, int(len(original) * 0.55)):
                return False
            if len(refined) > int(len(original) * 1.45):
                return False
            if self._material_keyword_overlap(original, refined) < 0.35:
                return False
            return True
        if len(refined) < max(40, int(len(original) * 0.7)):
            return False
        if len(refined) > int(len(original) * 1.2):
            return False
        return True

    @staticmethod
    def _material_keyword_overlap(original_text: str, refined_text: str) -> float:
        token_pattern = re.compile(r"[\u4e00-\u9fff]{2,}|[A-Za-z0-9]{2,}")
        original_tokens = set(token_pattern.findall(original_text or ""))
        refined_tokens = set(token_pattern.findall(refined_text or ""))
        if not original_tokens or not refined_tokens:
            return 0.0
        return len(original_tokens & refined_tokens) / max(1, len(original_tokens))

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

    def _legacy_strip_material_template_labels(self, text: str) -> str:
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

    def _legacy_needs_material_refinement(self, text: str) -> bool:
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
