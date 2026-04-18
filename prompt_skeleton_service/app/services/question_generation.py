from __future__ import annotations

import json
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
from app.services.distill_runtime_overlay import DistillRuntimeOverlayService
from app.services.input_decoder import DIFFICULTY_MAPPING
from app.services.llm_gateway import LLMGatewayService
from app.services.material_bridge import MaterialBridgeService
from app.services.meta_service import MetaService
from app.services.prompt_orchestrator import PromptOrchestratorService
from app.services.question_card_binding import QuestionCardBindingService
from app.services.question_generation_prompt_assets import load_question_generation_prompt_assets
from app.services.patch_scope_registry import resolve_repair_mode_scope
from app.services.question_repository import QuestionRepository
from app.services.question_snapshot_builder import QuestionSnapshotBuilder
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.sentence_fill_protocol import (
    normalize_sentence_fill_constraints,
    normalize_sentence_fill_function_type,
)
from app.services.question_validator import QuestionValidatorService
from app.services.source_question_analyzer import SourceQuestionAnalyzer
from app.services.source_question_parser import SourceQuestionParserService
from app.services.text_readability import (
    normalize_prompt_structure,
    normalize_prompt_text,
    normalize_reference_payload,
    normalize_readable_structure,
    normalize_readable_text,
    normalize_source_question_payload,
    normalize_user_material_payload,
)


logger = logging.getLogger(__name__)

_SENTENCE_ORDER_CANONICAL_CANDIDATE_TYPE = "sentence_block_group"
_SENTENCE_ORDER_CANDIDATE_TYPE_ALIASES = {
    "sentence_block_group": "sentence_block_group",
    "ordered_unit_group": "sentence_block_group",
    "weak_formal_order_group": "sentence_block_group",
}


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


class DistractorPatchDraft(BaseModel):
    option_text: str
    analysis: str


class AnalysisOnlyPatchDraft(BaseModel):
    analysis: str


class AnswerBindingPatchDraft(BaseModel):
    options: dict[str, str]
    answer: str
    analysis: str


class MaterialRefinementDraft(BaseModel):
    refined_text: str
    changed: bool = False
    reason: str | None = None


class QuestionGenerationService:
    MATERIAL_REFINEMENT_READABILITY_ASSIST = "readability_assist"
    MATERIAL_REFINEMENT_THEME_PRESERVING = "theme_preserving_rewrite"
    MATERIAL_REFINEMENT_FAMILY_COMPLIANCE = "family_shape_compliance"
    JUDGE_OVERALL_PASS_THRESHOLD = 80
    JUDGE_MATERIAL_ALIGNMENT_THRESHOLD = 70
    JUDGE_ANSWER_ANALYSIS_THRESHOLD = 70
    JUDGE_HARD_DIFFICULTY_THRESHOLD = 68
    QUALITY_REPAIR_RETRY_THRESHOLD = 84
    CONSISTENCY_HARD_FAIL_CHECKS = (
        "analysis_answer_consistency",
        "sentence_fill_anchor_grounding",
        "sentence_order_reference_unit_alignment",
    )
    MAX_ALIGNMENT_RETRIES = 2
    MAX_QUALITY_REPAIR_RETRIES = 2
    INTERNAL_EXTRA_CONSTRAINT_FIELDS = {
        "reference_business_cards",
        "reference_query_terms",
        "required_review_overrides",
        "review_instruction",
        "source_question_style_summary",
    }
    # Historical pattern IDs remain valid as input/route identifiers, but they are
    # never the sentence_fill runtime truth source. Runtime meaning must come from
    # the explicit canonical constraints below, not from parsing ID text.
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
        ("sentence_order", "dual_anchor_lock"): {
            "preferred_business_card_ids": ["sentence_order__head_tail_lock__abstract"],
            "structure_constraints": {
                "opening_anchor_type": "explicit_topic",
                "middle_structure_type": "local_binding",
                "closing_anchor_type": "conclusion",
            },
        },
        ("sentence_order", "carry_parallel_expand"): {
            "preferred_business_card_ids": ["sentence_order__deterministic_binding__abstract"],
            "structure_constraints": {
                "opening_anchor_type": "upper_context_link",
                "middle_structure_type": "parallel_expansion",
                "closing_anchor_type": "summary",
            },
        },
        ("sentence_order", "viewpoint_reason_action"): {
            "preferred_business_card_ids": ["sentence_order__discourse_logic__abstract"],
            "structure_constraints": {
                "opening_anchor_type": "viewpoint_opening",
                "middle_structure_type": "cause_effect_chain",
                "closing_anchor_type": "summary",
            },
        },
        ("sentence_order", "problem_solution_case_blocks"): {
            "preferred_business_card_ids": ["sentence_order__discourse_logic__abstract"],
            "structure_constraints": {
                "opening_anchor_type": "problem_opening",
                "middle_structure_type": "problem_solution_blocks",
                "closing_anchor_type": "case_support",
            },
        },
        ("sentence_order", "timeline_action_sequence"): {
            "preferred_business_card_ids": ["sentence_order__timeline_action_sequence__abstract"],
            "structure_constraints": {
                "opening_anchor_type": "background_intro",
                "middle_structure_type": "cause_effect_chain",
                "closing_anchor_type": "summary",
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
        self.distill_runtime_overlay = DistillRuntimeOverlayService()
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
        return cls.MATERIAL_REFINEMENT_FAMILY_COMPLIANCE

    def _build_material_refinement_prompts(
        self,
        *,
        refinement_mode: str,
        cleaned_seed: str,
        original_text: str,
        request_snapshot: dict[str, Any] | None = None,
        compliance_profile: dict[str, Any] | None = None,
        compliance_report: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        snapshot = request_snapshot or {}
        question_type = str(snapshot.get("question_type") or "").strip()
        business_subtype = str(snapshot.get("business_subtype") or "").strip()
        profile = compliance_profile or self._material_compliance_profile(
            question_type=question_type,
            business_subtype=business_subtype,
        )
        report = compliance_report or self._assess_material_compliance(cleaned_seed, profile)
        issue_labels = [str(item) for item in (report.get("issues") or []) if str(item).strip()]
        family_label = str(profile.get("family_label") or question_type or "general").strip()
        repair_permissions = [str(item) for item in (profile.get("repair_permissions") or []) if str(item).strip()]
        hard_guards = [str(item) for item in (profile.get("hard_guards") or []) if str(item).strip()]
        leaf_repair_policy = str(profile.get("leaf_structure_policy") or "").strip()

        if refinement_mode == self.MATERIAL_REFINEMENT_FAMILY_COMPLIANCE:
            system_prompt = (
                "你是一名公考真题材料修复助手。你的唯一优先目标，是把输入材料修到像真正会进入公考命题的材料。"
                "不要满足于可读或逻辑对，而要追求读起来就像真题材料。"
                "每次都必须先审查材料是否已经具备真题感；如果不具备，就主动重写到具备为止。"
                "你必须优先修复这些问题：新闻摘录感、评论切片感、条目体、手册体、小标题堆叠、资讯拼接、局部槽位太硬、上下文不自然、叶族结构不明显。"
                "允许在保留合法性真值的前提下做强修：重写句子、重组局部表达、补足自然上下文、压缩或扩展到更像真题的段落长度、把碎信息改写成自然正文。"
                "除合法性硬边界外，不要被原文表面句式束缚；如果原文展示形态不像真题，应优先改成像真题。"
                "绝对禁止新增原文没有的事实、数据、背景、立场和论证终点；绝对禁止改变主旨与合法性真值。"
            )
            user_prompt = "\n\n".join(
                [
                    f"请把下面材料修整成适合 {family_label} 制题前使用的真题材料形态。",
                    "最高目标：读起来像公考真题材料，而不是资讯摘录、评论切片、手册条目或知识点提纲。",
                    "如果当前材料只是基本可读但还不像真题，请继续主动修，直到像真题为止。",
                    f"当前合规问题：{'; '.join(issue_labels) if issue_labels else '无显式问题，但需要做母族规格化。'}",
                    f"长度约束：建议正文长度在 {profile.get('min_chars')} - {profile.get('max_chars')} 字符之间。",
                    f"形态要求：{'; '.join(profile.get('requirements') or [])}",
                    f"叶族结构策略：{leaf_repair_policy or '如果材料局部角色不够明显、叶族结构不清，请在不改变合法性真值的前提下主动修清。'}",
                    f"允许修复动作：{'; '.join(repair_permissions) if repair_permissions else '删除标题编号、改成连续正文、补足最小必要上下文、增强局部承接。'}",
                    f"硬边界：{'; '.join(hard_guards) if hard_guards else '不得新增事实，不得改变主旨、结论方向和合法性真值。'}",
                    "额外强约束：不要保留省略号新闻腔、不要保留碎标题/碎评述、不要只给一句半的硬槽位材料、不要让空位前后像机械拼接。",
                    "禁止：新增事实、改主旨、改结论方向、凭空补背景、重写成另一篇文章。",
                    "[清理后基底文段]",
                    cleaned_seed,
                    "[原始文段]",
                    original_text,
                ]
            )
            return system_prompt, user_prompt

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
        requested_pattern_id = self._extract_requested_pattern_id(
            type_slots=prepared_request.type_slots,
            extra_constraints=deepcopy(prepared_request.extra_constraints or {}),
        )
        if requested_pattern_id:
            standard_request["pattern_id"] = requested_pattern_id
            batch_meta_payload = decoded.get("batch_meta") or {}
            if isinstance(batch_meta_payload, dict):
                batch_meta_payload["pattern_id"] = requested_pattern_id
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
        pattern_warnings: list[str] = []
        standard_request["pattern_id"] = self._normalize_requested_pattern_id(
            question_type=standard_request["question_type"],
            pattern_id=standard_request.get("pattern_id"),
            warnings=pattern_warnings,
        )
        batch_meta_payload = decoded.get("batch_meta") or {}
        if isinstance(batch_meta_payload, dict):
            batch_meta_payload["pattern_id"] = standard_request.get("pattern_id")
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
        rejected_attempts: list[dict] = []
        rejected_candidates: list[dict] = []
        selected_materials = materials[: max(1, effective_count)]
        for material_index, material in enumerate(selected_materials):
            retry_result = self._run_primary_candidate_with_retries(
                material=material,
                material_index=material_index,
                retry_limit=3,
                standard_request=standard_request,
                source_question_analysis=source_question_analysis,
                request_snapshot=request_snapshot,
                batch_id=batch_id,
                request_id=request_id,
            )
            rejected_attempts.extend(retry_result["rejected_attempts"])
            if retry_result.get("best_rejected_candidate") is not None:
                rejected_candidates.append(retry_result["best_rejected_candidate"])
            if not retry_result["accepted"]:
                break
            built_item = retry_result["item"]
            self.repository.save_version(built_item.pop("_version_record"))
            self.repository.save_item(built_item)
            items.append(built_item)
            if len(items) >= effective_count:
                break

        fallback_record_path: str | None = None
        if rejected_attempts and len(items) < effective_count:
            fallback_record_path = self._write_failure_markdown_record(
                batch_id=batch_id,
                request_id=request_id,
                question_type=standard_request["question_type"],
                difficulty_target=standard_request["difficulty_target"],
                rejected_attempts=rejected_attempts,
            )

        added_fallback_count = 0
        if len(items) < effective_count and rejected_candidates:
            added_fallback_count = self._append_review_fallback_items(
                items=items,
                rejected_candidates=rejected_candidates,
                limit=effective_count,
                failure_record_path=fallback_record_path,
            )

        if len(items) < effective_count:
            raise DomainError(
                "Primary generation flow could not produce enough validator-passing questions within 3 attempts.",
                status_code=422,
                details={
                    "question_type": standard_request["question_type"],
                    "difficulty_target": standard_request["difficulty_target"],
                    "rejected_attempts": rejected_attempts[:5],
                    "failure_record_md": fallback_record_path,
                },
            )

        generation_warnings = (
            decoded.get("warnings", [])
            + pattern_warnings
            + ([target_override_warning] if target_override_warning else [])
            + material_warnings
        )
        if rejected_attempts:
            generation_warnings.append(
                f"Primary flow retried {len(rejected_attempts)} time(s) before returning validator-passing results."
            )
        if added_fallback_count:
            generation_warnings.append(
                f"Returned {added_fallback_count} blocked attempt(s) for manual review so the workbench still has reviewable questions."
            )
            if fallback_record_path:
                generation_warnings.append(f"Failure reasons were recorded to: {fallback_record_path}")

        response = {
            "batch_id": batch_id,
            "batch_meta": batch_meta.model_dump(),
            "items": items,
            "warnings": generation_warnings,
            "notes": [
                "Materials are fetched from the passage_service V2 material pool at generation time.",
                "If a reference question is provided, retrieval and generation both reuse its business-card and length signals.",
                "Reference-question runs raise generation difficulty by one level and treat the reference question as a style template.",
                "Primary generation no longer uses race-mode parallel candidates; each selected material is generated through a single chain with up to three retries.",
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
            materials = self._build_forced_user_material_candidates(
                user_material=request.user_material,
                question_card_binding=question_card_binding,
                request_snapshot=request_snapshot,
                count=max(1, effective_count),
            )
            return materials, [
                "Forced user-material mode enabled: using the user-supplied passage directly and bypassing passage_service retrieval.",
                "This result is tagged as cautionary for later adaptive analysis.",
            ]

        bridge_hints = self._merge_material_bridge_hints(
            self._material_bridge_hints(source_question_analysis),
            self._requested_taxonomy_bridge_hints(
                question_type=standard_request["question_type"],
                type_slots=request.type_slots,
                extra_constraints=request.extra_constraints,
            ),
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
            text_direction=None,
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
        request = self._normalize_request_readability_payloads(request)
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
        normalized_parsed = parsed.model_copy(update=self._normalize_source_question_payload_dict(parsed.model_dump()))
        return request.model_copy(update={"source_question": normalized_parsed})

    def _normalize_request_readability_payloads(self, request: QuestionGenerateRequest) -> QuestionGenerateRequest:
        update: dict[str, Any] = {}

        if request.source_question is not None:
            normalized_source_question = self._normalize_source_question_payload_dict(request.source_question.model_dump())
            update["source_question"] = request.source_question.model_copy(update=normalized_source_question)

        if request.user_material is not None:
            normalized_user_material = self._normalize_user_material_payload_dict(request.user_material.model_dump())
            update["user_material"] = request.user_material.model_copy(update=normalized_user_material)

        normalized_topic = normalize_readable_text(request.topic) if str(request.topic or "").strip() else request.topic
        if normalized_topic != request.topic:
            update["topic"] = normalized_topic

        if not update:
            return request
        return request.model_copy(update=update)

    @staticmethod
    def _normalize_source_question_payload_dict(source_question: dict[str, Any] | None) -> dict[str, Any]:
        return normalize_source_question_payload(source_question)

    @staticmethod
    def _normalize_user_material_payload_dict(user_material: dict[str, Any] | None) -> dict[str, Any]:
        return normalize_user_material_payload(user_material)

    @staticmethod
    def _coerce_option_text(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if value is None:
            return ""
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if dumped is not value:
                return QuestionGenerationService._coerce_option_text(dumped)
        if isinstance(value, dict):
            for key in ("text", "option_text", "value", "content", "option"):
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            for candidate in value.values():
                normalized = QuestionGenerationService._coerce_option_text(candidate)
                if normalized:
                    return normalized
            return ""
        if isinstance(value, (list, tuple)):
            for candidate in value:
                normalized = QuestionGenerationService._coerce_option_text(candidate)
                if normalized:
                    return normalized
            return ""
        return str(value).strip()

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
        merged_extra_constraints = self._merge_request_extra_constraints(request)
        requested_pattern_id = self._extract_requested_pattern_id(
            type_slots=request.type_slots,
            extra_constraints=merged_extra_constraints,
        )
        pattern_warnings: list[str] = []
        requested_pattern_id = self._normalize_requested_pattern_id(
            question_type=runtime_binding["question_type"],
            pattern_id=requested_pattern_id,
            warnings=pattern_warnings,
        )

        standard_request = {
            "question_type": runtime_binding["question_type"],
            "business_subtype": runtime_binding.get("business_subtype"),
            "pattern_id": requested_pattern_id,
            "difficulty_target": difficulty_target,
            "topic": request.topic,
            "count": effective_count,
            "passage_style": request.passage_style,
            "use_fewshot": request.use_fewshot,
            "fewshot_mode": request.fewshot_mode,
            "type_slots": deepcopy(request.type_slots or {}),
            "extra_constraints": merged_extra_constraints,
        }
        batch_meta = BatchMeta(
            requested_count=requested_count,
            effective_count=effective_count,
            question_type=runtime_binding["question_type"],
            business_subtype=runtime_binding.get("business_subtype"),
            pattern_id=requested_pattern_id,
            difficulty_target=difficulty_target,
        )
        return {
            "mapping_source": "question_card_id",
            "selected_special_type": None,
            "standard_request": standard_request,
            "batch_meta": batch_meta.model_dump(),
            "warnings": list(count_warnings) + pattern_warnings,
        }

    def _build_decode_request(self, request: QuestionGenerateRequest) -> tuple[DifyFormInput, str | None]:
        current_focus = str(request.question_focus or "").strip()
        current_business_subtype = str(request.business_subtype or "").strip()
        selected_special_types = [item for item in (request.special_question_types or []) if str(item).strip()]
        if current_focus.lower() in {"select", "auto"} or current_focus in {"不指定", "不指定（自动匹配）", "请选择"}:
            current_focus = ""
        if current_business_subtype.lower() in {"select", "auto"} or current_business_subtype in {
            "不指定",
            "不指定（自动匹配）",
            "请选择",
        }:
            current_business_subtype = ""

        warning_parts: list[str] = []
        if request.source_question is not None and (not current_focus or not current_business_subtype) and not selected_special_types:
            inferred_target = self.source_question_analyzer.infer_request_target(request.source_question) or {}
            inferred_focus = self._focus_value_for_target(
                question_type=str(inferred_target.get("question_type") or "").strip(),
                business_subtype=inferred_target.get("business_subtype"),
            )
            inferred_business_subtype = self._ui_business_subtype_for_target(
                question_type=str(inferred_target.get("question_type") or "").strip(),
                business_subtype=inferred_target.get("business_subtype"),
            )
            if not current_focus and inferred_focus:
                current_focus = inferred_focus
                warning_parts.append(f"question_focus={inferred_focus}")
            if not current_business_subtype and inferred_business_subtype:
                current_business_subtype = inferred_business_subtype
                warning_parts.append(f"business_subtype={inferred_business_subtype}")

        decode_request = request.to_dify_form_input().model_copy(
            update={
                "question_focus": current_focus,
                "business_subtype": current_business_subtype or None,
                "special_question_types": selected_special_types,
            }
        )
        warning = None
        if warning_parts:
            warning = "reference_question_inferred_target_applied: " + ", ".join(warning_parts)
        return decode_request, warning

    @staticmethod
    def _focus_value_for_target(*, question_type: str, business_subtype: str | None) -> str | None:
        if question_type == "sentence_order":
            return "sentence_order"
        if question_type == "sentence_fill":
            return "sentence_fill"
        if question_type == "main_idea" and business_subtype in {"center_understanding", "title_selection"}:
            return "center_understanding"
        return None

    @staticmethod
    def _ui_business_subtype_for_target(*, question_type: str, business_subtype: str | None) -> str | None:
        if question_type == "sentence_order":
            return "sentence_order_selection"
        if question_type == "sentence_fill":
            return "sentence_fill_selection"
        if question_type == "main_idea" and business_subtype in {"center_understanding", "title_selection"}:
            return business_subtype
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
        if not isinstance(merged, dict):
            return {}
        merged.pop("text_direction", None)
        merged.pop("\u6587\u672c\u65b9\u5411", None)
        return merged

    @staticmethod
    def _extract_requested_pattern_id(
        *,
        type_slots: dict[str, Any] | None,
        extra_constraints: dict[str, Any] | None,
    ) -> str | None:
        if isinstance(type_slots, dict):
            raw_value = str(type_slots.get("pattern_id") or "").strip()
            if raw_value:
                return raw_value
        if isinstance(extra_constraints, dict):
            raw_value = str(extra_constraints.pop("pattern_id", "") or "").strip()
            if raw_value:
                return raw_value
        return None

    def _enabled_pattern_ids_for_question_type(self, question_type: str | None) -> set[str] | None:
        normalized_question_type = str(question_type or "").strip()
        if not normalized_question_type:
            return None
        registry = getattr(getattr(self, "orchestrator", None), "registry", None)
        if registry is None or not hasattr(registry, "list_enabled_patterns"):
            return None
        try:
            return {
                str(pattern_id).strip()
                for pattern_id in (registry.list_enabled_patterns(normalized_question_type) or [])
                if str(pattern_id).strip()
            }
        except Exception:  # noqa: BLE001
            logger.exception("pattern_registry_lookup_failed", extra={"question_type": normalized_question_type})
            return None

    def _normalize_requested_pattern_id(
        self,
        *,
        question_type: str | None,
        pattern_id: str | None,
        warnings: list[str] | None = None,
    ) -> str | None:
        normalized_pattern_id = str(pattern_id or "").strip()
        if not normalized_pattern_id:
            return None
        enabled_pattern_ids = self._enabled_pattern_ids_for_question_type(question_type)
        if enabled_pattern_ids is None or normalized_pattern_id in enabled_pattern_ids:
            return normalized_pattern_id
        warning = (
            f"Ignored stale pattern_id '{normalized_pattern_id}' for question_type "
            f"'{str(question_type or '').strip() or 'unknown'}'."
        )
        logger.warning(
            "stale_pattern_id_ignored",
            extra={
                "question_type": str(question_type or "").strip(),
                "pattern_id": normalized_pattern_id,
                "enabled_patterns": sorted(enabled_pattern_ids),
            },
        )
        if warnings is not None:
            warnings.append(warning)
        return None

    def _build_prompt_request_from_snapshot(self, request_snapshot: dict) -> PromptBuildRequest:
        normalized_pattern_id = self._normalize_requested_pattern_id(
            question_type=request_snapshot.get("question_type"),
            pattern_id=request_snapshot.get("pattern_id"),
        )
        return PromptBuildRequest(
            question_type=request_snapshot["question_type"],
            business_subtype=request_snapshot.get("business_subtype"),
            pattern_id=normalized_pattern_id,
            difficulty_target=request_snapshot["difficulty_target"],
            topic=request_snapshot.get("topic"),
            count=1,
            passage_style=request_snapshot.get("passage_style"),
            use_fewshot=request_snapshot.get("use_fewshot", True),
            fewshot_mode=request_snapshot.get("fewshot_mode", "structure_only"),
            type_slots=deepcopy(request_snapshot.get("type_slots") or {}),
            extra_constraints=deepcopy(request_snapshot.get("extra_constraints") or {}),
        )

    @staticmethod
    def _canonicalize_sentence_order_candidate_type(value: Any) -> str | None:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None
        return _SENTENCE_ORDER_CANDIDATE_TYPE_ALIASES.get(raw_value)

    def _resolve_sentence_order_candidate_type(
        self,
        *,
        question_type: str | None,
        question_card_binding: dict[str, Any] | None,
        type_slots: dict[str, Any] | None = None,
        resolved_slots: dict[str, Any] | None = None,
    ) -> str | None:
        if str(question_type or "").strip() != "sentence_order":
            return None

        candidates: list[Any] = []
        if isinstance(type_slots, dict):
            candidates.append(type_slots.get("candidate_type"))
        if isinstance(resolved_slots, dict):
            candidates.append(resolved_slots.get("candidate_type"))

        if isinstance(question_card_binding, dict):
            question_card = question_card_binding.get("question_card") or {}
            if isinstance(question_card, dict):
                formal_runtime_spec = question_card.get("formal_runtime_spec") or {}
                if isinstance(formal_runtime_spec, dict):
                    candidates.append(formal_runtime_spec.get("candidate_type"))
            runtime_binding = question_card_binding.get("runtime_binding") or {}
            if isinstance(runtime_binding, dict):
                candidates.append(runtime_binding.get("candidate_type"))

        candidates.append(_SENTENCE_ORDER_CANONICAL_CANDIDATE_TYPE)
        for candidate in candidates:
            canonical = self._canonicalize_sentence_order_candidate_type(candidate)
            if canonical:
                return canonical
        return None

    def _hydrate_sentence_order_candidate_type_context(self, built_item: dict) -> None:
        if str(built_item.get("question_type") or "").strip() != "sentence_order":
            return

        request_snapshot = deepcopy(built_item.get("request_snapshot") or {})
        resolved_slots = deepcopy(built_item.get("resolved_slots") or {})
        candidate_type = self._resolve_sentence_order_candidate_type(
            question_type=built_item.get("question_type"),
            question_card_binding=request_snapshot.get("question_card_binding"),
            type_slots=request_snapshot.get("type_slots") or {},
            resolved_slots=resolved_slots,
        )
        if not candidate_type:
            return

        changed = False
        if resolved_slots.get("candidate_type") != candidate_type:
            resolved_slots["candidate_type"] = candidate_type
            built_item["resolved_slots"] = resolved_slots
            changed = True

        snapshot_type_slots = deepcopy(request_snapshot.get("type_slots") or {})
        if snapshot_type_slots.get("candidate_type") != candidate_type:
            snapshot_type_slots["candidate_type"] = candidate_type
            request_snapshot["type_slots"] = snapshot_type_slots
            changed = True

        if changed:
            built_item["request_snapshot"] = request_snapshot
            built_item["notes"] = built_item.get("notes", []) + ["sentence_order_candidate_type_hydrated"]

    def _generate_question(
        self,
        built_item: dict,
        material: MaterialSelectionResult,
        route,
        prompt_template: PromptTemplateRecord,
        feedback_notes: list[str] | None = None,
    ) -> tuple[GeneratedQuestion, dict]:
        prompt_package = built_item["prompt_package"]
        system_prompt = normalize_prompt_text(
            "\n\n".join(
                [
                    prompt_template.content,
                    prompt_package["system_prompt"],
                ]
            )
        )
        final_user_prompt = normalize_prompt_text(
            "\n\n".join(
                self._build_generation_prompt_sections(
                    built_item=built_item,
                    material=material,
                    prompt_package=prompt_package,
                    feedback_notes=feedback_notes or [],
                )
            )
        )
        try:
            response = self.llm_gateway.generate_json(
                route=route,
                system_prompt=system_prompt,
                user_prompt=final_user_prompt,
                schema_name="generated_question",
                schema=GeneratedQuestionDraft.model_json_schema(),
            )
        except DomainError as exc:
            if built_item["question_type"] == "sentence_order" and self._can_fallback_sentence_order_from_gateway_error(exc):
                fallback_question = self._build_sentence_order_fallback_question(
                    built_item=built_item,
                    material=material,
                    reason=f"gateway::{exc.message}",
                )
                return fallback_question, {"gateway_fallback": exc.details or {}}
            raise
        try:
            generated = self.generated_question_adapter.validate_python(response)
        except Exception as exc:  # noqa: BLE001
            repaired_response = self._repair_generated_question_response(response)
            if repaired_response is not None:
                try:
                    generated = self.generated_question_adapter.validate_python(repaired_response)
                except Exception:  # noqa: BLE001
                    generated = None
                else:
                    response = repaired_response
            else:
                generated = None
            if generated is not None:
                pass
            elif built_item["question_type"] == "sentence_order":
                fallback_question = self._build_sentence_order_fallback_question(
                    built_item=built_item,
                    material=material,
                    reason=str(exc),
                )
                return fallback_question, response
            else:
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
        if built_item["question_type"] == "sentence_fill":
            generated_question = self._enforce_sentence_fill_original_answer(
                generated_question=generated_question,
                material=material,
            )
        if built_item["question_type"] == "sentence_order":
            generated_question = self.build_sentence_order_question(generated_question, material_text=material.text)
            generated_question = self._enforce_sentence_order_six_unit_output(generated_question)
        return self._remap_answer_position(generated_question), response

    @staticmethod
    def _can_fallback_sentence_order_from_gateway_error(exc: DomainError) -> bool:
        details = exc.details if isinstance(exc.details, dict) else {}
        if "text_preview" in details or "fallback_retry_text_preview" in details:
            return True
        if details.get("provider") and details.get("status_code") in {401, 403, 429, 500, 502, 503, 504}:
            return True
        message = str(exc.message or "")
        return "structured text" in message.lower() or "json" in message.lower()

    def _build_sentence_order_fallback_question(
        self,
        *,
        built_item: dict[str, Any],
        material: MaterialSelectionResult,
        reason: str,
    ) -> GeneratedQuestion:
        fallback_question = GeneratedQuestion(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            pattern_id=built_item.get("pattern_id"),
            stem="将以下句子重新排列，语序正确的一项是：",
            original_sentences=[],
            correct_order=[],
            options={"A": "123456", "B": "123546", "C": "124356", "D": "213456"},
            answer="A",
            analysis="",
            metadata={
                "material_id": material.material_id,
                "article_id": material.article_id,
                "batch_prompt_pattern": built_item["selected_pattern"],
                "sentence_order_fallback_rebuilt": True,
                "sentence_order_fallback_reason": reason,
            },
        )
        fallback_question = self.build_sentence_order_question(
            fallback_question,
            material_text=material.text,
        )
        fallback_question = self._enforce_sentence_order_six_unit_output(fallback_question)
        return self._remap_answer_position(fallback_question)

    def _repair_generated_question_response(self, response: Any) -> dict[str, Any] | None:
        if not isinstance(response, dict):
            return None
        repaired = dict(response)
        question_payload = repaired.get("question")
        if not isinstance(question_payload, dict):
            nested_generated_question = repaired.get("generated_question")
            if isinstance(nested_generated_question, dict):
                question_payload = nested_generated_question
        if isinstance(question_payload, dict):
            question_stem = question_payload.get("stem") or question_payload.get("question_stem")
            question_original_sentences = question_payload.get("original_sentences")
            question_correct_order = question_payload.get("correct_order")
            question_options = question_payload.get("options")
            question_answer = question_payload.get("answer")
            question_analysis = question_payload.get("analysis")
            if question_stem and not isinstance(repaired.get("stem"), str):
                repaired["stem"] = question_stem
            elif question_stem and not str(repaired.get("stem") or "").strip():
                repaired["stem"] = question_stem
            original_sentences_missing = not isinstance(repaired.get("original_sentences"), list) or not repaired.get("original_sentences")
            if isinstance(question_original_sentences, list) and original_sentences_missing:
                repaired["original_sentences"] = question_original_sentences
            correct_order_missing = not isinstance(repaired.get("correct_order"), list) or not repaired.get("correct_order")
            if isinstance(question_correct_order, list) and correct_order_missing:
                repaired["correct_order"] = question_correct_order
            options_payload = repaired.get("options")
            options_missing = (
                (not isinstance(options_payload, dict) or not any(self._coerce_option_text(value) for value in options_payload.values()))
                and (not isinstance(options_payload, list) or not any(self._coerce_option_text(value) for value in options_payload))
            )
            if isinstance(question_options, (dict, list)) and options_missing:
                repaired["options"] = question_options
            if question_answer and str(repaired.get("answer") or "").strip().upper() not in {"A", "B", "C", "D"}:
                repaired["answer"] = question_answer
            if question_analysis and not str(repaired.get("analysis") or "").strip():
                repaired["analysis"] = question_analysis

        if not str(repaired.get("stem") or "").strip():
            for fallback_key in ("question", "question_stem", "prompt"):
                fallback_value = str(repaired.get(fallback_key) or "").strip()
                if fallback_value:
                    repaired["stem"] = fallback_value
                    break

        options_payload = repaired.get("options")
        normalized_options: dict[str, str] = {}
        if isinstance(options_payload, dict):
            for key, value in options_payload.items():
                letter = str(key or "").strip().upper()
                if letter in {"A", "B", "C", "D"}:
                    normalized_options[letter] = self._coerce_option_text(value)
            if len(normalized_options) < 4:
                # Recover non-standard keys such as option_a / a_option.
                for key, value in options_payload.items():
                    key_text = str(key or "").strip().lower()
                    for letter in ("a", "b", "c", "d"):
                        if letter in key_text and letter.upper() not in normalized_options:
                            normalized_options[letter.upper()] = self._coerce_option_text(value)
                            break
        elif isinstance(options_payload, list):
            for letter, value in zip(("A", "B", "C", "D"), options_payload[:4], strict=False):
                normalized_options[letter] = self._coerce_option_text(value)

        for letter in ("A", "B", "C", "D"):
            normalized_options.setdefault(letter, "")

        for letter, text in list(normalized_options.items()):
            normalized_options[letter] = re.sub(rf"^\s*{letter}[\.\u3001\uff0e:：]\s*", "", text).strip()

        repaired["options"] = normalized_options
        repaired["stem"] = str(repaired.get("stem") or "").strip()
        repaired["analysis"] = str(repaired.get("analysis") or "").strip()

        answer = str(repaired.get("answer") or "").strip().upper()
        if answer not in {"A", "B", "C", "D"}:
            answer = "A"
        repaired["answer"] = answer

        original_sentences = repaired.get("original_sentences")
        if not isinstance(original_sentences, list):
            repaired["original_sentences"] = []
        else:
            repaired["original_sentences"] = [str(item or "").strip() for item in original_sentences if str(item or "").strip()]

        correct_order = repaired.get("correct_order")
        if isinstance(correct_order, list):
            normalized_order: list[int] = []
            for item in correct_order:
                try:
                    normalized_order.append(int(item))
                except (TypeError, ValueError):
                    continue
            repaired["correct_order"] = normalized_order
        else:
            repaired["correct_order"] = []

        return repaired

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
        system_prompt = normalize_prompt_text(
            "You are a strict targeted-repair assistant for exam-question generation. "
            "Do not regenerate the whole question from scratch. "
            "Only edit the explicitly allowed fields, preserve locked fields exactly, "
            "do not cross the material boundary, and do not invent facts beyond the source material. "
            "If the requested repair cannot be completed safely, keep the object as close to the current version as possible."
        )
        current_payload = current_question.model_dump()
        user_prompt = normalize_prompt_text(
            "\n\n".join(
                [
                    f"question_type={built_item['question_type']}",
                    f"business_subtype={built_item.get('business_subtype') or ''}",
                    f"repair_mode={repair_plan.get('mode') or 'targeted_repair'}",
                    f"allowed_fields={', '.join(repair_plan.get('allowed_fields') or [])}",
                    f"locked_fields={', '.join(repair_plan.get('locked_fields') or [])}",
                    f"target_errors={', '.join(repair_plan.get('target_errors') or [])}",
                    f"target_checks={', '.join(repair_plan.get('target_checks') or [])}",
                    "Current generated question JSON:",
                    str(normalize_readable_structure(current_payload)),
                    "Original source material:",
                    normalize_readable_text(material.text),
                    "Repair requirements:",
                    "\n".join(feedback_notes or []),
                    "Return a full updated GeneratedQuestion object. "
                    "Only change the allowed fields. Keep all locked fields semantically unchanged.",
                ]
            )
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
        if built_item["question_type"] == "sentence_fill":
            merged_question = self._enforce_sentence_fill_original_answer(
                generated_question=merged_question,
                material=material,
            )
        if built_item["question_type"] == "sentence_order":
            merged_question = self._enforce_sentence_order_six_unit_output(merged_question)
        return self._remap_answer_position(merged_question), response

    def apply_analysis_only_repair(
        self,
        *,
        built_item: dict,
        material: MaterialSelectionResult,
        current_question: GeneratedQuestion,
        route,
        repair_plan: dict[str, Any],
        feedback_notes: list[str],
    ) -> dict[str, Any]:
        system_prompt = normalize_prompt_text(
            "You are a strict analysis-only repair assistant for exam-question generation. "
            "Only rewrite the analysis. "
            "Do not change stem, options, answer, structure truth, material boundary, or control intent. "
            "The updated analysis must remain grounded in the current correct option text and the source material."
        )
        user_prompt = normalize_prompt_text(
            "\n\n".join(
                [
                    f"question_type={built_item['question_type']}",
                    f"business_subtype={built_item.get('business_subtype') or ''}",
                    f"repair_mode={repair_plan.get('mode') or 'analysis_only_repair'}",
                    "Current generated question JSON:",
                    str(normalize_readable_structure(current_question.model_dump())),
                    "Original source material:",
                    normalize_readable_text(material.text),
                    "Repair requirements:",
                    "\n".join(feedback_notes or []),
                    "Return JSON with key analysis only.",
                ]
            )
        )
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="analysis_only_patch",
            schema=AnalysisOnlyPatchDraft.model_json_schema(),
        )
        patch_draft = AnalysisOnlyPatchDraft.model_validate(response)
        next_analysis_text = str(patch_draft.analysis or "").strip()
        current_analysis_text = str(current_question.analysis or "").strip()
        if not next_analysis_text:
            raise DomainError(
                "analysis_only_repair produced an empty analysis.",
                status_code=422,
                details={"repair_mode": repair_plan.get("mode")},
            )
        if next_analysis_text == current_analysis_text:
            raise DomainError(
                "analysis_only_repair did not change analysis.",
                status_code=422,
                details={"repair_mode": repair_plan.get("mode")},
            )

        repaired_question = current_question.model_copy(update={"analysis": next_analysis_text})
        validation_result = self.validator.validate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=repaired_question,
            material_text=material.text,
            original_material_text=material.original_text,
            material_source=material.source,
            validator_contract=material.validator_contract,
            difficulty_fit=built_item.get("difficulty_fit"),
            source_question=(built_item.get("request_snapshot") or {}).get("source_question"),
            source_question_analysis=(built_item.get("request_snapshot") or {}).get("source_question_analysis"),
            resolved_slots=built_item.get("resolved_slots"),
            control_logic=built_item.get("control_logic"),
        )
        self._apply_consistency_hard_fail_gate(validation_result=validation_result)
        evaluation_result = self.evaluator.evaluate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=repaired_question,
            validation_result=validation_result,
            material_text=material.text,
            difficulty_fit=built_item.get("difficulty_fit"),
        )
        quality_gate_errors = self._apply_evaluation_gate(
            validation_result=validation_result,
            evaluation_result=evaluation_result,
            difficulty_target=str(built_item.get("difficulty_target") or ""),
        )
        self._enforce_analysis_only_scope(
            before_item=built_item,
            before_question=current_question,
            after_question=repaired_question,
            validation_result=validation_result,
            evaluation_result=evaluation_result,
        )
        return {
            "generated_question": repaired_question,
            "raw_model_output": response,
            "validation_result": validation_result,
            "evaluation_result": evaluation_result,
            "quality_gate_errors": quality_gate_errors,
        }

    def apply_answer_binding_patch(
        self,
        *,
        built_item: dict,
        material: MaterialSelectionResult,
        current_question: GeneratedQuestion,
        route,
        repair_plan: dict[str, Any],
        feedback_notes: list[str],
    ) -> dict[str, Any]:
        system_prompt = normalize_prompt_text(
            "You are a strict answer-binding repair assistant for exam-question generation. "
            "Only adjust options, answer, and analysis. "
            "Do not change stem, material, structure truth, or control intent. "
            "The corrected answer must be supported by the source material."
        )
        user_prompt = normalize_prompt_text(
            "\n\n".join(
                [
                    f"question_type={built_item['question_type']}",
                    f"business_subtype={built_item.get('business_subtype') or ''}",
                    f"repair_mode={repair_plan.get('mode') or 'answer_binding_patch'}",
                    "Current generated question JSON:",
                    str(normalize_readable_structure(current_question.model_dump())),
                    "Original source material:",
                    normalize_readable_text(material.text),
                    "Repair requirements:",
                    "\n".join(feedback_notes or []),
                    "Return JSON with keys options, answer, and analysis only.",
                ]
            )
        )
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="answer_binding_patch",
            schema=AnswerBindingPatchDraft.model_json_schema(),
        )
        patch_draft = AnswerBindingPatchDraft.model_validate(response)
        next_options = {str(key).strip().upper(): str(value or "").strip() for key, value in (patch_draft.options or {}).items()}
        current_options = dict(current_question.options or {})
        if set(next_options.keys()) != set(current_options.keys()):
            raise DomainError(
                "answer_binding_patch must return the full options mapping with the same option keys.",
                status_code=422,
                details={"option_keys": sorted(next_options.keys())},
            )
        if any(not text for text in next_options.values()):
            raise DomainError(
                "answer_binding_patch cannot produce empty options.",
                status_code=422,
                details={"option_keys": sorted(next_options.keys())},
            )

        next_answer = str(patch_draft.answer or "").strip().upper()
        if next_answer not in next_options:
            raise DomainError(
                "answer_binding_patch requires answer to be one of the options.",
                status_code=422,
                details={"answer": next_answer, "option_keys": sorted(next_options.keys())},
            )
        next_analysis = str(patch_draft.analysis or "").strip()
        if not next_analysis:
            raise DomainError(
                "answer_binding_patch produced an empty analysis.",
                status_code=422,
                details={"repair_mode": repair_plan.get("mode")},
            )

        if (
            next_options == current_options
            and next_answer == str(current_question.answer or "").strip().upper()
            and next_analysis == str(current_question.analysis or "").strip()
        ):
            raise DomainError(
                "answer_binding_patch did not produce a scoped change.",
                status_code=422,
                details={"repair_mode": repair_plan.get("mode")},
            )

        repaired_question = current_question.model_copy(
            update={
                "options": next_options,
                "answer": next_answer,
                "analysis": next_analysis,
                "metadata": {
                    **(current_question.metadata or {}),
                    "repair_mode": repair_plan.get("mode") or "answer_binding_patch",
                },
            }
        )
        validation_result = self.validator.validate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=repaired_question,
            material_text=material.text,
            original_material_text=material.original_text,
            material_source=material.source,
            validator_contract=material.validator_contract,
            difficulty_fit=built_item.get("difficulty_fit"),
            source_question=(built_item.get("request_snapshot") or {}).get("source_question"),
            source_question_analysis=(built_item.get("request_snapshot") or {}).get("source_question_analysis"),
            resolved_slots=built_item.get("resolved_slots"),
            control_logic=built_item.get("control_logic"),
        )
        self._apply_consistency_hard_fail_gate(validation_result=validation_result)
        evaluation_result = self.evaluator.evaluate(
            question_type=built_item["question_type"],
            business_subtype=built_item.get("business_subtype"),
            generated_question=repaired_question,
            validation_result=validation_result,
            material_text=material.text,
            difficulty_fit=built_item.get("difficulty_fit"),
        )
        quality_gate_errors = self._apply_evaluation_gate(
            validation_result=validation_result,
            evaluation_result=evaluation_result,
            difficulty_target=str(built_item.get("difficulty_target") or ""),
        )
        self._enforce_answer_binding_scope(
            before_item=built_item,
            before_question=current_question,
            after_question=repaired_question,
            validation_result=validation_result,
            evaluation_result=evaluation_result,
        )
        return {
            "generated_question": repaired_question,
            "raw_model_output": response,
            "validation_result": validation_result,
            "evaluation_result": evaluation_result,
            "quality_gate_errors": quality_gate_errors,
        }

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

    @staticmethod
    def _classify_analysis_answer_consistency_scope(
        *,
        checks: dict[str, Any] | None,
        evaluation_result: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        checks = checks or {}
        analysis_answer = checks.get("analysis_answer_consistency") or {}
        analysis_answer_failed = analysis_answer.get("passed") is False
        analysis_mentions = (checks.get("analysis_mentions_correct_option_text") or {}).get("passed")
        answer_in_options = (checks.get("answer_in_options") or {}).get("passed")
        reference_grounding = checks.get("reference_answer_grounding") or {}
        reference_grounding_failed = reference_grounding.get("passed") is False
        evaluation_consistency = None
        if evaluation_result:
            evaluation_consistency = evaluation_result.get("answer_analysis_consistency")

        signals = {
            "analysis_answer_consistency": analysis_answer.get("passed"),
            "analysis_mentions_correct_option_text": analysis_mentions,
            "answer_in_options": answer_in_options,
            "reference_answer_grounding": reference_grounding.get("passed"),
            "evaluation_answer_analysis_consistency": evaluation_consistency,
        }

        if not analysis_answer_failed:
            return "ambiguous", signals
        if analysis_mentions is True and answer_in_options is True:
            return "analysis_only_candidate", signals
        if analysis_mentions is False and reference_grounding_failed:
            return "answer_binding_candidate", signals
        return "ambiguous", signals

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
            if "analysis must not be empty." in issue_keys:
                return {
                    "mode": "analysis_only_repair",
                    "allowed_fields": ["analysis"],
                    "locked_fields": ["stem", "original_sentences", "correct_order", "options", "answer"],
                    "target_errors": ["analysis must not be empty."],
                    "target_checks": ["analysis_mentions_correct_option_text"],
                    "notes": [
                        "Only improve the analysis.",
                        "Do not change stem, options, answer, or material-facing semantics.",
                        "The analysis must explicitly explain why the declared correct option text best matches the passage.",
                    ],
                }
            axis_mismatch_errors = {
                "main_axis_mismatch",
                "abstraction_level_mismatch",
                "argument_structure_mismatch",
                "local_point_as_main_axis",
                "example_promoted_to_main_idea",
            }
            if axis_mismatch_errors & issue_keys:
                return {
                    "mode": "main_idea_axis_repair",
                    "allowed_fields": ["options", "answer", "analysis"],
                    "locked_fields": ["stem", "original_sentences", "correct_order"],
                    "target_errors": sorted(axis_mismatch_errors),
                    "target_checks": ["analysis_mentions_correct_option_text"],
                    "notes": [
                        "Do not change the material scope or invent new facts.",
                        "Repair the correct option so it matches the passage main axis and target abstraction level.",
                        "Keep distractors on-topic but clearly below the correct option in coverage.",
                        "Rewrite the analysis so it explicitly explains why the correct option text fits best.",
                    ],
                }
            if "analysis_answer_consistency" in issue_keys:
                decision, _signals = self._classify_analysis_answer_consistency_scope(
                    checks=getattr(validation_result, "checks", None) or {},
                    evaluation_result=None,
                )
                if decision == "analysis_only_candidate":
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
                if decision == "answer_binding_candidate":
                    return {
                        "mode": "main_idea_axis_repair",
                        "allowed_fields": ["options", "answer", "analysis"],
                        "locked_fields": ["stem", "original_sentences", "correct_order"],
                        "target_errors": [],
                        "target_checks": ["analysis_mentions_correct_option_text"],
                        "notes": [
                            "Do not change the material scope or invent new facts.",
                            "Repair the correct option so it matches the passage main axis and target abstraction level.",
                            "Keep distractors on-topic but clearly below the correct option in coverage.",
                            "Rewrite the analysis so it explicitly explains why the correct option text fits best.",
                        ],
                    }
                return None
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
            resolved_slots=item.get("resolved_slots"),
            control_logic=item.get("control_logic"),
        )
        self._apply_consistency_hard_fail_gate(validation_result=validation_result)
        version_no = int(item.get("current_version_no", 1)) + 1
        item["current_version_no"] = version_no
        item["current_status"] = "generated" if validation_result.passed else "auto_failed"
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
        self._apply_manual_override_review_surface(item)
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
        material = self._prepare_question_service_material(
            material=material,
            question_type=request_snapshot["question_type"],
            request_snapshot=request_snapshot,
        )
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
        source_question_analysis = request_snapshot.get("source_question_analysis") or {}
        bridge_hints = self._merge_material_bridge_hints(
            self._material_bridge_hints(source_question_analysis),
            self._requested_taxonomy_bridge_hints(
                question_type=request_snapshot["question_type"],
                type_slots=request_snapshot.get("type_slots"),
                extra_constraints=request_snapshot.get("extra_constraints"),
            ),
            self._requested_pattern_bridge_hints(
                question_type=request_snapshot["question_type"],
                pattern_id=request_snapshot.get("pattern_id"),
            ),
        )
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
                business_card_ids=bridge_hints["business_card_ids"],
                preferred_business_card_ids=bridge_hints["preferred_business_card_ids"],
                query_terms=bridge_hints["query_terms"],
                target_length=source_question_analysis.get("target_length"),
                length_tolerance=(source_question_analysis.get("length_tolerance") or 120),
                structure_constraints=bridge_hints["structure_constraints"],
                enable_anchor_adaptation=bool(source_question_analysis),
                exclude_material_ids={previous_material_id} if previous_material_id else None,
                limit=24,
                difficulty_target=request_snapshot.get("difficulty_target", "medium"),
                preference_profile=request_snapshot.get("preference_profile"),
                usage_stats_lookup=self.repository.get_material_usage_stats,
            )
            materials = [candidate for candidate in replacement_candidates if candidate.material_id == requested_material_id]
            warnings = []
        else:
            materials, warnings = self.material_bridge.select_materials(
                question_type=request_snapshot["question_type"],
                business_subtype=request_snapshot.get("business_subtype"),
                question_card_id=request_snapshot.get("question_card_id"),
                difficulty_target=request_snapshot["difficulty_target"],
                topic=request_snapshot.get("topic"),
                text_direction=None,
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
        prepared_material = self._prepare_question_service_material(
            material=self._refine_material_if_needed(
                self._annotate_material_usage(materials[0]),
                request_snapshot=request_snapshot,
            ),
            question_type=request_snapshot["question_type"],
            request_snapshot=request_snapshot,
        )
        rebuilt = self._build_generated_item(
            build_request=self._build_prompt_request_from_snapshot(request_snapshot),
            material=prepared_material,
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
        revised_item["manual_override_active"] = True
        revised_item["preference_profile"] = self._preference_profile_from_snapshot(revised_item.get("request_snapshot") or {})
        revised_item["feedback_snapshot"] = {}
        revised_item["validation_result"] = None
        revised_item["evaluation_result"] = None

        version_no = int(item.get("current_version_no", 1)) + 1
        revised_item["current_version_no"] = version_no
        revised_item["revision_count"] = int(item.get("revision_count", 0)) + 1
        revised_item["current_status"] = "generated"
        revised_item["statuses"]["generation_status"] = "success"
        revised_item["statuses"]["validation_status"] = "not_started"
        revised_item["statuses"]["review_status"] = "waiting_review"
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
            validation_result=None,
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
            validation_result=None,
            evaluation_result=None,
            runtime_snapshot=runtime_snapshot,
        )
        return revised_item

    @staticmethod
    def _apply_manual_override_review_surface(item: dict[str, Any]) -> None:
        if not bool(item.get("manual_override_active")):
            return
        statuses = item.setdefault("statuses", {})
        item["current_status"] = "generated"
        statuses["generation_status"] = "success"
        statuses["validation_status"] = "not_started"
        statuses["review_status"] = "waiting_review"

    def _enforce_analysis_only_scope(
        self,
        *,
        before_item: dict[str, Any],
        before_question: GeneratedQuestion,
        after_question: GeneratedQuestion,
        validation_result,
        evaluation_result: dict | None,
    ) -> None:
        changed_fields: list[str] = []
        if before_question.stem != after_question.stem:
            changed_fields.append("stem")
        if list(before_question.original_sentences or []) != list(after_question.original_sentences or []):
            changed_fields.append("original_sentences")
        if list(before_question.correct_order or []) != list(after_question.correct_order or []):
            changed_fields.append("correct_order")
        if dict(before_question.options or {}) != dict(after_question.options or {}):
            changed_fields.append("options")
        if str(before_question.answer or "").strip().upper() != str(after_question.answer or "").strip().upper():
            changed_fields.append("answer")
        if str(before_question.analysis or "").strip() != str(after_question.analysis or "").strip():
            changed_fields.append("analysis")
        if before_question.question_type != after_question.question_type:
            changed_fields.append("question_type")
        if before_question.business_subtype != after_question.business_subtype:
            changed_fields.append("business_subtype")
        if before_question.pattern_id != after_question.pattern_id:
            changed_fields.append("pattern_id")

        if set(changed_fields) - {"analysis"}:
            raise DomainError(
                "analysis_only_repair changed fields outside analysis scope.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )
        if changed_fields != ["analysis"]:
            raise DomainError(
                "analysis_only_repair must only change analysis.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )

        before_answer = str(before_question.answer or "").strip().upper()
        before_options = dict(before_question.options or {})
        if before_answer != str(after_question.answer or "").strip().upper():
            raise DomainError(
                "analysis_only_repair cannot change answer.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )
        if str(before_options.get(before_answer) or "").strip() != str((after_question.options or {}).get(before_answer) or "").strip():
            raise DomainError(
                "analysis_only_repair cannot change the correct option text.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )
        if dict(before_options) != dict(after_question.options or {}):
            raise DomainError(
                "analysis_only_repair cannot change options.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )

        material_selection = before_item.get("material_selection") or {}
        if not material_selection:
            raise DomainError(
                "analysis_only_repair requires an existing material selection.",
                status_code=422,
                details={"has_material_selection": False},
            )
        if validation_result is None or evaluation_result is None:
            raise DomainError(
                "analysis_only_repair must rerun validator and evaluator.",
                status_code=422,
                details={
                    "has_validation_result": validation_result is not None,
                    "has_evaluation_result": evaluation_result is not None,
                },
            )

        before_checks = ((before_item.get("validation_result") or {}).get("checks") or {})
        after_checks = (getattr(validation_result, "checks", None) or {})
        self._ensure_validation_check_not_regressed(
            before_checks=before_checks,
            after_checks=after_checks,
            check_name="analysis_answer_consistency",
            error_message="analysis_only_repair cannot turn analysis_answer_consistency into a failed check.",
        )
        self._ensure_validation_check_not_regressed(
            before_checks=before_checks,
            after_checks=after_checks,
            check_name="analysis_mentions_correct_option_text",
            error_message="analysis_only_repair cannot degrade analysis_mentions_correct_option_text into a failed check.",
        )

    @staticmethod
    def _ensure_validation_check_not_regressed(
        *,
        before_checks: dict[str, Any],
        after_checks: dict[str, Any],
        check_name: str,
        error_message: str,
    ) -> None:
        before_passed = QuestionGenerationService._extract_validation_check_passed(before_checks, check_name)
        after_passed = QuestionGenerationService._extract_validation_check_passed(after_checks, check_name)
        if before_passed is not False and after_passed is False:
            raise DomainError(
                error_message,
                status_code=422,
                details={"check_name": check_name, "before_passed": before_passed, "after_passed": after_passed},
            )

    @staticmethod
    def _extract_validation_check_passed(checks: dict[str, Any], check_name: str) -> bool | None:
        payload = checks.get(check_name)
        if not isinstance(payload, dict):
            return None
        passed = payload.get("passed")
        if passed is None:
            return None
        return bool(passed)

    def _enforce_answer_binding_scope(
        self,
        *,
        before_item: dict[str, Any],
        before_question: GeneratedQuestion,
        after_question: GeneratedQuestion,
        validation_result,
        evaluation_result: dict | None,
    ) -> None:
        changed_fields: list[str] = []
        if before_question.stem != after_question.stem:
            changed_fields.append("stem")
        if list(before_question.original_sentences or []) != list(after_question.original_sentences or []):
            changed_fields.append("original_sentences")
        if list(before_question.correct_order or []) != list(after_question.correct_order or []):
            changed_fields.append("correct_order")
        if dict(before_question.options or {}) != dict(after_question.options or {}):
            changed_fields.append("options")
        if str(before_question.answer or "").strip().upper() != str(after_question.answer or "").strip().upper():
            changed_fields.append("answer")
        if str(before_question.analysis or "").strip() != str(after_question.analysis or "").strip():
            changed_fields.append("analysis")
        if before_question.question_type != after_question.question_type:
            changed_fields.append("question_type")
        if before_question.business_subtype != after_question.business_subtype:
            changed_fields.append("business_subtype")
        if before_question.pattern_id != after_question.pattern_id:
            changed_fields.append("pattern_id")

        if set(changed_fields) - {"options", "answer", "analysis"}:
            raise DomainError(
                "answer_binding_patch changed fields outside options/answer/analysis scope.",
                status_code=422,
                details={"changed_fields": changed_fields},
            )

        material_selection = before_item.get("material_selection") or {}
        if not material_selection:
            raise DomainError(
                "answer_binding_patch requires an existing material selection.",
                status_code=422,
                details={"has_material_selection": False},
            )
        if validation_result is None or evaluation_result is None:
            raise DomainError(
                "answer_binding_patch must rerun validator and evaluator.",
                status_code=422,
                details={
                    "has_validation_result": validation_result is not None,
                    "has_evaluation_result": evaluation_result is not None,
                },
            )

        before_checks = ((before_item.get("validation_result") or {}).get("checks") or {})
        after_checks = (getattr(validation_result, "checks", None) or {})
        self._ensure_validation_check_not_regressed(
            before_checks=before_checks,
            after_checks=after_checks,
            check_name="analysis_answer_consistency",
            error_message="answer_binding_patch cannot turn analysis_answer_consistency into a failed check.",
        )
        self._ensure_validation_check_not_regressed(
            before_checks=before_checks,
            after_checks=after_checks,
            check_name="analysis_mentions_correct_option_text",
            error_message="answer_binding_patch cannot degrade analysis_mentions_correct_option_text into a failed check.",
        )

    def apply_distractor_patch(
        self,
        item: dict,
        *,
        target_option: str,
        distractor_strategy: str,
        distractor_intensity: str,
        option_text: str,
        analysis: str,
        operator: str | None = None,
    ) -> dict:
        request_id = str(uuid4())
        normalized_target_option = str(target_option or "").strip().upper()
        normalized_distractor_strategy = str(distractor_strategy or "").strip()
        normalized_distractor_intensity = str(distractor_intensity or "").strip()
        normalized_option_text = str(option_text or "").strip()
        normalized_analysis = str(analysis or "").strip()
        current_question = GeneratedQuestion.model_validate(item.get("generated_question") or {})
        current_material = MaterialSelectionResult.model_validate(item["material_selection"])
        current_target_text = str(current_question.options.get(normalized_target_option) or "").strip()
        current_analysis_text = str(current_question.analysis or "").strip()
        requires_model_patch = bool(normalized_distractor_strategy or normalized_distractor_intensity)

        if normalized_target_option not in {"A", "B", "C", "D"}:
            raise DomainError(
                "distractor_patch requires target_option to be one of A/B/C/D.",
                status_code=422,
                details={"target_option": normalized_target_option},
            )
        if normalized_target_option == str(current_question.answer or "").strip().upper():
            raise DomainError(
                "distractor_patch only accepts a non-answer target_option.",
                status_code=422,
                details={"target_option": normalized_target_option, "answer": current_question.answer},
            )
        if not any(
            [
                normalized_distractor_strategy,
                normalized_distractor_intensity,
                normalized_option_text,
                normalized_analysis,
            ]
        ):
            raise DomainError(
                "distractor_patch requires at least one patch input.",
                status_code=422,
                details={"target_option": normalized_target_option},
            )

        if requires_model_patch:
            patch_revision = self._generate_distractor_patch_revision(
                item=item,
                current_question=current_question,
                material=current_material,
                target_option=normalized_target_option,
                draft_option_text=normalized_option_text or current_target_text,
                draft_analysis=normalized_analysis or current_analysis_text,
                distractor_strategy=normalized_distractor_strategy,
                distractor_intensity=normalized_distractor_intensity,
            )
            next_option_text = str(patch_revision.option_text or "").strip()
            next_analysis_text = str(patch_revision.analysis or "").strip()
        else:
            next_option_text = normalized_option_text or current_target_text
            next_analysis_text = normalized_analysis or current_analysis_text

        if not next_option_text:
            raise DomainError(
                "distractor_patch produced an empty target option.",
                status_code=422,
                details={"target_option": normalized_target_option},
            )
        if not next_analysis_text:
            raise DomainError(
                "distractor_patch produced an empty analysis.",
                status_code=422,
                details={"target_option": normalized_target_option},
            )
        if next_option_text == current_target_text and next_analysis_text == current_analysis_text:
            raise DomainError(
                "distractor_patch did not produce a scoped change.",
                status_code=422,
                details={"target_option": normalized_target_option},
            )

        normalized_options = dict(current_question.options)
        normalized_options[normalized_target_option] = next_option_text
        edited_question = current_question.model_copy(
            update={
                "options": normalized_options,
                "analysis": next_analysis_text,
                "metadata": {
                    **(current_question.metadata or {}),
                    "distractor_patch": True,
                    "distractor_patch_target": normalized_target_option,
                    "distractor_patch_strategy": normalized_distractor_strategy or None,
                    "distractor_patch_intensity": normalized_distractor_intensity or None,
                },
            }
        )

        revised_item = deepcopy(item)
        revised_item["generated_question"] = edited_question.model_dump()
        revised_item["stem_text"] = edited_question.stem
        revised_item["material_selection"] = deepcopy(item.get("material_selection") or {})
        revised_item["material_text"] = item.get("material_text")
        revised_item["material_source"] = deepcopy(item.get("material_source") or {})
        revised_item["material_usage_count_before"] = item.get("material_usage_count_before")
        revised_item["material_previously_used"] = item.get("material_previously_used")
        revised_item["material_last_used_at"] = item.get("material_last_used_at")
        revised_item["preference_profile"] = self._preference_profile_from_snapshot(revised_item.get("request_snapshot") or {})
        revised_item["feedback_snapshot"] = self._feedback_snapshot_from_material(current_material)

        validation_result = self.validator.validate(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            generated_question=edited_question,
            material_text=current_material.text,
            original_material_text=current_material.original_text,
            material_source=current_material.source,
            validator_contract=current_material.validator_contract,
            difficulty_fit=revised_item.get("difficulty_fit"),
            source_question=(revised_item.get("request_snapshot") or {}).get("source_question"),
            source_question_analysis=(revised_item.get("request_snapshot") or {}).get("source_question_analysis"),
            resolved_slots=revised_item.get("resolved_slots"),
            control_logic=revised_item.get("control_logic"),
        )
        revised_item["validation_result"] = validation_result.model_dump()
        revised_item["evaluation_result"] = self.evaluator.evaluate(
            question_type=revised_item["question_type"],
            business_subtype=revised_item.get("business_subtype"),
            generated_question=edited_question,
            validation_result=validation_result,
            material_text=current_material.text,
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
        revised_item["current_status"] = "generated" if validation_result.passed else "auto_failed"
        revised_item["statuses"]["generation_status"] = "success"
        revised_item["statuses"]["validation_status"] = validation_result.validation_status
        revised_item["statuses"]["review_status"] = "waiting_review" if validation_result.passed else "needs_revision"
        revised_item["latest_action"] = "distractor_patch"
        revised_item["latest_action_at"] = self.repository._utc_now()
        revised_item["notes"] = revised_item.get("notes", []) + [
            f"distractor_patch:{normalized_target_option}:{normalized_distractor_strategy or 'manual'}:{normalized_distractor_intensity or 'default'}:{operator or 'system'}"
        ]
        self._apply_manual_override_review_surface(revised_item)

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
            material=current_material,
            route=self.runtime_config.llm.routing.review_actions.question_modify,
            raw_model_output=None,
            parsed_structured_output=revised_item["generated_question"],
            parse_error=None,
            validation_result=revised_item["validation_result"],
        )
        runtime_snapshot = self._attach_feedback_runtime_context(
            built_item=revised_item,
            material=current_material,
            request_snapshot=revised_item.get("request_snapshot") or {},
            runtime_snapshot=runtime_snapshot,
        )
        revised_item["_version_record"] = self._build_version_record(
            item=revised_item,
            source_action="distractor_patch",
            parent_version_no=version_no - 1,
            version_no=version_no,
            target_difficulty=revised_item.get("difficulty_target"),
            material=current_material,
            prompt_template=template_record,
            raw_model_output=None,
            parsed_structured_output=revised_item["generated_question"],
            parse_error=None,
            validation_result=revised_item["validation_result"],
            evaluation_result=revised_item["evaluation_result"],
            runtime_snapshot=runtime_snapshot,
        )
        return revised_item

    def _generate_distractor_patch_revision(
        self,
        *,
        item: dict,
        current_question: GeneratedQuestion,
        material: MaterialSelectionResult,
        target_option: str,
        draft_option_text: str,
        draft_analysis: str,
        distractor_strategy: str,
        distractor_intensity: str,
    ) -> DistractorPatchDraft:
        route = self.runtime_config.llm.routing.review_actions.question_modify
        prompt_template = self._resolve_template(
            question_type=item["question_type"],
            business_subtype=item.get("business_subtype"),
            action_type="question_modify",
        )
        correct_answer = str(current_question.answer or "").strip().upper()
        correct_option_text = str(current_question.options.get(correct_answer) or "").strip()
        response = self.llm_gateway.generate_json(
            route=route,
            system_prompt=prompt_template.content,
            user_prompt="\n\n".join(
                [
                    "Current generated question JSON:",
                    json.dumps(current_question.model_dump(), ensure_ascii=False),
                    "Original source material:",
                    material.text,
                    f"Target wrong option to revise: {target_option}",
                    f"Current correct answer letter (must stay fixed): {correct_answer}",
                    f"Current correct option text (must stay unchanged): {correct_option_text}",
                    f"Current target option text: {current_question.options.get(target_option, '')}",
                    f"Target distractor strategy: {distractor_strategy or 'keep current error mode'}",
                    f"Target distractor intensity: {distractor_intensity or 'keep current intensity'}",
                    f"Current draft option text: {draft_option_text}",
                    f"Current draft analysis: {draft_analysis}",
                    "Only revise the target wrong option text and the full analysis.",
                    "Do not change stem, material, answer, or any other option.",
                    "The answer letter and correct option text must remain unchanged.",
                    "The analysis must still explain why the current correct option text fits.",
                    "Return JSON with keys option_text and analysis only.",
                ]
            ),
            schema_name="distractor_patch_revision",
            schema=DistractorPatchDraft.model_json_schema(),
        )
        return DistractorPatchDraft.model_validate(response)

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
        self._hydrate_sentence_order_candidate_type_context(built_item)
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
        self._apply_distill_runtime_overlay(
            build_request=build_request,
            built_item=built_item,
            material=material,
        )
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
        built_item["manual_override_active"] = bool((previous_item or {}).get("manual_override_active"))

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
            resolved_slots=built_item.get("resolved_slots"),
            control_logic=built_item.get("control_logic"),
        )
        self._apply_consistency_hard_fail_gate(validation_result=validation_result)

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
                scope = resolve_repair_mode_scope(repair_plan.get("mode"))
                if scope and scope.name == "analysis_only":
                    retry_bundle = self.apply_analysis_only_repair(
                        built_item=built_item,
                        material=material,
                        current_question=generated_question,
                        route=self._question_repair_route(),
                        repair_plan=repair_plan,
                        feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                    )
                    regenerated_question = retry_bundle["generated_question"]
                    retry_raw_output = retry_bundle["raw_model_output"]
                    retry_validation_result = retry_bundle["validation_result"]
                elif scope and scope.name == "answer_binding_patch":
                    retry_bundle = self.apply_answer_binding_patch(
                        built_item=built_item,
                        material=material,
                        current_question=generated_question,
                        route=self._question_repair_route(),
                        repair_plan=repair_plan,
                        feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                    )
                    regenerated_question = retry_bundle["generated_question"]
                    retry_raw_output = retry_bundle["raw_model_output"]
                    retry_validation_result = retry_bundle["validation_result"]
                else:
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
                        resolved_slots=built_item.get("resolved_slots"),
                        control_logic=built_item.get("control_logic"),
                    )
                self._apply_consistency_hard_fail_gate(validation_result=retry_validation_result)
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
                scope = resolve_repair_mode_scope(repair_plan.get("mode"))
                if scope and scope.name == "analysis_only":
                    repaired_bundle = self.apply_analysis_only_repair(
                        built_item=built_item,
                        material=material,
                        current_question=generated_question,
                        route=self._question_repair_route(),
                        repair_plan=repair_plan,
                        feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                    )
                    repaired_question = repaired_bundle["generated_question"]
                    repaired_raw_output = repaired_bundle["raw_model_output"]
                    repaired_validation_result = repaired_bundle["validation_result"]
                    repaired_evaluation_result = repaired_bundle["evaluation_result"]
                    repaired_quality_gate_errors = repaired_bundle["quality_gate_errors"]
                elif scope and scope.name == "answer_binding_patch":
                    repaired_bundle = self.apply_answer_binding_patch(
                        built_item=built_item,
                        material=material,
                        current_question=generated_question,
                        route=self._question_repair_route(),
                        repair_plan=repair_plan,
                        feedback_notes=[*(repair_plan.get("notes") or []), *feedback_notes],
                    )
                    repaired_question = repaired_bundle["generated_question"]
                    repaired_raw_output = repaired_bundle["raw_model_output"]
                    repaired_validation_result = repaired_bundle["validation_result"]
                    repaired_evaluation_result = repaired_bundle["evaluation_result"]
                    repaired_quality_gate_errors = repaired_bundle["quality_gate_errors"]
                else:
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
                        resolved_slots=built_item.get("resolved_slots"),
                        control_logic=built_item.get("control_logic"),
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
                self._apply_consistency_hard_fail_gate(validation_result=repaired_validation_result)
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
        built_item["current_status"] = "generated" if validation_result.passed else "auto_failed"
        built_item["statuses"]["review_status"] = "waiting_review" if validation_result.passed else "needs_revision"
        self._apply_manual_override_review_surface(built_item)

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
                        "item": {
                            "item_id": f"fallback::{uuid4().hex}",
                            "batch_id": batch_id,
                            "question_type": standard_request["question_type"],
                            "business_subtype": standard_request.get("business_subtype"),
                            "pattern_id": standard_request.get("pattern_id"),
                            "current_status": "auto_failed",
                            "validation_result": {"errors": ["sentence_order_material_unit_count_mismatch"]},
                        },
                        "material": material,
                        "summary": rejected_summary,
                        "rank_score": -999.0,
                    },
                }
            material = adapted_material

        material = self._refine_material_if_needed(material, request_snapshot=request_snapshot)
        material = self._prepare_question_service_material(
            material=material,
            question_type=standard_request["question_type"],
            request_snapshot=request_snapshot,
        )
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

    def _run_primary_candidate_with_retries(
        self,
        *,
        material: MaterialSelectionResult,
        material_index: int,
        retry_limit: int,
        standard_request: dict,
        source_question_analysis: dict | None,
        request_snapshot: dict,
        batch_id: str,
        request_id: str,
    ) -> dict[str, Any]:
        rejected_attempts: list[dict] = []
        best_rejected_candidate: dict[str, Any] | None = None
        for attempt_index in range(max(1, retry_limit)):
            result = self._run_race_candidate(
                material=material,
                index=attempt_index,
                standard_request=standard_request,
                source_question_analysis=source_question_analysis,
                request_snapshot=request_snapshot,
                batch_id=batch_id,
                request_id=request_id,
            )
            if result["accepted"]:
                candidate = result["candidate"]
                candidate["retry_count"] = attempt_index
                return {
                    "accepted": True,
                    "item": candidate["item"],
                    "material": candidate["material"],
                    "rejected_attempts": rejected_attempts,
                    "best_rejected_candidate": best_rejected_candidate,
                }
            summary = dict(result["summary"] or {})
            summary["material_retry_round"] = attempt_index + 1
            summary["material_index"] = material_index
            rejected_attempts.append(summary)
            candidate = result.get("candidate")
            if isinstance(candidate, dict) and (
                best_rejected_candidate is None
                or float(candidate.get("rank_score") or 0.0) > float(best_rejected_candidate.get("rank_score") or 0.0)
            ):
                best_rejected_candidate = candidate

        return {
            "accepted": False,
            "item": None,
            "material": material,
            "rejected_attempts": rejected_attempts,
            "best_rejected_candidate": best_rejected_candidate,
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
        source_question_payload = (
            self._normalize_source_question_payload_dict(request.source_question.model_dump()) if request.source_question else None
        )
        user_material_payload = (
            self._normalize_user_material_payload_dict(request.user_material.model_dump()) if request.user_material else None
        )
        normalized_source_question_analysis = normalize_readable_structure(deepcopy(source_question_analysis))
        resolved_topic = request.topic or normalized_source_question_analysis.get("topic")
        return {
            "request_id": request_id,
            "generation_mode": request.generation_mode,
            "question_type": standard_request["question_type"],
            "business_subtype": standard_request.get("business_subtype"),
            "pattern_id": resolved_pattern_id,
            "question_card_id": question_card_binding.get("question_card_id"),
            "question_card_binding": deepcopy(question_card_binding),
            "difficulty_target": standard_request["difficulty_target"],
            "topic": normalize_readable_text(resolved_topic) if str(resolved_topic or "").strip() else resolved_topic,
            "material_structure": request.material_structure,
            "passage_style": request.passage_style,
            "use_fewshot": request.use_fewshot,
            "fewshot_mode": request.fewshot_mode,
            "type_slots": deepcopy(standard_request.get("type_slots") or request.type_slots or {}),
            "extra_constraints": merged_extra_constraints,
            "preference_profile": normalized_preference_profile,
            "material_policy": request.material_policy.model_dump() if request.material_policy else None,
            "source_question": source_question_payload,
            "user_material": user_material_payload,
            "source_question_analysis": normalized_source_question_analysis,
            "source_form": {
                "question_card_id": request.question_card_id,
                "generation_mode": request.generation_mode,
                "question_focus": request.question_focus,
                "business_subtype": request.business_subtype,
                "difficulty_level": request.difficulty_level,
                "effective_difficulty_target": standard_request["difficulty_target"],
                "special_question_types": deepcopy(request.special_question_types),
                "mapping_source": decoded.get("mapping_source"),
                "selected_special_type": decoded.get("selected_special_type"),
            },
        }

    def _prepare_question_service_material(
        self,
        *,
        material: MaterialSelectionResult,
        question_type: str,
        request_snapshot: dict[str, Any],
    ) -> MaterialSelectionResult:
        cleaned_text = self._clean_material_text(material.text or material.original_text or "")
        cleaned_original_text = self._clean_material_text(material.original_text or material.text or "")
        base_material = material.model_copy(
            update={
                "text": cleaned_text,
                "original_text": cleaned_original_text or cleaned_text,
                "source": deepcopy(material.source or {}),
            }
        )

        if question_type == "sentence_fill":
            return self._derive_sentence_fill_ready_material(
                material=base_material,
                request_snapshot=request_snapshot,
            )

        if question_type == "sentence_order":
            return self._derive_sentence_order_ready_material(
                material=base_material,
                request_snapshot=request_snapshot,
            )

        return base_material

    def _derive_sentence_fill_ready_material(
        self,
        *,
        material: MaterialSelectionResult,
        request_snapshot: dict[str, Any],
    ) -> MaterialSelectionResult:
        source_question_analysis = request_snapshot.get("source_question_analysis") or {}
        structure_constraints = normalize_sentence_fill_constraints(
            source_question_analysis.get("structure_constraints")
            or source_question_analysis.get("retrieval_structure_constraints")
            or {}
        )
        type_slots = normalize_sentence_fill_constraints(request_snapshot.get("type_slots") or {})
        if type_slots:
            merged_constraints = dict(structure_constraints)
            merged_constraints.update({key: value for key, value in type_slots.items() if value})
            structure_constraints = normalize_sentence_fill_constraints(merged_constraints)

        blank_position = str(structure_constraints.get("blank_position") or "middle").strip() or "middle"
        function_type = normalize_sentence_fill_function_type(structure_constraints.get("function_type")) or ""
        logic_relation = str(structure_constraints.get("logic_relation") or "").strip()
        material_source = material.source if isinstance(material.source, dict) else {}
        existing_prompt_extras = (
            deepcopy(material_source.get("prompt_extras") or {})
            if isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        compliance_report = (
            deepcopy(material_source.get("material_compliance_report") or {})
            if isinstance(material_source.get("material_compliance_report"), dict)
            else {}
        )
        if self._sentence_fill_source_prompt_extras_polluted(
            prompt_extras=existing_prompt_extras,
            compliance_report=compliance_report,
        ):
            existing_prompt_extras = {}
        existing_anchor_text = str(existing_prompt_extras.get("answer_anchor_text") or "").strip()
        if self._sentence_fill_anchor_text_invalid(existing_anchor_text):
            existing_anchor_text = ""
        working_source_text = self._sentence_fill_working_source_text(material)

        if (
            not existing_anchor_text
            and self._sentence_fill_material_already_blank(working_source_text)
        ):
            raise DomainError(
                "sentence_fill requires original unblanked material so the removed source sentence can remain the only legal answer.",
                status_code=422,
                details={
                    "question_type": "sentence_fill",
                    "reason": "missing_original_answer_anchor",
                },
            )

        paragraphs = self._split_sentence_fill_paragraphs(working_source_text)
        paragraph_units = [self._split_sentence_fill_units(paragraph) for paragraph in paragraphs]
        blank_target = self._select_sentence_fill_blank_target(
            paragraph_units=paragraph_units,
            blank_position=blank_position,
        )

        if blank_target is None:
            updated_source = deepcopy(material.source or {})
            prompt_extras = deepcopy((updated_source.get("prompt_extras") or {}))
            prompt_extras.update(
                {
                    "blank_position": blank_position,
                    "function_type": function_type,
                    "logic_relation": logic_relation,
                }
            )
            updated_source["prompt_extras"] = prompt_extras
            return material.model_copy(update={"source": updated_source})

        rendered_paragraphs = [list(units) for units in paragraph_units]
        answer_anchor_text = self._normalize_sentence_fill_anchor_text(
            str(rendered_paragraphs[blank_target[0]][blank_target[1]] or "").strip()
        )
        if self._sentence_fill_anchor_text_invalid(answer_anchor_text):
            raise DomainError(
                "sentence_fill could not recover an original source sentence for the blank position.",
                status_code=422,
                details={
                    "question_type": "sentence_fill",
                    "reason": "invalid_answer_anchor_text",
                },
            )
        rendered_paragraphs[blank_target[0]][blank_target[1]] = "____"
        rendered_paragraphs = [self._dedupe_sentence_fill_units(units) for units in rendered_paragraphs]
        fill_ready_material = self._normalize_sentence_fill_display_text(
            self._render_sentence_fill_paragraphs(rendered_paragraphs)
        )
        fill_ready_local_material = self._normalize_sentence_fill_display_text(
            self._build_sentence_fill_local_window(
            original_units=paragraph_units[blank_target[0]],
            blank_index=blank_target[1],
            )
        )
        existing_blanked_display = self._normalize_sentence_fill_display_text(
            str(existing_prompt_extras.get("blanked_text") or "").replace("[BLANK]", "____")
        )
        fallback_fill_display = self._normalize_sentence_fill_display_text(
            self._replace_sentence_fill_anchor_once(working_source_text, answer_anchor_text)
        )
        if self._sentence_fill_display_collapsed(fallback_fill_display) and not self._sentence_fill_display_collapsed(existing_blanked_display):
            fallback_fill_display = existing_blanked_display
        if self._sentence_fill_display_collapsed(fill_ready_material):
            fill_ready_material = fallback_fill_display or fill_ready_material
        if self._sentence_fill_display_collapsed(fill_ready_local_material):
            fill_ready_local_material = fallback_fill_display or fill_ready_local_material
        if self._sentence_fill_display_not_exam_like(
            fill_ready_material=fill_ready_material,
            fill_ready_local_material=fill_ready_local_material,
            answer_anchor_text=answer_anchor_text,
        ):
            raise DomainError(
                "sentence_fill material window is too thin or the removed sentence dominates the display, which is not exam-like enough.",
                status_code=422,
                details={
                    "question_type": "sentence_fill",
                    "reason": "sentence_fill_display_not_exam_like",
                },
            )
        preferred_answer_shape, forbidden_answer_styles = self._sentence_fill_answer_shape_hints(
            blank_position=blank_position,
            function_type=function_type,
            logic_relation=logic_relation,
        )
        leaf_key, hard_logic_tags, hard_logic_rules = self._derive_sentence_fill_hard_logic_profile(
            blank_position=blank_position,
            function_type=function_type,
            logic_relation=logic_relation,
        )

        updated_source = deepcopy(material.source or {})
        prompt_extras = deepcopy((updated_source.get("prompt_extras") or {}))
        prompt_extras.update(
            {
                "blank_position": blank_position,
                "function_type": function_type,
                "logic_relation": logic_relation,
                "fill_ready_material": fill_ready_material,
                "fill_ready_local_material": fill_ready_local_material,
                "answer_anchor_text": answer_anchor_text,
                "require_original_answer_sentence": True,
                "preferred_answer_shape": preferred_answer_shape,
                "forbidden_answer_styles": forbidden_answer_styles,
                "hard_logic_leaf_key": leaf_key,
                "hard_logic_tags": hard_logic_tags,
                "hard_logic_rules": hard_logic_rules,
            }
        )
        updated_source["prompt_extras"] = prompt_extras

        return material.model_copy(
            update={
                "text": fill_ready_material,
                "original_text": working_source_text,
                "source": updated_source,
                "text_refined": True,
                "refinement_reason": f"sentence_fill_ready::{blank_position or 'middle'}::{function_type or 'unspecified'}",
            }
        )

    def _enforce_sentence_fill_original_answer(
        self,
        *,
        generated_question: GeneratedQuestion,
        material: MaterialSelectionResult,
    ) -> GeneratedQuestion:
        if generated_question.question_type != "sentence_fill":
            return generated_question
        material_source = material.source if isinstance(material.source, dict) else {}
        prompt_extras = material_source.get("prompt_extras") if isinstance(material_source.get("prompt_extras"), dict) else {}
        if not bool(prompt_extras.get("require_original_answer_sentence")):
            return generated_question
        answer_anchor_text = str(prompt_extras.get("answer_anchor_text") or "").strip()
        answer_key = str(generated_question.answer or "").strip().upper()
        if not answer_anchor_text or not answer_key:
            return generated_question
        options = dict(generated_question.options or {})
        if options.get(answer_key, "").strip() == answer_anchor_text:
            return generated_question
        options[answer_key] = answer_anchor_text
        metadata = dict(generated_question.metadata or {})
        metadata["sentence_fill_original_answer_enforced"] = True
        return generated_question.model_copy(update={"options": options, "metadata": metadata})

    @staticmethod
    def _sentence_fill_material_already_blank(text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return False
        if "[BLANK]" in normalized:
            return True
        if re.search(r"_{2,}|﹍+|（\s*）|\(\s*\)|“\s*”", normalized):
            return True
        if re.search(r"[，,：:；;]\s*[。！？!?]", normalized):
            return True
        if any(token in normalized for token in ("填入", "横线部分", "划横线部分", "画横线部分", "最恰当的一项", "最恰当的一句")):
            return True
        return False

    @staticmethod
    def _sentence_fill_anchor_text_invalid(text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return True
        if QuestionGenerationService._is_sentence_fill_structural_heading_unit(normalized):
            return True
        if QuestionGenerationService._sentence_fill_material_already_blank(normalized):
            return True
        semantic = re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)
        if not semantic:
            return True
        return False

    def _sentence_fill_working_source_text(self, material: MaterialSelectionResult) -> str:
        material_source = material.source if isinstance(material.source, dict) else {}
        prompt_extras = (
            deepcopy(material_source.get("prompt_extras") or {})
            if isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        current_text = self._clean_material_text(material.text or "")
        if bool(material.text_refined) and current_text and not self._sentence_fill_material_already_blank(current_text):
            return self._normalize_sentence_fill_display_text(current_text)
        original_text = self._clean_material_text(material.original_text or "")
        if original_text and not self._sentence_fill_material_already_blank(original_text):
            return self._normalize_sentence_fill_display_text(original_text)
        if current_text and not self._sentence_fill_material_already_blank(current_text):
            return self._normalize_sentence_fill_display_text(current_text)
        context_window = self._clean_material_text(str(prompt_extras.get("context_window") or ""))
        if context_window and not self._sentence_fill_material_already_blank(context_window):
            return self._normalize_sentence_fill_display_text(context_window)
        return self._normalize_sentence_fill_display_text(original_text or current_text)

    @staticmethod
    def _normalize_sentence_fill_display_text(text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        normalized = QuestionGenerationService._strip_sentence_fill_leading_fragment(normalized)
        normalized = re.sub(r"\.{3,}", "……", normalized)
        normalized = re.sub(r"…{3,}", "……", normalized)
        first_ellipsis_seen = False

        def _replace_extra_ellipsis(match: re.Match[str]) -> str:
            nonlocal first_ellipsis_seen
            if not first_ellipsis_seen:
                first_ellipsis_seen = True
                return "……"
            return "，"

        normalized = re.sub(r"……", _replace_extra_ellipsis, normalized)
        normalized = re.sub(r"[，,]\s*[，,]", "，", normalized)
        normalized = re.sub(r"……(?=[，。；;！？!?])", "", normalized)
        normalized = re.sub(r"\s+", "", normalized)
        return normalized.strip()

    @staticmethod
    def _strip_sentence_fill_leading_fragment(text: str) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return ""
        candidate = re.sub(r"^[\.\,，;；:：、]+", "", candidate).strip()
        candidate = re.sub(r"^(?:[(（]?\d+[)）]?|[一二三四五六七八九十百千万]+[、.．])\s*", "", candidate).strip()
        fragment_match = re.match(
            r"^([^\s，。！？；;：:]{4,16})(在|从|当|若|如|为|把|将|对)([^。！？!?]{8,})$",
            candidate,
        )
        if fragment_match:
            fragment = fragment_match.group(1)
            if len(re.findall(r"[的了和与及并或]", fragment)) <= 1:
                candidate = f"{fragment_match.group(2)}{fragment_match.group(3)}".strip()
        return candidate

    @staticmethod
    def _normalize_sentence_fill_anchor_text(text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        normalized = re.sub(r'^[”’」』】）)\]\s]+', "", normalized)
        normalized = re.sub(r"\s+", "", normalized)
        return normalized.strip()

    @classmethod
    def _sentence_fill_source_prompt_extras_polluted(
        cls,
        *,
        prompt_extras: dict[str, Any] | None,
        compliance_report: dict[str, Any] | None,
    ) -> bool:
        report = dict(compliance_report or {})
        issues = {str(issue).strip() for issue in (report.get("issues") or []) if str(issue).strip()}
        if report.get("passed") is False and "contains_blank_markers" in issues:
            return True
        extras = dict(prompt_extras or {})
        answer_anchor_text = str(extras.get("answer_anchor_text") or "").strip()
        return cls._sentence_fill_material_already_blank(answer_anchor_text)

    @staticmethod
    def _strip_sentence_fill_unit_prefix(text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        return re.sub(r"^\s*(?:[(（]?\d+[)）]?|[一二三四五六七八九十百千万]+[、.．])\s*", "", normalized).strip()

    @staticmethod
    def _is_sentence_fill_structural_heading_unit(text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return True
        if not re.match(r"^\s*(?:[(（]?\d+[)）]?|[一二三四五六七八九十百千万]+[、.．])\s*", raw):
            return False
        stripped = QuestionGenerationService._strip_sentence_fill_unit_prefix(raw)
        compact = re.sub(r"[\s。！？!?；;：:，,、]", "", stripped)
        if not compact:
            return True
        if len(compact) <= 12 and not re.search(r"[，,；;：:]", stripped):
            return True
        return False

    def _split_sentence_fill_paragraphs(self, text: str) -> list[str]:
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text or "") if part.strip()]
        return paragraphs or ([text.strip()] if str(text or "").strip() else [])

    def _split_sentence_fill_units(self, paragraph: str) -> list[str]:
        raw_units = [
            item.strip()
            for item in re.split(r"(?<=[。！？!?])\s*", str(paragraph or "").strip())
            if item.strip()
        ]
        units: list[str] = []
        for item in raw_units:
            if self._is_sentence_fill_structural_heading_unit(item):
                continue
            normalized = self._strip_sentence_fill_unit_prefix(item)
            units.extend(self._split_sentence_fill_long_unit(normalized or item.strip()))
        return units or ([str(paragraph or "").strip()] if str(paragraph or "").strip() else [])

    @staticmethod
    def _split_sentence_fill_long_unit(unit: str) -> list[str]:
        normalized = str(unit or "").strip()
        if not normalized:
            return []
        if len(normalized) < 34:
            return [normalized]
        if sum(normalized.count(token) for token in ("，", "；", ":", "：")) < 2:
            return [normalized]
        raw_parts = [
            part.strip()
            for part in re.split(r"(?<=[，；;：:])", normalized)
            if part and part.strip()
        ]
        if len(raw_parts) < 2:
            return [normalized]
        merged: list[str] = []
        buffer = ""
        for part in raw_parts:
            piece = str(part).strip()
            compact = re.sub(r"[，；;：:\s]", "", piece)
            if len(compact) < 8:
                buffer += piece
                continue
            if buffer:
                piece = f"{buffer}{piece}"
                buffer = ""
            merged.append(piece)
        if buffer:
            if merged:
                merged[-1] = f"{merged[-1]}{buffer}"
            else:
                merged.append(buffer)
        cleaned = [item.strip("，；;：: ").strip() for item in merged if item.strip("，；;：: ").strip()]
        return cleaned or [normalized]

    def _dedupe_sentence_fill_units(self, units: list[str]) -> list[str]:
        deduped: list[str] = []
        signatures: list[str] = []
        for unit in units:
            normalized = re.sub(r"\s+", "", str(unit or ""))
            if not normalized:
                continue
            if any(
                normalized == existing
                or normalized in existing
                or existing in normalized
                or SequenceMatcher(None, normalized, existing).ratio() >= 0.96
                for existing in signatures
            ):
                continue
            signatures.append(normalized)
            deduped.append(str(unit).strip())
        return deduped

    def _render_sentence_fill_paragraphs(self, paragraph_units: list[list[str]]) -> str:
        paragraphs = ["".join(unit for unit in units if str(unit or "").strip()).strip() for units in paragraph_units]
        paragraphs = [paragraph for paragraph in paragraphs if paragraph]
        return "\n\n".join(paragraphs).strip()

    def _select_sentence_fill_blank_target(
        self,
        *,
        paragraph_units: list[list[str]],
        blank_position: str,
    ) -> tuple[int, int] | None:
        flat_indices = [
            (paragraph_index, unit_index)
            for paragraph_index, units in enumerate(paragraph_units)
            for unit_index, unit in enumerate(units)
            if str(unit or "").strip() and not self._sentence_fill_anchor_text_invalid(unit)
        ]
        if not flat_indices:
            return None
        if blank_position == "opening":
            return flat_indices[0]
        if blank_position == "ending":
            return flat_indices[-1]
        middle = max(0, len(flat_indices) // 2)
        return flat_indices[min(len(flat_indices) - 1, middle)]

    def _build_sentence_fill_local_window(
        self,
        *,
        original_units: list[str],
        blank_index: int,
    ) -> str:
        if not original_units:
            return ""
        start = max(0, blank_index - 1)
        end = min(len(original_units), blank_index + 2)
        window_units: list[str] = []
        for index in range(start, end):
            unit = str(original_units[index] or "").strip()
            if index != blank_index and self._is_sentence_fill_structural_heading_unit(unit):
                continue
            if index == blank_index:
                window_units.append("____")
            else:
                window_units.append(self._strip_sentence_fill_unit_prefix(unit) or unit)
        return "".join(unit for unit in window_units if str(unit or "").strip()).strip()

    @staticmethod
    def _sentence_fill_display_collapsed(text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return True
        visible = re.sub(r"[_\s，,。；;：:！!？?……]+", "", normalized)
        return len(visible) < 8

    def _replace_sentence_fill_anchor_once(self, text: str, answer_anchor_text: str) -> str:
        base_text = self._normalize_sentence_fill_display_text(text)
        anchor = self._normalize_sentence_fill_anchor_text(answer_anchor_text)
        if not base_text:
            return ""
        if not anchor:
            return base_text
        anchor_index = base_text.find(anchor)
        if anchor_index < 0:
            return base_text
        return (base_text[:anchor_index] + "____" + base_text[anchor_index + len(anchor):]).strip()

    @staticmethod
    def _sentence_fill_display_not_exam_like(
        *,
        fill_ready_material: str,
        fill_ready_local_material: str,
        answer_anchor_text: str,
    ) -> bool:
        display = str(fill_ready_material or fill_ready_local_material or "").strip()
        if not display:
            return True
        normalized = display.replace("____", "[BLANK]")
        visible = re.sub(r"\[BLANK\]", "", normalized)
        visible = re.sub(r"[\W_]+", "", visible, flags=re.UNICODE)
        sentence_count = len([part for part in re.split(r"(?<=[。！？!?])\s*", normalized) if part.strip()])
        if len(visible) < 18 or sentence_count < 1:
            return True
        anchor = re.sub(r"[\W_]+", "", str(answer_anchor_text or ""), flags=re.UNICODE)
        if not anchor:
            return True
        if len(anchor) / max(1, len(visible) + len(anchor)) >= 0.88:
            return True
        return False

    def _sentence_fill_answer_shape_hints(
        self,
        *,
        blank_position: str,
        function_type: str,
        logic_relation: str,
    ) -> tuple[str, list[str]]:
        if blank_position == "opening":
            return "natural_lead", ["motto_like", "quote_like", "editorial_slogan"]
        if blank_position == "ending" and function_type in {"countermeasure", "conclusion"}:
            return "problem_response", ["policy_slogan", "macro_call", "future_vision"]
        if function_type == "summary" or logic_relation == "summary":
            return "closing_summary", ["policy_slogan", "editorial_commentary"]
        if function_type in {"bridge", "carry_previous", "lead_next"}:
            return "local_bridge", ["macro_call", "detached_commentary"]
        return "local_explanation", ["policy_slogan", "detached_commentary"]

    def _derive_sentence_fill_hard_logic_profile(
        self,
        *,
        blank_position: str,
        function_type: str,
        logic_relation: str,
    ) -> tuple[str, list[str], list[str]]:
        normalized_position = str(blank_position or "middle").strip() or "middle"
        normalized_function = str(function_type or "bridge").strip() or "bridge"
        normalized_relation = str(logic_relation or "continuation").strip() or "continuation"
        leaf_key = f"sentence_fill.{normalized_position}.{normalized_function}.{normalized_relation}"
        tags = [
            "sentence_fill.source_truth.original_removed_sentence",
            "sentence_fill.source_truth.full_passage_before_blank",
            "sentence_fill.uniqueness.local_slot_only",
            f"sentence_fill.leaf.{normalized_position}.{normalized_function}",
        ]
        rules = [
            "生成时就把原文被挖掉的句子当作唯一合法正确项，不要先自由生成再交给校验器兜底。",
            "空位必须在原文合法槽位内工作，正确项只允许回填该槽位，不允许借题发挥成更宏观的新句子。",
        ]
        if normalized_position == "opening":
            rules.append("opening 空位必须像自然起句，先承担领起作用，不要提前写成正文摘要或评论结论。")
        if normalized_position == "middle":
            rules.append("middle 空位必须留在段内局部逻辑链上，不要把局部承接句改写成段落级总评。")
        if normalized_position == "ending":
            rules.append("ending 空位必须完成收束，不要重新开启新任务、新口号或更远一层的宏观扩展。")
        if normalized_function == "bridge":
            rules.append("bridge 叶必须同时咬住左侧锚点和右侧落点，不能只顺一边。")
        elif normalized_function == "carry_previous":
            rules.append("carry_previous 叶必须先回扣前文刚建立的对象和判断，再考虑后续顺滑。")
        elif normalized_function == "lead_next":
            rules.append("lead_next 叶必须点亮后文马上展开的具体概念，不要只写泛泛的重要性。")
        elif normalized_function == "topic_intro":
            rules.append("topic_intro 叶优先保留题眼、引语、警句式起势，不要抹成现代说明句。")
        elif normalized_function in {"conclusion", "summary"}:
            rules.append("summary/conclusion 叶要落在本文当前对象的收束判断上，不要变成外加评论。")
        elif normalized_function == "countermeasure":
            rules.append("countermeasure 叶必须给出同尺度的原则或对策，不要把危害续写当成对策。")
        if normalized_relation == "explanation":
            rules.append("logic_relation=explanation 时，正确项应补足当前解释链，而不是另起评价。")
        elif normalized_relation == "focus_shift":
            rules.append("logic_relation=focus_shift 时，正确项要把重心准确转给后文对象，不要悬在中间。")
        elif normalized_relation == "summary":
            rules.append("logic_relation=summary 时，正确项应压缩已有信息并完成收口，不要新增论点。")
        return leaf_key, list(dict.fromkeys(tags)), list(dict.fromkeys(rules))

    def _derive_sentence_order_hard_logic_profile(
        self,
        *,
        request_snapshot: dict[str, Any],
        sortable_units: list[str],
        binding_pairs: list[tuple[int, int]],
        sentence_roles: dict[str, str],
    ) -> tuple[str, list[str], list[str]]:
        type_slots = deepcopy(request_snapshot.get("type_slots") or {})
        opening_anchor_type = str(type_slots.get("opening_anchor_type") or "explicit_topic").strip() or "explicit_topic"
        middle_structure_type = str(type_slots.get("middle_structure_type") or "local_binding").strip() or "local_binding"
        closing_anchor_type = str(type_slots.get("closing_anchor_type") or "conclusion").strip() or "conclusion"
        leaf_key = f"sentence_order.{opening_anchor_type}.{middle_structure_type}.{closing_anchor_type}"
        tags = [
            "sentence_order.source_truth.material_original_order",
            "sentence_order.source_truth.display_units_from_material_only",
            "sentence_order.uniqueness.single_defensible_sequence",
            f"sentence_order.leaf.{opening_anchor_type}.{middle_structure_type}.{closing_anchor_type}",
        ]
        rules = [
            "生成时就把原材料原始顺序当作唯一真值；模型只能决定展示怎么打乱，不能改写真正正确顺序。",
            "展示单元必须全部来自原材料，不允许重写、拆换或另造新句来让排序更好做。",
            "排序唯一性优先来自合法首句、局部捆绑和自然收束三者的共同支持，不要依赖讲解腔提示词强拉答案。",
        ]
        if sortable_units:
            rules.append(f"当前展示单元数固定为 {len(sortable_units)}，所有选项都必须是该长度的纯数字顺序。")
        if binding_pairs:
            rules.append("存在局部硬捆绑对时，正确顺序必须保住这些相邻约束，不要为了表面顺滑拆开。")
        if sentence_roles:
            rules.append("句群角色链应沿着材料原始推进顺序展开，不要让例子、转折或结论越位到更前面。")
        if opening_anchor_type in {"background_intro", "explicit_topic", "viewpoint_opening", "problem_opening"}:
            rules.append(f"opening_anchor_type={opening_anchor_type} 时，首句优先承担开题合法性，不要让局部细节句抢首位。")
        if middle_structure_type in {"local_binding", "parallel_expansion", "cause_effect_chain", "problem_solution_blocks"}:
            rules.append(f"middle_structure_type={middle_structure_type} 时，中段顺序要服从该推进骨架，不要只看表面通顺。")
        if closing_anchor_type in {"conclusion", "summary", "call_to_action", "case_support"}:
            rules.append(f"closing_anchor_type={closing_anchor_type} 时，尾句必须承担对应收束角色，不要提前泄露到前位。")
        return leaf_key, list(dict.fromkeys(tags)), list(dict.fromkeys(rules))

    def _derive_sentence_order_ready_material(
        self,
        *,
        material: MaterialSelectionResult,
        request_snapshot: dict[str, Any],
    ) -> MaterialSelectionResult:
        source = deepcopy(material.source or {})
        prompt_extras = deepcopy((source.get("prompt_extras") or {}))
        raw_text = self._clean_material_text(material.text or material.original_text or "")
        if not raw_text:
            return material

        source_question_analysis = request_snapshot.get("source_question_analysis") or {}
        coerced = self._coerce_sentence_order_material(
            material=material.model_copy(update={"text": raw_text, "original_text": material.original_text or raw_text}),
            source_question_analysis=source_question_analysis,
        )
        prepared_material = coerced or material.model_copy(
            update={"text": raw_text, "original_text": material.original_text or raw_text}
        )
        units = self._extract_sortable_units_from_text(prepared_material.text or "")
        target_unit_count = self._sentence_order_target_unit_count(source_question_analysis) or (
            len(units) if len(units) in {4, 5, 6} else 6
        )
        normalized_units = (
            self._normalize_sentence_order_units_to_six(units, target_count=target_unit_count)
            or self._normalize_sentence_order_units_to_six(units)
            or units
        )
        sortable_units = [
            self._clean_sentence_order_sortable_unit(unit)
            for unit in normalized_units[:target_unit_count]
            if self._clean_sentence_order_sortable_unit(unit)
        ]
        if target_unit_count not in {4, 5, 6} or len(sortable_units) < target_unit_count:
            return prepared_material

        sortable_material_text = self._format_sortable_units(sortable_units)
        natural_material_text = self._format_sentence_order_natural_material(
            raw_text=raw_text,
            sortable_units=sortable_units,
        )
        natural_material_text, presentation_meta = self._refine_sentence_order_presentation_material(
            raw_text=raw_text,
            current_text=natural_material_text,
            sortable_units=sortable_units,
            source=source,
        )
        binding_pairs = self._derive_sentence_order_binding_pairs(sortable_units)
        sentence_roles = self._derive_sentence_order_roles(sortable_units)
        leaf_key, hard_logic_tags, hard_logic_rules = self._derive_sentence_order_hard_logic_profile(
            request_snapshot=request_snapshot,
            sortable_units=sortable_units,
            binding_pairs=binding_pairs,
            sentence_roles=sentence_roles,
        )
        prompt_extras.update(
            {
                "sortable_units": sortable_units,
                "sortable_material_text": sortable_material_text,
                "sortable_unit_count": len(sortable_units),
                "head_anchor_text": sortable_units[0],
                "tail_anchor_text": sortable_units[-1],
                "binding_pairs": [list(pair) for pair in binding_pairs],
                "sentence_roles": sentence_roles,
                "hard_logic_leaf_key": leaf_key,
                "hard_logic_tags": hard_logic_tags,
                "hard_logic_rules": hard_logic_rules,
                "natural_material_text": natural_material_text,
                "generation_mode": "question_service_sentence_order_consumption",
            }
        )
        if presentation_meta:
            prompt_extras.update(presentation_meta)
        source["prompt_extras"] = prompt_extras
        source["sentence_order_material_applied"] = True

        validator_contract = deepcopy(prepared_material.validator_contract or {})
        if not isinstance(validator_contract, dict):
            validator_contract = {}
        sentence_order_contract = dict(validator_contract.get("sentence_order") or {})
        structure_contract = dict(validator_contract.get("structure_constraints") or {})
        sentence_order_contract["sortable_unit_count"] = len(sortable_units)
        structure_contract["sortable_unit_count"] = len(sortable_units)
        if binding_pairs:
            sentence_order_contract["binding_pairs"] = [list(pair) for pair in binding_pairs]
        if sentence_roles:
            sentence_order_contract["sentence_roles"] = sentence_roles
        validator_contract["sentence_order"] = sentence_order_contract
        validator_contract["structure_constraints"] = structure_contract

        return prepared_material.model_copy(
            update={
                "text": natural_material_text,
                "source": source,
                "validator_contract": validator_contract,
                "text_refined": True,
                "refinement_reason": "sentence_order_consumption_ready",
            }
        )

    def _derive_sentence_order_binding_pairs(self, units: list[str]) -> list[tuple[int, int]]:
        pairs: list[tuple[int, int]] = []
        pronoun_starts = ("这", "这类", "这种", "这一", "其", "其中", "同时", "此外", "因此", "所以", "随后", "然后", "接着")
        current_problem_markers = ("问题在于", "难点在于", "关键在于", "困境在于", "为何")
        next_solution_markers = ("因此", "所以", "由此", "应该", "应当", "需要", "必须")
        for index in range(len(units) - 1):
            current = str(units[index] or "").strip()
            nxt = str(units[index + 1] or "").strip()
            if not current or not nxt:
                continue
            if nxt.startswith(pronoun_starts):
                pairs.append((index + 1, index + 2))
                continue
            if any(token in current for token in current_problem_markers) and any(token in nxt for token in next_solution_markers):
                pairs.append((index + 1, index + 2))
                continue
            if ("只有" in current and "才" in nxt) or ("如果" in current and any(token in nxt for token in ("那么", "就", "还要", "因此"))):
                pairs.append((index + 1, index + 2))
        deduped: list[tuple[int, int]] = []
        for pair in pairs:
            if pair not in deduped:
                deduped.append(pair)
        return deduped[:3]

    def _derive_sentence_order_roles(self, units: list[str]) -> dict[str, str]:
        roles: dict[str, str] = {}
        for index, unit in enumerate(units, start=1):
            role = self._infer_sentence_order_role_hint(unit, is_last=index == len(units))
            if role:
                roles[str(index)] = role
        return roles

    def _infer_sentence_order_role_hint(self, text: str, *, is_last: bool = False) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return ""
        conclusion_markers = ("因此", "所以", "由此", "可见", "总之", "综上")
        transition_markers = ("接着", "随后", "然后", "进一步", "在此基础上", "与此同时", "另一方面", "同时", "此外", "但是", "然而", "不过", "进而")
        thesis_markers = ("首先", "起初", "一开始", "第一", "问题在于", "关键在于", "要想", "对于", "面对")
        if any(candidate.startswith(marker) for marker in thesis_markers):
            return "thesis"
        if any(candidate.startswith(marker) for marker in conclusion_markers):
            return "conclusion" if is_last else "transition"
        if any(candidate.startswith(marker) for marker in transition_markers):
            return "transition"
        if is_last:
            return "conclusion"
        return ""

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
        source_label = normalize_readable_text(user_material.source_label or "user_uploaded_material")
        article_title = normalize_readable_text(user_material.title or request_snapshot.get("topic") or "用户自带材料")
        topic = normalize_readable_text(user_material.topic or request_snapshot.get("topic"))
        cleaned_text = self._clean_material_text(user_material.text or "")
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
                    text=cleaned_text,
                    original_text=cleaned_text,
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
        question_card = question_card_binding.get("question_card") or {}
        card_base_slots = deepcopy(question_card.get("base_slots") or {}) if isinstance(question_card, dict) else {}
        current_type_slots = deepcopy(updated_request.get("type_slots") or {})
        if isinstance(card_base_slots, dict):
            # candidate_type is a hydrated runtime hint for sentence_order, not a public type_slot.
            card_base_slots.pop("candidate_type", None)
            merged_type_slots = dict(card_base_slots)
            merged_type_slots.update(current_type_slots)
        else:
            merged_type_slots = current_type_slots

        if isinstance(question_card, dict):
            formal_runtime_spec = question_card.get("formal_runtime_spec") or {}
            if isinstance(formal_runtime_spec, dict):
                candidate_type = formal_runtime_spec.get("candidate_type")
                if candidate_type:
                    extra_constraints = deepcopy(updated_request.get("extra_constraints") or {})
                    extra_constraints.setdefault("runtime_candidate_type_hint", candidate_type)
                    updated_request["extra_constraints"] = extra_constraints

        updated_request["type_slots"] = merged_type_slots
        return updated_request

    def _apply_distill_runtime_overlay(
        self,
        *,
        build_request: PromptBuildRequest,
        built_item: dict[str, Any],
        material: MaterialSelectionResult,
    ) -> None:
        request_snapshot = built_item.get("request_snapshot") or {}
        question_card_binding = request_snapshot.get("question_card_binding") or {}
        question_card = question_card_binding.get("question_card") if isinstance(question_card_binding, dict) else {}
        overlay = self.distill_runtime_overlay.resolve(
            question_type=built_item.get("question_type"),
            business_subtype=built_item.get("business_subtype"),
            question_card=question_card if isinstance(question_card, dict) else {},
            material_source=material.source if isinstance(material.source, dict) else {},
            resolved_slots=built_item.get("resolved_slots"),
        )
        if not overlay:
            return

        current_resolved_slots = deepcopy(built_item.get("resolved_slots") or {})
        overlay_slot_defaults = dict(overlay.get("resolved_slot_defaults") or {})
        if overlay_slot_defaults:
            merged_resolved_slots = dict(overlay_slot_defaults)
            merged_resolved_slots.update(current_resolved_slots)
            built_item["resolved_slots"] = merged_resolved_slots

        control_logic = deepcopy(built_item.get("control_logic") or {})
        overlay_special_fields = dict(overlay.get("control_logic_special_fields") or {})
        current_special_fields = dict(control_logic.get("special_fields") or {})
        if overlay_special_fields:
            merged_special_fields = dict(overlay_special_fields)
            merged_special_fields.update(current_special_fields)
            control_logic["special_fields"] = merged_special_fields
            built_item["control_logic"] = control_logic

        if not isinstance(material.source, dict):
            material.source = {}
        prompt_extras = deepcopy(material.source.get("prompt_extras") or {}) if isinstance(material.source.get("prompt_extras"), dict) else {}
        prompt_extras.update(
            {
                "distill_runtime_overlay_mode": overlay.get("overlay_mode"),
                "distill_mother_family_id": overlay.get("mother_family_id"),
                "distill_child_family_id": overlay.get("child_family_id"),
                "distill_leaf_key": overlay.get("leaf_key"),
                "distill_prompt_guard_lines": list(overlay.get("prompt_guard_lines") or []),
            }
        )
        material.source["prompt_extras"] = prompt_extras
        material.source["distill_runtime_overlay"] = {
            "mother_family_id": overlay.get("mother_family_id"),
            "child_family_id": overlay.get("child_family_id"),
            "leaf_key": overlay.get("leaf_key"),
            "overlay_mode": overlay.get("overlay_mode"),
        }

        built_item["material_source"] = material.source
        built_item["material_selection"] = material.model_dump()
        built_item["distill_runtime_overlay"] = {
            "mother_family_id": overlay.get("mother_family_id"),
            "child_family_id": overlay.get("child_family_id"),
            "leaf_key": overlay.get("leaf_key"),
            "overlay_mode": overlay.get("overlay_mode"),
        }
        built_item["notes"] = built_item.get("notes", []) + [
            "distill_runtime_overlay_applied",
            f"distill_taxonomy::{overlay.get('mother_family_id') or 'unknown'}::{overlay.get('child_family_id') or 'unknown'}::{overlay.get('leaf_key') or 'unknown'}",
        ]

        request_snapshot = deepcopy(request_snapshot)
        request_snapshot["distill_runtime_overlay"] = built_item["distill_runtime_overlay"]
        built_item["request_snapshot"] = request_snapshot
        built_item["prompt_package"] = self._rebuild_prompt_package(
            build_request=build_request,
            built_item=built_item,
        )

    def _rebuild_prompt_package(
        self,
        *,
        build_request: PromptBuildRequest,
        built_item: dict[str, Any],
    ) -> dict[str, Any]:
        type_config = self.orchestrator.registry.get_type(built_item["question_type"])
        subtype_config = None
        business_subtype = built_item.get("business_subtype")
        if business_subtype:
            for candidate in type_config.business_subtypes:
                if candidate.subtype_id == business_subtype:
                    subtype_config = candidate
                    break
        pattern = self.orchestrator._get_pattern(type_config, built_item["selected_pattern"])
        return self.orchestrator.prompt_builder.build(
            question_type_config=type_config,
            business_subtype_config=subtype_config,
            pattern=pattern,
            difficulty_target=build_request.difficulty_target,
            resolved_slots=deepcopy(built_item.get("resolved_slots") or {}),
            skeleton=deepcopy(built_item.get("skeleton") or {}),
            control_logic=deepcopy(built_item.get("control_logic") or {}),
            generation_logic=deepcopy(built_item.get("generation_logic") or {}),
            topic=build_request.topic,
            count=build_request.count,
            passage_style=build_request.passage_style,
            use_fewshot=build_request.use_fewshot,
            fewshot_mode=build_request.fewshot_mode,
            extra_constraints=deepcopy(build_request.extra_constraints or {}),
        )

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
            if not fallback_item_id:
                fallback_item_id = f"fallback::{uuid4().hex}"
                fallback_item["item_id"] = fallback_item_id
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
            version_record = fallback_item.pop("_version_record", None)
            if version_record is not None:
                self.repository.save_version(version_record)
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
            question_type = str(snapshot.get("question_type") or "").strip()
            allowed_type_slot_keys = {str(key) for key in snapshot["type_slots"].keys()}
            allowed_type_slot_keys.update(MetaService.review_control_keys(question_type))
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
        remapped_analysis = self._synchronize_sentence_order_analysis_answer(remapped_analysis, target_answer)
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
        digit_groups = re.findall(r"\d+", raw)
        if not digit_groups:
            return []
        if len(digit_groups) == 1 and len(digit_groups[0]) > 1:
            return [int(ch) for ch in digit_groups[0]]
        return [int(match) for match in digit_groups]

    def _format_order_sequence(self, order: list[int]) -> str:
        circled_markers = {
            1: "①",
            2: "②",
            3: "③",
            4: "④",
            5: "⑤",
            6: "⑥",
            7: "⑦",
            8: "⑧",
            9: "⑨",
            10: "⑩",
        }
        if order and all(value in circled_markers for value in order):
            return "".join(circled_markers[value] for value in order)
        return "".join(str(value) for value in order)

    def _is_valid_sentence_order_sequence(self, sequence: list[int], unit_count: int) -> bool:
        return len(sequence) == unit_count and sorted(sequence) == list(range(1, unit_count + 1))

    def _extract_sequence_from_analysis(self, text: str, unit_count: int) -> list[int]:
        analysis = str(text or "").strip()
        if not analysis or unit_count not in {4, 5, 6}:
            return []
        circled_matches = re.findall(rf"[①②③④⑤⑥⑦⑧⑨⑩]{{{unit_count}}}", analysis)
        for match in reversed(circled_matches):
            sequence = self._extract_order_sequence(match)
            if self._is_valid_sentence_order_sequence(sequence, unit_count):
                return sequence
        anchored_patterns = [
            rf"正确顺序(?:应为|为|是)[:：]?\s*([1-{unit_count}]{{{unit_count}}})",
            rf"综合排序(?:应为|为|是)[:：]?\s*([1-{unit_count}]{{{unit_count}}})",
            rf"最优顺序(?:应为|为|是)[:：]?\s*([1-{unit_count}]{{{unit_count}}})",
            rf"排序(?:应为|为|是)[:：]?\s*([1-{unit_count}]{{{unit_count}}})",
        ]
        for pattern in anchored_patterns:
            match = re.search(pattern, analysis)
            if not match:
                continue
            sequence = [int(ch) for ch in match.group(1)]
            if self._is_valid_sentence_order_sequence(sequence, unit_count):
                return sequence
        candidates = re.findall(rf"(?<!\d)([1-{unit_count}]{{{unit_count}}})(?!\d)", analysis)
        for candidate in reversed(candidates):
            sequence = [int(ch) for ch in candidate]
            if self._is_valid_sentence_order_sequence(sequence, unit_count):
                return sequence
        return []

    def _default_sentence_order_answer(self, unit_count: int) -> list[int]:
        sequence = list(range(1, unit_count + 1))
        if unit_count >= 5:
            sequence[0], sequence[1] = sequence[1], sequence[0]
            sequence[-1], sequence[-2] = sequence[-2], sequence[-1]
        return sequence

    @staticmethod
    def _is_identity_sentence_order_sequence(sequence: list[int], unit_count: int) -> bool:
        return sequence == list(range(1, unit_count + 1))

    def _invert_sentence_order_display_sequence(self, display_sequence: list[int]) -> list[int]:
        if not display_sequence:
            return []
        position_map = {material_index: display_index for display_index, material_index in enumerate(display_sequence, start=1)}
        return [position_map[index] for index in range(1, len(display_sequence) + 1) if index in position_map]

    def _choose_sentence_order_display_sequence(
        self,
        *,
        unit_count: int,
        material_units: list[str],
        model_original_sentences: list[str],
        answer_sequence: list[int],
        analysis_sequence: list[int],
        existing_correct_order: list[int],
    ) -> tuple[list[int], str]:
        model_to_material_index_map = self._sentence_order_model_to_material_index_map(
            model_units=model_original_sentences,
            material_units=material_units,
        )
        if model_to_material_index_map:
            model_display_sequence = [model_to_material_index_map[index] for index in range(1, unit_count + 1)]
            if (
                self._is_valid_sentence_order_sequence(model_display_sequence, unit_count)
                and not self._is_identity_sentence_order_sequence(model_display_sequence, unit_count)
            ):
                return model_display_sequence, "model_original_sentences"

        for sequence, source_name in (
            (answer_sequence, "answer_option"),
            (analysis_sequence, "analysis_text"),
            (existing_correct_order, "existing_correct_order"),
        ):
            if (
                self._is_valid_sentence_order_sequence(sequence, unit_count)
                and not self._is_identity_sentence_order_sequence(sequence, unit_count)
            ):
                return list(sequence), source_name

        fallback_sequence = self._default_sentence_order_answer(unit_count)
        if self._is_valid_sentence_order_sequence(fallback_sequence, unit_count):
            return fallback_sequence, "default_display_shuffle"
        return list(range(1, unit_count + 1)), "identity_fallback"

    def _normalize_sentence_order_unit_text(self, text: str) -> str:
        raw = str(text or "").strip()
        raw = re.sub(r"^[①②③④⑤⑥⑦⑧⑨⑩\d]+\s*[.、．]?\s*", "", raw)
        raw = raw.strip("。．；;，,：:!！?？ ")
        return raw

    def _sentence_order_model_to_material_index_map(
        self,
        *,
        model_units: list[str],
        material_units: list[str],
    ) -> dict[int, int] | None:
        if len(model_units) != len(material_units):
            return None
        normalized_material_positions: dict[str, list[int]] = {}
        for index, sentence in enumerate(material_units, start=1):
            normalized = self._normalize_sentence_order_unit_text(sentence)
            if not normalized:
                return None
            normalized_material_positions.setdefault(normalized, []).append(index)

        mapping: dict[int, int] = {}
        for model_index, sentence in enumerate(model_units, start=1):
            normalized = self._normalize_sentence_order_unit_text(sentence)
            candidates = normalized_material_positions.get(normalized) or []
            if not candidates:
                return None
            mapping[model_index] = candidates.pop(0)
        return mapping if len(mapping) == len(model_units) else None

    def _remap_sentence_order_sequence(
        self,
        sequence: list[int],
        *,
        index_map: dict[int, int] | None,
        unit_count: int,
    ) -> tuple[list[int], bool]:
        if not self._is_valid_sentence_order_sequence(sequence, unit_count):
            return sequence, False
        if not index_map:
            return sequence, False
        remapped = [index_map.get(value, value) for value in sequence]
        if not self._is_valid_sentence_order_sequence(remapped, unit_count):
            return sequence, False
        return remapped, remapped != sequence

    @staticmethod
    def _sentence_order_index_map_is_identity(index_map: dict[int, int] | None) -> bool:
        if not index_map:
            return True
        return all(int(source) == int(target) for source, target in index_map.items())

    def _sentence_order_stem(self, unit_count: int) -> str:
        resolved_count = unit_count if unit_count in {4, 5, 6} else 6
        return f"将以下{resolved_count}个句子重新排列，语序正确的一项是："

    @staticmethod
    def _sentence_order_analysis_hint(text: str, *, limit: int = 20) -> str:
        clean = normalize_prompt_text(text or "").strip()
        clean = re.sub(r"[。！？!?；;]+$", "", clean)
        if len(clean) <= limit:
            return clean
        short = clean[:limit].rstrip("，、：:；; ")
        return f"{short}……"

    def _derive_sentence_order_options(self, correct_order: list[int], existing_options: dict[str, str]) -> dict[str, str]:
        sequences: list[list[int]] = []
        seen: set[tuple[int, ...]] = set()
        unit_count = len(correct_order)
        valid_sequence = list(range(1, unit_count + 1))

        def add_sequence(sequence: list[int]) -> None:
            key = tuple(sequence)
            if len(sequence) != unit_count or sorted(sequence) != valid_sequence or key in seen:
                return
            seen.add(key)
            sequences.append(sequence)

        add_sequence(correct_order)
        for value in existing_options.values():
            add_sequence(self._extract_order_sequence(value))

        fallback_variants: list[list[int]] = []
        for index in range(max(0, unit_count - 1)):
            variant = correct_order[:]
            variant[index], variant[index + 1] = variant[index + 1], variant[index]
            fallback_variants.append(variant)
        if unit_count >= 4:
            fallback_variants.append(correct_order[1:3] + correct_order[:1] + correct_order[3:])
            fallback_variants.append(correct_order[:-3] + correct_order[-2:] + correct_order[-3:-2])
        for variant in fallback_variants:
            add_sequence(variant)
            if len(sequences) >= 4:
                break

        while len(sequences) < 4:
            pivot = len(sequences)
            variant = correct_order[:]
            left = pivot % max(1, unit_count - 1)
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
        first_hint = self._sentence_order_analysis_hint(first_sentence)
        last_hint = self._sentence_order_analysis_hint(last_sentence)
        first_role = self._infer_sentence_order_role_hint(first_sentence)
        last_role = self._infer_sentence_order_role_hint(last_sentence, is_last=True)
        first_mismatch_letters = []
        tail_mismatch_letters = []
        for letter, value in (options or {}).items():
            seq = self._extract_order_sequence(value)
            if len(seq) != len(correct_order) or seq == correct_order:
                continue
            if seq and seq[0] != correct_order[0]:
                first_mismatch_letters.append(letter)
            if seq and seq[-1] != correct_order[-1]:
                tail_mismatch_letters.append(letter)
        pieces = []
        pieces.append(f"正确顺序为{order_text}。")
        if first_hint:
            first_hint_display = first_hint if any(mark in first_hint for mark in ("“", "”", "\"")) else f"“{first_hint}”"
            if first_role == "thesis":
                pieces.append(
                    f"先看首句，{self._format_order_sequence([correct_order[0]])}句先提出{first_hint_display}这一总领性判断，更适合作为全段起点。"
                )
            else:
                pieces.append(
                    f"先看首句，{self._format_order_sequence([correct_order[0]])}句以{first_hint_display}起笔，更适合作为全段起点。"
                )
            if first_mismatch_letters:
                pieces.append(f"据此可先排除{ '、'.join(first_mismatch_letters) }项中首句放置不当的组合。")
        if len(correct_order) >= 4:
            middle_text = self._format_order_sequence(correct_order[1:-1])
            pieces.append(f"中间部分按{middle_text}依次展开，前后承接、语意推进和局部照应都更顺。")
        if last_hint:
            last_hint_display = last_hint if any(mark in last_hint for mark in ("“", "”", "\"")) else f"“{last_hint}”"
            if last_role == "conclusion" and last_sentence.endswith(("?", "？")):
                pieces.append(
                    f"再看尾句，{self._format_order_sequence([correct_order[-1]])}句以{last_hint_display}设问收束，把前文讨论自然引向结尾。"
                )
            elif last_role == "conclusion":
                pieces.append(
                    f"再看尾句，{self._format_order_sequence([correct_order[-1]])}句以{last_hint_display}形成收束，更符合完整行文。"
                )
            else:
                pieces.append(
                    f"再看尾句，{self._format_order_sequence([correct_order[-1]])}句落在{last_hint_display}，能和前文形成自然照应。"
                )
            if tail_mismatch_letters:
                pieces.append(f"由此还能进一步排除{ '、'.join(tail_mismatch_letters) }项中尾句收束不当的排序。")
        pieces.append(f"综合来看，只有{answer}项与正确顺序 {order_text} 完全一致，因此答案为{answer}。")
        return "".join(pieces)

    def _synchronize_sentence_order_analysis_answer(self, analysis: str, answer: str) -> str:
        cleaned = str(analysis or "").strip()
        resolved_answer = str(answer or "").strip().upper()
        if not cleaned or resolved_answer not in {"A", "B", "C", "D"}:
            return cleaned
        cleaned = re.sub(r"故正确答案为\s*[A-D]\s*[。.]?$", f"故正确答案为{resolved_answer}。", cleaned)
        cleaned = re.sub(r"因此答案为\s*[A-D]\s*[。.]?$", f"因此答案为{resolved_answer}。", cleaned)
        return cleaned

    def build_sentence_order_question(self, question: GeneratedQuestion, *, material_text: str) -> GeneratedQuestion:
        extracted_units = self._extract_sortable_units_from_text(material_text)
        normalized_units = self._normalize_sentence_order_units_to_six(extracted_units) or extracted_units
        material_units = normalized_units[:]
        unit_count = len(material_units)
        if unit_count not in {4, 5, 6}:
            return question

        model_original_sentences = list(question.original_sentences or [])
        existing_correct_order = list(question.correct_order or [])
        answer = str(question.answer or "").strip().upper()
        answer_sequence = self._extract_order_sequence((question.options or {}).get(answer, ""))
        analysis_sequence = self._extract_sequence_from_analysis(question.analysis or "", unit_count)
        display_sequence, display_sequence_source = self._choose_sentence_order_display_sequence(
            unit_count=unit_count,
            material_units=material_units,
            model_original_sentences=model_original_sentences,
            answer_sequence=answer_sequence,
            analysis_sequence=analysis_sequence,
            existing_correct_order=existing_correct_order,
        )
        original_sentences = [material_units[index - 1] for index in display_sequence]
        correct_order = self._invert_sentence_order_display_sequence(display_sequence)
        rebuilt_options = self._derive_sentence_order_options(correct_order, question.options or {})
        rebuilt_answer = next(
            (letter for letter, value in rebuilt_options.items() if self._extract_order_sequence(value) == correct_order),
            "A",
        )
        rebuilt_analysis = self._build_sentence_order_analysis(correct_order, original_sentences, rebuilt_options, rebuilt_answer)
        rebuilt_analysis = self._synchronize_sentence_order_analysis_answer(rebuilt_analysis, rebuilt_answer)
        metadata = dict(question.metadata or {})
        metadata["sentence_order_recomputed"] = True
        metadata["sentence_order_truth_source"] = "material_original_order"
        metadata["sentence_order_display_sequence_source"] = display_sequence_source
        metadata["sentence_order_display_sequence"] = display_sequence
        metadata["sentence_order_analysis_source"] = "rebuilt"
        metadata["sentence_order_model_answer_sequence"] = answer_sequence
        metadata["sentence_order_model_analysis_sequence"] = analysis_sequence
        metadata["sentence_order_model_existing_correct_order"] = existing_correct_order
        return question.model_copy(
            update={
                "stem": self._sentence_order_stem(unit_count),
                "original_sentences": original_sentences,
                "correct_order": correct_order,
                "options": rebuilt_options,
                "answer": rebuilt_answer,
                "analysis": rebuilt_analysis,
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

        snapshot = request_snapshot or {}
        compliance_profile = self._material_compliance_profile(
            question_type=str(snapshot.get("question_type") or "").strip(),
            business_subtype=str(snapshot.get("business_subtype") or "").strip(),
        )
        question_type = str(snapshot.get("question_type") or "").strip()
        working_material_text = (
            self._sentence_fill_working_source_text(material)
            if question_type == "sentence_fill"
            else (material.text or "")
        )
        cleaned_seed = self._clean_material_text(working_material_text)
        initial_compliance = self._assess_material_compliance(cleaned_seed, compliance_profile)
        if question_type == "sentence_fill":
            prompt_extras = (
                deepcopy((material.source or {}).get("prompt_extras") or {})
                if isinstance((material.source or {}).get("prompt_extras"), dict)
                else {}
            )
            raw_context = self._clean_material_text(str(prompt_extras.get("context_window") or "") or cleaned_seed)
            raw_anchor = str(prompt_extras.get("answer_anchor_text") or "").strip()
            if raw_context and raw_anchor and self._sentence_fill_display_not_exam_like(
                fill_ready_material=raw_context,
                fill_ready_local_material=raw_context,
                answer_anchor_text=raw_anchor,
            ):
                issues = [str(item) for item in (initial_compliance.get("issues") or []) if str(item).strip()]
                if "exam_like_rewrite_required" not in issues:
                    issues.append("exam_like_rewrite_required")
                initial_compliance = {
                    **initial_compliance,
                    "passed": False,
                    "needs_llm_repair": True,
                    "score": max(0.0, float(initial_compliance.get("score") or 0.0) - 18.0),
                    "issues": issues,
                }
        refinement_mode = self._resolve_material_refinement_mode(
            request_snapshot=request_snapshot,
            material=material.model_copy(update={"text": cleaned_seed}),
        )
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
                        "material_compliance_profile": compliance_profile,
                        "material_compliance_report": initial_compliance,
                    },
                }
            )
        elif refinement_mode != str((material.source or {}).get("material_refinement_mode") or "").strip():
            refined_material = material.model_copy(
                update={
                    "source": {
                        **(material.source or {}),
                        "material_refinement_mode": refinement_mode,
                        "material_compliance_profile": compliance_profile,
                        "material_compliance_report": initial_compliance,
                    },
                }
            )

        base_text = refined_material.text
        if not base_text:
            return refined_material
        original_prompt_seed = (
            self._clean_material_text(material.original_text or "")
            or self._clean_material_text(material.text or "")
            or base_text
        )

        try:
            system_prompt, user_prompt = self._build_material_refinement_prompts(
                refinement_mode=refinement_mode,
                cleaned_seed=base_text,
                original_text=original_prompt_seed,
                request_snapshot=request_snapshot,
                compliance_profile=compliance_profile,
                compliance_report=initial_compliance,
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
            refined_compliance = self._assess_material_compliance(refined_text, compliance_profile)
            if refined_text and self._is_safe_material_refinement(
                original_text=base_text,
                refined_text=refined_text,
                refinement_mode=refinement_mode,
            ) and (refined.changed or refined_text != base_text):
                if refined_compliance["score"] + 5 < initial_compliance["score"] and not refined_compliance["passed"]:
                    return refined_material
                return refined_material.model_copy(
                    update={
                        "text": refined_text,
                        "text_refined": True,
                        "refinement_reason": refined.reason or refinement_mode,
                        "source": {
                            **(refined_material.source or {}),
                            "material_refinement_mode": refinement_mode,
                            "material_compliance_profile": compliance_profile,
                            "material_compliance_report": refined_compliance,
                        },
                    }
                )
        except Exception:  # noqa: BLE001
            return refined_material
        return refined_material

    @classmethod
    def _material_compliance_profile(
        cls,
        *,
        question_type: str,
        business_subtype: str | None = None,
    ) -> dict[str, Any]:
        subtype = str(business_subtype or "").strip()
        if question_type == "sentence_fill":
            return {
                "family_label": "sentence_fill",
                "min_chars": 120,
                "max_chars": 520,
                "max_paragraphs": 3,
                "forbid_blank_markers": True,
                "forbid_outline_style": True,
                "forbid_list_heavy": True,
                "requirements": [
                    "连续自然段，不要条目体、目录体、知识点提纲体",
                    "空位前后要像同一语义链中的正文句",
                    "不要把小标题短语或编号项当成可挖空句",
                    "材料至少应提供自然前后文，不要只剩一个硬桥接句槽位",
                    "正确项展示应像自然填入的一句或分句，不应把整段题面主干原样搬进选项",
                ],
                "leaf_structure_policy": "如果空位的叶族功能不够明显，可以主动补足必要上下文、重写局部表达、把硬槽位修成自然挖口，让材料和空位都更像真题。",
                "repair_permissions": [
                    "允许把资讯摘录、手册条目、碎标题、评论切片重写成连续正文",
                    "允许补足必要上下文，让空位前后角色更清楚、更像真题段落",
                    "允许把过短、过硬、像机械槽位的局部改写成自然挖口，但必须保持原句合法性",
                    "允许把不自然的整句挖空改写成更像真题的合法分句挖空展示",
                ],
                "hard_guards": [
                    "不得新增或保留任何空位标记",
                    "不得改变 answer_anchor 对应原句的事实、对象和判断方向",
                    "不得把旧 blanked_text 或旧展示态材料当成新的原始材料继续加工",
                ],
            }
        if question_type == "sentence_order":
            return {
                "family_label": "sentence_order",
                "min_chars": 120,
                "max_chars": 560,
                "max_paragraphs": 3,
                "forbid_blank_markers": True,
                "forbid_outline_style": True,
                "forbid_list_heavy": True,
                "requirements": [
                    "句群都应来自正文，不是标题项、目录项、提示项",
                    "句子数量适中，能自然拆成排序展示单元",
                    "保留首尾锚点，不要是堆砌式清单",
                ],
                "leaf_structure_policy": "如果排序叶族结构不够明显，可以增强句群自足性、削弱列表感、补足最小承接，但必须保持顺序真值不变。",
                "repair_permissions": [
                    "允许去掉列表感、碎标题感和资讯摘录腔",
                    "允许增强句内自足性和局部承接，让句群更像正文",
                    "允许轻微规范标点和表达，使首尾锚点更清楚",
                ],
                "hard_guards": [
                    "不得增删句子，不得合并或拆分 sortable units",
                    "不得改动原始句序真值，不得重排内容",
                    "不得把句群改写成新的论证结构或新的中心结论",
                ],
            }
        if question_type == "main_idea" and subtype == "center_understanding":
            return {
                "family_label": "center_understanding",
                "min_chars": 180,
                "max_chars": 900,
                "max_paragraphs": 4,
                "forbid_blank_markers": True,
                "forbid_outline_style": True,
                "forbid_list_heavy": True,
                "requirements": [
                    "像独立可读的自然议论或说明文段",
                    "不要是关键词清单、政策标题串、会议模板稿",
                    "主旨链完整，能支撑概括而非只剩局部条目",
                ],
                "leaf_structure_policy": "如果中心链不够显、段内像新闻通稿或碎信息堆叠，可以主动整理成更像自然议论/说明文段的主旨链。",
                "repair_permissions": [
                    "允许压掉通稿壳、标题壳、会议模板壳",
                    "允许重写局部句子衔接，让中心论证链更清楚",
                    "允许把碎信息整理成自然正文，但不得改主旨",
                ],
                "hard_guards": [
                    "不得新增新的立场、结论和论证终点",
                    "不得把局部事实改造成新的中心判断",
                    "不得凭空补背景或引入材料中没有的政策任务",
                ],
            }
        return {
            "family_label": question_type or "general",
            "min_chars": 120,
            "max_chars": 720,
            "max_paragraphs": 4,
            "forbid_blank_markers": True,
            "forbid_outline_style": True,
            "forbid_list_heavy": True,
            "requirements": [
                "尽量整理成自然段材料，不要保留条目体和模板体",
            ],
            "leaf_structure_policy": "如果材料局部角色和结构不够明显，可以适度增强自然衔接与自足性，但不得改变证据基础。",
            "repair_permissions": [
                "允许删除模板标签、标题和编号",
                "允许补足最小必要衔接并整理成自然正文",
            ],
            "hard_guards": [
                "不得新增事实、立场和论证终点",
                "不得把材料改写成另一篇文章",
            ],
        }

    def _normalize_material_for_family_shape(self, text: str, profile: dict[str, Any]) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        lines = [line.strip() for line in normalized.split("\n")]
        cleaned_lines: list[str] = []
        for line in lines:
            if not line:
                cleaned_lines.append("")
                continue
            stripped_line = self._strip_sentence_fill_unit_prefix(line)
            if profile.get("forbid_outline_style") and self._is_sentence_fill_structural_heading_unit(line):
                continue
            cleaned_lines.append(stripped_line or line)
        normalized = "\n".join(cleaned_lines)
        normalized = re.sub(r"(?:\n\s*){3,}", "\n\n", normalized).strip()
        return normalized

    @classmethod
    def _assess_material_compliance(cls, text: str, profile: dict[str, Any]) -> dict[str, Any]:
        clean = str(text or "").strip()
        issues: list[str] = []
        if not clean:
            return {
                "passed": False,
                "score": 0.0,
                "issues": ["empty_material"],
                "needs_llm_repair": False,
            }
        visible = re.sub(r"\s+", "", clean)
        visible_length = len(visible)
        min_chars = int(profile.get("min_chars") or 0)
        max_chars = int(profile.get("max_chars") or 0)
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", clean) if part.strip()]
        lines = [line.strip() for line in clean.splitlines() if line.strip()]
        sentence_units = [part.strip() for part in re.split(r"(?<=[。！？!?；;])\s*", clean) if part.strip()]
        family_label = str(profile.get("family_label") or "").strip()
        heading_like_lines = sum(1 for line in lines if cls._is_sentence_fill_structural_heading_unit(line))
        list_like_lines = sum(
            1
            for line in lines
            if re.match(r"^\s*(?:[-•·*]|[(（]?\d+[)）]?|[一二三四五六七八九十百千万]+[、.．])", line)
        )
        duplicate_sentence_count = 0
        seen_sentence_signatures: list[str] = []
        for unit in sentence_units:
            signature = re.sub(r"\s+", "", unit)
            if len(signature) < 8:
                continue
            if any(
                signature == existing
                or signature in existing
                or existing in signature
                or SequenceMatcher(None, signature, existing).ratio() >= 0.95
                for existing in seen_sentence_signatures
            ):
                duplicate_sentence_count += 1
                continue
            seen_sentence_signatures.append(signature)
        blank_markers = any(token in clean for token in ("[BLANK]", "____", "___"))
        if min_chars and visible_length < min_chars:
            issues.append(f"too_short:{visible_length}")
        if max_chars and visible_length > max_chars:
            issues.append(f"too_long:{visible_length}")
        if profile.get("forbid_blank_markers") and blank_markers:
            issues.append("contains_blank_markers")
        if profile.get("forbid_outline_style") and heading_like_lines:
            issues.append(f"outline_heading_lines:{heading_like_lines}")
        if profile.get("forbid_list_heavy") and list_like_lines >= max(2, len(lines) // 2 if lines else 2):
            issues.append(f"list_heavy:{list_like_lines}")
        if duplicate_sentence_count:
            issues.append(f"duplicate_sentences:{duplicate_sentence_count}")
        max_paragraphs = int(profile.get("max_paragraphs") or 0)
        if max_paragraphs and len(paragraphs) > max_paragraphs:
            issues.append(f"too_many_paragraphs:{len(paragraphs)}")
        if cls._looks_like_fragmentary_material_opening(clean, sentence_units):
            issues.append("fragmentary_opening")
        if family_label == "sentence_fill" and len(sentence_units) < 2:
            issues.append(f"weak_local_chain:{len(sentence_units)}")
        if family_label == "sentence_order" and len(sentence_units) < 4:
            issues.append(f"weak_sortable_chain:{len(sentence_units)}")
        if family_label == "center_understanding" and len(sentence_units) < 3:
            issues.append(f"thin_argument_chain:{len(sentence_units)}")

        score = 100.0
        score -= 35.0 if any(issue.startswith("too_short") for issue in issues) else 0.0
        score -= 20.0 if any(issue.startswith("too_long") for issue in issues) else 0.0
        score -= 25.0 if any(issue.startswith("outline_heading_lines") for issue in issues) else 0.0
        score -= 15.0 if any(issue.startswith("list_heavy") for issue in issues) else 0.0
        score -= 15.0 if any(issue.startswith("duplicate_sentences") for issue in issues) else 0.0
        score -= 20.0 if "contains_blank_markers" in issues else 0.0
        score -= 10.0 if any(issue.startswith("too_many_paragraphs") for issue in issues) else 0.0
        score -= 15.0 if "fragmentary_opening" in issues else 0.0
        score -= 10.0 if any(issue.startswith("weak_local_chain") for issue in issues) else 0.0
        score -= 10.0 if any(issue.startswith("weak_sortable_chain") for issue in issues) else 0.0
        score -= 8.0 if any(issue.startswith("thin_argument_chain") for issue in issues) else 0.0
        needs_llm_repair = any(
            issue.startswith(prefix)
            for issue in issues
            for prefix in (
                "too_short",
                "too_long",
                "outline_heading_lines",
                "list_heavy",
                "duplicate_sentences",
                "too_many_paragraphs",
                "fragmentary_opening",
                "weak_local_chain",
                "weak_sortable_chain",
                "thin_argument_chain",
            )
        )
        return {
            "passed": not issues,
            "score": max(0.0, score),
            "issues": issues,
            "needs_llm_repair": needs_llm_repair,
            "visible_length": visible_length,
            "paragraph_count": len(paragraphs),
            "line_count": len(lines),
            "sentence_count": len(sentence_units),
        }

    @staticmethod
    def _looks_like_fragmentary_material_opening(text: str, sentence_units: list[str]) -> bool:
        clean = str(text or "").strip()
        if not clean:
            return False
        first_sentence = str((sentence_units or [clean])[0] or "").strip()
        compact = re.sub(r"\s+", "", first_sentence)
        if not compact:
            return False
        fragment_tokens = (
            "年",
            "月",
            "同比",
            "环比",
            "其中",
            "因此",
            "所以",
            "同时",
            "另外",
            "此外",
            "不过",
            "然而",
            "一方面",
            "另一方面",
            "并且",
            "而",
            "但",
        )
        if compact[:1] in {"年", "月"} and len(compact) <= 24:
            return True
        if any(compact.startswith(token) for token in fragment_tokens) and len(compact) <= 28:
            return True
        return False

    @staticmethod
    def _is_safe_material_refinement(
        *,
        original_text: str,
        refined_text: str,
        refinement_mode: str,
    ) -> bool:
        original_clean = str(original_text or "").strip()
        refined_clean = str(refined_text or "").strip()
        if not original_clean or not refined_clean:
            return False
        if any(token in refined_clean for token in ("[BLANK]", "____", "___")):
            return False

        original_visible = re.sub(r"\s+", "", original_clean)
        refined_visible = re.sub(r"\s+", "", refined_clean)
        if not original_visible or not refined_visible:
            return False

        length_ratio = len(refined_visible) / max(1, len(original_visible))
        if length_ratio < 0.35 or length_ratio > 4.0:
            return False

        similarity = SequenceMatcher(None, original_visible[:4000], refined_visible[:4000]).ratio()
        if refinement_mode == QuestionGenerationService.MATERIAL_REFINEMENT_FAMILY_COMPLIANCE:
            if similarity < 0.08:
                return False
        elif similarity < 0.26:
            return False

        original_numbers = {item for item in re.findall(r"\d+(?:\.\d+)?%?", original_clean) if item}
        if 0 < len(original_numbers) <= 4:
            matched_count = sum(1 for item in original_numbers if item in refined_clean)
            if matched_count == 0:
                return False

        return True

    def _clean_material_text(self, text: str) -> str:
        normalized = normalize_readable_text(text or "")
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        if not normalized:
            return ""

        normalized = self._strip_material_caption_noise(normalized)
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

    def _strip_material_caption_noise(self, text: str) -> str:
        cleaned = str(text or "")
        cleaned = re.sub(r"[（(]\s*审核[^)）\n]*[)）]\s*", "", cleaned)
        cleaned = re.sub(r"[（(]\s*审校[^)）\n]*[)）]\s*", "", cleaned)
        cleaned = re.sub(r"(?:新华社|记者)[^。\n]{0,24}[摄图]", "", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        cleaned = re.sub(r"(?:\n\s*){3,}", "\n\n", cleaned)
        return cleaned.strip()

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
        return lines

    def _legacy_reference_hard_constraint_residuals(self) -> list[str]:
        return []

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
            self._requested_taxonomy_bridge_hints(
                question_type=item["question_type"],
                type_slots=request_snapshot.get("type_slots"),
                extra_constraints=request_snapshot.get("extra_constraints"),
            ),
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
        sections.extend(self._build_round1_fewshot_sections(prompt_package=prompt_package))
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
        distill_lines = self._distill_overlay_prompt_lines(material_prompt_extras)
        if "sortable_units" in material_prompt_extras:
            units = [str(unit or "").strip() for unit in (material_prompt_extras.get("sortable_units") or []) if str(unit or "").strip()]
            binding_pairs = material_prompt_extras.get("binding_pairs") or []
            sentence_roles = material_prompt_extras.get("sentence_roles") or {}
            hard_logic_leaf_key = str(material_prompt_extras.get("hard_logic_leaf_key") or "").strip()
            hard_logic_tags = [str(tag).strip() for tag in (material_prompt_extras.get("hard_logic_tags") or []) if str(tag).strip()]
            hard_logic_rules = [str(rule).strip() for rule in (material_prompt_extras.get("hard_logic_rules") or []) if str(rule).strip()]
            unit_count = int(material_prompt_extras.get("sortable_unit_count") or len(units) or 0)
            lines = [
                f"以下 {unit_count or len(units) or 0} 个展示单元就是最终给考生看的排序材料，不要另造新句，也不要脱离这些展示单元重写链条。",
            ]
            if hard_logic_leaf_key:
                lines.append(f"当前叶子硬逻辑标签：{hard_logic_leaf_key}")
            if hard_logic_tags:
                lines.append(f"硬逻辑标记：{self._json_dump_prompt_value(hard_logic_tags)}")
            if units:
                lines.append("固定展示单元：")
                lines.extend([f"{index}. {unit}" for index, unit in enumerate(units, start=1)])
            head_anchor = str(material_prompt_extras.get("head_anchor_text") or "").strip()
            tail_anchor = str(material_prompt_extras.get("tail_anchor_text") or "").strip()
            if head_anchor:
                lines.append(f"优先核查首句是否能由以下开头锚点承担：{head_anchor}")
            if tail_anchor:
                lines.append(f"优先核查尾句是否能由以下收束锚点承担：{tail_anchor}")
            if binding_pairs:
                lines.append(f"重点观察的局部捆绑对：{self._json_dump_prompt_value(binding_pairs)}")
            if sentence_roles:
                lines.append(f"展示单元角色提示：{self._json_dump_prompt_value(sentence_roles)}")
            if hard_logic_rules:
                lines.append("生成时必须同时遵守以下叶子硬逻辑：")
                lines.extend([f"- {rule}" for rule in hard_logic_rules])
            lines.append("错项优先做近邻错序、局部捆绑拆错、首尾误配，不要随机乱排。")
            lines.extend(distill_lines)
            return [normalize_prompt_text(line) for line in lines if str(line or "").strip()]

        if "fill_ready_material" in material_prompt_extras:
            lines = []
            fill_ready_material = str(material_prompt_extras.get("fill_ready_material") or "").strip()
            fill_ready_local_material = str(material_prompt_extras.get("fill_ready_local_material") or "").strip()
            answer_anchor_text = str(material_prompt_extras.get("answer_anchor_text") or "").strip()
            hard_logic_leaf_key = str(material_prompt_extras.get("hard_logic_leaf_key") or "").strip()
            hard_logic_tags = [str(tag).strip() for tag in (material_prompt_extras.get("hard_logic_tags") or []) if str(tag).strip()]
            hard_logic_rules = [str(rule).strip() for rule in (material_prompt_extras.get("hard_logic_rules") or []) if str(rule).strip()]
            if fill_ready_material:
                lines.append(f"最终呈现材料：{fill_ready_material}")
            if fill_ready_local_material:
                lines.append(f"局部空位窗口：{fill_ready_local_material}")
            if answer_anchor_text:
                lines.append(f"被挖原句（正确答案必须回填此句）：{answer_anchor_text}")
            if hard_logic_leaf_key:
                lines.append(f"当前叶子硬逻辑标签：{hard_logic_leaf_key}")
            if hard_logic_tags:
                lines.append(f"硬逻辑标记：{self._json_dump_prompt_value(hard_logic_tags)}")
            if bool(material_prompt_extras.get("require_original_answer_sentence")):
                lines.append("硬约束：sentence_fill 正确答案必须是原文被挖掉的句子，不允许另写替代句。")
            blank_position = str(material_prompt_extras.get("blank_position") or "").strip()
            function_type = str(material_prompt_extras.get("function_type") or "").strip()
            logic_relation = str(material_prompt_extras.get("logic_relation") or "").strip()
            if blank_position or function_type or logic_relation:
                lines.append(
                    f"空位约束：blank_position={blank_position or 'unknown'}; "
                    f"function_type={function_type or 'unknown'}; logic_relation={logic_relation or 'unknown'}"
                )
            preferred_answer_shape = str(material_prompt_extras.get("preferred_answer_shape") or "").strip()
            forbidden_answer_styles = material_prompt_extras.get("forbidden_answer_styles") or []
            if preferred_answer_shape:
                lines.append(f"正确项形态优先：{preferred_answer_shape}")
            if forbidden_answer_styles:
                lines.append(f"禁止正确项风格：{self._json_dump_prompt_value(forbidden_answer_styles)}")
            if hard_logic_rules:
                lines.append("生成时必须同时遵守以下叶子硬逻辑：")
                lines.extend([f"- {rule}" for rule in hard_logic_rules])
            lines.extend(distill_lines)
            return [normalize_prompt_text(line) for line in lines if str(line or "").strip()]

        if distill_lines:
            return [normalize_prompt_text(line) for line in [*distill_lines, self._json_dump_prompt_value(material_prompt_extras)] if str(line or "").strip()]
        return [normalize_prompt_text(self._json_dump_prompt_value(material_prompt_extras))]

    def _distill_overlay_prompt_lines(self, material_prompt_extras: dict[str, object]) -> list[str]:
        mother_family_id = str(material_prompt_extras.get("distill_mother_family_id") or "").strip()
        child_family_id = str(material_prompt_extras.get("distill_child_family_id") or "").strip()
        leaf_key = str(material_prompt_extras.get("distill_leaf_key") or "").strip()
        overlay_mode = str(material_prompt_extras.get("distill_runtime_overlay_mode") or "").strip()
        prompt_guard_lines = [
            str(line).strip()
            for line in (material_prompt_extras.get("distill_prompt_guard_lines") or [])
            if str(line).strip()
        ]
        if not any([mother_family_id, child_family_id, leaf_key, overlay_mode, prompt_guard_lines]):
            return []
        lines: list[str] = []
        if overlay_mode:
            lines.append(f"蒸馏控制模式：{overlay_mode}")
        lines.append(
            "当前蒸馏归属："
            f"mother={mother_family_id or 'unknown'}; "
            f"child={child_family_id or 'unknown'}; "
            f"leaf={leaf_key or 'unknown'}"
        )
        if prompt_guard_lines:
            lines.append("除叶子硬逻辑外，还需补足以下母/子族共性：")
            lines.extend([f"- {line}" for line in prompt_guard_lines])
        return lines

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
        normalized_source_question_analysis = deepcopy(source_question_analysis)
        normalized_source_question_analysis["structure_constraints"] = normalize_sentence_fill_constraints(
            normalized_source_question_analysis.get("structure_constraints") or {}
        )
        normalized_source_question_analysis["retrieval_structure_constraints"] = normalize_sentence_fill_constraints(
            normalized_source_question_analysis.get("retrieval_structure_constraints") or {}
        )
        structure_constraints = normalized_source_question_analysis.get("structure_constraints") or {}
        return {
            "reference_payload": self._prepare_reference_prompt_payload(source_question),
            "source_question_analysis": normalized_source_question_analysis,
            "hard_constraints": self._build_reference_hard_constraints(
                question_type=question_type,
                structure_constraints=structure_constraints,
                question_card_binding=question_card_binding,
            ),
        }

    def _prepare_reference_prompt_payload(self, source_question: dict[str, object]) -> dict[str, object]:
        return normalize_reference_payload(source_question, passage_limit=600, omit_analysis=True)

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

    def _build_round1_fewshot_sections(self, *, prompt_package: dict[str, Any]) -> list[str]:
        fewshot_block = str(prompt_package.get("fewshot_text_block") or "").strip()
        if not fewshot_block or fewshot_block == "None":
            return []
        return self._make_prompt_section("round1_fewshot_asset", [fewshot_block])

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
            question_card_binding=question_card_binding,
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
        question_card_binding: dict[str, object] | None = None,
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
            answer_grounding_asset_family = self._resolve_main_idea_answer_grounding_asset_family(
                main_idea_subtype=main_idea_subtype,
                question_card_binding=question_card_binding,
            )
            if formal_facts.get("require_central_meaning_alignment"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        answer_grounding_asset_family,
                        "require_central_meaning_alignment_template",
                    )
                )
            if formal_facts.get("disallow_detail_as_correct_answer"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        answer_grounding_asset_family,
                        "disallow_detail_as_correct_answer_template",
                    )
                )
            if formal_facts.get("disallow_stronger_conclusion"):
                lines.append(
                    self._prompt_asset_text(
                        "answer_grounding",
                        answer_grounding_asset_family,
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

    @staticmethod
    def _resolve_main_idea_answer_grounding_asset_family(
        *,
        main_idea_subtype: str,
        question_card_binding: dict[str, object] | None = None,
    ) -> str:
        question_card = ((question_card_binding or {}).get("question_card") or {}) if isinstance(question_card_binding, dict) else {}
        compatibility_backbone = question_card.get("compatibility_backbone") or {}
        configured_family = str(compatibility_backbone.get("answer_grounding_asset_family_id") or "").strip()
        if configured_family:
            return configured_family
        if main_idea_subtype == "title_selection":
            return "title_selection"
        return "main_idea"

    def _answer_grounding_residuals(
        self,
        *,
        question_type: str | None,
        question_card_binding: dict[str, object] | None = None,
        formal_facts: dict[str, object] | None = None,
    ) -> list[str]:
        return []

    def _apply_consistency_hard_fail_gate(self, *, validation_result) -> list[str]:
        if validation_result is None:
            return []
        checks = getattr(validation_result, "checks", None) or {}
        existing_errors = self._normalize_error_list(getattr(validation_result, "errors", None))
        triggered: list[str] = []
        for check_name in self.CONSISTENCY_HARD_FAIL_CHECKS:
            payload = checks.get(check_name) or {}
            if isinstance(payload, dict) and payload.get("passed") is False:
                error_code = f"consistency_hard_fail::{check_name}"
                if error_code not in existing_errors:
                    existing_errors.append(error_code)
                triggered.append(error_code)
        if not triggered:
            return []
        validation_result.errors = existing_errors
        validation_result.passed = False
        validation_result.validation_status = "failed"
        validation_result.next_review_status = "needs_revision"
        validation_result.score = min(int(validation_result.score or 100), 60)
        return triggered

    @staticmethod
    def _normalize_error_list(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, (tuple, set)):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        try:
            return [str(item) for item in list(value) if str(item).strip()]  # type: ignore[arg-type]
        except TypeError:
            text = str(value).strip()
            return [text] if text and text != "None" else []

    def _apply_evaluation_gate(
        self,
        *,
        validation_result,
        evaluation_result: dict[str, object] | None,
        difficulty_target: str,
    ) -> list[str]:
        if not evaluation_result:
            return []
        if str(evaluation_result.get("status") or "").strip().lower() == "skipped":
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

        existing_warnings = list(getattr(validation_result, "warnings", None) or [])
        for error in errors:
            advisory = f"judge_signal::{error}"
            if advisory not in existing_warnings:
                existing_warnings.append(advisory)
        validation_result.warnings = existing_warnings
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


    def _should_retry_alignment(self, validation_result, source_question_analysis: dict | None) -> bool:
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
            "analysis_answer_consistency",
            "sentence_fill_anchor_grounding",
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
        if not bool(self.runtime_config.evaluation.judge.enabled):
            return False
        question_type = str((source_question_analysis.get("style_summary") or {}).get("question_type") or "")
        targeted_candidate = self._has_targeted_repair_candidate(
            question_type=question_type,
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=quality_gate_errors,
            source_question_analysis=source_question_analysis,
        )
        if self._should_use_compact_main_idea_path(source_question_analysis) and not self._has_targeted_repair_candidate(
            question_type=question_type,
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=quality_gate_errors,
            source_question_analysis=source_question_analysis,
        ):
            return False
        if quality_gate_errors:
            return False
        if validation_result is not None and not validation_result.passed:
            return targeted_candidate
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
            "structure_constraints": normalize_sentence_fill_constraints(dict(retrieval_structure_constraints or {})),
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

    @classmethod
    def _requested_taxonomy_bridge_hints(
        cls,
        *,
        question_type: str,
        type_slots: dict[str, Any] | None,
        extra_constraints: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_question_type = str(question_type or "").strip()
        normalized_type_slots = dict(type_slots or {})
        normalized_extra_constraints = dict(extra_constraints or {})
        requested_cards = [
            str(card_id).strip()
            for card_id in (normalized_extra_constraints.get("reference_business_cards") or [])
            if str(card_id).strip()
        ]
        requested_terms = [
            str(term).strip()
            for term in (normalized_extra_constraints.get("reference_query_terms") or [])
            if str(term).strip()
        ]
        structure_constraints: dict[str, Any] = {}
        if normalized_question_type == "sentence_fill":
            for key in (
                "blank_position",
                "function_type",
                "logic_relation",
                "context_dependency",
                "bidirectional_validation",
                "reference_dependency",
            ):
                value = normalized_type_slots.get(key)
                if value is not None and value != "" and value != []:
                    structure_constraints[key] = value
        elif normalized_question_type == "sentence_order":
            for key in (
                "opening_anchor_type",
                "middle_structure_type",
                "closing_anchor_type",
                "block_order_complexity",
                "distractor_modes",
                "distractor_strength",
            ):
                value = normalized_type_slots.get(key)
                if value is not None and value != "" and value != []:
                    structure_constraints[key] = value
        elif normalized_question_type == "main_idea":
            for key in (
                "structure_type",
                "main_point_source",
                "abstraction_level",
                "statement_visibility",
                "main_axis_source",
            ):
                value = normalized_type_slots.get(key)
                if value is not None and value != "" and value != []:
                    structure_constraints[key] = value
        return {
            "business_card_ids": list(requested_cards),
            "preferred_business_card_ids": list(requested_cards),
            "query_terms": requested_terms,
            "structure_constraints": structure_constraints,
        }

    @staticmethod
    def _merge_material_bridge_hints(base: dict[str, Any], *overrides: dict[str, Any]) -> dict[str, Any]:
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

        merged = {
            "business_card_ids": list((base or {}).get("business_card_ids") or []),
            "preferred_business_card_ids": list((base or {}).get("preferred_business_card_ids") or []),
            "query_terms": list((base or {}).get("query_terms") or []),
            "structure_constraints": dict((base or {}).get("structure_constraints") or {}),
        }
        for override in overrides:
            current = override or {}
            merged["business_card_ids"] = _merge_list(
                merged["business_card_ids"],
                current.get("business_card_ids") or [],
            )
            merged["preferred_business_card_ids"] = _merge_list(
                current.get("preferred_business_card_ids") or [],
                merged["preferred_business_card_ids"],
            )
            merged["query_terms"] = _merge_list(
                merged["query_terms"],
                current.get("query_terms") or [],
            )
            merged["structure_constraints"] = {
                **dict(merged["structure_constraints"] or {}),
                **dict(current.get("structure_constraints") or {}),
            }
        return merged

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
        cleaned_lines = [normalize_prompt_text(line) for line in lines if str(line or "").strip()]
        return [self._section_label(section_key), *cleaned_lines]

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
        return [normalize_prompt_text(self._json_dump_prompt_value(reference_payload))]

    def _build_reference_question_analysis_sections(self, *, source_question_analysis: dict) -> list[str]:
        return self._make_prompt_section(
            "reference_question_analysis",
            self._reference_question_analysis_lines(source_question_analysis),
        )

    def _reference_question_analysis_lines(self, source_question_analysis: dict) -> list[str]:
        return [normalize_prompt_text(self._json_dump_prompt_value(source_question_analysis))]

    def _reference_guidance_lines(self) -> list[str]:
        return self._prompt_asset_lines("reference_guidance")

    @staticmethod
    def _json_dump_prompt_value(value: Any) -> str:
        return json.dumps(normalize_readable_structure(value), ensure_ascii=False)

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
        return [normalize_prompt_text(item) for item in node]

    def _prompt_asset_text(self, *path: str) -> str:
        node = self._prompt_asset_node(*path)
        if isinstance(node, list):
            raise DomainError(
                "Prompt asset path does not resolve to a text value.",
                status_code=500,
                details={"path": ".".join(path)},
            )
        return normalize_prompt_text(node)

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
        exact_matches = sorted(exact_matches, key=self._sentence_order_candidate_priority_key, reverse=True)
        sufficient_matches = sorted(sufficient_matches, key=self._sentence_order_candidate_priority_key, reverse=True)
        near_matches = sorted(near_matches, key=self._sentence_order_candidate_priority_key, reverse=True)
        if exact_matches:
            return exact_matches + sufficient_matches + near_matches + others
        if sufficient_matches:
            return sufficient_matches + near_matches + others
        return near_matches + others

    def _sentence_order_candidate_priority_key(self, material: MaterialSelectionResult) -> tuple[float, ...]:
        source_text = material.original_text or material.text or ""
        units = self._extract_sortable_units_from_text(source_text)
        normalized_units = self._normalize_sentence_order_units_to_six(units) or units
        if len(normalized_units) not in {4, 5, 6}:
            normalized_units = units
        cleaned_units = [self._clean_sentence_order_sortable_unit(unit) for unit in normalized_units if self._clean_sentence_order_sortable_unit(unit)]
        binding_pair_count = len(self._derive_sentence_order_binding_pairs(cleaned_units))
        head_role = self._infer_sentence_order_role_hint(cleaned_units[0]) if cleaned_units else ""
        tail_role = self._infer_sentence_order_role_hint(cleaned_units[-1], is_last=True) if cleaned_units else ""
        dependent_opening_penalty = 1.0 if cleaned_units and self._sentence_order_unit_has_dependent_opening(cleaned_units[0]) else 0.0
        prompt_extras = ((material.source or {}).get("prompt_extras") or {}) if isinstance(material.source, dict) else {}
        scoring = ((material.source or {}).get("scoring") or {}) if isinstance(material.source, dict) else {}
        readability = 1.0 - float((material.local_profile or {}).get("context_dependency") or 0.0)
        readiness = float(scoring.get("readiness_score") or 0.0)
        quality = float(material.quality_score or 0.0)
        has_refined_units = 1.0 if prompt_extras.get("sortable_units") else 0.0
        question_tail = 1.0 if cleaned_units and cleaned_units[-1].endswith(("?", "？")) else 0.0
        return (
            float(binding_pair_count),
            1.0 if head_role == "thesis" else 0.0,
            1.0 if tail_role == "conclusion" else 0.0,
            question_tail,
            has_refined_units,
            readability,
            readiness,
            quality,
            -dependent_opening_penalty,
        )

    def _sentence_order_target_unit_count(self, source_question_analysis: dict | None) -> int | None:
        structure_constraints = (source_question_analysis or {}).get("structure_constraints") or {}
        candidate_values = [
            structure_constraints.get("sortable_unit_count"),
            (source_question_analysis or {}).get("sortable_unit_count"),
        ]
        for raw_value in candidate_values:
            try:
                parsed_value = int(raw_value)
            except (TypeError, ValueError):
                continue
            if parsed_value in {4, 5, 6}:
                return parsed_value
        return None

    def _extract_sortable_units_from_text(self, text: str) -> list[str]:
        raw = (text or "").strip()
        if not raw:
            return []
        normalized = raw.replace("\r\n", "\n").strip()
        newline_units = [self._clean_sentence_order_sortable_unit(item) for item in normalized.split("\n") if self._clean_sentence_order_sortable_unit(item)]
        if len(newline_units) in {4, 5, 6}:
            return newline_units
        normalized_newline_units = self._normalize_sentence_order_units_to_six(newline_units)
        if normalized_newline_units:
            return normalized_newline_units
        enumerated = re.split(r"(?=[①②③④⑤⑥⑦⑧⑨⑩])", normalized)
        enumerated = [part.strip() for part in enumerated if part.strip()]
        if len(enumerated) >= 2:
            cleaned: list[str] = []
            for part in enumerated:
                cleaned.append(self._clean_sentence_order_sortable_unit(part))
            units = [part for part in cleaned if part]
            normalized_units = self._normalize_sentence_order_units_to_six(units)
            return normalized_units or units
        space_units = [self._clean_sentence_order_sortable_unit(item) for item in re.split(r"\s+", normalized) if self._clean_sentence_order_sortable_unit(item)]
        if len(space_units) in {4, 5, 6}:
            return space_units
        units = [
            self._clean_sentence_order_sortable_unit(item)
            for item in re.split(r"(?<=[。！？!?；;])\s*|\n+", normalized)
            if self._clean_sentence_order_sortable_unit(item)
        ]
        normalized_units = self._normalize_sentence_order_units_to_six(units)
        return normalized_units or units
        return [item.strip() for item in re.split(r"(?<=[。！？!?；;])\s*|\n+", normalized) if item.strip()]

    def _normalize_sentence_order_units_to_six(self, units: list[str], target_count: int | None = None) -> list[str] | None:
        cleaned = [unit.strip() for unit in units if unit and unit.strip()]
        if target_count is None:
            if len(cleaned) in {4, 5, 6}:
                return cleaned
            target_count = 6
        if target_count not in {4, 5, 6}:
            return None
        if len(cleaned) < target_count or len(cleaned) > 12:
            return None
        if len(cleaned) == target_count:
            return cleaned
        merge_need = len(cleaned) - target_count
        if merge_need > len(cleaned) // 2:
            return None
        pair_scores: list[tuple[float, int]] = []
        for index in range(len(cleaned) - 1):
            left = cleaned[index]
            right = cleaned[index + 1]
            score = 0.0
            if right.startswith(("因此", "所以", "此外", "同时", "并且", "而", "但", "于是", "随后", "从而")):
                score += 0.30
            if len(left) <= 28 and len(right) <= 28:
                score += 0.12
            if len(left) + len(right) > 96:
                score -= 0.18
            pair_scores.append((score, index))
        pair_scores.sort(reverse=True)
        selected: list[int] = []
        used: set[int] = set()
        for score, index in pair_scores:
            if index in used or index + 1 in used:
                continue
            selected.append(index)
            used.add(index)
            used.add(index + 1)
            if len(selected) == merge_need:
                break
        if len(selected) != merge_need:
            return None
        selected_set = set(selected)
        normalized: list[str] = []
        index = 0
        while index < len(cleaned):
            if index in selected_set:
                normalized.append(self._merge_sentence_order_unit_pair(cleaned[index], cleaned[index + 1]))
                index += 2
                continue
            normalized.append(cleaned[index])
            index += 1
        if len(normalized) != target_count:
            return None
        return normalized

    def _merge_sentence_order_unit_pair(self, left: str, right: str) -> str:
        left_clean = left.strip()
        right_clean = right.strip()
        if not left_clean:
            return right_clean
        separator = ""
        if not left_clean.endswith(("。", "！", "？", "!", "?", "；", ";", "，", ",", "：", ":")):
            separator = "，"
        return f"{left_clean}{separator}{right_clean}".strip()

    def _format_sortable_units(self, units: list[str]) -> str:
        circled = "①②③④⑤⑥⑦⑧⑨⑩"
        return "\n".join(f"{circled[index] if index < len(circled) else f'{index + 1}.'} {unit}" for index, unit in enumerate(units))

    @staticmethod
    def _clean_sentence_order_sortable_unit(text: str) -> str:
        clean = normalize_prompt_text(text or "")
        if not clean:
            return ""
        clean = re.sub(r"^[①②③④⑤⑥⑦⑧⑨⑩]\s*", "", clean)
        clean = re.sub(r"^\s*(?:\(?\d+\)?(?:[\.、．]|\s+))\s*", "", clean)
        clean = re.sub(r"^[①②③④⑤⑥⑦⑧⑨⑩]\s*", "", clean)
        clean = re.sub(r"^\s*(?:\(?\d+\)?(?:[\.、．]|\s+))\s*", "", clean)
        return clean.strip()

    def _format_sentence_order_natural_material(
        self,
        *,
        raw_text: str,
        sortable_units: list[str],
    ) -> str:
        clean_raw = normalize_prompt_text(raw_text or "")
        if clean_raw and not self._looks_like_sentence_order_list_material(clean_raw):
            return clean_raw

        joined_units = "".join(str(unit or "").strip() for unit in sortable_units if str(unit or "").strip())
        joined_units = normalize_prompt_text(joined_units)
        if joined_units:
            return joined_units
        return clean_raw

    @staticmethod
    def _sentence_order_unit_signature(text: str) -> str:
        normalized = normalize_prompt_text(text or "")
        return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", normalized)

    @classmethod
    def _sentence_order_refinement_preserves_units(
        cls,
        original_units: list[str],
        refined_units: list[str],
    ) -> bool:
        if len(original_units) != len(refined_units):
            return False
        for original, refined in zip(original_units, refined_units):
            original_sig = cls._sentence_order_unit_signature(cls._clean_sentence_order_sortable_unit(original))
            refined_sig = cls._sentence_order_unit_signature(cls._clean_sentence_order_sortable_unit(refined))
            if not original_sig or not refined_sig:
                return False
            if original_sig == refined_sig:
                continue
            if original_sig in refined_sig or refined_sig in original_sig:
                continue
            if SequenceMatcher(None, original_sig, refined_sig).ratio() < 0.66:
                return False
        return True

    @staticmethod
    def _sentence_order_unit_has_dependent_opening(text: str) -> bool:
        clean = str(text or "").strip()
        if not clean:
            return False
        dependent_openers = (
            "这",
            "其",
            "其中",
            "因此",
            "所以",
            "同时",
            "另外",
            "此外",
            "例如",
            "比如",
            "随后",
            "然后",
            "接着",
            "但是",
            "但",
            "然而",
            "可见",
            "由此",
        )
        return any(clean.startswith(token) for token in dependent_openers)

    def _needs_sentence_order_presentation_refinement(
        self,
        *,
        raw_text: str,
        current_text: str,
        sortable_units: list[str],
    ) -> bool:
        clean_current = normalize_prompt_text(current_text or "")
        joined_units = normalize_prompt_text("".join(str(unit or "").strip() for unit in sortable_units if str(unit or "").strip()))
        if self._looks_like_sentence_order_list_material(raw_text):
            return True
        if clean_current and joined_units and clean_current == joined_units:
            return True
        if sortable_units and self._sentence_order_unit_has_dependent_opening(sortable_units[0]):
            return True
        return False

    def _refine_sentence_order_presentation_material(
        self,
        *,
        raw_text: str,
        current_text: str,
        sortable_units: list[str],
        source: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        normalized_current = normalize_prompt_text(current_text or "")
        material_source = source or {}
        if (
            not sortable_units
            or bool(material_source.get("forced_user_material"))
            or not self._needs_sentence_order_presentation_refinement(
                raw_text=raw_text,
                current_text=normalized_current,
                sortable_units=sortable_units,
            )
        ):
            return normalized_current, {}

        issue_labels: list[str] = []
        if self._looks_like_sentence_order_list_material(raw_text):
            issue_labels.append("list_like_source")
        if normalized_current == normalize_prompt_text("".join(sortable_units)):
            issue_labels.append("joined_units_presentation")
        if sortable_units and self._sentence_order_unit_has_dependent_opening(sortable_units[0]):
            issue_labels.append("dependent_opener")

        system_prompt = (
            "你是一名语句排序材料轻修助手。请把输入的排序展示句群整理成更像真题材料的自然短文。"
            "必须严格保持句子数量不变、句子顺序不变、句子核心语义不变；不得合并句子，不得拆分句子，不得删除或新增句子。"
            "你只能做最小必要的展示增强：去掉列表/条目痕迹，补足最少量的代词悬空或局部衔接，使整段更像正式公考材料。"
            "禁止把句群改写成教学口吻，禁止硬贴“首先、其次、最后”等教程式连接词，禁止改动原有的顺序真值。"
        )
        user_prompt = "\n\n".join(
            [
                "请把下面的语句排序展示句群整理成更自然的真题材料外观。",
                f"当前问题：{'; '.join(issue_labels) if issue_labels else '句群仍偏列表化，需要轻微自足化。'}",
                "硬约束：句子数量必须与原句群完全一致；顺序必须完全一致；每一句都必须仍能和对应原句一一对照。",
                "允许：删条目味、补最小衔接、把明显悬空的指代稍微补足到可读。",
                "禁止：合并句子、拆分句子、增删句子、改变句子先后、额外补背景、教程式显性提示。",
                "[当前展示句群]",
                "\n".join(f"{index}. {unit}" for index, unit in enumerate(sortable_units, start=1)),
                "[当前展示材料]",
                normalized_current or "(empty)",
                "[原始材料]",
                normalize_prompt_text(raw_text or ""),
            ]
        )
        try:
            response = self.llm_gateway.generate_json(
                route=self._material_refinement_route(),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema_name="material_refinement",
                schema=MaterialRefinementDraft.model_json_schema(),
            )
            refined = self.material_refinement_adapter.validate_python(response)
            refined_text = normalize_prompt_text(refined.refined_text or "")
            if not refined_text:
                return normalized_current, {}
            refined_units = self._extract_sortable_units_from_text(refined_text)
            refined_units = (
                self._normalize_sentence_order_units_to_six(refined_units, target_count=len(sortable_units))
                or refined_units
            )
            refined_units = [
                str(unit or "").strip()
                for unit in refined_units[: len(sortable_units)]
                if str(unit or "").strip()
            ]
            if len(refined_units) != len(sortable_units):
                return normalized_current, {}
            if not self._sentence_order_refinement_preserves_units(sortable_units, refined_units):
                return normalized_current, {}
            polished_text = normalize_prompt_text("".join(refined_units))
            if not polished_text:
                return normalized_current, {}
            return polished_text, {
                "sentence_order_presentation_refined": True,
                "sentence_order_presentation_refinement_reason": refined.reason or "sentence_order_presentation_polish",
                "sentence_order_presentation_refinement_issues": issue_labels,
            }
        except Exception:
            return normalized_current, {}

    @staticmethod
    def _looks_like_sentence_order_list_material(text: str) -> bool:
        clean = str(text or "").strip()
        if not clean:
            return False
        if len(re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", clean)) >= 2:
            return True
        if len(re.findall(r"(?:^|\n)\s*(?:\(?\d+\)?[\.、]?)\s*", clean)) >= 2:
            return True
        lines = [line.strip() for line in clean.splitlines() if line.strip()]
        if len(lines) >= 4 and len(lines) <= 8:
            punctuated = sum(1 for line in lines if line.endswith(("。", "！", "？", "；", ".", "!", "?", ";")))
            if punctuated >= max(3, len(lines) - 1):
                return True
        return False

    def _best_sortable_unit_count(self, material: MaterialSelectionResult) -> int:
        counts = [
            self._count_sortable_units_from_material(material.text),
            self._count_sortable_units_from_material(material.original_text or ""),
        ]
        best_count = max(counts)
        if best_count in {4, 5, 6}:
            return best_count
        if 6 in counts:
            return 6
        if 5 in counts:
            return 5
        if 4 in counts:
            return 4
        return best_count

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
            if target_count in {4, 5, 6}:
                normalized_units = self._normalize_sentence_order_units_to_six(units, target_count=target_count) or units
            else:
                normalized_units = self._normalize_sentence_order_units_to_six(units) or units
                if len(normalized_units) not in {4, 5, 6}:
                    normalized_units = (
                        self._normalize_sentence_order_units_to_six(units, target_count=6)
                        or self._normalize_sentence_order_units_to_six(units, target_count=5)
                        or self._normalize_sentence_order_units_to_six(units, target_count=4)
                        or units
                    )
                if len(normalized_units) in {4, 5, 6}:
                    target_count = len(normalized_units)
            candidate_count = target_count if target_count in {4, 5, 6} else len(normalized_units)
            if candidate_count in {4, 5, 6} and len(normalized_units) >= candidate_count:
                best_units = normalized_units[:candidate_count]
                break
            if len(normalized_units) > len(best_units):
                best_units = normalized_units
        if target_count not in {4, 5, 6}:
            target_count = len(best_units)
        if target_count not in {4, 5, 6} or len(best_units) < target_count:
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
        unit_count = len(generated_question.correct_order or [])
        if unit_count not in {4, 5, 6}:
            option_lengths = [
                len(sequence)
                for sequence in (
                    self._extract_order_sequence(value)
                    for value in (generated_question.options or {}).values()
                )
                if sequence
            ]
            unit_count = next((count for count in option_lengths if count in {4, 5, 6}), 0)
        if unit_count not in {4, 5, 6}:
            unit_count = len(generated_question.original_sentences or [])
        if unit_count not in {4, 5, 6}:
            unit_count = 6

        expected_markers = ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩"][:unit_count]
        stem = (generated_question.stem or "").strip()
        if stem:
            stem = re.sub(
                r"将[以上下列些]*\d+个(?:句子|部分)重新排列[，,]?\s*语序正确的一项是[:：]?",
                self._sentence_order_stem(unit_count),
                stem,
            )
            stem = re.sub(
                r"将[以上下列些]*[一二三四五六七八九十]+个(?:句子|部分)重新排列[，,]?\s*语序正确的一项是[:：]?",
                self._sentence_order_stem(unit_count),
                stem,
            )
            if "重新排列" in stem and f"{unit_count}个句子" not in stem:
                stem = self._sentence_order_stem(unit_count)

        normalized_options: dict[str, str] = {}
        for key, value in (generated_question.options or {}).items():
            text = str(value or "").strip()
            sequence = self._extract_order_sequence(text)
            if self._is_valid_sentence_order_sequence(sequence, unit_count):
                normalized_options[key] = self._format_order_sequence(sequence)
                continue
            circled = [marker for marker in re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", text) if marker in expected_markers]
            if len(circled) >= unit_count:
                normalized_options[key] = "".join(circled[:unit_count])
                continue
            normalized_options[key] = text

        analysis = str(generated_question.analysis or "")
        analysis = re.sub(r"(\d+)个(?:句子|部分)", f"{unit_count}个句子", analysis)
        analysis = re.sub(r"([七八九十7-9])(?:句|个句子|个部分)", f"{unit_count}个句子", analysis)
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
        units = self._extract_sortable_units_from_text(sortable_block)
        normalized_units = self._normalize_sentence_order_units_to_six(units) or units
        return len(normalized_units)
        enumerated = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", sortable_block)
        if enumerated:
            return len(set(enumerated))
        sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", sortable_block) if item.strip()]
        return len(sentences)

    def _build_reference_source_material(self, source_question) -> MaterialSelectionResult:
        passage = self._clean_material_text(source_question.passage or "")
        stem = normalize_readable_text(source_question.stem or "")
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

