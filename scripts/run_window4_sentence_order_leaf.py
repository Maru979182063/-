from __future__ import annotations

import csv
import json
import os
import random
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
PASSAGE_ENV = ROOT / "passage_service" / ".env"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
DATE_TAG = "2026-04-15"

DEFAULT_BATCH_DIR = "sentence_order_first_sentence_2026-04-14"
DEFAULT_PATTERN_TAG = "首句特征-背景引入"
QUESTION_CARD_ID = "question.sentence_order.standard_v1"
RANDOM_SEED = 20260415
SPLIT_SIZE = 5

BATCH_DIR = str(os.getenv("WINDOW4_BATCH_DIR") or DEFAULT_BATCH_DIR).strip()
LEAF_PATTERN_TAG = str(os.getenv("WINDOW4_LEAF_PATTERN_TAG") or DEFAULT_PATTERN_TAG).strip()
RUN_TAG = str(os.getenv("WINDOW4_RUN_TAG") or "window4_so_first_background_intro_v1").strip()
ACTIVE_SPLITS = {
    item.strip().lower()
    for item in str(os.getenv("WINDOW4_SPLITS") or "dev").split(",")
    if item.strip()
}

LEAF_REQUEST_HINTS = {
    "首句特征-背景引入": {
        "pattern_id": "first_sentence_background_intro",
        "type_slots": {
            "opening_anchor_type": "background_intro",
            "opening_signal_strength": "medium",
            "middle_structure_type": "mixed_layers",
            "local_binding_strength": "high",
            "closing_anchor_type": "none",
            "closing_signal_strength": "low",
            "block_order_complexity": "high",
            "binding_pairs": [
                "scope_or_background -> definition_or_total_judgement",
                "macro_scope -> symbolic_or_discourse_bridge -> narrowed_overview_or_specific_zone",
                "problem_frame -> example_or_specific_manifestation",
                "same_actor_practice -> simultaneous_same_actor_supplement",
                "value_or_feature -> side_effect_or_threshold -> therefore_requirement",
                "core_concept_correction -> feasibility_or_threshold_clarification",
                "urgency_call -> universality_or_scope_expansion",
            ],
            "distractor_modes": [
                "wrong_opening",
                "local_binding_break",
                "block_swap",
            ],
            "distractor_strength": "high",
            "sentence_roles": [
                "background_intro",
                "topic_setter",
                "bridge",
                "total_judgement",
                "problem_frame",
                "definition",
                "concept_correction",
                "overview",
                "detail",
                "feasibility_clarification",
                "scope_expansion",
                "urgency_call",
                "policy_action",
                "scene_landing",
            ],
            "head_constraints": {
                "must_not_have": [
                    "backward_reference",
                    "mid_transition",
                    "example_marker",
                    "scene_detail",
                ],
                "preferred_roles": [
                    "background_intro",
                    "topic_setter",
                    "total_judgement",
                    "definition",
                    "overview",
                ],
            },
            "tail_constraints": {
                "must_have": [],
                "preferred_roles": [
                    "scene_landing",
                    "result",
                    "observation",
                    "scope_expansion",
                    "call_to_action",
                    "summary",
                ],
            },
            "ordering_logic": "scope_or_background_opener -> bridge_or_core_clarification -> hard_local_binding_chain -> urgency_or_scope_widening_landing",
        },
    },
    "首句特征-提出观点": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "viewpoint_opening",
            "opening_signal_strength": "high",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "首句特征-下定义": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "high",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "非首句特征-关联词的后半部分": {
        "pattern_id": "first_sentence_background_intro",
        "type_slots": {
            "opening_anchor_type": "background_intro",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "low",
            "block_order_complexity": "high",
        },
    },
    "非首句特征-指代不清的指代词": {
        "pattern_id": "first_sentence_background_intro",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "low",
            "block_order_complexity": "high",
        },
    },
    "非首句特征-举例子": {
        "pattern_id": "first_sentence_background_intro",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "mixed_layers",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "非首句特征-其他": {
        "pattern_id": "first_sentence_background_intro",
        "type_slots": {
            "opening_anchor_type": "background_intro",
            "opening_signal_strength": "medium",
            "middle_structure_type": "mixed_layers",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "low",
            "block_order_complexity": "high",
        },
    },
    "关联词-并列": {
        "pattern_id": "carry_parallel_expand",
        "type_slots": {
            "opening_anchor_type": "upper_context_link",
            "opening_signal_strength": "medium",
            "middle_structure_type": "parallel_expansion",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "关联词-转折": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "viewpoint_opening",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "关联词-其他": {
        "pattern_id": "carry_parallel_expand",
        "type_slots": {
            "opening_anchor_type": "upper_context_link",
            "opening_signal_strength": "medium",
            "middle_structure_type": "mixed_layers",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "指代词": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "其他（确定捆绑）": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "结论": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "high",
            "block_order_complexity": "medium",
        },
    },
    "对策": {
        "pattern_id": "viewpoint_reason_action",
        "type_slots": {
            "opening_anchor_type": "viewpoint_opening",
            "opening_signal_strength": "medium",
            "middle_structure_type": "cause_effect_chain",
            "local_binding_strength": "high",
            "closing_anchor_type": "call_to_action",
            "closing_signal_strength": "high",
            "block_order_complexity": "medium",
        },
    },
    "行文逻辑-观点+解释说明": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "viewpoint_opening",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "行文逻辑-问题+对策": {
        "pattern_id": "problem_solution_case_blocks",
        "type_slots": {
            "opening_anchor_type": "problem_opening",
            "opening_signal_strength": "medium",
            "middle_structure_type": "problem_solution_blocks",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
    "日常逻辑-时间脉络": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "日常逻辑-行动顺序": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "行文逻辑-提问+回答": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "local_binding",
            "local_binding_strength": "high",
            "closing_anchor_type": "conclusion",
            "closing_signal_strength": "medium",
            "block_order_complexity": "medium",
        },
    },
    "行文逻辑-其他": {
        "pattern_id": "dual_anchor_lock",
        "type_slots": {
            "opening_anchor_type": "explicit_topic",
            "opening_signal_strength": "medium",
            "middle_structure_type": "mixed_layers",
            "local_binding_strength": "high",
            "closing_anchor_type": "summary",
            "closing_signal_strength": "medium",
            "block_order_complexity": "high",
        },
    },
}
STAGE_TAG = "_".join(sorted(ACTIVE_SPLITS)) if ACTIVE_SPLITS else "dev"
SOURCE_PATH = REPORTS_DIR / "distill_batches" / BATCH_DIR / "cleaned_truth_materials.jsonl"
MANIFEST_SOURCE_PATH = ROOT / "test" / "material_card_eval_assets" / "truth_segment_sample_manifest.yaml"
DOCX_FALLBACK_PATH = ROOT / "tmp_truth_docs" / "sentence_order_truth.docx"
RESULTS_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_results_{DATE_TAG}.csv"
PACK_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_pack_{DATE_TAG}.md"
MANIFEST_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_manifest_{DATE_TAG}.json"
ORDER_BATCH_SOURCE_DIRS = {
    "sentence_order_first_sentence_2026-04-14": Path(r"C:\Users\Maru\Desktop\语句排序题\确定首句"),
    "sentence_order_fixed_bundle_2026-04-14": Path(r"C:\Users\Maru\Desktop\语句排序题\确定捆绑"),
    "sentence_order_sequence_2026-04-14": Path(r"C:\Users\Maru\Desktop\语句排序题\确定顺序"),
    "sentence_order_tail_sentence_2026-04-14": Path(r"C:\Users\Maru\Desktop\语句排序题\确定尾句"),
}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PROMPT_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROMPT_SERVICE_ROOT))

import yaml  # noqa: E402

from app.core.exceptions import DomainError  # noqa: E402
from app.schemas.question import QuestionGenerateRequest  # noqa: E402
from scripts.round1_generation_smoke_rerun import DOCX_MAP, build_service, extract_docx_blocks  # noqa: E402


def _load_llm_env() -> None:
    api_key = str(os.getenv("API_KEY") or "").strip()
    base_url = str(os.getenv("BASE_URL") or "").strip()
    model = str(os.getenv("MODEL") or "").strip()
    if api_key:
        os.environ.setdefault("OPENAI_API_KEY", api_key)
        os.environ.setdefault("GENERATION_LLM_API_KEY", api_key)
        os.environ.setdefault("MATERIAL_LLM_API_KEY", api_key)
    if base_url:
        os.environ.setdefault("OPENAI_BASE_URL", base_url)
        os.environ.setdefault("GENERATION_LLM_BASE_URL", base_url)
        os.environ.setdefault("MATERIAL_LLM_BASE_URL", base_url)
    if model:
        os.environ.setdefault("GENERATION_LLM_MODEL", model)

    if not PASSAGE_ENV.exists():
        return
    for line in PASSAGE_ENV.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key == "PASSAGE_OPENAI_API_KEY":
            os.environ.setdefault("OPENAI_API_KEY", value)
            os.environ.setdefault("GENERATION_LLM_API_KEY", value)
            os.environ.setdefault("MATERIAL_LLM_API_KEY", value)
        elif key == "PASSAGE_OPENAI_BASE_URL":
            os.environ.setdefault("OPENAI_BASE_URL", value)
            os.environ.setdefault("GENERATION_LLM_BASE_URL", value)
            os.environ.setdefault("MATERIAL_LLM_BASE_URL", value)


def _load_batch_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_line in SOURCE_PATH.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if str(payload.get("pattern_tag") or "").strip() != LEAF_PATTERN_TAG:
            continue
        rows.append(payload)
    rows.sort(key=lambda row: str(row.get("question_id") or row.get("sample_id") or ""))
    random.Random(RANDOM_SEED).shuffle(rows)
    return rows


def _load_manifest_row_keys() -> tuple[list[str], list[str]]:
    if not MANIFEST_SOURCE_PATH.exists():
        return [], []
    payload = yaml.safe_load(MANIFEST_SOURCE_PATH.read_text(encoding="utf-8")) or {}
    batch_payload = ((payload.get("batches") or {}).get(BATCH_DIR) or {})
    train_row_keys: list[str] = []
    holdout_row_keys: list[str] = []
    for item in batch_payload.get("train") or []:
        if str(item.get("pattern_tag") or "").strip() == LEAF_PATTERN_TAG:
            train_row_keys.append(str(item.get("row_key") or ""))
    for item in batch_payload.get("holdout") or []:
        if str(item.get("pattern_tag") or "").strip() == LEAF_PATTERN_TAG:
            holdout_row_keys.append(str(item.get("row_key") or ""))
    return train_row_keys, holdout_row_keys


def _row_key(row: dict[str, Any]) -> str:
    sample_id = str(row.get("sample_id") or "")
    pattern_tag = str(row.get("pattern_tag") or "")
    source_doc = str(row.get("source_doc") or "")
    return f"{sample_id}::{pattern_tag}::{source_doc}"


def _choose_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    train_keys, holdout_keys = _load_manifest_row_keys()
    keyed_rows = {_row_key(row): row for row in rows}

    train_rows = [keyed_rows[key] for key in train_keys if key in keyed_rows]
    holdout_rows = [keyed_rows[key] for key in holdout_keys if key in keyed_rows]

    used_ids = {
        str(row.get("sample_id") or "")
        for row in [*train_rows, *holdout_rows]
        if str(row.get("sample_id") or "")
    }
    extras = [
        row
        for row in rows
        if str(row.get("sample_id") or "") not in used_ids
    ]

    dev_rows = train_rows[:SPLIT_SIZE]
    defense_rows = train_rows[SPLIT_SIZE : SPLIT_SIZE * 2]

    if len(dev_rows) < SPLIT_SIZE:
        needed = SPLIT_SIZE - len(dev_rows)
        dev_rows.extend(extras[:needed])
        extras = extras[needed:]
    if len(defense_rows) < SPLIT_SIZE:
        needed = SPLIT_SIZE - len(defense_rows)
        defense_rows.extend(extras[:needed])
        extras = extras[needed:]

    retest_rows = holdout_rows[:SPLIT_SIZE]
    if len(retest_rows) < SPLIT_SIZE:
        needed = SPLIT_SIZE - len(retest_rows)
        retest_rows.extend(extras[:needed])

    selected = [*dev_rows, *defense_rows, *retest_rows]
    if selected:
        return selected
    return rows[: SPLIT_SIZE * 3]


def _split_plan(total_rows: int) -> tuple[int, int, int]:
    if total_rows >= SPLIT_SIZE * 3:
        return SPLIT_SIZE, SPLIT_SIZE, SPLIT_SIZE
    if total_rows > SPLIT_SIZE * 2:
        return SPLIT_SIZE, SPLIT_SIZE, total_rows - SPLIT_SIZE * 2
    if total_rows > SPLIT_SIZE:
        return SPLIT_SIZE, total_rows - SPLIT_SIZE, 0
    return total_rows, 0, 0


def _split_label(index: int, total_rows: int) -> str:
    dev_count, defense_count, _ = _split_plan(total_rows)
    if index < dev_count:
        return "dev"
    if index < dev_count + defense_count:
        return "defense"
    return "retest"


def _normalize_material_text(text: str) -> str:
    normalized = " ".join(str(text or "").split())
    if not normalized:
        return ""
    if "\n" in normalized:
        return normalized
    parts = [part.strip() for part in normalized.split(" ") if part.strip()]
    if len(parts) >= 5:
        return "\n".join(parts)
    return normalized


def _resolve_request_material_text(row: dict[str, Any], gold_question: dict[str, Any]) -> str:
    gold_sentences = gold_question.get("original_sentences") if isinstance(gold_question, dict) else None
    if isinstance(gold_sentences, list):
        cleaned = [str(sentence).strip() for sentence in gold_sentences if str(sentence).strip()]
        if len(cleaned) >= 5:
            return "\n".join(cleaned)
    return _normalize_material_text(str(row.get("material_text") or ""))


def _build_request(row: dict[str, Any], gold_question: dict[str, Any]) -> QuestionGenerateRequest:
    material_text = _resolve_request_material_text(row, gold_question)
    request_hints = LEAF_REQUEST_HINTS.get(LEAF_PATTERN_TAG) or {}
    return QuestionGenerateRequest.model_validate(
        {
            "question_card_id": QUESTION_CARD_ID,
            "generation_mode": "forced_user_material",
            "question_focus": "sentence_order",
            "difficulty_level": "medium",
            "count": 1,
            "use_fewshot": True,
            "fewshot_mode": "structure_only",
            "type_slots": request_hints.get("type_slots") or {},
            "extra_constraints": {
                "pattern_id": request_hints.get("pattern_id"),
            } if request_hints.get("pattern_id") else {},
            "user_material": {
                "text": material_text,
                "title": str(row.get("question_id") or row.get("sample_id") or ""),
                "document_genre": "exam_reference",
                "source_label": str(row.get("source_doc") or "window4_sentence_order_leaf"),
            },
        }
    )


def _configure_service():
    service = build_service()
    service.RACE_CANDIDATE_COUNT = 1
    service.MAX_ALIGNMENT_RETRIES = 0
    service.MAX_QUALITY_REPAIR_RETRIES = 0
    model_override = str(os.getenv("MODEL") or "").strip()
    if model_override:
        provider = service.runtime_config.llm.providers["generation_llm"]
        provider.models.question_generation = model_override
        provider.models.question_repair = model_override
        provider.models.judge_review = model_override
    return service


def _generate_minimal_once(service, request: QuestionGenerateRequest) -> dict[str, Any]:
    prepared_request = service._prepare_request(request)
    decoded, _ = service._decode_generation_target(prepared_request)
    standard_request = dict(decoded["standard_request"])
    question_card_binding = service._resolve_question_card_binding(
        question_card_id=prepared_request.question_card_id,
        question_type=standard_request["question_type"],
        business_subtype=standard_request.get("business_subtype"),
        pattern_id=standard_request.get("pattern_id"),
    )
    standard_request = service._apply_question_card_binding(
        standard_request=standard_request,
        question_card_binding=question_card_binding,
    )
    standard_request["difficulty_target"] = service._effective_difficulty_target(
        standard_request["difficulty_target"],
        use_reference_question=bool(prepared_request.source_question),
    )

    request_id = str(uuid4())
    batch_id = str(uuid4())
    source_question_analysis = service.source_question_analyzer.analyze(
        source_question=prepared_request.source_question,
        question_type=standard_request["question_type"],
        business_subtype=standard_request.get("business_subtype"),
    )
    request_snapshot = service._build_request_snapshot(
        prepared_request,
        standard_request,
        decoded,
        request_id=request_id,
        source_question_analysis=source_question_analysis,
        question_card_binding=question_card_binding,
    )

    materials, material_warnings = service._resolve_generation_materials(
        request=prepared_request,
        standard_request=standard_request,
        source_question_analysis=source_question_analysis,
        question_card_binding=question_card_binding,
        request_snapshot=request_snapshot,
        effective_count=1,
    )
    if not materials:
        raise DomainError(
            "No eligible materials were returned by passage_service.",
            status_code=502,
            details={"question_type": standard_request["question_type"]},
        )
    materials = service._prioritize_material_candidates(
        materials,
        question_type=standard_request["question_type"],
        source_question_analysis=source_question_analysis,
    )

    material = service._annotate_material_usage(materials[0])
    if standard_request["question_type"] == "sentence_order":
        adapted_material = service._coerce_sentence_order_material(
            material=material,
            source_question_analysis=source_question_analysis,
        )
        if adapted_material is None:
            raise DomainError(
                "Selected sentence_order material could not be coerced into a sortable-unit group.",
                status_code=422,
                details={"material_id": material.material_id},
            )
        material = adapted_material
    material = service._refine_material_if_needed(material, request_snapshot=request_snapshot)

    build_request = service._build_prompt_request_from_snapshot(request_snapshot)
    built_item = service.orchestrator.build_prompt(build_request)
    built_item["item_id"] = f"minimal::{uuid4().hex}"
    built_item["batch_id"] = batch_id
    built_item["request_snapshot"] = deepcopy(request_snapshot)
    service._hydrate_sentence_order_candidate_type_context(built_item)
    built_item["material_selection"] = material.model_dump()
    built_item["generation_mode"] = str(request_snapshot.get("generation_mode") or "standard")
    built_item["material_source_type"] = str((material.source or {}).get("material_source_type") or "platform_selected")
    built_item["forced_generation"] = built_item["generation_mode"] == "forced_user_material"
    built_item["material_text"] = material.text
    built_item["material_source"] = material.source
    built_item["material_usage_count_before"] = material.usage_count_before
    built_item["material_previously_used"] = material.previously_used
    built_item["material_last_used_at"] = material.last_used_at
    built_item["preference_profile"] = service._preference_profile_from_snapshot(request_snapshot)
    built_item["feedback_snapshot"] = service._feedback_snapshot_from_material(material)
    built_item["revision_count"] = 0
    built_item["latest_action"] = "generate_minimal_once"
    built_item["latest_action_at"] = service.repository._utc_now()
    built_item["notes"] = list(built_item.get("notes") or [])
    built_item["notes"].extend(
        [
            "minimal_short_chain_single_pass",
            "no_validator_gate",
            "no_evaluator_gate",
            "no_alignment_repair",
            "no_quality_repair",
        ]
    )
    if built_item["forced_generation"]:
        built_item["notes"].extend(
            [
                "forced_user_material_generation",
                "caution:user_uploaded_material_unvalidated",
            ]
        )

    template_record = service._resolve_template(
        question_type=build_request.question_type,
        business_subtype=build_request.business_subtype,
        action_type="generate",
    )
    built_item["prompt_template_name"] = template_record.template_name
    built_item["prompt_template_version"] = template_record.template_version

    generated_question, raw_model_output = service._generate_question(
        built_item=built_item,
        material=material,
        route=service._question_generation_route(),
        prompt_template=template_record,
        feedback_notes=[],
    )
    built_item["generated_question"] = generated_question.model_dump()
    built_item["stem_text"] = generated_question.stem
    built_item["current_status"] = "generated_once"
    built_item["statuses"] = {
        "generation_status": "success",
        "validation_status": "skipped_minimal_chain",
        "review_status": "manual_compare_only",
    }
    built_item["validation_result"] = {
        "passed": None,
        "errors": [],
        "warnings": [],
        "validation_status": "skipped_minimal_chain",
    }
    built_item["evaluation_result"] = {
        "overall_score": None,
        "judge_reason": "",
        "status": "skipped_minimal_chain",
    }
    built_item["raw_model_output"] = raw_model_output

    return {
        "items": [built_item],
        "warnings": [
            *list(material_warnings or []),
            "Minimal chain mode: single generation only; validator/evaluator gates were skipped by the runner.",
        ],
    }


def _parse_sentence_order_gold(lines: list[str]) -> dict[str, object]:
    stem = ""
    options = {key: "" for key in ("A", "B", "C", "D")}
    answer = ""
    analysis_lines: list[str] = []
    original_sentences: list[str] = []

    stem_index = next((idx for idx, line in enumerate(lines) if line.startswith("将以下") or line.startswith("将以上")), None)
    if stem_index is not None:
        stem = lines[stem_index]

    options_index = next((idx for idx, line in enumerate(lines) if line.startswith("A.")), None)
    sentence_upper_bound = options_index if options_index is not None else len(lines)
    for line in lines[:sentence_upper_bound]:
        match = re.match(r"^(\d+)(.+)$", str(line).strip())
        if not match:
            continue
        original_sentences.append(match.group(2).strip())
    if options_index is not None:
        option_line = lines[options_index]
        matches = list(re.finditer(r"([A-D])\.(.*?)(?=([A-D]\.)|$)", option_line))
        for match in matches:
            options[match.group(1)] = match.group(2).strip()

    answer_index = next((idx for idx, line in enumerate(lines) if line.startswith("【答案】")), None)
    if answer_index is not None and answer_index + 1 < len(lines):
        answer = lines[answer_index + 1].strip()

    analysis_index = next((idx for idx, line in enumerate(lines) if line.startswith("【解析】")), None)
    if analysis_index is not None:
        stop_prefixes = ("【文段出处】", "【解析视频】", "【正确率】", "【易错项】", "【考点】")
        for line in lines[analysis_index + 1 :]:
            if any(line.startswith(prefix) for prefix in stop_prefixes):
                break
            analysis_lines.append(line)

    return {
        "stem": stem,
        "options": options,
        "answer": answer,
        "analysis": "\n".join(analysis_lines).strip(),
        "original_sentences": original_sentences,
        "material_text": "\n".join(original_sentences).strip(),
    }


def _extract_gold_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, object]]:
    gold_lookup: dict[str, dict[str, object]] = {}
    docx_paths: list[Path] = []

    batch_source_dir = ORDER_BATCH_SOURCE_DIRS.get(BATCH_DIR)
    if batch_source_dir and batch_source_dir.exists():
        for source_doc in sorted({str(row.get("source_doc") or "").strip() for row in rows if str(row.get("source_doc") or "").strip()}):
            candidate = batch_source_dir / source_doc
            if candidate.exists():
                docx_paths.append(candidate)

    for path in DOCX_MAP.values():
        if path.exists():
            docx_paths.append(path)
    if DOCX_FALLBACK_PATH.exists():
        docx_paths.append(DOCX_FALLBACK_PATH)
    if not docx_paths:
        raise FileNotFoundError("No sentence_order docx source was found.")

    for path in docx_paths:
        for qid, block in extract_docx_blocks(path).items():
            if qid in gold_lookup:
                continue
            gold_lookup[qid] = _parse_sentence_order_gold(list(block["lines"]))
    return gold_lookup


def _options_text(options: dict[str, str]) -> list[str]:
    return [f"- {key}: {options.get(key, '')}" for key in ("A", "B", "C", "D")]


def _render_case_markdown(row: dict[str, str]) -> list[str]:
    generated_options = json.loads(row["generated_options_json"] or "{}")
    gold_options = json.loads(row["gold_options_json"] or "{}")
    generated_original_sentences = json.loads(row["generated_original_sentences_json"] or "[]")
    generated_correct_order = json.loads(row["generated_correct_order_json"] or "[]")
    lines = [
        f"## {row['split']} :: {row['question_id']}",
        "",
        f"- sample_index: `{row['sample_index']}`",
        f"- pattern_tag: `{row['pattern_tag']}`",
        f"- validation_passed: `{row['validation_passed']}`",
        f"- overall_score: `{row['overall_score']}`",
        f"- generation_exception: `{row['generation_exception'] or 'none'}`",
        f"- generation_error_details: `{row['generation_error_details_json'] or 'none'}`",
        "",
        "**Material**",
        "",
        row["material_text"] or "(empty)",
        "",
        "**Generated Question**",
        "",
        f"> {row['generated_stem'] or '(empty)'}",
        "",
    ]
    lines.extend(_options_text(generated_options))
    lines.extend(
        [
            "",
            f"answer: `{row['generated_answer'] or ''}`",
            "",
            f"correct_order: `{generated_correct_order}`",
            "",
            "**Generated Original Sentences**",
            "",
        ]
    )
    for idx, sentence in enumerate(generated_original_sentences, start=1):
        lines.append(f"- {idx}: {sentence}")
    lines.extend(
        [
            "",
            "**Generated Analysis**",
            "",
            row["generated_analysis"] or "(empty)",
            "",
            "**Gold Question**",
            "",
            f"> {row['gold_stem'] or '(empty)'}",
            "",
        ]
    )
    lines.extend(_options_text(gold_options))
    lines.extend(
        [
            "",
            f"answer: `{row['gold_answer'] or ''}`",
            "",
            "**Gold Analysis**",
            "",
            row["gold_analysis"] or "(empty)",
            "",
            "**Aux Signals**",
            "",
            f"- validator_errors: {row['validator_errors'] or 'none'}",
            f"- validator_warnings: {row['validator_warnings'] or 'none'}",
            f"- notes: {row['notes'] or 'none'}",
            f"- warnings: {row['warnings'] or 'none'}",
            "",
        ]
    )
    return lines


def _write_csv(rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    _load_llm_env()
    leaf_rows = _load_batch_rows()
    if not leaf_rows:
        raise RuntimeError(f"No rows found for batch={BATCH_DIR} pattern_tag={LEAF_PATTERN_TAG}")

    selected_rows = _choose_rows(leaf_rows)
    total_rows = len(selected_rows)
    dev_count, defense_count, retest_count = _split_plan(total_rows)
    gold_lookup = _extract_gold_lookup(selected_rows)
    service = _configure_service()

    manifest_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, str]] = []
    pack_sections = [
        f"# {RUN_TAG}",
        "",
        f"- batch_dir: `{BATCH_DIR}`",
        f"- leaf_pattern_tag: `{LEAF_PATTERN_TAG}`",
        f"- question_card_id: `{QUESTION_CARD_ID}`",
        f"- random_seed: `{RANDOM_SEED}`",
        f"- split_policy: `{dev_count} dev + {defense_count} defense + {retest_count} retest`",
        f"- active_splits: `{', '.join(sorted(ACTIVE_SPLITS))}`",
        f"- shortest_chain: `question_card_id -> forced_user_material -> build_prompt -> single_generate -> local_struct_cleanup`",
        f"- chain_mode: `manual_compare_only / validator_skipped / evaluator_skipped / repair_skipped`",
        "",
    ]

    for index, row in enumerate(selected_rows):
        split = _split_label(index, total_rows)
        if ACTIVE_SPLITS and split not in ACTIVE_SPLITS:
            continue

        question_id = str(row.get("question_id") or "")
        gold_question = gold_lookup.get(question_id) or {}
        manifest_rows.append(
            {
                "sample_index": index + 1,
                "split": split,
                "sample_id": str(row.get("sample_id") or ""),
                "question_id": question_id,
                "pattern_tag": str(row.get("pattern_tag") or ""),
                "source_doc": str(row.get("source_doc") or ""),
            }
        )

        request = _build_request(row, gold_question)
        generation_exception = ""
        generation_error_details_json = ""
        response: dict[str, Any] = {}
        try:
            response = _generate_minimal_once(service, request)
        except DomainError as exc:
            generation_exception = f"{exc.__class__.__name__}: {exc}"
            generation_error_details_json = json.dumps(exc.details, ensure_ascii=False)
        except Exception as exc:  # noqa: BLE001
            generation_exception = f"{exc.__class__.__name__}: {exc}"

        item = ((response.get("items") or [{}])[0]) if response else {}
        generated = dict(item.get("generated_question") or {})
        validation = dict(item.get("validation_result") or {})
        evaluation = dict(item.get("evaluation_result") or {})
        generated_options = generated.get("options") if isinstance(generated.get("options"), dict) else {}
        gold_options = gold_question.get("options") if isinstance(gold_question.get("options"), dict) else {}

        result_row = {
            "sample_index": str(index + 1),
            "split": split,
            "sample_id": str(row.get("sample_id") or ""),
            "question_id": question_id,
            "pattern_tag": str(row.get("pattern_tag") or ""),
            "source_doc": str(row.get("source_doc") or ""),
            "material_text": _resolve_request_material_text(row, gold_question),
            "generated_stem": str(generated.get("stem") or ""),
            "generated_options_json": json.dumps(generated_options, ensure_ascii=False),
            "generated_answer": str(generated.get("answer") or ""),
            "generated_analysis": str(generated.get("analysis") or ""),
            "generated_original_sentences_json": json.dumps(
                generated.get("original_sentences") or [],
                ensure_ascii=False,
            ),
            "generated_correct_order_json": json.dumps(
                generated.get("correct_order") or [],
                ensure_ascii=False,
            ),
            "gold_stem": str(gold_question.get("stem") or ""),
            "gold_options_json": json.dumps(gold_options, ensure_ascii=False),
            "gold_answer": str(gold_question.get("answer") or ""),
            "gold_analysis": str(gold_question.get("analysis") or ""),
            "validation_passed": "true" if validation.get("passed") else "false",
            "validator_errors": "; ".join(validation.get("errors") or []),
            "validator_warnings": "; ".join(validation.get("warnings") or []),
            "overall_score": str(evaluation.get("overall_score") or ""),
            "notes": "; ".join(item.get("notes") or []),
            "warnings": "; ".join(item.get("warnings") or []),
            "generation_exception": generation_exception,
            "generation_error_details_json": generation_error_details_json,
        }
        result_rows.append(result_row)
        pack_sections.extend(_render_case_markdown(result_row))

    MANIFEST_PATH.write_text(json.dumps(manifest_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(result_rows)
    PACK_PATH.write_text("\n".join(pack_sections) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
