from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
DISTILL_BATCHES_DIR = REPORTS_DIR / "distill_batches"
MAPPING_CONFIG_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_material_card_id_mapping.yaml"
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
PASSAGE_ENV = PASSAGE_SERVICE_ROOT / ".env"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


@dataclass(frozen=True)
class BatchSpec:
    batch_dir: str
    family_id: str
    candidate_type: str
    question_card_id: str | None = None
    enable_fill_bridge: bool = False
    enable_order_bridge: bool = False
    row_mapping: dict[str, dict[str, dict[str, Any]]] | None = None


def _load_batch_specs() -> tuple[BatchSpec, ...]:
    raw = yaml.safe_load(MAPPING_CONFIG_PATH.read_text(encoding="utf-8"))
    specs: list[BatchSpec] = []
    for batch_dir, payload in (raw.get("batches") or {}).items():
        specs.append(
            BatchSpec(
                batch_dir=str(batch_dir),
                family_id=str(payload.get("business_family_id") or ""),
                candidate_type=str(payload.get("candidate_type") or ""),
                question_card_id=payload.get("question_card_id"),
                enable_fill_bridge=bool(payload.get("enable_fill_bridge") or False),
                enable_order_bridge=bool(payload.get("enable_order_bridge") or False),
                row_mapping=dict(payload.get("runtime_row_mapping") or {}),
            )
        )
    return tuple(specs)


BATCH_SPECS = _load_batch_specs()


def _load_llm_env() -> None:
    if not PASSAGE_ENV.exists():
        return
    for raw_line in PASSAGE_ENV.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


def _stable_split(sample_id: str, holdout_ratio: float) -> str:
    digest = hashlib.md5(sample_id.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return "holdout" if bucket < holdout_ratio else "train"


def _iter_truth_rows(batch_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    jsonl_path = batch_dir / "cleaned_truth_materials.jsonl"
    with jsonl_path.open("r", encoding="utf-8-sig") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _resolve_row_expectation(spec: BatchSpec, row: dict[str, Any]) -> dict[str, Any]:
    row_mapping = spec.row_mapping or {}
    for field_name in ("pattern_tag", "subfamily", "source_doc"):
        value = str(row.get(field_name) or "")
        if not value:
            continue
        matched = ((row_mapping.get(field_name) or {}).get(value) or {})
        if matched:
            return dict(matched)
    return {}


def _row_key(row: dict[str, Any]) -> str:
    sample_id = str(row.get("sample_id") or "")
    pattern_tag = str(row.get("pattern_tag") or "")
    source_doc = str(row.get("source_doc") or "")
    return f"{sample_id}::{pattern_tag}::{source_doc}"


def _load_sample_manifest(path: Path) -> dict[str, dict[str, dict[str, Any]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    batches = dict(raw.get("batches") or {})
    sample_lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for batch_dir, payload in batches.items():
        current: dict[str, dict[str, Any]] = {}
        for split_name in ("train", "holdout", "negative_probe"):
            for item in (payload.get(split_name) or []):
                row_key = str(item.get("row_key") or "")
                if row_key:
                    normalized = dict(item)
                    normalized.setdefault("split", split_name)
                    current[row_key] = normalized
        sample_lookup[str(batch_dir)] = current
    return sample_lookup


def _row_group_key(row: dict[str, Any]) -> str:
    for field_name in ("pattern_tag", "subfamily", "source_doc"):
        value = str(row.get(field_name) or "").strip()
        if value:
            return f"{field_name}:{value}"
    return "ungrouped"


def _limit_rows_per_group(rows: list[dict[str, Any]], limit_per_group: int) -> list[dict[str, Any]]:
    if limit_per_group <= 0:
        return rows
    counts: dict[str, int] = {}
    selected: list[dict[str, Any]] = []
    for row in rows:
        group_key = _row_group_key(row)
        current = counts.get(group_key, 0)
        if current >= limit_per_group:
            continue
        counts[group_key] = current + 1
        selected.append(row)
    return selected


def _build_material(row: dict[str, Any], spec: BatchSpec, text: str) -> SimpleNamespace:
    text = _normalize_material_text_for_eval(text, spec)
    return SimpleNamespace(
        id=str(row.get("sample_id") or row.get("question_id") or ""),
        text=text,
        span_type=spec.candidate_type,
        candidate_span_id=str(row.get("sample_id") or ""),
        paragraph_count=max(1, text.count("\n\n") + 1),
        sentence_count=max(1, sum(text.count(mark) for mark in ("。", "！", "？", ";", "；")) or 1),
        start_paragraph=0,
        end_paragraph=max(0, text.count("\n\n")),
        start_sentence=0,
        end_sentence=max(0, sum(text.count(mark) for mark in ("。", "！", "？", ";", "；")) - 1),
        quality_flags=[],
        feature_profile={},
        universal_profile={},
        source={
            "source_name": str(row.get("source_doc") or ""),
            "truth_pattern_tag": str(row.get("pattern_tag") or ""),
            "truth_subfamily": str(row.get("subfamily") or ""),
        },
    )


def _build_article(row: dict[str, Any], text: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=str(row.get("question_id") or row.get("sample_id") or ""),
        title=f"{row.get('family', '')}-{row.get('pattern_tag', '')}",
        clean_text=text,
        raw_text=text,
        source=str(row.get("source_doc") or "truth_distill"),
        source_url=None,
        domain="truth.local",
    )


def _normalize_material_text_for_eval(text: str, spec: BatchSpec) -> str:
    normalized = text.strip()
    if spec.family_id == "sentence_fill":
        normalized = re.sub(r"\s*填入画横线部分最恰当的一句是.*$", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
    if spec.family_id != "sentence_order":
        return normalized
    has_sentence_punctuation = any(mark in normalized for mark in ("。", "！", "？", "；", ";"))
    if has_sentence_punctuation:
        return normalized
    if normalized.count(" ") >= 5:
        parts = [part.strip() for part in normalized.split(" ") if part.strip()]
        if len(parts) >= 5:
            return "\n".join(parts)
    return normalized


def _looks_like_unresolved_fill_input(text: str) -> bool:
    markers = ("“ ”", "___", "____", "______", "（  ）", "( )")
    if any(marker in text for marker in markers):
        return True
    return bool(re.search(r"[一二三四五六七八九十]是\s*[；;]", text))


def _looks_like_question_stem_residue(text: str) -> bool:
    residue_markers = (
        "填入画横线处最恰当",
        "填入画横线部分最恰当",
        "填入划横线处最合适",
        "填入横线处最恰当",
        "文中括号处应引用的句子是",
        "下列填入画横线",
        "最合适的一项是",
    )
    if any(marker in text for marker in residue_markers):
        return True
    return bool(re.search(r"填入.{0,16}(横线|括号).{0,16}(恰当|合适)", text))


def _evaluate_row(
    pipeline: MaterialPipelineV2,
    row: dict[str, Any],
    spec: BatchSpec,
    split: str,
    sample_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expectation = _resolve_row_expectation(spec, row)
    expected_leaf_card = str(expectation.get("material_card_id") or "")
    text = str((sample_meta or {}).get("material_text_override") or row.get("material_text") or "").strip()
    text = _normalize_material_text_for_eval(text, spec)
    if spec.family_id == "sentence_fill" and _looks_like_unresolved_fill_input(text):
        return {
            "split": split,
            "batch_dir": spec.batch_dir,
            "family_id": spec.family_id,
            "expected_leaf_card": expected_leaf_card,
            "sample_id": str(row.get("sample_id") or ""),
            "question_id": str(row.get("question_id") or ""),
            "source_doc": str(row.get("source_doc") or ""),
            "pattern_tag": str(row.get("pattern_tag") or ""),
            "text_length": len(text),
            "text_preview": text[:160],
            "status": "skipped",
            "selected_material_card": "",
            "selected_business_card": "",
            "candidate_type": spec.candidate_type,
            "quality_score": "",
            "family_match_score": "",
            "family_match_hint": "",
            "llm_readiness_score": "",
            "task_score": "",
            "material_card_top3": "",
            "business_card_top3": "",
            "leaf_hit": "false",
            "reason": "unresolved_fill_input",
            "reject_stage": "input_cleaning",
            "trace_json": json.dumps({"reject_reason": "unresolved_fill_input"}, ensure_ascii=False),
        }
    if spec.family_id == "sentence_fill" and _looks_like_question_stem_residue(text):
        return {
            "split": split,
            "batch_dir": spec.batch_dir,
            "family_id": spec.family_id,
            "expected_leaf_card": expected_leaf_card,
            "sample_id": str(row.get("sample_id") or ""),
            "question_id": str(row.get("question_id") or ""),
            "source_doc": str(row.get("source_doc") or ""),
            "pattern_tag": str(row.get("pattern_tag") or ""),
            "text_length": len(text),
            "text_preview": text[:160],
            "status": "skipped",
            "selected_material_card": "",
            "selected_business_card": "",
            "candidate_type": spec.candidate_type,
            "quality_score": "",
            "family_match_score": "",
            "family_match_hint": "",
            "llm_readiness_score": "",
            "task_score": "",
            "material_card_top3": "",
            "business_card_top3": "",
            "leaf_hit": "false",
            "reason": "question_stem_residue",
            "reject_stage": "input_cleaning",
            "trace_json": json.dumps({"reject_reason": "question_stem_residue"}, ensure_ascii=False),
        }
    material = _build_material(row, spec, text)
    article = _build_article(row, text)
    item, trace = _build_item_with_trace(
        pipeline=pipeline,
        material=material,
        article=article,
        spec=spec,
        expectation=expectation,
    )

    result: dict[str, Any] = {
        "split": split,
        "batch_dir": spec.batch_dir,
        "family_id": spec.family_id,
        "expected_leaf_card": expected_leaf_card,
        "sample_id": str(row.get("sample_id") or ""),
        "question_id": str(row.get("question_id") or ""),
        "source_doc": str(row.get("source_doc") or ""),
        "pattern_tag": str(row.get("pattern_tag") or ""),
        "text_length": len(text),
        "text_preview": text[:160],
        "status": "rejected" if item is None else "accepted",
        "selected_material_card": "",
        "selected_business_card": "",
        "candidate_type": material.span_type,
        "quality_score": "",
        "family_match_score": "",
        "family_match_hint": "",
        "llm_readiness_score": "",
        "task_score": "",
        "material_card_top3": "",
        "business_card_top3": "",
        "leaf_hit": "false",
        "reason": "",
        "reject_stage": str(trace.get("reject_stage") or ""),
        "trace_json": json.dumps(trace, ensure_ascii=False),
    }
    if item is None:
        result["reason"] = str(trace.get("reject_reason") or "pipeline_rejected_or_runtime_gate_failed")
        return result

    selected_material_card = str(item.get("material_card_id") or "")
    selected_business_card = str(item.get("selected_business_card") or "")
    llm_generation_readiness = dict(item.get("llm_generation_readiness") or {})
    llm_family_match_hint = dict(item.get("llm_family_match_hint") or {})
    selected_task_scoring = dict(item.get("selected_task_scoring") or {})
    eligible_material_cards = list(item.get("eligible_material_cards") or [])
    eligible_business_cards = list(item.get("eligible_business_cards") or [])

    result.update(
        {
            "selected_material_card": selected_material_card,
            "selected_business_card": selected_business_card,
            "quality_score": str(item.get("quality_score") or ""),
            "family_match_score": str(llm_family_match_hint.get("score") or ""),
            "family_match_hint": str(llm_family_match_hint.get("decision") or ""),
            "llm_readiness_score": str(llm_generation_readiness.get("score") or ""),
            "task_score": str(selected_task_scoring.get("recommended_score") or selected_task_scoring.get("score") or ""),
            "material_card_top3": json.dumps(
                [
                    {
                        "card_id": card.get("card_id"),
                        "score": card.get("score"),
                    }
                    for card in eligible_material_cards[:3]
                ],
                ensure_ascii=False,
            ),
            "business_card_top3": json.dumps(
                [
                    {
                        "business_card_id": card.get("business_card_id"),
                        "score": card.get("score"),
                    }
                    for card in eligible_business_cards[:3]
                ],
                ensure_ascii=False,
            ),
            "leaf_hit": "true" if selected_material_card == expected_leaf_card else "false",
            "reason": "ok" if selected_material_card == expected_leaf_card else "leaf_mismatch",
        }
    )
    return result


def _build_item_with_trace(
    pipeline: MaterialPipelineV2,
    *,
    material: Any,
    article: Any,
    spec: BatchSpec,
    expectation: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    text = str(getattr(material, "text", "") or "").strip()
    if not text:
        return None, {"reject_stage": "empty_text", "reject_reason": "empty_text"}

    article_context = pipeline._build_material_context(material=material, article=article)
    question_card = (
        pipeline.registry.get_question_card(spec.question_card_id)
        if spec.question_card_id
        else pipeline.registry.get_default_question_card(spec.family_id)
    )
    runtime_binding = question_card.get("runtime_binding", {})
    signal_layer = pipeline.registry.get_signal_layer(spec.family_id)
    material_cards = pipeline.registry.get_material_cards(spec.family_id)
    business_cards = pipeline.registry.get_business_cards(
        spec.family_id,
        runtime_question_type=runtime_binding.get("question_type"),
        runtime_business_subtype=runtime_binding.get("business_subtype"),
    )
    candidate = {
        "candidate_id": str(getattr(material, "id", "")),
        "candidate_type": str(getattr(material, "span_type", "") or "material_span"),
        "text": text,
        "meta": {
            "precomputed_from_material": True,
            "candidate_span_id": str(getattr(material, "candidate_span_id", "") or ""),
            "paragraph_range": [0, max(0, int(getattr(material, "paragraph_count", 1) or 1) - 1)],
            "sentence_range": [0, max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)],
            "source_paragraph_range_original": [
                max(0, int(getattr(material, "start_paragraph", 0) or 0)),
                max(0, int(getattr(material, "end_paragraph", max(0, int(getattr(material, "paragraph_count", 1) or 1) - 1)) or 0)),
            ],
            "source_sentence_range_original": [
                max(0, int(getattr(material, "start_sentence", 0) or 0)),
                max(0, int(getattr(material, "end_sentence", max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)) or 0)),
            ],
            "anchor_adaptation": {
                "adapted": False,
                "reason": "precomputed_material_text",
            },
        },
        "quality_flags": list(getattr(material, "quality_flags", []) or []),
    }
    if spec.family_id == "sentence_fill":
        truth_blank_position = expectation.get("truth_blank_position")
        truth_function_type = expectation.get("truth_function_type")
        if truth_blank_position:
            candidate["meta"]["blank_position"] = truth_blank_position
        if truth_function_type:
            candidate["meta"]["function_type"] = truth_function_type
        # Truth segments for fill-card scoring are already post-cut material units.
        # Evaluate them directly as slot units instead of forcing rediscovery.
        if truth_blank_position and truth_function_type:
            candidate["candidate_type"] = "functional_slot_unit"
            candidate["meta"].update(
                {
                    "unit_type": "functional_slot_unit",
                    "slot_sentence_range": [0, max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)],
                    "slot_context_sentence_range": [0, max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)],
                    "slot_context_paragraph_range": [0, max(0, int(getattr(material, "paragraph_count", 1) or 1) - 1)],
                    "sentence_range": [0, max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)],
                    "blank_value_ready": True,
                    "blank_value_reason": "truth_segment_eval_slot_unit",
                    "planner_source": "truth_segment_eval",
                    "planner_reason": f"truth_fill_slot:{truth_blank_position}:{truth_function_type}",
                    "slot_source_candidate_type": str(getattr(material, "span_type", "") or "closed_span"),
                    "slot_sentence_text": text,
                    "left_context_text": "",
                    "right_context_text": "",
                }
            )
            if "truth_fill_slot_unit" not in candidate["quality_flags"]:
                candidate["quality_flags"].append("truth_fill_slot_unit")
    source_candidate = deepcopy(candidate)
    trace: dict[str, Any] = {
        "initial_candidate_type": candidate["candidate_type"],
    }
    if spec.family_id == "sentence_fill" and spec.enable_fill_bridge:
        bridged_candidate = pipeline._bridge_fill_candidate_to_functional_slot_unit(
            article_context=article_context,
            candidate=candidate,
        )
        if bridged_candidate is not None:
            candidate = bridged_candidate
            trace["bridged_candidate_type"] = candidate.get("candidate_type")
    if spec.family_id == "sentence_order" and spec.enable_order_bridge:
        bridged_candidate = pipeline._bridge_sentence_order_candidate_to_formal_group(
            article_context=article_context,
            candidate=candidate,
        )
        if bridged_candidate is not None:
            candidate = bridged_candidate
            trace["bridged_candidate_type"] = candidate.get("candidate_type")
    if candidate["candidate_type"] == "functional_slot_unit":
        candidate["meta"].update(
            pipeline._hydrate_functional_slot_meta(
                article_context=article_context,
                candidate=candidate,
            )
        )
    neutral_signal_profile, business_feature_profile, llm_signal_resolution = pipeline._resolve_main_card_profiles(
        article_context=article_context,
        candidate=candidate,
        business_family_id=spec.family_id,
        signal_layer=signal_layer,
    )
    signal_profile = pipeline._project_signal_profile(signal_layer=signal_layer, neutral_signal_profile=neutral_signal_profile)
    trace["final_candidate_type"] = candidate.get("candidate_type")
    trace["llm_signal_mode"] = str((llm_signal_resolution or {}).get("mode") or "")

    retrieval_match_profile = pipeline._build_retrieval_match_profile(
        article_context=article_context,
        candidate=candidate,
        query_terms=[],
        target_length=None,
        length_tolerance=120,
    )
    card_hits = pipeline._score_material_cards(
        material_cards=material_cards,
        signal_profile=signal_profile,
        candidate=candidate,
        business_family_id=spec.family_id,
        min_card_score=0.30,
    )
    if not card_hits:
        card_hits = [
            {
                "card_id": f"legacy.{spec.family_id}.precomputed",
                "score": 0.35,
                "generation_archetype": "legacy_material_fallback",
            }
        ]
    business_card_hits = pipeline._score_business_cards(
        business_cards=business_cards,
        business_feature_profile=business_feature_profile,
        neutral_signal_profile=neutral_signal_profile,
        requested_business_card_ids=set(),
        preferred_business_card_ids=set(),
        min_business_card_score=0.25,
    )
    top_hit = card_hits[0]
    top_business_hit = pipeline._select_primary_business_card(business_card_hits, neutral_signal_profile)
    trace["top_material_card_before_llm"] = top_hit.get("card_id")
    trace["top_business_card_before_llm"] = (top_business_hit or {}).get("business_card_id")
    trace["material_card_top3"] = [
        {"card_id": item.get("card_id"), "score": item.get("score")}
        for item in card_hits[:3]
    ]
    trace["business_card_top3"] = [
        {"business_card_id": item.get("business_card_id"), "score": item.get("score")}
        for item in business_card_hits[:3]
    ]

    llm_material_card_options = None
    llm_business_card_options = None
    if pipeline._use_llm_card_catalog_for_family(spec.family_id):
        llm_material_card_options = pipeline._build_llm_material_card_catalog(
            material_cards=material_cards,
            candidate=candidate,
            business_family_id=spec.family_id,
        )
        llm_business_card_options = pipeline._build_llm_business_card_catalog(
            business_cards=business_cards,
            business_feature_profile=business_feature_profile,
        )
        top_hit = pipeline._maybe_promote_legacy_top_hit_from_llm_catalog(
            top_hit=top_hit,
            llm_material_card_options=llm_material_card_options,
        )
        if top_business_hit is None and llm_business_card_options:
            top_business_hit = deepcopy(llm_business_card_options[0])

    if spec.family_id == "sentence_fill" and top_business_hit is None and not pipeline._use_llm_card_catalog_for_family(spec.family_id):
        trace["reject_stage"] = "business_card_precheck"
        trace["reject_reason"] = "sentence_fill_missing_business_card"
        return None, trace

    family_affinity = pipeline._family_affinity_topk(neutral_signal_profile)
    local_profile = dict(signal_profile)
    local_profile["family_affinity_topk"] = family_affinity
    local_profile["distractor_profile"] = pipeline._build_distractor_profile(question_card, top_hit, signal_profile)
    local_profile["business_feature_profile"] = business_feature_profile
    local_profile["retrieval_match_profile"] = retrieval_match_profile
    local_profile["business_card_affinity_topk"] = [
        {
            "business_card_id": item["business_card_id"],
            "score": item["score"],
        }
        for item in business_card_hits[:3]
    ]
    presentation = pipeline._build_presentation(
        business_family_id=spec.family_id,
        article_context=article_context,
        candidate=candidate,
        signal_profile=signal_profile,
    )
    consumable_text = pipeline._build_consumable_text(
        business_family_id=spec.family_id,
        candidate=candidate,
        presentation=presentation,
    )
    item = {
        "candidate_id": candidate["candidate_id"],
        "article_id": article_context["article_id"],
        "article_title": article_context["title"],
        "_business_family_id": spec.family_id,
        "candidate_type": candidate["candidate_type"],
        "material_card_id": top_hit["card_id"],
        "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
        "text": candidate["text"],
        "original_text": candidate["text"],
        "meta": candidate["meta"],
        "consumable_text": consumable_text,
        "presentation": presentation,
        "source": article_context["source"],
        "article_profile": article_context["article_profile"],
        "neutral_signal_profile": neutral_signal_profile,
        "task_scoring": neutral_signal_profile.get("task_scoring", {}),
        "selected_task_scoring": (neutral_signal_profile.get("task_scoring", {}) or {}).get(
            pipeline._task_family_scoring_key(spec.family_id) or "", {}
        ),
        "business_feature_profile": business_feature_profile,
        "retrieval_match_profile": retrieval_match_profile,
        "local_profile": local_profile,
        "family_affinity_topk": family_affinity,
        "eligible_material_cards": card_hits,
        "material_card_recommendations": [item["card_id"] for item in card_hits],
        "eligible_business_cards": business_card_hits,
        "business_card_recommendations": [item["business_card_id"] for item in business_card_hits],
        "preferred_question_cards": [question_card["card_id"]],
        "question_ready_context": {
            "question_card_id": question_card["card_id"],
            "runtime_binding": runtime_binding,
            "selected_material_card": top_hit["card_id"],
            "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
            "generation_archetype": top_hit["generation_archetype"],
            "resolved_slots": pipeline._resolve_slots(question_card, top_hit["card_id"], top_business_hit),
            "pattern_candidates": list((top_business_hit or {}).get("pattern_candidates") or []),
            "prompt_extras": pipeline._build_prompt_extras(top_business_hit),
            "validator_contract": question_card.get("validator_contract", {}),
        },
        "quality_flags": candidate.get("quality_flags", []),
        "quality_score": round(
            pipeline._score_candidate_quality(
                business_family_id=spec.family_id,
                signal_profile=signal_profile,
                top_card_score=top_hit["score"],
                top_business_score=top_business_hit["score"] if top_business_hit else 0.0,
                retrieval_match_score=0.0,
                length_fit_score=0.0,
                candidate=candidate,
                article_context=article_context,
            ),
            4,
        ),
        "_cached_business_family_id": spec.family_id,
        "_cached_index_version": pipeline.INDEX_VERSION,
    }
    if llm_signal_resolution:
        item["llm_signal_resolution"] = llm_signal_resolution
        item["question_ready_context"]["llm_signal_resolution"] = {
            "mode": llm_signal_resolution.get("mode"),
            "consensus_status": ((llm_signal_resolution.get("consensus") or {}).get("status")),
        }
        item["local_profile"]["llm_signal_resolution"] = {
            "enabled": True,
            "consensus_status": ((llm_signal_resolution.get("consensus") or {}).get("status")),
        }
    item = pipeline._attach_main_card_dual_judge_adjudication(
        item=item,
        business_family_id=spec.family_id,
        question_card=question_card,
        material_cards=material_cards,
        business_cards=business_cards,
        signal_profile=signal_profile,
        neutral_signal_profile=neutral_signal_profile,
        business_feature_profile=business_feature_profile,
        llm_material_card_options=llm_material_card_options,
        llm_business_card_options=llm_business_card_options,
    )
    item = pipeline._attach_llm_material_judgments(
        item=item,
        business_family_id=spec.family_id,
    )
    trace["llm_adjudication"] = item.get("llm_adjudication")
    trace["llm_generation_readiness"] = item.get("llm_generation_readiness")
    trace["llm_family_match_hint"] = item.get("llm_family_match_hint")
    if pipeline._llm_adjudication_requires_reject(item=item, business_family_id=spec.family_id):
        trace["reject_stage"] = "llm_adjudication"
        trace["reject_reason"] = "llm_adjudication_requires_reject"
        trace["selected_material_card_after_llm"] = item.get("material_card_id")
        trace["selected_business_card_after_llm"] = item.get("selected_business_card")
        return None, trace
    gate_passed, gate_meta = pipeline._passes_runtime_material_gate(
        item=item,
        business_family_id=spec.family_id,
        question_card=question_card,
        min_card_score=0.0,
        min_business_card_score=0.0,
        require_business_card=False,
    )
    trace["gate_meta"] = gate_meta
    if not gate_passed:
        trace["reject_stage"] = "runtime_gate"
        if isinstance(gate_meta, dict):
            trace["reject_reason"] = str(gate_meta.get("reason") or "runtime_gate_failed")
        elif gate_meta:
            trace["reject_reason"] = str(gate_meta)
        else:
            trace["reject_reason"] = "runtime_gate_failed"
        return None, trace
    trace["reject_stage"] = ""
    trace["reject_reason"] = ""
    return item, trace


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["split"]), str(row["batch_dir"]))
        bucket = buckets.setdefault(
            key,
            {
                "split": row["split"],
                "batch_dir": row["batch_dir"],
                "family_id": row["family_id"],
                "expected_leaf_cards": set(),
                "sample_count": 0,
                "accepted_count": 0,
                "rejected_count": 0,
                "leaf_hit_count": 0,
            },
        )
        if row["expected_leaf_card"]:
            bucket["expected_leaf_cards"].add(row["expected_leaf_card"])
        bucket["sample_count"] += 1
        if row["status"] == "accepted":
            bucket["accepted_count"] += 1
        else:
            bucket["rejected_count"] += 1
        if row["leaf_hit"] == "true":
            bucket["leaf_hit_count"] += 1
    summary_rows: list[dict[str, Any]] = []
    for bucket in buckets.values():
        sample_count = int(bucket["sample_count"])
        accepted_count = int(bucket["accepted_count"])
        leaf_hit_count = int(bucket["leaf_hit_count"])
        expected_leaf_cards = sorted(bucket.pop("expected_leaf_cards"))
        bucket["expected_leaf_card"] = ", ".join(expected_leaf_cards) if expected_leaf_cards else ""
        bucket["accept_rate"] = round(accepted_count / sample_count, 4) if sample_count else 0.0
        bucket["leaf_hit_rate"] = round(leaf_hit_count / sample_count, 4) if sample_count else 0.0
        bucket["leaf_hit_given_accept_rate"] = round(leaf_hit_count / accepted_count, 4) if accepted_count else 0.0
        summary_rows.append(bucket)
    summary_rows.sort(key=lambda item: (item["split"], item["family_id"], item["batch_dir"]))
    return summary_rows


def _write_markdown(path: Path, summary_rows: list[dict[str, Any]], detail_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Truth Segment Card Eval",
        "",
        "## Summary",
        "",
        "| split | batch | family | expected leaf | samples | accept | reject | leaf hits | accept_rate | leaf_hit_rate | leaf_hit_given_accept_rate |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row['split']} | {row['batch_dir']} | {row['family_id']} | {row['expected_leaf_card']} | "
            f"{row['sample_count']} | {row['accepted_count']} | {row['rejected_count']} | {row['leaf_hit_count']} | "
            f"{row['accept_rate']:.4f} | {row['leaf_hit_rate']:.4f} | {row['leaf_hit_given_accept_rate']:.4f} |"
        )
    lines.extend(["", "## Worst Mismatches", ""])
    mismatches = [
        row for row in detail_rows
        if row["status"] != "accepted" or row["leaf_hit"] != "true"
    ]
    mismatches = sorted(
        mismatches,
        key=lambda row: (
            row["split"],
            row["family_id"],
            row["batch_dir"],
            row["status"] == "accepted",
        ),
    )[:40]
    for row in mismatches:
        lines.extend(
            [
                f"### {row['split']} | {row['sample_id']}",
                "",
                f"- batch: `{row['batch_dir']}`",
                f"- family: `{row['family_id']}`",
                f"- expected_leaf_card: `{row['expected_leaf_card']}`",
                f"- selected_material_card: `{row['selected_material_card']}`",
                f"- selected_business_card: `{row['selected_business_card']}`",
                f"- status: `{row['status']}`",
                f"- reason: `{row['reason']}`",
                f"- pattern_tag: `{row['pattern_tag']}`",
                f"- text_preview: {row['text_preview']}",
                "",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--holdout-ratio", type=float, default=0.2)
    parser.add_argument("--limit-per-batch", type=int, default=0)
    parser.add_argument("--limit-per-group", type=int, default=0)
    parser.add_argument("--sample-manifest", type=str, default="")
    parser.add_argument("--sample-split", type=str, default="all", choices=("all", "train", "holdout", "negative_probe"))
    parser.add_argument("--tag", type=str, default="baseline")
    parser.add_argument("--batch-filter", type=str, default="")
    args = parser.parse_args()

    _load_llm_env()
    pipeline = MaterialPipelineV2()
    sample_lookup = _load_sample_manifest(Path(args.sample_manifest)) if args.sample_manifest else {}

    rows: list[dict[str, Any]] = []
    for spec in BATCH_SPECS:
        if args.batch_filter and args.batch_filter not in spec.batch_dir:
            continue
        batch_dir = DISTILL_BATCHES_DIR / spec.batch_dir
        batch_rows = _iter_truth_rows(batch_dir)
        batch_rows.sort(key=lambda row: str(row.get("sample_id") or row.get("question_id") or ""))
        batch_sample_lookup = sample_lookup.get(spec.batch_dir, {})
        if batch_sample_lookup:
            batch_rows = [
                row for row in batch_rows
                if _row_key(row) in batch_sample_lookup
            ]
        batch_rows = _limit_rows_per_group(batch_rows, args.limit_per_group)
        if args.limit_per_batch > 0:
            batch_rows = batch_rows[: args.limit_per_batch]
        for row in batch_rows:
            sample_id = str(row.get("sample_id") or "")
            sample_meta: dict[str, Any] | None = None
            if batch_sample_lookup:
                sample_meta = batch_sample_lookup.get(_row_key(row), {})
                split = str((sample_meta or {}).get("split") or "")
                if args.sample_split != "all" and split != args.sample_split:
                    continue
            else:
                split = _stable_split(sample_id, args.holdout_ratio)
            rows.append(
                _evaluate_row(
                    pipeline=pipeline,
                    row=row,
                    spec=spec,
                    split=split,
                    sample_meta=sample_meta,
                )
            )

    out_dir = REPORTS_DIR / "truth_segment_card_eval"
    out_dir.mkdir(parents=True, exist_ok=True)
    detail_path = out_dir / f"truth_segment_card_eval_{args.tag}.csv"
    summary_path = out_dir / f"truth_segment_card_eval_{args.tag}_summary.csv"
    md_path = out_dir / f"truth_segment_card_eval_{args.tag}.md"

    summary_rows = _build_summary(rows)
    _write_csv(detail_path, rows)
    _write_csv(summary_path, summary_rows)
    _write_markdown(md_path, summary_rows, rows)

    print(json.dumps({
        "detail_path": str(detail_path),
        "summary_path": str(summary_path),
        "markdown_path": str(md_path),
        "row_count": len(rows),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
