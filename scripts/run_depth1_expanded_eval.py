from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
MANIFEST_PATH = ROOT / "test" / "material_card_eval_assets" / "depth1_expanded_split_manifest.yaml"
REPORTS_DIR = ROOT / "reports" / "pressure_tests" / "expanded_eval_runs"


ACCEPTABLE_CARD_GROUPS: dict[str, list[set[str]]] = {
    "center_understanding": [
        {
            "center_material.relation_plain",
            "center_material.subsentence_other",
            "center_material.subsentence_data",
            "center_material.subsentence_prelude",
        },
        {
            "center_material.relation_variant",
            "center_material.subsentence_example",
            "center_material.subsentence_multi_angle",
        },
        {
            "center_material.relation_turning",
            "center_material.subsentence_prelude",
            "center_material.subsentence_example",
        },
        {
            "center_material.relation_parallel",
            "center_material.subsentence_multi_angle",
            "center_material.subsentence_data",
        },
        {
            "center_material.relation_countermeasure",
        },
    ],
    "sentence_order": [
        {
            "order_material.dual_anchor_lock",
            "order_material.first_sentence_gate",
            "order_material.tail_sentence_gate",
        },
        {
            "order_material.timeline_progression",
            "order_material.first_sentence_gate",
        },
        {
            "order_material.carry_parallel_expand",
            "order_material.timeline_progression",
        },
        {
            "order_material.viewpoint_reason_action",
            "order_material.problem_solution_case_blocks",
        },
    ],
    "sentence_fill": [
        {
            "fill_material.opening_summary",
            "fill_material.opening_topic_intro",
        },
        {
            "fill_material.middle_focus_shift",
            "fill_material.bridge_transition",
        },
        {
            "fill_material.ending_clause_summary",
            "fill_material.ending_countermeasure",
        },
    ],
}

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Depth1 expanded article evaluator.")
    parser.add_argument("--manifest-yaml", type=str, default=str(MANIFEST_PATH))
    parser.add_argument("--batch-id", type=str, default="")
    parser.add_argument("--batch-prefix", type=str, default="")
    parser.add_argument("--max-samples", type=int, default=0)
    parser.add_argument("--candidate-limit", type=int, default=12)
    parser.add_argument("--min-card-score", type=float, default=0.45)
    parser.add_argument("--min-business-card-score", type=float, default=0.2)
    parser.add_argument("--output-dir", type=str, default=str(REPORTS_DIR))
    return parser.parse_args()


def _load_rows_from_manifest(
    *,
    manifest_yaml: str,
    batch_id: str,
    batch_prefix: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    payload = yaml.safe_load(Path(manifest_yaml).read_text(encoding="utf-8"))
    execution_batches = dict(payload.get("execution_batches") or {})
    selected_batch_ids: list[str] = []
    for current_batch_id in execution_batches:
        if batch_id and current_batch_id != batch_id:
            continue
        if batch_prefix and not current_batch_id.startswith(batch_prefix):
            continue
        selected_batch_ids.append(current_batch_id)
    if not selected_batch_ids and batch_id:
        raise ValueError(f"batch_id not found: {batch_id}")
    if not selected_batch_ids and batch_prefix:
        raise ValueError(f"batch_prefix matched no batches: {batch_prefix}")

    rows: list[dict[str, Any]] = []
    for current_batch_id in selected_batch_ids:
        batch_payload = dict(execution_batches.get(current_batch_id) or {})
        for row in (batch_payload.get("rows") or []):
            merged = dict(row)
            merged["execution_batch_id"] = current_batch_id
            merged["execution_split"] = batch_payload.get("split")
            merged["execution_batch_kind"] = batch_payload.get("batch_kind")
            rows.append(merged)
    return rows, selected_batch_ids


def normalize_space(text: str) -> str:
    body = str(text or "").replace("[BLANK]", "")
    body = re.sub(r"[\s，。；、：:,.!?！？“”\"'‘’（）()【】\[\]<>《》…—-]+", "", body)
    return body.strip()


def loosely_contains(haystack: str, needle: str) -> bool:
    h = normalize_space(haystack)
    n = normalize_space(needle)
    if not h or not n:
        return False
    if n in h:
        return True
    if len(n) < 30:
        return False
    return n[:20] in h and n[-20:] in h


def clip(text: str, limit: int = 180) -> str:
    body = str(text or "").strip()
    if len(body) <= limit:
        return body
    return body[:limit].rstrip() + "..."


def _acceptable_material_card_ids(
    *,
    business_family_id: str,
    expected_material_card_id: str,
) -> list[str]:
    expected = str(expected_material_card_id or "").strip()
    if not expected:
        return []
    for group in ACCEPTABLE_CARD_GROUPS.get(str(business_family_id or "").strip(), []):
        if expected in group:
            return sorted(group)
    return [expected]


def _sentence_chunks(text: str) -> list[str]:
    body = str(text or "").strip()
    if not body:
        return []
    chunks = re.split(r"(?<=[。！？!?])", body)
    return [chunk.strip() for chunk in chunks if chunk and chunk.strip()]


def _soft_chunk_signature(text: str) -> str:
    body = str(text or "").strip()
    body = re.sub(r"^[，。；、：:,.!?！？\s]+", "", body)
    body = re.sub(r"^(进一步看|进一步说|换句话说)[，,:：]?", "", body)
    return normalize_space(body)


def _collapse_adjacent_repeated_chunks(chunks: list[str]) -> list[str]:
    if not chunks:
        return []
    current = list(chunks)
    changed = True
    while changed and len(current) >= 2:
        changed = False
        for start in range(0, len(current) - 1):
            max_window = (len(current) - start) // 2
            for window in range(max_window, 0, -1):
                left = [_soft_chunk_signature(item) for item in current[start : start + window]]
                right = [_soft_chunk_signature(item) for item in current[start + window : start + window * 2]]
                if left and left == right:
                    current = current[: start + window] + current[start + window * 2 :]
                    changed = True
                    break
            if changed:
                break
    collapsed: list[str] = []
    last_norm = ""
    for chunk in current:
        norm = _soft_chunk_signature(chunk)
        if norm and norm == last_norm:
            continue
        collapsed.append(chunk)
        last_norm = norm
    return collapsed


def _dedupe_redundant_chunks(chunks: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: list[str] = []
    for chunk in chunks:
        norm = _soft_chunk_signature(chunk)
        if not norm:
            continue
        duplicate = False
        for previous in seen:
            if norm == previous:
                duplicate = True
                break
            if min(len(norm), len(previous)) >= 18 and (norm in previous or previous in norm):
                duplicate = True
                break
        if duplicate:
            continue
        deduped.append(chunk.strip())
        seen.append(norm)
    return deduped


def _trim_incomplete_tail(text: str) -> str:
    body = str(text or "").strip()
    if not body:
        return body
    if re.search(r"[。！？!?；;]$", body):
        return body
    matches = list(re.finditer(r"[。！？!?；;]", body))
    if not matches:
        return body
    last_terminal = matches[-1].end()
    if last_terminal >= max(24, int(len(body) * 0.55)):
        return body[:last_terminal].strip()
    return body


def _strip_fill_prompt_residue(text: str) -> str:
    body = str(text or "").strip()
    if not body:
        return body
    stems = (
        "填入画线处最恰当的一句是",
        "填入横线处最恰当的一句是",
        "下列各项最适合填入横线处的句子是",
        "下列各项最适合填入横线的句子是",
        "文中括号处应引用的句子是",
    )
    for stem in stems:
        body = body.replace(stem, "")
    body = re.sub(r"[。！？!?]{2,}", "。", body)
    body = re.sub(r"\s+", "", body)
    return body.strip()


def _normalize_expanded_article_text(text: str) -> str:
    body = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    body = re.sub(r"^[，。；、：:,.!?！？\s]+", "", body)
    body = re.sub(r"(进一步看|进一步说|换句话说)[，,:：]?", "", body)
    chunks = _sentence_chunks(body)
    if not chunks:
        return _trim_incomplete_tail(body)
    chunks = _collapse_adjacent_repeated_chunks(chunks)
    chunks = _dedupe_redundant_chunks(chunks)
    normalized = "".join(chunks).strip()
    normalized = re.sub(r"^[，。；、：:,.!?！？\s]+", "", normalized)
    normalized = _trim_incomplete_tail(normalized or body)
    return normalized or body


def _normalize_sentence_order_article_text(text: str) -> str:
    body = str(text or "").strip()
    if not body:
        return body
    punctuation_count = len(re.findall(r"[。！？!?；;]", body))
    whitespace_count = len(re.findall(r"\s+", body))
    if punctuation_count == 0 and whitespace_count >= 3:
        body = re.sub(
            r"(?<=[\u4e00-\u9fff0-9）)】」』”])\s+(?=[\u4e00-\u9fff0-9（(【「『“])",
            "。",
            body,
        )
        body = re.sub(r"\s{2,}", "。", body)
        body = re.sub(r"\s+", " ", body)
    return body


def _repair_expanded_article_text(
    *,
    article_text: str,
    original_text: str,
    business_family_id: str,
) -> str:
    original = _trim_incomplete_tail(_normalize_expanded_article_text(original_text))
    body = _normalize_expanded_article_text(article_text)
    if business_family_id == "sentence_fill":
        original = _strip_fill_prompt_residue(original)
        body = _strip_fill_prompt_residue(body)
    if business_family_id == "sentence_order":
        original = _normalize_sentence_order_article_text(original)
        body = _normalize_sentence_order_article_text(body)

    if not body:
        return original or body

    body_chunks = _dedupe_redundant_chunks(_collapse_adjacent_repeated_chunks(_sentence_chunks(body)))
    if body_chunks:
        first_chunk = body_chunks[0].strip()
        first_head = re.sub(r"^[，。；、：:,.!?！？\s]+", "", first_chunk)[:12]
        last_chunk = body_chunks[-1].strip()
        if first_head and last_chunk.count(first_head) >= 1:
            echo_index = last_chunk.find(first_head)
            if echo_index >= max(10, len(last_chunk) // 3):
                trimmed_last = last_chunk[:echo_index].strip("，。；、：:,.!?！？ \n")
                if trimmed_last:
                    body_chunks[-1] = trimmed_last + "。"
                else:
                    body_chunks = body_chunks[:-1]
        last_signature = _soft_chunk_signature(body_chunks[-1])
        if last_signature and any(
            last_signature != _soft_chunk_signature(chunk)
            and last_signature in _soft_chunk_signature(chunk)
            for chunk in body_chunks[:-1]
        ):
            body_chunks = body_chunks[:-1]
            body = "".join(body_chunks).strip()

    if original and not loosely_contains(body, original):
        body_space = normalize_space(body)
        original_space = normalize_space(original)
        body_chunk_count = len(_sentence_chunks(body))
        original_chunk_count = len(_sentence_chunks(original))
        if (
            len(body_space) < max(48, int(len(original_space) * 1.15))
            or body_chunk_count <= max(1, original_chunk_count)
            or len(body_space) <= len(original_space)
        ):
            return original

    return body or original


def _extract_segment_payload(item: dict[str, Any]) -> dict[str, Any]:
    meta = dict(item.get("meta") or {})
    presentation = dict(item.get("presentation") or {})
    consumable_text = str(item.get("consumable_text") or "").strip()
    raw_text = str(item.get("text") or "").strip()
    chosen_text = raw_text or consumable_text
    return {
        "segment_text": chosen_text,
        "segment_text_preview": clip(chosen_text),
        "segment_source": "text" if raw_text else "consumable_text",
        "paragraph_range": meta.get("paragraph_range"),
        "sentence_range": meta.get("sentence_range"),
        "source_paragraph_range_original": meta.get("source_paragraph_range_original"),
        "source_sentence_range_original": meta.get("source_sentence_range_original"),
        "blanked_text_preview": clip(str(presentation.get("blanked_text") or "")) if presentation.get("blanked_text") else "",
    }


def _make_article(row: dict[str, Any]) -> SimpleNamespace:
    business_family_id = str(row.get("business_family_id") or "")
    article_text = _repair_expanded_article_text(
        article_text=str(row.get("article_text") or ""),
        original_text=str(row.get("original_text") or ""),
        business_family_id=business_family_id,
    )
    return SimpleNamespace(
        id=f"depth1_expanded::{row.get('sample_id')}",
        title=f"{business_family_id}::{row.get('child_family_id')}",
        source="depth1_expanded_truth",
        source_url=None,
        domain="truth.local",
        clean_text=article_text,
        raw_text=article_text,
    )


def _clean_nested(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): cleaned
            for key, item in value.items()
            if (cleaned := _clean_nested(item)) not in (None, "", [], {})
        }
    if isinstance(value, list):
        return [cleaned for item in value if (cleaned := _clean_nested(item)) not in (None, "", [], {})]
    return value


def _build_structure_constraints(row: dict[str, Any]) -> dict[str, Any]:
    question_card_features = dict(row.get("question_card_features") or {})
    base_subset = dict(question_card_features.get("base_slots_subset") or {})
    target_override = dict(question_card_features.get("target_material_override") or {})
    constraints: dict[str, Any] = {}
    constraints.update(base_subset)
    constraints.update(target_override)

    business_family_id = str(row.get("business_family_id") or "")
    truth_blank_position = str(row.get("truth_blank_position") or "").strip()
    truth_function_type = str(row.get("truth_function_type") or "").strip()

    if business_family_id == "sentence_fill":
        if truth_blank_position:
            constraints["blank_position"] = truth_blank_position
            constraints["preserve_blank_position"] = True
        if truth_function_type:
            constraints["function_type"] = truth_function_type
        constraints.setdefault("semantic_scope", target_override.get("semantic_scope") or base_subset.get("semantic_scope"))
        constraints.setdefault("logic_relation", target_override.get("logic_relation") or base_subset.get("logic_relation"))
    elif business_family_id == "sentence_order":
        if str(row.get("child_family_id") or "") == "sentence_order_fixed_bundle":
            constraints["preserve_unit_count"] = True
        constraints.setdefault("block_order_complexity", target_override.get("block_order_complexity") or base_subset.get("block_order_complexity"))
    elif business_family_id == "center_understanding":
        constraints.setdefault("argument_structure", target_override.get("argument_structure") or base_subset.get("argument_structure"))
        constraints.setdefault("main_axis_source", target_override.get("main_axis_source") or base_subset.get("main_axis_source"))
        constraints.setdefault("abstraction_level", target_override.get("abstraction_level") or base_subset.get("abstraction_level"))

    return _clean_nested(constraints) or {}


def _build_search_kwargs(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    structure_constraints = _build_structure_constraints(row)
    return {
        "articles": [_make_article(row)],
        "business_family_id": str(row.get("business_family_id") or ""),
        "question_card_id": row.get("question_card_id"),
        "candidate_limit": max(1, int(args.candidate_limit)),
        "min_card_score": float(args.min_card_score),
        "min_business_card_score": float(args.min_business_card_score),
        "target_length": max(300, len(str(row.get("article_text") or ""))),
        "length_tolerance": 220,
        "enable_anchor_adaptation": True,
        "preserve_anchor": True,
        "structure_constraints": structure_constraints,
    }


def _debug_runtime_path(
    *,
    service: MaterialPipelineV2Service,
    row: dict[str, Any],
    article_context: dict[str, Any],
    derived_candidates: list[dict[str, Any]],
    search_kwargs: dict[str, Any],
) -> dict[str, Any]:
    pipeline = service.pipeline
    business_family_id = str(row.get("business_family_id") or "")
    question_card = pipeline.registry.get_question_card(row.get("question_card_id"))
    runtime_binding = question_card.get("runtime_binding", {})
    signal_layer = pipeline.registry.get_signal_layer(business_family_id)
    material_cards = pipeline.registry.get_material_cards(business_family_id)
    business_cards = pipeline.registry.get_business_cards(
        business_family_id,
        runtime_question_type=runtime_binding.get("question_type"),
        runtime_business_subtype=runtime_binding.get("business_subtype"),
    )
    reason_counts: dict[str, int] = defaultdict(int)
    preview: list[dict[str, Any]] = []

    for candidate in derived_candidates:
        resolved_candidate = pipeline._adapt_candidate_window(
            article_context=article_context,
            candidate=candidate,
            target_length=search_kwargs["target_length"],
            length_tolerance=search_kwargs["length_tolerance"],
            enable_anchor_adaptation=search_kwargs["enable_anchor_adaptation"],
            preserve_anchor=search_kwargs["preserve_anchor"],
        )
        item = pipeline._build_runtime_search_item(
            article_context=article_context,
            candidate=resolved_candidate,
            source_candidate=candidate,
            business_family_id=business_family_id,
            question_card=question_card,
            runtime_binding=runtime_binding,
            signal_layer=signal_layer,
            material_cards=material_cards,
            business_cards=business_cards,
            requested_business_card_ids=set(),
            preferred_business_card_ids=set(),
            normalized_query_terms=[],
            normalized_structure_constraints=search_kwargs.get("structure_constraints") or {},
            target_length=search_kwargs["target_length"],
            length_tolerance=search_kwargs["length_tolerance"],
            topic=None,
            text_direction=None,
            document_genre=None,
            material_structure_label=None,
        )
        candidate_id = str(candidate.get("candidate_id") or "")
        candidate_type = str(candidate.get("candidate_type") or "")
        if item is None:
            reason = "front_filter_or_item_build_reject"
            reason_counts[reason] += 1
            preview.append({"candidate_id": candidate_id, "candidate_type": candidate_type, "reason": reason})
            continue
        if pipeline._llm_adjudication_requires_reject(item=item, business_family_id=business_family_id):
            reason = "llm_adjudication_rejected"
            reason_counts[reason] += 1
            adjudication = dict(item.get("llm_adjudication") or {})
            consensus = dict(adjudication.get("consensus") or {})
            preview.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_type": candidate_type,
                    "reason": reason,
                    "top_card_score": pipeline._top_card_score(item),
                    "top_business_card_score": pipeline._top_business_card_score(item),
                    "quality_score": item.get("quality_score"),
                    "structure_match_score": ((item.get("local_profile") or {}).get("structure_match_score")),
                    "retrieval_match_score": ((item.get("retrieval_match_profile") or {}).get("match_score")),
                    "selected_material_card": ((item.get("question_ready_context") or {}).get("selected_material_card")),
                    "selected_business_card": ((item.get("question_ready_context") or {}).get("selected_business_card")),
                    "llm_decision": consensus.get("decision"),
                    "llm_formal_layer": consensus.get("formal_layer"),
                    "llm_reason": consensus.get("reason"),
                    "llm_evidence_summary": consensus.get("evidence_summary"),
                }
            )
            continue
        gate_passed, gate_reason = pipeline._passes_runtime_material_gate(
            item=item,
            business_family_id=business_family_id,
            question_card=question_card,
            min_card_score=search_kwargs["min_card_score"],
            min_business_card_score=search_kwargs["min_business_card_score"],
            require_business_card=False,
        )
        reason = "accepted_preview" if gate_passed else (gate_reason or "unknown_gate_reject")
        reason_counts[reason] += 1
        preview.append(
            {
                "candidate_id": candidate_id,
                "candidate_type": candidate_type,
                "reason": reason,
                "material_card_id": item.get("material_card_id"),
                "selected_material_card": ((item.get("question_ready_context") or {}).get("selected_material_card")),
                "selected_business_card": ((item.get("question_ready_context") or {}).get("selected_business_card")),
            }
        )

    return {
        "reason_counts": dict(sorted(reason_counts.items())),
        "preview": preview[:8],
    }


def _evaluate_row(
    *,
    service: MaterialPipelineV2Service,
    row: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    search_kwargs = _build_search_kwargs(row, args)
    article = search_kwargs["articles"][0]
    original_text = _repair_expanded_article_text(
        article_text=str(row.get("original_text") or ""),
        original_text=str(row.get("original_text") or ""),
        business_family_id=str(row.get("business_family_id") or ""),
    )
    article_context = service.pipeline._build_article_context(article)
    derived_candidates = service.pipeline._derive_candidates(
        article_context=article_context,
        business_family_id=str(row.get("business_family_id") or ""),
    )
    runtime_debug = _debug_runtime_path(
        service=service,
        row=row,
        article_context=article_context,
        derived_candidates=derived_candidates,
        search_kwargs=search_kwargs,
    )
    response = service.pipeline.search(**search_kwargs)
    items = response.get("items") or []
    top = items[0] if items else {}
    qrc = top.get("question_ready_context") or {}
    extracted_segments = [_extract_segment_payload(item) for item in items]
    top_segment = extracted_segments[0] if extracted_segments else {}
    contains_original_material = any(loosely_contains(str(seg.get("segment_text") or ""), original_text) for seg in extracted_segments)
    top_contains_original_material = loosely_contains(str(top_segment.get("segment_text") or ""), original_text)
    selected_material_card = str(qrc.get("selected_material_card") or "")
    expected_material_card_id = str(row.get("expected_material_card_id") or "")
    acceptable_material_card_ids = _acceptable_material_card_ids(
        business_family_id=str(row.get("business_family_id") or ""),
        expected_material_card_id=expected_material_card_id,
    )
    strict_hit = bool(expected_material_card_id and selected_material_card == expected_material_card_id)
    acceptable_hit = bool(selected_material_card and selected_material_card in acceptable_material_card_ids)
    segment_emitted = bool(str(top_segment.get("segment_text") or "").strip())
    return {
        "sample_id": row.get("sample_id"),
        "split": row.get("execution_split"),
        "execution_batch_id": row.get("execution_batch_id"),
        "business_family_id": row.get("business_family_id"),
        "child_family_id": row.get("child_family_id"),
        "question_card_id": row.get("question_card_id"),
        "expected_material_card_id": expected_material_card_id,
        "pattern_tag": row.get("pattern_tag"),
        "subfamily": row.get("subfamily"),
        "derived_candidate_count": len(derived_candidates),
        "derived_candidate_types": sorted({str(item.get("candidate_type") or "") for item in derived_candidates}),
        "candidate_count": len(items),
        "segment_emitted": segment_emitted,
        "ingest_success": bool(items and selected_material_card and segment_emitted),
        "strict_hit": strict_hit,
        "acceptable_hit": acceptable_hit,
        "direction_hit": strict_hit,
        "slice_hit_any": contains_original_material,
        "slice_hit_top": top_contains_original_material,
        "structure_constraints": search_kwargs.get("structure_constraints") or {},
        "runtime_debug": runtime_debug,
        "top_selected_material_card": selected_material_card,
        "acceptable_material_card_ids": acceptable_material_card_ids,
        "top_selected_business_card": qrc.get("selected_business_card"),
        "top_candidate_type": top.get("candidate_type"),
        "top_card_score": top.get("selected_card_score"),
        "top_business_card_score": top.get("selected_business_card_score"),
        "top_segment_source": top_segment.get("segment_source"),
        "top_segment_preview": top_segment.get("segment_text_preview"),
        "top_segment_paragraph_range": top_segment.get("paragraph_range"),
        "top_segment_sentence_range": top_segment.get("sentence_range"),
        "top_segment_source_paragraph_range_original": top_segment.get("source_paragraph_range_original"),
        "top_segment_source_sentence_range_original": top_segment.get("source_sentence_range_original"),
        "top_blanked_text_preview": top_segment.get("blanked_text_preview"),
        "top_text_preview": clip(top.get("text") or ""),
        "warnings": response.get("warnings") or [],
    }


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def main() -> int:
    args = parse_args()
    rows, selected_batch_ids = _load_rows_from_manifest(
        manifest_yaml=args.manifest_yaml,
        batch_id=args.batch_id,
        batch_prefix=args.batch_prefix,
    )
    if args.max_samples > 0:
        rows = rows[: args.max_samples]
    if not rows:
        print("[depth1-expanded] no rows selected")
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    init_db()
    load_plugins()
    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        result_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            result_rows.append(_evaluate_row(service=service, row=row, args=args))
            if idx == 1 or idx % 5 == 0 or idx == len(rows):
                print(f"[depth1-expanded] progress {idx}/{len(rows)}")

        total = len(result_rows)
        summary = {
            "total_samples": total,
            "selected_batch_ids": selected_batch_ids,
            "segment_emitted_count": sum(1 for row in result_rows if row["segment_emitted"]),
            "ingest_success_count": sum(1 for row in result_rows if row["ingest_success"]),
            "strict_hit_count": sum(1 for row in result_rows if row["strict_hit"]),
            "acceptable_hit_count": sum(1 for row in result_rows if row["acceptable_hit"]),
            "direction_hit_count": sum(1 for row in result_rows if row["direction_hit"]),
            "slice_hit_any_count": sum(1 for row in result_rows if row["slice_hit_any"]),
            "slice_hit_top_count": sum(1 for row in result_rows if row["slice_hit_top"]),
        }
        summary["segment_emitted_rate"] = _rate(summary["segment_emitted_count"], total)
        summary["ingest_success_rate"] = _rate(summary["ingest_success_count"], total)
        summary["strict_hit_rate"] = _rate(summary["strict_hit_count"], total)
        summary["acceptable_hit_rate"] = _rate(summary["acceptable_hit_count"], total)
        summary["direction_hit_rate"] = _rate(summary["direction_hit_count"], total)
        summary["slice_hit_any_rate"] = _rate(summary["slice_hit_any_count"], total)
        summary["slice_hit_top_rate"] = _rate(summary["slice_hit_top_count"], total)

        batch_summaries: dict[str, dict[str, Any]] = {}
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in result_rows:
            grouped[str(row["execution_batch_id"])].append(row)
        for batch_id, batch_rows in sorted(grouped.items()):
            count = len(batch_rows)
            batch_summaries[batch_id] = {
                "row_count": count,
                "split": batch_rows[0]["split"],
                "business_family_id": batch_rows[0]["business_family_id"],
                "child_families": sorted({str(item.get("child_family_id") or "") for item in batch_rows}),
                "segment_emitted_rate": _rate(sum(1 for item in batch_rows if item["segment_emitted"]), count),
                "ingest_success_rate": _rate(sum(1 for item in batch_rows if item["ingest_success"]), count),
                "strict_hit_rate": _rate(sum(1 for item in batch_rows if item["strict_hit"]), count),
                "acceptable_hit_rate": _rate(sum(1 for item in batch_rows if item["acceptable_hit"]), count),
                "direction_hit_rate": _rate(sum(1 for item in batch_rows if item["direction_hit"]), count),
                "slice_hit_any_rate": _rate(sum(1 for item in batch_rows if item["slice_hit_any"]), count),
                "slice_hit_top_rate": _rate(sum(1 for item in batch_rows if item["slice_hit_top"]), count),
            }

        payload = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "args": vars(args),
            "summary": summary,
            "batch_summaries": batch_summaries,
            "rows": result_rows,
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"depth1_expanded_eval_{ts}.json"
        md_path = output_dir / f"depth1_expanded_eval_{ts}.md"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        md_lines = [
            "# Depth1 Expanded Eval Report",
            "",
            f"- total_samples: `{summary['total_samples']}`",
            f"- segment_emitted_rate: `{summary['segment_emitted_rate']}`",
            f"- ingest_success_rate: `{summary['ingest_success_rate']}`",
            f"- strict_hit_rate: `{summary['strict_hit_rate']}`",
            f"- acceptable_hit_rate: `{summary['acceptable_hit_rate']}`",
            f"- direction_hit_rate: `{summary['direction_hit_rate']}`",
            f"- slice_hit_any_rate: `{summary['slice_hit_any_rate']}`",
            f"- slice_hit_top_rate: `{summary['slice_hit_top_rate']}`",
            "",
            "## Batch Summaries",
        ]
        for batch_id, batch_summary in batch_summaries.items():
            md_lines.append(
                "- "
                f"`{batch_id}` "
                f"rows=`{batch_summary['row_count']}` "
                f"segment=`{batch_summary['segment_emitted_rate']}` "
                f"ingest=`{batch_summary['ingest_success_rate']}` "
                f"strict=`{batch_summary['strict_hit_rate']}` "
                f"acceptable=`{batch_summary['acceptable_hit_rate']}` "
                f"direction=`{batch_summary['direction_hit_rate']}` "
                f"slice_any=`{batch_summary['slice_hit_any_rate']}` "
                f"slice_top=`{batch_summary['slice_hit_top_rate']}`"
            )
        md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
        print(f"[depth1-expanded] report_json={json_path}")
        print(f"[depth1-expanded] report_md={md_path}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
