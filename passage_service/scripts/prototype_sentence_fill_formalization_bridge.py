from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from sqlalchemy import select  # noqa: E402

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402
from app.services.material_pipeline_v2 import (  # noqa: E402
    ACTION_MEASURE_MARKERS,
    CONCLUSION_MARKERS,
    COUNTERMEASURE_MARKERS,
    SUMMARY_MARKERS,
    MaterialPipelineV2,
)


FAMILY = "sentence_fill"
SOURCE_TYPES = {
    "sentence_group",
    "multi_paragraph_unit",
    "paragraph_window",
    "sentence_block_group",
}


def _clip(text: str, limit: int = 120) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit]}..."


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _candidate_seed(material: MaterialSpanORM) -> dict[str, Any]:
    paragraph_count = max(1, int(material.paragraph_count or 1))
    sentence_count = max(1, int(material.sentence_count or 1))
    return {
        "candidate_id": str(material.id),
        "candidate_type": str(material.span_type or "material_span"),
        "text": _safe_str(material.text),
        "meta": {
            "precomputed_from_material": True,
            "candidate_span_id": str(material.candidate_span_id or ""),
            "paragraph_range": [0, max(0, paragraph_count - 1)],
            "sentence_range": [0, max(0, sentence_count - 1)],
            "source_paragraph_range_original": [0, max(0, paragraph_count - 1)],
            "source_sentence_range_original": [0, max(0, sentence_count - 1)],
        },
        "quality_flags": list(material.quality_flags or []),
    }


def _bridge_probe(
    *,
    pipeline: MaterialPipelineV2,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    source_type = _safe_str(candidate.get("candidate_type"))
    if source_type == "functional_slot_unit":
        return {"bridgeable": True, "reason": "already_functional_slot_unit", "best": None, "reject_counts": {}}
    if source_type not in SOURCE_TYPES:
        return {"bridgeable": False, "reason": "unsupported_source_type", "best": None, "reject_counts": {}}

    source_text = _safe_str(candidate.get("text"))
    sentences = [s.strip() for s in pipeline.sentence_splitter.split(source_text) if s.strip()]
    if len(sentences) < 2:
        return {"bridgeable": False, "reason": "too_few_sentences", "best": None, "reject_counts": {}}

    rejects = Counter()
    proposals: list[dict[str, Any]] = []
    total_sentences = len(sentences)
    for sentence_index, sentence in enumerate(sentences):
        slot_role = "middle"
        if sentence_index == 0:
            slot_role = "opening"
        elif sentence_index == total_sentences - 1:
            slot_role = "ending"

        slot_function = pipeline._infer_functional_slot_function(
            slot_role=slot_role,
            slot_text=sentence,
            context_text="".join(sentences[max(0, sentence_index - 1) : min(total_sentences, sentence_index + 2)]),
            context_before=sentences[sentence_index - 1] if sentence_index > 0 else "",
            context_after=sentences[sentence_index + 1] if sentence_index + 1 < total_sentences else "",
        )
        if not slot_function:
            rejects["no_slot_function"] += 1
            continue
        if slot_role == "opening" and slot_function not in {"summary", "topic_intro"}:
            rejects["opening_function_mismatch"] += 1
            continue
        if slot_role == "middle" and slot_function not in {"carry_previous", "lead_next", "bridge_both_sides"}:
            rejects["middle_function_mismatch"] += 1
            continue
        if slot_role == "ending" and slot_function not in {"ending_summary", "countermeasure"}:
            rejects["ending_function_mismatch"] += 1
            continue

        slot_window = pipeline._functional_slot_sentence_window(
            slot_role=slot_role,
            slot_function=slot_function,
            local_sentences=sentences,
            slot_sentence_local_index=sentence_index,
        )
        if slot_window is None:
            rejects["no_sentence_window"] += 1
            continue
        local_start, local_end = slot_window
        if local_start < 0 or local_end >= total_sentences or local_end < local_start:
            rejects["invalid_sentence_window"] += 1
            continue
        slot_text = "".join(sentences[local_start : local_end + 1]).strip()
        if not slot_text:
            rejects["empty_slot_text"] += 1
            continue
        slot_sentence_count = local_end - local_start + 1
        if slot_sentence_count > 2:
            rejects["slot_window_too_wide"] += 1
            continue
        if slot_sentence_count == 1 and len(slot_text) > 160:
            rejects["slot_text_too_long_single"] += 1
            continue
        if slot_sentence_count == 2 and len(slot_text) > 240:
            rejects["slot_text_too_long_double"] += 1
            continue

        context_before = sentences[local_start - 1] if local_start > 0 else ""
        context_after = sentences[local_end + 1] if local_end + 1 < total_sentences else ""
        slot_context_text = "".join(part for part in [context_before, slot_text, context_after] if part)
        blank_ok, blank_reason = pipeline._functional_slot_has_blank_value(
            slot_role=slot_role,
            slot_function=slot_function,
            slot_text=slot_text,
            context_before=context_before,
            context_after=context_after,
            slot_context_text=slot_context_text,
        )
        if not blank_ok:
            rejects[f"blank_value_not_ready:{blank_reason}"] += 1
            continue

        if slot_role == "opening":
            opening_ok = (
                any(marker in slot_text for marker in ("当前", "如今", "近年来", "面对", "在此背景下"))
                or pipeline._marker_strength(slot_text, SUMMARY_MARKERS) >= 0.18
                or pipeline._core_object_anchor_strength(slot_text) >= 0.28
            )
            if not opening_ok:
                rejects["opening_gate_strict"] += 1
                continue
        if slot_role == "middle" and slot_function == "bridge_both_sides":
            trace = pipeline._classify_middle_functional_slot(
                slot_text=slot_text,
                context_before=context_before,
                context_after=context_after,
                slot_context_text=slot_context_text,
            )
            if min(float(trace.get("backward_score") or 0.0), float(trace.get("forward_score") or 0.0)) < 0.44:
                rejects["middle_bridge_bidirectional_weak"] += 1
                continue
        if slot_role == "middle" and slot_function == "carry_previous":
            trace = pipeline._classify_middle_functional_slot(
                slot_text=slot_text,
                context_before=context_before,
                context_after=context_after,
                slot_context_text=slot_context_text,
            )
            if float(trace.get("backward_score") or 0.0) < 0.54:
                rejects["middle_carry_backward_weak"] += 1
                continue
        if slot_role == "ending" and slot_function == "ending_summary":
            if pipeline._marker_strength(slot_text, SUMMARY_MARKERS + CONCLUSION_MARKERS) < 0.18:
                rejects["ending_summary_gate_strict"] += 1
                continue
        if slot_role == "ending" and slot_function == "countermeasure":
            if pipeline._marker_strength(slot_text, COUNTERMEASURE_MARKERS + ACTION_MEASURE_MARKERS) < 0.16:
                rejects["ending_countermeasure_gate_strict"] += 1
                continue

        bridge_action = pipeline._fill_bridge_action_name(slot_role=slot_role, slot_function=slot_function)
        priority = pipeline._fill_bridge_slot_priority(
            slot_role=slot_role,
            slot_function=slot_function,
            slot_text=slot_text,
            context_before=context_before,
            context_after=context_after,
        )
        proposals.append(
            {
                "slot_role": slot_role,
                "slot_function": slot_function,
                "slot_text": slot_text,
                "left_context_text": context_before,
                "right_context_text": context_after,
                "slot_sentence_range": [int(local_start), int(local_end)],
                "bridge_action": bridge_action,
                "priority": priority,
            }
        )

    if not proposals:
        reason = "no_valid_slot_after_gates"
        if rejects:
            reason = rejects.most_common(1)[0][0]
        return {
            "bridgeable": False,
            "reason": reason,
            "best": None,
            "reject_counts": dict(rejects),
        }

    proposals.sort(key=lambda item: float(item.get("priority") or 0.0), reverse=True)
    return {
        "bridgeable": True,
        "reason": "ok",
        "best": proposals[0],
        "reject_counts": dict(rejects),
    }


def _sample_payload(
    *,
    material: MaterialSpanORM,
    baseline_item: dict[str, Any] | None,
    bridged_item: dict[str, Any] | None,
    probe: dict[str, Any] | None,
    payload_candidate_type: str,
) -> dict[str, Any]:
    bridged_meta = dict((bridged_item or {}).get("meta") or {})
    best = dict((probe or {}).get("best") or {})
    return {
        "material_id": str(material.id),
        "article_id": str(material.article_id),
        "source_candidate_type": str(material.span_type or ""),
        "payload_candidate_type": payload_candidate_type,
        "baseline_rebuild_none": baseline_item is None,
        "after_bridge_rebuild_none": bridged_item is None,
        "bridge_success": bool(bridged_item),
        "bridged_candidate_type": str((bridged_item or {}).get("candidate_type") or ""),
        "slot_role": str(bridged_meta.get("slot_role") or best.get("slot_role") or ""),
        "slot_function": str(bridged_meta.get("slot_function") or best.get("slot_function") or ""),
        "slot_bridge_action": str(bridged_meta.get("slot_bridge_action") or best.get("bridge_action") or ""),
        "slot_sentence_text": str(bridged_meta.get("slot_sentence_text") or best.get("slot_text") or ""),
        "left_context_text": str(bridged_meta.get("left_context_text") or best.get("left_context_text") or ""),
        "right_context_text": str(bridged_meta.get("right_context_text") or best.get("right_context_text") or ""),
        "failure_reason": str((probe or {}).get("reason") or ""),
        "failure_reject_counts": dict((probe or {}).get("reject_counts") or {}),
        "source_text_clip": _clip(material.text, limit=180),
    }


def run(*, max_items: int | None, sample_per_bucket: int) -> dict[str, Any]:
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, Any] = {}
    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        all_rows = list(session.scalars(stmt))
        fill_rows: list[MaterialSpanORM] = []
        for row in all_rows:
            payload = row.v2_index_payload or {}
            if FAMILY not in set(row.v2_business_family_ids or []):
                continue
            if not isinstance(payload.get(FAMILY), dict):
                continue
            fill_rows.append(row)
        fill_rows.sort(key=lambda item: str(item.id))
        if max_items is not None:
            fill_rows = fill_rows[: max(1, int(max_items))]

        totals = Counter()
        by_source = defaultdict(Counter)
        slot_role_counter = Counter()
        slot_function_counter = Counter()
        bridge_action_counter = Counter()
        failure_reason_counter = Counter()
        recoverable_failure_reason_counter = Counter()
        scoring_counter = Counter()

        opening_samples: list[dict[str, Any]] = []
        middle_samples: list[dict[str, Any]] = []
        ending_samples: list[dict[str, Any]] = []
        failure_samples: list[dict[str, Any]] = []

        for material in fill_rows:
            payload = (material.v2_index_payload or {}).get(FAMILY) or {}
            payload_candidate_type = str(payload.get("candidate_type") or "")
            source_type = str(material.span_type or "")
            totals["total"] += 1
            if source_type in SOURCE_TYPES:
                totals["in_bridge_source_scope"] += 1
            if payload_candidate_type in SOURCE_TYPES:
                totals["payload_type_in_bridge_source_scope"] += 1
            by_source[source_type]["total"] += 1

            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                totals["missing_article"] += 1
                continue

            baseline_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=FAMILY,
                enable_fill_formalization_bridge=False,
            )
            bridged_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=FAMILY,
                enable_fill_formalization_bridge=True,
            )

            baseline_none = baseline_item is None
            bridged_none = bridged_item is None
            by_source[source_type]["baseline_rebuild_none"] += 1 if baseline_none else 0
            by_source[source_type]["after_bridge_rebuild_none"] += 1 if bridged_none else 0

            totals["baseline_rebuild_none"] += 1 if baseline_none else 0
            totals["after_bridge_rebuild_none"] += 1 if bridged_none else 0
            totals["baseline_non_none"] += 0 if baseline_none else 1
            totals["after_bridge_non_none"] += 0 if bridged_none else 1
            totals["recovered_from_rebuild_none"] += 1 if (baseline_none and not bridged_none) else 0

            baseline_selected_task = dict((baseline_item or {}).get("selected_task_scoring") or {})
            bridged_selected_task = dict((bridged_item or {}).get("selected_task_scoring") or {})
            scoring_counter["baseline_has_selected_task_scoring"] += 1 if baseline_selected_task else 0
            scoring_counter["after_bridge_has_selected_task_scoring"] += 1 if bridged_selected_task else 0

            bridged_type = str((bridged_item or {}).get("candidate_type") or "")
            if bridged_type == "functional_slot_unit":
                totals["after_bridge_functional_slot_unit"] += 1
                by_source[source_type]["after_bridge_functional_slot_unit"] += 1

            probe = None
            if source_type in SOURCE_TYPES:
                probe = _bridge_probe(
                    pipeline=pipeline,
                    candidate=_candidate_seed(material),
                )

            if bridged_item:
                bridged_meta = dict(bridged_item.get("meta") or {})
                role = str(bridged_meta.get("slot_role") or "")
                function = str(bridged_meta.get("slot_function") or "")
                action = str(bridged_meta.get("slot_bridge_action") or "")
                if role:
                    slot_role_counter[role] += 1
                if function:
                    slot_function_counter[function] += 1
                if action:
                    bridge_action_counter[action] += 1
                if baseline_none:
                    by_source[source_type]["recovered_from_rebuild_none"] += 1

                sample_payload = _sample_payload(
                    material=material,
                    baseline_item=baseline_item,
                    bridged_item=bridged_item,
                    probe=probe,
                    payload_candidate_type=payload_candidate_type,
                )
                if role == "opening" and len(opening_samples) < sample_per_bucket:
                    opening_samples.append(sample_payload)
                elif role == "middle" and len(middle_samples) < sample_per_bucket:
                    middle_samples.append(sample_payload)
                elif role == "ending" and len(ending_samples) < sample_per_bucket:
                    ending_samples.append(sample_payload)
            else:
                if probe is not None:
                    reason = str(probe.get("reason") or "bridge_failed_unknown")
                    failure_reason_counter[reason] += 1
                    if baseline_none and source_type in SOURCE_TYPES:
                        recoverable_failure_reason_counter[reason] += 1
                if source_type in SOURCE_TYPES and len(failure_samples) < sample_per_bucket:
                    failure_samples.append(
                        _sample_payload(
                            material=material,
                            baseline_item=baseline_item,
                            bridged_item=bridged_item,
                            probe=probe,
                            payload_candidate_type=payload_candidate_type,
                        )
                    )

        by_source_report = {
            source: {
                "total": int(values.get("total") or 0),
                "baseline_rebuild_none": int(values.get("baseline_rebuild_none") or 0),
                "after_bridge_rebuild_none": int(values.get("after_bridge_rebuild_none") or 0),
                "recovered_from_rebuild_none": int(values.get("recovered_from_rebuild_none") or 0),
                "after_bridge_functional_slot_unit": int(values.get("after_bridge_functional_slot_unit") or 0),
            }
            for source, values in sorted(by_source.items(), key=lambda item: item[1].get("total", 0), reverse=True)
        }
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "family": FAMILY,
            "bridge_source_types": sorted(SOURCE_TYPES),
            "totals": dict(totals),
            "scoring": dict(scoring_counter),
            "slot_role_distribution": dict(slot_role_counter),
            "slot_function_distribution": dict(slot_function_counter),
            "bridge_action_distribution": dict(bridge_action_counter),
            "by_source_candidate_type": by_source_report,
            "failure_reason_distribution": dict(failure_reason_counter),
            "recoverable_failure_reason_distribution": dict(recoverable_failure_reason_counter),
            "samples": {
                "opening_success": opening_samples,
                "middle_success": middle_samples,
                "ending_success": ending_samples,
                "bridge_failure": failure_samples,
            },
        }
    finally:
        session.close()


def _markdown(report: dict[str, Any]) -> str:
    totals = report.get("totals") or {}
    scoring = report.get("scoring") or {}
    lines: list[str] = []
    lines.append("# Sentence Fill Formalization Bridge Prototype")
    lines.append("")
    lines.append("## Core Metrics")
    lines.append(f"- total: {int(totals.get('total') or 0)}")
    lines.append(f"- in_bridge_source_scope: {int(totals.get('in_bridge_source_scope') or 0)}")
    lines.append(f"- baseline_rebuild_none: {int(totals.get('baseline_rebuild_none') or 0)}")
    lines.append(f"- after_bridge_rebuild_none: {int(totals.get('after_bridge_rebuild_none') or 0)}")
    lines.append(f"- recovered_from_rebuild_none: {int(totals.get('recovered_from_rebuild_none') or 0)}")
    lines.append(f"- after_bridge_functional_slot_unit: {int(totals.get('after_bridge_functional_slot_unit') or 0)}")
    lines.append(f"- baseline_has_selected_task_scoring: {int(scoring.get('baseline_has_selected_task_scoring') or 0)}")
    lines.append(f"- after_bridge_has_selected_task_scoring: {int(scoring.get('after_bridge_has_selected_task_scoring') or 0)}")
    lines.append("")
    lines.append("## Slot Distribution")
    for key, value in sorted((report.get("slot_role_distribution") or {}).items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- role {key}: {value}")
    for key, value in sorted((report.get("slot_function_distribution") or {}).items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- function {key}: {value}")
    lines.append("")
    lines.append("## Bridge Action Distribution")
    for key, value in sorted((report.get("bridge_action_distribution") or {}).items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Source Type Recovery")
    for source, payload in (report.get("by_source_candidate_type") or {}).items():
        lines.append(
            "- "
            + f"{source}: total={payload.get('total', 0)}, "
            + f"baseline_none={payload.get('baseline_rebuild_none', 0)}, "
            + f"after_bridge_none={payload.get('after_bridge_rebuild_none', 0)}, "
            + f"recovered={payload.get('recovered_from_rebuild_none', 0)}, "
            + f"functional_slot_unit={payload.get('after_bridge_functional_slot_unit', 0)}"
        )
    lines.append("")
    lines.append("## Failure Reasons")
    for key, value in sorted((report.get("failure_reason_distribution") or {}).items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Samples")
    for bucket, items in (report.get("samples") or {}).items():
        lines.append(f"### {bucket}")
        if not items:
            lines.append("- (none)")
            continue
        for item in items:
            lines.append(
                "- "
                + f"material={item.get('material_id')} "
                + f"source={item.get('source_candidate_type')} "
                + f"slot_role={item.get('slot_role')} "
                + f"slot_function={item.get('slot_function')} "
                + f"failure_reason={item.get('failure_reason') or 'n/a'}"
            )
            lines.append(f"  - slot_sentence: {_clip(str(item.get('slot_sentence_text') or ''), 100)}")
            lines.append(f"  - source_clip: {_clip(str(item.get('source_text_clip') or ''), 140)}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototype validator for sentence_fill formalization bridge.")
    parser.add_argument("--max-items", type=int, default=None, help="Optional cap for fill materials.")
    parser.add_argument("--sample-per-bucket", type=int, default=3, help="Sample size per role/failure bucket.")
    parser.add_argument("--output-json", type=Path, default=None, help="Optional JSON output path.")
    parser.add_argument("--output-md", type=Path, default=None, help="Optional Markdown output path.")
    args = parser.parse_args()

    report = run(
        max_items=args.max_items,
        sample_per_bucket=max(1, int(args.sample_per_bucket)),
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text, encoding="utf-8")
    else:
        print(text)

    if args.output_md:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
