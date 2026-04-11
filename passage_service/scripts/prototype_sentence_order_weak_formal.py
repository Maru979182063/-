import argparse
import json
import os
import sys
from collections import Counter
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
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402
from scripts.review_business_usable_rate import _abcd, _order_evaluate  # noqa: E402


BASELINE_FORMAL_TYPES = {"ordered_unit_group"}
PROTOTYPE_FORMAL_TYPES = {"ordered_unit_group", "weak_formal_order_group"}


def _evaluate(runtime_item: dict[str, Any] | None, source_text: str, formal_types: set[str]) -> dict[str, Any]:
    system_caught = runtime_item is not None
    candidate_type = str((runtime_item or {}).get("candidate_type") or "")
    formal_hit = candidate_type in formal_types
    structural, business, business_accept, issues, potential = _order_evaluate(
        runtime_item=runtime_item,
        source_text=source_text,
        formal_hit=formal_hit,
    )
    system_caught_strict = system_caught and formal_hit
    return {
        "system_caught": system_caught,
        "system_caught_strict": system_caught_strict,
        "formal_hit": formal_hit,
        "candidate_type": candidate_type,
        "structural_level": structural,
        "business_level": business,
        "business_accept": business_accept,
        "issues": issues,
        "potential": potential,
        "label": _abcd(system_caught_strict=system_caught_strict, business_accept=business_accept),
    }


def _builder_reason(pipeline: MaterialPipelineV2, text: str, source_type: str) -> str:
    units = pipeline._sentence_order_units(text, source_type)
    if len(units) != pipeline.SENTENCE_ORDER_FIXED_UNIT_COUNT:
        return "unit_count_not_six"
    worthwhile, reason, _, _, _ = pipeline._ordered_unit_group_worthwhile(units)
    if worthwhile:
        return "would_pass_builder"
    return reason


def _noise_bucket(*, profile: dict[str, Any], issues: list[str]) -> str:
    closing_rule = str(profile.get("closing_rule") or "none")
    local_binding = float(profile.get("local_binding_strength") or 0.0)
    progression = float(profile.get("discourse_progression_strength") or 0.0)
    closure = float(profile.get("context_closure_score") or 0.0)
    exchange_risk = float(profile.get("exchange_risk") or 0.0)
    multi_path_risk = float(profile.get("multi_path_risk") or 0.0)
    function_overlap = float(profile.get("function_overlap_score") or 0.0)

    if closing_rule == "none" and closure < 0.52:
        return "head_tail_direction_weak"
    if progression < 0.50 or closure < 0.50:
        return "progression_closure_weak"
    if exchange_risk > 0.42 or multi_path_risk > 0.44 or function_overlap > 0.50:
        return "risk_high"
    if "pairwise_weak" in issues or local_binding < 0.42:
        return "structure_like_but_low_task_value"
    return "structure_like_but_low_task_value"


def _summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter()
    for item in records:
        counter[f"class_{item['label']}"] += 1
        counter["formal_hit"] += 1 if item["formal_hit"] else 0
        counter["business_usable"] += 1 if item["business_level"] == "usable" else 0
        counter["business_borderline"] += 1 if item["business_level"] == "borderline" else 0
        counter["business_unusable"] += 1 if item["business_level"] == "unusable" else 0
    formal_hit = int(counter["formal_hit"])
    usable = int(counter["business_usable"])
    return {
        "a_b_c_d": {
            "A": int(counter["class_A"]),
            "B": int(counter["class_B"]),
            "C": int(counter["class_C"]),
            "D": int(counter["class_D"]),
        },
        "formal_hit": formal_hit,
        "business_usable": usable,
        "business_borderline": int(counter["business_borderline"]),
        "business_unusable": int(counter["business_unusable"]),
        "usable_ratio_within_formal": round(usable / formal_hit, 4) if formal_hit else 0.0,
    }


def run() -> dict[str, Any]:
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
        materials = list(session.scalars(stmt))
        rows = []
        for material in materials:
            fams = set(material.v2_business_family_ids or [])
            payload = material.v2_index_payload or {}
            if "sentence_order" in fams and isinstance(payload.get("sentence_order"), dict):
                rows.append(material)

        baseline_records: list[dict[str, Any]] = []
        pre_gate_records: list[dict[str, Any]] = []
        post_gate_records: list[dict[str, Any]] = []

        baseline_c_ids: set[str] = set()
        baseline_reason_by_id: dict[str, str] = {}

        pre_gate_noise_counter = Counter()
        pre_gate_noise_source_counter = Counter()
        pre_gate_noise_examples: list[dict[str, Any]] = []

        post_gate_lifted_source_counter = Counter()
        post_gate_lifted_type_counter = Counter()
        post_gate_lifted_reason_counter = Counter()
        post_gate_lifted_examples: list[dict[str, Any]] = []

        post_gate_noise_source_counter = Counter()
        post_gate_noise_examples: list[dict[str, Any]] = []

        for material in sorted(rows, key=lambda item: str(item.id)):
            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue

            source_text = str(material.text or "")
            source_type = str(material.span_type or "")
            material_id = str(material.id)

            baseline_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=False,
            )
            pre_gate_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=False,
            )
            post_gate_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=True,
            )

            baseline_eval = _evaluate(baseline_item, source_text, BASELINE_FORMAL_TYPES)
            pre_gate_eval = _evaluate(pre_gate_item, source_text, PROTOTYPE_FORMAL_TYPES)
            post_gate_eval = _evaluate(post_gate_item, source_text, PROTOTYPE_FORMAL_TYPES)

            baseline_records.append({"material_id": material_id, **baseline_eval})
            pre_gate_records.append({"material_id": material_id, **pre_gate_eval})
            post_gate_records.append({"material_id": material_id, **post_gate_eval})

            if baseline_eval["label"] == "C":
                baseline_c_ids.add(material_id)
                baseline_reason_by_id[material_id] = _builder_reason(pipeline, source_text, source_type)

            if (not baseline_eval["formal_hit"]) and pre_gate_eval["formal_hit"] and pre_gate_eval["business_level"] == "unusable":
                profile = dict(((pre_gate_item or {}).get("business_feature_profile") or {}).get("sentence_order_profile") or {})
                bucket = _noise_bucket(profile=profile, issues=list(pre_gate_eval["issues"]))
                pre_gate_noise_counter[bucket] += 1
                pre_gate_noise_source_counter[source_type] += 1
                if len(pre_gate_noise_examples) < 8:
                    pre_gate_noise_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": pre_gate_eval["candidate_type"],
                            "noise_bucket": bucket,
                            "issues_after": pre_gate_eval["issues"],
                            "closing_rule": profile.get("closing_rule"),
                            "local_binding_strength": profile.get("local_binding_strength"),
                            "discourse_progression_strength": profile.get("discourse_progression_strength"),
                            "context_closure_score": profile.get("context_closure_score"),
                            "exchange_risk": profile.get("exchange_risk"),
                            "multi_path_risk": profile.get("multi_path_risk"),
                            "function_overlap_score": profile.get("function_overlap_score"),
                            "text_clip": source_text[:180],
                        }
                    )

            if material_id in baseline_c_ids and post_gate_eval["formal_hit"]:
                post_gate_lifted_source_counter[source_type] += 1
                post_gate_lifted_type_counter[post_gate_eval["candidate_type"]] += 1
                post_gate_lifted_reason_counter[baseline_reason_by_id.get(material_id, "unknown")] += 1
                if len(post_gate_lifted_examples) < 8:
                    post_gate_lifted_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": post_gate_eval["candidate_type"],
                            "baseline_builder_reason": baseline_reason_by_id.get(material_id, "unknown"),
                            "business_level_after": post_gate_eval["business_level"],
                            "issues_after": post_gate_eval["issues"],
                            "text_clip": source_text[:180],
                        }
                    )

            if (not baseline_eval["formal_hit"]) and post_gate_eval["formal_hit"] and post_gate_eval["business_level"] == "unusable":
                post_gate_noise_source_counter[source_type] += 1
                if len(post_gate_noise_examples) < 8:
                    post_gate_noise_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": post_gate_eval["candidate_type"],
                            "issues_after": post_gate_eval["issues"],
                            "text_clip": source_text[:180],
                        }
                    )

        baseline_summary = _summarize(baseline_records)
        pre_gate_summary = _summarize(pre_gate_records)
        post_gate_summary = _summarize(post_gate_records)

        post_gate_lifted_total = sum(post_gate_lifted_source_counter.values())
        pre_gate_noise_total = int(sum(pre_gate_noise_source_counter.values()))
        post_gate_noise_total = int(sum(post_gate_noise_source_counter.values()))

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total": len(rows),
                "baseline": baseline_summary,
                "weak_pre_gate": pre_gate_summary,
                "weak_post_gate": post_gate_summary,
            },
            "pre_gate_noise_audit": {
                "new_formal_but_unusable_total": pre_gate_noise_total,
                "noise_bucket_distribution": dict(pre_gate_noise_counter),
                "source_type_distribution": dict(pre_gate_noise_source_counter),
                "examples": pre_gate_noise_examples,
            },
            "post_gate_lifted_from_baseline_c": {
                "baseline_c_total": len(baseline_c_ids),
                "lifted_total": post_gate_lifted_total,
                "lifted_ratio": round(post_gate_lifted_total / len(baseline_c_ids), 4) if baseline_c_ids else 0.0,
                "source_type_distribution": dict(post_gate_lifted_source_counter),
                "prototype_candidate_type_distribution": dict(post_gate_lifted_type_counter),
                "baseline_builder_reason_distribution": dict(post_gate_lifted_reason_counter),
                "examples": post_gate_lifted_examples,
            },
            "post_gate_noise_check": {
                "new_formal_but_unusable_total": post_gate_noise_total,
                "source_type_distribution": dict(post_gate_noise_source_counter),
                "examples": post_gate_noise_examples,
            },
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    baseline = summary.get("baseline") or {}
    pre_gate = summary.get("weak_pre_gate") or {}
    post_gate = summary.get("weak_post_gate") or {}
    noise = report.get("pre_gate_noise_audit") or {}
    lifted = report.get("post_gate_lifted_from_baseline_c") or {}
    post_noise = report.get("post_gate_noise_check") or {}
    lines = [
        "# Sentence Order Weak Formal Gate Prototype",
        "",
        "## Summary",
        f"- total: {summary.get('total', 0)}",
        f"- baseline A/B/C/D: {baseline.get('a_b_c_d', {})}",
        f"- weak_pre_gate A/B/C/D: {pre_gate.get('a_b_c_d', {})}",
        f"- weak_post_gate A/B/C/D: {post_gate.get('a_b_c_d', {})}",
        f"- baseline formal hit: {baseline.get('formal_hit', 0)}",
        f"- weak_pre_gate formal hit: {pre_gate.get('formal_hit', 0)}",
        f"- weak_post_gate formal hit: {post_gate.get('formal_hit', 0)}",
        f"- weak_pre_gate usable ratio within formal: {pre_gate.get('usable_ratio_within_formal', 0.0)}",
        f"- weak_post_gate usable ratio within formal: {post_gate.get('usable_ratio_within_formal', 0.0)}",
        "",
        "## Pre-gate Noise",
        f"- new_formal_but_unusable_total: {noise.get('new_formal_but_unusable_total', 0)}",
        f"- noise_bucket_distribution: {noise.get('noise_bucket_distribution', {})}",
        f"- source_type_distribution: {noise.get('source_type_distribution', {})}",
        "",
        "## Post-gate Lifted From Baseline C",
        f"- baseline_c_total: {lifted.get('baseline_c_total', 0)}",
        f"- lifted_total: {lifted.get('lifted_total', 0)}",
        f"- lifted_ratio: {lifted.get('lifted_ratio', 0.0)}",
        f"- source_type_distribution: {lifted.get('source_type_distribution', {})}",
        f"- prototype_candidate_type_distribution: {lifted.get('prototype_candidate_type_distribution', {})}",
        f"- baseline_builder_reason_distribution: {lifted.get('baseline_builder_reason_distribution', {})}",
        "",
        "## Post-gate Noise",
        f"- new_formal_but_unusable_total: {post_noise.get('new_formal_but_unusable_total', 0)}",
        f"- source_type_distribution: {post_noise.get('source_type_distribution', {})}",
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototype weak_formal_order_group gate for sentence_order.")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    args = parser.parse_args()

    report = run()
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")
    print(f"Wrote {args.output_json}")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()
