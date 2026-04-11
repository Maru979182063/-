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


def _summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter()
    for item in records:
        counter[f"class_{item['label']}"] += 1
        counter["formal_hit"] += 1 if item["formal_hit"] else 0
        counter["business_usable"] += 1 if item["business_level"] == "usable" else 0
        counter["business_borderline"] += 1 if item["business_level"] == "borderline" else 0
        counter["business_unusable"] += 1 if item["business_level"] == "unusable" else 0
        counter["formal_usable"] += 1 if item["formal_hit"] and item["business_level"] == "usable" else 0
    formal_hit = int(counter["formal_hit"])
    usable = int(counter["business_usable"])
    formal_usable = int(counter["formal_usable"])
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
        "usable_ratio_within_formal": round(formal_usable / formal_hit, 4) if formal_hit else 0.0,
    }


def _current_residual_bucket(*, candidate_type: str, issues: list[str], profile: dict[str, Any]) -> str:
    closing_rule = str(profile.get("closing_rule") or "none")
    if candidate_type == "weak_formal_order_group":
        if "last_weak" in issues and closing_rule == "none":
            return "weak_tail_without_closing_cue"
        if "last_weak" in issues:
            return "weak_tail_with_closing_cue"
        return "weak_formal_other"
    if candidate_type == "ordered_unit_group" and "pairwise_weak" in issues:
        return "strong_formal_pairwise_residual"
    return "strong_formal_other"


def _unlifted_c_bucket(*, pre_eval: dict[str, Any], pre_item: dict[str, Any] | None) -> str:
    if pre_eval["formal_hit"] and pre_eval["candidate_type"] == "weak_formal_order_group":
        return "weak_tail_formalized_but_not_ready"
    return "still_blocked_before_weak_formal"


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
        current_records: list[dict[str, Any]] = []
        prototype_records: list[dict[str, Any]] = []

        baseline_c_ids: set[str] = set()
        current_lifted_c_ids: set[str] = set()
        prototype_lifted_c_ids: set[str] = set()

        current_residual_counter = Counter()
        current_residual_examples: list[dict[str, Any]] = []

        unlifted_c_counter = Counter()
        unlifted_c_examples: list[dict[str, Any]] = []

        prototype_noise_source_counter = Counter()
        prototype_noise_examples: list[dict[str, Any]] = []

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
            current_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=True,
                enable_sentence_order_weak_formal_closing_gate=False,
            )
            prototype_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=True,
                enable_sentence_order_weak_formal_closing_gate=True,
            )

            baseline_eval = _evaluate(baseline_item, source_text, BASELINE_FORMAL_TYPES)
            current_eval = _evaluate(current_item, source_text, PROTOTYPE_FORMAL_TYPES)
            prototype_eval = _evaluate(prototype_item, source_text, PROTOTYPE_FORMAL_TYPES)

            baseline_records.append({"material_id": material_id, **baseline_eval})
            current_records.append({"material_id": material_id, **current_eval})
            prototype_records.append({"material_id": material_id, **prototype_eval})

            if baseline_eval["label"] == "C":
                baseline_c_ids.add(material_id)

            if baseline_eval["label"] == "C" and current_eval["formal_hit"]:
                current_lifted_c_ids.add(material_id)

            if baseline_eval["label"] == "C" and prototype_eval["formal_hit"]:
                prototype_lifted_c_ids.add(material_id)

            if current_eval["formal_hit"] and current_eval["business_level"] != "usable":
                current_profile = dict(((current_item or {}).get("business_feature_profile") or {}).get("sentence_order_profile") or {})
                bucket = _current_residual_bucket(
                    candidate_type=current_eval["candidate_type"],
                    issues=list(current_eval["issues"]),
                    profile=current_profile,
                )
                current_residual_counter[bucket] += 1
                if len(current_residual_examples) < 10:
                    current_residual_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": current_eval["candidate_type"],
                            "business_level": current_eval["business_level"],
                            "residual_bucket": bucket,
                            "issues_after": current_eval["issues"],
                            "closing_rule": current_profile.get("closing_rule"),
                            "discourse_progression_strength": current_profile.get("discourse_progression_strength"),
                            "context_closure_score": current_profile.get("context_closure_score"),
                            "exchange_risk": current_profile.get("exchange_risk"),
                            "multi_path_risk": current_profile.get("multi_path_risk"),
                            "function_overlap_score": current_profile.get("function_overlap_score"),
                            "text_clip": source_text[:180],
                        }
                    )

            if baseline_eval["label"] == "C" and not current_eval["formal_hit"]:
                pre_item = pipeline.build_cached_item_from_material(
                    material=material,
                    article=article,
                    business_family_id="sentence_order",
                    enable_sentence_order_weak_formal_bridge=True,
                    enable_sentence_order_weak_formal_gate=False,
                )
                pre_eval = _evaluate(pre_item, source_text, PROTOTYPE_FORMAL_TYPES)
                bucket = _unlifted_c_bucket(pre_eval=pre_eval, pre_item=pre_item)
                unlifted_c_counter[bucket] += 1
                if len(unlifted_c_examples) < 10:
                    pre_profile = dict(((pre_item or {}).get("business_feature_profile") or {}).get("sentence_order_profile") or {})
                    unlifted_c_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "pre_formal_type": pre_eval["candidate_type"],
                            "pre_business_level": pre_eval["business_level"],
                            "unlifted_bucket": bucket,
                            "issues_before_reject": pre_eval["issues"],
                            "closing_rule": pre_profile.get("closing_rule"),
                            "discourse_progression_strength": pre_profile.get("discourse_progression_strength"),
                            "context_closure_score": pre_profile.get("context_closure_score"),
                            "exchange_risk": pre_profile.get("exchange_risk"),
                            "multi_path_risk": pre_profile.get("multi_path_risk"),
                            "function_overlap_score": pre_profile.get("function_overlap_score"),
                            "text_clip": source_text[:180],
                        }
                    )

            if (not baseline_eval["formal_hit"]) and prototype_eval["formal_hit"] and prototype_eval["business_level"] == "unusable":
                prototype_noise_source_counter[source_type] += 1
                if len(prototype_noise_examples) < 8:
                    prototype_noise_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": prototype_eval["candidate_type"],
                            "issues_after": prototype_eval["issues"],
                            "text_clip": source_text[:180],
                        }
                    )

        baseline_summary = _summarize(baseline_records)
        current_summary = _summarize(current_records)
        prototype_summary = _summarize(prototype_records)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total": len(rows),
                "baseline": baseline_summary,
                "weak_post_gate_current": current_summary,
                "weak_residual_gate_prototype": prototype_summary,
            },
            "current_residual_audit": {
                "formal_but_not_business_usable_total": int(sum(current_residual_counter.values())),
                "residual_bucket_distribution": dict(current_residual_counter),
                "examples": current_residual_examples,
            },
            "current_unlifted_baseline_c_audit": {
                "baseline_c_total": len(baseline_c_ids),
                "current_lifted_total": len(current_lifted_c_ids),
                "current_unlifted_total": len(baseline_c_ids - current_lifted_c_ids),
                "unlifted_bucket_distribution": dict(unlifted_c_counter),
                "examples": unlifted_c_examples,
            },
            "prototype_effect": {
                "baseline_c_total": len(baseline_c_ids),
                "current_lifted_total": len(current_lifted_c_ids),
                "prototype_lifted_total": len(prototype_lifted_c_ids),
                "prototype_lifted_ratio": round(len(prototype_lifted_c_ids) / len(baseline_c_ids), 4) if baseline_c_ids else 0.0,
                "retained_from_current_lifted_c": len(current_lifted_c_ids & prototype_lifted_c_ids),
                "prototype_new_formal_but_unusable_total": int(sum(prototype_noise_source_counter.values())),
                "prototype_noise_source_distribution": dict(prototype_noise_source_counter),
                "prototype_noise_examples": prototype_noise_examples,
            },
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    baseline = summary.get("baseline") or {}
    current = summary.get("weak_post_gate_current") or {}
    prototype = summary.get("weak_residual_gate_prototype") or {}
    residual = report.get("current_residual_audit") or {}
    unlifted = report.get("current_unlifted_baseline_c_audit") or {}
    effect = report.get("prototype_effect") or {}
    lines = [
        "# Sentence Order Weak Formal Residual Gate Prototype",
        "",
        "## Summary",
        f"- total: {summary.get('total', 0)}",
        f"- baseline A/B/C/D: {baseline.get('a_b_c_d', {})}",
        f"- weak_post_gate_current A/B/C/D: {current.get('a_b_c_d', {})}",
        f"- weak_residual_gate_prototype A/B/C/D: {prototype.get('a_b_c_d', {})}",
        f"- weak_post_gate_current formal hit: {current.get('formal_hit', 0)}",
        f"- weak_residual_gate_prototype formal hit: {prototype.get('formal_hit', 0)}",
        f"- weak_post_gate_current usable ratio within formal: {current.get('usable_ratio_within_formal', 0.0)}",
        f"- weak_residual_gate_prototype usable ratio within formal: {prototype.get('usable_ratio_within_formal', 0.0)}",
        "",
        "## Current Residual Audit",
        f"- formal_but_not_business_usable_total: {residual.get('formal_but_not_business_usable_total', 0)}",
        f"- residual_bucket_distribution: {residual.get('residual_bucket_distribution', {})}",
        "",
        "## Current Unlifted Baseline C",
        f"- baseline_c_total: {unlifted.get('baseline_c_total', 0)}",
        f"- current_lifted_total: {unlifted.get('current_lifted_total', 0)}",
        f"- current_unlifted_total: {unlifted.get('current_unlifted_total', 0)}",
        f"- unlifted_bucket_distribution: {unlifted.get('unlifted_bucket_distribution', {})}",
        "",
        "## Prototype Effect",
        f"- current_lifted_total: {effect.get('current_lifted_total', 0)}",
        f"- prototype_lifted_total: {effect.get('prototype_lifted_total', 0)}",
        f"- retained_from_current_lifted_c: {effect.get('retained_from_current_lifted_c', 0)}",
        f"- prototype_new_formal_but_unusable_total: {effect.get('prototype_new_formal_but_unusable_total', 0)}",
        f"- prototype_noise_source_distribution: {effect.get('prototype_noise_source_distribution', {})}",
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototype residual weak formal gate for sentence_order.")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    args = parser.parse_args()

    report = run()
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
