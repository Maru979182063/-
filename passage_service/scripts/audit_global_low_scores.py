from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from sqlalchemy import select  # noqa: E402

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.services.card_registry_v2 import CardRegistryV2  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


PRIMARY_FAMILIES = ("title_selection", "sentence_fill", "sentence_order")
SECONDARY_FAMILIES = ("continuation",)
BINS = (
    ("ge_0_8", lambda value: value >= 0.8),
    ("0_6_to_0_8", lambda value: 0.6 <= value < 0.8),
    ("0_4_to_0_6", lambda value: 0.4 <= value < 0.6),
    ("0_2_to_0_4", lambda value: 0.2 <= value < 0.4),
    ("lt_0_2", lambda value: value < 0.2),
)


@dataclass
class PayloadRow:
    material_id: str
    article_id: str
    family: str
    payload: dict[str, Any]
    material: MaterialSpanORM


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clip_text(text: str, limit: int = 160) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    return normalized[:limit]


def _top_counter(values: list[str], limit: int = 8) -> list[list[Any]]:
    return [[key, count] for key, count in Counter([value for value in values if value]).most_common(limit)]


def _family_cards(registry: CardRegistryV2, family: str) -> list[str]:
    cards = registry.get_business_cards(family)
    return sorted(
        {
            str(((card.get("card_meta") or {}).get("business_card_id")) or "").strip()
            for card in cards
            if str(((card.get("card_meta") or {}).get("business_card_id")) or "").strip()
        }
    )


def _question_cards(registry: CardRegistryV2, family: str) -> list[str]:
    return sorted(
        {
            str(card.get("card_id") or "").strip()
            for card in registry.payload.get("question_cards_by_family", {}).get(family, [])
            if str(card.get("card_id") or "").strip()
        }
    )


def _payload_rows(materials: list[MaterialSpanORM], families: list[str]) -> list[PayloadRow]:
    rows: list[PayloadRow] = []
    family_set = set(families)
    for material in materials:
        payload = material.v2_index_payload or {}
        if not isinstance(payload, dict):
            continue
        for family in family_set.intersection(set(material.v2_business_family_ids or [])):
            family_payload = payload.get(family)
            if isinstance(family_payload, dict):
                rows.append(
                    PayloadRow(
                        material_id=str(material.id),
                        article_id=str(material.article_id),
                        family=family,
                        payload=family_payload,
                        material=material,
                    )
                )
    return rows


def _extract_scoring(payload: dict[str, Any]) -> dict[str, Any]:
    selected = payload.get("selected_task_scoring") or {}
    return selected if isinstance(selected, dict) else {}


def _top_dict_item(mapping: dict[str, Any]) -> tuple[str | None, float | None]:
    best_key: str | None = None
    best_value: float | None = None
    for key, raw in (mapping or {}).items():
        value = _safe_float(raw)
        if value is None:
            continue
        if best_value is None or value > best_value:
            best_key = str(key)
            best_value = value
    return best_key, best_value


def _low_dict_item(mapping: dict[str, Any]) -> tuple[str | None, float | None]:
    best_key: str | None = None
    best_value: float | None = None
    for key, raw in (mapping or {}).items():
        value = _safe_float(raw)
        if value is None:
            continue
        if best_value is None or value < best_value:
            best_key = str(key)
            best_value = value
    return best_key, best_value


def _candidate_type_sets(pipeline: MaterialPipelineV2) -> tuple[set[str], set[str]]:
    supported = set(pipeline._supported_candidate_types())
    formal = set(pipeline._formal_material_candidate_types())
    return supported, formal


def _manual_usable_hint(
    *,
    family: str,
    candidate_type: str,
    text: str,
    final_score: float | None,
    readiness_score: float | None,
    avg_structure: float | None,
    rebuild_none: bool,
) -> str:
    if family == "sentence_fill":
        if rebuild_none:
            if any(marker in text for marker in ("这一", "这种", "这也", "同时", "因此", "由此", "不仅", "而且")) and len(text) <= 120:
                return "表面像可填空句，但现行正式承载路径未接住"
            return "人工可用性弱"
        return "疑似可用，需结合上下文复核"
    if family == "sentence_order":
        if candidate_type == "sentence_block_group" and any(marker in text for marker in ("首先", "其次", "最后", "因此", "总之")):
            return "表面像排序材料，但首尾/局部约束明显偏弱"
        return "人工可用性弱"
    if family == "title_selection":
        if candidate_type in {"multi_paragraph_unit", "closed_span", "whole_passage"} and ((final_score or 0.0) >= 0.2 or (avg_structure or 0.0) >= 0.22):
            return "人工看并非不可用，但主轴抽象或单中心性仍偏弱"
        if (readiness_score or 0.0) >= 0.35:
            return "接近可用，但主旨收束不够硬"
    return "人工可用性弱"


def _failure_bucket(
    *,
    has_scoring: bool,
    rebuild_none: bool,
    candidate_type: str,
    supported_types: set[str],
    formal_types: set[str],
    final_score: float | None,
    readiness_score: float | None,
    recommended_threshold: float | None,
    avg_structure: float | None,
    top_penalty_value: float | None,
) -> str:
    if not has_scoring:
        if rebuild_none:
            return "pre_gate_no_formal_candidate"
        if candidate_type not in supported_types:
            return "unscored_candidate_type_unsupported"
        if candidate_type not in formal_types:
            return "unscored_candidate_type_nonformal"
        return "unscored_payload_without_clear_blocker"
    if candidate_type not in supported_types:
        return "candidate_type_unsupported"
    if candidate_type not in formal_types:
        return "candidate_type_nonformal"
    if (avg_structure or 0.0) < 0.2 and (top_penalty_value or 0.0) >= 0.8:
        return "structure_weak_and_penalty_dominant"
    if (avg_structure or 0.0) < 0.2:
        return "structure_weakness"
    if (top_penalty_value or 0.0) >= 0.8:
        return "penalty_dominant"
    if (readiness_score or 0.0) < 0.25:
        return "readiness_low"
    if final_score is not None and recommended_threshold is not None and final_score < recommended_threshold and final_score >= (recommended_threshold - 0.08):
        return "near_threshold_scoring_pressure"
    return "scored_below_threshold_other"


def _analyze_rows(
    *,
    rows: list[PayloadRow],
    pipeline: MaterialPipelineV2,
    article_repo: SQLAlchemyArticleRepository,
    sample_limit: int,
) -> dict[str, Any]:
    supported_types, formal_types = _candidate_type_sets(pipeline)
    task_thresholds = MaterialPipelineV2.TASK_SCORING_THRESHOLDS
    global_bins = Counter()
    global_counts = Counter()
    failure_buckets = Counter()
    family_reports: dict[str, Any] = {}
    card_reports: dict[str, Any] = {}
    sample_records: list[dict[str, Any]] = []
    rebuild_cache: dict[tuple[str, str], dict[str, Any] | None] = {}

    family_groups: dict[str, list[PayloadRow]] = defaultdict(list)
    for row in rows:
        family_groups[row.family].append(row)

    for family, family_rows in family_groups.items():
        family_counts = Counter()
        family_bins = Counter()
        card_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        diagnostic_rows: list[dict[str, Any]] = []

        for row in family_rows:
            payload = row.payload
            material = row.material
            scoring = _extract_scoring(payload)
            has_scoring = bool(scoring)
            final_score = _safe_float(scoring.get("final_candidate_score"))
            readiness_score = _safe_float(scoring.get("readiness_score"))
            structure_scores = scoring.get("structure_scores") or {}
            risk_penalties = scoring.get("risk_penalties") or {}
            structure_values = [_safe_float(value) for value in structure_scores.values()]
            structure_values = [value for value in structure_values if value is not None]
            avg_structure = mean(structure_values) if structure_values else None
            top_penalty_name, top_penalty_value = _top_dict_item(risk_penalties)
            low_structure_name, low_structure_value = _low_dict_item(structure_scores)
            candidate_type = str(payload.get("candidate_type") or "")
            qrc = payload.get("question_ready_context") or {}
            selected_card = str(qrc.get("selected_business_card") or "")
            recommended_threshold = task_thresholds.get(
                "main_idea" if family == "title_selection" else family,
                {},
            ).get("recommended")

            rebuild_none = False
            if not has_scoring:
                cache_key = (row.material_id, family)
                if cache_key not in rebuild_cache:
                    article = article_repo.get(material.article_id)
                    rebuild_cache[cache_key] = (
                        pipeline.build_cached_item_from_material(
                            material=material,
                            article=article,
                            business_family_id=family,
                        )
                        if article is not None
                        else None
                    )
                rebuild_none = rebuild_cache[cache_key] is None

            failure = _failure_bucket(
                has_scoring=has_scoring,
                rebuild_none=rebuild_none,
                candidate_type=candidate_type,
                supported_types=supported_types,
                formal_types=formal_types,
                final_score=final_score,
                readiness_score=readiness_score,
                recommended_threshold=recommended_threshold,
                avg_structure=avg_structure,
                top_penalty_value=top_penalty_value,
            )

            record = {
                "material_id": row.material_id,
                "article_id": row.article_id,
                "family": family,
                "card": selected_card or "(none)",
                "candidate_type": candidate_type,
                "span_type": str(material.span_type or ""),
                "sentence_count": int(getattr(material, "sentence_count", 0) or 0),
                "paragraph_count": int(getattr(material, "paragraph_count", 0) or 0),
                "selected_material_card": str(qrc.get("selected_material_card") or ""),
                "final_score": final_score,
                "readiness_score": readiness_score,
                "recommended_threshold": recommended_threshold,
                "avg_structure": avg_structure,
                "top_penalty_name": top_penalty_name,
                "top_penalty_value": top_penalty_value,
                "lowest_structure_name": low_structure_name,
                "lowest_structure_value": low_structure_value,
                "supported_candidate_type": candidate_type in supported_types,
                "formal_candidate_type": candidate_type in formal_types,
                "has_scoring": has_scoring,
                "rebuild_none": rebuild_none,
                "failure_bucket": failure,
                "quality_score": _safe_float(payload.get("quality_score")),
                "text_snippet": _clip_text(str(getattr(material, "text", "") or "")),
            }
            record["manual_usable_hint"] = _manual_usable_hint(
                family=family,
                candidate_type=candidate_type,
                text=record["text_snippet"],
                final_score=final_score,
                readiness_score=readiness_score,
                avg_structure=avg_structure,
                rebuild_none=rebuild_none,
            )
            diagnostic_rows.append(record)
            card_groups[record["card"]].append(record)

            family_counts["total"] += 1
            global_counts["total"] += 1
            if has_scoring:
                family_counts["scored"] += 1
                global_counts["scored"] += 1
                for name, predicate in BINS:
                    if final_score is not None and predicate(final_score):
                        family_bins[name] += 1
                        global_bins[name] += 1
                        break
                if recommended_threshold is not None and final_score is not None and final_score >= recommended_threshold:
                    family_counts["recommended_or_above"] += 1
                    global_counts["recommended_or_above"] += 1
            else:
                family_counts["unscored"] += 1
                global_counts["unscored"] += 1
            if record["supported_candidate_type"]:
                family_counts["supported_candidate_type"] += 1
                global_counts["supported_candidate_type"] += 1
            else:
                family_counts["unsupported_candidate_type"] += 1
                global_counts["unsupported_candidate_type"] += 1
            if record["formal_candidate_type"]:
                family_counts["formal_candidate_type"] += 1
                global_counts["formal_candidate_type"] += 1
            else:
                family_counts["nonformal_candidate_type"] += 1
                global_counts["nonformal_candidate_type"] += 1
            if rebuild_none:
                family_counts["rebuild_none"] += 1
                global_counts["rebuild_none"] += 1
            failure_buckets[failure] += 1

        family_reports[family] = {
            "totals": dict(family_counts),
            "score_bins": dict(family_bins),
            "recommended_threshold": task_thresholds.get("main_idea" if family == "title_selection" else family, {}).get("recommended"),
            "review_readiness_threshold": task_thresholds.get("main_idea" if family == "title_selection" else family, {}).get("review_readiness"),
            "review_penalty_threshold": task_thresholds.get("main_idea" if family == "title_selection" else family, {}).get("review_penalty"),
            "fallback_review_score_threshold": task_thresholds.get("main_idea" if family == "title_selection" else family, {}).get("fallback_review_score"),
            "candidate_type_top": _top_counter([item["candidate_type"] for item in diagnostic_rows]),
            "failure_bucket_top": _top_counter([item["failure_bucket"] for item in diagnostic_rows]),
            "selected_card_top": _top_counter([item["card"] for item in diagnostic_rows]),
            "top_penalty_top": _top_counter([item["top_penalty_name"] or "" for item in diagnostic_rows]),
            "lowest_structure_top": _top_counter([item["lowest_structure_name"] or "" for item in diagnostic_rows]),
            "average_scores": {
                "final_score": round(mean([item["final_score"] for item in diagnostic_rows if item["final_score"] is not None]), 4) if [item["final_score"] for item in diagnostic_rows if item["final_score"] is not None] else None,
                "readiness_score": round(mean([item["readiness_score"] for item in diagnostic_rows if item["readiness_score"] is not None]), 4) if [item["readiness_score"] for item in diagnostic_rows if item["readiness_score"] is not None] else None,
                "avg_structure": round(mean([item["avg_structure"] for item in diagnostic_rows if item["avg_structure"] is not None]), 4) if [item["avg_structure"] for item in diagnostic_rows if item["avg_structure"] is not None] else None,
                "top_penalty_value": round(mean([item["top_penalty_value"] for item in diagnostic_rows if item["top_penalty_value"] is not None]), 4) if [item["top_penalty_value"] for item in diagnostic_rows if item["top_penalty_value"] is not None] else None,
            },
        }

        for card, items in card_groups.items():
            key = f"{family}::{card}"
            card_reports[key] = {
                "family": family,
                "card": card,
                "totals": {
                    "total": len(items),
                    "scored": sum(1 for item in items if item["has_scoring"]),
                    "unscored": sum(1 for item in items if not item["has_scoring"]),
                    "recommended_or_above": sum(1 for item in items if item["final_score"] is not None and item["recommended_threshold"] is not None and item["final_score"] >= item["recommended_threshold"]),
                },
                "candidate_type_top": _top_counter([item["candidate_type"] for item in items], limit=5),
                "failure_bucket_top": _top_counter([item["failure_bucket"] for item in items], limit=5),
                "top_penalty_top": _top_counter([item["top_penalty_name"] or "" for item in items], limit=3),
                "lowest_structure_top": _top_counter([item["lowest_structure_name"] or "" for item in items], limit=3),
                "average_scores": {
                    "final_score": round(mean([item["final_score"] for item in items if item["final_score"] is not None]), 4) if [item["final_score"] for item in items if item["final_score"] is not None] else None,
                    "readiness_score": round(mean([item["readiness_score"] for item in items if item["readiness_score"] is not None]), 4) if [item["readiness_score"] for item in items if item["readiness_score"] is not None] else None,
                    "avg_structure": round(mean([item["avg_structure"] for item in items if item["avg_structure"] is not None]), 4) if [item["avg_structure"] for item in items if item["avg_structure"] is not None] else None,
                },
            }

        # sample sets per family
        scored_items = [item for item in diagnostic_rows if item["final_score"] is not None]
        near_top_but_low = sorted(
            [item for item in scored_items if item["recommended_threshold"] is not None and item["final_score"] < item["recommended_threshold"]],
            key=lambda item: item["final_score"],
            reverse=True,
        )[:sample_limit]
        low_but_maybe_usable = sorted(
            [
                item
                for item in diagnostic_rows
                if "人工看并非不可用" in item["manual_usable_hint"] or "接近可用" in item["manual_usable_hint"] or "表面像" in item["manual_usable_hint"]
            ],
            key=lambda item: (
                -1 if item["final_score"] is None else -item["final_score"],
                -1 if item["readiness_score"] is None else -item["readiness_score"],
            ),
        )[:sample_limit]
        sample_records.append(
            {
                "family": family,
                "top_but_still_low": near_top_but_low,
                "low_or_unscored_but_maybe_manually_usable": low_but_maybe_usable,
            }
        )

    global_report = {
        "family_payload_total": global_counts["total"],
        "family_payload_scored": global_counts["scored"],
        "family_payload_unscored": global_counts["unscored"],
        "recommended_or_above": global_counts["recommended_or_above"],
        "score_bins": dict(global_bins),
        "candidate_type_shape": {
            "supported_candidate_type": global_counts["supported_candidate_type"],
            "unsupported_candidate_type": global_counts["unsupported_candidate_type"],
            "formal_candidate_type": global_counts["formal_candidate_type"],
            "nonformal_candidate_type": global_counts["nonformal_candidate_type"],
        },
        "rebuild_none_count": global_counts["rebuild_none"],
        "top_failure_buckets": _top_counter(list(failure_buckets.elements()), limit=10),
    }
    return {
        "global": global_report,
        "families": family_reports,
        "cards": card_reports,
        "samples": sample_records,
    }


def run_audit(*, sample_limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    pipeline = MaterialPipelineV2()
    registry = CardRegistryV2()
    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        materials = list(session.scalars(stmt))
        primary_rows = _payload_rows(materials, list(PRIMARY_FAMILIES))
        secondary_rows = _payload_rows(materials, list(SECONDARY_FAMILIES))
        analysis = _analyze_rows(
            rows=primary_rows,
            pipeline=pipeline,
            article_repo=article_repo,
            sample_limit=sample_limit,
        )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": os.getenv("PASSAGE_DATABASE_URL", "sqlite:///./passage_service.db"),
            "index_version_current_code": pipeline.INDEX_VERSION,
            "scope": {
                "material_filter": {
                    "is_primary": True,
                    "status": "promoted",
                    "release_channel": "stable",
                    "v2_index_version_required": True,
                },
                "unique_material_count": len(materials),
                "scored_family_payload_count": len(primary_rows),
                "secondary_family_payload_count": len(secondary_rows),
                "primary_families": list(PRIMARY_FAMILIES),
                "secondary_families": list(SECONDARY_FAMILIES),
            },
            "family_catalog": {
                family: {
                    "question_cards": _question_cards(registry, family),
                    "business_cards": _family_cards(registry, family),
                }
                for family in [*PRIMARY_FAMILIES, *SECONDARY_FAMILIES]
            },
            "thresholds": {
                "absolute_high_score_definition": "final_candidate_score >= 0.8",
                "recommended_thresholds": MaterialPipelineV2.TASK_SCORING_THRESHOLDS,
            },
            "analysis": analysis,
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit global low-score distribution for cached material payloads.")
    parser.add_argument("--sample-limit", type=int, default=5, help="Per-family sample limit.")
    parser.add_argument("--report-path", type=Path, required=False, help="Optional JSON output path.")
    args = parser.parse_args()

    report = run_audit(sample_limit=args.sample_limit)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(text, encoding="utf-8")
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")


if __name__ == "__main__":
    main()
