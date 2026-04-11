from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select


PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.core.config import get_config_bundle  # noqa: E402
from app.core.enums import MaterialStatus, ReleaseChannel, ReviewStatus  # noqa: E402
from app.domain.services.ingest_service import run_crawl_for_source  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.orm.review import TaggingReviewORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.card_registry_v2 import CardRegistryV2  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


FAMILY_ORDER = ("title_selection", "continuation", "sentence_fill", "sentence_order")
SERVABLE_REVIEW_STATUSES = {
    ReviewStatus.AUTO_TAGGED.value,
    ReviewStatus.REVIEW_CONFIRMED.value,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand effective inventory per card id.")
    parser.add_argument("--target-per-card", type=int, default=20)
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--max-sources-per-round", type=int, default=4)
    parser.add_argument("--crawl-article-limit", type=int, default=None)
    parser.add_argument("--disable-llm-during-crawl", action="store_true")
    parser.add_argument("--per-article-cap", type=int, default=8)
    parser.add_argument("--min-quality-score", type=float, default=0.45)
    parser.add_argument("--enable-narrow-review-pending-bridge", action="store_true")
    parser.add_argument("--review-pending-recent-days", type=int, default=5)
    parser.add_argument("--review-pending-min-quality-score", type=float, default=0.62)
    parser.add_argument("--sources", nargs="*", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def preferred_sources() -> list[str]:
    configured = [item.get("id") for item in get_config_bundle().sources.get("sources", []) if item.get("enabled", True)]
    priority = [
        "people",
        "xinhuanet",
        "gmw",
        "qstheory",
        "gov",
        "banyuetan",
        "cyol",
        "ce",
        "lifeweek",
        "whb",
        "guokr",
        "thepaper",
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for source_id in priority + configured:
        if source_id and source_id in configured and source_id not in seen:
            seen.add(source_id)
            ordered.append(source_id)
    return ordered


def _apply_crawl_article_limit_override(source_ids: list[str], article_limit: int | None) -> None:
    if article_limit is None:
        return
    sources = get_config_bundle().sources.get("sources", [])
    for source in sources:
        source_id = str(source.get("id") or "")
        if source_id in source_ids:
            source["article_limit"] = int(article_limit)


def _apply_llm_disable_override(disable_llm: bool) -> None:
    if not disable_llm:
        return
    get_config_bundle().llm["enabled"] = False


def _all_material_card_ids(registry: CardRegistryV2) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for family in FAMILY_ORDER:
        result[family] = [str(card.get("card_id") or "") for card in registry.get_material_cards(family) if str(card.get("card_id") or "")]
    return result


def _all_business_card_ids(registry: CardRegistryV2) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for business_card_id, payload in sorted(registry.payload.get("business_cards", {}).items()):
        meta = payload.get("card_meta") or {}
        mother_family_id = str(meta.get("mother_family_id") or "").strip()
        runtime_family = "title_selection" if mother_family_id == "main_idea" else mother_family_id
        if runtime_family in FAMILY_ORDER:
            grouped[runtime_family].append(str(business_card_id))
    return dict(grouped)


def _runtime_item(
    pipeline: MaterialPipelineV2,
    *,
    material: MaterialSpanORM,
    article: ArticleORM,
    family: str,
) -> dict[str, Any] | None:
    return pipeline.build_cached_item_from_material(
        material=material,
        article=article,
        business_family_id=family,
        enable_fill_formalization_bridge=(family == "sentence_fill"),
        enable_sentence_order_weak_formal_bridge=(family == "sentence_order"),
        enable_sentence_order_weak_formal_gate=(family == "sentence_order"),
        enable_sentence_order_weak_formal_closing_gate=(family == "sentence_order"),
    )


def _material_served_cards(
    pipeline: MaterialPipelineV2,
    *,
    material: MaterialSpanORM,
    article: ArticleORM,
) -> list[dict[str, str]]:
    families = [str(fam) for fam in (material.v2_business_family_ids or []) if str(fam) in FAMILY_ORDER]
    served: list[dict[str, str]] = []
    for family in families:
        item = _runtime_item(pipeline, material=material, article=article, family=family)
        if not item:
            continue
        qctx = dict(item.get("question_ready_context") or {})
        material_card_id = str(qctx.get("selected_material_card") or "")
        business_card_id = str(qctx.get("selected_business_card") or "")
        question_card_id = str(qctx.get("question_card_id") or "")
        if not material_card_id:
            continue
        served.append(
            {
                "family": family,
                "material_card_id": material_card_id,
                "business_card_id": business_card_id,
                "question_card_id": question_card_id,
            }
        )
    return served


def _review_status_lookup(session, material_ids: list[str]) -> dict[str, str]:
    if not material_ids:
        return {}
    rows = session.execute(
        select(TaggingReviewORM.material_id, TaggingReviewORM.status).where(TaggingReviewORM.material_id.in_(material_ids))
    ).all()
    return {material_id: status for material_id, status in rows}


def _article_source_lookup(session) -> dict[str, str]:
    return {
        article.id: str(article.source or "unknown")
        for article in session.scalars(select(ArticleORM))
    }


COMMENTARY_SOURCE_NAMES = {
    "人民网",
    "新华网",
    "光明网",
    "求是网",
    "澎湃新闻",
    "中国青年报",
    "经济日报",
    "半月谈",
}

BAD_TITLE_PATTERNS = [
    r"全部导航",
    r"招聘",
    r"面试通知",
    r"通报",
    r"巡视对象公布",
    r"对象公布",
    r"公布$",
    r"快讯",
    r"直播",
    r"国际--人民网",
    r"答记者问",
    r"专场音乐会",
]

GOOD_TITLE_PATTERNS = [
    r"评论",
    r"时评",
    r"人民论坛",
    r"现场评论",
    r"社论",
    r"观察",
    r"论坛",
]


def _is_narrow_review_pending_bridge_candidate(
    *,
    item: MaterialSpanORM,
    article: ArticleORM | None,
    review_status: str | None,
    served_cards: list[dict[str, str]],
    quality_score: float,
    recent_cutoff: datetime,
    review_pending_min_quality_score: float,
) -> tuple[bool, str]:
    if review_status != ReviewStatus.REVIEW_PENDING.value:
        return False, "review_not_pending"
    if article is None:
        return False, "article_missing"
    if not item.is_primary or str(item.status) != MaterialStatus.GRAY.value or str(item.release_channel) != ReleaseChannel.GRAY.value:
        return False, "not_primary_gray"
    if not item.v2_business_family_ids or not served_cards:
        return False, "missing_v2_or_served_cards"
    targetable_served_cards = [
        card
        for card in served_cards
        if str(card.get("material_card_id") or "")
        and not str(card.get("material_card_id") or "").startswith("legacy.")
    ]
    if not targetable_served_cards:
        return False, "no_targetable_card_gap_surface"
    updated_at = getattr(article, "updated_at", None)
    if updated_at is not None and getattr(updated_at, "tzinfo", None) is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    if updated_at is None or updated_at < recent_cutoff:
        return False, "not_recent"
    if quality_score < review_pending_min_quality_score:
        return False, "quality_below_narrow_threshold"

    title = str(getattr(article, "title", "") or "")
    source = str(getattr(article, "source", "") or "")
    if any(re.search(pattern, title, flags=re.IGNORECASE) for pattern in BAD_TITLE_PATTERNS):
        return False, "bad_title_pattern"
    if source not in COMMENTARY_SOURCE_NAMES:
        return False, "source_not_in_narrow_commentary_set"

    if any(re.search(pattern, title, flags=re.IGNORECASE) for pattern in GOOD_TITLE_PATTERNS):
        return True, "commentary_title_pattern"

    if source in {"求是网", "人民网", "新华网"} and len(title) >= 6 and "国际" not in title:
        return True, "trusted_commentary_source"

    return False, "commentary_signal_too_weak"


def _compute_inventory(session, *, target_per_card: int) -> dict[str, Any]:
    registry = CardRegistryV2()
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, ArticleORM | None] = {}
    stable_materials = list(
        session.scalars(
            select(MaterialSpanORM).where(
                MaterialSpanORM.status == MaterialStatus.PROMOTED.value,
                MaterialSpanORM.release_channel == ReleaseChannel.STABLE.value,
                MaterialSpanORM.is_primary.is_(True),
                MaterialSpanORM.v2_index_version.is_not(None),
            )
        )
    )
    material_card_ids_by_family = _all_material_card_ids(registry)
    business_card_ids_by_family = _all_business_card_ids(registry)

    material_counts: Counter[str] = Counter()
    business_counts: Counter[str] = Counter()
    question_counts: Counter[str] = Counter()
    family_effective_counts: Counter[str] = Counter()
    stable_article_counts: Counter[str] = Counter()

    for material in stable_materials:
        article_id = str(material.article_id)
        if article_id not in article_cache:
            article_cache[article_id] = session.get(ArticleORM, article_id)
        article = article_cache.get(article_id)
        if article is None:
            continue
        served = _material_served_cards(pipeline, material=material, article=article)
        if not served:
            continue
        stable_article_counts[article_id] += 1
        for item in served:
            family_effective_counts[item["family"]] += 1
            material_counts[item["material_card_id"]] += 1
            if item["business_card_id"]:
                business_counts[item["business_card_id"]] += 1
            if item["question_card_id"]:
                question_counts[item["question_card_id"]] += 1

    material_gap_rows: list[dict[str, Any]] = []
    business_gap_rows: list[dict[str, Any]] = []
    for family in FAMILY_ORDER:
        for card_id in material_card_ids_by_family.get(family, []):
            current = int(material_counts.get(card_id, 0))
            material_gap_rows.append(
                {
                    "family": family,
                    "card_id": card_id,
                    "effective_count": current,
                    "gap": max(0, target_per_card - current),
                }
            )
        for card_id in business_card_ids_by_family.get(family, []):
            current = int(business_counts.get(card_id, 0))
            business_gap_rows.append(
                {
                    "family": family,
                    "card_id": card_id,
                    "effective_count": current,
                    "gap": max(0, target_per_card - current),
                }
            )

    material_gap_rows.sort(key=lambda row: (-row["gap"], row["family"], row["card_id"]))
    business_gap_rows.sort(key=lambda row: (-row["gap"], row["family"], row["card_id"]))
    return {
        "stable_primary_material_total": len(stable_materials),
        "family_effective_counts": dict(family_effective_counts),
        "question_card_effective_counts": dict(question_counts),
        "material_counts": material_counts,
        "business_counts": business_counts,
        "material_gap_rows": material_gap_rows,
        "business_gap_rows": business_gap_rows,
        "stable_article_counts": stable_article_counts,
    }


def _target_met(inventory: dict[str, Any]) -> bool:
    return all(row["gap"] == 0 for row in inventory["material_gap_rows"]) and all(row["gap"] == 0 for row in inventory["business_gap_rows"])


def _candidate_gain(served_cards: list[dict[str, str]], *, material_counts: Counter[str], business_counts: Counter[str], target_per_card: int) -> tuple[int, int]:
    material_gain = 0
    business_gain = 0
    for item in served_cards:
        material_gain += max(0, target_per_card - int(material_counts.get(item["material_card_id"], 0)))
        if item["business_card_id"]:
            business_gain += max(0, target_per_card - int(business_counts.get(item["business_card_id"], 0)))
    return material_gain, business_gain


def _article_outcomes(session, article_ids: list[str]) -> list[dict[str, Any]]:
    if not article_ids:
        return []
    pipeline = MaterialPipelineV2()
    article_rows = {item.id: item for item in session.scalars(select(ArticleORM).where(ArticleORM.id.in_(article_ids)))}
    material_rows = list(session.scalars(select(MaterialSpanORM).where(MaterialSpanORM.article_id.in_(article_ids))))
    grouped_materials: dict[str, list[MaterialSpanORM]] = defaultdict(list)
    for item in material_rows:
        grouped_materials[str(item.article_id)].append(item)

    outcomes: list[dict[str, Any]] = []
    for article_id in article_ids:
        article = article_rows.get(article_id)
        materials = grouped_materials.get(article_id, [])
        served_cards: list[dict[str, str]] = []
        for material in materials:
            if str(material.status) != MaterialStatus.PROMOTED.value or str(material.release_channel) != ReleaseChannel.STABLE.value:
                continue
            served_cards.extend(_material_served_cards(pipeline, material=material, article=article))
        outcomes.append(
            {
                "article_id": article_id,
                "title": str(getattr(article, "title", "") or ""),
                "source_url": str(getattr(article, "source_url", "") or ""),
                "article_status": str(getattr(article, "status", "") or ""),
                "material_count": len(materials),
                "stable_material_count": sum(
                    1
                    for item in materials
                    if str(item.status) == MaterialStatus.PROMOTED.value and str(item.release_channel) == ReleaseChannel.STABLE.value
                ),
                "gray_material_count": sum(
                    1
                    for item in materials
                    if str(item.release_channel) == ReleaseChannel.GRAY.value
                ),
                "effective_served_cards": served_cards[:10],
            }
        )
    return outcomes


def _promote_gray_for_card_gaps(
    session,
    *,
    target_per_card: int,
    per_article_cap: int,
    min_quality_score: float,
    enable_narrow_review_pending_bridge: bool,
    review_pending_recent_days: int,
    review_pending_min_quality_score: float,
    dry_run: bool,
) -> dict[str, Any]:
    pipeline = MaterialPipelineV2()
    inventory = _compute_inventory(session, target_per_card=target_per_card)
    material_counts: Counter[str] = inventory["material_counts"]
    business_counts: Counter[str] = inventory["business_counts"]
    stable_article_counts: Counter[str] = inventory["stable_article_counts"]
    article_source_lookup = _article_source_lookup(session)
    article_cache: dict[str, ArticleORM | None] = {}
    recent_cutoff = datetime.now(timezone.utc) - timedelta(days=review_pending_recent_days)

    gray_candidates = list(
        session.scalars(
            select(MaterialSpanORM).where(
                MaterialSpanORM.status == MaterialStatus.GRAY.value,
                MaterialSpanORM.release_channel == ReleaseChannel.GRAY.value,
                MaterialSpanORM.is_primary.is_(True),
                MaterialSpanORM.v2_index_version.is_not(None),
            )
        )
    )
    review_status_lookup = _review_status_lookup(session, [item.id for item in gray_candidates])

    ranked: list[tuple[float, MaterialSpanORM, list[dict[str, str]], dict[str, Any]]] = []
    pending_bridge_preview: list[dict[str, Any]] = []
    pending_bridge_reasons: Counter[str] = Counter()
    for item in gray_candidates:
        quality_score = float(item.quality_score or max((item.family_scores or {}).values() or [0.0]))
        if quality_score < min_quality_score:
            continue
        article_id = str(item.article_id)
        if article_id not in article_cache:
            article_cache[article_id] = session.get(ArticleORM, article_id)
        article = article_cache.get(article_id)
        if article is None:
            continue
        served_cards = _material_served_cards(pipeline, material=item, article=article)
        if not served_cards:
            continue
        review_status = review_status_lookup.get(item.id)
        admission_mode = "strict_review"
        if review_status not in SERVABLE_REVIEW_STATUSES:
            if not enable_narrow_review_pending_bridge:
                continue
            allowed, reason = _is_narrow_review_pending_bridge_candidate(
                item=item,
                article=article,
                review_status=review_status,
                served_cards=served_cards,
                quality_score=quality_score,
                recent_cutoff=recent_cutoff,
                review_pending_min_quality_score=review_pending_min_quality_score,
            )
            pending_bridge_reasons[reason] += 1
            if not allowed:
                continue
            admission_mode = "narrow_review_pending_bridge"
            if len(pending_bridge_preview) < 20:
                pending_bridge_preview.append(
                    {
                        "material_id": str(item.id),
                        "article_id": article_id,
                        "title": str(getattr(article, "title", "") or ""),
                        "source": str(getattr(article, "source", "") or ""),
                        "quality_score": quality_score,
                        "review_status": str(review_status or ""),
                        "bridge_reason": reason,
                        "served_cards": served_cards[:5],
                    }
                )
        material_gain, business_gain = _candidate_gain(
            served_cards,
            material_counts=material_counts,
            business_counts=business_counts,
            target_per_card=target_per_card,
        )
        if material_gain <= 0 and business_gain <= 0:
            continue
        source_name = article_source_lookup.get(article_id, "unknown")
        article_penalty = stable_article_counts.get(article_id, 0) * 0.10
        source_penalty = sum(1 for mid, source in article_source_lookup.items() if source == source_name and stable_article_counts.get(mid, 0) > 0) * 0.005
        score = round((quality_score * 2.0) + material_gain + 0.8 * business_gain - article_penalty - source_penalty, 6)
        ranked.append(
            (
                score,
                item,
                served_cards,
                {
                    "review_status": str(review_status or ""),
                    "admission_mode": admission_mode,
                    "quality_score": quality_score,
                },
            )
        )

    ranked.sort(key=lambda payload: payload[0], reverse=True)
    promoted: list[dict[str, Any]] = []
    promotion_mode_counts: Counter[str] = Counter()
    for _, item, served_cards, meta in ranked:
        if stable_article_counts.get(str(item.article_id), 0) >= per_article_cap:
            continue
        material_gain, business_gain = _candidate_gain(
            served_cards,
            material_counts=material_counts,
            business_counts=business_counts,
            target_per_card=target_per_card,
        )
        if material_gain <= 0 and business_gain <= 0:
            continue

        item.status = MaterialStatus.PROMOTED.value
        item.release_channel = ReleaseChannel.STABLE.value
        item.gray_ratio = 0.0
        item.gray_reason = "auto_expand_effective_card_inventory"
        promotion_mode_counts[meta["admission_mode"]] += 1
        promoted.append(
            {
                "material_id": item.id,
                "article_id": item.article_id,
                "quality_score": meta["quality_score"],
                "review_status": meta["review_status"],
                "admission_mode": meta["admission_mode"],
                "served_cards": served_cards,
            }
        )
        stable_article_counts[str(item.article_id)] += 1
        for served in served_cards:
            material_counts[served["material_card_id"]] += 1
            if served["business_card_id"]:
                business_counts[served["business_card_id"]] += 1

    if dry_run:
        session.rollback()
    else:
        session.commit()

    return {
        "promoted_count": len(promoted),
        "promoted_preview": promoted[:20],
        "promotion_mode_counts": dict(promotion_mode_counts),
        "pending_bridge_preview": pending_bridge_preview,
        "pending_bridge_reasons": dict(pending_bridge_reasons),
    }


def _to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Effective Card Inventory Expansion",
        "",
        f"- target_per_card: {report.get('target_per_card', 0)}",
        f"- rounds_executed: {report.get('rounds_executed', 0)}",
        f"- final_material_cards_below_target: {report.get('final_material_gap_summary', {}).get('cards_below_target', 0)}",
        f"- final_business_cards_below_target: {report.get('final_business_gap_summary', {}).get('cards_below_target', 0)}",
        "",
        "## Round History",
    ]
    for row in report.get("round_history", []):
        lines.append(
            f"- round={row['round']} sources={row['sources']} processed_articles={row['processed_article_count']} promoted={row['promoted_count']}"
        )
    return "\n".join(lines) + "\n"


def _write_report(output_json: Path, output_md: Path, report: dict[str, Any]) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    output_md.write_text(_to_markdown(report), encoding="utf-8")


def _build_report(
    *,
    args: argparse.Namespace,
    initial_inventory: dict[str, Any],
    final_inventory: dict[str, Any],
    round_history: list[dict[str, Any]],
    run_status: str = "completed",
    interruption_reason: str | None = None,
) -> dict[str, Any]:
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_status": run_status,
        "interruption_reason": interruption_reason,
        "target_per_card": args.target_per_card,
        "rounds_executed": len(round_history),
        "disable_llm_during_crawl": bool(args.disable_llm_during_crawl),
        "enable_narrow_review_pending_bridge": bool(args.enable_narrow_review_pending_bridge),
        "review_pending_recent_days": args.review_pending_recent_days,
        "review_pending_min_quality_score": args.review_pending_min_quality_score,
        "initial_material_gap_summary": {
            "cards_below_target": sum(1 for row in initial_inventory["material_gap_rows"] if row["gap"] > 0),
            "worst_gap": max((row["gap"] for row in initial_inventory["material_gap_rows"]), default=0),
        },
        "initial_business_gap_summary": {
            "cards_below_target": sum(1 for row in initial_inventory["business_gap_rows"] if row["gap"] > 0),
            "worst_gap": max((row["gap"] for row in initial_inventory["business_gap_rows"]), default=0),
        },
        "final_material_gap_summary": {
            "cards_below_target": sum(1 for row in final_inventory["material_gap_rows"] if row["gap"] > 0),
            "worst_gap": max((row["gap"] for row in final_inventory["material_gap_rows"]), default=0),
        },
        "final_business_gap_summary": {
            "cards_below_target": sum(1 for row in final_inventory["business_gap_rows"] if row["gap"] > 0),
            "worst_gap": max((row["gap"] for row in final_inventory["business_gap_rows"]), default=0),
        },
        "initial_top_material_gaps": initial_inventory["material_gap_rows"][:20],
        "initial_top_business_gaps": initial_inventory["business_gap_rows"][:20],
        "final_top_material_gaps": final_inventory["material_gap_rows"][:20],
        "final_top_business_gaps": final_inventory["business_gap_rows"][:20],
        "round_history": round_history,
    }
    return report


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()
    source_queue = list(args.sources or preferred_sources())
    _apply_crawl_article_limit_override(source_queue, args.crawl_article_limit)
    _apply_llm_disable_override(args.disable_llm_during_crawl)
    session = get_session()
    try:
        initial_inventory = _compute_inventory(session, target_per_card=args.target_per_card)
        round_history: list[dict[str, Any]] = []
        source_cursor = 0
        latest_inventory = initial_inventory

        for round_index in range(1, args.max_rounds + 1):
            current_inventory = _compute_inventory(session, target_per_card=args.target_per_card)
            latest_inventory = current_inventory
            if _target_met(current_inventory):
                break
            round_sources = source_queue[source_cursor : source_cursor + args.max_sources_per_round]
            if not round_sources:
                break
            source_cursor += len(round_sources)
            processed_article_ids: list[str] = []
            crawl_results: list[dict[str, Any]] = []
            for source_id in round_sources:
                try:
                    result = run_crawl_for_source(session, source_id)
                except Exception as exc:
                    result = {
                        "source_id": source_id,
                        "status": "error",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "processed_article_ids": [],
                    }
                crawl_results.append(result)
                processed_article_ids.extend(result.get("processed_article_ids", []))
                checkpoint_report = _build_report(
                    args=args,
                    initial_inventory=initial_inventory,
                    final_inventory=_compute_inventory(session, target_per_card=args.target_per_card),
                    round_history=round_history
                    + [
                        {
                            "round": round_index,
                            "sources": round_sources,
                            "processed_article_count": len(set(processed_article_ids)),
                            "processed_article_outcomes": _article_outcomes(session, list(dict.fromkeys(processed_article_ids)))[:10],
                            "crawl_results": crawl_results,
                            "promoted_count": 0,
                            "promotion_mode_counts": {},
                            "promoted_preview": [],
                            "pending_bridge_preview": [],
                            "pending_bridge_reasons": {},
                            "material_cards_below_target_after": sum(1 for row in _compute_inventory(session, target_per_card=args.target_per_card)["material_gap_rows"] if row["gap"] > 0),
                            "business_cards_below_target_after": sum(1 for row in _compute_inventory(session, target_per_card=args.target_per_card)["business_gap_rows"] if row["gap"] > 0),
                        }
                    ],
                    run_status="running",
                )
                _write_report(args.output_json, args.output_md, checkpoint_report)
            promote_result = _promote_gray_for_card_gaps(
                session,
                target_per_card=args.target_per_card,
                per_article_cap=args.per_article_cap,
                min_quality_score=args.min_quality_score,
                enable_narrow_review_pending_bridge=args.enable_narrow_review_pending_bridge,
                review_pending_recent_days=args.review_pending_recent_days,
                review_pending_min_quality_score=args.review_pending_min_quality_score,
                dry_run=args.dry_run,
            )
            after_inventory = _compute_inventory(session, target_per_card=args.target_per_card)
            latest_inventory = after_inventory
            round_history.append(
                {
                    "round": round_index,
                    "sources": round_sources,
                    "processed_article_count": len(set(processed_article_ids)),
                    "processed_article_outcomes": _article_outcomes(session, list(dict.fromkeys(processed_article_ids)))[:10],
                    "crawl_results": crawl_results,
                    "promoted_count": promote_result["promoted_count"],
                    "promotion_mode_counts": promote_result["promotion_mode_counts"],
                    "promoted_preview": promote_result["promoted_preview"][:10],
                    "pending_bridge_preview": promote_result["pending_bridge_preview"][:10],
                    "pending_bridge_reasons": promote_result["pending_bridge_reasons"],
                    "material_cards_below_target_after": sum(1 for row in after_inventory["material_gap_rows"] if row["gap"] > 0),
                    "business_cards_below_target_after": sum(1 for row in after_inventory["business_gap_rows"] if row["gap"] > 0),
                }
            )
            _write_report(
                args.output_json,
                args.output_md,
                _build_report(
                    args=args,
                    initial_inventory=initial_inventory,
                    final_inventory=after_inventory,
                    round_history=round_history,
                    run_status="running",
                ),
            )

        final_inventory = _compute_inventory(session, target_per_card=args.target_per_card)
        report = _build_report(
            args=args,
            initial_inventory=initial_inventory,
            final_inventory=final_inventory,
            round_history=round_history,
            run_status="completed",
        )
        _write_report(args.output_json, args.output_md, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except KeyboardInterrupt:
        interrupted_inventory = _compute_inventory(session, target_per_card=args.target_per_card)
        partial_report = _build_report(
            args=args,
            initial_inventory=initial_inventory,
            final_inventory=interrupted_inventory,
            round_history=round_history,
            run_status="interrupted",
            interruption_reason="keyboard_interrupt",
        )
        _write_report(args.output_json, args.output_md, partial_report)
        print(json.dumps(partial_report, ensure_ascii=False, indent=2))
        return 130
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
