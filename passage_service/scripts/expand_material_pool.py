from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.core.config import get_config_bundle
from app.core.enums import MaterialStatus, ReleaseChannel, ReviewStatus
from app.domain.services.ingest_service import run_crawl_for_source
from app.domain.services.pool_service import PoolService
from app.infra.db.orm.article import ArticleORM
from app.infra.db.orm.material_span import MaterialSpanORM
from app.infra.db.orm.review import TaggingReviewORM
from app.infra.db.session import get_session, init_db
from app.infra.plugins.loader import load_plugins


QUESTION_TYPES = ("main_idea", "continuation", "sentence_order", "sentence_fill")
SERVABLE_REVIEW_STATUSES = {
    ReviewStatus.AUTO_TAGGED.value,
    ReviewStatus.REVIEW_CONFIRMED.value,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand the stable material pool toward target size and question-type coverage.")
    parser.add_argument("--target-stable-materials", type=int, default=1000, help="Desired promoted/stable material count.")
    parser.add_argument("--min-per-question-type", type=int, default=100, help="Minimum promoted/stable coverage per question_type.")
    parser.add_argument("--max-rounds", type=int, default=4, help="Maximum crawl/promote rounds.")
    parser.add_argument("--per-article-cap", type=int, default=8, help="Maximum promoted/stable materials to keep from one article.")
    parser.add_argument("--min-quality-score", type=float, default=0.45, help="Soft minimum quality score for automatic promotion.")
    parser.add_argument("--sources", nargs="*", default=None, help="Optional subset of source ids to crawl.")
    parser.add_argument("--promote-only", action="store_true", help="Skip crawling and only expand from current gray materials.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write state changes.")
    return parser.parse_args()


def preferred_sources() -> list[str]:
    configured = [item.get("id") for item in get_config_bundle().sources.get("sources", []) if item.get("enabled", True)]
    priority = [
        "people",
        "xinhuanet",
        "gmw",
        "banyuetan",
        "ce",
        "cyol",
        "gov",
        "qstheory",
        "lifeweek",
        "whb",
        "thepaper",
        "guokr",
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for source_id in priority + configured:
        if source_id and source_id in configured and source_id not in seen:
            seen.add(source_id)
            ordered.append(source_id)
    return ordered


def stats_snapshot(session) -> dict[str, Any]:
    return PoolService(session).get_pool_stats()


def target_met(stats: dict[str, Any], *, target_stable_materials: int, min_per_question_type: int) -> bool:
    stable_total = int(stats.get("promoted_stable_total", 0))
    coverage = stats.get("question_type_coverage_promoted_stable", {}) or {}
    if stable_total < target_stable_materials:
        return False
    return all(int(coverage.get(question_type, 0)) >= min_per_question_type for question_type in QUESTION_TYPES)


def _material_question_types(pool_service: PoolService, item: MaterialSpanORM) -> set[str]:
    return pool_service._material_question_types(item)


def _article_source_lookup(session) -> dict[str, str]:
    return {
        article.id: str(article.source or "unknown")
        for article in session.scalars(select(ArticleORM))
    }


def _review_status_lookup(session, material_ids: list[str]) -> dict[str, str]:
    if not material_ids:
        return {}
    rows = session.execute(
        select(TaggingReviewORM.material_id, TaggingReviewORM.status).where(TaggingReviewORM.material_id.in_(material_ids))
    ).all()
    return {material_id: status for material_id, status in rows}


def _stable_state(session, pool_service: PoolService) -> tuple[Counter, Counter, Counter]:
    stable_items = list(
        session.scalars(
            select(MaterialSpanORM).where(
                MaterialSpanORM.status == MaterialStatus.PROMOTED.value,
                MaterialSpanORM.release_channel == ReleaseChannel.STABLE.value,
                MaterialSpanORM.is_primary.is_(True),
            )
        )
    )
    stable_question_counts: Counter[str] = Counter()
    stable_article_counts: Counter[str] = Counter()
    stable_source_counts: Counter[str] = Counter()
    article_source_lookup = _article_source_lookup(session)
    for item in stable_items:
        stable_article_counts[item.article_id] += 1
        stable_source_counts[article_source_lookup.get(item.article_id, "unknown")] += 1
        for question_type in _material_question_types(pool_service, item):
            stable_question_counts[question_type] += 1
    return stable_question_counts, stable_article_counts, stable_source_counts


def _candidate_score(
    *,
    item: MaterialSpanORM,
    covered_types: set[str],
    stable_question_counts: Counter,
    stable_article_counts: Counter,
    stable_source_counts: Counter,
    article_source_lookup: dict[str, str],
    min_per_question_type: int,
) -> float:
    quality_score = float(item.quality_score or max((item.family_scores or {}).values() or [0.0]))
    deficits = sum(max(0, min_per_question_type - stable_question_counts.get(question_type, 0)) for question_type in covered_types)
    deficit_boost = deficits / max(min_per_question_type, 1)
    quality_penalty = len(item.quality_flags or []) * 0.05
    article_penalty = stable_article_counts.get(item.article_id, 0) * 0.10
    source_name = article_source_lookup.get(item.article_id, "unknown")
    source_penalty = stable_source_counts.get(source_name, 0) * 0.01
    family_bonus = 0.0
    if "sentence_order" in covered_types:
        family_bonus += 0.35
    if "sentence_fill" in covered_types:
        family_bonus += 0.30
    if "continuation" in covered_types:
        family_bonus += 0.40
    return round((quality_score * 2.0) + deficit_boost + family_bonus - quality_penalty - article_penalty - source_penalty, 6)


def promote_gray_materials(
    session,
    *,
    target_stable_materials: int,
    min_per_question_type: int,
    per_article_cap: int,
    min_quality_score: float,
    dry_run: bool,
) -> dict[str, Any]:
    pool_service = PoolService(session)
    article_source_lookup = _article_source_lookup(session)
    stable_question_counts, stable_article_counts, stable_source_counts = _stable_state(session, pool_service)
    current_stable_total = sum(stable_article_counts.values())

    gray_candidates = list(
        session.scalars(
            select(MaterialSpanORM).where(
                MaterialSpanORM.status == MaterialStatus.GRAY.value,
                MaterialSpanORM.release_channel == ReleaseChannel.GRAY.value,
                MaterialSpanORM.is_primary.is_(True),
            )
        )
    )
    review_status_lookup = _review_status_lookup(session, [item.id for item in gray_candidates])
    ranked: list[tuple[float, MaterialSpanORM, set[str]]] = []
    for item in gray_candidates:
        if review_status_lookup.get(item.id) not in SERVABLE_REVIEW_STATUSES:
            continue
        covered_types = _material_question_types(pool_service, item)
        if not covered_types:
            continue
        quality_score = float(item.quality_score or max((item.family_scores or {}).values() or [0.0]))
        if quality_score < min_quality_score and all(stable_question_counts.get(question_type, 0) >= min_per_question_type for question_type in covered_types):
            continue
        score = _candidate_score(
            item=item,
            covered_types=covered_types,
            stable_question_counts=stable_question_counts,
            stable_article_counts=stable_article_counts,
            stable_source_counts=stable_source_counts,
            article_source_lookup=article_source_lookup,
            min_per_question_type=min_per_question_type,
        )
        ranked.append((score, item, covered_types))

    ranked.sort(key=lambda payload: payload[0], reverse=True)
    promoted: list[dict[str, Any]] = []
    for _, item, covered_types in ranked:
        if current_stable_total >= target_stable_materials and all(
            stable_question_counts.get(question_type, 0) >= min_per_question_type for question_type in QUESTION_TYPES
        ):
            break
        if stable_article_counts.get(item.article_id, 0) >= per_article_cap:
            continue

        item.status = MaterialStatus.PROMOTED.value
        item.release_channel = ReleaseChannel.STABLE.value
        item.gray_ratio = 0.0
        item.gray_reason = "auto_expand_pool"
        promoted.append(
            {
                "material_id": item.id,
                "article_id": item.article_id,
                "primary_family": item.primary_family,
                "question_types": sorted(covered_types),
                "quality_score": float(item.quality_score or 0.0),
            }
        )
        current_stable_total += 1
        stable_article_counts[item.article_id] += 1
        stable_source_counts[article_source_lookup.get(item.article_id, "unknown")] += 1
        for question_type in covered_types:
            stable_question_counts[question_type] += 1

    if dry_run:
        session.rollback()
    else:
        session.commit()
        for row in promoted:
            pool_service.audit_repo.log(
                "material",
                row["material_id"],
                "state_change",
                {"status": MaterialStatus.PROMOTED.value, "release_channel": ReleaseChannel.STABLE.value, "reason": "auto_expand_pool"},
            )

    return {
        "promoted_count": len(promoted),
        "promoted_preview": promoted[:20],
        "question_type_coverage_after": dict(stable_question_counts),
        "stable_total_after": current_stable_total,
    }


def crawl_round(session, source_ids: list[str]) -> dict[str, Any]:
    crawl_results: list[dict[str, Any]] = []
    new_processed_articles = 0
    for source_id in source_ids:
        result = run_crawl_for_source(session, source_id)
        crawl_results.append(result)
        new_processed_articles += len(result.get("processed_article_ids", []))
    return {
        "source_ids": source_ids,
        "crawl_results": crawl_results,
        "new_processed_articles": new_processed_articles,
    }


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()
    session = get_session()
    source_ids = args.sources or preferred_sources()

    try:
        rounds: list[dict[str, Any]] = []
        for round_index in range(1, args.max_rounds + 1):
            before_stats = stats_snapshot(session)
            promotion_result = promote_gray_materials(
                session,
                target_stable_materials=args.target_stable_materials,
                min_per_question_type=args.min_per_question_type,
                per_article_cap=args.per_article_cap,
                min_quality_score=args.min_quality_score,
                dry_run=args.dry_run,
            )
            after_promotion_stats = stats_snapshot(session)

            round_result: dict[str, Any] = {
                "round": round_index,
                "before": {
                    "promoted_stable_total": before_stats.get("promoted_stable_total", 0),
                    "question_type_coverage_promoted_stable": before_stats.get("question_type_coverage_promoted_stable", {}),
                },
                "promotion": promotion_result,
                "after_promotion": {
                    "promoted_stable_total": after_promotion_stats.get("promoted_stable_total", 0),
                    "question_type_coverage_promoted_stable": after_promotion_stats.get("question_type_coverage_promoted_stable", {}),
                },
            }

            if target_met(
                after_promotion_stats,
                target_stable_materials=args.target_stable_materials,
                min_per_question_type=args.min_per_question_type,
            ):
                rounds.append(round_result)
                break

            if args.promote_only:
                rounds.append(round_result)
                break

            crawl_result = crawl_round(session, source_ids)
            round_result["crawl"] = crawl_result
            after_crawl_stats = stats_snapshot(session)
            round_result["after_crawl"] = {
                "promoted_stable_total": after_crawl_stats.get("promoted_stable_total", 0),
                "question_type_coverage_promoted_stable": after_crawl_stats.get("question_type_coverage_promoted_stable", {}),
                "articles_total": after_crawl_stats.get("articles_total", 0),
                "materials_total": after_crawl_stats.get("materials_total", 0),
            }
            rounds.append(round_result)

            if crawl_result["new_processed_articles"] == 0 and promotion_result["promoted_count"] == 0:
                break

        final_stats = stats_snapshot(session)
        summary = {
            "requested_targets": {
                "target_stable_materials": args.target_stable_materials,
                "min_per_question_type": args.min_per_question_type,
            },
            "final_stats": final_stats,
            "targets_met": target_met(
                final_stats,
                target_stable_materials=args.target_stable_materials,
                min_per_question_type=args.min_per_question_type,
            ),
            "rounds": rounds,
        }
        export_path = PROJECT_ROOT / "exports" / f"material_pool_expand_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
