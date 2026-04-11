from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select


PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.orm.review import TaggingReviewORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit recent crawl admission blockers.")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def _sample_material(material: MaterialSpanORM, review_status: str | None) -> dict:
    return {
        "material_id": str(material.id),
        "quality_score": float(material.quality_score or 0.0),
        "status": str(material.status or ""),
        "release_channel": str(material.release_channel or ""),
        "is_primary": bool(material.is_primary),
        "gray_reason": str(material.gray_reason or ""),
        "v2_business_family_ids": [str(item) for item in (material.v2_business_family_ids or [])],
        "review_status": str(review_status or ""),
        "created_at": str(material.created_at),
        "updated_at": str(material.updated_at),
    }


def _article_row(article: ArticleORM, materials: list[MaterialSpanORM], review_map: dict[str, str]) -> dict:
    primary = [item for item in materials if item.is_primary]
    return {
        "article_id": str(article.id),
        "title": str(article.title or ""),
        "source": str(article.source or ""),
        "status": str(article.status or ""),
        "updated_at": str(article.updated_at),
        "material_count": len(materials),
        "primary_count": len(primary),
        "sample_primary": [_sample_material(item, review_map.get(item.id)) for item in primary[:3]],
    }


def _to_markdown(report: dict) -> str:
    summary = report["summary"]
    lines = [
        "# Recent Crawl Admission Blocker Audit",
        "",
        f"- recent_days: {report['days']}",
        f"- recent_articles: {summary['recent_articles']}",
        f"- tagged_articles: {summary['tagged_articles']}",
        f"- articles_zero_material: {summary['articles_zero_material']}",
        f"- articles_no_primary_material: {summary['articles_no_primary_material']}",
        f"- articles_primary_gray_review_pending: {summary['articles_primary_gray_review_pending']}",
        f"- articles_primary_gray_no_v2: {summary['articles_primary_gray_no_v2']}",
        f"- articles_primary_gray_review_pending_with_v2: {summary['articles_primary_gray_review_pending_with_v2']}",
        "",
        "## Material Status/Review Buckets",
    ]
    for key, value in sorted(report["material_bucket_counts"].items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Example Articles",
            "",
            "### No Primary Material",
        ]
    )
    for item in report["examples"]["no_primary_material"]:
        lines.append(f"- {item['source']} | {item['title']} | material_count={item['material_count']}")
    lines.extend(
        [
            "",
            "### Primary Gray Review Pending",
        ]
    )
    for item in report["examples"]["primary_gray_review_pending"]:
        lines.append(f"- {item['source']} | {item['title']} | primary_count={item['primary_count']}")
    lines.extend(
        [
            "",
            "### Primary Gray No V2",
        ]
    )
    for item in report["examples"]["primary_gray_no_v2"]:
        lines.append(f"- {item['source']} | {item['title']} | primary_count={item['primary_count']}")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    init_db()
    session = get_session()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
        articles = list(
            session.scalars(
                select(ArticleORM).where(ArticleORM.updated_at >= cutoff).order_by(ArticleORM.updated_at.desc())
            )
        )
        article_ids = [item.id for item in articles]
        materials = list(
            session.scalars(select(MaterialSpanORM).where(MaterialSpanORM.article_id.in_(article_ids)))
        ) if article_ids else []
        review_rows = session.execute(select(TaggingReviewORM.material_id, TaggingReviewORM.status)).all()
        review_map = {material_id: status for material_id, status in review_rows}

        grouped: dict[str, list[MaterialSpanORM]] = {article_id: [] for article_id in article_ids}
        for material in materials:
            grouped[str(material.article_id)].append(material)

        summary = {
            "recent_articles": len(articles),
            "tagged_articles": sum(1 for item in articles if str(item.status) == "tagged"),
            "articles_zero_material": 0,
            "articles_no_primary_material": 0,
            "articles_primary_gray_review_pending": 0,
            "articles_primary_gray_no_v2": 0,
            "articles_primary_gray_review_pending_with_v2": 0,
        }
        examples = {
            "no_primary_material": [],
            "primary_gray_review_pending": [],
            "primary_gray_no_v2": [],
        }
        material_bucket_counts: Counter[str] = Counter()

        for article in articles:
            article_materials = grouped.get(article.id, [])
            if not article_materials:
                summary["articles_zero_material"] += 1
                continue

            primary = [item for item in article_materials if item.is_primary]
            if not primary:
                summary["articles_no_primary_material"] += 1
                if len(examples["no_primary_material"]) < 10:
                    examples["no_primary_material"].append(_article_row(article, article_materials, review_map))
                continue

            for material in primary:
                bucket = "|".join(
                    [
                        f"status={material.status}",
                        f"release={material.release_channel}",
                        f"review={review_map.get(material.id) or 'missing'}",
                        f"v2={'yes' if material.v2_business_family_ids else 'no'}",
                    ]
                )
                material_bucket_counts[bucket] += 1

            has_gray_pending = any(
                item.release_channel == "gray" and review_map.get(item.id) == "review_pending"
                for item in primary
            )
            has_gray_no_v2 = any(
                item.release_channel == "gray" and not (item.v2_business_family_ids or [])
                for item in primary
            )
            has_gray_pending_with_v2 = any(
                item.release_channel == "gray"
                and review_map.get(item.id) == "review_pending"
                and (item.v2_business_family_ids or [])
                for item in primary
            )

            if has_gray_pending:
                summary["articles_primary_gray_review_pending"] += 1
                if len(examples["primary_gray_review_pending"]) < 10:
                    examples["primary_gray_review_pending"].append(_article_row(article, article_materials, review_map))
            if has_gray_no_v2:
                summary["articles_primary_gray_no_v2"] += 1
                if len(examples["primary_gray_no_v2"]) < 10:
                    examples["primary_gray_no_v2"].append(_article_row(article, article_materials, review_map))
            if has_gray_pending_with_v2:
                summary["articles_primary_gray_review_pending_with_v2"] += 1

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "days": args.days,
            "summary": summary,
            "material_bucket_counts": dict(material_bucket_counts),
            "examples": examples,
        }
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        args.output_md.write_text(_to_markdown(report), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
