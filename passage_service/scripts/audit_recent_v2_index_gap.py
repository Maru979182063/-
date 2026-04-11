from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select


PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.domain.services.material_v2_index_service import MaterialV2IndexService  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit recent material v2 index gaps.")
    parser.add_argument("--article-limit", type=int, default=120)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def _bucket_for_article(materials: list[MaterialSpanORM]) -> str:
    if not materials:
        return "zero_material"
    stable = [item for item in materials if item.status == "promoted" and item.release_channel == "stable"]
    if not stable:
        return "gray_only"
    return "stable_present"


def _precompute_diagnostic(session, article_ids: list[str]) -> dict[str, Any]:
    before_rows = list(session.scalars(select(MaterialSpanORM).where(MaterialSpanORM.article_id.in_(article_ids))))
    before_map = {item.id: list(item.v2_business_family_ids or []) for item in before_rows}
    service = MaterialV2IndexService(session)
    result = service.precompute({"article_ids": article_ids, "primary_only": True})
    after_rows = list(session.scalars(select(MaterialSpanORM).where(MaterialSpanORM.article_id.in_(article_ids))))
    changed: list[dict[str, Any]] = []
    for item in after_rows:
        before_ids = before_map.get(item.id, [])
        after_ids = list(item.v2_business_family_ids or [])
        if before_ids != after_ids:
            changed.append(
                {
                    "material_id": str(item.id),
                    "article_id": str(item.article_id),
                    "release_channel": str(item.release_channel or ""),
                    "status": str(item.status or ""),
                    "primary_family": str(item.primary_family or ""),
                    "primary_label": str(item.primary_label or ""),
                    "parallel_families": item.parallel_families or [],
                    "before": before_ids,
                    "after": after_ids,
                    "v2_index_version": str(item.v2_index_version or ""),
                }
            )
    session.rollback()
    return {"precompute_result": result, "changed": changed}


def _report(article_limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    try:
        recent_articles = list(
            session.scalars(
                select(ArticleORM).where(ArticleORM.status == "tagged").order_by(ArticleORM.created_at.desc()).limit(article_limit)
            )
        )
        materials = list(
            session.scalars(
                select(MaterialSpanORM).where(MaterialSpanORM.article_id.in_([item.id for item in recent_articles]))
            )
        )
        by_article: dict[str, list[MaterialSpanORM]] = defaultdict(list)
        for material in materials:
            by_article[str(material.article_id)].append(material)

        material_v2_counter = Counter()
        bucket_by_source: dict[str, Counter[str]] = defaultdict(Counter)
        bucket_counts = Counter()
        sample_articles: dict[str, list[dict[str, Any]]] = defaultdict(list)
        stable_empty_v2_samples: list[dict[str, Any]] = []

        for article in recent_articles:
            article_materials = by_article.get(str(article.id), [])
            bucket = _bucket_for_article(article_materials)
            bucket_counts[bucket] += 1
            bucket_by_source[bucket][str(article.source or "unknown")] += 1
            if len(sample_articles[bucket]) < 15:
                sample_articles[bucket].append(
                    {
                        "article_id": str(article.id),
                        "source": str(article.source or ""),
                        "title": str(article.title or ""),
                        "material_count": len(article_materials),
                        "stable_material_count": sum(
                            1 for item in article_materials if item.status == "promoted" and item.release_channel == "stable"
                        ),
                    }
                )

            for material in article_materials:
                v2_ids = list(material.v2_business_family_ids or [])
                key = "with_v2_ids" if v2_ids else "empty_v2_ids"
                material_v2_counter[key] += 1
                if material.release_channel == "gray":
                    material_v2_counter[f"gray_{key}"] += 1
                if material.status == "promoted" and material.release_channel == "stable":
                    material_v2_counter[f"stable_{key}"] += 1
                    if not v2_ids and len(stable_empty_v2_samples) < 20:
                        stable_empty_v2_samples.append(
                            {
                                "article_id": str(article.id),
                                "source": str(article.source or ""),
                                "title": str(article.title or ""),
                                "material_id": str(material.id),
                                "primary_family": str(material.primary_family or ""),
                                "primary_label": str(material.primary_label or ""),
                                "parallel_families": material.parallel_families or [],
                                "quality_score": float(material.quality_score or 0.0),
                            }
                        )

        diagnostic_article_ids = [
            "article_e7a9b68e05c241baa00789f60f191c99",
            "article_d0b5ce5da739461eb69be24d849119f5",
            "article_24b0c9bb761d471d971fa4f0b0ca5b76",
            "article_f5995a4907484d0f979738ea385040bd",
            "article_5de994f976834c5e925e954e4237a35b",
            "article_87b68aae7b6b40f2bf19508053ce689f",
        ]
        diagnostic = _precompute_diagnostic(session, diagnostic_article_ids)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "article_limit": article_limit,
            "bucket_counts": dict(bucket_counts),
            "bucket_by_source": {bucket: dict(counter) for bucket, counter in bucket_by_source.items()},
            "material_v2_counter": dict(material_v2_counter),
            "sample_articles": dict(sample_articles),
            "stable_empty_v2_samples": stable_empty_v2_samples,
            "precompute_diagnostic": diagnostic,
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Recent V2 Index Gap Audit",
        "",
        f"- article_limit: {report.get('article_limit', 0)}",
        f"- bucket_counts: {report.get('bucket_counts', {})}",
        f"- material_v2_counter: {report.get('material_v2_counter', {})}",
        "",
        "## Stable Empty V2 Samples",
    ]
    for row in report.get("stable_empty_v2_samples", [])[:15]:
        lines.append(
            f"- {row['material_id']} | {row['source']} | {row['primary_family']} / {row['primary_label']} | {row['title']}"
        )
    lines.extend(["", "## Precompute Diagnostic"])
    diag = report.get("precompute_diagnostic", {})
    lines.append(f"- precompute_result: {diag.get('precompute_result', {})}")
    for row in diag.get("changed", [])[:15]:
        lines.append(
            f"- {row['material_id']} | before={row['before']} -> after={row['after']} | {row['primary_family']} / {row['primary_label']}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    report = _report(article_limit=args.article_limit)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
