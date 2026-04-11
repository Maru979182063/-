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

from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


FAMILY_ORDER = {"title_selection", "sentence_fill", "sentence_order", "continuation"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit recent crawl conversion failures.")
    parser.add_argument("--article-limit", type=int, default=120)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def _runtime_effective_cards(pipeline: MaterialPipelineV2, *, material: MaterialSpanORM, article: ArticleORM) -> list[dict[str, str]]:
    served: list[dict[str, str]] = []
    families = [str(item) for item in (material.v2_business_family_ids or []) if str(item) in FAMILY_ORDER]
    for family in families:
        payload = pipeline.build_cached_item_from_material(
            material=material,
            article=article,
            business_family_id=family,
            enable_fill_formalization_bridge=(family == "sentence_fill"),
            enable_sentence_order_weak_formal_bridge=(family == "sentence_order"),
            enable_sentence_order_weak_formal_gate=(family == "sentence_order"),
            enable_sentence_order_weak_formal_closing_gate=(family == "sentence_order"),
        )
        qctx = dict(payload.get("question_ready_context") or {}) if payload else {}
        material_card_id = str(qctx.get("selected_material_card") or "")
        if not material_card_id:
            continue
        served.append(
            {
                "family": family,
                "material_card_id": material_card_id,
                "business_card_id": str(qctx.get("selected_business_card") or ""),
                "question_card_id": str(qctx.get("question_card_id") or ""),
            }
        )
    return served


def _classify_bucket(*, material_count: int, stable_material_count: int, effective_count: int) -> str:
    if material_count == 0:
        return "zero_material"
    if stable_material_count == 0:
        return "gray_only"
    if effective_count == 0:
        return "stable_no_effective"
    return "effective"


def _report(article_limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    pipeline = MaterialPipelineV2()
    try:
        articles = list(
            session.scalars(
                select(ArticleORM).where(ArticleORM.status == "tagged").order_by(ArticleORM.created_at.desc()).limit(article_limit)
            )
        )
        bucket_counts = Counter()
        bucket_by_source: dict[str, Counter[str]] = defaultdict(Counter)
        candidate_status_by_bucket: dict[str, Counter[str]] = defaultdict(Counter)
        article_rows: list[dict[str, Any]] = []

        for article in articles:
            candidates = list(session.scalars(select(CandidateSpanORM).where(CandidateSpanORM.article_id == article.id)))
            materials = list(session.scalars(select(MaterialSpanORM).where(MaterialSpanORM.article_id == article.id)))
            stable_materials = [item for item in materials if item.status == "promoted" and item.release_channel == "stable"]
            gray_materials = [item for item in materials if item.release_channel == "gray"]
            effective_cards: list[dict[str, str]] = []
            for material in stable_materials:
                effective_cards.extend(_runtime_effective_cards(pipeline, material=material, article=article))
            bucket = _classify_bucket(
                material_count=len(materials),
                stable_material_count=len(stable_materials),
                effective_count=len(effective_cards),
            )
            bucket_counts[bucket] += 1
            bucket_by_source[bucket][str(article.source or "unknown")] += 1
            for candidate in candidates:
                candidate_status_by_bucket[bucket][str(candidate.status or "unknown")] += 1
            article_rows.append(
                {
                    "article_id": str(article.id),
                    "source": str(article.source or ""),
                    "title": str(article.title or ""),
                    "source_url": str(article.source_url or ""),
                    "bucket": bucket,
                    "candidate_count": len(candidates),
                    "candidate_status_counts": dict(Counter(str(item.status or "unknown") for item in candidates)),
                    "material_count": len(materials),
                    "stable_material_count": len(stable_materials),
                    "gray_material_count": len(gray_materials),
                    "effective_count": len(effective_cards),
                    "effective_cards": effective_cards[:10],
                    "sample_candidate_texts": [str(item.text or "")[:180] for item in candidates[:5]],
                    "sample_materials": [
                        {
                            "material_id": str(item.id),
                            "release_channel": str(item.release_channel or ""),
                            "status": str(item.status or ""),
                            "primary_family": str(item.primary_family or ""),
                            "primary_label": str(item.primary_label or ""),
                            "v2_business_family_ids": [str(fam) for fam in (item.v2_business_family_ids or [])],
                            "text_preview": str(item.text or "")[:180],
                        }
                        for item in materials[:5]
                    ],
                }
            )

        samples_by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in article_rows:
            bucket = row["bucket"]
            if len(samples_by_bucket[bucket]) < 15:
                samples_by_bucket[bucket].append(
                    {
                        "article_id": row["article_id"],
                        "source": row["source"],
                        "title": row["title"],
                        "material_count": row["material_count"],
                        "stable_material_count": row["stable_material_count"],
                        "effective_count": row["effective_count"],
                        "candidate_status_counts": row["candidate_status_counts"],
                    }
                )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "article_limit": article_limit,
            "bucket_counts": dict(bucket_counts),
            "bucket_by_source": {bucket: dict(counter) for bucket, counter in bucket_by_source.items()},
            "candidate_status_by_bucket": {bucket: dict(counter) for bucket, counter in candidate_status_by_bucket.items()},
            "sample_articles_by_bucket": dict(samples_by_bucket),
            "articles": article_rows,
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Recent Crawl Conversion Failure Audit",
        "",
        f"- article_limit: {report.get('article_limit', 0)}",
        f"- bucket_counts: {report.get('bucket_counts', {})}",
        "",
        "## Bucket By Source",
    ]
    for bucket, payload in report.get("bucket_by_source", {}).items():
        lines.append(f"### {bucket}")
        for source, count in sorted(payload.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- {source}: {count}")
    lines.extend(["", "## Sample Articles"])
    for bucket, rows in report.get("sample_articles_by_bucket", {}).items():
        lines.append(f"### {bucket}")
        for row in rows:
            lines.append(
                f"- {row['article_id']} | {row['source']} | materials={row['material_count']} stable={row['stable_material_count']} effective={row['effective_count']} | {row['title']}"
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
