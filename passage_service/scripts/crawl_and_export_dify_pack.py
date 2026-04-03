from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import get_config_bundle
from app.domain.services.dify_export_service import DifyExportService
from app.domain.services.ingest_service import run_crawl_for_source
from app.infra.db.session import get_session, init_db
from app.infra.plugins.loader import load_plugins


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl articles and export a Dify-ready local material pack.")
    parser.add_argument("--target-articles", type=int, default=100, help="Target number of processed articles for this run.")
    parser.add_argument("--limit-materials", type=int, default=None, help="Optional export cap for material count.")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory for the exported Dify package.")
    parser.add_argument("--include-gray", action="store_true", help="Include gray-channel materials in export.")
    parser.add_argument("--sources", nargs="*", default=None, help="Optional subset of source ids to crawl.")
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


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()

    source_ids = args.sources or preferred_sources()
    session = get_session()
    try:
        processed_article_ids: list[str] = []
        crawl_results: list[dict] = []
        processed_seen: set[str] = set()

        for source_id in source_ids:
            result = run_crawl_for_source(session, source_id)
            crawl_results.append(result)
            for article_id in result.get("processed_article_ids", []):
                if article_id not in processed_seen:
                    processed_seen.add(article_id)
                    processed_article_ids.append(article_id)
            if len(processed_article_ids) >= args.target_articles:
                break

        output_dir = args.output_dir or str(
            Path(__file__).resolve().parent.parent / "exports" / f"dify_pack_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        export_result = DifyExportService(session).export_materials(
            article_ids=processed_article_ids,
            output_dir=output_dir,
            limit=args.limit_materials,
            include_gray=args.include_gray,
        )

        run_summary = {
            "requested_target_articles": args.target_articles,
            "processed_article_count": len(processed_article_ids),
            "processed_article_ids": processed_article_ids,
            "crawl_results": crawl_results,
            "export": export_result,
        }
        summary_path = Path(output_dir) / "crawl_run_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(run_summary, ensure_ascii=False, indent=2), encoding="utf-8")

        print(json.dumps(run_summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
