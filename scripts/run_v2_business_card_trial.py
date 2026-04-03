from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"
DB_PATH = PASSAGE_SERVICE_ROOT / "passage_service.db"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.config import get_config_bundle  # noqa: E402
from app.domain.services.ingest_service import IngestService  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.crawl.discovery import discover_article_urls  # noqa: E402
from app.infra.crawl.extractors.readability_extractor import ReadabilityLikeExtractor  # noqa: E402
from app.infra.crawl.fetchers.http_fetcher import HttpCrawlerFetcher  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl articles and export V2 business-card matched materials.")
    parser.add_argument("--target-articles", type=int, default=100, help="Target total article count to evaluate.")
    parser.add_argument("--business-family-id", type=str, default="title_selection", help="V2 business family id.")
    parser.add_argument("--candidate-limit", type=int, default=40, help="Max candidate count returned by V2 search.")
    parser.add_argument("--min-card-score", type=float, default=0.45, help="Material card threshold.")
    parser.add_argument("--min-business-card-score", type=float, default=0.2, help="Business card threshold.")
    parser.add_argument("--per-source-limit", type=int, default=25, help="Max newly ingested articles per source.")
    parser.add_argument("--sources", nargs="*", default=None, help="Optional subset of source ids.")
    parser.add_argument("--output-dir", type=str, default=None, help="Optional report directory.")
    return parser.parse_args()


def preferred_sources() -> list[str]:
    configured = [item.get("id") for item in get_config_bundle().sources.get("sources", []) if item.get("enabled", True)]
    priority = ["people", "xinhuanet", "qstheory", "gov", "gmw", "lifeweek", "guokr"]
    ordered: list[str] = []
    seen: set[str] = set()
    for source_id in priority + configured:
        if source_id and source_id in configured and source_id not in seen:
            seen.add(source_id)
            ordered.append(source_id)
    return ordered


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()

    before_total = count_articles()
    print(f"[trial] service_db={DB_PATH}")
    print(f"[trial] existing_articles={before_total}")

    source_ids = args.sources or preferred_sources()
    crawl_results: list[dict] = []
    newly_ingested_ids: list[str] = []

    session = get_session()
    try:
        for source_id in source_ids:
            current_total = count_articles()
            if current_total >= args.target_articles:
                break
            remaining = args.target_articles - current_total
            source_limit = min(args.per_source_limit, remaining)
            result = crawl_source_ingest_only(session, source_id, article_limit=source_limit)
            crawl_results.append(result)
            newly_ingested_ids.extend(result.get("ingested_article_ids", []))
            current_total = count_articles()
            print(
                f"[trial] source={source_id} ingested={result['ingested_count']} "
                f"failed={result['failed_count']} skipped_existing={result['skipped_existing_count']} total_articles={current_total}"
            )

        article_ids = latest_article_ids(limit=args.target_articles)
        payload = {
            "business_family_id": args.business_family_id,
            "article_ids": article_ids,
            "candidate_limit": args.candidate_limit,
            "min_card_score": args.min_card_score,
            "min_business_card_score": args.min_business_card_score,
        }
        response = MaterialPipelineV2Service(session).search(payload)
        items = response.get("items") or []
        successful_items = [item for item in items if _is_successful(item)]

        output_dir = Path(args.output_dir) if args.output_dir else REPORTS_ROOT
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"v2_business_card_trial_{timestamp}.json"
        md_path = output_dir / f"v2_business_card_trial_{timestamp}.md"

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "before_total_articles": before_total,
            "after_total_articles": count_articles(),
            "newly_ingested_count": len(newly_ingested_ids),
            "newly_ingested_article_ids": newly_ingested_ids,
            "business_family_id": args.business_family_id,
            "payload": payload,
            "crawl_results": crawl_results,
            "available_business_cards": response.get("available_business_cards") or [],
            "successful_count": len(successful_items),
            "successful_items": successful_items,
            "warnings": response.get("warnings") or [],
        }
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")

        print(f"[trial] successful_items={len(successful_items)} / total_candidates={len(items)}")
        print(f"[trial] json_report={json_path}")
        print(f"[trial] md_report={md_path}")
        return 0
    finally:
        session.close()


def crawl_source_ingest_only(session, source_id: str, *, article_limit: int) -> dict:
    source = find_source_config(source_id)
    if source is None:
        return {"source_id": source_id, "status": "not_found", "ingested_count": 0, "failed_count": 0, "skipped_existing_count": 0, "ingested_article_ids": []}

    fetcher = HttpCrawlerFetcher()
    extractor = ReadabilityLikeExtractor()
    list_urls = source.get("entry_urls") or [source.get("base_url")]
    discovery_limit = max(article_limit * 5, article_limit)
    discovered_urls: list[str] = []
    for list_url in list_urls:
        try:
            html = fetcher.fetch_text(list_url)
            discovered_urls.extend(discover_article_urls(html, list_url, source, limit=discovery_limit))
        except Exception as exc:  # noqa: BLE001
            print(f"[trial] list_failed source={source_id} url={list_url} error={exc}")

    seen: set[str] = set()
    candidates: list[str] = []
    for url in discovered_urls:
        if url not in seen:
            seen.add(url)
            candidates.append(url)

    article_repo = IngestService(session).article_repo
    existing_urls = article_repo.get_existing_source_urls(candidates)
    fresh_candidates = [url for url in candidates if url not in existing_urls]
    skipped_existing = len(candidates) - len(fresh_candidates)
    fresh_candidates = fresh_candidates[:article_limit]

    ingested_count = 0
    failed_count = 0
    ingested_article_ids: list[str] = []
    for index, article_url in enumerate(fresh_candidates, start=1):
        try:
            html = fetcher.fetch_text(article_url)
            parsed = extractor.extract(html, article_url, source)
            raw_text = (parsed.get("raw_text") or "").strip()
            if len(raw_text) < int(source.get("min_body_length", 180)):
                continue
            article = IngestService(session).ingest(
                {
                    "source": source.get("site_name") or source_id,
                    "source_url": article_url,
                    "title": parsed.get("title"),
                    "raw_text": raw_text,
                    "language": source.get("language", "zh"),
                    "domain": source.get("domain"),
                }
            )
            ingested_count += 1
            ingested_article_ids.append(article.id)
            if index == 1 or index % 5 == 0 or index == len(fresh_candidates):
                print(f"[trial] source={source_id} progress={index}/{len(fresh_candidates)} ingested={ingested_count}")
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            print(f"[trial] article_failed source={source_id} url={article_url} error={exc}")

    return {
        "source_id": source_id,
        "site_name": source.get("site_name"),
        "discovered_count": len(discovered_urls),
        "unique_candidate_count": len(candidates),
        "skipped_existing_count": skipped_existing,
        "candidate_count": len(fresh_candidates),
        "ingested_count": ingested_count,
        "failed_count": failed_count,
        "ingested_article_ids": ingested_article_ids,
        "status": "finished",
    }


def find_source_config(source_id: str) -> dict | None:
    sources = get_config_bundle().sources.get("sources", [])
    for source in sources:
        if source.get("id") == source_id:
            return source
    return None


def count_articles() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from articles").fetchone()[0])
    finally:
        conn.close()


def latest_article_ids(*, limit: int) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    try:
        return [row[0] for row in conn.execute("select id from articles order by created_at desc limit ?", (limit,)).fetchall()]
    finally:
        conn.close()


def _is_successful(item: dict) -> bool:
    ctx = item.get("question_ready_context") or {}
    return bool(ctx.get("selected_material_card") and ctx.get("selected_business_card"))


def build_markdown(report: dict) -> str:
    lines = [
        "# V2 业务卡试验报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 服务数据库：`{report['service_db']}`",
        f"- 试验题型：`{report['business_family_id']}`",
        f"- 运行前文章数：`{report['before_total_articles']}`",
        f"- 运行后文章数：`{report['after_total_articles']}`",
        f"- 本次新增文章数：`{report['newly_ingested_count']}`",
        f"- 制作成功材料数：`{report['successful_count']}`",
        "",
        "## 抓取摘要",
        "",
    ]
    for result in report.get("crawl_results", []):
        lines.append(
            f"- `{result.get('source_id')}`: 新增 `{result.get('ingested_count')}` 篇，失败 `{result.get('failed_count')}` 篇，跳过已有 `{result.get('skipped_existing_count')}` 篇"
        )

    lines.extend(["", "## 可用业务卡", ""])
    for item in report.get("available_business_cards", []):
        lines.append(
            f"- `{item.get('business_card_id')}`: {item.get('display_name') or item.get('business_subtype') or '未命名'}"
        )

    lines.extend(["", "## 制作成功材料", ""])
    successful_items = report.get("successful_items", [])
    if not successful_items:
        lines.append("暂无制作成功材料。")
        return "\n".join(lines)

    for index, item in enumerate(successful_items, start=1):
        ctx = item.get("question_ready_context") or {}
        profile = item.get("business_feature_profile") or {}
        lines.extend(
            [
                f"### 样本 {index}",
                f"- article_id: `{item.get('article_id')}`",
                f"- 标题: {item.get('article_title') or '无标题'}",
                f"- 材料卡: `{ctx.get('selected_material_card')}`",
                f"- 业务卡: `{ctx.get('selected_business_card')}`",
                f"- 原型: `{ctx.get('generation_archetype')}`",
                f"- 特征类型: `{profile.get('feature_type')}`",
                f"- 质量分: `{item.get('quality_score')}`",
                "",
                "```text",
                str(item.get("consumable_text") or item.get("text") or "").strip(),
                "```",
                "",
            ]
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
