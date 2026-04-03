from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
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
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from run_v2_business_card_trial import crawl_source_ingest_only  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ensure every supported business card has enough V2 materials.")
    parser.add_argument("--business-family-id", type=str, default="title_selection")
    parser.add_argument("--target-per-card", type=int, default=11)
    parser.add_argument("--candidate-limit", type=int, default=120)
    parser.add_argument("--min-card-score", type=float, default=0.45)
    parser.add_argument("--min-business-card-score", type=float, default=0.2)
    parser.add_argument("--per-source-limit", type=int, default=20)
    parser.add_argument("--max-total-articles", type=int, default=220)
    parser.add_argument("--sources", nargs="*", default=None)
    return parser.parse_args()


def source_plan() -> list[str]:
    configured = [item.get("id") for item in get_config_bundle().sources.get("sources", [])]
    priority = [
        "kepuchina",
        "gmw_tech",
        "pbc",
        "stats_gov",
        "mof",
        "yicai",
        "people",
        "xinhuanet",
        "qstheory",
        "gov",
        "guokr",
        "gmw",
        "lifeweek",
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
    session = get_session()
    before_total = count_articles()
    chosen_sources = args.sources or source_plan()
    crawl_results: list[dict] = []

    try:
        counts, response = evaluate_coverage(
            session,
            business_family_id=args.business_family_id,
            candidate_limit=args.candidate_limit,
            min_card_score=args.min_card_score,
            min_business_card_score=args.min_business_card_score,
            article_limit=args.max_total_articles,
        )
        supported_cards = [
            item.get("business_card_id")
            for item in (response.get("available_business_cards") or [])
            if item.get("business_card_id")
        ]
        print(f"[coverage] initial_counts={dict(counts)}")
        print(f"[coverage] supported_cards={supported_cards}")

        for source_id in chosen_sources:
            if all(counts.get(card_id, 0) >= args.target_per_card for card_id in supported_cards):
                break
            if count_articles() >= args.max_total_articles:
                break
            result = crawl_source_ingest_only(session, source_id, article_limit=args.per_source_limit)
            crawl_results.append(result)
            counts, response = evaluate_coverage(
                session,
                business_family_id=args.business_family_id,
                candidate_limit=args.candidate_limit,
                min_card_score=args.min_card_score,
                min_business_card_score=args.min_business_card_score,
                article_limit=args.max_total_articles,
            )
            print(f"[coverage] after_source={source_id} counts={dict(counts)}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        json_path = REPORTS_ROOT / f"business_card_coverage_{timestamp}.json"
        md_path = REPORTS_ROOT / f"business_card_coverage_{timestamp}.md"
        final_counts = {card_id: counts.get(card_id, 0) for card_id in supported_cards}
        successful_items = [
            item
            for item in (response.get("items") or [])
            if (item.get("question_ready_context") or {}).get("selected_business_card")
        ]

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "business_family_id": args.business_family_id,
            "target_per_card": args.target_per_card,
            "before_total_articles": before_total,
            "after_total_articles": count_articles(),
            "final_counts": final_counts,
            "all_cards_reached": all(value >= args.target_per_card for value in final_counts.values()),
            "available_business_cards": response.get("available_business_cards") or [],
            "crawl_results": crawl_results,
            "successful_items": successful_items,
            "warnings": response.get("warnings") or [],
        }
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(f"[coverage] json_report={json_path}")
        print(f"[coverage] md_report={md_path}")
        return 0
    finally:
        session.close()


def evaluate_coverage(
    session,
    *,
    business_family_id: str,
    candidate_limit: int,
    min_card_score: float,
    min_business_card_score: float,
    article_limit: int,
) -> tuple[Counter, dict]:
    response = MaterialPipelineV2Service(session).search(
        {
            "business_family_id": business_family_id,
            "article_ids": latest_article_ids(limit=article_limit),
            "candidate_limit": candidate_limit,
            "min_card_score": min_card_score,
            "min_business_card_score": min_business_card_score,
        }
    )
    counts: Counter = Counter()
    for item in response.get("items") or []:
        card_id = (item.get("question_ready_context") or {}).get("selected_business_card")
        if card_id:
            counts[card_id] += 1
    return counts, response


def latest_article_ids(*, limit: int) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    try:
        return [row[0] for row in conn.execute("select id from articles order by created_at desc limit ?", (limit,)).fetchall()]
    finally:
        conn.close()


def count_articles() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from articles").fetchone()[0])
    finally:
        conn.close()


def build_markdown(report: dict) -> str:
    lines = [
        "# 业务卡覆盖报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 题型：`{report['business_family_id']}`",
        f"- 目标：每张业务卡至少 `{report['target_per_card']}` 条",
        f"- 运行前文章数：`{report['before_total_articles']}`",
        f"- 运行后文章数：`{report['after_total_articles']}`",
        f"- 是否全部达标：`{report['all_cards_reached']}`",
        "",
        "## 覆盖计数",
        "",
    ]
    for card_id, count in report.get("final_counts", {}).items():
        lines.append(f"- `{card_id}`: `{count}`")

    lines.extend(["", "## 抓取过程", ""])
    for result in report.get("crawl_results", []):
        lines.append(
            f"- `{result.get('source_id')}`: 新增 `{result.get('ingested_count')}`，失败 `{result.get('failed_count')}`，跳过已有 `{result.get('skipped_existing_count')}`"
        )

    lines.extend(["", "## 样本", ""])
    samples_by_card: dict[str, list[dict]] = {}
    for item in report.get("successful_items", []):
        card_id = (item.get("question_ready_context") or {}).get("selected_business_card")
        if not card_id:
            continue
        samples_by_card.setdefault(card_id, [])
        if len(samples_by_card[card_id]) < 3:
            samples_by_card[card_id].append(item)

    for card_id, items in samples_by_card.items():
        lines.append(f"### {card_id}")
        for item in items:
            ctx = item.get("question_ready_context") or {}
            lines.append(f"- 标题: {item.get('article_title') or '无标题'}")
            lines.append(f"- 材料卡: `{ctx.get('selected_material_card')}`")
            lines.append(f"- 原型: `{ctx.get('generation_archetype')}`")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
