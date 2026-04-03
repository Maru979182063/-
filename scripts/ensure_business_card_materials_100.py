from __future__ import annotations

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
TARGET = 100
BUSINESS_FAMILY_ID = "title_selection"
CARD_IDS = [
    "cause_effect__conclusion_focus__main_idea",
    "necessary_condition_countermeasure__main_idea",
    "parallel_comprehensive_summary__main_idea",
    "theme_word_focus__main_idea",
    "turning_relation_focus__main_idea",
]
SOURCE_ORDER = [
    "people",
    "xinhuanet",
    "gmw",
    "qstheory",
    "gov",
    "ce",
    "cyol",
    "thepaper",
    "whb",
    "lifeweek",
    "banyuetan",
    "guokr",
    "kepuchina",
    "gmw_tech",
    "yicai",
    "pbc",
    "stats_gov",
    "mof",
    "shandong_gov_profile",
]

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.ingest_service import run_crawl_for_source  # noqa: E402
from app.domain.services.material_v2_index_service import MaterialV2IndexService  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def count_cards() -> Counter:
    conn = sqlite3.connect(DB_PATH)
    try:
        counter: Counter[str] = Counter()
        query = """
        select json_extract(v2_index_payload, '$.title_selection.question_ready_context.selected_business_card') as card_id,
               count(*)
        from material_spans
        where json_extract(v2_index_payload, '$.title_selection.question_ready_context.selected_business_card') is not null
        group by card_id
        """
        for card_id, count in conn.execute(query):
            if card_id:
                counter[card_id] = count
        return counter
    finally:
        conn.close()


def article_count() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from articles").fetchone()[0])
    finally:
        conn.close()


def material_count() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from material_spans").fetchone()[0])
    finally:
        conn.close()


def all_reached(counter: Counter) -> bool:
    return all(counter.get(card_id, 0) >= TARGET for card_id in CARD_IDS)


def compact_counts(counter: Counter) -> dict[str, int]:
    return {card_id: int(counter.get(card_id, 0)) for card_id in CARD_IDS}


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    crawl_results: list[dict] = []

    try:
        baseline_articles = article_count()
        baseline_materials = material_count()
        baseline_counts = count_cards()

        print(f"[quota] baseline_articles={baseline_articles}")
        print(f"[quota] baseline_materials={baseline_materials}")
        print(f"[quota] baseline_counts={compact_counts(baseline_counts)}")

        # First make sure all existing materials have a V2 cache.
        precompute_result = MaterialV2IndexService(session).precompute(
            {"primary_only": False, "status": None, "release_channel": None}
        )
        print(f"[quota] initial_precompute={precompute_result}")

        counts = count_cards()
        print(f"[quota] counts_after_initial_precompute={compact_counts(counts)}")

        for source_id in SOURCE_ORDER:
            if all_reached(counts):
                break
            before_articles = article_count()
            result = run_crawl_for_source(session, source_id)
            processed_article_ids = result.get("processed_article_ids") or []
            if processed_article_ids:
                reindex = MaterialV2IndexService(session).precompute(
                    {
                        "article_ids": processed_article_ids,
                        "primary_only": False,
                        "status": None,
                        "release_channel": None,
                    }
                )
            else:
                reindex = {"updated_count": 0, "material_count": 0}
            after_articles = article_count()
            counts = count_cards()
            snapshot = {
                **result,
                "before_articles": before_articles,
                "after_articles": after_articles,
                "reindex": reindex,
                "counts": compact_counts(counts),
            }
            crawl_results.append(snapshot)
            print(
                "[quota] source={source} ingested={ingested} processed={processed} failed={failed} "
                "articles={articles} counts={counts}".format(
                    source=source_id,
                    ingested=result.get("ingested_count"),
                    processed=len(processed_article_ids),
                    failed=result.get("failed_count"),
                    articles=after_articles,
                    counts=compact_counts(counts),
                )
            )

        final_counts = count_cards()
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "business_family_id": BUSINESS_FAMILY_ID,
            "target": TARGET,
            "baseline_articles": baseline_articles,
            "baseline_materials": baseline_materials,
            "baseline_counts": compact_counts(baseline_counts),
            "final_articles": article_count(),
            "final_materials": material_count(),
            "final_counts": compact_counts(final_counts),
            "all_reached": all_reached(final_counts),
            "crawl_results": crawl_results,
        }

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"business_card_material_quota_{timestamp}.json"
        md_path = REPORTS_ROOT / f"business_card_material_quota_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(f"[quota] json_report={json_path}")
        print(f"[quota] md_report={md_path}")
        print(f"[quota] final_counts={compact_counts(final_counts)}")
        return 0 if report["all_reached"] else 1
    finally:
        session.close()


def build_markdown(report: dict) -> str:
    lines = [
        "# 业务卡材料补量报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 题型：`{report['business_family_id']}`",
        f"- 目标：每张业务卡至少 `{report['target']}` 条材料",
        f"- 初始文章数：`{report['baseline_articles']}`",
        f"- 最终文章数：`{report['final_articles']}`",
        f"- 初始材料数：`{report['baseline_materials']}`",
        f"- 最终材料数：`{report['final_materials']}`",
        f"- 是否全部达标：`{report['all_reached']}`",
        "",
        "## 最终计数",
        "",
    ]
    for card_id, count in report.get("final_counts", {}).items():
        lines.append(f"- `{card_id}`: `{count}`")

    lines.extend(["", "## 逐源过程", ""])
    for item in report.get("crawl_results", []):
        lines.append(
            f"- `{item.get('source_id')}`: 新增 `{item.get('ingested_count')}` 篇，处理 `{item.get('processed_count')}` 篇，失败 `{item.get('failed_count')}` 篇，计数 `{item.get('counts')}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
