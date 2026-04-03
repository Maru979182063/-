from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"
DB_PATH = PASSAGE_SERVICE_ROOT / "passage_service.db"
BUSINESS_FAMILY_ID = "title_selection"
TARGET = 100
VERSION = "v2.candidate.backfill.20260402a"
CARD_IDS = [
    "cause_effect__conclusion_focus__main_idea",
    "necessary_condition_countermeasure__main_idea",
    "parallel_comprehensive_summary__main_idea",
    "theme_word_focus__main_idea",
    "turning_relation_focus__main_idea",
]

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.utils import new_id  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.ingest.dedupe.content_hash import build_content_hash  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


def count_selected_cards() -> Counter:
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


def summarize(counter: Counter) -> dict[str, int]:
    return {card_id: int(counter.get(card_id, 0)) for card_id in CARD_IDS}


def length_bucket(text: str) -> str:
    size = len((text or "").strip())
    if size < 120:
        return "short"
    if size < 260:
        return "medium"
    return "long"


def paragraph_count(text: str) -> int:
    return max(1, len([part for part in str(text or "").split("\n\n") if part.strip()]))


def sentence_count(text: str) -> int:
    rough = sum(str(text or "").count(token) for token in ("。", "！", "？", "!", "?"))
    return max(1, rough)


def make_material_payload(*, article, candidate, cached_item: dict) -> dict:
    qrc = cached_item.get("question_ready_context") or {}
    fit_scores = {
        item.get("card_id"): item.get("score")
        for item in (cached_item.get("eligible_material_cards") or [])
        if item.get("card_id")
    }
    return {
        "id": new_id("mat"),
        "article_id": article.id,
        "candidate_span_id": candidate.id,
        "text": candidate.text,
        "normalized_text_hash": build_content_hash(candidate.text),
        "material_family_id": "v2_candidate_backfill",
        "is_primary": True,
        "span_type": candidate.span_type,
        "length_bucket": length_bucket(candidate.text),
        "paragraph_count": paragraph_count(candidate.text),
        "sentence_count": sentence_count(candidate.text),
        "status": "gray",
        "release_channel": "gray",
        "gray_ratio": 0.0,
        "gray_reason": None,
        "segmentation_version": VERSION,
        "tag_version": VERSION,
        "fit_version": VERSION,
        "prompt_version": VERSION,
        "primary_family": "V2候选补量",
        "primary_subtype": qrc.get("selected_business_card"),
        "secondary_subtypes": list(cached_item.get("business_card_recommendations") or []),
        "universal_profile": dict(cached_item.get("neutral_signal_profile") or {}),
        "family_scores": {},
        "capability_scores": {},
        "parallel_families": [],
        "structure_features": dict(cached_item.get("article_profile") or {}),
        "family_profiles": {},
        "subtype_candidates": [],
        "secondary_candidates": [],
        "candidate_labels": list(cached_item.get("business_card_recommendations") or []),
        "primary_label": cached_item.get("material_card_id"),
        "decision_trace": {
            "source": "candidate_backfill",
            "selected_business_card": qrc.get("selected_business_card"),
            "selected_material_card": qrc.get("selected_material_card"),
        },
        "primary_route": {
            "business_family_id": BUSINESS_FAMILY_ID,
            "selected_business_card": qrc.get("selected_business_card"),
        },
        "reject_reason": None,
        "variants": [],
        "source": dict(cached_item.get("source") or {}),
        "source_tail": str((cached_item.get("source") or {}).get("source_url") or ""),
        "integrity": {
            "semantic_completeness_score": float((cached_item.get("business_feature_profile") or {}).get("semantic_completeness_score") or 0.0),
            "standalone_readability": float((cached_item.get("business_feature_profile") or {}).get("readability") or 0.0),
        },
        "quality_flags": list(cached_item.get("quality_flags") or []),
        "knowledge_tags": list(dict.fromkeys(
            [
                "v2_candidate_backfill",
                str(qrc.get("selected_business_card") or ""),
                str(cached_item.get("material_card_id") or ""),
            ]
        )),
        "fit_scores": fit_scores,
        "feature_profile": dict(cached_item.get("business_feature_profile") or {}),
        "quality_score": float(cached_item.get("quality_score") or 0.0),
        "v2_index_version": VERSION,
        "v2_business_family_ids": [BUSINESS_FAMILY_ID],
        "v2_index_payload": {BUSINESS_FAMILY_ID: cached_item},
        "usage_count": 0,
        "accept_count": 0,
        "reject_count": 0,
        "last_used_at": None,
    }


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    try:
        baseline_counts = count_selected_cards()
        print(f"[backfill] baseline_counts={summarize(baseline_counts)}")
        deficits = {card_id: max(0, TARGET - baseline_counts.get(card_id, 0)) for card_id in CARD_IDS}
        print(f"[backfill] deficits={deficits}")
        if all(value <= 0 for value in deficits.values()):
            return 0

        existing_candidate_ids = {
            row[0]
            for row in session.query(MaterialSpanORM.candidate_span_id).all()
            if row[0]
        }
        existing_hashes = {
            row[0]
            for row in session.query(MaterialSpanORM.normalized_text_hash).all()
            if row[0]
        }
        candidates = session.query(CandidateSpanORM).order_by(CandidateSpanORM.created_at.desc()).all()
        pipeline = MaterialPipelineV2()
        article_cache: dict[str, ArticleORM | None] = {}
        inserted: list[dict] = []
        inserted_counter: Counter[str] = Counter()

        for candidate in candidates:
            if all(deficits.get(card_id, 0) <= 0 for card_id in CARD_IDS):
                break
            if candidate.id in existing_candidate_ids:
                continue
            text = str(candidate.text or "").strip()
            if len(text) < 80:
                continue
            text_hash = build_content_hash(text)
            if text_hash in existing_hashes:
                continue
            article = article_cache.get(candidate.article_id)
            if article is None:
                article = session.get(ArticleORM, candidate.article_id)
                article_cache[candidate.article_id] = article
            if article is None:
                continue
            temp_material = SimpleNamespace(
                id=f"candmat::{candidate.id}",
                article_id=candidate.article_id,
                candidate_span_id=candidate.id,
                text=text,
                span_type=candidate.span_type,
                start_paragraph=candidate.start_paragraph,
                end_paragraph=candidate.end_paragraph,
                start_sentence=candidate.start_sentence,
                end_sentence=candidate.end_sentence,
                paragraph_count=max(1, candidate.end_paragraph - candidate.start_paragraph + 1),
                sentence_count=max(
                    1,
                    ((candidate.end_sentence or candidate.start_sentence or 0) - (candidate.start_sentence or 0) + 1)
                    if candidate.start_sentence is not None
                    else sentence_count(text),
                ),
                quality_flags=[],
            )
            cached_item = pipeline.build_cached_item_from_material(
                material=temp_material,
                article=article,
                business_family_id=BUSINESS_FAMILY_ID,
            )
            if not cached_item:
                continue
            selected_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
            if not selected_card or deficits.get(selected_card, 0) <= 0:
                continue

            material = MaterialSpanORM(**make_material_payload(article=article, candidate=candidate, cached_item=cached_item))
            session.add(material)
            existing_candidate_ids.add(candidate.id)
            existing_hashes.add(text_hash)
            deficits[selected_card] -= 1
            inserted_counter[selected_card] += 1
            inserted.append(
                {
                    "material_id": material.id,
                    "candidate_span_id": candidate.id,
                    "article_id": article.id,
                    "selected_business_card": selected_card,
                    "title": article.title,
                    "source": article.source,
                }
            )
            if len(inserted) % 50 == 0:
                session.commit()
                print(f"[backfill] inserted={len(inserted)} deficits={deficits}")

        session.commit()
        final_counts = count_selected_cards()
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "business_family_id": BUSINESS_FAMILY_ID,
            "target": TARGET,
            "baseline_articles": article_count(),
            "baseline_materials": material_count() - len(inserted),
            "baseline_counts": summarize(baseline_counts),
            "inserted_count": len(inserted),
            "inserted_counter": dict(inserted_counter),
            "final_articles": article_count(),
            "final_materials": material_count(),
            "final_counts": summarize(final_counts),
            "all_reached": all(final_counts.get(card_id, 0) >= TARGET for card_id in CARD_IDS),
            "inserted_samples": inserted[:200],
        }
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"business_card_candidate_backfill_{timestamp}.json"
        md_path = REPORTS_ROOT / f"business_card_candidate_backfill_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(f"[backfill] json_report={json_path}")
        print(f"[backfill] md_report={md_path}")
        print(f"[backfill] final_counts={report['final_counts']}")
        return 0 if report["all_reached"] else 1
    finally:
        session.close()


def build_markdown(report: dict) -> str:
    lines = [
        "# 候选段补池报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 题型：`{report['business_family_id']}`",
        f"- 目标：每张业务卡至少 `{report['target']}` 条材料",
        f"- 新增材料：`{report['inserted_count']}`",
        f"- 是否全部达标：`{report['all_reached']}`",
        "",
        "## 最终计数",
        "",
    ]
    for card_id, count in report.get("final_counts", {}).items():
        lines.append(f"- `{card_id}`: `{count}`")
    lines.extend(["", "## 本次新增", ""])
    for card_id, count in report.get("inserted_counter", {}).items():
        lines.append(f"- `{card_id}`: `{count}`")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
