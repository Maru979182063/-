from __future__ import annotations

import copy
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
VERSION = "v2.candidate.backfill.multi.20260402a"
TARGETS: dict[str, dict[str, int]] = {
    "title_selection": {
        "cause_effect__conclusion_focus__main_idea": 100,
        "necessary_condition_countermeasure__main_idea": 100,
        "parallel_comprehensive_summary__main_idea": 100,
        "theme_word_focus__main_idea": 100,
        "turning_relation_focus__main_idea": 100,
    },
    "sentence_fill": {
        "sentence_fill__opening_summary__abstract": 100,
        "sentence_fill__opening_topic_intro__abstract": 100,
        "sentence_fill__middle_carry_previous__abstract": 100,
        "sentence_fill__middle_lead_next__abstract": 100,
        "sentence_fill__middle_bridge_both_sides__abstract": 100,
        "sentence_fill__ending_summary__abstract": 100,
        "sentence_fill__ending_countermeasure__abstract": 100,
    },
    "sentence_order": {
        "sentence_order__head_tail_lock__abstract": 100,
        "sentence_order__deterministic_binding__abstract": 100,
        "sentence_order__discourse_logic__abstract": 100,
        "sentence_order__timeline_action_sequence__abstract": 100,
        "sentence_order__head_tail_logic__abstract": 100,
    },
}

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


def count_family_cards() -> dict[str, Counter]:
    conn = sqlite3.connect(DB_PATH)
    try:
        result: dict[str, Counter] = {}
        for family in TARGETS:
            counter: Counter[str] = Counter()
            query = f"""
            select json_extract(v2_index_payload, '$.{family}.question_ready_context.selected_business_card') as card_id,
                   count(*)
            from material_spans
            where json_extract(v2_index_payload, '$.{family}.question_ready_context.selected_business_card') is not null
            group by card_id
            """
            for card_id, count in conn.execute(query):
                if card_id:
                    counter[card_id] = count
            result[family] = counter
        return result
    finally:
        conn.close()


def material_count() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from material_spans").fetchone()[0])
    finally:
        conn.close()


def sentence_count(text: str) -> int:
    rough = sum(str(text or "").count(token) for token in ("。", "！", "？", "!", "?"))
    return max(1, rough)


def paragraph_count(text: str) -> int:
    return max(1, len([part for part in str(text or "").split("\n\n") if part.strip()]))


def length_bucket(text: str) -> str:
    size = len((text or "").strip())
    if size < 120:
        return "short"
    if size < 260:
        return "medium"
    return "long"


def material_payload(*, family: str, article, candidate, cached_item: dict, selected_business_card: str | None = None) -> dict:
    cached_item = copy.deepcopy(cached_item)
    qrc = dict(cached_item.get("question_ready_context") or {})
    if selected_business_card:
        qrc["selected_business_card"] = selected_business_card
    cached_item["question_ready_context"] = qrc
    primary_subtype = str(qrc.get("selected_business_card") or "")
    candidate_labels = list(dict.fromkeys([primary_subtype, *(cached_item.get("business_card_recommendations") or [])]))
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
        "material_family_id": f"v2_candidate_backfill.{family}",
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
        "primary_family": f"V2候选补量:{family}",
        "primary_subtype": primary_subtype,
        "secondary_subtypes": [card_id for card_id in candidate_labels if card_id and card_id != primary_subtype],
        "universal_profile": dict(cached_item.get("neutral_signal_profile") or {}),
        "family_scores": {},
        "capability_scores": {},
        "parallel_families": [],
        "structure_features": dict(cached_item.get("article_profile") or {}),
        "family_profiles": {},
        "subtype_candidates": [],
        "secondary_candidates": [],
        "candidate_labels": [card_id for card_id in candidate_labels if card_id],
        "primary_label": cached_item.get("material_card_id"),
        "decision_trace": {
            "source": "candidate_backfill_multi_family",
            "family": family,
            "selected_business_card": primary_subtype,
            "selected_material_card": qrc.get("selected_material_card"),
        },
        "primary_route": {
            "business_family_id": family,
            "selected_business_card": primary_subtype,
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
                "v2_candidate_backfill_multi_family",
                family,
                primary_subtype,
                str(cached_item.get("material_card_id") or ""),
            ]
        )),
        "fit_scores": fit_scores,
        "feature_profile": dict(cached_item.get("business_feature_profile") or {}),
        "quality_score": float(cached_item.get("quality_score") or 0.0),
        "v2_index_version": VERSION,
        "v2_business_family_ids": [family],
        "v2_index_payload": {family: cached_item},
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
        baseline_counts = count_family_cards()
        deficits = {
            family: {
                card_id: max(0, target - baseline_counts.get(family, Counter()).get(card_id, 0))
                for card_id, target in card_targets.items()
            }
            for family, card_targets in TARGETS.items()
        }
        print("[multi] baseline_counts", {family: dict(counter) for family, counter in baseline_counts.items()})
        print("[multi] deficits", deficits)
        if all(all(value <= 0 for value in card_map.values()) for card_map in deficits.values()):
            return 0

        existing_candidate_card_pairs: set[tuple[str, str, str]] = set()
        existing_hashes_by_family_card: dict[tuple[str, str], set[str]] = {}
        for candidate_span_id, material_family_id, v2_index_payload, payload, text_hash in session.query(
            MaterialSpanORM.candidate_span_id,
            MaterialSpanORM.material_family_id,
            MaterialSpanORM.v2_index_payload,
            MaterialSpanORM.v2_business_family_ids,
            MaterialSpanORM.normalized_text_hash,
        ).all():
            families = payload or []
            for family in TARGETS:
                cached_item = dict((v2_index_payload or {}).get(family) or {})
                selected_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                if family in families or cached_item:
                    if candidate_span_id and selected_card:
                        existing_candidate_card_pairs.add((candidate_span_id, family, selected_card))
                    if text_hash and selected_card:
                        existing_hashes_by_family_card.setdefault((family, selected_card), set()).add(text_hash)
            if material_family_id and material_family_id.startswith("v2_candidate_backfill."):
                family = material_family_id.split(".", 1)[1]
                cached_item = dict((v2_index_payload or {}).get(family) or {})
                selected_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                if candidate_span_id and selected_card:
                    existing_candidate_card_pairs.add((candidate_span_id, family, selected_card))
                if text_hash and selected_card:
                    existing_hashes_by_family_card.setdefault((family, selected_card), set()).add(text_hash)

        pipeline = MaterialPipelineV2()
        article_cache: dict[str, ArticleORM | None] = {}
        candidates = session.query(CandidateSpanORM).order_by(CandidateSpanORM.created_at.desc()).all()
        inserted_counter: dict[str, Counter] = {family: Counter() for family in TARGETS}
        inserted_samples: list[dict] = []

        for candidate in candidates:
            if all(all(value <= 0 for value in family_deficits.values()) for family_deficits in deficits.values()):
                break
            text = str(candidate.text or "").strip()
            if len(text) < 60:
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
            text_hash = build_content_hash(text)
            for family, family_targets in TARGETS.items():
                if all(value <= 0 for value in deficits[family].values()):
                    continue
                cached_item = pipeline.build_cached_item_from_material(
                    material=temp_material,
                    article=article,
                    business_family_id=family,
                )
                if not cached_item:
                    continue
                selected_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                candidate_cards = list(
                    dict.fromkeys(
                        [
                            selected_card,
                            *(cached_item.get("business_card_recommendations") or []),
                        ]
                    )
                )
                eligible_cards = [
                    card_id
                    for card_id in candidate_cards
                    if card_id and deficits[family].get(card_id, 0) > 0
                ]
                if not eligible_cards:
                    continue
                eligible_cards = sorted(
                    eligible_cards,
                    key=lambda card_id: (deficits[family].get(card_id, 0), 1 if card_id == selected_card else 0),
                    reverse=True,
                )
                for target_card in eligible_cards:
                    if (candidate.id, family, target_card) in existing_candidate_card_pairs:
                        continue
                    if text_hash in existing_hashes_by_family_card.get((family, target_card), set()):
                        continue
                    material = MaterialSpanORM(
                        **material_payload(
                            family=family,
                            article=article,
                            candidate=candidate,
                            cached_item=cached_item,
                            selected_business_card=target_card,
                        )
                    )
                    session.add(material)
                    existing_candidate_card_pairs.add((candidate.id, family, target_card))
                    existing_hashes_by_family_card.setdefault((family, target_card), set()).add(text_hash)
                    deficits[family][target_card] -= 1
                    inserted_counter[family][target_card] += 1
                    if len(inserted_samples) < 200:
                        inserted_samples.append(
                            {
                                "family": family,
                                "material_id": material.id,
                                "candidate_span_id": candidate.id,
                                "article_id": article.id,
                                "selected_business_card": target_card,
                                "title": article.title,
                                "source": article.source,
                            }
                        )
                    total_inserted = sum(sum(counter.values()) for counter in inserted_counter.values())
                    if total_inserted and total_inserted % 50 == 0:
                        session.commit()
                        print("[multi] inserted", total_inserted, "deficits", deficits)

        session.commit()
        final_counts = count_family_cards()
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "target": TARGETS,
            "baseline_materials": material_count() - sum(sum(counter.values()) for counter in inserted_counter.values()),
            "final_materials": material_count(),
            "baseline_counts": {family: dict(counter) for family, counter in baseline_counts.items()},
            "inserted_counter": {family: dict(counter) for family, counter in inserted_counter.items()},
            "final_counts": {family: dict(counter) for family, counter in final_counts.items()},
            "all_reached": all(
                final_counts.get(family, Counter()).get(card_id, 0) >= target
                for family, card_targets in TARGETS.items()
                for card_id, target in card_targets.items()
            ),
            "inserted_samples": inserted_samples,
        }
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"business_card_multi_family_backfill_{timestamp}.json"
        md_path = REPORTS_ROOT / f"business_card_multi_family_backfill_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(f"[multi] json_report={json_path}")
        print(f"[multi] md_report={md_path}")
        print(f"[multi] final_counts={report['final_counts']}")
        return 0 if report["all_reached"] else 1
    finally:
        session.close()


def build_markdown(report: dict) -> str:
    lines = [
        "# 多题型业务卡补池报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 最终材料数：`{report['final_materials']}`",
        f"- 是否全部达标：`{report['all_reached']}`",
        "",
    ]
    for family, counter in report.get("final_counts", {}).items():
        lines.append(f"## {family}")
        lines.append("")
        for card_id, count in counter.items():
            lines.append(f"- `{card_id}`: `{count}`")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
