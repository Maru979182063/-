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
VERSION = "v2.effective.backfill.20260403a"

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

SOURCE_PLAN = [
    "kepuchina",
    "gmw_tech",
    "guokr",
    "lifeweek",
    "banyuetan",
    "people",
    "xinhuanet",
    "qstheory",
    "gov",
    "yicai",
    "mof",
    "pbc",
    "stats_gov",
    "gmw",
]

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.ingest_service import run_crawl_for_source  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.utils import new_id  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.ingest.dedupe.content_hash import build_content_hash  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


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


def normalize_quality(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _recommendation_score(cached_item: dict, card_id: str) -> float:
    for hit in (cached_item.get("eligible_business_cards") or []):
        if str(hit.get("business_card_id") or "") == card_id:
            return normalize_quality(hit.get("score"))
    return 0.0


def _generic_ok(cached_item: dict, *, family: str) -> tuple[bool, list[str]]:
    neutral = cached_item.get("neutral_signal_profile") or {}
    business = cached_item.get("business_feature_profile") or {}
    quality = normalize_quality(cached_item.get("quality_score"))
    semantic = normalize_quality(business.get("semantic_completeness_score") or neutral.get("semantic_completeness_score"))
    readability = normalize_quality(business.get("readability") or neutral.get("standalone_readability"))
    reasons: list[str] = []
    if family == "title_selection" and quality < 0.50:
        reasons.append("quality_score")
    if semantic < 0.66:
        reasons.append("semantic_completeness")
    if readability < 0.60:
        reasons.append("readability")
    return (not reasons, reasons)


def is_effective_cached_item(*, family: str, card_id: str, cached_item: dict, exact_selected: bool) -> tuple[bool, list[str]]:
    qrc = cached_item.get("question_ready_context") or {}
    selected_card = str(qrc.get("selected_business_card") or "")
    if exact_selected and selected_card != card_id:
        return False, ["selected_business_card"]
    recommendation_score = _recommendation_score(cached_item, card_id)
    minimum_business_score = 0.50 if family == "sentence_order" else 0.24 if family == "title_selection" else 0.18
    if recommendation_score < minimum_business_score:
        return False, ["business_card_score"]

    generic_ok, generic_reasons = _generic_ok(cached_item, family=family)
    if not generic_ok:
        return False, generic_reasons

    neutral = cached_item.get("neutral_signal_profile") or {}
    business = cached_item.get("business_feature_profile") or {}
    order_profile = business.get("sentence_order_profile") or {}
    order = {**neutral, **order_profile}
    fill = business.get("sentence_fill_profile") or {}
    reasons: list[str] = []

    if family == "title_selection":
        if card_id == "turning_relation_focus__main_idea":
            if normalize_quality(neutral.get("turning_focus_strength")) < 0.62:
                reasons.append("turning_focus_strength")
        elif card_id == "cause_effect__conclusion_focus__main_idea":
            if normalize_quality(neutral.get("cause_effect_strength")) < 0.62:
                reasons.append("cause_effect_strength")
        elif card_id == "necessary_condition_countermeasure__main_idea":
            if normalize_quality(neutral.get("necessary_condition_strength")) < 0.60:
                reasons.append("necessary_condition_strength")
            if normalize_quality(neutral.get("countermeasure_signal_strength")) < 0.26:
                reasons.append("countermeasure_signal_strength")
        elif card_id == "parallel_comprehensive_summary__main_idea":
            if normalize_quality(neutral.get("parallel_enumeration_strength")) < 0.60:
                reasons.append("parallel_enumeration_strength")
            if normalize_quality(business.get("non_key_detail_density")) > 0.64:
                reasons.append("non_key_detail_density")
        elif card_id == "theme_word_focus__main_idea":
            if normalize_quality(neutral.get("topic_consistency_strength")) < 0.55:
                reasons.append("topic_consistency_strength")
            strongest_relation = max(
                normalize_quality(neutral.get("turning_focus_strength")),
                normalize_quality(neutral.get("cause_effect_strength")),
                normalize_quality(neutral.get("necessary_condition_strength")),
                normalize_quality(neutral.get("parallel_enumeration_strength")),
            )
            if strongest_relation > 0.82:
                reasons.append("relation_too_strong_for_theme")

    elif family == "sentence_fill":
        mapping = {
            "sentence_fill__opening_summary__abstract": ("opening", "summarize_following_text"),
            "sentence_fill__opening_topic_intro__abstract": ("opening", "topic_introduction"),
            "sentence_fill__middle_carry_previous__abstract": ("middle", "carry_previous"),
            "sentence_fill__middle_lead_next__abstract": ("middle", "lead_next"),
            "sentence_fill__middle_bridge_both_sides__abstract": ("middle", "bridge_both_sides"),
            "sentence_fill__ending_summary__abstract": ("ending", "summarize_previous_text"),
            "sentence_fill__ending_countermeasure__abstract": ("ending", "propose_countermeasure"),
        }
        expected = mapping.get(card_id)
        if expected:
            expected_position, expected_function = expected
            if str(fill.get("blank_position") or "") != expected_position:
                reasons.append("blank_position")
            if str(fill.get("function_type") or "") != expected_function:
                reasons.append("function_type")
        if card_id == "sentence_fill__middle_carry_previous__abstract" and normalize_quality(fill.get("backward_link_strength")) < 0.68:
            reasons.append("backward_link_strength")
        if card_id == "sentence_fill__middle_lead_next__abstract" and normalize_quality(fill.get("forward_link_strength")) < 0.68:
            reasons.append("forward_link_strength")
        if card_id == "sentence_fill__middle_bridge_both_sides__abstract":
            if normalize_quality(fill.get("bidirectional_validation")) < 0.60:
                reasons.append("bidirectional_validation")
            if min(normalize_quality(fill.get("backward_link_strength")), normalize_quality(fill.get("forward_link_strength"))) < 0.52:
                reasons.append("bridge_side_balance")
        if card_id == "sentence_fill__ending_countermeasure__abstract" and normalize_quality(fill.get("countermeasure_signal_strength")) < 0.58:
            reasons.append("countermeasure_signal_strength")

    elif family == "sentence_order":
        if normalize_quality(order.get("unique_opener_score")) < 0.40:
            reasons.append("unique_opener_score")
        if normalize_quality(order.get("binding_pair_count")) < 2.0:
            reasons.append("binding_pair_count")
        if normalize_quality(order.get("exchange_risk")) > 0.48:
            reasons.append("exchange_risk")
        if normalize_quality(order.get("function_overlap_score")) > 0.48:
            reasons.append("function_overlap_score")
        if normalize_quality(order.get("multi_path_risk")) > 0.50:
            reasons.append("multi_path_risk")
        if normalize_quality(order.get("discourse_progression_strength")) < 0.42:
            reasons.append("discourse_progression_strength")
        if normalize_quality(order.get("context_closure_score")) < 0.44:
            reasons.append("context_closure_score")
        if card_id == "sentence_order__head_tail_lock__abstract":
            if str(order.get("opening_rule") or "") not in {"explicit_opening", "definition_opening", "question_opening"}:
                reasons.append("opening_rule")
            if str(order.get("closing_rule") or "") == "none":
                reasons.append("closing_rule")
        elif card_id == "sentence_order__deterministic_binding__abstract":
            if not (order.get("binding_rules") or []):
                reasons.append("binding_rules")
            if normalize_quality(order.get("binding_pair_count")) < 2.0:
                reasons.append("binding_pair_count_strong")
        elif card_id == "sentence_order__discourse_logic__abstract":
            if "discourse_logic" not in (order.get("logic_modes") or []):
                reasons.append("logic_modes")
            if normalize_quality(order.get("discourse_progression_strength")) < 0.46:
                reasons.append("discourse_progression_strength_strong")
        elif card_id == "sentence_order__timeline_action_sequence__abstract":
            if not set(order.get("logic_modes") or []).intersection({"timeline_sequence", "action_sequence"}):
                reasons.append("timeline_or_action_mode")
            if max(
                normalize_quality(order.get("temporal_order_strength")),
                normalize_quality(order.get("action_sequence_irreversibility")),
            ) < 0.16:
                reasons.append("temporal_or_action_strength")
        elif card_id == "sentence_order__head_tail_logic__abstract":
            if str(order.get("opening_rule") or "") not in {"explicit_opening", "definition_opening", "question_opening"}:
                reasons.append("opening_rule")
            if normalize_quality(order.get("binding_pair_count")) < 2.0:
                reasons.append("binding_pair_count_strong")

    return (not reasons, reasons)


def count_articles() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from articles").fetchone()[0])
    finally:
        conn.close()


def count_materials() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        return int(conn.execute("select count(*) from material_spans").fetchone()[0])
    finally:
        conn.close()


def compact(counter_map: dict[str, Counter]) -> dict[str, dict[str, int]]:
    return {family: {card_id: int(counter.get(card_id, 0)) for card_id in targets} for family, (counter, targets) in ((family, (counter_map.get(family, Counter()), TARGETS[family])) for family in TARGETS)}


def material_payload(*, family: str, article, candidate, cached_item: dict, selected_business_card: str) -> dict:
    payload = copy.deepcopy(cached_item)
    qrc = dict(payload.get("question_ready_context") or {})
    qrc["selected_business_card"] = selected_business_card
    payload["question_ready_context"] = qrc
    payload.setdefault("source", {})
    payload["source"]["effective_backfill"] = True
    payload["source"]["effective_backfill_version"] = VERSION
    return {
        "id": new_id("mat"),
        "article_id": article.id,
        "candidate_span_id": candidate.id,
        "text": candidate.text,
        "normalized_text_hash": build_content_hash(candidate.text),
        "material_family_id": f"v2_effective_backfill.{family}",
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
        "primary_family": f"V2有效补量:{family}",
        "primary_subtype": selected_business_card,
        "secondary_subtypes": [card_id for card_id in (payload.get("business_card_recommendations") or []) if card_id and card_id != selected_business_card],
        "universal_profile": dict(payload.get("neutral_signal_profile") or {}),
        "family_scores": {},
        "capability_scores": {},
        "parallel_families": [],
        "structure_features": dict(payload.get("article_profile") or {}),
        "family_profiles": {},
        "subtype_candidates": [],
        "secondary_candidates": [],
        "candidate_labels": list(dict.fromkeys([selected_business_card, *(payload.get("business_card_recommendations") or [])])),
        "primary_label": payload.get("material_card_id"),
        "decision_trace": {
            "source": "effective_candidate_backfill",
            "family": family,
            "selected_business_card": selected_business_card,
            "selected_material_card": qrc.get("selected_material_card"),
        },
        "primary_route": {
            "business_family_id": family,
            "selected_business_card": selected_business_card,
        },
        "reject_reason": None,
        "variants": [],
        "source": dict(payload.get("source") or {}),
        "source_tail": str((payload.get("source") or {}).get("source_url") or ""),
        "integrity": {
            "semantic_completeness_score": normalize_quality((payload.get("business_feature_profile") or {}).get("semantic_completeness_score")),
            "standalone_readability": normalize_quality((payload.get("business_feature_profile") or {}).get("readability")),
        },
        "quality_flags": list(payload.get("quality_flags") or []),
        "knowledge_tags": list(dict.fromkeys(["v2_effective_backfill", family, selected_business_card, str(payload.get("material_card_id") or "")])),
        "fit_scores": {
            item.get("card_id"): item.get("score")
            for item in (payload.get("eligible_material_cards") or [])
            if item.get("card_id")
        },
        "feature_profile": dict(payload.get("business_feature_profile") or {}),
        "quality_score": normalize_quality(payload.get("quality_score")),
        "v2_index_version": VERSION,
        "v2_business_family_ids": [family],
        "v2_index_payload": {family: payload},
        "usage_count": 0,
        "accept_count": 0,
        "reject_count": 0,
        "last_used_at": None,
    }


def count_effective_existing(session) -> tuple[dict[str, Counter], set[tuple[str, str, str]], dict[tuple[str, str], set[str]]]:
    counters: dict[str, Counter] = {family: Counter() for family in TARGETS}
    existing_pairs: set[tuple[str, str, str]] = set()
    hashes_by_family_card: dict[tuple[str, str], set[str]] = {}
    rows = session.query(
        MaterialSpanORM.candidate_span_id,
        MaterialSpanORM.normalized_text_hash,
        MaterialSpanORM.v2_index_payload,
        MaterialSpanORM.v2_business_family_ids,
    ).all()
    for candidate_span_id, text_hash, payload, families in rows:
        payload = payload or {}
        families = families or []
        for family, targets in TARGETS.items():
            cached_item = dict(payload.get(family) or {})
            if not cached_item and family not in families:
                continue
            selected_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
            if not selected_card:
                continue
            ok, _ = is_effective_cached_item(
                family=family,
                card_id=selected_card,
                cached_item=cached_item,
                exact_selected=True,
            )
            if ok and selected_card in targets:
                counters[family][selected_card] += 1
                if candidate_span_id:
                    existing_pairs.add((candidate_span_id, family, selected_card))
                if text_hash:
                    hashes_by_family_card.setdefault((family, selected_card), set()).add(text_hash)
    return counters, existing_pairs, hashes_by_family_card


def deficits_from(counters: dict[str, Counter]) -> dict[str, dict[str, int]]:
    return {
        family: {card_id: max(0, target - counters.get(family, Counter()).get(card_id, 0)) for card_id, target in targets.items()}
        for family, targets in TARGETS.items()
    }


def backfill_from_candidates(session, deficits, existing_pairs, hashes_by_family_card) -> tuple[dict[str, Counter], list[dict]]:
    pipeline = MaterialPipelineV2()
    inserted_counter: dict[str, Counter] = {family: Counter() for family in TARGETS}
    inserted_samples: list[dict] = []
    article_cache: dict[str, ArticleORM | None] = {}
    candidates = session.query(CandidateSpanORM).order_by(CandidateSpanORM.created_at.desc()).all()

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
            candidate_cards = list(dict.fromkeys([
                str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or ""),
                *(cached_item.get("business_card_recommendations") or []),
            ]))
            for card_id in candidate_cards:
                if not card_id or deficits[family].get(card_id, 0) <= 0:
                    continue
                if (candidate.id, family, card_id) in existing_pairs:
                    continue
                if text_hash in hashes_by_family_card.get((family, card_id), set()):
                    continue
                ok, _ = is_effective_cached_item(
                    family=family,
                    card_id=card_id,
                    cached_item=cached_item,
                    exact_selected=False,
                )
                if not ok:
                    continue
                material = MaterialSpanORM(
                    **material_payload(
                        family=family,
                        article=article,
                        candidate=candidate,
                        cached_item=cached_item,
                        selected_business_card=card_id,
                    )
                )
                session.add(material)
                existing_pairs.add((candidate.id, family, card_id))
                hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                deficits[family][card_id] -= 1
                inserted_counter[family][card_id] += 1
                if len(inserted_samples) < 120:
                    inserted_samples.append(
                        {
                            "family": family,
                            "card_id": card_id,
                            "material_id": material.id,
                            "article_title": article.title,
                            "source_id": getattr(article, "source", None) or getattr(article, "source_url", None) or "",
                            "text_preview": text[:160],
                            "quality_score": normalize_quality(cached_item.get("quality_score")),
                        }
                    )
                break
    session.commit()
    return inserted_counter, inserted_samples


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    crawl_results: list[dict] = []
    try:
        baseline_articles = count_articles()
        baseline_materials = count_materials()
        baseline_counts, existing_pairs, hashes_by_family_card = count_effective_existing(session)
        deficits = deficits_from(baseline_counts)

        inserted_counter_total: dict[str, Counter] = {family: Counter() for family in TARGETS}
        inserted_samples_total: list[dict] = []

        inserted_counter, inserted_samples = backfill_from_candidates(session, deficits, existing_pairs, hashes_by_family_card)
        for family, counter in inserted_counter.items():
            inserted_counter_total[family].update(counter)
        inserted_samples_total.extend(inserted_samples)

        for source_id in SOURCE_PLAN:
            if all(all(value <= 0 for value in family_deficits.values()) for family_deficits in deficits.values()):
                break
            crawl_result = run_crawl_for_source(session, source_id)
            crawl_results.append(crawl_result)
            inserted_counter, inserted_samples = backfill_from_candidates(session, deficits, existing_pairs, hashes_by_family_card)
            for family, counter in inserted_counter.items():
                inserted_counter_total[family].update(counter)
            inserted_samples_total.extend(inserted_samples)

        final_counts, _, _ = count_effective_existing(session)
        final_deficits = deficits_from(final_counts)
        all_reached = all(all(value <= 0 for value in family_deficits.values()) for family_deficits in final_deficits.values())

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "service_db": str(DB_PATH),
            "version": VERSION,
            "baseline_articles": baseline_articles,
            "baseline_materials": baseline_materials,
            "baseline_effective_counts": compact(baseline_counts),
            "final_articles": count_articles(),
            "final_materials": count_materials(),
            "final_effective_counts": compact(final_counts),
            "final_deficits": final_deficits,
            "all_cards_reached": all_reached,
            "inserted_effective_counts": compact(inserted_counter_total),
            "crawl_results": crawl_results,
            "inserted_samples": inserted_samples_total[:120],
        }

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"effective_business_card_backfill_{timestamp}.json"
        md_path = REPORTS_ROOT / f"effective_business_card_backfill_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(json.dumps({"json": str(json_path), "markdown": str(md_path), "all_cards_reached": all_reached}, ensure_ascii=False))
        return 0 if all_reached else 1
    finally:
        session.close()


def build_markdown(report: dict) -> str:
    lines = [
        "# 有效业务卡材料补量报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 版本：`{report['version']}`",
        f"- 初始文章数：`{report['baseline_articles']}`",
        f"- 初始材料数：`{report['baseline_materials']}`",
        f"- 最终文章数：`{report['final_articles']}`",
        f"- 最终材料数：`{report['final_materials']}`",
        f"- 是否全部达到“每卡100条有效材料”：`{report['all_cards_reached']}`",
        "",
        "## 初始有效库存",
        "",
    ]
    for family, cards in report.get("baseline_effective_counts", {}).items():
        lines.append(f"### {family}")
        for card_id, count in cards.items():
            lines.append(f"- `{card_id}`: `{count}`")
        lines.append("")
    lines.extend(["## 最终有效库存", ""])
    for family, cards in report.get("final_effective_counts", {}).items():
        lines.append(f"### {family}")
        for card_id, count in cards.items():
            deficit = (((report.get("final_deficits") or {}).get(family) or {}).get(card_id) or 0)
            lines.append(f"- `{card_id}`: `{count}`（缺口 `{deficit}`）")
        lines.append("")
    lines.extend(["## 抓取过程", ""])
    for item in report.get("crawl_results", []):
        lines.append(
            f"- `{item.get('source_id')}`: 新增 `{item.get('ingested_count')}` 篇，处理 `{item.get('processed_count')}` 篇，失败 `{item.get('failed_count')}` 篇"
        )
    lines.extend(["", "## 样本", ""])
    for sample in report.get("inserted_samples", [])[:40]:
        lines.append(
            f"- `{sample['family']}` / `{sample['card_id']}` / `{sample['source_id']}` / `{sample['quality_score']}`: {sample['article_title']}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
