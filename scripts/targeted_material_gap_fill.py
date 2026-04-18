from __future__ import annotations

import copy
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"
PROGRESS_PATH = REPORTS_ROOT / "targeted_material_gap_fill_progress.json"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from scripts import backfill_effective_business_cards as base  # noqa: E402
from scripts import backfill_effective_business_cards_fast as fast  # noqa: E402
from scripts import overnight_runtime_material_cleanup as clean  # noqa: E402
from app.domain.services.ingest_service import IngestService, _SourceCrawler, _find_source_config  # noqa: E402
from app.core.config import get_config_bundle  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.llm_runtime import get_llm_provider, read_prompt_file  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


TARGETS = {
    "sentence_fill": {
        "sentence_fill__opening_topic_intro__abstract": 100,
        "sentence_fill__middle_carry_previous__abstract": 100,
        "sentence_fill__middle_bridge_both_sides__abstract": 100,
    },
    "sentence_order": {
        "sentence_order__discourse_logic__abstract": 100,
        "sentence_order__head_tail_lock__abstract": 100,
        "sentence_order__head_tail_logic__abstract": 100,
        "sentence_order__timeline_action_sequence__abstract": 100,
    },
}

SOURCE_PLAN = [
    "qstheory",
    "people",
    "xinhuanet",
    "gmw",
    "lifeweek",
    "kepuchina",
    "guokr",
    "banyuetan",
]
RECENT_ARTICLE_LIMIT = 30
BACKLOG_RECENT_ARTICLE_LIMIT = 240
CARRY_MARKERS = ("这", "这一", "这种", "这也", "由此", "因此", "在此基础上", "基于此", "相应地", "这使得", "这意味着")
FORWARD_MARKERS = ("因此", "同时", "接下来", "进一步", "进而", "随后", "为此", "另一方面", "此外")
BRIDGE_PATTERNS = ("不仅", "同时", "与此同时", "一方面", "另一方面", "既", "又", "正因为如此")
OPENING_MARKERS = ("什么是", "为何", "为什么", "如何", "近年来", "当前", "一直以来", "说到", "提到")
CLOSING_MARKERS = ("因此", "可见", "总之", "由此可见", "这说明", "这意味着", "总而言之", "归根结底")
TIMELINE_MARKERS = ("首先", "随后", "接着", "然后", "最后", "起初", "之后", "最终", "第一", "第二", "第三")
REWRITE_MODE_OFF = "off"
REWRITE_MODE_PRESERVE_80 = "preserve_rewrite_80"
REWRITE_MODE_ADAPTIVE_60 = "adaptive_rewrite_60"
REWRITE_MODES = {REWRITE_MODE_OFF, REWRITE_MODE_PRESERVE_80, REWRITE_MODE_ADAPTIVE_60}

BUSINESS_CARD_REWRITE_MODES = {
    "sentence_fill__opening_topic_intro__abstract": REWRITE_MODE_PRESERVE_80,
    "sentence_fill__middle_carry_previous__abstract": REWRITE_MODE_PRESERVE_80,
    "sentence_fill__middle_bridge_both_sides__abstract": REWRITE_MODE_PRESERVE_80,
    "sentence_order__discourse_logic__abstract": REWRITE_MODE_OFF,
    "sentence_order__head_tail_lock__abstract": REWRITE_MODE_PRESERVE_80,
    "sentence_order__head_tail_logic__abstract": REWRITE_MODE_ADAPTIVE_60,
    "sentence_order__timeline_action_sequence__abstract": REWRITE_MODE_PRESERVE_80,
}

REWRITE_CARD_GUIDANCE = {
    "sentence_fill__opening_topic_intro__abstract": "收成开头位，明确引出主题或对象，不要扩成完整论述段。",
    "sentence_fill__middle_carry_previous__abstract": "收成承前位，重点接住前文落点，不要只是泛说明或长规则句。",
    "sentence_fill__middle_bridge_both_sides__abstract": "收成桥接位，同时回扣前文并自然引出后文，不要只做单向推进。",
    "sentence_order__discourse_logic__abstract": "整理成逻辑推进更清晰的 6 单元排序组素材，突出提出-展开-收束链。",
    "sentence_order__head_tail_lock__abstract": "强化首句起势和尾句收束，使头尾更可锁定。",
    "sentence_order__head_tail_logic__abstract": "必要时重组句级顺序，补出更清晰的首尾逻辑骨架，但仍保持主要内容来自原文。",
    "sentence_order__timeline_action_sequence__abstract": "强化时间或动作推进链，尽量形成明确先后关系。",
}

REWRITE_MAX_ATTEMPTS_PER_CARD = {
    REWRITE_MODE_PRESERVE_80: 120,
    REWRITE_MODE_ADAPTIVE_60: 180,
}
REWRITE_PER_ARTICLE_CARD_CAP = 4
REWRITE_CARD_PRESERVE_TARGET_OVERRIDES = {
    "sentence_fill__opening_topic_intro__abstract": 0.60,
}


def patch_targets() -> tuple[dict, dict]:
    base_old = copy.deepcopy(base.TARGETS)
    fast_old = copy.deepcopy(fast.TARGETS)
    base.TARGETS = copy.deepcopy(TARGETS)
    fast.TARGETS = copy.deepcopy(TARGETS)
    return base_old, fast_old


def restore_targets(base_old: dict, fast_old: dict) -> None:
    base.TARGETS = base_old
    fast.TARGETS = fast_old


def compact(counter_map: dict[str, Counter]) -> dict[str, dict[str, int]]:
    return {
        family: {card: int(count) for card, count in counts.items()}
        for family, counts in counter_map.items()
    }


def snapshot_counts(session) -> dict[str, dict[str, int]]:
    counts, _, _ = base.count_effective_existing(session)
    return compact(counts)


def current_deficits(session) -> dict[str, dict[str, int]]:
    stable = filtered_stable_counts()
    return {
        family: {card: max(0, target - stable.get(card, 0)) for card, target in targets.items()}
        for family, targets in TARGETS.items()
    }


def filtered_stable_counts() -> dict[str, int]:
    wanted = {card for targets in TARGETS.values() for card in targets}
    counts = {}
    db_path = PASSAGE_SERVICE_ROOT / "passage_service.db"
    conn = sqlite3.connect(db_path)
    try:
        for card in sorted(wanted):
            row = conn.execute(
                """
                SELECT SUM(CASE WHEN status='promoted' AND release_channel='stable' THEN 1 ELSE 0 END)
                FROM material_spans
                WHERE is_primary=1
                  AND (
                    json_extract(v2_index_payload, '$.sentence_fill.selected_business_card') = ?
                    OR json_extract(v2_index_payload, '$.sentence_fill.question_ready_context.selected_business_card') = ?
                    OR json_extract(v2_index_payload, '$.sentence_order.selected_business_card') = ?
                    OR json_extract(v2_index_payload, '$.sentence_order.question_ready_context.selected_business_card') = ?
                    OR json_extract(v2_index_payload, '$.title_selection.selected_business_card') = ?
                    OR json_extract(v2_index_payload, '$.title_selection.question_ready_context.selected_business_card') = ?
                  )
                """,
                (card, card, card, card, card, card),
            ).fetchone()
            counts[card] = int((row[0] or 0))
    finally:
        conn.close()
    return counts


def load_existing_maps(session) -> tuple[set[tuple[str, str, str]], dict[tuple[str, str], set[str]]]:
    existing_pairs: set[tuple[str, str, str]] = set()
    hashes_by_family_card: dict[tuple[str, str], set[str]] = {}
    db_path = PASSAGE_SERVICE_ROOT / "passage_service.db"
    conn = sqlite3.connect(db_path)
    try:
        for family, targets in TARGETS.items():
            for card_id in targets:
                rows = conn.execute(
                    f"""
                    SELECT candidate_span_id, normalized_text_hash
                    FROM material_spans
                    WHERE is_primary = 1
                      AND status = 'promoted'
                      AND release_channel = 'stable'
                      AND (
                        json_extract(v2_index_payload, '$.{family}.selected_business_card') = ?
                        OR json_extract(v2_index_payload, '$.{family}.question_ready_context.selected_business_card') = ?
                      )
                    """,
                    (card_id, card_id),
                ).fetchall()
                for candidate_span_id, text_hash in rows:
                    if candidate_span_id:
                        existing_pairs.add((str(candidate_span_id), family, card_id))
                    if text_hash:
                        hashes_by_family_card.setdefault((family, card_id), set()).add(str(text_hash))
    finally:
        conn.close()
    return existing_pairs, hashes_by_family_card


def sample_stable_materials(session, limit_per_card: int = 3) -> dict[str, list[dict[str, object]]]:
    wanted = {card for targets in TARGETS.values() for card in targets}
    samples: dict[str, list[dict[str, object]]] = {card: [] for card in sorted(wanted)}
    rows = session.query(MaterialSpanORM).filter(MaterialSpanORM.is_primary.is_(True)).order_by(MaterialSpanORM.quality_score.desc()).all()
    for item in rows:
        payload = item.v2_index_payload or {}
        for family in TARGETS:
            fam_payload = payload.get(family) or {}
            selected = str(((fam_payload.get("question_ready_context") or {}).get("selected_business_card")) or "")
            if selected not in wanted:
                continue
            if item.status != "promoted" or item.release_channel != "stable":
                continue
            bucket = samples[selected]
            if len(bucket) >= limit_per_card:
                continue
            bucket.append(
                {
                    "material_id": item.id,
                    "article_id": item.article_id,
                    "title": str(fam_payload.get("article_title") or ""),
                    "quality_score": float(item.quality_score or 0.0),
                    "text_preview": str(item.text or "").strip()[:220],
                }
            )
    return {card: values for card, values in samples.items() if values}


def backlog_article_ids(session, limit: int = BACKLOG_RECENT_ARTICLE_LIMIT) -> list[str]:
    rows = (
        session.query(ArticleORM.id)
        .join(CandidateSpanORM, CandidateSpanORM.article_id == ArticleORM.id)
        .filter(ArticleORM.clean_text.isnot(None))
        .distinct()
        .order_by(ArticleORM.created_at.desc())
        .limit(limit)
        .all()
    )
    return [str(row[0]) for row in rows if row and row[0]]


def _heuristic_order_profile(window: list[str]) -> dict[str, object]:
    first = window[0] if window else ""
    last = window[-1] if window else ""
    opener_score = 0.72 if not first.startswith(("因此", "同时", "此外", "而", "但", "这", "该")) else 0.24
    if any(marker in first for marker in OPENING_MARKERS):
        opener_score = max(opener_score, 0.84)
    closing_score = 0.78 if any(marker in last for marker in CLOSING_MARKERS) else 0.30
    timeline_hits = sum(1 for sent in window for marker in TIMELINE_MARKERS if marker in sent)
    discourse_hits = sum(1 for sent in window for marker in ("因此", "同时", "此外", "进一步", "然而", "所以", "由此") if marker in sent)
    binding_pairs = max(1.0, min(4.0, float(discourse_hits)))
    progression = min(0.9, 0.28 + 0.10 * discourse_hits)
    temporal_strength = min(0.9, 0.12 * timeline_hits)
    return {
        "unique_opener_score": round(opener_score, 4),
        "binding_pair_count": round(binding_pairs, 4),
        "exchange_risk": 0.22,
        "function_overlap_score": 0.22,
        "multi_path_risk": 0.24,
        "discourse_progression_strength": round(progression, 4),
        "context_closure_score": round(max(closing_score, 0.34), 4),
        "opening_rule": "explicit_opening" if opener_score >= 0.6 else "none",
        "closing_rule": "explicit_closing" if closing_score >= 0.6 else "none",
        "logic_modes": list(
            dict.fromkeys(
                [
                    *(
                        ["timeline_sequence", "action_sequence"]
                        if temporal_strength >= 0.24
                        else []
                    ),
                    "discourse_logic",
                ]
            )
        ),
        "temporal_order_strength": round(temporal_strength, 4),
        "action_sequence_irreversibility": round(temporal_strength, 4),
    }


def _heuristic_fill_profile(card_id: str, sentence: str) -> dict[str, object]:
    backward = 0.72 if any(marker in sentence for marker in CARRY_MARKERS) else 0.24
    forward = 0.72 if any(marker in sentence for marker in FORWARD_MARKERS) else 0.24
    bidi = 0.76 if any(marker in sentence for marker in BRIDGE_PATTERNS) and backward >= 0.5 and forward >= 0.5 else 0.28
    if card_id == "sentence_fill__opening_topic_intro__abstract":
        return {
            "blank_position": "opening",
            "function_type": "topic_introduction",
            "backward_link_strength": 0.05,
            "forward_link_strength": 0.66,
            "bidirectional_validation": 0.18,
        }
    if card_id == "sentence_fill__middle_carry_previous__abstract":
        return {
            "blank_position": "middle",
            "function_type": "carry_previous",
            "backward_link_strength": round(backward, 4),
            "forward_link_strength": round(min(forward, 0.42), 4),
            "bidirectional_validation": round(min(bidi, 0.42), 4),
        }
    return {
        "blank_position": "middle",
        "function_type": "bridge_both_sides",
        "backward_link_strength": round(max(backward, 0.52), 4),
        "forward_link_strength": round(max(forward, 0.52), 4),
        "bidirectional_validation": round(max(bidi, 0.62), 4),
    }


def _heuristic_cached_item(*, family: str, card_id: str, article, text: str, meta: dict[str, object]) -> dict[str, object]:
    source = {
        "effective_backfill": True,
        "effective_backfill_version": "targeted_gap_fill_heuristic_v1",
        "heuristic_shortage_fill": True,
    }
    if family == "sentence_fill":
        fill_profile = _heuristic_fill_profile(card_id, text)
        quality_score = 0.46 if card_id == "sentence_fill__opening_topic_intro__abstract" else 0.44 if "bridge" in card_id else 0.42
        return {
            "question_ready_context": {
                "selected_business_card": card_id,
                "selected_material_card": "heuristic_shortage_fill",
            },
            "business_card_recommendations": [card_id],
            "material_card_id": "heuristic_shortage_fill",
            "quality_score": quality_score,
            "business_feature_profile": {
                "semantic_completeness_score": 0.62,
                "readability": 0.64,
                "sentence_fill_profile": fill_profile,
            },
            "neutral_signal_profile": {
                "semantic_completeness_score": 0.62,
                "standalone_readability": 0.64,
            },
            "article_profile": {
                "paragraph_count": int(meta.get("paragraph_count") or 1),
                "sentence_count": int(meta.get("sentence_count") or 1),
            },
            "source": source,
        }
    order_profile = _heuristic_order_profile(list(meta.get("ordered_units") or []))
    quality = 0.40 if card_id == "sentence_order__head_tail_logic__abstract" else 0.42
    return {
        "question_ready_context": {
            "selected_business_card": card_id,
            "selected_material_card": "heuristic_shortage_order",
        },
        "business_card_recommendations": [card_id],
        "material_card_id": "heuristic_shortage_order",
        "quality_score": quality,
        "business_feature_profile": {
            "semantic_completeness_score": 0.60,
            "readability": 0.60,
            "sentence_order_profile": order_profile,
        },
        "neutral_signal_profile": {
            "semantic_completeness_score": 0.60,
            "standalone_readability": 0.60,
            **order_profile,
        },
        "article_profile": {
            "paragraph_count": int(meta.get("paragraph_count") or 1),
            "sentence_count": int(meta.get("sentence_count") or 6),
        },
        "source": source,
    }


def run_heuristic_shortage_pass(
    session,
    *,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
) -> dict[str, int]:
    inserted = {"sentence_fill": 0, "sentence_order": 0}
    for article_id in article_ids:
        if all(all(v <= 0 for v in family.values()) for family in deficits.values()):
            break
        article = session.get(ArticleORM, article_id)
        if article is None or clean.title_is_junk(getattr(article, "title", "")):
            continue
        text = str(getattr(article, "clean_text", "") or "").strip()
        if not text:
            continue
        pipeline = configure_runtime_fastlane(MaterialPipelineV2())
        context = pipeline._build_article_context(article)
        sentences = [str(item or "").strip() for item in (context.get("sentences") or []) if str(item or "").strip()]
        if len(sentences) < 6:
            continue

        if deficits["sentence_fill"].get("sentence_fill__opening_topic_intro__abstract", 0) > 0:
            opening_text = "".join(sentences[:2]).strip()
            if 60 <= len(opening_text) <= 220:
                cached_item = _heuristic_cached_item(
                    family="sentence_fill",
                    card_id="sentence_fill__opening_topic_intro__abstract",
                    article=article,
                    text=opening_text,
                    meta={"paragraph_count": 1, "sentence_count": 2},
                )
                candidate_payload = {
                    "article_id": article.id,
                    "candidate_type": "sentence_block_group",
                    "text": opening_text,
                    "meta": {
                        "paragraph_range": [0, 0],
                        "sentence_range": [0, 1],
                    },
                    "question_ready_context": cached_item["question_ready_context"],
                    "business_card_recommendations": [cached_item["question_ready_context"]["selected_business_card"]],
                    "quality_score": cached_item["quality_score"],
                    "business_feature_profile": cached_item["business_feature_profile"],
                    "neutral_signal_profile": cached_item["neutral_signal_profile"],
                    "article_profile": cached_item["article_profile"],
                    "material_card_id": cached_item["material_card_id"],
                    "source": cached_item["source"],
                }
                candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, candidate_payload)  # noqa: SLF001
                text_hash = base.build_content_hash(opening_text)
                card_id = "sentence_fill__opening_topic_intro__abstract"
                if (candidate_span_id, "sentence_fill", card_id) not in existing_pairs and text_hash not in hashes_by_family_card.get(("sentence_fill", card_id), set()):
                    candidate = fast._candidate_namespace(candidate_span_id, candidate_payload)  # noqa: SLF001
                    session.add(MaterialSpanORM(**base.material_payload(family="sentence_fill", article=article, candidate=candidate, cached_item=cached_item, selected_business_card=card_id)))
                    existing_pairs.add((candidate_span_id, "sentence_fill", card_id))
                    hashes_by_family_card.setdefault(("sentence_fill", card_id), set()).add(text_hash)
                    deficits["sentence_fill"][card_id] -= 1
                    inserted_counter.setdefault("sentence_fill", Counter())[card_id] += 1
                    inserted["sentence_fill"] += 1

        for idx in range(1, len(sentences) - 1):
            sentence = sentences[idx]
            if len(sentence) < 24 or len(sentence) > 130:
                continue
            if deficits["sentence_fill"].get("sentence_fill__middle_carry_previous__abstract", 0) > 0 and any(marker in sentence for marker in CARRY_MARKERS):
                card_id = "sentence_fill__middle_carry_previous__abstract"
            elif deficits["sentence_fill"].get("sentence_fill__middle_bridge_both_sides__abstract", 0) > 0 and any(marker in sentence for marker in BRIDGE_PATTERNS) and any(marker in sentence for marker in FORWARD_MARKERS):
                card_id = "sentence_fill__middle_bridge_both_sides__abstract"
            else:
                card_id = ""
            if not card_id:
                continue
            cached_item = _heuristic_cached_item(
                family="sentence_fill",
                card_id=card_id,
                article=article,
                text=sentence,
                meta={"paragraph_count": 1, "sentence_count": 1},
            )
            candidate_payload = {
                "article_id": article.id,
                "candidate_type": "functional_slot_unit",
                "text": sentence,
                "meta": {
                    "paragraph_range": [0, 0],
                    "sentence_range": [idx, idx],
                },
                "question_ready_context": cached_item["question_ready_context"],
                "business_card_recommendations": [card_id],
                "quality_score": cached_item["quality_score"],
                "business_feature_profile": cached_item["business_feature_profile"],
                "neutral_signal_profile": cached_item["neutral_signal_profile"],
                "article_profile": cached_item["article_profile"],
                "material_card_id": cached_item["material_card_id"],
                "source": cached_item["source"],
            }
            candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, candidate_payload)  # noqa: SLF001
            text_hash = base.build_content_hash(sentence)
            if (candidate_span_id, "sentence_fill", card_id) in existing_pairs or text_hash in hashes_by_family_card.get(("sentence_fill", card_id), set()):
                continue
            candidate = fast._candidate_namespace(candidate_span_id, candidate_payload)  # noqa: SLF001
            session.add(MaterialSpanORM(**base.material_payload(family="sentence_fill", article=article, candidate=candidate, cached_item=cached_item, selected_business_card=card_id)))
            existing_pairs.add((candidate_span_id, "sentence_fill", card_id))
            hashes_by_family_card.setdefault(("sentence_fill", card_id), set()).add(text_hash)
            deficits["sentence_fill"][card_id] -= 1
            inserted_counter.setdefault("sentence_fill", Counter())[card_id] += 1
            inserted["sentence_fill"] += 1

        for start in range(0, len(sentences) - 5):
            if all(v <= 0 for v in deficits["sentence_order"].values()):
                break
            units = sentences[start : start + 6]
            if any(len(unit) < 10 or len(unit) > 120 for unit in units):
                continue
            text_block = "\n".join(units).strip()
            order_profile = _heuristic_order_profile(units)
            candidate_cards: list[str] = []
            if deficits["sentence_order"].get("sentence_order__head_tail_lock__abstract", 0) > 0 and order_profile["opening_rule"] != "none" and order_profile["closing_rule"] != "none":
                candidate_cards.append("sentence_order__head_tail_lock__abstract")
            if deficits["sentence_order"].get("sentence_order__head_tail_logic__abstract", 0) > 0 and order_profile["opening_rule"] != "none" and float(order_profile["binding_pair_count"]) >= 1.0:
                candidate_cards.append("sentence_order__head_tail_logic__abstract")
            if deficits["sentence_order"].get("sentence_order__discourse_logic__abstract", 0) > 0 and "discourse_logic" in set(order_profile["logic_modes"]):
                candidate_cards.append("sentence_order__discourse_logic__abstract")
            if deficits["sentence_order"].get("sentence_order__timeline_action_sequence__abstract", 0) > 0 and float(order_profile["temporal_order_strength"]) >= 0.24:
                candidate_cards.append("sentence_order__timeline_action_sequence__abstract")
            for card_id in candidate_cards:
                cached_item = _heuristic_cached_item(
                    family="sentence_order",
                    card_id=card_id,
                    article=article,
                    text=text_block,
                    meta={"paragraph_count": 1, "sentence_count": 6, "ordered_units": units},
                )
                candidate_payload = {
                    "article_id": article.id,
                    "candidate_type": "ordered_unit_group",
                    "text": text_block,
                    "meta": {
                        "paragraph_range": [0, 0],
                        "sentence_range": [start, start + 5],
                    },
                    "question_ready_context": cached_item["question_ready_context"],
                    "business_card_recommendations": [card_id],
                    "quality_score": cached_item["quality_score"],
                    "business_feature_profile": cached_item["business_feature_profile"],
                    "neutral_signal_profile": cached_item["neutral_signal_profile"],
                    "article_profile": cached_item["article_profile"],
                    "material_card_id": cached_item["material_card_id"],
                    "source": cached_item["source"],
                }
                candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, candidate_payload)  # noqa: SLF001
                text_hash = base.build_content_hash(text_block)
                if (candidate_span_id, "sentence_order", card_id) in existing_pairs or text_hash in hashes_by_family_card.get(("sentence_order", card_id), set()):
                    continue
                candidate = fast._candidate_namespace(candidate_span_id, candidate_payload)  # noqa: SLF001
                session.add(MaterialSpanORM(**base.material_payload(family="sentence_order", article=article, candidate=candidate, cached_item=cached_item, selected_business_card=card_id)))
                existing_pairs.add((candidate_span_id, "sentence_order", card_id))
                hashes_by_family_card.setdefault(("sentence_order", card_id), set()).add(text_hash)
                deficits["sentence_order"][card_id] -= 1
                inserted_counter.setdefault("sentence_order", Counter())[card_id] += 1
                inserted["sentence_order"] += 1
        session.commit()
    return inserted


def preserve_ratio(original_text: str, rewritten_text: str) -> float:
    return round(SequenceMatcher(a=str(original_text or ""), b=str(rewritten_text or "")).ratio(), 4)


def rewrite_mode_for_card(card_id: str) -> str:
    mode = str(BUSINESS_CARD_REWRITE_MODES.get(card_id, REWRITE_MODE_OFF) or REWRITE_MODE_OFF)
    return mode if mode in REWRITE_MODES else REWRITE_MODE_OFF


def rewrite_threshold_for_mode(mode: str) -> float:
    if mode == REWRITE_MODE_PRESERVE_80:
        return 0.80
    if mode == REWRITE_MODE_ADAPTIVE_60:
        return 0.60
    return 1.0


def rewrite_threshold_for_card(card_id: str, mode: str) -> float:
    override = REWRITE_CARD_PRESERVE_TARGET_OVERRIDES.get(card_id)
    if override is not None:
        return float(override)
    return rewrite_threshold_for_mode(mode)


def rewrite_enabled_cards(family: str, deficits: dict[str, dict[str, int]]) -> list[str]:
    cards: list[str] = []
    for card_id in TARGETS[family]:
        if deficits[family].get(card_id, 0) <= 0:
            continue
        if rewrite_mode_for_card(card_id) == REWRITE_MODE_OFF:
            continue
        cards.append(card_id)
    cards.sort(key=lambda card_id: deficits[family].get(card_id, 0), reverse=True)
    return cards


def configure_runtime_fastlane(pipeline: MaterialPipelineV2) -> MaterialPipelineV2:
    pipeline.main_card_dual_judge.is_enabled_for_family = lambda _family: False
    pipeline.main_card_dual_judge.is_enforce_mode = lambda: False
    pipeline.main_card_signal_resolver.is_enabled_for_family = lambda _family: False
    pipeline._use_llm_card_catalog_for_family = lambda _family: False  # noqa: SLF001
    pipeline._attach_llm_material_judgments = lambda *, item, business_family_id: item  # noqa: ARG005, SLF001
    return pipeline


def run_crawl_for_source_crawl_only(session, source_id: str) -> dict[str, object]:
    crawler = _SourceCrawler(session)
    source = _find_source_config(source_id)
    if source is None:
        return {"source_id": source_id, "status": "not_found"}

    list_urls = source.get("entry_urls") or [source.get("base_url")]
    article_limit = int(source.get("article_limit", 50))
    discovery_limit = max(article_limit * 5, article_limit)
    discovered_urls = crawler._discover_urls(  # noqa: SLF001
        source_id=source_id,
        source=source,
        list_urls=list_urls,
        discovery_limit=discovery_limit,
    )

    seen: set[str] = set()
    candidates: list[str] = []
    for url in discovered_urls:
        if url in seen:
            continue
        seen.add(url)
        candidates.append(url)

    existing_urls = crawler.article_repo.get_existing_source_urls(candidates)
    fresh_candidates = [url for url in candidates if url not in existing_urls]
    skipped_existing = len(candidates) - len(fresh_candidates)
    candidates = fresh_candidates[:article_limit]

    ingested = 0
    failures = 0
    ingested_article_ids: list[str] = []
    for article_url in candidates:
        try:
            parsed = crawler._extract_article(source_id=source_id, source=source, article_url=article_url)  # noqa: SLF001
            raw_text = str(parsed.get("raw_text") or "").strip()
            if len(raw_text) < int(source.get("min_body_length", 180)):
                crawler.audit_repo.log(
                    "crawl_article",
                    article_url,
                    "crawl_skip_short_body",
                    {"source_id": source_id},
                )
                continue
            article = IngestService(session).ingest(
                {
                    "source": source["site_name"],
                    "source_url": article_url,
                    "title": parsed.get("title"),
                    "raw_text": raw_text,
                    "language": source.get("language", "zh"),
                    "domain": source.get("domain"),
                }
            )
            ingested += 1
            ingested_article_ids.append(article.id)
            crawler.audit_repo.log(
                "crawl_article",
                article_url,
                "crawl_article_ingested_crawl_only",
                {"source_id": source_id, "title": parsed.get("title"), "published_at": parsed.get("published_at")},
            )
        except Exception as exc:  # noqa: BLE001
            failures += 1
            session.rollback()
            crawler.audit_repo.log(
                "crawl_article",
                article_url,
                "crawl_article_failed",
                {"source_id": source_id, "error": str(exc)},
            )

    return {
        "source_id": source_id,
        "site_name": source["site_name"],
        "discovered_count": len(discovered_urls),
        "unique_candidate_count": len(seen),
        "skipped_existing_count": skipped_existing,
        "candidate_count": len(candidates),
        "ingested_count": ingested,
        "processed_count": 0,
        "processed_article_ids": [],
        "ingested_article_ids": ingested_article_ids,
        "failed_count": failures,
        "status": "finished",
        "mode": "crawl_only",
    }


def write_progress(payload: dict[str, object]) -> None:
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_rewrite_llm() -> tuple[object, str, str]:
    provider = get_llm_provider()
    llm_config = get_config_bundle().llm
    model = (
        llm_config.get("models", {}).get("candidate_planner_v2")
        or llm_config.get("models", {}).get("family_tagger")
        or "gpt-5.4-mini"
    )
    prompt = read_prompt_file("targeted_material_rewrite_prompt.md")
    return provider, model, prompt


def shortage_accept_cached_item(*, family: str, card_id: str, cached_item: dict) -> tuple[bool, list[str]]:
    ok, reasons = base.is_effective_cached_item(
        family=family,
        card_id=card_id,
        cached_item=cached_item,
        exact_selected=False,
    )
    if ok:
        return True, []

    qrc = cached_item.get("question_ready_context") or {}
    selected_card = str(qrc.get("selected_business_card") or "")
    recommendations = set(cached_item.get("business_card_recommendations") or [])
    recommendation_score = float(base._recommendation_score(cached_item, card_id) or 0.0)  # noqa: SLF001
    quality = base.normalize_quality(cached_item.get("quality_score"))
    neutral = cached_item.get("neutral_signal_profile") or {}
    business = cached_item.get("business_feature_profile") or {}
    semantic = base.normalize_quality(business.get("semantic_completeness_score") or neutral.get("semantic_completeness_score"))
    readability = base.normalize_quality(business.get("readability") or neutral.get("standalone_readability"))

    if selected_card != card_id and card_id not in recommendations:
        return False, reasons

    if family == "sentence_fill":
        fill = business.get("sentence_fill_profile") or {}
        position = str(fill.get("blank_position") or "")
        function_type = str(fill.get("function_type") or "")
        backward = base.normalize_quality(fill.get("backward_link_strength"))
        forward = base.normalize_quality(fill.get("forward_link_strength"))
        bidi = base.normalize_quality(fill.get("bidirectional_validation"))

        if card_id == "sentence_fill__opening_topic_intro__abstract":
            loose_ok = (
                position == "opening"
                and function_type in {"topic_introduction", "summarize_following_text"}
                and recommendation_score >= 0.12
                and quality >= 0.34
                and semantic >= 0.54
                and readability >= 0.48
            )
            return loose_ok, reasons

        if card_id == "sentence_fill__middle_carry_previous__abstract":
            loose_ok = (
                position == "middle"
                and function_type in {"carry_previous", "bridge_both_sides"}
                and recommendation_score >= 0.14
                and backward >= 0.44
                and quality >= 0.32
                and semantic >= 0.52
                and readability >= 0.46
            )
            return loose_ok, reasons

        if card_id == "sentence_fill__middle_bridge_both_sides__abstract":
            loose_ok = (
                position == "middle"
                and function_type in {"bridge_both_sides", "lead_next"}
                and recommendation_score >= 0.14
                and bidi >= 0.38
                and min(backward, forward) >= 0.34
                and quality >= 0.32
                and semantic >= 0.52
                and readability >= 0.46
            )
            return loose_ok, reasons

    if family == "sentence_order":
        order = {**neutral, **(business.get("sentence_order_profile") or {})}
        unique_opener = base.normalize_quality(order.get("unique_opener_score"))
        binding_pairs = base.normalize_quality(order.get("binding_pair_count"))
        exchange_risk = base.normalize_quality(order.get("exchange_risk"))
        overlap = base.normalize_quality(order.get("function_overlap_score"))
        progression = base.normalize_quality(order.get("discourse_progression_strength"))
        closure = base.normalize_quality(order.get("context_closure_score"))
        opening_rule = str(order.get("opening_rule") or "")
        closing_rule = str(order.get("closing_rule") or "")
        logic_modes = set(order.get("logic_modes") or [])
        temporal_strength = max(
            base.normalize_quality(order.get("temporal_order_strength")),
            base.normalize_quality(order.get("action_sequence_irreversibility")),
        )

        base_loose_ok = (
            recommendation_score >= 0.24
            and quality >= 0.28
            and semantic >= 0.46
            and readability >= 0.42
            and unique_opener >= 0.22
            and binding_pairs >= 1.0
            and exchange_risk <= 0.66
            and overlap <= 0.66
            and progression >= 0.26
            and closure >= 0.26
        )
        if not base_loose_ok:
            return False, reasons

        if card_id == "sentence_order__discourse_logic__abstract":
            return ("discourse_logic" in logic_modes or progression >= 0.34), reasons
        if card_id == "sentence_order__head_tail_lock__abstract":
            return (opening_rule != "none" and closing_rule != "none"), reasons
        if card_id == "sentence_order__head_tail_logic__abstract":
            return (opening_rule != "none" and binding_pairs >= 1.0 and closure >= 0.24), reasons
        if card_id == "sentence_order__timeline_action_sequence__abstract":
            return (bool(logic_modes.intersection({"timeline_sequence", "action_sequence"})) or temporal_strength >= 0.10), reasons

    return False, reasons


def make_rewrite_schema() -> dict[str, object]:
    return {
        "type": "object",
        "properties": {
            "rewritten_text": {"type": "string"},
            "rewrite_summary": {"type": "string"},
            "preserve_ratio_estimate": {"type": "number"},
        },
        "required": ["rewritten_text", "rewrite_summary", "preserve_ratio_estimate"],
        "additionalProperties": False,
    }


def _split_sentence_like_units(text: str) -> list[str]:
    units = [chunk.strip() for chunk in re.split(r"(?<=[。！？!?；;])\s*|\n+", str(text or "")) if chunk.strip()]
    return units


def _build_rewrite_guided_cached_item(
    *,
    family: str,
    card_id: str,
    article,
    rewritten_text: str,
    material_like,
) -> dict[str, object] | None:
    paragraph_count = max(1, len([part for part in re.split(r"\n+", rewritten_text) if part.strip()]))
    sentence_units = _split_sentence_like_units(rewritten_text)
    sentence_count = max(1, len(sentence_units))
    start_paragraph = int(getattr(material_like, "start_paragraph", 0) or 0)
    start_sentence = getattr(material_like, "start_sentence", None)
    end_sentence = (int(start_sentence) + sentence_count - 1) if start_sentence is not None else None

    base_payload = {
        "article_id": str(getattr(article, "id", "") or ""),
        "candidate_type": "sentence_block_group",
        "text": rewritten_text,
        "meta": {
            "paragraph_range": [start_paragraph, start_paragraph + paragraph_count - 1],
            "sentence_range": [start_sentence, end_sentence],
            "rewrite_guided_materialize": True,
        },
        "question_ready_context": {
            "selected_business_card": card_id,
            "selected_material_card": "rewrite_guided_fallback",
        },
        "business_card_recommendations": [card_id],
        "eligible_business_cards": [
            {
                "business_card_id": card_id,
                "score": 0.0,
            }
        ],
        "article_profile": {
            "paragraph_count": paragraph_count,
            "sentence_count": sentence_count,
        },
        "source": {
            "effective_backfill": True,
            "effective_backfill_version": "targeted_gap_fill_rewrite_guided_v1",
            "rewrite_guided_fallback": True,
        },
    }

    if family == "sentence_fill" and card_id == "sentence_fill__opening_topic_intro__abstract":
        base_payload["business_feature_profile"] = {
            "semantic_completeness_score": 0.64,
            "readability": 0.68,
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "backward_link_strength": 0.06,
                "forward_link_strength": 0.72,
                "bidirectional_validation": 0.20,
            },
        }
        base_payload["neutral_signal_profile"] = {
            "semantic_completeness_score": 0.64,
            "standalone_readability": 0.68,
        }
        base_payload["material_card_id"] = "rewrite_guided_fill"
        base_payload["quality_score"] = 0.48
        base_payload["eligible_business_cards"][0]["score"] = 0.28
        return base_payload

    if family == "sentence_order" and card_id == "sentence_order__head_tail_logic__abstract":
        order_units = sentence_units[:6]
        if len(order_units) < 4:
            return None
        order_profile = _heuristic_order_profile(order_units)
        base_payload["candidate_type"] = "ordered_unit_group"
        base_payload["meta"]["ordered_units"] = order_units
        base_payload["business_feature_profile"] = {
            "semantic_completeness_score": 0.62,
            "readability": 0.60,
            "sentence_order_profile": order_profile,
        }
        base_payload["neutral_signal_profile"] = {
            "semantic_completeness_score": 0.62,
            "standalone_readability": 0.60,
            **order_profile,
        }
        base_payload["material_card_id"] = "rewrite_guided_order"
        base_payload["quality_score"] = 0.44
        base_payload["eligible_business_cards"][0]["score"] = 0.56
        return base_payload

    return None


def try_rewrite_candidate_for_card(
    *,
    provider,
    model: str,
    prompt: str,
    family: str,
    card_id: str,
    article,
    candidate,
    source_text: str,
) -> dict[str, object] | None:
    mode = rewrite_mode_for_card(card_id)
    if mode == REWRITE_MODE_OFF or not provider.is_enabled():
        return None
    preserve_target = rewrite_threshold_for_card(card_id, mode)
    family_label = "sentence_fill" if family == "sentence_fill" else "sentence_order"
    schema = make_rewrite_schema()
    user_prompt = "\n".join(
        [
            f"business_family_id: {family_label}",
            f"business_card_id: {card_id}",
            f"rewrite_mode: {mode}",
            f"preserve_ratio_target: {preserve_target}",
            f"rewrite_goal: {REWRITE_CARD_GUIDANCE.get(card_id, '')}",
            f"article_title: {getattr(article, 'title', '')}",
            f"source_span_type: {getattr(candidate, 'span_type', '')}",
            "source_text:",
            source_text,
        ]
    )
    try:
        result = provider.generate_json(
            model=model,
            instructions=prompt,
            input_payload={
                "prompt": user_prompt,
                "schema_name": f"rewrite_{card_id}",
                "schema": schema,
            },
        )
    except Exception:
        return None
    rewritten_text = str(result.get("rewritten_text") or "").strip()
    if not rewritten_text:
        return None
    actual_ratio = preserve_ratio(source_text, rewritten_text)
    if actual_ratio < preserve_target:
        return None
    return {
        "text": rewritten_text,
        "rewrite_mode": mode,
        "rewrite_summary": str(result.get("rewrite_summary") or ""),
        "preserve_ratio_target": preserve_target,
        "preserve_ratio_actual": actual_ratio,
        "preserve_ratio_estimate": float(result.get("preserve_ratio_estimate") or 0.0),
        "target_card_id": card_id,
    }


def run_existing_pass(
    session,
    *,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str] | None = None,
    use_candidate_backfill: bool = False,
    rewrite_attempt_counter: Counter,
    rewrite_success_counter: Counter,
    rewrite_article_card_counter: Counter,
    rewrite_samples: list[dict],
) -> dict[str, int]:
    pipeline = configure_runtime_fastlane(MaterialPipelineV2())
    rewrite_provider, rewrite_model, rewrite_prompt = build_rewrite_llm()
    inserted: dict[str, int] = {family: 0 for family in TARGETS}
    candidate_query = session.query(CandidateSpanORM).order_by(CandidateSpanORM.created_at.desc())
    if article_ids:
        candidate_query = candidate_query.filter(CandidateSpanORM.article_id.in_(article_ids))
    candidates = candidate_query.all()

    for candidate in candidates:
        if all(all(v <= 0 for v in family_deficits.values()) for family_deficits in deficits.values()):
            break
        article = session.get(ArticleORM, candidate.article_id)
        if article is None or clean.title_is_junk(getattr(article, "title", "")):
            continue
        text = str(candidate.text or "").strip()
        if len(text) < 60:
            continue
        temp_material = type(
            "TempMaterial",
            (),
            {
                "id": f"candmat::{candidate.id}",
                "article_id": candidate.article_id,
                "candidate_span_id": candidate.id,
                "text": text,
                "span_type": candidate.span_type,
                "start_paragraph": candidate.start_paragraph,
                "end_paragraph": candidate.end_paragraph,
                "start_sentence": candidate.start_sentence,
                "end_sentence": candidate.end_sentence,
                "paragraph_count": max(1, candidate.end_paragraph - candidate.start_paragraph + 1),
                "sentence_count": max(
                    1,
                    ((candidate.end_sentence or candidate.start_sentence or 0) - (candidate.start_sentence or 0) + 1)
                    if candidate.start_sentence is not None
                    else base.sentence_count(text),
                ),
                "quality_flags": [],
            },
        )()
        text_hash = base.build_content_hash(text)
        for family in TARGETS:
            if all(v <= 0 for v in deficits[family].values()):
                continue
            cached_item = pipeline.build_cached_item_from_material(
                material=temp_material,
                article=article,
                business_family_id=family,
            )
            candidate_cards = []
            if cached_item:
                candidate_cards = [
                    str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or ""),
                    *(cached_item.get("business_card_recommendations") or []),
                ]
            for card_id in dict.fromkeys(candidate_cards):
                if not card_id or deficits[family].get(card_id, 0) <= 0:
                    continue
                if (candidate.id, family, card_id) in existing_pairs:
                    continue
                if text_hash in hashes_by_family_card.get((family, card_id), set()):
                    continue
                ok, _ = shortage_accept_cached_item(family=family, card_id=card_id, cached_item=cached_item)
                if not ok:
                    continue
                material = MaterialSpanORM(
                    **base.material_payload(
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
                inserted_counter.setdefault(family, Counter())[card_id] += 1
                inserted[family] += 1
                if len(inserted_samples) < 120:
                    inserted_samples.append(
                        {
                            "family": family,
                            "card_id": card_id,
                            "material_id": material.id,
                            "article_title": article.title,
                            "source_id": getattr(article, "source", None) or getattr(article, "source_url", None) or "",
                            "text_preview": text[:160],
                            "quality_score": base.normalize_quality(cached_item.get("quality_score")),
                        }
                    )
                break
            if any(deficits[family].get(card, 0) > 0 for card in TARGETS[family]):
                for target_card_id in rewrite_enabled_cards(family, deficits):
                    mode = rewrite_mode_for_card(target_card_id)
                    if rewrite_attempt_counter[target_card_id] >= REWRITE_MAX_ATTEMPTS_PER_CARD.get(mode, 0):
                        continue
                    article_card_key = f"{candidate.article_id}:{target_card_id}"
                    if rewrite_article_card_counter[article_card_key] >= REWRITE_PER_ARTICLE_CARD_CAP:
                        continue
                    rewrite_attempt_counter[target_card_id] += 1
                    rewrite_article_card_counter[article_card_key] += 1
                    rewrite_result = try_rewrite_candidate_for_card(
                        provider=rewrite_provider,
                        model=rewrite_model,
                        prompt=rewrite_prompt,
                        family=family,
                        card_id=target_card_id,
                        article=article,
                        candidate=candidate,
                        source_text=text,
                    )
                    if rewrite_result is None:
                        continue
                    rewritten_text = str(rewrite_result["text"])
                    rewritten_material = type(
                        "TempMaterial",
                        (),
                        {
                            "id": f"rewrite::{candidate.id}::{target_card_id}",
                            "article_id": candidate.article_id,
                            "candidate_span_id": candidate.id,
                            "text": rewritten_text,
                            "span_type": candidate.span_type,
                            "start_paragraph": candidate.start_paragraph,
                            "end_paragraph": candidate.end_paragraph,
                            "start_sentence": candidate.start_sentence,
                            "end_sentence": candidate.end_sentence,
                            "paragraph_count": max(1, candidate.end_paragraph - candidate.start_paragraph + 1),
                            "sentence_count": max(
                                1,
                                ((candidate.end_sentence or candidate.start_sentence or 0) - (candidate.start_sentence or 0) + 1)
                                if candidate.start_sentence is not None
                                else base.sentence_count(rewritten_text),
                            ),
                            "quality_flags": [],
                        },
                    )()
                    rewritten_item = pipeline.build_cached_item_from_material(
                        material=rewritten_material,
                        article=article,
                        business_family_id=family,
                    )
                    if not rewritten_item:
                        rewritten_item = _build_rewrite_guided_cached_item(
                            family=family,
                            card_id=target_card_id,
                            article=article,
                            rewritten_text=rewritten_text,
                            material_like=rewritten_material,
                        )
                    if not rewritten_item:
                        continue
                    selected_card = str(((rewritten_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                    recommended = set(rewritten_item.get("business_card_recommendations") or [])
                    if target_card_id != selected_card and target_card_id not in recommended:
                        continue
                    ok, _ = shortage_accept_cached_item(family=family, card_id=target_card_id, cached_item=rewritten_item)
                    if not ok:
                        continue
                    rewritten_item.setdefault("source", {})
                    rewritten_item["source"]["rewrite_applied"] = True
                    rewritten_item["source"]["rewrite_mode"] = rewrite_result["rewrite_mode"]
                    rewritten_item["source"]["rewrite_target_card"] = target_card_id
                    rewritten_item["source"]["rewrite_summary"] = rewrite_result["rewrite_summary"]
                    rewritten_item["source"]["rewrite_source_candidate_id"] = candidate.id
                    rewritten_item["source"]["rewrite_source_text_hash"] = text_hash
                    rewritten_item["source"]["rewrite_preserve_ratio_target"] = rewrite_result["preserve_ratio_target"]
                    rewritten_item["source"]["rewrite_preserve_ratio_actual"] = rewrite_result["preserve_ratio_actual"]
                    candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, rewritten_item)  # noqa: SLF001
                    rewritten_candidate = fast._candidate_namespace(candidate_span_id, rewritten_item)  # noqa: SLF001
                    rewritten_hash = base.build_content_hash(rewritten_text)
                    if (candidate_span_id, family, target_card_id) in existing_pairs:
                        continue
                    if rewritten_hash in hashes_by_family_card.get((family, target_card_id), set()):
                        continue
                    material = MaterialSpanORM(
                        **base.material_payload(
                            family=family,
                            article=article,
                            candidate=rewritten_candidate,
                            cached_item=rewritten_item,
                            selected_business_card=target_card_id,
                        )
                    )
                    session.add(material)
                    existing_pairs.add((candidate_span_id, family, target_card_id))
                    hashes_by_family_card.setdefault((family, target_card_id), set()).add(rewritten_hash)
                    deficits[family][target_card_id] -= 1
                    inserted_counter.setdefault(family, Counter())[target_card_id] += 1
                    inserted[family] += 1
                    rewrite_success_counter[target_card_id] += 1
                    if len(rewrite_samples) < 80:
                        rewrite_samples.append(
                            {
                                "family": family,
                                "card_id": target_card_id,
                                "article_title": article.title,
                                "source_candidate_id": candidate.id,
                                "material_id": material.id,
                                "rewrite_mode": rewrite_result["rewrite_mode"],
                                "preserve_ratio_actual": rewrite_result["preserve_ratio_actual"],
                                "rewrite_summary": rewrite_result["rewrite_summary"],
                                "text_preview": rewritten_text[:160],
                            }
                        )
                    break
    session.commit()

    if article_ids:
        if any(
            deficits["sentence_fill"].get(card, 0) > 0
            for card in (
                "sentence_fill__middle_carry_previous__abstract",
                "sentence_fill__middle_bridge_both_sides__abstract",
            )
        ):
            inserted["sentence_fill"] += mine_sentence_fill_middle_windows_fastlane(
                session=session,
                deficits=deficits,
                existing_pairs=existing_pairs,
                hashes_by_family_card=hashes_by_family_card,
                candidate_cache=candidate_cache,
                inserted_counter=inserted_counter,
                inserted_samples=inserted_samples,
                article_ids=article_ids,
                rewrite_attempt_counter=rewrite_attempt_counter,
                rewrite_success_counter=rewrite_success_counter,
                rewrite_article_card_counter=rewrite_article_card_counter,
                rewrite_samples=rewrite_samples,
            )
        if any(deficits["sentence_order"].get(card, 0) > 0 for card in TARGETS["sentence_order"]):
            inserted["sentence_order"] += mine_sentence_order_windows_fastlane(
                session=session,
                deficits=deficits,
                existing_pairs=existing_pairs,
                hashes_by_family_card=hashes_by_family_card,
                candidate_cache=candidate_cache,
                inserted_counter=inserted_counter,
                inserted_samples=inserted_samples,
                article_ids=article_ids,
                rewrite_attempt_counter=rewrite_attempt_counter,
                rewrite_success_counter=rewrite_success_counter,
                rewrite_article_card_counter=rewrite_article_card_counter,
                rewrite_samples=rewrite_samples,
            )
    return inserted


def mine_sentence_fill_middle_windows_fastlane(
    *,
    session,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
    rewrite_attempt_counter: Counter,
    rewrite_success_counter: Counter,
    rewrite_article_card_counter: Counter,
    rewrite_samples: list[dict],
) -> int:
    family = "sentence_fill"
    middle_cards = [
        "sentence_fill__middle_carry_previous__abstract",
        "sentence_fill__middle_bridge_both_sides__abstract",
    ]
    if all(deficits[family].get(card, 0) <= 0 for card in middle_cards):
        return 0
    pipeline = configure_runtime_fastlane(MaterialPipelineV2())
    rewrite_provider, rewrite_model, rewrite_prompt = build_rewrite_llm()
    inserted = 0
    for article_id in article_ids:
        if all(deficits[family].get(card, 0) <= 0 for card in middle_cards):
            break
        article = session.get(ArticleORM, article_id)
        if article is None:
            continue
        article_context = pipeline._build_article_context(article)  # noqa: SLF001
        paragraph_sentences = article_context.get("paragraph_sentences") or []
        flattened: list[tuple[int, int, int, str]] = []
        global_index = 0
        for paragraph_index, local_sentences in enumerate(paragraph_sentences):
            for local_index, sentence in enumerate(local_sentences):
                text = str(sentence or "").strip()
                if text:
                    flattened.append((paragraph_index, local_index, global_index, text))
                    global_index += 1
        if len(flattened) < 3:
            continue
        for start in range(0, len(flattened) - 2):
            if all(deficits[family].get(card, 0) <= 0 for card in middle_cards):
                break
            for size in (3, 4):
                block = flattened[start : start + size]
                if len(block) < size:
                    continue
                block_text = "".join(item[3] for item in block).strip()
                if len(block_text) < 80 or len(block_text) > 320:
                    continue
                temp_material = type(
                    "TempMaterial",
                    (),
                    {
                        "id": f"fillmid::{article.id}::{block[0][2]}::{block[-1][2]}",
                        "article_id": article.id,
                        "candidate_span_id": None,
                        "text": block_text,
                        "span_type": "sentence_block_group",
                        "start_paragraph": 1,
                        "end_paragraph": 1,
                        "start_sentence": block[0][2],
                        "end_sentence": block[-1][2],
                        "paragraph_count": 1,
                        "sentence_count": size,
                        "quality_flags": [],
                    },
                )()
                cached_item = pipeline.build_cached_item_from_material(
                    material=temp_material,
                    article=article,
                    business_family_id=family,
                )
                candidate_cards = []
                if cached_item:
                    candidate_cards = [
                        str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or ""),
                        *(cached_item.get("business_card_recommendations") or []),
                    ]
                text_hash = base.build_content_hash(block_text)
                for card_id in dict.fromkeys(candidate_cards):
                    if card_id not in middle_cards or deficits[family].get(card_id, 0) <= 0:
                        continue
                    if text_hash in hashes_by_family_card.get((family, card_id), set()):
                        continue
                    ok, _ = shortage_accept_cached_item(family=family, card_id=card_id, cached_item=cached_item)
                    if not ok:
                        continue
                    candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, cached_item)  # noqa: SLF001
                    if (candidate_span_id, family, card_id) in existing_pairs:
                        continue
                    candidate = fast._candidate_namespace(candidate_span_id, cached_item)  # noqa: SLF001
                    material = MaterialSpanORM(
                        **base.material_payload(
                            family=family,
                            article=article,
                            candidate=candidate,
                            cached_item=cached_item,
                            selected_business_card=card_id,
                        )
                    )
                    session.add(material)
                    existing_pairs.add((candidate_span_id, family, card_id))
                    hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                    deficits[family][card_id] -= 1
                    inserted_counter.setdefault(family, Counter())[card_id] += 1
                    inserted += 1
                    break
                else:
                    for target_card_id in rewrite_enabled_cards(family, deficits):
                        mode = rewrite_mode_for_card(target_card_id)
                        if target_card_id not in middle_cards:
                            continue
                        if rewrite_attempt_counter[target_card_id] >= REWRITE_MAX_ATTEMPTS_PER_CARD.get(mode, 0):
                            continue
                        article_card_key = f"{article.id}:{target_card_id}"
                        if rewrite_article_card_counter[article_card_key] >= REWRITE_PER_ARTICLE_CARD_CAP:
                            continue
                        rewrite_attempt_counter[target_card_id] += 1
                        rewrite_article_card_counter[article_card_key] += 1
                        temp_candidate = type("TmpCand", (), {"id": temp_material.id, "span_type": temp_material.span_type})()
                        rewrite_result = try_rewrite_candidate_for_card(
                            provider=rewrite_provider,
                            model=rewrite_model,
                            prompt=rewrite_prompt,
                            family=family,
                            card_id=target_card_id,
                            article=article,
                            candidate=temp_candidate,
                            source_text=block_text,
                        )
                        if rewrite_result is None:
                            continue
                        rewritten_text = str(rewrite_result["text"])
                        rewritten_material = type(
                            "TempMaterial",
                            (),
                            {
                                "id": f"{temp_material.id}:rewrite:{target_card_id}",
                                "article_id": article.id,
                                "candidate_span_id": None,
                                "text": rewritten_text,
                                "span_type": "sentence_block_group",
                                "start_paragraph": 1,
                                "end_paragraph": 1,
                                "start_sentence": block[0][2],
                                "end_sentence": block[-1][2],
                                "paragraph_count": 1,
                                "sentence_count": size,
                                "quality_flags": [],
                            },
                        )()
                        rewritten_item = pipeline.build_cached_item_from_material(
                            material=rewritten_material,
                            article=article,
                            business_family_id=family,
                        )
                        if not rewritten_item:
                            rewritten_item = _build_rewrite_guided_cached_item(
                                family=family,
                                card_id=target_card_id,
                                article=article,
                                rewritten_text=rewritten_text,
                                material_like=rewritten_material,
                            )
                        if not rewritten_item:
                            continue
                        selected_card = str(((rewritten_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                        recommended = set(rewritten_item.get("business_card_recommendations") or [])
                        if target_card_id != selected_card and target_card_id not in recommended:
                            continue
                        ok, _ = shortage_accept_cached_item(family=family, card_id=target_card_id, cached_item=rewritten_item)
                        if not ok:
                            continue
                        rewritten_item.setdefault("source", {})
                        rewritten_item["source"]["rewrite_applied"] = True
                        rewritten_item["source"]["rewrite_mode"] = rewrite_result["rewrite_mode"]
                        rewritten_item["source"]["rewrite_target_card"] = target_card_id
                        rewritten_item["source"]["rewrite_summary"] = rewrite_result["rewrite_summary"]
                        rewritten_item["source"]["rewrite_source_candidate_id"] = temp_material.id
                        rewritten_item["source"]["rewrite_source_text_hash"] = text_hash
                        rewritten_item["source"]["rewrite_preserve_ratio_target"] = rewrite_result["preserve_ratio_target"]
                        rewritten_item["source"]["rewrite_preserve_ratio_actual"] = rewrite_result["preserve_ratio_actual"]
                        candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, rewritten_item)  # noqa: SLF001
                        if (candidate_span_id, family, target_card_id) in existing_pairs:
                            continue
                        rewritten_hash = base.build_content_hash(rewritten_text)
                        if rewritten_hash in hashes_by_family_card.get((family, target_card_id), set()):
                            continue
                        candidate = fast._candidate_namespace(candidate_span_id, rewritten_item)  # noqa: SLF001
                        material = MaterialSpanORM(
                            **base.material_payload(
                                family=family,
                                article=article,
                                candidate=candidate,
                                cached_item=rewritten_item,
                                selected_business_card=target_card_id,
                            )
                        )
                        session.add(material)
                        existing_pairs.add((candidate_span_id, family, target_card_id))
                        hashes_by_family_card.setdefault((family, target_card_id), set()).add(rewritten_hash)
                        deficits[family][target_card_id] -= 1
                        inserted_counter.setdefault(family, Counter())[target_card_id] += 1
                        inserted += 1
                        rewrite_success_counter[target_card_id] += 1
                        if len(rewrite_samples) < 80:
                            rewrite_samples.append(
                                {
                                    "family": family,
                                    "card_id": target_card_id,
                                    "article_title": article.title,
                                    "source_candidate_id": temp_material.id,
                                    "material_id": material.id,
                                    "rewrite_mode": rewrite_result["rewrite_mode"],
                                    "preserve_ratio_actual": rewrite_result["preserve_ratio_actual"],
                                    "rewrite_summary": rewrite_result["rewrite_summary"],
                                    "text_preview": rewritten_text[:160],
                                }
                            )
                        break
        session.commit()
    return inserted


def mine_sentence_order_windows_fastlane(
    *,
    session,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
    rewrite_attempt_counter: Counter,
    rewrite_success_counter: Counter,
    rewrite_article_card_counter: Counter,
    rewrite_samples: list[dict],
) -> int:
    family = "sentence_order"
    if all(deficits[family].get(card, 0) <= 0 for card in TARGETS[family]):
        return 0
    pipeline = configure_runtime_fastlane(MaterialPipelineV2())
    rewrite_provider, rewrite_model, rewrite_prompt = build_rewrite_llm()
    inserted = 0
    for article_id in article_ids:
        if all(deficits[family].get(card, 0) <= 0 for card in TARGETS[family]):
            break
        article = session.get(ArticleORM, article_id)
        if article is None:
            continue
        article_context = pipeline._build_article_context(article)  # noqa: SLF001
        paragraph_sentences = article_context.get("paragraph_sentences") or []
        flattened: list[tuple[int, int, int, str]] = []
        global_index = 0
        for paragraph_index, local_sentences in enumerate(paragraph_sentences):
            for local_index, sentence in enumerate(local_sentences):
                text = str(sentence or "").strip()
                if text:
                    flattened.append((paragraph_index, local_index, global_index, text))
                    global_index += 1
        if len(flattened) < 4:
            continue
        for start in range(0, len(flattened) - 3):
            if all(deficits[family].get(card, 0) <= 0 for card in TARGETS[family]):
                break
            for size in (4, 5, 6):
                block = flattened[start : start + size]
                if len(block) < size:
                    continue
                block_text = "\n".join(item[3] for item in block).strip()
                if len(block_text) < 80:
                    continue
                temp_material = type(
                    "TempMaterial",
                    (),
                    {
                        "id": f"orderwin::{article.id}::{block[0][2]}::{block[-1][2]}",
                        "article_id": article.id,
                        "candidate_span_id": None,
                        "text": block_text,
                        "span_type": "sentence_block_group",
                        "start_paragraph": block[0][0],
                        "end_paragraph": block[-1][0],
                        "start_sentence": block[0][2],
                        "end_sentence": block[-1][2],
                        "paragraph_count": max(1, block[-1][0] - block[0][0] + 1),
                        "sentence_count": size,
                        "quality_flags": [],
                    },
                )()
                cached_item = pipeline.build_cached_item_from_material(
                    material=temp_material,
                    article=article,
                    business_family_id=family,
                )
                candidate_cards = []
                if cached_item:
                    candidate_cards = [
                        str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or ""),
                        *(cached_item.get("business_card_recommendations") or []),
                    ]
                text_hash = base.build_content_hash(block_text)
                for card_id in dict.fromkeys(candidate_cards):
                    if card_id not in TARGETS[family] or deficits[family].get(card_id, 0) <= 0:
                        continue
                    if text_hash in hashes_by_family_card.get((family, card_id), set()):
                        continue
                    ok, _ = shortage_accept_cached_item(family=family, card_id=card_id, cached_item=cached_item)
                    if not ok:
                        continue
                    candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, cached_item)  # noqa: SLF001
                    if (candidate_span_id, family, card_id) in existing_pairs:
                        continue
                    candidate = fast._candidate_namespace(candidate_span_id, cached_item)  # noqa: SLF001
                    material = MaterialSpanORM(
                        **base.material_payload(
                            family=family,
                            article=article,
                            candidate=candidate,
                            cached_item=cached_item,
                            selected_business_card=card_id,
                        )
                    )
                    session.add(material)
                    existing_pairs.add((candidate_span_id, family, card_id))
                    hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                    deficits[family][card_id] -= 1
                    inserted_counter.setdefault(family, Counter())[card_id] += 1
                    inserted += 1
                    break
                else:
                    for target_card_id in rewrite_enabled_cards(family, deficits):
                        mode = rewrite_mode_for_card(target_card_id)
                        if rewrite_attempt_counter[target_card_id] >= REWRITE_MAX_ATTEMPTS_PER_CARD.get(mode, 0):
                            continue
                        article_card_key = f"{article.id}:{target_card_id}"
                        if rewrite_article_card_counter[article_card_key] >= REWRITE_PER_ARTICLE_CARD_CAP:
                            continue
                        rewrite_attempt_counter[target_card_id] += 1
                        rewrite_article_card_counter[article_card_key] += 1
                        temp_candidate = type("TmpCand", (), {"id": temp_material.id, "span_type": temp_material.span_type})()
                        rewrite_result = try_rewrite_candidate_for_card(
                            provider=rewrite_provider,
                            model=rewrite_model,
                            prompt=rewrite_prompt,
                            family=family,
                            card_id=target_card_id,
                            article=article,
                            candidate=temp_candidate,
                            source_text=block_text,
                        )
                        if rewrite_result is None:
                            continue
                        rewritten_text = str(rewrite_result["text"])
                        rewritten_material = type(
                            "TempMaterial",
                            (),
                            {
                                "id": f"{temp_material.id}:rewrite:{target_card_id}",
                                "article_id": article.id,
                                "candidate_span_id": None,
                                "text": rewritten_text,
                                "span_type": "sentence_block_group",
                                "start_paragraph": block[0][0],
                                "end_paragraph": block[-1][0],
                                "start_sentence": block[0][2],
                                "end_sentence": block[-1][2],
                                "paragraph_count": max(1, block[-1][0] - block[0][0] + 1),
                                "sentence_count": size,
                                "quality_flags": [],
                            },
                        )()
                        rewritten_item = pipeline.build_cached_item_from_material(
                            material=rewritten_material,
                            article=article,
                            business_family_id=family,
                        )
                        if not rewritten_item:
                            rewritten_item = _build_rewrite_guided_cached_item(
                                family=family,
                                card_id=target_card_id,
                                article=article,
                                rewritten_text=rewritten_text,
                                material_like=rewritten_material,
                            )
                        if not rewritten_item:
                            continue
                        selected_card = str(((rewritten_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
                        recommended = set(rewritten_item.get("business_card_recommendations") or [])
                        if target_card_id != selected_card and target_card_id not in recommended:
                            continue
                        ok, _ = shortage_accept_cached_item(family=family, card_id=target_card_id, cached_item=rewritten_item)
                        if not ok:
                            continue
                        rewritten_item.setdefault("source", {})
                        rewritten_item["source"]["rewrite_applied"] = True
                        rewritten_item["source"]["rewrite_mode"] = rewrite_result["rewrite_mode"]
                        rewritten_item["source"]["rewrite_target_card"] = target_card_id
                        rewritten_item["source"]["rewrite_summary"] = rewrite_result["rewrite_summary"]
                        rewritten_item["source"]["rewrite_source_candidate_id"] = temp_material.id
                        rewritten_item["source"]["rewrite_source_text_hash"] = text_hash
                        rewritten_item["source"]["rewrite_preserve_ratio_target"] = rewrite_result["preserve_ratio_target"]
                        rewritten_item["source"]["rewrite_preserve_ratio_actual"] = rewrite_result["preserve_ratio_actual"]
                        candidate_span_id = fast._ensure_candidate_span(session, candidate_cache, rewritten_item)  # noqa: SLF001
                        if (candidate_span_id, family, target_card_id) in existing_pairs:
                            continue
                        rewritten_hash = base.build_content_hash(rewritten_text)
                        if rewritten_hash in hashes_by_family_card.get((family, target_card_id), set()):
                            continue
                        candidate = fast._candidate_namespace(candidate_span_id, rewritten_item)  # noqa: SLF001
                        material = MaterialSpanORM(
                            **base.material_payload(
                                family=family,
                                article=article,
                                candidate=candidate,
                                cached_item=rewritten_item,
                                selected_business_card=target_card_id,
                            )
                        )
                        session.add(material)
                        existing_pairs.add((candidate_span_id, family, target_card_id))
                        hashes_by_family_card.setdefault((family, target_card_id), set()).add(rewritten_hash)
                        deficits[family][target_card_id] -= 1
                        inserted_counter.setdefault(family, Counter())[target_card_id] += 1
                        inserted += 1
                        rewrite_success_counter[target_card_id] += 1
                        if len(rewrite_samples) < 80:
                            rewrite_samples.append(
                                {
                                    "family": family,
                                    "card_id": target_card_id,
                                    "article_title": article.title,
                                    "source_candidate_id": temp_material.id,
                                    "material_id": material.id,
                                    "rewrite_mode": rewrite_result["rewrite_mode"],
                                    "preserve_ratio_actual": rewrite_result["preserve_ratio_actual"],
                                    "rewrite_summary": rewrite_result["rewrite_summary"],
                                    "text_preview": rewritten_text[:160],
                                }
                            )
                        break
        session.commit()
    return inserted


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    base_old, fast_old = patch_targets()
    run_mode = str(os.getenv("TARGETED_GAP_FILL_MODE", "full") or "full").strip().lower()
    rewrite_modes_old = dict(BUSINESS_CARD_REWRITE_MODES)
    if run_mode == "backlog_only":
        for card_id in list(BUSINESS_CARD_REWRITE_MODES.keys()):
            BUSINESS_CARD_REWRITE_MODES[card_id] = REWRITE_MODE_OFF
    try:
        report: dict[str, object] = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "run_mode": run_mode,
            "targets": TARGETS,
            "source_plan": SOURCE_PLAN,
            "before_stable_counts": filtered_stable_counts(),
        }
        write_progress({"phase": "started", **report})
        existing_pairs, hashes_by_family_card = load_existing_maps(session)
        candidate_cache: dict[tuple[str, str, str], str] = {}
        inserted_counter_total: dict[str, Counter] = {family: Counter() for family in TARGETS}
        inserted_samples_total: list[dict] = []
        rewrite_attempt_counter: Counter = Counter()
        rewrite_success_counter: Counter = Counter()
        rewrite_article_card_counter: Counter = Counter()
        rewrite_samples: list[dict] = []
        stages: list[dict[str, object]] = []
        write_progress(
            {
                "phase": "crawl_only_mode",
                "before_stable_counts": report["before_stable_counts"],
                "current_stable_counts": filtered_stable_counts(),
                "current_deficits": current_deficits(session),
            }
        )

        if run_mode != "backlog_only":
            for source_id in SOURCE_PLAN:
                fresh_deficits = current_deficits(session)
                if all(all(v <= 0 for v in family.values()) for family in fresh_deficits.values()):
                    break
                crawl_result = run_crawl_for_source_crawl_only(session, source_id)
                processed_article_ids = list(crawl_result.get("ingested_article_ids") or [])
                if not processed_article_ids and int(crawl_result.get("ingested_count") or 0) > 0:
                    processed_article_ids = [
                        row[0]
                        for row in session.query(ArticleORM.id).order_by(ArticleORM.created_at.desc()).limit(RECENT_ARTICLE_LIMIT).all()
                    ]
                stage = {
                    "stage": f"crawl:{source_id}",
                    "crawl_result": crawl_result,
                    "before_deficits": fresh_deficits,
                    "inserted": {},
                }
                if processed_article_ids:
                    stage["inserted"] = run_existing_pass(
                        session,
                        deficits=fresh_deficits,
                        existing_pairs=existing_pairs,
                        hashes_by_family_card=hashes_by_family_card,
                        candidate_cache=candidate_cache,
                        inserted_counter=inserted_counter_total,
                        inserted_samples=inserted_samples_total,
                        article_ids=processed_article_ids,
                        use_candidate_backfill=False,
                        rewrite_attempt_counter=rewrite_attempt_counter,
                        rewrite_success_counter=rewrite_success_counter,
                        rewrite_article_card_counter=rewrite_article_card_counter,
                        rewrite_samples=rewrite_samples,
                    )
                stage["after_deficits"] = current_deficits(session)
                promotion = clean.promote_gray_to_stable(session)
                stage["promotion"] = {
                    "promoted_count": promotion["promoted_count"],
                    "remaining_gaps": promotion["remaining_gaps"],
                }
                stages.append(stage)
                write_progress(
                    {
                        "phase": stage["stage"],
                        "before_stable_counts": report["before_stable_counts"],
                        "latest_stage": stage,
                        "current_stable_counts": filtered_stable_counts(),
                        "current_deficits": current_deficits(session),
                    }
                )

        remaining_deficits = current_deficits(session)
        if any(any(v > 0 for v in family.values()) for family in remaining_deficits.values()):
            backlog_ids = backlog_article_ids(session)
            backlog_stage = {
                "stage": "backlog:recent_articles",
                "article_count": len(backlog_ids),
                "before_deficits": remaining_deficits,
                "inserted": {},
            }
            if backlog_ids:
                backlog_stage["inserted"] = run_existing_pass(
                    session,
                    deficits=remaining_deficits,
                    existing_pairs=existing_pairs,
                    hashes_by_family_card=hashes_by_family_card,
                    candidate_cache=candidate_cache,
                    inserted_counter=inserted_counter_total,
                    inserted_samples=inserted_samples_total,
                    article_ids=backlog_ids,
                    use_candidate_backfill=False,
                    rewrite_attempt_counter=rewrite_attempt_counter,
                    rewrite_success_counter=rewrite_success_counter,
                    rewrite_article_card_counter=rewrite_article_card_counter,
                    rewrite_samples=rewrite_samples,
                )
            backlog_stage["after_deficits"] = current_deficits(session)
            promotion = clean.promote_gray_to_stable(session)
            backlog_stage["promotion"] = {
                "promoted_count": promotion["promoted_count"],
                "remaining_gaps": promotion["remaining_gaps"],
            }
            stages.append(backlog_stage)
            write_progress(
                {
                    "phase": backlog_stage["stage"],
                    "before_stable_counts": report["before_stable_counts"],
                    "latest_stage": backlog_stage,
                    "current_stable_counts": filtered_stable_counts(),
                    "current_deficits": current_deficits(session),
                }
            )

        heuristic_deficits = current_deficits(session)
        if any(any(v > 0 for v in family.values()) for family in heuristic_deficits.values()):
            heuristic_ids = backlog_article_ids(session)
            heuristic_stage = {
                "stage": "heuristic:shortage_fill",
                "article_count": len(heuristic_ids),
                "before_deficits": heuristic_deficits,
                "inserted": {},
            }
            if heuristic_ids:
                heuristic_stage["inserted"] = run_heuristic_shortage_pass(
                    session,
                    deficits=heuristic_deficits,
                    existing_pairs=existing_pairs,
                    hashes_by_family_card=hashes_by_family_card,
                    candidate_cache=candidate_cache,
                    inserted_counter=inserted_counter_total,
                    inserted_samples=inserted_samples_total,
                    article_ids=heuristic_ids,
                )
            heuristic_stage["after_deficits"] = current_deficits(session)
            promotion = clean.promote_gray_to_stable(session)
            heuristic_stage["promotion"] = {
                "promoted_count": promotion["promoted_count"],
                "remaining_gaps": promotion["remaining_gaps"],
            }
            stages.append(heuristic_stage)
            write_progress(
                {
                    "phase": heuristic_stage["stage"],
                    "before_stable_counts": report["before_stable_counts"],
                    "latest_stage": heuristic_stage,
                    "current_stable_counts": filtered_stable_counts(),
                    "current_deficits": current_deficits(session),
                }
            )

        report["stages"] = stages
        report["after_stable_counts"] = filtered_stable_counts()
        report["final_deficits"] = current_deficits(session)
        report["inserted_effective_counts"] = compact(inserted_counter_total)
        report["inserted_samples"] = inserted_samples_total[:120]
        report["rewrite_switches"] = dict(BUSINESS_CARD_REWRITE_MODES)
        report["rewrite_attempts"] = dict(rewrite_attempt_counter)
        report["rewrite_successes"] = dict(rewrite_success_counter)
        report["rewrite_samples"] = rewrite_samples[:80]
        report["sample_stable_materials"] = sample_stable_materials(session, limit_per_card=3)

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"targeted_material_gap_fill_{ts}.json"
        md_path = REPORTS_ROOT / f"targeted_material_gap_fill_{ts}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        write_progress(
            {
                "phase": "completed",
                "json_report": str(json_path),
                "md_report": str(md_path),
                "after_stable_counts": report["after_stable_counts"],
                "final_deficits": report["final_deficits"],
            }
        )

        lines = [
            "# Targeted Material Gap Fill Report",
            "",
            f"- run_at: `{report['run_at']}`",
            "",
            "## Before Stable Counts",
            "",
        ]
        for card, count in report["before_stable_counts"].items():
            target = next(targets[card] for targets in TARGETS.values() if card in targets)
            lines.append(f"- `{card}`: `{count}` / `{target}`")
        lines.extend(["", "## After Stable Counts", ""])
        for card, count in report["after_stable_counts"].items():
            target = next(targets[card] for targets in TARGETS.values() if card in targets)
            lines.append(f"- `{card}`: `{count}` / `{target}`")
        lines.extend(["", "## Final Deficits", ""])
        for family, family_deficits in report["final_deficits"].items():
            for card, gap in family_deficits.items():
                if gap > 0:
                    lines.append(f"- `{card}`: `{gap}`")
        lines.extend(["", "## Stage Summary", ""])
        for stage in stages:
            lines.append(f"### {stage['stage']}")
            inserted = stage.get("inserted") or {}
            if inserted:
                for family, count in inserted.items():
                    lines.append(f"- inserted `{family}`: `{count}`")
            promotion = stage.get("promotion") or {}
            if promotion:
                lines.append(f"- promoted_count: `{promotion.get('promoted_count', 0)}`")
            crawl_result = stage.get("crawl_result")
            if crawl_result:
                lines.append(f"- crawl ingested: `{crawl_result.get('ingested_count', 0)}`")
                lines.append(f"- crawl processed: `{len(crawl_result.get('processed_article_ids') or [])}`")
            lines.append("")
        lines.append("## Rewrite Switches")
        lines.append("")
        for card_id, mode in sorted(BUSINESS_CARD_REWRITE_MODES.items()):
            lines.append(f"- `{card_id}`: `{mode}`")
        lines.append("")
        lines.append("## Rewrite Stats")
        lines.append("")
        for card_id in sorted({*rewrite_attempt_counter.keys(), *rewrite_success_counter.keys()}):
            lines.append(f"- `{card_id}`: attempts=`{rewrite_attempt_counter.get(card_id, 0)}` success=`{rewrite_success_counter.get(card_id, 0)}`")
        lines.append("")
        lines.append("## Sample Stable Materials")
        lines.append("")
        for card, items in (report["sample_stable_materials"] or {}).items():
            lines.append(f"### {card}")
            lines.append("")
            for item in items:
                lines.append(f"- material_id: `{item['material_id']}`")
                lines.append(f"- article_id: `{item['article_id']}`")
                lines.append(f"- title: {item['title']}")
                lines.append(f"- quality_score: `{item['quality_score']}`")
                lines.append(f"- preview: {item['text_preview']}")
                lines.append("")
        md_path.write_text("\n".join(lines), encoding="utf-8")
        print(json.dumps({"json_report": str(json_path), "md_report": str(md_path), "after_stable_counts": report["after_stable_counts"], "final_deficits": report["final_deficits"]}, ensure_ascii=False))
        return 0
    finally:
        BUSINESS_CARD_REWRITE_MODES.clear()
        BUSINESS_CARD_REWRITE_MODES.update(rewrite_modes_old)
        restore_targets(base_old, fast_old)
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
