from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from scripts import overnight_runtime_material_cleanup as clean  # noqa: E402
from scripts import targeted_material_gap_fill as gap_fill  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


TARGET_CARDS = [
    "sentence_fill__opening_topic_intro__abstract",
    "sentence_fill__middle_carry_previous__abstract",
    "sentence_fill__middle_bridge_both_sides__abstract",
    "sentence_order__discourse_logic__abstract",
    "sentence_order__head_tail_lock__abstract",
    "sentence_order__head_tail_logic__abstract",
    "sentence_order__timeline_action_sequence__abstract",
]

TITLE_JUNK_TOKENS = (
    "版权",
    "免责声明",
    "服务条款",
    "用户协议",
    "隐私声明",
    "全部导航",
    "copyright",
    "privacy",
    "terms",
)
TEXT_JUNK_TOKENS = (
    "图库版权图片",
    "转载使用可能引发版权纠纷",
    "来源：中国经济网",
    "copyright",
)

SOFT_PURGE_LIMITS = {
    "sentence_fill__opening_topic_intro__abstract": 10,
    "sentence_fill__middle_carry_previous__abstract": 16,
    "sentence_fill__middle_bridge_both_sides__abstract": 12,
    "sentence_order__discourse_logic__abstract": 6,
    "sentence_order__head_tail_lock__abstract": 14,
    "sentence_order__head_tail_logic__abstract": 14,
    "sentence_order__timeline_action_sequence__abstract": 8,
}

SOFT_PURGE_THRESHOLDS = {
    "sentence_fill__opening_topic_intro__abstract": 0.48,
    "sentence_fill__middle_carry_previous__abstract": 0.54,
    "sentence_fill__middle_bridge_both_sides__abstract": 0.50,
    "sentence_order__discourse_logic__abstract": 0.46,
    "sentence_order__head_tail_lock__abstract": 0.50,
    "sentence_order__head_tail_logic__abstract": 0.52,
    "sentence_order__timeline_action_sequence__abstract": 0.46,
}

BACKLOG_LIMIT = 320
REFILL_ROUNDS = 4


def primary_target_card(payload: dict) -> str | None:
    for family in ("sentence_fill", "sentence_order"):
        fam_payload = payload.get(family) or {}
        qrc = fam_payload.get("question_ready_context") or {}
        card_id = str(qrc.get("selected_business_card") or fam_payload.get("selected_business_card") or "")
        if card_id in TARGET_CARDS:
            return card_id
    return None


def _fill_profile(payload: dict) -> dict:
    return ((payload.get("sentence_fill") or {}).get("business_feature_profile") or {}).get("sentence_fill_profile") or {}


def _order_profile(payload: dict) -> dict:
    return ((payload.get("sentence_order") or {}).get("business_feature_profile") or {}).get("sentence_order_profile") or {}


def _source_flags(item: MaterialSpanORM) -> dict[str, bool]:
    source = item.source or {}
    return {
        "heuristic": bool(source.get("heuristic_shortage_fill") or source.get("heuristic_shortage_order")),
        "rewrite": bool(source.get("rewrite_applied")),
    }


def assess_material(
    *,
    card_id: str,
    title: str,
    text: str,
    payload: dict,
    quality_score: float,
    flags: dict[str, bool],
) -> dict[str, object]:
    title_lower = (title or "").lower()
    text_lower = (text or "").lower()
    hard_reasons: list[str] = []
    soft_reasons: list[str] = []
    priority = 0.0

    if any(token in title_lower for token in TITLE_JUNK_TOKENS):
        hard_reasons.append("junk_title")
    if any(token in text_lower for token in TEXT_JUNK_TOKENS):
        hard_reasons.append("text_noise")

    fill = _fill_profile(payload)
    order = _order_profile(payload)
    text_len = len(text.strip())

    if flags["heuristic"]:
        priority += 0.08
        soft_reasons.append("heuristic_stock")

    if card_id == "sentence_fill__opening_topic_intro__abstract":
        if quality_score < 0.46:
            priority += 0.18
            soft_reasons.append("opening_low_quality")
        if text_len > 220:
            priority += 0.14
            soft_reasons.append("opening_too_wide")
        if not any(marker in text for marker in gap_fill.OPENING_MARKERS):
            priority += 0.18
            soft_reasons.append("opening_weak_intro")
    elif card_id == "sentence_fill__middle_carry_previous__abstract":
        backward = float(fill.get("backward_link_strength") or 0.0)
        forward = float(fill.get("forward_link_strength") or 0.0)
        if backward < 0.60:
            priority += 0.28
            soft_reasons.append("carry_previous_weak_backward")
        if forward > 0.46:
            priority += 0.10
            soft_reasons.append("carry_previous_forwardish")
        if text_len > 135:
            priority += 0.16
            soft_reasons.append("carry_previous_too_long")
        if not any(marker in text for marker in gap_fill.CARRY_MARKERS):
            priority += 0.18
            soft_reasons.append("carry_previous_no_marker")
    elif card_id == "sentence_fill__middle_bridge_both_sides__abstract":
        backward = float(fill.get("backward_link_strength") or 0.0)
        forward = float(fill.get("forward_link_strength") or 0.0)
        bidi = float(fill.get("bidirectional_validation") or 0.0)
        if min(backward, forward) < 0.52:
            priority += 0.24
            soft_reasons.append("bridge_weak_sides")
        if bidi < 0.60:
            priority += 0.26
            soft_reasons.append("bridge_weak_bidirectional")
        if text_len > 155:
            priority += 0.12
            soft_reasons.append("bridge_too_long")
        if not any(marker in text for marker in gap_fill.BRIDGE_PATTERNS):
            priority += 0.12
            soft_reasons.append("bridge_pattern_weak")
    elif card_id == "sentence_order__discourse_logic__abstract":
        progression = float(order.get("discourse_progression_strength") or 0.0)
        if progression < 0.42:
            priority += 0.22
            soft_reasons.append("discourse_logic_weak_progression")
        if float(order.get("binding_pair_count") or 0.0) < 1.4:
            priority += 0.12
            soft_reasons.append("discourse_logic_weak_binding")
    elif card_id == "sentence_order__head_tail_lock__abstract":
        opener = str(order.get("opening_rule") or "")
        closer = str(order.get("closing_rule") or "")
        if opener == "none":
            priority += 0.22
            soft_reasons.append("head_tail_lock_missing_open")
        if closer == "none":
            priority += 0.22
            soft_reasons.append("head_tail_lock_missing_close")
        if float(order.get("unique_opener_score") or 0.0) < 0.46:
            priority += 0.14
            soft_reasons.append("head_tail_lock_weak_opener")
        if float(order.get("context_closure_score") or 0.0) < 0.42:
            priority += 0.14
            soft_reasons.append("head_tail_lock_weak_closure")
    elif card_id == "sentence_order__head_tail_logic__abstract":
        if str(order.get("opening_rule") or "") == "none":
            priority += 0.18
            soft_reasons.append("head_tail_logic_missing_open")
        if float(order.get("binding_pair_count") or 0.0) < 1.8:
            priority += 0.20
            soft_reasons.append("head_tail_logic_weak_binding")
        if float(order.get("context_closure_score") or 0.0) < 0.40:
            priority += 0.18
            soft_reasons.append("head_tail_logic_weak_closure")
    elif card_id == "sentence_order__timeline_action_sequence__abstract":
        logic_modes = set(order.get("logic_modes") or [])
        temporal = max(
            float(order.get("temporal_order_strength") or 0.0),
            float(order.get("action_sequence_irreversibility") or 0.0),
        )
        if not logic_modes.intersection({"timeline_sequence", "action_sequence"}):
            priority += 0.22
            soft_reasons.append("timeline_missing_mode")
        if temporal < 0.26:
            priority += 0.22
            soft_reasons.append("timeline_weak_temporal_chain")

    if quality_score < 0.38:
        priority += 0.20
        soft_reasons.append("very_low_quality_score")
    elif quality_score < 0.44:
        priority += 0.10
        soft_reasons.append("low_quality_score")

    threshold = SOFT_PURGE_THRESHOLDS.get(card_id, 0.50)
    soft_purge = bool(soft_reasons) and priority >= threshold
    return {
        "hard_purge": bool(hard_reasons),
        "hard_reasons": hard_reasons,
        "soft_purge": soft_purge,
        "soft_reasons": soft_reasons,
        "priority": round(priority, 4),
    }


def cleanup_target_cards(session) -> dict[str, object]:
    hard_delete_ids: set[str] = set()
    soft_candidates: dict[str, list[dict[str, object]]] = defaultdict(list)
    purged_samples: list[dict[str, object]] = []

    rows = (
        session.query(MaterialSpanORM)
        .filter(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
        )
        .all()
    )

    for item in rows:
        payload = item.v2_index_payload or {}
        card_id = primary_target_card(payload)
        if card_id is None:
            continue
        title = str((item.source or {}).get("article_title") or "")
        text = str(item.text or "")
        quality = float(item.quality_score or 0.0)
        flags = _source_flags(item)
        assessment = assess_material(
            card_id=card_id,
            title=title,
            text=text,
            payload=payload,
            quality_score=quality,
            flags=flags,
        )
        sample = {
            "material_id": item.id,
            "article_id": item.article_id,
            "card_id": card_id,
            "quality_score": quality,
            "priority": assessment["priority"],
            "heuristic": flags["heuristic"],
            "rewrite": flags["rewrite"],
            "text_preview": text[:160],
        }
        if assessment["hard_purge"]:
            hard_delete_ids.add(item.id)
            sample["reasons"] = assessment["hard_reasons"]
            if len(purged_samples) < 40:
                purged_samples.append(sample)
            continue
        if assessment["soft_purge"]:
            sample["reasons"] = assessment["soft_reasons"]
            soft_candidates[card_id].append(sample)

    selected_ids = set(hard_delete_ids)
    purged_by_card: Counter = Counter()
    soft_kept: dict[str, int] = {}
    for card_id, candidates in soft_candidates.items():
        candidates.sort(key=lambda item: (-float(item["priority"]), float(item["quality_score"])))
        limit = SOFT_PURGE_LIMITS.get(card_id, 0)
        chosen = candidates[:limit]
        soft_kept[card_id] = max(0, len(candidates) - len(chosen))
        for sample in chosen:
            selected_ids.add(str(sample["material_id"]))
            purged_by_card[card_id] += 1
            if len(purged_samples) < 80:
                purged_samples.append(sample)

    if selected_ids:
        doomed = session.query(MaterialSpanORM).filter(MaterialSpanORM.id.in_(selected_ids)).all()
        for item in doomed:
            session.delete(item)
        session.commit()

    return {
        "purged_count": len(selected_ids),
        "hard_purge_count": len(hard_delete_ids),
        "soft_purge_by_card": dict(purged_by_card),
        "soft_candidates_kept": soft_kept,
        "purged_samples": purged_samples[:80],
    }


def run_refill_rounds(session, rounds: int = REFILL_ROUNDS) -> list[dict[str, object]]:
    round_reports: list[dict[str, object]] = []
    old_cap = clean.PER_ARTICLE_CAP
    try:
        clean.PER_ARTICLE_CAP = 128
        for index in range(1, rounds + 1):
            before = gap_fill.filtered_stable_counts()
            deficits = gap_fill.current_deficits(session)
            if all(all(v <= 0 for v in family.values()) for family in deficits.values()):
                round_reports.append({"round": index, "stopped": "no_deficits", "before": before})
                break

            existing_pairs, hashes = gap_fill.load_existing_maps(session)
            candidate_cache: dict[tuple[str, str, str], str] = {}
            inserted_counter = {family: Counter() for family in gap_fill.TARGETS}
            inserted_samples: list[dict] = []
            rewrite_attempt_counter: Counter = Counter()
            rewrite_success_counter: Counter = Counter()
            rewrite_article_card_counter: Counter = Counter()
            rewrite_samples: list[dict] = []

            article_ids = gap_fill.backlog_article_ids(session, limit=BACKLOG_LIMIT)
            direct_inserted = gap_fill.run_existing_pass(
                session,
                deficits=deficits,
                existing_pairs=existing_pairs,
                hashes_by_family_card=hashes,
                candidate_cache=candidate_cache,
                inserted_counter=inserted_counter,
                inserted_samples=inserted_samples,
                article_ids=article_ids,
                use_candidate_backfill=False,
                rewrite_attempt_counter=rewrite_attempt_counter,
                rewrite_success_counter=rewrite_success_counter,
                rewrite_article_card_counter=rewrite_article_card_counter,
                rewrite_samples=rewrite_samples,
            )
            promotion_primary = clean.promote_gray_to_stable(session)
            heuristic_inserted = {family: 0 for family in gap_fill.TARGETS}

            deficits_after_primary = gap_fill.current_deficits(session)
            if any(any(v > 0 for v in family.values()) for family in deficits_after_primary.values()):
                heuristic_inserted = gap_fill.run_heuristic_shortage_pass(
                    session,
                    deficits=deficits_after_primary,
                    existing_pairs=existing_pairs,
                    hashes_by_family_card=hashes,
                    candidate_cache=candidate_cache,
                    inserted_counter=inserted_counter,
                    inserted_samples=inserted_samples,
                    article_ids=article_ids,
                )
                promotion_secondary = clean.promote_gray_to_stable(session)
            else:
                promotion_secondary = {"promoted_count": 0, "remaining_gaps": None}

            after = gap_fill.filtered_stable_counts()
            round_reports.append(
                {
                    "round": index,
                    "before": before,
                    "direct_inserted": direct_inserted,
                    "heuristic_inserted": heuristic_inserted,
                    "promoted_count": int(promotion_primary.get("promoted_count", 0)) + int(promotion_secondary.get("promoted_count", 0)),
                    "rewrite_attempts": dict(rewrite_attempt_counter),
                    "rewrite_successes": dict(rewrite_success_counter),
                    "rewrite_samples": rewrite_samples[:20],
                    "after": after,
                    "remaining_gaps": gap_fill.current_deficits(session),
                }
            )
            if before == after:
                break
    finally:
        clean.PER_ARTICLE_CAP = old_cap
    return round_reports


def origin_mix(session) -> dict[str, dict[str, int]]:
    rows = (
        session.query(MaterialSpanORM)
        .filter(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
        )
        .all()
    )
    mix = {card_id: {"heuristic": 0, "rewrite": 0, "native": 0} for card_id in TARGET_CARDS}
    for item in rows:
        card_id = primary_target_card(item.v2_index_payload or {})
        if card_id not in mix:
            continue
        flags = _source_flags(item)
        if flags["rewrite"]:
            mix[card_id]["rewrite"] += 1
        elif flags["heuristic"]:
            mix[card_id]["heuristic"] += 1
        else:
            mix[card_id]["native"] += 1
    return mix


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    try:
        before = gap_fill.filtered_stable_counts()
        before_mix = origin_mix(session)
        cleanup_report = cleanup_target_cards(session)
        rounds = run_refill_rounds(session, rounds=REFILL_ROUNDS)
        after = gap_fill.filtered_stable_counts()
        after_mix = origin_mix(session)
        final_deficits = gap_fill.current_deficits(session)

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "before": before,
            "before_mix": before_mix,
            "cleanup": cleanup_report,
            "rounds": rounds,
            "after": after,
            "after_mix": after_mix,
            "final_deficits": final_deficits,
        }
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"targeted_card_cleanup_and_refill_{ts}.json"
        md_path = REPORTS_ROOT / f"targeted_card_cleanup_and_refill_{ts}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            "# Targeted Card Cleanup And Refill",
            "",
            f"- run_at: `{report['run_at']}`",
            "",
            "## Before",
            "",
        ]
        for card, count in before.items():
            lines.append(f"- `{card}`: `{count}` / mix=`{before_mix.get(card)}`")
        lines.extend(["", "## Cleanup", ""])
        lines.append(f"- purged_count: `{cleanup_report['purged_count']}`")
        lines.append(f"- hard_purge_count: `{cleanup_report['hard_purge_count']}`")
        lines.append(f"- soft_purge_by_card: `{cleanup_report['soft_purge_by_card']}`")
        for sample in cleanup_report["purged_samples"][:20]:
            lines.append(f"- `{sample['material_id']}` / `{sample['card_id']}` / `{sample['reasons']}` / priority=`{sample['priority']}`")
        lines.extend(["", "## Rounds", ""])
        for round_report in rounds:
            lines.append(f"### round {round_report.get('round')}")
            lines.append(f"- direct_inserted: `{round_report.get('direct_inserted')}`")
            lines.append(f"- heuristic_inserted: `{round_report.get('heuristic_inserted')}`")
            lines.append(f"- promoted_count: `{round_report.get('promoted_count')}`")
            lines.append(f"- rewrite_attempts: `{round_report.get('rewrite_attempts')}`")
            lines.append(f"- rewrite_successes: `{round_report.get('rewrite_successes')}`")
            lines.append(f"- remaining_gaps: `{round_report.get('remaining_gaps')}`")
            lines.append("")
        lines.extend(["## After", ""])
        for card, count in after.items():
            lines.append(f"- `{card}`: `{count}` / mix=`{after_mix.get(card)}`")
        lines.extend(["", "## Final Deficits", ""])
        for family, family_deficits in final_deficits.items():
            for card, gap in family_deficits.items():
                if gap > 0:
                    lines.append(f"- `{card}`: `{gap}`")
        md_path.write_text("\n".join(lines), encoding="utf-8")

        print(
            json.dumps(
                {
                    "json_report": str(json_path),
                    "md_report": str(md_path),
                    "after": after,
                    "after_mix": after_mix,
                    "final_deficits": final_deficits,
                },
                ensure_ascii=False,
            )
        )
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
