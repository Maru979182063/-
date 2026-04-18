from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from sqlalchemy import select


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"
DB_PATH = PASSAGE_SERVICE_ROOT / "passage_service.db"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.enums import MaterialStatus, ReleaseChannel  # noqa: E402
from app.domain.services.pool_service import PoolService  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.orm.paragraph import ParagraphORM  # noqa: E402
from app.infra.db.orm.sentence import SentenceORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


TARGETS = {
    "cause_effect__conclusion_focus__main_idea": 100,
    "necessary_condition_countermeasure__main_idea": 100,
    "parallel_comprehensive_summary__main_idea": 100,
    "theme_word_focus__main_idea": 100,
    "turning_relation_focus__main_idea": 100,
    "sentence_fill__opening_summary__abstract": 100,
    "sentence_fill__opening_topic_intro__abstract": 100,
    "sentence_fill__middle_carry_previous__abstract": 100,
    "sentence_fill__middle_lead_next__abstract": 100,
    "sentence_fill__middle_bridge_both_sides__abstract": 100,
    "sentence_fill__ending_summary__abstract": 100,
    "sentence_fill__ending_countermeasure__abstract": 100,
    "sentence_order__deterministic_binding__abstract": 100,
    "sentence_order__discourse_logic__abstract": 100,
    "sentence_order__head_tail_lock__abstract": 100,
    "sentence_order__head_tail_logic__abstract": 100,
    "sentence_order__timeline_action_sequence__abstract": 100,
}

FAMILIES = ["title_selection", "sentence_fill", "sentence_order"]
JUNK_TITLE_EXACT = {"全部导航"}
JUNK_TITLE_CONTAINS = {"隐私声明", "服务条款", "用户协议", "条款与条件"}
PER_ARTICLE_CAP = 16

MIN_QUALITY_BY_CARD = {
    "cause_effect__conclusion_focus__main_idea": 0.62,
    "necessary_condition_countermeasure__main_idea": 0.60,
    "parallel_comprehensive_summary__main_idea": 0.62,
    "theme_word_focus__main_idea": 0.62,
    "turning_relation_focus__main_idea": 0.68,
    "sentence_fill__opening_summary__abstract": 0.40,
    "sentence_fill__opening_topic_intro__abstract": 0.45,
    "sentence_fill__middle_carry_previous__abstract": 0.42,
    "sentence_fill__middle_lead_next__abstract": 0.42,
    "sentence_fill__middle_bridge_both_sides__abstract": 0.42,
    "sentence_fill__ending_summary__abstract": 0.42,
    "sentence_fill__ending_countermeasure__abstract": 0.45,
    "sentence_order__deterministic_binding__abstract": 0.40,
    "sentence_order__discourse_logic__abstract": 0.36,
    "sentence_order__head_tail_lock__abstract": 0.38,
    "sentence_order__head_tail_logic__abstract": 0.40,
    "sentence_order__timeline_action_sequence__abstract": 0.34,
}


def as_payload(raw: object) -> dict:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def title_is_junk(title: str) -> bool:
    text = str(title or "").strip()
    if not text:
        return True
    if text in JUNK_TITLE_EXACT:
        return True
    return any(token in text for token in JUNK_TITLE_CONTAINS)


def selected_cards(payload: dict) -> set[str]:
    covered: set[str] = set()
    for fam in FAMILIES:
        fam_payload = payload.get(fam) or {}
        qrc = fam_payload.get("question_ready_context") or {}
        card = str(qrc.get("selected_business_card") or fam_payload.get("selected_business_card") or "")
        if card in TARGETS:
            covered.add(card)
    return covered


def material_quality(item: MaterialSpanORM) -> float:
    try:
        return float(item.quality_score or 0.0)
    except (TypeError, ValueError):
        return 0.0


def stable_counts_sql() -> dict[str, int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        result: dict[str, int] = {}
        for card in TARGETS:
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
            result[card] = int((row[0] or 0))
        return result
    finally:
        conn.close()


def purge_junk_articles(session) -> dict[str, object]:
    articles = session.scalars(select(ArticleORM)).all()
    junk_articles = [article for article in articles if title_is_junk(getattr(article, "title", ""))]
    article_ids = [article.id for article in junk_articles]
    title_counter = Counter(str(article.title or "") for article in junk_articles)
    if not article_ids:
        return {"article_count": 0, "material_count": 0, "candidate_count": 0, "titles": {}}

    material_count = session.query(MaterialSpanORM).filter(MaterialSpanORM.article_id.in_(article_ids)).count()
    candidate_count = session.query(CandidateSpanORM).filter(CandidateSpanORM.article_id.in_(article_ids)).count()

    session.query(MaterialSpanORM).filter(MaterialSpanORM.article_id.in_(article_ids)).delete(synchronize_session=False)
    session.query(CandidateSpanORM).filter(CandidateSpanORM.article_id.in_(article_ids)).delete(synchronize_session=False)
    session.query(ParagraphORM).filter(ParagraphORM.article_id.in_(article_ids)).delete(synchronize_session=False)
    session.query(SentenceORM).filter(SentenceORM.article_id.in_(article_ids)).delete(synchronize_session=False)
    session.query(ArticleORM).filter(ArticleORM.id.in_(article_ids)).delete(synchronize_session=False)
    session.commit()

    return {
        "article_count": len(article_ids),
        "material_count": int(material_count),
        "candidate_count": int(candidate_count),
        "titles": dict(title_counter),
    }


def promote_gray_to_stable(session) -> dict[str, object]:
    pool = PoolService(session)
    stable_counts = Counter(stable_counts_sql())
    stable_article_counts: Counter[str] = Counter()
    for item in session.scalars(
        select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == MaterialStatus.PROMOTED.value,
            MaterialSpanORM.release_channel == ReleaseChannel.STABLE.value,
        )
    ):
        stable_article_counts[str(item.article_id)] += 1

    gray_items: list[tuple[MaterialSpanORM, set[str], float]] = []
    for item in session.scalars(
        select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == MaterialStatus.GRAY.value,
            MaterialSpanORM.release_channel == ReleaseChannel.GRAY.value,
        )
    ):
        article = session.get(ArticleORM, item.article_id)
        if article is None or title_is_junk(getattr(article, "title", "")):
            continue
        payload = as_payload(item.v2_index_payload)
        covered = {card for card in selected_cards(payload) if card in TARGETS}
        if not covered:
            continue
        q = material_quality(item)
        eligible_cards = {card for card in covered if q >= MIN_QUALITY_BY_CARD.get(card, 0.0)}
        if not eligible_cards:
            continue
        gray_items.append((item, eligible_cards, q))

    promoted: list[dict[str, object]] = []
    remaining = list(gray_items)

    while True:
        deficits = {card: max(0, TARGETS[card] - stable_counts.get(card, 0)) for card in TARGETS}
        if all(value <= 0 for value in deficits.values()):
            break
        best_idx = None
        best_score = -1.0
        for idx, (item, covered, q) in enumerate(remaining):
            if stable_article_counts.get(str(item.article_id), 0) >= PER_ARTICLE_CAP:
                continue
            gain_cards = [card for card in covered if deficits.get(card, 0) > 0]
            if not gain_cards:
                continue
            gain = sum(deficits[card] for card in gain_cards)
            score = gain * 10.0 + q
            if "sentence_order__head_tail_logic__abstract" in gain_cards:
                score += 1.5
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx is None:
            break
        item, covered, q = remaining.pop(best_idx)
        item.status = MaterialStatus.PROMOTED.value
        item.release_channel = ReleaseChannel.STABLE.value
        item.gray_ratio = 0.0
        item.gray_reason = "overnight_promote_after_cleanup"
        promoted.append(
            {
                "material_id": item.id,
                "article_id": item.article_id,
                "quality": q,
                "covered_cards": sorted(covered),
            }
        )
        stable_article_counts[str(item.article_id)] += 1
        for card in covered:
            if stable_counts.get(card, 0) < TARGETS[card]:
                stable_counts[card] += 1

    session.commit()
    for row in promoted:
        pool.audit_repo.log(
            "material",
            row["material_id"],
            "state_change",
            {
                "status": MaterialStatus.PROMOTED.value,
                "release_channel": ReleaseChannel.STABLE.value,
                "reason": "overnight_promote_after_cleanup",
                "covered_cards": row["covered_cards"],
            },
        )

    return {
        "promoted_count": len(promoted),
        "promoted_samples": promoted[:30],
        "final_stable_counts": dict(stable_counts),
        "remaining_gaps": {card: max(0, TARGETS[card] - stable_counts.get(card, 0)) for card in TARGETS if stable_counts.get(card, 0) < TARGETS[card]},
    }


def build_markdown(report: dict) -> str:
    lines = [
        "# Runtime Material Cleanup Report",
        "",
        f"- run_at: `{report['run_at']}`",
        f"- db: `{DB_PATH}`",
        "",
        "## Deleted Junk",
        "",
        f"- deleted_articles: `{report['deleted_junk']['article_count']}`",
        f"- deleted_materials: `{report['deleted_junk']['material_count']}`",
        f"- deleted_candidates: `{report['deleted_junk']['candidate_count']}`",
        "",
    ]
    for title, count in sorted((report["deleted_junk"].get("titles") or {}).items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"- `{title}`: `{count}`")
    lines.extend(["", "## Promotion", "", f"- promoted_count: `{report['promotion']['promoted_count']}`", ""])
    lines.append("## Final Stable Counts")
    lines.append("")
    for card, count in sorted((report["promotion"].get("final_stable_counts") or {}).items()):
        lines.append(f"- `{card}`: `{count}` / `{TARGETS[card]}`")
    if report["promotion"].get("remaining_gaps"):
        lines.extend(["", "## Remaining Gaps", ""])
        for card, gap in sorted(report["promotion"]["remaining_gaps"].items()):
            lines.append(f"- `{card}`: `{gap}`")
    return "\n".join(lines)


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    try:
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
        }
        report["before_counts"] = stable_counts_sql()
        report["deleted_junk"] = purge_junk_articles(session)
        report["promotion"] = promote_gray_to_stable(session)
        report["after_counts"] = stable_counts_sql()

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"overnight_runtime_material_cleanup_{ts}.json"
        md_path = REPORTS_ROOT / f"overnight_runtime_material_cleanup_{ts}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(json.dumps({"json_report": str(json_path), "md_report": str(md_path), "after_counts": report["after_counts"], "remaining_gaps": report["promotion"]["remaining_gaps"]}, ensure_ascii=False, indent=2))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
