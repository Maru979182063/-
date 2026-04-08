from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import run_v2_offline_backtest as backtest


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "reports"


@dataclass
class ReviewCandidate:
    material_id: str
    article_id: str
    review_status: str
    material_status: str
    release_channel: str
    family_count: int
    chosen_family: str
    chosen_task_family: str
    priority_score: float
    uncertainty_score: float
    salvage_score: float
    novelty_score: float
    disagreement_score: float
    age_score: float
    quality_score: float
    final_candidate_score: float
    readiness_score: float
    total_penalty: float
    recommended: bool
    needs_review: bool
    selected_material_card: str | None
    selected_business_card: str | None
    top_penalties: list[dict[str, Any]]
    top_difficulty_dimensions: list[dict[str, Any]]
    family_breakdown: list[dict[str, Any]]
    reasons: list[str]
    text_preview: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline-only active learning queue builder for manual material review."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=backtest.DEFAULT_DB_PATH,
        help="Path to passage_service SQLite database.",
    )
    parser.add_argument(
        "--families",
        nargs="*",
        default=None,
        help="Optional subset of business_family_id values to include.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=30,
        help="How many review candidates to return.",
    )
    parser.add_argument(
        "--per-family-cap",
        type=int,
        default=10,
        help="Maximum number of selected rows from the same business family.",
    )
    parser.add_argument(
        "--per-card-cap",
        type=int,
        default=6,
        help="Maximum number of selected rows sharing the same selected business card.",
    )
    parser.add_argument(
        "--include-missing-review",
        action="store_true",
        help="Include gray items with missing tagging review rows.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated reports.",
    )
    return parser.parse_args()


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def main() -> int:
    configure_stdout()
    args = parse_args()
    selected_families = set(args.families or [])
    records = backtest.load_records(args.db_path, selected_families if selected_families else None)
    queue_report = build_active_learning_report(
        records=records,
        top_k=max(1, args.top_k),
        per_family_cap=max(1, args.per_family_cap),
        per_card_cap=max(1, args.per_card_cap),
        include_missing_review=args.include_missing_review,
        db_path=args.db_path,
        selected_families=args.families,
    )
    json_path, md_path = write_report(queue_report, args.output_dir)
    print(json.dumps({"json_report": str(json_path), "markdown_report": str(md_path)}, ensure_ascii=False))
    return 0


def build_active_learning_report(
    *,
    records: list[backtest.Record],
    top_k: int,
    per_family_cap: int,
    per_card_cap: int,
    include_missing_review: bool,
    db_path: Path,
    selected_families: list[str] | None,
) -> dict[str, Any]:
    eligible_records = filter_records(records, include_missing_review=include_missing_review)
    business_card_counts = Counter(record.selected_business_card or "" for record in eligible_records)
    family_counts = Counter(record.business_family_id for record in eligible_records)

    candidates = build_material_candidates(
        records=eligible_records,
        business_card_counts=business_card_counts,
        family_counts=family_counts,
    )
    diversified = diversify_candidates(
        candidates=candidates,
        top_k=top_k,
        per_family_cap=per_family_cap,
        per_card_cap=per_card_cap,
    )

    queue: list[dict[str, Any]] = []
    for index, candidate in enumerate(diversified, start=1):
        item = candidate_to_dict(candidate)
        item["rank"] = index
        queue.append(item)

    family_distribution = Counter(candidate.chosen_family for candidate in diversified)
    card_distribution = Counter(candidate.selected_business_card or "" for candidate in diversified)
    reason_distribution = Counter(reason for candidate in diversified for reason in candidate.reasons)

    return {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "selected_families": selected_families or [],
        "filters": {
            "top_k": top_k,
            "per_family_cap": per_family_cap,
            "per_card_cap": per_card_cap,
            "include_missing_review": include_missing_review,
        },
        "summary": {
            "eligible_record_count": len(eligible_records),
            "eligible_material_count": len({record.material_id for record in eligible_records}),
            "selected_candidate_count": len(diversified),
            "selected_family_distribution": dict(family_distribution),
            "selected_card_distribution": {key: value for key, value in card_distribution.items() if key},
            "selected_reason_distribution": dict(reason_distribution),
        },
        "queue": queue,
        "notes": [
            "This queue is offline-only and meant for manual review planning.",
            "Priority favors borderline uncertainty, salvageable quality, under-covered families/cards, and queue diversity.",
            "No service endpoint, database state, or threshold configuration was modified.",
        ],
    }


def filter_records(records: list[backtest.Record], *, include_missing_review: bool) -> list[backtest.Record]:
    filtered: list[backtest.Record] = []
    for record in records:
        if not record.has_scoring:
            continue
        if record.material_status == "deprecated":
            continue
        if record.review_status == "review_pending":
            filtered.append(record)
            continue
        if record.material_status == "gray" and include_missing_review and not record.review_status:
            filtered.append(record)
    return filtered


def build_material_candidates(
    *,
    records: list[backtest.Record],
    business_card_counts: Counter[str],
    family_counts: Counter[str],
) -> list[ReviewCandidate]:
    grouped: dict[str, list[backtest.Record]] = defaultdict(list)
    for record in records:
        grouped[record.material_id].append(record)

    candidates: list[ReviewCandidate] = []
    for material_id, material_records in grouped.items():
        enriched = [
            score_record(
                record=record,
                business_card_counts=business_card_counts,
                family_counts=family_counts,
            )
            for record in material_records
        ]
        enriched.sort(key=lambda item: item["priority_score"], reverse=True)
        top = enriched[0]
        family_breakdown = [
            {
                "business_family_id": item["record"].business_family_id,
                "task_family": item["record"].task_family,
                "priority_score": round(item["priority_score"], 4),
                "quality_score": round(item["record"].quality_score, 4),
                "final_candidate_score": round(item["record"].final_candidate_score, 4),
                "readiness_score": round(item["record"].readiness_score, 4),
                "total_penalty": round(item["record"].total_penalty, 4),
                "recommended": item["record"].recommended,
                "needs_review": item["record"].needs_review,
                "selected_business_card": item["record"].selected_business_card,
            }
            for item in enriched[:4]
        ]
        top_record = top["record"]
        candidates.append(
            ReviewCandidate(
                material_id=material_id,
                article_id=top_record.article_id,
                review_status=top_record.review_status or "missing_review",
                material_status=top_record.material_status,
                release_channel=top_record.release_channel,
                family_count=len(material_records),
                chosen_family=top_record.business_family_id,
                chosen_task_family=top_record.task_family,
                priority_score=round(top["priority_score"], 4),
                uncertainty_score=round(top["uncertainty_score"], 4),
                salvage_score=round(top["salvage_score"], 4),
                novelty_score=round(top["novelty_score"], 4),
                disagreement_score=round(top["disagreement_score"], 4),
                age_score=round(top["age_score"], 4),
                quality_score=round(top_record.quality_score, 4),
                final_candidate_score=round(top_record.final_candidate_score, 4),
                readiness_score=round(top_record.readiness_score, 4),
                total_penalty=round(top_record.total_penalty, 4),
                recommended=top_record.recommended,
                needs_review=top_record.needs_review,
                selected_material_card=top_record.selected_material_card,
                selected_business_card=top_record.selected_business_card,
                top_penalties=backtest.top_float_items(top_record.risk_penalties),
                top_difficulty_dimensions=backtest.top_float_items(top_record.difficulty_vector),
                family_breakdown=family_breakdown,
                reasons=top["reasons"],
                text_preview=top_record.text_preview,
            )
        )
    candidates.sort(key=lambda item: item.priority_score, reverse=True)
    return candidates


def score_record(
    *,
    record: backtest.Record,
    business_card_counts: Counter[str],
    family_counts: Counter[str],
) -> dict[str, Any]:
    thresholds = backtest.CURRENT_THRESHOLDS.get(record.task_family, {})
    recommended_threshold = float(thresholds.get("recommended") or 0.50)
    review_readiness_threshold = float(thresholds.get("review_readiness") or 0.58)
    review_penalty_threshold = float(thresholds.get("review_penalty") or 0.28)
    fallback_threshold = float(thresholds.get("fallback_review_score") or max(0.0, recommended_threshold - 0.10))

    uncertainty_parts = [
        closeness(record.final_candidate_score, recommended_threshold, spread=0.16),
        closeness(record.readiness_score, review_readiness_threshold, spread=0.16),
        closeness(record.total_penalty, review_penalty_threshold, spread=0.18),
        closeness(record.final_candidate_score, fallback_threshold, spread=0.16),
    ]
    uncertainty_score = sum(uncertainty_parts) / len(uncertainty_parts)

    salvage_score = clamp(
        0.36 * record.readiness_score
        + 0.24 * record.quality_score
        + 0.22 * record.final_candidate_score
        + 0.18 * (1.0 - min(1.0, record.total_penalty / max(0.25, review_penalty_threshold + 0.20)))
    )

    card_count = max(1, business_card_counts[record.selected_business_card or ""])
    family_count = max(1, family_counts[record.business_family_id])
    novelty_score = clamp(
        0.60 * min(1.0, 1.0 / math.sqrt(card_count))
        + 0.40 * min(1.0, 1.0 / math.sqrt(family_count))
    )

    disagreement_score = 0.0
    if record.recommended and record.material_status == "gray":
        disagreement_score += 0.45
    if record.needs_review:
        disagreement_score += 0.25
    if not record.recommended and record.readiness_score >= max(0.45, review_readiness_threshold - 0.08):
        disagreement_score += 0.20
    if record.review_status == "missing_review":
        disagreement_score += 0.10
    disagreement_score = clamp(disagreement_score)

    review_age_days = backtest.age_in_days(record.review_created_at)
    age_score = clamp(min(1.0, (review_age_days or 0) / 30.0))

    family_bonus = 0.06 if record.business_family_id in {"sentence_fill", "sentence_order"} else 0.0
    multi_family_bonus = 0.06 if record.business_family_id != record.task_family else 0.0

    priority_score = clamp(
        0.34 * uncertainty_score
        + 0.26 * salvage_score
        + 0.16 * novelty_score
        + 0.16 * disagreement_score
        + 0.08 * age_score
        + family_bonus
        + multi_family_bonus
    )

    reasons: list[str] = []
    if uncertainty_score >= 0.65:
        reasons.append("borderline_threshold_candidate")
    if salvage_score >= 0.55:
        reasons.append("salvageable_quality_candidate")
    if novelty_score >= 0.55:
        reasons.append("coverage_or_card_novelty")
    if disagreement_score >= 0.40:
        reasons.append("model_decision_vs_gray_state_tension")
    if record.business_family_id in {"sentence_fill", "sentence_order"}:
        reasons.append("undercovered_family_bonus")
    if not reasons:
        reasons.append("general_review_value")

    return {
        "record": record,
        "priority_score": priority_score,
        "uncertainty_score": uncertainty_score,
        "salvage_score": salvage_score,
        "novelty_score": novelty_score,
        "disagreement_score": disagreement_score,
        "age_score": age_score,
        "reasons": reasons,
    }


def diversify_candidates(
    *,
    candidates: list[ReviewCandidate],
    top_k: int,
    per_family_cap: int,
    per_card_cap: int,
) -> list[ReviewCandidate]:
    selected: list[ReviewCandidate] = []
    seen_articles: set[str] = set()
    family_counter: Counter[str] = Counter()
    card_counter: Counter[str] = Counter()

    for candidate in candidates:
        if len(selected) >= top_k:
            break
        family = candidate.chosen_family
        card = candidate.selected_business_card or ""
        if family_counter[family] >= per_family_cap:
            continue
        if card and card_counter[card] >= per_card_cap:
            continue
        if candidate.article_id in seen_articles and family_counter[family] >= max(1, per_family_cap // 2):
            continue

        selected.append(candidate)
        seen_articles.add(candidate.article_id)
        family_counter[family] += 1
        if card:
            card_counter[card] += 1

    if len(selected) < top_k:
        selected_ids = {candidate.material_id for candidate in selected}
        for candidate in candidates:
            if len(selected) >= top_k:
                break
            if candidate.material_id in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(candidate.material_id)

    return selected


def candidate_to_dict(candidate: ReviewCandidate) -> dict[str, Any]:
    return {
        "material_id": candidate.material_id,
        "article_id": candidate.article_id,
        "review_status": candidate.review_status,
        "material_status": candidate.material_status,
        "release_channel": candidate.release_channel,
        "family_count": candidate.family_count,
        "chosen_family": candidate.chosen_family,
        "chosen_task_family": candidate.chosen_task_family,
        "priority_score": candidate.priority_score,
        "uncertainty_score": candidate.uncertainty_score,
        "salvage_score": candidate.salvage_score,
        "novelty_score": candidate.novelty_score,
        "disagreement_score": candidate.disagreement_score,
        "age_score": candidate.age_score,
        "quality_score": candidate.quality_score,
        "final_candidate_score": candidate.final_candidate_score,
        "readiness_score": candidate.readiness_score,
        "total_penalty": candidate.total_penalty,
        "recommended": candidate.recommended,
        "needs_review": candidate.needs_review,
        "selected_material_card": candidate.selected_material_card,
        "selected_business_card": candidate.selected_business_card,
        "top_penalties": candidate.top_penalties,
        "top_difficulty_dimensions": candidate.top_difficulty_dimensions,
        "family_breakdown": candidate.family_breakdown,
        "reasons": candidate.reasons,
        "text_preview": candidate.text_preview,
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines: list[str] = [
        "# V2 Active Learning Review Queue",
        "",
        f"- run_at: `{report['run_at']}`",
        f"- db_path: `{report['db_path']}`",
        f"- selected_families: `{', '.join(report['selected_families']) if report['selected_families'] else 'all'}`",
        f"- eligible_record_count: `{summary['eligible_record_count']}`",
        f"- eligible_material_count: `{summary['eligible_material_count']}`",
        f"- selected_candidate_count: `{summary['selected_candidate_count']}`",
        f"- selected_family_distribution: `{json.dumps(summary['selected_family_distribution'], ensure_ascii=False)}`",
        f"- selected_reason_distribution: `{json.dumps(summary['selected_reason_distribution'], ensure_ascii=False)}`",
        "",
        "| rank | material_id | family | priority | uncertainty | salvage | novelty | disagreement | final | readiness | penalty | card |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for item in report["queue"]:
        lines.append(
            "| {rank} | `{material_id}` | {family} | {priority:.4f} | {uncertainty:.4f} | {salvage:.4f} | {novelty:.4f} | {disagreement:.4f} | {final:.4f} | {readiness:.4f} | {penalty:.4f} | `{card}` |".format(
                rank=item["rank"],
                material_id=item["material_id"],
                family=item["chosen_family"],
                priority=item["priority_score"],
                uncertainty=item["uncertainty_score"],
                salvage=item["salvage_score"],
                novelty=item["novelty_score"],
                disagreement=item["disagreement_score"],
                final=item["final_candidate_score"],
                readiness=item["readiness_score"],
                penalty=item["total_penalty"],
                card=item["selected_business_card"] or "",
            )
        )

    lines.extend(["", "## Top Reasons", ""])
    for item in report["queue"][: min(12, len(report["queue"]))]:
        lines.append(
            f"- `{item['material_id']}` {item['chosen_family']} | reasons=`{json.dumps(item['reasons'], ensure_ascii=False)}` | penalties=`{json.dumps(item['top_penalties'], ensure_ascii=False)}` | preview={item['text_preview']}"
        )

    lines.extend(["", "## Notes", ""])
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"v2_active_learning_queue_{timestamp}.json"
    md_path = output_dir / f"v2_active_learning_queue_{timestamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def closeness(value: float, pivot: float, *, spread: float) -> float:
    if spread <= 0:
        return 0.0
    return clamp(1.0 - min(1.0, abs(value - pivot) / spread))


def clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


if __name__ == "__main__":
    raise SystemExit(main())
