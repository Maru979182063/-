from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(r"C:\Users\Maru\Documents\agent")
REPORTS = ROOT / "reports"
BATCH_DIR = REPORTS / "pilot_round1_annotation_batches_2026-04-12"
MASTER_PATH = REPORTS / "pilot_round1_annotation_execution_master_2026-04-12.csv"
FOCUS_PATH = REPORTS / "pilot_round1_center_understanding_review_focus_2026-04-12.csv"
ERROR_LOCKED_PATH = REPORTS / "pilot_round1_error_pool_locked_2026-04-12.csv"

GOLD_READY_PATH = REPORTS / "pilot_round1_gold_ready_pool_2026-04-12.csv"
REVIEW_HOLDOUT_PATH = REPORTS / "pilot_round1_review_holdout_pool_2026-04-12.csv"
ERROR_POOL_FINAL_PATH = REPORTS / "pilot_round1_error_pool_final_2026-04-12.csv"
EXPORT_ELIGIBLE_PATH = REPORTS / "pilot_round1_export_eligible_pool_2026-04-12.csv"
CLOSURE_REPORT_PATH = REPORTS / "pilot_round1_asset_closure_2026-04-12.md"

HOLDOUT_SAMPLE_ID = "pilot.r1.center_understanding.2052336"
ROUTING_ERROR_SAMPLE_ID = "pilot.r1.center_understanding.2052316"


def load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames or []


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def update_master_and_batches() -> list[dict[str, str]]:
    rows, fields = load_csv(MASTER_PATH)
    for row in rows:
        if row["sample_id"] == HOLDOUT_SAMPLE_ID:
            row["review_status"] = "review-needed"
            row["notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
            row["annotator_notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
            row["consistency_notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
            row["annotation_pool_route"] = "review_holdout"
            row["layer_after_annotation"] = "review-needed"
            row["consistency_check_status"] = "needs_review"
        elif row["sample_id"] == ROUTING_ERROR_SAMPLE_ID:
            row["review_status"] = "error-pool"
            row["notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
            row["annotator_notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
            row["consistency_notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
            row["annotation_pool_route"] = "error_pool"
            row["layer_after_annotation"] = "error-pool"
            row["completeness_check_status"] = "failed"
            row["consistency_check_status"] = "failed"
            row["export_eligibility_notes"] = (
                "round2_negative_control:route_block_non_center_understanding_sentence_fill_item"
            )
    write_csv(MASTER_PATH, rows, fields)

    for batch_file in sorted(BATCH_DIR.glob("pilot_r1_annot_cu_*.csv")):
        batch_rows, batch_fields = load_csv(batch_file)
        changed = False
        for row in batch_rows:
            if row["sample_id"] == HOLDOUT_SAMPLE_ID:
                row["review_status"] = "review-needed"
                row["notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
                row["annotator_notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
                row["consistency_notes"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
                row["annotation_pool_route"] = "review_holdout"
                row["layer_after_annotation"] = "review-needed"
                row["consistency_check_status"] = "needs_review"
                changed = True
            elif row["sample_id"] == ROUTING_ERROR_SAMPLE_ID:
                row["review_status"] = "error-pool"
                row["notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
                row["annotator_notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
                row["consistency_notes"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
                row["annotation_pool_route"] = "error_pool"
                row["layer_after_annotation"] = "error-pool"
                row["completeness_check_status"] = "failed"
                row["consistency_check_status"] = "failed"
                row["export_eligibility_notes"] = (
                    "round2_negative_control:route_block_non_center_understanding_sentence_fill_item"
                )
                changed = True
        if changed:
            write_csv(batch_file, batch_rows, batch_fields)

    focus_rows, focus_fields = load_csv(FOCUS_PATH)
    for row in focus_rows:
        if row["sample_id"] == HOLDOUT_SAMPLE_ID:
            row["review_status"] = "review-needed"
            row["focus_reason"] = "round1_review_holdout:middle_focus_in_fen_zong_fen_boundary"
            row["recommended_action"] = "keep_in_review_holdout"
        elif row["sample_id"] == ROUTING_ERROR_SAMPLE_ID:
            row["review_status"] = "error-pool"
            row["focus_reason"] = "routing_error_misrouted_non_center_understanding_sentence_fill_item"
            row["recommended_action"] = "use_as_round2_route_block_negative_control"
    write_csv(FOCUS_PATH, focus_rows, focus_fields)
    return rows


def build_pools(master_rows: list[dict[str, str]]) -> None:
    gold_rows = [r for r in master_rows if r["gate_status"] == "pass" and r["review_status"] == "gold-ready"]
    holdout_rows = [r for r in master_rows if r["sample_id"] == HOLDOUT_SAMPLE_ID]
    error_rows = [r for r in master_rows if r["review_status"] == "error-pool"]
    export_rows = [
        r
        for r in master_rows
        if r["review_status"] == "gold-ready" and r.get("formal_export_eligible") == "true"
    ]

    master_fields = list(master_rows[0].keys())
    write_csv(GOLD_READY_PATH, gold_rows, master_fields)
    write_csv(REVIEW_HOLDOUT_PATH, holdout_rows, master_fields)

    locked_rows, locked_fields = load_csv(ERROR_LOCKED_PATH)
    final_error_fields = list(dict.fromkeys(list(master_fields) + locked_fields + ["error_pool_source"]))
    combined_errors: list[dict[str, str]] = []
    for row in locked_rows:
        merged = {field: "" for field in final_error_fields}
        merged.update(row)
        merged["error_pool_source"] = "gate_blocked"
        combined_errors.append(merged)
    for row in error_rows:
        merged = {field: "" for field in final_error_fields}
        merged.update(row)
        merged["error_pool_source"] = "annotation_manual_recheck"
        combined_errors.append(merged)
    write_csv(ERROR_POOL_FINAL_PATH, combined_errors, final_error_fields)
    write_csv(EXPORT_ELIGIBLE_PATH, export_rows, master_fields)


def write_closure_report(master_rows: list[dict[str, str]]) -> None:
    pass_rows = [r for r in master_rows if r["gate_status"] == "pass"]
    gold_rows = [r for r in pass_rows if r["review_status"] == "gold-ready"]
    holdout_rows = [r for r in pass_rows if r["sample_id"] == HOLDOUT_SAMPLE_ID]
    error_rows = [r for r in master_rows if r["review_status"] == "error-pool"]
    export_rows = [r for r in gold_rows if r.get("formal_export_eligible") == "true"]

    by_family: dict[str, Counter] = defaultdict(Counter)
    for row in pass_rows:
        fam = row["business_family_id"]
        by_family[fam]["total"] += 1
        by_family[fam][row["review_status"]] += 1
        if row.get("formal_export_eligible") == "true":
            by_family[fam]["export_eligible"] += 1

    report = "\n".join(
        [
            "# Round 1 Asset Closure Report (2026-04-12)",
            "",
            "## Final Counts",
            f"- gold-ready total: {len(gold_rows)}",
            f"- review_holdout total: {len(holdout_rows)}",
            f"- error_pool total: {len(error_rows) + 3}",
            f"- export_eligible total: {len(export_rows)}",
            "",
            "## Family Status",
            f"- sentence_fill: total {by_family['sentence_fill']['total']}, gold-ready {by_family['sentence_fill']['gold-ready']}, review_holdout 0, error_pool 0, export_eligible 0",
            f"- center_understanding: total {by_family['center_understanding']['total']}, gold-ready {by_family['center_understanding']['gold-ready']}, review_holdout {1 if holdout_rows else 0}, error_pool {by_family['center_understanding']['error-pool']}, export_eligible 0",
            f"- sentence_order: total {by_family['sentence_order']['total']}, gold-ready {by_family['sentence_order']['gold-ready']}, review_holdout 0, error_pool 3, export_eligible {by_family['sentence_order']['export_eligible']}",
            "",
            "## Single-item Ruling",
            f"- {HOLDOUT_SAMPLE_ID}: keep in review_holdout",
            "  reason: the sample is a valid center_understanding item, but its focus lands on a middle-position claim in a fen-zong-fen structure; current canonical main_axis_source / argument_structure can represent it only by compression, so it should stay as a boundary holdout instead of being forced into gold-ready.",
            "",
            "## Routing Error Control",
            f"- {ROUTING_ERROR_SAMPLE_ID}: mark as routing_error / misrouted_pool_sample",
            "  use in round 2: keep it as a negative-control admission case. If a future center_understanding intake pipeline lets this sample pass into the formal pool again, that is a route-block failure and should trigger admission repair before expansion.",
            "",
            "## Asset Routing",
            f"- Direct to standard asset / few-shot consolidation: {len(gold_rows)} gold-ready samples plus {len(export_rows)} export_eligible sentence_order samples as the formal-export subset.",
            f"- Keep only in boundary sample pool: {HOLDOUT_SAMPLE_ID}",
            f"- Keep only in error pool: {ROUTING_ERROR_SAMPLE_ID}, pilot.r1.sentence_order.2054650, pilot.r1.sentence_order.2055498, pilot.r1.sentence_order.2256286",
            "",
            "## Restrictions",
            "- Do not use review_holdout or error_pool in formal export.",
            "- Do not enter training or self-learning from this round closure.",
        ]
    )
    CLOSURE_REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    master_rows = update_master_and_batches()
    build_pools(master_rows)
    write_closure_report(master_rows)


if __name__ == "__main__":
    main()
