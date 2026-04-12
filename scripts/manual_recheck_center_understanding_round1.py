from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(r"C:\Users\Maru\Documents\agent")
REPORTS = ROOT / "reports"
BATCH_DIR = REPORTS / "pilot_round1_annotation_batches_2026-04-12"
MASTER_PATH = REPORTS / "pilot_round1_annotation_execution_master_2026-04-12.csv"
BATCH_MANIFEST_PATH = REPORTS / "pilot_round1_annotation_batch_manifest_2026-04-12.csv"
FOCUS_PATH = REPORTS / "pilot_round1_center_understanding_review_focus_2026-04-12.csv"
SUMMARY_PATH = REPORTS / "pilot_round1_annotation_fill_summary_2026-04-12.md"
SECOND_PASS_REPORT_PATH = REPORTS / "pilot_round1_center_understanding_manual_recheck_2026-04-12.md"


DECISIONS = {
    "pilot.r1.center_understanding.2012702": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:fenzong_tail_summary",
    },
    "pilot.r1.center_understanding.2012562": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:parallel_time_progression",
    },
    "pilot.r1.center_understanding.2012586": {
        "annotation_main_axis_source": "solution_conclusion",
        "annotation_argument_structure": "problem_solution",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:visual_media_countermeasure_focus",
    },
    "pilot.r1.center_understanding.2052316": {
        "annotation_main_axis_source": "",
        "annotation_argument_structure": "",
        "review_status": "error-pool",
        "notes": "misrouted_non_center_understanding_sentence_fill_item",
    },
    "pilot.r1.center_understanding.2052326": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_responsibility_conclusion",
    },
    "pilot.r1.center_understanding.2052328": {
        "annotation_main_axis_source": "transition_after",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:turn_after_ant_sociality_focus",
    },
    "pilot.r1.center_understanding.2052330": {
        "annotation_main_axis_source": "solution_conclusion",
        "annotation_argument_structure": "problem_solution",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:problem_harm_solution",
    },
    "pilot.r1.center_understanding.2052334": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_claim_then_explanation",
    },
    "pilot.r1.center_understanding.2052336": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "review-needed",
        "notes": "middle_focus_in_fen_zong_fen_boundary",
    },
    "pilot.r1.center_understanding.2054038": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_summary_responsibility",
    },
    "pilot.r1.center_understanding.2054040": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:two_unfit_wage_modes_parallel",
    },
    "pilot.r1.center_understanding.2054046": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:mechanism_innovation_opening_focus",
    },
    "pilot.r1.center_understanding.2054654": {
        "annotation_main_axis_source": "transition_after",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:not_but_transition_focus",
    },
    "pilot.r1.center_understanding.2054656": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:two_difficulties_parallel",
    },
    "pilot.r1.center_understanding.2055172": {
        "annotation_main_axis_source": "transition_after",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:second_sentence_market_opening_focus",
    },
    "pilot.r1.center_understanding.2171688": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:constellation_development_parallel",
    },
    "pilot.r1.center_understanding.2056112": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_summary_on_multi_discipline_insight",
    },
    "pilot.r1.center_understanding.2056114": {
        "annotation_main_axis_source": "transition_after",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:turn_after_study_result_focus",
    },
    "pilot.r1.center_understanding.2056932": {
        "annotation_main_axis_source": "solution_conclusion",
        "annotation_argument_structure": "problem_solution",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:problem_example_solution",
    },
    "pilot.r1.center_understanding.2056834": {
        "annotation_main_axis_source": "transition_after",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:influencer_marketing_reason_focus",
    },
    "pilot.r1.center_understanding.2056848": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_claim_plus_example",
    },
    "pilot.r1.center_understanding.2056886": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_summary_on_riddle_origin",
    },
    "pilot.r1.center_understanding.2057220": {
        "annotation_main_axis_source": "solution_conclusion",
        "annotation_argument_structure": "problem_solution",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:problem_solution_with_benefit",
    },
    "pilot.r1.center_understanding.2057222": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_claim_then_example",
    },
    "pilot.r1.center_understanding.2057238": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_focus_on_content_choice",
    },
    "pilot.r1.center_understanding.2204111": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:dual_sources_parallel",
    },
    "pilot.r1.center_understanding.2204096": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_claim_then_timeline",
    },
    "pilot.r1.center_understanding.2204106": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "parallel",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:value_and_evaluation_parallel",
    },
    "pilot.r1.center_understanding.2204128": {
        "annotation_main_axis_source": "final_summary",
        "annotation_argument_structure": "sub_total",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:tail_focus_on_double_evidence_method",
    },
    "pilot.r1.center_understanding.2204709": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_definition_then_timeline",
    },
    "pilot.r1.center_understanding.2256130": {
        "annotation_main_axis_source": "global_abstraction",
        "annotation_argument_structure": "total_sub",
        "review_status": "gold-ready",
        "notes": "second_pass_confirmed:opening_claim_then_explanation",
    },
}


def load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, reader.fieldnames or []


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def apply_decision(row: dict[str, str]) -> None:
    decision = DECISIONS.get(row["sample_id"])
    if not decision:
        return

    row["annotation_main_axis_source"] = decision["annotation_main_axis_source"]
    row["annotation_argument_structure"] = decision["annotation_argument_structure"]
    row["review_status"] = decision["review_status"]
    row["notes"] = decision["notes"]
    row["annotator_notes"] = decision["notes"]

    if decision["review_status"] == "gold-ready":
        row["consistency_check_status"] = "passed"
        row["layer_after_annotation"] = "gold-ready"
        row["annotation_pool_route"] = "formal_annotation_candidate"
        row["consistency_notes"] = ""
        if not row.get("export_eligibility_notes"):
            row["export_eligibility_notes"] = ""
    elif decision["review_status"] == "review-needed":
        row["consistency_check_status"] = "needs_review"
        row["layer_after_annotation"] = "review-needed"
        row["annotation_pool_route"] = "formal_annotation_candidate"
        row["consistency_notes"] = decision["notes"]
    else:
        row["consistency_check_status"] = "failed"
        row["completeness_check_status"] = "failed"
        row["layer_after_annotation"] = "error-pool"
        row["annotation_pool_route"] = "error_pool"
        row["consistency_notes"] = decision["notes"]
        row["export_eligibility_notes"] = "removed_from_formal_pool_after_manual_recheck"


def recompute_batch_manifest() -> None:
    rows, fieldnames = load_csv(BATCH_MANIFEST_PATH)
    stats: dict[str, Counter] = defaultdict(Counter)

    for batch_file in sorted(BATCH_DIR.glob("pilot_r1_annot_*.csv")):
        batch_rows, _ = load_csv(batch_file)
        batch_id = batch_rows[0]["annotation_batch_id"] if batch_rows else batch_file.stem.replace("_", ".")
        counter = Counter(r["review_status"] for r in batch_rows)
        stats[batch_id]["gold-ready"] = counter.get("gold-ready", 0)
        stats[batch_id]["review-needed"] = counter.get("review-needed", 0)
        stats[batch_id]["error-pool"] = counter.get("error-pool", 0)

    for row in rows:
        batch_id = row["batch_id"]
        row["gold_ready_count"] = str(stats[batch_id]["gold-ready"])
        row["review_needed_count"] = str(stats[batch_id]["review-needed"])
        row["error_pool_count"] = str(stats[batch_id]["error-pool"])

    write_csv(BATCH_MANIFEST_PATH, rows, fieldnames)


def update_focus_file() -> tuple[int, int, int]:
    rows, fieldnames = load_csv(FOCUS_PATH)
    kept: list[dict[str, str]] = []
    promoted = 0
    for row in rows:
        decision = DECISIONS.get(row["sample_id"])
        if not decision:
            kept.append(row)
            continue
        row["annotation_main_axis_source"] = decision["annotation_main_axis_source"]
        row["annotation_argument_structure"] = decision["annotation_argument_structure"]
        row["review_status"] = decision["review_status"]
        row["focus_reason"] = decision["notes"]
        if decision["review_status"] == "gold-ready":
            promoted += 1
            continue
        row["recommended_action"] = (
            "move_to_error_pool" if decision["review_status"] == "error-pool" else "keep_in_second_pass_review"
        )
        kept.append(row)
    write_csv(FOCUS_PATH, kept, fieldnames)
    remaining_review = sum(1 for r in kept if r["review_status"] == "review-needed")
    remaining_error = sum(1 for r in kept if r["review_status"] == "error-pool")
    return promoted, remaining_review, remaining_error


def write_summary(master_rows: list[dict[str, str]]) -> None:
    overall = Counter(r["review_status"] for r in master_rows if r["gate_status"] == "pass")
    by_family: dict[str, Counter] = defaultdict(Counter)
    so_export_true = 0
    for row in master_rows:
        if row["gate_status"] != "pass":
            continue
        fam = row["business_family_id"]
        by_family[fam][row["review_status"]] += 1
        by_family[fam]["total"] += 1
        if fam == "sentence_order" and row.get("formal_export_eligible") == "true":
            so_export_true += 1

    summary = f"""# Pilot Round 1 Annotation Fill Summary (2026-04-12)

## Overall
- Total processed samples: {sum(counter['total'] for counter in by_family.values())}
- gold-ready: {overall.get('gold-ready', 0)}
- review-needed: {overall.get('review-needed', 0)}
- error-pool: {overall.get('error-pool', 0)}

## By Family
### sentence_fill
- Total: {by_family['sentence_fill']['total']}
- gold-ready: {by_family['sentence_fill'].get('gold-ready', 0)}
- review-needed: {by_family['sentence_fill'].get('review-needed', 0)}
- error-pool: {by_family['sentence_fill'].get('error-pool', 0)}
- Most divisive field: logic_relation
- Suggested for annotation-post replay check: yes

### center_understanding
- Total: {by_family['center_understanding']['total']}
- gold-ready: {by_family['center_understanding'].get('gold-ready', 0)}
- review-needed: {by_family['center_understanding'].get('review-needed', 0)}
- error-pool: {by_family['center_understanding'].get('error-pool', 0)}
- Most divisive field: main_axis_source / argument_structure
- Suggested for annotation-post replay check: yes

### sentence_order
- Total: {by_family['sentence_order']['total']}
- gold-ready: {by_family['sentence_order'].get('gold-ready', 0)}
- review-needed: {by_family['sentence_order'].get('review-needed', 0)}
- error-pool: {by_family['sentence_order'].get('error-pool', 0)}
- Most divisive field: opening_anchor_type / closing_anchor_type
- Suggested for annotation-post replay check: yes
- formal_export_eligible = true: {so_export_true}
- downgraded to review-needed during annotation: 0
- newly moved to error-pool: 0
"""
    SUMMARY_PATH.write_text(summary, encoding="utf-8")


def write_second_pass_report(
    promoted: int,
    remaining_review: int,
    remaining_error: int,
    master_rows: list[dict[str, str]],
) -> None:
    center_rows = [r for r in master_rows if r["business_family_id"] == "center_understanding" and r["gate_status"] == "pass"]
    counter = Counter(r["review_status"] for r in center_rows)
    review_samples = [r["sample_id"] for r in center_rows if r["review_status"] == "review-needed"]
    error_samples = [r["sample_id"] for r in center_rows if r["review_status"] == "error-pool"]
    report = "\n".join(
        [
            "# Center Understanding Manual Recheck Summary (2026-04-12)",
            "",
            "## Result",
            f"- Promoted from review-needed to gold-ready: {promoted}",
            f"- Remaining review-needed: {remaining_review}",
            f"- Newly moved to error-pool: {remaining_error}",
            "",
            "## Current Center Understanding Distribution",
            f"- Total: {len(center_rows)}",
            f"- gold-ready: {counter.get('gold-ready', 0)}",
            f"- review-needed: {counter.get('review-needed', 0)}",
            f"- error-pool: {counter.get('error-pool', 0)}",
            "",
            "## Remaining Review-needed Samples",
            *[f"- {sample_id}" for sample_id in review_samples],
            "",
            "## Error-pool Samples",
            *[f"- {sample_id}" for sample_id in error_samples],
        ]
    )
    SECOND_PASS_REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    master_rows, master_fields = load_csv(MASTER_PATH)
    for row in master_rows:
        apply_decision(row)
    write_csv(MASTER_PATH, master_rows, master_fields)

    for batch_file in sorted(BATCH_DIR.glob("pilot_r1_annot_cu_*.csv")):
        rows, fields = load_csv(batch_file)
        changed = False
        for row in rows:
            if row["sample_id"] in DECISIONS:
                apply_decision(row)
                changed = True
        if changed:
            write_csv(batch_file, rows, fields)

    recompute_batch_manifest()
    promoted, remaining_review, remaining_error = update_focus_file()
    write_summary(master_rows)
    write_second_pass_report(promoted, remaining_review, remaining_error, master_rows)


if __name__ == "__main__":
    main()
