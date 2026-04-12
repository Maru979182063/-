from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(r"C:\Users\Maru\Documents\agent")
REPORTS = ROOT / "reports"

GOLD_PATH = REPORTS / "pilot_round1_gold_ready_pool_2026-04-12.csv"
HOLDOUT_PATH = REPORTS / "pilot_round1_review_holdout_pool_2026-04-12.csv"
ERROR_PATH = REPORTS / "pilot_round1_error_pool_final_2026-04-12.csv"
EXPORT_PATH = REPORTS / "pilot_round1_export_eligible_pool_2026-04-12.csv"
CLOSURE_PATH = REPORTS / "pilot_round1_asset_closure_2026-04-12.md"

SF_OUT = REPORTS / "round1_fewshot_sentence_fill_candidates_2026-04-12.csv"
CU_OUT = REPORTS / "round1_fewshot_center_understanding_candidates_2026-04-12.csv"
SO_OUT = REPORTS / "round1_fewshot_sentence_order_candidates_2026-04-12.csv"
CARD_NOTES_OUT = REPORTS / "round1_card_refinement_notes_2026-04-12.md"
BOUNDARY_OUT = REPORTS / "round1_boundary_case_pack_2026-04-12.csv"
NEGATIVE_OUT = REPORTS / "round1_negative_case_pack_2026-04-12.csv"
BASELINE_OUT = REPORTS / "round1_regression_baseline_contract_2026-04-12.md"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def get_rows() -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    return load_csv(GOLD_PATH), load_csv(HOLDOUT_PATH), load_csv(ERROR_PATH), load_csv(EXPORT_PATH)


def first_n(rows: list[dict[str, str]], n: int) -> list[dict[str, str]]:
    return sorted(rows, key=lambda r: r["sample_id"])[:n]


def build_sentence_fill(gold_rows: list[dict[str, str]]) -> int:
    sf = [r for r in gold_rows if r["business_family_id"] == "sentence_fill"]
    combo_order = [
        ("middle", "carry_previous", "explanation", 3, "core_available.middle_carry_previous_explanation"),
        ("middle", "bridge", "continuation", 3, "core_available.middle_bridge_continuation"),
        ("middle", "bridge", "transition", 2, "core_available.middle_bridge_transition"),
        ("middle", "lead_next", "transition", 2, "core_available.middle_lead_next_transition"),
        ("ending", "countermeasure", "action", 2, "core_available.ending_countermeasure_action"),
    ]
    selected: list[dict[str, str]] = []
    used: set[str] = set()
    by_combo: defaultdict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in sf:
        by_combo[(row["blank_position"], row["function_type"], row["logic_relation"])].append(row)
    for combo, want, tag in [(c[:3], c[3], c[4]) for c in combo_order]:
        rows = first_n(by_combo[combo], want)
        for row in rows:
            if row["sample_id"] in used:
                continue
            used.add(row["sample_id"])
            selected.append(
                {
                    "sample_id": row["sample_id"],
                    "business_family_id": row["business_family_id"],
                    "question_card_id": row["question_card_id"],
                    "blank_position": row["blank_position"],
                    "function_type": row["function_type"],
                    "logic_relation": row["logic_relation"],
                    "fewshot_use_reason": "stable_gold_ready_sentence_fill_pattern",
                    "coverage_tag": tag,
                    "priority_rank": "",
                }
            )
    # annotate priority rank
    for idx, row in enumerate(selected, 1):
        row["priority_rank"] = str(idx)
    write_csv(
        SF_OUT,
        selected,
        [
            "sample_id",
            "business_family_id",
            "question_card_id",
            "blank_position",
            "function_type",
            "logic_relation",
            "fewshot_use_reason",
            "coverage_tag",
            "priority_rank",
        ],
    )
    return len(selected)


def build_center_understanding(gold_rows: list[dict[str, str]]) -> int:
    cu = [
        r
        for r in gold_rows
        if r["business_family_id"] == "center_understanding"
        and r["sample_id"] != "pilot.r1.center_understanding.2052336"
    ]
    for row in cu:
        row["_axis"] = row.get("annotation_main_axis_source") or row.get("main_axis_source", "")
        row["_arg"] = row.get("annotation_argument_structure") or row.get("argument_structure", "")
    combo_plan = [
        ("global_abstraction", "total_sub", 3, "core_target.global_abstraction_total_sub"),
        ("final_summary", "sub_total", 3, "core_target.final_summary_sub_total"),
        ("transition_after", "total_sub", 2, "core_target.transition_after_total_sub"),
        ("global_abstraction", "parallel", 2, "core_target.global_abstraction_parallel"),
        ("solution_conclusion", "problem_solution", 2, "core_target.solution_conclusion_problem_solution"),
    ]
    by_combo: defaultdict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in cu:
        by_combo[(row["_axis"], row["_arg"])].append(row)
    selected: list[dict[str, str]] = []
    used: set[str] = set()
    for axis, arg, want, tag in combo_plan:
        rows = first_n(by_combo[(axis, arg)], want)
        for row in rows:
            if row["sample_id"] in used:
                continue
            used.add(row["sample_id"])
            selected.append(
                {
                    "sample_id": row["sample_id"],
                    "business_family_id": row["business_family_id"],
                    "question_card_id": row["question_card_id"],
                    "main_axis_source": row["_axis"],
                    "argument_structure": row["_arg"],
                    "fewshot_use_reason": "stable_gold_ready_center_understanding_pattern",
                    "coverage_tag": tag,
                    "priority_rank": "",
                }
            )
    for idx, row in enumerate(selected, 1):
        row["priority_rank"] = str(idx)
    write_csv(
        CU_OUT,
        selected,
        [
            "sample_id",
            "business_family_id",
            "question_card_id",
            "main_axis_source",
            "argument_structure",
            "fewshot_use_reason",
            "coverage_tag",
            "priority_rank",
        ],
    )
    return len(selected)


def build_sentence_order(export_rows: list[dict[str, str]]) -> int:
    so = [r for r in export_rows if r["business_family_id"] == "sentence_order"]
    combo_plan = [
        ("sentence_block_group", "explicit_topic", "none", 5, "available.explicit_topic_none"),
        ("sentence_block_group", "explicit_topic", "call_to_action", 3, "available.explicit_topic_call_to_action"),
        ("sentence_block_group", "weak_opening", "none", 2, "available.weak_opening_none"),
    ]
    by_combo: defaultdict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in so:
        by_combo[(row["candidate_type"], row["opening_anchor_type"], row["closing_anchor_type"])].append(row)
    selected: list[dict[str, str]] = []
    used: set[str] = set()
    for candidate_type, opening, closing, want, tag in combo_plan:
        rows = first_n(by_combo[(candidate_type, opening, closing)], want)
        for row in rows:
            if row["sample_id"] in used:
                continue
            used.add(row["sample_id"])
            selected.append(
                {
                    "sample_id": row["sample_id"],
                    "business_family_id": row["business_family_id"],
                    "question_card_id": row["question_card_id"],
                    "candidate_type": row["candidate_type"],
                    "opening_anchor_type": row["opening_anchor_type"],
                    "closing_anchor_type": row["closing_anchor_type"],
                    "fewshot_use_reason": "strict_projection_passed_export_eligible_order_pattern",
                    "coverage_tag": tag,
                    "priority_rank": "",
                }
            )
    for idx, row in enumerate(selected, 1):
        row["priority_rank"] = str(idx)
    write_csv(
        SO_OUT,
        selected,
        [
            "sample_id",
            "business_family_id",
            "question_card_id",
            "candidate_type",
            "opening_anchor_type",
            "closing_anchor_type",
            "fewshot_use_reason",
            "coverage_tag",
            "priority_rank",
        ],
    )
    return len(selected)


def build_boundary_and_negative(holdout_rows: list[dict[str, str]], error_rows: list[dict[str, str]]) -> tuple[int, int]:
    boundary_rows = []
    for row in holdout_rows:
        boundary_rows.append(
            {
                "sample_id": row["sample_id"],
                "business_family_id": row["business_family_id"],
                "boundary_type": "middle_focus_in_fen_zong_fen",
                "why_boundary": "valid_center_understanding_item_but_main_focus_sits_on_middle_claim_and_current_canonical_axis_compresses_it",
                "admission_rule_hint": "if_center_understanding_focus_is_middle_claim_in_fen_zong_fen_route_to_review_holdout_before_gold_ready",
                "recommended_pool": "review_holdout",
                "notes": row["notes"],
            }
        )
    write_csv(
        BOUNDARY_OUT,
        boundary_rows,
        [
            "sample_id",
            "business_family_id",
            "boundary_type",
            "why_boundary",
            "admission_rule_hint",
            "recommended_pool",
            "notes",
        ],
    )

    negative_rows = []
    for row in error_rows:
        sid = row["sample_id"]
        if sid == "pilot.r1.center_understanding.2052316":
            negative_rows.append(
                {
                    "sample_id": sid,
                    "business_family_id": row["business_family_id"],
                    "negative_type": "routing_error_misrouted_pool_sample",
                    "blocked_reason": row.get("blocked_reason", "") or row.get("notes", ""),
                    "future_use": "round2_admission_negative_control_for_family_routing",
                    "regression_check_target": "routing_regression",
                    "notes": "should_never_reenter_center_understanding_formal_pool",
                }
            )
        else:
            negative_rows.append(
                {
                    "sample_id": sid,
                    "business_family_id": row["business_family_id"],
                    "negative_type": "ambiguous_projection_blocked_sample",
                    "blocked_reason": row.get("blocked_reason", "") or row.get("notes", ""),
                    "future_use": "projection_and_export_gate_negative_control",
                    "regression_check_target": "gate_regression_and_export_regression",
                    "notes": "keep_blocked_until_closing_anchor_projection_becomes_precise",
                }
            )
    write_csv(
        NEGATIVE_OUT,
        negative_rows,
        [
            "sample_id",
            "business_family_id",
            "negative_type",
            "blocked_reason",
            "future_use",
            "regression_check_target",
            "notes",
        ],
    )
    return len(boundary_rows), len(negative_rows)


def build_card_notes() -> None:
    content = """# Round 1 Card Refinement Notes (2026-04-12)

## sentence_fill
- Need to keep `logic_relation` stable, especially the split between `bridge+continuation` and `bridge+transition`.
- This is mainly a few-shot coverage problem, not a card schema failure.
- Next cut: add missing `opening` and `inserted` high-quality cases before changing card semantics.

## center_understanding
- The pressure points are still `main_axis_source` and `argument_structure`, especially around middle-focus items and compressed fen-zong-fen cases.
- `pilot.r1.center_understanding.2052336` is a boundary sample problem, not a routing failure: it should stay outside gold-ready until we decide whether the card needs an explicit middle-focus-compatible semantic hook.
- Next cut: first add a small boundary-rule layer and a dedicated boundary few-shot pack; only if that still fails should we revise card semantics.

## sentence_order
- The fields worth continuing to stabilize are `opening_anchor_type` and `closing_anchor_type`, especially because Round 1 export-eligible cases are concentrated in a narrow subset.
- The current missing combinations are coverage gaps rather than immediate card-contract bugs.
- Next cut: treat `summary` and `upper_context_link/problem_opening/viewpoint_opening` as boundary spot-check targets; do not change projection rules until new clean samples arrive.
"""
    CARD_NOTES_OUT.write_text(content, encoding="utf-8")


def build_baseline_contract() -> None:
    content = """# Round 1 Regression Baseline Contract (2026-04-12)

## Fixed Round 1 Baseline Files
- `reports/pilot_round1_gold_ready_pool_2026-04-12.csv`
- `reports/pilot_round1_review_holdout_pool_2026-04-12.csv`
- `reports/pilot_round1_error_pool_final_2026-04-12.csv`
- `reports/pilot_round1_export_eligible_pool_2026-04-12.csv`
- `reports/pilot_round1_asset_closure_2026-04-12.md`
- `reports/round1_fewshot_sentence_fill_candidates_2026-04-12.csv`
- `reports/round1_fewshot_center_understanding_candidates_2026-04-12.csv`
- `reports/round1_fewshot_sentence_order_candidates_2026-04-12.csv`
- `reports/round1_boundary_case_pack_2026-04-12.csv`
- `reports/round1_negative_case_pack_2026-04-12.csv`

## Do Not Overwrite
- Do not overwrite Round 1 sample membership.
- Do not overwrite Round 1 layer decisions for `gold-ready`, `review_holdout`, `error_pool`, or `export_eligible`.
- Do not remove `pilot.r1.center_understanding.2052336` from boundary control.
- Do not remove `pilot.r1.center_understanding.2052316` from routing negative control.

## gate_regression
- Run against `pilot_round1_gold_ready_pool_2026-04-12.csv`, `pilot_round1_error_pool_final_2026-04-12.csv`, and `pilot_round1_export_eligible_pool_2026-04-12.csv`.
- Any change to gate logic must preserve current pass/blocked outcomes for Round 1 unless an intentional migration is separately approved.

## routing_regression
- Run against `round1_negative_case_pack_2026-04-12.csv`.
- `pilot.r1.center_understanding.2052316` must stay blocked from center_understanding formal admission.

## fewshot_asset_regression
- Run against the three Round 1 few-shot candidate packs.
- Prompt or retrieval changes must not silently swap out coverage-defining examples without an explicit asset refresh decision.

## card_contract_regression
- Run against `pilot_round1_gold_ready_pool_2026-04-12.csv`, `pilot_round1_review_holdout_pool_2026-04-12.csv`, and `round1_boundary_case_pack_2026-04-12.csv`.
- Card changes must preserve canonical field names and must not force `2052336` into gold-ready unless the boundary rationale is explicitly revised.

## Change Trigger Rule
- Before changing prompt, card, gate, or mapping, run: gate regression, routing regression, few-shot asset regression, and card contract regression on the full Round 1 baseline.
"""
    BASELINE_OUT.write_text(content, encoding="utf-8")


def main() -> None:
    gold_rows, holdout_rows, error_rows, export_rows = get_rows()
    build_sentence_fill(gold_rows)
    build_center_understanding(gold_rows)
    build_sentence_order(export_rows)
    build_card_notes()
    build_boundary_and_negative(holdout_rows, error_rows)
    build_baseline_contract()


if __name__ == "__main__":
    main()
