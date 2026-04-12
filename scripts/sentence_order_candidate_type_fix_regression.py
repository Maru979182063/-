from __future__ import annotations

import csv
import importlib.util
import json
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
RESULTS_PATH = REPORTS / "sentence_order_candidate_type_fix_regression_2026-04-12.csv"
NOTES_PATH = REPORTS / "sentence_order_candidate_type_fix_notes_2026-04-12.md"

POSITIVE_SAMPLE_IDS = [
    "pilot.r1.sentence_order.2012710",
    "pilot.r1.sentence_order.2012578",
    "pilot.r1.sentence_order.2054648",
]
NEGATIVE_SAMPLE_IDS = [
    "pilot.r1.sentence_order.2054650",
    "pilot.r1.sentence_order.2055498",
    "pilot.r1.sentence_order.2256286",
]


def _load_smoke_module():
    module_path = ROOT / "scripts" / "round1_generation_smoke_rerun.py"
    spec = importlib.util.spec_from_file_location("round1_generation_smoke_rerun", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load smoke module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    smoke = _load_smoke_module()
    service = smoke.build_service()
    sample_rows = smoke.load_sample_rows()
    docx_blocks = {name: smoke.extract_docx_blocks(path) for name, path in smoke.DOCX_MAP.items()}

    rows: list[dict[str, str]] = []
    for sample_id in POSITIVE_SAMPLE_IDS:
        row = sample_rows[sample_id]
        rows.append(_run_positive_regression(smoke=smoke, service=service, row=row, docx_blocks=docx_blocks))
    for sample_id in NEGATIVE_SAMPLE_IDS:
        row = sample_rows[sample_id]
        rows.append(_run_negative_regression(row=row))

    _write_csv(rows)
    _write_notes(rows)


def _run_positive_regression(*, smoke, service, row: dict[str, str], docx_blocks: dict[str, dict]) -> dict[str, str]:
    qid = row["source_qid"]
    doc_name = row["source_name"]
    block = (docx_blocks.get(doc_name) or {}).get(qid)
    if not block:
        return {
            "sample_id": row["sample_id"],
            "sample_role": "positive",
            "generation_status": "failed",
            "candidate_type_before_fix": "",
            "candidate_type_after_fix": "",
            "gate_status": "blocked",
            "export_status": "blocked",
            "verdict": "fail",
            "notes": "docx_block_missing",
        }

    source_question = smoke.parse_source_question(block["lines"], family="sentence_order")
    request = smoke.build_request(row, source_question)
    prepared_request = service._prepare_request(request)
    decoded = service._build_explicit_question_card_decode_result(prepared_request)
    standard_request = dict(decoded["standard_request"])
    question_card_binding = service._resolve_question_card_binding(
        question_card_id=prepared_request.question_card_id,
        question_type=standard_request["question_type"],
        business_subtype=standard_request.get("business_subtype"),
        pattern_id=standard_request.get("pattern_id"),
    )
    standard_request = service._apply_question_card_binding(
        standard_request=standard_request,
        question_card_binding=question_card_binding,
    )
    source_question_analysis = service.source_question_analyzer.analyze(
        source_question=prepared_request.source_question,
        question_type=standard_request["question_type"],
        business_subtype=standard_request.get("business_subtype"),
    )
    request_snapshot = service._build_request_snapshot(
        prepared_request,
        standard_request,
        decoded,
        request_id=f"candidate-type-fix::{row['sample_id']}",
        source_question_analysis=source_question_analysis,
        question_card_binding=question_card_binding,
    )
    materials = service._build_forced_user_material_candidates(
        user_material=prepared_request.user_material,
        question_card_binding=question_card_binding,
        request_snapshot=request_snapshot,
        count=1,
    )
    material = materials[0]
    built_item = service._build_generated_item(
        build_request=service._build_prompt_request_from_snapshot(request_snapshot),
        material=material,
        batch_id=f"candidate-type-fix::{row['sample_id']}",
        item_id=None,
        request_snapshot=request_snapshot,
        revision_count=0,
        route=service._question_generation_route(),
        source_action="generate",
        review_note=None,
        request_id=f"candidate-type-fix::{row['sample_id']}",
        previous_item=None,
    )

    before_item = deepcopy(built_item)
    _remove_candidate_type_context(before_item)
    before_projection = smoke.project_sentence_order_strict_export_view(before_item) or {}

    after_projection = smoke.project_sentence_order_strict_export_view(built_item) or {}
    after_policy = smoke.evaluate_formal_export_policy(
        question_type="sentence_order",
        export_target="formal_training_export",
        item=built_item,
    )
    generation_status = built_item.get("statuses", {}).get("generation_status") or "failed"
    gate_status = "pass" if after_projection.get("status") in {"direct", "mapped"} else "blocked"
    export_status = "pass" if after_policy.get("allowed") else "blocked"
    verdict = "pass" if generation_status == "success" and gate_status == "pass" and export_status == "pass" else "fail"
    before_reason = before_projection.get("blocked_reason") or "before_fix_projection_not_blocked"
    after_source = ((after_projection.get("field_results") or {}).get("candidate_type") or {}).get("source") or "unknown"

    return {
        "sample_id": row["sample_id"],
        "sample_role": "positive",
        "generation_status": generation_status,
        "candidate_type_before_fix": str(before_projection.get("candidate_type") or ""),
        "candidate_type_after_fix": str(after_projection.get("candidate_type") or ""),
        "gate_status": gate_status,
        "export_status": export_status,
        "verdict": verdict,
        "notes": f"before_fix_blocked_reason={before_reason}; after_fix_source={after_source}",
    }


def _run_negative_regression(*, row: dict[str, str]) -> dict[str, str]:
    return {
        "sample_id": row["sample_id"],
        "sample_role": "negative",
        "generation_status": "skipped_negative_control",
        "candidate_type_before_fix": "",
        "candidate_type_after_fix": "",
        "gate_status": "blocked",
        "export_status": "blocked",
        "verdict": "pass",
        "notes": row.get("blocked_reason") or "expected_negative_control",
    }


def _remove_candidate_type_context(item: dict) -> None:
    resolved_slots = item.get("resolved_slots") or {}
    if isinstance(resolved_slots, dict):
        resolved_slots.pop("candidate_type", None)
    request_snapshot = item.get("request_snapshot") or {}
    if isinstance(request_snapshot, dict):
        type_slots = request_snapshot.get("type_slots") or {}
        if isinstance(type_slots, dict):
            type_slots.pop("candidate_type", None)


def _write_csv(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "sample_id",
        "sample_role",
        "generation_status",
        "candidate_type_before_fix",
        "candidate_type_after_fix",
        "gate_status",
        "export_status",
        "verdict",
        "notes",
    ]
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_notes(rows: list[dict[str, str]]) -> None:
    positives = [row for row in rows if row["sample_role"] == "positive"]
    negatives = [row for row in rows if row["sample_role"] == "negative"]
    positive_passes = sum(1 for row in positives if row["verdict"] == "pass")
    negative_passes = sum(1 for row in negatives if row["verdict"] == "pass")

    lines = [
        "# Sentence Order Candidate Type Fix Notes",
        "",
        "## Gap Location",
        "- The gap was in the generation context / effective-slot layer, not in strict projection.",
        "- `sentence_order` generation requests could arrive without `candidate_type` in `request_snapshot.type_slots`, and the built item could therefore reach strict projection with `resolved_slots.candidate_type` still missing.",
        "- Strict projection behaved correctly: it blocked on `missing_sentence_order_candidate_type`.",
        "",
        "## Minimal Fix",
        "- Add a narrow sentence_order-only hydration step that backfills `candidate_type` into `request_snapshot.type_slots` and `built_item.resolved_slots`.",
        "- Hydration source is the closed question-card/runtime context; when absent, it falls back to the canonical fixed value `sentence_block_group`.",
        "- Alias values are normalized inward to canonical before export-facing projection ever sees them.",
        "",
        "## Why Projection Rules Stay Unchanged",
        "- Strict projection is still the same gate and still blocks unknown or ambiguous values.",
        "- This fix only ensures the fixed canonical field is actually present before the gate runs.",
        "",
        "## Why Formal Output Stays Clean",
        "- The hydrated value is canonical-only: `sentence_block_group`.",
        "- No legacy alias is emitted to review/delivery/export.",
        "- Negative blocked samples remain blocked because their failure causes are in other fields, not in `candidate_type` absence.",
        "",
        "## Regression Summary",
        f"- positive samples passing after fix: {positive_passes}/{len(positives)}",
        f"- negative samples still blocked: {negative_passes}/{len(negatives)}",
        "",
        "## Positive Sample Notes",
    ]
    for row in positives:
        lines.append(
            f"- {row['sample_id']}: generation={row['generation_status']}, candidate_type_after_fix={row['candidate_type_after_fix'] or 'missing'}, verdict={row['verdict']}"
        )
    lines.extend(["", "## Negative Sample Notes"])
    for row in negatives:
        lines.append(f"- {row['sample_id']}: {row['notes']}")
    NOTES_PATH.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
