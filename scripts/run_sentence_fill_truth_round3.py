from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
PASSAGE_ENV = ROOT / "passage_service" / ".env"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.round1_generation_smoke_rerun import (  # noqa: E402
    build_request,
    build_service,
    extract_docx_blocks,
    load_sample_rows,
    parse_source_question,
)
from scripts.round1_material_to_question_regression import (  # noqa: E402
    DOCX_MAP,
    _attach_selected_material_identity,
    _load_llm_env,
    _material_lookup,
    _override_user_material,
)


DATE_TAG = "2026-04-14"
RESULTS_PATH = REPORTS_DIR / f"sentence_fill_truth_round3_results_{DATE_TAG}.csv"
PACK_PATH = REPORTS_DIR / f"sentence_fill_truth_round3_pack_{DATE_TAG}.md"
MANIFEST_PATH = REPORTS_DIR / "round1_material_to_question_regression_manifest_2026-04-12.csv"


def main() -> None:
    _load_llm_env()
    sample_rows = load_sample_rows()
    docx_blocks = {
        source_name: extract_docx_blocks(path)
        for source_name, path in DOCX_MAP.items()
        if path.exists()
    }
    manifest_rows = [
        row
        for row in _load_csv(MANIFEST_PATH)
        if row.get("family") == "sentence_fill"
    ]
    service = build_service()

    result_rows: list[dict[str, str]] = []
    pack_sections: list[str] = ["# sentence_fill Truth Rewrite Round 3", ""]

    for manifest_row in manifest_rows:
        sample_row = sample_rows[manifest_row["anchor_sample_id"]]
        block = (docx_blocks.get(sample_row["source_name"]) or {}).get(sample_row["source_qid"])
        source_question = parse_source_question(block["lines"], family="sentence_fill") if block else {
            "passage": "",
            "stem": "",
            "options": {},
            "answer": "",
            "analysis": "",
        }
        result = _run_single_case(
            service=service,
            manifest_row=manifest_row,
            sample_row=sample_row,
            source_question=source_question,
        )
        result_rows.append(result)
        pack_sections.extend(_render_case_markdown(result))

    _write_results(result_rows)
    PACK_PATH.write_text("\n".join(pack_sections) + "\n", encoding="utf-8")


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _run_single_case(
    *,
    service,
    manifest_row: dict[str, str],
    sample_row: dict[str, str],
    source_question: dict[str, object],
) -> dict[str, str]:
    selected_material = _material_lookup(manifest_row["selected_material_id"])
    request = build_request(sample_row, source_question)
    _override_user_material(request=request, manifest_row=manifest_row, selected_material=selected_material)

    generation_exception = ""
    built_item: dict[str, Any] | None = None
    prepared_material_text = ""
    local_blank_window = ""

    try:
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
            request_id=f"sf-round3::{uuid4().hex}",
            source_question_analysis=source_question_analysis,
            question_card_binding=question_card_binding,
        )
        material = service._build_forced_user_material_candidates(
            user_material=prepared_request.user_material,
            question_card_binding=question_card_binding,
            request_snapshot=request_snapshot,
            count=1,
        )[0]
        material = _attach_selected_material_identity(
            material=material,
            manifest_row=manifest_row,
            selected_material=selected_material,
        )
        prepared_material = service._prepare_question_service_material(
            material=material,
            question_type=standard_request["question_type"],
            request_snapshot=request_snapshot,
        )
        prepared_material_text = str(prepared_material.text or "").strip()
        local_blank_window = str(
            ((prepared_material.source or {}).get("prompt_extras") or {}).get("fill_ready_local_material") or ""
        ).strip()
        built_item = service._build_generated_item(
            build_request=service._build_prompt_request_from_snapshot(request_snapshot),
            material=prepared_material,
            batch_id=f"sf-round3::{uuid4().hex}",
            item_id=None,
            request_snapshot=request_snapshot,
            revision_count=0,
            route=service._question_generation_route(),
            source_action="generate",
            review_note=None,
            request_id=f"sf-round3-item::{uuid4().hex}",
            previous_item=None,
        )
    except Exception as exc:  # noqa: BLE001
        generation_exception = f"{exc.__class__.__name__}: {exc}"

    generated = dict((built_item or {}).get("generated_question") or {})
    validation_result = dict((built_item or {}).get("validation_result") or {})
    evaluation_result = dict((built_item or {}).get("evaluation_result") or {})
    errors = validation_result.get("errors") or []
    warnings = validation_result.get("warnings") or []

    options = generated.get("options") or {}
    if not isinstance(options, dict):
        options = {}

    return {
        "group_id": manifest_row["group_id"],
        "anchor_sample_id": manifest_row["anchor_sample_id"],
        "selected_material_id": manifest_row["selected_material_id"],
        "selected_material_type": manifest_row.get("selected_material_type") or "",
        "question_generation_succeeded": "true" if generated else "false",
        "validation_passed": "true" if validation_result.get("passed") else "false",
        "review_status": str((built_item or {}).get("review_status") or ""),
        "export_eligible": "true" if (built_item or {}).get("export_eligible") else "false",
        "generation_exception": generation_exception,
        "validator_errors": "; ".join(errors),
        "validator_warnings": "; ".join(warnings),
        "overall_score": str((evaluation_result or {}).get("overall_score") or ""),
        "final_presented_material": prepared_material_text,
        "local_blank_window": local_blank_window,
        "anchor_stem": str(source_question.get("stem") or ""),
        "anchor_answer": str(source_question.get("answer") or ""),
        "generated_stem": str(generated.get("stem") or ""),
        "generated_answer": str(generated.get("answer") or ""),
        "generated_options_json": json.dumps(options, ensure_ascii=False),
        "generated_analysis": str(generated.get("analysis") or ""),
        "anchor_passage": str(source_question.get("passage") or ""),
    }


def _render_case_markdown(row: dict[str, str]) -> list[str]:
    options = json.loads(row["generated_options_json"] or "{}")
    lines = [
        f"## {row['group_id']}",
        "",
        f"- validation_passed: `{row['validation_passed']}`",
        f"- overall_score: `{row['overall_score']}`",
        f"- validator_errors: `{row['validator_errors']}`",
        "",
        "**Final Presented Material**",
        "",
        row["final_presented_material"] or "(empty)",
        "",
        "**Local Blank Window**",
        "",
        row["local_blank_window"] or "(empty)",
        "",
        "**Anchor Stem**",
        "",
        f"> {row['anchor_stem']}",
        "",
        "**Generated Stem**",
        "",
        f"> {row['generated_stem']}",
        "",
        "**Generated Options**",
        "",
    ]
    for key in ("A", "B", "C", "D"):
        lines.append(f"- {key}: {options.get(key, '')}")
    lines.extend(
        [
            "",
            "**Generated Analysis**",
            "",
            row["generated_analysis"] or "(empty)",
            "",
        ]
    )
    return lines


def _write_results(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "group_id",
        "anchor_sample_id",
        "selected_material_id",
        "selected_material_type",
        "question_generation_succeeded",
        "validation_passed",
        "review_status",
        "export_eligible",
        "generation_exception",
        "validator_errors",
        "validator_warnings",
        "overall_score",
        "final_presented_material",
        "local_blank_window",
        "anchor_stem",
        "anchor_answer",
        "generated_stem",
        "generated_answer",
        "generated_options_json",
        "generated_analysis",
        "anchor_passage",
    ]
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
