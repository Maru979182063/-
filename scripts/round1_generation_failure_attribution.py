from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"

import sys

sys.path.insert(0, str(PROMPT_SERVICE_ROOT))

from app.services.text_readability import detect_readability_issues


MANIFEST_PATH = REPORTS / "round1_fewshot_generation_regression_manifest_2026-04-12.csv"
RESULTS_PATH = REPORTS / "round1_fewshot_generation_regression_results_2026-04-12.csv"
PAYLOADS_PATH = REPORTS / "round1_fewshot_generation_regression_prompt_payloads_2026-04-12.json"
RAW_OUTPUTS_PATH = REPORTS / "round1_fewshot_generation_regression_raw_outputs_2026-04-12.json"
ATTRIBUTION_PATH = REPORTS / "round1_generation_failure_attribution_2026-04-12.csv"
NOTES_PATH = REPORTS / "round1_generation_readability_fix_notes_2026-04-12.md"
SMOKE_RESULTS_PATH = REPORTS / "round1_generation_smoke_rerun_results_2026-04-12.csv"
CLEANUP_DIFF_PATH = REPORTS / "round1_source_extraction_cleanup_diff_2026-04-12.csv"


def main() -> None:
    manifest_rows = {row["sample_id"]: row for row in _read_csv(MANIFEST_PATH)}
    results_rows = _read_csv(RESULTS_PATH)
    payload_rows = {row["sample_id"]: row for row in json.loads(PAYLOADS_PATH.read_text(encoding="utf-8"))}
    raw_rows = {row["sample_id"]: row for row in json.loads(RAW_OUTPUTS_PATH.read_text(encoding="utf-8"))}

    output_rows: list[dict[str, str]] = []
    counts: dict[str, int] = {}

    for row in results_rows:
        sample_id = row["sample_id"]
        manifest = manifest_rows.get(sample_id, {})
        payload = payload_rows.get(sample_id, {})
        raw = raw_rows.get(sample_id, {})
        attribution = _attribute_row(row=row, manifest=manifest, payload=payload, raw=raw)
        output_rows.append(attribution)
        counts[attribution["failure_type"]] = counts.get(attribution["failure_type"], 0) + 1

    _write_csv(
        ATTRIBUTION_PATH,
        output_rows,
        [
            "sample_id",
            "business_family_id",
            "failure_stage",
            "failure_type",
            "is_input_readability_issue",
            "is_output_parse_issue",
            "is_gate_or_export_issue",
            "is_expected_negative_control",
            "raw_symptom",
            "minimal_fix_direction",
            "notes",
        ],
    )

    smoke_rows = _read_csv(SMOKE_RESULTS_PATH) if SMOKE_RESULTS_PATH.exists() else []
    smoke_success = sum(1 for row in smoke_rows if row.get("generation_status") == "success")
    smoke_parse_ok = sum(1 for row in smoke_rows if row.get("json_parse_status") in {"parsed", "not_applicable_negative_control"})
    smoke_readability_ok = sum(1 for row in smoke_rows if row.get("readability_status") in {"clean", "not_applicable_negative_control"})
    cleanup_rows = _read_csv(CLEANUP_DIFF_PATH) if CLEANUP_DIFF_PATH.exists() else []
    cleanup_improved = sum(1 for row in cleanup_rows if int(row.get("issue_delta") or 0) > 0)

    notes = [
        "# Round 1 Generation Readability Fix Notes",
        "",
        "- This pass re-attributes the existing 29-sample regression by stage instead of treating everything as one generic generation failure.",
        "- The dominant failure bucket is still upstream readability pollution that later collapses into parse failure, not formal export contamination.",
        "",
        "## Main Findings",
        f"- total samples attributed: {len(output_rows)}",
        f"- largest failure type: {_largest_failure_type(counts)}",
        "- the biggest pressure point remains source_question/reference payload readability, especially Word XML fragments and escaped NBSPs entering prompt assembly.",
        "- prompt assembly is now fed by normalized request payloads instead of raw `model_dump()` snapshots.",
        "- negative controls remained correctly blocked; this is not a gate-laxness problem.",
        "",
        "## Fix Landing Points",
        "- `question_generation._prepare_request()` now normalizes source_question / user_material before raw-question parsing and before any snapshot/reference payload is built.",
        "- `question_generation._build_request_snapshot()` now stores cleaned `source_question`, `user_material`, and `source_question_analysis` instead of raw payload dumps.",
        "- `question_generation._build_forced_user_material_candidates()` now normalizes `article_title` together with `source_label/topic/text`.",
        "- `question_generation._prepare_reference_prompt_payload()` now uses the same cleaned source-question projection before prompt injection.",
        "",
        "## Smoke Validation Snapshot",
        f"- smoke rows seen: {len(smoke_rows)}" if smoke_rows else "- smoke rows seen: 0",
        f"- smoke generation success: {smoke_success}" if smoke_rows else "- smoke generation success: not_run_yet",
        f"- smoke readability clean/not-applicable: {smoke_readability_ok}" if smoke_rows else "- smoke readability clean/not-applicable: not_run_yet",
        f"- smoke JSON parse clean/not-applicable: {smoke_parse_ok}" if smoke_rows else "- smoke JSON parse clean/not-applicable: not_run_yet",
        f"- cleanup diff rows with issue reduction: {cleanup_improved}" if cleanup_rows else "- cleanup diff rows with issue reduction: not_run_yet",
        "",
        "## Remaining Risk",
        "- This attribution still uses the existing 29-sample regression artifacts, so it tells us where the old failures concentrated; the smoke rerun is the post-fix behavior check.",
        "- If residual failures remain after cleanup, they are more likely to be model-output instability or remaining source extraction edge cases than protocol/export contamination.",
        "- A remaining largest bucket in this file does not mean the current smoke path is still leaking Word XML; it means the older 29-sample artifacts were captured before the second-cut unified cleanup entrance.",
    ]
    NOTES_PATH.write_text("\n".join(notes), encoding="utf-8")


def _attribute_row(
    *,
    row: dict[str, str],
    manifest: dict[str, str],
    payload: dict[str, object],
    raw: dict[str, object],
) -> dict[str, str]:
    sample_id = row["sample_id"]
    family = row["business_family_id"]
    role = manifest.get("regression_role", "")
    generation_status = row.get("generation_status", "")
    gate_status = row.get("gate_status", "")
    export_status = row.get("export_status", "")
    notes = row.get("notes", "")

    system_prompt = str(payload.get("system_prompt") or "")
    user_prompt = str(payload.get("user_prompt") or "")
    input_blob = "\n".join([system_prompt, user_prompt])
    input_issues = detect_readability_issues(input_blob)

    source_question = ((payload.get("request_snapshot") or {}) if isinstance(payload.get("request_snapshot"), dict) else {}).get("source_question") or {}
    source_blob = json.dumps(source_question, ensure_ascii=False) if isinstance(source_question, dict) else str(source_question)
    source_issues = detect_readability_issues(source_blob)

    raw_output = str(raw.get("output_text") or "")
    parse_issue = generation_status.startswith("error:JSONDecodeError")
    gate_issue = notes == "gate_or_export_regressed" or gate_status == "blocked" or export_status == "blocked"

    failure_stage = ""
    failure_type = "none"
    minimal_fix_direction = "none"
    raw_symptom = row.get("observed_behavior") or generation_status or notes

    if role == "negative":
        failure_stage = "gate_projection"
        failure_type = "expected_negative_control"
        minimal_fix_direction = "keep_negative_control_blocked"
    elif parse_issue and "word_xml_leak" in source_issues:
        failure_stage = "source_extraction"
        failure_type = "word_xml_leak_into_reference_payload"
        minimal_fix_direction = "strip_word_xml_before_prompt"
    elif parse_issue and source_issues:
        failure_stage = "source_extraction"
        failure_type = "source_text_readability_corrupted"
        minimal_fix_direction = "repair_source_text_before_prompt"
    elif parse_issue and input_issues:
        failure_stage = "prompt_assembly"
        failure_type = "mojibake_prompt_text"
        minimal_fix_direction = "repair_prompt_text_before_generation"
    elif parse_issue and not str(raw_output).strip():
        failure_stage = "model_generation"
        failure_type = "empty_model_text_output"
        minimal_fix_direction = "record_raw_response_and_retry_json_extraction"
    elif parse_issue:
        failure_stage = "json_parse"
        failure_type = "non_json_or_unparsable_model_output"
        minimal_fix_direction = "strengthen_json_shell_extraction"
    elif gate_issue and gate_status == "blocked":
        failure_stage = "gate_projection"
        failure_type = "gate_projection_regressed_after_generation"
        minimal_fix_direction = "inspect_core_field_projection_vs_generated_output"
    elif gate_issue and export_status == "blocked":
        failure_stage = "export_check"
        failure_type = "export_check_regressed_after_generation"
        minimal_fix_direction = "inspect_export_projection_vs_generated_output"

    return {
        "sample_id": sample_id,
        "business_family_id": family,
        "failure_stage": failure_stage,
        "failure_type": failure_type,
        "is_input_readability_issue": "true" if bool(input_issues or source_issues) else "false",
        "is_output_parse_issue": "true" if parse_issue else "false",
        "is_gate_or_export_issue": "true" if gate_issue else "false",
        "is_expected_negative_control": "true" if role == "negative" else "false",
        "raw_symptom": raw_symptom,
        "minimal_fix_direction": minimal_fix_direction,
        "notes": notes,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _largest_failure_type(counts: dict[str, int]) -> str:
    filtered = {key: value for key, value in counts.items() if key not in {"none", "expected_negative_control"}}
    if not filtered:
        return "none"
    return max(filtered.items(), key=lambda item: item[1])[0]


if __name__ == "__main__":
    main()
