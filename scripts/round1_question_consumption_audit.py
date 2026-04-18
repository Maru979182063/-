from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.round1_generation_smoke_rerun import (  # noqa: E402
    build_request,
    build_service,
    extract_docx_blocks,
    load_sample_rows,
    parse_source_question,
    project_family_outputs,
)
from scripts.round1_material_to_question_regression import (  # noqa: E402
    DOCX_MAP,
    MANIFEST_PATH as LINKED_MANIFEST_PATH,
    RESULTS_PATH as LINKED_RESULTS_PATH,
    _attach_selected_material_identity,
    _load_llm_env,
    _material_lookup,
    _override_user_material,
)


RESULTS_PATH = REPORTS_DIR / "round1_question_consumption_audit_results_2026-04-12.csv"
REPORT_PATH = REPORTS_DIR / "round1_question_consumption_audit_report_2026-04-12.md"

FAMILIES = ("sentence_fill", "center_understanding", "sentence_order")


def main() -> None:
    _load_llm_env()
    manifest_rows = _load_csv(LINKED_MANIFEST_PATH)
    linked_rows = {
        (row["family"], row["group_id"]): row
        for row in _load_csv(LINKED_RESULTS_PATH)
    }
    sample_rows = load_sample_rows()
    docx_blocks = {
        source_name: extract_docx_blocks(path)
        for source_name, path in DOCX_MAP.items()
        if path.exists()
    }
    service = build_service()

    audit_rows: list[dict[str, str]] = []
    for manifest_row in manifest_rows:
        linked_row = linked_rows[(manifest_row["family"], manifest_row["group_id"])]
        audit_rows.append(
            _audit_single_case(
                service=service,
                manifest_row=manifest_row,
                linked_row=linked_row,
                sample_rows=sample_rows,
                docx_blocks=docx_blocks,
            )
        )

    _write_results(audit_rows)
    _write_report(audit_rows)


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _audit_single_case(
    *,
    service,
    manifest_row: dict[str, str],
    linked_row: dict[str, str],
    sample_rows: dict[str, dict[str, str]],
    docx_blocks: dict[str, dict[str, dict[str, object]]],
) -> dict[str, str]:
    family = manifest_row["family"]
    anchor_sample_id = manifest_row["anchor_sample_id"]
    sample_row = sample_rows[anchor_sample_id]
    block = (docx_blocks.get(sample_row["source_name"]) or {}).get(sample_row["source_qid"])
    source_question = parse_source_question(block["lines"], family=family) if block else {"passage": "", "stem": "", "options": {}, "answer": "", "analysis": ""}
    selected_material = _material_lookup(manifest_row["selected_material_id"])

    request = build_request(sample_row, source_question)
    _override_user_material(request=request, manifest_row=manifest_row, selected_material=selected_material)

    built_item: dict[str, Any] | None = None
    generation_exception = ""
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
            request_id=f"audit::{uuid4().hex}",
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
        built_item = service._build_generated_item(
            build_request=service._build_prompt_request_from_snapshot(request_snapshot),
            material=material,
            batch_id=f"audit::{uuid4().hex}",
            item_id=None,
            request_snapshot=request_snapshot,
            revision_count=0,
            route=service._question_generation_route(),
            source_action="generate",
            review_note=None,
            request_id=f"audit-item::{uuid4().hex}",
            previous_item=None,
        )
    except Exception as exc:  # noqa: BLE001
        generation_exception = f"{exc.__class__.__name__}:{exc}"

    generated_question = dict((built_item or {}).get("generated_question") or {})
    validation_result = dict((built_item or {}).get("validation_result") or {})
    evaluation_result = dict((built_item or {}).get("evaluation_result") or {})
    gate_status = ""
    export_status = ""
    if built_item:
        gate_status, export_status, _ = project_family_outputs(family=family, item=built_item)

    dims = _evaluate_gap_dimensions(
        family=family,
        source_question=source_question,
        generated_question=generated_question,
        selected_material=selected_material,
        validation_result=validation_result,
        evaluation_result=evaluation_result,
        linked_row=linked_row,
    )
    classification = _classify_case(dims=dims, linked_row=linked_row, validation_result=validation_result)

    return {
        "family": family,
        "group_id": manifest_row["group_id"],
        "anchor_sample_id": anchor_sample_id,
        "selected_material_id": manifest_row["selected_material_id"],
        "question_generation_succeeded": linked_row["question_generation_succeeded"],
        "validation_passed": linked_row["validation_passed"],
        "review_status": linked_row["review_status"],
        "export_eligible": linked_row["export_eligible"],
        "error_stage": linked_row["error_stage"],
        "error_reason": linked_row["error_reason"],
        "anchor_stem": _clip(source_question.get("stem") or ""),
        "anchor_answer": str(source_question.get("answer") or ""),
        "anchor_options": _json_string(source_question.get("options") or {}),
        "generated_stem": _clip(generated_question.get("stem") or ""),
        "generated_answer": str(generated_question.get("answer") or ""),
        "generated_options": _json_string(generated_question.get("options") or {}),
        "generated_analysis": _clip(generated_question.get("analysis") or "", limit=240),
        "validator_errors": _json_string(validation_result.get("errors") or []),
        "validator_warnings": _json_string(validation_result.get("warnings") or []),
        "validator_checks": _json_string(validation_result.get("checks") or {}),
        "judge_score": str((evaluation_result.get("overall_score") or "")),
        "question_shape_gap": dims["question_shape_gap"],
        "ask_style_gap": dims["ask_style_gap"],
        "option_design_gap": dims["option_design_gap"],
        "answer_uniqueness_gap": dims["answer_uniqueness_gap"],
        "analysis_style_gap": dims["analysis_style_gap"],
        "family_contract_gap": dims["family_contract_gap"],
        "material_consumption_gap": dims["material_consumption_gap"],
        "validator_alignment_gap": dims["validator_alignment_gap"],
        "overall_real_question_distance": dims["overall_real_question_distance"],
        "audit_classification": classification,
        "audit_reason": dims["audit_reason"],
        "generation_exception": generation_exception,
    }


def _evaluate_gap_dimensions(
    *,
    family: str,
    source_question: dict[str, Any],
    generated_question: dict[str, Any],
    selected_material: dict[str, Any],
    validation_result: dict[str, Any],
    evaluation_result: dict[str, Any],
    linked_row: dict[str, str],
) -> dict[str, str]:
    errors = [str(item) for item in (validation_result.get("errors") or [])]
    error_blob = " ".join(errors).lower()
    stem = str(generated_question.get("stem") or "")
    options = generated_question.get("options") or {}
    analysis = str(generated_question.get("analysis") or "")
    anchor_stem = str(source_question.get("stem") or "")
    material_text = str(selected_material.get("text") or "")
    checks = validation_result.get("checks") or {}

    question_shape_gap = "medium"
    ask_style_gap = "medium"
    option_design_gap = "medium"
    answer_uniqueness_gap = "medium"
    analysis_style_gap = "medium"
    family_contract_gap = "medium"
    material_consumption_gap = "medium"
    validator_alignment_gap = "medium"
    notes: list[str] = []

    if family == "sentence_fill":
        has_fill_stem = any(token in stem for token in ("填入", "横线", "划横线", "空白处"))
        runtime_material_form = checks.get("sentence_fill_runtime_material_form") or {}
        runtime_gap_signal = checks.get("sentence_fill_gap_signal") or {}
        fill_ready_runtime = bool(runtime_material_form.get("passed"))
        has_blank_in_material = bool(fill_ready_runtime or runtime_gap_signal.get("passed")) or (
            "[BLANK]" in material_text or "____" in material_text or "横线" in material_text
        )
        question_shape_gap = "low" if has_fill_stem and len(options) == 4 else "high"
        ask_style_gap = "low" if has_fill_stem and ("最恰当" in stem or "最合适" in stem) else "medium"
        option_design_gap = _option_gap(options)
        answer_uniqueness_gap = "high" if "obvious blank marker" in error_blob else "medium"
        analysis_style_gap = "low" if len(analysis) >= 30 else "high"
        family_contract_gap = "high" if "obvious blank marker" in error_blob or "material_alignment" in error_blob else "medium"
        material_consumption_gap = "high" if not has_blank_in_material else "medium"
        validator_alignment_gap = "high" if question_shape_gap in {"low", "medium"} and has_fill_stem and not has_blank_in_material else "medium"
        if not has_blank_in_material:
            notes.append("generated fill question keeps fill-style stem, but selected material is handed off without an explicit blanked slot")
        elif fill_ready_runtime:
            notes.append("sentence_fill runtime now carries an explicit fill-ready blanked slot into generation and validation")
        if "difficulty projection is outside the target profile range." in errors:
            notes.append("difficulty projection error looks secondary to material-to-fill consumption contract")

    elif family == "center_understanding":
        has_main_idea_stem = any(token in stem for token in ("旨在说明", "意在说明", "旨在强调", "意在强调", "主旨", "概括最准确"))
        question_shape_gap = "low" if has_main_idea_stem and len(options) == 4 else "high"
        ask_style_gap = "low" if has_main_idea_stem else "medium"
        option_design_gap = _option_gap(options)
        answer_uniqueness_gap = "high" if "main_axis_mismatch" in error_blob else ("medium" if "abstraction_level_mismatch" in error_blob else "low")
        analysis_style_gap = "low" if len(analysis) >= 25 else "high"
        family_contract_gap = "high" if "main_axis_mismatch" in error_blob or "abstraction_level_mismatch" in error_blob else "medium"
        material_consumption_gap = "medium"
        validator_alignment_gap = "medium"
        if has_main_idea_stem and "main_axis_mismatch" not in error_blob and "abstraction_level_mismatch" in error_blob:
            validator_alignment_gap = "high"
            notes.append("question still looks like a center-understanding item, but validator rejects abstraction level")
        if "main_axis_mismatch" in error_blob:
            notes.append("generated options likely stay near segment summary rather than stable main axis")

    else:
        has_order_stem = "重新排列" in stem or "语序正确" in stem
        options_text = " ".join(str(value) for value in options.values())
        has_order_markers = any(marker in options_text for marker in ("①", "②", "③", "④", "⑤", "⑥"))
        question_shape_gap = "low" if has_order_stem and len(options) == 4 else "high"
        ask_style_gap = "low" if has_order_stem and "6个部分" in stem else "medium"
        option_design_gap = "low" if has_order_markers and len(options) == 4 else "high"
        answer_uniqueness_gap = "high" if "ordering_chain_incomplete" in error_blob or "role_order_conflict" in error_blob else ("medium" if "sentence_count_mismatch" in error_blob else "low")
        analysis_style_gap = "low" if len(analysis) >= 25 else "high"
        family_contract_gap = "high" if "ordering_chain_incomplete" in error_blob or "sentence_count_mismatch" in error_blob or "role_order_conflict" in error_blob else "medium"
        material_consumption_gap = "medium"
        validator_alignment_gap = "medium"
        if "sentence_count_mismatch" in error_blob:
            notes.append("question format looks like sentence order, but generated ordering chain does not satisfy the 6-unit contract")
        if "ordering_chain_incomplete" in error_blob:
            notes.append("formal shape is present, but the internal ordering chain is weaker than the anchor real question")

    if linked_row.get("question_generation_succeeded") != "true":
        material_consumption_gap = "medium"
        validator_alignment_gap = "low"
        notes.append("generation did not stabilize enough to complete the full quality read")

    overall_real_question_distance = _max_bucket(
        question_shape_gap,
        ask_style_gap,
        option_design_gap,
        answer_uniqueness_gap,
        analysis_style_gap,
        family_contract_gap,
    )

    if overall_real_question_distance == "medium" and material_consumption_gap == "high":
        overall_real_question_distance = "medium"
    if overall_real_question_distance == "high" and validator_alignment_gap == "high" and family_contract_gap != "high":
        overall_real_question_distance = "medium"

    if not notes:
        notes.append("generated question is not far from family shape, but current validator and service contract still reject it")

    return {
        "question_shape_gap": question_shape_gap,
        "ask_style_gap": ask_style_gap,
        "option_design_gap": option_design_gap,
        "answer_uniqueness_gap": answer_uniqueness_gap,
        "analysis_style_gap": analysis_style_gap,
        "family_contract_gap": family_contract_gap,
        "material_consumption_gap": material_consumption_gap,
        "validator_alignment_gap": validator_alignment_gap,
        "overall_real_question_distance": overall_real_question_distance,
        "audit_reason": "; ".join(notes),
    }


def _option_gap(options: dict[str, Any]) -> str:
    texts = [str(value or "").strip() for value in options.values() if str(value or "").strip()]
    if len(texts) < 4:
        return "high"
    normalized = [re.sub(r"\s+", "", text) for text in texts]
    unique_ratio = len(set(normalized)) / max(1, len(normalized))
    if unique_ratio < 0.75:
        return "high"
    if min(len(text) for text in texts) < 4:
        return "medium"
    return "low"


def _classify_case(*, dims: dict[str, str], linked_row: dict[str, str], validation_result: dict[str, Any]) -> str:
    overall = dims["overall_real_question_distance"]
    contract_gap = dims["family_contract_gap"]
    material_gap = dims["material_consumption_gap"]
    validator_gap = dims["validator_alignment_gap"]
    errors = " ".join(str(item) for item in (validation_result.get("errors") or [])).lower()

    if overall == "high" and contract_gap == "high" and validator_gap != "high":
        return "true_quality_gap_primary"
    if material_gap == "high" and overall in {"low", "medium"}:
        return "consumption_contract_gap_primary"
    if validator_gap == "high" and overall in {"low", "medium"}:
        return "validator_overstrict_or_misaligned"
    if "difficulty projection is outside the target profile range." in linked_row.get("error_reason", "") and overall == "medium":
        return "validator_overstrict_or_misaligned"
    if "main_axis_mismatch" in errors or "ordering_chain_incomplete" in errors:
        return "mixed_gap"
    return "mixed_gap"


def _max_bucket(*values: str) -> str:
    order = {"low": 0, "medium": 1, "high": 2}
    winner = max(values, key=lambda value: order.get(value, 0))
    return winner


def _clip(value: str, *, limit: int = 180) -> str:
    text = str(value or "").strip().replace("\r\n", "\n").replace("\n", " ")
    return text[:limit]


def _json_string(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _write_results(rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(rows: list[dict[str, str]]) -> None:
    classification_counter = Counter(row["audit_classification"] for row in rows)
    family_blocks: list[str] = []
    family_primary: dict[str, str] = {}

    for family in FAMILIES:
        family_rows = [row for row in rows if row["family"] == family]
        distance_counts = Counter(row["overall_real_question_distance"] for row in family_rows)
        class_counts = Counter(row["audit_classification"] for row in family_rows)
        primary_class = class_counts.most_common(1)[0][0]
        family_primary[family] = primary_class
        family_blocks.extend(
            [
                f"### {family}",
                f"- distance_low_medium_high: {distance_counts.get('low', 0)}/{distance_counts.get('medium', 0)}/{distance_counts.get('high', 0)}",
                f"- primary_classification: {primary_class}",
                f"- validator_failures: {sum(1 for row in family_rows if row['validation_passed'] != 'true')}/{len(family_rows)}",
                f"- main_notes: {family_rows[0]['audit_reason'] if family_rows else ''}",
                "",
            ]
        )

    if classification_counter["mixed_gap"] >= max(
        classification_counter["true_quality_gap_primary"],
        classification_counter["consumption_contract_gap_primary"],
        classification_counter["validator_overstrict_or_misaligned"],
    ):
        overall = "当前更像 mixed gap：既有真实题目质量问题，也有消费契约与 validator 对齐问题。"
    elif classification_counter["consumption_contract_gap_primary"] >= 4:
        overall = "消费链错位占明显主导，当前不是单纯“题很差”。"
    elif classification_counter["true_quality_gap_primary"] >= 6:
        overall = "当前主要是题本体质量与真题差距过大，validator 拦得基本合理。"
    else:
        overall = "当前更像 mixed gap：既有真题差距，也有消费契约与 validator 对齐问题。"

    if classification_counter["mixed_gap"] >= max(
        classification_counter["true_quality_gap_primary"],
        classification_counter["consumption_contract_gap_primary"],
        classification_counter["validator_overstrict_or_misaligned"],
    ):
        next_action = "need_family_specific_mixed_fix"
    elif classification_counter["validator_overstrict_or_misaligned"] >= 4:
        next_action = "need_validator_contract_realignment_first"
    elif classification_counter["consumption_contract_gap_primary"] >= 4:
        next_action = "need_question_service_consumption_hardening_first"
    elif classification_counter["true_quality_gap_primary"] >= 6:
        next_action = "need_true_generation_quality_fix_first"
    else:
        next_action = "need_family_specific_mixed_fix"

    lines = [
        "# Round 1 Question Consumption Audit & Real-Question Gap Review",
        "",
        "## Overall",
        f"- total_cases: {len(rows)}",
        f"- classification_counts: {json.dumps(classification_counter, ensure_ascii=False)}",
        f"- overall_judgment: {overall}",
        "",
        "## By Family",
        *family_blocks,
        "## Validator Alignment",
        "- current validator is not only blocking obviously bad questions.",
        "- it is also blocking a meaningful portion of questions whose outer family shape is already readable, but whose service contract / family alignment is still off.",
        "",
        "## Next Suggestion",
        f"- {next_action}",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
