from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
PASSAGE_ENV = ROOT / "passage_service" / ".env"
PASSAGE_PYTHON = ROOT / "passage_service" / ".venv" / "Scripts" / "python.exe"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.round1_generation_smoke_rerun import (  # noqa: E402
    build_request,
    build_service,
    extract_docx_blocks,
    load_sample_rows,
    parse_source_question,
    project_family_outputs,
    summarize_generated_item,
)
from app.schemas.question import MaterialSelectionResult  # noqa: E402
from app.services.sentence_fill_protocol import (  # noqa: E402
    normalize_sentence_fill_blank_position,
    normalize_sentence_fill_function_type,
    normalize_sentence_fill_logic_relation,
)
from app.services.sentence_order_protocol import (  # noqa: E402
    strict_sentence_order_export_field,
)


MANIFEST_PATH = REPORTS_DIR / "round1_material_to_question_regression_manifest_2026-04-12.csv"
RESULTS_PATH = REPORTS_DIR / "round1_material_to_question_regression_results_2026-04-12.csv"
REPORT_PATH = REPORTS_DIR / "round1_material_to_question_regression_report_2026-04-12.md"
REPLAY_RESULTS_PATH = REPORTS_DIR / "round1_material_scoring_replay_results_2026-04-12.csv"

GROUPS_PER_FAMILY = 5
FAMILIES = ("sentence_fill", "center_understanding", "sentence_order")
DOCX_MAP = {
    "语句表达-语句填空题.docx": Path(r"C:\Users\Maru\Desktop\语句表达-语句填空题.docx"),
    "片段阅读-中心理解题.docx": Path(r"C:\Users\Maru\Desktop\片段阅读-中心理解题.docx"),
    "语句表达-语句排序题.docx": Path(r"C:\Users\Maru\Desktop\语句表达-语句排序题.docx"),
}
SENTENCE_FILL_FUNCTION_ALIAS = {
    "propose_countermeasure": "countermeasure",
    "ending_summary": "conclusion",
    "summarize_following_text": "summary",
    "topic_introduction": "topic_intro",
    "bridge_both_sides": "bridge",
    "summarize_previous_text": "reference_summary",
    "inserted_reference": "reference_summary",
    "opening_summary": "summary",
}

_SELECTED_MATERIAL_CACHE: dict[str, dict[str, Any]] = {}


def main() -> None:
    _load_llm_env()
    selected_materials = _collect_selected_materials()
    _SELECTED_MATERIAL_CACHE.update({item["candidate_id"]: item for item in selected_materials})
    replay_top_rows = _load_replay_top_rows()
    sample_rows = load_sample_rows()
    docx_blocks = {
        source_name: extract_docx_blocks(path)
        for source_name, path in DOCX_MAP.items()
        if path.exists()
    }

    manifest_rows = _build_manifest_rows(
        selected_materials=selected_materials,
        replay_top_rows=replay_top_rows,
        sample_rows=sample_rows,
    )
    _write_manifest(manifest_rows)

    service = build_service()
    result_rows: list[dict[str, str]] = []
    for manifest_row in manifest_rows:
        result_rows.append(
            _run_linked_validation(
                service=service,
                manifest_row=manifest_row,
                sample_rows=sample_rows,
                docx_blocks=docx_blocks,
            )
        )

    _write_results(result_rows)
    _write_report(manifest_rows, result_rows)


def _load_llm_env() -> None:
    if not PASSAGE_ENV.exists():
        return
    for line in PASSAGE_ENV.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key == "PASSAGE_OPENAI_API_KEY" and not os.environ.get("OPENAI_API_KEY"):
            os.environ["OPENAI_API_KEY"] = value
        elif key == "PASSAGE_OPENAI_BASE_URL" and not os.environ.get("OPENAI_BASE_URL"):
            os.environ["OPENAI_BASE_URL"] = value


def _collect_selected_materials() -> list[dict[str, Any]]:
    snippet = r"""
import json
from scripts.round1_material_scoring_replay import _collect_ranked_items, _group_stability_observation

GROUPS_PER_FAMILY = 5
GROUP_SIZE = 3
items_by_family = _collect_ranked_items()
payload = []
for family in ("sentence_fill", "center_understanding", "sentence_order"):
    items = items_by_family.get(family) or []
    for idx in range(GROUPS_PER_FAMILY):
        group = items[idx * GROUP_SIZE : (idx + 1) * GROUP_SIZE]
        if not group:
            continue
        top = group[0]
        observation = _group_stability_observation(
            family,
            [
                {"candidate_id": str(entry.get("candidate_id") or ""), "_item": entry, "rank_position": inner_idx + 1}
                for inner_idx, entry in enumerate(group)
            ],
        )
        payload.append(
            {
                "family": family,
                "replay_group_id": f"{family}.r{idx + 1:02d}",
                "candidate_id": str(top.get("candidate_id") or ""),
                "article_id": str(top.get("article_id") or ""),
                "candidate_type": str(top.get("candidate_type") or ""),
                "rank_position": 1,
                "text": str(top.get("text") or ""),
                "original_text": str(top.get("original_text") or top.get("text") or ""),
                "source": dict(top.get("source") or {}),
                "business_feature_profile": dict(top.get("business_feature_profile") or {}),
                "neutral_signal_profile": dict(top.get("neutral_signal_profile") or {}),
                "question_ready_context": dict(top.get("question_ready_context") or {}),
                "llm_selection_score": float(top.get("llm_selection_score") or 0.0),
                "llm_reason_summary": top.get("llm_reason_summary") or {},
                "llm_family_match_hint": top.get("llm_family_match_hint") or {},
                "llm_generation_readiness": top.get("llm_generation_readiness") or {},
                "llm_main_axis_source_hint": top.get("llm_main_axis_source_hint") or {},
                "llm_argument_structure_hint": top.get("llm_argument_structure_hint") or {},
                "quality_score": float(top.get("quality_score") or 0.0),
                "selected_task_scoring": dict(top.get("selected_task_scoring") or {}),
                "material_structure_label": str(
                    (top.get("business_feature_profile") or {}).get("material_structure_label")
                    or (top.get("neutral_signal_profile") or {}).get("material_structure_label")
                    or ""
                ),
                "paragraph_span": list((top.get("meta") or {}).get("paragraph_range") or []),
                "sentence_span": list((top.get("meta") or {}).get("sentence_range") or []),
                "selection_observation": observation,
            }
        )
print(json.dumps(payload, ensure_ascii=False))
"""
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [str(PASSAGE_PYTHON), "-c", snippet],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    return list(json.loads(result.stdout))


def _load_replay_top_rows() -> dict[tuple[str, str], dict[str, str]]:
    rows: dict[tuple[str, str], dict[str, str]] = {}
    with REPLAY_RESULTS_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("is_top_candidate") != "true":
                continue
            rows[(row["family"], row["replay_group_id"])] = row
    return rows


def _build_manifest_rows(
    *,
    selected_materials: list[dict[str, Any]],
    replay_top_rows: dict[tuple[str, str], dict[str, str]],
    sample_rows: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    manifest_rows: list[dict[str, str]] = []
    family_rows = _family_sample_rows(sample_rows)

    for entry in selected_materials:
        replay_row = replay_top_rows.get((entry["family"], entry["replay_group_id"]), {})
        anchor_row, anchor_match_mode = _pick_anchor_row(entry=entry, family_rows=family_rows)
        selection_observation = entry.get("selection_observation") or {}
        stability_parts = [
            f"top1_stable={selection_observation.get('top1_stable_across_views') or ''}",
            f"flip_risk={selection_observation.get('flip_risk_level') or ''}",
        ]
        manifest_rows.append(
            {
                "family": entry["family"],
                "group_id": entry["replay_group_id"],
                "anchor_sample_id": anchor_row.get("sample_id") if anchor_row else "",
                "anchor_match_mode": anchor_match_mode,
                "question_card_id": anchor_row.get("question_card_id") if anchor_row else "",
                "selected_material_id": entry["candidate_id"],
                "selected_material_type": entry["candidate_type"],
                "selected_article_id": entry["article_id"],
                "selected_material_rank_reason": _selected_material_rank_reason(entry),
                "selection_stability_tag": ";".join(part for part in stability_parts if part),
                "rank_position": str(entry.get("rank_position") or 1),
                "llm_selection_score": _fmt_float(entry.get("llm_selection_score")),
                "llm_generation_readiness": _fmt_float((entry.get("llm_generation_readiness") or {}).get("score")),
                "llm_family_match_hint": _fmt_float((entry.get("llm_family_match_hint") or {}).get("score")),
                "quality_score": _fmt_float(entry.get("quality_score")),
                "selected_task_scoring": _fmt_float((entry.get("selected_task_scoring") or {}).get("final_candidate_score")),
                "material_structure_label": str(entry.get("material_structure_label") or ""),
                "paragraph_span": _format_span(entry.get("paragraph_span")),
                "sentence_span": _format_span(entry.get("sentence_span")),
                "replay_judgment": replay_row.get("replay_judgment", ""),
                "flip_risk_level": replay_row.get("flip_risk_level", ""),
            }
        )
    return manifest_rows


def _family_sample_rows(sample_rows: dict[str, dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in sample_rows.values():
        family = row.get("business_family_id") or ""
        if family in FAMILIES and row.get("review_status") in {"gold-ready", "review_holdout"}:
            grouped[family].append(row)
    return grouped


def _pick_anchor_row(
    *,
    entry: dict[str, Any],
    family_rows: dict[str, list[dict[str, str]]],
) -> tuple[dict[str, str] | None, str]:
    family = str(entry.get("family") or "")
    rows = family_rows.get(family) or []
    anchor_sample_ids = list(
        (((entry.get("llm_family_match_hint") or {}).get("asset_anchor") or {}).get("anchor_sample_ids") or [])
    )
    if not anchor_sample_ids and family == "center_understanding":
        anchor_sample_ids = list(
            (((entry.get("llm_main_axis_source_hint") or {}).get("asset_anchor") or {}).get("anchor_sample_ids") or [])
        )
    if anchor_sample_ids:
        row_map = {row["sample_id"]: row for row in rows}
        for sample_id in anchor_sample_ids:
            if sample_id in row_map:
                return row_map[sample_id], "asset_anchor_sample"

    if family == "sentence_fill":
        matched = _pick_sentence_fill_anchor(entry=entry, rows=rows)
        if matched:
            return matched
    elif family == "center_understanding":
        matched = _pick_center_anchor(entry=entry, rows=rows)
        if matched:
            return matched
    elif family == "sentence_order":
        matched = _pick_sentence_order_anchor(entry=entry, rows=rows)
        if matched:
            return matched

    return (rows[0] if rows else None), "family_fallback" if rows else "missing_anchor"


def _pick_sentence_fill_anchor(*, entry: dict[str, Any], rows: list[dict[str, str]]) -> tuple[dict[str, str], str] | None:
    profile = ((entry.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    resolved = ((entry.get("question_ready_context") or {}).get("resolved_slots") or {})
    blank_position = normalize_sentence_fill_blank_position(resolved.get("blank_position") or profile.get("blank_position"))
    raw_function_type = (
        profile.get("function_type")
        or resolved.get("function_type")
        or ""
    )
    function_type = normalize_sentence_fill_function_type(
        SENTENCE_FILL_FUNCTION_ALIAS.get(str(raw_function_type).strip(), raw_function_type)
    )
    logic_relation = normalize_sentence_fill_logic_relation(resolved.get("logic_relation") or profile.get("logic_relation"))

    exact = [
        row
        for row in rows
        if row.get("annotation_blank_position") == blank_position
        and row.get("annotation_function_type") == function_type
        and row.get("annotation_logic_relation") == logic_relation
    ]
    if exact:
        return exact[0], "exact_fill_tuple"
    partial = [
        row
        for row in rows
        if row.get("annotation_blank_position") == blank_position
        and row.get("annotation_function_type") == function_type
    ]
    if partial:
        return partial[0], "fill_blank_function_partial"
    blank_only = [row for row in rows if row.get("annotation_blank_position") == blank_position]
    if blank_only:
        return blank_only[0], "fill_blank_only"
    return None


def _pick_center_anchor(*, entry: dict[str, Any], rows: list[dict[str, str]]) -> tuple[dict[str, str], str] | None:
    resolved = ((entry.get("question_ready_context") or {}).get("resolved_slots") or {})
    axis = str((entry.get("llm_main_axis_source_hint") or {}).get("value") or resolved.get("main_axis_source") or "").strip()
    structure = str((entry.get("llm_argument_structure_hint") or {}).get("value") or resolved.get("argument_structure") or "").strip()

    exact = [
        row
        for row in rows
        if row.get("annotation_main_axis_source") == axis
        and row.get("annotation_argument_structure") == structure
    ]
    if exact:
        return exact[0], "exact_center_tuple"
    partial = [row for row in rows if row.get("annotation_main_axis_source") == axis]
    if partial:
        return partial[0], "center_axis_partial"
    return None


def _strict_order_value(field_name: str, source_field_name: str, value: Any) -> str:
    resolved = strict_sentence_order_export_field(
        canonical_field_name=field_name,
        source_field_name=source_field_name,
        value=value,
        source_name="linked_validation",
    )
    return str(resolved.get("value") or "").strip()


def _pick_sentence_order_anchor(*, entry: dict[str, Any], rows: list[dict[str, str]]) -> tuple[dict[str, str], str] | None:
    resolved = ((entry.get("question_ready_context") or {}).get("resolved_slots") or {})
    order_profile = ((entry.get("business_feature_profile") or {}).get("sentence_order_profile") or {})
    candidate_type = _strict_order_value("candidate_type", "candidate_type", resolved.get("candidate_type") or entry.get("candidate_type"))
    opening = _strict_order_value("opening_anchor_type", "opening_anchor_type", resolved.get("opening_anchor_type") or order_profile.get("opening_rule"))
    closing = _strict_order_value("closing_anchor_type", "closing_anchor_type", resolved.get("closing_anchor_type") or order_profile.get("closing_rule"))

    exact = [
        row
        for row in rows
        if row.get("annotation_candidate_type") == candidate_type
        and row.get("annotation_opening_anchor_type") == opening
        and row.get("annotation_closing_anchor_type") == closing
    ]
    if exact:
        return exact[0], "exact_order_tuple"
    partial = [
        row
        for row in rows
        if row.get("annotation_opening_anchor_type") == opening
        and row.get("annotation_closing_anchor_type") == closing
    ]
    if partial:
        return partial[0], "order_anchor_partial"
    return None


def _selected_material_rank_reason(entry: dict[str, Any]) -> str:
    summary = entry.get("llm_reason_summary")
    if isinstance(summary, dict):
        for key in ("reason", "summary", "selection_reason"):
            value = str(summary.get(key) or "").strip()
            if value:
                return value
    anchor_reason = str(
        (((entry.get("llm_family_match_hint") or {}).get("asset_anchor") or {}).get("reason") or "")
    ).strip()
    if anchor_reason:
        return anchor_reason
    return str(
        (((entry.get("question_ready_context") or {}).get("prompt_extras") or {}).get("business_core_rule") or "")
    ).strip()


def _material_lookup(candidate_id: str) -> dict[str, Any]:
    if not _SELECTED_MATERIAL_CACHE:
        for item in _collect_selected_materials():
            _SELECTED_MATERIAL_CACHE[item["candidate_id"]] = item
    return dict(_SELECTED_MATERIAL_CACHE.get(candidate_id) or {})


def _override_user_material(*, request, manifest_row: dict[str, str], selected_material: dict[str, Any]) -> None:
    request.user_material.text = str(selected_material.get("text") or "")
    request.user_material.title = str(
        selected_material.get("article_id")
        or ((selected_material.get("source") or {}).get("article_title") or "")
        or manifest_row["selected_material_id"]
    )
    request.user_material.source_label = str(
        ((selected_material.get("source") or {}).get("source_name") or "")
        or manifest_row["selected_material_id"]
    )


def _attach_selected_material_identity(*, material: MaterialSelectionResult, manifest_row: dict[str, str], selected_material: dict[str, Any]) -> MaterialSelectionResult:
    question_ready_context = dict(selected_material.get("question_ready_context") or {})
    source = dict(material.source or {})
    source.update(
        {
            "material_source_type": "linked_validation_selected_material",
            "selected_candidate_id": manifest_row["selected_material_id"],
            "selected_article_id": manifest_row["selected_article_id"],
            "selection_stability_tag": manifest_row["selection_stability_tag"],
            "rank_reason": manifest_row["selected_material_rank_reason"],
            "source_name": str(((selected_material.get("source") or {}).get("source_name") or source.get("source_name") or "")),
            "article_title": str(((selected_material.get("source") or {}).get("article_title") or source.get("article_title") or "")),
        }
    )
    return material.model_copy(
        update={
            "material_id": manifest_row["selected_material_id"],
            "article_id": manifest_row["selected_article_id"],
            "text": str(selected_material.get("text") or material.text),
            "original_text": str(selected_material.get("original_text") or selected_material.get("text") or material.original_text or material.text),
            "source": source,
            "source_tail": str(((selected_material.get("source") or {}).get("source_url") or material.source_tail or "")),
            "primary_label": str(question_ready_context.get("selected_material_card") or material.primary_label or ""),
            "document_genre": str(((selected_material.get("source") or {}).get("document_genre") or material.document_genre or "linked_validation")),
            "material_structure_label": str(selected_material.get("material_structure_label") or material.material_structure_label or ""),
            "quality_score": float(selected_material.get("quality_score") or material.quality_score or 0.0),
            "selection_reason": "linked_validation_selected_material_top1",
            "resolved_slots": question_ready_context.get("resolved_slots") or material.resolved_slots,
            "runtime_binding": question_ready_context.get("runtime_binding") or material.runtime_binding,
            "validator_contract": question_ready_context.get("validator_contract") or material.validator_contract,
            "fit_scores": {"llm_selection_score": float(selected_material.get("llm_selection_score") or 0.0)},
        }
    )


def _run_linked_validation(
    *,
    service,
    manifest_row: dict[str, str],
    sample_rows: dict[str, dict[str, str]],
    docx_blocks: dict[str, dict[str, dict[str, object]]],
) -> dict[str, str]:
    family = manifest_row["family"]
    sample_id = manifest_row["anchor_sample_id"]
    if not sample_id or sample_id not in sample_rows:
        return {
            **manifest_row,
            "question_generation_started": "false",
            "question_generation_succeeded": "false",
            "validation_passed": "false",
            "review_status": "not_started",
            "export_eligible": "false",
            "error_stage": "material_handoff",
            "error_reason": "anchor_sample_missing",
            "consumption_outcome": "handoff_fail",
            "family_level_read": "material_handoff_blocked",
            "material_to_question_gap_note": "no_round1_anchor_sample_resolved",
            "observed_behavior": "",
        }
    row = sample_rows[sample_id]
    block = (docx_blocks.get(row["source_name"]) or {}).get(row["source_qid"])
    if not block:
        return {
            **manifest_row,
            "question_generation_started": "false",
            "question_generation_succeeded": "false",
            "validation_passed": "false",
            "review_status": "not_started",
            "export_eligible": "false",
            "error_stage": "material_handoff",
            "error_reason": "reference_source_question_missing",
            "consumption_outcome": "handoff_fail",
            "family_level_read": "material_handoff_blocked",
            "material_to_question_gap_note": "anchor_source_question_missing",
            "observed_behavior": "",
        }

    source_question = parse_source_question(block["lines"], family=family)
    request = build_request(row, source_question)
    selected_material = _material_lookup(manifest_row["selected_material_id"])
    _override_user_material(request=request, manifest_row=manifest_row, selected_material=selected_material)

    question_generation_started = False
    question_generation_succeeded = False
    validation_passed = False
    review_status = "not_started"
    export_eligible = False

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
            request_id=f"linked::{uuid4().hex}",
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
    except Exception as exc:  # noqa: BLE001
        return {
            **manifest_row,
            "question_generation_started": "false",
            "question_generation_succeeded": "false",
            "validation_passed": "false",
            "review_status": "not_started",
            "export_eligible": "false",
            "error_stage": "material_handoff",
            "error_reason": f"{exc.__class__.__name__}:{exc}",
            "consumption_outcome": "handoff_fail",
            "family_level_read": "material_handoff_blocked",
            "material_to_question_gap_note": "failed_before_prompt_build",
            "observed_behavior": "",
        }

    try:
        question_generation_started = True
        built_item = service._build_generated_item(
            build_request=service._build_prompt_request_from_snapshot(request_snapshot),
            material=material,
            batch_id=f"linked::{uuid4().hex}",
            item_id=None,
            request_snapshot=request_snapshot,
            revision_count=0,
            route=service._question_generation_route(),
            source_action="generate",
            review_note=None,
            request_id=f"linked-item::{uuid4().hex}",
            previous_item=None,
        )
        generation_status = str((built_item.get("statuses") or {}).get("generation_status") or "failed")
        question_generation_succeeded = generation_status == "success"
        validation_result = built_item.get("validation_result") or {}
        validation_passed = bool(validation_result.get("passed"))
        review_status = str((built_item.get("statuses") or {}).get("review_status") or "")
        gate_status, export_status, projection_blob = project_family_outputs(family=family, item=built_item)
        export_eligible = export_status == "pass"
        observed_behavior = summarize_generated_item(built_item)

        if question_generation_succeeded and validation_passed and gate_status == "pass" and export_status == "pass":
            error_stage = ""
            error_reason = ""
            consumption_outcome = "usable"
            family_level_read = "stable_transfer"
            gap_note = "selected_material_transferred_cleanly_into_question_service"
        elif question_generation_succeeded and not validation_passed:
            error_stage = "validator"
            error_reason = ";".join(str(err) for err in (validation_result.get("errors") or [])[:4])
            consumption_outcome = "validation_fail"
            family_level_read = "material_good_but_validation_loss"
            gap_note = "material_selection_looks_stable_but_question_validator_rejected_output"
        elif question_generation_succeeded and validation_passed and (gate_status != "pass" or export_status != "pass"):
            error_stage = "review_gate"
            error_reason = projection_blob[:240]
            consumption_outcome = "review_hold"
            family_level_read = "material_good_but_export_gate_loss"
            gap_note = "generated_item_survived_validator_but_gate_or_export_blocked"
        else:
            error_stage = _generation_failure_stage(built_item)
            error_reason = _generation_failure_reason(built_item)
            consumption_outcome = "parse_fail" if error_stage == "generation_parse" else "generation_fail"
            family_level_read = "material_selected_but_generation_unstable"
            gap_note = "selected_material_reached_prompt_chain_but_generation_did_not_stabilize"

        return {
            **manifest_row,
            "question_generation_started": "true" if question_generation_started else "false",
            "question_generation_succeeded": "true" if question_generation_succeeded else "false",
            "validation_passed": "true" if validation_passed else "false",
            "review_status": review_status,
            "export_eligible": "true" if export_eligible else "false",
            "error_stage": error_stage,
            "error_reason": error_reason,
            "consumption_outcome": consumption_outcome,
            "family_level_read": family_level_read,
            "material_to_question_gap_note": gap_note,
            "observed_behavior": observed_behavior,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **manifest_row,
            "question_generation_started": "true" if question_generation_started else "false",
            "question_generation_succeeded": "false",
            "validation_passed": "false",
            "review_status": "not_started",
            "export_eligible": "false",
            "error_stage": "prompt_build",
            "error_reason": f"{exc.__class__.__name__}:{exc}",
            "consumption_outcome": "generation_fail",
            "family_level_read": "material_selected_but_prompt_chain_broke",
            "material_to_question_gap_note": "question_service_exception_before_usable_item",
            "observed_behavior": "",
        }


def _generation_failure_stage(built_item: dict[str, Any]) -> str:
    warnings = " ".join(str(item) for item in (built_item.get("warnings") or []))
    notes = " ".join(str(item) for item in (built_item.get("notes") or []))
    text = f"{warnings} {notes}".lower()
    if "parsed into generatedquestion" in text or "json" in text or "structured model output" in text:
        return "generation_parse"
    return "model_generation"


def _generation_failure_reason(built_item: dict[str, Any]) -> str:
    warnings = [str(item) for item in (built_item.get("warnings") or []) if str(item).strip()]
    if warnings:
        return ";".join(warnings[:3])
    notes = [str(item) for item in (built_item.get("notes") or []) if str(item).strip()]
    return ";".join(notes[:3])


def _write_manifest(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "family",
        "group_id",
        "anchor_sample_id",
        "anchor_match_mode",
        "question_card_id",
        "selected_material_id",
        "selected_material_type",
        "selected_article_id",
        "selected_material_rank_reason",
        "selection_stability_tag",
        "rank_position",
        "llm_selection_score",
        "llm_generation_readiness",
        "llm_family_match_hint",
        "quality_score",
        "selected_task_scoring",
        "material_structure_label",
        "paragraph_span",
        "sentence_span",
        "replay_judgment",
        "flip_risk_level",
    ]
    with MANIFEST_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_results(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "family",
        "group_id",
        "anchor_sample_id",
        "anchor_match_mode",
        "question_card_id",
        "selected_material_id",
        "selected_material_type",
        "selected_article_id",
        "selected_material_rank_reason",
        "selection_stability_tag",
        "rank_position",
        "llm_selection_score",
        "llm_generation_readiness",
        "llm_family_match_hint",
        "quality_score",
        "selected_task_scoring",
        "material_structure_label",
        "paragraph_span",
        "sentence_span",
        "replay_judgment",
        "flip_risk_level",
        "question_generation_started",
        "question_generation_succeeded",
        "validation_passed",
        "review_status",
        "export_eligible",
        "error_stage",
        "error_reason",
        "consumption_outcome",
        "family_level_read",
        "material_to_question_gap_note",
        "observed_behavior",
    ]
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_report(manifest_rows: list[dict[str, str]], result_rows: list[dict[str, str]]) -> None:
    total = len(result_rows)
    usable = sum(1 for row in result_rows if row["consumption_outcome"] == "usable")
    validation_loss = sum(1 for row in result_rows if row["consumption_outcome"] == "validation_fail")
    parse_loss = sum(1 for row in result_rows if row["consumption_outcome"] == "parse_fail")
    handoff_loss = sum(1 for row in result_rows if row["consumption_outcome"] == "handoff_fail")
    stage_counter = Counter(row["error_stage"] for row in result_rows if row["error_stage"])

    family_lines: list[str] = []
    family_verdicts: dict[str, str] = {}
    for family in FAMILIES:
        family_rows = [row for row in result_rows if row["family"] == family]
        usable_count = sum(1 for row in family_rows if row["consumption_outcome"] == "usable")
        gen_ok = sum(1 for row in family_rows if row["question_generation_succeeded"] == "true")
        validation_ok = sum(1 for row in family_rows if row["validation_passed"] == "true")
        export_ok = sum(1 for row in family_rows if row["export_eligible"] == "true")
        stage_counts = Counter(row["error_stage"] for row in family_rows if row["error_stage"])
        primary_stage = stage_counts.most_common(1)[0][0] if stage_counts else "none"
        if usable_count >= max(4, len(family_rows) - 1):
            verdict = "stable_material_transfer"
        elif usable_count >= max(2, len(family_rows) // 2):
            verdict = "material_good_but_consumption_loss_present"
        else:
            verdict = "question_service_consumption_fragile"
        family_verdicts[family] = verdict
        family_lines.extend(
            [
                f"### {family}",
                f"- sample_count: {len(family_rows)}",
                f"- usable: {usable_count}/{len(family_rows)}",
                f"- generation_succeeded: {gen_ok}/{len(family_rows)}",
                f"- validation_passed: {validation_ok}/{len(family_rows)}",
                f"- export_eligible: {export_ok}/{len(family_rows)}",
                f"- primary_loss_stage: {primary_stage}",
                f"- verdict: {verdict}",
                "",
            ]
        )

    if usable >= 11 and (validation_loss + parse_loss) <= 4:
        overall = "Current stable material selection already transfers measurable positive value into question generation."
    else:
        overall = "Material selection is stable, but downstream question-service consumption still absorbs a meaningful share of the gain."

    primary_bottleneck = stage_counter.most_common(1)[0][0] if stage_counter else "none"
    if primary_bottleneck in {"validator", "generation_parse", "model_generation", "prompt_build", "review_gate"}:
        next_action = "question_service_consumption_now_becomes_primary_bottleneck"
    elif primary_bottleneck == "material_handoff":
        next_action = "need_small_handoff_cleanup_between_material_and_question_service"
    else:
        next_action = "stay_in_material_selection_stable_phase_and_expand_linked_checks"

    lines = [
        "# Round 1 Material-to-Question Linked Regression",
        "",
        "## Overall",
        f"- total_cases: {total}",
        f"- usable: {usable}/{total}",
        f"- validation_fail: {validation_loss}",
        f"- parse_fail: {parse_loss}",
        f"- handoff_fail: {handoff_loss}",
        f"- primary_loss_stage: {primary_bottleneck}",
        f"- overall_read: {overall}",
        "",
        "## By Family",
        *family_lines,
        "## Bottleneck Shift",
        f"- current_primary_bottleneck: {primary_bottleneck}",
        f"- material_side_status: frozen_selection_stable_phase",
        f"- family_transfer_read: {json.dumps(family_verdicts, ensure_ascii=False)}",
        "",
        "## Next Suggestion",
        f"- {next_action}",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _fmt_float(value: Any) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return ""


def _format_span(value: Any) -> str:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return f"{value[0]}-{value[1]}"
    return ""


if __name__ == "__main__":
    main()
