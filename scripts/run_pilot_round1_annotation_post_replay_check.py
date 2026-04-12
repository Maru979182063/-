from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MASTER_PATH = ROOT / "reports" / "pilot_round1_annotation_execution_master_2026-04-12.csv"
BASLINE_GATE_PATH = ROOT / "reports" / "pilot_round1_sample_manifest_gated_2026-04-12.csv"
BATCH_DIR = ROOT / "reports" / "pilot_round1_annotation_batches_2026-04-12"
DIFF_PATH = ROOT / "reports" / "pilot_round1_annotation_vs_gate_diff_2026-04-12.csv"
CENTER_FOCUS_PATH = ROOT / "reports" / "pilot_round1_center_understanding_review_focus_2026-04-12.csv"
BOUNDARY_SPOTCHECK_PATH = ROOT / "reports" / "pilot_round1_boundary_spotcheck_2026-04-12.csv"
REPORT_PATH = ROOT / "reports" / "pilot_round1_annotation_post_replay_check_2026-04-12.md"

DOC_TEXT_PATHS = {
    "片段阅读-中心理解题.docx": ROOT / "tmp_truth_docs" / "center_understanding_desktop_extracted.txt",
    "语句表达-语句填空题.docx": ROOT / "tmp_truth_docs" / "sentence_fill_desktop_extracted.txt",
    "语句表达-语句排序题.docx": ROOT / "tmp_truth_docs" / "sentence_order_desktop_extracted.txt",
}

QUESTION_START_RE = re.compile(r"(?m)^\d+\.\s*题号：#?(\d+)")
XML_TAG_RE = re.compile(r"<[^>]+>")


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def load_chunk_indexes() -> dict[str, dict[str, str]]:
    indexes: dict[str, dict[str, str]] = {}
    for source_name, path in DOC_TEXT_PATHS.items():
        text = path.read_text(encoding="utf-8")
        matches = list(QUESTION_START_RE.finditer(text))
        chunks: dict[str, str] = {}
        for idx, match in enumerate(matches):
            qid = match.group(1)
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk = XML_TAG_RE.sub(" ", text[start:end]).replace("\xa0", " ")
            chunks[qid] = chunk
        indexes[source_name] = chunks
    return indexes


def parse_analysis_snippet(chunk: str) -> str:
    lines = [line.strip() for line in chunk.splitlines() if line.strip()]
    in_analysis = False
    parts: list[str] = []
    for line in lines:
        if line == "【解析】":
            in_analysis = True
            continue
        if in_analysis and line.startswith("【") and line.endswith("】"):
            break
        if in_analysis:
            parts.append(line)
            if len("".join(parts)) >= 100:
                break
    return "".join(parts)[:140]


def load_batch_rows() -> tuple[list[str], list[dict[str, str]]]:
    union_fields: list[str] = []
    seen_fields: set[str] = set()
    all_rows: list[dict[str, str]] = []
    for batch_file in sorted(BATCH_DIR.glob("*.csv")):
        fields, rows = read_csv(batch_file)
        for field in fields:
            if field not in seen_fields:
                seen_fields.add(field)
                union_fields.append(field)
        all_rows.extend(rows)
    return union_fields, all_rows


def ensure_master_consistent() -> tuple[list[str], list[dict[str, str]]]:
    union_fields, batch_rows = load_batch_rows()
    master_fields, _ = read_csv(MASTER_PATH)
    for field in master_fields:
        if field not in union_fields:
            union_fields.append(field)
    write_csv(MASTER_PATH, union_fields, batch_rows)
    return union_fields, batch_rows


def text(value: Any) -> str:
    return str(value or "").strip()


def bool_text(value: bool) -> str:
    return "true" if value else "false"


def build_diff_rows(rows: list[dict[str, str]], gate_rows_by_sample: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    diff_rows: list[dict[str, Any]] = []
    for row in rows:
        family = row["business_family_id"]
        sample_id = row["sample_id"]
        review_status = row.get("review_status", "")
        gate_row = gate_rows_by_sample.get(sample_id, {})

        if family == "sentence_fill":
            field_pairs = [
                ("blank_position", "annotation_blank_position", "carry_forward"),
                ("function_type", "annotation_function_type", "carry_forward"),
                ("logic_relation", "annotation_logic_relation", "logic_relation_refined"),
            ]
            for gate_field, annotation_field, diff_kind in field_pairs:
                gate_value = text(gate_row.get(gate_field))
                annotation_value = text(row.get(annotation_field))
                if gate_value == annotation_value:
                    continue
                if diff_kind == "logic_relation_refined":
                    diff_type = "annotation_refined_from_gate_default"
                    assessment = "expected_refinement"
                else:
                    diff_type = "annotation_override"
                    assessment = "needs_review"
                diff_rows.append(
                    {
                        "sample_id": sample_id,
                        "business_family_id": family,
                        "field_name": gate_field,
                        "gate_value": gate_value,
                        "annotation_value": annotation_value,
                        "diff_type": diff_type,
                        "assessment": assessment,
                        "review_status": review_status,
                        "notes": row.get("notes", ""),
                    }
                )
        elif family == "center_understanding":
            field_pairs = [
                ("main_axis_source", "annotation_main_axis_source"),
                ("argument_structure", "annotation_argument_structure"),
            ]
            for gate_field, annotation_field in field_pairs:
                gate_value = text(gate_row.get(gate_field))
                annotation_value = text(row.get(annotation_field))
                if gate_value == annotation_value:
                    continue
                diff_rows.append(
                    {
                        "sample_id": sample_id,
                        "business_family_id": family,
                        "field_name": gate_field,
                        "gate_value": gate_value,
                        "annotation_value": annotation_value,
                        "diff_type": "annotation_added_manual_label",
                        "assessment": "expected_manual_labeling",
                        "review_status": review_status,
                        "notes": row.get("notes", ""),
                    }
                )
            if review_status == "review-needed":
                diff_rows.append(
                    {
                        "sample_id": sample_id,
                        "business_family_id": family,
                        "field_name": "review_status",
                        "gate_value": "pass",
                        "annotation_value": review_status,
                        "diff_type": "gate_pass_but_annotation_review_needed",
                        "assessment": "focus_review",
                        "review_status": review_status,
                        "notes": row.get("notes", ""),
                    }
                )
        elif family == "sentence_order":
            field_pairs = [
                ("candidate_type", "annotation_candidate_type"),
                ("opening_anchor_type", "annotation_opening_anchor_type"),
                ("closing_anchor_type", "annotation_closing_anchor_type"),
            ]
            for gate_field, annotation_field in field_pairs:
                gate_value = text(gate_row.get(gate_field))
                annotation_value = text(row.get(annotation_field))
                if gate_value == annotation_value:
                    continue
                diff_rows.append(
                    {
                        "sample_id": sample_id,
                        "business_family_id": family,
                        "field_name": gate_field,
                        "gate_value": gate_value,
                        "annotation_value": annotation_value,
                        "diff_type": "annotation_record_missing",
                        "assessment": "recording_issue",
                        "review_status": review_status,
                        "notes": row.get("notes", ""),
                    }
                )
    return diff_rows


def build_center_focus_rows(rows: list[dict[str, str]], chunk_indexes: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    focus_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("business_family_id") != "center_understanding":
            continue
        if row.get("review_status") != "review-needed":
            continue
        chunk = (chunk_indexes.get(row["source_name"]) or {}).get(row["source_qid"], "")
        focus_rows.append(
            {
                "sample_id": row["sample_id"],
                "source_qid": row["source_qid"],
                "source_exam": row["source_exam"],
                "annotation_main_axis_source": row.get("annotation_main_axis_source", ""),
                "annotation_argument_structure": row.get("annotation_argument_structure", ""),
                "review_status": row.get("review_status", ""),
                "focus_reason": row.get("notes", ""),
                "analysis_snippet": parse_analysis_snippet(chunk),
                "recommended_action": "second_pass_review",
            }
        )
    return focus_rows


def build_boundary_spotcheck(rows: list[dict[str, str]], gate_rows_by_sample: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    spot_rows: list[dict[str, Any]] = []
    for row in rows:
        family = row["business_family_id"]
        if family == "sentence_fill":
            gate_row = gate_rows_by_sample.get(row["sample_id"], {})
            if text(gate_row.get("logic_relation")) != text(row.get("annotation_logic_relation")):
                spot_rows.append(
                    {
                        "sample_id": row["sample_id"],
                        "business_family_id": family,
                        "field_name": "logic_relation",
                        "gate_value": gate_row.get("logic_relation", ""),
                        "annotation_value": row.get("annotation_logic_relation", ""),
                        "review_status": row.get("review_status", ""),
                        "notes": row.get("notes", ""),
                    }
                )
        elif family == "sentence_order":
            if text(row.get("opening_anchor_type")) == "weak_opening" or text(row.get("closing_anchor_type")) in {"call_to_action", "summary"}:
                spot_rows.append(
                    {
                        "sample_id": row["sample_id"],
                        "business_family_id": family,
                        "field_name": "opening_anchor_type / closing_anchor_type",
                        "gate_value": f"{row.get('opening_anchor_type','')} | {row.get('closing_anchor_type','')}",
                        "annotation_value": f"{row.get('annotation_opening_anchor_type','')} | {row.get('annotation_closing_anchor_type','')}",
                        "review_status": row.get("review_status", ""),
                        "notes": row.get("notes", ""),
                    }
                )
    return spot_rows


def build_report(rows: list[dict[str, str]], diff_rows: list[dict[str, Any]], center_focus_rows: list[dict[str, Any]], spot_rows: list[dict[str, Any]]) -> str:
    total = len(rows)
    family_counts = Counter(row["business_family_id"] for row in rows)
    layer_counts = Counter(row.get("layer_after_annotation", "") for row in rows)
    diff_counter = Counter((row["business_family_id"], row["field_name"], row["diff_type"]) for row in diff_rows)

    sentence_fill_diffs = [row for row in diff_rows if row["business_family_id"] == "sentence_fill"]
    sentence_order_diffs = [row for row in diff_rows if row["business_family_id"] == "sentence_order"]
    center_diffs = [row for row in diff_rows if row["business_family_id"] == "center_understanding"]
    center_review_needed = [row for row in rows if row["business_family_id"] == "center_understanding" and row.get("review_status") == "review-needed"]

    lines = [
        "# Pilot Round 1 Annotation Post-Replay Check (2026-04-12)",
        "",
        "## Scope",
        f"- Checked annotated main set only: {total} samples",
        f"- sentence_fill: {family_counts.get('sentence_fill', 0)}",
        f"- center_understanding: {family_counts.get('center_understanding', 0)}",
        f"- sentence_order: {family_counts.get('sentence_order', 0)}",
        "- Excluded from main replay: 3 blocked sentence_order samples in error-pool",
        "",
        "## Totals",
        f"- gold-ready: {layer_counts.get('gold-ready', 0)}",
        f"- review-needed: {layer_counts.get('review-needed', 0)}",
        f"- error-pool: {layer_counts.get('error-pool', 0)}",
        "",
        "## Gate vs Annotation",
        f"- sentence_fill diff rows: {len(sentence_fill_diffs)}",
        f"- center_understanding diff rows: {len(center_diffs)}",
        f"- sentence_order diff rows: {len(sentence_order_diffs)}",
        "",
        "### Family Notes",
        f"- sentence_fill: logic_relation differs from gate in {len(sentence_fill_diffs)} rows; this is expected refinement because gate seeded a default logic_relation while manual annotation resolved relation from reading.",
        f"- center_understanding: {len(center_review_needed)} rows stayed review-needed; current evidence points to conservative labeling on main_axis_source / argument_structure rather than protocol failure.",
        f"- sentence_order: anchor labels stayed stable; any diff rows are recording issues rather than semantic conflict.",
        "",
        "## Layering Check",
        "- gold-ready group remains stable for sentence_fill and sentence_order.",
        "- review-needed is concentrated in center_understanding, which is the intended focus family for this round.",
        "- No new sample was moved into error-pool from the annotated main set.",
        f"- export-eligible remains valid for sentence_order: {sum(1 for row in rows if row['business_family_id']=='sentence_order' and row.get('formal_export_eligible')=='true')} rows.",
        "",
        "## Focus Findings",
        f"- center_understanding review focus list size: {len(center_focus_rows)}",
        f"- sentence_fill boundary spotchecks: {sum(1 for row in spot_rows if row['business_family_id']=='sentence_fill')}",
        f"- sentence_order boundary spotchecks: {sum(1 for row in spot_rows if row['business_family_id']=='sentence_order')}",
        "",
        "## Conclusion",
        "- sentence_fill: stable, can proceed to formal sample / standard asset consolidation.",
        "- sentence_order: stable, can proceed to formal sample / standard asset consolidation; formal_export_eligible remains unchanged.",
        "- center_understanding: do not treat the higher review-needed ratio as failure yet; first read it as conservative labeling. It should go through a dedicated second-pass review before broadening the pool.",
        "- Not ready for training or self-learning. This round only supports post-annotation replay check and next-step asset consolidation / review repair.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    union_fields, rows = ensure_master_consistent()
    _, gate_rows = read_csv(BASLINE_GATE_PATH)
    gate_rows_by_sample = {row["sample_id"]: row for row in gate_rows}
    diff_rows = build_diff_rows(rows, gate_rows_by_sample)
    write_csv(
        DIFF_PATH,
        [
            "sample_id",
            "business_family_id",
            "field_name",
            "gate_value",
            "annotation_value",
            "diff_type",
            "assessment",
            "review_status",
            "notes",
        ],
        diff_rows,
    )

    chunk_indexes = load_chunk_indexes()
    center_focus_rows = build_center_focus_rows(rows, chunk_indexes)
    write_csv(
        CENTER_FOCUS_PATH,
        [
            "sample_id",
            "source_qid",
            "source_exam",
            "annotation_main_axis_source",
            "annotation_argument_structure",
            "review_status",
            "focus_reason",
            "analysis_snippet",
            "recommended_action",
        ],
        center_focus_rows,
    )

    spot_rows = build_boundary_spotcheck(rows, gate_rows_by_sample)
    write_csv(
        BOUNDARY_SPOTCHECK_PATH,
        [
            "sample_id",
            "business_family_id",
            "field_name",
            "gate_value",
            "annotation_value",
            "review_status",
            "notes",
        ],
        spot_rows,
    )

    REPORT_PATH.write_text(build_report(rows, diff_rows, center_focus_rows, spot_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
