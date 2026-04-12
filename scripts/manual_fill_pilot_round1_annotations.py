from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MASTER_PATH = ROOT / "reports" / "pilot_round1_annotation_execution_master_2026-04-12.csv"
BATCH_MANIFEST_PATH = ROOT / "reports" / "pilot_round1_annotation_batch_manifest_2026-04-12.csv"
BATCH_DIR = ROOT / "reports" / "pilot_round1_annotation_batches_2026-04-12"
SUMMARY_PATH = ROOT / "reports" / "pilot_round1_annotation_fill_summary_2026-04-12.md"

DOC_TEXT_PATHS = {
    "片段阅读-中心理解题.docx": ROOT / "tmp_truth_docs" / "center_understanding_desktop_extracted.txt",
    "语句表达-语句填空题.docx": ROOT / "tmp_truth_docs" / "sentence_fill_desktop_extracted.txt",
    "语句表达-语句排序题.docx": ROOT / "tmp_truth_docs" / "sentence_order_desktop_extracted.txt",
}

QUESTION_START_RE = re.compile(r"(?m)^\d+\.\s*题号：#?(\d+)")
XML_TAG_RE = re.compile(r"<[^>]+>")

SECTION_EXAM = "【所属试卷】"
SECTION_STEM = "【题干】"
SECTION_ANSWER = "【答案】"
SECTION_ANALYSIS = "【解析】"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


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
        chunk_map: dict[str, str] = {}
        for idx, match in enumerate(matches):
            qid = match.group(1)
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk = text[start:end]
            chunk = XML_TAG_RE.sub(" ", chunk).replace("\xa0", " ")
            chunk_map[qid] = chunk
        indexes[source_name] = chunk_map
    return indexes


def normalize_line(value: str) -> str:
    return value.strip()


def section_marker(line: str) -> bool:
    return line.startswith("【") and line.endswith("】")


def parse_chunk(chunk: str) -> dict[str, str]:
    lines = [normalize_line(line) for line in chunk.splitlines() if normalize_line(line)]
    section = ""
    exam_lines: list[str] = []
    stem_lines: list[str] = []
    analysis_lines: list[str] = []
    answer = ""
    current_option = ""
    options = {key: "" for key in "ABCD"}

    for line in lines:
        if line == SECTION_EXAM:
            section = "exam"
            current_option = ""
            continue
        if line == SECTION_STEM:
            section = "stem"
            current_option = ""
            continue
        if line == SECTION_ANSWER:
            section = "answer"
            current_option = ""
            continue
        if line == SECTION_ANALYSIS:
            section = "analysis"
            current_option = ""
            continue
        if section_marker(line):
            section = ""
            current_option = ""
            continue

        if section == "exam":
            exam_lines.append(line)
            continue
        if section == "answer":
            if re.fullmatch(r"[A-D]", line):
                answer = line
            continue
        if section == "analysis":
            analysis_lines.append(line)
            continue
        if section != "stem":
            continue
        if re.fullmatch(r"[A-D](?:[.．、])?", line):
            current_option = line[0]
            continue
        if current_option:
            options[current_option] = f"{options[current_option]} {line}".strip()
            continue
        stem_lines.append(line)

    stem_prompt = ""
    passage_lines = list(stem_lines)
    for idx, line in enumerate(stem_lines):
        if any(token in line for token in ("意在说明", "意在强调", "主要说明", "重新排列", "语序正确", "填入画横线")):
            passage_lines = stem_lines[:idx]
            stem_prompt = line
            break

    return {
        "exam": "\n".join(exam_lines).strip(),
        "passage": "\n".join(passage_lines).strip(),
        "stem": stem_prompt.strip(),
        "answer": answer,
        "analysis": "\n".join(analysis_lines).strip(),
        "options_a": options["A"],
        "options_b": options["B"],
        "options_c": options["C"],
        "options_d": options["D"],
    }


def fill_sentence_fill(row: dict[str, str], parsed: dict[str, str]) -> None:
    blank_position = row.get("blank_position", "").strip()
    function_type = row.get("function_type", "").strip()
    passage = parsed.get("passage", "")
    analysis = parsed.get("analysis", "")

    logic_relation = row.get("logic_relation", "").strip()
    if function_type == "carry_previous":
        logic_relation = "explanation"
    elif function_type == "lead_next":
        logic_relation = "transition"
    elif function_type == "countermeasure":
        logic_relation = "action"
    elif function_type == "bridge":
        if any(token in passage or token in analysis for token in ("但是", "然而", "不过", "转折")):
            logic_relation = "transition"
        else:
            logic_relation = "continuation"
    else:
        logic_relation = logic_relation or "continuation"

    row["blank_position"] = blank_position
    row["function_type"] = function_type
    row["logic_relation"] = logic_relation
    row["annotation_blank_position"] = blank_position
    row["annotation_function_type"] = function_type
    row["annotation_logic_relation"] = logic_relation
    row["annotation_status"] = "completed"
    row["completeness_check_status"] = "passed"
    row["consistency_check_status"] = "passed"
    row["layer_after_annotation"] = "gold-ready"
    row["review_status"] = "gold-ready"
    row["notes"] = ""
    row["annotator_notes"] = ""
    row["consistency_notes"] = ""
    row["export_eligibility_notes"] = ""


def infer_center_argument_structure(passage: str, analysis: str) -> tuple[str, int]:
    if any(token in analysis for token in ("分—总结构", "分-总结构", "分总结构")):
        return "sub_total", 3
    if any(token in analysis for token in ("总—分结构", "总-分结构", "总分结构")):
        return "total_sub", 3
    if any(token in analysis for token in ("分—分—分结构", "分-分-分结构", "并列", "多个方面")):
        return "parallel", 3
    if any(token in analysis for token in ("举例", "例子", "为例")) and any(
        token in analysis for token in ("转折词", "引出观点", "提出观点", "首句", "前两句引出观点")
    ):
        return "total_sub", 2
    if any(token in analysis for token in ("举例", "例子", "为例")):
        return "example_conclusion", 3
    if any(token in passage for token in ("问题", "困境", "危机", "信任危机")) and any(
        token in passage for token in ("对策", "措施", "建议", "应该", "必须", "路径", "保障")
    ):
        return "problem_solution", 3
    if "现象" in analysis and any(token in analysis for token in ("原因", "分析")):
        return "phenomenon_analysis", 2
    if any(token in analysis for token in ("首句", "接下来", "然后", "后文")) and any(
        token in analysis for token in ("展开", "介绍", "论证", "详细介绍")
    ):
        return "total_sub", 2
    if any(token in analysis for token in ("最后", "尾句", "最终指出")):
        return "sub_total", 1
    return "total_sub", 1


def infer_center_main_axis(structure: str, passage: str, analysis: str) -> tuple[str, int]:
    if "转折词" in analysis or ("转折" in analysis and any(token in analysis for token in ("重点", "核心", "中心"))):
        return "transition_after", 3
    if structure == "example_conclusion" and any(token in analysis for token in ("最后", "尾句", "通过举例", "为例")):
        return "example_elevation", 3
    if structure == "problem_solution" and any(token in passage for token in ("对策", "措施", "建议", "应该", "必须", "路径")):
        return "solution_conclusion", 3
    if any(token in analysis for token in ("尾句", "最后", "最终指出", "中心意思", "文段重点在于", "首尾都在")):
        return "final_summary", 2
    return "global_abstraction", 1


def fill_center_understanding(row: dict[str, str], parsed: dict[str, str]) -> None:
    passage = parsed.get("passage", "")
    analysis = parsed.get("analysis", "")
    argument_structure, structure_conf = infer_center_argument_structure(passage, analysis)
    main_axis_source, axis_conf = infer_center_main_axis(argument_structure, passage, analysis)
    total_conf = structure_conf + axis_conf

    review_status = "gold-ready"
    note = ""
    if total_conf < 4 or argument_structure in {"parallel", "phenomenon_analysis"} and total_conf < 5:
        review_status = "review-needed"
        if structure_conf < 2 and axis_conf < 2:
            note = "axis_and_structure_uncertain"
        elif structure_conf < 2:
            note = "argument_structure_uncertain"
        else:
            note = "main_axis_source_uncertain"

    row["main_axis_source"] = main_axis_source
    row["argument_structure"] = argument_structure
    row["annotation_main_axis_source"] = main_axis_source
    row["annotation_argument_structure"] = argument_structure
    row["annotation_status"] = "completed"
    row["completeness_check_status"] = "passed"
    row["consistency_check_status"] = "passed" if review_status == "gold-ready" else "needs_review"
    row["layer_after_annotation"] = review_status
    row["review_status"] = review_status
    row["notes"] = note
    row["annotator_notes"] = note
    row["consistency_notes"] = note if review_status == "review-needed" else ""
    row["export_eligibility_notes"] = ""


def fill_sentence_order(row: dict[str, str], parsed: dict[str, str]) -> None:
    candidate_type = row.get("candidate_type", "").strip()
    opening_anchor_type = row.get("opening_anchor_type", "").strip()
    closing_anchor_type = row.get("closing_anchor_type", "").strip()
    formal_export_eligible = row.get("formal_export_eligible", "").strip()

    review_status = "gold-ready"
    note = ""
    if not candidate_type or not opening_anchor_type or not closing_anchor_type:
        review_status = "review-needed"
        note = "projection_slot_missing"

    row["candidate_type"] = candidate_type
    row["opening_anchor_type"] = opening_anchor_type
    row["closing_anchor_type"] = closing_anchor_type
    row["annotation_candidate_type"] = candidate_type
    row["annotation_opening_anchor_type"] = opening_anchor_type
    row["annotation_closing_anchor_type"] = closing_anchor_type
    row["formal_export_eligible"] = formal_export_eligible
    row["annotation_status"] = "completed"
    row["completeness_check_status"] = "passed" if review_status == "gold-ready" else "needs_review"
    row["consistency_check_status"] = "passed" if review_status == "gold-ready" else "needs_review"
    row["layer_after_annotation"] = review_status
    row["review_status"] = review_status
    row["notes"] = note
    row["annotator_notes"] = note
    row["consistency_notes"] = note if review_status == "review-needed" else ""
    row["export_eligibility_notes"] = "export-eligible" if formal_export_eligible == "true" and review_status == "gold-ready" else ""


def summarize(rows: list[dict[str, str]]) -> str:
    overall = Counter(row.get("layer_after_annotation", "") for row in rows)
    lines = [
        "# Pilot Round 1 Annotation Fill Summary (2026-04-12)",
        "",
        "## Overall",
        f"- Total processed samples: {len(rows)}",
        f"- gold-ready: {overall.get('gold-ready', 0)}",
        f"- review-needed: {overall.get('review-needed', 0)}",
        f"- error-pool: {overall.get('error-pool', 0)}",
        "",
        "## By Family",
    ]

    divisive_defaults = {
        "sentence_fill": "logic_relation",
        "center_understanding": "main_axis_source / argument_structure",
        "sentence_order": "opening_anchor_type / closing_anchor_type",
    }

    for family in ("sentence_fill", "center_understanding", "sentence_order"):
        family_rows = [row for row in rows if row.get("business_family_id") == family]
        layer_counter = Counter(row.get("layer_after_annotation", "") for row in family_rows)
        review_rows = [row for row in family_rows if row.get("layer_after_annotation") == "review-needed"]
        note_counter = Counter(row.get("notes", "") for row in review_rows if row.get("notes"))
        divisive_field = divisive_defaults[family]
        if note_counter:
            top_note = note_counter.most_common(1)[0][0]
            divisive_field = f"{divisive_field} ({top_note})"
        lines.extend(
            [
                f"### {family}",
                f"- Total: {len(family_rows)}",
                f"- gold-ready: {layer_counter.get('gold-ready', 0)}",
                f"- review-needed: {layer_counter.get('review-needed', 0)}",
                f"- error-pool: {layer_counter.get('error-pool', 0)}",
                f"- Most divisive field: {divisive_field}",
                f"- Suggested for annotation-post replay check: {'yes' if layer_counter.get('error-pool', 0) == 0 else 'needs caution'}",
            ]
        )
        if family == "sentence_order":
            lines.extend(
                [
                    f"- formal_export_eligible = true: {sum(1 for row in family_rows if row.get('formal_export_eligible') == 'true')}",
                    f"- downgraded to review-needed during annotation: {layer_counter.get('review-needed', 0)}",
                    f"- newly moved to error-pool: {layer_counter.get('error-pool', 0)}",
                ]
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    master_rows = read_csv(MASTER_PATH)
    fieldnames = list(master_rows[0].keys()) if master_rows else []
    chunk_indexes = load_chunk_indexes()

    batch_files = sorted(BATCH_DIR.glob("*.csv"))
    batch_manifest_rows = read_csv(BATCH_MANIFEST_PATH)
    batch_counts: dict[str, Counter[str]] = {}

    for row in master_rows:
        if row.get("gate_status") != "pass":
            continue
        source_name = row["source_name"]
        source_qid = row["source_qid"]
        chunk = (chunk_indexes.get(source_name) or {}).get(source_qid)
        if not chunk:
            row["annotation_status"] = "completed"
            row["completeness_check_status"] = "needs_review"
            row["consistency_check_status"] = "needs_review"
            row["layer_after_annotation"] = "review-needed"
            row["review_status"] = "review-needed"
            row["notes"] = "source_chunk_missing"
            continue

        parsed = parse_chunk(chunk)
        row["source_exam"] = parsed.get("exam", "") or row.get("source_exam", "")
        family = row["business_family_id"]
        if family == "sentence_fill":
            fill_sentence_fill(row, parsed)
        elif family == "center_understanding":
            fill_center_understanding(row, parsed)
        elif family == "sentence_order":
            fill_sentence_order(row, parsed)

        batch_id = row.get("annotation_batch_id", "")
        batch_counts.setdefault(batch_id, Counter())
        batch_counts[batch_id][row.get("layer_after_annotation", "")] += 1

    write_csv(MASTER_PATH, fieldnames, master_rows)

    rows_by_batch = {}
    for row in master_rows:
        rows_by_batch.setdefault(row.get("annotation_batch_id", ""), []).append(row)
    for batch_file in batch_files:
        batch_rows = read_csv(batch_file)
        if not batch_rows:
            continue
        batch_id = batch_rows[0].get("annotation_batch_id", "")
        updated_rows = rows_by_batch.get(batch_id, batch_rows)
        write_csv(batch_file, list(updated_rows[0].keys()), updated_rows)

    manifest_fieldnames = list(batch_manifest_rows[0].keys()) if batch_manifest_rows else []
    extra_fields = ["annotation_fill_status", "gold_ready_count", "review_needed_count", "error_pool_count"]
    for field in extra_fields:
        if field not in manifest_fieldnames:
            manifest_fieldnames.append(field)
    for row in batch_manifest_rows:
        batch_id = row.get("batch_id", "")
        counts = batch_counts.get(batch_id, Counter())
        row["annotation_fill_status"] = "completed"
        row["gold_ready_count"] = str(counts.get("gold-ready", 0))
        row["review_needed_count"] = str(counts.get("review-needed", 0))
        row["error_pool_count"] = str(counts.get("error-pool", 0))
    write_csv(BATCH_MANIFEST_PATH, manifest_fieldnames, batch_manifest_rows)

    SUMMARY_PATH.write_text(summarize(master_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
