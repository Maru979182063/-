from __future__ import annotations

import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROMPT_ROOT = ROOT / "prompt_skeleton_service"
if str(PROMPT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROMPT_ROOT))

from app.schemas.question import SourceQuestionPayload
from app.services.delivery_service import build_center_understanding_export_view
from app.services.delivery_service import evaluate_formal_export_policy
from app.services.sentence_fill_protocol import project_sentence_fill_strict_export_view
from app.services.sentence_fill_protocol import sentence_fill_default_slot
from app.services.sentence_order_protocol import project_sentence_order_strict_export_view
from app.services.source_question_analyzer import SourceQuestionAnalyzer


INPUT_MANIFEST = ROOT / "reports" / "pilot_round1_sample_manifest_2026-04-12.csv"
OUTPUT_MANIFEST = ROOT / "reports" / "pilot_round1_sample_manifest_gated_2026-04-12.csv"
BLOCKED_POOL = ROOT / "reports" / "pilot_round1_blocked_pool_2026-04-12.csv"
ANNOTATION_POOL = ROOT / "reports" / "pilot_round1_annotation_candidate_pool_2026-04-12.csv"
SUMMARY_REPORT = ROOT / "reports" / "pilot_round1_gate_summary_2026-04-12.md"

DOC_TEXT_PATHS = {
    "\u7247\u6bb5\u9605\u8bfb-\u4e2d\u5fc3\u7406\u89e3\u9898.docx": ROOT / "tmp_truth_docs" / "center_understanding_desktop_extracted.txt",
    "\u8bed\u53e5\u8868\u8fbe-\u8bed\u53e5\u586b\u7a7a\u9898.docx": ROOT / "tmp_truth_docs" / "sentence_fill_desktop_extracted.txt",
    "\u8bed\u53e5\u8868\u8fbe-\u8bed\u53e5\u6392\u5e8f\u9898.docx": ROOT / "tmp_truth_docs" / "sentence_order_desktop_extracted.txt",
}

QUESTION_START_RE = re.compile(r"(?m)^\d+\.\s*\u9898\u53f7\uff1a#?(\d+)")
XML_TAG_RE = re.compile(r"<[^>]+>")
OPTION_LINE_RE = re.compile(r"^([A-D])(?:[.\u3001\uff0e])?$")
ANSWER_RE = re.compile(r"\b([A-D])\b")

SECTION_EXAM = "\u3010\u6240\u5c5e\u8bd5\u5377\u3011"
SECTION_STEM = "\u3010\u9898\u5e72\u3011"
SECTION_ANSWER = "\u3010\u7b54\u6848\u3011"
SECTION_ANALYSIS = "\u3010\u89e3\u6790\u3011"

PASS = "pass"
BLOCKED = "blocked"


def load_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


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
            chunk = text[start:end]
            chunk = XML_TAG_RE.sub(" ", chunk).replace("\xa0", " ")
            chunks[qid] = chunk
        indexes[source_name] = chunks
    return indexes


def normalize_line(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").strip()


def section_marker(line: str) -> bool:
    return line.startswith("\u3010") and line.endswith("\u3011")


def is_stem_prompt_line(line: str, family: str) -> bool:
    if family == "sentence_fill":
        return any(token in line for token in ("\u586b\u5165", "\u6700\u6070\u5f53\u7684\u4e00\u53e5", "\u6700\u6070\u5f53\u7684\u4e00\u9879"))
    if family == "center_understanding":
        return any(
            token in line
            for token in (
                "\u610f\u5728\u8bf4\u660e",
                "\u610f\u5728\u5f3a\u8c03",
                "\u4e3b\u8981\u8bf4\u660e",
                "\u4e3b\u8981\u5f3a\u8c03",
                "\u4e2d\u5fc3\u7406\u89e3",
                "\u4e3b\u65e8",
                "\u6807\u9898",
                "\u7406\u89e3\u6b63\u786e",
            )
        )
    if family == "sentence_order":
        return any(token in line for token in ("\u91cd\u65b0\u6392\u5217", "\u8bed\u5e8f\u6b63\u786e", "\u6392\u5e8f"))
    return False


def parse_source_question(chunk: str, family: str) -> tuple[SourceQuestionPayload, str]:
    lines = [normalize_line(line) for line in chunk.splitlines() if normalize_line(line)]
    section = ""
    exam_lines: list[str] = []
    stem_block_lines: list[str] = []
    analysis_lines: list[str] = []
    options = {key: "" for key in "ABCD"}
    answer = None
    current_option = ""

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
            answer_match = ANSWER_RE.search(line)
            if answer_match is not None:
                answer = answer_match.group(1)
            continue
        if section == "analysis":
            analysis_lines.append(line)
            continue
        if section != "stem":
            continue

        option_match = OPTION_LINE_RE.match(line)
        if option_match is not None:
            current_option = option_match.group(1)
            continue
        if current_option:
            options[current_option] = f"{options[current_option]} {line}".strip()
            continue
        stem_block_lines.append(line)

    prompt_index = next((idx for idx, line in enumerate(stem_block_lines) if is_stem_prompt_line(line, family)), -1)
    if prompt_index >= 0:
        passage_lines = stem_block_lines[:prompt_index]
        stem = stem_block_lines[prompt_index]
    elif stem_block_lines:
        passage_lines = stem_block_lines[:-1]
        stem = stem_block_lines[-1]
    else:
        passage_lines = []
        stem = ""
    passage = "\n".join(passage_lines).strip() or "\n".join(stem_block_lines).strip()
    analysis = "\n".join(analysis_lines).strip() or None

    return (
        SourceQuestionPayload(
            passage=passage or None,
            stem=stem or "",
            options=options,
            answer=answer,
            analysis=analysis,
        ),
        "\n".join(exam_lines).strip(),
    )


def truthy_text(value: Any) -> str:
    text = str(value or "").strip()
    return text


def bool_text(value: bool | None) -> str:
    if value is None:
        return ""
    return "true" if value else "false"


def blocked_main_reason(reason: str) -> str:
    text = truthy_text(reason)
    return text.split(":", 1)[0] if text else ""


def is_expected_intercept(family: str, reason: str) -> bool:
    prefix = blocked_main_reason(reason)
    if family == "sentence_order":
        return prefix.startswith("ambiguous_sentence_order_") or prefix.startswith("unknown_sentence_order_")
    if family == "sentence_fill":
        return prefix.startswith("unknown_sentence_fill_") or prefix.startswith("non_canonical_sentence_fill_")
    if family == "center_understanding":
        return prefix.startswith("title_selection_leaked_to_center_understanding_export")
    return False


def family_gate(
    *,
    row: dict[str, str],
    source_question: SourceQuestionPayload,
    analyzer: SourceQuestionAnalyzer,
) -> dict[str, Any]:
    family = row["business_family_id"]
    result: dict[str, Any] = {
        "gate_status": BLOCKED,
        "blocked_reason": "",
        "is_canonical_clean": "false",
        "formal_export_eligible": "",
        "review_status": "blocked",
        "notes": row.get("notes", ""),
        "blank_position": "",
        "function_type": "",
        "logic_relation": "",
        "main_axis_source": "",
        "argument_structure": "",
        "candidate_type": "",
        "opening_anchor_type": "",
        "closing_anchor_type": "",
    }

    if family == "sentence_fill":
        analysis = analyzer.analyze(
            source_question=source_question,
            question_type="sentence_fill",
            business_subtype="sentence_fill_selection",
        )
        structure_constraints = dict(analysis.get("structure_constraints") or {})
        type_slots: dict[str, Any] = {}
        for field_name in ("blank_position", "function_type"):
            if truthy_text(structure_constraints.get(field_name)):
                type_slots[field_name] = structure_constraints[field_name]
        type_slots["logic_relation"] = sentence_fill_default_slot("logic_relation")
        item = {
            "item_id": row["sample_id"],
            "question_type": "sentence_fill",
            "request_snapshot": {
                "type_slots": type_slots,
                "source_question_analysis": analysis,
            },
        }
        view = project_sentence_fill_strict_export_view(item) or {}
        result["blank_position"] = truthy_text(view.get("blank_position"))
        result["function_type"] = truthy_text(view.get("function_type"))
        result["logic_relation"] = truthy_text(view.get("logic_relation"))
        result["blocked_reason"] = truthy_text(view.get("blocked_reason"))
        result["notes"] = "; ".join(
            part
            for part in (
                row.get("notes", ""),
                f"analysis_mode:{analysis.get('analysis_mode') or 'rule_fallback'}",
                f"logic_relation_seeded_from_default_slot:{type_slots['logic_relation']}",
            )
            if truthy_text(part)
        )
        if view.get("status") in {"direct", "mapped"} and not result["blocked_reason"]:
            result["gate_status"] = PASS
            result["is_canonical_clean"] = "true"
            result["review_status"] = "ready_for_review"
        return result

    if family == "center_understanding":
        inferred_target = analyzer.infer_request_target(source_question)
        generated_subtype = truthy_text(inferred_target.get("business_subtype")) or "center_understanding"
        item = {
            "item_id": row["sample_id"],
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "generated_question": {"business_subtype": generated_subtype},
            "request_snapshot": {
                "business_subtype": "center_understanding",
                "question_card_id": row["question_card_id"],
            },
        }
        view = build_center_understanding_export_view(item) or {}
        result["blocked_reason"] = truthy_text(view.get("blocked_reason"))
        result["notes"] = "; ".join(
            part
            for part in (
                row.get("notes", ""),
                f"inferred_target:{generated_subtype or 'none'}",
                "main_axis_source_pending_manual_label",
                "argument_structure_pending_manual_label",
            )
            if truthy_text(part)
        )
        if view.get("status") in {"direct", "mapped"} and not result["blocked_reason"]:
            result["gate_status"] = PASS
            result["is_canonical_clean"] = "true"
            result["review_status"] = "ready_for_review"
        return result

    if family == "sentence_order":
        analysis = analyzer.analyze(
            source_question=source_question,
            question_type="sentence_order",
            business_subtype="sentence_order_selection",
        )
        structure_constraints = dict(analysis.get("structure_constraints") or {})
        type_slots: dict[str, Any] = {"candidate_type": "sentence_block_group"}
        for field_name in ("opening_anchor_type", "closing_anchor_type", "opening_rule", "closing_rule"):
            if truthy_text(structure_constraints.get(field_name)):
                type_slots[field_name] = structure_constraints[field_name]
        item = {
            "item_id": row["sample_id"],
            "question_type": "sentence_order",
            "request_snapshot": {
                "type_slots": type_slots,
                "source_question_analysis": analysis,
            },
            "material_selection": {
                "runtime_binding": {
                    "candidate_type": "ordered_unit_group",
                }
            },
        }
        view = project_sentence_order_strict_export_view(item) or {}
        policy = evaluate_formal_export_policy(
            question_type="sentence_order",
            export_target="formal_training_export",
            item=item,
        )
        result["candidate_type"] = truthy_text(view.get("candidate_type"))
        result["opening_anchor_type"] = truthy_text(view.get("opening_anchor_type"))
        result["closing_anchor_type"] = truthy_text(view.get("closing_anchor_type"))
        result["blocked_reason"] = truthy_text(view.get("blocked_reason"))
        result["formal_export_eligible"] = bool_text(bool(policy.get("allowed")))
        result["notes"] = "; ".join(
            part
            for part in (
                row.get("notes", ""),
                f"analysis_mode:{analysis.get('analysis_mode') or 'rule_fallback'}",
                f"formal_export_policy:{policy.get('status') or 'unknown'}",
                f"formal_export_policy_reason:{truthy_text(policy.get('blocked_reason')) or 'none'}",
            )
            if truthy_text(part)
        )
        if view.get("status") in {"direct", "mapped"} and not result["blocked_reason"]:
            result["gate_status"] = PASS
            result["is_canonical_clean"] = "true"
            result["review_status"] = "ready_for_review"
        return result

    result["blocked_reason"] = f"unsupported_business_family:{family}"
    result["notes"] = "; ".join(part for part in (row.get("notes", ""), result["blocked_reason"]) if truthy_text(part))
    return result


def build_blocked_pool(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    blocked_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("gate_status") != BLOCKED:
            continue
        reason = row.get("blocked_reason", "")
        family = row.get("business_family_id", "")
        blocked_rows.append(
            {
                "sample_id": row.get("sample_id", ""),
                "business_family_id": family,
                "business_subtype_id": row.get("business_subtype_id", ""),
                "source_name": row.get("source_name", ""),
                "source_batch": row.get("source_batch", ""),
                "gate_status": row.get("gate_status", ""),
                "blocked_reason": reason,
                "blocked_main_reason": blocked_main_reason(reason),
                "is_expected_intercept": bool_text(is_expected_intercept(family, reason)),
                "route_to_error_pool": "true",
                "notes": row.get("notes", ""),
            }
        )
    return blocked_rows


def build_annotation_pool(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    annotation_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("gate_status") != PASS:
            continue
        family = row.get("business_family_id", "")
        formal_export_eligible = row.get("formal_export_eligible", "")
        if family == "sentence_order":
            if formal_export_eligible == "true":
                route = "formal_annotation_and_training_export_candidate"
            else:
                route = "review_replay_error_analysis_only"
        else:
            route = "formal_annotation_candidate"
        payload = dict(row)
        payload["annotation_pool_route"] = route
        annotation_rows.append(payload)
    return annotation_rows


def summarize_family(rows: list[dict[str, str]], family: str) -> dict[str, Any]:
    family_rows = [row for row in rows if row.get("business_family_id") == family]
    blocked_rows = [row for row in family_rows if row.get("gate_status") == BLOCKED]
    blocked_reasons = Counter(blocked_main_reason(row.get("blocked_reason", "")) for row in blocked_rows if row.get("blocked_reason"))
    canonical_clean_count = sum(1 for row in family_rows if row.get("is_canonical_clean") == "true")
    formal_export_eligible_count = sum(1 for row in family_rows if row.get("formal_export_eligible") == "true")
    chain_issue_count = sum(1 for row in blocked_rows if not is_expected_intercept(family, row.get("blocked_reason", "")))

    if family == "center_understanding":
        recommended = chain_issue_count == 0 and canonical_clean_count > 0
    elif family == "sentence_fill":
        recommended = chain_issue_count <= max(2, len(family_rows) // 5) and canonical_clean_count > 0
    else:
        recommended = formal_export_eligible_count > 0 and chain_issue_count <= max(2, len(family_rows) // 4)

    return {
        "total": len(family_rows),
        "pass_count": sum(1 for row in family_rows if row.get("gate_status") == PASS),
        "blocked_count": len(blocked_rows),
        "canonical_clean_count": canonical_clean_count,
        "formal_export_eligible_count": formal_export_eligible_count,
        "blocked_reason_top": blocked_reasons.most_common(5),
        "recommended_for_formal_annotation_pool": recommended,
        "expected_blocked_count": sum(1 for row in blocked_rows if is_expected_intercept(family, row.get("blocked_reason", ""))),
        "chain_issue_blocked_count": chain_issue_count,
    }


def build_summary(rows: list[dict[str, str]]) -> str:
    total = len(rows)
    pass_count = sum(1 for row in rows if row.get("gate_status") == PASS)
    blocked_count = sum(1 for row in rows if row.get("gate_status") == BLOCKED)
    pass_rate = round((pass_count / total) * 100, 2) if total else 0.0
    blocked_rate = round((blocked_count / total) * 100, 2) if total else 0.0

    family_summaries = {
        family: summarize_family(rows, family)
        for family in ("sentence_fill", "center_understanding", "sentence_order")
    }

    blocked_rows = [row for row in rows if row.get("gate_status") == BLOCKED]
    expected_blocked = [row for row in blocked_rows if is_expected_intercept(row.get("business_family_id", ""), row.get("blocked_reason", ""))]
    chain_issue_blocked = [row for row in blocked_rows if not is_expected_intercept(row.get("business_family_id", ""), row.get("blocked_reason", ""))]

    ready_families = [family for family, summary in family_summaries.items() if summary["recommended_for_formal_annotation_pool"]]
    sentence_order_export_candidates = [
        row["sample_id"]
        for row in rows
        if row.get("business_family_id") == "sentence_order" and row.get("formal_export_eligible") == "true"
    ]

    lines = [
        "# Pilot Round 1 Gate Summary (2026-04-12)",
        "",
        "## Overall",
        f"- Total samples: {total}",
        f"- Pass count: {pass_count}",
        f"- Blocked count: {blocked_count}",
        f"- Pass rate: {pass_rate}%",
        f"- Blocked rate: {blocked_rate}%",
        "",
        "## By Family",
    ]

    for family in ("sentence_fill", "center_understanding", "sentence_order"):
        summary = family_summaries[family]
        lines.extend(
            [
                f"### {family}",
                f"- Total: {summary['total']}",
                f"- Pass: {summary['pass_count']}",
                f"- Blocked: {summary['blocked_count']}",
                f"- Canonical clean: {summary['canonical_clean_count']}",
                f"- sentence_order formal_export_eligible: {summary['formal_export_eligible_count']}" if family == "sentence_order" else "- sentence_order formal_export_eligible: n/a",
                f"- Recommended for formal annotation pool: {'yes' if summary['recommended_for_formal_annotation_pool'] else 'no'}",
                f"- Expected blocked count: {summary['expected_blocked_count']}",
                f"- Chain issue blocked count: {summary['chain_issue_blocked_count']}",
                "- Blocked reason Top N:",
            ]
        )
        if summary["blocked_reason_top"]:
            lines.extend([f"  - {reason}: {count}" for reason, count in summary["blocked_reason_top"]])
        else:
            lines.append("  - none")
        lines.append("")

    lines.extend(
        [
            "## Conclusion",
            f"- Families ready for the next formal annotation pool: {', '.join(ready_families) if ready_families else 'none'}",
            f"- Families needing upstream repair first: {', '.join(family for family in family_summaries if family not in ready_families) or 'none'}",
            f"- sentence_order formal training export candidate count: {len(sentence_order_export_candidates)}",
            f"- Expected blocked samples: {len(expected_blocked)}",
            f"- Chain issue blocked samples: {len(chain_issue_blocked)}",
            "",
            "### Blocked Interpretation",
            f"- Expected blocked samples are mainly protocol-safe intercepts such as ambiguous or unknown legacy values.",
            f"- Chain issue blocked samples point to parse, value sourcing, or export-chain gaps that still need repair before the next round.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    manifest_rows = load_manifest(INPUT_MANIFEST)
    chunk_indexes = load_chunk_indexes()
    analyzer = SourceQuestionAnalyzer(None)

    gated_rows: list[dict[str, str]] = []
    for row in manifest_rows:
        source_name = row["source_name"]
        source_qid = row["source_qid"]
        chunk = (chunk_indexes.get(source_name) or {}).get(source_qid)
        updated = dict(row)
        if not chunk:
            updated["gate_status"] = BLOCKED
            updated["blocked_reason"] = f"missing_source_chunk:{source_name}:{source_qid}"
            updated["is_canonical_clean"] = "false"
            updated["formal_export_eligible"] = ""
            updated["review_status"] = "blocked"
            updated["notes"] = "; ".join(part for part in (row.get("notes", ""), "source_chunk_missing") if truthy_text(part))
            gated_rows.append(updated)
            continue

        source_question, source_exam = parse_source_question(chunk, row["business_family_id"])
        updated["source_exam"] = source_exam or row.get("source_exam", "")

        gate_payload = family_gate(
            row=row,
            source_question=source_question,
            analyzer=analyzer,
        )
        updated.update({key: str(value) if value is not None else "" for key, value in gate_payload.items()})
        gated_rows.append(updated)

    write_csv(OUTPUT_MANIFEST, list(gated_rows[0].keys()) if gated_rows else [], gated_rows)

    blocked_rows = build_blocked_pool(gated_rows)
    write_csv(
        BLOCKED_POOL,
        [
            "sample_id",
            "business_family_id",
            "business_subtype_id",
            "source_name",
            "source_batch",
            "gate_status",
            "blocked_reason",
            "blocked_main_reason",
            "is_expected_intercept",
            "route_to_error_pool",
            "notes",
        ],
        blocked_rows,
    )

    annotation_rows = build_annotation_pool(gated_rows)
    annotation_fields = list(gated_rows[0].keys()) + ["annotation_pool_route"] if gated_rows else ["annotation_pool_route"]
    write_csv(ANNOTATION_POOL, annotation_fields, annotation_rows)

    SUMMARY_REPORT.write_text(build_summary(gated_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
