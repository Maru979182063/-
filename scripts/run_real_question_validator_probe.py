from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.round1_generation_smoke_rerun import build_service, extract_docx_blocks  # noqa: E402
from app.schemas.item import GeneratedQuestion  # noqa: E402


RESULTS_PATH = REPORTS / "real_question_validator_probe_2026-04-13.csv"
REPORT_PATH = REPORTS / "real_question_validator_probe_2026-04-13.md"

DOCX_SPECS = (
    {
        "family": "sentence_fill",
        "question_type": "sentence_fill",
        "business_subtype": None,
        "path": Path(r"C:\Users\Maru\Desktop\语句表达-语句填空题.docx"),
        "sample_qids": ["2012712", "2012582", "2054222"],
    },
    {
        "family": "center_understanding",
        "question_type": "main_idea",
        "business_subtype": "center_understanding",
        "path": Path(r"C:\Users\Maru\Desktop\片段阅读-中心理解题.docx"),
        "sample_qids": ["2012694", "2012698", "2054046"],
    },
    {
        "family": "sentence_order",
        "question_type": "sentence_order",
        "business_subtype": None,
        "path": Path(r"C:\Users\Maru\Desktop\语句表达-语句排序题.docx"),
        "sample_qids": ["2012708", "2012710", "2012576"],
    },
)


def main() -> None:
    service = build_service()
    rows: list[dict[str, str]] = []

    for spec in DOCX_SPECS:
        blocks = extract_docx_blocks(spec["path"])
        for qid in spec["sample_qids"]:
            block = blocks.get(qid)
            if not block:
                rows.append(
                    {
                        "family": spec["family"],
                        "question_type": spec["question_type"],
                        "business_subtype": spec["business_subtype"] or "",
                        "source_doc": spec["path"].name,
                        "qid": qid,
                        "validator_status": "missing_block",
                        "passed": "false",
                        "score": "0",
                        "errors": "missing_docx_block",
                        "warnings": "",
                        "notes": "question block not found in source docx",
                        "stem": "",
                        "answer": "",
                    }
                )
                continue
            rows.append(_probe_single(service=service, spec=spec, qid=qid, block=block))

    _write_results(rows)
    _write_report(rows)


def _probe_single(
    *,
    service,
    spec: dict[str, Any],
    qid: str,
    block: dict[str, Any],
) -> dict[str, str]:
    source_question = _parse_real_question_block(block["lines"], family=spec["family"])
    generated_question = _build_generated_question(
        family=spec["family"],
        question_type=spec["question_type"],
        business_subtype=spec["business_subtype"],
        source_question=source_question,
    )
    material_text = str(source_question.get("passage") or "")
    analyzer = getattr(service, "source_question_analyzer", None)
    source_question_analysis = {}
    if analyzer is not None:
        try:
            source_question_analysis = analyzer.analyze(
                source_question=source_question,
                question_type=spec["question_type"],
                business_subtype=spec["business_subtype"],
            )
        except Exception as exc:  # noqa: BLE001
            source_question_analysis = {"analysis_error": f"{exc.__class__.__name__}:{exc}"}

    validation = service.validator.validate(
        question_type=spec["question_type"],
        business_subtype=spec["business_subtype"],
        generated_question=generated_question,
        material_text=material_text,
        original_material_text=material_text,
        material_source={},
        validator_contract={},
        difficulty_fit=None,
        source_question=source_question,
        source_question_analysis=source_question_analysis,
        resolved_slots={},
        control_logic={},
    )

    notes = []
    if spec["family"] == "sentence_order":
        notes.append(f"original_sentences={len(generated_question.original_sentences)}")
        notes.append(f"correct_order={generated_question.correct_order}")
    if spec["family"] == "sentence_fill":
        notes.append(f"blank_marker_present={_has_blank_marker(material_text)}")
    return {
        "family": spec["family"],
        "question_type": spec["question_type"],
        "business_subtype": spec["business_subtype"] or "",
        "source_doc": spec["path"].name,
        "qid": qid,
        "validator_status": validation.validation_status,
        "passed": str(validation.passed).lower(),
        "score": str(validation.score),
        "errors": _json_compact(validation.errors),
        "warnings": _json_compact(validation.warnings),
        "notes": "; ".join(notes),
        "stem": _clip(generated_question.stem),
        "answer": generated_question.answer,
    }


def _build_generated_question(
    *,
    family: str,
    question_type: str,
    business_subtype: str | None,
    source_question: dict[str, Any],
) -> GeneratedQuestion:
    original_sentences: list[str] = []
    correct_order: list[int] = []
    if family == "sentence_order":
        original_sentences = _extract_sentence_order_units(str(source_question.get("passage") or ""))
        answer_letter = str(source_question.get("answer") or "").strip().upper()
        option_value = str((source_question.get("options") or {}).get(answer_letter) or "")
        correct_order = [int(ch) for ch in option_value if ch.isdigit()]
    return GeneratedQuestion(
        question_type=question_type,
        business_subtype=business_subtype,
        stem=str(source_question.get("stem") or ""),
        original_sentences=original_sentences,
        correct_order=correct_order,
        options={key: str(value or "") for key, value in (source_question.get("options") or {}).items()},
        answer=str(source_question.get("answer") or ""),
        analysis=str(source_question.get("analysis") or ""),
        metadata={"probe": "real_question_validator"},
    )


def _parse_real_question_block(lines: list[str], *, family: str) -> dict[str, Any]:
    clean_lines = [str(line or "").strip() for line in lines if str(line or "").strip()]
    question_lines = _section_lines(clean_lines, "【题干】", ("【答案】",))
    answer_lines = _section_lines(clean_lines, "【答案】", ("【解析】",))
    analysis_lines = _section_lines(
        clean_lines,
        "【解析】",
        ("【出处】", "【解析视频】", "【正确率】", "【易错项】", "【考点】"),
    )
    answer = _extract_answer(answer_lines)

    if family in {"sentence_fill", "center_understanding"}:
        stem, passage, options = _parse_passage_question_parts(question_lines)
        return {
            "passage": passage,
            "stem": stem,
            "options": options,
            "answer": answer,
            "analysis": "\n".join(analysis_lines).strip(),
        }

    stem, numbered_lines, options = _parse_sentence_order_parts(question_lines)
    return {
        "passage": "\n".join(numbered_lines).strip(),
        "stem": stem,
        "options": options,
        "answer": answer,
        "analysis": "\n".join(analysis_lines).strip(),
    }


def _section_lines(lines: list[str], start_marker: str, end_markers: tuple[str, ...]) -> list[str]:
    start = None
    for idx, line in enumerate(lines):
        if line.startswith(start_marker):
            start = idx
            break
    if start is None:
        return []
    result: list[str] = []
    first = lines[start][len(start_marker) :].strip()
    if first:
        result.append(first)
    for line in lines[start + 1 :]:
        if any(line.startswith(marker) for marker in end_markers):
            break
        result.append(line)
    return result


def _extract_answer(answer_lines: list[str]) -> str:
    joined = "\n".join(answer_lines)
    match = re.search(r"\b([A-D])\b", joined)
    return match.group(1) if match else ""


def _parse_passage_question_parts(question_lines: list[str]) -> tuple[str, str, dict[str, str]]:
    joined = "\n".join(question_lines)
    stem_match = re.search(
        r"(填入画横线部分最恰当的一句是\(\s*\s*\)\。?|填入划横线部分最恰当的一句是\(\s*\s*\)\。?|这段文字意在说明\(\s*\s*\)\。?|这段文字旨在说明\(\s*\s*\)\。?)",
        joined,
    )
    if not stem_match:
        return "", joined.strip(), {key: "" for key in "ABCD"}
    stem = stem_match.group(1).strip()
    passage = joined[: stem_match.start()].strip()
    option_blob = joined[stem_match.end() :].strip()
    options = _parse_options_blob(option_blob)
    return stem, passage, options


def _parse_sentence_order_parts(question_lines: list[str]) -> tuple[str, list[str], dict[str, str]]:
    numbered_lines: list[str] = []
    stem = ""
    option_blob = ""
    for line in question_lines:
        if re.match(r"^[1-6][\.、]?\s*", line):
            numbered_lines.append(line)
            continue
        if "将以上6个句子重新排列" in line:
            stem_match = re.search(r"(将以上6个句子重新排列.*?是\(\s*\s*\)\。?)", line)
            if stem_match:
                stem = stem_match.group(1).strip()
                option_blob = line[stem_match.end() :].strip()
            else:
                stem = line.strip()
            continue
        if stem and line:
            option_blob += line.strip()
    options = _parse_options_blob(option_blob)
    return stem, numbered_lines, options


def _parse_options_blob(blob: str) -> dict[str, str]:
    options = {key: "" for key in "ABCD"}
    if not blob:
        return options
    pattern = re.compile(r"([A-D])[\.、:：]?(.*?)(?=([A-D])[\.、:：]|$)")
    for match in pattern.finditer(blob):
        letter = match.group(1)
        text = match.group(2).strip()
        options[letter] = text
    return options


def _extract_sentence_order_units(passage: str) -> list[str]:
    units: list[str] = []
    for raw_line in (passage or "").splitlines():
        line = raw_line.strip()
        match = re.match(r"^([1-6])[\.、]?\s*(.+)$", line)
        if match:
            units.append(match.group(2).strip())
    return units


def _has_blank_marker(text: str) -> bool:
    return any(token in (text or "") for token in ("____", "___", "( )", "（ ）", "画横线", "划横线"))


def _write_results(rows: list[dict[str, str]]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "family",
        "question_type",
        "business_subtype",
        "source_doc",
        "qid",
        "validator_status",
        "passed",
        "score",
        "errors",
        "warnings",
        "notes",
        "stem",
        "answer",
    ]
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(rows: list[dict[str, str]]) -> None:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["family"]].append(row)

    lines = ["# Real Question Validator Probe", ""]
    lines.append(f"- Total samples: {len(rows)}")
    lines.append("")

    verdict_counter = Counter()
    for family in ("sentence_fill", "center_understanding", "sentence_order"):
        family_rows = grouped.get(family, [])
        passed = sum(1 for row in family_rows if row["passed"] == "true")
        verdict_counter[family] = passed
        lines.append(f"## {family}")
        lines.append(f"- Passed: {passed}/{len(family_rows)}")
        error_counter = Counter()
        for row in family_rows:
            for err in json.loads(row["errors"] or "[]"):
                error_counter[err] += 1
        if error_counter:
            lines.append("- Top errors:")
            for err, count in error_counter.most_common(5):
                lines.append(f"  - `{err}` x {count}")
        lines.append("")
        for row in family_rows:
            lines.append(
                f"- `{row['qid']}` passed={row['passed']} score={row['score']} stem={row['stem']}"
            )
            if row["errors"] and row["errors"] != "[]":
                lines.append(f"  - errors: {row['errors']}")
            if row["warnings"] and row["warnings"] != "[]":
                lines.append(f"  - warnings: {row['warnings']}")
            if row["notes"]:
                lines.append(f"  - notes: {row['notes']}")
        lines.append("")

    lines.append("## Summary")
    lines.append(
        f"- Pass counts by family: {json.dumps(dict(verdict_counter), ensure_ascii=False)}"
    )
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _clip(text: str, limit: int = 80) -> str:
    text = str(text or "").strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


if __name__ == "__main__":
    main()
