from __future__ import annotations

import csv
import json
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
DESKTOP = Path.home() / "Desktop"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
if str(PROMPT_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROMPT_SERVICE_ROOT))

from app.services.text_readability import normalize_extracted_lines, normalize_readable_text  # noqa: E402


DATE_TAG = "2026-04-14"
UNIFIED_CSV = REPORTS / f"truth_distillation_assets_{DATE_TAG}.csv"
UNIFIED_JSONL = REPORTS / f"truth_distillation_assets_{DATE_TAG}.jsonl"
SF_CSV = REPORTS / f"truth_distillation_assets_sentence_fill_{DATE_TAG}.csv"
CU_CSV = REPORTS / f"truth_distillation_assets_center_understanding_{DATE_TAG}.csv"
SO_CSV = REPORTS / f"truth_distillation_assets_sentence_order_{DATE_TAG}.csv"
SUMMARY_MD = REPORTS / f"truth_distillation_summary_{DATE_TAG}.md"

DOCX_SPECS = (
    {
        "family": "sentence_fill",
        "path": DESKTOP / "语句表达-语句填空题.docx",
    },
    {
        "family": "center_understanding",
        "path": DESKTOP / "片段阅读-中心理解题.docx",
    },
    {
        "family": "sentence_order",
        "path": DESKTOP / "语句表达-语句排序题.docx",
    },
)

COMMON_FIELDS = [
    "sample_id",
    "qid",
    "family",
    "source_doc",
    "source_exam",
    "stem",
    "passage",
    "options_json",
    "answer",
    "analysis",
    "correct_rate",
    "easy_wrong_option",
    "exam_tags",
]

FAMILY_FIELDS = {
    "sentence_fill": [
        "blank_position",
        "function_type_guess",
        "logic_relation_guess",
        "correct_answer_shape",
        "distractor_modes",
    ],
    "center_understanding": [
        "main_axis_source_guess",
        "argument_structure_guess",
        "correct_option_level_guess",
        "distractor_modes",
    ],
    "sentence_order": [
        "opener_type_guess",
        "closer_type_guess",
        "binding_pairs_guess",
        "chain_features",
        "distractor_pattern_guess",
    ],
}


def main() -> None:
    all_rows: list[dict[str, str]] = []
    family_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    family_stats: dict[str, Counter[str]] = defaultdict(Counter)

    for spec in DOCX_SPECS:
        blocks = extract_docx_blocks(spec["path"])
        for qid, block in blocks.items():
            row = parse_block(family=spec["family"], source_doc=spec["path"].name, qid=qid, lines=block["lines"])
            all_rows.append(row)
            family_rows[spec["family"]].append(row)
            collect_stats(family_stats[spec["family"]], row)

    write_csv(UNIFIED_CSV, all_rows, build_unified_fields())
    write_jsonl(UNIFIED_JSONL, all_rows)
    write_csv(SF_CSV, family_rows["sentence_fill"], build_fields_for_family("sentence_fill"))
    write_csv(CU_CSV, family_rows["center_understanding"], build_fields_for_family("center_understanding"))
    write_csv(SO_CSV, family_rows["sentence_order"], build_fields_for_family("sentence_order"))
    write_summary(all_rows=all_rows, family_rows=family_rows, family_stats=family_stats)


def extract_docx_blocks(path: Path) -> dict[str, dict[str, list[str]]]:
    with zipfile.ZipFile(path) as zf:
        root = ET.fromstring(zf.read("word/document.xml"))
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraph_pairs: list[tuple[str, str]] = []
    for para in root.findall(".//w:p", ns):
        text = "".join((node.text or "") for node in para.findall(".//w:t", ns)).strip()
        cleaned = normalize_readable_text(text)
        if cleaned:
            paragraph_pairs.append((text, cleaned))

    header_pattern = re.compile(r"^\d+\.\s*题号[:：]#?(\d+)$")
    blocks: dict[str, dict[str, list[str]]] = {}
    current_qid: str | None = None
    current_lines: list[str] = []
    for _, line in paragraph_pairs:
        match = header_pattern.match(line)
        if match:
            if current_qid:
                blocks[current_qid] = {"lines": current_lines[:]}
            current_qid = match.group(1)
            current_lines = []
            continue
        if current_qid:
            current_lines.append(line)
    if current_qid:
        blocks[current_qid] = {"lines": current_lines[:]}
    return blocks


def parse_block(*, family: str, source_doc: str, qid: str, lines: list[str]) -> dict[str, str]:
    clean_lines = normalize_extracted_lines(lines)
    source_exam = first_section_value(clean_lines, "【所属试卷】")
    question_lines = section_lines(clean_lines, "【题干】", ("【答案】",))
    answer_lines = section_lines(clean_lines, "【答案】", ("【解析】",))
    analysis_lines = section_lines(clean_lines, "【解析】", ("【出处】", "【文段出处】", "【解析视频】", "【正确率】", "【易错项】", "【考点】"))
    correct_rate = first_section_value(clean_lines, "【正确率】")
    easy_wrong_option = first_section_value(clean_lines, "【易错项】")
    exam_tags = first_section_value(clean_lines, "【考点】")

    if family == "sentence_order":
        stem, passage, options = parse_sentence_order_question(question_lines)
    else:
        stem, passage, options = parse_standard_question(question_lines)
    answer = extract_answer(answer_lines)
    analysis = "\n".join(analysis_lines).strip()

    row: dict[str, str] = {
        "sample_id": f"truth.{family}.{qid}",
        "qid": qid,
        "family": family,
        "source_doc": source_doc,
        "source_exam": source_exam,
        "stem": stem,
        "passage": passage,
        "options_json": json.dumps(options, ensure_ascii=False),
        "answer": answer,
        "analysis": analysis,
        "correct_rate": correct_rate,
        "easy_wrong_option": easy_wrong_option,
        "exam_tags": exam_tags,
    }
    row.update(derive_family_fields(family=family, row=row, options=options))
    return row


def section_lines(lines: list[str], start_marker: str, end_markers: tuple[str, ...]) -> list[str]:
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


def first_section_value(lines: list[str], marker: str) -> str:
    values = section_lines(lines, marker, tuple())
    return values[0] if values else ""


def parse_standard_question(question_lines: list[str]) -> tuple[str, str, dict[str, str]]:
    stem = ""
    stem_idx = -1
    stem_pattern = re.compile(
        r"^(?:填入[画划]横线部分最恰当的一句是[（(].*[)）]。?|"
        r"这段文字(?:旨在|意在|主要|重在|主要是)?(?:说明|强调|讨论|介绍)[（(].*[)）]。?|"
        r"这段文字主要说明的是[（(].*[)）]。?)$"
    )
    for idx, line in enumerate(question_lines):
        if stem_pattern.match(line.strip()):
            stem = line.strip()
            stem_idx = idx
            break
    if stem_idx < 0:
        joined = "\n".join(question_lines).strip()
        return "", joined, {key: "" for key in "ABCD"}

    passage = "\n".join(question_lines[:stem_idx]).strip()
    option_lines = [line for line in question_lines[stem_idx + 1 :] if line.strip()]
    options = parse_options(option_lines)
    return stem, passage, options


def parse_sentence_order_question(question_lines: list[str]) -> tuple[str, str, dict[str, str]]:
    numbered_lines = [
        line
        for line in question_lines
        if re.match(r"^(?:[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]|\d{1,2}(?:[\.、．])?)", line)
    ]
    stem = ""
    option_lines: list[str] = []
    for idx, line in enumerate(question_lines):
        if "将以上" in line and "重新排列" in line:
            stem = line.strip()
            option_lines = question_lines[idx + 1 :]
            break
    options = parse_options(option_lines)
    return stem, "\n".join(numbered_lines).strip(), options


def parse_options(lines: list[str]) -> dict[str, str]:
    options = {key: "" for key in ("A", "B", "C", "D")}
    inline_blob = "\n".join(lines).strip()
    inline_matches = list(
        re.finditer(r"([A-D])[\.、．:：]\s*(.*?)(?=(?:[A-D][\.、．:：])|$)", inline_blob, re.S)
    )
    if len(inline_matches) >= 2:
        for match in inline_matches:
            options[match.group(1)] = normalize_readable_text(match.group(2))
        return options

    option_pattern = re.compile(r"^([A-D])[\.、．:：\s]+(.+)$")
    explicit = [(idx, option_pattern.match(line)) for idx, line in enumerate(lines)]
    matches = [(idx, m.group(1), m.group(2).strip()) for idx, m in explicit if m]
    if matches:
        for pos, (idx, letter, text) in enumerate(matches):
            end = matches[pos + 1][0] if pos + 1 < len(matches) else len(lines)
            chunk = [text] + [line for line in lines[idx + 1 : end] if not line.startswith("【")]
            options[letter] = "\n".join(item for item in chunk if item).strip()
        return options
    for match in re.finditer(r"([A-D])[\.、．:：]\s*(.*?)(?=(?:[A-D][\.、．:：])|$)", inline_blob, re.S):
        options[match.group(1)] = normalize_readable_text(match.group(2))
    return options


def extract_answer(answer_lines: list[str]) -> str:
    match = re.search(r"\b([A-D])\b", "\n".join(answer_lines))
    return match.group(1) if match else ""


def derive_family_fields(*, family: str, row: dict[str, str], options: dict[str, str]) -> dict[str, str]:
    if family == "sentence_fill":
        return derive_sentence_fill_fields(row=row, options=options)
    if family == "center_understanding":
        return derive_center_understanding_fields(row=row, options=options)
    return derive_sentence_order_fields(row=row, options=options)


def derive_sentence_fill_fields(*, row: dict[str, str], options: dict[str, str]) -> dict[str, str]:
    tags = row["exam_tags"]
    analysis = row["analysis"]
    blank_position = "unknown"
    if "开头" in tags or "横线在开头" in analysis:
        blank_position = "opening"
    elif "结尾" in tags or "横线在结尾" in analysis:
        blank_position = "ending"
    elif "中间" in tags or "横线在中间" in analysis:
        blank_position = "middle"

    function_type = "unknown"
    if "承上启下" in tags or "承上启下" in analysis:
        function_type = "bridge"
    elif "总结前文" in tags or "总结前文" in analysis:
        function_type = "summary"
    elif "启下" in tags or "启下" in analysis:
        function_type = "lead_next"
    elif "解释说明" in tags or "解释说明" in analysis:
        function_type = "carry_previous"
    elif "提问" in row["stem"] or "问" in row["stem"]:
        function_type = "topic_intro"

    logic_relation = "unknown"
    if "承上启下" in analysis:
        logic_relation = "transition"
    elif "总结前文" in analysis:
        logic_relation = "summary"
    elif "解释说明" in analysis:
        logic_relation = "explanation"
    elif "启下" in analysis:
        logic_relation = "continuation"

    correct_text = options.get(row["answer"], "")
    distractor_modes = ",".join(sorted(detect_distractor_modes(row["analysis"])))
    return {
        "blank_position": blank_position,
        "function_type_guess": function_type,
        "logic_relation_guess": logic_relation,
        "correct_answer_shape": classify_answer_shape(correct_text),
        "distractor_modes": distractor_modes,
    }


def derive_center_understanding_fields(*, row: dict[str, str], options: dict[str, str]) -> dict[str, str]:
    analysis = row["analysis"]
    tags = row["exam_tags"]

    main_axis = "unknown"
    if "转折" in tags or "转折" in analysis:
        main_axis = "transition_after"
    elif "对策" in tags or "对策" in analysis:
        main_axis = "solution_conclusion"
    elif "尾句" in analysis or "最后" in analysis or "结尾" in analysis:
        main_axis = "final_summary"
    elif "主题词" in tags or "中心句" in analysis or "中心句" in tags:
        main_axis = "global_abstraction"

    arg_structure = "unknown"
    if "总分总" in tags or "总分总" in analysis:
        arg_structure = "total_sub_total"
    elif "总分" in tags or "总分" in analysis:
        arg_structure = "total_sub"
    elif "分总" in tags or "分总" in analysis:
        arg_structure = "sub_total"
    elif "并列" in tags or "并列" in analysis:
        arg_structure = "parallel"
    elif "对策" in tags or "问题" in analysis and "对策" in analysis:
        arg_structure = "problem_solution"

    correct_text = options.get(row["answer"], "")
    level = "unknown"
    if len(correct_text) >= 28 or any(token in correct_text for token in ("说明", "表明", "体现", "强调")):
        level = "global_abstraction"
    elif any(token in correct_text for token in ("启示", "应当", "需要", "必须")):
        level = "policy_or_action"
    else:
        level = "moderate_abstraction"

    return {
        "main_axis_source_guess": main_axis,
        "argument_structure_guess": arg_structure,
        "correct_option_level_guess": level,
        "distractor_modes": ",".join(sorted(detect_distractor_modes(analysis))),
    }


def derive_sentence_order_fields(*, row: dict[str, str], options: dict[str, str]) -> dict[str, str]:
    analysis = row["analysis"]
    answer_option = options.get(row["answer"], "")
    binding_clues = []
    for token, label in (
        ("指代", "reference"),
        ("关联词", "connector"),
        ("共同信息", "shared_info"),
        ("观点+解释", "claim_explanation"),
        ("日常逻辑顺序", "everyday_logic"),
        ("时间顺序", "time_order"),
        ("背景引入", "background_opening"),
        ("提出观点", "claim_opening"),
    ):
        if token in row["exam_tags"] or token in analysis:
            binding_clues.append(label)

    opener = "unknown"
    if "背景引入" in row["exam_tags"] or "背景引入" in analysis:
        opener = "background_opening"
    elif "提出观点" in row["exam_tags"] or "观点" in analysis:
        opener = "claim_opening"
    elif "非首句特征" in row["exam_tags"]:
        opener = "nonfirst_rule_driven"

    closer = "unknown"
    if "总结" in analysis or "结尾" in analysis or "最后" in analysis:
        closer = "summary_closure"
    elif "结果" in analysis:
        closer = "result_closure"

    distractor = "adjacent_swap"
    if len(re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", answer_option)) >= 6:
        distractor = "full_permutation_option"
    return {
        "opener_type_guess": opener,
        "closer_type_guess": closer,
        "binding_pairs_guess": ",".join(binding_clues),
        "chain_features": ",".join(binding_clues),
        "distractor_pattern_guess": distractor,
    }


def classify_answer_shape(text: str) -> str:
    text = str(text or "").strip()
    if not text:
        return "unknown"
    if "____" in text:
        return "contains_blank"
    if len(text) <= 8:
        return "short_phrase"
    if text.endswith("？") or text.endswith("?"):
        return "question_sentence"
    if "；" in text or ";" in text:
        return "compound_clause"
    if text.endswith("。") or "，" in text:
        return "full_sentence"
    return "clause"


def detect_distractor_modes(text: str) -> set[str]:
    modes = set()
    for token, label in (
        ("无中生有", "fabricated"),
        ("片面", "partial"),
        ("偷换", "scope_shift"),
        ("话题不符", "topic_mismatch"),
        ("文意不符", "meaning_mismatch"),
        ("表述绝对", "overstatement"),
        ("无关", "irrelevant"),
        ("衔接不当", "cohesion_mismatch"),
        ("位置不符", "position_mismatch"),
        ("逻辑不当", "logic_mismatch"),
    ):
        if token in text:
            modes.add(label)
    return modes


def collect_stats(counter: Counter[str], row: dict[str, str]) -> None:
    if row["family"] == "sentence_fill":
        counter[f"blank_position:{row['blank_position']}"] += 1
        counter[f"function_type_guess:{row['function_type_guess']}"] += 1
        counter[f"logic_relation_guess:{row['logic_relation_guess']}"] += 1
    elif row["family"] == "center_understanding":
        counter[f"main_axis_source_guess:{row['main_axis_source_guess']}"] += 1
        counter[f"argument_structure_guess:{row['argument_structure_guess']}"] += 1
    else:
        for feature in filter(None, row["chain_features"].split(",")):
            counter[f"chain_feature:{feature}"] += 1


def build_unified_fields() -> list[str]:
    fields = COMMON_FIELDS[:]
    for family in ("sentence_fill", "center_understanding", "sentence_order"):
        for field in FAMILY_FIELDS[family]:
            if field not in fields:
                fields.append(field)
    return fields


def build_fields_for_family(family: str) -> list[str]:
    return COMMON_FIELDS + FAMILY_FIELDS[family]


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_summary(
    *,
    all_rows: list[dict[str, str]],
    family_rows: dict[str, list[dict[str, str]]],
    family_stats: dict[str, Counter[str]],
) -> None:
    lines = [
        f"# Truth Distillation Summary ({DATE_TAG})",
        "",
        f"- 总真题数：{len(all_rows)}",
        f"- sentence_fill：{len(family_rows['sentence_fill'])}",
        f"- center_understanding：{len(family_rows['center_understanding'])}",
        f"- sentence_order：{len(family_rows['sentence_order'])}",
        "",
        "## sentence_fill",
        "- 可直接蒸出的主字段：`blank_position / function_type / logic_relation / correct_rate / easy_wrong_option / exam_tags`",
        *format_top_stats(family_stats["sentence_fill"], prefix_order=("blank_position:", "function_type_guess:", "logic_relation_guess:")),
        "",
        "## center_understanding",
        "- 可直接蒸出的主字段：`main_axis_source / argument_structure / correct_option_level / correct_rate / easy_wrong_option / exam_tags`",
        *format_top_stats(family_stats["center_understanding"], prefix_order=("main_axis_source_guess:", "argument_structure_guess:")),
        "",
        "## sentence_order",
        "- 可直接蒸出的主字段：`opener_type / closer_type / binding_pairs / chain_features / correct_rate / easy_wrong_option / exam_tags`",
        *format_top_stats(family_stats["sentence_order"], prefix_order=("chain_feature:",)),
        "",
        "## 输出文件",
        f"- `{UNIFIED_CSV.name}`",
        f"- `{UNIFIED_JSONL.name}`",
        f"- `{SF_CSV.name}`",
        f"- `{CU_CSV.name}`",
        f"- `{SO_CSV.name}`",
    ]
    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def format_top_stats(counter: Counter[str], *, prefix_order: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for prefix in prefix_order:
        subset = [(key[len(prefix) :], count) for key, count in counter.items() if key.startswith(prefix)]
        subset.sort(key=lambda item: (-item[1], item[0]))
        top = " / ".join(f"`{name}`={count}" for name, count in subset[:6]) if subset else "无"
        lines.append(f"- {prefix[:-1]} top：{top}")
    return lines


if __name__ == "__main__":
    main()
