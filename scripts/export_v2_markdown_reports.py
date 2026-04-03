from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import export_v2_business_mapping_report as business
import export_v2_difficulty_and_production_html as cards
import export_v2_selection_control_html as selection


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"


def md_text(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value)
    return text.replace("|", "\\|").replace("\r\n", "\n").replace("\n", "<br>")


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    header_line = "| " + " | ".join(md_text(header) for header in headers) + " |"
    separator_line = "| " + " | ".join("---" for _ in headers) + " |"
    body_lines = [
        "| " + " | ".join(md_text(cell) for cell in row) + " |"
        for row in rows
    ]
    return "\n".join([header_line, separator_line, *body_lines])


def add_block(lines: list[str], text: str) -> None:
    if text:
        lines.append(text)
        lines.append("")


def export_business_markdown(timestamp: str, generated_at: str) -> Path:
    md_path = REPORTS_DIR / f"v2_business_mapping_report_{timestamp}.md"
    lines: list[str] = [
        "# V2 中间逻辑与题卡结构映射表",
        "",
        f"生成时间：{generated_at}",
        "",
        "用途：给业务直接查看当前 V2 版本里“中间逻辑、结构卡、题卡槽位、生成原型”之间的关系，方便按题型讨论要不要调整口径。",
        "",
        "源头文件：`card_specs/normalized/*`、`passage_service/app/services/material_pipeline_v2.py`、`prompt_skeleton_service/app/services/material_bridge_v2.py`",
        "",
        "## 一、V2 中间逻辑总览",
        "",
        md_table(["步骤", "当前实现位置", "当前逻辑", "业务理解"], business.build_logic_rows()),
        "",
    ]

    summary_rows: list[list[str]] = []
    for family_id in business.FAMILY_ORDER:
        bundle = business.family_bundle(family_id)
        summary_rows.append(
            business.build_family_summary_row(
                family_id,
                bundle["signal_layer"],
                bundle["material_registry"],
                bundle["question_card"],
                business.signal_description_map(bundle["signal_layer"]),
            )
        )

    add_block(lines, "## 二、题型总览")
    add_block(
        lines,
        md_table(
            ["业务题型", "当前 question card", "运行时绑定", "候选切片类型", "信号层规模", "结构卡/原型数量", "题卡默认槽位", "关键判断信号", "候选切片补充规则", "最终消费方式"],
            summary_rows,
        ),
    )

    add_block(lines, "## 三、输出字段口径")
    add_block(lines, md_table(["字段", "当前含义", "业务看法建议"], business.build_output_field_rows()))

    for family_id in business.FAMILY_ORDER:
        bundle = business.family_bundle(family_id)
        signal_layer = bundle["signal_layer"]
        material_registry = bundle["material_registry"]
        question_card = bundle["question_card"]
        descriptions = business.signal_description_map(signal_layer)
        mapping_rows, _ = business.build_mapping_rows(
            family_id,
            material_registry,
            question_card,
            descriptions,
        )
        archetype_rows = business.build_archetype_rows(question_card)
        runtime_text = business.runtime_binding_text(question_card.get("runtime_binding") or {})

        add_block(lines, f"## {business.FAMILY_META[family_id]['label']}")
        add_block(lines, question_card.get("description", ""))
        lines.extend(
            [
                f"- 当前 question card：`{question_card.get('card_id', '-')}`",
                f"- runtime 绑定：{runtime_text}",
                f"- required candidate types：{business.format_list(question_card.get('upstream_contract', {}).get('required_candidate_types', []))}",
                f"- required profiles：{business.format_profiles(question_card.get('upstream_contract', {}).get('required_profiles', []), descriptions)}",
                f"- validator 规则：{business.format_list((question_card.get('validator_contract') or {}).get('extension_rules', []))}",
                "",
                "### 中间结构卡映射",
                "",
            ]
        )

        for row in mapping_rows:
            index, card_info, selection_core, structures, candidate_types, required_signals, archetype_summary, override_text, observation_text = row
            card_parts = card_info.split("\n", 1)
            card_name = card_parts[0]
            card_id = card_parts[1] if len(card_parts) > 1 else ""
            lines.extend(
                [
                    f"#### {index}. {card_name}",
                    "",
                    f"- 结构卡 ID：`{card_id}`" if card_id else "- 结构卡 ID：-",
                    f"- 选材核心：{selection_core}",
                    f"- 对应篇章结构：{structures}",
                    f"- 可吃的候选切片：{candidate_types}",
                    f"- 命中门槛：{required_signals}",
                    f"- 对应生成原型：{archetype_summary}",
                    f"- 题卡覆盖槽位：{override_text}",
                ]
            )
            for line in observation_text.split("\n"):
                lines.append(f"- {line}")
            lines.append("")

        add_block(lines, "### 生成原型说明")
        add_block(lines, md_table(["原型 ID", "处理方式", "正确项逻辑", "建议分析框架"], archetype_rows))

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def export_difficulty_markdown(timestamp: str, generated_at: str) -> Path:
    md_path = REPORTS_DIR / f"v2_difficulty_control_cards_{timestamp}.md"
    payloads = cards.load_types()
    lines: list[str] = [
        "# V2 难度控制卡总表",
        "",
        f"生成时间：{generated_at}",
        "",
        "按当前题型配置整理 easy / medium / hard 难度目标范围，以及默认槽位、few-shot 策略和子题型偏置。",
        "",
        "源头文件：`prompt_skeleton_service/configs/types/*.yaml`",
        "",
        "## 一、题型总览",
        "",
        md_table(
            ["业务题型", "当前展示名", "别名", "默认制作卡", "骨架", "默认槽位", "few-shot 策略"],
            cards.difficulty_summary_rows(payloads),
        ),
        "",
    ]

    for payload in payloads:
        lines.extend(
            [
                f"## {cards.TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-'))}",
                "",
                str(payload.get("task_definition") or ""),
                "",
                f"- 默认制作卡：`{payload.get('default_pattern_id', '-')}`",
                f"- 骨架：{cards.format_dict(payload.get('skeleton'))}",
                f"- 默认槽位：{cards.format_dict(payload.get('default_slots'))}",
                f"- few-shot 策略：{cards.format_dict(payload.get('fewshot_policy'))}",
                f"- 默认 few-shot：{cards.format_dict(payload.get('default_fewshot'))}",
                "",
                "### 难度控制卡",
                "",
            ]
        )

        difficulty_rows = []
        for level in ["easy", "medium", "hard"]:
            difficulty_rows.append(
                [
                    cards.label_value(level),
                    cards.format_range_profile(payload.get("difficulty_target_profiles", {}).get(level, {})),
                ]
            )
        add_block(lines, md_table(["难度档位", "目标范围"], difficulty_rows))

        if payload.get("business_subtypes"):
            subtype_rows = []
            for subtype in payload.get("business_subtypes", []):
                subtype_rows.append(
                    [
                        subtype.get("display_name", "-"),
                        str(subtype.get("subtype_id") or "-"),
                        str(subtype.get("description") or "-"),
                        cards.format_list(subtype.get("preferred_patterns", [])),
                        cards.format_dict(subtype.get("default_slot_overrides")),
                        cards.format_dict(subtype.get("fewshot_policy")),
                    ]
                )
            add_block(lines, "### 业务子题型偏置卡")
            add_block(lines, md_table(["子题型", "subtype_id", "说明", "优先制作卡", "默认槽位覆盖", "few-shot 策略"], subtype_rows))

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def export_production_markdown(timestamp: str, generated_at: str) -> Path:
    md_path = REPORTS_DIR / f"v2_production_cards_{timestamp}.md"
    payloads = cards.load_types()
    lines: list[str] = [
        "# V2 制作卡总表",
        "",
        f"生成时间：{generated_at}",
        "",
        "按当前题型配置整理 patterns 制作卡，包括匹配条件、控制逻辑、制作逻辑、难度映射，以及 main_idea 的业务子题型偏置。",
        "",
        "源头文件：`prompt_skeleton_service/configs/types/*.yaml`",
        "",
        "## 一、制作卡总览",
        "",
        md_table(["业务题型", "制作卡数量", "默认制作卡", "骨架", "默认槽位"], cards.production_summary_rows(payloads)),
        "",
    ]

    for payload in payloads:
        lines.extend(
            [
                f"## {cards.TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-'))}",
                "",
                str(payload.get("task_definition") or ""),
                "",
                f"- 默认制作卡：`{payload.get('default_pattern_id', '-')}`",
                f"- 骨架：{cards.format_dict(payload.get('skeleton'))}",
                f"- 默认槽位：{cards.format_dict(payload.get('default_slots'))}",
                "",
                "### 制作卡",
                "",
            ]
        )

        for pattern in payload.get("patterns", []) or []:
            control_logic = pattern.get("control_logic") or {}
            generation_logic = pattern.get("generation_logic") or {}
            lines.extend(
                [
                    f"#### {pattern.get('pattern_name')} (`{pattern.get('pattern_id')}`)",
                    "",
                    f"- 适配条件：{cards.format_dict(pattern.get('match_rules'))}",
                    f"- 难度主要来源：{cards.format_scalar(control_logic.get('difficulty_source'))}",
                    f"- 选项迷惑度：{cards.format_scalar(control_logic.get('option_confusion'))}",
                    f"- 控制杠杆：{cards.format_dict(control_logic.get('control_levers'))}",
                    f"- 特殊字段：{cards.format_dict(control_logic.get('special_fields'))}",
                    f"- 制作核心：{generation_logic.get('generation_core') or '-'}",
                    f"- 处理方式：{generation_logic.get('processing_type') or '-'}",
                    f"- 正确项逻辑：{generation_logic.get('correct_logic') or '-'}",
                    f"- 高频陷阱：{cards.format_list(generation_logic.get('high_freq_traps', []))}",
                    f"- 干扰项组织：{generation_logic.get('distractor_pattern') or '-'}",
                    f"- 分析步骤：{cards.format_analysis_steps(generation_logic.get('analysis_steps'))}",
                    f"- 难度映射：{cards.flatten_difficulty_rules(pattern.get('difficulty_rules') or {})}",
                    f"- few-shot 示例：{cards.format_dict(pattern.get('fewshot_example'))}",
                    "",
                ]
            )

        if payload.get("business_subtypes"):
            subtype_rows = []
            for subtype in payload.get("business_subtypes", []):
                subtype_rows.append(
                    [
                        subtype.get("display_name", "-"),
                        str(subtype.get("subtype_id") or "-"),
                        cards.format_list(subtype.get("preferred_patterns", [])),
                        cards.format_dict(subtype.get("default_slot_overrides")),
                        cards.format_dict(subtype.get("fewshot_examples", [{}])[0] if subtype.get("fewshot_examples") else None),
                    ]
                )
            add_block(lines, "### 业务子题型制作偏置")
            add_block(lines, md_table(["子题型", "subtype_id", "优先制作卡", "默认槽位覆盖", "示例卡片"], subtype_rows))

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def export_selection_markdown(timestamp: str, generated_at: str) -> Path:
    md_path = REPORTS_DIR / f"v2_selection_control_{timestamp}.md"
    payloads = selection.load_type_payloads()
    lines: list[str] = [
        "# V2 选择控制总表",
        "",
        f"生成时间：{generated_at}",
        "",
        "这份表专门说明系统当前如何做“入口映射、槽位自动控制、制作卡选择、few-shot 选择、模板选择和复审覆盖”。",
        "",
        "源头文件：`prompt_skeleton_service/app/services/input_decoder.py`、`slot_resolver.py`、`prompt_builder.py`、`prompt_template_registry.py`、`question_generation.py`",
        "",
        "## 一、选择控制总流程",
        "",
        md_table(["步骤", "实现位置", "当前逻辑", "业务理解"], selection.FLOW_ROWS),
        "",
        "## 二、入口映射控制",
        "",
        "### question_focus 映射",
        "",
        md_table(["入口类型", "前台值", "question_type", "business_subtype", "pattern_id", "业务解释"], selection.QUESTION_FOCUS_ROWS),
        "",
        "### special_question_types 映射",
        "",
        md_table(["入口类型", "前台值", "question_type", "business_subtype", "pattern_id", "业务解释"], selection.SPECIAL_TYPE_ROWS),
        "",
        "### 难度值映射",
        "",
        md_table(["前台值", "内部 difficulty_level", "difficulty_target", "说明"], selection.DIFFICULTY_ROWS),
        "",
        "## 三、槽位自动选择控制",
        "",
        "### 合并优先级",
        "",
        md_table(
            ["优先级从低到高", "来源", "说明"],
            [
                ["1", "question_type.default_slots", "题型默认槽位，作为最基础控制口径。"],
                ["2", "business_subtype.default_slot_overrides", "如果有子题型，先用它覆盖题型默认值。"],
                ["3", "slot_schema.default", "前两层都没填时，补 slot_schema 的字段默认值。"],
                ["4", "incoming type_slots", "用户显式传入的 type_slots 最优先。"],
                ["5", "difficulty auto profile", "仅对用户没手填的槽位，根据难度自动调档。"],
            ],
        ),
        "",
        "### 随难度升高自动上调的槽位",
        "",
        md_table(["slot", "业务名", "档位顺序", "说明"], selection.ASCENDING_ROWS),
        "",
        "### 随难度升高自动下调的槽位",
        "",
        md_table(["slot", "业务名", "档位顺序", "说明"], selection.DESCENDING_ROWS),
        "",
        "## 四、制作卡选择控制",
        "",
        md_table(["选择模式", "触发条件", "当前逻辑"], selection.SELECTION_MODE_ROWS),
        "",
        "### 各题型当前选择口径",
        "",
        md_table(["业务题型", "当前展示名", "默认制作卡", "制作卡数量", "题型默认槽位", "子题型优先制作卡"], selection.build_type_pattern_rows(payloads)),
        "",
        "## 五、few-shot 与模板选择控制",
        "",
        "### few-shot 选择",
        "",
        md_table(["控制点", "实现位置", "当前逻辑"], selection.FEWSHOT_ROWS),
        "",
        "### 模板选择",
        "",
        md_table(["控制点", "实现位置", "当前逻辑"], selection.TEMPLATE_ROWS),
        "",
        "## 六、复审改题时的选择覆盖",
        "",
        md_table(["控制点", "实现位置", "当前逻辑"], selection.REVIEW_OVERRIDE_ROWS),
        "",
    ]

    for payload in payloads:
        lines.extend(
            [
                f"## {selection.TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-'))}",
                "",
                str(payload.get("task_definition") or ""),
                "",
                f"- 默认制作卡：`{payload.get('default_pattern_id', '-')}`",
                f"- 默认槽位：{selection.format_dict(payload.get('default_slots'))}",
                "",
                "### 制作卡与选择条件",
                "",
            ]
        )

        pattern_rows = []
        for pattern in payload.get("patterns", []) or []:
            pattern_rows.append(
                [
                    f"{pattern.get('pattern_name')}<br>`{pattern.get('pattern_id')}`",
                    selection.format_dict(pattern.get("match_rules")),
                    selection.format_scalar((pattern.get("control_logic") or {}).get("difficulty_source")),
                    selection.format_scalar((pattern.get("control_logic") or {}).get("option_confusion")),
                    selection.format_dict((pattern.get("control_logic") or {}).get("control_levers")),
                ]
            )
        add_block(lines, md_table(["制作卡", "match_rules 适配条件", "难度主要来源", "选项迷惑度", "控制杠杆"], pattern_rows))

        if payload.get("business_subtypes"):
            subtype_rows = []
            for subtype in payload.get("business_subtypes", []):
                subtype_rows.append(
                    [
                        subtype.get("display_name", "-"),
                        str(subtype.get("subtype_id") or "-"),
                        selection.format_list(subtype.get("preferred_patterns", [])),
                        selection.format_dict(subtype.get("default_slot_overrides")),
                    ]
                )
            add_block(lines, "### 子题型选择偏置")
            add_block(lines, md_table(["子题型", "subtype_id", "优先制作卡", "默认槽位覆盖"], subtype_rows))

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def export_index(timestamp: str, generated_at: str, files: list[Path]) -> Path:
    md_path = REPORTS_DIR / f"v2_reports_index_{timestamp}.md"
    lines = [
        "# V2 报告索引",
        "",
        f"生成时间：{generated_at}",
        "",
        "## Markdown 文件",
        "",
    ]
    for file in files:
        lines.append(f"- [{file.name}]({file.name})")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def export_markdown_reports() -> list[Path]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    files = [
        export_business_markdown(timestamp, generated_at),
        export_difficulty_markdown(timestamp, generated_at),
        export_production_markdown(timestamp, generated_at),
        export_selection_markdown(timestamp, generated_at),
    ]
    files.append(export_index(timestamp, generated_at, files))
    return files


if __name__ == "__main__":
    for file in export_markdown_reports():
        print(file)
