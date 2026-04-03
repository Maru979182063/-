from __future__ import annotations

import html
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
TYPE_CONFIG_DIR = ROOT / "prompt_skeleton_service" / "configs" / "types"
REPORTS_DIR = ROOT / "reports"

TYPE_ORDER = [
    "main_idea",
    "continuation",
    "sentence_order",
    "sentence_fill",
]

TYPE_LABELS = {
    "main_idea": "主旨/中心/标题/结构概括题",
    "continuation": "接语选择题",
    "sentence_order": "语句排序题",
    "sentence_fill": "语句填空题",
}

SLOT_LABELS = {
    "structure_type": "结构类型",
    "main_point_source": "主旨来源",
    "abstraction_level": "抽象层级",
    "coverage_requirement": "覆盖要求",
    "target_form": "目标形式",
    "title_style": "标题风格",
    "distractor_modes": "干扰项模式",
    "distractor_strength": "干扰项强度",
    "statement_visibility": "观点显性度",
    "anchor_type": "锚点类型",
    "operation_type": "操作类型",
    "target_type": "目标类型",
    "anchor_focus": "尾句落点",
    "continuation_type": "续写方向",
    "progression_mode": "推进方式",
    "ending_function": "结尾功能",
    "anchor_clarity": "锚点清晰度",
    "option_confusion": "选项迷惑度",
    "opening_anchor_type": "开头锚点类型",
    "opening_signal_strength": "开头锚点强度",
    "middle_structure_type": "中段关系",
    "local_binding_strength": "局部绑定强度",
    "closing_anchor_type": "结尾锚点类型",
    "closing_signal_strength": "结尾收束强度",
    "block_order_complexity": "板块排序复杂度",
    "blank_position": "空位位置",
    "function_type": "空位功能",
    "logic_relation": "逻辑关系",
    "context_dependency": "上下文依赖",
    "bidirectional_validation": "双向验证",
    "reference_dependency": "指代依赖",
    "fewshot_policy": "few-shot 策略",
    "preferred_patterns": "优先制作卡",
    "default_slot_overrides": "默认槽位覆盖",
    "fit_slots": "适配槽位",
    "difficulty_source": "难度主要来源",
    "control_levers": "控制杠杆",
    "special_fields": "特殊反推字段",
    "generation_core": "制作核心",
    "processing_type": "处理方式",
    "correct_logic": "正确项逻辑",
    "high_freq_traps": "高频陷阱",
    "distractor_pattern": "干扰项组织方式",
    "analysis_steps": "建议分析步骤",
    "complexity": "复杂度",
    "ambiguity": "歧义度",
    "reasoning_depth": "推理深度",
    "distractor_similarity": "干扰项相似度",
}

VALUE_LABELS = {
    "main_idea": "主旨类",
    "title_selection": "标题填入",
    "continuation": "接语选择",
    "sentence_order": "语句排序",
    "sentence_fill": "语句填空",
    "easy": "简单",
    "medium": "中等",
    "hard": "困难",
    "high": "高",
    "medium_high": "中高",
    "medium": "中",
    "low": "低",
    "none": "无",
    "whole_passage_structure": "整篇结构",
    "tail_sentence": "尾句",
    "blank_position_context": "空位上下文",
    "sentence_group_interfaces": "句群接口",
    "integrate": "整合",
    "extend": "续写",
    "reorder": "重排",
    "fill_by_function": "按功能填入",
    "core_meaning": "核心意义",
    "next_step_content": "下文内容",
    "coherent_sequence": "连贯顺序",
    "valid_bridge_sentence": "有效衔接句",
    "structure_only": "只看结构",
    "turning": "转折型",
    "progressive": "递进型",
    "contrast": "对比型",
    "multi_paragraph_hidden": "多段隐含主旨型",
    "local_paragraph": "局部段落型",
    "explicit_single_center": "显性单中心",
    "whole_passage": "整篇材料",
    "conclusion_sentence": "结论句",
    "tail_sentence": "尾句",
    "close_rephrase": "贴近改写",
    "integrated": "综合覆盖",
    "abstract_generalization": "抽象泛化",
    "central_meaning": "中心意义",
    "article_task": "文章任务",
    "title_label": "标题项",
    "structure_summary": "结构概括",
    "local_paragraph_meaning": "局部段意",
    "neutral": "中性",
    "direct_label": "直给式",
    "thematic": "主题式",
    "rhetorical": "修辞式",
    "abstract": "抽象式",
    "tail_anchor": "尾句新落点",
    "problem_exposed": "问题暴露",
    "mechanism_named": "机制点名",
    "theme_raised": "主题抬升",
    "new_pivot": "新支点",
    "judgement_given": "判断已给出",
    "macro_shift": "宏观切换",
    "branch_focus": "分支聚焦",
    "tension_retained": "张力保留",
    "method_opening": "方法开启",
    "explain": "解释",
    "countermeasure": "对策",
    "deepen_mechanism": "机制展开",
    "subtopic_expand": "分话题展开",
    "deepen_pivot": "支点深化",
    "reason_argument": "判断后论证",
    "macro_unfold": "案例到宏观",
    "focus_branch": "分支聚焦",
    "resolve_tension": "解释张力",
    "method_expand": "分析到方法",
    "one_level_down": "下沉一层",
    "problem_to_solution": "问题到对策",
    "object_to_mechanism": "对象到机制",
    "theme_to_subtopic": "主题到分话题",
    "summary_to_new_pivot": "总结到新支点",
    "judgement_to_reason": "判断到论证",
    "case_to_macro": "案例到宏观",
    "multi_branch_to_focus": "多支路到聚焦",
    "tension_to_explanation": "张力到解释",
    "analysis_to_method": "分析到方法",
    "open_only": "只留开放口",
    "summary_plus_open": "总结后再打开",
    "judgement_trigger": "判断触发",
    "transition_trigger": "转场触发",
    "tension_hold": "张力保留",
    "explicit_topic": "显性主题开头",
    "upper_context_link": "上文承接开头",
    "viewpoint_opening": "观点开头",
    "problem_opening": "问题开头",
    "weak_opening": "弱开头",
    "local_binding": "局部绑定",
    "parallel_expansion": "并列展开",
    "cause_effect_chain": "因果链",
    "problem_solution_blocks": "问题-对策板块",
    "mixed_layers": "混合层次",
    "conclusion": "结论句",
    "summary": "总结句",
    "call_to_action": "行动号召",
    "case_support": "案例支撑",
    "opening": "段首",
    "middle": "段中",
    "ending": "段尾",
    "inserted": "插入位",
    "mixed": "混合位",
    "bridge": "桥梁句",
    "middle_explanation": "中间解释",
    "middle_focus_shift": "中间聚焦切换",
    "opening_summary": "段首总起",
    "ending_summary": "段尾总结",
    "ending_elevation": "段尾升华",
    "inserted_reference": "插入位照应",
    "comprehensive_match": "综合多约束匹配",
    "continuation": "承接",
    "transition": "转承",
    "focus_shift": "焦点转移",
    "elevation": "升华",
    "reference_match": "指代匹配",
    "multi_constraint": "多约束匹配",
}


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_types() -> list[dict[str, Any]]:
    payloads = []
    for type_id in TYPE_ORDER:
        path = TYPE_CONFIG_DIR / f"{type_id}.yaml"
        payloads.append(read_yaml(path))
    return payloads


def label_key(key: str) -> str:
    return SLOT_LABELS.get(key, key)


def label_value(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value)
    return VALUE_LABELS.get(text, text)


def format_scalar(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "是" if value else "否"
    return label_value(value)


def format_list(values: list[Any] | None, sep: str = "、") -> str:
    if not values:
        return "-"
    return sep.join(format_scalar(item) for item in values)


def format_dict(mapping: dict[str, Any] | None) -> str:
    if not mapping:
        return "-"
    parts = []
    for key, value in mapping.items():
        parts.append(f"{label_key(str(key))}：{format_nested(value)}")
    return "；".join(parts)


def format_nested(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, dict):
        return format_dict(value)
    if isinstance(value, list):
        return format_list(value)
    return format_scalar(value)


def format_range_profile(profile: dict[str, Any]) -> str:
    parts = []
    for metric in ["complexity", "ambiguity", "reasoning_depth", "distractor_similarity"]:
        block = profile.get(metric) or {}
        minimum = block.get("min")
        maximum = block.get("max")
        if minimum is None and maximum is None:
            continue
        parts.append(f"{label_key(metric)}：{minimum:.2f} - {maximum:.2f}")
    return "；".join(parts) if parts else "-"


def flatten_difficulty_rules(rules: dict[str, Any]) -> str:
    if not rules:
        return "-"
    lines: list[str] = []
    for metric, detail in rules.items():
        metric_label = label_key(metric)
        if not isinstance(detail, dict):
            lines.append(f"{metric_label}：{format_nested(detail)}")
            continue
        if "base" in detail:
            lines.append(f"{metric_label}：base={detail['base']}")
        for axis, axis_detail in detail.items():
            if axis == "base":
                continue
            if isinstance(axis_detail, dict):
                for slot_name, slot_values in axis_detail.items():
                    if isinstance(slot_values, dict):
                        mapping_text = "、".join(
                            f"{label_value(slot_value)}={score}"
                            for slot_value, score in slot_values.items()
                        )
                        lines.append(f"{metric_label} / {label_key(axis)} / {label_key(slot_name)}：{mapping_text}")
                    else:
                        lines.append(f"{metric_label} / {label_key(axis)} / {label_key(slot_name)}：{format_nested(slot_values)}")
            else:
                lines.append(f"{metric_label} / {label_key(axis)}：{format_nested(axis_detail)}")
    return "；".join(lines) if lines else "-"


def format_analysis_steps(steps: dict[str, Any] | None) -> str:
    if not steps:
        return "-"
    parts = []
    for key, value in steps.items():
        step_label = str(key).split("_", 1)[-1] if "_" in str(key) else str(key)
        parts.append(f"{step_label}：{value}")
    return "；".join(parts)


def table_html(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body = []
    for row in rows:
        cells = []
        for cell in row:
            cells.append(f"<td>{html.escape(cell or '-').replace(chr(10), '<br>')}</td>")
        body.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def page_shell(title: str, generated_at: str, intro: str, sections: list[str]) -> str:
    styles = """
    body { font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif; margin: 24px; color: #1f2937; background: #f7f8fb; }
    h1, h2, h3 { color: #0f172a; margin: 0 0 12px; }
    h1 { font-size: 28px; }
    h2 { font-size: 22px; margin-top: 28px; }
    h3 { font-size: 18px; margin-top: 24px; }
    p { line-height: 1.75; }
    .card { background: #ffffff; border: 1px solid #dbe3ef; border-radius: 14px; padding: 18px 20px; margin: 18px 0; box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04); }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; background: #fff; margin-top: 12px; }
    th, td { border: 1px solid #dbe3ef; padding: 10px 12px; vertical-align: top; text-align: left; line-height: 1.65; word-break: break-word; }
    th { background: #eaf1fb; color: #0f172a; position: sticky; top: 0; z-index: 1; }
    .meta, .note { color: #475569; }
    .toc a { color: #1d4ed8; text-decoration: none; margin-right: 14px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; color: #3730a3; margin-right: 8px; font-size: 12px; }
    code { background: #eef2f7; padding: 2px 6px; border-radius: 6px; }
    """
    toc = "".join(
        f'<a href="#{payload["type_id"]}">{html.escape(TYPE_LABELS.get(payload["type_id"], payload["display_name"]))}</a>'
        for payload in load_types()
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{styles}</style>
</head>
<body>
  <div class="card">
    <h1>{html.escape(title)}</h1>
    <p class="meta">生成时间：{html.escape(generated_at)}</p>
    <p class="meta">{html.escape(intro)}</p>
    <p class="note">源头文件：<code>prompt_skeleton_service/configs/types/*.yaml</code></p>
    <div class="toc"><span class="pill">快速跳转</span>{toc}</div>
  </div>
  {''.join(sections)}
</body>
</html>
"""


def difficulty_summary_rows(type_payloads: list[dict[str, Any]]) -> list[list[str]]:
    rows = []
    for payload in type_payloads:
        rows.append(
            [
                TYPE_LABELS.get(payload["type_id"], payload.get("display_name", "-")),
                payload.get("display_name", "-"),
                format_list(payload.get("aliases", [])),
                str(payload.get("default_pattern_id") or "-"),
                format_dict(payload.get("skeleton")),
                format_dict(payload.get("default_slots")),
                format_dict(payload.get("fewshot_policy")),
            ]
        )
    return rows


def difficulty_sections(type_payloads: list[dict[str, Any]]) -> list[str]:
    sections = [
        f"""
  <div class="card">
    <h2>一、题型总览</h2>
    {table_html(["业务题型", "当前展示名", "别名", "默认制作卡", "骨架", "默认槽位", "few-shot 策略"], difficulty_summary_rows(type_payloads))}
  </div>
"""
    ]

    for payload in type_payloads:
        difficulty_rows = []
        for level in ["easy", "medium", "hard"]:
            difficulty_rows.append(
                [
                    label_value(level),
                    format_range_profile(payload.get("difficulty_target_profiles", {}).get(level, {})),
                ]
            )

        subtype_block = ""
        if payload.get("business_subtypes"):
            subtype_rows = []
            for subtype in payload.get("business_subtypes", []):
                subtype_rows.append(
                    [
                        subtype.get("display_name", "-"),
                        str(subtype.get("subtype_id") or "-"),
                        str(subtype.get("description") or "-"),
                        format_list(subtype.get("preferred_patterns", [])),
                        format_dict(subtype.get("default_slot_overrides")),
                        format_dict(subtype.get("fewshot_policy")),
                    ]
                )
            subtype_block = f"""
    <h3>业务子题型偏置卡</h3>
    {table_html(["子题型", "subtype_id", "说明", "优先制作卡", "默认槽位覆盖", "few-shot 策略"], subtype_rows)}
"""

        sections.append(
            f"""
  <div class="card" id="{html.escape(payload['type_id'])}">
    <h2>{html.escape(TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-')))}</h2>
    <p class="note">{html.escape(str(payload.get('task_definition') or ''))}</p>
    <p class="note">默认制作卡：<code>{html.escape(str(payload.get('default_pattern_id') or '-'))}</code></p>
    <h3>难度控制卡</h3>
    {table_html(["难度档位", "目标范围"], difficulty_rows)}
    <h3>槽位与默认口径</h3>
    {table_html(["项目", "当前值"], [["骨架", format_dict(payload.get("skeleton"))], ["默认槽位", format_dict(payload.get("default_slots"))], ["few-shot 策略", format_dict(payload.get("fewshot_policy"))], ["默认 few-shot", format_dict(payload.get("default_fewshot"))]])}
    {subtype_block}
  </div>
"""
        )
    return sections


def production_summary_rows(type_payloads: list[dict[str, Any]]) -> list[list[str]]:
    rows = []
    for payload in type_payloads:
        rows.append(
            [
                TYPE_LABELS.get(payload["type_id"], payload.get("display_name", "-")),
                str(len(payload.get("patterns", []) or [])),
                str(payload.get("default_pattern_id") or "-"),
                format_dict(payload.get("skeleton")),
                format_dict(payload.get("default_slots")),
            ]
        )
    return rows


def production_sections(type_payloads: list[dict[str, Any]]) -> list[str]:
    sections = [
        f"""
  <div class="card">
    <h2>一、制作卡总览</h2>
    {table_html(["业务题型", "制作卡数量", "默认制作卡", "骨架", "默认槽位"], production_summary_rows(type_payloads))}
  </div>
"""
    ]

    for payload in type_payloads:
        pattern_rows = []
        for pattern in payload.get("patterns", []) or []:
            control_logic = pattern.get("control_logic") or {}
            generation_logic = pattern.get("generation_logic") or {}
            pattern_rows.append(
                [
                    f"{pattern.get('pattern_name')}\n{pattern.get('pattern_id')}",
                    format_dict(pattern.get("match_rules")),
                    "；".join(
                        part
                        for part in [
                            f"难度来源：{format_scalar(control_logic.get('difficulty_source'))}" if control_logic.get("difficulty_source") else "",
                            f"选项迷惑度：{format_scalar(control_logic.get('option_confusion'))}" if control_logic.get("option_confusion") else "",
                            f"控制杠杆：{format_dict(control_logic.get('control_levers'))}" if control_logic.get("control_levers") else "",
                            f"特殊字段：{format_dict(control_logic.get('special_fields'))}" if control_logic.get("special_fields") else "",
                        ]
                        if part
                    ) or "-",
                    "；".join(
                        part
                        for part in [
                            f"制作核心：{generation_logic.get('generation_core')}" if generation_logic.get("generation_core") else "",
                            f"处理方式：{generation_logic.get('processing_type')}" if generation_logic.get("processing_type") else "",
                            f"正确项逻辑：{generation_logic.get('correct_logic')}" if generation_logic.get("correct_logic") else "",
                            f"高频陷阱：{format_list(generation_logic.get('high_freq_traps', []))}" if generation_logic.get("high_freq_traps") else "",
                            f"干扰项组织：{generation_logic.get('distractor_pattern')}" if generation_logic.get("distractor_pattern") else "",
                            f"分析步骤：{format_analysis_steps(generation_logic.get('analysis_steps'))}" if generation_logic.get("analysis_steps") else "",
                        ]
                        if part
                    ) or "-",
                    flatten_difficulty_rules(pattern.get("difficulty_rules") or {}),
                    format_dict(pattern.get("fewshot_example")),
                ]
            )

        subtype_block = ""
        if payload.get("business_subtypes"):
            subtype_rows = []
            for subtype in payload.get("business_subtypes", []):
                subtype_rows.append(
                    [
                        subtype.get("display_name", "-"),
                        str(subtype.get("subtype_id") or "-"),
                        format_list(subtype.get("preferred_patterns", [])),
                        format_dict(subtype.get("default_slot_overrides")),
                        format_dict(subtype.get("fewshot_examples", [{}])[0] if subtype.get("fewshot_examples") else None),
                    ]
                )
            subtype_block = f"""
    <h3>业务子题型制作偏置</h3>
    {table_html(["子题型", "subtype_id", "优先制作卡", "默认槽位覆盖", "示例卡片"], subtype_rows)}
"""

        sections.append(
            f"""
  <div class="card" id="{html.escape(payload['type_id'])}">
    <h2>{html.escape(TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-')))}</h2>
    <p class="note">{html.escape(str(payload.get('task_definition') or ''))}</p>
    <h3>制作卡</h3>
    {table_html(["制作卡", "适配条件", "控制逻辑", "制作逻辑", "难度映射", "few-shot 示例"], pattern_rows)}
    {subtype_block}
  </div>
"""
        )
    return sections


def export_html() -> tuple[Path, Path]:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    difficulty_path = REPORTS_DIR / f"v2_difficulty_control_cards_{timestamp}.html"
    production_path = REPORTS_DIR / f"v2_production_cards_{timestamp}.html"

    payloads = load_types()
    difficulty_html = page_shell(
        title="V2 难度控制卡总表",
        generated_at=generated_at,
        intro="按当前题型配置整理 easy / medium / hard 难度目标范围，以及默认槽位、few-shot 策略和子题型偏置。",
        sections=difficulty_sections(payloads),
    )
    production_html = page_shell(
        title="V2 制作卡总表",
        generated_at=generated_at,
        intro="按当前题型配置整理 patterns 制作卡，包括匹配条件、控制逻辑、制作逻辑、难度映射，以及 main_idea 的业务子题型偏置。",
        sections=production_sections(payloads),
    )

    difficulty_path.write_text(difficulty_html, encoding="utf-8")
    production_path.write_text(production_html, encoding="utf-8")
    return difficulty_path, production_path


if __name__ == "__main__":
    difficulty_file, production_file = export_html()
    print(difficulty_file)
    print(production_file)
