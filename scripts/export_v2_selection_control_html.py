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

QUESTION_FOCUS_ROWS = [
    ["题型聚焦", "标题填入题", "main_idea", "title_selection", "-", "走主旨大类，但直接挂到标题填入子类。"],
    ["题型聚焦", "接语选择题", "continuation", "-", "-", "直接进入接语选择主类型。"],
    ["题型聚焦", "语句排序题", "sentence_order", "-", "-", "直接进入排序主类型。"],
    ["题型聚焦", "语句填空题", "sentence_fill", "-", "-", "直接进入填空主类型。"],
    ["题型聚焦", "中心理解题", "main_idea", "center_understanding", "-", "走主旨大类，但挂到中心理解子类。"],
]

SPECIAL_TYPE_ROWS = [
    ["特殊题型", "主旨概括", "main_idea", "main_idea_general", "-", "直接锁到主旨概括子类。"],
    ["特殊题型", "选择标题", "main_idea", "title_selection", "-", "直接锁到标题填入子类。"],
    ["特殊题型", "结构概括", "main_idea", "structure_summary", "-", "直接锁到结构概括子类。"],
    ["特殊题型", "局部段意概括", "main_idea", "local_paragraph_summary", "-", "直接锁到局部段意概括子类。"],
    ["特殊题型", "中心理解题", "main_idea", "center_understanding", "-", "直接锁到中心理解子类。"],
    ["特殊题型", "尾句直接承接", "continuation", "-", "tail_anchor_direct_extend", "越过自动挑卡，直接指定制作卡。"],
    ["特殊题型", "总结后开启新支点", "continuation", "-", "summary_with_new_pivot", "直接指定接语选择制作卡。"],
    ["特殊题型", "问题后接对策", "continuation", "-", "problem_solution_hook", "直接指定接语选择制作卡。"],
    ["特殊题型", "机制展开", "continuation", "-", "mechanism_unfolding", "直接指定接语选择制作卡。"],
    ["特殊题型", "主题转分话题", "continuation", "-", "raised_theme_to_subtopic", "直接指定接语选择制作卡。"],
    ["特殊题型", "观点后接原因", "continuation", "-", "judgement_to_reason", "直接指定接语选择制作卡。"],
    ["特殊题型", "个案到宏观展开", "continuation", "-", "case_to_macro_unfold", "直接指定接语选择制作卡。"],
    ["特殊题型", "多分支聚焦", "continuation", "-", "multi_branch_focus", "直接指定接语选择制作卡。"],
    ["特殊题型", "张力解释", "continuation", "-", "tension_explained", "直接指定接语选择制作卡。"],
    ["特殊题型", "方法延展", "continuation", "-", "method_expansion", "直接指定接语选择制作卡。"],
    ["特殊题型", "双锚点锁定", "sentence_order", "-", "dual_anchor_lock", "直接指定排序制作卡。"],
    ["特殊题型", "承接并列展开", "sentence_order", "-", "carry_parallel_expand", "直接指定排序制作卡。"],
    ["特殊题型", "观点-原因-行动排序", "sentence_order", "-", "viewpoint_reason_action", "直接指定排序制作卡。"],
    ["特殊题型", "问题-对策-案例排序", "sentence_order", "-", "problem_solution_case_blocks", "直接指定排序制作卡。"],
    ["特殊题型", "问题—对策—案例排序", "sentence_order", "-", "problem_solution_case_blocks", "和上面是同义入口。"],
    ["特殊题型", "定位插入匹配", "sentence_fill", "-", "inserted_reference_match", "直接指定填空制作卡。"],
    ["特殊题型", "开头总起", "sentence_fill", "-", "opening_summary", "直接指定填空制作卡。"],
    ["特殊题型", "衔接过渡", "sentence_fill", "-", "bridge_transition", "直接指定填空制作卡。"],
    ["特殊题型", "中段焦点切换", "sentence_fill", "-", "middle_focus_shift", "直接指定填空制作卡。"],
    ["特殊题型", "中段解释说明", "sentence_fill", "-", "middle_explanation", "直接指定填空制作卡。"],
    ["特殊题型", "结尾总结", "sentence_fill", "-", "ending_summary", "直接指定填空制作卡。"],
    ["特殊题型", "结尾升华", "sentence_fill", "-", "ending_elevation", "直接指定填空制作卡。"],
    ["特殊题型", "综合多点匹配", "sentence_fill", "-", "comprehensive_multi_match", "直接指定填空制作卡。"],
]

DIFFICULTY_ROWS = [
    ["简单 / easy", "easy", "easy", "简单和 easy 都会归一成 easy。"],
    ["中等 / medium", "medium", "medium", "中等和 medium 都会归一成 medium。"],
    ["困难 / hard", "hard", "hard", "困难和 hard 都会归一成 hard。"],
]

FLOW_ROWS = [
    ["1. 入口映射", "input_decoder.py", "先看 `special_question_types`，如果有值就优先走特殊题型映射；没有再走 `question_focus` 映射。", "特殊题型优先级高于普通题型聚焦。"],
    ["2. 数量与难度清洗", "input_decoder.py", "`count` 会被压到 1-5；难度只允许 easy / medium / hard。", "前台乱填也会被收口。"],
    ["3. 类型配置装载", "config_registry.py", "按 `type_id / aliases` 装载题型 YAML，并校验默认卡、子题型、难度档位是否完整。", "所有选择控制都建立在配置先合法。"],
    ["4. 子题型选择", "slot_resolver.py::_get_business_subtype", "如果带 `business_subtype`，先查这个子题型是否属于当前 question_type。", "子题型是第二层分流。"],
    ["5. 槽位合并", "slot_resolver.py::_resolve_slots", "按“题型默认槽位 -> 子题型覆盖 -> schema 默认值 -> 用户传入 type_slots”依次合并。", "这是选择控制真正的输入面。"],
    ["6. 难度自动抬档", "slot_resolver.py::_apply_difficulty_slot_profile", "用户没手填的槽位，系统会根据 easy / medium / hard 自动往高或往低档位调。", "难度不是只看最后分数，也会前置影响槽位。"],
    ["7. 制作卡选择", "slot_resolver.py::_select_pattern", "优先看指定 `pattern_id`；否则先在子题型 preferred_patterns 内挑；再全局挑；最后回退 default_pattern。", "业务偏置、自动匹配、兜底卡三层都在这一步。"],
    ["8. 难度投影", "slot_resolver.py::_project_difficulty", "命中制作卡后，再把 slot 值和 control/generation 文本字段映射成四个难度指标。", "真正判断“是否达到 easy/medium/hard”是在这里。"],
    ["9. few-shot 选择", "prompt_builder.py::_select_fewshot", "优先级是 business_subtype > pattern > type default，只选 1 个结构示例。", "few-shot 不控制难度，只控制风格示范。"],
    ["10. 模板选择", "prompt_template_registry.py::resolve_default", "优先找 question_type + action_type + business_subtype 的模板；找不到就回退到不带 business_subtype 的模板；同组取最高版本。", "模板层也有精确命中和回退。"],
]

ASCENDING_ROWS = [
    ["option_confusion", "选项迷惑度", "low -> low_medium -> medium -> medium_high -> high", "难度越高，选项越接近。"],
    ["distractor_strength", "干扰项强度", "low -> medium -> high", "难度越高，干扰项越强。"],
    ["abstraction_level", "抽象层级", "low -> medium -> high", "难度越高，答案越抽象。"],
    ["context_dependency", "上下文依赖", "low -> medium -> high", "难度越高，越依赖上下文。"],
    ["bidirectional_validation", "双向验证", "low -> medium -> high", "难度越高，越要同时承前启后。"],
    ["reference_dependency", "指代依赖", "low -> medium -> high", "难度越高，指代和对象匹配越重。"],
    ["block_order_complexity", "板块排序复杂度", "low -> medium -> high", "难度越高，排序题更容易变成板块级判断。"],
    ["coverage_requirement", "覆盖要求", "close_rephrase -> integrated -> abstract_generalization", "主旨类难度越高越偏抽象统摄。"],
    ["blank_position", "空位位置", "opening -> middle -> ending -> inserted -> mixed", "填空题越往后越复杂。"],
    ["function_type", "空位功能", "opening_summary -> bridge -> middle_explanation -> middle_focus_shift -> ending_summary -> ending_elevation -> inserted_reference -> comprehensive_match", "填空题难度会把功能向更复杂位置推。"],
    ["logic_relation", "逻辑关系", "continuation -> transition -> explanation -> focus_shift -> summary -> elevation -> reference_match -> multi_constraint", "填空题难度越高越偏多约束。"],
]

DESCENDING_ROWS = [
    ["statement_visibility", "观点显性度", "high -> medium -> low", "难度越高，线索越不直给。"],
    ["anchor_clarity", "锚点清晰度", "high -> medium -> low", "难度越高，尾句往下文的方向越隐。"],
    ["opening_signal_strength", "开头锚点强度", "high -> medium -> low -> none", "排序题难度越高，首句越不明显。"],
    ["closing_signal_strength", "结尾收束强度", "high -> medium -> low -> none", "排序题难度越高，尾句越不明显。"],
    ["local_binding_strength", "局部绑定强度", "high -> medium -> low", "排序题难度越高，句间接口越弱。"],
]

SELECTION_MODE_ROWS = [
    ["direct", "用户或特殊题型显式传了 pattern_id", "直接使用指定制作卡，只校验它是否存在。"],
    ["auto_match", "没有指定卡，系统根据 match_rules 算分", "如果第一名分数 > 0 且不和第二名并列，就选它。"],
    ["fallback_default", "自动匹配没有清晰赢家", "回退到 question_type 的 default_pattern_id；如果配置没写默认卡，就退到第一张启用卡。"],
]

FEWSHOT_ROWS = [
    ["入口条件", "prompt_builder.py::_select_fewshot", "只有 `use_fewshot=true` 且 `fewshot_mode=structure_only` 才启用。"],
    ["优先级", "prompt_builder.py::_select_fewshot", "business_subtype few-shot > pattern few-shot > question_type default_fewshot。"],
    ["打分规则", "prompt_builder.py::_pick_best_fewshot", "命中 selected_pattern +2 分；每命中一个 fit_slot +1 分；只保留得分最高的 1 条。"],
    ["约束", "config.py::FewshotPolicyConfig", "当前强制 `bind_to_difficulty=false`、`mode=structure_only`、`max_examples=1`。"],
]

TEMPLATE_ROWS = [
    ["精确命中", "prompt_template_registry.py::resolve_default", "先找 `question_type + action_type + business_subtype` 完全一致的激活模板。"],
    ["回退命中", "prompt_template_registry.py::resolve_default", "如果带了 business_subtype 但没找到，就回退到同 question_type + action_type 且 business_subtype 为空的模板。"],
    ["版本选择", "prompt_template_registry.py::resolve_default", "同组模板里按 `template_version` 倒序，取最新版本。"],
]

REVIEW_OVERRIDE_ROWS = [
    ["保留字段", "question_generation.py::_apply_control_overrides", "question_type / business_subtype / pattern_id / difficulty_target / topic / passage_style / use_fewshot / fewshot_mode / material_policy 可直接覆盖。"],
    ["type_slots 覆盖", "question_generation.py::_apply_control_overrides", "传入 `type_slots` 会和原 snapshot 合并；除此之外的非保留字段也会被当成直接 slot 覆盖。"],
    ["额外记录", "question_generation.py::_apply_control_overrides", "所有覆盖项会写进 `extra_constraints.required_review_overrides`，instruction 会写进 `review_instruction`。"],
]

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
}


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_type_payloads() -> list[dict[str, Any]]:
    return [read_yaml(TYPE_CONFIG_DIR / f"{type_id}.yaml") for type_id in TYPE_ORDER]


def label_key(key: str) -> str:
    return SLOT_LABELS.get(key, key)


def label_value(value: Any) -> str:
    if value is None:
        return "-"
    return VALUE_LABELS.get(str(value), str(value))


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
        if isinstance(value, list):
            rendered = format_list(value)
        elif isinstance(value, dict):
            rendered = "；".join(f"{sub_key}={sub_value}" for sub_key, sub_value in value.items())
        else:
            rendered = format_scalar(value)
        parts.append(f"{label_key(str(key))}：{rendered}")
    return "；".join(parts)


def table_html(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cells = []
        for cell in row:
            cells.append(f"<td>{html.escape(cell or '-').replace(chr(10), '<br>')}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def build_type_pattern_rows(payloads: list[dict[str, Any]]) -> list[list[str]]:
    rows = []
    for payload in payloads:
        subtype_summary = "；".join(
            f"{subtype['display_name']} -> {format_list(subtype.get('preferred_patterns', []))}"
            for subtype in payload.get("business_subtypes", []) or []
        ) or "-"
        rows.append(
            [
                TYPE_LABELS.get(payload["type_id"], payload.get("display_name", "-")),
                payload.get("display_name", "-"),
                str(payload.get("default_pattern_id") or "-"),
                str(len(payload.get("patterns", []) or [])),
                format_dict(payload.get("default_slots")),
                subtype_summary,
            ]
        )
    return rows


def build_type_sections(payloads: list[dict[str, Any]]) -> list[str]:
    sections = []
    for payload in payloads:
        pattern_rows = []
        for pattern in payload.get("patterns", []) or []:
            pattern_rows.append(
                [
                    f"{pattern.get('pattern_name')}\n{pattern.get('pattern_id')}",
                    format_dict(pattern.get("match_rules")),
                    format_scalar((pattern.get("control_logic") or {}).get("difficulty_source")),
                    format_scalar((pattern.get("control_logic") or {}).get("option_confusion")),
                    format_dict((pattern.get("control_logic") or {}).get("control_levers")),
                ]
            )

        subtype_rows = []
        for subtype in payload.get("business_subtypes", []) or []:
            subtype_rows.append(
                [
                    subtype.get("display_name", "-"),
                    str(subtype.get("subtype_id") or "-"),
                    format_list(subtype.get("preferred_patterns", [])),
                    format_dict(subtype.get("default_slot_overrides")),
                ]
            )
        subtype_block = ""
        if subtype_rows:
            subtype_block = f"""
    <h3>子题型选择偏置</h3>
    {table_html(["子题型", "subtype_id", "优先制作卡", "默认槽位覆盖"], subtype_rows)}
"""

        sections.append(
            f"""
  <div class="card" id="{html.escape(payload['type_id'])}">
    <h2>{html.escape(TYPE_LABELS.get(payload['type_id'], payload.get('display_name', '-')))}</h2>
    <p class="note">{html.escape(str(payload.get('task_definition') or ''))}</p>
    <p class="note">默认制作卡：<code>{html.escape(str(payload.get('default_pattern_id') or '-'))}</code></p>
    {table_html(["制作卡", "match_rules 适配条件", "难度主要来源", "选项迷惑度", "控制杠杆"], pattern_rows)}
    {subtype_block}
  </div>
"""
        )
    return sections


def build_html(generated_at: str, payloads: list[dict[str, Any]]) -> str:
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
        f'<a href="#{payload["type_id"]}">{html.escape(TYPE_LABELS.get(payload["type_id"], payload.get("display_name", "-")))}</a>'
        for payload in payloads
    )
    sections = build_type_sections(payloads)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>V2 选择控制总表</title>
  <style>{styles}</style>
</head>
<body>
  <div class="card">
    <h1>V2 选择控制总表</h1>
    <p class="meta">生成时间：{html.escape(generated_at)}</p>
    <p class="meta">这份表专门说明系统当前如何做“入口映射、槽位自动控制、制作卡选择、few-shot 选择、模板选择和复审覆盖”。</p>
    <p class="note">源头文件：<code>prompt_skeleton_service/app/services/input_decoder.py</code>、<code>slot_resolver.py</code>、<code>prompt_builder.py</code>、<code>prompt_template_registry.py</code>、<code>question_generation.py</code></p>
    <div class="toc"><span class="pill">快速跳转</span>{toc}</div>
  </div>

  <div class="card">
    <h2>一、选择控制总流程</h2>
    {table_html(["步骤", "实现位置", "当前逻辑", "业务理解"], FLOW_ROWS)}
  </div>

  <div class="card">
    <h2>二、入口映射控制</h2>
    <h3>question_focus 映射</h3>
    {table_html(["入口类型", "前台值", "question_type", "business_subtype", "pattern_id", "业务解释"], QUESTION_FOCUS_ROWS)}
    <h3>special_question_types 映射</h3>
    {table_html(["入口类型", "前台值", "question_type", "business_subtype", "pattern_id", "业务解释"], SPECIAL_TYPE_ROWS)}
    <h3>难度值映射</h3>
    {table_html(["前台值", "内部 difficulty_level", "difficulty_target", "说明"], DIFFICULTY_ROWS)}
  </div>

  <div class="card">
    <h2>三、槽位自动选择控制</h2>
    <h3>合并优先级</h3>
    {table_html(["优先级从低到高", "来源", "说明"], [
        ["1", "question_type.default_slots", "题型默认槽位，作为最基础控制口径。"],
        ["2", "business_subtype.default_slot_overrides", "如果有子题型，先用它覆盖题型默认值。"],
        ["3", "slot_schema.default", "前两层都没填时，补 slot_schema 的字段默认值。"],
        ["4", "incoming type_slots", "用户显式传入的 type_slots 最优先。"],
        ["5", "difficulty auto profile", "仅对用户没手填的槽位，根据难度自动调档。"],
    ])}
    <h3>随难度升高自动上调的槽位</h3>
    {table_html(["slot", "业务名", "档位顺序", "说明"], ASCENDING_ROWS)}
    <h3>随难度升高自动下调的槽位</h3>
    {table_html(["slot", "业务名", "档位顺序", "说明"], DESCENDING_ROWS)}
  </div>

  <div class="card">
    <h2>四、制作卡选择控制</h2>
    {table_html(["选择模式", "触发条件", "当前逻辑"], SELECTION_MODE_ROWS)}
    <h3>各题型当前选择口径</h3>
    {table_html(["业务题型", "当前展示名", "默认制作卡", "制作卡数量", "题型默认槽位", "子题型优先制作卡"], build_type_pattern_rows(payloads))}
  </div>

  <div class="card">
    <h2>五、few-shot 与模板选择控制</h2>
    <h3>few-shot 选择</h3>
    {table_html(["控制点", "实现位置", "当前逻辑"], FEWSHOT_ROWS)}
    <h3>模板选择</h3>
    {table_html(["控制点", "实现位置", "当前逻辑"], TEMPLATE_ROWS)}
  </div>

  <div class="card">
    <h2>六、复审改题时的选择覆盖</h2>
    {table_html(["控制点", "实现位置", "当前逻辑"], REVIEW_OVERRIDE_ROWS)}
  </div>

  {''.join(sections)}
</body>
</html>
"""


def export_html() -> Path:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / f"v2_selection_control_{timestamp}.html"
    payloads = load_type_payloads()
    output_path.write_text(build_html(generated_at, payloads), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    report_file = export_html()
    print(report_file)
