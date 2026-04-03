from __future__ import annotations

import csv
import html
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
NORMALIZED_ROOT = ROOT / "card_specs" / "normalized"
REPORTS_DIR = ROOT / "reports"

FAMILY_ORDER = [
    "title_selection",
    "continuation",
    "sentence_order",
    "sentence_fill",
]

FAMILY_META = {
    "title_selection": {
        "label": "标题填入",
        "candidate_rule_note": "优先从整篇、闭合段、多段组合中取材；长文会压缩整篇候选，避免过长、列举式或标题感过弱的材料直接入池。",
        "presentation_note": "最终更关注标题可命名性、对象稳定性，以及是否能直接转成标题项。",
    },
    "continuation": {
        "label": "接语选择",
        "candidate_rule_note": "常规从闭合段、多段组合、整篇中取尾段候选，后续靠尾句开放度、推进方向、锚点清晰度做区分。",
        "presentation_note": "最终更关注尾句往下文的自然推进方向，而不是全文主旨是否已经闭合。",
    },
    "sentence_order": {
        "label": "语句排序",
        "candidate_rule_note": "只取句群块或分句组；输出时会自动补上文托底、下文托底和首中尾结构提示。",
        "presentation_note": "最终会把材料拆成可排序单元，并附上首句/中段/尾句线索。",
    },
    "sentence_fill": {
        "label": "语句填空",
        "candidate_rule_note": "支持整篇、闭合段、多段组合和插入位上下文窗；会自动判断空位位置与功能。",
        "presentation_note": "最终会输出 blank_position、function_type 和 blanked_text，便于直接做填空题。",
    },
}

CANDIDATE_TYPE_LABELS = {
    "whole_passage": "整篇材料",
    "closed_span": "闭合段",
    "multi_paragraph_unit": "多段组合",
    "sentence_block_group": "句群块",
    "phrase_or_clause_group": "短语/分句组",
    "insertion_context_unit": "插入位上下文窗",
}

SLOT_KEY_LABELS = {
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
    "strongest_distractor_gap": "最强干扰项间距",
    "distractor_hierarchy_profile": "干扰层级",
    "core_object_anchor_required": "核心对象锚点必需",
    "title_namingness_min": "标题感下限",
    "object_scope_stability_min": "对象稳定性下限",
}

VALUE_LABELS = {
    "turning": "转折型",
    "progressive": "递进型",
    "medium": "中",
    "high": "高",
    "low": "低",
    "integrated": "综合覆盖",
    "title_label": "标题项",
    "thematic": "主题式",
    "direct_label": "直给式",
    "abstract": "抽象式",
    "rhetorical": "修辞式",
    "whole_passage": "整篇材料",
    "closed_span": "闭合段",
    "multi_paragraph_unit": "多段组合",
    "sentence_block_group": "句群块",
    "phrase_or_clause_group": "短语/分句组",
    "insertion_context_unit": "插入位上下文窗",
    "small_or_medium": "小或中",
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
    "call_to_action": "行动号召句",
    "case_support": "案例支撑句",
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
    "explanation": "解释",
    "focus_shift": "焦点转移",
    "elevation": "升华",
    "reference_match": "指代匹配",
    "multi_constraint": "多约束匹配",
    "open_only": "只留开放口",
    "summary_plus_open": "总结后再打开",
    "judgement_trigger": "判断触发",
    "transition_trigger": "转场触发",
    "tension_hold": "张力保留",
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
    "main_idea": "主旨类",
    "title_selection": "标题填入",
    "continuation": "接语选择",
    "sentence_order": "语句排序",
    "sentence_fill": "语句填空",
    "true": "是",
    "false": "否",
}

CSV_COLUMNS = [
    ("family_label", "业务题型"),
    ("family_id", "family_id"),
    ("question_card_name", "当前题卡"),
    ("question_card_id", "question_card_id"),
    ("runtime_question_type", "运行时题型"),
    ("runtime_business_subtype", "运行时业务子类"),
    ("material_card_name", "中间结构卡名称"),
    ("material_card_id", "中间结构卡ID"),
    ("selection_core", "选材核心"),
    ("structures", "对应篇章结构"),
    ("allowed_candidate_types", "允许候选切片"),
    ("preferred_candidate_types", "优先候选切片"),
    ("required_signals", "命中门槛"),
    ("preferred_signals", "偏好信号"),
    ("avoid_signals", "规避信号"),
    ("card_bias", "结构卡偏向"),
    ("default_generation_archetype", "默认生成原型ID"),
    ("archetype_processing_type", "原型处理方式"),
    ("archetype_correct_logic", "正确项逻辑"),
    ("archetype_analysis_framework", "建议分析框架"),
    ("question_base_slots", "题卡默认槽位"),
    ("question_slot_overrides", "题卡覆盖槽位"),
    ("validator_rules", "校验规则"),
    ("distractor_bias", "常见错项方向"),
]


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def family_bundle(family_id: str) -> dict[str, Any]:
    return {
        "signal_layer": read_yaml(NORMALIZED_ROOT / "signal_layers" / f"{family_id}_signal_layer.normalized.yaml"),
        "material_registry": read_yaml(NORMALIZED_ROOT / "material_cards" / f"{family_id}_intermediate_material_cards.normalized.yaml"),
        "question_card": read_yaml(NORMALIZED_ROOT / "question_cards" / f"{family_id}_standard_question_card.normalized.yaml"),
    }


def signal_description_map(signal_layer: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for section in ("signals", "derived_signals"):
        for item in signal_layer.get(section, []) or []:
            signal_id = item.get("signal_id")
            if signal_id:
                mapping[str(signal_id)] = str(item.get("description") or signal_id)
    return mapping


def label_for_key(value: str) -> str:
    return SLOT_KEY_LABELS.get(value, value)


def label_for_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return VALUE_LABELS.get(str(value).lower(), "是" if value else "否")
    text = str(value)
    return VALUE_LABELS.get(text, CANDIDATE_TYPE_LABELS.get(text, text))


def format_list(values: list[Any] | tuple[Any, ...] | None, sep: str = "、") -> str:
    if not values:
        return "-"
    return sep.join(label_for_value(item) for item in values)


def format_pairs(mapping: dict[str, Any] | None) -> str:
    if not mapping:
        return "-"
    parts = []
    for key, value in mapping.items():
        label = label_for_key(str(key))
        if isinstance(value, list):
            rendered = format_list(list(value))
        else:
            rendered = label_for_value(value)
        parts.append(f"{label}：{rendered}")
    return "；".join(parts)


def format_required_signals(required: dict[str, Any], descriptions: dict[str, str]) -> str:
    if not required:
        return "-"
    parts = []
    for signal_id, requirement in required.items():
        signal_text = descriptions.get(str(signal_id), str(signal_id))
        parts.append(f"{signal_text} {requirement}")
    return "；".join(parts)


def format_profiles(profile_ids: list[str], descriptions: dict[str, str]) -> str:
    if not profile_ids:
        return "-"
    return "；".join(descriptions.get(signal_id, signal_id) for signal_id in profile_ids)


def table_html(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cells = []
        for cell in row:
            escaped = html.escape(cell or "-").replace("\n", "<br>")
            cells.append(f"<td>{escaped}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def build_logic_rows() -> list[list[str]]:
    return [
        [
            "1. 题型绑定",
            "prompt_skeleton_service/app/services/material_bridge_v2.py",
            "先把题型请求映射成统一 family_id。当前只有 `main_idea + title_selection` 会折算为 `title_selection`，其余三个题型沿用同名 family。",
            "统一 family 入口，避免题卡直接绑死到老的 question_type 命名。",
        ],
        [
            "2. 候选检索",
            "prompt_skeleton_service/app/services/material_bridge_v2.py -> /materials/v2/search",
            "向 passage_service 发送 `business_family_id / article_limit / candidate_limit / min_card_score`，其中难度会影响最低卡分：easy=0.48，medium=0.55，hard=0.60。",
            "先做召回和粗筛，把不达标材料挡在 prompt 前面。",
        ],
        [
            "3. 候选切片",
            "passage_service/app/services/material_pipeline_v2.py::_derive_candidates",
            "按 question card 的 `required_candidate_types` 切出候选：整篇、闭合段、多段组合、句群块、分句组、插入位上下文窗等。",
            "同一篇文章会被拆成多个可消费候选，供不同中间结构卡命中。",
        ],
        [
            "4. 信号画像",
            "passage_service/app/services/material_pipeline_v2.py::_build_signal_profile",
            "对每个候选生成 signal profile，只保留当前 family 的信号层里允许的中性信号和派生信号。",
            "把“篇章结构、尾句开放度、空位功能、首尾锚点”等判断口径收口成统一字段。",
        ],
        [
            "5. 中间结构卡打分",
            "passage_service/app/services/material_pipeline_v2.py::_score_material_cards",
            "逐张 material card 校验 `required_signals`，平均分后再叠加 `preferred_candidate_type +0.08`，高上下文依赖材料会扣分。",
            "每个候选允许同时命中多张结构卡，但只保留 Top-N，第一名进入题卡消费阶段。",
        ],
        [
            "6. 题卡落位",
            "passage_service/app/services/material_pipeline_v2.py::_resolve_slots",
            "question card 先给出 `base_slots`，再按命中的 material card 套用 `slot_overrides`，形成最终 `resolved_slots`。",
            "这一步就是“结构卡 -> 题卡结构要求”的真正映射关系。",
        ],
        [
            "7. 展示层加工",
            "passage_service/app/services/material_pipeline_v2.py::_build_presentation / _build_consumable_text",
            "排序题会补 `lead_context / follow_context / structure_hints`；填空题会补 `blank_position / function_type / blanked_text`。",
            "输出不只是原文片段，而是已经转成可出题的消费形态。",
        ],
        [
            "8. prompt 侧二次排序",
            "prompt_skeleton_service/app/services/material_bridge_v2.py::_score_candidate",
            "在已命中结构卡的候选里，再按 `quality_score + 可读性 + topic/text_direction/document_genre/material_structure_label/material_policy` 进行二次排序。",
            "业务或运营如果想控制“更像什么材料”，主要就看这一层的偏好因子。",
        ],
        [
            "9. 最终输出口径",
            "prompt_skeleton_service/app/services/material_bridge_v2.py::_to_material_selection",
            "最终会吐出 `primary_label=选中的中间结构卡`、`material_structure_label=篇章结构标签`、`material_structure_reason=生成原型`、`fit_scores`、`selection_reason` 等字段。",
            "业务侧看到的“结构”和“题卡原因”，本质都来自这套映射链路。",
        ],
    ]


def build_output_field_rows() -> list[list[str]]:
    return [
        ["primary_label", "最终命中的中间结构卡 ID", "业务看这列，就知道候选材料最后落到了哪张结构卡。"],
        ["material_structure_label", "材料自身的篇章结构标签（来自 discourse_shape）", "例如“转折归旨”“问题-分析-结论”“并列展开”。"],
        ["material_structure_reason", "最终生成原型", "即 question card 消费该结构卡后采用的生成逻辑原型。"],
        ["fit_scores", "候选对多张结构卡的适配分", "可用来比较“第一命中”和“次优命中”。"],
        ["knowledge_tags", "补充标签", "当前会附带候选切片类型、结构卡、生成原型、核心对象等。"],
        ["selection_reason", "prompt 侧排序原因", "会说明是因为质量分、可读性、题材命中还是文体/结构偏好被选中。"],
    ]


def build_family_summary_row(
    family_id: str,
    signal_layer: dict[str, Any],
    material_registry: dict[str, Any],
    question_card: dict[str, Any],
    descriptions: dict[str, str],
) -> list[str]:
    runtime_binding = question_card.get("runtime_binding", {})
    runtime_text = f"{label_for_value(runtime_binding.get('question_type'))} / {label_for_value(runtime_binding.get('business_subtype'))}"
    candidate_types = format_list(question_card.get("upstream_contract", {}).get("required_candidate_types", []))
    profiles = format_profiles(question_card.get("upstream_contract", {}).get("required_profiles", []), descriptions)
    base_slots = format_pairs(question_card.get("base_slots"))
    signal_count = len(signal_layer.get("signals", []) or [])
    derived_count = len(signal_layer.get("derived_signals", []) or [])
    card_count = len(material_registry.get("cards", []) or [])
    arch_count = len(question_card.get("generation_archetypes", {}) or {})
    notes = FAMILY_META[family_id]
    return [
        notes["label"],
        question_card.get("display_name", "-"),
        runtime_text,
        candidate_types,
        f"显式信号 {signal_count} 个；派生信号 {derived_count} 个",
        f"中间结构卡 {card_count} 张；生成原型 {arch_count} 个",
        base_slots,
        profiles,
        notes["candidate_rule_note"],
        notes["presentation_note"],
    ]


def runtime_binding_text(runtime_binding: dict[str, Any]) -> str:
    question_type = label_for_value(runtime_binding.get("question_type"))
    business_subtype = label_for_value(runtime_binding.get("business_subtype"))
    return f"题型：{question_type}；业务子类：{business_subtype}"


def build_mapping_rows(
    family_id: str,
    material_registry: dict[str, Any],
    question_card: dict[str, Any],
    descriptions: dict[str, str],
) -> tuple[list[list[str]], list[dict[str, str]]]:
    rows: list[list[str]] = []
    csv_rows: list[dict[str, str]] = []
    overrides_lookup = {
        item.get("material_card"): item.get("slot_overrides", {})
        for item in question_card.get("material_card_overrides", []) or []
    }
    archetypes = question_card.get("generation_archetypes", {}) or {}

    for index, card in enumerate(material_registry.get("cards", []) or [], start=1):
        material_card_id = str(card.get("card_id") or "")
        override_slots = overrides_lookup.get(material_card_id) or {}
        archetype_id = str(card.get("default_generation_archetype") or "")
        archetype = archetypes.get(archetype_id, {}) or {}
        required_signals = format_required_signals(card.get("required_signals", {}), descriptions)
        allowed_types = format_list((card.get("candidate_contract") or {}).get("allowed_candidate_types", []))
        preferred_types = format_list((card.get("candidate_contract") or {}).get("preferred_candidate_types", []))
        structures = format_list(card.get("structures", []), " / ")
        preferred_signals = format_list(card.get("preferred_signals", []))
        avoid_signals = format_list(card.get("avoid_signals", []))
        distractor_bias = format_list(card.get("distractor_bias", []))
        override_text = format_pairs(override_slots)
        archetype_summary = "；".join(
            part
            for part in [
                f"处理方式：{archetype.get('processing_type')}" if archetype.get("processing_type") else "",
                f"正确项逻辑：{archetype.get('correct_logic')}" if archetype.get("correct_logic") else "",
            ]
            if part
        ) or archetype_id

        rows.append(
            [
                str(index),
                f"{card.get('display_name')}\n{material_card_id}",
                str(card.get("selection_core") or "-"),
                structures,
                f"允许：{allowed_types}\n优先：{preferred_types}",
                required_signals,
                archetype_summary,
                override_text,
                f"偏好信号：{preferred_signals}\n规避信号：{avoid_signals}\n常见错项：{distractor_bias}",
            ]
        )

        csv_rows.append(
            {
                "family_id": family_id,
                "family_label": FAMILY_META[family_id]["label"],
                "question_card_id": str(question_card.get("card_id") or ""),
                "question_card_name": str(question_card.get("display_name") or ""),
                "runtime_question_type": label_for_value((question_card.get("runtime_binding") or {}).get("question_type")),
                "runtime_business_subtype": label_for_value((question_card.get("runtime_binding") or {}).get("business_subtype")),
                "material_card_id": material_card_id,
                "material_card_name": str(card.get("display_name") or ""),
                "selection_core": str(card.get("selection_core") or ""),
                "structures": structures,
                "allowed_candidate_types": allowed_types,
                "preferred_candidate_types": preferred_types,
                "required_signals": required_signals,
                "preferred_signals": preferred_signals,
                "avoid_signals": avoid_signals,
                "card_bias": str(card.get("card_bias") or ""),
                "default_generation_archetype": archetype_id,
                "archetype_processing_type": str(archetype.get("processing_type") or ""),
                "archetype_correct_logic": str(archetype.get("correct_logic") or ""),
                "archetype_analysis_framework": format_list(archetype.get("analysis_framework", [])),
                "question_base_slots": format_pairs(question_card.get("base_slots")),
                "question_slot_overrides": override_text,
                "validator_rules": format_list((question_card.get("validator_contract") or {}).get("extension_rules", [])),
                "distractor_bias": distractor_bias,
            }
        )

    return rows, csv_rows


def build_archetype_rows(question_card: dict[str, Any]) -> list[list[str]]:
    rows: list[list[str]] = []
    for archetype_id, detail in (question_card.get("generation_archetypes", {}) or {}).items():
        rows.append(
            [
                archetype_id,
                str(detail.get("processing_type") or "-"),
                str(detail.get("correct_logic") or "-"),
                format_list(detail.get("analysis_framework", [])),
            ]
        )
    return rows


def render_html(
    generated_at: str,
    family_sections: list[str],
    summary_rows: list[list[str]],
) -> str:
    styles = """
    body { font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif; margin: 24px; color: #1f2937; background: #f7f8fb; }
    h1, h2, h3 { color: #0f172a; margin: 0 0 12px; }
    h1 { font-size: 28px; }
    h2 { font-size: 22px; margin-top: 28px; }
    h3 { font-size: 18px; margin-top: 24px; }
    p, li { line-height: 1.7; }
    .meta, .note { color: #475569; }
    .card { background: #ffffff; border: 1px solid #dbe3ef; border-radius: 14px; padding: 18px 20px; margin: 18px 0; box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04); }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; background: #fff; margin-top: 12px; }
    th, td { border: 1px solid #dbe3ef; padding: 10px 12px; vertical-align: top; text-align: left; line-height: 1.65; word-break: break-word; }
    th { background: #eaf1fb; color: #0f172a; position: sticky; top: 0; z-index: 1; }
    .small { font-size: 13px; color: #475569; }
    .toc a { color: #1d4ed8; text-decoration: none; margin-right: 14px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; color: #3730a3; margin-right: 8px; font-size: 12px; }
    code { background: #eef2f7; padding: 2px 6px; border-radius: 6px; }
    """
    toc = "".join(
        f'<a href="#{family_id}">{html.escape(FAMILY_META[family_id]["label"])}</a>'
        for family_id in FAMILY_ORDER
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>V2 中间逻辑与题卡结构映射表</title>
  <style>{styles}</style>
</head>
<body>
  <div class="card">
    <h1>V2 中间逻辑与题卡结构映射表</h1>
    <p class="meta">生成时间：{html.escape(generated_at)}</p>
    <p class="meta">用途：给业务直接查看当前 V2 版本里“中间逻辑、结构卡、题卡槽位、生成原型”之间的关系，方便按题型讨论要不要调整口径。</p>
    <p class="small">源头文件：<code>card_specs/normalized/*</code>、<code>passage_service/app/services/material_pipeline_v2.py</code>、<code>prompt_skeleton_service/app/services/material_bridge_v2.py</code></p>
    <div class="toc"><span class="pill">快速跳转</span>{toc}</div>
  </div>

  <div class="card">
    <h2>一、V2 中间逻辑总览</h2>
    {table_html(["步骤", "当前实现位置", "当前逻辑", "业务理解"], build_logic_rows())}
  </div>

  <div class="card">
    <h2>二、题型总览</h2>
    {table_html(["业务题型", "当前 question card", "运行时绑定", "候选切片类型", "信号层规模", "结构卡/原型数量", "题卡默认槽位", "关键判断信号", "候选切片补充规则", "最终消费方式"], summary_rows)}
  </div>

  <div class="card">
    <h2>三、输出字段口径</h2>
    {table_html(["字段", "当前含义", "业务看法建议"], build_output_field_rows())}
  </div>

  {''.join(family_sections)}
</body>
</html>
"""


def export_report() -> tuple[Path, Path]:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = REPORTS_DIR / f"v2_business_mapping_report_{timestamp}.html"
    csv_path = REPORTS_DIR / f"v2_business_mapping_master_{timestamp}.csv"

    summary_rows: list[list[str]] = []
    family_sections: list[str] = []
    csv_rows: list[dict[str, str]] = []

    for family_id in FAMILY_ORDER:
        bundle = family_bundle(family_id)
        signal_layer = bundle["signal_layer"]
        material_registry = bundle["material_registry"]
        question_card = bundle["question_card"]
        descriptions = signal_description_map(signal_layer)

        summary_rows.append(build_family_summary_row(family_id, signal_layer, material_registry, question_card, descriptions))
        mapping_rows, family_csv_rows = build_mapping_rows(family_id, material_registry, question_card, descriptions)
        csv_rows.extend(family_csv_rows)
        archetype_rows = build_archetype_rows(question_card)

        family_sections.append(
            f"""
  <div class="card" id="{html.escape(family_id)}">
    <h2>{html.escape(FAMILY_META[family_id]['label'])}</h2>
    <p class="note">{html.escape(question_card.get('description') or '')}</p>
    <p class="small">
      当前 question card：<code>{html.escape(str(question_card.get('card_id') or '-'))}</code><br>
      runtime 绑定：{html.escape(runtime_binding_text(question_card.get('runtime_binding') or {}))}<br>
      required candidate types：{html.escape(format_list(question_card.get('upstream_contract', {}).get('required_candidate_types', [])))}<br>
      required profiles：{html.escape(format_profiles(question_card.get('upstream_contract', {}).get('required_profiles', []), descriptions))}<br>
      validator 规则：{html.escape(format_list((question_card.get('validator_contract') or {}).get('extension_rules', [])))}
    </p>
    <h3>中间结构卡映射</h3>
    {table_html(["序号", "中间结构卡", "选材核心", "对应篇章结构", "可吃的候选切片", "命中门槛（必备信号）", "对应生成原型", "题卡覆盖槽位", "业务观察点"], mapping_rows)}
    <h3>生成原型说明</h3>
    {table_html(["原型 ID", "处理方式", "正确项逻辑", "建议分析框架"], archetype_rows)}
  </div>
"""
        )

    html_path.write_text(render_html(generated_at, family_sections, summary_rows), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([header for _, header in CSV_COLUMNS])
        for row in csv_rows:
            writer.writerow([row.get(key, "") for key, _ in CSV_COLUMNS])

    return html_path, csv_path


if __name__ == "__main__":
    html_file, csv_file = export_report()
    print(html_file)
    print(csv_file)
