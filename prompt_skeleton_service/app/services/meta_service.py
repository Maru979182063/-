from __future__ import annotations

from typing import Any, Final

from app.services.config_registry import ConfigRegistry


REVIEW_CONTROL_SPECS: Final[dict[str, list[dict[str, Any]]]] = {
    "main_idea": [
        {
            "control_key": "abstraction_level",
            "label": "抽象层级",
            "description": "正确项抽象层级，控制答案更贴原文还是更概括、更总括。",
            "option_labels": {
                "low": "贴原文",
                "medium": "适中概括",
                "high": "高度概括",
            },
        },
        {
            "control_key": "statement_visibility",
            "label": "中心显隐",
            "description": "中心是否显性，控制主旨是明说还是更隐含。",
            "option_labels": {
                "high": "中心明说",
                "medium": "部分显性",
                "low": "中心隐含",
            },
        },
        {
            "control_key": "main_point_source",
            "label": "主旨来源",
            "description": "主旨主要从哪儿读出来，控制答案更偏抓全文、结论句、尾句还是局部段落。",
            "option_labels": {
                "whole_passage": "全文整合",
                "conclusion_sentence": "结论句",
                "tail_sentence": "尾句",
                "local_paragraph": "局部段落",
            },
        },
    ],
    "sentence_fill": [
        {
            "control_key": "context_dependency",
            "label": "上下文依赖",
            "description": "对上下文依赖程度，越高越需要真读上下文才能选。",
            "option_labels": {
                "low": "弱依赖",
                "medium": "中依赖",
                "high": "强依赖",
            },
        },
        {
            "control_key": "bidirectional_validation",
            "label": "双向卡口",
            "description": "是否必须同时承前启后，控制空位是不是双向卡口。",
            "option_labels": {
                "low": "单向为主",
                "medium": "兼顾双向",
                "high": "强双向卡口",
            },
        },
        {
            "control_key": "reference_dependency",
            "label": "指代依赖",
            "description": "指代依赖强度，控制是否需要严格对齐人称、主语和概念对象。",
            "option_labels": {
                "low": "弱指代约束",
                "medium": "中指代约束",
                "high": "强指代约束",
            },
        },
    ],
    "sentence_order": [
        {
            "control_key": "block_order_complexity",
            "label": "板块复杂度",
            "description": "板块顺序判断复杂度，控制整体排序链条有多绕。",
            "option_labels": {
                "low": "低复杂度",
                "medium": "中复杂度",
                "high": "高复杂度",
            },
        },
        {
            "control_key": "distractor_modes",
            "label": "干扰方式",
            "description": "错误排序主要采用哪类错法，单次最多选 2 个。",
            "max_selected": 2,
            "option_labels": {
                "wrong_opening": "首句错置",
                "wrong_closing": "尾句错置",
                "local_binding_break": "局部捆绑拆错",
                "block_swap": "板块互换",
                "connector_mislead": "连接词误导",
                "parallel_misorder": "并列错位",
                "summary_misplace": "总结句错放",
                "reason_fronting": "原因前置误导",
                "action_fronting": "行动前置误导",
            },
        },
        {
            "control_key": "distractor_strength",
            "label": "干扰强度",
            "description": "干扰强度，控制近邻错序有多像正确答案。",
            "option_labels": {
                "low": "弱干扰",
                "medium": "中干扰",
                "high": "强干扰",
            },
        },
    ],
}


class MetaService:
    def __init__(self, registry: ConfigRegistry) -> None:
        self.registry = registry

    def list_question_types(self) -> dict:
        configs = self.registry.list_types()
        return {
            "count": len(configs),
            "items": [
                {
                    "question_type": config.type_id,
                    "display_name": config.display_name,
                    "task_definition": config.task_definition,
                    "business_subtypes": [
                        {
                            "subtype_id": subtype.subtype_id,
                            "display_name": subtype.display_name,
                            "description": subtype.description,
                        }
                        for subtype in config.business_subtypes
                    ],
                }
                for config in configs
            ],
        }

    def get_controls(self, question_type: str) -> dict:
        config = self.registry.get_type(question_type)
        controls = []
        for spec in REVIEW_CONTROL_SPECS.get(config.type_id, []):
            key = spec["control_key"]
            slot = config.slot_schema.get(key)
            if slot is None:
                continue
            controls.append(
                {
                    "control_key": key,
                    "label": spec["label"],
                    "control_type": slot.type,
                    "options": self._build_options(slot.allowed or [], spec.get("option_labels") or {}),
                    "default_value": config.default_slots.get(key, slot.default),
                    "max_selected": spec.get("max_selected"),
                    "required": slot.required,
                    "affects_difficulty": False,
                    "editable_by": "reviewer_only",
                    "mapped_action": "question_modify",
                    "read_only": False,
                    "description": spec["description"],
                }
            )
        return {"question_type": config.type_id, "controls": controls}

    @staticmethod
    def review_control_keys(question_type: str) -> set[str]:
        return {
            str(spec.get("control_key") or "").strip()
            for spec in REVIEW_CONTROL_SPECS.get(question_type, [])
            if str(spec.get("control_key") or "").strip()
        }

    @staticmethod
    def _build_options(allowed_values: list[Any], option_labels: dict[str, str]) -> list[dict[str, Any]]:
        options: list[dict[str, Any]] = []
        for value in allowed_values:
            key = str(value)
            options.append({"value": value, "label": option_labels.get(key, key)})
        return options
