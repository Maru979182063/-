from __future__ import annotations

import hashlib
import re
from difflib import SequenceMatcher
from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider, read_prompt_file


class NearMissRepairService:
    SUPPORTED_FAMILIES = {"center_understanding", "title_selection", "sentence_fill", "sentence_order"}
    FAMILY_ALIASES = {
        "title_selection": "center_understanding",
    }
    FAILED_REPAIR_KEYS: set[str] = set()
    MAX_FAILED_REPAIR_KEYS = 2000
    DISALLOWED_FAILURE_REASONS = {
        "missing_candidate_id",
        "missing_article_id",
        "empty_candidate_text",
        "paragraph_span_not_traceable",
        "sentence_span_not_traceable",
        "illegal_control_character_detected",
        "llm_adjudication_rejected",
        "sentence_order_unit_count_below_floor",
        "difficulty_band_not_allowed",
    }
    DEPENDENCY_OPENINGS = ("对此", "与此同时", "另一方面", "此外", "因此", "所以", "由此", "这也")
    RULE_BASED_NOISE_STATES = {
        "page_residue",
        "question_stem_contamination",
        "layout_break",
        "structure_signal_impure",
        "over_appended_context",
        "example_overdominant",
    }

    def __init__(
        self,
        *,
        provider: BaseLLMProvider | None = None,
        llm_config: dict[str, Any] | None = None,
    ) -> None:
        self.provider = provider or get_llm_provider()
        self.llm_config = llm_config or get_config_bundle().llm
        self.config = dict(self.llm_config.get("depth2_repair") or {})
        self.prompt = read_prompt_file("targeted_material_rewrite_prompt.md")

    def is_enabled_for_family(self, business_family_id: str) -> bool:
        normalized = self._normalize_family_id(business_family_id)
        if business_family_id not in self.SUPPORTED_FAMILIES and normalized not in self.SUPPORTED_FAMILIES:
            return False
        if not bool(self.config.get("enabled", True)):
            return False
        if not self.provider.is_enabled():
            return False
        families = dict(self.config.get("families") or {})
        family_config = dict(families.get(business_family_id) or families.get(normalized) or {})
        return bool(family_config.get("enabled", True))

    def evaluate_entry(
        self,
        *,
        item: dict[str, Any],
        business_family_id: str,
        failure_reason: str,
        question_card_id: str,
    ) -> dict[str, Any]:
        normalized_family_id = self._normalize_family_id(business_family_id)
        text = str(item.get("text") or item.get("original_text") or "").strip()
        target_business_card = self._target_business_card(item)
        if not self.is_enabled_for_family(business_family_id):
            return {"repair_candidate": False, "entry_reason": "repair_disabled"}
        if self._already_repaired_or_locked(item):
            return {"repair_candidate": False, "entry_reason": "repair_already_attempted_or_locked"}
        if not text:
            return {"repair_candidate": False, "entry_reason": "empty_text"}
        if not target_business_card:
            return {"repair_candidate": False, "entry_reason": "missing_target_business_card"}
        if self._failure_key(item=item, business_family_id=business_family_id, target_business_card=target_business_card) in self.FAILED_REPAIR_KEYS:
            return {"repair_candidate": False, "entry_reason": "repair_previously_failed"}
        if failure_reason in self.DISALLOWED_FAILURE_REASONS:
            return {"repair_candidate": False, "entry_reason": f"failure_reason_not_repairable:{failure_reason}"}
        dirty_states = self.collect_dirty_states(
            item=item,
            business_family_id=business_family_id,
            failure_reason=failure_reason,
        )
        if not dirty_states:
            return {"repair_candidate": False, "entry_reason": "no_repairable_dirty_state"}
        min_chars = int(self.config.get("min_text_chars", 80))
        max_chars = int(self.config.get("max_text_chars", 1200))
        if len(text) < min_chars and not self._allow_short_repair(
            business_family_id=business_family_id,
            dirty_states=dirty_states,
            text=text,
        ):
            return {"repair_candidate": False, "entry_reason": "text_too_short"}
        if len(text) > max_chars:
            return {"repair_candidate": False, "entry_reason": "text_too_long"}

        return {
            "repair_candidate": True,
            "entry_reason": failure_reason or "runtime_gate_near_miss",
            "dirty_states": dirty_states,
            "target_business_card": target_business_card,
            "question_card_id": question_card_id,
            "normalized_family_id": normalized_family_id,
        }

    def repair(
        self,
        *,
        item: dict[str, Any],
        business_family_id: str,
        question_card_id: str,
        target_business_card: str,
        failure_reason: str,
        dirty_states: list[str],
    ) -> dict[str, Any] | None:
        family_config = self._family_config(business_family_id)
        rewrite_mode = str(family_config.get("rewrite_mode") or "preserve_rewrite_80")
        preserve_ratio_target = float(family_config.get("preserve_ratio_target") or 0.8)
        source_text = str(item.get("text") or item.get("original_text") or "").strip()
        if not source_text:
            return None
        rule_based_text = self._rule_based_repair(
            source_text=source_text,
            business_family_id=business_family_id,
            dirty_states=dirty_states,
        )
        if rule_based_text and rule_based_text != source_text:
            preserve_ratio_actual = self.preserve_ratio(source_text, rule_based_text)
            if preserve_ratio_actual >= preserve_ratio_target or set(dirty_states).issubset(self.RULE_BASED_NOISE_STATES):
                return {
                    "rewritten_text": rule_based_text,
                    "rewrite_summary": "规则型轻修：清除题干/版面残留并收束明显噪声，不改动主旨与结构主轴。",
                    "preserve_ratio_target": preserve_ratio_target,
                    "preserve_ratio_estimate": preserve_ratio_actual,
                    "preserve_ratio_actual": preserve_ratio_actual,
                    "rewrite_mode": f"{rewrite_mode}+rule_clean",
                    "target_business_card": target_business_card,
                    "dirty_states": list(dirty_states),
                }
        schema = {
            "type": "object",
            "properties": {
                "rewritten_text": {"type": "string"},
                "rewrite_summary": {"type": "string"},
                "preserve_ratio_estimate": {"type": "number"},
            },
            "required": ["rewritten_text", "rewrite_summary", "preserve_ratio_estimate"],
            "additionalProperties": False,
        }
        user_prompt = "\n".join(
            [
                f"business_family_id: {self._normalize_family_id(business_family_id)}",
                f"question_card_id: {question_card_id}",
                f"business_card_id: {target_business_card}",
                f"rewrite_mode: {rewrite_mode}",
                f"preserve_ratio_target: {preserve_ratio_target}",
                f"rewrite_goal: {self._rewrite_goal(business_family_id=business_family_id, target_business_card=target_business_card, dirty_states=dirty_states)}",
                f"rewrite_guardrail: {self._rewrite_guardrail(business_family_id=business_family_id, target_business_card=target_business_card)}",
                f"near_miss_reason: {failure_reason}",
                f"dirty_states: {', '.join(dirty_states)}",
                f"candidate_type: {str(item.get('candidate_type') or '')}",
                f"quality_score: {float(item.get('quality_score') or 0.0)}",
                f"llm_generation_readiness: {float(((item.get('llm_generation_readiness') or {}).get('score')) or 0.0)}",
                "source_text:",
                source_text,
            ]
        )
        try:
            result = self.provider.generate_json(
                model=str(
                    family_config.get("model")
                    or self.llm_config.get("models", {}).get("candidate_planner_v2")
                    or self.llm_config.get("models", {}).get("family_tagger")
                    or "gpt-4o-mini"
                ),
                instructions=self.prompt,
                input_payload={
                    "prompt": user_prompt,
                    "schema_name": f"depth2_repair_{target_business_card}",
                    "schema": schema,
                },
            )
        except Exception:
            return None

        rewritten_text = str((result or {}).get("rewritten_text") or "").strip()
        if not rewritten_text:
            return None
        preserve_ratio_actual = self.preserve_ratio(source_text, rewritten_text)
        if preserve_ratio_actual < preserve_ratio_target:
            return None
        if rewritten_text == source_text:
            return None
        return {
            "rewritten_text": rewritten_text,
            "rewrite_summary": str((result or {}).get("rewrite_summary") or "").strip(),
            "preserve_ratio_target": preserve_ratio_target,
            "preserve_ratio_estimate": float((result or {}).get("preserve_ratio_estimate") or 0.0),
            "preserve_ratio_actual": preserve_ratio_actual,
            "rewrite_mode": rewrite_mode,
            "target_business_card": target_business_card,
            "dirty_states": list(dirty_states),
        }

    def collect_dirty_states(
        self,
        *,
        item: dict[str, Any],
        business_family_id: str,
        failure_reason: str,
    ) -> list[str]:
        text = str(item.get("text") or item.get("original_text") or "").strip()
        candidate_type = str(item.get("candidate_type") or "")
        neutral = dict(item.get("neutral_signal_profile") or {})
        business = dict(item.get("business_feature_profile") or {})
        flags = list(item.get("quality_flags") or [])
        dirty_states: list[str] = []

        if re.search(r"<(?:/?w:|/?xml|/?html|/?body|/?p|/?span|/?div)[^>]*>", text, flags=re.IGNORECASE):
            dirty_states.append("page_residue")
        if "\t" in text or text.count("\n\n\n") > 0 or re.search(r"[ ]{3,}", text):
            dirty_states.append("layout_break")
        if re.search(
            r"^\s*\u3010[^\u3011]{0,20}(?:\u9898\u5e72|\u6750\u6599|\u63d0\u793a)[^\u3011]{0,20}\u3011"
            r"|^\s*(?:\u8fd9\u6bb5\u6750\u6599|\u8fd9\u5219\u6750\u6599|\u8fd9\u6bb5\u6587\u5b57)\u610f\u5728\u8bf4\u660e[:\uff1a]"
            r"|\u9898\u5e72\u63d0\u793a"
            r"|\u4e0b\u5217\u9009\u9879\u4e2d"
            r"|\u6a2a\u7ebf\u5904"
            r"|\u4e0a\u6587\u6a2a\u7ebf\u5904"
            r"|\u4f9d\u6b21\u586b\u5165"
            r"|\u586b\u5165.{0,12}(?:\u6700\u6070\u5f53|\u6700\u5408\u9002)",
            text,
        ):
            dirty_states.append("question_stem_contamination")
        if "context_opening" in flags or text.startswith(self.DEPENDENCY_OPENINGS):
            dirty_states.append("truncated_context")
        if candidate_type in {"whole_passage", "multi_paragraph_unit"} and len(text) >= 360:
            dirty_states.append("over_appended_context")
        if failure_reason in {
            "material_card_score_below_threshold",
            "business_card_score_below_threshold",
            "final_candidate_score_below_contract_floor",
            "readiness_score_below_contract_floor",
            "constraint_intensity_below_contract_floor",
            "reasoning_depth_below_contract_floor",
            "complexity_below_contract_floor",
        }:
            dirty_states.append("structure_signal_impure")

        normalized_family = self._normalize_family_id(business_family_id)
        if normalized_family == "center_understanding":
            non_key_detail_density = float(
                business.get("non_key_detail_density")
                or neutral.get("non_key_detail_density")
                or 0.0
            )
            single_center = float(neutral.get("single_center_strength") or 0.0)
            summary_strength = float(neutral.get("summary_strength") or 0.0)
            if non_key_detail_density >= 0.40 and single_center >= 0.38:
                dirty_states.append("example_overdominant")
            if single_center < 0.62 and summary_strength < 0.62:
                dirty_states.append("main_axis_diluted")
        elif normalized_family == "sentence_fill":
            fill_profile = dict(business.get("sentence_fill_profile") or {})
            if candidate_type != "functional_slot_unit":
                dirty_states.append("shape_misaligned_for_task")
            if float(fill_profile.get("bidirectional_validation") or 0.0) < 0.48:
                dirty_states.append("structure_signal_impure")
        elif normalized_family == "sentence_order":
            order_profile = dict(business.get("sentence_order_profile") or {})
            if candidate_type not in {"ordered_unit_group", "sentence_block_group", "weak_formal_order_group"}:
                dirty_states.append("shape_misaligned_for_task")
            if float(order_profile.get("sequence_integrity") or 0.0) < 0.54:
                dirty_states.append("structure_signal_impure")

        return list(dict.fromkeys(dirty_states))

    def mark_failure(
        self,
        *,
        item: dict[str, Any],
        business_family_id: str,
        target_business_card: str,
    ) -> None:
        key = self._failure_key(
            item=item,
            business_family_id=business_family_id,
            target_business_card=target_business_card,
        )
        if not key:
            return
        if len(self.FAILED_REPAIR_KEYS) >= self.MAX_FAILED_REPAIR_KEYS:
            self.FAILED_REPAIR_KEYS.clear()
        self.FAILED_REPAIR_KEYS.add(key)

    @staticmethod
    def preserve_ratio(source_text: str, rewritten_text: str) -> float:
        if not source_text:
            return 0.0
        return round(SequenceMatcher(a=source_text, b=rewritten_text).ratio(), 4)

    def _rewrite_goal(
        self,
        *,
        business_family_id: str,
        target_business_card: str,
        dirty_states: list[str],
    ) -> str:
        normalized_family = self._normalize_family_id(business_family_id)
        if normalized_family == "sentence_fill":
            if "opening" in target_business_card:
                family_goal = "收成开头位，明确引出主话题或对象，保持原文方向不变。"
            elif "carry_previous" in target_business_card:
                family_goal = "收成承前位，重点接住前文落点，避免改成泛泛说明。"
            elif "bridge" in target_business_card:
                family_goal = "收成桥接位，同时回扣前文并自然引出后文，避免单向推进。"
            else:
                family_goal = "收紧为可消费的槽位承载单元，突出原文已有的上下文约束。"
        elif normalized_family == "sentence_order":
            if "timeline" in target_business_card:
                family_goal = "强化原文已有的时间或动作推进链，不改写主题。"
            elif "head_tail" in target_business_card:
                family_goal = "强化原文已有的首尾方向感和顺序锚点，不重造逻辑。"
            else:
                family_goal = "强化原文已有的顺序链、局部绑定和推进关系。"
        else:
            family_goal = "收束原文主轴，削弱枝杈和噪声，让中心主旨更清晰，但不改变论证方向。"
        if dirty_states:
            return f"{family_goal} 优先处理这些脏状态：{', '.join(dirty_states)}。"
        return family_goal

    def _rewrite_guardrail(
        self,
        *,
        business_family_id: str,
        target_business_card: str,
    ) -> str:
        normalized_family = self._normalize_family_id(business_family_id)
        if normalized_family == "sentence_fill":
            return (
                "只能做精细清洗和槽位特征显化，不得把答案直接补回空位，不得把局部材料改成已完成填空的成品段落。"
                "如果源文混入“填入画横线部分最恰当的一句”“下列选项中”“上文横线处”等题干或选项提示，只删除这些残留，保留可供后续题目服务消费的材料载体。"
            )
        if normalized_family == "sentence_order":
            return "只能恢复原文已有的顺序锚点、局部绑定和推进关系，不要新增原文没有的论证节点或换一种主题写法。"
        return "只能做去噪、收束、特征显化，不能改变主旨、结构主轴和关键事实。"

    def _allow_short_repair(
        self,
        *,
        business_family_id: str,
        dirty_states: list[str],
        text: str,
    ) -> bool:
        normalized_family = self._normalize_family_id(business_family_id)
        if normalized_family != "sentence_fill":
            return False
        dirty_state_set = set(dirty_states)
        allowed_companions = {"structure_signal_impure", "shape_misaligned_for_task"}
        if not dirty_state_set:
            return False
        stripped = text.strip()
        if dirty_state_set.issubset(self.RULE_BASED_NOISE_STATES) and len(stripped) >= 20:
            return True
        rule_based_text = self._rule_based_repair(
            source_text=stripped,
            business_family_id=business_family_id,
            dirty_states=dirty_states,
        )
        if not rule_based_text or rule_based_text == stripped:
            return False
        return (
            "question_stem_contamination" in dirty_state_set
            and dirty_state_set.issubset(self.RULE_BASED_NOISE_STATES | allowed_companions)
            and len(rule_based_text) >= 10
        )

    def _rule_based_repair(
        self,
        *,
        source_text: str,
        business_family_id: str,
        dirty_states: list[str],
    ) -> str:
        if not set(dirty_states).intersection(self.RULE_BASED_NOISE_STATES):
            return ""
        text = source_text
        text = re.sub(
            r"^\s*(?:\u3010\u9898\u5e72\u63d0\u793a\u3011)?(?:\u8fd9\u6bb5\u6750\u6599|\u8fd9\u5219\u6750\u6599|\u8fd9\u6bb5\u6587\u5b57)\u610f\u5728\u8bf4\u660e[:\uff1a]?\s*",
            "",
            text,
        )
        text = re.sub(r"^\s*【第\d+页[^】]*】[。．]?\s*", "", text)
        text = re.sub(r"\s*【第\d+页[^】]*】\s*", "", text)
        text = re.sub(r"\s*[（(]第\d+页[^）)]*[）)]\s*", "", text)
        text = re.sub(r"\s*〔版心残留〕\s*", "", text)
        text = re.sub(r"\s*[（(]补注[）)]\s*", "", text)
        text = re.sub(r"\s*[（(]数据旁注[）)]\s*", "", text)
        text = text.replace("\t", "")
        text = re.sub(r"\n{2,}", "\n", text)
        text = re.sub(r"[ ]{2,}", " ", text)
        text = re.sub(r"\n(?=[，。；：])", "", text)
        text = re.sub(r"\s+\n", "\n", text)
        if self._normalize_family_id(business_family_id) == "center_understanding" and "example_overdominant" in set(dirty_states):
            text = self._trim_inserted_example_sentence(text)
        text = self._collapse_repeated_units(text)
        text = self._trim_repeated_tail(text)
        if "over_appended_context" in set(dirty_states):
            text = self._drop_repeated_units(text)
        if self._normalize_family_id(business_family_id) == "sentence_order" and "over_appended_context" in set(dirty_states):
            text = self._collapse_exact_repeat(text)
        return text.strip()

    @staticmethod
    def _trim_repeated_tail(text: str) -> str:
        units = re.findall(r"[^。！？；!?;]+[。！？；!?;]?", text)
        units = [unit.strip() for unit in units if unit.strip()]
        if len(units) >= 2 and units[-1] == units[-2]:
            units = units[:-1]
        return "".join(units) if units else text

    @staticmethod
    def _collapse_repeated_units(text: str) -> str:
        units = re.findall(r"[^。！？；!?;]+[。！？；!?;]?", text)
        units = [unit.strip() for unit in units if unit.strip()]
        if not units:
            return text
        collapsed: list[str] = []
        for unit in units:
            if collapsed and collapsed[-1] == unit:
                continue
            collapsed.append(unit)
        return "".join(collapsed)

    @staticmethod
    def _trim_inserted_example_sentence(text: str) -> str:
        units = re.findall(r"[^。！？；!?;]+[。！？；!?;]?", text)
        units = [unit.strip() for unit in units if unit.strip()]
        if len(units) <= 2:
            return text
        filtered: list[str] = []
        removed = False
        for index, unit in enumerate(units):
            if removed:
                filtered.append(unit)
                continue
            if index not in {0, len(units) - 1} and re.match(r"^(比如|例如|譬如)[，,]", unit):
                removed = True
                continue
            filtered.append(unit)
        return "".join(filtered) if removed else text

    @staticmethod
    def _drop_repeated_units(text: str) -> str:
        units = re.findall(r"[^。！？；!?;]+[。！？；!?;]?", text)
        units = [unit.strip() for unit in units if unit.strip()]
        if not units:
            return text
        seen: set[str] = set()
        filtered: list[str] = []
        for unit in units:
            if unit in seen:
                continue
            seen.add(unit)
            filtered.append(unit)
        return "".join(filtered)

    @staticmethod
    def _collapse_exact_repeat(text: str) -> str:
        stripped = text.strip()
        if not stripped or len(stripped) % 2 != 0:
            return stripped
        half = len(stripped) // 2
        if stripped[:half] == stripped[half:]:
            return stripped[:half].strip()
        return stripped

    def _family_config(self, business_family_id: str) -> dict[str, Any]:
        normalized = self._normalize_family_id(business_family_id)
        families = dict(self.config.get("families") or {})
        return dict(families.get(business_family_id) or families.get(normalized) or {})

    def _normalize_family_id(self, business_family_id: str) -> str:
        return str(self.FAMILY_ALIASES.get(business_family_id, business_family_id))

    @staticmethod
    def _target_business_card(item: dict[str, Any]) -> str:
        selected = str(item.get("selected_business_card") or ((item.get("question_ready_context") or {}).get("selected_business_card")) or "").strip()
        if selected:
            return selected
        recommendations = list(item.get("business_card_recommendations") or [])
        return str(recommendations[0]).strip() if recommendations else ""

    @staticmethod
    def _already_repaired_or_locked(item: dict[str, Any]) -> bool:
        candidate_id = str(item.get("candidate_id") or "").strip()
        meta = dict(item.get("meta") or {})
        question_ready_context = dict(item.get("question_ready_context") or {})
        depth2_repair = question_ready_context.get("depth2_repair")
        return bool(
            candidate_id.endswith(":repair")
            or meta.get("repair_generated")
            or item.get("repair_trace")
            or depth2_repair
        )

    def _failure_key(
        self,
        *,
        item: dict[str, Any],
        business_family_id: str,
        target_business_card: str,
    ) -> str:
        candidate_id = str(item.get("candidate_id") or "").strip()
        article_id = str(item.get("article_id") or "").strip()
        text = str(item.get("text") or item.get("original_text") or "").strip()
        if not target_business_card or not text:
            return ""
        payload = "|".join(
            [
                self._normalize_family_id(business_family_id),
                target_business_card,
                article_id,
                candidate_id,
                hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
            ]
        )
        return payload
