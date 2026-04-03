from __future__ import annotations

import re
from collections import Counter
from typing import Any

from app.schemas.question import SourceQuestionPayload


_STOPWORDS = {
    "下列",
    "最适合",
    "最能够",
    "最恰当",
    "不正确",
    "正确",
    "错误",
    "不符合",
    "理解",
    "标题",
    "主旨",
    "文段",
    "文章",
    "材料",
    "语句",
    "中心",
    "意在",
    "说明",
    "表明",
    "根据",
    "根据这段文字",
    "根据文段",
    "重点在于",
    "中心判断",
    "中心意图",
    "关于",
}

_TURNING_MARKERS = ("但是", "但", "然而", "不过", "却", "其实", "事实上", "实际上", "尽管", "虽然")
_CAUSE_MARKERS = ("因为", "由于", "缘于", "起因于", "所以", "因此", "因而", "故而", "故", "于是", "可见", "看来", "由此")
_NECESSARY_MARKERS = ("只有", "才", "必须", "应当", "采取", "通过", "对策", "措施", "途径", "方式", "前提", "基础", "保障")
_PARALLEL_MARKERS = ("此外", "另外", "同时", "以及", "并且", "一方面", "另一方面", "其一", "其二", "首先", "其次", "再次")
_THEME_MARKERS = ("围绕", "主题", "核心", "主线", "关键")

_ORDER_DEFINITION_MARKERS = ("就是", "是指", "指的是")
_ORDER_PRONOUN_MARKERS = ("这", "那", "其", "该", "此", "这些", "那些", "他们", "它们")
_ORDER_TURNING_BINDING_MARKERS = ("虽然", "但是", "可是", "然而", "不过", "却")
_ORDER_PARALLEL_BINDING_MARKERS = ("同时", "同样", "此外", "也", "另一方面")
_ORDER_TIMELINE_MARKERS = ("起初", "后来", "随后", "接着", "最终", "先", "再", "最后", "当时", "近年来")
_ORDER_ACTION_MARKERS = ("首先", "其次", "再次", "最后", "第一步", "第二步", "第三步")
_ORDER_PROBLEM_MARKERS = ("问题在于", "问题是", "困境在于", "难点在于")
_ORDER_SOLUTION_MARKERS = ("因此", "所以", "由此", "应当", "应该", "必须", "要")
_ORDER_QUESTION_MARKERS = ("为什么", "如何", "怎么办", "怎样")
_ORDER_SUMMARY_CLOSING_MARKERS = ("因此", "所以", "可见", "看来", "总之", "由此")

_FILL_BLANK_MARKERS = ("____", "___", "______", "[BLANK]", "（  ）", "( )", "——")

_BUSINESS_CARD_MAP = {
    "turning": "turning_relation_focus__main_idea",
    "cause_effect": "cause_effect__conclusion_focus__main_idea",
    "necessary_condition": "necessary_condition_countermeasure__main_idea",
    "parallel": "parallel_comprehensive_summary__main_idea",
    "theme": "theme_word_focus__main_idea",
}

_SENTENCE_ORDER_CARD_IDS = {
    "head_tail_logic": "sentence_order__head_tail_logic__abstract",
    "head_tail": "sentence_order__head_tail_lock__abstract",
    "binding": "sentence_order__deterministic_binding__abstract",
    "discourse": "sentence_order__discourse_logic__abstract",
    "timeline": "sentence_order__timeline_action_sequence__abstract",
}

_SENTENCE_FILL_CARD_IDS = {
    "opening_summary": "sentence_fill__opening_summary__abstract",
    "opening_topic_intro": "sentence_fill__opening_topic_intro__abstract",
    "middle_carry_previous": "sentence_fill__middle_carry_previous__abstract",
    "middle_lead_next": "sentence_fill__middle_lead_next__abstract",
    "middle_bridge_both_sides": "sentence_fill__middle_bridge_both_sides__abstract",
    "ending_summary": "sentence_fill__ending_summary__abstract",
    "ending_countermeasure": "sentence_fill__ending_countermeasure__abstract",
}


class SourceQuestionAnalyzer:
    def infer_request_target(
        self,
        source_question: SourceQuestionPayload | None,
    ) -> dict[str, Any]:
        if source_question is None:
            return {}
        stem = (source_question.stem or "").strip()
        combined = f"{stem}\n{source_question.passage or ''}"
        sentence_order_markers = (
            "\u6392\u5e8f",
            "\u91cd\u65b0\u6392\u5217",
            "\u8bed\u5e8f\u6b63\u786e",
            "\u5c06\u4ee5\u4e0a",
            "\u5c06\u4ee5\u4e0b",
        )
        sentence_fill_markers = (
            "\u586b\u5165",
            "\u6a2a\u7ebf",
            "\u6700\u6070\u5f53\u7684\u4e00\u53e5",
            "\u6700\u6070\u5f53\u7684\u4e00\u9879",
        )
        continuation_markers = (
            "\u63a5\u5728",
            "\u63a5\u7eed",
            "\u63a5\u8bed",
            "\u8854\u63a5",
        )
        center_understanding_markers = (
            "\u610f\u5728\u5f3a\u8c03",
            "\u610f\u5728\u8bf4\u660e",
            "\u4e3b\u8981\u8bf4\u660e",
            "\u4e3b\u8981\u5f3a\u8c03",
            "\u4e2d\u5fc3\u7406\u89e3",
            "\u4e3b\u65e8",
        )
        if any(token in combined for token in sentence_order_markers):
            return {
                "question_type": "sentence_order",
                "business_subtype": None,
                "reason": "source_question_stem_detected_sentence_order",
            }
        if any(token in combined for token in sentence_fill_markers):
            return {
                "question_type": "sentence_fill",
                "business_subtype": None,
                "reason": "source_question_stem_detected_sentence_fill",
            }
        if any(token in combined for token in continuation_markers):
            return {
                "question_type": "continuation",
                "business_subtype": None,
                "reason": "source_question_stem_detected_continuation",
            }
        if "\u6807\u9898" in combined:
            return {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "reason": "source_question_stem_detected_title_selection",
            }
        if any(token in combined for token in center_understanding_markers):
            return {
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "reason": "source_question_stem_detected_center_understanding",
            }
        return {}

    def analyze(
        self,
        *,
        source_question: SourceQuestionPayload | None,
        question_type: str,
        business_subtype: str | None,
    ) -> dict[str, Any]:
        if source_question is None:
            return {}

        joined_parts = [
            source_question.passage or "",
            source_question.stem or "",
            " ".join(value for _, value in sorted((source_question.options or {}).items())),
            source_question.analysis or "",
        ]
        combined_text = "\n".join(part.strip() for part in joined_parts if part and part.strip())
        normalized = self._normalize_text(combined_text)

        if question_type == "sentence_order":
            analysis = self._analyze_sentence_order(source_question)
        elif question_type == "sentence_fill":
            analysis = self._analyze_sentence_fill(source_question)
        else:
            analysis = self._analyze_main_idea(source_question, normalized)

        target_length = self._derive_target_length(
            passage=source_question.passage,
            question_type=question_type,
            business_subtype=business_subtype,
            structure_constraints=analysis.get("structure_constraints") or {},
        )
        length_tolerance = 75

        return {
            "reference_present": True,
            "topic": analysis.get("topic"),
            "query_terms": analysis.get("query_terms") or [],
            "business_card_ids": analysis.get("business_card_ids") or [],
            "business_card_scores": analysis.get("business_card_scores") or [],
            "target_length": target_length,
            "length_tolerance": length_tolerance,
            "anchor_strategy": "preserve_anchor_trim_expand",
            "structure_priority": "high",
            "content_priority": "secondary",
            "style_summary": self._build_style_summary(source_question, question_type=question_type),
            "structure_constraints": analysis.get("structure_constraints") or {},
        }

    def _analyze_main_idea(self, source_question: SourceQuestionPayload, normalized: str) -> dict[str, Any]:
        focus_text = self._normalize_text("\n".join([source_question.passage or "", source_question.stem or ""]))
        business_scores = self._score_main_idea_business_cards(focus_text or normalized)
        business_card_ids = [card_id for card_id, score in business_scores if score >= 0.45][:3]
        if not business_card_ids:
            business_card_ids = [_BUSINESS_CARD_MAP["theme"]]

        query_source = "\n".join(
            part.strip()
            for part in [
                source_question.passage or "",
                " ".join(value for _, value in sorted((source_question.options or {}).items())),
            ]
            if part and part.strip()
        )
        query_terms = self._extract_query_terms(query_source)
        return {
            "topic": query_terms[0] if query_terms else None,
            "query_terms": query_terms,
            "business_card_ids": business_card_ids,
            "business_card_scores": [
                {"business_card_id": card_id, "score": round(score, 4)}
                for card_id, score in business_scores
                if score >= 0.30
            ],
            "structure_constraints": {},
        }

    def _analyze_sentence_order(self, source_question: SourceQuestionPayload) -> dict[str, Any]:
        passage = source_question.passage or ""
        analysis = source_question.analysis or ""
        normalized = self._normalize_text("\n".join([passage, source_question.stem, analysis]))
        units = self._extract_sentence_order_units(passage)
        unit_count = len(units)
        logic_modes = self._infer_sentence_order_logic_modes(normalized)
        binding_types = self._infer_sentence_order_binding_types(normalized)
        opening_rule = self._infer_sentence_order_opening_rule(units)
        closing_rule = self._infer_sentence_order_closing_rule(units)
        expected_binding_pair_count = max(2, min(4, len(binding_types) + (1 if unit_count >= 5 else 0)))
        temporal_or_action_sequence_presence = any(mode in logic_modes for mode in {"timeline_sequence", "action_sequence"})
        discourse_progression_pattern = "discourse_logic"
        if "problem_solution" in logic_modes:
            discourse_progression_pattern = "problem_solution"
        elif "question_answer" in logic_modes:
            discourse_progression_pattern = "question_answer"
        elif "viewpoint_explanation" in logic_modes:
            discourse_progression_pattern = "viewpoint_explanation"
        elif temporal_or_action_sequence_presence:
            discourse_progression_pattern = "timeline_or_action_sequence"
        expected_unique_answer_strength = 0.58
        if opening_rule in {"definition_opening", "explicit_opening"}:
            expected_unique_answer_strength += 0.10
        if closing_rule in {"summary_or_conclusion", "countermeasure"}:
            expected_unique_answer_strength += 0.10
        if expected_binding_pair_count >= 2:
            expected_unique_answer_strength += 0.08
        if temporal_or_action_sequence_presence:
            expected_unique_answer_strength += 0.06
        expected_unique_answer_strength = round(min(0.92, expected_unique_answer_strength), 4)

        card_scores: list[tuple[str, float]] = []
        canonical_score = 0.32
        if opening_rule in {"definition_opening", "explicit_opening"}:
            canonical_score += 0.16
        if closing_rule in {"summary_or_conclusion", "countermeasure"}:
            canonical_score += 0.16
        canonical_score += min(0.16, 0.06 * expected_binding_pair_count)
        if discourse_progression_pattern != "discourse_logic":
            canonical_score += 0.08
        if temporal_or_action_sequence_presence:
            canonical_score += 0.06
        card_scores.append((_SENTENCE_ORDER_CARD_IDS["head_tail_logic"], min(1.0, canonical_score)))

        head_tail_score = 0.42
        if opening_rule in {"definition_opening", "explicit_opening"}:
            head_tail_score += 0.20
        if closing_rule in {"summary_or_conclusion", "countermeasure"}:
            head_tail_score += 0.20
        if unit_count >= 5:
            head_tail_score += 0.08
        card_scores.append((_SENTENCE_ORDER_CARD_IDS["head_tail"], min(1.0, head_tail_score)))

        binding_score = 0.30 + 0.18 * len(binding_types)
        if "deterministic_binding" in logic_modes:
            binding_score += 0.16
        card_scores.append((_SENTENCE_ORDER_CARD_IDS["binding"], min(1.0, binding_score)))

        discourse_score = 0.22
        if "discourse_logic" in logic_modes:
            discourse_score += 0.46
        if any(mode in logic_modes for mode in {"problem_solution", "question_answer", "viewpoint_explanation"}):
            discourse_score += 0.16
        card_scores.append((_SENTENCE_ORDER_CARD_IDS["discourse"], min(1.0, discourse_score)))

        timeline_score = 0.20
        if "timeline_sequence" in logic_modes:
            timeline_score += 0.42
        if "action_sequence" in logic_modes:
            timeline_score += 0.24
        card_scores.append((_SENTENCE_ORDER_CARD_IDS["timeline"], min(1.0, timeline_score)))

        card_scores.sort(key=lambda item: item[1], reverse=True)
        business_card_ids = [card_id for card_id, score in card_scores if score >= 0.42][:3]
        if not business_card_ids:
            business_card_ids = [_SENTENCE_ORDER_CARD_IDS["head_tail_logic"]]

        query_terms = self._extract_query_terms(passage or analysis)
        return {
            "topic": query_terms[0] if query_terms else None,
            "query_terms": query_terms,
            "business_card_ids": business_card_ids,
            "business_card_scores": [
                {"business_card_id": card_id, "score": round(score, 4)}
                for card_id, score in card_scores
                if score >= 0.20
            ],
            "structure_constraints": {
                "sortable_unit_count": unit_count,
                "logic_modes": logic_modes,
                "binding_types": binding_types,
                "opening_rule": opening_rule,
                "closing_rule": closing_rule,
                "ordering_skeleton": {
                    "opener_type": opening_rule,
                    "closing_type": closing_rule,
                    "binding_types": binding_types,
                    "expected_binding_pair_count": expected_binding_pair_count,
                    "discourse_progression_pattern": discourse_progression_pattern,
                    "temporal_or_action_sequence_presence": temporal_or_action_sequence_presence,
                    "expected_unique_answer_strength": expected_unique_answer_strength,
                },
                "orderability_profile": {
                    "opener_type": opening_rule,
                    "closing_type": closing_rule,
                    "binding_types": binding_types,
                    "expected_binding_pair_count": expected_binding_pair_count,
                    "discourse_progression_pattern": discourse_progression_pattern,
                    "temporal_or_action_sequence_presence": temporal_or_action_sequence_presence,
                    "expected_unique_answer_strength": expected_unique_answer_strength,
                },
                "expected_binding_pair_count": expected_binding_pair_count,
                "discourse_progression_pattern": discourse_progression_pattern,
                "temporal_or_action_sequence_presence": temporal_or_action_sequence_presence,
                "expected_unique_answer_strength": expected_unique_answer_strength,
                "preserve_unit_count": True,
            },
        }

    def _analyze_sentence_fill(self, source_question: SourceQuestionPayload) -> dict[str, Any]:
        passage = source_question.passage or ""
        analysis = source_question.analysis or ""
        normalized = self._normalize_text("\n".join([passage, source_question.stem, analysis]))
        blank_position = self._infer_blank_position(passage)
        function_type = self._infer_fill_business_function(
            normalized=normalized,
            blank_position=blank_position,
        )
        unit_type = self._infer_fill_unit_type(passage)

        card_scores = self._score_sentence_fill_cards(blank_position=blank_position, function_type=function_type)
        business_card_ids = [card_id for card_id, score in card_scores if score >= 0.46][:3]
        if not business_card_ids:
            business_card_ids = [_SENTENCE_FILL_CARD_IDS["middle_bridge_both_sides"]]

        query_terms = self._extract_query_terms(passage or analysis)
        return {
            "topic": query_terms[0] if query_terms else None,
            "query_terms": query_terms,
            "business_card_ids": business_card_ids,
            "business_card_scores": [
                {"business_card_id": card_id, "score": round(score, 4)}
                for card_id, score in card_scores
                if score >= 0.20
            ],
            "structure_constraints": {
                "blank_position": blank_position,
                "function_type": function_type,
                "unit_type": unit_type,
                "preserve_blank_position": True,
            },
        }

    def _normalize_text(self, text: str) -> str:
        normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\s+", "", normalized)
        return normalized

    def _score_main_idea_business_cards(self, text: str) -> list[tuple[str, float]]:
        scores = {
            _BUSINESS_CARD_MAP["turning"]: self._marker_score(text, _TURNING_MARKERS),
            _BUSINESS_CARD_MAP["cause_effect"]: self._marker_score(text, _CAUSE_MARKERS),
            _BUSINESS_CARD_MAP["necessary_condition"]: self._marker_score(text, _NECESSARY_MARKERS),
            _BUSINESS_CARD_MAP["parallel"]: self._marker_score(text, _PARALLEL_MARKERS),
            _BUSINESS_CARD_MAP["theme"]: 0.34 + self._marker_score(text, _THEME_MARKERS) * 0.4,
        }
        if text.count("；") + text.count(";") >= 1:
            scores[_BUSINESS_CARD_MAP["parallel"]] += 0.12
        if "只有" in text and "才" in text:
            scores[_BUSINESS_CARD_MAP["necessary_condition"]] += 0.22
        if ("因为" in text and "所以" in text) or ("由于" in text and "因此" in text):
            scores[_BUSINESS_CARD_MAP["cause_effect"]] += 0.22
        if ("虽然" in text and "但是" in text) or ("尽管" in text and "可是" in text):
            scores[_BUSINESS_CARD_MAP["turning"]] += 0.22
        return sorted(((card_id, min(1.0, score)) for card_id, score in scores.items()), key=lambda item: item[1], reverse=True)

    def _marker_score(self, text: str, markers: tuple[str, ...]) -> float:
        hits = sum(text.count(marker) for marker in markers)
        return min(1.0, 0.20 * hits + (0.28 if hits > 0 else 0.0))

    def _extract_query_terms(self, text: str) -> list[str]:
        counts: Counter[str] = Counter()
        clauses = re.split(r"[，。；：、“”‘’（）()\n\r\t ]+", text or "")
        for clause in clauses:
            normalized = re.sub(r"[^\u4e00-\u9fff]", "", clause)
            if len(normalized) < 3:
                continue
            if 3 <= len(normalized) <= 8:
                if self._is_valid_query_term(normalized):
                    counts[normalized] += 2
                continue
            for size in (4, 5, 6):
                for start in range(0, max(0, len(normalized) - size + 1)):
                    piece = normalized[start : start + size]
                    if self._is_valid_query_term(piece):
                        counts[piece] += 1
        ranked = [term for term, _ in counts.most_common(16)]
        compact: list[str] = []
        for term in ranked:
            if any(term in existing or existing in term for existing in compact):
                continue
            compact.append(term)
            if len(compact) >= 8:
                break
        return compact

    def _is_valid_query_term(self, term: str) -> bool:
        if term in _STOPWORDS:
            return False
        if term in _TURNING_MARKERS or term in _CAUSE_MARKERS or term in _NECESSARY_MARKERS or term in _PARALLEL_MARKERS:
            return False
        if term in _THEME_MARKERS:
            return False
        return len(term) >= 3

    def _derive_target_length(
        self,
        *,
        passage: str | None,
        question_type: str,
        business_subtype: str | None,
        structure_constraints: dict[str, Any],
    ) -> int:
        if passage:
            return max(120, min(700, len(passage.strip())))
        if question_type == "sentence_order":
            unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
            return 90 + max(unit_count, 4) * 28
        if question_type == "sentence_fill":
            return 200
        if question_type == "continuation":
            return 260
        if question_type == "main_idea" and business_subtype == "center_understanding":
            return 260
        return 300

    def _build_style_summary(self, source_question: SourceQuestionPayload, *, question_type: str) -> dict[str, Any]:
        option_lengths = [len((value or "").strip()) for value in (source_question.options or {}).values() if (value or "").strip()]
        passage = (source_question.passage or "").strip()
        return {
            "question_type": question_type,
            "option_count": len(source_question.options or {}),
            "average_option_length": round(sum(option_lengths) / len(option_lengths), 2) if option_lengths else 0.0,
            "has_reference_analysis": bool((source_question.analysis or "").strip()),
            "has_reference_passage": bool(passage),
            "stem_length": len((source_question.stem or "").strip()),
            "passage_length": len(passage),
        }

    def _extract_sentence_order_units(self, passage: str) -> list[str]:
        text = (passage or "").strip()
        if not text:
            return []
        enumerated = [part.strip() for part in re.split(r"(?=[①②③④⑤⑥⑦⑧⑨⑩])", text) if part.strip()]
        if len(enumerated) >= 4:
            return enumerated
        numbered = re.findall(r"(?:^|[\n\r])\s*[(（]?\d+[)）\.、]?\s*([^\n\r]+)", text)
        if len(numbered) >= 4:
            return [item.strip() for item in numbered if item.strip()]
        sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", text) if item.strip()]
        return sentences

    def _infer_sentence_order_logic_modes(self, text: str) -> list[str]:
        modes: list[str] = []
        if any(marker in text for marker in _ORDER_TIMELINE_MARKERS):
            modes.append("timeline_sequence")
        if any(marker in text for marker in _ORDER_ACTION_MARKERS):
            modes.append("action_sequence")
        if any(marker in text for marker in _ORDER_PROBLEM_MARKERS) and any(marker in text for marker in _ORDER_SOLUTION_MARKERS):
            modes.extend(["discourse_logic", "problem_solution"])
        if any(marker in text for marker in _ORDER_QUESTION_MARKERS):
            modes.extend(["discourse_logic", "question_answer"])
        if any(marker in text for marker in ("观点", "看法", "认识", "意味着")):
            modes.extend(["discourse_logic", "viewpoint_explanation"])
        if any(marker in text for marker in _ORDER_PRONOUN_MARKERS + _ORDER_TURNING_BINDING_MARKERS + _ORDER_PARALLEL_BINDING_MARKERS):
            modes.append("deterministic_binding")
        deduped: list[str] = []
        for mode in modes:
            if mode not in deduped:
                deduped.append(mode)
        return deduped or ["discourse_logic"]

    def _infer_sentence_order_binding_types(self, text: str) -> list[str]:
        binding_types: list[str] = []
        if any(marker in text for marker in _ORDER_PRONOUN_MARKERS):
            binding_types.append("pronoun_reference")
        if any(marker in text for marker in _ORDER_TURNING_BINDING_MARKERS):
            binding_types.append("turning_connector")
        if any(marker in text for marker in _ORDER_PARALLEL_BINDING_MARKERS):
            binding_types.append("parallel_connector")
        return binding_types

    def _infer_sentence_order_opening_rule(self, units: list[str]) -> str:
        if not units:
            return "weak_opening"
        first = units[0]
        if any(marker in first for marker in _ORDER_DEFINITION_MARKERS):
            return "definition_opening"
        if first.startswith(_ORDER_PRONOUN_MARKERS) or any(marker in first for marker in ("但是", "然而", "比如", "例如")):
            return "weak_opening"
        return "explicit_opening"

    def _infer_sentence_order_closing_rule(self, units: list[str]) -> str:
        if not units:
            return "none"
        last = units[-1]
        if any(marker in last for marker in _ORDER_SUMMARY_CLOSING_MARKERS):
            return "summary_or_conclusion"
        if any(marker in last for marker in ("应该", "应当", "必须", "要")):
            return "countermeasure"
        return "none"

    def _infer_blank_position(self, passage: str) -> str:
        text = (passage or "").strip()
        if not text:
            return "middle"
        marker_index = -1
        marker_len = 0
        for marker in _FILL_BLANK_MARKERS:
            idx = text.find(marker)
            if idx >= 0:
                marker_index = idx
                marker_len = len(marker)
                break
        if marker_index < 0:
            return "middle"
        ratio = marker_index / max(len(text), 1)
        if ratio <= 0.22:
            return "opening"
        if ratio >= 0.70:
            return "ending"
        return "middle"

    def _infer_fill_business_function(self, *, normalized: str, blank_position: str) -> str:
        if blank_position == "opening":
            if any(marker in normalized for marker in ("围绕", "总体来看", "总的来说", "概括")):
                return "summarize_following_text"
            return "topic_introduction"
        if blank_position == "ending":
            if any(marker in normalized for marker in ("应该", "应当", "必须", "要", "对策", "措施")):
                return "propose_countermeasure"
            return "summarize_previous_text"
        backward_hits = sum(normalized.count(marker) for marker in ("这", "这种", "上述", "前文", "由此"))
        forward_hits = sum(normalized.count(marker) for marker in ("因此", "从而", "接下来", "进一步", "后文"))
        if abs(backward_hits - forward_hits) <= 1:
            return "bridge_both_sides"
        if backward_hits > forward_hits:
            return "carry_previous"
        return "lead_next"

    def _infer_fill_unit_type(self, passage: str) -> str:
        text = (passage or "").strip()
        if not text:
            return "sentence"
        if any(marker in text for marker in ("，____", "，___", "____，", "___，", "；____", "；___")):
            return "clause"
        return "sentence"

    def _score_sentence_fill_cards(self, *, blank_position: str, function_type: str) -> list[tuple[str, float]]:
        base_scores = {
            _SENTENCE_FILL_CARD_IDS["opening_summary"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["opening_topic_intro"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["middle_carry_previous"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["middle_lead_next"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["middle_bridge_both_sides"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["ending_summary"]: 0.18,
            _SENTENCE_FILL_CARD_IDS["ending_countermeasure"]: 0.18,
        }
        mapping = {
            ("opening", "summarize_following_text"): _SENTENCE_FILL_CARD_IDS["opening_summary"],
            ("opening", "topic_introduction"): _SENTENCE_FILL_CARD_IDS["opening_topic_intro"],
            ("middle", "carry_previous"): _SENTENCE_FILL_CARD_IDS["middle_carry_previous"],
            ("middle", "lead_next"): _SENTENCE_FILL_CARD_IDS["middle_lead_next"],
            ("middle", "bridge_both_sides"): _SENTENCE_FILL_CARD_IDS["middle_bridge_both_sides"],
            ("ending", "summarize_previous_text"): _SENTENCE_FILL_CARD_IDS["ending_summary"],
            ("ending", "propose_countermeasure"): _SENTENCE_FILL_CARD_IDS["ending_countermeasure"],
        }
        primary = mapping.get((blank_position, function_type))
        if primary:
            base_scores[primary] = 0.92
        if blank_position == "middle" and function_type != "bridge_both_sides":
            base_scores[_SENTENCE_FILL_CARD_IDS["middle_bridge_both_sides"]] = max(
                base_scores[_SENTENCE_FILL_CARD_IDS["middle_bridge_both_sides"]],
                0.42,
            )
        return sorted(base_scores.items(), key=lambda item: item[1], reverse=True)
