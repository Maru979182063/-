from __future__ import annotations

import sys
import types
from unittest import TestCase
from unittest.mock import Mock


def _install_test_stubs() -> None:
    if "yaml" not in sys.modules:
        yaml = types.ModuleType("yaml")
        yaml.safe_load = lambda *args, **kwargs: {}
        sys.modules["yaml"] = yaml


_install_test_stubs()

from app.schemas.question import SourceQuestionPayload
from app.services.source_question_analyzer import SourceQuestionAnalyzer


class SourceQuestionAnalyzerUnitTest(TestCase):
    def setUp(self) -> None:
        self.analyzer = SourceQuestionAnalyzer()

    def test_sentence_order_analysis_normalizes_reference_unit_count_to_six(self) -> None:
        source_question = SourceQuestionPayload(
            passage="。".join(f"这是第{i}句" for i in range(1, 25)) + "。",
            stem="将下列句子重新排列，语序正确的一项是",
            options={
                "A": "①②③④⑤⑥",
                "B": "⑥⑤④③②①",
                "C": "①③⑤②④⑥",
                "D": "②①③④⑥⑤",
            },
        )

        result = self.analyzer.analyze(
            source_question=source_question,
            question_type="sentence_order",
            business_subtype=None,
        )

        self.assertEqual(result["style_summary"]["question_type"], "sentence_order")
        self.assertEqual(result["structure_constraints"]["sortable_unit_count"], 6)
        self.assertGreater(result["structure_constraints"]["reference_detected_sortable_unit_count"], 10)

    def test_llm_first_main_idea_analysis_overrides_weak_rule_signal(self) -> None:
        source_question = SourceQuestionPayload(
            passage="人工智能进入城市治理场景之后，不只是提升效率，更推动治理方式重构。文章进一步分析制度、数据和协同机制。",
            stem="这篇文章主要说明的是",
            options={"A": "", "B": "", "C": "", "D": ""},
        )
        analyzer = SourceQuestionAnalyzer()
        analyzer.llm_gateway = Mock()
        analyzer._resolve_llm_route = Mock(return_value=object())
        analyzer.llm_gateway.generate_json.return_value = {
            "topic": "城市治理重构",
            "query_terms": ["城市治理", "治理方式重构"],
            "business_card_ids": ["necessary_condition_countermeasure__main_idea"],
            "structure_constraints": {
                "sortable_unit_count": None,
                "logic_modes": [],
                "binding_types": [],
                "opening_rule": None,
                "closing_rule": None,
                "expected_binding_pair_count": None,
                "discourse_progression_pattern": None,
                "temporal_or_action_sequence_presence": None,
                "expected_unique_answer_strength": None,
                "blank_position": None,
                "function_type": None,
                "unit_type": None,
                "preserve_unit_count": None,
                "preserve_blank_position": None,
            },
            "confidence": 0.82,
            "analysis_summary": "本文围绕城市治理重构展开，主旨单一。",
            "risk_flags": [],
        }

        result = analyzer.analyze(
            source_question=source_question,
            question_type="main_idea",
            business_subtype="center_understanding",
        )

        self.assertEqual(result["analysis_mode"], "llm_first")
        self.assertEqual(result["business_card_ids"], ["necessary_condition_countermeasure__main_idea"])
        self.assertEqual(result["retrieval_preferred_business_card_ids"], ["necessary_condition_countermeasure__main_idea"])
        self.assertEqual(result["query_terms"], ["城市治理", "治理方式重构"])
