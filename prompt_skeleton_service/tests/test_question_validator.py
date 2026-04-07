from __future__ import annotations

from unittest import TestCase

from app.schemas.item import GeneratedQuestion
from app.services.question_validator import QuestionValidatorService


class QuestionValidatorUnitTest(TestCase):
    def setUp(self) -> None:
        self.validator = QuestionValidatorService()

    def test_validator_adds_exam_style_and_material_warnings(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                stem="以下是根据提供的材料生成的一道题，请你选择正确答案。",
                options={
                    "A": "人工智能正在重塑产业格局",
                    "B": "城市治理需要更多数据支持",
                    "C": "生态文明建设离不开制度保障",
                    "D": "科技创新推动社会结构转型",
                },
                answer="A",
                analysis="正确答案是A。",
            ),
            material_text="量子通信实验持续推进，深空探测任务进入新的观测阶段。",
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.passed)
        self.assertTrue(any("meta or AI-style phrasing" in warning for warning in result.warnings))
        self.assertTrue(any("weak lexical overlap" in warning for warning in result.warnings))

    def test_validator_builds_difficulty_review(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                stem="下列最适合作为这段文字标题的一项是：",
                options={
                    "A": "社区花园的生态意义",
                    "B": "社区花园为何成为复合型公共空间",
                    "C": "儿童自然教育的实施路径",
                    "D": "城市更新中的景观工程",
                },
                answer="B",
                analysis="正确答案是B。文段强调社区花园从景观项目转变为兼具生态、教育与社会功能的公共空间，因此B最能概括全文。",
            ),
            material_text="社区花园逐渐从单纯的景观项目，转变为兼具生态、教育与社会功能的公共空间。",
            difficulty_fit={
                "in_range": False,
                "deviations": [{"metric": "complexity", "target_min": 0.38, "target_max": 0.62, "actual": 0.22}],
            },
        )

        self.assertIsNotNone(result.difficulty_review)
        self.assertFalse(result.difficulty_review["in_range"])
        self.assertEqual(result.difficulty_review["deviation_count"], 1)
        self.assertIn("difficulty projection is outside the target profile range.", result.errors)

    def test_validator_fails_when_analysis_explicitly_points_to_different_answer(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                stem="下列最适合作为这段文字标题的一项是：",
                options={
                    "A": "社区花园的景观升级",
                    "B": "城市更新中的生态教育",
                    "C": "社区花园成为复合型公共空间",
                    "D": "公共空间激活邻里情感",
                },
                answer="C",
                analysis="答案是A。文段强调社区花园已经从单纯景观转向复合型公共空间。",
            ),
            material_text="社区花园逐渐从单纯的景观项目，转变为兼具生态、教育与社会功能的公共空间。",
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertFalse(result.passed)
        self.assertIn(
            "analysis explicitly marks option A as correct but answer is C.",
            result.errors,
        )

    def test_title_selection_special_rules_require_validator_contract(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="title_selection",
            stem="下列最适合作为这段文字标题的一项是：",
            options={
                "A": "以系统治理的方式持续推进城市更新的综合实践",
                "B": "城市更新实践",
                "C": "推进治理",
                "D": "治理实践与经验",
            },
            answer="A",
            analysis="正确答案是A，材料整体围绕城市更新与治理展开。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="title_selection",
            generated_question=generated_question,
            material_text="政府工作报告提出总体要求。会议高度评价相关部署，并强调持续推进城市更新。",
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["title_selection_title_like"]["passed"])
        self.assertFalse(result.checks["title_selection_title_like"]["required"])
        self.assertEqual(result.checks["title_selection_title_like"]["source"], "compatibility_disabled")
        self.assertFalse(result.checks["title_selection_material_fit"]["required"])
        self.assertFalse(result.checks["title_selection_option_diversity"]["required"])
        self.assertFalse(any("title_selection" in error for error in result.errors))

    def test_title_selection_contract_enables_card_specific_checks(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="title_selection",
            stem="下列最适合作为这段文字标题的一项是：",
            options={
                "A": "以系统治理的方式持续推进城市更新、全面提升治理效能的综合实践",
                "B": "城市更新实践",
                "C": "推进治理",
                "D": "治理实践与经验",
            },
            answer="A",
            analysis="正确答案是A，材料整体围绕城市更新与治理展开。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="title_selection",
            generated_question=generated_question,
            material_text="政府工作报告提出总体要求。会议高度评价相关部署，并强调持续推进城市更新。",
            validator_contract={
                "title_selection": {
                    "enforce_title_like": True,
                    "enforce_material_fit": True,
                    "enforce_option_diversity": True,
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["title_selection_title_like"]["required"])
        self.assertEqual(result.checks["title_selection_title_like"]["source"], "validator_contract")
        self.assertIn(
            "title_selection correct option reads like a long summary sentence rather than a title.",
            result.errors,
        )
        self.assertIn(
            "title_selection material is too close to a meeting-summary or report-style passage and should not be used directly.",
            result.errors,
        )

    def test_sentence_order_without_contract_does_not_invent_fixed_standard_card_rules(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下句子重新排序，最恰当的一项是：",
            original_sentences=[
                "首先提出背景。",
                "接着说明问题。",
                "然后给出做法。",
                "最后总结全文。",
            ],
            correct_order=[1, 2, 3, 4],
            options={
                "A": "①②③④",
                "B": "①③②④",
                "C": "②①③④",
                "D": "①②④③",
            },
            answer="A",
            analysis="正确顺序为①②③④，首句先提出背景，尾句完成总结。",
        )

        result = self.validator.validate(
            question_type="sentence_order",
            generated_question=generated_question,
            material_text="①首先提出背景。②接着说明问题。③然后给出做法。④最后总结全文。",
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["sentence_order_original_sentences"]["passed"])
        self.assertEqual(result.checks["sentence_order_original_sentences"]["source"], "compatibility_disabled")
        self.assertTrue(result.checks["sentence_order_correct_order"]["passed"])
        self.assertFalse(any("exactly 6 units" in error for error in result.errors))

    def test_sentence_order_contract_enforces_card_specific_constraints(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下句子重新排序，最恰当的一项是：",
            original_sentences=[
                "首先提出背景。",
                "接着说明问题。",
                "然后给出做法。",
                "最后总结全文。",
            ],
            correct_order=[1, 2, 3, 4],
            options={
                "A": "①②③④",
                "B": "①③②④",
                "C": "②①③④",
                "D": "①②④③",
            },
            answer="A",
            analysis="正确顺序为①②③④，首句先提出背景，尾句完成总结。",
        )

        result = self.validator.validate(
            question_type="sentence_order",
            generated_question=generated_question,
            material_text="①首先提出背景。②接着说明问题。③然后给出做法。④最后总结全文。",
            validator_contract={
                "sentence_order": {
                    "sortable_unit_count": 6,
                    "expected_binding_pair_count": 2,
                },
                "thresholds": {
                    "unique_opener_min_score": 0.56,
                    "closure_min_score": 0.6,
                },
                "reasoning": {
                    "required_modes": ["head_tail_roles"],
                },
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(result.checks["sentence_order_original_sentences"]["source"], "validator_contract")
        self.assertTrue(result.checks["sentence_order_unique_opener"]["required"])
        self.assertIn("sentence_order original_sentences must contain exactly 6 units.", result.errors)

    def test_sentence_fill_prefers_runtime_prompt_extras_over_source_analysis(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这句话承上启下，既回应前文，也引出后文。",
                "B": "这句话只重复前文信息。",
                "C": "这句话另起话题。",
                "D": "这句话只总结全文。",
            },
            answer="A",
            analysis="该句承上启下，既照应前文观点，也引出后文展开。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先提出背景。____。后文继续展开做法。",
            material_source={
                "prompt_extras": {
                    "blank_position": "middle",
                    "function_type": "bridge_both_sides",
                }
            },
            source_question_analysis={
                "structure_constraints": {
                    "blank_position": "opening",
                    "function_type": "carry_previous",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(
            result.checks["sentence_fill_blank_position_alignment"]["source"],
            "material_source.prompt_extras",
        )
        self.assertEqual(
            result.checks["sentence_fill_bridge_reasoning"]["source"],
            "material_source.prompt_extras",
        )
        self.assertTrue(result.checks["sentence_fill_bridge_reasoning"]["passed"])

    def test_sentence_order_contract_values_override_source_question_analysis(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下句子重新排序，最恰当的一项是：",
            original_sentences=[
                "首先提出背景。",
                "接着说明问题。",
                "然后给出做法。",
                "随后补充条件。",
                "再推进论证。",
                "最后总结全文。",
            ],
            correct_order=[1, 2, 3, 4, 5, 6],
            options={
                "A": "①②③④⑤⑥",
                "B": "①②③⑤④⑥",
                "C": "②①③④⑤⑥",
                "D": "①③②④⑤⑥",
            },
            answer="A",
            analysis="正确顺序为①②③④⑤⑥，首句先提出背景，尾句完成总结。",
        )

        result = self.validator.validate(
            question_type="sentence_order",
            generated_question=generated_question,
            material_text="①首先提出背景。②接着说明问题。③然后给出做法。④随后补充条件。⑤再推进论证。⑥最后总结全文。",
            validator_contract={
                "sentence_order": {
                    "sortable_unit_count": 6,
                    "expected_binding_pair_count": 2,
                    "expected_unique_answer_strength": 0.6,
                },
                "thresholds": {
                    "unique_opener_min_score": 0.56,
                    "closure_min_score": 0.6,
                },
                "reasoning": {
                    "required_modes": ["head_tail_roles"],
                },
            },
            source_question_analysis={
                "structure_constraints": {
                    "sortable_unit_count": 4,
                    "expected_binding_pair_count": 99,
                    "expected_unique_answer_strength": 0.99,
                    "logic_modes": ["timeline_sequence"],
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(result.checks["sentence_order_original_sentences"]["source"], "validator_contract")
        self.assertEqual(result.checks["sentence_order_original_sentences"]["expected"], 6)
        self.assertEqual(result.checks["sentence_order_binding_pairs"]["source"], "validator_contract")
        self.assertEqual(result.checks["sentence_order_binding_pairs"]["expected"], 2)
        self.assertEqual(result.checks["sentence_order_unique_answer_strength"]["source"], "validator_contract")
        self.assertEqual(result.checks["sentence_order_head_tail_reasoning"]["source"], "validator_contract")
        self.assertNotIn("sentence_order_timeline_reasoning", result.checks)

    def test_sentence_fill_validator_contract_overrides_source_analysis_without_runtime_extras(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这句话承上启下，既回应前文，也引出后文。",
                "B": "这句话只重复前文信息。",
                "C": "这句话另起话题。",
                "D": "这句话只总结全文。",
            },
            answer="A",
            analysis="该句承上启下，既照应前文观点，也引出后文展开。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先提出背景。____。后文继续展开做法。",
            validator_contract={
                "sentence_fill": {
                    "blank_position": "middle",
                    "function_type": "bridge_both_sides",
                }
            },
            source_question_analysis={
                "structure_constraints": {
                    "blank_position": "opening",
                    "function_type": "carry_previous",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(
            result.checks["sentence_fill_blank_position_alignment"]["source"],
            "validator_contract",
        )
        self.assertEqual(
            result.checks["sentence_fill_bridge_reasoning"]["source"],
            "validator_contract",
        )
        self.assertTrue(result.checks["sentence_fill_bridge_reasoning"]["passed"])

    def test_continuation_exam_style_prompt_is_family_common_not_contract_driven(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="continuation",
            stem="接下来最可能写的是：",
            options={
                "A": "继续解释原因",
                "B": "完全跳到无关话题",
                "C": "只重复上一句",
                "D": "直接改写标题",
            },
            answer="A",
            analysis="正确答案是A，因为下文最自然会顺着尾句的新落点继续展开。",
        )

        result = self.validator.validate(
            question_type="continuation",
            generated_question=generated_question,
            material_text="文段最后一句提出了新的问题线索。",
            validator_contract={},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["continuation_exam_style_prompt"]["passed"])
        self.assertNotIn("validator_contract", str(result.checks["continuation_exam_style_prompt"]))
