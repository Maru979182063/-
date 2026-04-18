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

    def test_center_understanding_soft_difficulty_miss_becomes_warning(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                business_subtype="center_understanding",
                stem="这段文字旨在说明（ ）。",
                options={
                    "A": "科技创新和产业创新深度融合是提升竞争优势的战略选择",
                    "B": "国际竞争加剧要求所有技术都尽快完成产业化",
                    "C": "产业升级只能依靠最新科技成果直接落地",
                    "D": "新质生产力的发展主要取决于扩大产业规模",
                },
                answer="A",
                analysis="文段先谈国际竞争，再谈产业升级，最后落到科技创新和产业创新深度融合这一总体判断，故A项最能概括全文。",
            ),
            material_text="国际科技和产业竞争加剧，要求科技创新前瞻谋划产业化。与此同时，产业转型升级也必须主动吸纳最新科技进展。可见，科技创新和产业创新深度融合已成为提升竞争优势、实现并跑乃至领跑的战略选择。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "phenomenon_analysis",
                    "main_axis_source": "global_abstraction",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "phenomenon_analysis",
                    "main_axis_source": "global_abstraction",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={
                "in_range": False,
                "deviations": [{"metric": "complexity", "target_min": 0.38, "target_max": 0.62, "actual": 0.29}],
            },
        )

        self.assertNotIn("difficulty projection is outside the target profile range.", result.errors)
        self.assertIn("center_understanding difficulty projection is slightly below the target profile range.", result.warnings)

    def test_center_understanding_soft_distractor_similarity_miss_becomes_warning(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                business_subtype="center_understanding",
                stem="这段文字旨在说明（ ）。",
                options={
                    "A": "科技创新和产业创新深度融合是提升竞争优势的重要抓手",
                    "B": "产业升级必须同步吸收科技创新成果才能形成持续竞争力",
                    "C": "科技成果转化速度加快对产业布局提出更高前瞻要求",
                    "D": "科技创新和产业创新深度融合已成为培育新质生产力的战略选择",
                },
                answer="D",
                analysis="文段先从国际竞争和技术周期缩短切入，再说明产业体系必须吸纳科技成果，最后总结科技创新和产业创新深度融合已成为培育新质生产力的战略选择，D项概括最全面。",
            ),
            material_text="在国际科技和产业竞争中，技术从发现到应用、产业化的周期不断缩短。科技创新必须前瞻性对接产业化方向，产业体系也要主动吸纳最新科技成果。可见，科技创新和产业创新深度融合已经成为培育新质生产力的战略选择。",
            difficulty_fit={
                "in_range": False,
                "deviations": [
                    {
                        "metric": "distractor_similarity",
                        "target_min": 0.45,
                        "target_max": 0.72,
                        "actual": 0.78,
                    }
                ],
            },
        )

        self.assertNotIn("difficulty projection is outside the target profile range.", result.errors)
        self.assertIn(
            "center_understanding difficulty projection is slightly below the target profile range.",
            result.warnings,
        )

    def test_sentence_order_soft_difficulty_miss_requires_sound_structure(self) -> None:
        checks = {
            "sentence_order_unique_opener": {"passed": True, "status": "active"},
            "sentence_order_binding_pairs": {"passed": True, "status": "active"},
            "sentence_order_closure": {"passed": True, "status": "active"},
            "sentence_order_exchange_risk": {"passed": True, "status": "active"},
            "sentence_order_multi_path_risk": {"passed": True, "status": "active"},
            "sentence_order_function_overlap": {"passed": True, "status": "active"},
            "sentence_order_unique_answer_strength": {"passed": True, "status": "active"},
            "sentence_order_single_truth_option": {"passed": True},
            "sentence_order_answer_binding": {"passed": True},
            "sentence_order_analysis_binding": {"passed": True},
            "sentence_order_head_enforcement": {"passed": True},
            "sentence_order_tail_enforcement": {"passed": True},
            "sentence_order_binding_enforcement": {"passed": True},
        }
        difficulty_review = {
            "in_range": False,
            "deviation_count": 1,
            "deviations": [{"metric": "complexity", "target_min": 0.42, "target_max": 0.62, "actual": 0.36}],
        }

        self.assertTrue(
            self.validator._is_sentence_order_soft_difficulty_miss(
                difficulty_review=difficulty_review,
                checks=checks,
            )
        )
        checks["sentence_order_binding_pairs"]["passed"] = False
        self.assertFalse(
            self.validator._is_sentence_order_soft_difficulty_miss(
                difficulty_review=difficulty_review,
                checks=checks,
            )
        )

    def test_sentence_order_natural_openers_get_opening_credit(self) -> None:
        ifengshuo = self.validator._sentence_order_unit_opener_score("如果说制度与技术的完善为残疾学生搭建了受教育的平台，那么特教教师则是连接最后一公里的关键桥梁。", index=0)
        quoted = self.validator._sentence_order_unit_opener_score("在《辞源》里，“寿岳”的注释就是南岳。", index=0)
        concessive = self.validator._sentence_order_unit_opener_score("虽然数字变了又变，但有一点从来没变过：祝融峰是南岳主峰和最高峰。", index=0)
        dependent = self.validator._sentence_order_unit_opener_score("这也意味着后续讨论必须回到制度执行层面。", index=0)

        self.assertGreater(ifengshuo, dependent)
        self.assertGreater(quoted, dependent)
        self.assertGreater(concessive, dependent)
        self.assertEqual(
            self.validator._sentence_order_unit_role("如果说制度与技术的完善为残疾学生搭建了受教育的平台，那么特教教师则是连接最后一公里的关键桥梁。"),
            "opening_anchor",
        )
        self.assertEqual(
            self.validator._sentence_order_unit_role("虽然数字变了又变，但有一点从来没变过：祝融峰是南岳主峰和最高峰。"),
            "opening_anchor",
        )

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

        self.assertIsNone(result.checks["title_selection_title_like"]["passed"])
        self.assertFalse(result.checks["title_selection_title_like"]["required"])
        self.assertEqual(result.checks["title_selection_title_like"]["source"], "compatibility_disabled")
        self.assertEqual(result.checks["title_selection_title_like"]["status"], "skipped_missing_contract")
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
        self.assertIsNone(result.checks["sentence_order_unique_opener"]["passed"])
        self.assertEqual(result.checks["sentence_order_unique_opener"]["status"], "skipped_missing_contract")
        self.assertIsNone(result.checks["sentence_order_head_tail_reasoning"]["passed"])
        self.assertTrue(result.checks["sentence_order_correct_order"]["passed"])
        self.assertFalse(any("exactly 6 units" in error for error in result.errors))

    def test_sentence_order_source_question_analysis_does_not_become_default_truth(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下句子重新排序，最恰当的一项是：",
            original_sentences=["甲。", "乙。", "丙。", "丁。"],
            correct_order=[1, 2, 3, 4],
            options={
                "A": "①②③④",
                "B": "①③②④",
                "C": "②①③④",
                "D": "①②④③",
            },
            answer="A",
            analysis="正确顺序是①②③④。",
        )

        result = self.validator.validate(
            question_type="sentence_order",
            generated_question=generated_question,
            material_text="①甲。②乙。③丙。④丁。",
            source_question_analysis={
                "structure_constraints": {
                    "sortable_unit_count": 6,
                    "expected_binding_pair_count": 99,
                    "expected_unique_answer_strength": 0.99,
                    "logic_modes": ["timeline_sequence"],
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(result.checks["sentence_order_original_sentences"]["source"], "compatibility_disabled")
        self.assertNotIn("sentence_order_binding_pairs", {k: v for k, v in result.checks.items() if v.get("source") == "compatibility_source_question_analysis"})
        self.assertNotIn("sentence_order_timeline_reasoning", result.checks)

    def test_sentence_order_runtime_roles_override_source_question_analysis_roles(self) -> None:
        roles = self.validator._extract_sentence_order_roles(
            {
                "material_source": {
                    "prompt_extras": {
                        "sentence_roles": {"1": "thesis", "6": "conclusion"},
                    }
                },
                "source_question_analysis": {
                    "structure_constraints": {
                        "sentence_roles": {"3": "conclusion"},
                    }
                },
            }
        )

        self.assertEqual(roles, {1: "thesis", 6: "conclusion"})

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
        self.assertIn("sentence_count_mismatch", result.errors)

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
        self.assertEqual(
            result.checks["sentence_fill_bridge_reasoning"]["function_type"],
            "bridge",
        )
        self.assertTrue(result.checks["sentence_fill_bridge_reasoning"]["passed"])

    def test_sentence_fill_standard_prompt_is_recognized(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这句话承接前文并自然引出后文。",
                "B": "这句话只重复后文。",
                "C": "这句话改写了无关话题。",
                "D": "这句话直接给出宏观结论。",
            },
            answer="A",
            analysis="A项既承接前文，也引出后文，适合填入横线处。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先提出背景。____。后文继续展开做法。",
            material_source={
                "prompt_extras": {
                    "fill_ready_material": "前文先提出背景。____。后文继续展开做法。",
                    "blank_position": "middle",
                    "function_type": "bridge",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["sentence_fill_exam_style_prompt"]["passed"])
        self.assertNotIn(
            "sentence_fill stem does not look like a standard fill-in-the-blank prompt.",
            result.warnings,
        )

    def test_sentence_fill_requires_original_removed_sentence_when_flag_enabled(self) -> None:
        anchor_sentence = "因此，必须完善协同机制，持续提升治理效能。"
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="填入画横线部分最恰当的一句是（    ）。",
            options={
                "A": anchor_sentence,
                "B": "因此，应持续优化治理体系，推进高质量发展。",
                "C": "可见，治理效率提升主要依赖制度改革。",
                "D": "总之，前文已经完整解释了问题成因。",
            },
            answer="A",
            analysis="A项与前后文衔接自然，且与被挖原句一致，最适合填入。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文分析现实压力。____。后文展开执行路径。",
            material_source={
                "prompt_extras": {
                    "fill_ready_material": "前文分析现实压力。____。后文展开执行路径。",
                    "answer_anchor_text": anchor_sentence,
                    "require_original_answer_sentence": True,
                    "blank_position": "middle",
                    "function_type": "bridge",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(result.checks["sentence_fill_anchor_grounding"]["passed"])
        self.assertTrue(result.checks["sentence_fill_anchor_grounding"]["exact_anchor_match"])
        self.assertTrue(result.checks["sentence_fill_material_question_consistency"]["passed"])
        self.assertNotIn("sentence_fill correct option must be the original removed sentence.", result.errors)

    def test_sentence_fill_rejects_paraphrase_when_original_answer_is_required(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="填入画横线部分最恰当的一句是（    ）。",
            options={
                "A": "因此，应持续优化治理体系，推进高质量发展。",
                "B": "因此，必须完善协同机制，持续提升治理效能。",
                "C": "可见，治理效率提升主要依赖制度改革。",
                "D": "总之，前文已经完整解释了问题成因。",
            },
            answer="A",
            analysis="A项与前后文衔接较好，能够承接语义。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文分析现实压力。____。后文展开执行路径。",
            material_source={
                "prompt_extras": {
                    "fill_ready_material": "前文分析现实压力。____。后文展开执行路径。",
                    "answer_anchor_text": "因此，必须完善协同机制，持续提升治理效能。",
                    "require_original_answer_sentence": True,
                    "blank_position": "middle",
                    "function_type": "bridge",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertFalse(result.checks["sentence_fill_anchor_grounding"]["passed"])
        self.assertFalse(result.checks["sentence_fill_anchor_grounding"]["exact_anchor_match"])
        self.assertIn("sentence_fill correct option must be the original removed sentence.", result.errors)
        self.assertIn("sentence_fill_material_question_consistency_fail", result.errors)

    def test_sentence_fill_prefers_local_validator_material_for_blank_position(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "因此，必须从制度协同和资源配置两端同时发力。",
                "B": "这一问题主要来自历史条件差异。",
                "C": "例如，可以再补充一个案例。",
                "D": "总之，前文已经说明了全部结论。",
            },
            answer="A",
            analysis="A项放在文末能够承接前文分析，并自然落到解决路径上。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文分析现实压力。中间补充政策背景。____",
            material_source={
                "prompt_extras": {
                    "fill_ready_material": "第一段先交代现实压力。第二句补充政策背景。第三句分析执行难点。____",
                    "fill_ready_local_material": "第三句分析执行难点。____",
                    "blank_position": "ending",
                    "function_type": "countermeasure",
                }
            },
            validator_contract={"sentence_fill": {"blank_position": "ending", "function_type": "countermeasure"}},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(
            result.checks["sentence_fill_runtime_material_form"]["validation_source"],
            "material_source.prompt_extras.fill_ready_local_material",
        )
        self.assertEqual(
            result.checks["sentence_fill_blank_position_alignment"]["generated_blank_position"],
            "ending",
        )
        self.assertNotIn("position_function_mismatch", result.errors)

    def test_sentence_fill_soft_reasoning_depth_miss_becomes_warning(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这句话承接前文并自然引出后文。",
                "B": "这句话只重复后文。",
                "C": "这句话改写了无关话题。",
                "D": "这句话直接给出宏观结论。",
            },
            answer="A",
            analysis="A项既承接前文，也引出后文，适合填入横线处。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先提出背景。____。后文继续展开做法。",
            material_source={
                "prompt_extras": {
                    "fill_ready_material": "前文先提出背景。____。后文继续展开做法。",
                    "blank_position": "middle",
                    "function_type": "bridge",
                }
            },
            difficulty_fit={
                "in_range": False,
                "deviations": [
                    {
                        "metric": "reasoning_depth",
                        "target_min": 0.38,
                        "target_max": 0.64,
                        "actual": 0.30,
                    }
                ],
            },
        )

        self.assertNotIn("difficulty projection is outside the target profile range.", result.errors)
        self.assertIn(
            "sentence_fill difficulty projection is slightly below the target profile range.",
            result.warnings,
        )

    def test_main_idea_structure_mode_prefers_explicit_argument_structure_over_business_card(self) -> None:
        mode = self.validator._derive_main_idea_structure_mode(
            argument_structure="phenomenon_analysis",
            main_axis_source="global_abstraction",
            legacy_structure_type="progressive",
            business_card_id="title_material.turning_relation_focus",
        )

        self.assertEqual(mode, "cause_effect")

    def test_main_idea_structure_mode_prefers_explicit_argument_structure_over_legacy_turning(self) -> None:
        mode = self.validator._derive_main_idea_structure_mode(
            argument_structure="phenomenon_analysis",
            main_axis_source="global_abstraction",
            legacy_structure_type="turning",
            business_card_id="title_material.turning_relation_focus",
        )

        self.assertEqual(mode, "cause_effect")

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

    def test_sentence_order_prefers_generated_original_sentences_for_runtime_material(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下6个部分重新排列，语序正确的一项是：",
            original_sentences=[
                "在此基础上，才能进一步分析成因。",
                "要想看清问题，先要回到事实本身。",
                "只有把事实摆清楚，讨论才有可靠起点。",
                "找到成因之后，还要比较不同路径的得失。",
                "因此，结论也就更容易成立。",
                "经过这样的层层推进，思路才会逐渐完整。",
            ],
            correct_order=[2, 3, 1, 4, 6, 5],
            options={
                "A": "③①②④⑥⑤",
                "B": "①②③④⑤⑥",
                "C": "②①③④⑤⑥",
                "D": "①③②④⑤⑥",
            },
            answer="A",
            analysis="先看起句是否能作为论述起点，再看尾句是否形成结论收束。",
        )

        result = self.validator.validate(
            question_type="sentence_order",
            generated_question=generated_question,
            material_text="原始材料抽取失败时的脏文本占位",
            material_source={
                "prompt_extras": {
                    "sortable_units": [
                        "要想看清问题，先要回到事实本身。",
                        "只有把事实摆清楚，讨论才有可靠起点。",
                        "在此基础上，才能进一步分析成因。",
                        "找到成因之后，还要比较不同路径的得失。",
                        "经过这样的层层推进，思路才会逐渐完整。",
                        "因此，结论也就更容易成立。",
                    ],
                    "sortable_material_text": (
                        "① 要想看清问题，先要回到事实本身。\n"
                        "② 只有把事实摆清楚，讨论才有可靠起点。\n"
                        "③ 在此基础上，才能进一步分析成因。\n"
                        "④ 找到成因之后，还要比较不同路径的得失。\n"
                        "⑤ 经过这样的层层推进，思路才会逐渐完整。\n"
                        "⑥ 因此，结论也就更容易成立。"
                    ),
                }
            },
            validator_contract={
                "sentence_order": {
                    "sortable_unit_count": 6,
                    "expected_binding_pair_count": 1,
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertEqual(
            result.checks["sentence_order_material_unit_count"]["source"],
            "generated_question.original_sentences",
        )
        self.assertEqual(result.checks["sentence_order_material_unit_count"]["count"], 6)

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
                    "function_type": "bridge",
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
        self.assertEqual(
            result.checks["sentence_fill_bridge_reasoning"]["function_type"],
            "bridge",
        )
        self.assertTrue(result.checks["sentence_fill_bridge_reasoning"]["passed"])

    def test_sentence_fill_source_question_analysis_does_not_become_default_truth(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这句话承上启下。",
                "B": "这句话只重复前文。",
                "C": "这句话另起话题。",
                "D": "这句话只总结全文。",
            },
            answer="A",
            analysis="该句承上启下。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先提出背景。___。后文继续展开做法。",
            source_question_analysis={
                "structure_constraints": {
                    "blank_position": "opening",
                    "function_type": "carry_previous",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertNotIn("sentence_fill_blank_position_alignment", result.checks)
        self.assertNotIn("sentence_fill_bridge_reasoning", result.checks)

    def test_sentence_fill_runtime_function_drift_fails_for_opening_topic_intro(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "人工智能正重新塑造城市治理方式。",
                "B": "总之，这一问题值得关注。",
                "C": "这一理论能够解释上述现象。",
                "D": "因此，需要尽快完善制度。",
            },
            answer="A",
            analysis="该句用于引入主题，正确答案为A。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="____。后文将围绕人工智能在交通、医疗和教育中的应用展开讨论。",
            material_source={"prompt_extras": {"blank_position": "opening", "function_type": "summary"}},
            validator_contract={"sentence_fill": {"blank_position": "opening", "function_type": "topic_intro"}},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("position_function_mismatch", result.errors)

    def test_sentence_fill_runtime_function_drift_fails_for_middle_lead_next(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "接下来，需要从制度协同、资金投入和人才培养三个方面继续展开。",
                "B": "总之，前文已经说清了问题。",
                "C": "这一理论能够概括全部内容。",
                "D": "比如，可以举一个国外案例。",
            },
            answer="A",
            analysis="该句主要引出后文展开，正确答案为A。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文先交代现实背景。____。后文依次展开制度协同、资金投入和人才培养三项措施。",
            material_source={"prompt_extras": {"blank_position": "middle", "function_type": "bridge"}},
            validator_contract={"sentence_fill": {"blank_position": "middle", "function_type": "lead_next"}},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("position_function_mismatch", result.errors)

    def test_sentence_fill_runtime_function_drift_fails_for_ending_countermeasure(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "总之，这一问题值得持续关注。",
                "B": "因此，应通过完善评估机制和资源配置来解决这一问题。",
                "C": "这一理论也能说明前文现象。",
                "D": "例如，还可以引用其他案例。",
            },
            answer="A",
            analysis="句子位于结尾，正确答案为A。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文分析了公共服务供给不足的原因。____",
            material_source={"prompt_extras": {"blank_position": "ending", "function_type": "conclusion"}},
            validator_contract={"sentence_fill": {"blank_position": "ending", "function_type": "countermeasure"}},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("position_function_mismatch", result.errors)

    def test_sentence_fill_reference_anchor_missing_when_theory_has_no_antecedent(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这一理论不仅解释了时间差异，也解释了地区差异。",
                "B": "这种现象值得进一步关注。",
                "C": "总之，上述现象并不复杂。",
                "D": "因此，需要继续研究。",
            },
            answer="A",
            analysis="该句需要前文存在明确理论对象，正确答案为A。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文只是列举了几个盐湖采集的现象。____。后文继续讨论内陆与沿海的差异。",
            validator_contract={
                "sentence_fill": {
                    "blank_position": "middle",
                    "function_type": "reference_summary",
                    "reference_anchor": "required",
                    "bidirectional_check": {"previous_valid": True, "next_valid": True},
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("reference_anchor_missing", result.errors)

    def test_sentence_fill_bridge_fails_when_only_one_side_holds(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "这一变化延续了前文关于成本上升的判断。",
                "B": "接下来，将从三项制度安排继续展开。",
                "C": "这句话承上启下，既回应前文，也引出后文。",
                "D": "总之，问题已经说明白了。",
            },
            answer="A",
            analysis="该句只能承前，不能启后。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文指出成本持续上升、企业压力加大。____。后文转向讨论绿色转型的长期价值。",
            validator_contract={
                "sentence_fill": {
                    "blank_position": "middle",
                    "function_type": "bridge",
                    "bidirectional_check": {"previous_valid": True, "next_valid": True},
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("bidirectional_failure", result.errors)

    def test_sentence_fill_countermeasure_requires_specific_action(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="下列句子填入文中横线处，最恰当的一项是：",
            options={
                "A": "总之，这一问题值得重视。",
                "B": "因此，应通过完善培训机制和资金支持来解决这一问题。",
                "C": "这一理论还能解释前文。",
                "D": "比如，可以再举一个例子。",
            },
            answer="A",
            analysis="该句位于结尾。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文分析了基层治理中的执行难题。____",
            validator_contract={"sentence_fill": {"blank_position": "ending", "function_type": "countermeasure"}},
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(
            "position_function_mismatch" in result.errors or "function_scope_mismatch" in result.errors
        )

    def test_sentence_fill_real_reference_summary_sample_rejects_missing_anchor_support(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="以下这段文字，最适合放在文中哪个位置？",
            options={
                "A": "这一理论不仅能够解释人类制盐出现的大致时间范围，也能解释为何内陆的盐业开发要较之沿海地区为早的现象。",
                "B": "这种现象主要来自自然盐资源的分布差异。",
                "C": "总之，早期人类已经掌握了成熟制盐技术。",
                "D": "因此，考古学家很难获得任何证据。",
            },
            answer="A",
            analysis="该句需要前文先提出理论，并承接后文地域差异讨论。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="前文只列举了天然盐资源的获取方式和若干考古现象。____。后文继续讨论沿海地区与内陆地区的制盐差异。",
            validator_contract={
                "sentence_fill": {
                    "blank_position": "middle",
                    "function_type": "reference_summary",
                    "reference_anchor": "required",
                    "bidirectional_check": {"previous_valid": True, "next_valid": True},
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("reference_anchor_missing", result.errors)

    def test_sentence_fill_real_bridge_like_sample_rejects_one_sided_middle_option(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="sentence_fill",
            stem="填入第三段划横线处最恰当的一项是（    ）。",
            options={
                "A": "其中，心理因素是女性作出生育决策的重要因素。",
                "B": "生育带来的巨大生理变化是影响生育的主要原因。",
                "C": "因此，职业女性在回归职场后会出现工作懈怠。",
                "D": "职业女性作出生育决策是基于综合性、深层次的考虑。",
            },
            answer="C",
            analysis="该句只是顺着前文下结论，没有有效引出后文的生理心理变化展开。",
        )

        result = self.validator.validate(
            question_type="sentence_fill",
            generated_question=generated_question,
            material_text="女性在作出生育决策时，考虑的因素众多。____。怀孕对女性来说，是一次巨大的生理变化和心理应激过程，当应激反应导致心理变化发生异常而不能自行调整和应对时，则出现心理上的焦虑、抑郁、恐惧等状态。",
            validator_contract={
                "sentence_fill": {
                    "blank_position": "middle",
                    "function_type": "lead_next",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertTrue(
            "position_function_mismatch" in result.errors or "function_scope_mismatch" in result.errors
        )

    def test_center_understanding_turning_rejects_pre_transition_background(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "短视频会削弱阅读耐心",
                "B": "短视频正在倒逼知识传播方式升级",
                "C": "阅读习惯变化带来了知识焦虑",
                "D": "新媒体传播仍存在很多争议",
            },
            answer="A",
            analysis="文段主旨应放在转折之后，而不是停留在转折前的旧判断。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="很多人认为短视频会削弱阅读耐心。然而，更值得关注的是，短视频正在倒逼知识传播方式升级，使内容表达更高效。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "sub_total",
                    "main_axis_source": "transition_after",
                    "abstraction_level": "medium",
                    "distractor_types": ["detail_as_main", "focus_shift"],
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "sub_total",
                    "main_axis_source": "transition_after",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("main_axis_mismatch", result.errors)

    def test_center_understanding_cause_effect_rejects_single_cause_branch(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "硬化地表增多是热岛效应加剧的重要原因",
                "B": "缓解热岛效应需要从地表材料和城市通风系统两端协同治理",
                "C": "通风廊道被挤占导致城市热岛效应加剧",
                "D": "城市建设会带来热环境问题",
            },
            answer="A",
            analysis="正确项如果只抓某一个原因枝节，就不能统摄整段的因果归结。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="城市热岛效应加剧，一方面与硬化地表增多有关，另一方面与通风廊道被挤占有关。因此，缓解热岛效应需要从地表材料和城市通风系统两端协同治理。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "phenomenon_analysis",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "phenomenon_analysis",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("local_point_as_main_axis", result.errors)

    def test_center_understanding_example_to_conclusion_rejects_example_as_main_idea(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "植物在遭遇虫害时会释放化学信号",
                "B": "植物会以各自方式完成对环境的适应",
                "C": "植物之间存在复杂的竞争关系",
                "D": "植物行为研究仍有很多未解之谜",
            },
            answer="A",
            analysis="如果把前面的例子直接抬成主旨，就会错过后面抽出来的总判断。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="一些植物在遭遇虫害时会释放化学信号，另一些植物则会通过改变生长节律减少损失。这些现象看似神奇，但更值得看到的是，植物会以各自方式完成对环境的适应。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "example_conclusion",
                    "main_axis_source": "example_elevation",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "example_conclusion",
                    "main_axis_source": "example_elevation",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("example_promoted_to_main_idea", result.errors)

    def test_center_understanding_final_summary_rejects_middle_local_point(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "数字化让观众获得更多互动体验",
                "B": "数字化正在重塑博物馆的整体运行方式",
                "C": "数字化会让展陈设计更复杂",
                "D": "博物馆教育活动越来越依赖技术",
            },
            answer="A",
            analysis="若只抓中段的一个并列点，就不足以覆盖末段总结层。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="博物馆数字化让藏品展示更丰富，也让观众获得更多互动体验。与此同时，它也倒逼展陈设计、教育活动和公共服务同步升级。总的来看，数字化正在重塑博物馆的整体运行方式。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "total_sub",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "total_sub",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("local_point_as_main_axis", result.errors)

    def test_center_understanding_rejects_over_abstract_option(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "城市发展具有重要意义",
                "B": "社区花园正转变为兼具生态、教育与社会功能的公共空间",
                "C": "景观项目需要更多财政投入",
                "D": "自然教育越来越受重视",
            },
            answer="A",
            analysis="过于空泛的拔高不能算全文主旨。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="社区花园逐渐从单纯的景观项目，转变为兼具生态、教育与社会功能的公共空间。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "total_sub",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "total_sub",
                    "main_axis_source": "final_summary",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("abstraction_level_mismatch", result.errors)

    def test_center_understanding_runtime_contract_mismatch_fails_explicitly(self) -> None:
        generated_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            stem="下列最能概括这段文字中心的一项是：",
            options={
                "A": "短视频正在倒逼知识传播方式升级",
                "B": "短视频会削弱阅读耐心",
                "C": "新媒体正在改变文化产业结构",
                "D": "阅读习惯变化值得研究",
            },
            answer="A",
            analysis="这里重点看运行约束与 contract 是否一致。",
        )

        result = self.validator.validate(
            question_type="main_idea",
            business_subtype="center_understanding",
            generated_question=generated_question,
            material_text="很多人认为短视频会削弱阅读耐心。然而，更值得关注的是，短视频正在倒逼知识传播方式升级，使内容表达更高效。",
            material_source={
                "prompt_extras": {
                    "argument_structure": "sub_total",
                    "main_axis_source": "transition_after",
                    "abstraction_level": "medium",
                }
            },
            validator_contract={
                "center_understanding": {
                    "argument_structure": "example_conclusion",
                    "main_axis_source": "example_elevation",
                    "abstraction_level": "medium",
                }
            },
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertIn("argument_structure_mismatch", result.errors)

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
