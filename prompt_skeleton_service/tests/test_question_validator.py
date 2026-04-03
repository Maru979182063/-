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
                analysis="正确答案为B。文段强调社区花园从景观项目转变为兼具生态、教育与社会功能的公共空间，因此B最能概括全文。",
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
        self.assertTrue(any("difficulty projection is outside" in warning for warning in result.warnings))

    def test_validator_fails_when_analysis_explicitly_points_to_different_answer(self) -> None:
        result = self.validator.validate(
            question_type="main_idea",
            generated_question=GeneratedQuestion(
                question_type="main_idea",
                stem="下列最适合作为这段文字标题的一项是（ ）。",
                options={
                    "A": "社区花园的景观升级",
                    "B": "城市更新中的生态教育",
                    "C": "社区花园成为复合型公共空间",
                    "D": "公共空间激活邻里情感",
                },
                answer="C",
                analysis="A（正确）。文段强调社区花园已经从单纯景观转向复合型公共空间。",
            ),
            material_text="社区花园逐渐从单纯的景观项目，转变为兼具生态、教育与社会功能的公共空间。",
            difficulty_fit={"in_range": True, "deviations": []},
        )

        self.assertFalse(result.passed)
        self.assertIn(
            "analysis explicitly marks option A as correct but answer is C.",
            result.errors,
        )
