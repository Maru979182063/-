from __future__ import annotations

from unittest import TestCase

from app.services.question_generation import QuestionGenerationService


class QuestionGenerationUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = QuestionGenerationService.__new__(QuestionGenerationService)

    def test_remap_option_references_updates_explicit_correct_markers(self) -> None:
        analysis = "A（正确），B项偏题，因此正确答案是A，故选A。"
        mapping = {"A": "C", "B": "A", "C": "D", "D": "B"}

        remapped = self.service._remap_option_references(analysis, mapping)

        self.assertIn("C（正确）", remapped)
        self.assertIn("正确答案是C", remapped)
        self.assertIn("故选C", remapped)
        self.assertNotIn("A（正确）", remapped)
        self.assertNotIn("正确答案是A", remapped)
