from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import ValidationError

from app.core.exceptions import DomainError
from app.schemas.question import QuestionReviewActionRequest
from app.services.question_repository import QuestionRepository
from app.services.question_review import QuestionReviewService


class _NoOpGenerationService:
    def revise_minor_edit(self, current_item, instruction):
        return deepcopy(current_item)

    def revise_question_modify(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)

    def revise_text_modify(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)

    def apply_manual_edit(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)


class _TruthDriftGenerationService(_NoOpGenerationService):
    def revise_minor_edit(self, current_item, instruction):
        revised = deepcopy(current_item)
        revised["generated_question"] = {
            **(current_item.get("generated_question") or {}),
            "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
            "answer": "B",
            "analysis": "新的解析",
        }
        revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
        revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
        return revised


class _MaterialDriftGenerationService(_NoOpGenerationService):
    def revise_minor_edit(self, current_item, instruction):
        revised = deepcopy(current_item)
        revised["material_text"] = "新的材料"
        revised["material_selection"] = {
            **(current_item.get("material_selection") or {}),
            "material_id": "mat-override",
            "text": "新的材料",
        }
        revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
        revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
        return revised


class QuestionReviewUnitTest(TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.repository = QuestionRepository(Path(self.tempdir.name) / "question_review.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _save_base_item(self, *, item_id: str = "item-1") -> dict:
        item = {
            "item_id": item_id,
            "batch_id": "batch-1",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "pattern_id": "pattern-1",
            "difficulty_target": "medium",
            "generated_question": {
                "stem": "下列最适合作为标题的一项是：",
                "options": {"A": "标题A", "B": "标题B", "C": "标题C", "D": "标题D"},
                "answer": "A",
                "analysis": "正确答案是A。",
            },
            "material_selection": {
                "material_id": "mat-1",
                "article_id": "art-1",
                "text": "材料原文",
                "source": {"site": "demo"},
            },
            "material_text": "材料原文",
            "material_source": {"site": "demo"},
            "request_snapshot": {
                "type_slots": {"slot_a": "value_a"},
                "extra_constraints": {"keep": True},
            },
            "statuses": {
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "pending_review",
            "revision_count": 0,
            "notes": [],
        }
        self.repository.save_item(item)
        return item

    def test_review_action_schema_rejects_unknown_top_level_field(self) -> None:
        with self.assertRaises(ValidationError):
            QuestionReviewActionRequest.model_validate(
                {
                    "action": "question_modify",
                    "control_overrides": {"difficulty_target": "hard"},
                    "unexpected_field": True,
                }
            )

    def test_question_modify_rejects_unknown_nested_extra_constraints(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="question_modify",
                    control_overrides={"extra_constraints": {"new_semantic_switch": True}},
                ),
            )

    def test_question_modify_rejects_unknown_nested_type_slots(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="question_modify",
                    control_overrides={"type_slots": {"new_slot": "value"}},
                ),
            )

    def test_manual_edit_rejects_unknown_option_patch_key(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="manual_edit",
                    control_overrides={
                        "manual_patch": {
                            "options": {"A": "甲", "B": "乙", "E": "越界"},
                            "answer": "A",
                        }
                    },
                ),
            )

    def test_minor_edit_truth_drift_is_rejected(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _TruthDriftGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(action="minor_edit", instruction="只改表述"),
            )

        self.assertEqual(self.repository.list_review_actions(item_id="item-1", limit=1), [])

    def test_minor_edit_material_drift_is_rejected(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _MaterialDriftGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(action="minor_edit", instruction="只改表述"),
            )

        self.assertEqual(self.repository.list_review_actions(item_id="item-1", limit=1), [])
