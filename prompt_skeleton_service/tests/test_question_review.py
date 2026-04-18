from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import ValidationError

from app.core.exceptions import DomainError
from app.schemas.question import QuestionReviewActionRequest
from app.services.patch_scope_registry import get_patch_scope
from app.services.question_repository import QuestionRepository
from app.services.question_review import QuestionReviewService
from app.services.review_query_service import ReviewQueryService


class _NoOpGenerationService:
    def revise_minor_edit(self, current_item, instruction):
        return deepcopy(current_item)

    def revise_question_modify(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)

    def revise_text_modify(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)

    def apply_manual_edit(self, current_item, instruction, control_overrides):
        return deepcopy(current_item)

    def apply_distractor_patch(
        self,
        current_item,
        *,
        target_option,
        distractor_strategy="",
        distractor_intensity="",
        option_text,
        analysis,
        operator=None,
    ):
        revised = deepcopy(current_item)
        question = deepcopy(current_item.get("generated_question") or {})
        options = deepcopy(question.get("options") or {})
        options[target_option] = option_text
        question["options"] = options
        question["analysis"] = analysis
        revised["generated_question"] = question
        revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
        revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
        revised["current_status"] = "pending_review"
        revised["statuses"]["generation_status"] = "success"
        revised["statuses"]["validation_status"] = "passed"
        revised["statuses"]["review_status"] = "waiting_review"
        revised["validation_result"] = {
            "passed": True,
            "validation_status": "passed",
            "checks": {
                "analysis_answer_consistency": {"passed": True},
                "analysis_mentions_correct_option_text": {"passed": True},
            },
        }
        revised["evaluation_result"] = {"overall_score": 80}
        return revised


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


class _DistractorPatchAnswerDriftGenerationService(_NoOpGenerationService):
    def apply_distractor_patch(
        self,
        current_item,
        *,
        target_option,
        distractor_strategy="",
        distractor_intensity="",
        option_text,
        analysis,
        operator=None,
    ):
        revised = super().apply_distractor_patch(
            current_item,
            target_option=target_option,
            distractor_strategy=distractor_strategy,
            distractor_intensity=distractor_intensity,
            option_text=option_text,
            analysis=analysis,
            operator=operator,
        )
        revised["generated_question"]["answer"] = "B"
        return revised


class _DistractorPatchScopeDriftGenerationService(_NoOpGenerationService):
    def apply_distractor_patch(
        self,
        current_item,
        *,
        target_option,
        distractor_strategy="",
        distractor_intensity="",
        option_text,
        analysis,
        operator=None,
    ):
        revised = super().apply_distractor_patch(
            current_item,
            target_option=target_option,
            distractor_strategy=distractor_strategy,
            distractor_intensity=distractor_intensity,
            option_text=option_text,
            analysis=analysis,
            operator=operator,
        )
        revised["generated_question"]["options"]["C"] = "越界改动"
        return revised


class _DistractorPatchStrategyGenerationService(_NoOpGenerationService):
    def apply_distractor_patch(
        self,
        current_item,
        *,
        target_option,
        distractor_strategy="",
        distractor_intensity="",
        option_text,
        analysis,
        operator=None,
    ):
        next_option = option_text or f"{target_option}项改为{distractor_strategy or '新干扰方式'}"
        next_analysis = analysis or f"A项正确；{target_option}项按{distractor_intensity or '中等'}强度调整为新的错误项。"
        return super().apply_distractor_patch(
            current_item,
            target_option=target_option,
            distractor_strategy=distractor_strategy,
            distractor_intensity=distractor_intensity,
            option_text=next_option,
            analysis=next_analysis,
            operator=operator,
        )


class QuestionReviewUnitTest(TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.repository = QuestionRepository(Path(self.tempdir.name) / "question_review.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _save_base_item(
        self,
        *,
        item_id: str = "item-1",
        type_slots: dict[str, object] | None = None,
    ) -> dict:
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
                "type_slots": dict(type_slots or {"slot_a": "value_a"}),
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

    def test_question_modify_accepts_review_control_slot_not_present_in_snapshot(self) -> None:
        self._save_base_item(type_slots={"abstraction_level": "medium"})
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        response = service.apply_action(
            "item-1",
            QuestionReviewActionRequest(
                action="question_modify",
                control_overrides={"type_slots": {"statement_visibility": "low"}},
            ),
        )

        self.assertEqual(response["action"], "question_modify")

    def test_question_modify_rejects_more_than_two_type_slot_updates(self) -> None:
        self._save_base_item(type_slots={"slot_a": "value_a", "slot_b": "value_b", "slot_c": "value_c"})
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="question_modify",
                    control_overrides={"type_slots": {"slot_a": "x", "slot_b": "y", "slot_c": "z"}},
                ),
            )

    def test_question_modify_rejects_multi_select_with_more_than_two_values(self) -> None:
        self._save_base_item(type_slots={"distractor_modes": ["wrong_opening"]})
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="question_modify",
                    control_overrides={
                        "type_slots": {
                            "distractor_modes": ["wrong_opening", "wrong_closing", "block_swap"],
                        }
                    },
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

    def test_distractor_patch_rejects_targeting_answer_option(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="distractor_patch",
                    target_option="A",
                    option_text="新的干扰项",
                    analysis="A项正确，因为材料主旨最完整。",
                ),
            )

    def test_distractor_patch_answer_drift_is_rejected(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _DistractorPatchAnswerDriftGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="distractor_patch",
                    target_option="B",
                    option_text="新的干扰项",
                    analysis="A项正确，因为材料主旨最完整。",
                ),
            )

        self.assertEqual(self.repository.list_review_actions(item_id="item-1", limit=1), [])

    def test_distractor_patch_non_target_option_drift_is_rejected(self) -> None:
        self._save_base_item()
        service = QuestionReviewService(self.repository, _DistractorPatchScopeDriftGenerationService())

        with self.assertRaises(DomainError):
            service.apply_action(
                "item-1",
                QuestionReviewActionRequest(
                    action="distractor_patch",
                    target_option="B",
                    option_text="新的干扰项",
                    analysis="A项正确，因为材料主旨最完整。",
                ),
            )

        self.assertEqual(self.repository.list_review_actions(item_id="item-1", limit=1), [])

    def test_distractor_patch_accepts_strategy_and_intensity_as_patch_inputs(self) -> None:
        item = self._save_base_item()
        service = QuestionReviewService(self.repository, _DistractorPatchStrategyGenerationService())

        response = service.apply_action(
            item["item_id"],
            QuestionReviewActionRequest(
                action="distractor_patch",
                target_option="B",
                distractor_strategy="concept_swap",
                distractor_intensity="strong",
                operator="demo",
            ),
        )

        self.assertEqual(response["action"], "distractor_patch")
        revised = response["item"]
        self.assertEqual(revised["generated_question"]["answer"], "A")
        self.assertEqual(revised["generated_question"]["options"]["A"], item["generated_question"]["options"]["A"])
        self.assertEqual(revised["generated_question"]["options"]["C"], item["generated_question"]["options"]["C"])
        self.assertEqual(revised["generated_question"]["options"]["D"], item["generated_question"]["options"]["D"])
        self.assertNotEqual(revised["generated_question"]["options"]["B"], item["generated_question"]["options"]["B"])
        latest_action = self.repository.list_review_actions(item_id=item["item_id"], limit=1)[0]
        self.assertEqual(latest_action["action_type"], "distractor_patch")
        self.assertEqual(latest_action["payload"]["patch"]["distractor_strategy"], "concept_swap")
        self.assertEqual(latest_action["payload"]["patch"]["distractor_intensity"], "strong")
        self.assertIn("distractor_strategy", latest_action["payload"]["patch"]["input_fields"])
        self.assertIn("distractor_intensity", latest_action["payload"]["patch"]["input_fields"])
        self.assertEqual(latest_action["payload"]["patch"]["scope_name"], "single_distractor_patch")
        self.assertEqual(latest_action["payload"]["patch"]["scope_kind"], "patch")

    def test_minor_edit_marks_non_patch_scope_kind(self) -> None:
        item = self._save_base_item()
        service = QuestionReviewService(self.repository, _NoOpGenerationService())

        response = service.apply_action(
            item["item_id"],
            QuestionReviewActionRequest(action="minor_edit", instruction="调整措辞"),
        )

        self.assertEqual(response["action"], "minor_edit")
        latest_action = self.repository.list_review_actions(item_id=item["item_id"], limit=1)[0]
        self.assertEqual(latest_action["payload"]["scope_kind"], "non_patch")
        self.assertEqual(latest_action["payload"]["patch"]["scope_kind"], "non_patch")

    def test_patch_scope_registry_exposes_answer_binding_scope(self) -> None:
        scope = get_patch_scope("answer_binding_patch")
        self.assertIsNotNone(scope)
        self.assertIn("options", scope.allowed_fields)
        self.assertIn("answer", scope.allowed_fields)
        self.assertIn("analysis", scope.allowed_fields)

    def test_review_history_returns_blocked_sentence_fill_view_without_crashing(self) -> None:
        self._save_sentence_fill_item(
            item_id="sf-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "legacy_new_name",
                "logic_relation": "continuation",
            },
        )
        service = ReviewQueryService(self.repository)

        history = service.get_item_history("sf-1")

        view = history["item"]["sentence_fill_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["blocked_reason"], "unknown_sentence_fill_function_type_alias:legacy_new_name")
        self.assertIsNone(view["function_type"])

    def test_review_list_returns_canonical_sentence_fill_view(self) -> None:
        self._save_sentence_fill_item(
            item_id="sf-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "bridge_both_sides",
                "logic_relation": "continuation_or_transition",
            },
        )
        service = ReviewQueryService(self.repository)

        payload = service.list_items(
            status=None,
            question_type=None,
            business_subtype=None,
            batch_id=None,
            page=1,
            page_size=20,
            keyword=None,
        )

        item = next(entry for entry in payload["items"] if entry["item_id"] == "sf-1")
        view = item["sentence_fill_export_view"]
        self.assertEqual(view["status"], "mapped")
        self.assertEqual(view["function_type"], "bridge")
        self.assertEqual(view["logic_relation"], "continuation")
        self.assertNotEqual(view["function_type"], "bridge_both_sides")

    def test_review_marks_center_understanding_item_blocked_without_crashing(self) -> None:
        self._save_center_understanding_item(
            item_id="center-1",
            item_business_subtype="title_selection",
            request_business_subtype="center_understanding",
        )
        service = ReviewQueryService(self.repository)

        history = service.get_item_history("center-1")

        view = history["item"]["center_understanding_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(
            view["blocked_reason"],
            "title_selection_leaked_to_center_understanding_export:item.business_subtype",
        )
        self.assertIsNone(history["item"]["business_subtype"])

    def test_review_keeps_center_understanding_canonical_when_clean(self) -> None:
        self._save_center_understanding_item(
            item_id="center-1",
            item_business_subtype="center_understanding",
            request_business_subtype="center_understanding",
        )
        service = ReviewQueryService(self.repository)

        payload = service.list_items(
            status=None,
            question_type=None,
            business_subtype=None,
            batch_id=None,
            page=1,
            page_size=20,
            keyword=None,
        )

        item = next(entry for entry in payload["items"] if entry["item_id"] == "center-1")
        view = item["center_understanding_export_view"]
        self.assertEqual(view["status"], "direct")
        self.assertEqual(item["business_subtype"], "center_understanding")

    def test_review_marks_sentence_order_item_blocked_without_crashing(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={"candidate_type": "ordered_unit_group"},
            runtime_binding={
                "opening_rule": "explicit_opening",
                "closing_rule": "summary_or_conclusion",
            },
        )
        service = ReviewQueryService(self.repository)

        history = service.get_item_history("so-1")

        view = history["item"]["sentence_order_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["blocked_reason"], "ambiguous_sentence_order_closing_anchor:summary_or_conclusion")
        self.assertIsNone(view["closing_anchor_type"])
        self.assertNotIn("closing_rule", view)

    def test_review_list_returns_canonical_sentence_order_view(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={
                "candidate_type": "ordered_unit_group",
                "opening_anchor_type": "definition_opening",
                "closing_anchor_type": "countermeasure",
            },
        )
        service = ReviewQueryService(self.repository)

        payload = service.list_items(
            status=None,
            question_type=None,
            business_subtype=None,
            batch_id=None,
            page=1,
            page_size=20,
            keyword=None,
        )

        item = next(entry for entry in payload["items"] if entry["item_id"] == "so-1")
        view = item["sentence_order_export_view"]
        self.assertEqual(view["status"], "mapped")
        self.assertEqual(view["candidate_type"], "sentence_block_group")
        self.assertEqual(view["opening_anchor_type"], "explicit_topic")
        self.assertEqual(view["closing_anchor_type"], "call_to_action")
        self.assertNotIn("ordered_unit_group", str(view))
        self.assertNotIn("definition_opening", str(view))

    def _save_sentence_fill_item(self, *, item_id: str, resolved_slots: dict[str, str]) -> None:
        self.repository.save_batch(
            "batch-sf",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "sentence_fill",
                    "business_subtype": "sentence_fill_selection",
                    "difficulty_target": "medium",
                }
            },
        )
        item = {
            "item_id": item_id,
            "batch_id": "batch-sf",
            "question_type": "sentence_fill",
            "business_subtype": "sentence_fill_selection",
            "pattern_id": "bridge_transition",
            "difficulty_target": "medium",
            "resolved_slots": dict(resolved_slots),
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "sentence_fill",
                "business_subtype": "sentence_fill_selection",
                "pattern_id": "bridge_transition",
                "stem": "填入画横线部分最恰当的一项是（ ）。",
                "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                "answer": "A",
                "analysis": "解析",
            },
            "material_selection": {
                "material_id": "mat-sf-1",
                "article_id": "art-sf-1",
                "text": "材料",
                "resolved_slots": dict(resolved_slots),
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "type_slots": {
                    "blank_position": "opening",
                    "function_type": "summary",
                    "logic_relation": "summary",
                }
            },
            "statuses": {
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "pending_review",
            "selected_pattern": "bridge_transition",
        }
        self.repository.save_item(item)

    def _save_sentence_order_item(
        self,
        *,
        item_id: str,
        material_resolved_slots: dict[str, str] | None = None,
        runtime_binding: dict[str, str] | None = None,
    ) -> None:
        self.repository.save_batch(
            "batch-so",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "sentence_order",
                    "business_subtype": "sentence_order_selection",
                    "difficulty_target": "medium",
                }
            },
        )
        material_resolved_slots = dict(material_resolved_slots or {})
        item = {
            "item_id": item_id,
            "batch_id": "batch-so",
            "question_type": "sentence_order",
            "business_subtype": "sentence_order_selection",
            "pattern_id": "dual_anchor_lock",
            "difficulty_target": "medium",
            "resolved_slots": {},
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "sentence_order",
                "business_subtype": "sentence_order_selection",
                "pattern_id": "dual_anchor_lock",
                "stem": "将以下6个句子重新排列，语序正确的是：",
                "options": {"A": "1-2-3-4-5-6", "B": "1-3-2-4-5-6", "C": "2-1-3-4-5-6", "D": "1-2-4-3-5-6"},
                "answer": "A",
                "analysis": "先看首句和尾句，再看中间衔接。",
            },
            "material_selection": {
                "material_id": "mat-so-1",
                "article_id": "art-so-1",
                "text": "①先交代背景。②再说明问题。③随后分析原因。④接着提出对策。⑤再补充条件。⑥最后总结判断。",
                "resolved_slots": material_resolved_slots,
                "runtime_binding": dict(runtime_binding or {}),
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "type_slots": {
                    "candidate_type": "sentence_block_group",
                    "opening_anchor_type": "explicit_topic",
                    "closing_anchor_type": "conclusion",
                }
            },
            "statuses": {
                "review_status": "approved",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "approved",
            "selected_pattern": "dual_anchor_lock",
        }
        self.repository.save_item(item)

    def _save_center_understanding_item(
        self,
        *,
        item_id: str,
        item_business_subtype: str,
        request_business_subtype: str,
    ) -> None:
        self.repository.save_batch(
            "batch-center",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "main_idea",
                    "business_subtype": "center_understanding",
                    "difficulty_target": "medium",
                }
            },
        )
        item = {
            "item_id": item_id,
            "batch_id": "batch-center",
            "question_type": "main_idea",
            "business_subtype": item_business_subtype,
            "pattern_id": "whole_passage_integration",
            "difficulty_target": "medium",
            "resolved_slots": {},
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "main_idea",
                "business_subtype": item_business_subtype,
                "pattern_id": "whole_passage_integration",
                "stem": "主旨是？",
                "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                "answer": "A",
                "analysis": "解析",
            },
            "material_selection": {
                "material_id": "mat-center-1",
                "article_id": "art-center-1",
                "text": "材料",
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "business_subtype": request_business_subtype,
                "question_card_id": "question.center_understanding.standard_v1",
            },
            "statuses": {
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "pending_review",
            "selected_pattern": "whole_passage_integration",
        }
        self.repository.save_item(item)
