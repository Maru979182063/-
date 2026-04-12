from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from app.core.exceptions import DomainError
from app.services.delivery_service import build_center_understanding_export_view
from app.services.delivery_service import DeliveryService
from app.services.delivery_service import evaluate_formal_export_policy
from app.services.question_repository import QuestionRepository


class DeliveryServiceSentenceFillExportTest(TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.repository = QuestionRepository(Path(self.tempdir.name) / "delivery_service.db")
        self.service = DeliveryService(self.repository)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_delivery_maps_known_sentence_fill_aliases_to_canonical_view(self) -> None:
        self._save_sentence_fill_item(
            item_id="item-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "bridge_both_sides",
                "logic_relation": "continuation_or_transition",
            },
        )

        payload = self.service.get_batch_delivery("batch-1")
        self.assertEqual(payload["approved_count"], 1)
        view = payload["items"][0]["sentence_fill_export_view"]
        self.assertEqual(view["status"], "mapped")
        self.assertEqual(view["blank_position"], "middle")
        self.assertEqual(view["function_type"], "bridge")
        self.assertEqual(view["logic_relation"], "continuation")
        self.assertNotIn("alias_trace", view)

    def test_delivery_blocks_unknown_sentence_fill_alias(self) -> None:
        self._save_sentence_fill_item(
            item_id="item-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "legacy_new_name",
                "logic_relation": "continuation",
            },
        )

        with self.assertRaises(DomainError):
            self.service.get_batch_delivery("batch-1")

    def test_markdown_export_reuses_same_gate(self) -> None:
        self._save_sentence_fill_item(
            item_id="item-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "legacy_new_name",
                "logic_relation": "continuation",
            },
        )

        with self.assertRaises(DomainError):
            self.service.export_markdown("batch-1")

    def test_delivery_blocks_center_understanding_title_selection_leak(self) -> None:
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
        self.repository.save_item(
            {
                "item_id": "center-1",
                "batch_id": "batch-center",
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "pattern_id": "whole_passage_integration",
                "difficulty_target": "medium",
                "resolved_slots": {},
                "skeleton": {},
                "control_logic": {},
                "generation_logic": {},
                "prompt_package": {},
                "generated_question": {
                    "question_type": "main_idea",
                    "business_subtype": "title_selection",
                    "pattern_id": "whole_passage_integration",
                    "stem": "主旨是？",
                    "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                    "answer": "A",
                    "analysis": "解析",
                },
                "material_selection": {
                    "material_id": "mat-center",
                    "article_id": "art-center",
                    "text": "材料",
                    "source": {"source_name": "demo"},
                },
                "request_snapshot": {
                    "business_subtype": "center_understanding",
                    "question_card_id": "question.center_understanding.standard_v1",
                },
                "statuses": {
                    "review_status": "approved",
                    "generation_status": "success",
                    "validation_status": "passed",
                },
                "validation_result": {"passed": True, "validation_status": "passed"},
                "current_version_no": 1,
                "current_status": "approved",
                "selected_pattern": "whole_passage_integration",
            }
        )

        with self.assertRaises(DomainError):
            self.service.get_batch_delivery("batch-center")

    def test_center_understanding_view_passes_only_center_understanding(self) -> None:
        view = build_center_understanding_export_view(
            {
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "generated_question": {"business_subtype": "center_understanding"},
                "request_snapshot": {"question_card_id": "question.center_understanding.standard_v1"},
            }
        )
        assert view is not None
        self.assertEqual(view["status"], "direct")
        self.assertEqual(view["business_subtype"], "center_understanding")

    def test_sentence_order_formal_training_export_policy_is_blocked(self) -> None:
        policy = evaluate_formal_export_policy(question_type="sentence_order", export_target="formal_training_export")
        self.assertFalse(policy["allowed"])
        self.assertEqual(
            policy["blocked_reason"],
            "missing_sentence_order_projection_for_export:formal_training_export",
        )

    def test_sentence_order_non_formal_export_paths_remain_allowed(self) -> None:
        for export_target in ("offline_replay", "audit", "error_analysis"):
            policy = evaluate_formal_export_policy(question_type="sentence_order", export_target=export_target)
            self.assertTrue(policy["allowed"])

    def test_delivery_returns_canonical_sentence_order_view(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={
                "candidate_type": "ordered_unit_group",
                "opening_anchor_type": "definition_opening",
                "closing_anchor_type": "countermeasure",
            },
        )

        payload = self.service.get_batch_delivery("batch-so")

        self.assertEqual(payload["approved_count"], 1)
        view = payload["items"][0]["sentence_order_export_view"]
        self.assertEqual(view["status"], "mapped")
        self.assertEqual(view["candidate_type"], "sentence_block_group")
        self.assertEqual(view["opening_anchor_type"], "explicit_topic")
        self.assertEqual(view["closing_anchor_type"], "call_to_action")
        self.assertNotIn("alias_trace", view)

    def test_delivery_blocks_dirty_sentence_order_item(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={"candidate_type": "ordered_unit_group"},
            runtime_binding={
                "opening_rule": "explicit_opening",
                "closing_rule": "summary_or_conclusion",
            },
        )

        with self.assertRaises(DomainError):
            self.service.get_batch_delivery("batch-so")

    def test_sentence_order_markdown_export_reuses_same_gate(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={"candidate_type": "ordered_unit_group"},
            runtime_binding={
                "opening_rule": "explicit_opening",
                "closing_rule": "summary_or_conclusion",
            },
        )

        with self.assertRaises(DomainError):
            self.service.export_markdown("batch-so")

    def test_sentence_order_formal_training_export_policy_allows_clean_projected_item(self) -> None:
        item = self._sentence_order_item_payload(
            item_id="so-1",
            material_resolved_slots={
                "candidate_type": "ordered_unit_group",
                "opening_anchor_type": "definition_opening",
                "closing_anchor_type": "countermeasure",
            },
        )

        policy = evaluate_formal_export_policy(
            question_type="sentence_order",
            export_target="formal_training_export",
            item=item,
        )

        self.assertTrue(policy["allowed"])

    def test_sentence_order_formal_training_export_policy_blocks_dirty_item(self) -> None:
        item = self._sentence_order_item_payload(
            item_id="so-1",
            material_resolved_slots={"candidate_type": "ordered_unit_group"},
            runtime_binding={
                "opening_rule": "explicit_opening",
                "closing_rule": "summary_or_conclusion",
            },
        )

        policy = evaluate_formal_export_policy(
            question_type="sentence_order",
            export_target="formal_training_export",
            item=item,
        )

        self.assertFalse(policy["allowed"])
        self.assertEqual(policy["blocked_reason"], "ambiguous_sentence_order_closing_anchor:summary_or_conclusion")

    def _save_sentence_fill_item(self, *, item_id: str, resolved_slots: dict[str, str]) -> None:
        self.repository.save_batch(
            "batch-1",
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
            "batch_id": "batch-1",
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
                "material_id": "mat-1",
                "article_id": "art-1",
                "text": "材料原文",
                "resolved_slots": dict(resolved_slots),
                "source": {"source_name": "demo"},
            },
            "request_snapshot": {
                "type_slots": {
                    "blank_position": "opening",
                    "function_type": "summary",
                    "logic_relation": "summary",
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
        self.repository.save_item(
            self._sentence_order_item_payload(
                item_id=item_id,
                material_resolved_slots=material_resolved_slots,
                runtime_binding=runtime_binding,
            )
        )

    def _sentence_order_item_payload(
        self,
        *,
        item_id: str,
        material_resolved_slots: dict[str, str] | None = None,
        runtime_binding: dict[str, str] | None = None,
    ) -> dict:
        return {
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
                "resolved_slots": dict(material_resolved_slots or {}),
                "runtime_binding": dict(runtime_binding or {}),
                "source": {"source_name": "demo"},
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
