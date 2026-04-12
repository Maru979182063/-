from __future__ import annotations

from unittest import TestCase

from app.services.sentence_order_protocol import project_sentence_order_strict_export_view
from app.services.sentence_order_protocol import strict_sentence_order_export_field


class SentenceOrderProtocolTest(TestCase):
    def test_canonical_candidate_type_passes_directly(self) -> None:
        result = strict_sentence_order_export_field(
            canonical_field_name="candidate_type",
            source_field_name="candidate_type",
            value="sentence_block_group",
            source_name="item.resolved_slots",
        )
        self.assertEqual(result["status"], "direct")
        self.assertEqual(result["value"], "sentence_block_group")
        self.assertIsNone(result["alias_trace"])

    def test_ordered_unit_group_is_mapped_to_sentence_block_group(self) -> None:
        result = strict_sentence_order_export_field(
            canonical_field_name="candidate_type",
            source_field_name="candidate_type",
            value="ordered_unit_group",
            source_name="item.material_selection.runtime_binding",
        )
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(result["value"], "sentence_block_group")
        self.assertEqual(result["alias_trace"]["raw_value"], "ordered_unit_group")

    def test_weak_formal_order_group_is_mapped_to_sentence_block_group(self) -> None:
        result = strict_sentence_order_export_field(
            canonical_field_name="candidate_type",
            source_field_name="candidate_type",
            value="weak_formal_order_group",
            source_name="item.material_selection.runtime_binding",
        )
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(result["value"], "sentence_block_group")

    def test_opening_aliases_are_mapped_to_canonical_anchor(self) -> None:
        for raw_value, expected in (
            ("definition_opening", "explicit_topic"),
            ("explicit_opening", "explicit_topic"),
            ("background_opening", "upper_context_link"),
        ):
            result = strict_sentence_order_export_field(
                canonical_field_name="opening_anchor_type",
                source_field_name="opening_rule",
                value=raw_value,
                source_name="item.request_snapshot.source_question_analysis.retrieval_structure_constraints",
            )
            self.assertEqual(result["status"], "mapped")
            self.assertEqual(result["value"], expected)

    def test_countermeasure_maps_to_call_to_action(self) -> None:
        result = strict_sentence_order_export_field(
            canonical_field_name="closing_anchor_type",
            source_field_name="closing_rule",
            value="countermeasure",
            source_name="item.material_selection.runtime_binding",
        )
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(result["value"], "call_to_action")

    def test_summary_or_conclusion_blocks_without_precise_anchor(self) -> None:
        item = {
            "question_type": "sentence_order",
            "material_selection": {
                "resolved_slots": {
                    "candidate_type": "ordered_unit_group",
                },
                "runtime_binding": {
                    "opening_rule": "explicit_opening",
                    "closing_rule": "summary_or_conclusion",
                },
            },
        }
        view = project_sentence_order_strict_export_view(item)
        assert view is not None
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["blocked_reason"], "ambiguous_sentence_order_closing_anchor:summary_or_conclusion")
        self.assertIsNone(view["closing_anchor_type"])

    def test_unknown_values_block(self) -> None:
        candidate = strict_sentence_order_export_field(
            canonical_field_name="candidate_type",
            source_field_name="candidate_type",
            value="paragraph_window",
            source_name="item.resolved_slots",
        )
        opening = strict_sentence_order_export_field(
            canonical_field_name="opening_anchor_type",
            source_field_name="opening_rule",
            value="legacy_new_opening",
            source_name="item.resolved_slots",
        )
        closing = strict_sentence_order_export_field(
            canonical_field_name="closing_anchor_type",
            source_field_name="closing_rule",
            value="legacy_new_closing",
            source_name="item.resolved_slots",
        )
        self.assertEqual(candidate["status"], "blocked")
        self.assertEqual(opening["status"], "blocked")
        self.assertEqual(closing["status"], "blocked")

    def test_alias_trace_only_appears_for_mapped_fields(self) -> None:
        item = {
            "question_type": "sentence_order",
            "resolved_slots": {
                "candidate_type": "sentence_block_group",
                "opening_anchor_type": "explicit_topic",
                "closing_anchor_type": "conclusion",
            },
        }
        direct_view = project_sentence_order_strict_export_view(item)
        assert direct_view is not None
        self.assertEqual(direct_view["status"], "direct")
        self.assertEqual(direct_view["alias_trace"], [])

        mapped_item = {
            "question_type": "sentence_order",
            "material_selection": {
                "resolved_slots": {
                    "candidate_type": "ordered_unit_group",
                    "opening_anchor_type": "definition_opening",
                    "closing_anchor_type": "countermeasure",
                }
            },
        }
        mapped_view = project_sentence_order_strict_export_view(mapped_item)
        assert mapped_view is not None
        self.assertEqual(mapped_view["status"], "mapped")
        self.assertEqual(len(mapped_view["alias_trace"]), 3)
