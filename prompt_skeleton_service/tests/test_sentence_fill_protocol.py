from __future__ import annotations

from unittest import TestCase

from app.services.sentence_fill_protocol import project_sentence_fill_strict_export_view
from app.services.sentence_fill_protocol import strict_sentence_fill_export_field


class SentenceFillProtocolStrictExportTest(TestCase):
    def test_blank_position_canonical_value_passes_directly(self) -> None:
        result = strict_sentence_fill_export_field("blank_position", "middle", source_name="test")
        self.assertEqual(result["status"], "direct")
        self.assertEqual(result["value"], "middle")
        self.assertIsNone(result["alias_trace"])
        self.assertIsNone(result["blocked_reason"])

    def test_function_type_known_alias_is_mapped_with_trace(self) -> None:
        result = strict_sentence_fill_export_field("function_type", "opening_summary", source_name="test")
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(result["value"], "summary")
        self.assertEqual(result["alias_trace"]["raw_value"], "opening_summary")
        self.assertEqual(result["alias_trace"]["mapped_value"], "summary")

    def test_logic_relation_known_alias_is_mapped_with_trace(self) -> None:
        result = strict_sentence_fill_export_field("logic_relation", "continuation_or_transition", source_name="test")
        self.assertEqual(result["status"], "mapped")
        self.assertEqual(result["value"], "continuation")
        self.assertEqual(result["alias_trace"]["raw_value"], "continuation_or_transition")
        self.assertEqual(result["alias_trace"]["mapped_value"], "continuation")

    def test_unknown_function_type_is_blocked(self) -> None:
        result = strict_sentence_fill_export_field("function_type", "legacy_new_name", source_name="test")
        self.assertEqual(result["status"], "blocked")
        self.assertIsNone(result["value"])
        self.assertIsNone(result["alias_trace"])
        self.assertEqual(result["blocked_reason"], "unknown_sentence_fill_function_type_alias:legacy_new_name")

    def test_unknown_logic_relation_is_blocked(self) -> None:
        result = strict_sentence_fill_export_field("logic_relation", "summary_then_turn", source_name="test")
        self.assertEqual(result["status"], "blocked")
        self.assertIsNone(result["value"])
        self.assertIsNone(result["alias_trace"])
        self.assertEqual(result["blocked_reason"], "unknown_sentence_fill_logic_relation_alias:summary_then_turn")

    def test_non_canonical_blank_position_is_blocked(self) -> None:
        result = strict_sentence_fill_export_field("blank_position", "opening_summary", source_name="test")
        self.assertEqual(result["status"], "blocked")
        self.assertIsNone(result["value"])
        self.assertIsNone(result["alias_trace"])
        self.assertEqual(result["blocked_reason"], "non_canonical_sentence_fill_blank_position:opening_summary")

    def test_alias_trace_only_exists_when_alias_is_mapped(self) -> None:
        direct = strict_sentence_fill_export_field("function_type", "bridge", source_name="test")
        mapped = strict_sentence_fill_export_field("function_type", "bridge_both_sides", source_name="test")
        self.assertIsNone(direct["alias_trace"])
        self.assertIsNotNone(mapped["alias_trace"])

    def test_projection_prefers_resolved_slots_over_request_snapshot(self) -> None:
        item = {
            "question_type": "sentence_fill",
            "resolved_slots": {
                "blank_position": "middle",
                "function_type": "middle_explanation",
                "logic_relation": "explanation",
            },
            "material_selection": {
                "resolved_slots": {
                    "blank_position": "opening",
                    "function_type": "opening_summary",
                    "logic_relation": "summary",
                }
            },
            "request_snapshot": {
                "type_slots": {
                    "blank_position": "ending",
                    "function_type": "conclusion",
                    "logic_relation": "summary",
                }
            },
        }

        view = project_sentence_fill_strict_export_view(item)
        assert view is not None
        self.assertEqual(view["status"], "mapped")
        self.assertEqual(view["blank_position"], "middle")
        self.assertEqual(view["function_type"], "carry_previous")
        self.assertEqual(view["logic_relation"], "explanation")
        self.assertEqual(len(view["alias_trace"]), 1)

