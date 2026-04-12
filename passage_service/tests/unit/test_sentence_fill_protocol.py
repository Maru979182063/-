from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest

import yaml

try:
    import pydantic_settings  # type: ignore
except ModuleNotFoundError:
    stub = types.ModuleType("pydantic_settings")

    class _BaseSettings:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _settings_config_dict(**kwargs):
        return dict(kwargs)

    stub.BaseSettings = _BaseSettings
    stub.SettingsConfigDict = _settings_config_dict
    sys.modules["pydantic_settings"] = stub

from app.services.main_card_signal_resolver import MainCardSignalResolver
from app.services.material_pipeline_v2 import MaterialPipelineV2
from app.services.sentence_fill_protocol import (
    normalize_sentence_fill_function_type,
    normalize_sentence_fill_logic_relation,
)


ROOT = Path(__file__).resolve().parents[3]


class SentenceFillProtocolTests(unittest.TestCase):
    def test_legacy_sentence_fill_inputs_normalize_to_canonical(self) -> None:
        self.assertEqual(normalize_sentence_fill_function_type("bridge_both_sides"), "bridge")
        self.assertEqual(normalize_sentence_fill_function_type("summarize_following_text"), "summary")
        self.assertEqual(normalize_sentence_fill_function_type("topic_introduction"), "topic_intro")
        self.assertEqual(normalize_sentence_fill_function_type("summarize_previous_text"), "conclusion")
        self.assertEqual(normalize_sentence_fill_function_type("propose_countermeasure"), "countermeasure")
        self.assertEqual(normalize_sentence_fill_logic_relation("continuation_or_transition"), "continuation")

    def test_signal_layer_and_material_cards_only_use_canonical_sentence_fill_vocab(self) -> None:
        type_config = yaml.safe_load(
            (ROOT / "prompt_skeleton_service" / "configs" / "types" / "sentence_fill.yaml").read_text(encoding="utf-8")
        )
        structure_schema = dict(type_config.get("structure_schema") or {})
        self.assertIn("blank_position", structure_schema)
        self.assertNotIn("position", structure_schema)

        signal_layer = yaml.safe_load(
            (ROOT / "card_specs" / "normalized" / "signal_layers" / "sentence_fill_signal_layer.normalized.yaml").read_text(
                encoding="utf-8"
            )
        )
        function_signal = next(item for item in signal_layer["signals"] if item["signal_id"] == "function_type")
        logic_signal = next(item for item in signal_layer["signals"] if item["signal_id"] == "logic_relation")
        self.assertEqual(
            function_signal["allowed_values"],
            [
                "summary",
                "topic_intro",
                "carry_previous",
                "lead_next",
                "bridge",
                "reference_summary",
                "countermeasure",
                "conclusion",
            ],
        )
        self.assertIn("action", logic_signal["allowed_values"])
        self.assertNotIn("opening_summary", function_signal["allowed_values"])

        material_card_registry = yaml.safe_load(
            (
                ROOT
                / "card_specs"
                / "normalized"
                / "material_cards"
                / "sentence_fill_intermediate_material_cards.normalized.yaml"
            ).read_text(encoding="utf-8")
        )
        legacy_function_values = {
            "opening_summary",
            "middle_explanation",
            "middle_focus_shift",
            "ending_summary",
            "ending_elevation",
            "inserted_reference",
            "comprehensive_match",
        }
        for card in material_card_registry.get("cards") or []:
            required = dict(card.get("required_signals") or {})
            function_type = str(required.get("function_type") or "")
            logic_relation = str(required.get("logic_relation") or "")
            self.assertNotIn(function_type, legacy_function_values)
            self.assertNotEqual(logic_relation, "continuation_or_transition")

    def test_build_sentence_fill_business_profile_returns_canonical_values(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        profile = pipeline._build_sentence_fill_business_profile(
            {
                "blank_position": "middle",
                "function_type": "bridge_both_sides",
                "logic_relation": "continuation_or_transition",
                "slot_explicit_ready": True,
                "backward_link_strength": 0.7,
                "forward_link_strength": 0.72,
                "bidirectional_validation": 0.76,
                "countermeasure_signal_strength": 0.1,
                "summary_need_strength": 0.2,
                "abstraction_level": 0.3,
                "object_match_strength": 0.4,
            }
        )

        self.assertEqual(profile["function_type"], "bridge")
        self.assertEqual(profile["logic_relation"], "continuation")
        self.assertNotIn("slot_role", profile)
        self.assertNotIn("slot_function", profile)

    def test_functional_slot_meta_and_trace_use_canonical_field_names(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        candidate = {
            "candidate_type": "functional_slot_unit",
            "text": "因此，我们还需要持续推进。",
            "meta": {
                "slot_role": "middle",
                "slot_function": "bridge_both_sides",
                "sentence_range": [1, 1],
                "paragraph_range": [1, 1],
            },
        }

        hydrated = pipeline._hydrate_functional_slot_meta(article_context={}, candidate=candidate)
        generated_by = pipeline._candidate_generated_by(
            {"candidate_type": "functional_slot_unit", "meta": hydrated},
            planner_source="unit_test",
        )

        self.assertEqual(hydrated["blank_position"], "middle")
        self.assertEqual(hydrated["function_type"], "bridge")
        self.assertNotIn("slot_role", hydrated)
        self.assertNotIn("slot_function", hydrated)
        self.assertIn("blank_position=middle", generated_by)
        self.assertIn("function_type=bridge", generated_by)
        self.assertNotIn("slot_role=", generated_by)
        self.assertNotIn("slot_function=", generated_by)

    def test_sentence_fill_scoring_trace_uses_canonical_source_fields(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.sentence_splitter = types.SimpleNamespace(split=lambda text: [segment for segment in [text] if segment])
        candidate = {
            "candidate_type": "functional_slot_unit",
            "text": "对此，我们还需要进一步完善协同机制。",
            "meta": {
                "blank_value_ready": True,
                "blank_value_reason": "middle_bridge_prechecked_gap",
                "slot_bridge_dependency_score": 0.82,
                "slot_carry_dependency_score": 0.34,
                "slot_forward_dependency_score": 0.56,
                "slot_context_sentence_range": [0, 2],
                "slot_sentence_range": [1, 1],
            },
        }
        scoring = pipeline._build_sentence_fill_scoring(
            signal_profile={
                "blank_position": "middle",
                "function_type": "bridge",
                "slot_explicit_ready": True,
                "summary_strength": 0.1,
                "countermeasure_signal_strength": 0.05,
                "object_match_strength": 0.2,
                "standalone_readability": 0.35,
            },
            candidate=candidate,
        )

        self.assertEqual(scoring["difficulty_trace"]["source_fields"]["blank_position"], "middle")
        self.assertEqual(scoring["difficulty_trace"]["source_fields"]["function_type"], "bridge")
        self.assertNotIn("slot_role", scoring["difficulty_trace"]["source_fields"])
        self.assertNotIn("slot_function", scoring["difficulty_trace"]["source_fields"])
        self.assertNotIn("slot_role", scoring["score_trace"]["source_fields"])
        self.assertNotIn("slot_function", scoring["score_trace"]["source_fields"])

    def test_sentence_fill_expected_profile_prefers_slot_projection_canonical_function(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        result = pipeline._sentence_fill_expected_profile(
            {
                "card_meta": {"business_card_id": "sentence_fill__middle_bridge_both_sides__abstract"},
                "mother_card_id": "sentence_fill",
                "feature_signature": {"business_function": "bridge_both_sides"},
                "slot_projection": {
                    "type_slots": {
                        "blank_position": "middle",
                        "function_type": "bridge",
                    }
                },
            }
        )

        self.assertTrue(result["allow_continue"])
        self.assertEqual(result["value"]["blank_position"], "middle")
        self.assertEqual(result["value"]["business_function"], "bridge")
        self.assertEqual(result["source_fields"]["business_function"], "slot_projection.type_slots.function_type")

    def test_main_card_signal_resolver_sentence_fill_schema_is_canonical(self) -> None:
        resolver = MainCardSignalResolver.__new__(MainCardSignalResolver)
        schema = resolver._response_schema("sentence_fill")
        self.assertNotIn("slot_role", schema["properties"])
        self.assertNotIn("slot_function", schema["properties"])
        self.assertEqual(
            schema["properties"]["function_type"]["enum"],
            ["summary", "topic_intro", "carry_previous", "lead_next", "bridge", "reference_summary", "countermeasure", "conclusion"],
        )


if __name__ == "__main__":
    unittest.main()
