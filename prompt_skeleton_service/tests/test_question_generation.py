from __future__ import annotations

import sys
import types
from unittest import TestCase
from unittest.mock import Mock


def _install_test_stubs() -> None:
    if "fastapi" not in sys.modules:
        fastapi = types.ModuleType("fastapi")
        fastapi.FastAPI = type("FastAPI", (), {})
        fastapi.Request = type("Request", (), {})
        sys.modules["fastapi"] = fastapi
    if "fastapi.responses" not in sys.modules:
        responses = types.ModuleType("fastapi.responses")
        responses.JSONResponse = type("JSONResponse", (), {})
        sys.modules["fastapi.responses"] = responses
    if "yaml" not in sys.modules:
        yaml = types.ModuleType("yaml")
        yaml.safe_load = lambda *args, **kwargs: {}
        sys.modules["yaml"] = yaml


_install_test_stubs()

from app.schemas.question import QuestionGenerateRequest
from app.services.question_generation import QuestionGenerationService


class QuestionGenerationUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = QuestionGenerationService.__new__(QuestionGenerationService)

    def test_remap_option_references_updates_explicit_correct_markers(self) -> None:
        analysis = "A（正确）而B项偏题，因此正确答案是A，故选A。"
        mapping = {"A": "C", "B": "A", "C": "D", "D": "B"}

        remapped = self.service._remap_option_references(analysis, mapping)

        self.assertIn("C（正确）", remapped)
        self.assertIn("正确答案是C", remapped)
        self.assertIn("故选C", remapped)
        self.assertNotIn("A（正确）", remapped)
        self.assertNotIn("正确答案是A", remapped)

    def test_explicit_question_card_decode_result_uses_runtime_binding(self) -> None:
        self.service.question_card_binding = Mock()
        self.service.question_card_binding.resolve.return_value = {
            "question_card_id": "question.title_selection.standard_v1",
            "runtime_binding": {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
            },
            "binding_source": "explicit_question_card_id",
            "binding_reason": "explicit_question_card_id",
            "warning": None,
        }
        request = QuestionGenerateRequest.model_validate(
            {
                "question_card_id": "question.title_selection.standard_v1",
                "question_focus": "center_understanding",
                "special_question_types": ["dual_anchor_lock"],
                "difficulty_level": "medium",
                "count": 1,
                "topic": "topic-x",
                "type_slots": {"slot_a": "value_a"},
                "extra_constraints": {"keep": True},
                "text_direction": "policy",
            }
        )

        decoded = self.service._build_explicit_question_card_decode_result(request)

        self.assertEqual(decoded["mapping_source"], "question_card_id")
        self.assertEqual(decoded["standard_request"]["question_type"], "main_idea")
        self.assertEqual(decoded["standard_request"]["business_subtype"], "title_selection")
        self.assertEqual(decoded["standard_request"]["topic"], "topic-x")
        self.assertEqual(decoded["standard_request"]["type_slots"], {"slot_a": "value_a"})
        self.assertTrue(decoded["standard_request"]["extra_constraints"]["keep"])
        self.assertEqual(decoded["standard_request"]["extra_constraints"]["text_direction"], "policy")

    def test_decode_request_does_not_override_explicit_focus_or_special_type(self) -> None:
        self.service.source_question_analyzer = Mock()
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "sentence_order",
                "special_question_types": ["dual_anchor_lock"],
                "difficulty_level": "medium",
                "count": 1,
            }
        )

        decode_request, warning = self.service._build_decode_request(request)

        self.service.source_question_analyzer.infer_request_target.assert_not_called()
        self.assertIsNone(warning)
        self.assertEqual(decode_request.question_focus, "sentence_order")
        self.assertEqual(decode_request.special_question_types, ["dual_anchor_lock"])

    def test_list_replacement_materials_preserves_question_card_id(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.material_bridge.list_material_options.return_value = []
        item = {
            "item_id": "item-1",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "difficulty_target": "medium",
            "request_snapshot": {
                "question_card_id": "question.title_selection.standard_v1",
                "source_question_analysis": {},
            },
            "material_selection": {
                "material_id": "mat-1",
            },
        }

        self.service.list_replacement_materials(item, limit=3)

        self.assertEqual(
            self.service.material_bridge.list_material_options.call_args.kwargs["question_card_id"],
            "question.title_selection.standard_v1",
        )
        self.assertEqual(
            self.service.material_bridge.list_material_options.call_args.kwargs["preferred_business_card_ids"],
            [],
        )
        self.assertNotIn("business_card_ids", self.service.material_bridge.list_material_options.call_args.kwargs)

    def test_list_replacement_materials_demotes_source_business_cards_to_preferred(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.material_bridge.list_material_options.return_value = []
        item = {
            "item_id": "item-1",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "difficulty_target": "medium",
            "request_snapshot": {
                "question_card_id": "question.title_selection.standard_v1",
                "source_question_analysis": {
                    "business_card_ids": ["turning_relation_focus__main_idea"],
                },
            },
            "material_selection": {
                "material_id": "mat-1",
            },
        }

        self.service.list_replacement_materials(item, limit=3)

        self.assertEqual(
            self.service.material_bridge.list_material_options.call_args.kwargs["preferred_business_card_ids"],
            ["turning_relation_focus__main_idea"],
        )
        self.assertNotIn("business_card_ids", self.service.material_bridge.list_material_options.call_args.kwargs)

    def test_revise_text_modify_preserves_question_card_id(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "question_card_id": "question.title_selection.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {},
                "extra_constraints": {},
                "material_structure": None,
                "topic": None,
                "material_policy": None,
            }
        )
        self.service._material_policy_from_snapshot = Mock(return_value=None)
        self.service._clean_material_text = Mock(return_value="")
        material = Mock()
        material.material_id = "mat-2"
        self.service.material_bridge.select_materials.return_value = ([material], [])
        self.service._refine_material_if_needed = Mock(return_value=material)
        self.service._annotate_material_usage = Mock(return_value=material)
        self.service._build_generated_item = Mock(return_value={"warnings": []})
        self.service.runtime_config = types.SimpleNamespace(
            llm=types.SimpleNamespace(
                routing=types.SimpleNamespace(
                    review_actions=types.SimpleNamespace(text_modify="text_modify_route")
                )
            )
        )
        item = {
            "item_id": "item-1",
            "batch_id": "batch-1",
            "revision_count": 0,
            "request_snapshot": {},
            "material_selection": {"material_id": "mat-1"},
        }

        self.service.revise_text_modify(item, instruction=None, control_overrides={})

        self.assertEqual(
            self.service.material_bridge.select_materials.call_args.kwargs["question_card_id"],
            "question.title_selection.standard_v1",
        )
        self.assertEqual(
            self.service.material_bridge.select_materials.call_args.kwargs["preferred_business_card_ids"],
            [],
        )
        self.assertNotIn("business_card_ids", self.service.material_bridge.select_materials.call_args.kwargs)
