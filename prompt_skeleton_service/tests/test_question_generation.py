from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock

from pydantic import TypeAdapter


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
    if "fastapi.exceptions" not in sys.modules:
        exceptions = types.ModuleType("fastapi.exceptions")
        exceptions.RequestValidationError = type("RequestValidationError", (Exception,), {})
        sys.modules["fastapi.exceptions"] = exceptions
    if "yaml" not in sys.modules:
        try:
            import yaml as _yaml  # type: ignore
        except ImportError:
            yaml = types.ModuleType("yaml")
            yaml.safe_load = lambda *args, **kwargs: {}
            sys.modules["yaml"] = yaml
        else:
            sys.modules["yaml"] = _yaml


_install_test_stubs()

from app.core.exceptions import DomainError
from app.schemas.item import GeneratedQuestion
from app.schemas.question import MaterialSelectionResult, QuestionGenerateRequest
from app.services.patch_scope_registry import get_patch_scope, resolve_repair_mode_scope
from app.services.question_generation import MaterialRefinementDraft, QuestionGenerationService


class QuestionGenerationUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = QuestionGenerationService.__new__(QuestionGenerationService)
        self.service.orchestrator = Mock()
        self.service.orchestrator.registry = Mock()
        self.service.material_bridge = Mock()
        self.service.material_bridge._normalize_preference_profile = Mock(return_value={})
        self.service.source_question_parser = Mock()
        self.service.source_question_analyzer = Mock()
        self.service.llm_gateway = Mock()
        self.service.material_refinement_adapter = TypeAdapter(MaterialRefinementDraft)
        self.service.distill_runtime_overlay = Mock()
        self.service.distill_runtime_overlay.resolve.return_value = {}
        self.service.runtime_config = SimpleNamespace(
            evaluation=SimpleNamespace(judge=SimpleNamespace(enabled=True)),
            llm=SimpleNamespace(
                routing=SimpleNamespace(
                    material_refinement="material_refinement",
                    question_repair="question_repair",
                    review_actions=SimpleNamespace(minor_edit="material_refinement"),
                )
            )
        )
        self.service.orchestrator.registry.list_enabled_patterns.side_effect = lambda question_type: {
            "sentence_order": ["dual_anchor_lock", "carry_parallel_expand", "viewpoint_reason_action", "problem_solution_case_blocks"],
            "sentence_fill": [
                "inserted_reference_match",
                "opening_summary",
                "bridge_transition",
                "middle_focus_shift",
                "middle_explanation",
                "ending_summary",
                "ending_elevation",
                "comprehensive_multi_match",
            ],
            "main_idea": [],
        }.get(question_type, [])

    def test_prepare_request_normalizes_source_question_and_user_material_payloads(self) -> None:
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "sentence_fill",
                "difficulty_level": "medium",
                "count": 1,
                "source_question": {
                    "passage": "<w:p><w:t>第一段\\u00a0内容</w:t></w:p>\x07",
                    "stem": "题干",
                    "options": {"A": "<w:t>选项A</w:t>", "B": "选项B"},
                },
                "user_material": {
                    "text": "<w:p><w:t>材料正文\\u00a0</w:t></w:p>",
                    "title": "<w:t>材料标题</w:t>",
                    "source_label": "<w:t>来源</w:t>",
                },
            }
        )

        prepared = self.service._prepare_request(request)

        self.assertEqual(prepared.source_question.passage, "第一段 内容")
        self.assertEqual(prepared.source_question.options["A"], "选项A")
        self.assertEqual(prepared.user_material.text, "材料正文")
        self.assertEqual(prepared.user_material.title, "材料标题")
        self.service.source_question_parser.parse.assert_not_called()

    def test_remap_option_references_updates_explicit_correct_markers(self) -> None:
        analysis = "A（正确）而B项偏题，因此正确答案是A，故选A。"
        mapping = {"A": "C", "B": "A", "C": "D", "D": "B"}

        remapped = self.service._remap_option_references(analysis, mapping)

        self.assertIn("C（正确）", remapped)
        self.assertIn("正确答案是C", remapped)
        self.assertIn("故选C", remapped)
        self.assertNotIn("A（正确）", remapped)
        self.assertNotIn("正确答案是A", remapped)

    def test_explicit_question_card_decode_result_drops_deprecated_text_direction(self) -> None:
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
                "extra_constraints": {"keep": True, "text_direction": "policy"},
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
        self.assertNotIn("text_direction", decoded["standard_request"]["extra_constraints"])

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

    def test_apply_control_overrides_accepts_review_control_slot_not_in_snapshot(self) -> None:
        updated = self.service._apply_control_overrides(
            {
                "question_type": "main_idea",
                "type_slots": {"abstraction_level": "medium"},
                "extra_constraints": {},
            },
            {"type_slots": {"statement_visibility": "low"}},
            instruction=None,
        )

        self.assertEqual(updated["type_slots"]["abstraction_level"], "medium")
        self.assertEqual(updated["type_slots"]["statement_visibility"], "low")

    def test_sentence_order_extract_units_normalizes_seven_sentences_into_six_units(self) -> None:
        text = (
            "第一句先交代背景。第二句补充现实限制。"
            "因此第三句把前文条件收束成新的判断。第四句接着提出推进思路。"
            "第五句说明配套条件。第六句归纳阶段重点。第七句最后给出总结。"
        )

        units = self.service._extract_sortable_units_from_text(text)

        self.assertEqual(len(units), 6)
        self.assertTrue(any("因此第三句把前文条件收束成新的判断" in unit for unit in units))

    def test_sentence_order_coerce_material_preserves_six_units_with_two_sentence_block(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-1",
            article_id="a-1",
            text=(
                "第一句先交代背景。第二句补充现实限制。"
                "因此第三句把前文条件收束成新的判断。第四句接着提出推进思路。"
                "第五句说明配套条件。第六句归纳阶段重点。第七句最后给出总结。"
            ),
            source={"article_title": "x", "source_name": "x", "source_id": "x"},
            document_genre="news",
            selection_reason="test",
        )

        coerced = self.service._coerce_sentence_order_material(
            material=material,
            source_question_analysis={"structure_constraints": {"sortable_unit_count": 6}},
        )

        self.assertIsNotNone(coerced)
        self.assertEqual(self.service._count_sortable_units_from_material(coerced.text), 6)

    def test_decode_request_infers_missing_target_from_source_question(self) -> None:
        self.service.source_question_analyzer = Mock()
        self.service.source_question_analyzer.infer_request_target.return_value = {
            "question_type": "main_idea",
            "business_subtype": "title_selection",
        }
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "",
                "difficulty_level": "medium",
                "count": 1,
                "source_question": {
                    "stem": "根据下列材料，下列标题最恰当的一项是？",
                    "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                },
            }
        )

        decode_request, warning = self.service._build_decode_request(request)

        self.service.source_question_analyzer.infer_request_target.assert_called_once()
        self.assertEqual(decode_request.question_focus, "center_understanding")
        self.assertEqual(decode_request.business_subtype, "title_selection")
        self.assertIn("reference_question_inferred_target_applied", warning)

    def test_decode_request_preserves_explicit_focus_but_completes_missing_business_subtype(self) -> None:
        self.service.source_question_analyzer = Mock()
        self.service.source_question_analyzer.infer_request_target.return_value = {
            "question_type": "sentence_fill",
            "business_subtype": None,
        }
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "sentence_fill",
                "difficulty_level": "medium",
                "count": 1,
                "source_question": {
                    "stem": "将最恰当的一句填入文中横线处",
                    "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                },
            }
        )

        decode_request, warning = self.service._build_decode_request(request)

        self.assertEqual(decode_request.question_focus, "sentence_fill")
        self.assertEqual(decode_request.business_subtype, "sentence_fill_selection")
        self.assertIn("business_subtype=sentence_fill_selection", warning)

    def test_requested_taxonomy_bridge_hints_reads_sentence_fill_slots_and_reference_cards(self) -> None:
        hints = self.service._requested_taxonomy_bridge_hints(
            question_type="sentence_fill",
            type_slots={
                "blank_position": "ending",
                "function_type": "countermeasure",
                "logic_relation": "action",
                "reference_dependency": "high",
            },
            extra_constraints={
                "reference_business_cards": ["sentence_fill__ending_countermeasure__abstract"],
                "reference_query_terms": ["治理", "路径"],
            },
        )

        self.assertEqual(hints["business_card_ids"], ["sentence_fill__ending_countermeasure__abstract"])
        self.assertEqual(hints["preferred_business_card_ids"], ["sentence_fill__ending_countermeasure__abstract"])
        self.assertEqual(hints["query_terms"], ["治理", "路径"])
        self.assertEqual(hints["structure_constraints"]["blank_position"], "ending")
        self.assertEqual(hints["structure_constraints"]["function_type"], "countermeasure")
        self.assertEqual(hints["structure_constraints"]["logic_relation"], "action")

    def test_requested_taxonomy_bridge_hints_reads_center_understanding_cards_and_slots(self) -> None:
        hints = self.service._requested_taxonomy_bridge_hints(
            question_type="main_idea",
            type_slots={
                "structure_type": "turning",
                "main_point_source": "whole_passage",
                "main_axis_source": "transition_after",
            },
            extra_constraints={
                "reference_business_cards": ["turning_relation_focus__main_idea"],
            },
        )

        self.assertEqual(hints["business_card_ids"], ["turning_relation_focus__main_idea"])
        self.assertEqual(hints["preferred_business_card_ids"], ["turning_relation_focus__main_idea"])
        self.assertEqual(hints["structure_constraints"]["structure_type"], "turning")
        self.assertEqual(hints["structure_constraints"]["main_axis_source"], "transition_after")

    def test_requested_pattern_bridge_hints_support_sentence_order_patterns(self) -> None:
        hints = self.service._requested_pattern_bridge_hints(
            question_type="sentence_order",
            pattern_id="problem_solution_case_blocks",
        )

        self.assertEqual(hints["preferred_business_card_ids"], ["sentence_order__discourse_logic__abstract"])
        self.assertEqual(
            hints["structure_constraints"],
            {
                "opening_anchor_type": "problem_opening",
                "middle_structure_type": "problem_solution_blocks",
                "closing_anchor_type": "case_support",
            },
        )

    def test_normalize_requested_pattern_id_drops_cross_type_stale_pattern(self) -> None:
        warnings: list[str] = []

        normalized = self.service._normalize_requested_pattern_id(
            question_type="sentence_order",
            pattern_id="ending_summary",
            warnings=warnings,
        )

        self.assertIsNone(normalized)
        self.assertEqual(len(warnings), 1)
        self.assertIn("Ignored stale pattern_id 'ending_summary'", warnings[0])

    def test_normalize_requested_pattern_id_keeps_valid_same_type_pattern(self) -> None:
        warnings: list[str] = []

        normalized = self.service._normalize_requested_pattern_id(
            question_type="sentence_fill",
            pattern_id="ending_summary",
            warnings=warnings,
        )

        self.assertEqual(normalized, "ending_summary")
        self.assertEqual(warnings, [])

    def test_build_prompt_request_from_snapshot_drops_stale_pattern_but_keeps_slots(self) -> None:
        build_request = self.service._build_prompt_request_from_snapshot(
            {
                "question_type": "sentence_order",
                "business_subtype": None,
                "pattern_id": "ending_summary",
                "difficulty_target": "medium",
                "topic": None,
                "passage_style": None,
                "use_fewshot": True,
                "fewshot_mode": "structure_only",
                "type_slots": {
                    "opening_anchor_type": "problem_opening",
                    "middle_structure_type": "problem_solution_blocks",
                    "closing_anchor_type": "case_support",
                },
                "extra_constraints": {
                    "requested_guard_lines": ["按问题-对策-案例板块推进。"],
                    "reference_business_cards": ["sentence_order__discourse_logic__abstract"],
                },
            }
        )

        self.assertIsNone(build_request.pattern_id)
        self.assertEqual(build_request.type_slots["opening_anchor_type"], "problem_opening")
        self.assertEqual(build_request.type_slots["middle_structure_type"], "problem_solution_blocks")
        self.assertEqual(build_request.type_slots["closing_anchor_type"], "case_support")

    def test_build_request_snapshot_keeps_explicit_pattern_only(self) -> None:
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "sentence_fill",
                "difficulty_level": "medium",
                "count": 1,
            }
        )

        snapshot = self.service._build_request_snapshot(
            request,
            {
                "question_type": "sentence_fill",
                "business_subtype": None,
                "pattern_id": None,
                "difficulty_target": "medium",
                "extra_constraints": {},
            },
            {"mapping_source": "focus", "selected_special_type": None},
            request_id="req-1",
            source_question_analysis={
                "topic": "topic-x",
                "business_card_ids": ["card-a"],
                "query_terms": ["term-a"],
                "style_summary": {"tone": "formal"},
                "structure_constraints": {"blank_position": "opening"},
            },
            question_card_binding={"question_card_id": "question.card"},
        )

        self.assertIsNone(snapshot["pattern_id"])
        self.assertEqual(snapshot["extra_constraints"], {"preference_profile": {}})
        self.assertEqual(snapshot["source_question_analysis"]["business_card_ids"], ["card-a"])

    def test_build_request_snapshot_stores_normalized_source_payloads(self) -> None:
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "sentence_fill",
                "difficulty_level": "medium",
                "count": 1,
                "topic": "<w:t>主题</w:t>",
                "source_question": {
                    "passage": "<w:p><w:t>第一段\\u00a0内容</w:t></w:p>",
                    "stem": "题干",
                    "options": {"A": "<w:t>选项A</w:t>", "B": "选项B"},
                },
                "user_material": {
                    "text": "<w:p><w:t>材料正文\\u00a0</w:t></w:p>",
                    "title": "<w:t>材料标题</w:t>",
                },
            }
        )

        snapshot = self.service._build_request_snapshot(
            request,
            {
                "question_type": "sentence_fill",
                "business_subtype": None,
                "pattern_id": None,
                "difficulty_target": "medium",
                "extra_constraints": {},
                "type_slots": {},
            },
            {"mapping_source": "focus", "selected_special_type": None},
            request_id="req-clean",
            source_question_analysis={
                "topic": "<w:t>主题</w:t>",
                "business_card_ids": ["card-a"],
                "query_terms": ["term-a"],
                "style_summary": {"tone": "formal"},
                "structure_constraints": {"blank_position": "opening"},
            },
            question_card_binding={"question_card_id": "question.card"},
        )

        self.assertEqual(snapshot["topic"], "主题")
        self.assertEqual(snapshot["source_question"]["passage"], "第一段 内容")
        self.assertEqual(snapshot["source_question"]["options"]["A"], "选项A")
        self.assertEqual(snapshot["user_material"]["title"], "材料标题")

    def test_build_request_snapshot_keeps_explicit_constraints_without_reference_sidechannel(self) -> None:
        request = QuestionGenerateRequest.model_validate(
            {
                "question_focus": "main_idea",
                "difficulty_level": "medium",
                "count": 1,
                "extra_constraints": {"validator_contract": {"main_idea": {"enforce_alignment": True}}},
                "source_question": {
                    "stem": "根据材料选择标题",
                    "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                },
            }
        )

        snapshot = self.service._build_request_snapshot(
            request,
            {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "pattern_id": "pattern.alpha",
                "difficulty_target": "medium",
                "extra_constraints": {},
            },
            {"mapping_source": "focus", "selected_special_type": None},
            request_id="req-2",
            source_question_analysis={
                "topic": "topic-x",
                "business_card_ids": ["card-a"],
                "query_terms": ["term-a"],
                "style_summary": {"tone": "formal"},
            },
            question_card_binding={"question_card_id": "question.card"},
        )

        self.assertEqual(
            snapshot["extra_constraints"],
            {
                "validator_contract": {"main_idea": {"enforce_alignment": True}},
                "preference_profile": {},
            },
        )
        self.assertEqual(snapshot["source_question_analysis"]["query_terms"], ["term-a"])

    def test_resolve_sentence_order_candidate_type_reads_question_card_runtime_spec(self) -> None:
        candidate_type = self.service._resolve_sentence_order_candidate_type(
            question_type="sentence_order",
            question_card_binding={
                "question_card_id": "question.sentence_order.standard_v1",
                "runtime_binding": {"question_type": "sentence_order", "business_subtype": None},
                "question_card": {"formal_runtime_spec": {"candidate_type": "sentence_block_group"}},
            },
            type_slots={"opening_anchor_type": "weak_opening", "closing_anchor_type": "none"},
        )

        self.assertEqual(candidate_type, "sentence_block_group")

    def test_hydrate_sentence_order_candidate_type_context_backfills_resolved_slots(self) -> None:
        built_item = {
            "question_type": "sentence_order",
            "resolved_slots": {
                "opening_anchor_type": "weak_opening",
                "closing_anchor_type": "none",
            },
            "request_snapshot": {
                "question_type": "sentence_order",
                "type_slots": {
                    "opening_anchor_type": "weak_opening",
                    "closing_anchor_type": "none",
                },
                "question_card_binding": {
                    "question_card_id": "question.sentence_order.standard_v1",
                    "question_card": {"formal_runtime_spec": {"candidate_type": "sentence_block_group"}},
                },
            },
            "notes": [],
        }

        self.service._hydrate_sentence_order_candidate_type_context(built_item)

        self.assertEqual(built_item["resolved_slots"]["candidate_type"], "sentence_block_group")
        self.assertEqual(built_item["request_snapshot"]["type_slots"]["candidate_type"], "sentence_block_group")
        self.assertIn("sentence_order_candidate_type_hydrated", built_item["notes"])

    def test_collect_answer_grounding_facts_for_center_understanding_reads_meaning_preserving_mode(self) -> None:
        facts = self.service._collect_answer_grounding_facts(
            question_type="main_idea",
            business_subtype="center_understanding",
            question_card_binding={
                "runtime_binding": {"question_type": "main_idea", "business_subtype": "center_understanding"},
                "question_card": {
                    "business_subtype_id": "center_understanding",
                    "compatibility_backbone": {"answer_grounding_asset_family_id": "title_selection"},
                    "answer_grounding": {
                        "require_material_traceability": True,
                        "require_central_meaning_alignment": True,
                        "disallow_detail_as_correct_answer": True,
                        "disallow_stronger_conclusion": True,
                        "expression_fidelity_mode": "meaning_preserving",
                        "allow_meaning_preserving_creation": True,
                        "allow_cross_sentence_abstraction": True,
                        "allow_exam_style_rephrasing": True,
                    },
                },
            },
        )

        self.assertEqual(facts["main_idea_subtype"], "center_understanding")
        self.assertEqual(facts["expression_fidelity_mode"], "meaning_preserving")
        self.assertTrue(facts["allow_meaning_preserving_creation"])
        self.assertTrue(facts["allow_cross_sentence_abstraction"])
        self.assertTrue(facts["allow_exam_style_rephrasing"])

    def test_build_answer_grounding_rules_for_center_understanding_without_reference_question(self) -> None:
        self.service.prompt_assets = {
            "answer_grounding": {
                "base": {
                    "require_material_traceability_template": "traceable",
                    "disallow_unsupported_extensions_template": "unsupported: {extension_types}",
                    "require_correct_option_material_defensibility_template": "defensible",
                    "distractor_sources_template": "distractors: {distractor_sources}",
                    "require_analysis_material_evidence_template": "analysis_evidence",
                    "align_with_reference_elimination_style_template": "align_reference",
                },
                "title_selection": {
                    "require_central_meaning_alignment_template": "central_meaning",
                    "disallow_detail_as_correct_answer_template": "no_detail",
                    "disallow_stronger_conclusion_template": "no_stronger_conclusion",
                },
                "main_idea": {
                    "source_strict_template": "source_strict",
                    "meaning_preserving_template": "meaning_preserving",
                    "allow_cross_sentence_abstraction_template": "cross_sentence_abstraction",
                    "allow_exam_style_rephrasing_template": "exam_style_rephrasing",
                    "allow_meaning_preserving_creation_template": "meaning_preserving_creation",
                },
            }
        }

        rules = self.service._build_answer_grounding_rules(
            question_type="main_idea",
            business_subtype="center_understanding",
            source_question={},
            question_card_binding={
                "runtime_binding": {"question_type": "main_idea", "business_subtype": "center_understanding"},
                "question_card": {
                    "business_subtype_id": "center_understanding",
                    "compatibility_backbone": {"answer_grounding_asset_family_id": "title_selection"},
                    "answer_grounding": {
                        "require_material_traceability": True,
                        "require_central_meaning_alignment": True,
                        "disallow_detail_as_correct_answer": True,
                        "disallow_stronger_conclusion": True,
                        "expression_fidelity_mode": "meaning_preserving",
                        "allow_meaning_preserving_creation": True,
                        "allow_cross_sentence_abstraction": True,
                        "allow_exam_style_rephrasing": True,
                    },
                },
            },
        )

        self.assertIn("traceable", rules)
        self.assertIn("central_meaning", rules)
        self.assertIn("meaning_preserving", rules)
        self.assertIn("cross_sentence_abstraction", rules)
        self.assertIn("exam_style_rephrasing", rules)
        self.assertIn("meaning_preserving_creation", rules)

    def test_generation_prompt_sections_include_center_understanding_grounding_without_reference_question(self) -> None:
        self.service.prompt_assets = {
            "section_labels": {
                "selected_material": "[Selected Material]",
                "original_material_evidence": "[Original Material Evidence]",
                "material_meta": "[Material Meta]",
                "material_readability_contract": "[Material Readability Contract]",
                "material_prompt_extras": "[Material Prompt Extras]",
                "answer_grounding_contract": "[Answer Grounding Contract]",
            },
            "material_readability_contract": ["readable"],
            "answer_grounding": {
                "base": {
                    "require_material_traceability_template": "traceable",
                    "disallow_unsupported_extensions_template": "unsupported: {extension_types}",
                    "require_correct_option_material_defensibility_template": "defensible",
                    "distractor_sources_template": "distractors: {distractor_sources}",
                    "require_analysis_material_evidence_template": "analysis_evidence",
                    "align_with_reference_elimination_style_template": "align_reference",
                },
                "title_selection": {
                    "require_central_meaning_alignment_template": "central_meaning",
                    "disallow_detail_as_correct_answer_template": "no_detail",
                    "disallow_stronger_conclusion_template": "no_stronger_conclusion",
                },
                "main_idea": {
                    "source_strict_template": "source_strict",
                    "meaning_preserving_template": "meaning_preserving",
                    "allow_cross_sentence_abstraction_template": "cross_sentence_abstraction",
                    "allow_exam_style_rephrasing_template": "exam_style_rephrasing",
                    "allow_meaning_preserving_creation_template": "meaning_preserving_creation",
                },
            },
            "final_generation_instruction": "final_instruction",
        }
        self.service._resolve_section_question_card_binding = Mock(
            return_value={
                "runtime_binding": {"question_type": "main_idea", "business_subtype": "center_understanding"},
                "question_card": {
                    "business_subtype_id": "center_understanding",
                    "compatibility_backbone": {"answer_grounding_asset_family_id": "title_selection"},
                    "answer_grounding": {
                        "require_material_traceability": True,
                        "require_central_meaning_alignment": True,
                        "disallow_detail_as_correct_answer": True,
                        "disallow_stronger_conclusion": True,
                        "expression_fidelity_mode": "meaning_preserving",
                        "allow_meaning_preserving_creation": True,
                        "allow_cross_sentence_abstraction": True,
                        "allow_exam_style_rephrasing": True,
                    },
                },
            }
        )

        sections = self.service._build_generation_prompt_sections(
            built_item={
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "request_snapshot": {},
            },
            material=types.SimpleNamespace(
                text="material text",
                original_text="material text",
                material_id="mat-1",
                article_id="art-1",
                selection_reason="selected",
                source={},
            ),
            prompt_package={"user_prompt": "generate"},
            feedback_notes=[],
        )

        self.assertIn("[Answer Grounding Contract]", sections)
        self.assertIn("meaning_preserving", sections)
        self.assertIn("final_instruction", sections)

    def test_build_answer_grounding_rules_for_title_selection_stays_source_strict(self) -> None:
        self.service.prompt_assets = {
            "answer_grounding": {
                "base": {
                    "require_material_traceability_template": "traceable",
                    "disallow_unsupported_extensions_template": "unsupported: {extension_types}",
                    "require_correct_option_material_defensibility_template": "defensible",
                    "distractor_sources_template": "distractors: {distractor_sources}",
                    "require_analysis_material_evidence_template": "analysis_evidence",
                    "align_with_reference_elimination_style_template": "align_reference",
                },
                "title_selection": {
                    "require_central_meaning_alignment_template": "central_meaning",
                    "disallow_detail_as_correct_answer_template": "no_detail",
                    "disallow_stronger_conclusion_template": "no_stronger_conclusion",
                },
                "main_idea": {
                    "source_strict_template": "source_strict",
                    "meaning_preserving_template": "meaning_preserving",
                    "allow_cross_sentence_abstraction_template": "cross_sentence_abstraction",
                    "allow_exam_style_rephrasing_template": "exam_style_rephrasing",
                    "allow_meaning_preserving_creation_template": "meaning_preserving_creation",
                },
            }
        }

        rules = self.service._build_answer_grounding_rules(
            question_type="main_idea",
            business_subtype="title_selection",
            source_question={},
            question_card_binding={
                "runtime_binding": {"question_type": "main_idea", "business_subtype": "title_selection"},
                "question_card": {
                    "business_subtype_id": "title_selection",
                    "answer_grounding": {
                        "require_material_traceability": True,
                        "require_central_meaning_alignment": True,
                        "disallow_detail_as_correct_answer": True,
                        "disallow_stronger_conclusion": True,
                        "expression_fidelity_mode": "source_strict",
                    },
                },
            },
        )

        self.assertIn("source_strict", rules)
        self.assertNotIn("meaning_preserving", rules)

    def test_effective_difficulty_target_does_not_raise_for_reference_question(self) -> None:
        self.assertEqual(
            self.service._effective_difficulty_target("easy", use_reference_question=True),
            "easy",
        )

    def test_generation_source_has_single_active_definition_for_override_cleanup(self) -> None:
        file_path = Path("C:/Users/Maru/Documents/agent/prompt_skeleton_service/app/services/question_generation.py")
        text = file_path.read_text(encoding="utf-8")

        self.assertEqual(text.count("def _build_reference_hard_constraints("), 1)
        self.assertEqual(text.count("def _legacy_reference_hard_constraint_residuals("), 1)
        self.assertEqual(text.count("def _refine_material_if_needed("), 1)
        self.assertEqual(text.count("def _prepare_question_service_material("), 1)
        self.assertEqual(text.count("def _clean_material_text("), 1)
        self.assertEqual(text.count("def _strip_material_template_labels("), 1)
        self.assertEqual(text.count("def _needs_material_refinement("), 1)

    def test_prepare_question_service_material_builds_sentence_fill_ready_view(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-1",
            article_id="a-1",
            text="第一句交代背景。第二句承接说明。第三句收束观点。",
            original_text="第一句交代背景。第二句承接说明。第三句收束观点。",
            source={"source_name": "src", "source_id": "src", "article_title": "title"},
            document_genre="news",
            selection_reason="test",
        )

        prepared = self.service._prepare_question_service_material(
            material=material,
            question_type="sentence_fill",
            request_snapshot={
                "type_slots": {"blank_position": "ending", "function_type": "summary", "logic_relation": "summary"},
                "source_question_analysis": {"structure_constraints": {}},
            },
        )

        self.assertIn("____", prepared.text)
        self.assertEqual(prepared.text.count("____"), 1)
        self.assertIn("第三句收束观点。", prepared.original_text or "")
        prompt_extras = (prepared.source or {}).get("prompt_extras") or {}
        self.assertEqual(prompt_extras.get("blank_position"), "ending")
        self.assertEqual(prompt_extras.get("preferred_answer_shape"), "closing_summary")
        self.assertIn("____", prompt_extras.get("fill_ready_local_material", ""))

    def test_prepare_question_service_material_keeps_legal_opening_slot(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-2",
            article_id="a-2",
            text="“活到老学到老”是他的座右铭。第二句介绍人物经历。第三句展开事迹。",
            original_text="“活到老学到老”是他的座右铭。第二句介绍人物经历。第三句展开事迹。",
            source={"source_name": "src", "source_id": "src", "article_title": "title"},
            document_genre="feature",
            selection_reason="test",
        )

        prepared = self.service._prepare_question_service_material(
            material=material,
            question_type="sentence_fill",
            request_snapshot={
                "type_slots": {"blank_position": "opening", "function_type": "summary", "logic_relation": "summary"},
                "source_question_analysis": {"structure_constraints": {}},
            },
        )

        self.assertTrue(prepared.text.startswith("____"))
        self.assertIn("“活到老学到老”是他的座右铭。", prepared.original_text or "")

    def test_enforce_sentence_fill_original_answer_replaces_non_original_correct_option(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-3",
            article_id="a-3",
            text="前文。____。后文。",
            original_text="前文。原句答案。后文。",
            source={
                "prompt_extras": {
                    "require_original_answer_sentence": True,
                    "answer_anchor_text": "原句答案。",
                }
            },
            document_genre="news",
            selection_reason="test",
        )
        generated = GeneratedQuestion(
            question_type="sentence_fill",
            stem="填入画横线部分最恰当的一句是（    ）。",
            options={"A": "改写答案。", "B": "干扰1。", "C": "干扰2。", "D": "干扰3。"},
            answer="A",
            analysis="A项衔接最自然。",
            metadata={},
        )

        enforced = self.service._enforce_sentence_fill_original_answer(
            generated_question=generated,
            material=material,
        )

        self.assertEqual(enforced.options["A"], "原句答案。")
        self.assertTrue(enforced.metadata.get("sentence_fill_original_answer_enforced"))

    def test_enforce_sentence_fill_original_answer_keeps_dominant_answer_if_already_original(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-4",
            article_id="a-4",
            text="前文。____。后文。",
            original_text="前文。原句答案。后文。",
            source={
                "prompt_extras": {
                    "require_original_answer_sentence": True,
                    "answer_anchor_text": "原句答案。",
                }
            },
            document_genre="news",
            selection_reason="test",
        )
        generated = GeneratedQuestion(
            question_type="sentence_fill",
            stem="填入画横线部分最恰当的一句是（    ）。",
            options={"A": "原句答案。", "B": "干扰1。", "C": "干扰2。", "D": "干扰3。"},
            answer="A",
            analysis="A项衔接最自然。",
            metadata={},
        )

        enforced = self.service._enforce_sentence_fill_original_answer(
            generated_question=generated,
            material=material,
        )

        self.assertEqual(enforced.options["A"], "原句答案。")
        self.assertFalse(enforced.metadata.get("sentence_fill_original_answer_enforced", False))

    def test_clean_material_text_strips_caption_noise(self) -> None:
        cleaned = self.service._clean_material_text(
            "新华社记者张三摄\n\n第一段内容。（审核：李四）\n\n第二段内容。"
        )

        self.assertNotIn("新华社记者", cleaned)
        self.assertNotIn("审核", cleaned)
        self.assertIn("第一段内容。", cleaned)
        self.assertIn("第二段内容。", cleaned)

    def test_prepare_question_service_material_builds_sentence_order_ready_contract(self) -> None:
        material = MaterialSelectionResult(
            material_id="m-so",
            article_id="a-so",
            text="第一句交代背景。第二句承接解释。第三句继续推进。第四句补充条件。第五句转入结果。第六句总结收束。",
            original_text="第一句交代背景。第二句承接解释。第三句继续推进。第四句补充条件。第五句转入结果。第六句总结收束。",
            source={"source_name": "src", "source_id": "src", "article_title": "title"},
            document_genre="analysis",
            selection_reason="test",
        )

        prepared = self.service._prepare_question_service_material(
            material=material,
            question_type="sentence_order",
            request_snapshot={"source_question_analysis": {"structure_constraints": {"sortable_unit_count": 6}}},
        )

        prompt_extras = (prepared.source or {}).get("prompt_extras") or {}
        self.assertEqual(prompt_extras.get("sortable_unit_count"), 6)
        self.assertEqual(len(prompt_extras.get("sortable_units") or []), 6)
        self.assertIn("sentence_order", prepared.validator_contract or {})

    def test_prepare_question_service_material_can_polish_sentence_order_presentation(self) -> None:
        self.service.llm_gateway.generate_json.return_value = {
            "refined_text": (
                "围绕城乡融合这一主题，第一句交代背景。第二句承接解释。第三句继续推进。"
                "第四句补充条件。第五句转入结果。第六句总结收束。"
            ),
            "changed": True,
            "reason": "sentence_order_presentation_polish",
        }
        material = MaterialSelectionResult(
            material_id="m-so-list",
            article_id="a-so-list",
            text="1. 第一句交代背景。\n2. 第二句承接解释。\n3. 第三句继续推进。\n4. 第四句补充条件。\n5. 第五句转入结果。\n6. 第六句总结收束。",
            original_text="1. 第一句交代背景。\n2. 第二句承接解释。\n3. 第三句继续推进。\n4. 第四句补充条件。\n5. 第五句转入结果。\n6. 第六句总结收束。",
            source={"source_name": "src", "source_id": "src", "article_title": "title"},
            document_genre="analysis",
            selection_reason="test",
        )

        prepared = self.service._prepare_question_service_material(
            material=material,
            question_type="sentence_order",
            request_snapshot={"source_question_analysis": {"structure_constraints": {"sortable_unit_count": 6}}},
        )

        prompt_extras = (prepared.source or {}).get("prompt_extras") or {}
        self.assertTrue(prompt_extras.get("sentence_order_presentation_refined"))
        self.assertEqual(
            prepared.text,
            "围绕城乡融合这一主题,第一句交代背景。第二句承接解释。第三句继续推进。第四句补充条件。第五句转入结果。第六句总结收束。",
        )
        self.assertEqual(len(self.service._extract_sortable_units_from_text(prepared.text)), 6)

    def test_sentence_order_presentation_refinement_rejects_reordered_units(self) -> None:
        sortable_units = [
            "第一句交代背景。",
            "第二句承接解释。",
            "第三句继续推进。",
            "第四句补充条件。",
            "第五句转入结果。",
            "第六句总结收束。",
        ]
        current_text = self.service._format_sentence_order_natural_material(
            raw_text="\n".join(f"{index}. {unit}" for index, unit in enumerate(sortable_units, start=1)),
            sortable_units=sortable_units,
        )
        self.service.llm_gateway.generate_json.return_value = {
            "refined_text": (
                "第二句承接解释。第一句交代背景。第三句继续推进。"
                "第四句补充条件。第五句转入结果。第六句总结收束。"
            ),
            "changed": True,
            "reason": "bad_reorder",
        }

        refined_text, metadata = self.service._refine_sentence_order_presentation_material(
            raw_text="\n".join(f"{index}. {unit}" for index, unit in enumerate(sortable_units, start=1)),
            current_text=current_text,
            sortable_units=sortable_units,
            source={},
        )

        self.assertEqual(refined_text, current_text)
        self.assertEqual(metadata, {})

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
        self.assertEqual(
            self.service.material_bridge.list_material_options.call_args.kwargs["business_card_ids"],
            [],
        )

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
        self.assertEqual(
            self.service.material_bridge.list_material_options.call_args.kwargs["business_card_ids"],
            [],
        )

    def test_weak_main_idea_reference_signal_is_detected(self) -> None:
        analysis = {
            "style_summary": {"question_type": "main_idea"},
            "business_card_ids": ["theme_word_focus__main_idea"],
            "query_terms": [],
            "business_card_scores": [{"card_id": "theme_word_focus__main_idea", "score": 0.41}],
        }

        self.assertTrue(self.service._is_weak_main_idea_reference_signal(analysis))

    def test_weak_main_idea_reference_signal_disables_alignment_retry(self) -> None:
        validation_result = types.SimpleNamespace(
            passed=False,
            checks={},
        )
        analysis = {
            "style_summary": {"question_type": "main_idea"},
            "business_card_ids": ["theme_word_focus__main_idea"],
            "query_terms": [],
            "business_card_scores": [{"card_id": "theme_word_focus__main_idea", "score": 0.41}],
        }

        self.assertFalse(self.service._should_retry_alignment(validation_result, analysis))

    def test_weak_main_idea_reference_signal_disables_quality_repair_retry(self) -> None:
        validation_result = types.SimpleNamespace(
            passed=False,
            errors=[],
            checks={},
        )
        analysis = {
            "style_summary": {"question_type": "main_idea"},
            "business_card_ids": ["theme_word_focus__main_idea"],
            "query_terms": [],
            "business_card_scores": [{"card_id": "theme_word_focus__main_idea", "score": 0.41}],
        }

        self.assertFalse(
            self.service._should_retry_quality_repair(
                validation_result=validation_result,
                quality_gate_errors=["quality_gate_failed"],
                evaluation_result={"overall_score": 55},
                source_question_analysis=analysis,
            )
        )

    def test_main_idea_axis_errors_enable_targeted_quality_repair_even_on_compact_path(self) -> None:
        validation_result = types.SimpleNamespace(
            passed=False,
            errors=["main_axis_mismatch", "abstraction_level_mismatch"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": False}},
        )
        analysis = {
            "style_summary": {"question_type": "main_idea"},
            "business_card_ids": ["turning_relation_focus__main_idea"],
            "query_terms": ["战略物资"],
            "business_card_scores": [{"card_id": "turning_relation_focus__main_idea", "score": 0.78}],
        }

        self.assertTrue(
            self.service._should_retry_quality_repair(
                validation_result=validation_result,
                quality_gate_errors=[],
                evaluation_result={"overall_score": 55},
                source_question_analysis=analysis,
            )
        )

    def test_build_targeted_repair_plan_for_main_idea_locks_scope(self) -> None:
        validation_result = types.SimpleNamespace(
            errors=["main_axis_mismatch", "abstraction_level_mismatch"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": False}},
        )

        plan = self.service._build_targeted_repair_plan(
            question_type="main_idea",
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis={"style_summary": {"question_type": "main_idea"}},
        )

        self.assertEqual(plan["mode"], "main_idea_axis_repair")
        self.assertEqual(plan["allowed_fields"], ["options", "answer", "analysis"])
        self.assertIn("stem", plan["locked_fields"])
        self.assertIn("argument_structure_mismatch", plan["target_errors"])
        self.assertIn("local_point_as_main_axis", plan["target_errors"])
        self.assertIn("example_promoted_to_main_idea", plan["target_errors"])

    def test_analysis_missing_triggers_analysis_only_repair(self) -> None:
        validation_result = types.SimpleNamespace(
            errors=["analysis must not be empty."],
            warnings=[],
            checks={
                "analysis_present": {"passed": False},
                "analysis_mentions_correct_option_text": {"passed": False},
                "answer_in_options": {"passed": True},
            },
        )

        plan = self.service._build_targeted_repair_plan(
            question_type="main_idea",
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis={"style_summary": {"question_type": "main_idea"}},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["mode"], "analysis_only_repair")
        self.assertEqual(plan["allowed_fields"], ["analysis"])

    def test_analysis_answer_consistency_prefers_analysis_only_repair(self) -> None:
        validation_result = types.SimpleNamespace(
            errors=[],
            warnings=[],
            checks={
                "analysis_answer_consistency": {"passed": False},
                "analysis_mentions_correct_option_text": {"passed": True},
                "answer_in_options": {"passed": True},
                "reference_answer_grounding": {"passed": True},
            },
        )

        plan = self.service._build_targeted_repair_plan(
            question_type="main_idea",
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis={"style_summary": {"question_type": "main_idea"}},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["mode"], "analysis_only_repair")

    def test_analysis_answer_consistency_prefers_answer_binding_repair(self) -> None:
        validation_result = types.SimpleNamespace(
            errors=[],
            warnings=[],
            checks={
                "analysis_answer_consistency": {"passed": False},
                "analysis_mentions_correct_option_text": {"passed": False},
                "answer_in_options": {"passed": True},
                "reference_answer_grounding": {"passed": False},
            },
        )

        plan = self.service._build_targeted_repair_plan(
            question_type="main_idea",
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis={"style_summary": {"question_type": "main_idea"}},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["mode"], "main_idea_axis_repair")

    def test_analysis_answer_consistency_ambiguous_returns_none(self) -> None:
        validation_result = types.SimpleNamespace(
            errors=[],
            warnings=[],
            checks={
                "analysis_answer_consistency": {"passed": False},
                "analysis_mentions_correct_option_text": {"passed": False},
                "answer_in_options": {"passed": True},
                "reference_answer_grounding": {"passed": True},
            },
        )

        plan = self.service._build_targeted_repair_plan(
            question_type="main_idea",
            business_subtype="center_understanding",
            validation_result=validation_result,
            quality_gate_errors=[],
            source_question_analysis={"style_summary": {"question_type": "main_idea"}},
        )

        self.assertIsNone(plan)

    def test_merge_repaired_question_with_scope_only_updates_allowed_fields(self) -> None:
        current_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="whole_passage_integration",
            stem="这段文字意在说明：",
            options={"A": "旧A", "B": "旧B", "C": "旧C", "D": "旧D"},
            answer="A",
            analysis="旧解析",
            metadata={"v": 1},
        )
        repaired_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="whole_passage_integration",
            stem="被错误改动的题干",
            options={"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
            answer="C",
            analysis="新解析",
            metadata={"v": 2},
        )

        merged = self.service._merge_repaired_question_with_scope(
            current_question=current_question,
            repaired_question=repaired_question,
            repair_plan={"mode": "main_idea_axis_repair", "allowed_fields": ["options", "answer", "analysis"]},
        )

        self.assertEqual(merged.stem, "这段文字意在说明：")
        self.assertEqual(merged.options["A"], "新A")
        self.assertEqual(merged.answer, "C")
        self.assertEqual(merged.analysis, "新解析")

    def test_should_accept_targeted_repair_requires_target_failure_improvement(self) -> None:
        current_validation = types.SimpleNamespace(
            passed=False,
            errors=["main_axis_mismatch", "abstraction_level_mismatch"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": False}},
        )
        repaired_validation = types.SimpleNamespace(
            passed=False,
            errors=["abstraction_level_mismatch"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": True}},
        )

        accepted = self.service._should_accept_targeted_repair(
            repair_plan={
                "target_errors": ["main_axis_mismatch", "abstraction_level_mismatch"],
                "target_checks": ["analysis_mentions_correct_option_text"],
            },
            current_validation_result=current_validation,
            current_evaluation_result={"overall_score": 42},
            repaired_validation_result=repaired_validation,
            repaired_evaluation_result={"overall_score": 44},
            repaired_quality_gate_errors=[],
        )

        self.assertTrue(accepted)

    def test_material_bridge_hints_relax_weak_fill_analysis(self) -> None:
        analysis = {
            "analysis_confidence": 0.42,
            "risk_flags": ["fill_function_drift"],
            "business_card_ids": ["sentence_fill__middle_bridge_both_sides__abstract"],
            "query_terms": ["转折", "衔接", "句子"],
            "structure_constraints": {
                "blank_position": "middle",
                "function_type": "bridge_both_sides",
                "unit_type": "sentence",
                "preserve_blank_position": True,
            },
            "retrieval_business_card_ids": [],
            "retrieval_preferred_business_card_ids": [],
            "retrieval_query_terms": ["转折", "衔接"],
            "retrieval_structure_constraints": {
                "blank_position": "middle",
                "unit_type": "sentence",
                "preserve_blank_position": True,
            },
        }

        hints = self.service._material_bridge_hints(analysis)

        self.assertEqual(hints["business_card_ids"], [])
        self.assertEqual(hints["preferred_business_card_ids"], [])
        self.assertEqual(hints["query_terms"], ["转折", "衔接"])
        self.assertEqual(
            hints["structure_constraints"],
            {"blank_position": "middle", "unit_type": "sentence", "preserve_blank_position": True},
        )

    def test_material_bridge_hints_normalize_legacy_sentence_fill_structure_constraints(self) -> None:
        hints = self.service._material_bridge_hints(
            {
                "retrieval_structure_constraints": {
                    "blank_position": "middle",
                    "function_type": "bridge_both_sides",
                    "logic_relation": "continuation_or_transition",
                }
            }
        )

        self.assertEqual(
            hints["structure_constraints"],
            {
                "blank_position": "middle",
                "function_type": "bridge",
                "logic_relation": "continuation",
            },
        )

    def test_requested_sentence_fill_pattern_hints_use_canonical_constraints_not_id_text(self) -> None:
        hints = self.service._requested_pattern_bridge_hints(
            question_type="sentence_fill",
            pattern_id="bridge_transition",
        )

        self.assertEqual(
            hints["preferred_business_card_ids"],
            ["sentence_fill__middle_bridge_both_sides__abstract"],
        )
        self.assertEqual(
            hints["structure_constraints"],
            {
                "blank_position": "middle",
                "function_type": "bridge",
                "logic_relation": "continuation",
            },
        )

    def test_reference_generation_context_normalizes_legacy_sentence_fill_constraints(self) -> None:
        self.service._prepare_reference_prompt_payload = lambda payload: payload  # type: ignore[method-assign]
        self.service._build_reference_hard_constraints = lambda **kwargs: kwargs["structure_constraints"]  # type: ignore[method-assign]

        context = self.service._build_reference_generation_context(
            question_type="sentence_fill",
            source_question={"stem": "demo"},
            source_question_analysis={
                "structure_constraints": {
                    "blank_position": "middle",
                    "function_type": "bridge_both_sides",
                    "logic_relation": "continuation_or_transition",
                }
            },
        )

        self.assertEqual(
            context["source_question_analysis"]["structure_constraints"],
            {
                "blank_position": "middle",
                "function_type": "bridge",
                "logic_relation": "continuation",
            },
        )

    def test_revise_text_modify_preserves_question_card_id(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._material_bridge_hints = Mock(return_value={})
        self.service._requested_taxonomy_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._requested_pattern_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._merge_material_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "question_card_id": "question.title_selection.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {"target_length": 220, "length_tolerance": 90},
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
            ["turning_relation_focus__main_idea"],
        )
        self.assertEqual(
            self.service.material_bridge.select_materials.call_args.kwargs["business_card_ids"],
            ["turning_relation_focus__main_idea"],
        )

    def test_revise_text_modify_requested_material_reuses_replacement_filters(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._material_bridge_hints = Mock(return_value={})
        self.service._requested_taxonomy_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._requested_pattern_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._merge_material_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "question_card_id": "question.center_understanding.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {"target_length": 220, "length_tolerance": 90},
                "extra_constraints": {},
                "type_slots": {"structure_type": "turning"},
                "material_structure": None,
                "topic": None,
                "material_policy": None,
                "preference_profile": {"quality": 0.2},
                "pattern_id": None,
            }
        )
        self.service._material_policy_from_snapshot = Mock(return_value=None)
        self.service._clean_material_text = Mock(return_value="")
        selected_material = Mock()
        selected_material.material_id = "mat-2"
        self.service.material_bridge.list_material_options.return_value = [selected_material]
        self.service._refine_material_if_needed = Mock(return_value=selected_material)
        self.service._annotate_material_usage = Mock(return_value=selected_material)
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

        self.service.revise_text_modify(item, instruction=None, control_overrides={"material_id": "mat-2"})

        kwargs = self.service.material_bridge.list_material_options.call_args.kwargs
        self.assertEqual(kwargs["question_card_id"], "question.center_understanding.standard_v1")
        self.assertEqual(kwargs["business_card_ids"], ["turning_relation_focus__main_idea"])
        self.assertEqual(kwargs["preferred_business_card_ids"], ["turning_relation_focus__main_idea"])
        self.assertEqual(kwargs["query_terms"], ["数字遗产"])
        self.assertEqual(kwargs["structure_constraints"], {"structure_type": "turning"})
        self.assertEqual(kwargs["target_length"], 220)
        self.assertEqual(kwargs["length_tolerance"], 90)
        self.assertEqual(kwargs["exclude_material_ids"], {"mat-1"})

    def test_revise_question_modify_reprepares_sentence_fill_material_before_rebuild(self) -> None:
        self.service.repository = Mock()
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "sentence_fill",
                "business_subtype": None,
                "difficulty_target": "medium",
                "type_slots": {"blank_position": "ending", "function_type": "conclusion", "logic_relation": "summary"},
                "extra_constraints": {},
            }
        )
        current_material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="article-1",
            text="当前已经被处理过的材料",
            original_text="原始未挖空材料",
            source={"prompt_extras": {"fill_ready_material": "当前已经被处理过的材料"}},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = current_material.model_copy(update={"text": "重新挖空后的材料"})
        self.service._annotate_material_usage = Mock(return_value=current_material)
        self.service._refine_material_if_needed = Mock(return_value=current_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
        self.service._build_generated_item = Mock(return_value={"item_id": "item-1"})
        self.service.runtime_config = types.SimpleNamespace(
            llm=types.SimpleNamespace(
                routing=types.SimpleNamespace(
                    review_actions=types.SimpleNamespace(question_modify="review-actions.question_modify")
                )
            )
        )
        item = {
            "item_id": "item-1",
            "batch_id": "batch-1",
            "revision_count": 0,
            "request_snapshot": {},
            "material_selection": current_material.model_dump(),
        }

        self.service.revise_question_modify(item, instruction=None, control_overrides={"type_slots": {"blank_position": "ending"}})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "重新挖空后的材料",
        )

    def test_revise_text_modify_reprepares_sentence_fill_material_before_rebuild(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._material_bridge_hints = Mock(return_value={})
        self.service._requested_taxonomy_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._requested_pattern_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._merge_material_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "sentence_fill",
                "business_subtype": None,
                "question_card_id": "question.sentence_fill.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {},
                "extra_constraints": {},
                "type_slots": {"blank_position": "ending", "function_type": "conclusion", "logic_relation": "summary"},
                "material_structure": None,
                "topic": None,
                "material_policy": None,
                "pattern_id": "ending_summary",
            }
        )
        self.service._material_policy_from_snapshot = Mock(return_value=None)
        self.service._clean_material_text = Mock(return_value="")
        selected_material = MaterialSelectionResult(
            material_id="mat-2",
            article_id="article-2",
            text="完整原文材料",
            original_text="完整原文材料",
            source={},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = selected_material.model_copy(update={"text": "____ 挖空后的材料"})
        self.service.material_bridge.list_material_options.return_value = [selected_material]
        self.service._annotate_material_usage = Mock(return_value=selected_material)
        self.service._refine_material_if_needed = Mock(return_value=selected_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
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

        self.service.revise_text_modify(item, instruction=None, control_overrides={"material_id": "mat-2"})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "____ 挖空后的材料",
        )

    def test_revise_question_modify_reprepares_sentence_order_material_before_rebuild(self) -> None:
        self.service.repository = Mock()
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "sentence_order",
                "business_subtype": None,
                "difficulty_target": "medium",
                "type_slots": {
                    "opening_anchor_type": "problem_opening",
                    "middle_structure_type": "problem_solution_blocks",
                    "closing_anchor_type": "case_support",
                },
                "extra_constraints": {},
            }
        )
        current_material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="article-1",
            text="上一轮处理过的排序展示材料",
            original_text="第一句。第二句。第三句。第四句。第五句。第六句。",
            source={"prompt_extras": {"sortable_units": ["上一轮", "处理过", "的", "排序", "展示", "材料"]}},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = current_material.model_copy(update={"text": "① 第一组\n② 第二组\n③ 第三组\n④ 第四组\n⑤ 第五组\n⑥ 第六组"})
        self.service._annotate_material_usage = Mock(return_value=current_material)
        self.service._refine_material_if_needed = Mock(return_value=current_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
        self.service._build_generated_item = Mock(return_value={"item_id": "item-1"})
        self.service.runtime_config = types.SimpleNamespace(
            llm=types.SimpleNamespace(
                routing=types.SimpleNamespace(
                    review_actions=types.SimpleNamespace(question_modify="review-actions.question_modify")
                )
            )
        )
        item = {
            "item_id": "item-1",
            "batch_id": "batch-1",
            "revision_count": 0,
            "request_snapshot": {},
            "material_selection": current_material.model_dump(),
        }

        self.service.revise_question_modify(item, instruction=None, control_overrides={"type_slots": {"opening_anchor_type": "problem_opening"}})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "① 第一组\n② 第二组\n③ 第三组\n④ 第四组\n⑤ 第五组\n⑥ 第六组",
        )

    def test_revise_text_modify_reprepares_sentence_order_material_before_rebuild(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._material_bridge_hints = Mock(return_value={})
        self.service._requested_taxonomy_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {"sortable_unit_count": 6},
            }
        )
        self.service._requested_pattern_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._merge_material_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {"sortable_unit_count": 6},
            }
        )
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "sentence_order",
                "business_subtype": None,
                "question_card_id": "question.sentence_order.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {},
                "extra_constraints": {},
                "type_slots": {"opening_anchor_type": "problem_opening"},
                "material_structure": None,
                "topic": None,
                "material_policy": None,
                "pattern_id": "problem_solution_case_blocks",
            }
        )
        self.service._material_policy_from_snapshot = Mock(return_value=None)
        self.service._clean_material_text = Mock(return_value="")
        selected_material = MaterialSelectionResult(
            material_id="mat-2",
            article_id="article-2",
            text="第一句。第二句。第三句。第四句。第五句。第六句。",
            original_text="第一句。第二句。第三句。第四句。第五句。第六句。",
            source={},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = selected_material.model_copy(update={"text": "① 第一句\n② 第二句\n③ 第三句\n④ 第四句\n⑤ 第五句\n⑥ 第六句"})
        self.service.material_bridge.list_material_options.return_value = [selected_material]
        self.service._annotate_material_usage = Mock(return_value=selected_material)
        self.service._refine_material_if_needed = Mock(return_value=selected_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
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

        self.service.revise_text_modify(item, instruction=None, control_overrides={"material_id": "mat-2"})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "① 第一句\n② 第二句\n③ 第三句\n④ 第四句\n⑤ 第五句\n⑥ 第六句",
        )

    def test_revise_question_modify_reprepares_center_understanding_material_before_rebuild(self) -> None:
        self.service.repository = Mock()
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "difficulty_target": "medium",
                "type_slots": {"structure_type": "turning", "main_axis_source": "transition_after"},
                "extra_constraints": {},
            }
        )
        current_material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="article-1",
            text="上一轮处理过的主旨材料",
            original_text="原始主旨材料",
            source={},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = current_material.model_copy(update={"text": "重新清洗后的主旨材料"})
        self.service._annotate_material_usage = Mock(return_value=current_material)
        self.service._refine_material_if_needed = Mock(return_value=current_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
        self.service._build_generated_item = Mock(return_value={"item_id": "item-1"})
        self.service.runtime_config = types.SimpleNamespace(
            llm=types.SimpleNamespace(
                routing=types.SimpleNamespace(
                    review_actions=types.SimpleNamespace(question_modify="review-actions.question_modify")
                )
            )
        )
        item = {
            "item_id": "item-1",
            "batch_id": "batch-1",
            "revision_count": 0,
            "request_snapshot": {},
            "material_selection": current_material.model_dump(),
        }

        self.service.revise_question_modify(item, instruction=None, control_overrides={"type_slots": {"structure_type": "turning"}})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "重新清洗后的主旨材料",
        )

    def test_revise_text_modify_reprepares_center_understanding_material_before_rebuild(self) -> None:
        self.service.material_bridge = Mock()
        self.service.repository = Mock()
        self.service.repository.get_material_usage_stats = Mock()
        self.service._material_bridge_hints = Mock(return_value={})
        self.service._requested_taxonomy_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._requested_pattern_bridge_hints = Mock(
            return_value={
                "business_card_ids": [],
                "preferred_business_card_ids": [],
                "query_terms": [],
                "structure_constraints": {},
            }
        )
        self.service._merge_material_bridge_hints = Mock(
            return_value={
                "business_card_ids": ["turning_relation_focus__main_idea"],
                "preferred_business_card_ids": ["turning_relation_focus__main_idea"],
                "query_terms": ["数字遗产"],
                "structure_constraints": {"structure_type": "turning"},
            }
        )
        self.service._apply_control_overrides = Mock(
            return_value={
                "question_type": "main_idea",
                "business_subtype": "center_understanding",
                "question_card_id": "question.center_understanding.standard_v1",
                "difficulty_target": "medium",
                "source_question_analysis": {"target_length": 220, "length_tolerance": 90},
                "extra_constraints": {},
                "type_slots": {"structure_type": "turning"},
                "material_structure": None,
                "topic": None,
                "material_policy": None,
                "pattern_id": None,
            }
        )
        self.service._material_policy_from_snapshot = Mock(return_value=None)
        self.service._clean_material_text = Mock(return_value="")
        selected_material = MaterialSelectionResult(
            material_id="mat-2",
            article_id="article-2",
            text="原始主旨材料全文",
            original_text="原始主旨材料全文",
            source={},
            document_genre="commentary",
            selection_reason="test",
        )
        prepared_material = selected_material.model_copy(update={"text": "重新清洗后的主旨材料全文"})
        self.service.material_bridge.list_material_options.return_value = [selected_material]
        self.service._annotate_material_usage = Mock(return_value=selected_material)
        self.service._refine_material_if_needed = Mock(return_value=selected_material)
        self.service._prepare_question_service_material = Mock(return_value=prepared_material)
        self.service._build_prompt_request_from_snapshot = Mock(return_value=SimpleNamespace())
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

        self.service.revise_text_modify(item, instruction=None, control_overrides={"material_id": "mat-2"})

        self.service._prepare_question_service_material.assert_called_once()
        self.assertEqual(
            self.service._build_generated_item.call_args.kwargs["material"].text,
            "重新清洗后的主旨材料全文",
        )

    def test_build_round1_fewshot_sections_returns_empty_for_missing_block(self) -> None:
        self.service.prompt_assets = {
            "section_labels": {
                "round1_fewshot_asset": "[Round 1 Few-shot Asset]",
            }
        }

        sections = self.service._build_round1_fewshot_sections(prompt_package={})

        self.assertEqual(sections, [])

    def test_build_round1_fewshot_sections_includes_configured_section(self) -> None:
        self.service.prompt_assets = {
            "section_labels": {
                "round1_fewshot_asset": "[Round 1 Few-shot Asset]",
            }
        }

        sections = self.service._build_round1_fewshot_sections(
            prompt_package={"fewshot_text_block": "Few-shot 1: sample-a\ncanonical_view=x"}
        )

        self.assertEqual(
            sections,
            [
                "[Round 1 Few-shot Asset]",
                "Few-shot 1: sample-a\ncanonical_view=x",
            ],
        )

    def test_apply_distractor_patch_uses_scoped_revision_when_strategy_or_intensity_is_present(self) -> None:
        validation_result = Mock()
        validation_result.passed = True
        validation_result.validation_status = "passed"
        validation_result.model_dump.return_value = {
            "passed": True,
            "validation_status": "passed",
            "checks": {
                "analysis_answer_consistency": {"passed": True},
                "analysis_mentions_correct_option_text": {"passed": True},
            },
        }
        self.service.validator = Mock()
        self.service.validator.validate.return_value = validation_result
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 88}
        self.service.repository = Mock()
        self.service.repository._utc_now.return_value = "2026-04-12T12:00:00Z"
        self.service.snapshot_builder = Mock()
        self.service.snapshot_builder.build.return_value = {"snapshot": True}
        self.service.runtime_config = SimpleNamespace(
            llm=SimpleNamespace(
                routing=SimpleNamespace(
                    review_actions=SimpleNamespace(question_modify="review-actions.question_modify")
                )
            )
        )
        self.service.llm_gateway = Mock()
        self.service.llm_gateway.generate_json.return_value = {
            "option_text": "新的B项干扰项",
            "analysis": "A项正确，因为它最符合材料主旨；B项被改成偷换概念的强干扰项。",
        }
        self.service._resolve_template = Mock(return_value=SimpleNamespace(content="system prompt"))
        self.service._preference_profile_from_snapshot = Mock(return_value={"profile": "demo"})
        self.service._feedback_snapshot_from_material = Mock(return_value={"selection_state": "hold"})
        self.service._apply_evaluation_gate = Mock()
        self.service._build_prompt_request_from_snapshot = Mock(
            return_value=SimpleNamespace(model_dump=lambda: {"question_type": "main_idea"})
        )
        self.service._attach_feedback_runtime_context = Mock(side_effect=lambda **kwargs: kwargs["runtime_snapshot"])
        self.service._build_version_record = Mock(return_value={"source_action": "distractor_patch"})

        item = {
            "item_id": "item-1",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "difficulty_target": "medium",
            "generated_question": {
                "question_type": "main_idea",
                "pattern_id": "pattern-1",
                "stem": "根据材料选择最合适的标题。",
                "options": {
                    "A": "正确标题",
                    "B": "原始错误项",
                    "C": "错误项C",
                    "D": "错误项D",
                },
                "answer": "A",
                "analysis": "A项正确，因为它最符合材料主旨。",
            },
            "material_selection": {
                "material_id": "mat-1",
                "article_id": "art-1",
                "text": "材料原文",
                "source": {"site": "demo"},
                "document_genre": "commentary",
                "selection_reason": "unit-test",
            },
            "material_text": "材料原文",
            "material_source": {"site": "demo"},
            "request_snapshot": {
                "source_form": {"question_focus": "main_idea"},
                "question_type": "main_idea",
            },
            "statuses": {
                "generation_status": "success",
                "validation_status": "passed",
                "review_status": "waiting_review",
            },
            "current_version_no": 1,
            "revision_count": 0,
        }

        revised = self.service.apply_distractor_patch(
            item,
            target_option="B",
            distractor_strategy="concept_swap",
            distractor_intensity="strong",
            option_text="",
            analysis="",
            operator="demo",
        )

        self.assertEqual(revised["generated_question"]["options"]["A"], "正确标题")
        self.assertEqual(revised["generated_question"]["options"]["C"], "错误项C")
        self.assertEqual(revised["generated_question"]["options"]["D"], "错误项D")
        self.assertEqual(revised["generated_question"]["answer"], "A")
        self.assertEqual(revised["generated_question"]["options"]["B"], "新的B项干扰项")
        self.assertEqual(
            revised["generated_question"]["analysis"],
            "A项正确，因为它最符合材料主旨；B项被改成偷换概念的强干扰项。",
        )
        self.assertEqual(revised["latest_action"], "distractor_patch")
        self.assertEqual(revised["current_version_no"], 2)
        self.assertEqual(revised["revision_count"], 1)
        self.assertIn("distractor_patch:B:concept_swap:strong:demo", revised["notes"])
        self.service.llm_gateway.generate_json.assert_called_once()
        llm_call = self.service.llm_gateway.generate_json.call_args.kwargs
        self.assertEqual(llm_call["route"], "review-actions.question_modify")
        self.assertIn("Target wrong option to revise: B", llm_call["user_prompt"])
        self.assertIn("Target distractor strategy: concept_swap", llm_call["user_prompt"])
        self.assertIn("Target distractor intensity: strong", llm_call["user_prompt"])
        self.service.validator.validate.assert_called_once()
        self.service.evaluator.evaluate.assert_called_once()

    def test_apply_manual_edit_takes_over_current_item_without_validation_or_evaluation(self) -> None:
        self.service.repository = Mock()
        self.service.repository._utc_now.return_value = "2026-04-12T12:00:00Z"
        self.service.snapshot_builder = Mock()
        self.service.snapshot_builder.build.return_value = {"snapshot": True}
        self.service.runtime_config = SimpleNamespace(
            llm=SimpleNamespace(
                routing=SimpleNamespace(
                    review_actions=SimpleNamespace(question_modify="review-actions.question_modify")
                )
            )
        )
        self.service.validator = Mock()
        self.service.evaluator = Mock()
        self.service._resolve_template = Mock(return_value=SimpleNamespace(template_name="tmpl", template_version="v1"))
        self.service._preference_profile_from_snapshot = Mock(return_value={"profile": "demo"})
        self.service._build_prompt_request_from_snapshot = Mock(
            return_value=SimpleNamespace(model_dump=lambda: {"question_type": "main_idea"})
        )
        self.service._attach_feedback_runtime_context = Mock(side_effect=lambda **kwargs: kwargs["runtime_snapshot"])
        self.service._build_version_record = Mock(return_value={"source_action": "manual_edit"})

        item = {
            "item_id": "item-1",
            "batch_id": "batch-1",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "difficulty_target": "medium",
            "generated_question": {
                "question_type": "main_idea",
                "pattern_id": "pattern-1",
                "stem": "根据材料选择最合适的标题。",
                "options": {
                    "A": "正确标题",
                    "B": "错误项B",
                    "C": "错误项C",
                    "D": "错误项D",
                },
                "answer": "A",
                "analysis": "A项正确，因为它最符合材料主旨。",
            },
            "material_selection": {
                "material_id": "mat-1",
                "article_id": "art-1",
                "text": "材料原文",
                "original_text": "材料原文",
                "source": {"site": "demo"},
                "document_genre": "commentary",
                "selection_reason": "unit-test",
            },
            "material_text": "材料原文",
            "material_source": {"site": "demo"},
            "request_snapshot": {
                "source_form": {"question_focus": "main_idea"},
                "question_type": "main_idea",
            },
            "statuses": {
                "generation_status": "success",
                "validation_status": "passed",
                "review_status": "waiting_review",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "evaluation_result": {"overall_score": 90},
            "current_version_no": 1,
            "revision_count": 0,
        }

        revised = self.service.apply_manual_edit(
            item,
            instruction="manual takeover",
            control_overrides={
                "manual_patch": {
                    "material_text": "手工改后的材料",
                    "stem": "手工改后的题干",
                    "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
                    "answer": "B",
                    "analysis": "手工改后的解析",
                }
            },
        )

        self.assertTrue(revised["manual_override_active"])
        self.assertEqual(revised["material_text"], "手工改后的材料")
        self.assertEqual(revised["generated_question"]["stem"], "手工改后的题干")
        self.assertEqual(revised["generated_question"]["answer"], "B")
        self.assertEqual(revised["generated_question"]["analysis"], "手工改后的解析")
        self.assertIsNone(revised["validation_result"])
        self.assertIsNone(revised["evaluation_result"])
        self.assertEqual(revised["current_status"], "generated")
        self.assertEqual(revised["statuses"]["generation_status"], "success")
        self.assertEqual(revised["statuses"]["validation_status"], "not_started")
        self.assertEqual(revised["statuses"]["review_status"], "waiting_review")
        self.service.validator.validate.assert_not_called()
        self.service.evaluator.evaluate.assert_not_called()

    def test_apply_analysis_only_repair_only_updates_analysis_and_reruns_validation_and_evaluation(self) -> None:
        validation_result = Mock()
        validation_result.passed = True
        validation_result.validation_status = "passed"
        validation_result.checks = {
            "analysis_answer_consistency": {"passed": True},
            "analysis_mentions_correct_option_text": {"passed": True},
        }
        validation_result.model_dump.return_value = {
            "passed": True,
            "validation_status": "passed",
            "checks": validation_result.checks,
        }
        self.service.validator = Mock()
        self.service.validator.validate.return_value = validation_result
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 90}
        self.service._apply_evaluation_gate = Mock(return_value=[])
        self.service.llm_gateway = Mock()
        self.service.llm_gateway.generate_json.return_value = {
            "analysis": "A项正确，因为它最完整概括了材料主旨，同时其余选项都只是局部信息或偏离中心。"
        }

        current_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "正确选项文本", "B": "错误项B", "C": "错误项C", "D": "错误项D"},
            answer="A",
            analysis="原解析",
        )
        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        built_item = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "difficulty_target": "medium",
            "request_snapshot": {
                "source_question": {},
                "source_question_analysis": {},
            },
            "validation_result": {
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                }
            },
            "material_selection": material.model_dump(),
            "material_text": material.text,
            "material_source": material.source,
        }

        result = self.service.apply_analysis_only_repair(
            built_item=built_item,
            material=material,
            current_question=current_question,
            route="repair-route",
            repair_plan={"mode": "analysis_only_repair"},
            feedback_notes=["Only improve the analysis."],
        )

        repaired_question = result["generated_question"]
        self.assertEqual(repaired_question.stem, current_question.stem)
        self.assertEqual(repaired_question.options, current_question.options)
        self.assertEqual(repaired_question.answer, current_question.answer)
        self.assertEqual(
            repaired_question.analysis,
            "A项正确，因为它最完整概括了材料主旨，同时其余选项都只是局部信息或偏离中心。",
        )
        self.assertEqual(result["quality_gate_errors"], [])
        self.service.validator.validate.assert_called_once()
        self.service.evaluator.evaluate.assert_called_once()
        llm_call = self.service.llm_gateway.generate_json.call_args.kwargs
        self.assertEqual(llm_call["route"], "repair-route")
        self.assertEqual(llm_call["schema_name"], "analysis_only_patch")
        self.assertIn("Return JSON with key analysis only.", llm_call["user_prompt"])

    def test_apply_analysis_only_repair_rejects_regressed_analysis_check(self) -> None:
        validation_result = Mock()
        validation_result.passed = False
        validation_result.validation_status = "failed"
        validation_result.checks = {
            "analysis_answer_consistency": {"passed": True},
            "analysis_mentions_correct_option_text": {"passed": False},
        }
        validation_result.model_dump.return_value = {
            "passed": False,
            "validation_status": "failed",
            "checks": validation_result.checks,
        }
        self.service.validator = Mock()
        self.service.validator.validate.return_value = validation_result
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 72}
        self.service._apply_evaluation_gate = Mock(return_value=["quality_gate_failed"])
        self.service.llm_gateway = Mock()
        self.service.llm_gateway.generate_json.return_value = {
            "analysis": "新的解析没有明确解释为什么当前正确项文本最符合材料。"
        }

        current_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "正确选项文本", "B": "错误项B", "C": "错误项C", "D": "错误项D"},
            answer="A",
            analysis="原解析",
        )
        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        built_item = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "difficulty_target": "medium",
            "request_snapshot": {
                "source_question": {},
                "source_question_analysis": {},
            },
            "validation_result": {
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                }
            },
            "material_selection": material.model_dump(),
            "material_text": material.text,
            "material_source": material.source,
        }

        with self.assertRaises(DomainError):
            self.service.apply_analysis_only_repair(
                built_item=built_item,
                material=material,
                current_question=current_question,
                route="repair-route",
                repair_plan={"mode": "analysis_only_repair"},
                feedback_notes=["Only improve the analysis."],
            )

    def test_build_generated_item_routes_analysis_only_repair_to_scoped_executor(self) -> None:
        initial_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "正确选项文本", "B": "错误项B", "C": "错误项C", "D": "错误项D"},
            answer="A",
            analysis="原解析",
        )
        repaired_question = initial_question.model_copy(update={"analysis": "修复后的解析"})
        initial_validation = SimpleNamespace(
            passed=False,
            score=60,
            errors=["analysis_mentions_correct_option_text"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": False}},
            validation_status="failed",
            model_dump=lambda: {
                "passed": False,
                "score": 60,
                "errors": ["analysis_mentions_correct_option_text"],
                "warnings": [],
                "checks": {"analysis_mentions_correct_option_text": {"passed": False}},
                "validation_status": "failed",
            },
        )
        repaired_validation = SimpleNamespace(
            passed=True,
            score=90,
            errors=[],
            warnings=[],
            checks={
                "analysis_answer_consistency": {"passed": True},
                "analysis_mentions_correct_option_text": {"passed": True},
            },
            validation_status="passed",
            model_dump=lambda: {
                "passed": True,
                "score": 90,
                "errors": [],
                "warnings": [],
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                },
                "validation_status": "passed",
            },
        )

        self.service.orchestrator = Mock()
        self.service.orchestrator.build_prompt.return_value = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "pattern_id": "pattern-1",
            "difficulty_fit": {},
            "statuses": {},
            "notes": [],
            "warnings": [],
        }
        self.service._resolve_template = Mock(return_value=SimpleNamespace(template_name="tpl", template_version="1"))
        self.service._generate_question = Mock(return_value=(initial_question, {"raw": "initial"}))
        self.service.validator = Mock()
        self.service.validator.validate.return_value = initial_validation
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 91}
        self.service._apply_evaluation_gate = Mock(return_value=[])
        self.service._should_retry_alignment = Mock(side_effect=[True, False])
        self.service._build_targeted_repair_plan = Mock(
            return_value={
                "mode": "analysis_only_repair",
                "allowed_fields": ["analysis"],
                "locked_fields": ["stem", "options", "answer"],
                "target_errors": [],
                "target_checks": ["analysis_mentions_correct_option_text"],
                "notes": ["Only improve the analysis."],
            }
        )
        self.service._build_alignment_feedback_notes = Mock(return_value=["补充解释正确项文本为何更贴合材料。"])
        self.service.apply_analysis_only_repair = Mock(
            return_value={
                "generated_question": repaired_question,
                "raw_model_output": {"analysis": "修复后的解析"},
                "validation_result": repaired_validation,
                "evaluation_result": {"overall_score": 89},
                "quality_gate_errors": [],
            }
        )
        self.service._run_targeted_question_repair = Mock(side_effect=AssertionError("should not call full repair"))
        self.service._should_retry_quality_repair = Mock(return_value=False)
        self.service.snapshot_builder = Mock()
        self.service.snapshot_builder.build.return_value = {"snapshot": True}
        self.service._attach_feedback_runtime_context = Mock(side_effect=lambda **kwargs: kwargs["runtime_snapshot"])
        self.service._build_version_record = Mock(return_value={"source_action": "generate"})
        self.service.repository = Mock()
        self.service.repository._utc_now.return_value = "2026-04-12T12:00:00Z"
        self.service.runtime_config = SimpleNamespace(
            llm=SimpleNamespace(
                routing=SimpleNamespace(
                    question_repair="repair-route",
                )
            )
        )

        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        build_request = SimpleNamespace(
            question_type="main_idea",
            business_subtype="center_understanding",
            difficulty_target="medium",
            model_dump=lambda: {"question_type": "main_idea", "difficulty_target": "medium"},
        )

        result = self.service._build_generated_item(
            build_request=build_request,
            material=material,
            batch_id="batch-1",
            item_id="item-1",
            request_snapshot={"source_form": {}, "source_question_analysis": {"style_summary": {"question_type": "main_idea"}}},
            revision_count=0,
            route="generate-route",
            source_action="generate",
            review_note=None,
            request_id="req-1",
            previous_item=None,
        )

        self.service.apply_analysis_only_repair.assert_called_once()
        self.assertEqual(result["generated_question"]["analysis"], "修复后的解析")
        self.assertEqual(result["generated_question"]["stem"], initial_question.stem)

    def test_apply_answer_binding_patch_updates_options_answer_and_analysis(self) -> None:
        validation_result = Mock()
        validation_result.passed = True
        validation_result.validation_status = "passed"
        validation_result.checks = {
            "analysis_answer_consistency": {"passed": True},
            "analysis_mentions_correct_option_text": {"passed": True},
        }
        validation_result.model_dump.return_value = {
            "passed": True,
            "validation_status": "passed",
            "checks": validation_result.checks,
        }
        self.service.validator = Mock()
        self.service.validator.validate.return_value = validation_result
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 92}
        self.service._apply_evaluation_gate = Mock(return_value=[])
        self.service.llm_gateway = Mock()
        self.service.llm_gateway.generate_json.return_value = {
            "options": {"A": "新正确项", "B": "新错误项B", "C": "新错误项C", "D": "新错误项D"},
            "answer": "A",
            "analysis": "新的解析明确解释A为何最符合材料。"
        }

        current_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "旧正确项", "B": "旧错误项B", "C": "旧错误项C", "D": "旧错误项D"},
            answer="B",
            analysis="原解析",
        )
        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        built_item = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "difficulty_target": "medium",
            "request_snapshot": {
                "source_question": {},
                "source_question_analysis": {},
            },
            "validation_result": {
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                }
            },
            "material_selection": material.model_dump(),
            "material_text": material.text,
            "material_source": material.source,
        }

        result = self.service.apply_answer_binding_patch(
            built_item=built_item,
            material=material,
            current_question=current_question,
            route="repair-route",
            repair_plan={"mode": "main_idea_axis_repair"},
            feedback_notes=["Only repair options, answer, and analysis."],
        )

        repaired_question = result["generated_question"]
        self.assertEqual(repaired_question.stem, current_question.stem)
        self.assertEqual(repaired_question.options["A"], "新正确项")
        self.assertEqual(repaired_question.answer, "A")
        self.assertEqual(repaired_question.analysis, "新的解析明确解释A为何最符合材料。")
        self.service.validator.validate.assert_called_once()
        self.service.evaluator.evaluate.assert_called_once()
        llm_call = self.service.llm_gateway.generate_json.call_args.kwargs
        self.assertEqual(llm_call["schema_name"], "answer_binding_patch")
        self.assertIn("Return JSON with keys options, answer, and analysis only.", llm_call["user_prompt"])

    def test_apply_answer_binding_patch_rejects_scope_drift(self) -> None:
        validation_result = Mock()
        validation_result.passed = True
        validation_result.validation_status = "passed"
        validation_result.checks = {
            "analysis_answer_consistency": {"passed": True},
            "analysis_mentions_correct_option_text": {"passed": True},
        }
        validation_result.model_dump.return_value = {
            "passed": True,
            "validation_status": "passed",
            "checks": validation_result.checks,
        }
        self.service.validator = Mock()
        self.service.validator.validate.return_value = validation_result
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 92}
        self.service._apply_evaluation_gate = Mock(return_value=[])
        self.service.llm_gateway = Mock()
        self.service.llm_gateway.generate_json.return_value = {
            "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
            "answer": "E",
            "analysis": "新的解析",
        }

        current_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "旧A", "B": "旧B", "C": "旧C", "D": "旧D"},
            answer="A",
            analysis="原解析",
        )
        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        built_item = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "difficulty_target": "medium",
            "request_snapshot": {
                "source_question": {},
                "source_question_analysis": {},
            },
            "validation_result": {
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                }
            },
            "material_selection": material.model_dump(),
            "material_text": material.text,
            "material_source": material.source,
        }

        with self.assertRaises(DomainError):
            self.service.apply_answer_binding_patch(
                built_item=built_item,
                material=material,
                current_question=current_question,
                route="repair-route",
                repair_plan={"mode": "main_idea_axis_repair"},
                feedback_notes=["Only repair options, answer, and analysis."],
            )

    def test_build_generated_item_routes_answer_binding_repair_to_scoped_executor(self) -> None:
        initial_question = GeneratedQuestion(
            question_type="main_idea",
            business_subtype="center_understanding",
            pattern_id="pattern-1",
            stem="根据材料选择最能概括主旨的一项。",
            options={"A": "旧A", "B": "旧B", "C": "旧C", "D": "旧D"},
            answer="A",
            analysis="原解析",
        )
        repaired_question = initial_question.model_copy(
            update={
                "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
                "answer": "A",
                "analysis": "修复后的解析",
            }
        )
        initial_validation = SimpleNamespace(
            passed=False,
            score=60,
            errors=["main_axis_mismatch"],
            warnings=[],
            checks={"analysis_mentions_correct_option_text": {"passed": False}},
            validation_status="failed",
            model_dump=lambda: {
                "passed": False,
                "score": 60,
                "errors": ["main_axis_mismatch"],
                "warnings": [],
                "checks": {"analysis_mentions_correct_option_text": {"passed": False}},
                "validation_status": "failed",
            },
        )
        repaired_validation = SimpleNamespace(
            passed=True,
            score=90,
            errors=[],
            warnings=[],
            checks={
                "analysis_answer_consistency": {"passed": True},
                "analysis_mentions_correct_option_text": {"passed": True},
            },
            validation_status="passed",
            model_dump=lambda: {
                "passed": True,
                "score": 90,
                "errors": [],
                "warnings": [],
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                },
                "validation_status": "passed",
            },
        )

        self.service.orchestrator = Mock()
        self.service.orchestrator.build_prompt.return_value = {
            "question_type": "main_idea",
            "business_subtype": "center_understanding",
            "pattern_id": "pattern-1",
            "difficulty_fit": {},
            "statuses": {},
            "notes": [],
            "warnings": [],
        }
        self.service._resolve_template = Mock(return_value=SimpleNamespace(template_name="tpl", template_version="1"))
        self.service._generate_question = Mock(return_value=(initial_question, {"raw": "initial"}))
        self.service.validator = Mock()
        self.service.validator.validate.return_value = initial_validation
        self.service.evaluator = Mock()
        self.service.evaluator.evaluate.return_value = {"overall_score": 91}
        self.service._apply_evaluation_gate = Mock(return_value=[])
        self.service._should_retry_alignment = Mock(side_effect=[True, False])
        self.service._build_targeted_repair_plan = Mock(
            return_value={
                "mode": "main_idea_axis_repair",
                "allowed_fields": ["options", "answer", "analysis"],
                "locked_fields": ["stem", "original_sentences", "correct_order"],
                "target_errors": ["main_axis_mismatch"],
                "target_checks": ["analysis_mentions_correct_option_text"],
                "notes": ["Only repair option mapping, answer, and explanation."],
            }
        )
        self.service._build_alignment_feedback_notes = Mock(return_value=["修正主旨轴线和答案绑定。"])
        self.service.apply_answer_binding_patch = Mock(
            return_value={
                "generated_question": repaired_question,
                "raw_model_output": {"options": repaired_question.options, "answer": "A", "analysis": "修复后的解析"},
                "validation_result": repaired_validation,
                "evaluation_result": {"overall_score": 89},
                "quality_gate_errors": [],
            }
        )
        self.service._run_targeted_question_repair = Mock(side_effect=AssertionError("should not call full repair"))
        self.service._should_retry_quality_repair = Mock(return_value=False)
        self.service.snapshot_builder = Mock()
        self.service.snapshot_builder.build.return_value = {"snapshot": True}
        self.service._attach_feedback_runtime_context = Mock(side_effect=lambda **kwargs: kwargs["runtime_snapshot"])
        self.service._build_version_record = Mock(return_value={"source_action": "generate"})
        self.service.repository = Mock()
        self.service.repository._utc_now.return_value = "2026-04-12T12:00:00Z"
        self.service.runtime_config = SimpleNamespace(
            llm=SimpleNamespace(
                routing=SimpleNamespace(
                    question_repair="repair-route",
                )
            )
        )

        material = MaterialSelectionResult(
            material_id="mat-1",
            article_id="art-1",
            text="材料原文",
            original_text="材料原文",
            source={"site": "demo"},
            document_genre="commentary",
            selection_reason="unit-test",
        )
        build_request = SimpleNamespace(
            question_type="main_idea",
            business_subtype="center_understanding",
            difficulty_target="medium",
            model_dump=lambda: {"question_type": "main_idea", "difficulty_target": "medium"},
        )

        result = self.service._build_generated_item(
            build_request=build_request,
            material=material,
            batch_id="batch-1",
            item_id="item-1",
            request_snapshot={"source_form": {}, "source_question_analysis": {"style_summary": {"question_type": "main_idea"}}},
            revision_count=0,
            route="generate-route",
            source_action="generate",
            review_note=None,
            request_id="req-1",
            previous_item=None,
        )

        self.service.apply_answer_binding_patch.assert_called_once()
        self.assertEqual(result["generated_question"]["analysis"], "修复后的解析")
        self.assertEqual(result["generated_question"]["stem"], initial_question.stem)

    def test_patch_scope_registry_resolves_repair_modes(self) -> None:
        analysis_scope = resolve_repair_mode_scope("analysis_only_repair")
        answer_scope = resolve_repair_mode_scope("main_idea_axis_repair")
        explicit_scope = resolve_repair_mode_scope("answer_binding_patch")
        self.assertIsNotNone(analysis_scope)
        self.assertIsNotNone(answer_scope)
        self.assertEqual(analysis_scope.name, "analysis_only")
        self.assertEqual(answer_scope.name, "answer_binding_patch")
        self.assertEqual(explicit_scope.name, "answer_binding_patch")

    def test_patch_scope_registry_returns_scope_definition(self) -> None:
        scope = get_patch_scope("single_distractor_patch")
        self.assertIsNotNone(scope)
        self.assertEqual(scope.name, "single_distractor_patch")
        self.assertIn("options.target", scope.allowed_fields)
