from __future__ import annotations

import sys
import types
from pathlib import Path
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

from app.schemas.item import GeneratedQuestion
from app.schemas.question import QuestionGenerateRequest
from app.services.question_generation import QuestionGenerationService


class QuestionGenerationUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = QuestionGenerationService.__new__(QuestionGenerationService)
        self.service.material_bridge = Mock()
        self.service.material_bridge._normalize_preference_profile = Mock(return_value={})

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

    def test_decode_request_does_not_infer_focus_from_source_question(self) -> None:
        self.service.source_question_analyzer = Mock()
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

        self.service.source_question_analyzer.infer_request_target.assert_not_called()
        self.assertIsNone(warning)
        self.assertEqual(decode_request.question_focus, "")

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

    def test_collect_answer_grounding_facts_for_center_understanding_reads_meaning_preserving_mode(self) -> None:
        facts = self.service._collect_answer_grounding_facts(
            question_type="main_idea",
            business_subtype="center_understanding",
            question_card_binding={
                "runtime_binding": {"question_type": "main_idea", "business_subtype": "center_understanding"},
                "question_card": {
                    "business_subtype_id": "center_understanding",
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
        self.assertEqual(text.count("def _clean_material_text("), 1)
        self.assertEqual(text.count("def _strip_material_template_labels("), 1)
        self.assertEqual(text.count("def _needs_material_refinement("), 1)

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
        self.assertEqual(
            self.service.material_bridge.select_materials.call_args.kwargs["business_card_ids"],
            [],
        )
