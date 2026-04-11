import unittest

from app.domain.services.material_v2_index_service import MaterialV2IndexService
from app.services.main_card_dual_judge import MainCardDualJudge
from app.services.main_card_family_landing_resolver import MainCardFamilyLandingResolver
from app.services.main_card_signal_resolver import MainCardSignalResolver
from app.services.material_pipeline_v2 import MaterialPipelineV2


class SingleJudgeAndTitleSelectionFixTests(unittest.TestCase):
    def test_family_landing_supports_single_judge_mode(self) -> None:
        class _FakeProvider:
            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                return {
                    "selected_main_cards": ["center_understanding"],
                    "reason": "single-judge",
                    "evidence_summary": "single-judge",
                    "confidence": 0.83,
                }

        resolver = MainCardFamilyLandingResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_family_landing": {
                    "enabled": True,
                    "mode": "enforce",
                    "judge_count": 1,
                    "models": {"judge_a": "model-a"},
                    "common_instructions": "{allowed_main_cards_json}",
                    "user_prompt_template": "{article_title}\\n{material_text}\\n{mechanical_v2_families_json}",
                    "runtime_families": {
                        "center_understanding": {
                            "label": "Center",
                            "goal": "main idea",
                            "formal_unit_definition": "single center",
                            "accept_definition": "usable for main idea",
                            "reject_definition": "only topic-related",
                        }
                    },
                }
            },
        )

        article = type("Article", (), {"title": "sample article", "source": "people"})()
        material = type(
            "Material",
            (),
            {
                "id": "mat-1",
                "text": "This material stays on one main idea throughout.",
                "status": "tagged",
                "release_channel": "stable",
                "quality_score": 0.82,
                "paragraph_count": 3,
                "sentence_count": 8,
                "primary_family": "",
                "primary_subtype": "",
                "parallel_families": [],
                "family_scores": {},
                "universal_profile": {"single_center_strength": 0.81},
                "feature_profile": {"theme_words": ["main idea"]},
            },
        )()

        result = resolver.resolve(material=material, article=article, mechanical_v2_families=["title_selection"])

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "single")
        self.assertEqual(result["runtime_families"], ["title_selection"])

    def test_dual_judge_supports_single_judge_mode(self) -> None:
        class _FakeProvider:
            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                return {
                    "decision": "accept",
                    "formal_layer": "strong",
                    "selected_material_card": "title_material.plain_main_recovery",
                    "selected_business_card": None,
                    "reason": "single judge accepted",
                    "evidence_summary": "main idea is stable",
                    "confidence": 0.88,
                }

        judge = MainCardDualJudge(
            provider=_FakeProvider(),
            llm_config={
                "main_card_dual_judge": {
                    "enabled": True,
                    "mode": "enforce",
                    "judge_count": 1,
                    "models": {"judge_a": "model-a"},
                    "common_instructions": "{family_goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {
                        "center_understanding": {
                            "label": "Center",
                            "goal": "judge main-idea fit",
                            "formal_unit_definition": "single center",
                            "strong_accept_definition": "stable main idea",
                            "weak_accept_definition": "mostly stable main idea",
                            "reject_definition": "topic-related only",
                        }
                    },
                }
            },
        )

        result = judge.adjudicate(
            business_family_id="title_selection",
            item={"text": "This material stays on one main idea throughout.", "question_ready_context": {}, "meta": {}},
            question_card={"card_id": "question.center_understanding.standard_v1"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "single")
        self.assertTrue(judge.consensus_allows_accept(result))

    def test_dual_judge_does_not_accept_single_vote_when_configured_for_two_judges(self) -> None:
        class _FakeProvider:
            def __init__(self) -> None:
                self.calls = 0

            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "decision": "accept",
                        "formal_layer": "strong",
                        "selected_material_card": "title_material.plain_main_recovery",
                        "selected_business_card": None,
                        "reason": "judge a accepted",
                        "evidence_summary": "main idea is stable",
                        "confidence": 0.88,
                    }
                raise RuntimeError("judge b failed")

        judge = MainCardDualJudge(
            provider=_FakeProvider(),
            llm_config={
                "main_card_dual_judge": {
                    "enabled": True,
                    "mode": "enforce",
                    "judge_count": 2,
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{family_goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {
                        "center_understanding": {
                            "label": "Center",
                            "goal": "judge main-idea fit",
                            "formal_unit_definition": "single center",
                            "strong_accept_definition": "stable main idea",
                            "weak_accept_definition": "mostly stable main idea",
                            "reject_definition": "topic-related only",
                        }
                    },
                }
            },
        )

        result = judge.adjudicate(
            business_family_id="title_selection",
            item={"text": "This material stays on one main idea throughout.", "question_ready_context": {}, "meta": {}},
            question_card={"card_id": "question.center_understanding.standard_v1"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "insufficient_votes")
        self.assertFalse(judge.consensus_allows_accept(result))

    def test_signal_resolver_supports_single_judge_mode(self) -> None:
        class _FakeProvider:
            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                return {
                    "opening_anchor_type": "viewpoint_opening",
                    "opening_rule": "explicit_opening",
                    "opening_signal_strength": 0.81,
                    "closing_anchor_type": "summary",
                    "closing_rule": "summary_or_conclusion",
                    "closing_signal_strength": 0.72,
                    "local_binding_strength": 0.73,
                    "unique_opener_score": 0.68,
                    "binding_pair_count": 2,
                    "exchange_risk": 0.19,
                    "function_overlap_score": 0.22,
                    "multi_path_risk": 0.21,
                    "discourse_progression_strength": 0.74,
                    "context_closure_score": 0.71,
                    "temporal_order_strength": 0.12,
                    "action_sequence_irreversibility": 0.15,
                    "sequence_integrity": 0.75,
                    "unit_count": 6,
                    "binding_rules": ["pronoun_reference"],
                    "logic_modes": ["discourse_logic"],
                    "reason": "single-judge",
                }

        resolver = MainCardSignalResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_signal_resolver": {
                    "enabled": True,
                    "mode": "enforce",
                    "judge_count": 1,
                    "models": {"judge_a": "model-a"},
                    "common_instructions": "{goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {"sentence_order": {"label": "Order", "goal": "order", "signal_goal": "signals"}},
                }
            },
        )

        result = resolver.resolve(
            business_family_id="sentence_order",
            article_context={"title": "order sample", "source": {"domain": "example.com"}},
            candidate={"candidate_type": "weak_formal_order_group", "text": "A. B. C. D. E. F.", "meta": {}},
            neutral_signal_profile={},
            business_feature_profile={},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "single")
        self.assertEqual(result["consensus"]["neutral_signal_overrides"]["opening_rule"], "explicit_opening")

    def test_signal_resolver_requires_full_votes_in_two_judge_mode(self) -> None:
        class _FakeProvider:
            def __init__(self) -> None:
                self.calls = 0

            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "opening_anchor_type": "viewpoint_opening",
                        "opening_rule": "explicit_opening",
                        "opening_signal_strength": 0.81,
                        "reason": "judge a",
                    }
                raise RuntimeError("judge b failed")

        resolver = MainCardSignalResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_signal_resolver": {
                    "enabled": True,
                    "mode": "enforce",
                    "judge_count": 2,
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {"sentence_order": {"label": "Order", "goal": "order", "signal_goal": "signals"}},
                }
            },
        )

        result = resolver.resolve(
            business_family_id="sentence_order",
            article_context={"title": "order sample", "source": {"domain": "example.com"}},
            candidate={"candidate_type": "weak_formal_order_group", "text": "A. B. C. D. E. F.", "meta": {}},
            neutral_signal_profile={},
            business_feature_profile={},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "insufficient_votes")
        self.assertFalse(resolver.consensus_allows_override(result))

    def test_title_selection_contract_accepts_paragraph_window_as_multi_paragraph_unit(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        contract_types = pipeline._candidate_contract_types(
            {"candidate_type": "paragraph_window"},
            business_family_id="title_selection",
        )
        self.assertIn("paragraph_window", contract_types)
        self.assertIn("multi_paragraph_unit", contract_types)

    def test_legacy_top_hit_stays_legacy_when_llm_catalog_has_multiple_choices(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        promoted = pipeline._maybe_promote_legacy_top_hit_from_llm_catalog(
            top_hit={"card_id": "legacy.title_selection.precomputed", "generation_archetype": "legacy_material_fallback"},
            llm_material_card_options=[
                {"card_id": "title_material.plain_main_recovery"},
                {"card_id": "title_material.problem_essence_judgement"},
            ],
        )
        self.assertEqual(promoted["card_id"], "legacy.title_selection.precomputed")

    def test_material_v2_index_service_uses_llm_empty_decision_over_mechanical_main_cards(self) -> None:
        service = MaterialV2IndexService.__new__(MaterialV2IndexService)
        service.family_to_v2 = {
            "summary_family": "title_selection",
            "continuation_family": "continuation",
        }
        service.main_card_family_landing = type(
            "Landing",
            (),
            {
                "resolve": staticmethod(
                    lambda **kwargs: {
                        "consensus": {"status": "single", "selected_main_cards": []},
                        "runtime_families": [],
                    }
                )
            },
        )()
        material = type(
            "Material",
            (),
            {
                "primary_family": "summary_family",
                "parallel_families": [{"family": "continuation_family", "score": 0.42}],
                "family_scores": {"summary_family": 0.73},
                "paragraph_count": 3,
                "sentence_count": 7,
                "quality_score": 0.81,
            },
        )()

        resolved = service._resolve_v2_families(
            material=material,
            article=object(),
        )
        self.assertEqual(resolved, ["continuation"])

    def test_material_v2_index_service_uses_mechanical_only_when_llm_fails(self) -> None:
        service = MaterialV2IndexService.__new__(MaterialV2IndexService)
        service.family_to_v2 = {
            "summary_family": "title_selection",
            "continuation_family": "continuation",
        }
        service.main_card_family_landing = type(
            "Landing",
            (),
            {
                "resolve": staticmethod(
                    lambda **kwargs: {
                        "consensus": {"status": "insufficient_votes", "selected_main_cards": []},
                        "runtime_families": [],
                    }
                )
            },
        )()
        material = type(
            "Material",
            (),
            {
                "primary_family": "summary_family",
                "parallel_families": [{"family": "continuation_family", "score": 0.42}],
            },
        )()

        resolved = service._resolve_v2_families(
            material=material,
            article=object(),
        )
        self.assertEqual(resolved, ["continuation", "title_selection"])


if __name__ == "__main__":
    unittest.main()
