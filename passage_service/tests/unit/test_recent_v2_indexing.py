import app.domain.services.tag_service as tag_service_module
import unittest
from unittest.mock import patch

from app.domain.services.material_v2_index_service import MaterialV2IndexService
from app.domain.services.tag_service import TagService
from app.services.main_card_family_landing_resolver import MainCardFamilyLandingResolver
from app.services.main_card_dual_judge import MainCardDualJudge
from app.services.main_card_signal_resolver import MainCardSignalResolver
from app.services.material_pipeline_v2 import MaterialPipelineV2


class _FakePipeline:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def build_cached_item_from_material(self, **kwargs):
        self.calls.append(kwargs)
        return {"question_ready_context": {"selected_material_card": "ok"}}


class RecentV2IndexingTests(unittest.TestCase):
    def test_main_card_family_landing_resolver_supports_single_judge_mode(self) -> None:
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
                    "user_prompt_template": "{article_title}\n{material_text}\n{mechanical_v2_families_json}",
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

    def test_sentence_order_material_bridge_normalizes_seven_units_to_six(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.SENTENCE_ORDER_FIXED_UNIT_COUNT = 6
        pipeline._sentence_order_units = lambda text, candidate_type: [f"绗瑊i}鍙ャ€? for i in range(1, 8)]  # type: ignore[method-assign]
        pipeline._normalize_ordered_units_to_six = lambda raw_units: (  # type: ignore[method-assign]
            ["绗?鍙ャ€?, "绗?鍙ワ紝绗?鍙ャ€?, "绗?鍙ャ€?, "绗?鍙ャ€?, "绗?鍙ャ€?, "绗?鍙ャ€?],
            [
                "single_sentence_unit",
                "grouped_unit",
                "single_sentence_unit",
                "single_sentence_unit",
                "single_sentence_unit",
                "single_sentence_unit",
            ],
            [{"before": 1, "after": 1, "reason": "reference_binding_pair", "source_indices": [1, 2]}],
            "merged_to_six:1",
        )
        pipeline._ordered_unit_group_worthwhile = lambda units: (  # type: ignore[method-assign]
            True,
            "ordered_unit_group_ready",
            [{"kind": "precedence", "before": 0, "after": 1}],
            [0],
            [5],
        )

        bridged = pipeline._bridge_sentence_order_candidate_to_formal_group(
            article_context={"article_id": "article-1"},
            candidate={
                "candidate_id": "cand-1",
                "candidate_type": "sentence_group",
                "text": "绗?鍙ャ€傜2鍙ャ€傜3鍙ャ€傜4鍙ャ€傜5鍙ャ€傜6鍙ャ€傜7鍙ャ€?,
                "meta": {"sentence_range": [0, 6], "paragraph_range": [0, 1]},
                "quality_flags": [],
            },
        )

        self.assertIsNotNone(bridged)
        self.assertEqual(bridged["candidate_type"], "ordered_unit_group")
        self.assertEqual(bridged["meta"]["group_size"], 6)
        self.assertEqual(bridged["meta"]["normalization_reason"], "merged_to_six:1")
        self.assertEqual(bridged["meta"]["grouped_unit_count"], 1)
        self.assertEqual(bridged["meta"]["unit_forms"][1], "grouped_unit")
        self.assertEqual(bridged["meta"]["ordering_reason_trace"]["normalization_reason"], "merged_to_six:1")

    def test_main_card_signal_resolver_merges_unanimous_numeric_votes_for_sentence_order(self) -> None:
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
                        "opening_signal_strength": 0.82,
                        "closing_anchor_type": "summary",
                        "closing_rule": "summary_or_conclusion",
                        "closing_signal_strength": 0.74,
                        "local_binding_strength": 0.77,
                        "unique_opener_score": 0.73,
                        "binding_pair_count": 3,
                        "exchange_risk": 0.18,
                        "function_overlap_score": 0.21,
                        "multi_path_risk": 0.19,
                        "discourse_progression_strength": 0.78,
                        "context_closure_score": 0.75,
                        "temporal_order_strength": 0.12,
                        "action_sequence_irreversibility": 0.14,
                        "sequence_integrity": 0.79,
                        "unit_count": 6,
                        "binding_rules": ["pronoun_reference", "parallel_connector"],
                        "logic_modes": ["discourse_logic", "deterministic_binding"],
                        "reason": "judge-a",
                    }
                return {
                    "opening_anchor_type": "viewpoint_opening",
                    "opening_rule": "explicit_opening",
                    "opening_signal_strength": 0.78,
                    "closing_anchor_type": "summary",
                    "closing_rule": "summary_or_conclusion",
                    "closing_signal_strength": 0.70,
                    "local_binding_strength": 0.73,
                    "unique_opener_score": 0.69,
                    "binding_pair_count": 3,
                    "exchange_risk": 0.22,
                    "function_overlap_score": 0.25,
                    "multi_path_risk": 0.23,
                    "discourse_progression_strength": 0.74,
                    "context_closure_score": 0.71,
                    "temporal_order_strength": 0.16,
                    "action_sequence_irreversibility": 0.18,
                    "sequence_integrity": 0.75,
                    "unit_count": 6,
                    "binding_rules": ["pronoun_reference"],
                    "logic_modes": ["discourse_logic", "deterministic_binding"],
                    "reason": "judge-b",
                }

        resolver = MainCardSignalResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_signal_resolver": {
                    "enabled": True,
                    "mode": "enforce",
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {"sentence_order": {"label": "璇彞鎺掑簭", "goal": "鎺掑簭", "signal_goal": "缁撴瀯淇″彿"}},
                }
            },
        )

        result = resolver.resolve(
            business_family_id="sentence_order",
            article_context={"title": "鎺掑簭鏉愭枡", "source": {"domain": "example.com"}},
            candidate={"candidate_type": "weak_formal_order_group", "text": "鐢层€備箼銆備笝銆備竵銆傛垔銆傚繁銆?, "meta": {}},
            neutral_signal_profile={},
            business_feature_profile={},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "unanimous")
        consensus = result["consensus"]
        self.assertEqual(consensus["neutral_signal_overrides"]["opening_rule"], "explicit_opening")
        self.assertAlmostEqual(consensus["neutral_signal_overrides"]["opening_signal_strength"], 0.8)
        self.assertEqual(consensus["neutral_signal_overrides"]["binding_rules"], ["pronoun_reference"])
        self.assertEqual(consensus["business_feature_profile_overrides"]["sentence_order_profile"]["closing_rule"], "summary_or_conclusion")

    def test_main_card_signal_resolver_rejects_split_vote_for_fill_key_fields(self) -> None:
        class _FakeProvider:
            def __init__(self) -> None:
                self.calls = 0

            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "slot_role": "opening",
                        "slot_function": "summary",
                        "blank_position": "opening",
                        "function_type": "summarize_following_text",
                        "logic_relation": "summary",
                        "bidirectional_validation": 0.71,
                        "backward_link_strength": 0.22,
                        "forward_link_strength": 0.81,
                        "summary_need_strength": 0.78,
                        "countermeasure_signal_strength": 0.08,
                        "reference_dependency": 0.18,
                        "slot_explicit_ready": True,
                        "reason": "judge-a",
                    }
                return {
                    "slot_role": "opening",
                    "slot_function": "topic_intro",
                    "blank_position": "opening",
                    "function_type": "topic_introduction",
                    "logic_relation": "introduction",
                    "bidirectional_validation": 0.69,
                    "backward_link_strength": 0.18,
                    "forward_link_strength": 0.84,
                    "summary_need_strength": 0.42,
                    "countermeasure_signal_strength": 0.05,
                    "reference_dependency": 0.21,
                    "slot_explicit_ready": True,
                    "reason": "judge-b",
                }

        resolver = MainCardSignalResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_signal_resolver": {
                    "enabled": True,
                    "mode": "enforce",
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{goal}",
                    "user_prompt_template": "{candidate_text}",
                    "families": {"sentence_fill": {"label": "璇彞濉┖", "goal": "濉┖", "signal_goal": "slot"}},
                }
            },
        )

        result = resolver.resolve(
            business_family_id="sentence_fill",
            article_context={"title": "濉┖鏉愭枡", "source": {"domain": "example.com"}},
            candidate={"candidate_type": "functional_slot_unit", "text": "杩欐槸涓€鍙ュ緟濉┖鍙ャ€?, "meta": {}},
            neutral_signal_profile={},
            business_feature_profile={},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "split_vote")
        self.assertEqual(result["consensus"]["neutral_signal_overrides"], {})

    def test_material_pipeline_v2_resolve_main_card_profiles_prefers_llm_consensus_and_keeps_mechanical_fallback(self) -> None:
        class _FakeResolver:
            def is_enabled_for_family(self, business_family_id: str) -> bool:
                return business_family_id == "sentence_order"

            def resolve(self, **kwargs):
                return {
                    "enabled": True,
                    "mode": "enforce",
                    "consensus": {
                        "status": "unanimous",
                        "neutral_signal_overrides": {
                            "opening_rule": "explicit_opening",
                            "opening_signal_strength": 0.88,
                        },
                        "business_feature_profile_overrides": {
                            "sentence_order_profile": {
                                "opening_rule": "explicit_opening",
                                "closing_rule": "summary_or_conclusion",
                            }
                        },
                    },
                }

        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.main_card_signal_resolver = _FakeResolver()
        pipeline._build_neutral_signal_profile = lambda **kwargs: {"opening_rule": "weak_opening", "opening_signal_strength": 0.31}  # type: ignore[method-assign]
        pipeline._build_business_feature_profile = lambda **kwargs: {"sentence_order_profile": {"opening_rule": "weak_opening", "closing_rule": "none"}}  # type: ignore[method-assign]
        pipeline._project_signal_profile = lambda **kwargs: {}  # type: ignore[method-assign]

        neutral, business, resolution = pipeline._resolve_main_card_profiles(
            article_context={"title": "鎺掑簭鏉愭枡"},
            candidate={"candidate_type": "weak_formal_order_group", "text": "鐢层€備箼銆備笝銆備竵銆傛垔銆傚繁銆?, "meta": {}},
            business_family_id="sentence_order",
            signal_layer={},
        )

        self.assertEqual(neutral["opening_rule"], "explicit_opening")
        self.assertIn("mechanical_signal_profile", neutral)
        self.assertEqual(neutral["mechanical_signal_profile"]["opening_rule"], "weak_opening")
        self.assertEqual(business["sentence_order_profile"]["closing_rule"], "summary_or_conclusion")
        self.assertEqual((resolution.get("consensus") or {}).get("status"), "unanimous")

    def test_main_card_family_landing_resolver_intersects_dual_judge_results_and_maps_center_to_title_selection(self) -> None:
        class _FakeProvider:
            def __init__(self) -> None:
                self.calls = 0

            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "selected_main_cards": ["center_understanding", "sentence_fill"],
                        "reason": "sample-a",
                        "evidence_summary": "sample-a",
                        "confidence": 0.81,
                    }
                return {
                    "selected_main_cards": ["center_understanding"],
                    "reason": "sample-b",
                    "evidence_summary": "sample-b",
                    "confidence": 0.77,
                }

        resolver = MainCardFamilyLandingResolver(
            provider=_FakeProvider(),
            llm_config={
                "main_card_family_landing": {
                    "enabled": True,
                    "mode": "enforce",
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{allowed_main_cards_json}",
                    "user_prompt_template": "{article_title}\n{material_text}\n{mechanical_v2_families_json}",
                    "runtime_families": {
                        "center_understanding": {
                            "label": "涓績鐞嗚В",
                            "goal": "褰掓棬",
                            "formal_unit_definition": "寮哄崟涓績",
                            "accept_definition": "鍙壙杞戒富鏃ㄩ",
                            "reject_definition": "涓婚鐩稿叧浣嗕笉鑱氱劍",
                        },
                        "sentence_fill": {
                            "label": "璇彞濉┖",
                            "goal": "slot",
                            "formal_unit_definition": "functional_slot_unit",
                            "accept_definition": "blank-value ready",
                            "reject_definition": "涓嶅€煎緱濉?,
                        },
                        "sentence_order": {
                            "label": "璇彞鎺掑簭",
                            "goal": "order",
                            "formal_unit_definition": "ordered/weak formal",
                            "accept_definition": "鏈夐『搴忛摼",
                            "reject_definition": "澶氳В",
                        },
                    },
                }
            },
        )

        article = type("Article", (), {"title": "鏀跨哗瑙傞棶棰樻槸涓€涓牴鏈€ч棶棰?, "source": "qstheory"})()
        material = type(
            "Material",
            (),
            {
                "id": "mat-1",
                "text": "鍏ㄦ枃鍥寸粫鏀跨哗瑙傚睍寮€銆?,
                "status": "gray",
                "release_channel": "gray",
                "quality_score": 0.82,
                "paragraph_count": 3,
                "sentence_count": 8,
                "primary_family": "涔辩爜鏃?,
                "primary_subtype": "",
                "parallel_families": [{"family": "姒傛嫭褰掔撼鍨?}],
                "family_scores": {"姒傛嫭褰掔撼鍨?: 0.72},
                "universal_profile": {"single_center_strength": 0.81},
                "feature_profile": {"theme_words": ["鏀跨哗瑙?]},
            },
        )()

        result = resolver.resolve(
            material=material,
            article=article,
            mechanical_v2_families=[],
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "intersected")
        self.assertEqual(result["consensus"]["selected_main_cards"], ["center_understanding"])
        self.assertEqual(result["runtime_families"], ["title_selection"])

    def test_main_card_dual_judge_treats_title_selection_as_center_understanding_alias(self) -> None:
        class _FakeProvider:
            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                return {
                    "decision": "accept",
                    "formal_layer": "strong",
                    "selected_material_card": "title_material.conclusion_focus",
                    "selected_business_card": "cause_effect__conclusion_focus__main_idea",
                    "reason": "涓昏酱鏀舵潫鎴愮珛",
                    "evidence_summary": "鍏ㄦ枃鍥寸粫鍚屼竴涓绘棬灞曞紑銆?,
                    "confidence": 0.9,
                }

        judge = MainCardDualJudge(
            provider=_FakeProvider(),
            llm_config={
                "main_card_dual_judge": {
                    "enabled": True,
                    "mode": "enforce",
                    "models": {"judge_a": "model-a", "judge_b": "model-b"},
                    "common_instructions": "{family_goal}",
                    "user_prompt_template": "{business_family_id}|{runtime_business_family_id}|{candidate_text}",
                    "families": {
                        "center_understanding": {
                            "label": "涓績鐞嗚В",
                            "goal": "鍒ゆ柇鏄惁涓轰腑蹇冪悊瑙ｆ寮忔壙杞藉崟鍏?,
                            "formal_unit_definition": "寮哄崟涓績褰掓棬鍗曞厓",
                            "strong_accept_definition": "涓昏酱寮烘敹鏉?,
                            "weak_accept_definition": "涓昏酱鍩烘湰鏀舵潫",
                            "reject_definition": "涓婚鐩稿叧浣嗕富鏃ㄤ笉绋?,
                        }
                    },
                }
            },
        )

        result = judge.adjudicate(
            business_family_id="title_selection",
            item={"text": "鏂囩珷鍥寸粫鍚屼竴涓绘棬灞曞紑銆?},
            question_card={"card_id": "question.center_understanding.standard_v1"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["business_family_id"], "center_understanding")
        self.assertEqual(result["runtime_business_family_id"], "title_selection")
        self.assertEqual(result["consensus"]["decision"], "accept")

    def test_main_card_dual_judge_uses_yaml_style_prompt_config_and_unanimous_consensus(self) -> None:
        class _FakeProvider:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def is_enabled(self) -> bool:
                return True

            def generate_json(self, *, model, instructions, input_payload):
                self.calls.append(
                    {
                        "model": model,
                        "instructions": instructions,
                        "prompt": input_payload["prompt"],
                    }
                )
                return {
                    "decision": "accept",
                    "formal_layer": "weak",
                    "selected_material_card": "order_material.dual_anchor_lock",
                    "selected_business_card": "sentence_order__head_tail_lock__abstract",
                    "reason": "鎺掑簭鎰忎箟鎴愮珛",
                    "evidence_summary": "棣栧彞鏂瑰悜鏄庣‘涓斿瓨鍦ㄥ眬閮ㄧ粦瀹氥€?,
                    "confidence": 0.83,
                }

        llm_config = {
            "main_card_dual_judge": {
                "enabled": True,
                "mode": "shadow",
                "consensus_rule": "unanimous",
                "use_full_card_catalog": True,
                "models": {"judge_a": "model-a", "judge_b": "model-b"},
                "common_instructions": "绯荤粺瑕佹眰锛歿family_goal}",
                "user_prompt_template": "涓诲崱={family_label}\nformal={formal_unit_definition}\n姝ｆ枃={candidate_text}",
                "families": {
                    "sentence_order": {
                        "label": "璇彞鎺掑簭",
                        "goal": "鍒ゆ柇鎺掑簭棰樻劅鏄惁鎴愮珛",
                        "formal_unit_definition": "ordered/weak ordered",
                        "strong_accept_definition": "strong",
                        "weak_accept_definition": "weak",
                        "reject_definition": "reject",
                    }
                },
            }
        }
        judge = MainCardDualJudge(provider=_FakeProvider(), llm_config=llm_config)
        result = judge.adjudicate(
            business_family_id="sentence_order",
            item={
                "candidate_type": "weak_formal_order_group",
                "text": "绗竴鍙ャ€傜浜屽彞銆傜涓夊彞銆?,
                "article_title": "鎺掑簭鏉愭枡",
                "material_card_id": "order_material.dual_anchor_lock",
                "selected_business_card": "sentence_order__head_tail_lock__abstract",
                "meta": {"ordering_tier": "weak_formal"},
                "neutral_signal_profile": {"unique_opener_score": 0.57},
                "selected_task_scoring": {"recommended": 0.61},
                "business_feature_profile": {"sentence_order_profile": {"closing_rule": "summary_or_conclusion"}},
                "eligible_material_cards": [{"card_id": "order_material.dual_anchor_lock", "score": 0.77}],
                "eligible_business_cards": [{"business_card_id": "sentence_order__head_tail_lock__abstract", "score": 0.72}],
                "question_ready_context": {"selected_material_card": "order_material.dual_anchor_lock"},
                "quality_score": 0.74,
            },
            question_card={"card_id": "question.sentence_order.standard_v1"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["consensus"]["status"], "unanimous")
        self.assertEqual(result["consensus"]["decision"], "accept")

    def test_main_card_dual_judge_supports_single_judge_mode(self) -> None:
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

    def test_main_card_signal_resolver_supports_single_judge_mode(self) -> None:
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

    def test_candidate_contract_types_map_title_selection_paragraph_window(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        contract_types = pipeline._candidate_contract_types(
            {"candidate_type": "paragraph_window"},
            business_family_id="title_selection",
        )
        self.assertIn("paragraph_window", contract_types)
        self.assertIn("multi_paragraph_unit", contract_types)

    def test_legacy_top_hit_is_not_replaced_by_arbitrary_llm_card_when_multiple_options_exist(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        original = {"card_id": "legacy.title_selection.precomputed", "generation_archetype": "legacy_material_fallback"}
        promoted = pipeline._maybe_promote_legacy_top_hit_from_llm_catalog(
            top_hit=original,
            llm_material_card_options=[
                {"card_id": "title_material.plain_main_recovery"},
                {"card_id": "title_material.problem_essence_judgement"},
            ],
        )
        self.assertEqual(promoted["card_id"], "legacy.title_selection.precomputed")
        self.assertIn("鍒ゆ柇鎺掑簭棰樻劅鏄惁鎴愮珛", provider_calls[0]["instructions"])

    def test_material_pipeline_v2_attach_main_card_dual_judge_adjudication_records_shadow_result(self) -> None:
        class _FakeJudge:
            def is_enabled_for_family(self, business_family_id: str) -> bool:
                return True

            def is_enforce_mode(self) -> bool:
                return False

            def use_full_card_catalog(self) -> bool:
                return True

            def consensus_allows_accept(self, adjudication) -> bool:
                return True

            def adjudicate(self, *, business_family_id, item, question_card):
                return {
                    "enabled": True,
                    "mode": "shadow",
                    "consensus": {
                        "status": "unanimous",
                        "decision": "accept",
                        "formal_layer": "weak",
                        "selected_material_card": "order_material.dual_anchor_lock",
                        "selected_business_card": "sentence_order__head_tail_lock__abstract",
                    },
                    "judge_results": [],
                }

        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.main_card_dual_judge = _FakeJudge()
        pipeline._score_material_cards = lambda **kwargs: [{"card_id": "order_material.dual_anchor_lock", "score": 0.77}]  # type: ignore[method-assign]
        pipeline._score_business_cards = lambda **kwargs: [{"business_card_id": "sentence_order__head_tail_lock__abstract", "score": 0.72}]  # type: ignore[method-assign]
        item = {
            "question_ready_context": {"selected_material_card": "order_material.dual_anchor_lock"},
            "local_profile": {},
        }

        updated = pipeline._attach_main_card_dual_judge_adjudication(
            item=item,
            business_family_id="sentence_order",
            question_card={"card_id": "question.sentence_order.standard_v1"},
            material_cards=[],
            business_cards=[],
            signal_profile={},
            neutral_signal_profile={},
            business_feature_profile={},
        )

        self.assertIn("llm_adjudication", updated)
        self.assertEqual(updated["question_ready_context"]["selected_material_card"], "order_material.dual_anchor_lock")
        self.assertEqual(updated["question_ready_context"]["llm_adjudication"]["mode"], "shadow")
        self.assertEqual(updated["local_profile"]["llm_adjudication"]["consensus_status"], "unanimous")

    def test_material_pipeline_v2_runtime_gate_respects_llm_enforce_accept(self) -> None:
        class _FakeJudge:
            def is_enforce_mode(self) -> bool:
                return True

            def consensus_allows_accept(self, adjudication) -> bool:
                return True

        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.main_card_dual_judge = _FakeJudge()

        passed, reason = pipeline._passes_runtime_material_gate(
            item={"llm_adjudication": {"consensus": {"status": "unanimous", "decision": "accept"}}},
            business_family_id="sentence_order",
            question_card={"card_id": "question.sentence_order.standard_v1"},
            min_card_score=0.55,
            min_business_card_score=0.45,
            require_business_card=True,
        )

        self.assertTrue(passed)
        self.assertEqual(reason, "")

    def test_material_pipeline_v2_apply_llm_adjudication_selection_overrides_selected_cards_in_enforce_mode(self) -> None:
        class _FakeJudge:
            def consensus_allows_accept(self, adjudication) -> bool:
                return True

        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline.main_card_dual_judge = _FakeJudge()
        updated = pipeline._apply_llm_adjudication_selection(
            {
                "material_card_id": "order_material.dual_anchor_lock",
                "selected_business_card": "sentence_order__head_tail_lock__abstract",
                "question_ready_context": {
                    "selected_material_card": "order_material.dual_anchor_lock",
                    "selected_business_card": "sentence_order__head_tail_lock__abstract",
                },
                "llm_adjudication": {
                    "consensus": {
                        "status": "unanimous",
                        "decision": "accept",
                        "formal_layer": "weak",
                        "selected_material_card": "order_material.problem_solution_case_blocks",
                        "selected_business_card": "sentence_order__problem_solution__abstract",
                    }
                },
                "llm_candidate_material_cards": [
                    {"card_id": "order_material.dual_anchor_lock", "score": 0.55},
                    {"card_id": "order_material.problem_solution_case_blocks", "score": 0.31},
                ],
                "llm_candidate_business_cards": [
                    {"business_card_id": "sentence_order__head_tail_lock__abstract", "score": 0.57},
                    {"business_card_id": "sentence_order__problem_solution__abstract", "score": 0.29},
                ],
            }
        )

        self.assertEqual(updated["material_card_id"], "order_material.problem_solution_case_blocks")
        self.assertEqual(updated["selected_business_card"], "sentence_order__problem_solution__abstract")
        self.assertEqual(updated["question_ready_context"]["selected_material_card"], "order_material.problem_solution_case_blocks")

    def test_material_pipeline_v2_continuation_contract_types_align_common_precompute_shapes(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        paragraph_window_types = pipeline._candidate_contract_types(
            {"candidate_type": "paragraph_window"},
            business_family_id="continuation",
        )
        sentence_group_types = pipeline._candidate_contract_types(
            {"candidate_type": "sentence_group"},
            business_family_id="continuation",
        )
        single_paragraph_types = pipeline._candidate_contract_types(
            {"candidate_type": "single_paragraph"},
            business_family_id="continuation",
        )
        title_paragraph_window_types = pipeline._candidate_contract_types(
            {"candidate_type": "paragraph_window"},
            business_family_id="title_selection",
        )

        self.assertIn("multi_paragraph_unit", paragraph_window_types)
        self.assertIn("closed_span", sentence_group_types)
        self.assertIn("closed_span", single_paragraph_types)
        self.assertIn("multi_paragraph_unit", title_paragraph_window_types)

    def test_material_v2_index_service_build_cached_item_uses_family_specific_runtime_flags(self) -> None:
        service = MaterialV2IndexService.__new__(MaterialV2IndexService)
        service.pipeline = _FakePipeline()
        service.main_card_family_landing = type("_Resolver", (), {"resolve": staticmethod(lambda **kwargs: None)})()

        service._build_cached_item(material="material", article="article", family="sentence_fill")
        service._build_cached_item(material="material", article="article", family="sentence_order")
        service._build_cached_item(material="material", article="article", family="title_selection")

        self.assertTrue(service.pipeline.calls[0]["enable_fill_formalization_bridge"])
        self.assertFalse(service.pipeline.calls[0]["enable_sentence_order_weak_formal_bridge"])

        self.assertFalse(service.pipeline.calls[1]["enable_fill_formalization_bridge"])
        self.assertTrue(service.pipeline.calls[1]["enable_sentence_order_weak_formal_bridge"])
        self.assertTrue(service.pipeline.calls[1]["enable_sentence_order_weak_formal_gate"])
        self.assertTrue(service.pipeline.calls[1]["enable_sentence_order_weak_formal_closing_gate"])
        self.assertTrue(service.pipeline.calls[1]["enable_sentence_order_strong_formal_demote"])

        self.assertFalse(service.pipeline.calls[2]["enable_fill_formalization_bridge"])
        self.assertFalse(service.pipeline.calls[2]["enable_sentence_order_weak_formal_bridge"])
        self.assertFalse(service.pipeline.calls[2]["enable_sentence_order_strong_formal_demote"])

    def test_material_v2_index_service_prefers_llm_landing_for_main_cards_and_keeps_non_main_mechanical_families(self) -> None:
        service = MaterialV2IndexService.__new__(MaterialV2IndexService)
        service.family_to_v2 = {
            "姒傛嫭褰掔撼鍨?: "title_selection",
            "灏炬缁啓鍨?: "continuation",
        }
        service.main_card_family_landing = type(
            "_Resolver",
            (),
            {
                "resolve": staticmethod(
                    lambda **kwargs: {
                        "runtime_families": ["sentence_order"],
                        "consensus": {"status": "unanimous", "selected_main_cards": ["sentence_order"]},
                    }
                )
            },
        )()

        material = type(
            "Material",
            (),
            {
                "primary_family": "姒傛嫭褰掔撼鍨?,
                "parallel_families": [{"family": "灏炬缁啓鍨?}],
            },
        )()

        families = service._resolve_v2_families(material=material, article=object())
        self.assertEqual(families, ["continuation", "sentence_order"])

    def test_material_pipeline_v2_demotes_strong_formal_sentence_order_candidate_when_pairwise_is_weak_and_risk_is_high(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline._build_sentence_order_scoring = lambda *, signal_profile, candidate: {  # type: ignore[method-assign]
            "structure_scores": {
                "pairwise_constraint_score": 0.54,
            }
        }
        candidate = {
            "candidate_type": "ordered_unit_group",
            "meta": {
                "ordering_tier": "strong_formal",
                "ordering_reason_trace": {"ordering_reason": "ordered_unit_group_ready"},
            },
            "text": "A\nB\nC\nD\nE\nF",
        }
        demoted = pipeline._maybe_demote_sentence_order_strong_formal_candidate(
            candidate=candidate,
            signal_profile={
                "exchange_risk": 0.45,
                "multi_path_risk": 0.47,
                "function_overlap_score": 0.52,
            },
        )

        self.assertEqual(demoted["candidate_type"], "weak_formal_order_group")
        self.assertEqual(demoted["meta"]["ordering_tier"], "weak_formal")
        self.assertEqual(demoted["meta"]["weak_formal_reason"], "strong_formal_pairwise_demoted")

    def test_material_pipeline_v2_keeps_strong_formal_sentence_order_candidate_when_pairwise_is_stable(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        pipeline._build_sentence_order_scoring = lambda *, signal_profile, candidate: {  # type: ignore[method-assign]
            "structure_scores": {
                "pairwise_constraint_score": 0.61,
            }
        }
        candidate = {
            "candidate_type": "ordered_unit_group",
            "meta": {"ordering_tier": "strong_formal"},
            "text": "A\nB\nC\nD\nE\nF",
        }
        kept = pipeline._maybe_demote_sentence_order_strong_formal_candidate(
            candidate=candidate,
            signal_profile={
                "exchange_risk": 0.46,
                "multi_path_risk": 0.48,
                "function_overlap_score": 0.52,
            },
        )

        self.assertIs(kept, candidate)

    def test_tag_service_precompute_v2_index_for_article_triggers_backfill_and_audit(self) -> None:
        observed_payloads: list[dict] = []
        observed_audits: list[tuple[str, str, str, dict]] = []

        class _FakeV2IndexService:
            def __init__(self, session) -> None:
                self.session = session

            def precompute(self, payload: dict) -> dict:
                observed_payloads.append(payload)
                return {
                    "index_version": "test-v2",
                    "material_count": 3,
                    "updated_count": 2,
                    "skipped_count": 1,
                    "families": {"title_selection": 2},
                }

        service = TagService.__new__(TagService)
        service.session = object()
        service.audit_repo = type(
            "_AuditRepo",
            (),
            {
                "log": staticmethod(
                    lambda entity_type, entity_id, event_type, payload: observed_audits.append(
                        (entity_type, entity_id, event_type, payload)
                    )
                )
            },
        )()

        with patch.object(tag_service_module, "MaterialV2IndexService", _FakeV2IndexService):
            result = service._precompute_v2_index_for_article(
                article_id="article-1",
                created_material_ids=["mat-1", "mat-2"],
            )

        self.assertEqual(observed_payloads, [{"article_ids": ["article-1"], "primary_only": True}])
        self.assertTrue(result["triggered"])
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(
            observed_audits,
            [
                (
                    "article",
                    "article-1",
                    "v2_precompute",
                    {
                        "triggered": True,
                        "index_version": "test-v2",
                        "material_count": 3,
                        "updated_count": 2,
                        "skipped_count": 1,
                        "families": {"title_selection": 2},
                    },
                )
            ],
        )

    def test_tag_service_replace_existing_primary_materials_skips_demote_when_no_new_materials(self) -> None:
        observed_calls: list[tuple[str, object]] = []
        observed_audits: list[tuple[str, str, str, dict]] = []

        service = TagService.__new__(TagService)
        service.material_repo = type(
            "_MaterialRepo",
            (),
            {
                "demote_existing_for_article": staticmethod(
                    lambda article_id, exclude_material_ids=None: observed_calls.append(
                        (article_id, exclude_material_ids)
                    )
                )
            },
        )()
        service.audit_repo = type(
            "_AuditRepo",
            (),
            {
                "log": staticmethod(
                    lambda entity_type, entity_id, event_type, payload: observed_audits.append(
                        (entity_type, entity_id, event_type, payload)
                    )
                )
            },
        )()

        result = service._replace_existing_primary_materials(
            article_id="article-1",
            created_material_ids=[],
        )

        self.assertEqual(observed_calls, [])
        self.assertEqual(result, {"triggered": False, "demoted_count": 0})
        self.assertEqual(
            observed_audits,
            [
                (
                    "article",
                    "article-1",
                    "primary_replace_skipped",
                    {
                        "reason": "no_created_materials",
                        "created_material_count": 0,
                    },
                )
            ],
        )

    def test_tag_service_replace_existing_primary_materials_demotes_only_old_materials(self) -> None:
        observed_calls: list[tuple[str, object]] = []
        observed_audits: list[tuple[str, str, str, dict]] = []

        service = TagService.__new__(TagService)
        service.material_repo = type(
            "_MaterialRepo",
            (),
            {
                "demote_existing_for_article": staticmethod(
                    lambda article_id, exclude_material_ids=None: (
                        observed_calls.append((article_id, exclude_material_ids)) or 3
                    )
                )
            },
        )()
        service.audit_repo = type(
            "_AuditRepo",
            (),
            {
                "log": staticmethod(
                    lambda entity_type, entity_id, event_type, payload: observed_audits.append(
                        (entity_type, entity_id, event_type, payload)
                    )
                )
            },
        )()

        result = service._replace_existing_primary_materials(
            article_id="article-2",
            created_material_ids=["mat-1", "mat-2"],
        )

        self.assertEqual(observed_calls, [("article-2", ["mat-1", "mat-2"])])
        self.assertEqual(result, {"triggered": True, "demoted_count": 3})
        self.assertEqual(
            observed_audits,
            [
                (
                    "article",
                    "article-2",
                    "primary_replace",
                    {
                        "triggered": True,
                        "created_material_count": 2,
                        "demoted_count": 3,
                    },
                )
            ],
        )

    def test_material_pipeline_v2_builds_llm_full_material_card_catalog_without_mechanical_scores(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        candidate = {"candidate_type": "weak_formal_order_group", "text": "A\nB\nC\nD\nE\nF", "meta": {}}
        material_cards = [
            {
                "card_id": "order_material.dual_anchor_lock",
                "display_name": "棣栧熬閿佸畾",
                "default_generation_archetype": "head_tail",
                "selection_core": "anchors",
                "candidate_contract": {"allowed_candidate_types": ["sentence_block_group", "paragraph_window"]},
            },
            {
                "card_id": "order_material.problem_solution_case_blocks",
                "display_name": "闂瑙ｅ喅",
                "default_generation_archetype": "problem_solution",
                "selection_core": "problem_solution",
                "candidate_contract": {"allowed_candidate_types": ["sentence_group"]},
            },
        ]

        options = pipeline._build_llm_material_card_catalog(
            material_cards=material_cards,
            candidate=candidate,
            business_family_id="sentence_order",
        )

        self.assertEqual(len(options), 2)
        self.assertEqual(options[0]["score"], 0.0)
        self.assertEqual(options[0]["reason"], "llm_full_catalog")

    def test_material_pipeline_v2_builds_llm_full_business_card_catalog_without_threshold_gate(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        business_cards = [
            {
                "card_meta": {
                    "business_card_id": "sentence_fill__opening_summary__abstract",
                    "display_name": "寮€绡囨€荤粨",
                },
                "slot_projection": {
                    "question_type": "sentence_fill",
                    "business_subtype": "sentence_fill",
                    "pattern_candidates": ["opening_summary"],
                    "type_slots": {"slot_role": "opening"},
                    "prompt_extras": {"slot_function": "summary"},
                },
                "feature_signature": {"slot_role": "opening"},
            }
        ]

        options = pipeline._build_llm_business_card_catalog(
            business_cards=business_cards,
            business_feature_profile={},
        )

        self.assertEqual(len(options), 1)
        self.assertEqual(options[0]["business_card_id"], "sentence_fill__opening_summary__abstract")
        self.assertEqual(options[0]["score"], 0.0)
        self.assertEqual(options[0]["reason"], "llm_full_catalog")


if __name__ == "__main__":
    unittest.main()

