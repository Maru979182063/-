from __future__ import annotations

import sys
import types
from unittest import TestCase
from types import SimpleNamespace


if "yaml" not in sys.modules:
    yaml = types.ModuleType("yaml")
    yaml.safe_load = lambda *args, **kwargs: {}
    sys.modules["yaml"] = yaml

if "pydantic_settings" not in sys.modules:
    pydantic_settings = types.ModuleType("pydantic_settings")

    class BaseSettings:  # pragma: no cover - test stub
        def __init__(self, *args, **kwargs) -> None:
            pass

    def SettingsConfigDict(**kwargs):
        return dict(kwargs)

    pydantic_settings.BaseSettings = BaseSettings
    pydantic_settings.SettingsConfigDict = SettingsConfigDict
    sys.modules["pydantic_settings"] = pydantic_settings

if "app.domain.services._common" not in sys.modules:
    common = types.ModuleType("app.domain.services._common")

    class ServiceBase:  # pragma: no cover - test stub
        def __init__(self, session=None) -> None:
            self.session = session

    common.ServiceBase = ServiceBase
    sys.modules["app.domain.services._common"] = common


from app.services.material_pipeline_v2 import MaterialPipelineV2
from app.services.card_registry_v2 import CardRegistryV2
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service


class BusinessFeatureRoutingUnitTest(TestCase):
    def test_business_slot_projection_prefers_card_strategy_map(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        card = {
            "slot_projection": {
                "question_type": "sentence_order",
                "business_subtype": None,
                "pattern_candidates": ["carry_parallel_expand"],
                "type_slots": {
                    "closing_signal_strength": "medium",
                },
                "prompt_extras": {
                    "business_feature_card_id": "sentence_order__head_tail_logic__abstract",
                },
                "slot_strategy_map": {
                    "head_tail_lock": {
                        "when": {
                            "opening_rule": "explicit_opening",
                            "closing_rule": "summary_or_conclusion",
                        },
                        "pattern_candidates": ["dual_anchor_lock"],
                        "type_slots": {
                            "closing_signal_strength": "high",
                        },
                    }
                },
            }
        }

        projection = pipeline._resolve_business_slot_projection(
            card,
            {
                "sentence_order_profile": {
                    "opening_rule": "explicit_opening",
                    "closing_rule": "summary_or_conclusion",
                    "binding_rules": [],
                    "logic_modes": [],
                },
                "sentence_fill_profile": {},
            },
        )

        self.assertEqual(projection["pattern_candidates"], ["dual_anchor_lock"])
        self.assertEqual(projection["type_slots"]["closing_signal_strength"], "high")
        self.assertEqual(projection["prompt_extras"]["business_feature_strategy_ids"], ["head_tail_lock"])

    def test_select_primary_business_card_keeps_top_ranked_hit(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        selected = pipeline._select_primary_business_card(
            [
                {"business_card_id": "theme_word_focus__main_idea", "score": 0.71},
                {"business_card_id": "turning_relation_focus__main_idea", "score": 0.69},
            ],
            {
                "turning_focus_strength": 0.92,
                "cause_effect_strength": 0.0,
                "necessary_condition_strength": 0.0,
                "parallel_enumeration_strength": 0.0,
            },
        )

        self.assertEqual(selected["business_card_id"], "theme_word_focus__main_idea")

    def test_sentence_fill_scoring_prefers_card_projection_over_card_id_mapping(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        score = pipeline._score_sentence_fill_business_card(
            {
                "card_meta": {
                    "business_card_id": "custom.fill.card",
                },
                "slot_projection": {
                    "pattern_candidates": ["bridge_transition", "middle_focus_shift"],
                    "type_slots": {
                        "blank_position": "middle",
                        "logic_relation": "transition",
                        "bidirectional_validation": "low",
                    },
                    "prompt_extras": {
                        "business_core_rule": "空句主要负责把话题引向后文展开",
                    },
                },
            },
            {
                "sentence_fill_profile": {
                    "blank_position": "middle",
                    "function_type": "lead_next",
                    "backward_link_strength": 0.2,
                    "forward_link_strength": 0.8,
                    "bidirectional_validation": 0.3,
                    "countermeasure_signal_strength": 0.0,
                    "reference_dependency": 0.2,
                }
            },
        )

        self.assertGreater(score, 0.6)

    def test_sentence_fill_expected_profile_prefers_explicit_business_function(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        profile = pipeline._sentence_fill_expected_profile(
            {
                "feature_signature": {
                    "business_function": "carry_previous",
                },
                "slot_projection": {
                    "pattern_candidates": ["middle_focus_shift"],
                    "type_slots": {
                        "blank_position": "middle",
                        "logic_relation": "transition",
                        "bidirectional_validation": "high",
                    },
                    "prompt_extras": {
                        "business_core_rule": "空句负责把话题引向后文展开",
                    },
                },
            }
        )

        self.assertEqual(
            profile,
            {
                "blank_position": "middle",
                "business_function": "carry_previous",
            },
        )

    def test_sentence_order_scoring_prefers_card_formal_fields_over_card_id(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        score = pipeline._score_sentence_order_business_card(
            {
                "card_meta": {
                    "business_card_id": "custom.order.card",
                },
                "feature_signature": {
                    "relation_type": "行文逻辑",
                },
                "slot_projection": {
                    "pattern_candidates": ["viewpoint_reason_action", "dual_anchor_lock"],
                    "type_slots": {
                        "middle_structure_type": "cause_effect_chain",
                    },
                },
            },
            {
                "sentence_order_profile": {
                    "unit_count": 6,
                    "opening_rule": "explicit_opening",
                    "closing_rule": "summary_or_conclusion",
                    "binding_rules": [],
                    "logic_modes": ["discourse_logic", "viewpoint_explanation"],
                    "opening_signal_strength": 0.7,
                    "closing_signal_strength": 0.7,
                    "local_binding_strength": 0.4,
                    "sequence_integrity": 0.8,
                    "unique_opener_score": 0.5,
                    "binding_pair_count": 2,
                    "exchange_risk": 0.1,
                    "function_overlap_score": 0.1,
                    "multi_path_risk": 0.1,
                    "discourse_progression_strength": 0.8,
                    "context_closure_score": 0.8,
                    "temporal_order_strength": 0.0,
                    "action_sequence_irreversibility": 0.0,
                }
            },
        )

        self.assertGreater(score, 0.6)

    def test_sentence_order_scoring_mode_prefers_explicit_formal_field(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        scoring_mode = pipeline._sentence_order_scoring_mode(
            {
                "card_meta": {
                    "business_card_id": "sentence_order__timeline_action_sequence__abstract",
                },
                "feature_signature": {
                    "sentence_order_scoring_mode": "head_tail_lock",
                    "relation_type": "时间与行动顺序",
                },
                "slot_projection": {
                    "pattern_candidates": ["carry_parallel_expand"],
                    "type_slots": {
                        "opening_signal_strength": "medium",
                        "closing_signal_strength": "medium",
                    },
                },
            }
        )

        self.assertEqual(scoring_mode, "head_tail_lock")

    def test_sentence_order_scoring_mode_falls_back_only_when_formal_field_missing(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        scoring_mode = pipeline._sentence_order_scoring_mode(
            {
                "card_meta": {
                    "business_card_id": "legacy.order.card",
                },
                "feature_signature": {
                    "relation_type": "时间与行动顺序",
                },
                "slot_projection": {
                    "pattern_candidates": ["carry_parallel_expand"],
                    "type_slots": {},
                },
            }
        )

        self.assertEqual(scoring_mode, "timeline_action_sequence")

    def test_soft_relation_match_uses_canonical_relation_fields_not_display_name(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
        score = pipeline._soft_relation_match(
            {
                "card_meta": {
                    "display_name": "通用业务卡",
                },
                "feature_signature": {
                    "relation_type": "因果关系",
                },
                "canonical_projection": {
                    "expected_universal_profile": {
                        "logic_relations": ["因果"],
                    },
                    "expected_business_fields": {
                        "feature_type": "因果关系",
                    },
                },
            },
            {"因果"},
            {
                "cause_effect_strength": 0.76,
            },
            {
                "explicit_marker_hits": {
                    "turning_markers": [],
                    "cause_markers": ["因为"],
                    "conclusion_markers": ["因此"],
                    "necessary_condition_markers": [],
                    "countermeasure_markers": [],
                    "parallel_markers": [],
                },
                "conclusion_position": "tail_or_late",
            },
        )

        self.assertGreaterEqual(score, 0.9)

    def test_business_card_relation_families_prefers_explicit_formal_field(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        families = pipeline._business_card_relation_families(
            {
                "feature_signature": {
                    "relation_family": "并列",
                    "relation_type": "因果关系",
                },
                "canonical_projection": {
                    "expected_universal_profile": {
                        "logic_relations": ["因果"],
                    },
                    "expected_business_fields": {
                        "feature_type": "因果关系",
                    },
                },
            }
        )

        self.assertEqual(families, {"并列"})

    def test_business_card_relation_families_fall_back_only_when_formal_field_missing(self) -> None:
        pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)

        families = pipeline._business_card_relation_families(
            {
                "feature_signature": {
                    "relation_type": "因果关系",
                },
                "canonical_projection": {
                    "expected_universal_profile": {
                        "logic_relations": ["因果"],
                    },
                    "expected_business_fields": {
                        "feature_type": "因果关系",
                    },
                },
            }
        )

        self.assertEqual(families, {"因果"})

    def test_registry_excludes_runtime_ineligible_business_cards(self) -> None:
        registry = CardRegistryV2.__new__(CardRegistryV2)
        registry.payload = {
            "business_cards": {
                "sentence_fill__opening_summary__abstract": {
                    "card_meta": {
                        "business_card_id": "sentence_fill__opening_summary__abstract",
                        "mother_family_id": "sentence_fill",
                        "enabled": True,
                        "runtime_eligible": True,
                    }
                },
                "sentence_fill__position_function__abstract": {
                    "card_meta": {
                        "business_card_id": "sentence_fill__position_function__abstract",
                        "mother_family_id": "sentence_fill",
                        "enabled": True,
                        "runtime_eligible": False,
                    }
                },
            }
        }

        cards = registry.get_business_cards("sentence_fill", runtime_question_type="sentence_fill")

        self.assertEqual(
            [card["card_meta"]["business_card_id"] for card in cards],
            ["sentence_fill__opening_summary__abstract"],
        )

    def test_cached_search_does_not_hard_gate_preferred_structure_constraints(self) -> None:
        service = MaterialPipelineV2Service.__new__(MaterialPipelineV2Service)
        service.material_repo = SimpleNamespace(
            list_v2_cached=lambda **kwargs: [
                SimpleNamespace(
                    article_id="article-1",
                    last_used_at=None,
                    quality_score=0.86,
                    v2_index_payload={
                        "sentence_order": {
                            "text": "示例材料",
                            "original_text": "示例材料",
                            "article_title": "示例标题",
                            "quality_score": 0.86,
                            "business_feature_profile": {
                                "sentence_order_profile": {
                                    "unit_count": 5,
                                    "logic_modes": [],
                                    "binding_rules": [],
                                }
                            },
                            "question_ready_context": {
                                "selected_business_card": "sentence_order__head_tail_logic__abstract",
                            },
                            "business_card_recommendations": ["sentence_order__head_tail_logic__abstract"],
                        }
                    },
                )
            ]
        )
        service.pipeline = SimpleNamespace(
            refresh_cached_item=lambda **kwargs: kwargs["cached_item"],
            registry=SimpleNamespace(
                get_default_question_card=lambda business_family_id: {
                    "card_id": "question.sentence_order.standard_v1",
                    "business_family_id": "sentence_order",
                    "business_subtype_id": "standard",
                    "runtime_binding": {"question_type": "sentence_order", "business_subtype": None},
                },
                get_business_cards=lambda *args, **kwargs: [],
            ),
            _select_diverse_items=lambda items, limit: items[:limit],
            INDEX_VERSION="test",
        )

        result = service._search_cached(
            {
                "business_family_id": "sentence_order",
                "preferred_business_card_ids": ["sentence_order__head_tail_logic__abstract"],
                "structure_constraints": {
                    "sortable_unit_count": 6,
                    "preserve_unit_count": True,
                },
                "candidate_limit": 5,
            }
        )

        self.assertIsNotNone(result)
        self.assertEqual(len(result["items"]), 1)
