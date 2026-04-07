from __future__ import annotations

import sys
import types
from unittest import TestCase
from unittest.mock import Mock, patch


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

from app.core.exceptions import DomainError
from app.schemas.runtime import MaterialsConfig
from app.services import material_bridge_v2 as bridge_module
from app.services.material_bridge_v2 import MaterialBridgeV2Service


class MaterialBridgeV2UnitTest(TestCase):
    def setUp(self) -> None:
        self.registry = {
            "cards_by_id": {
                "question.title_selection.standard_v1": {
                    "card_id": "question.title_selection.standard_v1",
                    "business_family_id": "title_selection",
                    "runtime_binding": {"question_type": "main_idea", "business_subtype": "title_selection"},
                },
                "question.sentence_order.standard_v1": {
                    "card_id": "question.sentence_order.standard_v1",
                    "business_family_id": "sentence_order",
                    "runtime_binding": {"question_type": "sentence_order", "business_subtype": None},
                },
            },
            "cards_by_runtime_binding": {
                ("main_idea", "title_selection"): [
                    {
                        "card_id": "question.title_selection.standard_v1",
                        "business_family_id": "title_selection",
                        "runtime_binding": {"question_type": "main_idea", "business_subtype": "title_selection"},
                    }
                ],
                ("sentence_order", None): [
                    {
                        "card_id": "question.sentence_order.standard_v1",
                        "business_family_id": "sentence_order",
                        "runtime_binding": {"question_type": "sentence_order", "business_subtype": None},
                    }
                ],
            },
        }
        self.registry_patcher = patch.object(bridge_module, "QuestionCardBindingService")
        binding_service_cls = self.registry_patcher.start()
        binding_service_cls.return_value.resolve.side_effect = self._resolve_binding
        self.service = MaterialBridgeV2Service(
            MaterialsConfig(base_url="http://127.0.0.1:8001")
        )

    def tearDown(self) -> None:
        self.registry_patcher.stop()

    def _resolve_binding(
        self,
        *,
        question_card_id: str | None = None,
        question_type: str | None = None,
        business_subtype: str | None = None,
        require_match: bool = False,
    ) -> dict:
        if question_card_id:
            card = self.registry["cards_by_id"].get(question_card_id)
            if card is None:
                raise DomainError("Unknown question_card_id.", status_code=422)
            return {
                "question_card_id": question_card_id,
                "question_card": card,
                "runtime_binding": card["runtime_binding"],
                "binding_source": "explicit_question_card_id",
                "binding_reason": "explicit_question_card_id",
                "warning": None,
            }
        matches = self.registry["cards_by_runtime_binding"].get((question_type, business_subtype), [])
        if len(matches) == 1:
            card = matches[0]
            return {
                "question_card_id": card["card_id"],
                "question_card": card,
                "runtime_binding": card["runtime_binding"],
                "binding_source": "runtime_binding_lookup",
                "binding_reason": "question_card.runtime_binding",
                "warning": None,
            }
        raise DomainError("No normalized question card matches the requested runtime binding.", status_code=422)

    def test_resolve_business_family_id_maps_title_selection(self) -> None:
        binding = self.service._resolve_question_card_binding(
            question_type="main_idea",
            business_subtype="title_selection",
        )
        family_id = self.service._resolve_business_family_id(binding)
        self.assertEqual(family_id, "title_selection")

    def test_resolve_business_family_id_uses_explicit_question_card(self) -> None:
        binding = self.service._resolve_question_card_binding(
            question_type="main_idea",
            business_subtype=None,
            question_card_id="question.title_selection.standard_v1",
        )
        family_id = self.service._resolve_business_family_id(binding)
        self.assertEqual(family_id, "title_selection")

    def test_resolve_business_family_id_rejects_unbound_main_idea_subtype(self) -> None:
        with self.assertRaises(DomainError):
            self.service._resolve_question_card_binding(
                question_type="main_idea",
                business_subtype="center_understanding",
            )

    def test_to_material_selection_adapts_v2_candidate_shape(self) -> None:
        item = {
            "candidate_id": "article-1:whole_passage:1",
            "article_id": "article-1",
            "text": "示例材料",
            "consumable_text": "示例材料（可消费）",
            "source": {"source_name": "old"},
            "candidate_type": "whole_passage",
            "quality_score": 0.82,
            "article_profile": {"document_genre": "评论议论", "discourse_shape": "转折归旨"},
            "local_profile": {"context_dependency": 0.2, "core_object": "食虫植物"},
            "eligible_material_cards": [{"card_id": "title_material.single_object_exposition", "score": 0.91}],
            "question_ready_context": {
                "selected_material_card": "title_material.single_object_exposition",
                "generation_archetype": "single_object",
            },
        }

        result = self.service._to_material_selection(item, "test_reason")

        self.assertEqual(result.material_id, "article-1:whole_passage:1")
        self.assertEqual(result.text, "示例材料（可消费）")
        self.assertEqual(result.primary_label, "title_material.single_object_exposition")
        self.assertEqual(result.material_structure_reason, "single_object")
        self.assertEqual(result.fit_scores["title_material.single_object_exposition"], 0.91)

    def test_search_candidates_keeps_preferred_business_cards_as_soft_hint(self) -> None:
        payloads: list[dict] = []

        def fake_post(payload: dict) -> dict:
            payloads.append(dict(payload))
            if len(payloads) == 1:
                return {"items": []}
            return {"items": [{"candidate_id": "mat-1"}]}

        self.service._post_v2_search = Mock(side_effect=fake_post)

        items = self.service._search_candidates(
            business_family_id="title_selection",
            question_card_id="question.title_selection.standard_v1",
            article_ids=[],
            article_limit=12,
            candidate_limit=8,
            min_card_score=0.55,
            business_card_ids=["theme_word_focus__main_idea"],
            preferred_business_card_ids=["turning_relation_focus__main_idea"],
            query_terms=["主题"],
            target_length=220,
            length_tolerance=120,
            structure_constraints={},
            enable_anchor_adaptation=True,
        )

        self.assertEqual(items, [{"candidate_id": "mat-1"}])
        self.assertEqual(payloads[0]["business_card_ids"], ["theme_word_focus__main_idea"])
        self.assertEqual(payloads[0]["preferred_business_card_ids"], ["turning_relation_focus__main_idea"])
        self.assertEqual(payloads[1]["business_card_ids"], [])
        self.assertEqual(payloads[1]["preferred_business_card_ids"], ["turning_relation_focus__main_idea"])

    def test_structure_constraints_do_not_hard_reject_without_explicit_business_card(self) -> None:
        result = self.service._score_candidate(
            {
                "quality_score": 0.8,
                "text": "示例材料",
                "article_profile": {},
                "local_profile": {},
                "retrieval_match_profile": {},
                "business_feature_profile": {
                    "sentence_order_profile": {
                        "unit_count": 5,
                    }
                },
                "question_ready_context": {},
                "business_card_recommendations": [],
            },
            topic=None,
            text_direction=None,
            document_genre=None,
            material_structure_label=None,
            material_policy=None,
            has_explicit_question_card=False,
            requested_business_card_ids=[],
            structure_constraints={
                "sortable_unit_count": 6,
                "preserve_unit_count": True,
            },
            query_terms=[],
            target_length=None,
        )

        self.assertGreater(result["score"], -999.0)
        self.assertIn("sentence_order_unit_count_penalty", result["reason"])
