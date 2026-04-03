from unittest import TestCase

from app.schemas.runtime import MaterialsConfig
from app.services.material_bridge_v2 import MaterialBridgeV2Service


class MaterialBridgeV2UnitTest(TestCase):
    def setUp(self) -> None:
        self.service = MaterialBridgeV2Service(
            MaterialsConfig(base_url="http://127.0.0.1:8001")
        )

    def test_resolve_business_family_id_maps_title_selection(self) -> None:
        family_id = self.service._resolve_business_family_id("main_idea", "title_selection")
        self.assertEqual(family_id, "title_selection")

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
