from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from app.core.exceptions import ConflictError
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service
from app.domain.services.pool_service import PoolService
from scripts.expand_material_pool import promote_gray_materials


class _FakeRegistry:
    def get_default_question_card(self, business_family_id: str) -> dict:
        return {
            "card_id": f"question.{business_family_id}.default",
            "business_family_id": business_family_id,
            "business_subtype_id": None,
            "runtime_binding": {},
        }

    def get_business_cards(self, business_family_id: str, runtime_question_type=None, runtime_business_subtype=None) -> list[dict]:
        return []


class _FakePipeline:
    INDEX_VERSION = "test-index"

    def __init__(self) -> None:
        self.registry = _FakeRegistry()

    def refresh_cached_item(self, *, cached_item, **kwargs):
        return dict(cached_item)

    def _select_diverse_items(self, items, candidate_limit):
        return items[:candidate_limit]


def _cached_material(material_id: str, article_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=material_id,
        article_id=article_id,
        v2_index_payload={
            "continuation": {
                "candidate_id": material_id,
                "article_id": article_id,
                "text": f"text-{material_id}",
                "original_text": f"text-{material_id}",
                "article_title": f"title-{article_id}",
                "question_ready_context": {},
                "business_card_recommendations": [],
                "quality_score": 0.8,
            }
        },
        last_used_at=datetime(2026, 4, 8, tzinfo=timezone.utc),
        quality_score=0.8,
    )


class MaterialGovernanceUnitTest(TestCase):
    def test_material_pipeline_v2_search_defaults_to_stable_reviewed_materials(self) -> None:
        session = Mock()
        session.execute.return_value.all.return_value = [
            ("mat-pending", "review_pending"),
            ("mat-auto", "auto_tagged"),
            ("mat-confirmed", "review_confirmed"),
        ]
        service = MaterialPipelineV2Service(session)
        service.pipeline = _FakePipeline()
        service.material_repo.list_v2_cached = Mock(
            return_value=[
                _cached_material("mat-pending", "article-1"),
                _cached_material("mat-auto", "article-2"),
                _cached_material("mat-confirmed", "article-3"),
            ]
        )

        result = service.search({"business_family_id": "continuation", "candidate_limit": 5})

        service.material_repo.list_v2_cached.assert_called_once_with(
            business_family_id="continuation",
            material_ids=None,
            article_ids=None,
            status="promoted",
            release_channel="stable",
            limit=80,
        )
        self.assertEqual([item["candidate_id"] for item in result["items"]], ["mat-auto", "mat-confirmed"])
        self.assertEqual([item["review_status"] for item in result["items"]], ["auto_tagged", "review_confirmed"])

    def test_pool_service_blocks_review_pending_material_from_stable_promotion(self) -> None:
        session = Mock()
        session.scalar.return_value = "review_pending"
        service = PoolService(session)
        service.material_repo.update_status = Mock()

        with self.assertRaises(ConflictError):
            service.promote("mat-1", "promoted", "stable")

        service.material_repo.update_status.assert_not_called()

    def test_material_pipeline_v2_search_marks_article_fallback_as_degraded(self) -> None:
        session = Mock()
        service = MaterialPipelineV2Service(session)
        service._search_cached = Mock(return_value=None)
        service._apply_external_fallback_if_needed = Mock(side_effect=lambda *, payload, base_result: base_result)
        service.article_repo.list = Mock(return_value=[SimpleNamespace(id="article-1")])
        service.pipeline.search = Mock(return_value={"items": [{"candidate_id": "article-1:candidate-1"}], "warnings": []})

        result = service.search({"business_family_id": "continuation", "candidate_limit": 5})

        self.assertFalse(result["cache_hit"])
        self.assertEqual(result["result_mode"], "article_fallback")
        self.assertEqual(result["governance_status"], "degraded_unreviewed_source")
        self.assertIn(
            "reviewed_material_cache_miss:continuation:falling_back_to_article_pipeline",
            result["warnings"],
        )
        self.assertEqual(result["article_ids"], ["article-1"])

    def test_expand_material_pool_skips_review_pending_gray_materials(self) -> None:
        session = Mock()
        pending_item = SimpleNamespace(
            id="mat-pending",
            article_id="article-1",
            status="gray",
            release_channel="gray",
            primary_family="continuation",
            quality_score=0.9,
            family_scores={"continuation": 0.9},
            quality_flags=[],
            gray_ratio=1.0,
            gray_reason=None,
        )
        reviewed_item = SimpleNamespace(
            id="mat-reviewed",
            article_id="article-2",
            status="gray",
            release_channel="gray",
            primary_family="continuation",
            quality_score=0.9,
            family_scores={"continuation": 0.9},
            quality_flags=[],
            gray_ratio=1.0,
            gray_reason=None,
        )
        session.scalars.return_value = [pending_item, reviewed_item]
        session.execute.return_value.all.return_value = [
            ("mat-pending", "review_pending"),
            ("mat-reviewed", "auto_tagged"),
        ]

        with patch("scripts.expand_material_pool._article_source_lookup", return_value={"article-1": "src", "article-2": "src"}), patch(
            "scripts.expand_material_pool._stable_state",
            return_value=(Counter(), Counter(), Counter()),
        ), patch(
            "scripts.expand_material_pool._material_question_types",
            return_value={"continuation"},
        ):
            result = promote_gray_materials(
                session,
                target_stable_materials=10,
                min_per_question_type=1,
                per_article_cap=3,
                min_quality_score=0.1,
                dry_run=True,
            )

        self.assertEqual([row["material_id"] for row in result["promoted_preview"]], ["mat-reviewed"])
        self.assertEqual(pending_item.status, "gray")
        self.assertEqual(reviewed_item.status, "promoted")
