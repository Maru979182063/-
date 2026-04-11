from types import SimpleNamespace

from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service


class _FakeRegistry:
    def get_question_card(self, card_id: str) -> dict:
        return {
            "card_id": card_id,
            "business_family_id": "title_selection",
            "business_subtype_id": "title_selection",
            "runtime_binding": {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
            },
        }

    def get_default_question_card(self, business_family_id: str) -> dict:
        return self.get_question_card("question.title_selection.standard_v1")

    def get_business_cards(self, business_family_id: str, *, runtime_question_type: str | None = None, runtime_business_subtype: str | None = None) -> list[dict]:
        return []


class _FakePipeline:
    INDEX_VERSION = "test-index"

    def __init__(self) -> None:
        self.registry = _FakeRegistry()
        self.rebuild_calls: list[str] = []

    def _selected_task_scoring_for_item(self, *, item: dict, business_family_id: str) -> dict:
        return dict(item.get("selected_task_scoring") or {})

    def refresh_cached_item(self, *, cached_item: dict, query_terms: list[str], target_length, length_tolerance: int, enable_anchor_adaptation: bool, preserve_anchor: bool) -> dict:
        return dict(cached_item)

    def build_cached_item_from_material(self, *, material, article, business_family_id: str, question_card_id: str | None = None, **kwargs) -> dict:
        self.rebuild_calls.append(str(question_card_id))
        return {
            "candidate_id": material.id,
            "article_id": material.article_id,
            "article_title": "rebuilt",
            "text": "rebuilt candidate",
            "original_text": "rebuilt candidate",
            "question_ready_context": {
                "question_card_id": question_card_id,
                "selected_material_card": "title_material.plain_main_recovery",
            },
            "eligible_material_cards": [{"card_id": "title_material.plain_main_recovery", "score": 0.91}],
            "eligible_business_cards": [],
            "business_card_recommendations": [],
            "selected_task_scoring": {
                "task_family": "main_idea",
                "final_candidate_score": 0.62,
                "readiness_score": 0.68,
                "risk_penalties": {"example_dominance_penalty": 0.12},
                "difficulty_vector": {
                    "reasoning_depth_score": 0.61,
                    "ambiguity_score": 0.32,
                },
                "difficulty_band_hint": "medium",
            },
            "quality_score": 0.82,
            "source": {"source_name": "src"},
            "meta": {},
        }

    def _passes_runtime_material_gate(self, *, item: dict, business_family_id: str, question_card: dict, min_card_score: float, min_business_card_score: float, require_business_card: bool) -> tuple[bool, str]:
        cards = item.get("eligible_material_cards") or []
        top_card_score = float(cards[0].get("score") or 0.0) if cards else 0.0
        if top_card_score < min_card_score:
            return False, "material_card_score_below_threshold"
        return bool(item.get("selected_task_scoring")), ""

    def _select_diverse_items(self, items: list[dict], limit: int) -> list[dict]:
        return items[:limit]


def test_search_cached_rebuilds_question_card_mismatched_cached_item() -> None:
    service = MaterialPipelineV2Service.__new__(MaterialPipelineV2Service)
    service.pipeline = _FakePipeline()
    service.material_repo = SimpleNamespace(
        list_v2_cached=lambda **kwargs: [
            SimpleNamespace(
                id="mat-1",
                article_id="article-1",
                v2_index_payload={
                    "title_selection": {
                        "candidate_id": "mat-1",
                        "article_id": "article-1",
                        "article_title": "stale",
                        "text": "stale candidate",
                        "original_text": "stale candidate",
                        "question_ready_context": {
                            "question_card_id": "question.center_understanding.standard_v1",
                            "selected_material_card": "title_material.problem_essence_judgement",
                        },
                        "eligible_material_cards": [{"card_id": "title_material.problem_essence_judgement", "score": 0.94}],
                        "eligible_business_cards": [],
                        "business_card_recommendations": [],
                        "selected_task_scoring": {},
                        "quality_score": 0.88,
                        "source": {"source_name": "src"},
                        "meta": {},
                    }
                },
                usage_count=0,
                last_used_at=None,
            )
        ]
    )
    service.article_repo = SimpleNamespace(get=lambda article_id: SimpleNamespace(id=article_id))
    service._load_review_status_map = lambda material_ids: {}
    service._apply_review_gate = lambda **kwargs: (kwargs["materials"], {"mode": kwargs["mode"]})
    service._cached_structure_match_score = lambda **kwargs: 1.0
    service._minimum_structure_score = lambda *args, **kwargs: 0.0

    result = service._search_cached(
        {
            "business_family_id": "title_selection",
            "question_card_id": "question.title_selection.standard_v1",
            "candidate_limit": 3,
            "min_card_score": 0.55,
            "min_business_card_score": 0.45,
        }
    )

    assert result is not None
    assert [item["candidate_id"] for item in result["items"]] == ["mat-1"]
    assert result["items"][0]["question_ready_context"]["question_card_id"] == "question.title_selection.standard_v1"
    assert service.pipeline.rebuild_calls == ["question.title_selection.standard_v1"]
