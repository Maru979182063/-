from types import SimpleNamespace

from app.services.material_pipeline_v2 import MaterialPipelineV2
from app.services.near_miss_repair_service import NearMissRepairService


def test_near_miss_repair_service_marks_sentence_fill_contextual_near_miss() -> None:
    class FakeProvider:
        def is_enabled(self) -> bool:
            return True

    service = NearMissRepairService(provider=FakeProvider(), llm_config={"depth2_repair": {"enabled": True, "min_text_chars": 20}})
    item = {
        "text": "因此，这一句仍然需要依赖前文才能成立，但它其实承接着上一层意思，也把话题继续往下一步解释推进，只是当前边界还不够清楚，所以第一次正式判分没有通过。",
        "candidate_type": "closed_span",
        "quality_flags": ["context_opening"],
        "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        "business_card_recommendations": ["sentence_fill__middle_carry_previous__abstract"],
        "business_feature_profile": {
            "sentence_fill_profile": {
                "bidirectional_validation": 0.32,
            }
        },
    }

    result = service.evaluate_entry(
        item=item,
        business_family_id="sentence_fill",
        failure_reason="readiness_score_below_contract_floor",
        question_card_id="question.sentence_fill.standard_v1",
    )

    assert result["repair_candidate"] is True
    assert "truncated_context" in result["dirty_states"]
    assert "shape_misaligned_for_task" in result["dirty_states"]


def test_runtime_search_uses_depth2_repair_for_near_miss_gate_failures() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-depth2",
        title="深度二修复测试",
        clean_text="原始文段用于模拟 near-miss repair。",
        raw_text=None,
        source="unit-test",
        source_url="http://example.com/depth2",
        domain="example.com",
    )
    candidate = {
        "candidate_id": "cand-1",
        "candidate_type": "closed_span",
        "text": "因此，这一句仍然需要依赖前文才能成立，但它其实承接着上一层意思。",
        "meta": {"paragraph_range": [0, 0], "sentence_range": [0, 0]},
        "quality_flags": ["context_opening"],
    }
    original_item = {
        "candidate_id": "cand-1",
        "article_id": "article-depth2",
        "article_title": article.title,
        "_business_family_id": "sentence_fill",
        "candidate_type": "closed_span",
        "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        "business_card_recommendations": ["sentence_fill__middle_carry_previous__abstract"],
        "text": candidate["text"],
        "original_text": candidate["text"],
        "meta": candidate["meta"],
        "source": {"source_name": article.source},
        "question_ready_context": {
            "question_card_id": "question.sentence_fill.standard_v1",
            "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        },
        "local_profile": {},
        "quality_flags": candidate["quality_flags"],
        "quality_score": 0.38,
        "llm_generation_readiness": {"score": 0.41, "status": "blocked"},
        "selected_task_scoring": {"final_candidate_score": 0.40},
    }
    repaired_item = {
        **original_item,
        "candidate_id": "cand-1:repair",
        "text": "这句话承接前文要点，并把讨论自然推进到下一层说明。",
        "consumable_text": "这句话承接前文要点，并把讨论自然推进到下一层说明。",
        "presentation": {},
        "quality_score": 0.66,
        "llm_generation_readiness": {"score": 0.72, "status": "ready"},
        "selected_task_scoring": {"final_candidate_score": 0.70},
    }

    class FakeRepairService:
        def evaluate_entry(self, **kwargs):
            return {
                "repair_candidate": True,
                "entry_reason": "readiness_score_below_contract_floor",
                "dirty_states": ["truncated_context", "shape_misaligned_for_task"],
                "target_business_card": "sentence_fill__middle_carry_previous__abstract",
            }

        def repair(self, **kwargs):
            return {
                "rewritten_text": repaired_item["text"],
                "rewrite_summary": "收紧上下文依赖并强化承前槽位。",
                "preserve_ratio_target": 0.8,
                "preserve_ratio_actual": 0.83,
                "rewrite_mode": "preserve_rewrite_80",
                "dirty_states": ["truncated_context", "shape_misaligned_for_task"],
            }

    pipeline.near_miss_repair_service = FakeRepairService()
    pipeline._derive_candidates = lambda **kwargs: [candidate]
    pipeline._adapt_candidate_window = lambda **kwargs: candidate

    def _fake_build_runtime_search_item(*, candidate, **kwargs):
        if str(candidate.get("candidate_id")) == "cand-1:repair":
            return repaired_item
        return original_item

    pipeline._build_runtime_search_item = _fake_build_runtime_search_item
    pipeline._llm_adjudication_requires_reject = lambda **kwargs: False

    def _fake_gate(*, item, **kwargs):
        if str(item.get("candidate_id")) == "cand-1:repair":
            return True, ""
        return False, "readiness_score_below_contract_floor"

    pipeline._passes_runtime_material_gate = _fake_gate

    result = pipeline.search(
        articles=[article],
        business_family_id="sentence_fill",
        candidate_limit=3,
    )

    assert result["items"]
    first_item = result["items"][0]
    assert first_item["candidate_id"] == "cand-1:repair"
    assert first_item["repair_trace"]["repair_applied"] is True
    assert first_item["repair_trace"]["repair_outcome"] == "pass_strong"
    assert first_item["question_ready_context"]["depth2_repair"]["repair_target_business_card"] == "sentence_fill__middle_carry_previous__abstract"


def test_near_miss_repair_service_blocks_retry_after_mark_failure() -> None:
    class FakeProvider:
        def is_enabled(self) -> bool:
            return True

    NearMissRepairService.FAILED_REPAIR_KEYS.clear()
    service = NearMissRepairService(provider=FakeProvider(), llm_config={"depth2_repair": {"enabled": True, "min_text_chars": 20}})
    item = {
        "candidate_id": "cand-block-1",
        "article_id": "article-block-1",
        "text": "因此，这一句仍然依赖前文才能成立，但它本身确实承接了上一层意思，也在继续向下推进。",
        "candidate_type": "closed_span",
        "quality_flags": ["context_opening"],
        "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        "business_card_recommendations": ["sentence_fill__middle_carry_previous__abstract"],
        "business_feature_profile": {
            "sentence_fill_profile": {
                "bidirectional_validation": 0.32,
            }
        },
    }

    initial = service.evaluate_entry(
        item=item,
        business_family_id="sentence_fill",
        failure_reason="readiness_score_below_contract_floor",
        question_card_id="question.sentence_fill.standard_v1",
    )
    assert initial["repair_candidate"] is True

    service.mark_failure(
        item=item,
        business_family_id="sentence_fill",
        target_business_card="sentence_fill__middle_carry_previous__abstract",
    )

    blocked = service.evaluate_entry(
        item=item,
        business_family_id="sentence_fill",
        failure_reason="readiness_score_below_contract_floor",
        question_card_id="question.sentence_fill.standard_v1",
    )
    assert blocked["repair_candidate"] is False
    assert blocked["entry_reason"] == "repair_previously_failed"
    NearMissRepairService.FAILED_REPAIR_KEYS.clear()


def test_runtime_search_marks_failed_repair_once_and_rejects_item() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-depth2-fail",
        title="深度二失败封锁测试",
        clean_text="原始文本用于模拟失败后的锁定。",
        raw_text=None,
        source="unit-test",
        source_url="http://example.com/depth2-fail",
        domain="example.com",
    )
    candidate = {
        "candidate_id": "cand-fail-1",
        "candidate_type": "closed_span",
        "text": "因此，这一句仍然依赖前文才能成立，但它本身确实承接了上一层意思。",
        "meta": {"paragraph_range": [0, 0], "sentence_range": [0, 0]},
        "quality_flags": ["context_opening"],
    }
    original_item = {
        "candidate_id": "cand-fail-1",
        "article_id": "article-depth2-fail",
        "article_title": article.title,
        "_business_family_id": "sentence_fill",
        "candidate_type": "closed_span",
        "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        "business_card_recommendations": ["sentence_fill__middle_carry_previous__abstract"],
        "text": candidate["text"],
        "original_text": candidate["text"],
        "meta": candidate["meta"],
        "source": {"source_name": article.source},
        "question_ready_context": {
            "question_card_id": "question.sentence_fill.standard_v1",
            "selected_business_card": "sentence_fill__middle_carry_previous__abstract",
        },
        "local_profile": {},
        "quality_flags": candidate["quality_flags"],
        "quality_score": 0.38,
        "llm_generation_readiness": {"score": 0.41, "status": "blocked"},
        "selected_task_scoring": {"final_candidate_score": 0.40},
    }
    repaired_item = {
        **original_item,
        "candidate_id": "cand-fail-1:repair",
        "text": "这句话承接前文，但修复后仍未达到正式消费门槛。",
        "quality_score": 0.39,
        "llm_generation_readiness": {"score": 0.41, "status": "blocked"},
        "selected_task_scoring": {"final_candidate_score": 0.40},
    }

    class FakeRepairService:
        def __init__(self) -> None:
            self.failure_marks = []

        def evaluate_entry(self, **kwargs):
            return {
                "repair_candidate": True,
                "entry_reason": "readiness_score_below_contract_floor",
                "dirty_states": ["truncated_context"],
                "target_business_card": "sentence_fill__middle_carry_previous__abstract",
            }

        def repair(self, **kwargs):
            return {
                "rewritten_text": repaired_item["text"],
                "rewrite_summary": "轻修但未达标。",
                "preserve_ratio_target": 0.8,
                "preserve_ratio_actual": 0.84,
                "rewrite_mode": "preserve_rewrite_80",
                "dirty_states": ["truncated_context"],
            }

        def mark_failure(self, **kwargs):
            self.failure_marks.append(kwargs)

    fake_service = FakeRepairService()
    pipeline.near_miss_repair_service = fake_service
    pipeline._derive_candidates = lambda **kwargs: [candidate]
    pipeline._adapt_candidate_window = lambda **kwargs: candidate

    def _fake_build_runtime_search_item(*, candidate, **kwargs):
        if str(candidate.get("candidate_id")) == "cand-fail-1:repair":
            return repaired_item
        return original_item

    pipeline._build_runtime_search_item = _fake_build_runtime_search_item
    pipeline._llm_adjudication_requires_reject = lambda **kwargs: False

    def _fake_gate(*, item, **kwargs):
        if str(item.get("candidate_id")) == "cand-fail-1:repair":
            return False, "readiness_score_below_contract_floor"
        return False, "readiness_score_below_contract_floor"

    pipeline._passes_runtime_material_gate = _fake_gate

    result = pipeline.search(
        articles=[article],
        business_family_id="sentence_fill",
        candidate_limit=3,
    )

    assert result["items"] == []
    assert len(fake_service.failure_marks) == 1
    assert fake_service.failure_marks[0]["target_business_card"] == "sentence_fill__middle_carry_previous__abstract"
