from types import SimpleNamespace

from app.services.card_registry_v2 import CardRegistryV2
from app.services.material_pipeline_v2 import MaterialPipelineV2


def test_card_registry_v2_loads_normalized_specs() -> None:
    registry = CardRegistryV2()
    title_question_card = registry.get_default_question_card("title_selection")
    sentence_order_card = registry.get_default_question_card("sentence_order")

    assert title_question_card["business_family_id"] == "title_selection"
    assert title_question_card["card_id"] == "question.title_selection.standard_v1"
    assert "sentence_block_group" in sentence_order_card["upstream_contract"]["required_candidate_types"]


def test_runtime_material_gate_rejects_sentence_fill_item_without_task_scoring() -> None:
    pipeline = MaterialPipelineV2()
    question_card = pipeline.registry.get_question_card("question.sentence_fill.standard_v1")
    item = {
        "eligible_material_cards": [{"card_id": "fill_material.bridge_transition", "score": 0.92}],
        "eligible_business_cards": [{"business_card_id": "sentence_fill__bridge", "score": 0.88}],
        "selected_task_scoring": {},
    }

    passed, reason = pipeline._passes_runtime_material_gate(
        item=item,
        business_family_id="sentence_fill",
        question_card=question_card,
        min_card_score=0.55,
        min_business_card_score=0.45,
        require_business_card=True,
    )

    assert passed is False
    assert reason == "missing_task_scoring"


def test_candidate_planner_materializes_llm_specs_when_provider_is_available() -> None:
    class FakeProvider:
        def is_enabled(self) -> bool:
            return True

        def generate_json(self, *, model, instructions, input_payload):
            return {
                "candidates": [
                    {
                        "candidate_type": "closed_span",
                        "paragraph_start": 1,
                        "paragraph_end": 1,
                        "sentence_start_in_first_paragraph": None,
                        "sentence_end_in_last_paragraph": None,
                        "composition": "paragraph_span",
                        "priority": 0.92,
                        "reason": "Pick the locally complete second paragraph.",
                    }
                ]
            }

    pipeline = MaterialPipelineV2()
    pipeline.provider = FakeProvider()
    article = SimpleNamespace(
        id="article-llm-plan",
        title="整篇理解切分",
        clean_text=(
            "第一段主要交代背景与话题来源，但它本身更像引子。"
            "\n\n"
            "第二段先提出明确观点，再解释原因，最后给出收束判断，因此这一段更适合作为独立候选材料。"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/llm-plan",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        candidate_types=["closed_span"],
    )

    assert any((item.get("meta") or {}).get("planner_source") == "llm_candidate_planner" for item in candidates)
    assert any("第二段先提出明确观点" in item["text"] for item in candidates)


def test_candidate_planner_scores_complete_sentence_block_above_contextual_fragment() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-score",
        title="排序评分",
        clean_text=(
            "第一句先明确提出本段讨论的核心对象和总体判断。第二句随后补足背景原因与现实条件。第三句进一步说明关键限制和内在关联。第四句因此把前文信息收束到清晰结论。"
            "\n\n"
            "而客户却成为了待宰的羔羊。面对质疑，平台负责人把责任甩给一线人员。这种甩锅式整改暴露了平台缺乏诚信。因此，所谓整改承诺更像是舆情托词。"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-score",
        domain="example.com",
    )
    article_context = pipeline._build_article_context(article)
    good_candidate = {
        "candidate_id": "good",
        "candidate_type": "sentence_block_group",
        "text": "第一句先明确提出本段讨论的核心对象和总体判断。第二句随后补足背景原因与现实条件。第三句进一步说明关键限制和内在关联。第四句因此把前文信息收束到清晰结论。",
        "meta": {"paragraph_range": [0, 0], "composition": "single_paragraph_window"},
        "quality_flags": [],
    }
    fragment_candidate = {
        "candidate_id": "fragment",
        "candidate_type": "sentence_block_group",
        "text": "而客户却成为了待宰的羔羊。面对质疑，平台负责人把责任甩给一线人员。这种甩锅式整改暴露了平台缺乏诚信。因此，所谓整改承诺更像是舆情托词。",
        "meta": {"paragraph_range": [1, 1], "composition": "single_paragraph_window"},
        "quality_flags": ["context_opening"],
    }

    good_profile = pipeline._build_neutral_signal_profile(article_context=article_context, candidate=good_candidate)
    fragment_profile = pipeline._build_neutral_signal_profile(article_context=article_context, candidate=fragment_candidate)

    good_score = pipeline._candidate_plan_score(article_context=article_context, candidate=good_candidate, neutral_signal_profile=good_profile)
    fragment_score = pipeline._candidate_plan_score(article_context=article_context, candidate=fragment_candidate, neutral_signal_profile=fragment_profile)

    assert good_score > fragment_score


def test_title_selection_pipeline_returns_question_ready_candidate() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-title",
        title="\u98df\u866b\u690d\u7269\u4e3a\u4f55\u4f1a\u8fdb\u5316\u51fa\u201c\u667a\u6167\u201d",
        clean_text="\u98df\u866b\u690d\u7269\u5e38\u88ab\u770b\u6210\u88ab\u52a8\u7684\u6355\u98df\u8005\uff0c\u4f46\u5b83\u4eec\u5e76\u4e0d\u53ea\u662f\u7b49\u5f85\u730e\u7269\u9760\u8fd1\u3002\u7814\u7a76\u8005\u53d1\u73b0\uff0c\u4e0d\u540c\u79cd\u7c7b\u7684\u98df\u866b\u690d\u7269\u4f1a\u6839\u636e\u73af\u5883\u548c\u730e\u7269\u7279\u70b9\u8c03\u6574\u6355\u98df\u65b9\u5f0f\u3002\u66f4\u91cd\u8981\u7684\u662f\uff0c\u8fd9\u79cd\u8c03\u6574\u5e76\u4e0d\u662f\u96f6\u6563\u73b0\u8c61\uff0c\u800c\u662f\u56f4\u7ed5\u751f\u5b58\u6548\u7387\u5f62\u6210\u7684\u7a33\u5b9a\u7b56\u7565\u3002\u56e0\u6b64\uff0c\u7406\u89e3\u98df\u866b\u690d\u7269\u7684\u201c\u667a\u6167\u201d\uff0c\u5173\u952e\u5728\u4e8e\u770b\u5230\u5b83\u4eec\u5982\u4f55\u5728\u9650\u5236\u6761\u4ef6\u4e0b\u4f5c\u51fa\u6700\u4f18\u9009\u62e9\u3002",
        raw_text=None,
        source="old",
        source_url="http://example.com/title",
        domain="example.com",
    )

    result = pipeline.search(articles=[article], business_family_id="title_selection", candidate_limit=5, min_card_score=0.45)

    assert result["items"]
    first_item = result["items"][0]
    assert first_item["question_ready_context"]["question_card_id"] == "question.title_selection.standard_v1"
    assert first_item["question_ready_context"]["selected_material_card"].startswith("title_material.")
    assert first_item["neutral_signal_profile"]["candidate_type"] == first_item["candidate_type"]
    assert first_item["material_card_recommendations"]
    assert "recommended_generation_archetype" not in first_item["local_profile"]


def test_sentence_order_pipeline_prefers_sentence_block_group_with_complete_units() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order",
        title="\u6807\u51c6\u6392\u5e8f",
            clean_text=(
                "\u8fd9\u4e00\u6bb5\u5148\u4ea4\u4ee3\u8ba8\u8bba\u80cc\u666f\u4e0e\u95ee\u9898\u4e3b\u4f53\uff0c\u4e3a\u540e\u6587\u7684\u6392\u5e8f\u53e5\u7ec4\u63d0\u4f9b\u8bed\u5883\u4e0e\u8bdd\u9898\u6a21\u677f\u3002"
                "\n\n"
                "\u7b2c\u4e00\u53e5\u63d0\u51fa\u89c2\u70b9\u5e76\u6982\u62ec\u8fd9\u4e00\u6bb5\u7684\u8ba8\u8bba\u91cd\u5fc3\u4e0e\u6838\u5fc3\u5bf9\u8c61\uff0c\u4e3a\u540e\u7eed\u53e5\u7ec4\u786e\u5b9a\u8d77\u70b9\u3002"
                "\u7b2c\u4e8c\u53e5\u9996\u5148\u8865\u8db3\u4ea7\u751f\u8fd9\u4e00\u95ee\u9898\u7684\u73b0\u5b9e\u80cc\u666f\u3001\u57fa\u672c\u539f\u56e0\u4e0e\u76f8\u5173\u6761\u4ef6\u3002"
                "\u7b2c\u4e09\u53e5\u5176\u6b21\u6307\u51fa\u5176\u4e2d\u7684\u5173\u952e\u5236\u7ea6\u4e0e\u903b\u8f91\u9547\u70b9\uff0c\u8ba9\u53e5\u7ec4\u4e4b\u95f4\u4fdd\u6301\u8fde\u52a8\u548c\u9012\u8fdb\u3002"
                "\u7b2c\u56db\u53e5\u56e0\u6b64\u628a\u524d\u6587\u4fe1\u606f\u6536\u675f\u5230\u53ef\u4ee5\u6267\u884c\u7684\u7ed3\u8bba\u4e0a\uff0c\u8ba9\u903b\u8f91\u65b9\u5411\u8fdb\u4e00\u6b65\u6e05\u6670\u3002"
                "\u7b2c\u4e94\u53e5\u603b\u4e4b\u518d\u5bf9\u8fd9\u4e00\u89c2\u70b9\u8fdb\u884c\u63d0\u70bc\uff0c\u8ba9\u6574\u4e2a\u53e5\u7ec4\u66f4\u9002\u5408\u4ee5\u72ec\u7acb\u6bb5\u843d\u7684\u5f62\u6001\u88ab\u9605\u8bfb\u3002"
                "\u7b2c\u516d\u53e5\u8fdb\u4e00\u6b65\u8868\u660e\u8be5\u7ed3\u8bba\u5bf9\u540e\u7eed\u8ba8\u8bba\u7684\u4f5c\u7528\uff0c\u8ba9\u7ed3\u5c40\u66f4\u5b8c\u6574\u3002"
            ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order",
        domain="example.com",
    )

    article_context = pipeline._build_article_context(article)
    generic_candidates = pipeline._derive_candidates(article_context=article_context)

    candidate_types = {item["candidate_type"] for item in generic_candidates}
    assert "sentence_block_group" in candidate_types
    assert candidate_types & {"whole_passage", "closed_span", "multi_paragraph_unit"}
    block_candidates = [item for item in generic_candidates if item["candidate_type"] == "sentence_block_group"]
    assert block_candidates
    assert all(pipeline._sentence_order_unit_count(item["text"], item["candidate_type"]) >= 4 for item in block_candidates)


def test_select_diverse_items_reduces_same_article_concentration() -> None:
    pipeline = MaterialPipelineV2()
    items = [
        {
            "candidate_id": f"a1:closed_span:{index}",
            "article_id": "a1",
            "source": {"source_name": "source-1"},
            "text": f"\u91cd\u590d\u6750\u6599{index}\u3002" if index < 3 else f"\u4e0d\u540c\u6750\u6599{index}\u3002",
            "quality_score": 0.95 - index * 0.01,
            "question_ready_context": {"selected_material_card": "title_material.plain_main_recovery"},
        }
        for index in range(4)
    ] + [
        {
            "candidate_id": "a2:closed_span:1",
            "article_id": "a2",
            "source": {"source_name": "source-2"},
            "text": "\u53e6\u4e00\u7bc7\u6750\u6599\u3002",
            "quality_score": 0.80,
            "question_ready_context": {"selected_material_card": "title_material.example_then_recovery"},
        }
    ]

    selected = pipeline._select_diverse_items(items, limit=3)

    assert len(selected) == 3
    assert len({item["article_id"] for item in selected}) >= 2


def test_attach_llm_material_judgments_records_round1_anchor_trace() -> None:
    pipeline = MaterialPipelineV2()
    item = {
        "candidate_id": "fill-anchor-1",
        "article_id": "article-1",
        "candidate_type": "functional_slot_unit",
        "text": "这句话承接前文并继续解释原因。",
        "meta": {"paragraph_range": [0, 0], "sentence_range": [0, 0]},
        "business_feature_profile": {
            "readability": 0.82,
            "sentence_fill_profile": {
                "blank_position": "middle",
                "function_type": "carry_previous",
                "logic_relation": "explanation",
                "explicit_slot_ready": True,
                "bidirectional_validation": 0.78,
                "backward_link_strength": 0.80,
                "forward_link_strength": 0.64,
                "reference_dependency": 0.18,
            },
        },
        "neutral_signal_profile": {
            "topic_consistency_strength": 0.76,
            "bidirectional_validation": 0.78,
            "backward_link_strength": 0.80,
            "forward_link_strength": 0.64,
            "reference_dependency": 0.18,
        },
        "question_ready_context": {},
        "local_profile": {},
    }

    updated = pipeline._attach_llm_material_judgments(
        item=item,
        business_family_id="sentence_fill",
    )

    assert updated["llm_generation_readiness"]["status"] in {"ready", "borderline"}
    assert updated["llm_family_match_hint"]["asset_anchor"]["anchor_role"] == "gold_ready_positive"
    assert updated["llm_family_match_hint"]["asset_anchor"]["coverage_tag"] == "core_available.middle_carry_previous_explanation"


def test_runtime_material_gate_allows_llm_ready_main_family_item_without_task_scoring() -> None:
    class _FakeJudge:
        def is_enforce_mode(self) -> bool:
            return False

    pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
    pipeline.main_card_dual_judge = _FakeJudge()

    passed, reason = pipeline._passes_runtime_material_gate(
        item={
            "candidate_id": "cu-1",
            "article_id": "article-1",
            "candidate_type": "closed_span",
            "text": "这段材料围绕同一主轴展开，论证完整。",
            "meta": {"paragraph_range": [0, 1], "sentence_range": [0, 4]},
            "llm_generation_readiness": {"status": "ready", "score": 0.82, "reason": "round1_anchor_match"},
        },
        business_family_id="center_understanding",
        question_card={"card_id": "question.center_understanding.standard_v1"},
        min_card_score=0.55,
        min_business_card_score=0.45,
        require_business_card=False,
    )

    assert passed is True
    assert reason == ""


def test_runtime_material_gate_keeps_sentence_order_technical_floor() -> None:
    class _FakeJudge:
        def is_enforce_mode(self) -> bool:
            return False

    pipeline = MaterialPipelineV2()
    pipeline.main_card_dual_judge = _FakeJudge()

    passed, reason = pipeline._passes_runtime_material_gate(
        item={
            "candidate_id": "so-1",
            "article_id": "article-1",
            "candidate_type": "sentence_block_group",
            "text": "第一句说明背景。第二句补充原因。第三句简单收束。",
            "meta": {"paragraph_range": [0, 0], "sentence_range": [0, 2]},
            "llm_generation_readiness": {"status": "ready", "score": 0.88},
        },
        business_family_id="sentence_order",
        question_card={"card_id": "question.sentence_order.standard_v1"},
        min_card_score=0.55,
        min_business_card_score=0.45,
        require_business_card=False,
    )

    assert passed is False
    assert reason == "sentence_order_unit_count_below_floor"


def test_runtime_material_gate_does_not_require_sentence_fill_business_card_anymore() -> None:
    class _FakeJudge:
        def is_enforce_mode(self) -> bool:
            return False

    pipeline = MaterialPipelineV2.__new__(MaterialPipelineV2)
    pipeline.main_card_dual_judge = _FakeJudge()

    passed, reason = pipeline._passes_runtime_material_gate(
        item={
            "candidate_id": "sf-1",
            "article_id": "article-1",
            "candidate_type": "functional_slot_unit",
            "text": "这句话承接前文并继续解释。",
            "meta": {"paragraph_range": [0, 0], "sentence_range": [0, 0]},
            "llm_generation_readiness": {"status": "ready", "score": 0.79, "reason": "round1_sentence_fill_tuple_match"},
        },
        business_family_id="sentence_fill",
        question_card={"card_id": "question.sentence_fill.standard_v1"},
        min_card_score=0.55,
        min_business_card_score=0.45,
        require_business_card=False,
    )

    assert passed is True
    assert reason == ""


def test_select_diverse_items_prefers_round1_llm_selection_score() -> None:
    pipeline = MaterialPipelineV2()
    items = [
        {
            "candidate_id": "high-quality-low-llm",
            "article_id": "article-1",
            "source": {"source_name": "source-1"},
            "text": "材料一，质量分高但与Round1锚点不够接近。",
            "quality_score": 0.96,
            "llm_selection_score": 0.42,
            "llm_family_match_hint": {"score": 0.40},
            "question_ready_context": {"selected_material_card": "card-a"},
        },
        {
            "candidate_id": "lower-quality-high-llm",
            "article_id": "article-2",
            "source": {"source_name": "source-2"},
            "text": "材料二，与Round1锚点更接近，适配更稳定。",
            "quality_score": 0.71,
            "llm_selection_score": 0.88,
            "llm_family_match_hint": {"score": 0.84},
            "question_ready_context": {"selected_material_card": "card-b"},
        },
    ]

    selected = pipeline._select_diverse_items(items, limit=2)

    assert selected[0]["candidate_id"] == "lower-quality-high-llm"


def test_attach_center_understanding_semantic_hints_from_round1_assets() -> None:
    pipeline = MaterialPipelineV2()
    item = {
        "candidate_id": "cu-hint-1",
        "article_id": "article-1",
        "candidate_type": "closed_span",
        "text": "材料先铺陈背景，最后集中收束出核心判断。",
        "meta": {"paragraph_range": [0, 1], "sentence_range": [0, 4]},
        "business_feature_profile": {
            "readability": 0.83,
            "material_structure_label": "sub_total",
        },
        "neutral_signal_profile": {
            "single_center_strength": 0.78,
            "topic_consistency_strength": 0.74,
            "summary_strength": 0.82,
            "closure_score": 0.84,
            "titleability": 0.62,
            "analysis_to_conclusion_strength": 0.79,
            "branch_focus_strength": 0.16,
            "example_to_theme_strength": 0.20,
            "turning_focus_strength": 0.34,
            "value_judgement_strength": 0.41,
            "object_scope_stability": 0.72,
            "material_structure_label": "sub_total",
        },
        "question_ready_context": {},
        "local_profile": {},
    }

    updated = pipeline._attach_llm_material_judgments(
        item=item,
        business_family_id="center_understanding",
    )

    assert updated["llm_main_axis_source_hint"]["value"] in {"final_summary", "solution_conclusion"}
    assert updated["llm_argument_structure_hint"]["value"] == "sub_total"
    assert "llm_center_understanding_reason" in updated


def test_sentence_fill_sort_key_prefers_stable_middle_over_uncovered_opening() -> None:
    pipeline = MaterialPipelineV2()
    stable_middle = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-middle",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "middle",
                "bidirectional_validation": 0.82,
                "backward_link_strength": 0.80,
                "forward_link_strength": 0.73,
            }
        },
        "llm_selection_score": 0.78,
        "llm_family_match_hint": {
            "score": 0.79,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": ["pilot.r1.sentence_fill.2012582"]},
        },
        "llm_structure_integrity_judgment": {"score": 0.80},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.71,
    }
    uncovered_opening = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-opening",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "bidirectional_validation": 0.83,
                "backward_link_strength": 0.79,
                "forward_link_strength": 0.75,
            }
        },
        "llm_selection_score": 0.80,
        "llm_family_match_hint": {
            "score": 0.78,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.81},
        "selected_task_scoring": {"final_candidate_score": 0.75},
        "quality_score": 0.72,
    }

    assert pipeline._item_selection_sort_key(stable_middle) > pipeline._item_selection_sort_key(uncovered_opening)


def test_sentence_fill_sort_key_demotes_wider_uncovered_opening_when_llm_gap_is_small() -> None:
    pipeline = MaterialPipelineV2()
    tighter_opening = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-opening-tight",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 1]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.56,
                "backward_link_strength": 0.42,
                "forward_link_strength": 0.69,
                "reference_dependency": 0.0,
                "material_structure_label": "总分",
            }
        },
        "llm_selection_score": 0.8263,
        "llm_family_match_hint": {
            "score": 0.8696,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.7493},
        "selected_task_scoring": {"final_candidate_score": 0.0},
        "quality_score": 0.5224,
    }
    wider_opening = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-opening-wide",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 2]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.56,
                "backward_link_strength": 0.42,
                "forward_link_strength": 0.69,
                "reference_dependency": 0.0,
                "material_structure_label": "观点-论证",
            }
        },
        "llm_selection_score": 0.8263,
        "llm_family_match_hint": {
            "score": 0.8696,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.7493},
        "selected_task_scoring": {"final_candidate_score": 0.0},
        "quality_score": 0.5540,
    }

    assert pipeline._item_selection_sort_key(tighter_opening) > pipeline._item_selection_sort_key(wider_opening)


def test_sentence_fill_sort_key_keeps_strong_opening_when_llm_clearly_dominates() -> None:
    pipeline = MaterialPipelineV2()
    dominant_opening = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-opening-dominant",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 2]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.63,
                "backward_link_strength": 0.46,
                "forward_link_strength": 0.74,
                "reference_dependency": 0.0,
                "material_structure_label": "观点-论证",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.66, "titleability": 0.74},
        "llm_selection_score": 0.94,
        "llm_family_match_hint": {
            "score": 0.90,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.82},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.70,
    }
    stable_middle = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-middle-strong",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "middle",
                "bidirectional_validation": 0.81,
                "backward_link_strength": 0.79,
                "forward_link_strength": 0.74,
            }
        },
        "neutral_signal_profile": {"closure_score": 0.62, "titleability": 0.66},
        "llm_selection_score": 0.81,
        "llm_family_match_hint": {
            "score": 0.82,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": ["pilot.r1.sentence_fill.2012582"]},
        },
        "llm_structure_integrity_judgment": {"score": 0.81},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.71,
    }

    assert pipeline._item_selection_sort_key(dominant_opening) > pipeline._item_selection_sort_key(stable_middle)


def test_sentence_fill_sort_key_keeps_card_score_as_late_tiebreak_only() -> None:
    pipeline = MaterialPipelineV2()
    stable_middle = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-middle-card-weak",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "middle",
                "bidirectional_validation": 0.82,
                "backward_link_strength": 0.81,
                "forward_link_strength": 0.75,
            }
        },
        "neutral_signal_profile": {"closure_score": 0.67, "titleability": 0.66},
        "llm_selection_score": 0.79,
        "llm_family_match_hint": {
            "score": 0.80,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": ["pilot.r1.sentence_fill.2012582"]},
        },
        "llm_structure_integrity_judgment": {"score": 0.80},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.69,
    }
    uncovered_opening_with_better_cards = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-opening-card-strong",
        "question_ready_context": {
            "runtime_binding": {"question_type": "sentence_fill"},
            "material_card_matches": [{"score": 0.99}],
            "business_card_matches": [{"score": 0.99}],
        },
        "meta": {"paragraph_range": [0, 3]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.58,
                "backward_link_strength": 0.44,
                "forward_link_strength": 0.68,
                "reference_dependency": 0.0,
                "material_structure_label": "时间演进",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.60, "titleability": 0.64},
        "llm_selection_score": 0.79,
        "llm_family_match_hint": {
            "score": 0.80,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.80},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.72,
    }

    assert pipeline._item_selection_sort_key(stable_middle) > pipeline._item_selection_sort_key(
        uncovered_opening_with_better_cards
    )


def test_sentence_fill_boundary_guardrail_promotes_compact_closed_span_in_low_gap_pair() -> None:
    pipeline = MaterialPipelineV2()
    broader_top = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-broad-top",
        "candidate_type": "multi_paragraph_unit",
        "article_id": "article-fill-broad",
        "text": "第一段提出背景。第二段继续补充背景，并把话题拉长。",
        "question_ready_context": {
            "runtime_binding": {"question_type": "sentence_fill"},
            "material_card_matches": [{"score": 0.93}],
            "business_card_matches": [{"score": 0.91}],
        },
        "meta": {"paragraph_range": [0, 2]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.60,
                "backward_link_strength": 0.47,
                "forward_link_strength": 0.66,
                "reference_dependency": 0.0,
                "material_structure_label": "时间演进",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.58, "titleability": 0.60},
        "llm_selection_score": 0.602,
        "llm_family_match_hint": {
            "score": 0.63,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.62},
        "selected_task_scoring": {"final_candidate_score": 0.60},
        "quality_score": 0.76,
        "eligible_material_cards": [{"score": 0.93}],
        "eligible_business_cards": [{"score": 0.91}],
    }
    compact_challenger = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-compact-challenger",
        "candidate_type": "closed_span",
        "article_id": "article-fill-compact",
        "text": "承上启下的一句更紧，空位消费形态更集中。",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 1]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.44,
                "backward_link_strength": 0.37,
                "forward_link_strength": 0.48,
                "reference_dependency": 0.0,
                "material_structure_label": "时间演进",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.66, "titleability": 0.60},
        "llm_selection_score": 0.565,
        "llm_family_match_hint": {
            "score": 0.61,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.64},
        "selected_task_scoring": {"final_candidate_score": 0.61},
        "quality_score": 0.58,
    }

    assert pipeline._item_selection_sort_key(broader_top) > pipeline._item_selection_sort_key(compact_challenger)

    selected = pipeline._select_diverse_items([broader_top, compact_challenger], 2)

    assert [item["candidate_id"] for item in selected[:2]] == [
        "sf-compact-challenger",
        "sf-broad-top",
    ]


def test_sentence_fill_boundary_guardrail_does_not_override_llm_dominant_broader_candidate() -> None:
    pipeline = MaterialPipelineV2()
    llm_dominant_broad = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-broad-dominant",
        "candidate_type": "multi_paragraph_unit",
        "article_id": "article-fill-dominant",
        "text": "这一组候选虽然更宽，但 LLM 主判断明显更强，而且结构完成度也更高。",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 2]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.64,
                "backward_link_strength": 0.50,
                "forward_link_strength": 0.72,
                "reference_dependency": 0.0,
                "material_structure_label": "观点-论证",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.66, "titleability": 0.70},
        "llm_selection_score": 0.95,
        "llm_family_match_hint": {
            "score": 0.90,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.79},
        "selected_task_scoring": {"final_candidate_score": 0.74},
        "quality_score": 0.73,
    }
    compact_but_weaker = {
        "_business_family_id": "sentence_fill",
        "candidate_id": "sf-compact-weaker",
        "candidate_type": "closed_span",
        "article_id": "article-fill-weaker",
        "text": "这条更紧，但整体主判断没有明显压过前者。",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_fill"}},
        "meta": {"paragraph_range": [0, 1]},
        "business_feature_profile": {
            "sentence_fill_profile": {
                "blank_position": "opening",
                "function_type": "topic_introduction",
                "bidirectional_validation": 0.50,
                "backward_link_strength": 0.42,
                "forward_link_strength": 0.54,
                "reference_dependency": 0.0,
                "material_structure_label": "观点-论证",
            }
        },
        "neutral_signal_profile": {"closure_score": 0.72, "titleability": 0.66},
        "llm_selection_score": 0.80,
        "llm_family_match_hint": {
            "score": 0.79,
            "asset_anchor": {"anchor_role": "gold_ready_positive", "anchor_sample_ids": []},
        },
        "llm_structure_integrity_judgment": {"score": 0.76},
        "selected_task_scoring": {"final_candidate_score": 0.72},
        "quality_score": 0.61,
    }

    selected = pipeline._select_diverse_items([llm_dominant_broad, compact_but_weaker], 2)

    assert [item["candidate_id"] for item in selected[:2]] == [
        "sf-broad-dominant",
        "sf-compact-weaker",
    ]


def test_center_understanding_sort_key_demotes_boundary_holdout_anchor() -> None:
    pipeline = MaterialPipelineV2()
    stable = {
        "_business_family_id": "center_understanding",
        "candidate_id": "cu-stable",
        "question_ready_context": {"runtime_binding": {"question_type": "center_understanding"}},
        "llm_selection_score": 0.78,
        "llm_single_center_judgment": {"score": 0.82},
        "llm_main_axis_source_hint": {"score": 0.76},
        "llm_argument_structure_hint": {"score": 0.74},
        "llm_family_match_hint": {"score": 0.80, "asset_anchor": {"anchor_role": "gold_ready_positive"}},
        "llm_structure_integrity_judgment": {"score": 0.79},
        "selected_task_scoring": {"final_candidate_score": 0.72},
        "quality_score": 0.70,
    }
    boundary = {
        "_business_family_id": "center_understanding",
        "candidate_id": "cu-boundary",
        "question_ready_context": {"runtime_binding": {"question_type": "center_understanding"}},
        "llm_selection_score": 0.80,
        "llm_single_center_judgment": {"score": 0.81},
        "llm_main_axis_source_hint": {"score": 0.77},
        "llm_argument_structure_hint": {"score": 0.75},
        "llm_family_match_hint": {"score": 0.79, "asset_anchor": {"anchor_role": "review_holdout_boundary"}},
        "llm_structure_integrity_judgment": {"score": 0.80},
        "selected_task_scoring": {"final_candidate_score": 0.73},
        "quality_score": 0.71,
    }

    assert pipeline._item_selection_sort_key(stable) > pipeline._item_selection_sort_key(boundary)


def test_sentence_order_sort_key_prefers_sentence_block_group_with_clearer_anchor_signals() -> None:
    pipeline = MaterialPipelineV2()
    clean_block = {
        "_business_family_id": "sentence_order",
        "candidate_id": "so-clean",
        "candidate_type": "sentence_block_group",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_order"}},
        "business_feature_profile": {
            "sentence_order_profile": {
                "opening_signal_strength": 0.84,
                "closing_signal_strength": 0.78,
                "sequence_integrity": 0.82,
                "multi_path_risk": 0.12,
            }
        },
        "llm_selection_score": 0.79,
        "llm_family_match_hint": {"score": 0.80, "asset_anchor": {"anchor_role": "gold_ready_positive"}},
        "selected_task_scoring": {"final_candidate_score": 0.73},
        "quality_score": 0.70,
    }
    weak_group = {
        "_business_family_id": "sentence_order",
        "candidate_id": "so-weak",
        "candidate_type": "ordered_unit_group",
        "question_ready_context": {"runtime_binding": {"question_type": "sentence_order"}},
        "business_feature_profile": {
            "sentence_order_profile": {
                "opening_signal_strength": 0.62,
                "closing_signal_strength": 0.58,
                "sequence_integrity": 0.63,
                "multi_path_risk": 0.31,
            }
        },
        "llm_selection_score": 0.80,
        "llm_family_match_hint": {"score": 0.78, "asset_anchor": {"anchor_role": "gold_ready_positive"}},
        "selected_task_scoring": {"final_candidate_score": 0.72},
        "quality_score": 0.72,
    }

    assert pipeline._item_selection_sort_key(clean_block) > pipeline._item_selection_sort_key(weak_group)


def test_cached_prefilter_sort_key_puts_llm_before_card_score() -> None:
    pipeline = MaterialPipelineV2()
    weak_card_strong_llm = {
        "_cached_business_family_id": "center_understanding",
        "question_ready_context": {"runtime_binding": {"question_type": "center_understanding"}},
        "llm_selection_score": 0.84,
        "llm_single_center_judgment": {"score": 0.82},
        "llm_main_axis_source_hint": {"score": 0.76},
        "llm_argument_structure_hint": {"score": 0.74},
        "llm_family_match_hint": {"score": 0.80, "asset_anchor": {"anchor_role": "gold_ready_positive"}},
        "llm_structure_integrity_judgment": {"score": 0.78},
        "selected_task_scoring": {"final_candidate_score": 0.72},
        "quality_score": 0.70,
    }
    strong_card_weaker_llm = {
        "_cached_business_family_id": "center_understanding",
        "question_ready_context": {"runtime_binding": {"question_type": "center_understanding"}},
        "llm_selection_score": 0.68,
        "llm_single_center_judgment": {"score": 0.80},
        "llm_main_axis_source_hint": {"score": 0.73},
        "llm_argument_structure_hint": {"score": 0.70},
        "llm_family_match_hint": {"score": 0.74, "asset_anchor": {"anchor_role": "gold_ready_positive"}},
        "llm_structure_integrity_judgment": {"score": 0.75},
        "selected_task_scoring": {"final_candidate_score": 0.71},
        "quality_score": 0.72,
    }

    key_a = pipeline._cached_prefilter_sort_key(
        cached_item=weak_card_strong_llm,
        business_family_id="center_understanding",
        card_score=0.2,
        structure_score=0.4,
        hit_count=0,
        quality_score=0.70,
    )
    key_b = pipeline._cached_prefilter_sort_key(
        cached_item=strong_card_weaker_llm,
        business_family_id="center_understanding",
        card_score=2.0,
        structure_score=0.4,
        hit_count=0,
        quality_score=0.72,
    )

    assert key_a > key_b


def test_title_selection_skips_long_whole_passage_candidates_from_very_long_articles() -> None:
    pipeline = MaterialPipelineV2()
    paragraphs = [
        "\u4e00\u662f\u7ecf\u6d4e\u6307\u6807\u6301\u7eed\u56de\u5347\uff0c\u5916\u8d38\u548c\u6295\u8d44\u4fdd\u6301\u97e7\u6027\u3002",
        "\u4e8c\u662f\u79d1\u6280\u521b\u65b0\u7ec6\u5206\u9886\u57df\u5168\u9762\u63a8\u8fdb\uff0c\u65b0\u8d28\u751f\u4ea7\u529b\u52a0\u5feb\u6210\u957f\u3002",
        "\u4e09\u662f\u6c11\u751f\u4fdd\u969c\u529b\u5ea6\u6301\u7eed\u52a0\u5927\uff0c\u6559\u80b2\u3001\u533b\u7597\u3001\u517b\u8001\u7b49\u9886\u57df\u7a33\u6b65\u6539\u5584\u3002",
    ] * 6
    article = SimpleNamespace(
        id="article-title-long",
        title="\u5de5\u4f5c\u62a5\u544a",
        clean_text="\n\n".join(paragraphs),
        raw_text=None,
        source="old",
        source_url="http://example.com/title-long",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["whole_passage", "multi_paragraph_unit"],
        business_family_id="title_selection",
    )

    assert all(item["candidate_type"] != "whole_passage" for item in candidates)


def test_sentence_order_candidates_stay_within_single_paragraph_windows() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-local",
        title="\u5c40\u90e8\u6392\u5e8f",
        clean_text=(
            "\u7532\u6bb5\u7b2c\u4e00\u53e5\u8bf4\u660e\u653f\u7b56\u80cc\u666f\u3001\u8ba8\u8bba\u5bf9\u8c61\u4e0e\u603b\u4f53\u73b0\u72b6\uff0c\u4e3a\u540e\u6587\u94fa\u8bbe\u8bed\u5883\u3002"
            "\u7532\u6bb5\u7b2c\u4e8c\u53e5\u8fdb\u4e00\u6b65\u8865\u5145\u95ee\u9898\u4ea7\u751f\u7684\u73b0\u5b9e\u539f\u56e0\u3001\u9650\u5236\u6761\u4ef6\u4e0e\u57fa\u672c\u80cc\u666f\u3002"
            "\u7532\u6bb5\u7b2c\u4e09\u53e5\u63d0\u51fa\u53ef\u4ee5\u5bf9\u5e94\u7684\u89e3\u51b3\u8def\u5f84\u3001\u6267\u884c\u65b9\u5f0f\u4e0e\u63a8\u8fdb\u6b65\u9aa4\u3002"
            "\u7532\u6bb5\u7b2c\u56db\u53e5\u5bf9\u524d\u6587\u89c2\u70b9\u505a\u51fa\u5c0f\u7ed3\uff0c\u5e76\u63d0\u70bc\u8fd9\u4e00\u6bb5\u7684\u5173\u952e\u843d\u70b9\u4e0e\u76ee\u6807\u65b9\u5411\u3002"
            "\n\n"
            "\u4e59\u6bb5\u7b2c\u4e00\u53e5\u4ece\u53e6\u4e00\u4e2a\u89d2\u5ea6\u5f15\u5165\u65b0\u7684\u8ba8\u8bba\u7ebf\u7d22\uff0c\u4e3a\u65b0\u7684\u53e5\u7ec4\u5efa\u7acb\u8d77\u70b9\u3002"
            "\u4e59\u6bb5\u7b2c\u4e8c\u53e5\u5206\u6790\u53d8\u5316\u80cc\u540e\u7684\u5236\u7ea6\u6761\u4ef6\u3001\u5185\u5728\u903b\u8f91\u4e0e\u5404\u65b9\u53cd\u5e94\u3002"
            "\u4e59\u6bb5\u7b2c\u4e09\u53e5\u7ed3\u5408\u6848\u4f8b\u5bf9\u76f8\u5173\u89c2\u70b9\u505a\u51fa\u8bf4\u660e\uff0c\u8ba9\u672c\u6bb5\u7684\u903b\u8f91\u66f4\u52a0\u5177\u4f53\u3002"
            "\u4e59\u6bb5\u7b2c\u56db\u53e5\u7ed9\u51fa\u8fd9\u4e00\u5c42\u610f\u4e49\u7684\u5f52\u7eb3\uff0c\u4f7f\u6574\u4e2a\u53e5\u7ec4\u53ef\u4ee5\u4f5c\u4e3a\u72ec\u7acb\u5c0f\u6bb5\u88ab\u9605\u8bfb\u3002"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-local",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["sentence_block_group"],
        business_family_id="sentence_order",
    )

    block_candidates = [item for item in candidates if item["candidate_type"] == "sentence_block_group"]
    assert block_candidates
    for item in block_candidates:
        meta = item.get("meta") or {}
        paragraph_range = meta.get("paragraph_range") or []
        if "\u7532\u6bb5" in item["text"] and "\u4e59\u6bb5" in item["text"]:
            assert meta.get("composition") == "adjacent_paragraph_pair"
            assert paragraph_range == [0, 1]
        else:
            assert len(set(paragraph_range)) <= 1


def test_sentence_order_skips_fragmentary_three_sentence_blocks() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-fragment",
        title="\u788e\u7247\u6392\u5e8f",
        clean_text=(
            "\u7b2c\u4e00\u53e5\u53ea\u662f\u63d0\u51fa\u4e00\u4e2a\u7b80\u5355\u89c2\u70b9\u3002"
            "\u7b2c\u4e8c\u53e5\u8876\u63a5\u5bf9\u8fd9\u4e2a\u89c2\u70b9\u7684\u4e00\u70b9\u8865\u5145\u3002"
            "\u7b2c\u4e09\u53e5\u7ed9\u51fa\u7b80\u77ed\u6536\u675f\uff0c\u4f46\u6574\u4f53\u53e5\u7ec4\u4ecd\u8fc7\u4e8e\u788e\u7247\u3002"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-fragment",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["sentence_block_group"],
        business_family_id="sentence_order",
    )

    assert not candidates


def test_sentence_order_can_derive_adjacent_paragraph_pair_blocks() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-adjacent",
        title="\u76f8\u90bb\u6bb5\u843d\u8054\u5408",
        clean_text=(
                "\u7b2c\u4e00\u6bb5\u7b2c\u4e00\u53e5\u5148\u63d0\u51fa\u8fd9\u4e00\u90e8\u5206\u8ba8\u8bba\u7684\u80cc\u666f\u3001\u95ee\u9898\u4e3b\u4f53\u4e0e\u6574\u4f53\u89c2\u5bdf\u89d2\u5ea6\uff0c\u4e3a\u540e\u7eed\u53e5\u7ec4\u94fa\u57ab\u8db3\u591f\u7684\u8bed\u4e49\u7a7a\u95f4\u3002"
                "\u7b2c\u4e00\u6bb5\u7b2c\u4e8c\u53e5\u518d\u8865\u8db3\u5173\u952e\u539f\u56e0\u3001\u53d8\u5316\u6761\u4ef6\u4e0e\u73b0\u5b9e\u5236\u7ea6\uff0c\u8ba9\u524d\u4e00\u90e8\u5206\u7684\u95ee\u9898\u5448\u73b0\u5f97\u66f4\u52a0\u5b8c\u6574\u3002"
                "\u7b2c\u4e00\u6bb5\u7b2c\u4e09\u53e5\u7b80\u8981\u5f52\u7eb3\u8fd9\u4e00\u90e8\u5206\u7684\u610f\u4e49\u7ebf\u7d22\uff0c\u4e3a\u540e\u7eed\u8f6c\u5165\u4e0b\u4e00\u6bb5\u57cb\u4e0b\u7ebf\u7d22\u3002"
                "\n\n"
                "\u7b2c\u4e8c\u6bb5\u7b2c\u4e00\u53e5\u987a\u7740\u524d\u6587\u5c55\u5f00\u89e3\u51b3\u601d\u8def\u3001\u63a8\u8fdb\u8def\u5f84\u4e0e\u5177\u4f53\u6267\u884c\u65b9\u5f0f\uff0c\u8ba9\u6574\u4e2a\u53e5\u7ec4\u51fa\u73b0\u8fde\u7eed\u9012\u8fdb\u7684\u7ed3\u6784\u3002"
                "\u7b2c\u4e8c\u6bb5\u7b2c\u4e8c\u53e5\u5bf9\u524d\u6587\u4fe1\u606f\u8fdb\u884c\u5c0f\u7ed3\u5e76\u6536\u675f\u672c\u6bb5\u903b\u8f91\uff0c\u4f7f\u8fd9\u4e00\u8054\u5408\u53e5\u7ec4\u53ef\u4ee5\u88ab\u76f4\u63a5\u5f53\u6210\u4e00\u6bb5\u5b8c\u6574\u6750\u6599\u6765\u9605\u8bfb\u3002"
                "\u7b2c\u4e8c\u6bb5\u7b2c\u4e09\u53e5\u518d\u5c06\u903b\u8f91\u7ed3\u5c40\u4e0e\u7ed3\u8bba\u63a8\u5230\u53ef\u6267\u884c\u5c42\u9762\uff0c\u4f7f\u53e5\u7ec4\u5b8c\u6574\u76f8\u7cfb\u3002"
            ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-adjacent",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["sentence_block_group"],
        business_family_id="sentence_order",
    )

    assert candidates
    assert any((item.get("meta") or {}).get("composition") == "adjacent_paragraph_pair" for item in candidates)


def test_sentence_order_result_contains_support_context_presentation() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-present",
        title="\u6392\u5e8f\u5c55\u793a",
        clean_text=(
            "\u8fd9\u4e00\u6bb5\u5148\u4ea4\u4ee3\u8ba8\u8bba\u80cc\u666f\u4e0e\u95ee\u9898\u4e3b\u4f53\uff0c\u4e3a\u540e\u6587\u7684\u6392\u5e8f\u53e5\u7ec4\u63d0\u4f9b\u8bed\u5883\u4e0e\u8bdd\u9898\u6a21\u677f\u3002"
            "\n\n"
            "\u7b2c\u4e00\u53e5\u63d0\u51fa\u89c2\u70b9\u5e76\u6982\u62ec\u8fd9\u4e00\u6bb5\u7684\u8ba8\u8bba\u91cd\u5fc3\u4e0e\u6838\u5fc3\u5bf9\u8c61\uff0c\u4e3a\u540e\u7eed\u53e5\u7ec4\u786e\u5b9a\u8d77\u70b9\u3002"
            "\u7b2c\u4e8c\u53e5\u9996\u5148\u8865\u8db3\u4ea7\u751f\u8fd9\u4e00\u95ee\u9898\u7684\u73b0\u5b9e\u80cc\u666f\u3001\u57fa\u672c\u539f\u56e0\u4e0e\u76f8\u5173\u6761\u4ef6\u3002"
            "\u7b2c\u4e09\u53e5\u5176\u6b21\u6307\u51fa\u5176\u4e2d\u7684\u5173\u952e\u5236\u7ea6\u4e0e\u903b\u8f91\u9547\u70b9\uff0c\u8ba9\u53e5\u7ec4\u4e4b\u95f4\u4fdd\u6301\u8fde\u52a8\u548c\u9012\u8fdb\u3002"
            "\u7b2c\u56db\u53e5\u56e0\u6b64\u628a\u524d\u6587\u4fe1\u606f\u6536\u675f\u5230\u53ef\u4ee5\u6267\u884c\u7684\u7ed3\u8bba\u4e0a\uff0c\u8ba9\u903b\u8f91\u65b9\u5411\u8fdb\u4e00\u6b65\u6e05\u6670\u3002"
            "\u7b2c\u4e94\u53e5\u603b\u4e4b\u518d\u5bf9\u8fd9\u4e00\u89c2\u70b9\u8fdb\u884c\u63d0\u70bc\uff0c\u8ba9\u6574\u4e2a\u53e5\u7ec4\u66f4\u9002\u5408\u4ee5\u72ec\u7acb\u6bb5\u843d\u7684\u5f62\u6001\u88ab\u9605\u8bfb\u3002"
            "\u7b2c\u516d\u53e5\u8fdb\u4e00\u6b65\u8868\u660e\u8be5\u7ed3\u8bba\u5bf9\u540e\u7eed\u8ba8\u8bba\u7684\u4f5c\u7528\uff0c\u8ba9\u7ed3\u5c40\u66f4\u5b8c\u6574\u3002"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-present",
        domain="example.com",
    )

    article_context = pipeline._build_article_context(article)
    candidates = pipeline._derive_candidates(
        article_context=article_context,
        required_candidate_types=["sentence_block_group"],
        business_family_id="sentence_order",
    )

    candidate = next(item for item in candidates if item["candidate_type"] == "sentence_block_group")
    signal_profile = pipeline._build_signal_profile(
        signal_layer=pipeline.registry.get_signal_layer("sentence_order"),
        article_context=article_context,
        candidate=candidate,
    )
    presentation = pipeline._build_presentation(
        business_family_id="sentence_order",
        article_context=article_context,
        candidate=candidate,
        signal_profile=signal_profile,
    )

    assert presentation["mode"] == "sentence_order"
    assert presentation["sortable_block"]
    assert presentation["lead_context"]


def test_sentence_order_pipeline_can_merge_adjacent_sentences_into_six_units() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-order-six-units",
        title="六单元排序",
        clean_text=(
            "第一句先提出本段讨论的背景。"
            "第二句补充现实限制，因此要重新安排执行节奏。"
            "第三句接着说明推进思路。"
            "第四句同时补充配套条件，让方案更具可行性。"
            "第五句然后归纳阶段重点。"
            "第六句再提示后续衔接方向。"
            "第七句最后给出总结判断。"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/order-six-units",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["sentence_block_group"],
        business_family_id="sentence_order",
    )

    block_candidates = [item for item in candidates if item["candidate_type"] == "sentence_block_group"]
    assert block_candidates
    merged = next(item for item in block_candidates if (item.get("meta") or {}).get("grouped_unit_count"))
    meta = merged.get("meta") or {}
    assert len(meta.get("ordered_units") or []) == 6
    assert meta.get("grouped_unit_count", 0) >= 1
    assert pipeline._sentence_order_unit_count(merged["text"], merged["candidate_type"]) == 6


def test_sentence_fill_result_contains_blanked_consumable_text() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-fill-present",
        title="\u586b\u7a7a\u5c55\u793a",
        clean_text="\u5927\u91cf\u7814\u7a76\u6307\u51fa\uff0c\u571f\u58e4\u5fae\u751f\u7269\u5f71\u54cd\u690d\u7269\u751f\u957f\u3002\u5b83\u4eec\u4e0d\u4ec5\u53c2\u4e0e\u517b\u5206\u5faa\u73af\uff0c\u8fd8\u80fd\u5f71\u54cd\u690d\u7269\u5bf9\u73af\u5883\u53d8\u5316\u7684\u9002\u5e94\u3002\u56e0\u6b64\uff0c\u4ece\u66f4\u957f\u8fdc\u7684\u89d2\u5ea6\u770b\uff0c\u4fdd\u62a4\u571f\u58e4\u751f\u6001\u7cfb\u7edf\u5c31\u662f\u4fdd\u62a4\u519c\u4e1a\u751f\u4ea7\u529b\u3002",
        raw_text=None,
        source="old",
        source_url="http://example.com/fill-present",
        domain="example.com",
    )

    result = pipeline.search(articles=[article], business_family_id="sentence_fill", candidate_limit=5, min_card_score=0.35)

    assert result["items"]
    first_item = result["items"][0]
    assert "[BLANK]" in first_item["consumable_text"]
    assert first_item["presentation"]["mode"] == "sentence_fill"


def test_sentence_order_dual_anchor_lock_rejects_qa_style_blocks() -> None:
    pipeline = MaterialPipelineV2()
    candidate = {
        "candidate_id": "order-qa:1",
        "candidate_type": "sentence_block_group",
        "text": "如何申请参保？失能人员或其家属可通过医保服务窗口提出申请。材料主要包括身份证件、申请表和诊断书等。在收到材料后，经办机构会审核并反馈结果。",
        "meta": {"paragraph_range": [0, 0]},
        "quality_flags": [],
    }
    signal_profile = {
        "opening_signal_strength": 0.82,
        "opening_anchor_type": "explicit_topic",
        "middle_structure_type": "mixed_layers",
        "local_binding_strength": 0.35,
        "closing_anchor_type": "call_to_action",
        "closing_signal_strength": 0.76,
        "sequence_integrity": 0.72,
        "context_dependency": 0.18,
    }

    hits = pipeline._score_material_cards(
        material_cards=pipeline.registry.get_material_cards("sentence_order"),
        signal_profile=signal_profile,
        candidate=candidate,
        min_card_score=0.35,
        business_family_id="sentence_order",
    )

    assert "order_material.dual_anchor_lock" not in {item["card_id"] for item in hits}


def test_continuation_skips_long_whole_passage_candidates_from_very_long_articles() -> None:
    pipeline = MaterialPipelineV2()
    paragraphs = [
        "新华社北京3月13日电",
        "政府工作报告",
        "各位代表：",
    ] + [f"第{i}段围绕宏观政策、产业发展和民生举措展开说明，并继续补充背景与细节。" for i in range(1, 12)]
    article = SimpleNamespace(
        id="article-cont-long",
        title="政府工作报告",
        clean_text="\n\n".join(paragraphs),
        raw_text=None,
        source="old",
        source_url="http://example.com/continuation-long",
        domain="example.com",
    )

    candidates = pipeline._derive_candidates(
        article_context=pipeline._build_article_context(article),
        required_candidate_types=["whole_passage", "closed_span", "multi_paragraph_unit"],
        business_family_id="continuation",
    )

    assert all(item["candidate_type"] != "whole_passage" for item in candidates)


def test_continuation_result_uses_tail_window_and_strips_front_matter() -> None:
    pipeline = MaterialPipelineV2()
    article = SimpleNamespace(
        id="article-cont-present",
        title="社保第六险来了",
        clean_text=(
            "新华社北京3月13日电\n\n"
            "社保“第六险”来了 读懂这些关键“热词”\n\n"
            "记者 黄敬文 摄\n\n"
            "长期护理保险制度正在更多地区推进，政策框架逐渐清晰。\n\n"
            "与基本医保不同，它主要面向失能人员的长期照护需求，核心在于减轻家庭照护负担。\n\n"
            "接下来，制度如何细化服务方式、评估标准与支付机制，决定了这项制度能否真正落地见效。"
        ),
        raw_text=None,
        source="old",
        source_url="http://example.com/continuation-present",
        domain="example.com",
    )

    result = pipeline.search(articles=[article], business_family_id="continuation", candidate_limit=5, min_card_score=0.35)

    assert result["items"]
    first_item = result["items"][0]
    assert first_item["presentation"]["mode"] == "continuation"
    assert "新华社" not in first_item["consumable_text"]
    assert len(first_item["consumable_text"]) < len(first_item["text"])
