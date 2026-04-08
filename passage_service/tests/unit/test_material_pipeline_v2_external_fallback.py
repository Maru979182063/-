from app.services.material_pipeline_v2 import MaterialPipelineV2


def test_external_fallback_ranking_prefers_structurally_aligned_main_idea_item() -> None:
    pipeline = MaterialPipelineV2()
    reference_item = {
        "candidate_id": "ref-1",
        "candidate_type": "multi_paragraph_unit",
        "text": "第一段提出核心问题。第二段分析原因。第三段给出归纳判断。",
        "selected_task_scoring": {"readiness_score": 0.72, "final_candidate_score": 0.68},
        "neutral_signal_profile": {
            "main_idea_single_center_score": 0.76,
            "main_idea_closure_score": 0.71,
            "main_idea_lift_score": 0.69,
        },
    }
    strong_item = {
        "candidate_id": "ext-strong",
        "candidate_type": "multi_paragraph_unit",
        "text": "先提出讨论对象。随后解释原因。最后回收到总判断。",
        "quality_score": 0.70,
        "selected_task_scoring": {"readiness_score": 0.69, "final_candidate_score": 0.66},
        "neutral_signal_profile": {
            "main_idea_single_center_score": 0.73,
            "main_idea_closure_score": 0.68,
            "main_idea_lift_score": 0.65,
            "context_dependency": 0.16,
            "branch_focus_strength": 0.18,
        },
        "retrieval_match_profile": {"match_score": 0.82},
    }
    weak_item = {
        "candidate_id": "ext-weak",
        "candidate_type": "closed_span",
        "text": "先讲一个案例，后面突然切到另一件事，没有明显收束。",
        "quality_score": 0.48,
        "selected_task_scoring": {"readiness_score": 0.44, "final_candidate_score": 0.39},
        "neutral_signal_profile": {
            "main_idea_single_center_score": 0.38,
            "main_idea_closure_score": 0.32,
            "main_idea_lift_score": 0.34,
            "context_dependency": 0.35,
            "branch_focus_strength": 0.62,
        },
        "retrieval_match_profile": {"match_score": 0.64},
    }

    ranked = pipeline.rank_external_fallback_items(
        items=[weak_item, strong_item],
        business_family_id="title_selection",
        query_terms=["核心", "判断"],
        reference_items=[reference_item],
        candidate_limit=5,
    )

    assert [item["candidate_id"] for item in ranked["items"]] == ["ext-strong"]
    assert ranked["rejected_items"][0]["candidate_id"] == "ext-weak"
    assert ranked["items"][0]["external_match_profile"]["reason"] == "external_structure_aligned"


def test_external_fallback_ranking_rejects_structure_mismatch_without_reference_gap() -> None:
    pipeline = MaterialPipelineV2()
    weak_item = {
        "candidate_id": "ext-mismatch",
        "candidate_type": "functional_slot_unit",
        "text": "因此，这里只是一个局部承接句。",
        "quality_score": 0.56,
        "selected_task_scoring": {"readiness_score": 0.41, "final_candidate_score": 0.40},
        "neutral_signal_profile": {
            "context_dependency": 0.42,
            "branch_focus_strength": 0.30,
        },
        "retrieval_match_profile": {"match_score": 0.76},
    }

    ranked = pipeline.rank_external_fallback_items(
        items=[weak_item],
        business_family_id="title_selection",
        query_terms=["主旨"],
        reference_items=[],
        candidate_limit=5,
    )

    assert ranked["items"] == []
    assert ranked["rejected_items"][0]["external_match_profile"]["reason"] in {
        "structure_mismatch",
        "task_readiness_weak",
        "final_score_below_threshold",
    }
