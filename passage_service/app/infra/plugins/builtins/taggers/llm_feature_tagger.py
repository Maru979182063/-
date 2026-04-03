from typing import Any

from app.domain.models.plugin_contracts import BaseTagger, RunContext, TaggingResult


class LLMFeatureTagger(BaseTagger):
    name = "llm_feature_tagger"
    version = "0.1.0"

    def __init__(self, default_keep_threshold: int = 120) -> None:
        self.default_keep_threshold = default_keep_threshold

    def tag(
        self,
        candidate_span: dict[str, Any],
        article_context: dict[str, Any],
        node_config: dict[str, Any],
        run_context: RunContext,
    ) -> TaggingResult:
        text = candidate_span["text"]
        keep = len(text.strip()) >= self.default_keep_threshold
        paragraph_count = max(1, text.count("\n\n") + 1)
        feature_profile = {
            "structure_hints": ["多段并进"] if paragraph_count >= 2 else [],
            "logic_relations": ["解释说明"] if "因为" in text or "因此" in text else [],
            "position_roles": [],
            "single_center_strength": min(len(text) / 800, 1.0),
            "conclusion_sentence_strength": 0.6 if text.endswith(("。", "！", "？")) else 0.2,
            "transition_strength": 0.7 if any(token in text for token in ("但是", "然而", "不过")) else 0.2,
            "summary_strength": 0.7 if any(token in text for token in ("总之", "因此", "可见")) else 0.3,
            "explanation_strength": 0.8 if any(token in text for token in ("例如", "比如", "即")) else 0.3,
            "ordering_anchor_strength": 0.8 if any(token in text for token in ("首先", "其次", "最后")) else 0.2,
            "story_completeness_strength": 0.7 if any(token in text for token in ("后来", "最终", "于是")) else 0.2,
            "distractor_space": 0.5,
            "independence_score": 0.8 if paragraph_count >= 2 else 0.5,
            "question_worthiness_score": 0.8 if keep else 0.3,
        }
        return TaggingResult(
            keep=keep,
            boundary_adjustment={},
            feature_profile=feature_profile,
            reasons=["heuristic feature tagging"],
            hits=[],
            review_status="auto_tagged" if keep else "review_pending",
            extra={"node_id": node_config.get("node_id")},
        )
