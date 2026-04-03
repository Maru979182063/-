from typing import Any

from app.domain.models.plugin_contracts import BaseTagger, RunContext, TaggingResult


class KeywordTagger(BaseTagger):
    name = "keyword_tagger"
    version = "0.1.0"

    def __init__(self, match_mode: str = "contains") -> None:
        self.match_mode = match_mode

    def tag(
        self,
        candidate_span: dict[str, Any],
        article_context: dict[str, Any],
        node_config: dict[str, Any],
        run_context: RunContext,
    ) -> TaggingResult:
        aliases = node_config.get("aliases", [])
        hits = []
        text = candidate_span["text"]
        for alias in aliases:
            if alias and alias in text:
                hits.append(
                    {
                        "knowledge_node_id": node_config.get("node_id"),
                        "confidence": 0.7,
                        "evidence": alias,
                        "plugin_name": self.name,
                    }
                )
        return TaggingResult(
            keep=True,
            boundary_adjustment={},
            feature_profile={},
            reasons=["keyword match"] if hits else ["no keyword match"],
            hits=hits,
            review_status="auto_tagged" if hits else "review_pending",
        )
