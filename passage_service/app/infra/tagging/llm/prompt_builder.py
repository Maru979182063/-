from typing import Any


def build_tag_prompt(candidate_span: dict[str, Any], article_context: dict[str, Any], knowledge_node: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_span": candidate_span,
        "article_context": article_context,
        "knowledge_node": knowledge_node,
        "instructions": "Return structured JSON only.",
    }
