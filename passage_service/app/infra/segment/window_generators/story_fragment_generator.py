from typing import Any

from app.infra.segment.window_generators.base import BaseWindowGenerator


class StoryFragmentGenerator(BaseWindowGenerator):
    def generate(self, paragraphs: list[dict[str, Any]], sentences: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        version = config.get("version", "seg.v1")
        spans: list[dict[str, Any]] = []
        if len(paragraphs) >= 3:
            spans.append(
                {
                    "start_paragraph": 0,
                    "end_paragraph": min(2, len(paragraphs) - 1),
                    "start_sentence": paragraphs[0].get("sentence_start"),
                    "end_sentence": paragraphs[min(2, len(paragraphs) - 1)].get("sentence_end"),
                    "span_type": "story_fragment",
                    "text": "\n\n".join(p["text"] for p in paragraphs[:3]),
                    "generated_by": "story_fragment_generator",
                    "status": "new",
                    "segmentation_version": version,
                }
            )
        return spans
