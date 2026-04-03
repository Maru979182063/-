from typing import Any

from app.infra.segment.window_generators.base import BaseWindowGenerator


class SentenceWindowGenerator(BaseWindowGenerator):
    def generate(self, paragraphs: list[dict[str, Any]], sentences: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        spans: list[dict[str, Any]] = []
        version = config.get("version", "seg.v1")
        min_window = config.get("sentence_window", {}).get("min_sentences", 3)
        max_window = config.get("sentence_window", {}).get("max_sentences", 6)
        for size in range(min_window, max_window + 1):
            for idx in range(len(sentences) - size + 1):
                selected = sentences[idx : idx + size]
                spans.append(
                    {
                        "start_paragraph": selected[0]["paragraph_index"],
                        "end_paragraph": selected[-1]["paragraph_index"],
                        "start_sentence": selected[0]["sentence_index"],
                        "end_sentence": selected[-1]["sentence_index"],
                        "span_type": "sentence_group",
                        "text": "".join(s["text"] for s in selected),
                        "generated_by": "sentence_window_generator",
                        "status": "new",
                        "segmentation_version": version,
                    }
                )
        return spans
