from typing import Any

from app.infra.segment.window_generators.base import BaseWindowGenerator


class ParagraphWindowGenerator(BaseWindowGenerator):
    def generate(self, paragraphs: list[dict[str, Any]], sentences: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        spans: list[dict[str, Any]] = []
        version = config.get("version", "seg.v1")
        for idx, paragraph in enumerate(paragraphs):
            spans.append(
                {
                    "start_paragraph": idx,
                    "end_paragraph": idx,
                    "start_sentence": paragraph.get("sentence_start"),
                    "end_sentence": paragraph.get("sentence_end"),
                    "span_type": "single_paragraph",
                    "text": paragraph["text"],
                    "generated_by": "paragraph_window_generator",
                    "status": "new",
                    "segmentation_version": version,
                }
            )
        for window_size in (2, 3):
            for idx in range(len(paragraphs) - window_size + 1):
                selected = paragraphs[idx : idx + window_size]
                spans.append(
                    {
                        "start_paragraph": idx,
                        "end_paragraph": idx + window_size - 1,
                        "start_sentence": selected[0].get("sentence_start"),
                        "end_sentence": selected[-1].get("sentence_end"),
                        "span_type": "paragraph_window",
                        "text": "\n\n".join(p["text"] for p in selected),
                        "generated_by": "paragraph_window_generator",
                        "status": "new",
                        "segmentation_version": version,
                    }
                )
        return spans
