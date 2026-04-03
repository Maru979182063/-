from app.core.config import get_config_bundle
from app.core.enums import ArticleStatus
from app.domain.services._common import ServiceBase
from app.infra.segment.paragraph_splitters.default_splitter import DefaultParagraphSplitter
from app.infra.segment.sentence_splitters.default_splitter import DefaultSentenceSplitter
from app.infra.segment.window_generators.paragraph_window_generator import ParagraphWindowGenerator
from app.infra.segment.window_generators.sentence_window_generator import SentenceWindowGenerator
from app.infra.segment.window_generators.story_fragment_generator import StoryFragmentGenerator
from app.services.logical_segment_refiner import LogicalSegmentRefiner


class SegmentService(ServiceBase):
    def segment(self, article_id: str) -> dict:
        article = self.article_repo.get(article_id)
        if article is None:
            return {"article_id": article_id, "status": "not_found"}
        config = get_config_bundle().segmentation
        paragraph_splitter = DefaultParagraphSplitter()
        sentence_splitter = DefaultSentenceSplitter()

        paragraphs = [
            {"paragraph_index": idx, "text": text, "char_count": len(text)}
            for idx, text in enumerate(paragraph_splitter.split(article.clean_text))
        ]
        paragraph_records = self.paragraph_repo.replace_for_article(article_id, paragraphs)

        sentences: list[dict] = []
        global_sentence_index = 0
        for paragraph in paragraphs:
            for sentence in sentence_splitter.split(paragraph["text"]):
                sentences.append(
                    {
                        "paragraph_id": None,
                        "paragraph_index": paragraph["paragraph_index"],
                        "sentence_index": global_sentence_index,
                        "text": sentence,
                    }
                )
                global_sentence_index += 1
        sentence_records = self.sentence_repo.replace_for_article(article_id, sentences)
        paragraph_sentence_ranges: dict[int, list[int]] = {}
        for sentence in sentences:
            paragraph_sentence_ranges.setdefault(sentence["paragraph_index"], []).append(sentence["sentence_index"])
        enriched_paragraphs = []
        for paragraph in paragraphs:
            indexes = paragraph_sentence_ranges.get(paragraph["paragraph_index"], [])
            enriched_paragraphs.append(
                {
                    **paragraph,
                    "sentence_start": indexes[0] if indexes else None,
                    "sentence_end": indexes[-1] if indexes else None,
                }
            )

        generators = [ParagraphWindowGenerator(), SentenceWindowGenerator(), StoryFragmentGenerator()]
        spans: list[dict] = []
        for generator in generators:
            spans.extend(generator.generate(enriched_paragraphs, sentences, config))
        spans = self._throttle_short_article_spans(spans, enriched_paragraphs, sentences, article.clean_text)
        spans = LogicalSegmentRefiner().refine(spans)
        candidate_records = self.candidate_repo.replace_for_article(article_id, spans)

        self.article_repo.update_status(article_id, ArticleStatus.SEGMENTED.value)
        self.audit_repo.log("article", article_id, "segment", {"candidate_count": len(candidate_records)})
        return {
            "article_id": article_id,
            "paragraph_count": len(paragraph_records),
            "sentence_count": len(sentence_records),
            "candidate_span_count": len(candidate_records),
            "status": ArticleStatus.SEGMENTED.value,
        }

    def _throttle_short_article_spans(
        self,
        spans: list[dict],
        paragraphs: list[dict],
        sentences: list[dict],
        clean_text: str,
    ) -> list[dict]:
        short_article = len(sentences) <= 8 or len(clean_text) <= 1200
        if not short_article:
            return spans

        single_paragraphs = [item for item in spans if item["span_type"] == "single_paragraph"]
        paragraph_windows = [item for item in spans if item["span_type"] == "paragraph_window"]
        sentence_groups = [item for item in spans if item["span_type"] == "sentence_group"]
        story_fragments = [item for item in spans if item["span_type"] == "story_fragment"]

        throttled: list[dict] = []
        throttled.extend(single_paragraphs[: max(1, min(len(paragraphs), 4))])
        throttled.extend(paragraph_windows[:2])
        throttled.extend(sentence_groups[:1])
        if len(sentences) >= 6:
            throttled.extend(story_fragments[:1])
        return throttled
