from app.infra.segment.paragraph_splitters.default_splitter import DefaultParagraphSplitter
from app.infra.segment.sentence_splitters.default_splitter import DefaultSentenceSplitter


def test_basic_segmentation() -> None:
    paragraphs = DefaultParagraphSplitter().split("第一段。\n\n第二段。")
    sentences = DefaultSentenceSplitter().split("第一句。第二句。")
    assert len(paragraphs) == 2
    assert len(sentences) == 2
