import re

from app.infra.segment.sentence_splitters.base import BaseSentenceSplitter


class DefaultSentenceSplitter(BaseSentenceSplitter):
    def split(self, paragraph: str) -> list[str]:
        parts = re.split(r"(?<=[。！？!?])", paragraph)
        return [part.strip() for part in parts if part.strip()]
