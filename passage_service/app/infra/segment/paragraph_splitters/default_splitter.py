from app.infra.segment.paragraph_splitters.base import BaseParagraphSplitter


class DefaultParagraphSplitter(BaseParagraphSplitter):
    def split(self, text: str) -> list[str]:
        return [chunk.strip() for chunk in text.split("\n\n") if chunk.strip()]
