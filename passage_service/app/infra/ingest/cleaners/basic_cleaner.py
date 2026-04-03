import re

from app.infra.ingest.cleaners.base import BaseCleaner


class BasicCleaner(BaseCleaner):
    def clean(self, raw_text: str) -> str:
        text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()
