from abc import ABC, abstractmethod
from typing import Any


class BaseArticleExtractor(ABC):
    @abstractmethod
    def extract(self, html: str, url: str, source_config: dict[str, Any]) -> dict[str, Any]:
        ...
