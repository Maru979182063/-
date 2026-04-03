from abc import ABC, abstractmethod


class BaseCrawlerFetcher(ABC):
    @abstractmethod
    def fetch_text(self, url: str) -> str:
        ...
