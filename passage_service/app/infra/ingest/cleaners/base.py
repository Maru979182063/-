from abc import ABC, abstractmethod


class BaseCleaner(ABC):
    @abstractmethod
    def clean(self, raw_text: str) -> str:
        ...
