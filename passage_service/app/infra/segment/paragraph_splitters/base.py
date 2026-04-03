from abc import ABC, abstractmethod


class BaseParagraphSplitter(ABC):
    @abstractmethod
    def split(self, text: str) -> list[str]:
        ...
