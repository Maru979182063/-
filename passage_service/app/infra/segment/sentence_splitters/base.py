from abc import ABC, abstractmethod


class BaseSentenceSplitter(ABC):
    @abstractmethod
    def split(self, paragraph: str) -> list[str]:
        ...
