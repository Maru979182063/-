from abc import ABC, abstractmethod
from typing import Any


class BaseWindowGenerator(ABC):
    @abstractmethod
    def generate(self, paragraphs: list[dict[str, Any]], sentences: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        ...
