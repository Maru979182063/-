from abc import ABC, abstractmethod
from typing import Any


class BaseLLMProvider(ABC):
    @abstractmethod
    def is_enabled(self) -> bool:
        ...

    @abstractmethod
    def generate_json(self, *, model: str, instructions: str, input_payload: dict[str, Any]) -> dict[str, Any]:
        ...
