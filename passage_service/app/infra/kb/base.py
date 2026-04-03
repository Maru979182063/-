from abc import ABC, abstractmethod
from typing import Any


class KnowledgeBaseAdapter(ABC):
    name: str

    @abstractmethod
    def upsert_material(self, material: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
        ...

    @abstractmethod
    def update_sync_state(self, material_id: str, enabled: bool) -> None:
        ...

    @abstractmethod
    def query_candidates(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        ...
