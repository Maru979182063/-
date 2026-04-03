from typing import Any

from app.infra.kb.base import KnowledgeBaseAdapter


class DifyKnowledgeBaseAdapter(KnowledgeBaseAdapter):
    name = "dify"

    def upsert_material(self, material: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
        return {"adapter": self.name, "material_id": material["id"], "status": "stubbed"}

    def update_sync_state(self, material_id: str, enabled: bool) -> None:
        return None

    def query_candidates(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        return []
