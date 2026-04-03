from app.core.config import get_config_bundle
from app.domain.services._common import ServiceBase
from app.infra.kb.dify_adapter import DifyKnowledgeBaseAdapter
from app.infra.kb.noop_adapter import NoopKnowledgeBaseAdapter


class SyncService(ServiceBase):
    def __init__(self, session) -> None:
        super().__init__(session)
        adapter_name = get_config_bundle().sync.get("default_adapter", "noop")
        self.adapter = DifyKnowledgeBaseAdapter() if adapter_name == "dify" else NoopKnowledgeBaseAdapter()

    def upsert_material(self, material_id: str) -> dict:
        material = self.material_repo.get(material_id)
        result = self.adapter.upsert_material(
            {"id": material.id, "text": material.text},
            {"knowledge_tags": material.knowledge_tags, "fit_scores": material.fit_scores},
        )
        self.audit_repo.log("material", material_id, "sync", result)
        return result
