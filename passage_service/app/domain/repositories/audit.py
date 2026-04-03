from typing import Any, Protocol

from app.infra.db.orm.audit import AuditEventORM


class AuditRepository(Protocol):
    def log(self, entity_type: str, entity_id: str, action: str, payload: dict[str, Any] | None = None, actor: str = "system") -> AuditEventORM: ...
