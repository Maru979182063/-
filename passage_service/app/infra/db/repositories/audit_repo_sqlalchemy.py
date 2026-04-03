from typing import Any

from sqlalchemy.orm import Session

from app.infra.db.orm.audit import AuditEventORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyAuditRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def log(self, entity_type: str, entity_id: str, action: str, payload: dict[str, Any] | None = None, actor: str = "system") -> AuditEventORM:
        event = AuditEventORM(
            id=new_id("audit"),
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            actor=actor,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.commit()
        self.session.refresh(event)
        return event
