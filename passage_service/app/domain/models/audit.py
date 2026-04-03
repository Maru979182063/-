from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AuditEvent(BaseModel):
    id: str
    entity_type: str
    entity_id: str
    action: str
    actor: str = "system"
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
