from datetime import datetime

from pydantic import BaseModel


class Article(BaseModel):
    id: str
    source: str
    source_url: str
    title: str | None = None
    raw_text: str
    clean_text: str
    language: str = "zh"
    domain: str | None = None
    status: str
    hash: str
    created_at: datetime
    updated_at: datetime
