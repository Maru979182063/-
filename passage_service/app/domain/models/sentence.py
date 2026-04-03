from pydantic import BaseModel


class Sentence(BaseModel):
    id: str
    article_id: str
    paragraph_id: str | None = None
    paragraph_index: int
    sentence_index: int
    text: str
