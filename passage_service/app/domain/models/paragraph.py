from pydantic import BaseModel


class Paragraph(BaseModel):
    id: str
    article_id: str
    paragraph_index: int
    text: str
    char_count: int
