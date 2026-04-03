from pydantic import BaseModel, Field


class SpanVersionSet(BaseModel):
    segment_version: str
    universal_tag_version: str
    route_version: str
    family_tag_version: str


class SourceInfo(BaseModel):
    source_id: str
    source_name: str
    source_url: str
    article_title: str
    publish_time: str
    channel: str
    crawl_batch: str


class LabelDecisionTrace(BaseModel):
    selected: str | None = None
    rejected: list[str] = Field(default_factory=list)
    reason: str = ""


class SpanRecord(BaseModel):
    span_id: str
    article_id: str
    text: str
    paragraph_count: int
    sentence_count: int
    source_domain: str | None = None
    source: SourceInfo | None = None
    status: str = "new"
    version: SpanVersionSet
