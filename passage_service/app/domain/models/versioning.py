from pydantic import BaseModel


class RuleVersionSnapshot(BaseModel):
    segmentation_version: str
    tag_version: str
    fit_version: str
