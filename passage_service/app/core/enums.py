from enum import StrEnum


class ArticleStatus(StrEnum):
    NEW = "new"
    CLEANED = "cleaned"
    SEGMENTED = "segmented"
    TAGGED = "tagged"
    ARCHIVED = "archived"


class CandidateSpanStatus(StrEnum):
    NEW = "new"
    TAGGED = "tagged"
    REJECTED = "rejected"
    PROMOTED = "promoted"


class MaterialStatus(StrEnum):
    GRAY = "gray"
    ACTIVE = "active"
    PROMOTED = "promoted"
    DEPRECATED = "deprecated"
    REJECTED = "rejected"


class ReleaseChannel(StrEnum):
    GRAY = "gray"
    STABLE = "stable"


class ReviewStatus(StrEnum):
    AUTO_TAGGED = "auto_tagged"
    REVIEW_PENDING = "review_pending"
    REVIEW_CONFIRMED = "review_confirmed"
    REVIEW_REJECTED = "review_rejected"


class AuditAction(StrEnum):
    INGEST = "ingest"
    SEGMENT = "segment"
    TAG = "tag"
    PROMOTE = "promote"
    STATE_CHANGE = "state_change"
    SYNC = "sync"
    FEEDBACK = "feedback"
    REVIEW = "review"
