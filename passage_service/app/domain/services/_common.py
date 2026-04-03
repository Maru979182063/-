from sqlalchemy.orm import Session

from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository
from app.infra.db.repositories.audit_repo_sqlalchemy import SQLAlchemyAuditRepository
from app.infra.db.repositories.candidate_span_repo_sqlalchemy import SQLAlchemyCandidateSpanRepository
from app.infra.db.repositories.feedback_repo_sqlalchemy import SQLAlchemyFeedbackRepository
from app.infra.db.repositories.job_repo_sqlalchemy import SQLAlchemyJobRepository
from app.infra.db.repositories.material_span_repo_sqlalchemy import SQLAlchemyMaterialSpanRepository
from app.infra.db.repositories.paragraph_repo_sqlalchemy import SQLAlchemyParagraphRepository
from app.infra.db.repositories.review_repo_sqlalchemy import SQLAlchemyReviewRepository
from app.infra.db.repositories.sentence_repo_sqlalchemy import SQLAlchemySentenceRepository


class ServiceBase:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.article_repo = SQLAlchemyArticleRepository(session)
        self.paragraph_repo = SQLAlchemyParagraphRepository(session)
        self.sentence_repo = SQLAlchemySentenceRepository(session)
        self.candidate_repo = SQLAlchemyCandidateSpanRepository(session)
        self.material_repo = SQLAlchemyMaterialSpanRepository(session)
        self.feedback_repo = SQLAlchemyFeedbackRepository(session)
        self.review_repo = SQLAlchemyReviewRepository(session)
        self.audit_repo = SQLAlchemyAuditRepository(session)
        self.job_repo = SQLAlchemyJobRepository(session)
