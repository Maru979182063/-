from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.infra.db.orm.article import ArticleORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyArticleRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, article_id: str) -> ArticleORM | None:
        return self.session.get(ArticleORM, article_id)

    def get_by_hash(self, content_hash: str) -> ArticleORM | None:
        return self.session.scalar(select(ArticleORM).where(ArticleORM.hash == content_hash))

    def get_by_source_url(self, source_url: str) -> ArticleORM | None:
        return self.session.scalar(select(ArticleORM).where(ArticleORM.source_url == source_url))

    def get_existing_source_urls(self, source_urls: list[str]) -> set[str]:
        if not source_urls:
            return set()
        stmt = select(ArticleORM.source_url).where(ArticleORM.source_url.in_(source_urls))
        return {row[0] for row in self.session.execute(stmt).all()}

    def list(self, limit: int = 200) -> list[ArticleORM]:
        stmt = select(ArticleORM).order_by(ArticleORM.created_at.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    def search_by_terms(self, terms: list[str], limit: int = 200) -> list[ArticleORM]:
        normalized_terms = [term.strip() for term in terms if term and term.strip()]
        if not normalized_terms:
            return self.list(limit=limit)
        like_conditions = []
        for term in normalized_terms[:12]:
            like_token = f"%{term}%"
            like_conditions.append(ArticleORM.title.ilike(like_token))
            like_conditions.append(ArticleORM.clean_text.ilike(like_token))
        stmt = select(ArticleORM).where(or_(*like_conditions)).order_by(ArticleORM.created_at.desc()).limit(limit)
        return list(self.session.scalars(stmt))

    def create(self, **kwargs) -> ArticleORM:
        article = ArticleORM(id=new_id("article"), **kwargs)
        self.session.add(article)
        self.session.commit()
        self.session.refresh(article)
        return article

    def update(self, article_id: str, **kwargs) -> ArticleORM:
        article = self.session.get(ArticleORM, article_id)
        for key, value in kwargs.items():
            setattr(article, key, value)
        self.session.commit()
        self.session.refresh(article)
        return article

    def update_status(self, article_id: str, status: str) -> ArticleORM:
        article = self.session.get(ArticleORM, article_id)
        article.status = status
        self.session.commit()
        self.session.refresh(article)
        return article
