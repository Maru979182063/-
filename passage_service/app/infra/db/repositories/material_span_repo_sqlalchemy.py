from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.infra.db.orm.article import ArticleORM
from app.infra.db.orm.material_span import MaterialSpanORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyMaterialSpanRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create(self, **kwargs) -> MaterialSpanORM:
        material = MaterialSpanORM(id=new_id("mat"), **kwargs)
        self.session.add(material)
        self.session.commit()
        self.session.refresh(material)
        return material

    def get(self, material_id: str) -> MaterialSpanORM | None:
        return self.session.get(MaterialSpanORM, material_id)

    def search(self, filters: dict[str, Any]) -> list[MaterialSpanORM]:
        stmt = select(MaterialSpanORM)
        if filters.get("primary_only", True):
            stmt = stmt.where(MaterialSpanORM.is_primary.is_(True))
        if article_id := filters.get("article_id"):
            stmt = stmt.where(MaterialSpanORM.article_id == article_id)
        if status := filters.get("status"):
            stmt = stmt.where(MaterialSpanORM.status == status)
        if release_channel := filters.get("release_channel"):
            stmt = stmt.where(MaterialSpanORM.release_channel == release_channel)
        if domain := filters.get("domain"):
            stmt = stmt.join(ArticleORM, MaterialSpanORM.article_id == ArticleORM.id).where(ArticleORM.domain == domain)
        if length_bucket := filters.get("length_bucket"):
            stmt = stmt.where(MaterialSpanORM.length_bucket == length_bucket)
        return list(self.session.scalars(stmt.order_by(MaterialSpanORM.created_at.desc())))

    def update_status(self, material_id: str, status: str, release_channel: str | None = None) -> MaterialSpanORM:
        material = self.session.get(MaterialSpanORM, material_id)
        material.status = status
        if release_channel:
            material.release_channel = release_channel
        self.session.commit()
        self.session.refresh(material)
        return material

    def update_metrics(self, material_id: str, **kwargs) -> MaterialSpanORM:
        material = self.session.get(MaterialSpanORM, material_id)
        for key, value in kwargs.items():
            setattr(material, key, value)
        self.session.commit()
        self.session.refresh(material)
        return material

    def demote_existing_for_article(self, article_id: str, exclude_material_ids: list[str] | None = None) -> int:
        stmt = (
            update(MaterialSpanORM)
            .where(MaterialSpanORM.article_id == article_id, MaterialSpanORM.is_primary.is_(True))
            .values(is_primary=False, status="deprecated")
        )
        if exclude_material_ids:
            stmt = stmt.where(MaterialSpanORM.id.not_in(exclude_material_ids))
        result = self.session.execute(stmt)
        self.session.commit()
        return int(result.rowcount or 0)

    def list_for_v2_index(
        self,
        *,
        material_ids: list[str] | None = None,
        article_ids: list[str] | None = None,
        status: str | None = None,
        release_channel: str | None = None,
        primary_only: bool = True,
        limit: int | None = None,
    ) -> list[MaterialSpanORM]:
        stmt = select(MaterialSpanORM)
        if primary_only:
            stmt = stmt.where(MaterialSpanORM.is_primary.is_(True))
        if material_ids:
            stmt = stmt.where(MaterialSpanORM.id.in_(material_ids))
        if article_ids:
            stmt = stmt.where(MaterialSpanORM.article_id.in_(article_ids))
        if status:
            stmt = stmt.where(MaterialSpanORM.status == status)
        if release_channel:
            stmt = stmt.where(MaterialSpanORM.release_channel == release_channel)
        stmt = stmt.order_by(MaterialSpanORM.updated_at.desc())
        if limit:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))

    def list_v2_cached(
        self,
        *,
        business_family_id: str,
        material_ids: list[str] | None = None,
        article_ids: list[str] | None = None,
        status: str | None = None,
        release_channel: str | None = None,
        limit: int | None = None,
    ) -> list[MaterialSpanORM]:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        if material_ids:
            stmt = stmt.where(MaterialSpanORM.id.in_(material_ids))
        if article_ids:
            stmt = stmt.where(MaterialSpanORM.article_id.in_(article_ids))
        if status:
            stmt = stmt.where(MaterialSpanORM.status == status)
        if release_channel:
            stmt = stmt.where(MaterialSpanORM.release_channel == release_channel)
        stmt = stmt.order_by(MaterialSpanORM.quality_score.desc(), MaterialSpanORM.updated_at.desc())
        items = list(self.session.scalars(stmt))
        filtered = [
            item
            for item in items
            if business_family_id in (item.v2_business_family_ids or [])
            and isinstance(item.v2_index_payload, dict)
            and item.v2_index_payload.get(business_family_id)
        ]
        if limit:
            return filtered[:limit]
        return filtered
