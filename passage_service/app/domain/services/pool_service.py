from collections import Counter

from sqlalchemy import select

from app.domain.services._common import ServiceBase
from app.infra.db.orm.article import ArticleORM
from app.infra.db.orm.material_span import MaterialSpanORM
from app.rules.family_config import get_family_names


class PoolService(ServiceBase):
    def __init__(self, session) -> None:
        super().__init__(session)
        family_names = get_family_names()
        self.family_to_question_type = {
            family_names[0]: "main_idea",
            family_names[1]: "main_idea",
            family_names[2]: "sentence_fill",
            family_names[3]: "sentence_order",
            family_names[4]: "continuation",
        }

    def create_material(self, **kwargs):
        material = self.material_repo.create(**kwargs)
        self.audit_repo.log("material", material.id, "promote", {"status": material.status, "release_channel": material.release_channel})
        return material

    def search(self, filters: dict) -> list[dict]:
        items = self.material_repo.search(filters)
        threshold = filters.get("fit_score_threshold")
        subtype = filters.get("subtype")
        primary_family = filters.get("primary_family")
        document_genre = filters.get("document_genre")
        material_structure_label = filters.get("material_structure_label")
        serialized: list[dict] = []
        for item in items:
            if primary_family and item.primary_family != primary_family:
                continue
            if subtype and subtype not in item.secondary_subtypes and item.primary_subtype != subtype:
                continue
            if threshold is not None and max(item.fit_scores.values() or [0.0]) < threshold:
                continue
            item_feature_profile = item.feature_profile or {}
            item_universal_profile = item.universal_profile or {}
            item_document_genre = (item.feature_profile or {}).get("document_genre") or (item.universal_profile or {}).get("document_genre")
            if document_genre and item_document_genre != document_genre:
                continue
            item_material_structure = item_feature_profile.get("material_structure_label") or item_universal_profile.get("material_structure_label")
            if material_structure_label and item_material_structure != material_structure_label:
                continue
            serialized.append(
                {
                    "id": item.id,
                    "article_id": item.article_id,
                    "material_family_id": item.material_family_id,
                    "text": item.text,
                    "source": item.source,
                    "source_tail": item.source_tail,
                    "integrity": item.integrity,
                    "status": item.status,
                    "release_channel": item.release_channel,
                    "primary_family": item.primary_family,
                    "primary_label": item.primary_label,
                    "candidate_labels": item.candidate_labels,
                    "parallel_families": item.parallel_families,
                    "capability_scores": item.capability_scores,
                    "structure_features": item.structure_features,
                    "primary_route": item.primary_route,
                    "subtype_candidates": item.subtype_candidates,
                    "secondary_candidates": item.secondary_candidates,
                    "decision_trace": item.decision_trace,
                    "variants": item.variants,
                    "fit_scores": item.fit_scores,
                    "family_scores": item.family_scores,
                    "document_genre": item_document_genre,
                    "document_genre_candidates": (item.feature_profile or {}).get("document_genre_candidates", []),
                    "material_structure_label": item_material_structure,
                    "material_structure_reason": item_feature_profile.get("material_structure_reason") or item_universal_profile.get("material_structure_reason"),
                    "standalone_readability": item_feature_profile.get("standalone_readability") or item_universal_profile.get("standalone_readability") or 0.0,
                    "reject_reason": item.reject_reason,
                    "knowledge_tags": item.knowledge_tags,
                    "quality_flags": item.quality_flags,
                    "quality_score": item.quality_score,
                }
            )
        return serialized

    def get_material(self, material_id: str) -> dict | None:
        item = self.material_repo.get(material_id)
        if item is None:
            return None
        return {
            "id": item.id,
            "article_id": item.article_id,
            "candidate_span_id": item.candidate_span_id,
            "normalized_text_hash": item.normalized_text_hash,
            "material_family_id": item.material_family_id,
            "is_primary": item.is_primary,
            "text": item.text,
            "source": item.source,
            "source_tail": item.source_tail,
            "integrity": item.integrity,
            "universal_profile": item.universal_profile,
            "family_scores": item.family_scores,
            "capability_scores": item.capability_scores,
            "parallel_families": item.parallel_families,
            "structure_features": item.structure_features,
            "family_profiles": item.family_profiles,
            "subtype_candidates": item.subtype_candidates,
            "secondary_candidates": item.secondary_candidates,
            "candidate_labels": item.candidate_labels,
            "primary_label": item.primary_label,
            "decision_trace": item.decision_trace,
            "primary_route": item.primary_route,
            "reject_reason": item.reject_reason,
            "variants": item.variants,
            "feature_profile": item.feature_profile,
            "document_genre": (item.feature_profile or {}).get("document_genre") or (item.universal_profile or {}).get("document_genre"),
            "document_genre_candidates": (item.feature_profile or {}).get("document_genre_candidates", []),
            "material_structure_label": (item.feature_profile or {}).get("material_structure_label") or (item.universal_profile or {}).get("material_structure_label"),
            "material_structure_reason": (item.feature_profile or {}).get("material_structure_reason") or (item.universal_profile or {}).get("material_structure_reason"),
            "standalone_readability": (item.feature_profile or {}).get("standalone_readability") or (item.universal_profile or {}).get("standalone_readability") or 0.0,
            "fit_scores": item.fit_scores,
            "quality_flags": item.quality_flags,
            "status": item.status,
            "release_channel": item.release_channel,
        }

    def promote(self, material_id: str, status: str, release_channel: str):
        material = self.material_repo.update_status(material_id, status=status, release_channel=release_channel)
        self.audit_repo.log("material", material_id, "state_change", {"status": status, "release_channel": release_channel})
        return material

    def get_pool_stats(self, *, status: str | None = None, release_channel: str | None = None) -> dict:
        articles = list(self.session.scalars(select(ArticleORM)))
        materials = list(self.session.scalars(select(MaterialSpanORM)))

        article_by_id = {article.id: article for article in articles}
        filtered_materials = [
            item
            for item in materials
            if (status is None or item.status == status)
            and (release_channel is None or item.release_channel == release_channel)
        ]
        promoted_stable = [
            item
            for item in materials
            if item.status == "promoted" and item.release_channel == "stable"
        ]

        article_status_counts = Counter(article.status for article in articles)
        article_source_counts = Counter(article.source for article in articles)
        material_status_channel_counts = Counter(f"{item.status}:{item.release_channel}" for item in materials)
        promoted_source_counts = Counter(
            (article_by_id.get(item.article_id).source if article_by_id.get(item.article_id) else "unknown")
            for item in promoted_stable
        )
        promoted_genre_counts = Counter(
            (item.feature_profile or {}).get("document_genre")
            or (item.universal_profile or {}).get("document_genre")
            or "unknown"
            for item in promoted_stable
        )
        promoted_family_counts = Counter(item.primary_family or "unknown" for item in promoted_stable)
        promoted_article_counts = Counter(item.article_id for item in promoted_stable)
        question_type_coverage_all = Counter()
        question_type_coverage_stable = Counter()
        for item in materials:
            covered_types = self._material_question_types(item)
            for question_type in covered_types:
                question_type_coverage_all[question_type] += 1
                if item.status == "promoted" and item.release_channel == "stable":
                    question_type_coverage_stable[question_type] += 1

        top_articles: list[dict] = []
        for article_id, count in promoted_article_counts.most_common(10):
            article = article_by_id.get(article_id)
            top_articles.append(
                {
                    "article_id": article_id,
                    "title": article.title if article else None,
                    "source": article.source if article else None,
                    "material_count": count,
                }
            )

        return {
            "filters": {"status": status, "release_channel": release_channel},
            "articles_total": len(articles),
            "materials_total": len(materials),
            "filtered_materials_total": len(filtered_materials),
            "promoted_stable_total": len(promoted_stable),
            "promoted_stable_unique_articles": len({item.article_id for item in promoted_stable}),
            "article_status_counts": dict(article_status_counts),
            "article_source_counts": dict(article_source_counts.most_common()),
            "material_status_channel_counts": dict(material_status_channel_counts),
            "promoted_stable_by_source": dict(promoted_source_counts.most_common()),
            "promoted_stable_by_document_genre": dict(promoted_genre_counts.most_common()),
            "promoted_stable_by_primary_family": dict(promoted_family_counts.most_common()),
            "question_type_coverage_all": dict(question_type_coverage_all),
            "question_type_coverage_promoted_stable": dict(question_type_coverage_stable),
            "promoted_stable_top_articles": top_articles,
        }

    def _material_question_types(self, item: MaterialSpanORM) -> set[str]:
        families = {item.primary_family} if item.primary_family else set()
        for family_entry in item.parallel_families or []:
            if isinstance(family_entry, dict) and family_entry.get("family"):
                families.add(family_entry["family"])
        covered: set[str] = set()
        for family in families:
            question_type = self.family_to_question_type.get(family)
            if question_type:
                covered.add(question_type)
        return covered
