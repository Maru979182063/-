import json
from pathlib import Path

from app.core.config import get_settings
from app.domain.services._common import ServiceBase


class ReviewExportService(ServiceBase):
    def export_article_review(self, article_id: str) -> dict:
        article = self.article_repo.get(article_id)
        if article is None:
            return {"article_id": article_id, "status": "not_found"}

        candidates = self.candidate_repo.list_by_article(article_id)
        materials = self.material_repo.search({"article_id": article_id})

        export_dir = Path(get_settings().config_dir).parent.parent / "review_samples" / "processed" / article_id
        export_dir.mkdir(parents=True, exist_ok=True)

        summary = {
            "article_id": article.id,
            "source": article.source,
            "source_url": article.source_url,
            "title": article.title,
            "status": article.status,
            "candidate_count": len(candidates),
            "material_count": len(materials),
            "candidates": [
                {
                    "id": item.id,
                    "span_type": item.span_type,
                    "status": item.status,
                    "text_preview": item.text[:200],
                }
                for item in candidates
            ],
            "materials": [
                {
                    "id": item.id,
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
                    "reject_reason": item.reject_reason,
                    "quality_flags": item.quality_flags,
                    "family_scores": item.family_scores,
                    "source": item.source,
                    "source_tail": item.source_tail,
                    "integrity": item.integrity,
                    "variants": item.variants,
                    "text_preview": item.text[:300],
                }
                for item in materials
            ],
        }

        json_path = export_dir / "review.json"
        txt_path = export_dir / "review.txt"
        json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            f"ARTICLE_ID: {article.id}",
            f"SOURCE: {article.source}",
            f"URL: {article.source_url}",
            f"TITLE: {article.title}",
            f"STATUS: {article.status}",
            f"CANDIDATE_COUNT: {len(candidates)}",
            f"MATERIAL_COUNT: {len(materials)}",
            "",
            "MATERIALS:",
        ]
        for item in materials:
            lines.extend(
                [
                    f"- MATERIAL_ID: {item.id}",
                    f"  STATUS: {item.status}",
                    f"  RELEASE_CHANNEL: {item.release_channel}",
                    f"  PRIMARY_FAMILY: {item.primary_family}",
                    f"  PRIMARY_LABEL: {item.primary_label}",
                    f"  CANDIDATE_LABELS: {json.dumps(item.candidate_labels, ensure_ascii=False)}",
                    f"  PARALLEL_FAMILIES: {json.dumps(item.parallel_families, ensure_ascii=False)}",
                    f"  CAPABILITY_SCORES: {json.dumps(item.capability_scores, ensure_ascii=False)}",
                    f"  STRUCTURE_FEATURES: {json.dumps(item.structure_features, ensure_ascii=False)}",
                    f"  PRIMARY_ROUTE: {json.dumps(item.primary_route, ensure_ascii=False)}",
                    f"  SECONDARY_CANDIDATES: {json.dumps(item.secondary_candidates, ensure_ascii=False)}",
                    f"  DECISION_TRACE: {json.dumps(item.decision_trace, ensure_ascii=False)}",
                    f"  REJECT_REASON: {item.reject_reason}",
                    f"  QUALITY_FLAGS: {json.dumps(item.quality_flags, ensure_ascii=False)}",
                    f"  FAMILY_SCORES: {json.dumps(item.family_scores, ensure_ascii=False)}",
                    f"  SUBTYPE_CANDIDATES: {json.dumps(item.subtype_candidates, ensure_ascii=False)}",
                    f"  SOURCE_TAIL: {item.source_tail}",
                    f"  INTEGRITY: {json.dumps(item.integrity, ensure_ascii=False)}",
                    f"  VARIANTS: {json.dumps(item.variants, ensure_ascii=False)}",
                    f"  TEXT_PREVIEW: {item.text[:300]}",
                    "",
                ]
            )
        txt_path.write_text("\n".join(lines), encoding="utf-8")

        self.audit_repo.log("article", article_id, "review_export", {"dir": str(export_dir)})
        return {
            "article_id": article_id,
            "status": "exported",
            "dir": str(export_dir),
            "json_path": str(json_path),
            "txt_path": str(txt_path),
        }
