import csv
import json
import re
from datetime import datetime
from pathlib import Path

from app.core.config import get_settings
from app.domain.services._common import ServiceBase


def _safe_slug(value: str, fallback: str) -> str:
    text = (value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"[<>:\"/\\|?*\r\n\t]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:80] or fallback


class DifyExportService(ServiceBase):
    def export_materials(
        self,
        article_ids: list[str] | None = None,
        output_dir: str | None = None,
        limit: int | None = None,
        include_gray: bool = True,
    ) -> dict:
        root = Path(output_dir) if output_dir else self._default_output_dir()
        docs_dir = root / "materials"
        article_docs_dir = root / "articles_clean"
        docs_dir.mkdir(parents=True, exist_ok=True)
        article_docs_dir.mkdir(parents=True, exist_ok=True)

        article_filter = set(article_ids or [])
        exported_rows: list[dict] = []
        article_rows: list[dict] = []

        if article_filter:
            articles = [self.article_repo.get(article_id) for article_id in article_filter]
            articles = [item for item in articles if item is not None]
        else:
            articles = self.article_repo.list(limit=1000)

        for article in articles:
            article_rows.append(
                {
                    "article_id": article.id,
                    "source": article.source,
                    "source_url": article.source_url,
                    "title": article.title,
                    "status": article.status,
                    "domain": article.domain,
                    "created_at": article.created_at.isoformat() if article.created_at else None,
                    "updated_at": article.updated_at.isoformat() if article.updated_at else None,
                }
            )
            article_file_name = f"{_safe_slug((article.title or article.id), article.id)}_{article.id}.md"
            article_file_path = article_docs_dir / article_file_name
            article_file_path.write_text(self._build_article_markdown(article), encoding="utf-8")

            materials = self.material_repo.search({"article_id": article.id, "primary_only": True})
            for material in materials:
                if material.status in {"rejected", "deprecated"}:
                    continue
                if not include_gray and material.release_channel == "gray":
                    continue
                exported_rows.append(self._serialize_export_row(article, material, root))

        exported_rows.sort(
            key=lambda item: (
                item["release_channel"] != "stable",
                -item["quality_score"],
                item["article_id"],
                item["material_id"],
            )
        )
        if limit is not None:
            exported_rows = exported_rows[:limit]

        manifest_path = root / "manifest.jsonl"
        csv_path = root / "materials.csv"
        articles_csv_path = root / "articles.csv"
        summary_path = root / "summary.json"

        with manifest_path.open("w", encoding="utf-8") as fh:
            for row in exported_rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

        with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "material_id",
                    "article_id",
                    "source_name",
                    "article_title",
                    "source_url",
                    "publish_time",
                    "primary_family",
                    "primary_label",
                    "release_channel",
                    "quality_score",
                    "file_path",
                ],
            )
            writer.writeheader()
            for row in exported_rows:
                writer.writerow(
                    {
                        "material_id": row["material_id"],
                        "article_id": row["article_id"],
                        "source_name": row["source"]["source_name"],
                        "article_title": row["source"]["article_title"],
                        "source_url": row["source"]["source_url"],
                        "publish_time": row["source"]["publish_time"],
                        "primary_family": row["primary_family"],
                        "primary_label": row["primary_label"],
                        "release_channel": row["release_channel"],
                        "quality_score": row["quality_score"],
                        "file_path": row["file_path"],
                    }
                )

        with articles_csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "article_id",
                    "source",
                    "source_url",
                    "title",
                    "status",
                    "domain",
                    "created_at",
                    "updated_at",
                ],
            )
            writer.writeheader()
            for row in article_rows:
                writer.writerow(row)

        summary = {
            "exported_at": datetime.now().isoformat(),
            "output_dir": str(root),
            "article_count": len({row["article_id"] for row in exported_rows}),
            "material_count": len(exported_rows),
            "include_gray": include_gray,
            "manifest_path": str(manifest_path),
            "materials_csv_path": str(csv_path),
            "articles_csv_path": str(articles_csv_path),
            "article_docs_dir": str(article_docs_dir),
            "materials_dir": str(docs_dir),
        }
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    def _serialize_export_row(self, article, material, root: Path) -> dict:
        source = material.source or {}
        source_name = source.get("source_name") or article.source or "unknown_source"
        article_title = source.get("article_title") or article.title or material.id
        publish_time = source.get("publish_time") or ""
        file_slug = _safe_slug(f"{source_name}_{article_title}_{material.id}", material.id)
        file_name = f"{file_slug}.md"
        file_rel_path = str(Path("materials") / file_name)
        file_abs_path = root / file_rel_path
        file_abs_path.parent.mkdir(parents=True, exist_ok=True)
        file_abs_path.write_text(self._build_markdown(material, source_name, article_title, publish_time), encoding="utf-8")

        return {
            "material_id": material.id,
            "article_id": article.id,
            "candidate_span_id": material.candidate_span_id,
            "primary_family": material.primary_family,
            "primary_label": material.primary_label,
            "candidate_labels": material.candidate_labels or [],
            "parallel_families": material.parallel_families or [],
            "capability_scores": material.capability_scores or {},
            "structure_features": material.structure_features or {},
            "decision_trace": material.decision_trace or {},
            "source": {
                "source_id": source.get("source_id") or article.source,
                "source_name": source_name,
                "source_url": source.get("source_url") or article.source_url,
                "article_title": article_title,
                "publish_time": publish_time,
                "channel": source.get("channel") or "",
                "crawl_batch": source.get("crawl_batch") or "",
            },
            "source_tail": material.source_tail or "",
            "release_channel": material.release_channel,
            "status": material.status,
            "quality_score": material.quality_score,
            "quality_flags": material.quality_flags or [],
            "integrity": material.integrity or {},
            "knowledge_tags": material.knowledge_tags or [],
            "text": material.text,
            "file_path": file_rel_path,
        }

    def _build_markdown(self, material, source_name: str, article_title: str, publish_time: str) -> str:
        source_tail = material.source_tail or f"【来源：{source_name}《{article_title}》，{publish_time}】"
        candidate_labels = "、".join(material.candidate_labels or [])
        primary_label = material.primary_label or ""
        primary_family = material.primary_family or ""
        return "\n".join(
            [
                f"# {article_title}",
                "",
                f"- material_id: {material.id}",
                f"- primary_family: {primary_family}",
                f"- primary_label: {primary_label}",
                f"- candidate_labels: {candidate_labels}",
                f"- release_channel: {material.release_channel}",
                "",
                material.text.strip(),
                "",
                source_tail,
                "",
            ]
        ).strip() + "\n"

    def _build_article_markdown(self, article) -> str:
        return "\n".join(
            [
                f"# {article.title or article.id}",
                "",
                f"- article_id: {article.id}",
                f"- source: {article.source}",
                f"- source_url: {article.source_url}",
                f"- domain: {article.domain or ''}",
                "",
                (article.clean_text or article.raw_text or "").strip(),
                "",
            ]
        ).strip() + "\n"

    def _default_output_dir(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return Path(get_settings().config_dir).parent.parent / "exports" / f"dify_pack_{timestamp}"
