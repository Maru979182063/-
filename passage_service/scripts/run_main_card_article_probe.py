from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from app.domain.services.material_v2_index_service import MaterialV2IndexService
from app.infra.db.orm.article import ArticleORM
from app.infra.db.orm.material_span import MaterialSpanORM
from app.infra.db.session import get_session, init_db
from app.infra.plugins.loader import load_plugins

MAIN_FAMILIES = ("title_selection", "sentence_fill", "sentence_order")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sequential main-card article probe with per-article checkpoints.")
    parser.add_argument("--article-id", dest="article_ids", action="append", required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    parser.add_argument("--model", type=str, default="unknown")
    parser.add_argument("--base-url", type=str, default="")
    return parser.parse_args()


def article_materials(session, article_id: str) -> list[MaterialSpanORM]:
    return list(
        session.scalars(
            select(MaterialSpanORM).where(
                MaterialSpanORM.article_id == article_id,
                MaterialSpanORM.is_primary.is_(True),
            )
        )
    )


def served_summary(materials: list[MaterialSpanORM]) -> dict:
    family_total = Counter()
    family_nonlegacy = Counter()
    cards = defaultdict(Counter)
    for material in materials:
        payload = getattr(material, "v2_index_payload", None) or {}
        for family, item in payload.items():
            if family not in MAIN_FAMILIES:
                continue
            family_total[family] += 1
            qctx = dict((item or {}).get("question_ready_context") or {})
            material_card = str(qctx.get("selected_material_card") or "")
            if material_card:
                cards[family][material_card] += 1
                if not material_card.startswith("legacy."):
                    family_nonlegacy[family] += 1
    return {
        "family_total": dict(family_total),
        "family_nonlegacy": dict(family_nonlegacy),
        "cards": {family: dict(counter) for family, counter in cards.items()},
    }


def to_markdown(report: dict) -> str:
    lines = [
        "# Main Card Article Probe",
        "",
        f"- started_at: {report.get('started_at', '')}",
        f"- model: {report.get('model', '')}",
        f"- base_url: {report.get('base_url', '')}",
        f"- status: {report.get('status', '')}",
        "",
        "## Articles",
    ]
    for item in report.get("articles", []):
        lines.extend(
            [
                f"### {item.get('title', item.get('article_id', ''))}",
                f"- article_id: {item.get('article_id', '')}",
                f"- source: {item.get('source', '')}",
                f"- status: {item.get('status', item.get('status_before', ''))}",
                f"- primary_material_count_before: {item.get('primary_material_count_before', 0)}",
                f"- primary_material_count_after: {item.get('primary_material_count_after', '')}",
                f"- before.family_nonlegacy: {json.dumps((item.get('before') or {}).get('family_nonlegacy', {}), ensure_ascii=False)}",
                f"- after.family_nonlegacy: {json.dumps((item.get('after') or {}).get('family_nonlegacy', {}), ensure_ascii=False)}",
                "",
            ]
        )
        if item.get("error_message"):
            lines.append(f"- error: {item['error_message']}")
            lines.append("")
    return "\n".join(lines) + "\n"


def write_report(output_json: Path, output_md: Path, report: dict) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    output_md.write_text(to_markdown(report), encoding="utf-8")


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()
    session = get_session()
    service = MaterialV2IndexService(session)
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "model": args.model,
        "base_url": args.base_url,
        "status": "running",
        "articles": [],
    }
    write_report(args.output_json, args.output_md, report)
    try:
        for article_id in args.article_ids:
            article = session.get(ArticleORM, article_id)
            if article is None:
                row = {"article_id": article_id, "status": "missing"}
                report["articles"].append(row)
                write_report(args.output_json, args.output_md, report)
                continue

            before_materials = article_materials(session, article_id)
            row = {
                "article_id": article_id,
                "title": str(getattr(article, "title", "") or ""),
                "source": str(getattr(article, "source", "") or ""),
                "status_before": str(getattr(article, "status", "") or ""),
                "primary_material_count_before": len(before_materials),
                "before": served_summary(before_materials),
                "status": "running",
            }
            report["articles"].append(row)
            write_report(args.output_json, args.output_md, report)

            try:
                outcome = service.precompute({"article_ids": [article_id], "primary_only": True})
                session.commit()
                row["precompute"] = outcome
                row["status"] = "ok"
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                row["status"] = "error"
                row["error_type"] = type(exc).__name__
                row["error_message"] = str(exc)
                write_report(args.output_json, args.output_md, report)
                continue

            after_materials = article_materials(session, article_id)
            row["after"] = served_summary(after_materials)
            row["primary_material_count_after"] = len(after_materials)
            write_report(args.output_json, args.output_md, report)

        report["status"] = "completed"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_report(args.output_json, args.output_md, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
