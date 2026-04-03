from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DB_URL = "sqlite:///C:/Users/Maru/Documents/agent/passage_service/passage_service.db"
os.environ.setdefault("PASSAGE_DATABASE_URL", DEFAULT_DB_URL)

from app.domain.services.process_service import ProcessService
from app.infra.db.session import get_session, init_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reprocess existing articles to refresh segmentation/tagging/material metadata.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of articles to reprocess. 0 means all matches.")
    parser.add_argument(
        "--missing-structure-only",
        action="store_true",
        help="Only reprocess articles whose material spans are missing structure/readability fields.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Optional source id filter. Can be passed multiple times.",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=None,
        help="Optional article status filter. Can be passed multiple times.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "exports" / f"reprocess_material_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        help="Path to the JSON summary output.",
    )
    return parser.parse_args()


def load_article_ids(args: argparse.Namespace) -> list[str]:
    session = get_session()
    try:
        conditions: list[str] = []
        params: dict[str, object] = {}

        if args.missing_structure_only:
            conditions.append(
                "("
                "json_extract(coalesce(ms.universal_profile, '{}'), '$.material_structure_label') is null "
                "or json_extract(coalesce(ms.universal_profile, '{}'), '$.standalone_readability') is null"
                ")"
            )

        if args.source:
            placeholders = []
            for index, value in enumerate(args.source):
                key = f"source_{index}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(f"a.source in ({', '.join(placeholders)})")

        if args.status:
            placeholders = []
            for index, value in enumerate(args.status):
                key = f"status_{index}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(f"a.status in ({', '.join(placeholders)})")

        where_clause = f"where {' and '.join(conditions)}" if conditions else ""
        limit_clause = "limit :limit" if args.limit and args.limit > 0 else ""
        if limit_clause:
            params["limit"] = args.limit

        query = f"""
            select distinct a.id
            from articles a
            left join material_spans ms on ms.article_id = a.id
            {where_clause}
            order by a.updated_at desc
            {limit_clause}
        """
        return list(session.execute(text(query), params).scalars())
    finally:
        session.close()


def reprocess_articles(article_ids: list[str]) -> dict[str, object]:
    summary: dict[str, object] = {
        "requested_count": len(article_ids),
        "processed_count": 0,
        "errors": [],
        "processed_article_ids": [],
    }
    for index, article_id in enumerate(article_ids, start=1):
        session = get_session()
        try:
            ProcessService(session).process_article(article_id, "full")
            summary["processed_count"] = int(summary["processed_count"]) + 1
            processed_ids = summary["processed_article_ids"]
            assert isinstance(processed_ids, list)
            processed_ids.append(article_id)
            print(f"[{index}/{len(article_ids)}] ok {article_id}")
        except Exception as exc:  # noqa: BLE001
            errors = summary["errors"]
            assert isinstance(errors, list)
            errors.append({"article_id": article_id, "error": str(exc)})
            print(f"[{index}/{len(article_ids)}] error {article_id}: {exc}")
        finally:
            session.close()
    return summary


def main() -> int:
    args = parse_args()
    init_db()
    article_ids = load_article_ids(args)
    if not article_ids:
        payload = {"requested_count": 0, "processed_count": 0, "errors": [], "processed_article_ids": []}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("No matching articles found.")
        print(f"Summary written to: {args.output}")
        return 0

    summary = reprocess_articles(article_ids)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Summary written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
