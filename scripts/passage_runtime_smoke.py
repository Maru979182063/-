from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx


DEFAULT_BASE_URL = "http://127.0.0.1:8001"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke check passage_service runtime database and v2 search.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="passage_service base URL.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds.")
    parser.add_argument(
        "--expected-database-mode",
        default="primary",
        help="Optional expected database mode. Pass an empty string to disable the check.",
    )
    parser.add_argument(
        "--expected-database-name",
        default="passage_service.db",
        help="Optional expected database filename. Pass an empty string to disable the check.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    base_url = args.base_url.rstrip("/")
    with httpx.Client(timeout=args.timeout, trust_env=False) as client:
        ready = client.get(f"{base_url}/readyz")
        ready.raise_for_status()
        ready_payload = ready.json()
        settings = ready_payload.get("settings") or {}
        checks = ready_payload.get("checks") or []
        database_check = next(
            (check.get("details") or {} for check in checks if check.get("name") == "database"),
            {},
        )
        database_mode = str(settings.get("database_mode") or database_check.get("database_mode") or "")
        resolved_database_path = str(
            settings.get("resolved_database_path") or database_check.get("resolved_database_path") or ""
        )
        if args.expected_database_mode and database_mode != args.expected_database_mode:
            raise RuntimeError(
                f"Unexpected database mode for {base_url}: expected {args.expected_database_mode!r}, got {database_mode!r}."
            )
        if args.expected_database_name:
            if not resolved_database_path:
                raise RuntimeError(f"readyz for {base_url} did not expose resolved_database_path.")
            if Path(resolved_database_path).name != args.expected_database_name:
                raise RuntimeError(
                    "Unexpected database filename for "
                    f"{base_url}: expected {args.expected_database_name!r}, got {Path(resolved_database_path).name!r}."
                )
        print("# readyz")
        print(json.dumps(ready_payload, ensure_ascii=False, indent=2))

        payload = {
            "business_family_id": "center_understanding",
            "question_card_id": "question.center_understanding.standard_v1",
            "candidate_limit": 5,
            "article_limit": 12,
            "min_card_score": 0.55,
            "business_card_ids": [],
            "preferred_business_card_ids": [],
            "query_terms": [],
            "topic": None,
            "text_direction": None,
            "document_genre": None,
            "material_structure_label": None,
            "target_length": None,
            "length_tolerance": 120,
            "structure_constraints": {},
            "enable_anchor_adaptation": True,
            "article_ids": [],
        }
        search = client.post(f"{base_url}/materials/v2/search", json=payload)
        search.raise_for_status()
        search_payload = search.json()
        items = search_payload.get("items") or []
        preview = [
            {
                "material_id": item.get("material_id") or item.get("candidate_id"),
                "article_id": item.get("article_id"),
                "selected_material_card": item.get("selected_material_card"),
                "selected_business_card": item.get("selected_business_card"),
            }
            for item in items[:3]
        ]
        print("\n# search")
        print(
            json.dumps(
                {
                    "status_code": search.status_code,
                    "database_mode": database_mode,
                    "resolved_database_path": resolved_database_path,
                    "item_count": len(items),
                    "warnings": search_payload.get("warnings") or [],
                    "preview": preview,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
