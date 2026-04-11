from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.ingest_service import IngestService  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.domain.services.material_v2_index_service import MaterialV2IndexService  # noqa: E402
from app.domain.services.process_service import ProcessService  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.orm.review import TaggingReviewORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


DEFAULT_FAMILIES = [
    "sentence_fill",
    "sentence_order",
    "title_selection",
    "continuation",
]


@dataclass
class ParsedArticle:
    source_file: str
    title: str
    source: str
    source_url: str
    domain: str | None
    raw_text: str
    meta: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual import + consumption audit for Coze material files.")
    parser.add_argument("--input-files", nargs="+", default=None, help="JSON/JSONL files from Coze export.")
    parser.add_argument("--business-families", nargs="+", default=DEFAULT_FAMILIES, help="Families used for consumption probe.")
    parser.add_argument("--min-text-length", type=int, default=180, help="Minimum cleaned text length for ingest.")
    parser.add_argument("--max-items-per-file", type=int, default=0, help="Optional cap for each file; 0 means no cap.")
    parser.add_argument("--truncate-chars", type=int, default=0, help="Optional hard truncate before ingest; 0 means no truncate.")
    parser.add_argument("--process-existing", action="store_true", help="Also process deduped existing articles.")
    parser.add_argument("--dry-run", action="store_true", help="Only parse/validate files; no DB writes.")
    parser.add_argument("--report-path", type=str, default="", help="Optional fixed JSON report path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()

    input_values = list(args.input_files or [])
    if not input_values:
        input_values = [str(path) for path in discover_default_input_files()]

    files = [resolve_input_path(item) for item in input_values]
    missing_files = [str(path) for path in files if not path.exists()]
    if not files:
        missing_files.append("NO_INPUT_FILES_FOUND: provide --input-files or place point_*.json under workspace")

    parsed: list[ParsedArticle] = []
    parse_errors: list[dict[str, Any]] = []
    per_file_raw_count: Counter[str] = Counter()
    per_file_accepted_count: Counter[str] = Counter()

    for file_path in files:
        if not file_path.exists():
            continue
        try:
            parsed_items = parse_file(file_path)
        except Exception as exc:  # noqa: BLE001
            parse_errors.append({"file": str(file_path), "error": str(exc)})
            continue
        if args.max_items_per_file > 0:
            parsed_items = parsed_items[: args.max_items_per_file]
        per_file_raw_count[str(file_path)] = len(parsed_items)
        for item in parsed_items:
            raw_text = item.raw_text.strip()
            if args.truncate_chars and args.truncate_chars > 0:
                raw_text = raw_text[: args.truncate_chars].strip()
            if len(raw_text) < args.min_text_length:
                continue
            parsed.append(
                ParsedArticle(
                    source_file=item.source_file,
                    title=item.title,
                    source=item.source,
                    source_url=item.source_url,
                    domain=item.domain,
                    raw_text=raw_text,
                    meta=item.meta,
                )
            )
            per_file_accepted_count[str(file_path)] += 1

    session = get_session()
    try:
        baseline_article_ids = set(session.scalars(select(ArticleORM.id)).all())
        ingest_service = IngestService(session)
        process_service = ProcessService(session)
        v2_index_service = MaterialV2IndexService(session)
        v2_service = MaterialPipelineV2Service(session)

        ingested_article_ids: list[str] = []
        new_article_ids: set[str] = set()
        ingest_failures: list[dict[str, Any]] = []

        if not args.dry_run:
            for idx, item in enumerate(parsed, start=1):
                try:
                    article = ingest_service.ingest(
                        {
                            "source": item.source,
                            "source_url": item.source_url,
                            "title": item.title,
                            "raw_text": item.raw_text,
                            "language": "zh",
                            "domain": item.domain,
                        }
                    )
                    ingested_article_ids.append(article.id)
                    if article.id not in baseline_article_ids:
                        new_article_ids.add(article.id)
                except Exception as exc:  # noqa: BLE001
                    ingest_failures.append(
                        {
                            "index": idx,
                            "source_file": item.source_file,
                            "source_url": item.source_url,
                            "error": str(exc),
                        }
                    )

        processed_article_ids: list[str] = []
        process_failures: list[dict[str, Any]] = []
        target_process_ids = sorted(set(ingested_article_ids)) if args.process_existing else sorted(new_article_ids)
        if not args.dry_run:
            for article_id in target_process_ids:
                try:
                    process_service.process_article(article_id, mode="full")
                    processed_article_ids.append(article_id)
                except Exception as exc:  # noqa: BLE001
                    process_failures.append({"article_id": article_id, "error": str(exc)})

        precompute_result = {"material_count": 0, "updated_count": 0, "skipped_count": 0, "families": {}}
        all_imported_article_ids = sorted(set(ingested_article_ids))
        if not args.dry_run and all_imported_article_ids:
            precompute_result = precompute_in_chunks(v2_index_service, all_imported_article_ids, chunk_size=300)

        imported_materials = []
        review_status_counter: Counter[str] = Counter()
        if all_imported_article_ids:
            imported_materials = list(
                session.scalars(
                    select(MaterialSpanORM).where(
                        MaterialSpanORM.article_id.in_(all_imported_article_ids),
                    )
                )
            )
            rows = session.execute(
                select(TaggingReviewORM.status, MaterialSpanORM.id)
                .join(MaterialSpanORM, MaterialSpanORM.id == TaggingReviewORM.material_id)
                .where(MaterialSpanORM.article_id.in_(all_imported_article_ids))
            ).all()
            for status, _ in rows:
                review_status_counter[str(status)] += 1

        family_counter: Counter[str] = Counter()
        status_counter: Counter[str] = Counter()
        release_counter: Counter[str] = Counter()
        quality_values: list[float] = []
        for material in imported_materials:
            status_counter[str(material.status)] += 1
            release_counter[str(material.release_channel)] += 1
            quality_values.append(float(getattr(material, "quality_score", 0.0) or 0.0))
            for family in material.v2_business_family_ids or []:
                family_counter[str(family)] += 1

        quality_summary = summarize_quality(quality_values)
        consumption_probe = {}
        if all_imported_article_ids and not args.dry_run:
            for family in args.business_families:
                response = v2_service.search(
                    {
                        "business_family_id": family,
                        "article_ids": all_imported_article_ids,
                        "candidate_limit": 20,
                        "review_gate_mode": "stable_relaxed",
                    }
                )
                items = response.get("items") or []
                consumption_probe[family] = {
                    "item_count": len(items),
                    "article_count": response.get("article_count"),
                    "cache_hit": bool(response.get("cache_hit")),
                    "review_gate": response.get("review_gate"),
                    "avg_quality_in_items": round(
                        sum(float(item.get("quality_score") or 0.0) for item in items) / max(1, len(items)),
                        4,
                    ),
                    "warnings": response.get("warnings") or [],
                }

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "workspace_root": str(ROOT),
            "missing_files": missing_files,
            "parse_errors": parse_errors,
            "dry_run": bool(args.dry_run),
            "config": {
                "input_files": [str(path) for path in files],
                "business_families": args.business_families,
                "min_text_length": args.min_text_length,
                "max_items_per_file": args.max_items_per_file,
                "truncate_chars": args.truncate_chars,
                "process_existing": bool(args.process_existing),
            },
            "parse_summary": {
                "accepted_for_ingest": len(parsed),
                "raw_count_per_file": dict(per_file_raw_count),
                "accepted_count_per_file": dict(per_file_accepted_count),
            },
            "ingest_summary": {
                "attempted": len(parsed),
                "ingested_rows": len(ingested_article_ids),
                "unique_article_ids": len(set(ingested_article_ids)),
                "new_article_count": len(new_article_ids),
                "dedup_existing_count": max(0, len(set(ingested_article_ids)) - len(new_article_ids)),
                "failures": ingest_failures[:50],
            },
            "process_summary": {
                "target_count": len(target_process_ids),
                "processed_count": len(processed_article_ids),
                "failures": process_failures[:50],
            },
            "precompute_summary": precompute_result,
            "material_summary": {
                "article_count": len(all_imported_article_ids),
                "material_count": len(imported_materials),
                "status_counts": dict(status_counter),
                "release_channel_counts": dict(release_counter),
                "review_status_counts": dict(review_status_counter),
                "v2_family_counts": dict(family_counter),
                "quality": quality_summary,
            },
            "consumption_probe": consumption_probe,
            "assessment": build_assessment(quality_summary, consumption_probe, len(imported_materials)),
        }

        report_path = write_report(report, args.report_path)
        print(json.dumps({"report_path": str(report_path), "assessment": report["assessment"]}, ensure_ascii=False, indent=2))
        return 0 if not missing_files else 2
    finally:
        session.close()


def parse_file(file_path: Path) -> list[ParsedArticle]:
    if file_path.suffix.lower() == ".jsonl":
        records = []
        for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            _ = line_no
    else:
        payload = json.loads(file_path.read_text(encoding="utf-8", errors="ignore"))
        records = unfold_payload(payload)

    parsed: list[ParsedArticle] = []
    for idx, node in enumerate(records, start=1):
        parsed_item = parse_node_to_article(node=node, source_file=file_path.name, index=idx)
        if parsed_item is None:
            continue
        parsed.append(parsed_item)
    return parsed


def discover_default_input_files() -> list[Path]:
    roots = [
        PASSAGE_SERVICE_ROOT,
        ROOT,
        ROOT / "tmp_truth_docs",
    ]
    found: list[Path] = []
    seen: set[str] = set()
    for base in roots:
        if not base.exists():
            continue
        for pattern in ("point_*.json", "point_*.jsonl"):
            for path in sorted(base.glob(pattern)):
                resolved = path.resolve()
                key = str(resolved).lower()
                if key in seen:
                    continue
                seen.add(key)
                found.append(resolved)
    return found


def resolve_input_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    candidates = [
        (PASSAGE_SERVICE_ROOT / path),
        (ROOT / path),
        path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return (PASSAGE_SERVICE_ROOT / path).resolve()


def unfold_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("materials", "articles", "items", "data", "records", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    return []


def parse_node_to_article(*, node: Any, source_file: str, index: int) -> ParsedArticle | None:
    if isinstance(node, str):
        text = node.strip()
        if not text:
            return None
        title = f"{Path(source_file).stem}#{index}"
        source_url = build_fallback_url(source_file=source_file, index=index, text=text)
        return ParsedArticle(
            source_file=source_file,
            title=title,
            source="coze_manual",
            source_url=source_url,
            domain="coze",
            raw_text=text,
            meta={"node_type": "str"},
        )
    if not isinstance(node, dict):
        return None

    article = node.get("article") if isinstance(node.get("article"), dict) else node

    segments = article.get("segments")
    text_parts: list[str] = []
    if isinstance(segments, list) and segments:
        ordered_segments = sorted(
            [seg for seg in segments if isinstance(seg, dict)],
            key=lambda seg: int(seg.get("paragraph_index") or 0),
        )
        for seg in ordered_segments:
            content = str(seg.get("content") or "").strip()
            if content:
                text_parts.append(content)
    if not text_parts:
        for key in ("raw_text", "clean_text", "content", "body", "text", "passage", "stem"):
            value = article.get(key)
            if isinstance(value, str) and value.strip():
                text_parts = [value.strip()]
                break
    if not text_parts:
        return None

    raw_text = "\n\n".join(text_parts).strip()
    if not raw_text:
        return None

    title = str(article.get("title") or article.get("name") or f"{Path(source_file).stem}#{index}").strip()
    source = str(
        article.get("source")
        or article.get("source_site")
        or article.get("author")
        or "coze_manual"
    ).strip()
    domain = str(article.get("domain") or article.get("source_site") or "coze").strip()
    source_url = str(article.get("source_url") or article.get("url") or "").strip()
    if not source_url:
        source_url = build_fallback_url(source_file=source_file, index=index, text=raw_text)

    return ParsedArticle(
        source_file=source_file,
        title=title,
        source=source or "coze_manual",
        source_url=source_url,
        domain=domain or "coze",
        raw_text=raw_text,
        meta={key: article.get(key) for key in ("publish_date", "category", "crawl_time")},
    )


def build_fallback_url(*, source_file: str, index: int, text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"coze://manual/{Path(source_file).stem}/{index}/{digest}"


def precompute_in_chunks(v2_index_service: MaterialV2IndexService, article_ids: list[str], chunk_size: int = 300) -> dict[str, Any]:
    aggregate = {
        "index_version": None,
        "material_count": 0,
        "updated_count": 0,
        "skipped_count": 0,
        "families": Counter(),
    }
    for start in range(0, len(article_ids), chunk_size):
        chunk = article_ids[start : start + chunk_size]
        result = v2_index_service.precompute(
            {
                "article_ids": chunk,
                "primary_only": False,
                "status": None,
                "release_channel": None,
            }
        )
        aggregate["index_version"] = result.get("index_version")
        aggregate["material_count"] += int(result.get("material_count") or 0)
        aggregate["updated_count"] += int(result.get("updated_count") or 0)
        aggregate["skipped_count"] += int(result.get("skipped_count") or 0)
        for family, count in (result.get("families") or {}).items():
            aggregate["families"][str(family)] += int(count or 0)
    aggregate["families"] = dict(aggregate["families"])
    return aggregate


def summarize_quality(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "avg": 0.0,
            "median": 0.0,
            "p10": 0.0,
            "p90": 0.0,
            "low_ratio_lt_045": 0.0,
            "high_ratio_gte_065": 0.0,
        }
    sorted_values = sorted(values)
    return {
        "avg": round(sum(sorted_values) / len(sorted_values), 4),
        "median": round(statistics.median(sorted_values), 4),
        "p10": round(percentile(sorted_values, 10), 4),
        "p90": round(percentile(sorted_values, 90), 4),
        "low_ratio_lt_045": round(sum(1 for value in sorted_values if value < 0.45) / len(sorted_values), 4),
        "high_ratio_gte_065": round(sum(1 for value in sorted_values if value >= 0.65) / len(sorted_values), 4),
    }


def percentile(sorted_values: list[float], rank: int) -> float:
    if not sorted_values:
        return 0.0
    clipped_rank = max(0, min(100, int(rank)))
    index = int(round((clipped_rank / 100) * (len(sorted_values) - 1)))
    return float(sorted_values[index])


def build_assessment(quality_summary: dict[str, Any], probe: dict[str, Any], material_count: int) -> dict[str, Any]:
    risk_flags: list[str] = []
    if material_count == 0:
        risk_flags.append("no_material_generated")
    if float(quality_summary.get("low_ratio_lt_045") or 0.0) >= 0.40:
        risk_flags.append("high_low_quality_ratio")
    if probe:
        empty_families = [family for family, result in probe.items() if int(result.get("item_count") or 0) == 0]
        if empty_families:
            risk_flags.append(f"empty_consumption_probe:{','.join(empty_families)}")

    verdict = "good"
    if "no_material_generated" in risk_flags:
        verdict = "blocked"
    elif risk_flags:
        verdict = "needs_improvement"
    return {"verdict": verdict, "risk_flags": risk_flags}


def write_report(report: dict[str, Any], custom_report_path: str) -> Path:
    if custom_report_path:
        report_path = Path(custom_report_path).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORTS_ROOT / f"coze_manual_import_audit_{timestamp}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


if __name__ == "__main__":
    raise SystemExit(main())
