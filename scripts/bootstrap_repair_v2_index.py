from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports" / "bootstrap_index"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.material_v2_index_service import MaterialV2IndexService  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap v2 index with depth2 repair fallback for existing material spans.")
    parser.add_argument("--limit", type=int, default=0, help="0 means all eligible materials after filtering.")
    parser.add_argument("--chunk-size", type=int, default=50, help="Chunk size for bootstrap processing.")
    parser.add_argument("--audit-every", type=int, default=5, help="Run a lightweight audit snapshot every N chunks.")
    parser.add_argument("--audit-sample-size", type=int, default=2, help="How many samples per outcome to keep in each audit snapshot.")
    parser.add_argument("--status", type=str, default="", help="Optional material status filter.")
    parser.add_argument("--release-channel", type=str, default="", help="Optional release channel filter.")
    parser.add_argument("--only-missing-index", action="store_true", default=True)
    parser.add_argument("--all-primary", action="store_true", help="Process all primary materials, not only missing-index ones.")
    parser.add_argument("--include-secondary", action="store_true", help="Include non-primary materials in the bootstrap sweep.")
    parser.add_argument("--use-mechanical-depth3", action="store_true", help="Use mechanical-only depth3 for bootstrap. Default keeps full current LLM judges.")
    return parser.parse_args()


def build_markdown(report: dict) -> str:
    lines = [
        "# Bootstrap Repair V2 Index Report",
        "",
        f"- run_at: `{report['run_at']}`",
        f"- index_version: `{report['index_version']}`",
        f"- material_count: `{report['material_count']}`",
        f"- updated_count: `{report['updated_count']}`",
        f"- skipped_count: `{report['skipped_count']}`",
        f"- direct_pass_count: `{report['direct_pass_count']}`",
        f"- repaired_pass_count: `{report['repaired_pass_count']}`",
        f"- discarded_family_count: `{report['discarded_family_count']}`",
        f"- chunks: `{report.get('chunk_count', 0)}`",
        "",
        "## Families",
        "",
    ]
    families = report.get("families") or {}
    if not families:
        lines.append("- none")
    else:
        for family, count in families.items():
            lines.append(f"- `{family}`: `{count}`")
    lines.extend(["", "## Discard Reasons", ""])
    discard_reasons = report.get("discard_reasons") or {}
    if not discard_reasons:
        lines.append("- none")
    else:
        for reason, count in discard_reasons.items():
            lines.append(f"- `{reason}`: `{count}`")
    audits = report.get("audit_snapshots") or []
    lines.extend(["", "## Audit Snapshots", ""])
    if not audits:
        lines.append("- none")
    else:
        for audit in audits:
            lines.append(
                f"- chunk `{audit['chunk_index']}` / processed `{audit['processed_materials']}`:"
                f" direct=`{audit['counts'].get('direct_pass', 0)}`"
                f" repaired=`{audit['counts'].get('repaired_pass', 0)}`"
                f" discarded=`{audit['counts'].get('discarded', 0)}`"
            )
            for sample in audit.get("samples", [])[:9]:
                lines.append(
                    f"  - `{sample['material_id']}` `{sample['outcome']}`"
                    f" family=`{sample['primary_family']}`"
                    f" routed=`{','.join(sample.get('routed_families') or []) or '-'}'"
                    f" reject=`{sample.get('reject_reason') or '-'}`"
                )
                lines.append(f"    preview: {sample.get('text_preview') or '-'}")
    return "\n".join(lines)


def _fetch_target_material_ids(session, *, status: str | None, release_channel: str | None, primary_only: bool, only_missing_index: bool, limit: int | None) -> list[str]:
    where = []
    params: dict[str, Any] = {}
    if status:
        where.append("status = :status")
        params["status"] = status
    if release_channel:
        where.append("release_channel = :release_channel")
        params["release_channel"] = release_channel
    if primary_only:
        where.append("is_primary = 1")
    if only_missing_index:
        where.append("(v2_index_version is null or v2_index_payload is null or v2_index_payload = '{}' or v2_index_payload = '')")
    sql = "select id from material_spans"
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by updated_at asc, id asc"
    if limit:
        sql += " limit :limit"
        params["limit"] = int(limit)
    rows = session.execute(text(sql), params).all()
    return [str(row[0]) for row in rows]


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx: idx + size] for idx in range(0, len(items), size)]


def _truncate(text_value: str, limit: int = 140) -> str:
    text_value = " ".join((text_value or "").split())
    if len(text_value) <= limit:
        return text_value
    return text_value[:limit].rstrip() + "..."


def _sample_outcomes(session, *, material_ids: list[str], sample_size: int) -> dict:
    rows = (
        session.query(MaterialSpanORM)
        .filter(MaterialSpanORM.id.in_(material_ids))
        .all()
    )
    by_outcome: dict[str, list[dict]] = {
        "direct_pass": [],
        "repaired_pass": [],
        "discarded": [],
    }
    for row in rows:
        trace = dict(row.decision_trace or {})
        bootstrap = dict(trace.get("bootstrap_v2_index") or {})
        families = dict(bootstrap.get("families") or {})
        routed = sorted(families.keys())
        any_repaired = any(str((fam_trace or {}).get("outcome") or "") == "depth2_repaired_pass" for fam_trace in families.values())
        is_discarded = bool(row.reject_reason and str(row.reject_reason).startswith("bootstrap_depth2_discarded|"))
        if is_discarded:
            outcome = "discarded"
        elif any_repaired:
            outcome = "repaired_pass"
        else:
            outcome = "direct_pass"
        sample = {
            "material_id": row.id,
            "outcome": outcome,
            "primary_family": row.primary_family or "",
            "routed_families": routed,
            "reject_reason": row.reject_reason or "",
            "quality_flags": list(row.quality_flags or []),
            "text_preview": _truncate(row.text or ""),
        }
        bucket = by_outcome.setdefault(outcome, [])
        if len(bucket) < sample_size:
            bucket.append(sample)
    counts = {key: len(value) for key, value in by_outcome.items()}
    flat_samples = by_outcome["direct_pass"] + by_outcome["repaired_pass"] + by_outcome["discarded"]
    return {
        "counts": counts,
        "samples": flat_samples,
    }


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    session = get_session()
    try:
        service = MaterialV2IndexService(session)
        primary_only = not bool(args.include_secondary)
        only_missing_index = False if args.all_primary else bool(args.only_missing_index)
        target_ids = _fetch_target_material_ids(
            session,
            status=args.status or None,
            release_channel=args.release_channel or None,
            primary_only=primary_only,
            only_missing_index=only_missing_index,
            limit=args.limit or None,
        )
        chunk_size = max(1, int(args.chunk_size or 50))
        chunks = _chunked(target_ids, chunk_size)
        aggregate = {
            "index_version": service.pipeline.INDEX_VERSION,
            "material_count": len(target_ids),
            "updated_count": 0,
            "skipped_count": 0,
            "direct_pass_count": 0,
            "repaired_pass_count": 0,
            "discarded_family_count": 0,
            "families": {},
            "discard_reasons": {},
            "chunk_count": len(chunks),
            "audit_snapshots": [],
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        latest_json_path = REPORTS_ROOT / "bootstrap_repair_v2_index_latest.json"
        latest_md_path = REPORTS_ROOT / "bootstrap_repair_v2_index_latest.md"

        def _write_checkpoint(report_payload: dict) -> None:
            latest_json_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            latest_md_path.write_text(build_markdown(report_payload), encoding="utf-8")

        for chunk_index, material_ids in enumerate(chunks, start=1):
            payload = {
                "material_ids": material_ids,
                "primary_only": primary_only,
                "only_missing_index": only_missing_index,
                "use_llm_family_landing": False,
                "use_mechanical_depth3": bool(args.use_mechanical_depth3),
            }
            result = service.bootstrap_precompute(payload)
            aggregate["updated_count"] += int(result.get("updated_count") or 0)
            aggregate["skipped_count"] += int(result.get("skipped_count") or 0)
            aggregate["direct_pass_count"] += int(result.get("direct_pass_count") or 0)
            aggregate["repaired_pass_count"] += int(result.get("repaired_pass_count") or 0)
            aggregate["discarded_family_count"] += int(result.get("discarded_family_count") or 0)
            for family, count in dict(result.get("families") or {}).items():
                aggregate["families"][family] = int(aggregate["families"].get(family, 0)) + int(count)
            for reason, count in dict(result.get("discard_reasons") or {}).items():
                aggregate["discard_reasons"][reason] = int(aggregate["discard_reasons"].get(reason, 0)) + int(count)
            print(
                f"[bootstrap] chunk={chunk_index}/{len(chunks)} materials={len(material_ids)}"
                f" updated={result.get('updated_count', 0)}"
                f" direct={result.get('direct_pass_count', 0)}"
                f" repaired={result.get('repaired_pass_count', 0)}"
            )
            if args.audit_every > 0 and (chunk_index % args.audit_every == 0 or chunk_index == len(chunks)):
                audit = _sample_outcomes(
                    session,
                    material_ids=material_ids,
                    sample_size=max(1, int(args.audit_sample_size or 2)),
                )
                aggregate["audit_snapshots"].append(
                    {
                        "chunk_index": chunk_index,
                        "processed_materials": min(chunk_index * chunk_size, len(target_ids)),
                        "counts": audit.get("counts") or {},
                        "samples": audit.get("samples") or [],
                    }
                )
            checkpoint_report = {
                "run_at": datetime.now().isoformat(timespec="seconds"),
                **aggregate,
                "args": {
                    "limit": args.limit,
                    "chunk_size": chunk_size,
                    "audit_every": args.audit_every,
                    "audit_sample_size": args.audit_sample_size,
                    "status": args.status,
                    "release_channel": args.release_channel,
                    "only_missing_index": only_missing_index,
                    "primary_only": primary_only,
                    "use_mechanical_depth3": bool(args.use_mechanical_depth3),
                },
                "progress": {
                    "current_chunk": chunk_index,
                    "total_chunks": len(chunks),
                    "processed_materials": min(chunk_index * chunk_size, len(target_ids)),
                    "remaining_materials": max(0, len(target_ids) - chunk_index * chunk_size),
                },
            }
            _write_checkpoint(checkpoint_report)
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            **aggregate,
            "args": {
                "limit": args.limit,
                "chunk_size": chunk_size,
                "audit_every": args.audit_every,
                "audit_sample_size": args.audit_sample_size,
                "status": args.status,
                "release_channel": args.release_channel,
                "only_missing_index": only_missing_index,
                "primary_only": primary_only,
                "use_mechanical_depth3": bool(args.use_mechanical_depth3),
            },
        }
        json_path = REPORTS_ROOT / f"bootstrap_repair_v2_index_{ts}.json"
        md_path = REPORTS_ROOT / f"bootstrap_repair_v2_index_{ts}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        _write_checkpoint(report)
        print(f"[bootstrap] report_json={json_path}")
        print(f"[bootstrap] report_md={md_path}")
        print(f"[bootstrap] updated={report['updated_count']} direct={report['direct_pass_count']} repaired={report['repaired_pass_count']}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
