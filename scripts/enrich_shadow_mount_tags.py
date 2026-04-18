from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports" / "bootstrap_index"
HIERARCHY_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_family_hierarchy_mapping.yaml"
MATERIAL_MAPPING_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_material_card_id_mapping.yaml"
SHADOW_VERSION = "shadow_mount.v1"
TARGET_MOTHER_FAMILIES = {
    "center_understanding",
    "sentence_fill",
    "sentence_order",
}

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402


@dataclass(frozen=True)
class ChildFamilyRule:
    child_family_id: str
    mother_family_id: str
    material_card_ids: tuple[str, ...]
    truth_blank_position: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich existing v2 index payloads with isolated shadow mount tags.")
    parser.add_argument("--limit", type=int, default=0, help="0 means all eligible indexed materials.")
    parser.add_argument("--chunk-size", type=int, default=120, help="Chunk size for db writes.")
    parser.add_argument("--audit-every", type=int, default=5, help="Audit snapshot cadence in chunks.")
    parser.add_argument("--audit-sample-size", type=int, default=2, help="How many samples to keep per outcome in audit snapshots.")
    parser.add_argument("--status", type=str, default="", help="Optional material status filter.")
    parser.add_argument("--release-channel", type=str, default="", help="Optional release channel filter.")
    parser.add_argument("--include-secondary", action="store_true", help="Include non-primary materials.")
    parser.add_argument("--only-missing-shadow", action="store_true", default=True, help="Skip family payloads that already contain this shadow version.")
    parser.add_argument("--include-title-selection", action="store_true", help="Also inspect title_selection payloads for center shadow carryover. Default keeps only mapped mother families.")
    parser.add_argument("--write", action="store_true", help="Actually persist shadow tags to sqlite. Default is dry-run.")
    return parser.parse_args()


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


class ShadowMountMapper:
    def __init__(self) -> None:
        hierarchy = _load_yaml(HIERARCHY_PATH)
        _ = _load_yaml(MATERIAL_MAPPING_PATH)
        child_families = dict(hierarchy.get("child_families") or {})
        self.child_rules: dict[str, ChildFamilyRule] = {}
        self.material_to_children: dict[tuple[str, str], list[str]] = defaultdict(list)
        for child_family_id, cfg in child_families.items():
            rule = ChildFamilyRule(
                child_family_id=child_family_id,
                mother_family_id=str(cfg.get("mother_family_id") or ""),
                material_card_ids=tuple(str(x) for x in (cfg.get("material_card_ids") or [])),
                truth_blank_position=str(cfg.get("truth_blank_position") or "") or None,
            )
            self.child_rules[child_family_id] = rule
            for material_card_id in rule.material_card_ids:
                self.material_to_children[(rule.mother_family_id, material_card_id)].append(child_family_id)

    def build_shadow_mount(self, *, family: str, item: dict[str, Any]) -> dict[str, Any]:
        qrc = dict(item.get("question_ready_context") or {})
        resolved_slots = dict(qrc.get("resolved_slots") or {})
        selected_material_card = str(qrc.get("selected_material_card") or item.get("material_card_id") or "")
        selected_business_card = str(qrc.get("selected_business_card") or item.get("selected_business_card") or "")
        pattern_candidates = [str(x) for x in (qrc.get("pattern_candidates") or item.get("pattern_candidates") or []) if x]

        shadow: dict[str, Any] = {
            "version": SHADOW_VERSION,
            "mount_source": "runtime_depth3_payload",
            "mother_family_id": family,
            "runtime_selected_material_card": selected_material_card or None,
            "runtime_selected_business_card": selected_business_card or None,
            "signals": {
                "blank_position": resolved_slots.get("blank_position"),
                "function_type": resolved_slots.get("function_type"),
                "pattern_candidates": pattern_candidates,
            },
        }

        expected_material_card_id: str | None = None
        child_candidates: list[str] = list(self.material_to_children.get((family, selected_material_card), []))

        if family == "sentence_fill":
            expected_material_card_id = self._infer_fill_material_card(
                selected_material_card=selected_material_card,
                resolved_slots=resolved_slots,
            )
            child_candidates = self._infer_fill_child_candidates(
                expected_material_card_id=expected_material_card_id,
                resolved_slots=resolved_slots,
            )
        elif family == "sentence_order":
            expected_material_card_id = self._infer_order_material_card(
                selected_material_card=selected_material_card,
                selected_business_card=selected_business_card,
                pattern_candidates=pattern_candidates,
            )
            child_candidates = self._infer_order_child_candidates(
                expected_material_card_id=expected_material_card_id,
                selected_business_card=selected_business_card,
            )
        elif family == "center_understanding":
            expected_material_card_id = self._infer_center_material_card(selected_material_card=selected_material_card)
            if expected_material_card_id:
                child_candidates = list(self.material_to_children.get((family, expected_material_card_id), []))

        status = "unmapped"
        if len(child_candidates) == 1:
            status = "mapped_unique"
            shadow["child_family_id"] = child_candidates[0]
        elif len(child_candidates) > 1:
            status = "ambiguous_child_family"

        shadow["status"] = status
        shadow["child_family_candidates"] = child_candidates
        shadow["expected_material_card_id"] = expected_material_card_id
        shadow["notes"] = self._build_notes(
            family=family,
            status=status,
            selected_material_card=selected_material_card,
            selected_business_card=selected_business_card,
            expected_material_card_id=expected_material_card_id,
        )
        return shadow

    def _infer_center_material_card(self, *, selected_material_card: str) -> str | None:
        if selected_material_card.startswith("center_material."):
            return selected_material_card
        return None

    def _infer_fill_material_card(self, *, selected_material_card: str, resolved_slots: dict[str, Any]) -> str | None:
        if selected_material_card.startswith("fill_material."):
            return selected_material_card
        blank_position = str(resolved_slots.get("blank_position") or "")
        function_type = str(resolved_slots.get("function_type") or "")
        if blank_position == "opening":
            if function_type in {"opening_summary", "summary"}:
                return "fill_material.opening_summary"
            if function_type in {"topic_intro"}:
                return "fill_material.opening_topic_intro"
        if blank_position == "middle":
            if function_type in {"bridge"}:
                return "fill_material.bridge_transition"
            if function_type in {"middle_explanation", "carry_previous", "explanation"}:
                return "fill_material.middle_explanation"
            if function_type in {"middle_focus_shift", "lead_next"}:
                return "fill_material.middle_focus_shift"
        if blank_position == "ending":
            if function_type in {"ending_summary", "summary", "conclusion"}:
                return "fill_material.ending_summary"
            if function_type in {"countermeasure"}:
                return "fill_material.ending_countermeasure"
        return None

    def _infer_fill_child_candidates(self, *, expected_material_card_id: str | None, resolved_slots: dict[str, Any]) -> list[str]:
        blank_position = str(resolved_slots.get("blank_position") or "")
        if blank_position == "opening":
            return ["sentence_fill_head_start"]
        if blank_position == "middle":
            return ["sentence_fill_middle"]
        if blank_position == "ending":
            return ["sentence_fill_tail_end"]
        if expected_material_card_id:
            return list(self.material_to_children.get(("sentence_fill", expected_material_card_id), []))
        return []

    def _infer_order_material_card(
        self,
        *,
        selected_material_card: str,
        selected_business_card: str,
        pattern_candidates: list[str],
    ) -> str | None:
        if selected_material_card.startswith("order_material."):
            return selected_material_card
        if "viewpoint_reason_action" in pattern_candidates:
            return "order_material.viewpoint_reason_action"
        if "problem_solution_case_blocks" in pattern_candidates:
            return "order_material.problem_solution_case_blocks"
        if "timeline_progression" in pattern_candidates:
            return "order_material.timeline_progression"
        if "carry_parallel_expand" in pattern_candidates and selected_business_card == "sentence_order__deterministic_binding__abstract":
            return "order_material.carry_parallel_expand"
        if "dual_anchor_lock" in pattern_candidates and selected_business_card in {
            "sentence_order__deterministic_binding__abstract",
            "sentence_order__discourse_logic__abstract",
        }:
            return "order_material.dual_anchor_lock"
        return None

    def _infer_order_child_candidates(self, *, expected_material_card_id: str | None, selected_business_card: str) -> list[str]:
        if expected_material_card_id:
            candidates = list(self.material_to_children.get(("sentence_order", expected_material_card_id), []))
            if candidates:
                return candidates
        if selected_business_card == "sentence_order__deterministic_binding__abstract":
            return ["sentence_order_fixed_bundle"]
        if selected_business_card in {
            "sentence_order__timeline_action_sequence__abstract",
            "sentence_order__discourse_logic__abstract",
        }:
            return ["sentence_order_sequence"]
        if selected_business_card in {
            "sentence_order__head_tail_lock__abstract",
            "sentence_order__head_tail_logic__abstract",
        }:
            return ["sentence_order_first_sentence", "sentence_order_tail_sentence"]
        return []

    def _build_notes(
        self,
        *,
        family: str,
        status: str,
        selected_material_card: str,
        selected_business_card: str,
        expected_material_card_id: str | None,
    ) -> list[str]:
        notes: list[str] = []
        if expected_material_card_id and expected_material_card_id != selected_material_card:
            notes.append("expected_material_card_inferred_from_runtime_signals")
        if family == "center_understanding" and status == "ambiguous_child_family":
            notes.append("center_child_mapping_is_ambiguous_under_current_runtime_material_card")
        if family == "sentence_order" and status == "ambiguous_child_family":
            notes.append("order_head_tail_family_not_forced_without stronger leaf evidence")
        if family == "sentence_fill" and not expected_material_card_id:
            notes.append("fill_child_kept_by_blank_position_only")
        if not expected_material_card_id:
            notes.append("expected_material_card_unresolved")
        if not selected_business_card:
            notes.append("runtime_selected_business_card_missing")
        return notes


def _fetch_target_material_ids(
    session,
    *,
    status: str | None,
    release_channel: str | None,
    primary_only: bool,
    limit: int | None,
) -> list[str]:
    where = [
        "v2_index_version is not null",
        "v2_index_payload is not null",
        "v2_index_payload != '{}'",
        "v2_index_payload != ''",
        "(" + " or ".join(
            f"instr(coalesce(v2_business_family_ids, ''), '{family}') > 0"
            for family in sorted(TARGET_MOTHER_FAMILIES)
        ) + ")",
    ]
    params: dict[str, Any] = {}
    if status:
        where.append("status = :status")
        params["status"] = status
    if release_channel:
        where.append("release_channel = :release_channel")
        params["release_channel"] = release_channel
    if primary_only:
        where.append("is_primary = 1")
    sql = "select id from material_spans where " + " and ".join(where) + " order by updated_at asc, id asc"
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


def _has_current_shadow(item: dict[str, Any]) -> bool:
    shadow = dict(item.get("shadow_mount") or {})
    return str(shadow.get("version") or "") == SHADOW_VERSION


def _build_report_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Shadow Mount Enrichment Report",
        "",
        f"- run_at: `{report['run_at']}`",
        f"- dry_run: `{report['dry_run']}`",
        f"- scanned_material_count: `{report['scanned_material_count']}`",
        f"- updated_material_count: `{report['updated_material_count']}`",
        f"- skipped_material_count: `{report['skipped_material_count']}`",
        f"- target_family_payload_count: `{report['target_family_payload_count']}`",
        "",
        "## Family Status Counts",
        "",
    ]
    status_counts = report.get("family_status_counts") or {}
    if not status_counts:
        lines.append("- none")
    else:
        for key, count in sorted(status_counts.items()):
            lines.append(f"- `{key}`: `{count}`")
    lines.extend(["", "## Audit Snapshots", ""])
    audits = report.get("audit_snapshots") or []
    if not audits:
        lines.append("- none")
    else:
        for audit in audits:
            lines.append(
                f"- chunk `{audit['chunk_index']}` / processed `{audit['processed_materials']}`:"
                f" unique=`{audit['counts'].get('mapped_unique', 0)}`"
                f" ambiguous=`{audit['counts'].get('ambiguous_child_family', 0)}`"
                f" unmapped=`{audit['counts'].get('unmapped', 0)}`"
            )
            for sample in audit.get("samples") or []:
                lines.append(
                    f"  - `{sample['material_id']}` family=`{sample['family']}`"
                    f" status=`{sample['status']}`"
                    f" child=`{sample.get('child_family_id') or '-'}`"
                    f" expected_card=`{sample.get('expected_material_card_id') or '-'}`"
                )
                lines.append(f"    preview: {sample.get('text_preview') or '-'}")
    return "\n".join(lines)


def _sample_shadow_outcomes(rows: list[MaterialSpanORM], *, sample_size: int) -> dict[str, Any]:
    samples_by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts: Counter[str] = Counter()
    for row in rows:
        payload = dict(row.v2_index_payload or {})
        for family in TARGET_MOTHER_FAMILIES:
            item = dict(payload.get(family) or {})
            shadow = dict(item.get("shadow_mount") or {})
            if not shadow:
                continue
            status = str(shadow.get("status") or "unmapped")
            counts[status] += 1
            if len(samples_by_status[status]) >= sample_size:
                continue
            samples_by_status[status].append(
                {
                    "material_id": row.id,
                    "family": family,
                    "status": status,
                    "child_family_id": shadow.get("child_family_id"),
                    "expected_material_card_id": shadow.get("expected_material_card_id"),
                    "text_preview": _truncate(row.text or ""),
                }
            )
    flat_samples: list[dict[str, Any]] = []
    for status in ["mapped_unique", "ambiguous_child_family", "unmapped"]:
        flat_samples.extend(samples_by_status.get(status) or [])
    return {
        "counts": dict(counts),
        "samples": flat_samples,
    }


def main() -> int:
    args = parse_args()
    init_db()
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    mapper = ShadowMountMapper()
    session = get_session()
    try:
        target_ids = _fetch_target_material_ids(
            session,
            status=args.status or None,
            release_channel=args.release_channel or None,
            primary_only=not bool(args.include_secondary),
            limit=args.limit or None,
        )
        chunks = _chunked(target_ids, max(1, int(args.chunk_size or 120)))
        aggregate: dict[str, Any] = {
            "run_at": "",
            "dry_run": not bool(args.write),
            "scanned_material_count": len(target_ids),
            "updated_material_count": 0,
            "skipped_material_count": 0,
            "target_family_payload_count": 0,
            "family_status_counts": Counter(),
            "audit_snapshots": [],
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        latest_json_path = REPORTS_ROOT / "shadow_mount_enrichment_latest.json"
        latest_md_path = REPORTS_ROOT / "shadow_mount_enrichment_latest.md"

        def _write_checkpoint(report_payload: dict[str, Any]) -> None:
            latest_json_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            latest_md_path.write_text(_build_report_markdown(report_payload), encoding="utf-8")

        for chunk_index, material_ids in enumerate(chunks, start=1):
            rows = (
                session.query(MaterialSpanORM)
                .filter(MaterialSpanORM.id.in_(material_ids))
                .all()
            )
            changed_rows = 0
            chunk_target_payloads = 0
            for row in rows:
                payload = dict(row.v2_index_payload or {})
                decision_trace = dict(row.decision_trace or {})
                shadow_trace = dict(decision_trace.get("shadow_mount_v1") or {})
                shadow_trace_families = dict(shadow_trace.get("families") or {})
                row_changed = False
                row_had_target = False
                for family, item in list(payload.items()):
                    if family not in TARGET_MOTHER_FAMILIES:
                        continue
                    row_had_target = True
                    row_item = dict(item or {})
                    if args.only_missing_shadow and _has_current_shadow(row_item):
                        continue
                    shadow = mapper.build_shadow_mount(family=family, item=row_item)
                    row_item["shadow_mount"] = shadow
                    payload[family] = row_item
                    shadow_trace_families[family] = {
                        "status": shadow.get("status"),
                        "child_family_id": shadow.get("child_family_id"),
                        "child_family_candidates": shadow.get("child_family_candidates") or [],
                        "expected_material_card_id": shadow.get("expected_material_card_id"),
                    }
                    aggregate["family_status_counts"][f"{family}:{shadow.get('status')}"] += 1
                    chunk_target_payloads += 1
                    row_changed = True
                if not row_had_target:
                    aggregate["skipped_material_count"] += 1
                    continue
                if not row_changed:
                    continue
                shadow_trace["version"] = SHADOW_VERSION
                shadow_trace["updated_at"] = datetime.now().isoformat(timespec="seconds")
                shadow_trace["families"] = shadow_trace_families
                decision_trace["shadow_mount_v1"] = shadow_trace
                row.v2_index_payload = payload
                row.decision_trace = decision_trace
                changed_rows += 1
            aggregate["target_family_payload_count"] += chunk_target_payloads
            if args.audit_every > 0 and (chunk_index % args.audit_every == 0 or chunk_index == len(chunks)):
                sample_payload = _sample_shadow_outcomes(rows, sample_size=max(1, int(args.audit_sample_size or 2)))
                aggregate["audit_snapshots"].append(
                    {
                        "chunk_index": chunk_index,
                        "processed_materials": min(chunk_index * max(1, int(args.chunk_size or 120)), len(target_ids)),
                        "counts": sample_payload.get("counts") or {},
                        "samples": sample_payload.get("samples") or [],
                    }
                )
            if args.write and changed_rows:
                session.commit()
                aggregate["updated_material_count"] += changed_rows
            else:
                session.rollback()
            print(
                f"[shadow_mount] chunk={chunk_index}/{len(chunks)} materials={len(rows)}"
                f" changed={changed_rows} target_payloads={chunk_target_payloads}"
                f" dry_run={not bool(args.write)}"
            )
            checkpoint_report = {
                "run_at": datetime.now().isoformat(timespec="seconds"),
                "dry_run": not bool(args.write),
                "scanned_material_count": aggregate["scanned_material_count"],
                "updated_material_count": aggregate["updated_material_count"],
                "skipped_material_count": aggregate["skipped_material_count"],
                "target_family_payload_count": aggregate["target_family_payload_count"],
                "family_status_counts": dict(aggregate["family_status_counts"]),
                "audit_snapshots": aggregate["audit_snapshots"],
                "args": {
                    "limit": args.limit,
                    "chunk_size": args.chunk_size,
                    "audit_every": args.audit_every,
                    "audit_sample_size": args.audit_sample_size,
                    "status": args.status,
                    "release_channel": args.release_channel,
                    "include_secondary": bool(args.include_secondary),
                    "only_missing_shadow": bool(args.only_missing_shadow),
                    "include_title_selection": bool(args.include_title_selection),
                    "write": bool(args.write),
                },
            }
            _write_checkpoint(checkpoint_report)

        final_report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "dry_run": not bool(args.write),
            "scanned_material_count": aggregate["scanned_material_count"],
            "updated_material_count": aggregate["updated_material_count"],
            "skipped_material_count": aggregate["skipped_material_count"],
            "target_family_payload_count": aggregate["target_family_payload_count"],
            "family_status_counts": dict(aggregate["family_status_counts"]),
            "audit_snapshots": aggregate["audit_snapshots"],
        }
        json_path = REPORTS_ROOT / f"shadow_mount_enrichment_{ts}.json"
        md_path = REPORTS_ROOT / f"shadow_mount_enrichment_{ts}.md"
        json_path.write_text(json.dumps(final_report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(_build_report_markdown(final_report), encoding="utf-8")
        latest_json_path.write_text(json.dumps(final_report, ensure_ascii=False, indent=2), encoding="utf-8")
        latest_md_path.write_text(_build_report_markdown(final_report), encoding="utf-8")
        print(f"[shadow_mount] wrote report: {json_path}")
        print(f"[shadow_mount] wrote report: {md_path}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
