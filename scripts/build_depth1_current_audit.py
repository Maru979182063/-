from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from run_depth1_expanded_eval import _acceptable_material_card_ids


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "reports" / "pressure_tests" / "expanded_eval_runs"
OUT_PATH = ROOT / "reports" / "pressure_tests" / "depth1" / "depth1_current_audit_2026-04-16.md"

REPORT_FILES = [
    "depth1_expanded_eval_20260416_032846.json",
    "depth1_expanded_eval_20260416_033000.json",
    "depth1_expanded_eval_20260416_035553.json",
    "depth1_expanded_eval_20260416_035656.json",
    "depth1_expanded_eval_20260416_120650.json",
    "depth1_expanded_eval_20260416_123614.json",
    "depth1_expanded_eval_20260416_140354.json",
    "depth1_expanded_eval_20260416_131952.json",
    "depth1_expanded_eval_20260416_133109.json",
]


def _rate(num: int, den: int) -> float:
    return round(num / den, 4) if den else 0.0


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    business_family_id = str(normalized.get("business_family_id") or "")
    expected = str(normalized.get("expected_material_card_id") or "")
    selected = str(normalized.get("top_selected_material_card") or "")
    acceptable = normalized.get("acceptable_material_card_ids")
    if not isinstance(acceptable, list):
        acceptable = _acceptable_material_card_ids(
            business_family_id=business_family_id,
            expected_material_card_id=expected,
        )
    strict_hit = bool(normalized.get("strict_hit"))
    if "strict_hit" not in normalized:
        strict_hit = bool(expected and selected == expected)
    acceptable_hit = bool(normalized.get("acceptable_hit"))
    if "acceptable_hit" not in normalized:
        acceptable_hit = bool(selected and selected in acceptable)
    normalized["acceptable_material_card_ids"] = acceptable
    normalized["strict_hit"] = strict_hit
    normalized["acceptable_hit"] = acceptable_hit
    return normalized


def main() -> int:
    family_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    evidence: list[str] = []
    for file_name in REPORT_FILES:
        path = RUNS_DIR / file_name
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("rows") or []:
            normalized = _coerce_row(row)
            family_rows[str(normalized.get("business_family_id") or "")].append(normalized)
        evidence.append(f"- `{file_name}`")

    lines = [
        "# Depth1 Current Audit",
        "",
        "## Overall Verdict",
        "",
    ]

    for family_id in ("sentence_fill", "center_understanding", "sentence_order"):
        rows = family_rows.get(family_id, [])
        total = len(rows)
        lines.append(
            "- "
            f"`{family_id}`: total={total}, "
            f"ingest={_rate(sum(1 for row in rows if row.get('ingest_success')), total)}, "
            f"strict={_rate(sum(1 for row in rows if row.get('strict_hit')), total)}, "
            f"acceptable={_rate(sum(1 for row in rows if row.get('acceptable_hit')), total)}, "
            f"slice_top={_rate(sum(1 for row in rows if row.get('slice_hit_top')), total)}"
        )

    lines.extend(["", "## Family Detail", ""])
    for family_id in ("sentence_fill", "center_understanding", "sentence_order"):
        rows = family_rows.get(family_id, [])
        total = len(rows)
        lines.append(f"### {family_id}")
        lines.append("")
        lines.append(f"- total: `{total}`")
        lines.append(f"- segment_emitted_rate: `{_rate(sum(1 for row in rows if row.get('segment_emitted')), total)}`")
        lines.append(f"- ingest_success_rate: `{_rate(sum(1 for row in rows if row.get('ingest_success')), total)}`")
        lines.append(f"- strict_hit_rate: `{_rate(sum(1 for row in rows if row.get('strict_hit')), total)}`")
        lines.append(f"- acceptable_hit_rate: `{_rate(sum(1 for row in rows if row.get('acceptable_hit')), total)}`")
        lines.append(f"- slice_hit_top_rate: `{_rate(sum(1 for row in rows if row.get('slice_hit_top')), total)}`")
        miss_counter: Counter[str] = Counter()
        acceptable_counter: Counter[str] = Counter()
        for row in rows:
            selected = str(row.get("top_selected_material_card") or "")
            if row.get("acceptable_hit") and not row.get("strict_hit") and selected:
                acceptable_counter[selected] += 1
            elif row.get("ingest_success") and not row.get("acceptable_hit") and selected:
                miss_counter[selected] += 1
        if acceptable_counter:
            lines.append(f"- acceptable overlap cards: `{acceptable_counter.most_common(6)}`")
        if miss_counter:
            lines.append(f"- still-wrong top cards: `{miss_counter.most_common(6)}`")
        lines.append("")

    lines.extend(["## Evidence", ""])
    lines.extend(evidence)
    OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(OUT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
