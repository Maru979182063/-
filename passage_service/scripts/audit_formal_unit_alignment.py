from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

PRIMARY_FAMILIES = ("title_selection", "sentence_order", "sentence_fill")
FORMAL_TYPES = {
    "title_selection": {"whole_passage", "closed_span", "multi_paragraph_unit"},
    "sentence_order": {"ordered_unit_group"},
    "sentence_fill": {"functional_slot_unit"},
}

# unicode-escaped markers
SUMMARY = ("\u603b\u4e4b", "\u53ef\u89c1", "\u56e0\u6b64", "\u7531\u6b64")
TURNING = ("\u4f46\u662f", "\u7136\u800c", "\u4e0d\u8fc7", "\u5374")
CAUSE = ("\u56e0\u4e3a", "\u7531\u4e8e")
PARALLEL = ("\u4e00\u65b9\u9762", "\u53e6\u4e00\u65b9\u9762", "\u540c\u65f6", "\u6b64\u5916")
COUNTER = ("\u5e94\u8be5", "\u5e94\u5f53", "\u9700\u8981", "\u5fc5\u987b", "\u91c7\u53d6", "\u63aa\u65bd")
EXAMPLE = ("\u4f8b\u5982", "\u6bd4\u5982", "\u4e3e\u4f8b", "\u6848\u4f8b")
PRONOUN = ("\u8fd9", "\u90a3", "\u5176", "\u8be5", "\u6b64", "\u8fd9\u4e9b")
CONTEXT_HEAD = ("\u5bf9\u6b64", "\u4e0e\u6b64\u540c\u65f6", "\u53e6\u4e00\u65b9\u9762", "\u6b64\u5916", "\u56e0\u6b64")
FORWARD = ("\u63a5\u4e0b\u6765", "\u8fdb\u4e00\u6b65", "\u4e8e\u662f", "\u56e0\u6b64", "\u4ece\u800c", "\u66f4\u91cd\u8981\u7684\u662f")


def _json_field(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return default


def _sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[\u3002\uff01\uff1f!?])", text or "") if s and s.strip()]


def _marker_density(text: str, markers: tuple[str, ...], sent_n: int) -> float:
    if sent_n <= 0:
        return 0.0
    return min(1.0, sum(1 for m in markers if m in text) / sent_n)


def _first_ok(sent: str) -> bool:
    if not sent:
        return False
    if sent.startswith(PRONOUN) or sent.startswith(CONTEXT_HEAD):
        return False
    if any(m in sent for m in EXAMPLE):
        return False
    return True


def _last_ok(sent: str) -> bool:
    if not sent:
        return False
    if any(m in sent for m in SUMMARY + COUNTER):
        return True
    return False


def _binding_count(units: list[str]) -> int:
    c = 0
    for i in range(len(units) - 1):
        left, right = units[i], units[i + 1]
        if right.startswith(PRONOUN) or right.startswith(CONTEXT_HEAD):
            c += 1
        elif any(m in right for m in TURNING + PARALLEL):
            c += 1
        elif any(m in left for m in CAUSE) and any(m in right for m in SUMMARY):
            c += 1
    return c


def _center_bridgeable(candidate_type: str, text: str, payload: dict[str, Any]) -> tuple[bool, str]:
    if candidate_type not in {"paragraph_window", "sentence_group", "single_paragraph", "story_fragment"}:
        return False, ""
    sents = _sentences(text)
    if len(sents) < 3 or len(sents) > 10:
        return False, ""
    closure = _marker_density(text, SUMMARY, len(sents))
    turning = _marker_density(text, TURNING, len(sents))
    example = _marker_density(text, EXAMPLE, len(sents))
    if closure < 0.15 and turning < 0.15:
        return False, ""
    if example > 0.40 and closure < 0.25:
        return False, ""
    return True, "extract_center_support_bundle_to_closed_span_or_multi_paragraph_unit"


def _order_bridgeable(candidate_type: str, text: str) -> tuple[bool, str]:
    if candidate_type not in {"sentence_block_group", "paragraph_window", "sentence_group", "single_paragraph"}:
        return False, ""
    sents = _sentences(text)
    if len(sents) < 6:
        return False, ""
    for start in range(0, len(sents) - 5):
        unit = sents[start : start + 6]
        if _first_ok(unit[0]) and _last_ok(unit[-1]) and _binding_count(unit) >= 1:
            return True, "slice_to_six_sentence_ordered_unit_group"
    return False, ""


def _fill_bridgeable(candidate_type: str, text: str) -> tuple[bool, str]:
    if candidate_type not in {"sentence_group", "paragraph_window", "sentence_block_group", "single_paragraph", "closed_span", "multi_paragraph_unit", "whole_passage"}:
        return False, ""
    sents = _sentences(text)
    if len(sents) < 2:
        return False, ""
    for idx, sent in enumerate(sents):
        if len(sent) < 8 or len(sent) > 180:
            continue
        role = "opening" if idx == 0 else "ending" if idx == len(sents) - 1 else "middle"
        if role == "middle":
            if (any(m in sent for m in CONTEXT_HEAD) or sent.startswith(PRONOUN)) and any(m in sent for m in FORWARD + TURNING + PARALLEL):
                return True, "extract_middle_bridge_sentence_with_context"
            if sent.startswith(PRONOUN):
                return True, "extract_middle_carry_sentence_with_context"
        elif role == "opening":
            if any(m in sent for m in SUMMARY) or any(m in sent for m in ("\u5f53\u524d", "\u5982\u4eca", "\u8fd1\u5e74\u6765", "\u5728\u6b64\u80cc\u666f\u4e0b")):
                return True, "extract_opening_slot_sentence"
        else:
            if any(m in sent for m in SUMMARY + COUNTER):
                return True, "extract_ending_slot_sentence"
    return False, ""


def _classify(family: str, candidate_type: str, text: str, payload: dict[str, Any]) -> tuple[str, str]:
    if candidate_type in FORMAL_TYPES[family]:
        if family == "title_selection":
            scoring = payload.get("selected_task_scoring") or {}
            struct = scoring.get("structure_scores") or {}
            a = float(struct.get("single_center_score") or 0.0)
            b = float(struct.get("closure_score") or 0.0)
            c = float(struct.get("theme_lift_score") or 0.0)
            return ("already_formal", "") if min(a, b, c) >= 0.40 else ("formal_but_weak", "")
        if family == "sentence_fill":
            signal = payload.get("neutral_signal_profile") or {}
            if signal.get("slot_role") and signal.get("slot_function"):
                return "already_formal", ""
            return "formal_but_weak", "hydrate_slot_role_and_slot_function"
        return "already_formal", ""

    if family == "title_selection":
        ok, action = _center_bridgeable(candidate_type, text, payload)
    elif family == "sentence_order":
        ok, action = _order_bridgeable(candidate_type, text)
    else:
        ok, action = _fill_bridgeable(candidate_type, text)
    return ("bridgeable", action) if ok else ("wrong_shape", "")


def _load_rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT id, article_id, span_type, text, v2_business_family_ids, v2_index_payload
            FROM material_spans
            WHERE is_primary = 1 AND status = 'promoted' AND release_channel = 'stable' AND v2_index_version IS NOT NULL
            """
        )
        items: list[dict[str, Any]] = []
        for material_id, article_id, span_type, text, fam_raw, payload_raw in cur.fetchall():
            fams = _json_field(fam_raw, [])
            payload = _json_field(payload_raw, {})
            if not isinstance(payload, dict):
                continue
            for fam in PRIMARY_FAMILIES:
                if fam not in set(str(x) for x in fams):
                    continue
                fam_payload = payload.get(fam)
                if not isinstance(fam_payload, dict):
                    continue
                items.append(
                    {
                        "material_id": str(material_id),
                        "article_id": str(article_id),
                        "family": fam,
                        "candidate_type": str(fam_payload.get("candidate_type") or span_type or ""),
                        "text": str(text or ""),
                        "payload": fam_payload,
                    }
                )
        return items
    finally:
        conn.close()


def run(sample_limit: int) -> dict[str, Any]:
    db_path = ROOT / "passage_service.db"
    rows = _load_rows(db_path)
    fam_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        fam_groups[r["family"]].append(r)

    analysis: dict[str, Any] = {"families": {}, "global": {}}
    g = Counter()
    for fam in PRIMARY_FAMILIES:
        group = fam_groups.get(fam, [])
        c = Counter()
        type_counter = Counter()
        bridge_counter = Counter()
        samples: list[dict[str, Any]] = []
        for row in group:
            status, action = _classify(row["family"], row["candidate_type"], row["text"], row["payload"])
            c[status] += 1
            g[status] += 1
            type_counter[row["candidate_type"]] += 1
            if action:
                bridge_counter[f"{row['candidate_type']}->{action}"] += 1
            if len(samples) < sample_limit:
                samples.append(
                    {
                        "material_id": row["material_id"],
                        "candidate_type": row["candidate_type"],
                        "status": status,
                        "bridge_action": action,
                        "text_snippet": " ".join(row["text"].split())[:120],
                    }
                )
        total = len(group)
        analysis["families"][fam] = {
            "formal_unit": list(FORMAL_TYPES[fam]),
            "totals": {"total": total, **dict(c)},
            "candidate_type_top": [[k, v] for k, v in type_counter.most_common(8)],
            "bridge_top": [[k, v] for k, v in bridge_counter.most_common(8)],
            "samples": samples,
            "ratios": {
                "formal_ready_ratio": round((c["already_formal"] + c["formal_but_weak"]) / total, 4) if total else 0.0,
                "bridgeable_ratio": round(c["bridgeable"] / total, 4) if total else 0.0,
                "wrong_shape_ratio": round(c["wrong_shape"] / total, 4) if total else 0.0,
            },
        }
    analysis["global"] = {"total": len(rows), **dict(g)}
    return {"database_path": str(db_path), "row_count": len(rows), "analysis": analysis}


def render_md(report: dict[str, Any]) -> str:
    lines = ["# Formal Unit Alignment Audit", ""]
    lines.append(f"- row_count: `{report['row_count']}`")
    lines.append(f"- database_path: `{report['database_path']}`")
    lines.append("")
    for fam in PRIMARY_FAMILIES:
        block = report["analysis"]["families"][fam]
        t = block["totals"]
        lines.append(f"## {fam}")
        lines.append(f"- total: `{t.get('total', 0)}`")
        lines.append(f"- already_formal: `{t.get('already_formal', 0)}`")
        lines.append(f"- formal_but_weak: `{t.get('formal_but_weak', 0)}`")
        lines.append(f"- bridgeable: `{t.get('bridgeable', 0)}`")
        lines.append(f"- wrong_shape: `{t.get('wrong_shape', 0)}`")
        lines.append(f"- ratios: `{block['ratios']}`")
        lines.append("- candidate_type_top:")
        for k, v in block["candidate_type_top"][:6]:
            lines.append(f"  - `{k}`: `{v}`")
        lines.append("- bridge_top:")
        for k, v in block["bridge_top"][:6]:
            lines.append(f"  - `{k}`: `{v}`")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-limit", type=int, default=12)
    parser.add_argument("--output-json", type=str, default="")
    parser.add_argument("--output-md", type=str, default="")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d")
    out_json = Path(args.output_json) if args.output_json else (ROOT.parent / "reports" / f"formal_unit_alignment_audit_{stamp}.json")
    out_md = Path(args.output_md) if args.output_md else (ROOT.parent / "reports" / f"formal_unit_alignment_audit_{stamp}.md")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)

    report = run(sample_limit=max(3, args.sample_limit))
    report["generated_at"] = now.isoformat()
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(render_md(report), encoding="utf-8")
    print(f"[ok] json={out_json}")
    print(f"[ok] md={out_md}")


if __name__ == "__main__":
    main()
