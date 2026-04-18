from __future__ import annotations

import glob
import json
import os
import random
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DISTILL_BASE = ROOT / "reports" / "distill_batches"
OUT_DIR = ROOT / "reports" / "pressure_tests" / "depth1"


def read_lines_with_fallback(path: Path) -> list[str]:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            text = raw.decode(enc)
            return [ln for ln in text.splitlines() if ln.strip()]
        except UnicodeDecodeError:
            continue
    text = raw.decode("utf-8", errors="replace")
    return [ln for ln in text.splitlines() if ln.strip()]

def cjk_ratio(text: str) -> float:
    if not text:
        return 0.0
    cjk = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return cjk / max(1, len(text))


def maybe_fix_mojibake(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    baseline = cjk_ratio(text)
    # Typical mojibake marker set for UTF-8<->GBK mismatch.
    markers = ("锛", "銆", "涓", "浠", "鐨", "鍙", "鎴")
    if baseline > 0.65 and not any(m in text for m in markers):
        return text
    candidates = [text]
    try:
        candidates.append(text.encode("gbk", errors="ignore").decode("utf-8", errors="ignore"))
    except Exception:
        pass
    try:
        candidates.append(text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore"))
    except Exception:
        pass
    best = text
    best_score = baseline
    for cand in candidates:
        score = cjk_ratio(cand)
        if score > best_score + 0.15:
            best = cand
            best_score = score
    return best


def infer_business_family(batch: str) -> str | None:
    if batch.startswith("center_understanding"):
        return "center_understanding"
    if batch.startswith("sentence_fill"):
        return "sentence_fill"
    if batch.startswith("sentence_order"):
        return "sentence_order"
    return None


def main() -> int:
    random.seed(42)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    for path in sorted(glob.glob(str(DISTILL_BASE / "*" / "cleaned_truth_materials.jsonl"))):
        p = Path(path)
        batch = p.parent.name
        biz = infer_business_family(batch)
        if not biz:
            continue
        for ln in read_lines_with_fallback(p):
            try:
                obj = json.loads(ln)
            except json.JSONDecodeError:
                continue
            for k, v in list(obj.items()):
                if isinstance(v, str):
                    obj[k] = maybe_fix_mojibake(v)
            rows.append(
                {
                    "group_id": f"{biz}||{obj.get('subfamily') or ''}||{obj.get('pattern_tag') or ''}",
                    "business_family_id": biz,
                    "batch": batch,
                    "sample_id": obj.get("sample_id"),
                    "question_id": obj.get("question_id"),
                    "family": obj.get("family"),
                    "subfamily": obj.get("subfamily"),
                    "pattern_tag": obj.get("pattern_tag"),
                    "source_doc": obj.get("source_doc"),
                    "material_text": obj.get("material_text"),
                }
            )

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[r["group_id"]].append(r)

    selected: list[dict] = []
    for gid, items in sorted(groups.items()):
        items_sorted = sorted(items, key=lambda x: len(str(x.get("material_text") or "")))
        picks = [items_sorted[0], items_sorted[-1]] if len(items_sorted) >= 2 else items_sorted
        seen: set[str] = set()
        for p in picks:
            sid = str(p.get("sample_id") or "")
            if sid in seen:
                continue
            seen.add(sid)
            selected.append(p)

    seed_file = OUT_DIR / "depth1_seed_samples_2_per_group.jsonl"
    with seed_file.open("w", encoding="utf-8") as w:
        for r in selected:
            w.write(json.dumps(r, ensure_ascii=False) + "\n")

    for i in range(3):
        shard = selected[i::3]
        shard_file = OUT_DIR / f"depth1_seed_samples_shard{i+1}.jsonl"
        with shard_file.open("w", encoding="utf-8") as w:
            for r in shard:
                w.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"rows={len(selected)} groups={len(groups)} out={seed_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
