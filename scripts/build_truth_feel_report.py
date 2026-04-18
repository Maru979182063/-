from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.round1_generation_smoke_rerun import (  # noqa: E402
    DOCX_MAP,
    extract_docx_blocks,
    load_sample_rows,
    parse_source_question,
)


@dataclass
class FamilyRun:
    family: str
    csv_path: Path


RUNS = [
    FamilyRun("center_understanding", REPORTS / "center_understanding_truth_round1_results_2026-04-14.csv"),
    FamilyRun("sentence_fill", REPORTS / "sentence_fill_truth_round3_results_2026-04-14.csv"),
    FamilyRun("sentence_order", REPORTS / "sentence_order_truth_round1_results_2026-04-14.csv"),
]


def _norm(text: str) -> str:
    text = (text or "").strip().replace("\u3000", " ")
    return re.sub(r"\s+", "", text)


def _stem_style_bucket(family: str, stem: str) -> str:
    stem = stem or ""
    if family == "center_understanding":
        if any(key in stem for key in ("意在", "旨在", "强调", "概括最准确", "鎰忓湪", "鏃ㄥ湪", "寮鸿皟", "姒傛嫭鏈€鍑嗙‘")):
            return "exam_main_idea"
        return "other"
    if family == "sentence_fill":
        if any(
            key in stem
            for key in (
                "填入画横线部分最恰当的一句是",
                "填入划横线部分最恰当的一句是",
                "濉叆鐢绘í绾块儴鍒嗘渶鎭板綋鐨勪竴鍙ユ槸",
            )
        ):
            return "exam_fill"
        return "other"
    if family == "sentence_order":
        if (
            ("重新排列" in stem and any(k in stem for k in ("语序正确", "顺序")))
            or ("閲嶆柊鎺掑垪" in stem and any(k in stem for k in ("璇簭姝ｇ‘", "椤哄簭")))
        ):
            return "exam_order"
        return "other"
    return "other"


def _analysis_style_score(analysis: str) -> float:
    analysis = analysis or ""
    score = 0.0
    if any(key in analysis for key in ("故正确答案为", "故答案为", "因此答案为", "鏁呮纭瓟妗堜负")):
        score += 0.4
    if any(k in analysis for k in ("排除", "错误项", "错在", "鎺掗櫎", "閿欒椤?", "閿欏湪")):
        score += 0.3
    if any(k in analysis for k in ("文段", "材料", "主旨", "空位", "首句", "尾句", "绑定", "鏂囨", "鏉愭枡", "涓绘棬", "绌轰綅", "棣栧彞", "灏惧彞", "缁戝畾")):
        score += 0.3
    return min(score, 1.0)


def _option_shape_score(src_opts: dict[str, str], gen_opts: dict[str, str]) -> float:
    if not src_opts or not gen_opts:
        return 0.0
    if len(src_opts) != len(gen_opts):
        return 0.2
    src_lens = [len(_norm(v)) for _, v in sorted(src_opts.items())]
    gen_lens = [len(_norm(v)) for _, v in sorted(gen_opts.items())]
    if not src_lens or not gen_lens:
        return 0.0
    src_avg = mean(src_lens)
    gen_avg = mean(gen_lens)
    if src_avg == 0:
        return 0.0
    avg_ratio = min(gen_avg / src_avg, src_avg / gen_avg) if gen_avg > 0 else 0.0
    src_span = max(src_lens) - min(src_lens)
    gen_span = max(gen_lens) - min(gen_lens)
    span_ratio = 1.0 if src_span == 0 and gen_span == 0 else 0.0
    if src_span > 0 and gen_span > 0:
        span_ratio = min(gen_span / src_span, src_span / gen_span)
    return max(0.0, min(1.0, 0.65 * avg_ratio + 0.35 * span_ratio))


def _material_tone_score(source_passage: str, presented_material: str) -> float:
    # Tone/feel proxy: length band + punctuation rhythm + paragraph integrity.
    s = _norm(source_passage)
    g = _norm(presented_material)
    if not s or not g:
        return 0.0
    len_ratio = min(len(s), len(g)) / max(len(s), len(g))
    src_punc = len(re.findall(r"[，。；：！？]", source_passage or ""))
    gen_punc = len(re.findall(r"[，。；：！？]", presented_material or ""))
    if src_punc == 0 and gen_punc == 0:
        punc_ratio = 1.0
    elif src_punc == 0 or gen_punc == 0:
        punc_ratio = 0.0
    else:
        punc_ratio = min(src_punc, gen_punc) / max(src_punc, gen_punc)
    return max(0.0, min(1.0, 0.7 * len_ratio + 0.3 * punc_ratio))


def main() -> None:
    sample_rows = load_sample_rows()
    docx_blocks = {
        source_name: extract_docx_blocks(path)
        for source_name, path in DOCX_MAP.items()
        if path.exists()
    }

    out_dir = REPORTS / "distill_runs" / "truth_feel_round_20260414_r01"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "truth_feel_report.csv"
    md_path = out_dir / "truth_feel_report.md"

    rows: list[dict[str, str]] = []

    for run in RUNS:
        if not run.csv_path.exists():
            continue
        with run.csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            for item in csv.DictReader(f):
                sample_id = item.get("anchor_sample_id", "")
                sample = sample_rows.get(sample_id, {})
                source_name = sample.get("source_name", "")
                source_qid = sample.get("source_qid", "")
                block = (docx_blocks.get(source_name) or {}).get(source_qid)
                source_q = parse_source_question(block["lines"], family=run.family) if block else {
                    "passage": "",
                    "stem": "",
                    "options": {},
                    "analysis": "",
                    "answer": "",
                }

                generated_options: dict[str, str] = {}
                try:
                    generated_options = json.loads(item.get("generated_options_json") or "{}") or {}
                except Exception:
                    generated_options = {}

                source_options = source_q.get("options") if isinstance(source_q.get("options"), dict) else {}
                source_stem = str(source_q.get("stem") or "")
                generated_stem = str(item.get("generated_stem") or "")
                source_passage = str(source_q.get("passage") or "")
                presented_material = str(item.get("final_presented_material") or "")
                generated_analysis = str(item.get("generated_analysis") or "")

                stem_bucket_src = _stem_style_bucket(run.family, source_stem)
                stem_bucket_gen = _stem_style_bucket(run.family, generated_stem)
                stem_style_score = 1.0 if stem_bucket_src == stem_bucket_gen and stem_bucket_gen != "other" else 0.0
                analysis_style = _analysis_style_score(generated_analysis)
                option_shape = _option_shape_score(source_options, generated_options)
                material_tone = _material_tone_score(source_passage, presented_material)

                feel_score = round(
                    100.0 * (0.30 * stem_style_score + 0.25 * option_shape + 0.25 * analysis_style + 0.20 * material_tone),
                    2,
                )
                feel_pass = feel_score >= 70.0

                rows.append(
                    {
                        "family": run.family,
                        "group_id": item.get("group_id", ""),
                        "anchor_sample_id": sample_id,
                        "generation_succeeded": item.get("question_generation_succeeded", ""),
                        "validator_passed": item.get("validation_passed", ""),
                        "stem_style_score": f"{stem_style_score:.4f}",
                        "option_shape_score": f"{option_shape:.4f}",
                        "analysis_style_score": f"{analysis_style:.4f}",
                        "material_tone_score": f"{material_tone:.4f}",
                        "truth_feel_score": f"{feel_score:.2f}",
                        "truth_feel_pass": str(feel_pass).lower(),
                    }
                )

    if not rows:
        raise SystemExit("No rows available for truth-feel report.")

    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    family_stats: dict[str, dict[str, float]] = {}
    for row in rows:
        fam = row["family"]
        fam_bucket = family_stats.setdefault(fam, {"n": 0, "feel_pass": 0, "avg_feel_score": 0.0})
        fam_bucket["n"] += 1
        fam_bucket["feel_pass"] += 1 if row["truth_feel_pass"] == "true" else 0
        fam_bucket["avg_feel_score"] += float(row["truth_feel_score"])
    for fam in family_stats:
        n = max(1, int(family_stats[fam]["n"]))
        family_stats[fam]["avg_feel_score"] = round(family_stats[fam]["avg_feel_score"] / n, 2)

    lines = ["# 真题语感/题感一致性报告（R1）", ""]
    for fam, stats in family_stats.items():
        lines.append(f"## {fam}")
        lines.append(f"- 样本数: {int(stats['n'])}")
        lines.append(f"- 题感通过: {int(stats['feel_pass'])}/{int(stats['n'])}")
        lines.append(f"- 题感均分: {stats['avg_feel_score']}")
        lines.append("")
    lines.append("说明：本报告以题感一致性为主，不要求与原题逐字一致。")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
