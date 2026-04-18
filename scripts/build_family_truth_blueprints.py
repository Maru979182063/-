from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
DATE_TAG = "2026-04-14"


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def percent_to_float(value: str) -> float | None:
    text = str(value or "").strip().replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def top_counter(rows: list[dict[str, str]], field: str, limit: int = 6) -> list[tuple[str, int]]:
    counter = Counter(str(row.get(field) or "").strip() or "unknown" for row in rows)
    return counter.most_common(limit)


def render_counter(items: list[tuple[str, int]]) -> str:
    return " / ".join(f"`{key}`={count}" for key, count in items)


def build_center_understanding_blueprint() -> str:
    path = REPORTS / f"truth_distillation_assets_center_understanding_{DATE_TAG}.csv"
    rows = load_rows(path)
    axis_top = top_counter(rows, "main_axis_source_guess")
    structure_top = top_counter(rows, "argument_structure_guess")
    level_top = top_counter(rows, "correct_option_level_guess")
    distractor_top = top_counter(rows, "distractor_modes")
    rates = [rate for rate in (percent_to_float(row.get("correct_rate", "")) for row in rows) if rate is not None]
    avg_rate = sum(rates) / len(rates) if rates else 0.0

    return "\n".join(
        [
            "# center_understanding 真题反向蒸馏蓝图 v1",
            "",
            "## 资产来源",
            "- 源文件：`C:\\Users\\Maru\\Desktop\\片段阅读-中心理解题.docx`",
            f"- 结构化资产：`{path}`",
            f"- 总样本数：`{len(rows)}`",
            "",
            "## 真题统计快照",
            f"- 主轴来源 top：{render_counter(axis_top)}",
            f"- 结构 top：{render_counter(structure_top)}",
            f"- 正确项抽象层级 top：{render_counter(level_top)}",
            f"- 干扰项类型 top：{render_counter(distractor_top)}",
            f"- 平均正确率：`{avg_rate:.2f}%`",
            "",
            "## 材料怎么蒸",
            "1. 材料必须保留完整展开，不能压成摘要或主轴提示卡。",
            "2. 文段里要能读出主轴来源：常见是转折后、尾段总结、问题-对策后的结论句。",
            "3. 举例、背景、案例只是支撑，不应替代中心句本身。",
            "4. 如果材料本身是多分句/多层展开，question service 应优先保留 2 到 4 段完整正文，而不是只保留轴句附近两句。",
            "",
            "## 题干怎么蒸",
            "1. 题干高度稳定，核心就是“这段文字意在说明( ) / 这段文字主要说明的是( )”。",
            "2. 不要写成长解释式题干，不要把结构提示写进题干。",
            "3. 题干应保持考中心、考主轴、考主旨的纯度，不转成态度题、细节题、启示题。",
            "",
            "## 正确项 / 干扰项怎么蒸",
            "1. 正确项必须是对全文主轴的统摄性归纳，不是局部段意复述。",
            "2. 正确项抽象层级要略高于材料表层，但不能拔高到脱离原文。",
            "3. 干扰项要优先做成真题常见错法：片面概括、局部替整体、过度引申、偷换论题、把例子当主旨。",
            "4. 如果要比真题略难，应增加干扰项之间的近误程度，而不是把正确项做得更虚。",
            "",
            "## 解析怎么蒸",
            "1. 解析应先点中心句/主轴来源，再说明为什么正确项是统摄性归纳。",
            "2. 解析要顺手拆一到两个典型错项，而不是把所有选项都写成长文。",
            "3. 解析口气应像公考真题解析，不要写成系统说明书或评分报告。",
            "",
            "## 难度怎么接正确率",
            "1. 正确率高的题：主轴更集中，干扰项更典型。",
            "2. 正确率中等的题：正确项抽象层级更高一点，干扰项更容易出现“局部像主旨”的错觉。",
            "3. 正确率低的题：可适度提高片面项/引申项的迷惑性，但不能把材料压成摘要来偷难度。",
            "",
            "## validator 应该怎么对齐真题",
            "1. validator 先问：材料是否像一段可独立阅读的真题正文。",
            "2. 再问：正确项是不是全文主轴归纳，而不是局部复述。",
            "3. 最后问：干扰项是不是常见真题错法，而不是明显无关。",
            "4. 不允许把“结构更规整”当成比真题感更高的标准。",
            "",
            "## 对当前 family 重写的直接要求",
            "1. 固定完整正文优先的材料消费策略。",
            "2. 在 prompt 里显式加入 `main_axis_source / argument_structure / correct_option_abstraction_level / distractor_taxonomy`。",
            "3. validator 只作为真题感守门器和重做触发器，不得替代真题本身。",
            "",
        ]
    )


def build_sentence_order_blueprint() -> str:
    path = REPORTS / f"truth_distillation_assets_sentence_order_{DATE_TAG}.csv"
    rows = load_rows(path)
    opener_top = top_counter(rows, "opener_type_guess")
    closer_top = top_counter(rows, "closer_type_guess")
    chain_top = top_counter(rows, "chain_features")
    distractor_top = top_counter(rows, "distractor_pattern_guess")
    rates = [rate for rate in (percent_to_float(row.get("correct_rate", "")) for row in rows) if rate is not None]
    avg_rate = sum(rates) / len(rates) if rates else 0.0

    return "\n".join(
        [
            "# sentence_order 真题反向蒸馏蓝图 v2",
            "",
            "## 资产来源",
            "- 源文件：`C:\\Users\\Maru\\Desktop\\语句表达-语句排序题.docx`",
            f"- 结构化资产：`{path}`",
            f"- 总样本数：`{len(rows)}`",
            "",
            "## 真题统计快照",
            f"- opener top：{render_counter(opener_top)}",
            f"- closer top：{render_counter(closer_top)}",
            f"- chain feature top：{render_counter(chain_top)}",
            f"- distractor pattern top：{render_counter(distractor_top)}",
            f"- 平均正确率：`{avg_rate:.2f}%`",
            "",
            "## 材料怎么蒸",
            "1. 排序题材料不是整段摘要，而是 6 个能形成唯一顺序链的自然显示单元。",
            "2. 先找链，再出题：必须先抽到 opener 约束、binding pair、closure 约束都清楚的材料。",
            "3. 显示单元要像自然句，不得加工成“首先/其次/最后”的教学腔提示句。",
            "4. 允许最小显示单元改造，但不允许改坏原文逻辑。",
            "",
            "## 题干怎么蒸",
            "1. 题干高度固定：`将以上6个句子重新排列，语序正确的是( )。`",
            "2. 题干不解释规则，不在 stem 里提示排序依据。",
            "3. 重点不在 stem，而在 6 句链条本身够不够硬。",
            "",
            "## 选项 / 答案怎么蒸",
            "1. 正确答案必须建立在唯一顺序链上，而不是“勉强能排通”。",
            "2. 错项要优先来自相邻交换、强绑定拆散、首尾误配、局部误绑。",
            "3. 不要用随机乱序凑错项，也不要只凭表面连接词堆迷惑性。",
            "4. 若要比真题略难，应增强近邻错序和局部误绑迷惑性，但不能把唯一链做松。",
            "",
            "## 解析怎么蒸",
            "1. 解析必须短、硬，先抓首句/尾句/绑定对中的一两个关键抓手。",
            "2. 解析不是逐句翻译，不是流水账，不是“先看1句再看2句”。",
            "3. 解析应顺手点出至少一种错项为什么错，比如绑定拆错、首尾放错、转折链断掉。",
            "",
            "## 难度怎么接正确率",
            "1. 正确率高：首句和强绑定非常明显，错项更典型。",
            "2. 正确率中等：近邻交换更迷惑，但依然能靠一两个硬约束定序。",
            "3. 正确率低：可增加局部误绑和近邻错序迷惑性，但绝不能牺牲唯一顺序。",
            "",
            "## validator 应该怎么对齐真题",
            "1. 先问：6 个显示单元是否都是自然句且可完整阅读。",
            "2. 再问：是否至少存在 opener / binding / closure 这三类中的两类以上硬约束。",
            "3. 再问：错项是不是近邻误排，而不是随机乱排。",
            "4. 如果真题 probe 都过不去的规则，必须下线或重写，不能继续当铁律。",
            "",
            "## 对当前 family 重写的直接要求",
            "1. 先把完整题面稳定输出，再继续磨真题感。",
            "2. 在 prompt 里显式加入 `opener_type / closer_type / binding_pairs / chain_features / distractor_permutation_style`。",
            "3. repair loop 不要只看 overall_score，要看题链是否像真题硬链。",
            "",
        ]
    )


def main() -> None:
    center_md = REPORTS / f"center_understanding_truth_blueprint_{DATE_TAG}.md"
    sentence_order_md = REPORTS / f"sentence_order_truth_blueprint_{DATE_TAG}.md"
    center_md.write_text(build_center_understanding_blueprint() + "\n", encoding="utf-8")
    sentence_order_md.write_text(build_sentence_order_blueprint() + "\n", encoding="utf-8")
    print(center_md)
    print(sentence_order_md)


if __name__ == "__main__":
    main()
