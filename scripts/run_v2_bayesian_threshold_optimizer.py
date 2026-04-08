from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import run_v2_offline_backtest as backtest


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "reports"


@dataclass(frozen=True)
class ThresholdPolicy:
    min_final_candidate_score: float
    min_readiness_score: float
    max_penalty: float
    min_fallback_review_score: float

    def as_dict(self) -> dict[str, float]:
        return {
            "min_final_candidate_score": round(self.min_final_candidate_score, 4),
            "min_readiness_score": round(self.min_readiness_score, 4),
            "max_penalty": round(self.max_penalty, 4),
            "min_fallback_review_score": round(self.min_fallback_review_score, 4),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline-only Bayesian-style threshold optimizer for V2 scoring payloads."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=backtest.DEFAULT_DB_PATH,
        help="Path to passage_service SQLite database.",
    )
    parser.add_argument(
        "--task-families",
        nargs="*",
        default=None,
        help="Subset of task families to optimize. Default: auto-detect from scored records.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=72,
        help="Sequential optimization iterations per task family.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Random seed for reproducible search.",
    )
    parser.add_argument(
        "--min-scored-records",
        type=int,
        default=40,
        help="Minimum scored records required before optimizing a task family.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated reports.",
    )
    return parser.parse_args()


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def main() -> int:
    configure_stdout()
    args = parse_args()
    random.seed(args.seed)

    records = backtest.load_records(args.db_path, selected_families=None)
    grouped = group_scored_records(records)
    selected_task_families = args.task_families or sorted(grouped.keys())

    report: dict[str, Any] = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(args.db_path),
        "optimizer": {
            "type": "lightweight_sequential_bayesian_search",
            "iterations": args.iterations,
            "seed": args.seed,
            "min_scored_records": args.min_scored_records,
            "note": "Pure Python surrogate search with kernel-regression mean/uncertainty; offline only.",
        },
        "task_results": {},
        "notes": [
            "This optimizer does not modify service thresholds or write back any configuration.",
            "Objective uses material status/release_channel as weak proxy labels because feedback tables are empty.",
            "Policies here are wrapper-style offline thresholds, not a claim of exact parity with in-service logic.",
        ],
    }

    for task_family in selected_task_families:
        task_records = grouped.get(task_family, [])
        if len(task_records) < args.min_scored_records:
            report["task_results"][task_family] = {
                "status": "skipped_insufficient_scored_records",
                "scored_record_count": len(task_records),
            }
            continue
        result = optimize_task_family(
            task_family=task_family,
            records=task_records,
            iterations=max(12, args.iterations),
            seed=args.seed,
        )
        report["task_results"][task_family] = result

    json_path, md_path = write_report(report, args.output_dir)
    print(json.dumps({"json_report": str(json_path), "markdown_report": str(md_path)}, ensure_ascii=False))
    return 0


def group_scored_records(records: list[backtest.Record]) -> dict[str, list[backtest.Record]]:
    grouped: dict[str, list[backtest.Record]] = defaultdict(list)
    for record in records:
        if record.has_scoring:
            grouped[record.task_family].append(record)
    return grouped


def optimize_task_family(
    *,
    task_family: str,
    records: list[backtest.Record],
    iterations: int,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed + sum(ord(ch) for ch in task_family))
    current_thresholds = backtest.CURRENT_THRESHOLDS.get(task_family)
    if not current_thresholds:
        return {
            "status": "skipped_missing_current_thresholds",
            "scored_record_count": len(records),
        }

    signal_summary = {
        "max_final_candidate_score": round(max(record.final_candidate_score for record in records), 4),
        "max_readiness_score": round(max(record.readiness_score for record in records), 4),
        "min_penalty": round(min(record.total_penalty for record in records), 4),
        "median_final_candidate_score": round(percentile([record.final_candidate_score for record in records], 0.50), 4),
        "median_readiness_score": round(percentile([record.readiness_score for record in records], 0.50), 4),
    }
    if signal_summary["max_final_candidate_score"] <= 0.0:
        return {
            "status": "skipped_no_discriminative_final_score_signal",
            "scored_record_count": len(records),
            "signal_summary": signal_summary,
        }

    space = build_policy_space(task_family=task_family, records=records)
    if not space:
        return {
            "status": "skipped_empty_policy_space",
            "scored_record_count": len(records),
        }

    current_policy = ThresholdPolicy(
        min_final_candidate_score=current_thresholds["recommended"],
        min_readiness_score=current_thresholds["review_readiness"],
        max_penalty=current_thresholds["review_penalty"],
        min_fallback_review_score=current_thresholds["fallback_review_score"],
    )
    observed: dict[ThresholdPolicy, dict[str, Any]] = {}

    def evaluate_and_store(policy: ThresholdPolicy) -> None:
        if policy not in observed:
            observed[policy] = evaluate_policy(policy=policy, records=records)

    evaluate_and_store(current_policy)

    initial_samples = min(max(8, iterations // 4), max(1, len(space)))
    remaining = [policy for policy in space if policy != current_policy]
    rng.shuffle(remaining)
    for policy in remaining[:initial_samples]:
        evaluate_and_store(policy)

    while len(observed) < min(len(space), iterations):
        candidate = select_next_policy(
            observed=observed,
            candidate_space=space,
            rng=rng,
        )
        if candidate is None:
            break
        evaluate_and_store(candidate)

    ranked = sorted(
        observed.items(),
        key=lambda item: (
            item[1]["objective"],
            item[1]["selection_proxy_precision"],
            -item[1]["selection_deprecated_rate"],
            item[1]["selected_count"],
        ),
        reverse=True,
    )
    best_policy, best_metrics = ranked[0]
    current_metrics = observed[current_policy]
    recommendation = build_recommendation(best_metrics)

    return {
        "status": "ok",
        "scored_record_count": len(records),
        "signal_summary": signal_summary,
        "policy_space_size": len(space),
        "evaluated_policies": len(observed),
        "current_policy": current_policy.as_dict(),
        "current_metrics": current_metrics,
        "best_policy": best_policy.as_dict(),
        "best_metrics": best_metrics,
        "recommendation": recommendation,
        "improvement_vs_current": {
            "objective_delta": round(best_metrics["objective"] - current_metrics["objective"], 4),
            "selection_proxy_precision_delta": round(
                best_metrics["selection_proxy_precision"] - current_metrics["selection_proxy_precision"],
                4,
            ),
            "selection_deprecated_rate_delta": round(
                best_metrics["selection_deprecated_rate"] - current_metrics["selection_deprecated_rate"],
                4,
            ),
            "selected_count_delta": int(best_metrics["selected_count"] - current_metrics["selected_count"]),
            "review_queue_count_delta": int(best_metrics["review_queue_count"] - current_metrics["review_queue_count"]),
        },
        "top_candidates": [
            {
                "policy": policy.as_dict(),
                "metrics": metrics,
            }
            for policy, metrics in ranked[:8]
        ],
    }


def build_policy_space(
    *,
    task_family: str,
    records: list[backtest.Record],
) -> list[ThresholdPolicy]:
    current = backtest.CURRENT_THRESHOLDS[task_family]
    final_values = candidate_values(
        values=[record.final_candidate_score for record in records],
        anchors=[
            current["recommended"] - 0.08,
            current["recommended"] - 0.04,
            current["recommended"],
            current["recommended"] + 0.04,
            current["recommended"] + 0.08,
        ],
        floor=0.15,
        ceil=0.90,
    )
    readiness_values = candidate_values(
        values=[record.readiness_score for record in records],
        anchors=[
            current["review_readiness"] - 0.10,
            current["review_readiness"] - 0.05,
            current["review_readiness"],
            current["review_readiness"] + 0.05,
        ],
        floor=0.15,
        ceil=0.95,
    )
    penalty_values = candidate_values(
        values=[record.total_penalty for record in records],
        anchors=[
            current["review_penalty"] - 0.08,
            current["review_penalty"] - 0.04,
            current["review_penalty"],
            current["review_penalty"] + 0.08,
            current["review_penalty"] + 0.16,
        ],
        floor=0.0,
        ceil=max(1.5, percentile([record.total_penalty for record in records], 0.95) * 1.05),
    )
    fallback_values = candidate_values(
        values=[record.final_candidate_score for record in records],
        anchors=[
            current["fallback_review_score"] - 0.10,
            current["fallback_review_score"] - 0.05,
            current["fallback_review_score"],
            current["fallback_review_score"] + 0.05,
        ],
        floor=0.05,
        ceil=0.85,
    )

    policies: list[ThresholdPolicy] = []
    for final_min in final_values:
        for readiness_min in readiness_values:
            for penalty_max in penalty_values:
                for fallback_min in fallback_values:
                    if fallback_min > final_min:
                        continue
                    policy = ThresholdPolicy(
                        min_final_candidate_score=round(final_min, 4),
                        min_readiness_score=round(readiness_min, 4),
                        max_penalty=round(penalty_max, 4),
                        min_fallback_review_score=round(fallback_min, 4),
                    )
                    policies.append(policy)
    unique = list(dict.fromkeys(policies))
    return unique


def candidate_values(
    *,
    values: list[float],
    anchors: list[float],
    floor: float,
    ceil: float,
) -> list[float]:
    cleaned = sorted(max(floor, min(ceil, float(value))) for value in values if value is not None)
    quantiles = [0.20, 0.35, 0.50, 0.65, 0.75, 0.85, 0.92]
    candidates = [max(floor, min(ceil, anchor)) for anchor in anchors]
    if cleaned:
        candidates.extend(percentile(cleaned, q) for q in quantiles)
    rounded = sorted({round(item, 4) for item in candidates})
    return rounded


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * ratio
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def evaluate_policy(
    *,
    policy: ThresholdPolicy,
    records: list[backtest.Record],
) -> dict[str, Any]:
    selected: list[backtest.Record] = []
    review_queue: list[backtest.Record] = []
    review_readiness_floor = max(0.15, policy.min_readiness_score - 0.08)

    for record in records:
        if (
            record.final_candidate_score >= policy.min_final_candidate_score
            and record.readiness_score >= policy.min_readiness_score
            and record.total_penalty <= policy.max_penalty
        ):
            selected.append(record)
        elif (
            record.final_candidate_score >= policy.min_fallback_review_score
            and record.readiness_score >= review_readiness_floor
        ):
            review_queue.append(record)

    stable_selected = sum(
        1
        for record in selected
        if record.material_status != "deprecated"
        and (record.material_status == "promoted" or record.release_channel == "stable")
    )
    deprecated_selected = sum(1 for record in selected if record.material_status == "deprecated")
    gray_selected = sum(1 for record in selected if record.material_status == "gray")
    gray_review = sum(1 for record in review_queue if record.material_status == "gray")
    deprecated_review = sum(1 for record in review_queue if record.material_status == "deprecated")

    selected_count = len(selected)
    review_count = len(review_queue)
    total_count = max(1, len(records))

    selection_proxy_precision = stable_selected / selected_count if selected_count else 0.0
    selection_deprecated_rate = deprecated_selected / selected_count if selected_count else 0.0
    selection_gray_rate = gray_selected / selected_count if selected_count else 0.0
    selection_support = selected_count / total_count

    review_gray_focus = gray_review / review_count if review_count else 0.0
    review_deprecated_rate = deprecated_review / review_count if review_count else 0.0
    review_support = review_count / total_count

    objective = (
        1.00 * selection_proxy_precision
        - 1.75 * selection_deprecated_rate
        - 0.30 * selection_gray_rate
        + min(0.12, selection_support * 0.24)
        + min(0.06, review_gray_focus * 0.06)
        - 0.16 * review_deprecated_rate
        + min(0.03, review_support * 0.03)
    )

    return {
        "objective": round(objective, 4),
        "selected_count": selected_count,
        "selected_rate": round(selection_support, 4),
        "selected_stable_count": stable_selected,
        "selected_gray_count": gray_selected,
        "selected_deprecated_count": deprecated_selected,
        "selection_proxy_precision": round(selection_proxy_precision, 4),
        "selection_deprecated_rate": round(selection_deprecated_rate, 4),
        "selection_gray_rate": round(selection_gray_rate, 4),
        "review_queue_count": review_count,
        "review_queue_rate": round(review_support, 4),
        "review_gray_focus": round(review_gray_focus, 4),
        "review_deprecated_rate": round(review_deprecated_rate, 4),
    }


def build_recommendation(best_metrics: dict[str, Any]) -> dict[str, Any]:
    objective = float(best_metrics.get("objective") or 0.0)
    selected_count = int(best_metrics.get("selected_count") or 0)
    deprecated_rate = float(best_metrics.get("selection_deprecated_rate") or 0.0)
    if objective <= 0:
        return {
            "decision": "do_not_apply",
            "reason": "no_positive_proxy_objective_found",
        }
    if selected_count == 0:
        return {
            "decision": "do_not_apply",
            "reason": "review_only_policy_without_positive_selection",
        }
    if deprecated_rate >= 0.35:
        return {
            "decision": "do_not_apply",
            "reason": "deprecated_leakage_too_high",
        }
    return {
        "decision": "candidate_for_manual_review_only",
        "reason": "offline_proxy_positive_but_still_requires_human_confirmation",
    }


def select_next_policy(
    *,
    observed: dict[ThresholdPolicy, dict[str, Any]],
    candidate_space: list[ThresholdPolicy],
    rng: random.Random,
) -> ThresholdPolicy | None:
    unseen = [policy for policy in candidate_space if policy not in observed]
    if not unseen:
        return None
    if len(observed) < 3:
        return rng.choice(unseen)

    best_policy: ThresholdPolicy | None = None
    best_acquisition = float("-inf")
    observations = [
        (policy_to_vector(policy), metrics["objective"])
        for policy, metrics in observed.items()
    ]
    length_scale = 0.22
    beta = 1.35

    for policy in unseen:
        vector = policy_to_vector(policy)
        weights: list[float] = []
        weighted_values: list[tuple[float, float]] = []
        for obs_vector, obs_value in observations:
            distance_sq = squared_distance(vector, obs_vector)
            weight = math.exp(-distance_sq / max(1e-6, 2 * length_scale * length_scale))
            weights.append(weight)
            weighted_values.append((weight, obs_value))
        total_weight = sum(weights)
        if total_weight <= 1e-9:
            acquisition = rng.random()
        else:
            mean = sum(weight * value for weight, value in weighted_values) / total_weight
            variance = sum(weight * ((value - mean) ** 2) for weight, value in weighted_values) / total_weight
            novelty_bonus = 1.0 / math.sqrt(1.0 + total_weight)
            std = math.sqrt(max(0.0, variance)) + 0.10 * novelty_bonus
            acquisition = mean + beta * std + 0.02 * novelty_bonus
        if acquisition > best_acquisition:
            best_acquisition = acquisition
            best_policy = policy
    return best_policy


def policy_to_vector(policy: ThresholdPolicy) -> tuple[float, float, float, float]:
    return (
        policy.min_final_candidate_score,
        policy.min_readiness_score,
        policy.max_penalty,
        policy.min_fallback_review_score,
    )


def squared_distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sum((left_item - right_item) ** 2 for left_item, right_item in zip(left, right, strict=False))


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = [
        "# V2 离线贝叶斯阈值优化报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 数据库：`{report['db_path']}`",
        f"- 优化器：`{report['optimizer']['type']}`",
        f"- 迭代次数：`{report['optimizer']['iterations']}`",
        "",
    ]

    for task_family, result in report["task_results"].items():
        lines.extend([f"## {task_family}", ""])
        if result.get("status") != "ok":
            lines.append(f"- 状态：`{result.get('status')}`")
            lines.append(f"- scored_record_count：`{result.get('scored_record_count', 0)}`")
            lines.append("")
            continue

        lines.extend(
            [
                f"- scored_record_count：`{result['scored_record_count']}`",
                f"- policy_space_size：`{result['policy_space_size']}`",
                f"- evaluated_policies：`{result['evaluated_policies']}`",
                f"- current_policy：`{json.dumps(result['current_policy'], ensure_ascii=False)}`",
                f"- best_policy：`{json.dumps(result['best_policy'], ensure_ascii=False)}`",
                f"- recommendation：`{json.dumps(result['recommendation'], ensure_ascii=False)}`",
                f"- improvement_vs_current：`{json.dumps(result['improvement_vs_current'], ensure_ascii=False)}`",
                "",
                "| rank | min_final | min_readiness | max_penalty | min_fallback_review | objective | precision_proxy | deprecated_rate | selected_count | review_queue_count |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for index, item in enumerate(result["top_candidates"], start=1):
            policy = item["policy"]
            metrics = item["metrics"]
            lines.append(
                "| {rank} | {min_final:.4f} | {min_readiness:.4f} | {max_penalty:.4f} | {min_fallback:.4f} | {objective:.4f} | {precision:.4f} | {deprecated:.4f} | {selected_count} | {review_count} |".format(
                    rank=index,
                    min_final=policy["min_final_candidate_score"],
                    min_readiness=policy["min_readiness_score"],
                    max_penalty=policy["max_penalty"],
                    min_fallback=policy["min_fallback_review_score"],
                    objective=metrics["objective"],
                    precision=metrics["selection_proxy_precision"],
                    deprecated=metrics["selection_deprecated_rate"],
                    selected_count=metrics["selected_count"],
                    review_count=metrics["review_queue_count"],
                )
            )
        lines.append("")

    lines.extend(["## 说明", ""])
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"v2_bayesian_threshold_optimizer_{timestamp}.json"
    md_path = output_dir / f"v2_bayesian_threshold_optimizer_{timestamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    raise SystemExit(main())
