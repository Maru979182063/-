from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
DEFAULT_DB_PATH = ROOT / "passage_service" / "passage_service.db"
DEFAULT_OUTPUT_DIR = ROOT / "reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-click offline suite for V2 backtest, threshold optimization, and active learning queue."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to passage_service SQLite database.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated reports.",
    )
    parser.add_argument(
        "--families",
        nargs="*",
        default=None,
        help="Optional subset of business_family_id values for backtest and active learning queue.",
    )
    parser.add_argument(
        "--task-families",
        nargs="*",
        default=None,
        help="Optional subset of task families for threshold optimization.",
    )
    parser.add_argument(
        "--backtest-review-top-k",
        type=int,
        default=20,
        help="Top-k review items to include in the backtest report.",
    )
    parser.add_argument(
        "--optimizer-iterations",
        type=int,
        default=72,
        help="Sequential optimization iterations per task family.",
    )
    parser.add_argument(
        "--optimizer-seed",
        type=int,
        default=7,
        help="Random seed for the optimizer.",
    )
    parser.add_argument(
        "--optimizer-min-scored-records",
        type=int,
        default=40,
        help="Minimum scored records before optimizing a task family.",
    )
    parser.add_argument(
        "--active-top-k",
        type=int,
        default=30,
        help="Top-k review candidates for the active learning queue.",
    )
    parser.add_argument(
        "--active-per-family-cap",
        type=int,
        default=10,
        help="Per-family cap for the active learning queue.",
    )
    parser.add_argument(
        "--active-per-card-cap",
        type=int,
        default=6,
        help="Per-card cap for the active learning queue.",
    )
    parser.add_argument(
        "--include-missing-review",
        action="store_true",
        help="Include gray items with missing tagging review rows in the active learning queue.",
    )
    return parser.parse_args()


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def main() -> int:
    configure_stdout()
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    backtest_result = run_component(
        script_path=SCRIPTS_DIR / "run_v2_offline_backtest.py",
        extra_args=build_backtest_args(args),
    )
    optimizer_result = run_component(
        script_path=SCRIPTS_DIR / "run_v2_bayesian_threshold_optimizer.py",
        extra_args=build_optimizer_args(args),
    )
    active_result = run_component(
        script_path=SCRIPTS_DIR / "run_v2_active_learning_queue.py",
        extra_args=build_active_queue_args(args),
    )

    backtest_report = read_json(backtest_result["json_report"])
    optimizer_report = read_json(optimizer_result["json_report"])
    active_report = read_json(active_result["json_report"])

    suite_report = build_suite_report(
        db_path=args.db_path,
        args=args,
        backtest_result=backtest_result,
        optimizer_result=optimizer_result,
        active_result=active_result,
        backtest_report=backtest_report,
        optimizer_report=optimizer_report,
        active_report=active_report,
    )
    json_path, md_path = write_report(suite_report, args.output_dir)
    print(json.dumps({"json_report": str(json_path), "markdown_report": str(md_path)}, ensure_ascii=False))
    return 0


def build_backtest_args(args: argparse.Namespace) -> list[str]:
    command = [
        "--db-path",
        str(args.db_path),
        "--output-dir",
        str(args.output_dir),
        "--review-top-k",
        str(max(1, args.backtest_review_top_k)),
    ]
    if args.families:
        command.extend(["--families", *args.families])
    return command


def build_optimizer_args(args: argparse.Namespace) -> list[str]:
    command = [
        "--db-path",
        str(args.db_path),
        "--output-dir",
        str(args.output_dir),
        "--iterations",
        str(max(12, args.optimizer_iterations)),
        "--seed",
        str(args.optimizer_seed),
        "--min-scored-records",
        str(max(1, args.optimizer_min_scored_records)),
    ]
    if args.task_families:
        command.extend(["--task-families", *args.task_families])
    return command


def build_active_queue_args(args: argparse.Namespace) -> list[str]:
    command = [
        "--db-path",
        str(args.db_path),
        "--output-dir",
        str(args.output_dir),
        "--top-k",
        str(max(1, args.active_top_k)),
        "--per-family-cap",
        str(max(1, args.active_per_family_cap)),
        "--per-card-cap",
        str(max(1, args.active_per_card_cap)),
    ]
    if args.families:
        command.extend(["--families", *args.families])
    if args.include_missing_review:
        command.append("--include-missing-review")
    return command


def run_component(*, script_path: Path, extra_args: list[str]) -> dict[str, str]:
    command = [sys.executable, str(script_path), *extra_args]
    completed = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Component failed: {script_path.name}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

    payload = parse_json_line(completed.stdout)
    json_report = str(Path(payload["json_report"]).resolve())
    markdown_report = str(Path(payload["markdown_report"]).resolve())
    return {
        "script": str(script_path),
        "json_report": json_report,
        "markdown_report": markdown_report,
    }


def parse_json_line(stdout: str) -> dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "json_report" in payload and "markdown_report" in payload:
            return payload
    raise ValueError(f"Unable to find component JSON payload in stdout: {stdout!r}")


def read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_suite_report(
    *,
    db_path: Path,
    args: argparse.Namespace,
    backtest_result: dict[str, str],
    optimizer_result: dict[str, str],
    active_result: dict[str, str],
    backtest_report: dict[str, Any],
    optimizer_report: dict[str, Any],
    active_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "mode": "offline_external_only",
        "parameters": {
            "families": args.families or [],
            "task_families": args.task_families or [],
            "backtest_review_top_k": args.backtest_review_top_k,
            "optimizer_iterations": args.optimizer_iterations,
            "optimizer_seed": args.optimizer_seed,
            "optimizer_min_scored_records": args.optimizer_min_scored_records,
            "active_top_k": args.active_top_k,
            "active_per_family_cap": args.active_per_family_cap,
            "active_per_card_cap": args.active_per_card_cap,
            "include_missing_review": args.include_missing_review,
        },
        "reports": {
            "backtest": backtest_result,
            "optimizer": optimizer_result,
            "active_learning_queue": active_result,
        },
        "highlights": {
            "backtest": summarize_backtest(backtest_report),
            "optimizer": summarize_optimizer(optimizer_report),
            "active_learning_queue": summarize_active_queue(active_report),
        },
        "notes": [
            "This suite orchestrates offline-only scripts and does not modify service code, thresholds, database rows, or runtime endpoints.",
            "The component reports remain the source of truth; this suite report is a convenience index and summary.",
            "If future work requires touching service logic or internal integration, stop and request confirmation first.",
        ],
    }


def summarize_backtest(report: dict[str, Any]) -> dict[str, Any]:
    summary = report.get("summary", {})
    family_summary = summary.get("by_business_family", {})
    family_snapshot = []
    for family, values in sorted(
        family_summary.items(),
        key=lambda item: item[1].get("scoring_available_count", 0),
        reverse=True,
    )[:6]:
        family_snapshot.append(
            {
                "business_family_id": family,
                "count": values.get("count", 0),
                "scoring_available_count": values.get("scoring_available_count", 0),
                "scoring_missing_count": values.get("scoring_missing_count", 0),
                "avg_final_candidate_score": values.get("avg_final_candidate_score", 0.0),
                "avg_readiness_score": values.get("avg_readiness_score", 0.0),
                "avg_total_penalty": values.get("avg_total_penalty", 0.0),
            }
        )

    threshold_snapshot = []
    for task_family, values in sorted(report.get("threshold_sweep", {}).items()):
        threshold_snapshot.append(
            {
                "task_family": task_family,
                "top_scenario_count": len(values.get("top_scenarios", [])),
                "current_thresholds": values.get("current_thresholds", {}),
            }
        )

    return {
        "run_at": report.get("run_at"),
        "total_records": summary.get("total_records", 0),
        "total_materials": summary.get("total_materials", 0),
        "business_families": summary.get("business_families", []),
        "family_snapshot": family_snapshot,
        "threshold_snapshot": threshold_snapshot,
        "review_priority_queue_size": len(report.get("review_priority_queue", [])),
    }


def summarize_optimizer(report: dict[str, Any]) -> dict[str, Any]:
    task_results = report.get("task_results", {})
    task_snapshot = []
    for task_family, values in sorted(task_results.items()):
        best_metrics = values.get("best_metrics", {})
        recommendation = values.get("recommendation", {})
        task_snapshot.append(
            {
                "task_family": task_family,
                "status": values.get("status", "unknown"),
                "scored_record_count": values.get("scored_record_count", 0),
                "best_objective": best_metrics.get("objective"),
                "selected_count": best_metrics.get("selected_count"),
                "review_count": best_metrics.get("review_count"),
                "recommendation": recommendation.get("decision"),
                "recommendation_reason": recommendation.get("reason"),
            }
        )

    return {
        "run_at": report.get("run_at"),
        "optimizer": report.get("optimizer", {}),
        "task_snapshot": task_snapshot,
    }


def summarize_active_queue(report: dict[str, Any]) -> dict[str, Any]:
    summary = report.get("summary", {})
    queue = report.get("queue", [])
    top_candidates = []
    for item in queue[:8]:
        top_candidates.append(
            {
                "rank": item.get("rank"),
                "material_id": item.get("material_id"),
                "chosen_family": item.get("chosen_family"),
                "chosen_task_family": item.get("chosen_task_family"),
                "selected_business_card": item.get("selected_business_card"),
                "priority_score": item.get("priority_score"),
                "reasons": item.get("reasons", []),
            }
        )

    return {
        "run_at": report.get("run_at"),
        "eligible_record_count": summary.get("eligible_record_count", 0),
        "eligible_material_count": summary.get("eligible_material_count", 0),
        "selected_candidate_count": summary.get("selected_candidate_count", 0),
        "selected_family_distribution": summary.get("selected_family_distribution", {}),
        "selected_reason_distribution": summary.get("selected_reason_distribution", {}),
        "top_candidates": top_candidates,
    }


def render_markdown(report: dict[str, Any]) -> str:
    backtest = report["highlights"]["backtest"]
    optimizer = report["highlights"]["optimizer"]
    active = report["highlights"]["active_learning_queue"]

    lines = [
        "# V2 External Offline Suite",
        "",
        f"- run_at: `{report['run_at']}`",
        f"- db_path: `{report['db_path']}`",
        f"- mode: `{report['mode']}`",
        "",
        "## Reports",
        "",
        f"- backtest json: `{report['reports']['backtest']['json_report']}`",
        f"- backtest markdown: `{report['reports']['backtest']['markdown_report']}`",
        f"- optimizer json: `{report['reports']['optimizer']['json_report']}`",
        f"- optimizer markdown: `{report['reports']['optimizer']['markdown_report']}`",
        f"- active queue json: `{report['reports']['active_learning_queue']['json_report']}`",
        f"- active queue markdown: `{report['reports']['active_learning_queue']['markdown_report']}`",
        "",
        "## Backtest Highlights",
        "",
        f"- total_records: `{backtest['total_records']}`",
        f"- total_materials: `{backtest['total_materials']}`",
        f"- business_families: `{json.dumps(backtest['business_families'], ensure_ascii=False)}`",
        f"- review_priority_queue_size: `{backtest['review_priority_queue_size']}`",
        "",
        "| family | count | scoring_ready | scoring_missing | avg_final | avg_readiness | avg_penalty |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for item in backtest["family_snapshot"]:
        lines.append(
            "| {business_family_id} | {count} | {scoring_available_count} | {scoring_missing_count} | {avg_final_candidate_score:.4f} | {avg_readiness_score:.4f} | {avg_total_penalty:.4f} |".format(
                **item
            )
        )

    lines.extend(
        [
            "",
            "## Optimizer Highlights",
            "",
            "| task_family | status | scored_records | best_objective | selected_count | review_count | recommendation | reason |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )

    for item in optimizer["task_snapshot"]:
        lines.append(
            "| {task_family} | {status} | {scored_record_count} | {best_objective} | {selected_count} | {review_count} | {recommendation} | {recommendation_reason} |".format(
                task_family=item["task_family"],
                status=item["status"],
                scored_record_count=item["scored_record_count"],
                best_objective=render_scalar(item["best_objective"]),
                selected_count=render_scalar(item["selected_count"]),
                review_count=render_scalar(item["review_count"]),
                recommendation=render_scalar(item["recommendation"]),
                recommendation_reason=render_scalar(item["recommendation_reason"]),
            )
        )

    lines.extend(
        [
            "",
            "## Active Queue Highlights",
            "",
            f"- eligible_record_count: `{active['eligible_record_count']}`",
            f"- eligible_material_count: `{active['eligible_material_count']}`",
            f"- selected_candidate_count: `{active['selected_candidate_count']}`",
            f"- selected_family_distribution: `{json.dumps(active['selected_family_distribution'], ensure_ascii=False)}`",
            f"- selected_reason_distribution: `{json.dumps(active['selected_reason_distribution'], ensure_ascii=False)}`",
            "",
            "| rank | material_id | family | task_family | priority | reasons | card |",
            "| ---: | --- | --- | --- | ---: | --- | --- |",
        ]
    )

    for item in active["top_candidates"]:
        lines.append(
            "| {rank} | `{material_id}` | {chosen_family} | {chosen_task_family} | {priority_score:.4f} | `{reasons}` | `{selected_business_card}` |".format(
                rank=item["rank"],
                material_id=item["material_id"],
                chosen_family=item["chosen_family"],
                chosen_task_family=item["chosen_task_family"],
                priority_score=item["priority_score"] or 0.0,
                reasons=json.dumps(item["reasons"], ensure_ascii=False),
                selected_business_card=item["selected_business_card"] or "",
            )
        )

    lines.extend(["", "## Notes", ""])
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def render_scalar(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"v2_external_suite_{timestamp}.json"
    md_path = output_dir / f"v2_external_suite_{timestamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    raise SystemExit(main())
