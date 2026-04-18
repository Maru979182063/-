from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import httpx


DEFAULT_STANDARD_PAYLOAD = {
    "question_focus": "center_understanding",
    "difficulty_level": "medium",
    "count": 1,
    "source_question": {
        "passage": (
            "人工智能正在进入城市治理、医疗协同和公共服务等领域。"
            "技术提升效率的同时，也要求制度设计、伦理审查和数据治理同步跟进。"
        ),
        "stem": "根据材料，下列最适合作为标题的一项是？",
        "options": {
            "A": "智能技术应用需要与制度建设同步推进",
            "B": "只要引入人工智能，治理问题就会自然消失",
            "C": "技术企业应独自承担全部公共治理责任",
            "D": "公共服务数字化意味着传统治理方式已经失效",
        },
        "answer": "A",
        "analysis": "A项能够概括材料主旨，兼顾技术应用与制度建设两方面要求。",
    },
}

DEFAULT_FORCED_USER_MATERIAL_PAYLOAD = {
    "question_focus": "center_understanding",
    "difficulty_level": "medium",
    "count": 1,
    "generation_mode": "forced_user_material",
    "user_material": {
        "text": (
            "人工智能正逐步参与城市治理、交通调度和公共服务。"
            "但技术越深入现实场景，越需要配套的数据治理、伦理规范与责任分工。"
            "只有把效率提升与制度完善放在一起考虑，技术才能真正形成公共价值。"
        )
    },
    "source_question": {
        "stem": "根据材料，下列最适合作为标题的一项是？",
        "options": {
            "A": "推动智能治理必须兼顾技术效率与制度完善",
            "B": "人工智能能够完全替代公共治理中的制度安排",
            "C": "技术落地后，治理责任可以自然消失",
            "D": "城市治理只需要算力，不再需要规则约束",
        },
        "answer": "A",
        "analysis": "A项完整概括材料中心，兼顾技术应用与制度建设。",
    },
}


class DiagnosticsSampler:
    def __init__(self, client: httpx.Client, diagnostics_url: str, interval_seconds: float) -> None:
        self._client = client
        self._diagnostics_url = diagnostics_url
        self._interval_seconds = max(0.2, interval_seconds)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="diagnostics-sampler", daemon=True)
        self.samples: list[dict[str, Any]] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(2.0, self._interval_seconds * 3))

    def _run(self) -> None:
        while not self._stop_event.is_set():
            captured_at = time.time()
            try:
                response = self._client.get(self._diagnostics_url)
                payload = response.json()
                queue_state = ((payload.get("runtime_state") or {}).get("generation_queue") or {})
                self.samples.append(
                    {
                        "captured_at": captured_at,
                        "status_code": response.status_code,
                        "service_status": payload.get("status"),
                        "active_requests": int(queue_state.get("active_requests", 0) or 0),
                        "waiting_requests": int(queue_state.get("waiting_requests", 0) or 0),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                self.samples.append(
                    {
                        "captured_at": captured_at,
                        "status_code": None,
                        "service_status": "probe_error",
                        "error": str(exc),
                    }
                )
            self._stop_event.wait(self._interval_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load test /api/v1/questions/generate with queue-aware diagnostics.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8111", help="Prompt service base URL.")
    parser.add_argument("--endpoint", default="/api/v1/questions/generate", help="Generate endpoint path.")
    parser.add_argument("--requests", type=int, default=10, help="Total requests to send.")
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum in-flight requests.")
    parser.add_argument("--timeout-seconds", type=float, default=300.0, help="Per-request timeout.")
    parser.add_argument("--diagnostics-path", default="/api/v1/diagnostics/dependencies", help="Diagnostics endpoint path.")
    parser.add_argument("--diagnostics-interval", type=float, default=1.0, help="Diagnostics polling interval.")
    parser.add_argument("--payload-file", help="Optional JSON payload file for generate requests.")
    parser.add_argument(
        "--preset",
        choices=("standard", "forced_user_material"),
        default="forced_user_material",
        help="Built-in payload preset when no payload file is supplied.",
    )
    parser.add_argument("--auth-token", help="Optional bearer token for protected deployments.")
    parser.add_argument("--output-json", help="Optional path to write the full JSON report.")
    parser.add_argument("--warmup", type=int, default=1, help="Warm-up request count before the measured run.")
    parser.add_argument("--allow-unready", action="store_true", help="Continue even if /readyz is not 200.")
    return parser


def load_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.payload_file:
        return json.loads(Path(args.payload_file).read_text(encoding="utf-8"))
    if args.preset == "standard":
        return json.loads(json.dumps(DEFAULT_STANDARD_PAYLOAD, ensure_ascii=False))
    return json.loads(json.dumps(DEFAULT_FORCED_USER_MATERIAL_PAYLOAD, ensure_ascii=False))


def percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * ratio
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def parse_json_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return response.json()
        except json.JSONDecodeError:
            return None
    return None


def build_headers(args: argparse.Namespace) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if args.auth_token:
        headers["Authorization"] = f"Bearer {args.auth_token}"
    return headers


def issue_request(
    client: httpx.Client,
    url: str,
    payload: dict[str, Any],
    request_index: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    started_at = time.time()
    response: httpx.Response | None = None
    body: Any = None
    error_text: str | None = None
    try:
        response = client.post(url, json=payload)
        body = parse_json_response(response)
        if body is None:
            error_text = response.text[:500]
    except Exception as exc:  # noqa: BLE001
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "request_index": request_index,
            "started_at": started_at,
            "duration_ms": duration_ms,
            "status_code": None,
            "ok": False,
            "exception": type(exc).__name__,
            "message": str(exc),
        }

    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    payload_error = ((body or {}).get("error") or {}) if isinstance(body, dict) else {}
    error_details = payload_error.get("details") if isinstance(payload_error, dict) else {}
    items = (body or {}).get("items") if isinstance(body, dict) else None
    return {
        "request_index": request_index,
        "started_at": started_at,
        "duration_ms": duration_ms,
        "status_code": response.status_code,
        "ok": response.is_success,
        "batch_id": (body or {}).get("batch_id") if isinstance(body, dict) else None,
        "item_count": len(items) if isinstance(items, list) else None,
        "request_id": response.headers.get("X-Request-ID"),
        "queue_position": _safe_int(response.headers.get("X-Generation-Queue-Position")),
        "queue_wait_seconds": _safe_float(response.headers.get("X-Generation-Wait-Seconds")),
        "active_requests": _safe_int(response.headers.get("X-Generation-Active")),
        "waiting_requests": _safe_int(response.headers.get("X-Generation-Waiting")),
        "error_message": payload_error.get("message") if isinstance(payload_error, dict) else error_text,
        "error_reason": error_details.get("reason") if isinstance(error_details, dict) else None,
        "error_details": error_details if isinstance(error_details, dict) else None,
    }


def _safe_int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _safe_float(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def summarize_results(
    *,
    args: argparse.Namespace,
    payload: dict[str, Any],
    ready_probe: dict[str, Any],
    warmup_results: list[dict[str, Any]],
    results: list[dict[str, Any]],
    diagnostics_samples: list[dict[str, Any]],
    total_seconds: float,
) -> dict[str, Any]:
    success_results = [item for item in results if item["ok"]]
    failure_results = [item for item in results if not item["ok"]]
    durations = [float(item["duration_ms"]) for item in results]
    queue_waits = [float(item["queue_wait_seconds"]) for item in results if item.get("queue_wait_seconds") is not None]
    queued_requests = [item for item in results if (item.get("queue_position") or 0) > 0 or (item.get("queue_wait_seconds") or 0) > 0]
    status_counter = Counter(str(item["status_code"]) for item in results)
    error_reason_counter = Counter(item.get("error_reason") or item.get("exception") or "unknown" for item in failure_results)
    diagnostics_active = [sample["active_requests"] for sample in diagnostics_samples if sample.get("active_requests") is not None]
    diagnostics_waiting = [sample["waiting_requests"] for sample in diagnostics_samples if sample.get("waiting_requests") is not None]

    summary = {
        "base_url": args.base_url.rstrip("/"),
        "endpoint": args.endpoint,
        "preset": args.preset if not args.payload_file else "custom_file",
        "requests": args.requests,
        "concurrency": args.concurrency,
        "timeout_seconds": args.timeout_seconds,
        "payload": payload,
        "ready_probe": ready_probe,
        "warmup": {
            "count": len(warmup_results),
            "success": sum(1 for item in warmup_results if item["ok"]),
            "failure": sum(1 for item in warmup_results if not item["ok"]),
        },
        "results": results,
        "summary": {
            "success_count": len(success_results),
            "failure_count": len(failure_results),
            "success_rate": round(len(success_results) / len(results), 4) if results else 0.0,
            "throughput_rps": round(len(results) / total_seconds, 3) if total_seconds > 0 else None,
            "total_seconds": round(total_seconds, 3),
            "latency_ms": {
                "min": round(min(durations), 2) if durations else None,
                "avg": round(statistics.fmean(durations), 2) if durations else None,
                "p50": round(percentile(durations, 0.5), 2) if durations else None,
                "p90": round(percentile(durations, 0.9), 2) if durations else None,
                "p95": round(percentile(durations, 0.95), 2) if durations else None,
                "max": round(max(durations), 2) if durations else None,
            },
            "queue": {
                "queued_request_count": len(queued_requests),
                "max_queue_position_seen": max((item.get("queue_position") or 0) for item in results) if results else 0,
                "wait_seconds_avg": round(statistics.fmean(queue_waits), 4) if queue_waits else 0.0,
                "wait_seconds_p95": round(percentile(queue_waits, 0.95), 4) if queue_waits else 0.0,
                "wait_seconds_max": round(max(queue_waits), 4) if queue_waits else 0.0,
            },
            "status_codes": dict(status_counter),
            "error_reasons": dict(error_reason_counter),
        },
        "diagnostics": {
            "sample_count": len(diagnostics_samples),
            "max_active_requests": max(diagnostics_active) if diagnostics_active else 0,
            "max_waiting_requests": max(diagnostics_waiting) if diagnostics_waiting else 0,
            "samples": diagnostics_samples,
        },
    }
    return summary


def _linked_passage_summary(ready_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(ready_payload, dict):
        return None
    for check in ready_payload.get("checks") or []:
        if check.get("name") != "passage_service":
            continue
        details = check.get("details") or {}
        return {
            "base_url": details.get("base_url"),
            "service_status": details.get("service_status"),
            "database_mode": details.get("database_mode"),
            "resolved_database_path": details.get("resolved_database_path"),
            "primary_material_count": details.get("primary_material_count"),
            "v2_indexed_primary_count": details.get("v2_indexed_primary_count"),
        }
    return None


def print_summary(report: dict[str, Any]) -> None:
    summary = report["summary"]
    queue = summary["queue"]
    latency = summary["latency_ms"]
    diagnostics = report["diagnostics"]
    ready_probe = report["ready_probe"]
    linked_passage = _linked_passage_summary(ready_probe.get("payload"))

    print("=== Load Test Summary ===")
    print(f"Base URL: {report['base_url']}")
    print(f"Endpoint: {report['endpoint']}")
    print(f"Preset: {report['preset']}")
    print(f"Requests: {report['requests']} total, concurrency={report['concurrency']}")
    print(
        "Ready probe: "
        f"status_code={ready_probe.get('status_code')} service_status={ready_probe.get('service_status')} "
        f"continue={ready_probe.get('continued')}"
    )
    if linked_passage:
        print(
            "Linked passage: "
            f"base_url={linked_passage.get('base_url')} "
            f"mode={linked_passage.get('database_mode')} "
            f"primary_materials={linked_passage.get('primary_material_count')} "
            f"v2_indexed_primary={linked_passage.get('v2_indexed_primary_count')}"
        )
    print(
        "Success: "
        f"{summary['success_count']}/{report['requests']} "
        f"({round(summary['success_rate'] * 100, 2)}%), failures={summary['failure_count']}"
    )
    print(
        "Latency(ms): "
        f"min={latency['min']} avg={latency['avg']} p50={latency['p50']} "
        f"p90={latency['p90']} p95={latency['p95']} max={latency['max']}"
    )
    print(
        "Queue: "
        f"queued={queue['queued_request_count']} "
        f"max_position={queue['max_queue_position_seen']} "
        f"avg_wait={queue['wait_seconds_avg']}s "
        f"p95_wait={queue['wait_seconds_p95']}s "
        f"max_wait={queue['wait_seconds_max']}s"
    )
    print(
        "Diagnostics: "
        f"max_active={diagnostics['max_active_requests']} "
        f"max_waiting={diagnostics['max_waiting_requests']} "
        f"samples={diagnostics['sample_count']}"
    )
    print(f"Status codes: {json.dumps(summary['status_codes'], ensure_ascii=False)}")
    if summary["error_reasons"]:
        print(f"Error reasons: {json.dumps(summary['error_reasons'], ensure_ascii=False)}")


def probe_ready(client: httpx.Client, ready_url: str, allow_unready: bool) -> dict[str, Any]:
    response = client.get(ready_url)
    payload = parse_json_response(response)
    service_status = payload.get("status") if isinstance(payload, dict) else None
    if not response.is_success and not allow_unready:
        raise RuntimeError(f"ready probe failed with status {response.status_code}: {response.text[:300]}")
    return {
        "status_code": response.status_code,
        "service_status": service_status,
        "continued": allow_unready or response.is_success,
        "payload": payload,
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = load_payload(args)

    base_url = args.base_url.rstrip("/")
    endpoint_url = f"{base_url}{args.endpoint}"
    diagnostics_url = f"{base_url}{args.diagnostics_path}"
    ready_url = f"{base_url}/readyz"
    headers = build_headers(args)
    timeout = httpx.Timeout(args.timeout_seconds)

    with httpx.Client(timeout=timeout, headers=headers, trust_env=False) as client:
        ready_probe = probe_ready(client, ready_url, args.allow_unready)

        warmup_results: list[dict[str, Any]] = []
        for index in range(args.warmup):
            warmup_results.append(issue_request(client, endpoint_url, payload, request_index=-(index + 1)))

        sampler = DiagnosticsSampler(client, diagnostics_url, args.diagnostics_interval)
        sampler.start()
        started = time.perf_counter()
        results: list[dict[str, Any]] = []
        try:
            with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as executor:
                futures = [
                    executor.submit(issue_request, client, endpoint_url, payload, request_index=index + 1)
                    for index in range(args.requests)
                ]
                for future in as_completed(futures):
                    results.append(future.result())
        finally:
            total_seconds = time.perf_counter() - started
            sampler.stop()

    ordered_results = sorted(results, key=lambda item: item["request_index"])
    report = summarize_results(
        args=args,
        payload=payload,
        ready_probe=ready_probe,
        warmup_results=warmup_results,
        results=ordered_results,
        diagnostics_samples=sampler.samples,
        total_seconds=total_seconds,
    )
    print_summary(report)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Report written to: {output_path}")

    return 0 if report["summary"]["failure_count"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
