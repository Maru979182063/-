from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe OpenAI-compatible gateway stability.")
    parser.add_argument("--base-url", type=str, default=os.getenv("BASE_URL", "https://api.shubiaobiao.com/v1"))
    parser.add_argument("--model", type=str, default=os.getenv("MODEL", "gpt-5-nano"))
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=25.0)
    return parser.parse_args()


def run_get_models(client: httpx.Client, repeat: int) -> dict:
    statuses = Counter()
    latencies = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        try:
            resp = client.get("/models")
            statuses[resp.status_code] += 1
        except Exception:
            statuses["EXC"] += 1
        latencies.append(round((time.perf_counter() - t0) * 1000, 2))
    return {"statuses": dict(statuses), "latency_ms": latencies}


def run_post_chat(client: httpx.Client, model: str, repeat: int) -> dict:
    statuses = Counter()
    latencies = []
    body_preview = Counter()
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
        "temperature": 0,
    }
    for _ in range(repeat):
        t0 = time.perf_counter()
        try:
            resp = client.post("/chat/completions", json=payload)
            statuses[resp.status_code] += 1
            if resp.status_code >= 400:
                body_preview[(resp.text or "")[:80]] += 1
        except Exception as exc:  # noqa: BLE001
            statuses["EXC"] += 1
            body_preview[f"{type(exc).__name__}"] += 1
        latencies.append(round((time.perf_counter() - t0) * 1000, 2))
    return {"statuses": dict(statuses), "latency_ms": latencies, "error_preview": dict(body_preview)}


def run_post_responses(client: httpx.Client, model: str, repeat: int) -> dict:
    statuses = Counter()
    latencies = []
    body_preview = Counter()
    payload = {
        "model": model,
        "input": "ping",
        "max_output_tokens": 8,
    }
    for _ in range(repeat):
        t0 = time.perf_counter()
        try:
            resp = client.post("/responses", json=payload)
            statuses[resp.status_code] += 1
            if resp.status_code >= 400:
                body_preview[(resp.text or "")[:80]] += 1
        except Exception as exc:  # noqa: BLE001
            statuses["EXC"] += 1
            body_preview[f"{type(exc).__name__}"] += 1
        latencies.append(round((time.perf_counter() - t0) * 1000, 2))
    return {"statuses": dict(statuses), "latency_ms": latencies, "error_preview": dict(body_preview)}


def summarize(section: dict) -> dict:
    latencies = section.get("latency_ms") or []
    if not latencies:
        return {"p50_ms": None, "p95_ms": None}
    sorted_vals = sorted(latencies)
    p50 = sorted_vals[int(0.5 * (len(sorted_vals) - 1))]
    p95 = sorted_vals[int(0.95 * (len(sorted_vals) - 1))]
    return {"p50_ms": p50, "p95_ms": p95}


def main() -> int:
    args = parse_args()
    api_key = os.getenv("API_KEY", "").strip()
    if not api_key:
        raise SystemExit("API_KEY is required in environment.")

    with httpx.Client(
        base_url=args.base_url.rstrip("/"),
        timeout=args.timeout,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    ) as client:
        models = run_get_models(client, args.repeat)
        chat = run_post_chat(client, args.model, args.repeat)
        responses = run_post_responses(client, args.model, args.repeat)

    result = {
        "base_url": args.base_url,
        "model": args.model,
        "repeat": args.repeat,
        "models": models,
        "chat_completions": chat,
        "responses": responses,
        "summary": {
            "models": summarize(models),
            "chat_completions": summarize(chat),
            "responses": summarize(responses),
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
