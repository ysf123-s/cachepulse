"""Measure cache invalidation with the demo webhook healthy and returning HTTP 500."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import time
import uuid

import redis
import requests


WEBHOOK = "/platform/demo/webhook"
DELIVERIES = "/platform/deliveries"
PROBE_SECONDS = 0.005


def wait_for_invalidation(cache, key, deadline):
    while time.perf_counter() < deadline:
        if not cache.exists(key):
            return time.perf_counter()
        time.sleep(PROBE_SECONDS)
    raise TimeoutError(f"Cache key {key} was not invalidated within 10 seconds")


def wait_for_deliveries(api, subscription_id, count, failing, terminal):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        tasks = api("GET", DELIVERIES, params={"subscriptionId": subscription_id, "limit": 200})["items"]
        observed = []
        for task in tasks:
            status = task["status"]
            if status in ("PENDING", "IN_FLIGHT"):
                continue
            if failing:
                if status not in ("RETRY_WAIT", "FAILED") or not (task["lastError"] or "").startswith("HTTP 500"):
                    raise RuntimeError(f"Expected HTTP 500 for delivery {task['taskId']}, got {status}")
                if terminal and status != "FAILED":
                    continue
            elif status != "SUCCEEDED":
                raise RuntimeError(f"Expected successful delivery {task['taskId']}, got {status}")
            observed.append(task)
        if len(observed) == count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"Expected {count} matching deliveries for subscription {subscription_id}")


def measure_phase(api, cache, row_id, tag, warmup, samples, failing):
    department = f"/departments/{row_id}"
    key = f"department:{row_id}"
    code = api("GET", department)["code"]
    subscription = None
    timings = []
    try:
        subscription = api("POST", "/platform/subscriptions", json={
            "table": "departments",
            "targetName": "demo-webhook",
            "operations": ["UPDATE"],
            "filterField": "id",
            "filterValue": str(row_id),
        })
        subscription_id = subscription["subscriptionId"]
        with ThreadPoolExecutor(max_workers=1) as observer:
            for index in range(warmup + samples):
                cached = api("GET", department)
                raw = cache.hget(key, "data")
                if cached["code"] != code or raw is None or json.loads(raw)["code"] != code:
                    raise RuntimeError(f"Previous department version was not present in Redis for iteration {index + 1}")

                code = f"{tag}-{index + 1}"
                start = time.perf_counter()
                invalidated = observer.submit(wait_for_invalidation, cache, key, start + 10)
                api("PUT", department, json={"name": code, "code": code})
                elapsed_ms = (invalidated.result() - start) * 1_000
                fresh = api("GET", department)
                if fresh["code"] != code:
                    raise RuntimeError(f"Expected the written department version after invalidation at iteration {index + 1}")

                if index + 1 == warmup:
                    wait_for_deliveries(api, subscription_id, warmup, failing, terminal=False)
                if index >= warmup:
                    timings.append(elapsed_ms)

        wait_for_deliveries(api, subscription_id, warmup + samples, failing, terminal=True)
    finally:
        if subscription is not None:
            api(
                "PUT",
                f"/platform/subscriptions/{subscription['subscriptionId']}/enabled",
                params={"expectedVersion": subscription["ruleVersion"]},
                json={"enabled": False},
            )

    ordered = sorted(timings)
    return {
        "webhook": "HTTP_500" if failing else "SUCCESS",
        "verified_deliveries": warmup + samples,
        "p50_ms": round(statistics.median(timings), 3),
        "p95_ms": round(ordered[math.ceil(0.95 * samples) - 1], 3),
        "p99_ms": round(ordered[math.ceil(0.99 * samples) - 1], 3),
        "samples_ms": [round(value, 3) for value in timings],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18090", help="CachePulse gateway URL")
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--redis-port", type=int, default=os.getenv("REDIS_PORT", "6379"))
    parser.add_argument("--redis-db", type=int, default=os.getenv("REDIS_DB", "1"))
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.warmup < 1 or args.samples < 1 or args.warmup + args.samples > 200:
        parser.error("warmup and samples must be positive and add up to at most 200")
    token = os.getenv("CACHEPULSE_BENCH_TOKEN")
    if not token:
        parser.error("set CACHEPULSE_BENCH_TOKEN to a gateway administrator token")

    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "warmup_per_phase": args.warmup,
        "samples_per_phase": args.samples,
        "method": "Sequential updates to one department, from before HTTP PUT to the first Redis probe with no cache key.",
        "redis_probe_interval_ms": PROBE_SECONDS * 1_000,
    }
    with requests.Session() as session, redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=os.getenv("REDIS_PASSWORD"),
        socket_timeout=3,
        socket_connect_timeout=3,
    ) as cache:
        session.headers["X-CachePulse-Token"] = token
        base = args.base_url.rstrip("/") + "/api"

        def api(method, path, **kwargs):
            response = session.request(method, base + path, timeout=20, **kwargs)
            response.raise_for_status()
            return None if response.status_code == 204 else response.json()

        cache.ping()
        if api("GET", WEBHOOK + "/records")["currentChaosMode"] != "NORMAL":
            raise RuntimeError("The demo webhook must be in NORMAL mode before the comparison")

        tag = "isolation-" + uuid.uuid4().hex[:10]
        row_id = api("POST", "/departments", json={"name": tag, "code": tag})["id"]
        try:
            print("Measuring with a healthy demo webhook", flush=True)
            report["normal"] = measure_phase(api, cache, row_id, tag + "-ok", args.warmup, args.samples, failing=False)
            print("Measuring with a demo webhook returning HTTP 500", flush=True)
            try:
                api("POST", WEBHOOK + "/chaos", json={"mode": "CHAOS_500"})
                report["failure"] = measure_phase(api, cache, row_id, tag + "-err", args.warmup, args.samples, failing=True)
            finally:
                api("POST", WEBHOOK + "/chaos", json={"mode": "NORMAL"})
        finally:
            api("DELETE", f"/departments/{row_id}")

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Cache invalidation P99: healthy {report['normal']['p99_ms']} ms, "
          f"HTTP 500 {report['failure']['p99_ms']} ms. Report: {args.output}")


if __name__ == "__main__":
    main()
