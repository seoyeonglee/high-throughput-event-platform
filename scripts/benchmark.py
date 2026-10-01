#!/usr/bin/env python3
import argparse
import asyncio
import math
import statistics
import time
import uuid
from datetime import UTC, datetime

import httpx


def build_batch(batch_size: int) -> dict:
    now = datetime.now(UTC).isoformat()
    return {
        "events": [
            {
                "event_id": f"bench_{uuid.uuid4().hex}",
                "user_id": f"bench_user_{i % 10000}",
                "event_type": "product_view",
                "timestamp": now,
                "properties": {"product_id": f"SKU-{i % 1000:04d}"},
            }
            for i in range(batch_size)
        ]
    }


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil((p / 100) * len(ordered)) - 1)
    return ordered[index]


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    request_count = math.ceil(args.events / args.batch_size)
    semaphore = asyncio.Semaphore(args.concurrency)
    latencies: list[float] = []
    failures = 0

    async with httpx.AsyncClient(timeout=60) as client:
        async def send_one(index: int) -> None:
            nonlocal failures
            remaining = args.events - index * args.batch_size
            size = min(args.batch_size, remaining)
            if size <= 0:
                return
            async with semaphore:
                started = time.perf_counter()
                response = await client.post(
                    f"{args.url}/v1/events/batch",
                    json=build_batch(size),
                    headers={"x-api-key": "benchmark"},
                )
                latencies.append((time.perf_counter() - started) * 1000)
                if response.status_code >= 400:
                    failures += 1

        started = time.perf_counter()
        await asyncio.gather(*(send_one(i) for i in range(request_count)))
        elapsed = time.perf_counter() - started

    print(f"events={args.events}")
    print(f"requests={request_count}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"throughput_events_per_second={args.events / elapsed:.1f}")
    print(f"latency_ms_mean={statistics.mean(latencies):.2f}")
    print(f"latency_ms_p50={percentile(latencies, 50):.2f}")
    print(f"latency_ms_p95={percentile(latencies, 95):.2f}")
    print(f"latency_ms_p99={percentile(latencies, 99):.2f}")
    print(f"failed_requests={failures}")


if __name__ == "__main__":
    asyncio.run(main())
