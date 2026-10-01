#!/usr/bin/env python3
import argparse
import asyncio
import random
import time
import uuid
from datetime import UTC, datetime

import httpx

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase"]


def build_event(index: int) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=[45, 35, 15, 5], k=1)[0]
    properties = {"source": random.choice(["web", "app", "email"])}
    if event_type in {"product_view", "add_to_cart", "purchase"}:
        properties["product_id"] = f"SKU-{random.randint(1, 500):04d}"
    if event_type == "purchase":
        properties["amount"] = random.randint(1000, 300000)
        properties["currency"] = "KRW"

    return {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "user_id": f"user_{index % 5000:05d}",
        "event_type": event_type,
        "timestamp": datetime.now(UTC).isoformat(),
        "properties": properties,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    started = time.perf_counter()
    accepted = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for start in range(0, args.count, args.batch_size):
            batch = [build_event(i) for i in range(start, min(start + args.batch_size, args.count))]
            response = await client.post(f"{args.url}/v1/events/batch", json={"events": batch})
            response.raise_for_status()
            accepted += response.json()["accepted"]

    elapsed = time.perf_counter() - started
    print(f"sent={args.count} accepted={accepted} elapsed={elapsed:.2f}s rate={args.count/elapsed:.0f} events/s")


if __name__ == "__main__":
    asyncio.run(main())
