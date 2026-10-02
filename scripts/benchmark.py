#!/usr/bin/env python3
"""Measure response acceptance separately from committed, run-scoped DB rows.

PostgreSQL is observed with read-only transactions. Observation timestamps are
polling upper bounds, not database commit timestamps. No request is retried: a
lost response can be ambiguous even when the event later appears in PostgreSQL.
"""

import argparse
import asyncio
import json
import math
import os
import platform
import re
import statistics
import subprocess
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx

DEFAULT_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/events"
OUTCOMES = (
    "accepted",
    "duplicates",
    "rejected",
    "http_error",
    "network_failure",
    "malformed_response",
)


@dataclass(frozen=True)
class BenchmarkConfig:
    url: str = "http://localhost:8000"
    events: int = 10000
    concurrency: int = 100
    batch_size: int = 100
    users: int = 10000
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    database_url: str = field(default=DEFAULT_DATABASE_URL, repr=False)
    api_key: str = field(default="benchmark", repr=False)
    request_timeout: float = 60.0
    drain_timeout: float = 120.0
    poll_interval: float = 0.25
    worker_count: int | None = None
    environment_label: str | None = None
    output: str | None = None

    def __post_init__(self):
        for name in ("events", "concurrency", "batch_size", "users"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.batch_size > 1000:
            raise ValueError("batch_size must be at most 1000 (API limit)")
        if self.worker_count is not None and self.worker_count <= 0:
            raise ValueError("worker_count must be positive")
        for name in ("request_timeout", "drain_timeout", "poll_interval"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if not re.fullmatch(r"[A-Za-z0-9-]{1,40}", self.run_id):
            raise ValueError("run_id must be 1–40 ASCII letters, digits, or hyphens")
        if len(f"bench_{self.run_id}_{self.events - 1}") > 64:
            raise ValueError("event IDs would exceed the API's 64-character limit")
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("url must be an absolute HTTP(S) URL")
        if parsed.query or parsed.fragment:
            raise ValueError("url must not include a query or fragment")
        _ = parsed.port  # Validate malformed ports before creating the client.
        if urlsplit(self.database_url).scheme not in {
            "postgres",
            "postgresql",
            "postgresql+asyncpg",
        }:
            raise ValueError("database_url must use PostgreSQL")


def build_batch(
    batch_size: int, *, run_id: str | None = None, start_index: int = 0, users: int = 10000
) -> dict:
    run_id = run_id or uuid.uuid4().hex
    now = datetime.now(UTC).isoformat()
    return {
        "events": [
            {
                "event_id": f"bench_{run_id}_{index}",
                "user_id": f"bench_user_{index % users}",
                "event_type": "product_view",
                "timestamp": now,
                "properties": {"product_id": f"SKU-{index % 1000:04d}"},
            }
            for index in range(start_index, start_index + batch_size)
        ]
    }


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((p / 100) * len(ordered)) - 1))
    return ordered[index]


def latency_summary(values: list[float]) -> dict:
    return {
        "count": len(values),
        "mean": statistics.mean(values) if values else None,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values) if values else None,
    }


def environment_metadata() -> dict:
    dependencies = {}
    for package in ("httpx", "asyncpg", "SQLAlchemy", "fastapi", "redis"):
        try:
            dependencies[package] = version(package)
        except PackageNotFoundError:
            dependencies[package] = None
    root = Path(__file__).resolve().parents[1]

    def git(*args):
        try:
            return subprocess.check_output(
                ["git", "-C", str(root), *args],
                stderr=subprocess.DEVNULL,
                timeout=2,
                text=True,
            ).strip()
        except (OSError, subprocess.SubprocessError):
            return None

    dirty = git("status", "--porcelain")
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "client_dependencies": dependencies,
        "git_revision": git("rev-parse", "HEAD"),
        "git_dirty": bool(dirty) if dirty is not None else None,
        "server_configuration": "not inspected; supply deployment configuration with the report",
    }


def public_config(config: BenchmarkConfig) -> dict:
    parsed = urlsplit(config.url)
    # Never serialize passwords, API keys, or the database connection URL.
    host = parsed.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if parsed.port:
        host += f":{parsed.port}"
    return {
        "url": urlunsplit((parsed.scheme, host, parsed.path, "", "")),
        "events": config.events,
        "concurrency": config.concurrency,
        "batch_size": config.batch_size,
        "users": config.users,
        "request_timeout_seconds": config.request_timeout,
        "drain_timeout_seconds": config.drain_timeout,
        "poll_interval_seconds": config.poll_interval,
        "worker_count": config.worker_count,
        "environment_label": config.environment_label,
    }


def response_counts(response: httpx.Response, size: int) -> dict:
    body = response.json()
    if not isinstance(body, dict):
        raise ValueError("response is not an object")
    counts = {name: body.get(name) for name in ("received", "accepted", "duplicates")}
    counts["rejected"] = body.get("rejected", 0)
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise ValueError("counts must be nonnegative integers")
    if (
        counts["received"] != size
        or sum(counts[name] for name in ("accepted", "duplicates", "rejected")) != size
    ):
        raise ValueError("response counts do not account for the submitted batch")
    return counts


class PostgresObserver:
    """One connection; SELECT-only, read-only transactions scoped to this run."""

    def __init__(self, database_url: str, timeout: float = 60):
        self.database_url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        self.timeout = timeout
        self.connection = None
        self.server_version = None

    async def __call__(self, run_id: str) -> set[str]:
        if self.connection is None:
            import asyncpg

            self.connection = await asyncpg.connect(
                self.database_url,
                timeout=self.timeout,
                command_timeout=self.timeout,
                server_settings={
                    "application_name": "event-platform-benchmark-observer",
                    "default_transaction_read_only": "on",
                },
            )
            # asyncpg's version tuple normalizes 17.11 to (17, 0, 11).
            # Preserve PostgreSQL's original version string instead.
            self.server_version = self.connection.get_settings().server_version
        # Escape LIKE metacharacters, including the literal underscores in our prefix.
        prefix = f"bench_{run_id}_".replace("_", r"\_")
        async with self.connection.transaction(readonly=True):
            rows = await self.connection.fetch(
                "SELECT event_id FROM events WHERE event_id LIKE $1", prefix + "%"
            )
        return {row["event_id"] for row in rows}

    async def close(self):
        if self.connection is not None:
            await self.connection.close(timeout=self.timeout)


async def run_benchmark(
    config: BenchmarkConfig,
    client: httpx.AsyncClient,
    observer: Callable[[str], Awaitable[set[str]]],
) -> dict:
    """Run bounded senders and a concurrent DB poller; return JSON-safe evidence."""
    request_count = math.ceil(config.events / config.batch_size)
    report = {
        "schema_version": 1,
        "run_id": config.run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "config": public_config(config),
        "environment": environment_metadata(),
        "events": {
            "requested": config.events,
            "attempted": 0,
            "db_completed": 0,
            **dict.fromkeys(OUTCOMES, 0),
        },
        "requests": {
            "planned": request_count,
            "attempted": 0,
            "accepted": 0,
            "http_error": 0,
            "network_failure": 0,
            "malformed_response": 0,
            "http_status_counts": {},
        },
        "completion": {
            "all_requested_completed": False,
            "timed_out": False,
            "reason": None,
            "observer_errors": [],
            "polls": 0,
            "target_unique_events": config.events,
        },
    }
    completion = report["completion"]
    events = report["events"]
    requests = report["requests"]
    request_latencies = []
    completion_latencies = []
    completed_ids = set()
    sent_at = {}
    ingestion_end = None
    started = time.perf_counter()
    preflight_started = started
    preflight_seconds = 0.0

    async def poll(timeout):
        completion["polls"] += 1
        async with asyncio.timeout(timeout):
            return await observer(config.run_id)

    def finish():
        ended = time.perf_counter()
        ingestion_seconds = (ingestion_end - started) if ingestion_end is not None else 0.0
        total_seconds = ended - started
        events["db_completed"] = len(completed_ids)
        failed_outcomes = [name for name in OUTCOMES if name != "accepted" and events[name]]
        report["failure_reasons"] = failed_outcomes
        if not completion["all_requested_completed"]:
            report["failure_reasons"].append(completion["reason"] or "incomplete")
        report["success"] = not report["failure_reasons"] and events["accepted"] == config.events
        report["exit_code"] = 0 if report["success"] else 1
        report["finished_at"] = datetime.now(UTC).isoformat()
        rate_window = (
            "load_start_to_end_of_ingestion_and_database_drain"
            if ingestion_end is not None
            else "no_load_started"
        )
        report["timing"] = {
            "preflight_seconds": preflight_seconds,
            "ingestion_seconds": ingestion_seconds,
            "drain_seconds": max(0.0, ended - ingestion_end) if ingestion_end else 0.0,
            "total_seconds": total_seconds,
            "description": (
                "Timed load excludes metadata collection and preflight, includes dispatch and "
                "task cleanup, and awaits both sender completion and database observation. "
                "started_at and finished_at describe the broader report lifecycle."
                if ingestion_end is not None
                else "No load started; total_seconds measures preflight and its cleanup."
            ),
        }
        report["throughput"] = {
            "ingestion_events_per_second": (
                events["accepted"] / ingestion_seconds if ingestion_seconds else 0.0
            ),
            "completed_events_per_second": len(completed_ids) / total_seconds,
            "completed_rate_window": rate_window,
        }
        report["request_latency_ms"] = latency_summary(request_latencies)
        report["db_observed_end_to_end_latency_ms"] = {
            **latency_summary(completion_latencies),
            "measurement": "polling_upper_bound",
            "poll_interval_seconds": config.poll_interval,
            "description": (
                "Client request start to first committed-row observation. Includes HTTP, queue, "
                "processing, polling delay and query time; not exact commit latency. "
                "Poll scheduling "
                "and query duration can add more than one configured poll interval."
            ),
        }
        report["environment"]["postgresql_version"] = getattr(observer, "server_version", None)
        return report

    try:
        if await poll(config.request_timeout):
            completion["reason"] = "run_id_collision"
    except Exception as error:
        completion["reason"] = "observer_error"
        completion["observer_errors"].append(type(error).__name__)
    finally:
        preflight_seconds = time.perf_counter() - preflight_started
    if completion["reason"]:
        return finish()

    started = time.perf_counter()
    next_index = 0

    async def sender():
        nonlocal next_index
        while next_index < request_count:
            index = next_index
            next_index += 1  # No await between acquiring an index and incrementing it.
            start_index = index * config.batch_size
            size = min(config.batch_size, config.events - start_index)
            payload = build_batch(
                size, run_id=config.run_id, start_index=start_index, users=config.users
            )
            request_started = time.perf_counter()
            sent_at.update((event["event_id"], request_started) for event in payload["events"])
            events["attempted"] += size
            requests["attempted"] += 1
            try:
                async with asyncio.timeout(config.request_timeout):
                    response = await client.post(
                        f"{config.url.rstrip('/')}/v1/events/batch",
                        json=payload,
                        headers={"x-api-key": config.api_key},
                        timeout=config.request_timeout,
                    )
                status_key = str(response.status_code)
                statuses = requests["http_status_counts"]
                statuses[status_key] = statuses.get(status_key, 0) + 1
                if response.status_code != 202:
                    requests["http_error"] += 1
                    events["http_error"] += size
                    continue
                try:
                    counts = response_counts(response, size)
                except (ValueError, TypeError):
                    requests["malformed_response"] += 1
                    events["malformed_response"] += size
                    continue
                requests["accepted"] += 1
                for name in ("accepted", "duplicates", "rejected"):
                    events[name] += counts[name]
            except (httpx.RequestError, TimeoutError):
                requests["network_failure"] += 1
                events["network_failure"] += size
            finally:
                request_latencies.append((time.perf_counter() - request_started) * 1000)

    async def observe_until_drained():
        while True:
            remaining = (
                ingestion_end + config.drain_timeout - time.perf_counter()
                if ingestion_end is not None
                else config.request_timeout
            )
            if remaining <= 0:
                completion["timed_out"] = True
                completion["reason"] = "drain_timeout"
                return
            try:
                rows = await poll(min(config.request_timeout, remaining))
            except Exception as error:
                completion["observer_errors"].append(type(error).__name__)
                completion["reason"] = "observer_error"
                return
            observed = time.perf_counter()
            for event_id in rows - completed_ids:
                if event_id in sent_at:
                    completed_ids.add(event_id)
                    completion_latencies.append((observed - sent_at[event_id]) * 1000)
            if len(completed_ids) == config.events:
                completion["all_requested_completed"] = True
                completion["reason"] = "completed"
                return
            delay = config.poll_interval
            if ingestion_end is not None:
                delay = min(delay, max(0, ingestion_end + config.drain_timeout - observed))
            await asyncio.sleep(delay)

    observer_task = asyncio.create_task(observe_until_drained())
    senders = [asyncio.create_task(sender()) for _ in range(min(config.concurrency, request_count))]
    try:
        await asyncio.gather(*senders)
        ingestion_end = time.perf_counter()
        try:
            async with asyncio.timeout(config.drain_timeout):
                await observer_task
        except TimeoutError:
            completion["timed_out"] = True
            completion["reason"] = "drain_timeout"
    finally:
        for task in [*senders, observer_task]:
            if not task.done():
                task.cancel()
        await asyncio.gather(*senders, observer_task, return_exceptions=True)
    return finish()


def parse_args(argv: list[str] | None = None) -> BenchmarkConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--users", type=int, default=10000)
    parser.add_argument("--run-id", default=uuid.uuid4().hex)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--request-timeout", type=float, default=60)
    parser.add_argument("--drain-timeout", type=float, default=120)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument(
        "--worker-count",
        type=int,
        default=None,
        help="Caller-supplied deployment metadata; not auto-detected",
    )
    parser.add_argument("--environment-label", default=None)
    parser.add_argument("--output", default=None, help="Also write the JSON report to this file")
    try:
        return BenchmarkConfig(
            **vars(parser.parse_args(argv)), api_key=os.getenv("BENCHMARK_API_KEY", "benchmark")
        )
    except ValueError as error:
        parser.error(str(error))


async def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    observer = PostgresObserver(config.database_url, config.request_timeout)
    try:
        async with httpx.AsyncClient(
            timeout=config.request_timeout,
            limits=httpx.Limits(
                max_connections=config.concurrency, max_keepalive_connections=config.concurrency
            ),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            report = await run_benchmark(config, client, observer)
    finally:
        await observer.close()
    serialized = json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if config.output:
        output = Path(config.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized)
    print(serialized, end="")
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
