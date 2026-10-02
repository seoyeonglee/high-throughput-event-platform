"""Opt-in integration fixtures; never flush a Redis DB or touch existing SQL tables."""

import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.models.base import Base
from app.schemas.event import EventIn
from app.workers import event_worker

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Stack:
    redis: Redis
    engine: AsyncEngine
    sessions: async_sessionmaker
    schema: str
    stream: str
    group: str
    dlq: str
    database_url: str
    redis_url: str
    directory: Path
    event_keys: set[str] = field(default_factory=set)
    processes: list[asyncio.subprocess.Process] = field(default_factory=list)
    logs: list[Path] = field(default_factory=list)

    def event(self, **overrides: Any) -> EventIn:
        values = {
            "event_id": f"{self.schema}_{uuid4().hex[:12]}",
            "user_id": f"{self.schema}_user",
            "event_type": "purchase",
            "timestamp": datetime.now(UTC),
            "properties": {"amount": "12.50"},
        }
        values.update(overrides)
        event = EventIn(**values)
        self.event_keys.add(f"idempotency:{event.event_id}")
        return event

    async def pending(self) -> int:
        return (await self.redis.xpending(self.stream, self.group))["pending"]

    async def read(self, count: int = 100, consumer: str = "test") -> list:
        response = await self.redis.xreadgroup(
            self.group, consumer, {self.stream: ">"}, count=count
        )
        return response[0][1] if response else []

    async def wait_for(
        self, predicate: Callable[[], Awaitable[bool]], description: str, timeout: float = 15
    ) -> None:
        try:
            async with asyncio.timeout(timeout):
                while not await predicate():
                    failed = [p.returncode for p in self.processes if p.returncode is not None]
                    if failed:
                        pytest.fail(
                            f"Worker exited before {description}: {failed}\n{self.log_text()}"
                        )
                    await asyncio.sleep(0.05)
        except TimeoutError:
            pytest.fail(f"Timed out waiting for {description}\n{self.log_text()}")

    def log_text(self) -> str:
        return "\n".join(path.read_text(errors="replace")[-8000:] for path in self.logs)

    async def start_worker(
        self, mode: str = "normal", **overrides: str
    ) -> asyncio.subprocess.Process:
        index = len(self.logs)
        ready = self.directory / f"worker-{index}.ready"
        log = self.directory / f"worker-{index}.log"
        environment = {
            **os.environ,
            "DATABASE_URL": self.database_url,
            "REDIS_URL": self.redis_url,
            "REDIS_STREAM": self.stream,
            "REDIS_CONSUMER_GROUP": self.group,
            "REDIS_DLQ_STREAM": self.dlq,
            "WORKER_BATCH_SIZE": "10",
            "WORKER_BLOCK_MS": "50",
            "WORKER_CLAIM_IDLE_MS": "150",
            "WORKER_CLAIM_INTERVAL_MS": "50",
            "WORKER_MAX_RETRIES": "2",
            "PYTHONPATH": str(ROOT),
            **overrides,
        }
        with log.open("wb") as output:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(Path(__file__).with_name("worker_process.py")),
                mode,
                self.schema,
                str(ready),
                cwd=ROOT,
                env=environment,
                stdout=output,
                stderr=asyncio.subprocess.STDOUT,
            )
        self.logs.append(log)
        self.processes.append(process)
        if mode != "normal":

            async def is_ready() -> bool:
                return ready.exists()

            await self.wait_for(is_ready, f"{mode} worker checkpoint")
        return process

    async def stop_worker(self, process: asyncio.subprocess.Process, *, kill: bool = False) -> None:
        if process.returncode is None:
            if kill:
                process.kill()
            else:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)
        self.processes.remove(process)


@pytest_asyncio.fixture
async def stack(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    missing = [name for name in ("DATABASE_URL", "REDIS_URL") if not os.environ.get(name)]
    if missing:
        pytest.fail(f"RUN_INTEGRATION=1 requires explicit {', '.join(missing)}")
    url = make_url(os.environ["DATABASE_URL"])
    if url.get_backend_name() != "postgresql":
        pytest.fail("Integration tests require an actual PostgreSQL DATABASE_URL")
    database_url = url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    redis_url = os.environ["REDIS_URL"]
    token = "it_" + uuid4().hex
    schema = token
    admin = create_async_engine(database_url, connect_args={"timeout": 5, "command_timeout": 5})
    engine = create_async_engine(
        database_url,
        connect_args={
            "server_settings": {"search_path": schema},
            "timeout": 5,
            "command_timeout": 5,
        },
    )
    redis = Redis.from_url(
        redis_url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5
    )
    state = Stack(
        redis=redis,
        engine=engine,
        sessions=async_sessionmaker(engine, expire_on_commit=False),
        schema=schema,
        stream=f"{token}:events",
        group=f"{token}:workers",
        dlq=f"{token}:dlq",
        database_url=database_url,
        redis_url=redis_url,
        directory=tmp_path,
    )
    created_schema = False
    try:
        await redis.ping()
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        created_schema = True
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await redis.xgroup_create(state.stream, state.group, id="0", mkstream=True)
        monkeypatch.setattr(settings, "redis_stream", state.stream)
        monkeypatch.setattr(settings, "redis_consumer_group", state.group)
        monkeypatch.setattr(settings, "redis_dlq_stream", state.dlq)
        monkeypatch.setattr(settings, "worker_claim_idle_ms", 100)
        monkeypatch.setattr(settings, "worker_batch_size", 1)
        monkeypatch.setattr(settings, "worker_max_retries", 2)
        monkeypatch.setattr(event_worker, "async_session_maker", state.sessions)
        yield state
    finally:
        for process in list(state.processes):
            await state.stop_worker(process)
        try:
            await redis.delete(state.stream, state.dlq, *state.event_keys)
        finally:
            await redis.aclose()
            await engine.dispose()
            try:
                if created_schema:
                    async with admin.begin() as connection:
                        await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            finally:
                await admin.dispose()
