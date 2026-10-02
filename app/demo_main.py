"""Free-tier live demo entrypoint.

This module exposes the same browser-facing contract as the production-oriented
Redis/PostgreSQL application, but uses an in-process asyncio queue and ephemeral
memory so the public portfolio demo can run on a zero-cost web service.

The production/reference path remains app.main + Redis Streams + PostgreSQL and
is runnable with Docker Compose.
"""

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=64)
    timestamp: datetime
    properties: dict[str, Any] = Field(default_factory=dict)


class EventOut(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    occurred_at: str
    received_at: str
    properties: dict[str, Any]


class UserProfileOut(BaseModel):
    user_id: str
    total_events: int
    purchase_count: int
    total_purchase_amount: str
    last_seen_at: str | None


queue: asyncio.Queue[EventIn] = asyncio.Queue()
events: list[EventOut] = []
seen_event_ids: set[str] = set()
profiles: dict[str, dict[str, Any]] = {}
worker_task: asyncio.Task[None] | None = None


async def process_events() -> None:
    while True:
        event = await queue.get()
        try:
            now = datetime.now(UTC)
            events.append(
                EventOut(
                    event_id=event.event_id,
                    user_id=event.user_id,
                    event_type=event.event_type,
                    occurred_at=event.timestamp.astimezone(UTC).isoformat(),
                    received_at=now.isoformat(),
                    properties=event.properties,
                )
            )
            profile = profiles.setdefault(
                event.user_id,
                {
                    "user_id": event.user_id,
                    "total_events": 0,
                    "purchase_count": 0,
                    "total_purchase_amount": Decimal("0"),
                    "last_seen_at": None,
                },
            )
            profile["total_events"] += 1
            if event.event_type == "purchase":
                profile["purchase_count"] += 1
                amount = event.properties.get("amount", 0)
                try:
                    profile["total_purchase_amount"] += Decimal(str(amount))
                except Exception:
                    pass
            profile["last_seen_at"] = event.timestamp.astimezone(UTC).isoformat()
        finally:
            queue.task_done()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task
    worker_task = asyncio.create_task(process_events())
    try:
        yield
    finally:
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="High Throughput Event Platform — Live Demo",
    version="1.0.0-demo",
    description=(
        "Zero-cost public demo using an in-process async queue and ephemeral memory. "
        "The repository's production/reference path uses Redis Streams and PostgreSQL."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": "high-throughput-event-platform-demo",
        "status": "ok",
        "mode": "free-tier-ephemeral-demo",
        "docs": "/docs",
    }


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok", "mode": "demo"}


@app.get("/health/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready", "queue": "asyncio", "storage": "ephemeral-memory"}


@app.post("/v1/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest(payload: EventIn, request: Request) -> dict[str, Any]:
    if payload.event_id in seen_event_ids:
        return {
            "event_id": payload.event_id,
            "accepted": False,
            "status": "duplicate",
            "request_id": request.headers.get("x-request-id"),
        }
    seen_event_ids.add(payload.event_id)
    await queue.put(payload)
    return {
        "event_id": payload.event_id,
        "accepted": True,
        "status": "queued",
        "request_id": request.headers.get("x-request-id") or str(uuid4()),
    }


@app.get("/v1/events/recent", response_model=list[EventOut])
async def recent(limit: int = Query(default=12, ge=1, le=100)) -> list[EventOut]:
    return list(reversed(events[-limit:]))


@app.get("/v1/users/{user_id}", response_model=UserProfileOut)
async def user_profile(user_id: str) -> UserProfileOut:
    profile = profiles.get(user_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="User not found")
    return UserProfileOut(
        user_id=profile["user_id"],
        total_events=profile["total_events"],
        purchase_count=profile["purchase_count"],
        total_purchase_amount=str(profile["total_purchase_amount"]),
        last_seen_at=profile["last_seen_at"],
    )


@app.get("/v1/analytics/summary")
async def analytics(hours: int = Query(default=24, ge=1, le=720)) -> dict[str, Any]:
    cutoff = datetime.now(UTC).timestamp() - (hours * 3600)
    recent_events = [
        event for event in events
        if datetime.fromisoformat(event.occurred_at).timestamp() >= cutoff
    ]
    counts = Counter(event.event_type for event in recent_events)
    return {
        "window_hours": hours,
        "total_events": len(recent_events),
        "total_users": len(profiles),
        "events_by_type": dict(counts),
    }
