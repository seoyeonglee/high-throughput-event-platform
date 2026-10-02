"""Real Redis/PostgreSQL boundaries, including actual worker process deaths."""

import asyncio
import os
import signal
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select, text

from app.api.routes.events import ingestion_service, router
from app.core.config import settings
from app.models.event import Event
from app.models.user_profile import UserProfile
from app.services.ingestion import EventIngestionService
from app.services.rate_limit import enforce_rate_limit
from app.workers.event_worker import handle_message, requeue_or_dlq

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="Set RUN_INTEGRATION=1"),
]


async def event_count(stack) -> int:
    async with stack.sessions() as session:
        return (await session.scalar(select(func.count()).select_from(Event))) or 0


async def assert_profile(stack, *, events: int, purchases: int, amount: str, last_seen=None):
    async with stack.sessions() as session:
        profiles = (await session.scalars(select(UserProfile))).all()
        assert len(profiles) == 1
        profile = profiles[0]
        assert profile.total_events == events
        assert profile.purchase_count == purchases
        assert profile.total_purchase_amount == Decimal(amount)
        if last_seen is not None:
            assert profile.last_seen_at == last_seen


async def enqueue_raw(stack, event, **fields):
    return await stack.redis.xadd(
        stack.stream, {"payload": event.model_dump_json(), "retry_count": "0", **fields}
    )


async def install_poison_constraint(stack):
    async with stack.engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE events ADD CONSTRAINT reject_poison CHECK (event_type <> 'poison')")
        )


async def test_concurrent_http_ingress_deduplicates_single_and_batch_requests(stack):
    application = FastAPI()
    application.include_router(router)
    service = EventIngestionService(stack.redis)
    application.dependency_overrides[ingestion_service] = lambda: service
    application.dependency_overrides[enforce_rate_limit] = lambda: None
    event = stack.event()
    payload = event.model_dump(mode="json")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://integration"
    ) as client:
        responses = await asyncio.gather(*(client.post("/events", json=payload) for _ in range(12)))
        assert all(response.status_code == 202 for response in responses)
        assert sum(response.json()["accepted"] for response in responses) == 1
        response = await client.post("/events/batch", json={"events": [payload, payload]})
        assert response.json() == {"received": 2, "accepted": 0, "duplicates": 2}
    assert await stack.redis.xlen(stack.stream) == 1
    message_id, fields = (await stack.read())[0]
    await handle_message(stack.redis, message_id, fields)
    assert await event_count(stack) == 1
    assert await stack.pending() == 0


async def test_duplicate_deliveries_update_aggregate_once_and_preserve_newest_timestamp(stack):
    newest = datetime.now(UTC)
    recent = stack.event(timestamp=newest, properties={"amount": "12.50"})
    older = stack.event(timestamp=newest - timedelta(days=2), properties={"amount": "7.25"})
    for event in [recent, recent, recent]:
        await enqueue_raw(stack, event)
    await asyncio.gather(
        *(handle_message(stack.redis, mid, fields) for mid, fields in await stack.read())
    )
    # Deliver the old event later so a last-writer-wins timestamp regression is observable.
    for event in [older, older]:
        await enqueue_raw(stack, event)
    await asyncio.gather(
        *(handle_message(stack.redis, mid, fields) for mid, fields in await stack.read())
    )
    assert await event_count(stack) == 2
    assert await stack.pending() == 0
    await assert_profile(stack, events=2, purchases=2, amount="19.75", last_seen=newest)


@pytest.mark.parametrize("checkpoint,committed", [("before_commit", 0), ("after_commit", 1)])
async def test_killed_worker_pending_entry_is_reclaimed_without_double_counting(
    stack, checkpoint, committed
):
    event = stack.event()
    await enqueue_raw(stack, event)
    victim = await stack.start_worker(checkpoint)
    assert await stack.pending() == 1
    assert await event_count(stack) == committed
    await stack.stop_worker(victim, kill=True)
    assert victim.returncode == -signal.SIGKILL
    assert await stack.pending() == 1
    await stack.start_worker()

    async def recovered():
        return await event_count(stack) == 1 and await stack.pending() == 0

    await stack.wait_for(recovered, "replacement worker to commit and ACK the reclaimed event")
    await assert_profile(stack, events=1, purchases=1, amount="12.50", last_seen=event.timestamp)
    assert await stack.redis.xlen(stack.dlq) == 0


@pytest.mark.parametrize("fields", [{"payload": "not-json"}, {"retry_count": "0"}])
async def test_malformed_payload_is_dead_lettered_and_acknowledged(stack, fields):
    await stack.redis.xadd(stack.stream, fields)
    message_id, message = (await stack.read())[0]
    await handle_message(stack.redis, message_id, message)
    dead = await stack.redis.xrange(stack.dlq)
    assert len(dead) == 1
    assert dead[0][1]["failed_message_id"] == message_id
    assert dead[0][1]["error"]
    assert await stack.pending() == 0
    assert await event_count(stack) == 0


async def test_poison_event_exhausts_retries_without_blocking_healthy_event(stack):
    await install_poison_constraint(stack)
    poison = stack.event(event_type="poison")
    healthy = stack.event()
    await enqueue_raw(stack, poison)
    await enqueue_raw(stack, healthy)
    await stack.start_worker()

    async def finished():
        return (
            await stack.redis.xlen(stack.dlq) == 1
            and await stack.pending() == 0
            and await event_count(stack) == 1
        )

    await stack.wait_for(finished, "poison event to exhaust its retry budget")
    entries = await stack.redis.xrange(stack.stream)
    attempts = [
        fields["retry_count"] for _, fields in entries if poison.event_id in fields["payload"]
    ]
    assert attempts == ["0", "1", "2"]
    dead = (await stack.redis.xrange(stack.dlq))[0][1]
    assert dead["retry_count"] == "3"
    assert "reject_poison" in dead["error"]
    await assert_profile(stack, events=1, purchases=1, amount="12.50")


@pytest.mark.parametrize("retry_count", ["broken", "-1"])
async def test_invalid_retry_metadata_cannot_strand_pending_poison_message(stack, retry_count):
    await install_poison_constraint(stack)
    await enqueue_raw(stack, stack.event(event_type="poison"), retry_count=retry_count)
    message_id, fields = (await stack.read())[0]
    await handle_message(stack.redis, message_id, fields)
    assert await stack.redis.xlen(stack.dlq) == 1
    assert await stack.redis.xlen(stack.stream) == 1
    assert await stack.pending() == 0


@pytest.mark.parametrize("destination", ["retry", "dlq", "validation"])
async def test_repeated_failure_handling_cannot_create_duplicate_retry_or_dlq_copies(
    stack, destination
):
    fields = {"payload": "not-json", "retry_count": "2" if destination == "dlq" else "0"}
    await stack.redis.xadd(stack.stream, fields)
    message_id, fields = (await stack.read())[0]

    async def fail_again():
        if destination == "validation":
            await handle_message(stack.redis, message_id, fields)
        else:
            await requeue_or_dlq(stack.redis, message_id, fields, RuntimeError("poison"))

    await asyncio.gather(fail_again(), fail_again())
    # Also model a lost Redis response: a client retries the completed move.
    await fail_again()
    assert await stack.pending() == 0
    assert await stack.redis.xlen(stack.stream) == (2 if destination == "retry" else 1)
    assert await stack.redis.xlen(stack.dlq) == (0 if destination == "retry" else 1)


async def test_failed_dead_letter_write_keeps_original_pending(stack):
    await stack.redis.set(stack.dlq, "wrong-type")
    fields = {"payload": "{}", "retry_count": str(settings.worker_max_retries)}
    await stack.redis.xadd(stack.stream, fields)
    message_id, fields = (await stack.read())[0]
    with pytest.raises(Exception, match="WRONGTYPE"):
        await requeue_or_dlq(stack.redis, message_id, fields, RuntimeError("poison"))
    assert await stack.pending() == 1
    await stack.redis.delete(stack.dlq)
    await requeue_or_dlq(stack.redis, message_id, fields, RuntimeError("poison"))
    assert await stack.pending() == 0
    assert await stack.redis.xlen(stack.dlq) == 1


async def test_worker_continues_reclaim_cursor_past_fresh_pending_prefix(stack):
    events = [stack.event() for _ in range(11)]
    ids = [await enqueue_raw(stack, event) for event in events]
    assert len(await stack.read()) == 11
    # XAUTOCLAIM examines at most COUNT * 10 entries. The first page is all fresh.
    await stack.redis.xclaim(stack.stream, stack.group, "dead-worker", 0, ids[-1:], idle=120_000)
    await stack.start_worker(WORKER_BATCH_SIZE="1", WORKER_CLAIM_IDLE_MS="60000")

    async def reached_stale_tail():
        async with stack.sessions() as session:
            return await session.get(Event, events[-1].event_id) is not None

    await stack.wait_for(
        reached_stale_tail, "reclaim scan to advance past ten fresh pending entries", timeout=5
    )
    assert await stack.pending() == 10
