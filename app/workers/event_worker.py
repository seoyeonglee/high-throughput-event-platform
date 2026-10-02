import asyncio
import logging
import os
import socket
import time

from pydantic import ValidationError
from redis.asyncio import Redis

from app.core.config import settings
from app.core.database import async_session_maker, close_database, init_database
from app.core.metrics import EVENTS_FAILED, EVENTS_PROCESSED, PROCESSING_SECONDS
from app.core.redis import close_redis, get_redis
from app.core.streams import ensure_consumer_group
from app.schemas.event import EventIn
from app.services.event_processor import process_event

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("event-worker")


def consumer_name() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def requeue_or_dlq(
    redis: Redis,
    message_id: str,
    fields: dict[str, str],
    error: Exception,
) -> None:
    retry_count = int(fields.get("retry_count", "0")) + 1
    payload = fields.get("payload", "")
    if retry_count <= settings.worker_max_retries:
        await redis.xadd(
            settings.redis_stream,
            {
                "payload": payload,
                "retry_count": retry_count,
                "previous_message_id": message_id,
            },
        )
        EVENTS_FAILED.labels(stage="retry").inc()
    else:
        await redis.xadd(
            settings.redis_dlq_stream,
            {
                "payload": payload,
                "retry_count": retry_count,
                "failed_message_id": message_id,
                "error": repr(error)[:1000],
            },
        )
        EVENTS_FAILED.labels(stage="dlq").inc()

    await redis.xack(settings.redis_stream, settings.redis_consumer_group, message_id)


async def handle_message(redis: Redis, message_id: str, fields: dict[str, str]) -> None:
    started = time.perf_counter()
    try:
        event = EventIn.model_validate_json(fields["payload"])
        async with async_session_maker() as session:
            await process_event(session, event)
        await redis.xack(settings.redis_stream, settings.redis_consumer_group, message_id)
        EVENTS_PROCESSED.labels(event_type=event.event_type).inc()
    except (ValidationError, KeyError) as exc:
        # Malformed payloads are not transient: send directly to DLQ.
        await redis.xadd(
            settings.redis_dlq_stream,
            {
                "payload": fields.get("payload", ""),
                "retry_count": fields.get("retry_count", "0"),
                "failed_message_id": message_id,
                "error": repr(exc)[:1000],
            },
        )
        await redis.xack(settings.redis_stream, settings.redis_consumer_group, message_id)
        EVENTS_FAILED.labels(stage="validation").inc()
    except Exception as exc:  # noqa: BLE001 - worker must isolate per-message failures
        logger.exception("event processing failed", extra={"message_id": message_id})
        await requeue_or_dlq(redis, message_id, fields, exc)
    finally:
        PROCESSING_SECONDS.observe(time.perf_counter() - started)


async def claim_stale_messages(redis: Redis, name: str) -> list[tuple[str, dict[str, str]]]:
    """Recover messages left pending by workers that died before ACKing them."""
    try:
        result = await redis.xautoclaim(
            name=settings.redis_stream,
            groupname=settings.redis_consumer_group,
            consumername=name,
            min_idle_time=settings.worker_claim_idle_ms,
            start_id="0-0",
            count=settings.worker_batch_size,
        )
    except Exception:  # noqa: BLE001 - compatibility/connection issues are logged by main loop
        logger.exception("failed to autoclaim stale messages")
        return []

    # redis-py returns (next_start_id, messages[, deleted_ids]) depending on Redis version.
    return result[1] if len(result) >= 2 else []


async def run_worker() -> None:
    await init_database()
    redis = await get_redis()
    await ensure_consumer_group(redis)
    name = consumer_name()
    logger.info("worker started consumer=%s", name)

    try:
        last_claim = 0.0
        while True:
            tasks = []
            now = time.monotonic()
            if now - last_claim >= 30:
                stale = await claim_stale_messages(redis, name)
                tasks.extend(
                    handle_message(redis, message_id, fields)
                    for message_id, fields in stale
                )
                last_claim = now

            messages = await redis.xreadgroup(
                groupname=settings.redis_consumer_group,
                consumername=name,
                streams={settings.redis_stream: ">"},
                count=settings.worker_batch_size,
                block=settings.worker_block_ms,
            )
            for _, stream_messages in messages or []:
                tasks.extend(
                    handle_message(redis, message_id, fields)
                    for message_id, fields in stream_messages
                )

            if tasks:
                await asyncio.gather(*tasks)
    finally:
        await close_redis()
        await close_database()


if __name__ == "__main__":
    asyncio.run(run_worker())
