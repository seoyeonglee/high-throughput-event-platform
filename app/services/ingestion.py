import asyncio

from redis.asyncio import Redis

from app.core.config import settings
from app.core.redis import get_redis
from app.schemas.event import EventIn

ENQUEUE_LUA = r"""
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], 'queued', 'EX', ARGV[1])
-- Lua errors do not roll back prior writes. Release only the marker this
-- invocation created when the append fails, so a caller can safely retry.
local appended = redis.pcall('XADD', KEYS[2], '*', 'payload', ARGV[2], 'retry_count', '0')
if type(appended) == 'table' and appended.err then
  redis.call('DEL', KEYS[1])
  return redis.error_reply(appended.err)
end
return 1
"""


class EventIngestionService:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    @classmethod
    async def create(cls) -> "EventIngestionService":
        return cls(await get_redis())

    async def enqueue(self, event: EventIn) -> bool:
        key = f"idempotency:{event.event_id}"
        result = await self.redis.eval(
            ENQUEUE_LUA,
            2,
            key,
            settings.redis_stream,
            settings.idempotency_ttl_seconds,
            event.model_dump_json(),
        )
        return bool(result)

    async def enqueue_batch(self, events: list[EventIn]) -> list[bool]:
        # Bounded concurrency prevents a large client batch from creating an unbounded
        # number of Redis round trips at once.
        semaphore = asyncio.Semaphore(100)

        async def submit(event: EventIn) -> bool:
            async with semaphore:
                return await self.enqueue(event)

        return list(await asyncio.gather(*(submit(event) for event in events)))
