import pytest

from app.core.config import settings
from app.workers.event_worker import requeue_or_dlq


class FakeRedis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[str, object]]] = []
        self.acked: list[str] = []

    async def xadd(self, stream: str, fields: dict[str, object]):
        self.added.append((stream, fields))

    async def xack(self, stream: str, group: str, message_id: str):
        self.acked.append(message_id)


@pytest.mark.asyncio
async def test_retry_before_dlq() -> None:
    redis = FakeRedis()
    await requeue_or_dlq(
        redis,  # type: ignore[arg-type]
        "1-0",
        {"payload": "{}", "retry_count": "0"},
        RuntimeError("boom"),
    )
    assert redis.added[0][0] == settings.redis_stream
    assert redis.acked == ["1-0"]


@pytest.mark.asyncio
async def test_dlq_after_max_retries() -> None:
    redis = FakeRedis()
    await requeue_or_dlq(
        redis,  # type: ignore[arg-type]
        "1-0",
        {"payload": "{}", "retry_count": str(settings.worker_max_retries)},
        RuntimeError("boom"),
    )
    assert redis.added[0][0] == settings.redis_dlq_stream
    assert redis.acked == ["1-0"]
