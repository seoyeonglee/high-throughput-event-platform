import pytest

from app.core.config import settings
from app.workers.event_worker import requeue_or_dlq


class FakeRedis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[str, object]]] = []
        self.acked: list[str] = []

    async def eval(self, script, numkeys, source, destination, group, message_id, *arguments):
        # Test double for routing/metadata only. Real Lua atomicity is covered by
        # the opt-in integration suite, including repeated and competing moves.
        fields = dict(zip(arguments[::2], arguments[1::2], strict=True))
        self.added.append((destination, fields))
        self.acked.append(message_id)
        return 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_count", ["not-a-number", "-1"])
async def test_invalid_retry_metadata_goes_to_dlq(retry_count: str) -> None:
    redis = FakeRedis()
    await requeue_or_dlq(
        redis,  # type: ignore[arg-type]
        "1-0",
        {"payload": "{}", "retry_count": retry_count},
        RuntimeError("boom"),
    )
    assert redis.added[0][0] == settings.redis_dlq_stream
    assert redis.acked == ["1-0"]


@pytest.mark.asyncio
async def test_reclaim_returns_scan_cursor_even_when_page_has_no_stale_messages() -> None:
    from app.workers.event_worker import claim_stale_messages

    class ClaimRedis:
        async def xautoclaim(self, **kwargs):
            return ["41-0", [], []]

    cursor, messages = await claim_stale_messages(ClaimRedis(), "replacement")
    assert cursor == "41-0"
    assert messages == []


@pytest.mark.asyncio
async def test_reclaim_continues_from_previous_cursor() -> None:
    from app.workers.event_worker import claim_stale_messages

    class ClaimRedis:
        async def xautoclaim(self, **kwargs):
            assert kwargs["start_id"] == "41-0"
            return ["0-0", [("42-0", {"payload": "{}"})], []]

    cursor, messages = await claim_stale_messages(ClaimRedis(), "replacement", "41-0")
    assert cursor == "0-0"
    assert messages == [("42-0", {"payload": "{}"})]
