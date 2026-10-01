from datetime import UTC, datetime

import pytest

from app.schemas.event import EventIn
from app.services.ingestion import EventIngestionService


class FakeRedis:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.stream: list[str] = []

    async def eval(self, script: str, numkeys: int, key: str, stream: str, ttl: int, payload: str):
        assert script
        assert numkeys == 2
        assert ttl > 0
        if key in self.keys:
            return 0
        self.keys.add(key)
        self.stream.append(payload)
        return 1


@pytest.mark.asyncio
async def test_enqueue_is_idempotent() -> None:
    redis = FakeRedis()
    service = EventIngestionService(redis)  # type: ignore[arg-type]
    event = EventIn(
        event_id="evt_1",
        user_id="user_1",
        event_type="purchase",
        timestamp=datetime.now(UTC),
        properties={"amount": 1000},
    )

    assert await service.enqueue(event) is True
    assert await service.enqueue(event) is False
    assert len(redis.stream) == 1
