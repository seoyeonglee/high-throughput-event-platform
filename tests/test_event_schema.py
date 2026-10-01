from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.event import BatchIngestRequest, EventIn


def test_event_normalizes_timestamp_to_utc() -> None:
    event = EventIn(
        event_id="evt_1",
        user_id="user_1",
        event_type="product_view",
        timestamp=datetime(2026, 10, 2, 12, 0, tzinfo=UTC),
    )
    assert event.timestamp.tzinfo == UTC


def test_event_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        EventIn(
            event_id="evt_1",
            user_id="user_1",
            event_type="product_view",
            timestamp=datetime(2026, 10, 2, 12, 0),
        )


def test_batch_limit() -> None:
    event = EventIn(event_id="evt_1", user_id="u", event_type="view")
    payload = BatchIngestRequest(events=[event])
    assert len(payload.events) == 1
