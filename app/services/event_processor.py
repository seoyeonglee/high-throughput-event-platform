from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.event import Event
from app.models.user_profile import UserProfile
from app.schemas.event import EventIn


async def process_event(session: AsyncSession, event: EventIn) -> bool:
    event_stmt = (
        pg_insert(Event)
        .values(
            event_id=event.event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            occurred_at=event.timestamp,
            properties=event.properties,
        )
        .on_conflict_do_nothing(index_elements=[Event.event_id])
        .returning(Event.event_id)
    )
    inserted = (await session.execute(event_stmt)).scalar_one_or_none()
    if inserted is None:
        await session.commit()
        return False

    purchase_increment = 1 if event.event_type == "purchase" else 0
    amount_increment = Decimal("0")
    if purchase_increment:
        try:
            amount_increment = Decimal(str(event.properties.get("amount", 0)))
        except (InvalidOperation, ValueError):
            amount_increment = Decimal("0")

    profile_stmt = pg_insert(UserProfile).values(
        user_id=event.user_id,
        total_events=1,
        purchase_count=purchase_increment,
        total_purchase_amount=amount_increment,
        last_seen_at=event.timestamp,
    )
    profile_stmt = profile_stmt.on_conflict_do_update(
        index_elements=[UserProfile.user_id],
        set_={
            "total_events": UserProfile.total_events + 1,
            "purchase_count": UserProfile.purchase_count + purchase_increment,
            "total_purchase_amount": UserProfile.total_purchase_amount + amount_increment,
            "last_seen_at": func.greatest(UserProfile.last_seen_at, event.timestamp),
        },
    )
    await session.execute(profile_stmt)
    await session.commit()
    return True
