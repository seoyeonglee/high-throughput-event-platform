from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from app.core.database import async_session_maker
from app.models.event import Event
from app.models.user_profile import UserProfile
from app.schemas.analytics import AnalyticsSummary

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/summary", response_model=AnalyticsSummary)
async def summary(hours: int = Query(default=24, ge=1, le=24 * 30)) -> AnalyticsSummary:
    since = datetime.now(UTC) - timedelta(hours=hours)
    async with async_session_maker() as session:
        events_stmt = select(func.count()).select_from(Event).where(Event.occurred_at >= since)
        users_stmt = select(func.count()).select_from(UserProfile)
        type_stmt = (
            select(Event.event_type, func.count(Event.event_id))
            .where(Event.occurred_at >= since)
            .group_by(Event.event_type)
            .order_by(func.count(Event.event_id).desc())
        )
        total_events = (await session.execute(events_stmt)).scalar_one()
        total_users = (await session.execute(users_stmt)).scalar_one()
        rows = (await session.execute(type_stmt)).all()
    return AnalyticsSummary(
        window_hours=hours,
        total_events=total_events,
        total_users=total_users,
        events_by_type={event_type: count for event_type, count in rows},
    )
