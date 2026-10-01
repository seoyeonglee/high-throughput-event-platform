from pydantic import BaseModel


class AnalyticsSummary(BaseModel):
    window_hours: int
    total_events: int
    total_users: int
    events_by_type: dict[str, int]
