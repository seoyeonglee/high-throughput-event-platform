from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class UserProfileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    total_events: int
    purchase_count: int
    total_purchase_amount: Decimal
    last_seen_at: datetime | None
