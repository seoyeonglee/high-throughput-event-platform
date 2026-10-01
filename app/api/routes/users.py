from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.core.database import async_session_maker
from app.models.user_profile import UserProfile
from app.schemas.user import UserProfileOut

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/{user_id}", response_model=UserProfileOut)
async def get_user_profile(user_id: str) -> UserProfileOut:
    async with async_session_maker() as session:
        result = await session.execute(select(UserProfile).where(UserProfile.user_id == user_id))
        profile = result.scalar_one_or_none()
        if profile is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        return UserProfileOut.model_validate(profile)
