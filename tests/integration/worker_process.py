"""Subprocess entrypoint for real workers with test-only crash checkpoints."""

import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core import database
from app.core.config import settings
from app.models.event import Event
from app.models.user_profile import UserProfile


async def main() -> None:
    mode, schema, checkpoint = sys.argv[1:]
    # Only the DB namespace is changed. The normal mode runs the production worker.
    await database.engine.dispose()
    database.engine = create_async_engine(
        settings.database_url,
        connect_args={"server_settings": {"search_path": schema}, "command_timeout": 5},
    )
    database.async_session_maker = async_sessionmaker(database.engine, expire_on_commit=False)
    from app.workers import event_worker

    if mode != "normal":
        original = event_worker.process_event

        async def stop_at_checkpoint(session, event):
            if mode == "before_commit":

                async def stop_before_commit():
                    # The real processor has issued both SQL writes. Inspect
                    # this same uncommitted transaction before exposing the
                    # checkpoint, then let the parent kill the entire process.
                    rows = await session.scalar(select(func.count()).select_from(Event))
                    profile = await session.get(UserProfile, event.user_id)
                    Path(checkpoint).write_text(
                        json.dumps(
                            {
                                "event_id": event.event_id,
                                "event_rows": rows,
                                "profile_events": profile.total_events if profile else 0,
                                "in_transaction": session.in_transaction(),
                            }
                        )
                    )
                    await asyncio.Event().wait()

                session.commit = stop_before_commit
                await original(session, event)
            elif mode == "after_commit":
                await original(session, event)
                Path(checkpoint).write_text(event.event_id)
                await asyncio.Event().wait()
            else:
                raise ValueError(f"Unknown checkpoint {mode}")

        event_worker.process_event = stop_at_checkpoint
    await event_worker.run_worker()


if __name__ == "__main__":
    asyncio.run(main())
