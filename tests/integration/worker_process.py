"""Subprocess entrypoint for real workers with test-only crash checkpoints."""

import asyncio
import sys
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core import database
from app.core.config import settings


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
            if mode == "after_commit":
                await original(session, event)
            elif mode != "before_commit":
                raise ValueError(f"Unknown checkpoint {mode}")
            Path(checkpoint).write_text(event.event_id)
            await asyncio.Event().wait()

        event_worker.process_event = stop_at_checkpoint
    await event_worker.run_worker()


if __name__ == "__main__":
    asyncio.run(main())
