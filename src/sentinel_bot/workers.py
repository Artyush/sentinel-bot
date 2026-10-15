"""Cancellation-safe execution of short blocking state operations."""

import asyncio
from collections.abc import Callable


async def run_blocking[Result](operation: Callable[[], Result]) -> Result:
    """Keep ownership until a started thread finishes, including on cancellation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancelling an await cannot stop SQLite; its state lock must remain held.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        task.result()
        raise
