"""Synthetic website fixtures, reachable only on the isolated demo network."""

import asyncio
import signal

from aiohttp import web


async def main() -> None:
    states = {"primary": "up", "secondary": "up"}
    capacity = asyncio.Semaphore(8)

    async def observe(request: web.Request) -> web.Response:
        alias = request.match_info["alias"]
        if alias not in states:
            raise web.HTTPNotFound
        if capacity.locked():
            raise web.HTTPServiceUnavailable
        async with capacity:
            state = states[alias]
            if state == "slow":
                await asyncio.sleep(3)
            if state == "down":
                return web.Response(status=503, text="Synthetic outage")
            return web.Response(text="READY")

    async def control(request: web.Request) -> web.Response:
        alias = request.match_info["alias"]
        if alias not in states:
            raise web.HTTPNotFound
        state = await request.text()
        if state not in {"up", "down", "slow"}:
            raise web.HTTPBadRequest
        states[alias] = state
        return web.Response(text=state)

    async def healthy(request: web.Request) -> web.Response:
        return web.Response(text="READY")

    app = web.Application(client_max_size=32)
    app.router.add_get("/health", healthy)
    app.router.add_get("/{alias}", observe)
    app.router.add_post("/control/{alias}", control)
    runner = web.AppRunner(
        app,
        access_log=None,
        handler_cancellation=True,
        shutdown_timeout=1,
        max_headers=32,
        max_line_size=2048,
        max_field_size=1024,
        keepalive_timeout=2,
    )
    await runner.setup()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    try:
        await web.TCPSite(runner, "0.0.0.0", 8080).start()
        await stopped.wait()
    finally:
        await runner.cleanup()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(main())
