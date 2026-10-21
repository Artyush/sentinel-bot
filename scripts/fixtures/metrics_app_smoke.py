"""Exercise the real metrics runtime against loopback exporter and email HTTP fixtures."""

from __future__ import annotations

import asyncio
import errno
import os
import signal
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from aiohttp import web
from demo.metrics_fixture import SyntheticNode

from sentinel_bot import email_runtime, metrics_runtime, runtime
from sentinel_bot.config import EmailSettings, FilesystemConfig, ServerConfig, Settings
from sentinel_bot.email_api import MailchimpEmailAPI
from sentinel_bot.metrics import MetricsChecker
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore
from sentinel_bot.telegram_commands import Command, render_command

KEY = "synthetic_metrics_email_key_1234567890"


async def until(
    condition: Callable[[], bool], app: asyncio.Task[None], timeout: float = 20
) -> None:
    async with asyncio.timeout(timeout):
        while not await asyncio.to_thread(condition):
            if app.done():
                await app
                raise AssertionError("The runtime stopped before the metrics condition")
            await asyncio.sleep(0.02)


async def scenario(
    *, verify_container: bool = True, state_dir: Path = Path("/var/lib/sentinel")
) -> None:
    if verify_container:
        assert os.geteuid() == 10001
        try:
            Path("/app/metrics-smoke-forbidden-write").touch()
        except OSError as error:
            assert error.errno == errno.EROFS
        else:
            raise AssertionError("The application root filesystem is writable")

    node = SyntheticNode()
    accepted: list[str] = []
    scrapes = 0

    @web.middleware
    async def count_scrapes(request: web.Request, handler):
        nonlocal scrapes
        if request.path == "/metrics":
            scrapes += 1
        return await handler(request)

    async def email(request: web.Request) -> web.Response:
        payload = await request.json()
        assert payload["key"] == KEY
        message = payload["message"]
        accepted.append(message["subject"])
        return web.json_response(
            [{"email": "operator@example.test", "status": "sent", "_id": str(len(accepted))}]
        )

    async def exporter(request: web.Request) -> web.Response:
        if node.state == "down":
            return web.Response(status=503)
        return web.Response(text=node.exposition(), content_type="text/plain")

    web_app = web.Application(client_max_size=16384)
    web_app.middlewares.append(count_scrapes)
    web_app.router.add_get("/metrics", exporter)
    web_app.router.add_post("/api/1.4/messages/send", email)
    runner = web.AppRunner(web_app, access_log=None, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
    providers: list[MailchimpEmailAPI] = []
    checkers: list[MetricsChecker] = []

    def email_factory(key: str, sender: str, **kwargs: object) -> MailchimpEmailAPI:
        api = MailchimpEmailAPI(key, sender, fixture_base_url=origin, **kwargs)
        providers.append(api)
        return api

    def checker_factory(
        max_concurrency: int, fixture_hosts: frozenset[str], **kwargs: object
    ) -> MetricsChecker:
        checker = MetricsChecker(max_concurrency, frozenset({"127.0.0.1"}), **kwargs)
        checkers.append(checker)
        return checker

    email_runtime.MailchimpEmailAPI = email_factory
    metrics_runtime.MetricsChecker = checker_factory
    settings = Settings(
        state_dir,
        mode="monitor",
        heartbeat_interval=0.1,
        health_timeout=2,
        email=EmailSettings("mailchimp", KEY, "sentinel@example.test", ("operator@example.test",)),
        servers=(
            ServerConfig(
                "node",
                origin + "/metrics",
                poll_interval=0.1,
                connect_timeout=0.1,
                total_timeout=0.3,
                mounts=(FilesystemConfig("root", "/"),),
            ),
        ),
    )

    def status() -> dict[str, object]:
        return status_snapshot(settings)

    def rule_states() -> dict[str, dict[str, object]]:
        return {row["name"]: row for row in status()["servers"][0]["rules"]}

    async def stop(app: asyncio.Task[None], *, sigterm: bool = False) -> None:
        if sigterm:
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            app.cancel()
        try:
            await asyncio.wait_for(app, 8)
        except asyncio.CancelledError:
            if sigterm:
                raise
        assert all(api._session is None and api._resolver is None for api in providers)
        assert all(not checker._connections for checker in checkers)

    app: asyncio.Task[None] | None = asyncio.create_task(runtime.run(settings))
    try:
        await until(lambda: runtime.healthcheck(settings), app)
        store = SQLiteStore(state_dir)
        await until(lambda: all(row["state"] == "UP" for row in rule_states().values()), app)
        assert accepted == []
        node.advance()
        node.state = "high"
        await until(lambda: store.email_delivery_status()["accepted"] == 3, app)
        opened = rule_states()
        identities = {name: opened[name]["incident_id"] for name in ("cpu", "memory", "fs_root")}
        assert all(opened[name]["state"] == "DOWN" for name in identities)
        assert opened["exporter"]["state"] == "UP"

        node.advance()
        node.state = "between"
        await until(lambda: status()["servers"][0]["memory_percent"] == 80, app)
        await asyncio.sleep(0.4)
        assert all(rule_states()[name]["state"] == "DOWN" for name in identities)
        assert len(accepted) == 3

        node.advance()
        node.state = "missing"
        await until(lambda: store.email_delivery_status()["accepted"] == 4, app)
        assert status()["servers"][0]["memory_percent"] is None
        assert rule_states()["memory"]["state"] == "DOWN"
        assert rule_states()["memory"]["fresh"] is False
        assert status()["servers"][0]["cpu_percent"] is not None

        node.advance()
        node.state = "down"
        await until(lambda: store.email_delivery_status()["accepted"] == 5, app)
        assert rule_states()["exporter"]["state"] == "DOWN"
        assert rule_states()["cpu"]["incident_id"] == identities["cpu"]
        assert status()["servers"][0]["cpu_percent"] is None
        assert runtime.healthcheck(settings)
        await stop(app)
        app = None
        assert status()["servers"][0]["fresh"] is False
        app = asyncio.create_task(runtime.run(settings))
        await until(lambda: runtime.healthcheck(settings), app)
        assert rule_states()["cpu"]["incident_id"] == identities["cpu"]
        assert len(accepted) == 5

        node.advance()
        node.state = "up"
        await until(lambda: store.email_delivery_status()["accepted"] == 8, app)
        assert all(row["state"] == "UP" for row in rule_states().values())
        before_commands = scrapes
        cached = status()
        listing = render_command(Command("servers"), cached, now=datetime.now(UTC))
        detail = render_command(Command("server", "node"), cached, now=datetime.now(UTC))
        unknown = render_command(Command("server", "not_configured"), cached, now=datetime.now(UTC))
        assert "node" in listing and "node" in detail and "Unknown server alias" in unknown
        assert scrapes == before_commands
        assert KEY not in repr(cached) and origin not in repr(cached)
        assert "/dev/synthetic" not in repr(cached) and "operator@example.test" not in repr(cached)

        node.advance()
        node.user_seconds = node.idle_seconds = 0
        await until(lambda: status()["servers"][0]["cpu_percent"] is None, app)
        await until(lambda: status()["servers"][0]["cpu_percent"] is not None, app)
        await asyncio.sleep(0.4)
        assert len(accepted) == 8 and store.email_pending_count() == 0
        await stop(app, sigterm=True)
        app = None
        assert not runtime.healthcheck(settings)
        print(
            "Metrics application smoke passed: real exposition/SQLite/email HTTP, CPU warmup/reset, "
            "resource hysteresis and unknown data, separate exporter incident, restart identity, "
            "cached commands without probes, private status, health and SIGTERM cleanup",
            flush=True,
        )
    finally:
        if app is not None:
            await stop(app)
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(scenario())
