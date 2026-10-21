"""Run only inside the disposable smoke container; all HTTP stays on loopback."""

import asyncio
import errno
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import web

from sentinel_bot import monitoring, runtime, telegram_runtime
from sentinel_bot.config import Settings, SiteConfig, TelegramSettings
from sentinel_bot.http import HttpChecker
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import DATABASE_NAME, SQLiteStore
from sentinel_bot.telegram_api import TelegramAPI

TOKEN = "123456:synthetic_test_credential_only_123456"
CHAT = -1001234567890
USER = 123456789


@dataclass
class Fixture:
    up: bool = True
    reject_down: bool = True
    startups: int = 0
    offsets: list[int] = field(default_factory=list)
    updates: list[dict[str, object]] = field(default_factory=list)
    attempts: list[tuple[str, float]] = field(default_factory=list)
    accepted: list[str] = field(default_factory=list)

    def command(self, identity: int, user: int) -> None:
        self.updates.append(
            {
                "update_id": identity,
                "message": {
                    "chat": {"id": CHAT, "type": "supergroup"},
                    "from": {"id": user, "is_bot": False},
                    "text": "/status",
                },
            }
        )

    async def site(self, request: web.Request) -> web.Response:
        return web.Response(
            text="READY" if self.up else "unavailable", status=200 if self.up else 503
        )

    async def bot(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        parameters = await request.json()
        if method == "getMe":
            self.startups += 1
            result: object = {"id": 123456, "is_bot": True, "username": "SyntheticBot"}
        elif method == "getWebhookInfo":
            result = {"url": ""}
        elif method == "getUpdates":
            assert parameters["allowed_updates"] == ["message"] and parameters["limit"] == 20
            offset = parameters.get("offset", 0)
            self.offsets.append(offset)
            await asyncio.sleep(0.05)
            result = [update for update in self.updates if update["update_id"] >= offset]
        elif method == "sendMessage":
            assert parameters["chat_id"] == CHAT and "parse_mode" not in parameters
            assert parameters["link_preview_options"] == {"is_disabled": True}
            text = parameters["text"]
            self.attempts.append((text, time.monotonic()))
            if text.startswith("DOWN:") and self.reject_down:
                self.reject_down = False
                return web.json_response(
                    {"ok": False, "error_code": 429, "parameters": {"retry_after": 1}}, status=429
                )
            self.accepted.append(text)
            result = {"message_id": len(self.accepted)}
        else:
            raise AssertionError("The application used an unexpected Telegram method")
        return web.json_response({"ok": True, "result": result})


async def until(
    condition: Callable[[], bool], app: asyncio.Task[None], timeout: float = 15
) -> None:
    async with asyncio.timeout(timeout):
        while not await asyncio.to_thread(condition):
            if app.done():
                await app
                raise AssertionError("The application stopped before the smoke condition")
            await asyncio.sleep(0.05)


async def stop(app: asyncio.Task[None], instances: list[TelegramAPI]) -> None:
    app.cancel()
    try:
        await asyncio.wait_for(app, 8)
    except asyncio.CancelledError:
        pass
    assert all(api._session is None and api._resolver is None for api in instances)


async def scenario() -> None:
    assert os.geteuid() == 10001
    try:
        Path("/app/smoke-forbidden-write").touch()
    except OSError as error:
        assert error.errno == errno.EROFS
    else:
        raise AssertionError("The application root filesystem is writable")

    fixture = Fixture()
    web_app = web.Application()
    web_app.router.add_get("/site", fixture.site)
    web_app.router.add_post(f"/bot{TOKEN}/{{method}}", fixture.bot)
    runner = web.AppRunner(web_app, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
    instances: list[TelegramAPI] = []

    def api_factory(token: str) -> TelegramAPI:
        assert token == TOKEN
        api = TelegramAPI(token, fixture_base_url=origin)
        instances.append(api)
        return api

    def checker_factory(
        max_concurrency: int,
        fixture_hosts: frozenset[str],
        *,
        semaphore: asyncio.Semaphore | None = None,
    ) -> HttpChecker:
        return HttpChecker(max_concurrency, frozenset({"127.0.0.1"}), semaphore=semaphore)

    telegram_runtime.TelegramAPI = api_factory
    monitoring.HttpChecker = checker_factory
    settings = Settings(
        Path("/var/lib/sentinel"),
        mode="monitor",
        heartbeat_interval=0.1,
        health_timeout=2,
        sites=(
            SiteConfig(
                "primary",
                origin + "/site",
                marker="READY",
                poll_interval=0.2,
                connect_timeout=0.2,
                total_timeout=0.5,
            ),
        ),
        telegram=TelegramSettings(TOKEN, CHAT, frozenset({USER})),
    )
    app: asyncio.Task[None] | None = asyncio.create_task(runtime.run(settings))
    try:
        await until(lambda: bool(fixture.offsets) and runtime.healthcheck(settings), app)
        store = SQLiteStore(settings.state_dir)
        await until(lambda: store.read_site("primary").state.confirmed.value == "UP", app)
        fixture.command(1, USER + 1)
        await until(lambda: 2 in fixture.offsets, app)
        await asyncio.sleep(3.3)
        assert fixture.attempts == [], "Startup or an unauthorized user caused a send"
        fixture.command(2, USER)
        await until(lambda: len(fixture.accepted) == 1, app)
        assert "Sentinel status" in fixture.accepted[0] and "primary: UP" in fixture.accepted[0]

        fixture.up = False
        await until(lambda: not fixture.reject_down, app)
        await until(lambda: store.delivery_status()["retrying"] == 1, app)
        assert runtime.healthcheck(settings)
        incident = store.read_site("primary").state.incident_id
        await until(lambda: len(fixture.accepted) == 2 and store.pending_count() == 0, app)
        assert fixture.accepted[-1].startswith("DOWN:")
        assert store.delivery_status()["accepted"] == 1
        assert fixture.attempts[-1][1] - fixture.attempts[-2][1] >= 3.05
        await stop(app, instances)
        app = None

        app = asyncio.create_task(runtime.run(settings))
        await until(lambda: fixture.startups == 2 and runtime.healthcheck(settings), app)
        await until(lambda: status_snapshot(settings)["sites"][0]["fresh"], app)
        assert store.read_site("primary").state.incident_id == incident
        await asyncio.sleep(3.3)
        assert len(fixture.accepted) == 2, "Restart resent an accepted DOWN"
        fixture.up = True
        await until(lambda: len(fixture.accepted) == 3 and store.pending_count() == 0, app)
        assert fixture.accepted[-1].startswith("RECOVERY:")
        assert f"Incident: {incident}" in fixture.accepted[-2]
        assert f"Incident: {incident}" in fixture.accepted[-1]
        assert store.delivery_status()["accepted"] == 2 and runtime.healthcheck(settings)
        await stop(app, instances)
        app = None
        with closing(sqlite3.connect(settings.state_dir / DATABASE_NAME)) as database:
            assert database.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (2,)
        print(
            "Telegram smoke passed: real HTTP adapter and runtime, authorized cached command, "
            "429 retry, ordered DOWN/RECOVERY, restart without accepted resend, "
            "closed sessions, non-root/read-only and persistent state",
            flush=True,
        )
    finally:
        if app is not None:
            await stop(app, instances)
        await runner.cleanup()


asyncio.run(scenario())
