"""Run the real receiver and sender with synthetic loopback-only HTTP peers."""

import asyncio
import errno
import os
import signal
import socket
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aiohttp import ClientConnectionError, ClientSession, web

from sentinel_bot import monitoring, runtime, telegram_runtime
from sentinel_bot.config import IngressSettings, PushSource, Settings, TelegramSettings
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import DATABASE_NAME, SQLiteStore
from sentinel_bot.telegram_api import TelegramAPI

TOKEN = "123456:synthetic_test_credential_only_123456"
SOURCE_TOKEN = "synthetic_push_credential_only_1234567890"
CHAT = -1001234567890
USER = 123456789


@dataclass
class Fixture:
    startups: int = 0
    reject_firing: bool = True
    attempts: list[tuple[str, float]] = field(default_factory=list)
    accepted: list[str] = field(default_factory=list)

    async def bot(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        parameters = await request.json()
        if method == "getMe":
            self.startups += 1
            result: object = {"id": 123456, "is_bot": True, "username": "SyntheticBot"}
        elif method == "getWebhookInfo":
            result = {"url": ""}
        elif method == "getUpdates":
            await asyncio.sleep(0.05)
            result = []
        elif method == "sendMessage":
            assert parameters["chat_id"] == CHAT and "parse_mode" not in parameters
            assert parameters["link_preview_options"] == {"is_disabled": True}
            text = parameters["text"]
            self.attempts.append((text, time.monotonic()))
            if text.startswith("FIRING:") and self.reject_firing:
                self.reject_firing = False
                return web.json_response(
                    {"ok": False, "error_code": 429, "parameters": {"retry_after": 1}},
                    status=429,
                )
            self.accepted.append(text)
            result = {"message_id": len(self.accepted)}
        else:
            raise AssertionError("An unexpected Telegram API operation was requested")
        return web.json_response({"ok": True, "result": result})


def event(**changes: object) -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "notification",
        "type": "security.attack",
        "severity": "warning",
        "summary": "Synthetic source report.",
        **changes,
    }


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def until(
    condition: Callable[[], bool], app: asyncio.Task[None], timeout: float = 20
) -> None:
    async with asyncio.timeout(timeout):
        while not await asyncio.to_thread(condition):
            if app.done():
                await app
                raise AssertionError("The application stopped before the smoke condition")
            await asyncio.sleep(0.05)


async def stop(
    app: asyncio.Task[None], instances: list[TelegramAPI], *, sigterm: bool = False
) -> None:
    if sigterm:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(app, 8)
    else:
        app.cancel()
        try:
            await asyncio.wait_for(app, 8)
        except asyncio.CancelledError:
            pass
    assert all(api._session is None and api._resolver is None for api in instances)


async def scenario(
    *, verify_container: bool = True, state_dir: Path = Path("/var/lib/sentinel")
) -> None:
    if verify_container:
        assert os.geteuid() == 10001
        try:
            Path("/app/smoke-forbidden-write").touch()
        except OSError as error:
            assert error.errno == errno.EROFS
        else:
            raise AssertionError("The application root filesystem is writable")

    fixture = Fixture()
    web_app = web.Application()
    web_app.router.add_post(f"/bot{TOKEN}/{{method}}", fixture.bot)
    runner = web.AppRunner(web_app, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    provider_origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
    instances: list[TelegramAPI] = []

    def api_factory(token: str) -> TelegramAPI:
        assert token == TOKEN
        api = TelegramAPI(token, fixture_base_url=provider_origin)
        instances.append(api)
        return api

    def forbidden_checker(*args: object, **kwargs: object) -> None:
        raise AssertionError("A push-only source started an outbound website checker")

    telegram_runtime.TelegramAPI = api_factory
    monitoring.HttpChecker = forbidden_checker
    settings = Settings(
        state_dir,
        mode="monitor",
        heartbeat_interval=0.1,
        health_timeout=2,
        telegram=TelegramSettings(TOKEN, CHAT, frozenset({USER})),
        ingress=IngressSettings(port=unused_port(), source_burst=8),
        sources=(PushSource("edge", frozenset({"security.attack"}), SOURCE_TOKEN),),
    )
    assert settings.ingress is not None
    url = f"http://127.0.0.1:{settings.ingress.port}/api/v1/events"
    auth = {"Authorization": f"Bearer {SOURCE_TOKEN}"}
    app: asyncio.Task[None] | None = asyncio.create_task(runtime.run(settings))
    try:
        await until(lambda: fixture.startups == 1 and runtime.healthcheck(settings), app)
        store = SQLiteStore(state_dir)
        async with ClientSession(trust_env=False) as client:
            async with client.post(url, data=b"not JSON") as response:
                assert response.status == 401
                assert await response.json() == {"error": {"code": "unauthorized"}}

            opening = event(kind="firing", incident_key="attack-1")
            async with client.post(url, headers=auth, json=opening) as response:
                assert response.status == 202
                receipt = await response.json()
                assert receipt["outcome"] == "opened"
            async with client.post(url, headers=auth, json=opening) as response:
                assert response.status == 202 and await response.json() == receipt
            async with client.post(
                url, headers=auth, json={**opening, "summary": "Changed"}
            ) as response:
                assert response.status == 409

            tombstone = event(kind="resolved", incident_key="arrived-resolved")
            async with client.post(url, headers=auth, json=tombstone) as response:
                assert response.status == 202 and (await response.json())["outcome"] == "noop"
            late_opening = event(kind="firing", incident_key="arrived-resolved")
            async with client.post(url, headers=auth, json=late_opening) as response:
                assert response.status == 202 and (await response.json())["outcome"] == "noop"

            resolution = event(kind="resolved", incident_key="attack-1", severity="info")
            async with client.post(url, headers=auth, json=resolution) as response:
                assert response.status == 202 and (await response.json())["outcome"] == "resolved"
            notification = event(summary="Independent one-shot synthetic alert.")
            async with client.post(url, headers=auth, json=notification) as response:
                assert (
                    response.status == 202 and (await response.json())["outcome"] == "notification"
                )
            for _ in range(20):
                async with client.post(url, headers=auth, json={}) as response:
                    if response.status == 429:
                        assert 1 <= int(response.headers["Retry-After"]) <= 60
                        assert (await response.json())["error"]["code"] == "rate_limited"
                        break
                    assert response.status == 400
            else:
                raise AssertionError("The configured ingress request budget was not enforced")

            snapshot = await asyncio.to_thread(status_snapshot, settings)
            assert snapshot["sites"] == []
            assert snapshot["sources"][0]["availability"] == "UNKNOWN"
            assert snapshot["sources"][0]["active_incidents"] == 0
            await until(lambda: not fixture.reject_firing, app)
            await until(lambda: store.delivery_status()["retrying"] == 1, app)
            assert runtime.healthcheck(settings)
            await until(lambda: len(fixture.accepted) == 3 and store.pending_count() == 0, app)
            assert [text.split(":", 1)[0] for text in fixture.accepted] == [
                "FIRING",
                "RESOLVED",
                "NOTIFICATION",
            ]
            assert all("arrived-resolved" not in text for text in fixture.accepted)
            assert store.delivery_status()["accepted"] == 3
            assert fixture.attempts[1][1] - fixture.attempts[0][1] >= 3.05
            assert all(
                "Event time (UTC):" in text and "Received time (UTC):" in text
                for text in fixture.accepted
            )
            await stop(app, instances)
            app = None

            app = asyncio.create_task(runtime.run(settings))
            await until(lambda: fixture.startups == 2 and runtime.healthcheck(settings), app)
            async with client.post(url, headers=auth, json=opening) as response:
                assert response.status == 202 and await response.json() == receipt
            await asyncio.sleep(3.3)
            assert len(fixture.attempts) == 4 and len(fixture.accepted) == 3
            assert store.pending_count() == 0 and runtime.healthcheck(settings)
            await stop(app, instances, sigterm=True)
            app = None

        assert not runtime.healthcheck(settings)
        async with ClientSession(trust_env=False) as client:
            try:
                async with client.post(url, headers=auth, json=opening):
                    raise AssertionError("The incoming listener remained open after shutdown")
            except ClientConnectionError:
                pass
        with closing(sqlite3.connect(state_dir / DATABASE_NAME)) as database:
            assert database.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (2,)
            assert database.execute("SELECT COUNT(*) FROM event_receipts").fetchone() == (5,)
        print(
            "Incoming-events smoke passed: authenticated real HTTP receiver, durable duplicate "
            "and conflict handling, terminal no-reopen, request limits, ordered FIRING/RESOLVED "
            "and one-shot delivery, Telegram 429 retry, push-only isolation, healthy restart "
            "without accepted resend, SIGTERM and closed listener/sessions",
            flush=True,
        )
    finally:
        if app is not None:
            await stop(app, instances)
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(scenario())
