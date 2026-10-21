"""Run email-only ingress against a loopback Transactional HTTP fixture."""

from __future__ import annotations

import asyncio
import errno
import os
import signal
import socket
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aiohttp import ClientSession, web

from sentinel_bot import email_runtime, monitoring, runtime, telegram_runtime
from sentinel_bot.config import EmailSettings, IngressSettings, PushSource, Settings
from sentinel_bot.email_api import MailchimpEmailAPI
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import DATABASE_NAME, SQLiteStore

KEY = "synthetic_transactional_key_1234567890"
SOURCE_TOKEN = "synthetic_email_source_credential_1234567890"
SENDER = "sentinel@example.test"
RECIPIENTS = ("operator@example.test", "backup@example.test", "invalid@example.test")


@dataclass
class Fixture:
    accepted_recipient: str
    invalid_recipient: str
    retry_recipient: str
    retried: bool = False
    attempts: list[tuple[str, float]] = field(default_factory=list)
    accepted: list[str] = field(default_factory=list)

    async def send(self, request: web.Request) -> web.Response:
        payload = await request.json()
        assert payload["key"] == KEY and payload["async"] is False
        message = payload["message"]
        assert message["from_email"] == SENDER
        assert len(message["to"]) == 1 and message["to"][0]["type"] == "to"
        recipient = message["to"][0]["email"]
        assert recipient in RECIPIENTS
        assert "NOTIFICATION" in message["subject"]
        assert "Event time (UTC):" in message["text"]
        assert "Received time (UTC):" in message["text"]
        self.attempts.append((recipient, time.monotonic()))
        if recipient == self.invalid_recipient:
            return web.json_response([{"email": recipient, "status": "invalid"}])
        if recipient == self.retry_recipient and not self.retried:
            self.retried = True
            return web.json_response({}, status=429, headers={"Retry-After": "1"})
        self.accepted.append(recipient)
        return web.json_response(
            [
                {
                    "email": recipient,
                    "status": "queued",
                    "_id": "synthetic-" + str(len(self.accepted)),
                }
            ]
        )


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
                raise AssertionError("The application stopped before the email smoke condition")
            await asyncio.sleep(0.05)


async def stop(
    app: asyncio.Task[None], instances: list[MailchimpEmailAPI], *, sigterm: bool = False
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
            Path("/app/email-smoke-forbidden-write").touch()
        except OSError as error:
            assert error.errno == errno.EROFS
        else:
            raise AssertionError("The application root filesystem is writable")

    email = EmailSettings("mailchimp", KEY, SENDER, RECIPIENTS)
    routes = sorted(zip(email.destinations, RECIPIENTS, strict=True))
    fixture = Fixture(routes[0][1], routes[1][1], routes[2][1])
    web_app = web.Application()
    web_app.router.add_post("/api/1.4/messages/send", fixture.send)
    runner = web.AppRunner(web_app, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
    instances: list[MailchimpEmailAPI] = []

    def api_factory(key: str, sender: str, **kwargs: object) -> MailchimpEmailAPI:
        assert key == KEY and sender == SENDER
        api = MailchimpEmailAPI(key, sender, fixture_base_url=origin, **kwargs)
        instances.append(api)
        return api

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("Email-only push operation started a website checker or Telegram API")

    email_runtime.MailchimpEmailAPI = api_factory
    monitoring.HttpChecker = forbidden
    telegram_runtime.TelegramAPI = forbidden
    settings = Settings(
        state_dir,
        mode="monitor",
        heartbeat_interval=0.1,
        health_timeout=2,
        email=email,
        ingress=IngressSettings(port=unused_port()),
        sources=(PushSource("edge", frozenset({"service.health"}), SOURCE_TOKEN),),
    )
    assert settings.ingress is not None
    url = f"http://127.0.0.1:{settings.ingress.port}/api/v1/events"
    auth = {"Authorization": f"Bearer {SOURCE_TOKEN}"}
    notification = {
        "event_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "notification",
        "type": "service.health",
        "severity": "warning",
        "summary": "Synthetic independent recipient check.",
    }
    app: asyncio.Task[None] | None = asyncio.create_task(runtime.run(settings))
    try:
        await until(lambda: runtime.healthcheck(settings), app)
        store = SQLiteStore(state_dir)
        async with ClientSession(trust_env=False) as client:
            async with client.post(url, headers=auth, json=notification) as response:
                assert response.status == 202
                receipt = await response.json()
                assert receipt["outcome"] == "notification"
            await until(
                lambda: (
                    (progress := store.email_delivery_status())["accepted"] == 1
                    and progress["failed"] == 1
                    and progress["retrying"] == 1
                    and progress["in_flight"] == 0
                ),
                app,
            )
            status = store.email_delivery_status()
            assert status["accepted"] == 1 and status["failed"] == 1
            assert fixture.accepted == [fixture.accepted_recipient]
            assert len(fixture.attempts) == 3 and runtime.healthcheck(settings)
            await stop(app, instances)
            app = None

            app = asyncio.create_task(runtime.run(settings))
            await until(lambda: store.email_delivery_status()["accepted"] == 2, app)
            assert fixture.accepted == [fixture.accepted_recipient, fixture.retry_recipient]
            assert Counter(recipient for recipient, _ in fixture.attempts) == {
                fixture.accepted_recipient: 1,
                fixture.invalid_recipient: 1,
                fixture.retry_recipient: 2,
            }
            assert fixture.attempts[-1][1] - fixture.attempts[-2][1] >= 0.95
            assert store.email_delivery_status()["failed"] == 1
            assert store.email_pending_count() == 0
            async with client.post(url, headers=auth, json=notification) as response:
                assert response.status == 202 and await response.json() == receipt
            await asyncio.sleep(1.1)
            assert len(fixture.attempts) == 4
            snapshot = await asyncio.to_thread(status_snapshot, settings)
            assert snapshot["telegram"]["enabled"] is False
            assert snapshot["email"]["enabled"] is True
            assert snapshot["sites"] == [] and snapshot["sources"][0]["availability"] == "UNKNOWN"
            assert all(
                value not in repr(snapshot) for value in (KEY, SOURCE_TOKEN, SENDER, *RECIPIENTS)
            )
            await stop(app, instances, sigterm=True)
            app = None

        assert not runtime.healthcheck(settings)
        with closing(sqlite3.connect(state_dir / DATABASE_NAME)) as database:
            assert database.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (2,)
            assert database.execute("SELECT COUNT(*) FROM event_receipts").fetchone() == (1,)
        print(
            "Email provider smoke passed: email-only real ingress, per-recipient queued acceptance, "
            "permanent recipient failure, 429 retry across restart, no accepted resend, immutable "
            "duplicate, push-only isolation, private cached status, health and SIGTERM cleanup",
            flush=True,
        )
    finally:
        if app is not None:
            await stop(app, instances)
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(scenario())
