import asyncio
import socket
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import aiohttp
import pytest

import sentinel_bot.monitoring as monitoring
import sentinel_bot.runtime as runtime
import sentinel_bot.telegram_runtime as telegram
from sentinel_bot.config import IngressSettings, PushSource, Settings, TelegramSettings
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore
from sentinel_bot.telegram_api import BotIdentity, TelegramError, TelegramErrorKind
from sentinel_bot.telegram_commands import Command, render_command


class LocalTelegram:
    def __init__(self):
        self.accepted = []
        self.fail_next = True
        self.closed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed += 1

    async def startup(self):
        return BotIdentity(123456, "SentinelFixtureBot")

    async def get_updates(self, offset):
        await asyncio.sleep(0.01)
        return ()

    async def send_message(self, chat_id, text, *, topic_id=None):
        if self.fail_next:
            self.fail_next = False
            raise TelegramError(TelegramErrorKind.TRANSIENT, status=503)
        self.accepted.append(text)
        return len(self.accepted)


def local_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def configuration(tmp_path):
    return Settings(
        tmp_path,
        mode="monitor",
        heartbeat_interval=0.01,
        telegram=TelegramSettings("123456:" + "a" * 30, -100123456, frozenset({123})),
        ingress=IngressSettings(port=local_port()),
        sources=(PushSource("app", frozenset({"service.health"}), "s" * 32),),
    )


async def until(predicate, application):
    async with asyncio.timeout(5):
        while not predicate():
            if application.done():
                await application
                raise AssertionError("Application stopped before the expected state")
            await asyncio.sleep(0.01)


async def stop(application):
    application.cancel()
    with pytest.raises(asyncio.CancelledError):
        await application


def test_push_only_http_to_telegram_retry_order_restart_and_cached_status(tmp_path, monkeypatch):
    settings = configuration(tmp_path)
    provider = LocalTelegram()
    monkeypatch.setattr(telegram, "TelegramAPI", lambda _: provider)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0.01)
    monkeypatch.setattr(telegram, "_retry_delay", lambda *_: 0.01)

    def forbidden_probe(*args):
        raise AssertionError("Push-only operation must not construct a website checker")

    monkeypatch.setattr(monitoring, "HttpChecker", forbidden_probe)

    async def scenario():
        application = asyncio.create_task(runtime.run(settings))
        receipt = None
        try:
            await until(lambda: runtime.healthcheck(settings), application)
            url = f"http://127.0.0.1:{settings.ingress.port}/api/v1/events"
            payload = {
                "event_id": str(uuid4()),
                "occurred_at": datetime.now(UTC).isoformat(),
                "kind": "firing",
                "type": "service.health",
                "severity": "critical",
                "summary": "Synthetic worker stopped: проверка",
                "incident_key": "worker-one",
            }
            headers = {"Authorization": "Bearer " + settings.sources[0].token}
            async with aiohttp.ClientSession() as client:
                async with client.post(url, headers=headers, json=payload) as response:
                    assert response.status == 202
                    receipt = await response.json()
                resolution = {
                    **payload,
                    "event_id": str(uuid4()),
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "kind": "resolved",
                    "severity": "info",
                    "summary": "Synthetic worker recovered.",
                }
                async with client.post(url, headers=headers, json=resolution) as response:
                    assert response.status == 202
                one_shot = {
                    **payload,
                    "event_id": str(uuid4()),
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "kind": "notification",
                }
                del one_shot["incident_key"]
                async with client.post(url, headers=headers, json=one_shot) as response:
                    assert response.status == 202
                await until(lambda: len(provider.accepted) == 3, application)
                await until(lambda: SQLiteStore(tmp_path).pending_count() == 0, application)
                assert [text.split(":", 1)[0] for text in provider.accepted] == [
                    "FIRING",
                    "RESOLVED",
                    "NOTIFICATION",
                ]
                assert all("Received time (UTC)" in text for text in provider.accepted)
                assert "проверка" in provider.accepted[0]
                async with client.post(url, headers=headers, json=payload) as response:
                    assert response.status == 202 and await response.json() == receipt
            snapshot = status_snapshot(settings)
            assert snapshot["sites"] == []
            assert snapshot["telegram"]["accepted"] == 3
            source = snapshot["sources"][0]
            assert source["availability"] == "UNKNOWN"
            assert source["active_incidents"] == 0
            assert source["last_received_at"] is not None
            assert source["last_event_at"] is not None
            rendered = render_command(Command("status"), snapshot, now=datetime.now(UTC))
            assert "Push app: availability=UNKNOWN" in rendered
            assert settings.sources[0].token not in rendered
        finally:
            await stop(application)
        assert not runtime.healthcheck(settings)
        assert provider.closed == 1
        application = asyncio.create_task(runtime.run(settings))
        try:
            await until(lambda: runtime.healthcheck(settings), application)
            async with aiohttp.ClientSession() as client:
                async with client.post(url, headers=headers, json=payload) as response:
                    assert response.status == 202 and await response.json() == receipt
                await asyncio.sleep(0.05)
                assert len(provider.accepted) == 3
        finally:
            await stop(application)
        assert provider.closed == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("ingress_progress", [None, 900.0, float("nan"), True, 1000.0])
def test_ingress_health_requires_current_listener_progress(tmp_path, monkeypatch, ingress_progress):
    settings = replace(configuration(tmp_path), telegram=None)
    monkeypatch.setattr(runtime.time, "monotonic", lambda: 1000.0)
    progress = MonitoringProgress(ingress=ingress_progress)
    with runtime._InstanceGuard(tmp_path) as owner:
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        assert runtime.healthcheck(settings) is (ingress_progress == 1000.0)
