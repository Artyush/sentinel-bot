import asyncio
import math
import socket
from collections import Counter, deque
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import aiohttp
import pytest

import sentinel_bot.email_runtime as email
import sentinel_bot.monitoring as monitoring
import sentinel_bot.runtime as runtime
import sentinel_bot.telegram_runtime as telegram
from sentinel_bot.config import (
    EmailSettings,
    IngressSettings,
    PushSource,
    Settings,
    SiteConfig,
    TelegramSettings,
)
from sentinel_bot.email_api import EmailError
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore, initialize_state
from sentinel_bot.telegram_api import BotIdentity, TelegramError, TelegramErrorKind

NOW = datetime(2026, 1, 1, tzinfo=UTC)
FIRST = "first@example.test"
SECOND = "second@example.test"


def prepare(tmp_path, *, recipients=(FIRST, SECOND)):
    site = SiteConfig(
        "website",
        "https://example.com",
        failure_confirmation=1,
        recovery_confirmation=1,
        reminder_interval=10,
    )
    settings = Settings(
        tmp_path,
        mode="monitor",
        sites=(site,),
        email=EmailSettings("mailchimp", "x" * 24, "sentinel@example.test", recipients),
    )
    initialize_state(tmp_path, NOW)
    store = SQLiteStore(tmp_path)
    store.configure_email(settings.email.destinations, NOW)
    store.reconcile(settings.sites)
    return settings, store


def observe(settings, store, success, seconds=0):
    store.apply_observation(
        settings.sites[0],
        Observation(
            NOW + timedelta(seconds=seconds),
            CheckResult(success, None if success else ErrorKind.CONNECTION),
        ),
    )


class Provider:
    def __init__(self, effects=None):
        self.effects = {key: deque(values) for key, values in (effects or {}).items()}
        self.calls = []
        self.accepted = []
        self.closed = 0
        self.started = None
        self.block = None
        self.on_send = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed += 1

    async def send(self, recipient, subject, body):
        self.calls.append((recipient, subject, body))
        if self.started is not None:
            self.started.set()
        if self.block is not None:
            await self.block.wait()
        if self.on_send is not None:
            self.on_send()
        effects = self.effects.get(recipient, ())
        result = effects.popleft() if effects else None
        if isinstance(result, BaseException):
            raise result
        self.accepted.append((recipient, subject, body))


def install(monkeypatch, provider):
    monkeypatch.setattr(email, "MailchimpEmailAPI", lambda *args, **kwargs: provider)
    monkeypatch.setattr(email, "SEND_INTERVAL", 0)


async def until(predicate, task):
    async with asyncio.timeout(4):
        while not predicate():
            if task.done():
                await task
                raise AssertionError("Email worker stopped before the expected state")
            await asyncio.sleep(0.001)


async def cancel(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_per_recipient_retry_does_not_resend_an_accepted_destination_after_restart(
    tmp_path, monkeypatch
):
    settings, store = prepare(tmp_path)
    observe(settings, store, False)
    provider = Provider({SECOND: [EmailError("server_error", True, retry_after=0.1)]})
    install(monkeypatch, provider)

    async def scenario():
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(
                lambda: (
                    len(provider.accepted) == 1
                    and any(
                        record.last_error == "server_error"
                        for record in store.pending_email_notifications()
                    )
                ),
                task,
            )
            assert [item[0] for item in provider.accepted] == [FIRST]
        finally:
            await cancel(task)
        saved = store.pending_email_notifications()[0]
        assert saved.attempts == 1 and saved.retry_delay_seconds == 0.1
        store.configure_email(settings.email.destinations, NOW)
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(lambda: store.email_pending_count() == 0, task)
        finally:
            await cancel(task)
        assert Counter(item[0] for item in provider.accepted) == {FIRST: 1, SECOND: 1}
        assert Counter(item[0] for item in provider.calls) == {FIRST: 1, SECOND: 2}
        assert provider.closed == 2
        assert store.pending_notifications() == ()

    asyncio.run(scenario())


def test_retry_exhaustion_blocks_only_that_recipients_recovery(tmp_path, monkeypatch):
    settings, store = prepare(tmp_path)
    observe(settings, store, False)
    observe(settings, store, True, 1)
    provider = Provider({FIRST: [EmailError("network", True, retry_after=0)] * 8})
    install(monkeypatch, provider)

    async def scenario():
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(lambda: store.email_pending_count() == 0, task)
        finally:
            await cancel(task)
        assert Counter(item[0] for item in provider.calls) == {FIRST: 8, SECOND: 2}
        assert [item[1].split()[1] for item in provider.accepted] == ["DOWN:", "RECOVERY:"]
        status = store.email_delivery_status()
        assert (status["failed"], status["blocked"], status["accepted"]) == (1, 1, 2)
        assert status["last_error"] == "retries_exhausted"

    asyncio.run(scenario())


def test_permanent_channel_error_preserves_pending_mail_until_reconfigured(tmp_path, monkeypatch):
    settings, store = prepare(tmp_path)
    observe(settings, store, False)
    provider = Provider({FIRST: [EmailError("unauthorized", False, channel_wide=True)]})
    install(monkeypatch, provider)

    async def scenario():
        progress = MonitoringProgress()
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), progress, asyncio.Lock())
        )
        try:
            await until(lambda: store.email_delivery_status()["paused"], task)
            await asyncio.sleep(0.03)
            assert len(provider.calls) == 1
            assert store.email_pending_count() == 2
            assert store.email_delivery_status()["failed"] == 0
            assert set(progress.email) == email.WORKER_NAMES
        finally:
            await cancel(task)
        store.configure_email(settings.email.destinations, NOW)
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(lambda: store.email_pending_count() == 0, task)
            assert not store.email_delivery_status()["paused"]
        finally:
            await cancel(task)
        assert Counter(item[0] for item in provider.accepted) == {FIRST: 1, SECOND: 1}

    asyncio.run(scenario())


def test_cancelled_external_send_retains_unknown_outcome_and_closes_provider(tmp_path, monkeypatch):
    settings, store = prepare(tmp_path, recipients=(FIRST,))
    observe(settings, store, False)
    provider = Provider()
    install(monkeypatch, provider)

    async def scenario():
        provider.started = asyncio.Event()
        provider.block = asyncio.Event()
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        await provider.started.wait()
        await cancel(task)
        interrupted = store.pending_email_notifications()[0]
        assert interrupted.in_flight and interrupted.attempts == 1
        assert provider.closed == 1
        store.configure_email(settings.email.destinations, NOW)
        restored = store.pending_email_notifications()[0]
        assert restored.id == interrupted.id
        assert restored.last_error == "interrupted" and restored.attempts == 1
        assert not restored.in_flight

    asyncio.run(scenario())


def test_unexpected_provider_programming_failure_remains_visible(tmp_path, monkeypatch):
    settings, store = prepare(tmp_path, recipients=(FIRST,))
    observe(settings, store, False)
    provider = Provider({FIRST: [RuntimeError("synthetic programming failure")]})
    install(monkeypatch, provider)

    async def scenario():
        with pytest.raises(ExceptionGroup) as failure:
            await email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        assert isinstance(failure.value.exceptions[0], RuntimeError)
        assert "synthetic programming failure" in str(failure.value.exceptions[0])
        assert provider.closed == 1
        assert store.pending_email_notifications()[0].in_flight

    asyncio.run(scenario())


@pytest.mark.parametrize("wall_shift", [-86400, 86400])
def test_restored_retry_and_shared_pause_use_parallel_monotonic_deadlines(
    tmp_path, monkeypatch, wall_shift
):
    settings, store = prepare(tmp_path, recipients=(FIRST,))
    observe(settings, store, False)
    record = store.reserve_due_email_delivery(NOW)
    store.retry_email_delivery(
        record.id, NOW + timedelta(seconds=wall_shift), "rate_limit", delay_seconds=7
    )
    store.set_email_pause(NOW + timedelta(seconds=wall_shift), 10)
    clock = [100.0]
    provider = Provider()
    install(monkeypatch, provider)
    monkeypatch.setattr(email, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    async def scenario():
        stopped = asyncio.Event()
        provider.on_send = stopped.set

        async def advance(seconds, stop, progress, worker="delivery"):
            assert not provider.calls
            clock[0] += seconds

        monkeypatch.setattr(email, "_pause", advance)
        await email._deliver(
            provider, settings, store, asyncio.Lock(), stopped, MonitoringProgress(), {}
        )
        assert math.isclose(clock[0], 110)
        assert len(provider.accepted) == 1
        assert store.get_email_pause() is None
        assert store.email_pending_count() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("fresh", [False, True])
@pytest.mark.parametrize("wall_shift", [-86400, 86400])
def test_reminder_timers_are_recipient_specific_and_ignore_wall_clock_jumps(
    tmp_path, monkeypatch, fresh, wall_shift
):
    settings, store = prepare(tmp_path)
    observe(settings, store, False)
    first = store.reserve_due_email_delivery(NOW)
    store.acknowledge_email(first.id, NOW)
    clock = [100.0]
    monkeypatch.setattr(email, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    class WallClock:
        @staticmethod
        def now(zone):
            return NOW + timedelta(seconds=wall_shift)

    monkeypatch.setattr(email, "datetime", WallClock)

    async def scenario():
        stopped = asyncio.Event()
        deadlines = {}
        progress = MonitoringProgress(observations={"website": 100.0} if fresh else {})
        phases = [0]

        async def advance(seconds, stop, state, worker="delivery"):
            phases[0] += 1
            if phases[0] == 1:
                assert deadlines[("website", first.destination)][2] == 110
                second = store.reserve_due_email_delivery(NOW)
                store.acknowledge_email(second.id, NOW + timedelta(seconds=3))
                clock[0] = 105
            elif phases[0] == 2:
                assert len(deadlines) == 2
                assert sorted(value[2] for value in deadlines.values()) == [110, 115]
                clock[0] = 110
            else:
                stopped.set()

        monkeypatch.setattr(email, "_pause", advance)
        await email._reminders(settings, store, asyncio.Lock(), stopped, progress, deadlines)
        pending = store.pending_email_notifications()
        assert len(pending) == int(fresh)
        if fresh:
            assert pending[0].event.kind is EventKind.REMINDER
            assert pending[0].destination == first.destination

    asyncio.run(scenario())


def test_channel_cooldown_still_applies_when_last_attempt_is_exhausted(tmp_path, monkeypatch):
    settings, store = prepare(tmp_path, recipients=(FIRST,))
    observe(settings, store, False)
    for _ in range(7):
        record = store.reserve_due_email_delivery(NOW)
        store.retry_email_delivery(record.id, NOW, "network", delay_seconds=0)
    provider = Provider(
        {FIRST: [EmailError("rate_limit", True, retry_after=10, channel_wide=True)]}
    )
    install(monkeypatch, provider)

    async def scenario():
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(lambda: store.get_email_pause() is not None, task)
            assert store.email_delivery_status()["failed"] == 1
            assert store.get_email_pause()[1] == 10
        finally:
            await cancel(task)

    asyncio.run(scenario())


@pytest.mark.parametrize("retry_after", [float("nan"), float("inf"), -1, 604801])
def test_invalid_transport_retry_delay_cannot_create_unbounded_retry(
    tmp_path, monkeypatch, retry_after
):
    settings, store = prepare(tmp_path, recipients=(FIRST,))
    observe(settings, store, False)
    provider = Provider({FIRST: [EmailError("invalid_response", True, retry_after=retry_after)]})
    install(monkeypatch, provider)

    async def scenario():
        task = asyncio.create_task(
            email.run_email(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())
        )
        try:
            await until(lambda: store.email_delivery_status()["failed"] == 1, task)
            assert len(provider.calls) == 1
            assert store.email_pending_count() == 0
        finally:
            await cancel(task)

    asyncio.run(scenario())


@pytest.mark.parametrize("outage", ["telegram", "email"])
def test_real_application_keeps_other_channel_checks_and_ingress_running(
    tmp_path, monkeypatch, outage
):
    settings, store = prepare(tmp_path)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    source = PushSource("gateway", frozenset({"service.health"}), "s" * 32)
    settings = replace(
        settings,
        heartbeat_interval=0.01,
        sites=(replace(settings.sites[0], poll_interval=0.01),),
        telegram=TelegramSettings("123456:" + "x" * 30, -100123, frozenset({1})),
        ingress=IngressSettings(port=port),
        sources=(source,),
    )
    effects = (
        {
            FIRST: [EmailError("unauthorized", False, channel_wide=True)],
            SECOND: [EmailError("unauthorized", False, channel_wide=True)],
        }
        if outage == "email"
        else {}
    )
    provider = Provider(effects)
    install(monkeypatch, provider)

    class Telegram:
        def __init__(self):
            self.broken = outage == "telegram"
            self.accepted = []
            self.closed = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed += 1

        async def startup(self):
            if self.broken:
                raise TelegramError(TelegramErrorKind.TRANSIENT)
            return BotIdentity(123456, "SentinelFixtureBot")

        async def get_updates(self, offset):
            await asyncio.sleep(0.01)
            return ()

        async def send_message(self, chat_id, text, *, topic_id=None):
            self.accepted.append(text)
            return len(self.accepted)

    class Checker:
        calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def check(self, site):
            assert site.alias == "website"
            self.calls += 1
            return CheckResult(True, http_status=200)

    bot, checker = Telegram(), Checker()
    monkeypatch.setattr(telegram, "TelegramAPI", lambda _: bot)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)
    monkeypatch.setattr(monitoring, "HttpChecker", lambda *args: checker)

    async def scenario():
        task = asyncio.create_task(runtime.run(settings))
        url = f"http://127.0.0.1:{port}/api/v1/events"
        payload = {
            "event_id": str(uuid4()),
            "occurred_at": datetime.now(UTC).isoformat(),
            "kind": "notification",
            "type": "service.health",
            "severity": "warning",
            "summary": "Synthetic independent channel test.",
        }
        try:
            await until(lambda: runtime.healthcheck(settings) and checker.calls > 1, task)
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    url, headers={"Authorization": "Bearer " + source.token}, json=payload
                ) as response:
                    assert response.status == 202
                    receipt = await response.json()
            if outage == "email":
                await until(
                    lambda: bool(bot.accepted) and store.email_delivery_status()["paused"], task
                )
                assert len(provider.calls) == 1
                assert store.email_pending_count() == 2
            else:
                await until(lambda: len(provider.accepted) == 2, task)
                assert not bot.accepted
                assert len(store.pending_notifications()) == 1
            assert runtime.healthcheck(settings)
            assert checker.calls > 2
        finally:
            await cancel(task)
        assert provider.closed == bot.closed == 1
        provider.effects.clear()
        bot.broken = False
        task = asyncio.create_task(runtime.run(settings))
        try:
            await until(lambda: len(provider.accepted) == 2 and len(bot.accepted) == 1, task)
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    url, headers={"Authorization": "Bearer " + source.token}, json=payload
                ) as response:
                    assert response.status == 202 and await response.json() == receipt
            snapshot = status_snapshot(settings)
            assert snapshot["email"]["accepted"] == 2
            assert len(snapshot["email"]["recipients"]) == 2
            assert snapshot["sources"][0]["availability"] == "UNKNOWN"
            assert all(
                private not in repr(snapshot)
                for private in (FIRST, SECOND, settings.email.api_key, source.token)
            )
        finally:
            await cancel(task)
        assert provider.closed == bot.closed == 2
        assert Counter(item[0] for item in provider.accepted) == {FIRST: 1, SECOND: 1}

    asyncio.run(scenario())
