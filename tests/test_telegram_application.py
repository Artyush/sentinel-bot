import asyncio
import json
import os
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

import sentinel_bot.__main__ as cli
import sentinel_bot.monitoring as monitoring
import sentinel_bot.runtime as runtime
import sentinel_bot.telegram_runtime as telegram
from sentinel_bot.config import Settings, SiteConfig, TelegramSettings
from sentinel_bot.models import CheckResult, ErrorKind, Observation, SiteState
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore, initialize_state
from sentinel_bot.telegram_api import BotIdentity, TelegramError, TelegramErrorKind


class FakeAPI:
    def __init__(self) -> None:
        self.startup_error: TelegramError | None = None
        self.poll_error: Exception | None = None
        self.send_error: TelegramError | None = None
        self.startups = 0
        self.polls = 0
        self.sends = 0
        self.closed = False

    async def __aenter__(self) -> FakeAPI:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def startup(self) -> BotIdentity:
        self.startups += 1
        await asyncio.sleep(0)
        if self.startup_error is not None:
            raise self.startup_error
        return BotIdentity(123456, "SentinelFixtureBot")

    async def get_updates(self, offset: int | None) -> tuple[dict[str, object], ...]:
        self.polls += 1
        await asyncio.sleep(0.01)
        if self.poll_error is not None:
            raise self.poll_error
        return ()

    async def send_message(self, chat_id: int, text: str, *, topic_id: int | None = None) -> int:
        self.sends += 1
        await asyncio.sleep(0)
        if self.send_error is not None:
            raise self.send_error
        return self.sends


class FakeChecker:
    def __init__(self, failing: frozenset[str] = frozenset()) -> None:
        self.failing = failing
        self.calls: dict[str, int] = {}

    async def __aenter__(self) -> FakeChecker:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def check(self, site: SiteConfig) -> CheckResult:
        self.calls[site.alias] = self.calls.get(site.alias, 0) + 1
        await asyncio.sleep(0)
        failed = site.alias in self.failing
        return CheckResult(not failed, ErrorKind.TIMEOUT if failed else None)


def settings_for(tmp_path: Path, mode: str = "idle") -> Settings:
    sites = ()
    if mode == "monitor":
        sites = (
            SiteConfig(
                "website",
                "https://example.test/",
                poll_interval=0.02,
                connect_timeout=0.1,
                total_timeout=0.2,
                failure_confirmation=1,
                recovery_confirmation=1,
            ),
        )
    return Settings(
        tmp_path,
        mode=mode,
        heartbeat_interval=0.01,
        sites=sites,
        telegram=TelegramSettings("123456:" + "a" * 30, -100123456, frozenset({123})),
    )


def install_fakes(monkeypatch: pytest.MonkeyPatch, api: FakeAPI, checker: FakeChecker) -> None:
    monkeypatch.setattr(telegram, "TelegramAPI", lambda token: api)
    monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)


async def wait_until(predicate: Callable[[], bool], task: asyncio.Task[None]) -> None:
    async with asyncio.timeout(4):
        while not predicate():
            if task.done():
                await task
                raise AssertionError("Application stopped before reaching the expected state")
            await asyncio.sleep(0.01)


async def cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("mode", ["idle", "monitor"])
def test_application_telegram_health_and_shutdown_without_implicit_messages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    settings = settings_for(tmp_path, mode)
    api, checker = FakeAPI(), FakeChecker()
    install_fakes(monkeypatch, api, checker)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            await wait_until(lambda: runtime.healthcheck(settings) and api.polls > 0, task)
            if mode == "monitor":
                await wait_until(lambda: checker.calls.get("website", 0) >= 3, task)
                assert SQLiteStore(tmp_path).read_site("website").state.confirmed is SiteState.UP
            heartbeat = json.loads((tmp_path / runtime.HEALTH_NAME).read_text())
            assert set(heartbeat["telegram"]) == telegram.WORKER_NAMES
            assert status_snapshot(settings)["telegram"]["enabled"] is True
            assert api.sends == 0
        finally:
            await cancel(task)
        assert api.closed and not runtime.healthcheck(settings)
        assert not (tmp_path / runtime.HEALTH_NAME).exists()
        with runtime._InstanceGuard(tmp_path):
            pass

    asyncio.run(scenario())


def test_website_checks_progress_and_health_remains_good_during_telegram_startup_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path, "monitor")
    api = FakeAPI()
    api.startup_error = TelegramError(TelegramErrorKind.TRANSIENT)
    checker = FakeChecker(frozenset({"website"}))
    install_fakes(monkeypatch, api, checker)
    monkeypatch.setattr(telegram, "_retry_delay", lambda *args: 0.02)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            await wait_until(
                lambda: api.startups >= 3 and checker.calls.get("website", 0) >= 3, task
            )
            assert runtime.healthcheck(settings)
            snapshot = status_snapshot(settings)
            assert snapshot["sites"][0]["state"] == "DOWN"
            assert snapshot["sites"][0]["fresh"] is True
            assert snapshot["telegram"]["last_error"] == "network"
            assert snapshot["telegram"]["pending"] == 1
            assert api.sends == api.polls == 0
        finally:
            await cancel(task)

    asyncio.run(scenario())


def test_delivery_outage_does_not_stop_other_source_or_bot_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path, "monitor")
    settings = replace(
        settings, sites=(*settings.sites, replace(settings.sites[0], alias="healthy"))
    )
    api = FakeAPI()
    api.send_error = TelegramError(TelegramErrorKind.TRANSIENT, status=503)
    checker = FakeChecker(frozenset({"website"}))
    install_fakes(monkeypatch, api, checker)
    monkeypatch.setattr(telegram, "_retry_delay", lambda *args: 0.02)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0.01)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            await wait_until(lambda: api.sends >= 2 and checker.calls.get("healthy", 0) >= 5, task)
            assert runtime.healthcheck(settings)
            store = SQLiteStore(tmp_path)
            assert store.read_site("website").state.confirmed is SiteState.DOWN
            assert store.read_site("healthy").state.confirmed is SiteState.UP
            assert store.delivery_status()["pending"] == 1
            assert store.delivery_status()["accepted"] == 0
            assert store.pending_notifications()[0].attempts >= 2
        finally:
            await cancel(task)

    asyncio.run(scenario())


def test_stalled_channel_makes_health_bad_without_hiding_fresh_down_observation(
    tmp_path: Path,
) -> None:
    settings = settings_for(tmp_path, "monitor")
    initialize_state(tmp_path, datetime.now(UTC))
    store = SQLiteStore(tmp_path)
    store.configure_telegram(settings.telegram.destination, datetime.now(UTC))
    store.reconcile(settings.sites)
    store.apply_observation(
        settings.sites[0], Observation(datetime.now(UTC), CheckResult(False, ErrorKind.TIMEOUT))
    )
    now = time.monotonic()
    progress = monitoring.MonitoringProgress(
        workers={"website": now},
        observations={"website": now},
        telegram={name: now for name in telegram.WORKER_NAMES},
    )
    with runtime._InstanceGuard(tmp_path) as owner:
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        assert runtime.healthcheck(settings)
        progress.telegram["poll"] = now - telegram.WORKER_TIMEOUT - 1
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        assert not runtime.healthcheck(settings)
        snapshot = status_snapshot(settings)
        assert snapshot["sites"][0]["state"] == "DOWN"
        assert snapshot["sites"][0]["fresh"] is True


@pytest.mark.parametrize("fatal", [TelegramErrorKind.CONFLICT, TelegramErrorKind.UNAUTHORIZED])
def test_fatal_poll_configuration_is_a_safe_cli_error_without_exception_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fatal: TelegramErrorKind,
) -> None:
    settings = settings_for(tmp_path)
    api, checker = FakeAPI(), FakeChecker()
    api.poll_error = TelegramError(fatal)
    install_fakes(monkeypatch, api, checker)
    monkeypatch.setattr(cli, "load_settings", lambda path: settings)
    assert cli.main(["run"]) == 1
    output = capsys.readouterr()
    assert "Runtime error: Telegram configuration failed:" in output.err
    assert fatal.value in output.err
    assert "Traceback" not in output.err and "ExceptionGroup" not in output.err
    assert settings.telegram.token not in output.err
    assert api.closed and not runtime.healthcheck(settings)


def test_unexpected_worker_bug_remains_visible_to_caller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    api, checker = FakeAPI(), FakeChecker()
    api.poll_error = ValueError("synthetic programming error")
    install_fakes(monkeypatch, api, checker)
    monkeypatch.setattr(cli, "load_settings", lambda path: settings)
    with pytest.raises(ExceptionGroup, match="TaskGroup") as raised:
        cli.main(["run"])
    assert "synthetic programming error" in str(raised.value.exceptions[0])
    assert api.closed


def test_cancel_during_provider_acknowledgement_keeps_state_guard_until_commit_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path, "monitor")
    api, checker = FakeAPI(), FakeChecker(frozenset({"website"}))
    install_fakes(monkeypatch, api, checker)
    entered, release = threading.Event(), threading.Event()
    acknowledge = SQLiteStore.acknowledge

    def delayed_ack(store: SQLiteStore, notification_id: str, accepted_at: datetime) -> None:
        entered.set()
        if not release.wait(3):
            raise RuntimeError("Test did not release the acknowledgement")
        acknowledge(store, notification_id, accepted_at)

    monkeypatch.setattr(SQLiteStore, "acknowledge", delayed_ack)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            await wait_until(entered.is_set, task)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            with pytest.raises(runtime.InstanceError):
                await runtime.run(settings)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
        assert api.sends == 1 and api.closed
        assert SQLiteStore(tmp_path).pending_count() == 0
        assert SQLiteStore(tmp_path).delivery_status()["accepted"] == 1
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())


def test_sigterm_closes_telegram_and_cleans_up_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    api, checker = FakeAPI(), FakeChecker()
    install_fakes(monkeypatch, api, checker)
    previous = signal.getsignal(signal.SIGTERM)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        await wait_until(lambda: runtime.healthcheck(settings) and api.polls > 0, task)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 3)
        assert not task.cancelled() and api.closed
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())
    assert signal.getsignal(signal.SIGTERM) == previous
