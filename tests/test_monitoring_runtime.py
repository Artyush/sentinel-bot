import asyncio
import json
import os
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import sentinel_bot.monitoring as monitoring
import sentinel_bot.runtime as runtime
from sentinel_bot.config import Settings, SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore, initialize_state


async def wait_until(condition: Callable[[], bool], task: asyncio.Task[None]) -> None:
    async with asyncio.timeout(3):
        while not condition():
            if task.done():
                await task
                raise AssertionError("Runtime stopped before the expected condition")
            await asyncio.sleep(0.001)


async def cancel_runtime(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def settings_for(tmp_path: Path) -> Settings:
    return Settings(
        tmp_path,
        mode="monitor",
        heartbeat_interval=0.01,
        sites=(
            SiteConfig(
                "site",
                "https://example.test/",
                poll_interval=0.01,
                connect_timeout=0.01,
                total_timeout=0.05,
            ),
        ),
    )


class FailingChecker:
    def __init__(self, *args: object) -> None:
        self.closed = False

    async def __aenter__(self) -> FailingChecker:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def check(self, site: SiteConfig) -> CheckResult:
        return CheckResult(False, ErrorKind.HTTP_STATUS, http_status=503, latency_ms=2)


def test_down_site_does_not_make_monitoring_runtime_unhealthy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    checker = FailingChecker()
    monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            await wait_until(lambda: runtime.healthcheck(settings), task)
            store = SQLiteStore(tmp_path)
            await wait_until(
                lambda: store.read_site("site").state.confirmed is SiteState.DOWN, task
            )
            await wait_until(lambda: status_snapshot(settings)["sites"][0]["fresh"], task)
            assert runtime.healthcheck(settings)
            assert [record.event.kind for record in store.pending_notifications()] == [
                EventKind.DOWN
            ]
        finally:
            await cancel_runtime(task)
        assert checker.closed
        assert not runtime.healthcheck(settings)
        assert not (tmp_path / runtime.HEALTH_NAME).exists()

    asyncio.run(scenario())


def test_stalled_source_invalidates_health_while_heartbeat_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)

    async def scenario() -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class StalledChecker(FailingChecker):
            async def check(self, site: SiteConfig) -> CheckResult:
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
                raise AssertionError("A blocked checker must be cancelled")

        checker = StalledChecker()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        task = asyncio.create_task(runtime.run(settings))
        try:
            await asyncio.wait_for(started.wait(), 3)
            await wait_until(lambda: not runtime.healthcheck(settings), task)
            heartbeat = json.loads((tmp_path / runtime.HEALTH_NAME).read_text())
            assert time.monotonic() - heartbeat["progress_monotonic"] < settings.health_timeout
            assert (
                time.monotonic() - heartbeat["workers"]["site"] > settings.sites[0].freshness_limit
            )
            assert runtime.observation_progress(settings) == {}
        finally:
            await cancel_runtime(task)
        assert cancelled.is_set()
        assert checker.closed

    asyncio.run(scenario())


def test_restart_preserves_down_but_restored_observation_is_stale_until_new_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    initialize_state(tmp_path, datetime.now(UTC))
    store = SQLiteStore(tmp_path)
    store.reconcile(settings.sites)
    began = datetime(2026, 1, 1, tzinfo=UTC)
    for seconds in range(3):
        store.apply_observation(
            settings.sites[0],
            Observation(began + timedelta(seconds=seconds), CheckResult(False, ErrorKind.DNS)),
        )
    original_id = store.read_site("site").state.incident_id

    async def scenario() -> None:
        release = asyncio.Event()
        entered = asyncio.Event()

        class FirstReadingGate(FailingChecker):
            async def check(self, site: SiteConfig) -> CheckResult:
                entered.set()
                await release.wait()
                return await super().check(site)

        checker = FirstReadingGate()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        task = asyncio.create_task(runtime.run(settings))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            before = status_snapshot(settings)["sites"][0]
            assert before["state"] == "DOWN"
            assert before["incident_id"] == original_id
            assert before["fresh"] is False
            release.set()
            await wait_until(lambda: status_snapshot(settings)["sites"][0]["fresh"], task)
            after = status_snapshot(settings)["sites"][0]
            assert after["state"] == "DOWN"
            assert after["incident_id"] == original_id
            assert store.pending_count() == 1
        finally:
            await cancel_runtime(task)

    asyncio.run(scenario())


def test_unexpected_monitor_failure_stops_runtime_and_releases_state_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)

    class BrokenChecker(FailingChecker):
        async def check(self, site: SiteConfig) -> CheckResult:
            raise RuntimeError("synthetic monitor failure")

    monkeypatch.setattr(monitoring, "HttpChecker", BrokenChecker)
    with pytest.raises(ExceptionGroup) as error:
        asyncio.run(runtime.run(settings))
    failures = error.value.subgroup(RuntimeError)
    assert failures is not None
    assert "synthetic monitor failure" in repr(failures)
    assert not (tmp_path / runtime.HEALTH_NAME).exists()
    assert not runtime.healthcheck(settings)
    with runtime._InstanceGuard(tmp_path):
        pass


def test_sigterm_cancels_active_probe_and_restores_signal_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    original = signal.getsignal(signal.SIGTERM)

    async def scenario() -> None:
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        class BlockedChecker(FailingChecker):
            async def check(self, site: SiteConfig) -> CheckResult:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
                raise AssertionError("A blocked checker must be cancelled")

        checker = BlockedChecker()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        task = asyncio.create_task(runtime.run(settings))
        await asyncio.wait_for(entered.wait(), 3)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 3)
        assert not task.cancelled()
        assert cancelled.is_set()
        assert checker.closed
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())
    assert signal.getsignal(signal.SIGTERM) == original
