import asyncio
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sentinel_bot.config import Settings, SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.storage import SQLiteStore, initialize_state


async def wait_until(condition: Callable[[], bool], timeout: float = 3) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.001)


class ControlledChecker:
    def __init__(self) -> None:
        self.closed = False
        self.calls: dict[str, int] = defaultdict(int)
        self.active: dict[str, int] = defaultdict(int)
        self.maximum_active: dict[str, int] = defaultdict(int)
        self.results: dict[str, CheckResult] = {}
        self.gates: dict[str, asyncio.Event] = {}
        self.entered: dict[str, asyncio.Event] = {}
        self.cancelled: set[str] = set()

    async def __aenter__(self) -> ControlledChecker:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def check(self, site: SiteConfig) -> CheckResult:
        alias = site.alias
        self.calls[alias] += 1
        self.active[alias] += 1
        self.maximum_active[alias] = max(self.maximum_active[alias], self.active[alias])
        if alias in self.entered:
            self.entered[alias].set()
        try:
            if alias in self.gates:
                await self.gates[alias].wait()
            return self.results.get(alias, CheckResult(True, http_status=200, latency_ms=1))
        except asyncio.CancelledError:
            self.cancelled.add(alias)
            raise
        finally:
            self.active[alias] -= 1


def monitoring_settings(tmp_path: Path, *aliases: str) -> Settings:
    return Settings(
        tmp_path,
        mode="monitor",
        sites=tuple(
            SiteConfig(alias, f"https://example.test/{alias}", poll_interval=0.01)
            for alias in aliases
        ),
    )


def prepare_store(settings: Settings) -> SQLiteStore:
    initialize_state(settings.state_dir, datetime.now(UTC))
    store = SQLiteStore(settings.state_dir)
    store.reconcile(settings.sites)
    return store


def test_slow_source_does_not_block_down_or_recovery_of_other_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "slow", "fast")
    store = prepare_store(settings)
    progress = monitoring.MonitoringProgress()

    async def scenario() -> None:
        checker = ControlledChecker()
        checker.gates["slow"] = asyncio.Event()
        checker.entered["slow"] = asyncio.Event()
        checker.results["fast"] = CheckResult(False, ErrorKind.TIMEOUT)
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        stopped = asyncio.Event()
        task = asyncio.create_task(monitoring.monitor_sites(settings, stopped, progress))
        try:
            await asyncio.wait_for(checker.entered["slow"].wait(), 3)
            await wait_until(lambda: store.read_site("fast").state.confirmed is SiteState.DOWN)
            assert checker.calls["slow"] == 1
            assert store.read_site("slow").observation is None
            assert [record.event.kind for record in store.pending_notifications()] == [
                EventKind.DOWN
            ]
            checker.results["fast"] = CheckResult(True, http_status=200)
            await wait_until(lambda: store.read_site("fast").state.confirmed is SiteState.UP)
            assert [record.event.kind for record in store.pending_notifications()] == [
                EventKind.DOWN,
                EventKind.RECOVERY,
            ]
            assert "fast" in progress.observations
            assert checker.maximum_active["fast"] == 1
            assert checker.maximum_active["slow"] == 1
        finally:
            stopped.set()
            await asyncio.wait_for(task, 3)
        assert checker.closed
        assert checker.cancelled == {"slow"}

    asyncio.run(scenario())


def test_cancelled_monitor_closes_checker_and_cancels_active_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "site")
    store = prepare_store(settings)

    async def scenario() -> None:
        checker = ControlledChecker()
        checker.gates["site"] = asyncio.Event()
        checker.entered["site"] = asyncio.Event()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        task = asyncio.create_task(
            monitoring.monitor_sites(settings, asyncio.Event(), monitoring.MonitoringProgress())
        )
        await asyncio.wait_for(checker.entered["site"].wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert checker.cancelled == {"site"}
        assert checker.closed
        assert store.read_site("site").observation is None
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_unexpected_checker_failure_is_visible_and_cancels_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "broken", "waiting")
    prepare_store(settings)

    async def scenario() -> None:
        waiting_started = asyncio.Event()

        class BrokenChecker(ControlledChecker):
            async def check(self, site: SiteConfig) -> CheckResult:
                if site.alias == "broken":
                    await waiting_started.wait()
                    raise RuntimeError("synthetic unexpected probe bug")
                waiting_started.set()
                return await super().check(site)

        checker = BrokenChecker()
        checker.gates["waiting"] = asyncio.Event()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        with pytest.raises(ExceptionGroup) as error:
            await monitoring.monitor_sites(
                settings, asyncio.Event(), monitoring.MonitoringProgress()
            )
        subgroup = error.value.subgroup(RuntimeError)
        assert subgroup is not None
        assert "synthetic unexpected probe bug" in repr(subgroup)
        assert checker.cancelled == {"waiting"}
        assert checker.closed

    asyncio.run(scenario())


def test_restored_down_state_does_not_generate_second_opening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "site")
    store = prepare_store(settings)
    site = settings.sites[0]
    started = datetime(2026, 1, 1, tzinfo=UTC)
    for seconds in range(3):
        store.apply_observation(
            site,
            Observation(started + timedelta(seconds=seconds), CheckResult(False, ErrorKind.DNS)),
        )
    original_incident = store.read_site("site").state.incident_id
    original_queue = store.pending_notifications()
    store.reconcile(settings.sites)
    progress = monitoring.MonitoringProgress()
    assert progress.observations == {}

    async def scenario() -> None:
        checker = ControlledChecker()
        checker.results["site"] = CheckResult(False, ErrorKind.CONNECTION)
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        stopped = asyncio.Event()
        task = asyncio.create_task(monitoring.monitor_sites(settings, stopped, progress))
        try:
            await wait_until(lambda: checker.calls["site"] >= 4)
        finally:
            stopped.set()
            await asyncio.wait_for(task, 3)
        assert store.read_site("site").state.incident_id == original_incident
        assert store.pending_notifications() == original_queue
        assert "site" in progress.observations

    asyncio.run(scenario())


def test_disabled_sources_never_create_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "enabled", "disabled")
    settings = replace(
        settings, sites=(settings.sites[0], replace(settings.sites[1], enabled=False))
    )
    store = prepare_store(settings)

    async def scenario() -> None:
        checker = ControlledChecker()
        monkeypatch.setattr(monitoring, "HttpChecker", lambda *args, **kwargs: checker)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            monitoring.monitor_sites(settings, stopped, monitoring.MonitoringProgress())
        )
        try:
            await wait_until(lambda: store.read_site("enabled").state.confirmed is SiteState.UP)
        finally:
            stopped.set()
            await asyncio.wait_for(task, 3)
        assert checker.calls["disabled"] == 0
        assert [record.alias for record in store.read_sites()] == ["enabled"]

    asyncio.run(scenario())


def test_stale_gap_resets_failure_confirmation_without_fabricated_transition(
    tmp_path: Path,
) -> None:
    import sentinel_bot.monitoring as monitoring

    settings = monitoring_settings(tmp_path, "site")
    site = settings.sites[0]
    store = prepare_store(settings)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for seconds in range(2):
        store.apply_observation(
            site, Observation(start + timedelta(seconds=seconds), CheckResult(False, ErrorKind.DNS))
        )

    async def scenario() -> None:
        stopped = asyncio.Event()
        progress = monitoring.MonitoringProgress()
        progress.observations[site.alias] = time.monotonic() - site.freshness_limit - 1

        class OnceChecker:
            async def check(self, configured: SiteConfig) -> CheckResult:
                stopped.set()
                return CheckResult(False, ErrorKind.DNS)

        await monitoring._site_loop(site, OnceChecker(), store, asyncio.Lock(), stopped, progress)
        record = store.read_site(site.alias)
        assert record.state.confirmed is SiteState.UNKNOWN
        assert record.state.failures == 1
        assert store.pending_count() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("confirmed_down", [False, True])
def test_local_capacity_deferral_cannot_confirm_failure_or_queue_reminder(
    tmp_path: Path, confirmed_down: bool
) -> None:
    import sentinel_bot.monitoring as monitoring
    from sentinel_bot.http import CheckDeferred

    settings = monitoring_settings(tmp_path, "site")
    site = settings.sites[0]
    store = prepare_store(settings)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for seconds in range(3 if confirmed_down else 2):
        store.apply_observation(
            site, Observation(start + timedelta(seconds=seconds), CheckResult(False, ErrorKind.DNS))
        )
    if confirmed_down:
        store.acknowledge(store.pending_notifications()[0].id, start + timedelta(seconds=3))
        store.apply_observation(site, Observation(start + timedelta(seconds=4), CheckResult(True)))
    before = store.read_site(site.alias)

    async def scenario() -> None:
        stopped = asyncio.Event()
        previous_progress = time.monotonic() - site.freshness_limit - 1
        progress = monitoring.MonitoringProgress(observations={site.alias: previous_progress})

        class DeferredChecker:
            async def check(self, configured: SiteConfig) -> CheckResult:
                stopped.set()
                raise CheckDeferred

        await monitoring._site_loop(
            site, DeferredChecker(), store, asyncio.Lock(), stopped, progress
        )
        record = store.read_site(site.alias)
        assert record.state.confirmed is before.state.confirmed
        assert record.state.incident_id == before.state.incident_id
        assert record.state.failures == record.state.successes == 0
        assert record.observation == before.observation
        assert progress.observations[site.alias] == previous_progress
        assert progress.workers[site.alias] > previous_progress
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_slow_commit_does_not_refresh_old_probe_or_queue_stale_reminder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_bot.monitoring as monitoring

    site = SiteConfig("site", "https://example.test/", poll_interval=0.01, total_timeout=0.01)
    settings = Settings(tmp_path, mode="monitor", sites=(site,))
    store = prepare_store(settings)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for seconds in range(3):
        store.apply_observation(
            site, Observation(start + timedelta(seconds=seconds), CheckResult(False, ErrorKind.DNS))
        )
    store.acknowledge(store.pending_notifications()[0].id, start + timedelta(seconds=3))

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        commit_started = asyncio.Event()
        release_commit = threading.Event()
        original_apply = SQLiteStore.apply_observation

        def slow_commit(
            configured_store: SQLiteStore, configured_site: SiteConfig, reading: Observation
        ) -> object:
            result = original_apply(configured_store, configured_site, reading)
            loop.call_soon_threadsafe(commit_started.set)
            if not release_commit.wait(3):
                raise RuntimeError("Test did not release SQLite commit")
            return result

        monkeypatch.setattr(SQLiteStore, "apply_observation", slow_commit)
        stopped = asyncio.Event()
        progress = monitoring.MonitoringProgress()

        class OnceChecker:
            async def check(self, configured_site: SiteConfig) -> CheckResult:
                stopped.set()
                return CheckResult(False, ErrorKind.DNS)

        task = asyncio.create_task(
            monitoring._site_loop(site, OnceChecker(), store, asyncio.Lock(), stopped, progress)
        )
        try:
            await asyncio.wait_for(commit_started.wait(), 3)
            await asyncio.sleep(site.freshness_limit + 0.02)
        finally:
            release_commit.set()
            await asyncio.wait_for(task, 3)
        assert (
            progress.workers[site.alias] - progress.observations[site.alias] > site.freshness_limit
        )
        assert time.monotonic() - progress.observations[site.alias] > site.freshness_limit
        assert store.pending_count() == 0
        assert store.read_site(site.alias).state.confirmed is SiteState.DOWN

    asyncio.run(scenario())
