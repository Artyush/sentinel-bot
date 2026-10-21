import asyncio
import json
from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime

import pytest

import sentinel_bot.metrics_runtime as metrics_runtime
import sentinel_bot.runtime as runtime
from sentinel_bot.config import FilesystemConfig, ServerConfig, Settings
from sentinel_bot.metric_models import CpuCounter, FilesystemReading, MetricData
from sentinel_bot.metrics import ScrapeResult
from sentinel_bot.models import CheckResult, ErrorKind, SiteState
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import SQLiteStore
from sentinel_bot.telegram_commands import Command, render_command


class Exporters:
    def __init__(self):
        self.modes = defaultdict(lambda: "up")
        self.calls = defaultdict(int)
        self.gates = {}
        self.closed = 0
        self.broken = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed += 1

    async def check(self, server):
        if self.broken:
            raise RuntimeError("Synthetic metric worker failure")
        self.calls[server.alias] += 1
        if server.alias in self.gates:
            await self.gates[server.alias].wait()
        mode = self.modes[server.alias]
        if mode == "down":
            return ScrapeResult(CheckResult(False, ErrorKind.CONNECTION), None)
        high = mode in {"high", "band"}
        busy = 95 if high else 10
        # Accumulate counters across state changes, as an actual exporter does.
        previous = getattr(self, "counters", {})
        old = previous.get(server.alias, (0, 0))
        user, idle = old[0] + busy, old[1] + 100 - busy
        previous[server.alias] = (user, idle)
        self.counters = previous
        cpu = tuple(
            CpuCounter("0", name, value)
            for name, value in [
                ("idle", idle),
                ("user", user),
                ("system", 0),
                ("iowait", 0),
                ("nice", 0),
                ("irq", 0),
                ("softirq", 0),
                ("steal", 0),
            ]
        )
        missing = mode == "missing"
        percent = 80 if mode == "band" else 95 if high else 10
        return ScrapeResult(
            CheckResult(True, http_status=200),
            MetricData(
                None if missing else cpu,
                1_700_000_000.0,
                None if missing else 100,
                None if missing else 100 - percent,
                (
                    FilesystemReading(
                        "root", None if missing else percent, "missing" if missing else None
                    ),
                ),
            ),
        )


def configured(tmp_path, *, two=False):
    server = ServerConfig(
        "primary",
        "https://node.example.test/metrics",
        "x" * 32,
        poll_interval=0.01,
        connect_timeout=0.01,
        total_timeout=0.2,
        failure_confirmation=2,
        recovery_confirmation=2,
        mounts=(FilesystemConfig("root", "/private/synthetic/mount"),),
    )
    return Settings(
        tmp_path,
        mode="monitor",
        heartbeat_interval=0.01,
        servers=(server, replace(server, alias="secondary")) if two else (server,),
    )


async def until(condition, task):
    async with asyncio.timeout(3):
        while not condition():
            if task.done():
                await task
                raise AssertionError("Application stopped before the expected state")
            await asyncio.sleep(0.002)


async def cancel(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def install(monkeypatch, checker):
    monkeypatch.setattr(metrics_runtime, "MetricsChecker", lambda *args, **kwargs: checker)


def test_resources_unknown_hysteresis_recovery_and_cached_commands(tmp_path, monkeypatch):
    settings = configured(tmp_path, two=True)
    checker = Exporters()
    install(monkeypatch, checker)

    async def scenario():
        task = asyncio.create_task(runtime.run(settings))
        try:
            await until(lambda: runtime.healthcheck(settings), task)
            store = SQLiteStore(tmp_path)
            await until(
                lambda: store.read_site("primary/cpu").state.confirmed is SiteState.UP, task
            )
            checker.modes["primary"] = "high"
            await until(
                lambda: all(
                    store.read_site("primary/" + name).state.confirmed is SiteState.DOWN
                    for name in ("cpu", "memory", "fs_root")
                ),
                task,
            )
            assert store.read_site("primary/exporter").state.confirmed is SiteState.UP
            incident = store.read_site("primary/memory").state.incident_id
            checker.modes["primary"] = "band"
            await until(
                lambda: status_snapshot(settings)["servers"][0]["memory_percent"] == 80, task
            )
            assert store.read_site("primary/memory").state.incident_id == incident
            checker.modes["primary"] = "missing"
            await until(
                lambda: status_snapshot(settings)["servers"][0]["memory_percent"] is None, task
            )
            assert store.read_site("primary/memory").state.incident_id == incident
            snapshot = status_snapshot(settings)
            primary = snapshot["servers"][0]
            assert primary["fresh"] and primary["availability"] == "UP"
            assert all(not row["fresh"] for row in primary["rules"] if row["name"] != "exporter")
            assert all(
                private not in json.dumps(snapshot)
                for private in (
                    settings.servers[0].url,
                    settings.servers[0].token,
                    settings.servers[0].mounts[0].mount,
                )
            )
            calls = dict(checker.calls)
            for command in (Command("servers"), Command("server", "primary"), Command("status")):
                response = render_command(command, snapshot, now=datetime.now(UTC))
                assert "primary" in response and "UNKNOWN" in response
                assert len(response.encode("utf-16-le")) // 2 <= 3500
            assert "Unknown server alias" in render_command(
                Command("server", "missing"), snapshot, now=datetime.now(UTC)
            )
            assert dict(checker.calls) == calls
            checker.modes["primary"] = "up"
            await until(
                lambda: all(
                    store.read_site("primary/" + name).state.confirmed is SiteState.UP
                    for name in ("cpu", "memory", "fs_root")
                ),
                task,
            )
            assert checker.calls["secondary"] > 5
            assert runtime.healthcheck(settings)
            records = store.pending_notifications()
            assert sum(record.event.kind.value == "DOWN" for record in records) == 3
            assert sum(record.event.kind.value == "RECOVERY" for record in records) == 3
        finally:
            await cancel(task)
        assert checker.closed == 1
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())


def test_restart_preserves_incident_but_cpu_warms_up_and_cached_readings_stale(
    tmp_path, monkeypatch
):
    settings = configured(tmp_path)
    checker = Exporters()
    checker.modes["primary"] = "high"
    install(monkeypatch, checker)

    async def scenario():
        task = asyncio.create_task(runtime.run(settings))
        await until(lambda: runtime.healthcheck(settings), task)
        store = SQLiteStore(tmp_path)
        try:
            await until(
                lambda: store.read_site("primary/cpu").state.confirmed is SiteState.DOWN, task
            )
            incident = store.read_site("primary/cpu").state.incident_id
        finally:
            await cancel(task)
        gate = asyncio.Event()
        checker.gates["primary"] = gate
        task = asyncio.create_task(runtime.run(settings))
        try:
            await until(lambda: runtime.healthcheck(settings), task)
            cached = status_snapshot(settings)["servers"][0]
            assert cached["fresh"] is False
            assert store.read_site("primary/cpu").state.incident_id == incident
            checker.modes["primary"] = "up"
            gate.set()
            await until(
                lambda: store.read_site("primary/cpu").state.confirmed is SiteState.UP, task
            )
            assert not [
                row
                for row in store.pending_notifications()
                if row.source_alias == "primary/exporter"
            ]
        finally:
            await cancel(task)
        assert checker.closed == 2

    asyncio.run(scenario())


def test_stalled_exporter_does_not_hide_other_server_freshness(tmp_path, monkeypatch):
    settings = configured(tmp_path, two=True)
    checker = Exporters()
    install(monkeypatch, checker)

    async def scenario():
        checker.gates["secondary"] = asyncio.Event()
        task = asyncio.create_task(runtime.run(settings))
        try:
            await until(lambda: (tmp_path / runtime.HEALTH_NAME).exists(), task)
            await until(lambda: checker.calls["primary"] >= 60, task)
            assert not runtime.healthcheck(settings)
            servers = status_snapshot(settings)["servers"]
            assert servers[0]["fresh"] is True
            assert servers[1]["fresh"] is False
        finally:
            await cancel(task)
        assert checker.closed == 1

    asyncio.run(scenario())


def test_exporter_outage_does_not_invent_resource_recovery(tmp_path, monkeypatch):
    settings = configured(tmp_path)
    checker = Exporters()
    checker.modes["primary"] = "high"
    install(monkeypatch, checker)

    async def scenario():
        task = asyncio.create_task(runtime.run(settings))
        await until(lambda: runtime.healthcheck(settings), task)
        store = SQLiteStore(tmp_path)
        try:
            await until(
                lambda: store.read_site("primary/memory").state.confirmed is SiteState.DOWN, task
            )
            checker.modes["primary"] = "down"
            await until(
                lambda: store.read_site("primary/exporter").state.confirmed is SiteState.DOWN, task
            )
            snapshot = status_snapshot(settings)["servers"][0]
            assert snapshot["memory_percent"] is None
            assert store.read_site("primary/memory").state.confirmed is SiteState.DOWN
            assert runtime.healthcheck(settings)
            assert not [
                record
                for record in store.pending_notifications()
                if record.event.kind.value == "RECOVERY"
            ]
        finally:
            await cancel(task)

    asyncio.run(scenario())


def test_programming_error_is_visible_and_releases_process_ownership(tmp_path, monkeypatch):
    checker = Exporters()
    checker.broken = True
    install(monkeypatch, checker)
    with pytest.raises(ExceptionGroup, match="TaskGroup"):
        asyncio.run(runtime.run(configured(tmp_path)))
    assert checker.closed == 1
    assert not (tmp_path / runtime.HEALTH_NAME).exists()
