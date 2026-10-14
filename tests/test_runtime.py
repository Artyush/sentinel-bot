import asyncio
import json
import os
import signal
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

import sentinel_bot.runtime as runtime
from sentinel_bot.config import Settings
from sentinel_bot.storage import DATABASE_NAME, StorageError


async def wait_for_health(settings: Settings, task: asyncio.Task[None]) -> None:
    async with asyncio.timeout(3):
        while not runtime.healthcheck(settings):
            if task.done():
                await task
                raise AssertionError("Runtime ended before becoming healthy")
            await asyncio.sleep(0.01)


async def cancel_runtime(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_idle_progress_cancellation_and_restart(tmp_path: Path) -> None:
    settings = Settings(state_dir=tmp_path, heartbeat_interval=0.01)

    async def scenario() -> None:
        first = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, first)
        heartbeat_path = tmp_path / runtime.HEALTH_NAME
        heartbeat = json.loads(heartbeat_path.read_text())
        first_run = heartbeat["run_id"]
        async with asyncio.timeout(3):
            while (
                json.loads(heartbeat_path.read_text())["progress_monotonic"]
                <= heartbeat["progress_monotonic"]
            ):
                await asyncio.sleep(0.01)
        await cancel_runtime(first)
        assert not runtime.healthcheck(settings)
        assert not heartbeat_path.exists()
        assert (tmp_path / runtime.LOCK_NAME).exists()

        second = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, second)
        assert json.loads(heartbeat_path.read_text())["run_id"] != first_run
        await cancel_runtime(second)

    asyncio.run(scenario())
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (2,)


def test_competing_instance_cannot_change_current_metadata_or_health(tmp_path: Path) -> None:
    settings = Settings(state_dir=tmp_path)

    async def scenario() -> None:
        owner = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, owner)
        before = (tmp_path / runtime.HEALTH_NAME).read_bytes()
        try:
            with pytest.raises(RuntimeError, match="Another Sentinel instance"):
                await runtime.run(settings)
            assert (tmp_path / runtime.HEALTH_NAME).read_bytes() == before
            assert runtime.healthcheck(settings)
            with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
                assert connection.execute(
                    "SELECT startup_count FROM runtime_metadata"
                ).fetchone() == (1,)
        finally:
            await cancel_runtime(owner)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", uuid4().hex),
        ("pid", True),
        ("pid", -1),
        ("status", "healthy"),
        ("progress_monotonic", None),
        ("progress_monotonic", True),
        ("progress_monotonic", "123"),
        ("progress_monotonic", float("nan")),
        ("progress_monotonic", float("inf")),
        ("updated_at", "2026-01-01T00:00:00"),
        ("updated_at", "2026-01-01T00:00:00+04:00"),
        ("updated_at", None),
    ],
)
def test_health_rejects_invalid_heartbeat(tmp_path: Path, field: str, value: object) -> None:
    settings = Settings(state_dir=tmp_path)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, task)
        try:
            path = tmp_path / runtime.HEALTH_NAME
            heartbeat = json.loads(path.read_text())
            heartbeat[field] = value
            path.write_text(json.dumps(heartbeat))
            assert not runtime.healthcheck(settings)
        finally:
            await cancel_runtime(task)

    asyncio.run(scenario())


@pytest.mark.parametrize("age", [-1, 6])
def test_health_rejects_future_or_stale_progress(tmp_path: Path, age: float) -> None:
    settings = Settings(state_dir=tmp_path)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, task)
        try:
            path = tmp_path / runtime.HEALTH_NAME
            heartbeat = json.loads(path.read_text())
            heartbeat["progress_monotonic"] = time.monotonic() - age
            path.write_text(json.dumps(heartbeat))
            assert not runtime.healthcheck(settings)
        finally:
            await cancel_runtime(task)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "content", [b"{}", b"[]", b"invalid", b"x" * 4097, b"\xff", b"[" * 1500 + b"]" * 1500]
)
def test_health_rejects_malformed_or_large_file(tmp_path: Path, content: bytes) -> None:
    settings = Settings(state_dir=tmp_path)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, task)
        try:
            (tmp_path / runtime.HEALTH_NAME).write_bytes(content)
            assert not runtime.healthcheck(settings)
        finally:
            await cancel_runtime(task)

    asyncio.run(scenario())


def test_unowned_state_is_unhealthy_even_with_stale_heartbeat(tmp_path: Path) -> None:
    settings = Settings(state_dir=tmp_path)
    assert not runtime.healthcheck(settings)
    (tmp_path / runtime.LOCK_NAME).write_text("{}")
    (tmp_path / runtime.HEALTH_NAME).write_text("{}")
    assert not runtime.healthcheck(settings)


def test_schema_failure_releases_lock_and_removes_stale_health(tmp_path: Path) -> None:
    settings = Settings(state_dir=tmp_path)
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("PRAGMA user_version = 99")
    (tmp_path / runtime.HEALTH_NAME).write_text("synthetic stale health")
    for _ in range(2):
        with pytest.raises(StorageError, match="Unsupported state schema"):
            asyncio.run(runtime.run(settings))
        assert not runtime.healthcheck(settings)
        assert not (tmp_path / runtime.HEALTH_NAME).exists()


def test_worker_failure_is_visible_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(state_dir: Path, run_id: str) -> None:
        raise RuntimeError("synthetic heartbeat failure")

    monkeypatch.setattr(runtime, "_write_heartbeat", fail_write)
    with pytest.raises(RuntimeError, match="synthetic heartbeat failure"):
        asyncio.run(runtime.run(Settings(state_dir=tmp_path)))
    assert not runtime.healthcheck(Settings(state_dir=tmp_path))


def test_cancellation_keeps_lock_until_sqlite_thread_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = threading.Event()
    release = threading.Event()
    initialize_state = runtime.initialize_state

    def delayed_initialize(state_dir: Path, started_at: runtime.datetime) -> object:
        started.set()
        if not release.wait(timeout=3):
            raise RuntimeError("Test did not release the SQLite operation")
        return initialize_state(state_dir, started_at)

    monkeypatch.setattr(runtime, "initialize_state", delayed_initialize)
    settings = Settings(state_dir=tmp_path)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            with pytest.raises(RuntimeError, match="Another Sentinel instance"):
                await runtime.run(settings)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())


def test_sigterm_stops_without_cancelling_caller(tmp_path: Path) -> None:
    settings = Settings(state_dir=tmp_path)
    original_handler = signal.getsignal(signal.SIGTERM)

    async def scenario() -> None:
        task = asyncio.create_task(runtime.run(settings))
        await wait_for_health(settings, task)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, timeout=3)
        assert not task.cancelled()
        assert not runtime.healthcheck(settings)

    asyncio.run(scenario())
    assert signal.getsignal(signal.SIGTERM) == original_handler
