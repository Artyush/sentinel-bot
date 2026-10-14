"""Single-instance idle lifecycle and local progress health checks."""

import asyncio
import fcntl
import json
import math
import os
import signal
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from sentinel_bot.config import Settings
from sentinel_bot.storage import initialize_state

LOCK_NAME = "instance.lock"
HEALTH_NAME = "health.json"
MAX_HEALTH_BYTES = 4096


class InstanceError(RuntimeError):
    """Another active process owns the configured state directory."""


class _InstanceGuard:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.run_id = uuid4().hex
        self.fd: int | None = None

    def __enter__(self) -> _InstanceGuard:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.state_dir / LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise InstanceError("Another Sentinel instance owns the state directory") from exc
            owner = json.dumps({"run_id": self.run_id, "pid": os.getpid()}).encode()
            os.ftruncate(fd, 0)
            os.write(fd, owner)
            self.fd = fd
            return self
        except BaseException:
            os.close(fd)
            raise

    def __exit__(self, *_: object) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _write_heartbeat(state_dir: Path, run_id: str) -> None:
    heartbeat = {
        "run_id": run_id,
        "pid": os.getpid(),
        "progress_monotonic": time.monotonic(),
        "updated_at": datetime.now(UTC).isoformat(),
        "status": "idle",
    }
    fd, temporary = tempfile.mkstemp(prefix=".health-", dir=state_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(heartbeat, stream)
        os.replace(temporary, state_dir / HEALTH_NAME)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def _blocking[Result](operation: Callable[[], Result]) -> Result:
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A cancelled await cannot stop a SQLite thread; keep the lock until it exits.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        task.result()
        raise


async def _heartbeat(settings: Settings, run_id: str, stopped: asyncio.Event) -> None:
    while not stopped.is_set():
        _write_heartbeat(settings.state_dir, run_id)
        try:
            async with asyncio.timeout(settings.heartbeat_interval):
                await stopped.wait()
        except TimeoutError:
            pass


async def run(settings: Settings) -> None:
    """Run explicit idle mode until a stop signal, cancellation, or worker failure."""
    if settings.mode != "idle":
        raise RuntimeError("Only explicit idle mode is implemented")
    with _InstanceGuard(settings.state_dir) as guard:
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        previous_handlers = {
            signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)
        }
        installed: list[signal.Signals] = []
        try:
            for signum in previous_handlers:
                loop.add_signal_handler(signum, stopped.set)
                installed.append(signum)
            (settings.state_dir / HEALTH_NAME).unlink(missing_ok=True)
            await _blocking(lambda: initialize_state(settings.state_dir, datetime.now(UTC)))
            worker = asyncio.create_task(_heartbeat(settings, guard.run_id, stopped))
            stop_waiter = asyncio.create_task(stopped.wait())
            try:
                done, _ = await asyncio.wait(
                    {worker, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                if worker in done:
                    await worker
                    if not stopped.is_set():
                        raise RuntimeError("Heartbeat worker stopped unexpectedly")
            finally:
                stopped.set()
                stop_waiter.cancel()
                await asyncio.gather(stop_waiter, return_exceptions=True)
                await asyncio.wait_for(worker, timeout=settings.shutdown_timeout)
        finally:
            try:
                (settings.state_dir / HEALTH_NAME).unlink(missing_ok=True)
            finally:
                for signum in installed:
                    loop.remove_signal_handler(signum)
                    signal.signal(signum, previous_handlers[signum])


def _read_record(path: Path) -> dict[str, object]:
    with path.open("rb") as stream:
        raw = stream.read(MAX_HEALTH_BYTES + 1)
    if len(raw) > MAX_HEALTH_BYTES:
        raise ValueError("Local health record is too large")
    try:
        value: object = json.loads(raw)
    except RecursionError as exc:
        raise ValueError("Local health record is too deeply nested") from exc
    if not isinstance(value, dict):
        raise ValueError("Local health record must be an object")
    return value


def _lock_is_held(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    fcntl.flock(fd, fcntl.LOCK_UN)
    return False


def healthcheck(settings: Settings) -> bool:
    """Check current lock ownership and recent idle-loop progress without networking."""
    fd: int | None = None
    try:
        fd = os.open(settings.state_dir / LOCK_NAME, os.O_RDWR)
        if not _lock_is_held(fd):
            return False
        owner = _read_record(settings.state_dir / LOCK_NAME)
        heartbeat = _read_record(settings.state_dir / HEALTH_NAME)
        run_id = owner.get("run_id")
        pid = owner.get("pid")
        if not isinstance(run_id, str) or len(run_id) != 32:
            return False
        UUID(hex=run_id)
        if type(pid) is not int or pid <= 0:
            return False
        if heartbeat.get("run_id") != run_id or heartbeat.get("pid") != pid:
            return False
        if type(heartbeat.get("pid")) is not int or heartbeat.get("status") != "idle":
            return False
        progress = heartbeat.get("progress_monotonic")
        if type(progress) not in (float, int):
            return False
        if not isinstance(progress, (float, int)) or not math.isfinite(progress):
            return False
        age = time.monotonic() - progress
        if not 0 <= age <= settings.health_timeout:
            return False
        updated_at = heartbeat.get("updated_at")
        if not isinstance(updated_at, str):
            return False
        timestamp = datetime.fromisoformat(updated_at)
        if timestamp.tzinfo is None or timestamp.utcoffset() != UTC.utcoffset(timestamp):
            return False
        return _lock_is_held(fd) and _read_record(settings.state_dir / LOCK_NAME) == owner
    except OSError, ValueError, TypeError, OverflowError:
        return False
    finally:
        if fd is not None:
            os.close(fd)
