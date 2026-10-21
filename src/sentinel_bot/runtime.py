"""Single-instance lifecycle and local worker progress health checks."""

import asyncio
import fcntl
import json
import math
import os
import signal
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from sentinel_bot.config import Settings
from sentinel_bot.monitoring import MonitoringProgress, monitor_sites
from sentinel_bot.policy import is_fresh
from sentinel_bot.storage import SQLiteStore, initialize_state
from sentinel_bot.workers import await_completion
from sentinel_bot.workers import run_blocking as _blocking

LOCK_NAME = "instance.lock"
HEALTH_NAME = "health.json"
MAX_HEALTH_BYTES = 8192


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


def _write_heartbeat(
    state_dir: Path,
    run_id: str,
    settings: Settings | None = None,
    progress: MonitoringProgress | None = None,
) -> None:
    heartbeat: dict[str, object] = {
        "run_id": run_id,
        "pid": os.getpid(),
        "progress_monotonic": time.monotonic(),
        "updated_at": datetime.now(UTC).isoformat(),
        "status": "idle" if settings is None else settings.mode,
    }
    if progress is not None:
        heartbeat["workers"] = progress.workers.copy()
        heartbeat["observations"] = progress.observations.copy()
        if progress.telegram:
            heartbeat["telegram"] = progress.telegram.copy()
        if progress.ingress is not None:
            heartbeat["ingress"] = progress.ingress
        if progress.email:
            heartbeat["email"] = progress.email.copy()
    encoded = json.dumps(heartbeat).encode()
    if len(encoded) > MAX_HEALTH_BYTES:
        raise RuntimeError("Local health record exceeds its size bound")
    fd, temporary = tempfile.mkstemp(prefix=".health-", dir=state_dir)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
        os.replace(temporary, state_dir / HEALTH_NAME)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def _heartbeat(
    settings: Settings,
    run_id: str,
    stopped: asyncio.Event,
    progress: MonitoringProgress | None = None,
) -> None:
    while not stopped.is_set():
        if progress is None:
            _write_heartbeat(settings.state_dir, run_id)
        else:
            _write_heartbeat(settings.state_dir, run_id, settings, progress)
        try:
            async with asyncio.timeout(settings.heartbeat_interval):
                await stopped.wait()
        except TimeoutError:
            pass


async def _shutdown(
    workers: list[asyncio.Task[None]], stop_waiter: asyncio.Task[bool], timeout: float
) -> list[BaseException | None]:
    stop_waiter.cancel()
    await asyncio.gather(stop_waiter, return_exceptions=True)
    for worker in workers:
        worker.cancel()
    return await asyncio.wait_for(asyncio.gather(*workers, return_exceptions=True), timeout=timeout)


async def run(settings: Settings) -> None:
    """Run validated workers until a stop signal, cancellation, or visible failure."""
    if settings.mode not in {"idle", "monitor", "demo"}:
        raise RuntimeError("Unsupported runtime mode")
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
            store = SQLiteStore(settings.state_dir)
            await _blocking(
                lambda: store.configure_telegram(
                    None if settings.telegram is None else settings.telegram.destination,
                    datetime.now(UTC),
                )
            )
            await _blocking(
                lambda: store.configure_email(
                    () if settings.email is None else settings.email.destinations,
                    datetime.now(UTC),
                )
            )
            await _blocking(lambda: store.reconcile(settings.polled_rules, max_rules=40))
            await _blocking(
                lambda: store.reconcile_servers(
                    tuple((server.alias, server.fingerprint) for server in settings.servers)
                )
            )
            await _blocking(lambda: store.reconcile_push_sources(settings.sources))
            state_lock = asyncio.Lock()
            request_slots = asyncio.Semaphore(settings.max_concurrency)
            progress = None
            if (
                settings.mode != "idle"
                or settings.telegram is not None
                or settings.ingress is not None
                or settings.email is not None
            ):
                progress = MonitoringProgress(
                    workers={
                        site.alias: time.monotonic()
                        for site in settings.polled_sources
                        if site.enabled
                    }
                )
                if settings.telegram is not None:
                    from sentinel_bot.telegram_runtime import WORKER_NAMES

                    progress.telegram = {name: time.monotonic() for name in WORKER_NAMES}
                if settings.email is not None:
                    from sentinel_bot.email_runtime import WORKER_NAMES as EMAIL_WORKER_NAMES

                    progress.email = {name: time.monotonic() for name in EMAIL_WORKER_NAMES}
            workers = [asyncio.create_task(_heartbeat(settings, guard.run_id, stopped, progress))]
            if progress is not None and settings.sites:
                workers.append(
                    asyncio.create_task(
                        monitor_sites(
                            settings,
                            stopped,
                            progress,
                            state_lock=state_lock,
                            request_slots=request_slots,
                        )
                    )
                )
            if progress is not None and settings.servers:
                from sentinel_bot.metrics_runtime import monitor_servers

                workers.append(
                    asyncio.create_task(
                        monitor_servers(
                            settings,
                            stopped,
                            progress,
                            state_lock=state_lock,
                            request_slots=request_slots,
                        )
                    )
                )
            if progress is not None and settings.telegram is not None:
                from sentinel_bot.telegram_runtime import run_telegram

                workers.append(
                    asyncio.create_task(run_telegram(settings, stopped, progress, state_lock))
                )
            if progress is not None and settings.ingress is not None:
                from sentinel_bot.ingress import run_ingress

                workers.append(
                    asyncio.create_task(run_ingress(settings, stopped, progress, state_lock))
                )
            if progress is not None and settings.email is not None:
                from sentinel_bot.email_runtime import run_email

                workers.append(
                    asyncio.create_task(run_email(settings, stopped, progress, state_lock))
                )
            stop_waiter = asyncio.create_task(stopped.wait())
            try:
                done, _ = await asyncio.wait(
                    {*workers, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                for worker in workers:
                    if worker in done:
                        await worker
                        if not stopped.is_set():
                            raise RuntimeError("Runtime worker stopped unexpectedly")
            finally:
                stopped.set()
                results = await await_completion(
                    asyncio.create_task(_shutdown(workers, stop_waiter, settings.shutdown_timeout))
                )
                for result in results:
                    if isinstance(result, BaseException) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        raise result
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


def _current_heartbeat(
    settings: Settings, *, check_workers: bool = True
) -> dict[str, object] | None:
    """Read only a bounded heartbeat owned by the current, progressing process."""
    fd: int | None = None
    try:
        fd = os.open(settings.state_dir / LOCK_NAME, os.O_RDWR)
        if not _lock_is_held(fd):
            return None
        owner = _read_record(settings.state_dir / LOCK_NAME)
        heartbeat = _read_record(settings.state_dir / HEALTH_NAME)
        run_id = owner.get("run_id")
        pid = owner.get("pid")
        if not isinstance(run_id, str) or len(run_id) != 32:
            return None
        UUID(hex=run_id)
        if type(pid) is not int or pid <= 0:
            return None
        if heartbeat.get("run_id") != run_id or heartbeat.get("pid") != pid:
            return None
        if type(heartbeat.get("pid")) is not int or heartbeat.get("status") != settings.mode:
            return None
        progress = heartbeat.get("progress_monotonic")
        if type(progress) not in (float, int):
            return None
        if not isinstance(progress, (float, int)) or not math.isfinite(progress):
            return None
        age = time.monotonic() - progress
        if not 0 <= age <= settings.health_timeout:
            return None
        updated_at = heartbeat.get("updated_at")
        if not isinstance(updated_at, str):
            return None
        timestamp = datetime.fromisoformat(updated_at)
        if timestamp.tzinfo is None or timestamp.utcoffset() != UTC.utcoffset(timestamp):
            return None
        if settings.mode != "idle":
            workers = heartbeat.get("workers")
            if not isinstance(workers, dict) or set(workers) != {
                s.alias for s in settings.polled_sources if s.enabled
            }:
                return None
            for site in settings.polled_sources:
                if not site.enabled:
                    continue
                value = workers.get(site.alias)
                if type(value) not in (int, float) or not isinstance(value, (int, float)):
                    return None
                if check_workers and not is_fresh(
                    value, time.monotonic(), 2 * site.poll_interval + 2 * site.total_timeout
                ):
                    return None
        if settings.telegram is not None:
            from sentinel_bot.telegram_runtime import WORKER_NAMES, WORKER_TIMEOUT

            channel = heartbeat.get("telegram")
            if not isinstance(channel, dict) or set(channel) != WORKER_NAMES:
                return None
            for name in WORKER_NAMES:
                value = channel[name]
                if type(value) not in (float, int) or not isinstance(value, (float, int)):
                    return None
                if check_workers and not is_fresh(value, time.monotonic(), WORKER_TIMEOUT):
                    return None
        if settings.ingress is not None:
            value = heartbeat.get("ingress")
            if type(value) not in (float, int) or not isinstance(value, (float, int)):
                return None
            if check_workers and not is_fresh(value, time.monotonic(), settings.health_timeout):
                return None
        if settings.email is not None:
            from sentinel_bot.email_runtime import WORKER_NAMES as EMAIL_WORKER_NAMES
            from sentinel_bot.email_runtime import WORKER_TIMEOUT as EMAIL_WORKER_TIMEOUT

            channel = heartbeat.get("email")
            if not isinstance(channel, dict) or set(channel) != EMAIL_WORKER_NAMES:
                return None
            for name in EMAIL_WORKER_NAMES:
                value = channel[name]
                if type(value) not in (float, int) or not isinstance(value, (float, int)):
                    return None
                if check_workers and not is_fresh(value, time.monotonic(), EMAIL_WORKER_TIMEOUT):
                    return None
        if not _lock_is_held(fd) or _read_record(settings.state_dir / LOCK_NAME) != owner:
            return None
        return heartbeat
    except OSError, ValueError, TypeError, OverflowError:
        return None
    finally:
        if fd is not None:
            os.close(fd)


def healthcheck(settings: Settings) -> bool:
    """Check current ownership and worker progress, independently of website health."""
    return _current_heartbeat(settings) is not None


def observation_progress(settings: Settings) -> dict[str, float]:
    """Read current-run times independently of a different source's stalled worker."""
    heartbeat = _current_heartbeat(settings, check_workers=False)
    if heartbeat is None:
        return {}
    values = heartbeat.get("observations")
    if not isinstance(values, dict):
        return {}
    return {
        site.alias: float(value)
        for site in (*settings.polled_rules, *settings.servers)
        if site.enabled
        if type(value := values.get(site.alias)) in (int, float)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    }
