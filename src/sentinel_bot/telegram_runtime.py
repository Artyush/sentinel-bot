"""Supervised Telegram polling, cached replies and durable incident delivery."""

import asyncio
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import cast

from sentinel_bot.config import Settings, TelegramSettings
from sentinel_bot.models import EventKind
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.policy import is_fresh
from sentinel_bot.storage import OutboxRecord, SQLiteStore
from sentinel_bot.telegram_api import BotIdentity, TelegramAPI, TelegramError, TelegramErrorKind
from sentinel_bot.telegram_commands import CommandGate, bound_text, parse_message, render_command
from sentinel_bot.workers import run_blocking

SEND_INTERVAL = 3.1
MAX_ATTEMPTS = 8
MAX_RETRY_DELAY = 604800.0
WORKER_NAMES = frozenset({"poll", "delivery", "replies", "reminders"})
WORKER_TIMEOUT = 45.0
POLL_CURSOR_TTL = 86400.0
_LOG = logging.getLogger(__name__)


class TelegramConfigurationError(RuntimeError):
    """Telegram cannot operate with the supplied credentials or polling ownership."""


async def _state[Result](lock: asyncio.Lock, operation: Callable[[], Result]) -> Result:
    async with lock:
        return await run_blocking(operation)


def _category(error: TelegramError) -> str:
    if error.kind is TelegramErrorKind.RATE_LIMITED:
        return "rate_limit"
    if error.kind is TelegramErrorKind.PROTOCOL:
        return "invalid_response"
    if error.kind is TelegramErrorKind.PERMANENT:
        return "forbidden" if error.status == 403 else "bad_request"
    if error.kind is TelegramErrorKind.TRANSIENT:
        return "server_error" if error.status is not None else "network"
    return error.kind.value


def _fatal(error: TelegramError) -> None:
    if error.kind in {
        TelegramErrorKind.UNAUTHORIZED,
        TelegramErrorKind.CONFLICT,
        TelegramErrorKind.WEBHOOK,
    }:
        raise TelegramConfigurationError(
            f"Telegram configuration failed: {error.kind.value}."
        ) from None


def _retry_delay(attempt: int, error: TelegramError) -> float:
    delay = min(300.0, 2.0 ** min(attempt, 9))
    delay = min(300.0, delay + random.uniform(0, min(1, delay / 10)))
    return max(delay, float(error.retry_after or 0))


async def _pause(
    delay: float, stopped: asyncio.Event, progress: MonitoringProgress, worker: str | None
) -> None:
    deadline = time.monotonic() + max(0, delay)
    while not stopped.is_set():
        if worker is None:
            progress.telegram.update({name: time.monotonic() for name in WORKER_NAMES})
        else:
            progress.telegram[worker] = time.monotonic()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            async with asyncio.timeout(min(1.0, remaining)):
                await stopped.wait()
        except TimeoutError:
            pass


@dataclass(frozen=True)
class _Reply:
    text: str
    topic_id: int | None


class _Sender:
    """Serialize sends and share Telegram's cooldown between replies and incidents."""

    def __init__(
        self,
        api: TelegramAPI,
        config: TelegramSettings,
        store: SQLiteStore,
        state_lock: asyncio.Lock,
        stopped: asyncio.Event,
        progress: MonitoringProgress,
    ) -> None:
        self.api = api
        self.config = config
        self.store = store
        self.state_lock = state_lock
        self.stopped = stopped
        self.progress = progress
        self._lock = asyncio.Lock()
        self._cooldown_lock = asyncio.Lock()
        self._next_send = 0.0
        self._paused_until = 0.0

    async def restore_pause(self) -> None:
        self._next_send = time.monotonic() + SEND_INTERVAL
        pause = await _state(self.state_lock, self.store.get_send_pause)
        if pause is not None:
            # Restart cannot prove elapsed wall time; repeat the bounded saved pause.
            self._paused_until = time.monotonic() + pause[1]

    async def _acquire(self, worker: str) -> None:
        waiter = asyncio.create_task(self._lock.acquire())
        try:
            while not waiter.done():
                self.progress.telegram[worker] = time.monotonic()
                if self.stopped.is_set():
                    raise asyncio.CancelledError
                await asyncio.wait({waiter}, timeout=1)
            await waiter
        except BaseException:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            if not waiter.cancelled() and waiter.exception() is None and waiter.result():
                self._lock.release()
            raise

    async def rate_limited(self, error: TelegramError) -> None:
        pause = float(error.retry_after or 2)
        if pause > MAX_RETRY_DELAY:
            raise TelegramConfigurationError(
                "Telegram Retry-After exceeds the supported seven-day pause."
            ) from None
        async with self._cooldown_lock:
            self._paused_until = max(self._paused_until, time.monotonic() + pause)
            remaining = min(MAX_RETRY_DELAY, max(0, self._paused_until - time.monotonic()))
            await _state(
                self.state_lock,
                partial(
                    self.store.set_send_pause,
                    datetime.now(UTC) + timedelta(seconds=remaining),
                    remaining,
                ),
            )

    async def send(self, text: str, topic_id: int | None, worker: str) -> int:
        await self._acquire(worker)
        try:
            if self.stopped.is_set():
                raise asyncio.CancelledError
            while True:
                async with self._cooldown_lock:
                    delay = max(self._next_send, self._paused_until) - time.monotonic()
                    if delay <= 0:
                        if self._paused_until:
                            await _state(self.state_lock, self.store.clear_send_pause)
                            self._paused_until = 0.0
                        break
                await _pause(delay, self.stopped, self.progress, worker)
                if self.stopped.is_set():
                    raise asyncio.CancelledError
            self._next_send = time.monotonic() + SEND_INTERVAL
            self.progress.telegram[worker] = time.monotonic()
            try:
                result = await self.api.send_message(self.config.chat_id, text, topic_id=topic_id)
            except TelegramError as error:
                if error.kind is TelegramErrorKind.RATE_LIMITED:
                    await self.rate_limited(error)
                raise
            self.progress.telegram[worker] = time.monotonic()
            return result
        finally:
            self._lock.release()


async def _startup(
    api: TelegramAPI,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
) -> BotIdentity | None:
    attempt = 0
    while not stopped.is_set():
        progress.telegram.update({name: time.monotonic() for name in WORKER_NAMES})
        try:
            identity = await api.startup()
        except TelegramError as error:
            _fatal(error)
            if not error.retryable:
                raise TelegramConfigurationError("Telegram startup was rejected.") from None
            attempt = min(attempt + 1, 9)
            delay = _retry_delay(attempt, error)
            if delay > MAX_RETRY_DELAY:
                raise TelegramConfigurationError(
                    "Telegram startup retry delay is too large."
                ) from None
            await _state(lock, partial(store.set_telegram_error, _category(error)))
            _LOG.warning("Telegram startup unavailable: %s", _category(error))
            await _pause(delay, stopped, progress, None)
        else:
            await _state(lock, partial(store.set_telegram_error, None))
            return identity
    return None


async def _poll(
    api: TelegramAPI,
    identity: BotIdentity,
    settings: Settings,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    replies: asyncio.Queue[_Reply],
    sender: _Sender,
) -> None:
    from sentinel_bot.status import status_snapshot

    config = settings.telegram
    assert config is not None
    gate = CommandGate(config, identity.username)
    offset, received_at = await _state(lock, partial(store.get_poll_checkpoint, identity.id))
    age = None if received_at is None else (datetime.now(UTC) - received_at).total_seconds()
    remaining = (
        POLL_CURSOR_TTL if age is None else (max(0, POLL_CURSOR_TTL - age) if age >= 0 else 0)
    )
    cursor_deadline = time.monotonic() + remaining
    failures = 0
    while not stopped.is_set():
        progress.telegram["poll"] = time.monotonic()
        if offset and time.monotonic() >= cursor_deadline:
            # Telegram retains updates for 24h and can randomize IDs after a quiet week.
            await _state(lock, partial(store.reset_poll_cursor, identity.id))
            offset = 0
        try:
            updates = await api.get_updates(offset)
        except TelegramError as error:
            _fatal(error)
            if not error.retryable:
                raise TelegramConfigurationError("Telegram polling was rejected.") from None
            failures = min(failures + 1, 9)
            delay = _retry_delay(failures, error)
            if delay > MAX_RETRY_DELAY:
                raise TelegramConfigurationError(
                    "Telegram polling retry delay is too large."
                ) from None
            if error.kind is TelegramErrorKind.RATE_LIMITED:
                await sender.rate_limited(error)
            await _state(lock, partial(store.set_telegram_error, _category(error)))
            _LOG.warning("Telegram polling unavailable: %s", _category(error))
            await _pause(delay, stopped, progress, "poll")
            continue
        failures = 0
        await _state(lock, partial(store.set_telegram_error, None))
        for update in sorted(updates, key=lambda item: cast(int, item["update_id"])):
            update_id = update["update_id"]
            assert type(update_id) is int
            if update_id < offset:
                continue
            message = parse_message(update)
            command = None if message is None else gate.accept(message, now=time.monotonic())
            reply = None
            if command is not None and message is not None and not replies.full():
                snapshot = await _state(lock, partial(status_snapshot, settings))
                reply = _Reply(
                    render_command(command, snapshot, now=datetime.now(UTC)), message.topic_id
                )
            offset = update_id + 1
            await _state(
                lock,
                partial(store.set_poll_cursor, identity.id, offset, received_at=datetime.now(UTC)),
            )
            cursor_deadline = time.monotonic() + POLL_CURSOR_TTL
            if reply is not None:
                replies.put_nowait(reply)
        progress.telegram["poll"] = time.monotonic()
        if not updates:
            await _pause(0.2, stopped, progress, "poll")


def _notification(record: OutboxRecord) -> str:
    event = record.event
    if event.kind in {EventKind.NOTIFICATION, EventKind.FIRING, EventKind.RESOLVED}:
        assert event.received_at is not None
        return bound_text(
            f"{event.kind.value}: {record.source_alias}\n"
            f"Type: {event.event_type}; severity: {event.severity}\n"
            f"{event.summary}\n"
            f"Occurrence: {event.incident_id}\n"
            f"Event time (UTC): {event.occurred_at.astimezone(UTC).isoformat()}\n"
            f"Received time (UTC): {event.received_at.astimezone(UTC).isoformat()}\n"
            "Source-reported event; current status may have changed."
        )
    return bound_text(
        f"{event.kind.value}: {record.source_alias}\n"
        f"Incident: {event.incident_id}\n"
        f"Event time (UTC): {event.occurred_at.astimezone(UTC).isoformat()}\n"
        f"Incident started (UTC): {event.started_at.astimezone(UTC).isoformat()}\n"
        "Queued monitoring event; current status may have changed."
    )


async def _deliver(
    sender: _Sender,
    settings: Settings,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    reminders: dict[str, tuple[str, datetime, float]],
) -> None:
    config = settings.telegram
    assert config is not None
    retries: dict[str, float] = {}
    sites = {site.alias: site for site in settings.polled_rules}
    while not stopped.is_set():
        progress.telegram["delivery"] = time.monotonic()
        pending = await _state(lock, store.pending_notifications)
        active_ids = {item.id for item in pending}
        retries = {key: value for key, value in retries.items() if key in active_ids}
        ready: list[str] = []
        for item in pending:
            if item.destination != config.destination or item.in_flight:
                continue
            if item.attempts >= MAX_ATTEMPTS:
                await _state(
                    lock,
                    partial(store.fail_delivery, item.id, datetime.now(UTC), "retries_exhausted"),
                )
                continue
            if item.id not in retries:
                retries[item.id] = time.monotonic() + float(item.retry_delay_seconds or 0)
            if retries[item.id] <= time.monotonic():
                ready.append(item.id)
        record = (
            None
            if not ready
            else await _state(
                lock, partial(store.reserve_due_delivery, datetime.now(UTC), ready_ids=ready)
            )
        )
        if record is None:
            await _pause(0.2, stopped, progress, "delivery")
            continue
        try:
            await sender.send(_notification(record), config.topic_id, "delivery")
        except TelegramError as error:
            _fatal(error)
            category = _category(error)
            if not error.retryable or record.attempts >= MAX_ATTEMPTS:
                if error.retryable:
                    category = "retries_exhausted"
                await _state(
                    lock, partial(store.fail_delivery, record.id, datetime.now(UTC), category)
                )
            else:
                delay = _retry_delay(record.attempts, error)
                retries[record.id] = time.monotonic() + delay
                await _state(
                    lock,
                    partial(
                        store.retry_delivery,
                        record.id,
                        datetime.now(UTC) + timedelta(seconds=delay),
                        category,
                        delay_seconds=delay,
                    ),
                )
            _LOG.warning("Telegram incident delivery failed: %s", category)
        else:
            accepted_at = datetime.now(UTC)
            accepted_monotonic = time.monotonic()
            await _state(lock, partial(store.acknowledge, record.id, accepted_at))
            site = sites.get(record.source_alias)
            if site is not None and record.event.kind in {EventKind.DOWN, EventKind.REMINDER}:
                reminders[site.alias] = (
                    record.event.incident_id,
                    accepted_at,
                    accepted_monotonic + site.reminder_interval,
                )
        progress.telegram["delivery"] = time.monotonic()


async def _reply_worker(
    sender: _Sender,
    replies: asyncio.Queue[_Reply],
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
) -> None:
    while not stopped.is_set():
        progress.telegram["replies"] = time.monotonic()
        try:
            async with asyncio.timeout(1):
                reply = await replies.get()
        except TimeoutError:
            continue
        try:
            for attempt in range(1, 4):
                try:
                    await sender.send(reply.text, reply.topic_id, "replies")
                except TelegramError as error:
                    _fatal(error)
                    await _state(lock, partial(store.set_telegram_error, _category(error)))
                    if not error.retryable or attempt == 3:
                        break
                    await _pause(_retry_delay(attempt, error), stopped, progress, "replies")
                else:
                    break
        finally:
            replies.task_done()


async def _reminder_worker(
    settings: Settings,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    deadlines: dict[str, tuple[str, datetime, float]],
) -> None:
    config = settings.telegram
    assert config is not None
    sites = {site.alias: site for site in settings.polled_rules}
    while not stopped.is_set():
        progress.telegram["reminders"] = time.monotonic()
        records = await _state(lock, store.reminder_progress)
        aliases = {
            record.source_alias for record in records if record.destination == config.destination
        }
        for alias in set(deadlines) - aliases:
            del deadlines[alias]
        for record in records:
            site = sites.get(record.source_alias)
            if site is None or record.destination != config.destination:
                continue
            previous = deadlines.get(site.alias)
            signature = (record.incident_id, record.last_delivered_at)
            if previous is None or previous[:2] != signature:
                deadlines[site.alias] = (*signature, time.monotonic() + site.reminder_interval)
            deadline = deadlines[site.alias][2]
            if time.monotonic() >= deadline:
                fresh = is_fresh(
                    progress.observations.get(site.alias), time.monotonic(), site.freshness_limit
                )
                await _state(
                    lock, partial(store.queue_reminder, site, datetime.now(UTC), fresh, due=True)
                )
        await _pause(0.5, stopped, progress, "reminders")


async def run_telegram(
    settings: Settings,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    state_lock: asyncio.Lock,
) -> None:
    """Keep source checks running through expected channel outages; supervise all workers."""
    config = settings.telegram
    assert config is not None
    if (settings.mode == "demo") != (config.provider == "demo"):
        raise TelegramConfigurationError("Demo mode requires the synthetic Telegram provider.")
    store = SQLiteStore(settings.state_dir)
    api = (
        TelegramAPI(config.token, demo=True)
        if config.provider == "demo"
        else TelegramAPI(config.token)
    )
    async with api:
        identity = await _startup(api, store, state_lock, stopped, progress)
        if identity is None:
            return
        sender = _Sender(api, config, store, state_lock, stopped, progress)
        await sender.restore_pause()
        replies: asyncio.Queue[_Reply] = asyncio.Queue(maxsize=20)
        reminder_deadlines: dict[str, tuple[str, datetime, float]] = {}
        try:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(
                        _poll(
                            api,
                            identity,
                            settings,
                            store,
                            state_lock,
                            stopped,
                            progress,
                            replies,
                            sender,
                        )
                    ),
                    group.create_task(
                        _deliver(
                            sender,
                            settings,
                            store,
                            state_lock,
                            stopped,
                            progress,
                            reminder_deadlines,
                        )
                    ),
                    group.create_task(
                        _reply_worker(sender, replies, store, state_lock, stopped, progress)
                    ),
                    group.create_task(
                        _reminder_worker(
                            settings, store, state_lock, stopped, progress, reminder_deadlines
                        )
                    ),
                ]
                try:
                    await stopped.wait()
                finally:
                    for task in tasks:
                        task.cancel()
        except ExceptionGroup as errors:
            expected, unexpected = errors.split(TelegramConfigurationError)
            if unexpected is not None:
                raise
            assert expected is not None
            first: BaseException = expected
            while isinstance(first, BaseExceptionGroup):
                first = first.exceptions[0]
            raise first from None
