"""Supervised email delivery and recipient-specific website reminders."""

import asyncio
import logging
import math
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial

from sentinel_bot.config import Settings
from sentinel_bot.email_api import EmailError, EmailTransport, LocalSMTPEmailAPI, MailchimpEmailAPI
from sentinel_bot.models import EventKind
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.policy import is_fresh
from sentinel_bot.storage import OutboxRecord, SQLiteStore
from sentinel_bot.workers import run_blocking

SEND_INTERVAL = 1.0
MAX_ATTEMPTS = 8
MAX_RETRY_DELAY = 604800.0
WORKER_NAMES = frozenset({"delivery", "reminders"})
WORKER_TIMEOUT = 45.0
_LOG = logging.getLogger(__name__)
type ReminderDeadlines = dict[tuple[str, str], tuple[str, datetime, float]]


async def _state[Result](lock: asyncio.Lock, operation: Callable[[], Result]) -> Result:
    async with lock:
        return await run_blocking(operation)


async def _pause(
    seconds: float,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    worker: str = "delivery",
) -> None:
    progress.email[worker] = time.monotonic()
    try:
        async with asyncio.timeout(seconds):
            await stopped.wait()
    except TimeoutError:
        pass


def _retry_delay(attempt: int, error: EmailError) -> float:
    if error.retry_after is not None:
        return error.retry_after
    base = min(300.0, 2.0 ** min(attempt - 1, 9))
    return min(300.0, base + random.uniform(0, base / 2))


def render_email(record: OutboxRecord) -> tuple[str, str]:
    """Render fixed headers and bounded event values without exposing source URLs."""
    event = record.event
    subject = f"[Sentinel] {event.kind.value}: {record.source_alias}"
    lines = [subject, f"Notification: {record.id}", f"Occurrence: {event.incident_id}"]
    if event.kind in {EventKind.NOTIFICATION, EventKind.FIRING, EventKind.RESOLVED}:
        assert event.received_at is not None
        lines.extend(
            [
                f"Type: {event.event_type}; severity: {event.severity}",
                event.summary or "",
                f"Received time (UTC): {event.received_at.astimezone(UTC).isoformat()}",
            ]
        )
    lines.extend(
        [
            f"Event time (UTC): {event.occurred_at.astimezone(UTC).isoformat()}",
            f"Incident started (UTC): {event.started_at.astimezone(UTC).isoformat()}",
            "Queued technical event; current status may have changed.",
        ]
    )
    return subject, "\n".join(lines) + "\n"


async def _deliver(
    api: EmailTransport,
    settings: Settings,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    reminders: ReminderDeadlines,
) -> None:
    config = settings.email
    assert config is not None
    saved_pause = await _state(lock, store.get_email_pause)
    restored = await _state(lock, store.email_pending_schedule)
    restored_at = time.monotonic()
    pause_until = 0.0 if saved_pause is None else restored_at + saved_pause[1]
    pause_saved = saved_pause is not None
    suspended = False
    next_send = time.monotonic() + SEND_INTERVAL
    retries = {item.id: restored_at + float(item.retry_delay_seconds or 0) for item in restored}
    sites = {site.alias: site for site in settings.polled_rules}
    destinations = frozenset(config.destinations)
    while not stopped.is_set():
        progress.email["delivery"] = time.monotonic()
        if suspended:
            await _pause(0.2, stopped, progress)
            continue
        now = time.monotonic()
        if now < max(pause_until, next_send):
            await _pause(min(0.2, max(pause_until, next_send) - now), stopped, progress)
            continue
        if pause_saved:
            await _state(lock, store.clear_email_pause)
            pause_saved = False
        pending = await _state(lock, store.email_pending_schedule)
        active_ids = {record.id for record in pending}
        retries = {key: value for key, value in retries.items() if key in active_ids}
        ready: list[str] = []
        for item in pending:
            if item.destination not in destinations or item.in_flight:
                continue
            if item.attempts >= MAX_ATTEMPTS:
                await _state(
                    lock,
                    partial(
                        store.fail_email_delivery, item.id, datetime.now(UTC), "retries_exhausted"
                    ),
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
                lock, partial(store.reserve_due_email_delivery, datetime.now(UTC), ready_ids=ready)
            )
        )
        if record is None:
            await _pause(0.2, stopped, progress)
            continue
        next_send = time.monotonic() + SEND_INTERVAL
        try:
            subject, body = render_email(record)
            await api.send(config.recipient_for(record.destination), subject, body)
        except EmailError as error:
            category = error.category
            delay = _retry_delay(record.attempts, error)
            if error.channel_wide and not error.retryable:
                if record.attempts >= MAX_ATTEMPTS:
                    await _state(
                        lock,
                        partial(
                            store.fail_email_delivery,
                            record.id,
                            datetime.now(UTC),
                            "retries_exhausted",
                        ),
                    )
                else:
                    await _state(
                        lock,
                        partial(
                            store.retry_email_delivery,
                            record.id,
                            datetime.now(UTC),
                            category,
                            delay_seconds=0,
                        ),
                    )
                suspended = True
                await _state(lock, partial(store.set_email_suspended, True))
            elif not math.isfinite(delay) or not 0 <= delay <= MAX_RETRY_DELAY:
                category = "invalid_response"
                await _state(
                    lock, partial(store.fail_email_delivery, record.id, datetime.now(UTC), category)
                )
            elif not error.retryable or record.attempts >= MAX_ATTEMPTS:
                if error.retryable:
                    category = "retries_exhausted"
                await _state(
                    lock, partial(store.fail_email_delivery, record.id, datetime.now(UTC), category)
                )
            else:
                retries[record.id] = time.monotonic() + delay
                await _state(
                    lock,
                    partial(
                        store.retry_email_delivery,
                        record.id,
                        datetime.now(UTC) + timedelta(seconds=delay),
                        category,
                        delay_seconds=delay,
                    ),
                )
            if (
                error.channel_wide
                and error.retryable
                and math.isfinite(delay)
                and 0 <= delay <= MAX_RETRY_DELAY
            ):
                pause_until = time.monotonic() + delay
                pause_saved = True
                await _state(
                    lock,
                    partial(
                        store.set_email_pause,
                        datetime.now(UTC) + timedelta(seconds=delay),
                        delay,
                    ),
                )
            await _state(lock, partial(store.set_email_error, category))
            _LOG.warning("Email delivery failed: %s", category)
        else:
            accepted_at = datetime.now(UTC)
            accepted_monotonic = time.monotonic()
            await _state(lock, partial(store.acknowledge_email, record.id, accepted_at))
            await _state(lock, partial(store.set_email_error, None))
            site = sites.get(record.source_alias)
            if site is not None and record.event.kind in {EventKind.DOWN, EventKind.REMINDER}:
                reminders[(site.alias, record.destination)] = (
                    record.event.incident_id,
                    accepted_at,
                    accepted_monotonic + site.reminder_interval,
                )


async def _reminders(
    settings: Settings,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    deadlines: ReminderDeadlines,
) -> None:
    config = settings.email
    assert config is not None
    sites = {site.alias: site for site in settings.polled_rules}
    while not stopped.is_set():
        progress.email["reminders"] = time.monotonic()
        records = await _state(lock, store.email_reminder_progress)
        active = {
            (record.source_alias, record.destination)
            for record in records
            if record.destination in config.destinations and record.source_alias in sites
        }
        for key in set(deadlines) - active:
            del deadlines[key]
        for record in records:
            key = (record.source_alias, record.destination)
            if key not in active:
                continue
            site = sites[record.source_alias]
            signature = (record.incident_id, record.last_delivered_at)
            previous = deadlines.get(key)
            if previous is None or previous[:2] != signature:
                deadlines[key] = (*signature, time.monotonic() + site.reminder_interval)
            if time.monotonic() >= deadlines[key][2]:
                fresh = is_fresh(
                    progress.observations.get(site.alias), time.monotonic(), site.freshness_limit
                )
                await _state(
                    lock,
                    partial(
                        store.queue_email_reminder,
                        site,
                        record.destination,
                        datetime.now(UTC),
                        fresh,
                        due=True,
                    ),
                )
        await _pause(0.5, stopped, progress, "reminders")


async def run_email(
    settings: Settings,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    state_lock: asyncio.Lock,
) -> None:
    """Deliver each recipient independently without pausing checks or Telegram."""
    config = settings.email
    assert config is not None
    api = (
        MailchimpEmailAPI(
            config.api_key, config.from_address, request_timeout=config.request_timeout
        )
        if config.provider == "mailchimp"
        else LocalSMTPEmailAPI(
            config.from_address,
            host=config.local_host,
            port=config.local_port,
            request_timeout=config.request_timeout,
        )
    )
    store = SQLiteStore(settings.state_dir)
    async with api:
        deadlines: ReminderDeadlines = {}
        async with asyncio.TaskGroup() as group:
            tasks = [
                group.create_task(
                    _deliver(api, settings, store, state_lock, stopped, progress, deadlines)
                ),
                group.create_task(
                    _reminders(settings, store, state_lock, stopped, progress, deadlines)
                ),
            ]
            try:
                await stopped.wait()
            finally:
                for task in tasks:
                    task.cancel()
