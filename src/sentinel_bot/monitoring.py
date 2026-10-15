"""Independent, sequential website loops and serialized state commits."""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial

from sentinel_bot.config import Settings, SiteConfig
from sentinel_bot.http import CheckDeferred, HttpChecker
from sentinel_bot.models import Observation
from sentinel_bot.policy import is_fresh
from sentinel_bot.storage import SQLiteStore
from sentinel_bot.workers import run_blocking


@dataclass
class MonitoringProgress:
    """Current-run progress; restored observations never acquire fresh timestamps."""

    workers: dict[str, float] = field(default_factory=dict)
    observations: dict[str, float] = field(default_factory=dict)


async def _site_loop(
    site: SiteConfig,
    checker: HttpChecker,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
) -> None:
    deadline = time.monotonic()
    while not stopped.is_set():
        try:
            result = await checker.check(site)
        except CheckDeferred:
            async with lock:
                await run_blocking(lambda: store.reset_pending(site.alias))
        else:
            observed_monotonic = time.monotonic()
            observation = Observation(datetime.now(UTC), result)
            async with lock:
                previous = progress.observations.get(site.alias)
                if previous is not None and not is_fresh(
                    previous, observed_monotonic, site.freshness_limit
                ):
                    await run_blocking(lambda: store.reset_pending(site.alias))
                await run_blocking(partial(store.apply_observation, site, observation))
                await run_blocking(
                    partial(
                        store.queue_reminder,
                        site,
                        observation.checked_at,
                        fresh=is_fresh(observed_monotonic, time.monotonic(), site.freshness_limit),
                    )
                )
            progress.observations[site.alias] = observed_monotonic
        completed = time.monotonic()
        progress.workers[site.alias] = completed
        # Skip missed slots rather than accumulating work or overlapping checks.
        deadline += site.poll_interval
        if deadline <= completed:
            missed = int((completed - deadline) // site.poll_interval) + 1
            deadline += missed * site.poll_interval
        try:
            async with asyncio.timeout(max(0, deadline - time.monotonic())):
                await stopped.wait()
        except TimeoutError:
            pass


async def monitor_sites(
    settings: Settings, stopped: asyncio.Event, progress: MonitoringProgress
) -> None:
    """Supervise all configured sites after startup has reconciled their state."""
    store = SQLiteStore(settings.state_dir)
    state_lock = asyncio.Lock()
    fixture_hosts = frozenset({"site-fixture"}) if settings.mode == "demo" else frozenset()
    async with HttpChecker(settings.max_concurrency, fixture_hosts) as checker:
        async with asyncio.TaskGroup() as group:
            tasks = [
                group.create_task(_site_loop(site, checker, store, state_lock, stopped, progress))
                for site in settings.sites
                if site.enabled
            ]
            try:
                await stopped.wait()
            finally:
                for task in tasks:
                    task.cancel()
