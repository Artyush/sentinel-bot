"""Sequential exporter polling with atomic snapshots and independent resource rules."""

import asyncio
import time
from datetime import UTC, datetime
from functools import partial

from sentinel_bot.config import ServerConfig, Settings
from sentinel_bot.http import CheckDeferred
from sentinel_bot.metric_models import (
    CpuBaseline,
    FilesystemReading,
    MetricValues,
    ServerSnapshot,
    calculate_metrics,
    classify_metric,
)
from sentinel_bot.metrics import MetricsChecker
from sentinel_bot.models import CheckResult, ErrorKind, Observation
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.policy import is_fresh
from sentinel_bot.storage import SQLiteStore
from sentinel_bot.workers import run_blocking


def rule_values(server: ServerConfig, values: MetricValues) -> dict[str, float | None]:
    """Match fixed internal rule identities without exposing configured mount paths."""
    return {
        f"{server.alias}/cpu": values.cpu_percent,
        f"{server.alias}/memory": values.memory_percent,
        **{f"{server.alias}/fs_{item.alias}": item.value for item in values.filesystems},
    }


async def _server_loop(
    server: ServerConfig,
    checker: MetricsChecker,
    store: SQLiteStore,
    lock: asyncio.Lock,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    *,
    schedule_reminders: bool = False,
) -> None:
    deadline = time.monotonic()
    baseline: CpuBaseline | None = None
    rules = server.rules
    while not stopped.is_set():
        try:
            scrape = await checker.check(server)
        except CheckDeferred:
            baseline = None
            async with lock:
                for rule in rules:
                    await run_blocking(partial(store.reset_pending, rule.alias))
        else:
            observed = time.monotonic()
            checked_at = datetime.now(UTC)
            values, baseline = calculate_metrics(
                scrape.snapshot, baseline, observed, server.freshness_limit, checked_at=checked_at
            )
            if scrape.snapshot is None:
                values = MetricValues(
                    filesystems=tuple(
                        FilesystemReading(m.alias, None, "scrape") for m in server.mounts
                    )
                )
            snapshot = ServerSnapshot(checked_at, scrape.result, values)
            readings = rule_values(server, values)
            observations: list[Observation | None] = []
            for rule in rules:
                if rule.trigger is None:
                    observations.append(Observation(checked_at, scrape.result))
                else:
                    assert rule.recover is not None
                    classified = classify_metric(
                        readings.get(rule.alias), rule.trigger, rule.recover
                    )
                    observations.append(
                        None
                        if classified is None
                        else Observation(
                            checked_at,
                            CheckResult(classified, None if classified else ErrorKind.METRICS),
                        )
                    )
            async with lock:
                for rule in rules:
                    previous = progress.observations.get(rule.alias)
                    if previous is not None and not is_fresh(
                        previous, observed, rule.freshness_limit
                    ):
                        await run_blocking(partial(store.reset_pending, rule.alias))
                await run_blocking(
                    partial(
                        store.apply_server_observation,
                        server.alias,
                        server.fingerprint,
                        snapshot,
                        rules,
                        observations,
                    )
                )
                for rule in rules:
                    valid = rule.trigger is None or readings.get(rule.alias) is not None
                    if valid:
                        progress.observations[rule.alias] = observed
                    else:
                        progress.observations.pop(rule.alias, None)
                    if schedule_reminders:
                        await run_blocking(
                            partial(
                                store.queue_reminder,
                                rule,
                                checked_at,
                                fresh=valid
                                and is_fresh(observed, time.monotonic(), rule.freshness_limit),
                            )
                        )
            progress.observations[server.alias] = observed
        completed = time.monotonic()
        progress.workers[server.alias] = completed
        deadline += server.poll_interval
        if deadline <= completed:
            deadline += (
                int((completed - deadline) // server.poll_interval) + 1
            ) * server.poll_interval
        try:
            async with asyncio.timeout(max(0, deadline - time.monotonic())):
                await stopped.wait()
        except TimeoutError:
            pass


async def monitor_servers(
    settings: Settings,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    *,
    state_lock: asyncio.Lock,
    request_slots: asyncio.Semaphore | None = None,
) -> None:
    """Supervise explicitly configured exporters; push-only inventory creates no client."""
    fixture_hosts = frozenset({"metrics-fixture"}) if settings.mode == "demo" else frozenset()
    async with MetricsChecker(
        settings.max_concurrency, fixture_hosts, semaphore=request_slots
    ) as checker:
        async with asyncio.TaskGroup() as group:
            tasks = [
                group.create_task(
                    _server_loop(
                        server,
                        checker,
                        SQLiteStore(settings.state_dir),
                        state_lock,
                        stopped,
                        progress,
                        schedule_reminders=settings.telegram is None and settings.email is None,
                    )
                )
                for server in settings.servers
                if server.enabled
            ]
            try:
                await stopped.wait()
            finally:
                for task in tasks:
                    task.cancel()
