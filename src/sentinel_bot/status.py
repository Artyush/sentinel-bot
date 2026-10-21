"""Cached operator status without starting checks or exposing source URLs."""

import time

from sentinel_bot.config import Settings
from sentinel_bot.metric_models import MetricValues
from sentinel_bot.policy import is_fresh
from sentinel_bot.runtime import observation_progress
from sentinel_bot.storage import SQLiteStore


def status_snapshot(settings: Settings) -> dict[str, object]:
    """Keep confirmed incident state separate from current-run freshness."""
    store = SQLiteStore(settings.state_dir)
    records = {record.alias: record for record in store.read_sites()}
    progress = observation_progress(settings)
    now = time.monotonic()
    sites: list[dict[str, object]] = []
    source_modes = {source.alias: source.mode for source in settings.sources}
    email: dict[str, object] = {
        "enabled": settings.email is not None,
        **store.email_delivery_status(),
    }
    email["recipients"] = (
        []
        if settings.email is None
        else [
            {"recipient": f"recipient_{index}", **store.email_delivery_status(destination)}
            for index, destination in enumerate(settings.email.destinations, start=1)
        ]
    )
    for site in settings.sites:
        if not site.enabled:
            continue
        record = records.get(site.alias)
        if record is None or record.fingerprint != site.fingerprint:
            sites.append(
                {
                    "alias": site.alias,
                    "state": "UNKNOWN",
                    "fresh": False,
                    "incident_id": None,
                    "incident_started_at": None,
                    "observation": None,
                }
            )
            continue
        observation = record.observation
        sites.append(
            {
                "alias": site.alias,
                "state": record.state.confirmed.value,
                "fresh": observation is not None
                and is_fresh(progress.get(site.alias), now, site.freshness_limit),
                "incident_id": record.state.incident_id,
                "incident_started_at": None
                if record.state.incident_started_at is None
                else record.state.incident_started_at.isoformat(),
                "observation": None
                if observation is None
                else {
                    "checked_at": observation.checked_at.isoformat(),
                    "success": observation.result.success,
                    "error": None
                    if observation.result.error is None
                    else observation.result.error.value,
                    "http_status": observation.result.http_status,
                    "latency_ms": observation.result.latency_ms,
                },
            }
        )
    snapshots = {record.alias: record for record in store.read_server_snapshots()}
    servers: list[dict[str, object]] = []
    for server in settings.servers:
        cached = snapshots.get(server.alias)
        snapshot = (
            None if cached is None or cached.fingerprint != server.fingerprint else cached.snapshot
        )
        values = MetricValues() if snapshot is None else snapshot.values
        current_values = {
            f"{server.alias}/cpu": values.cpu_percent,
            f"{server.alias}/memory": values.memory_percent,
            **{f"{server.alias}/fs_{item.alias}": item.value for item in values.filesystems},
        }
        rule_rows: list[dict[str, object]] = []
        for rule in server.rules:
            record = records.get(rule.alias)
            matching = record is not None and record.fingerprint == rule.fingerprint
            state = record.state if matching and record is not None else None
            rule_rows.append(
                {
                    "name": rule.alias.split("/", 1)[1],
                    "state": "UNKNOWN" if state is None else state.confirmed.value,
                    "fresh": matching
                    and (rule.trigger is None or current_values.get(rule.alias) is not None)
                    and is_fresh(progress.get(rule.alias), now, rule.freshness_limit),
                    "incident_id": None if state is None else state.incident_id,
                }
            )
        availability = next(
            (row["state"] for row in rule_rows if row["name"] == "exporter"), "UNKNOWN"
        )
        servers.append(
            {
                "alias": server.alias,
                "availability": availability,
                "fresh": snapshot is not None
                and is_fresh(progress.get(server.alias), now, server.freshness_limit),
                "checked_at": None if snapshot is None else snapshot.checked_at.isoformat(),
                "error": None
                if snapshot is None or snapshot.result.error is None
                else snapshot.result.error.value,
                "cpu_percent": values.cpu_percent,
                "memory_percent": values.memory_percent,
                "uptime_seconds": values.uptime_seconds,
                "filesystems": [
                    {"alias": item.alias, "used_percent": item.value, "error": item.error}
                    for item in values.filesystems
                ],
                "rules": rule_rows,
            }
        )
    return {
        "mode": settings.mode,
        "sites": sites,
        "servers": servers,
        "pending_notifications": store.pending_count(),
        "telegram": {"enabled": settings.telegram is not None, **store.delivery_status()},
        "email": email,
        "ingress": {"enabled": settings.ingress is not None},
        "sources": [
            {
                **source_status,
                "mode": source_modes[str(source_status["alias"])],
                "availability": "UNKNOWN",
            }
            for source_status in store.incoming_status(tuple(source_modes))
        ],
    }
