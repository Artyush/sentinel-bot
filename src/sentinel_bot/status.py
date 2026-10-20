"""Cached operator status without starting checks or exposing source URLs."""

import time

from sentinel_bot.config import Settings
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
    return {
        "mode": settings.mode,
        "sites": sites,
        "pending_notifications": store.pending_count(),
        "telegram": {"enabled": settings.telegram is not None, **store.delivery_status()},
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
