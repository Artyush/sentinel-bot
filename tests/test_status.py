import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import sentinel_bot.runtime as runtime
from sentinel_bot.__main__ import main
from sentinel_bot.config import Settings, SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind, Observation
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.status import status_snapshot
from sentinel_bot.storage import DATABASE_NAME, SQLiteStore, initialize_state

START = datetime(2026, 1, 1, tzinfo=UTC)


def prepare(tmp_path: Path) -> tuple[Settings, SQLiteStore]:
    site = SiteConfig("website", "https://example.test/private-path")
    settings = Settings(tmp_path, mode="monitor", sites=(site,))
    initialize_state(tmp_path, START)
    store = SQLiteStore(tmp_path)
    store.reconcile(settings.sites)
    for seconds in range(3):
        store.apply_observation(
            site,
            Observation(
                START + timedelta(seconds=seconds),
                CheckResult(False, ErrorKind.HTTP_STATUS, http_status=503, latency_ms=12.5),
            ),
        )
    return settings, store


def test_stopped_status_preserves_incident_and_marks_cached_reading_stale(
    tmp_path: Path,
) -> None:
    settings, store = prepare(tmp_path)
    result = status_snapshot(settings)
    assert result["mode"] == "monitor"
    assert result["pending_notifications"] == 1
    assert result["sites"] == [
        {
            "alias": "website",
            "state": "DOWN",
            "fresh": False,
            "incident_id": store.read_site("website").state.incident_id,
            "incident_started_at": START.isoformat(),
            "observation": {
                "checked_at": (START + timedelta(seconds=2)).isoformat(),
                "success": False,
                "error": "http_status",
                "http_status": 503,
                "latency_ms": 12.5,
            },
        }
    ]
    rendered = json.dumps(result)
    assert "https://" not in rendered
    assert "example.test" not in rendered
    assert "private-path" not in rendered


@pytest.mark.parametrize("age", [69.999, 70, 70.001])
def test_freshness_uses_current_owner_observation_progress_at_exact_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age: float
) -> None:
    settings, _ = prepare(tmp_path)
    monkeypatch.setattr("time.monotonic", lambda: 1000)
    progress = MonitoringProgress(workers={"website": 1000}, observations={"website": 1000 - age})
    with runtime._InstanceGuard(tmp_path) as owner:
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        result = status_snapshot(settings)
        assert result["sites"][0]["fresh"] is (age <= settings.sites[0].freshness_limit)
        assert result["sites"][0]["state"] == "DOWN"
    assert status_snapshot(settings)["sites"][0]["fresh"] is False


def test_current_owner_without_post_start_reading_keeps_snapshot_stale(tmp_path: Path) -> None:
    settings, _ = prepare(tmp_path)
    with runtime._InstanceGuard(tmp_path) as owner:
        progress = MonitoringProgress(workers={"website": runtime.time.monotonic()})
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        assert runtime.healthcheck(settings)
        site = status_snapshot(settings)["sites"][0]
        assert site["fresh"] is False
        assert site["state"] == "DOWN"
        assert site["observation"]["error"] == "http_status"


def test_unmatched_source_configuration_is_unknown_without_obsolete_snapshot(
    tmp_path: Path,
) -> None:
    settings, _ = prepare(tmp_path)
    changed = replace(settings, sites=(replace(settings.sites[0], marker="changed"),))
    site = status_snapshot(changed)["sites"][0]
    assert site["state"] == "UNKNOWN"
    assert site["fresh"] is False
    assert site["observation"] is None
    assert site["incident_id"] is None


def test_configured_source_missing_from_database_remains_unknown(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    store.reconcile(())
    result = status_snapshot(settings)
    assert result["sites"][0]["state"] == "UNKNOWN"
    assert result["sites"][0]["observation"] is None
    assert result["pending_notifications"] == 0


def test_missing_latency_and_status_remain_null(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    store.apply_observation(
        settings.sites[0],
        Observation(START + timedelta(seconds=10), CheckResult(False, ErrorKind.DNS)),
    )
    observed = status_snapshot(settings)["sites"][0]["observation"]
    assert observed["latency_ms"] is None
    assert observed["http_status"] is None
    assert observed["error"] == "dns"


def test_status_cli_reads_cached_json_without_network_or_startup_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    settings, _ = prepare(tmp_path)
    config = tmp_path / "monitor.toml"
    config.write_text(
        '[runtime]\nmode = "monitor"\n[[sites]]\n'
        'alias = "website"\nurl = "https://example.test/private-path"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("SENTINEL_STATE_DIR", str(tmp_path))

    def forbidden_connection(*args: object, **kwargs: object) -> None:
        pytest.fail("Cached status must not create a network connection")

    monkeypatch.setattr("socket.create_connection", forbidden_connection)
    monkeypatch.setattr("socket.socket.connect", forbidden_connection)
    before_database = (tmp_path / DATABASE_NAME).read_bytes()
    assert main(["--config", str(config), "status"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert json.loads(output.out) == status_snapshot(settings)
    assert (tmp_path / DATABASE_NAME).read_bytes() == before_database
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (1,)


def test_status_before_initialization_has_safe_error_without_creating_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "idle.toml"
    config.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")
    state = tmp_path / "missing-state"
    monkeypatch.setenv("SENTINEL_STATE_DIR", str(state))
    assert main(["--config", str(config), "status"]) == 1
    output = capsys.readouterr()
    assert "Status error:" in output.err
    assert "Traceback" not in output.err
    assert output.out == ""
    assert not state.exists()


def test_stalled_worker_does_not_hide_another_sources_fresh_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    other = replace(settings.sites[0], alias="other", url="https://example.test/other")
    settings = replace(settings, sites=(*settings.sites, other))
    store.reconcile(settings.sites)
    store.apply_observation(other, Observation(START + timedelta(seconds=10), CheckResult(True)))
    monkeypatch.setattr("time.monotonic", lambda: 1000)
    progress = MonitoringProgress(
        workers={"website": 0, "other": 1000},
        observations={"website": 0, "other": 1000},
    )
    with runtime._InstanceGuard(tmp_path) as owner:
        runtime._write_heartbeat(tmp_path, owner.run_id, settings, progress)
        assert not runtime.healthcheck(settings)
        sites = status_snapshot(settings)["sites"]
        assert sites[0]["state"] == "DOWN"
        assert sites[0]["fresh"] is False
        assert sites[1]["state"] == "UP"
        assert sites[1]["fresh"] is True
