import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sentinel_bot.config import IngressSettings, PushSource
from sentinel_bot.events import IncomingEvent
from sentinel_bot.metric_models import FilesystemReading, IncidentRule, MetricValues, ServerSnapshot
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.storage import (
    MAX_PENDING_NOTIFICATIONS,
    SQLiteStore,
    StorageError,
    initialize_state,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
ALIAS = "linux"
FINGERPRINT = "synthetic-server-v1"
TELEGRAM = "telegram:-100123:0"
FIRST = "email:" + "a" * 64
SECOND = "email:" + "b" * 64
THIRD = "email:" + "c" * 64
RULES = (
    IncidentRule("linux/exporter", "exporter-v1", failure_confirmation=1, recovery_confirmation=1),
    IncidentRule("linux/cpu", "cpu-v1", failure_confirmation=1, recovery_confirmation=1),
    IncidentRule("linux/memory", "memory-v1", failure_confirmation=1, recovery_confirmation=1),
    IncidentRule("linux/fs_root", "filesystem-v1", failure_confirmation=1, recovery_confirmation=1),
)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    initialize_state(tmp_path, NOW)
    result = SQLiteStore(tmp_path)
    result.configure_telegram(TELEGRAM, NOW)
    result.configure_email((FIRST, SECOND), NOW)
    result.reconcile(RULES)
    result.reconcile_servers(((ALIAS, FINGERPRINT),))
    return result


def snapshot(seconds: int = 0, *, success: bool = True) -> ServerSnapshot:
    return ServerSnapshot(
        NOW + timedelta(seconds=seconds),
        CheckResult(success, None if success else ErrorKind.CONNECTION, latency_ms=1.5),
        MetricValues(95.0, 40.0, 1200.0, (FilesystemReading("root", 95.0),))
        if success
        else MetricValues(filesystems=(FilesystemReading("root", None, "unavailable"),)),
    )


def apply(
    store: SQLiteStore,
    outcomes: tuple[bool | None, ...] = (True, False, True, False),
    seconds: int = 0,
    *,
    rules: tuple[IncidentRule, ...] = RULES,
    reading: ServerSnapshot | None = None,
) -> None:
    reading = snapshot(seconds) if reading is None else reading
    observations = tuple(
        None
        if outcome is None
        else Observation(
            reading.checked_at,
            CheckResult(outcome, None if outcome else ErrorKind.CONNECTION),
        )
        for outcome in outcomes
    )
    store.apply_server_observation(ALIAS, FINGERPRINT, reading, rules, observations)


def query(store: SQLiteStore, sql: str) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(store.database)) as connection:
        return connection.execute(sql).fetchall()


def test_schema_five_upgrade_preserves_both_channels_receipts_and_frozen_routes(
    store: SQLiteStore,
) -> None:
    apply(store)
    first = store.reserve_due_email_delivery(NOW)
    assert first is not None
    store.acknowledge_email(first.id, NOW)
    source = PushSource("detector", frozenset({"security.attack"}), "x" * 32)
    receipt = store.accept_event(
        source,
        IncomingEvent(
            str(uuid4()), NOW, "firing", "security.attack", "warning", "Synthetic report", "key"
        ),
        NOW,
        IngressSettings(),
    )
    tables = (
        "sites",
        "notification_outbox",
        "email_outbox",
        "email_site_routes",
        "email_push_routes",
        "email_progress",
        "event_receipts",
        "pushed_incidents",
    )
    before = {table: query(store, f"SELECT * FROM {table}") for table in tables}
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("DROP TABLE server_snapshots")
        connection.execute("PRAGMA user_version = 5")
    metadata = initialize_state(store.database.parent, NOW + timedelta(days=1))
    assert metadata.schema_version == 6 and metadata.startup_count == 2
    assert metadata.first_started_at == NOW.isoformat()
    assert {table: query(store, f"SELECT * FROM {table}") for table in tables} == before
    assert store.read_server_snapshots() == ()
    assert store.email_delivery_status()["accepted"] == 1
    assert receipt.outcome == "opened"


def test_failed_schema_six_migration_rolls_back_ddl_metadata_and_user_version(
    store: SQLiteStore,
) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("DROP TABLE server_snapshots")
        connection.execute("PRAGMA user_version = 5")
        connection.execute(
            "CREATE TRIGGER reject_startup BEFORE UPDATE ON runtime_metadata "
            "BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END"
        )
    before = query(store, "SELECT * FROM runtime_metadata")
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(store.database.parent, NOW + timedelta(days=1))
    assert query(store, "PRAGMA user_version") == [(5,)]
    assert query(store, "SELECT * FROM runtime_metadata") == before
    assert query(store, "SELECT name FROM sqlite_master WHERE name='server_snapshots'") == []


def test_missing_schema_six_table_fails_startup_instead_of_silent_recreation(
    store: SQLiteStore,
) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("DROP TABLE server_snapshots")
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(store.database.parent, NOW)


def test_latest_server_snapshot_and_all_rules_fan_out_atomically(store: SQLiteStore) -> None:
    apply(store)
    record = store.read_server_snapshot(ALIAS)
    assert record is not None and record.snapshot == snapshot()
    assert store.read_server_snapshots() == (record,)
    assert [item.state.confirmed for item in store.read_sites()] == [
        SiteState.DOWN,
        SiteState.UP,
        SiteState.DOWN,
        SiteState.UP,
    ]
    assert {item.source_alias for item in store.pending_notifications()} == {
        "linux/cpu",
        "linux/fs_root",
    }
    assert len(store.pending_email_notifications()) == 4


@pytest.mark.parametrize("table", ["notification_outbox", "email_outbox", "server_snapshots"])
def test_failed_multi_rule_scrape_rolls_back_snapshot_state_and_every_destination(
    store: SQLiteStore, table: str
) -> None:
    operation = "UPDATE" if table == "server_snapshots" else "INSERT"
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            f"CREATE TRIGGER reject_scrape BEFORE {operation} ON {table} "
            "BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END"
        )
    with pytest.raises(StorageError, match="Cannot commit SQLite"):
        apply(store)
    assert all(item.state.confirmed is SiteState.UNKNOWN for item in store.read_sites())
    assert store.pending_count() == 0
    record = store.read_server_snapshot(ALIAS)
    assert record is not None and record.snapshot is None
    assert query(store, "SELECT * FROM incident_audit") == []


def test_repeated_cached_scrape_cannot_advance_confirmations(store: SQLiteStore) -> None:
    rules = tuple(replace(rule, failure_confirmation=2) for rule in RULES)
    store.reconcile(rules)
    apply(store, rules=rules)
    apply(store, rules=rules)
    assert store.read_site("linux/cpu").state.failures == 1
    assert store.pending_count() == 0
    apply(store, seconds=1, rules=rules)
    assert store.read_site("linux/cpu").state.confirmed is SiteState.DOWN
    assert store.pending_count() == 6


def test_unknown_and_hysteresis_band_reset_pending_without_resolving_active_incident(
    store: SQLiteStore,
) -> None:
    rules = tuple(replace(rule, recovery_confirmation=2) for rule in RULES)
    store.reconcile(rules)
    apply(store, rules=rules)
    incident = store.read_site("linux/cpu").state.incident_id
    apply(store, (True, True, True, None), 1, rules=rules)
    assert store.read_site("linux/cpu").state.successes == 1
    apply(store, (True, None, True, None), 2, rules=rules)
    current = store.read_site("linux/cpu")
    assert current.state.confirmed is SiteState.DOWN
    assert current.state.incident_id == incident
    assert current.state.successes == current.state.failures == 0
    assert current.observation is None
    assert all(item.event.kind is EventKind.DOWN for item in store.pending_notifications())
    apply(store, (True, True, True, None), 3, rules=rules)
    assert store.read_site("linux/cpu").state.confirmed is SiteState.DOWN
    apply(store, (True, True, True, None), 4, rules=rules)
    assert store.read_site("linux/cpu").state.confirmed is SiteState.UP


def test_exporter_failure_does_not_manufacture_resource_recovery(store: SQLiteStore) -> None:
    apply(store)
    incident = store.read_site("linux/cpu").state.incident_id
    apply(store, (False, None, None, None), 1, reading=snapshot(1, success=False))
    assert store.read_site("linux/exporter").state.confirmed is SiteState.DOWN
    assert store.read_site("linux/cpu").state.incident_id == incident
    assert store.read_site("linux/cpu").observation is None
    current = store.read_server_snapshot(ALIAS)
    assert current is not None and current.snapshot is not None
    assert current.snapshot.values.cpu_percent is None
    assert current.snapshot.result.error is ErrorKind.CONNECTION


def test_restart_preserves_incident_identity_and_latest_values_but_discards_pending_counts(
    store: SQLiteStore,
) -> None:
    rules = tuple(replace(rule, recovery_confirmation=2) for rule in RULES)
    store.reconcile(rules)
    apply(store, rules=rules)
    apply(store, (True, True, True, True), 1, rules=rules)
    before = store.read_server_snapshot(ALIAS)
    incident = store.read_site("linux/cpu").state.incident_id
    initialize_state(store.database.parent, NOW + timedelta(days=1))
    store.reconcile(rules)
    store.reconcile_servers(((ALIAS, FINGERPRINT),))
    assert store.read_server_snapshot(ALIAS) == before
    current = store.read_site("linux/cpu")
    assert current.state.incident_id == incident
    assert current.state.successes == 0
    assert store.pending_count() == 6


def test_changed_threshold_resets_only_affected_rule_without_fake_recovery(
    store: SQLiteStore,
) -> None:
    apply(store)
    other = store.read_site("linux/fs_root")
    rules = tuple(
        replace(rule, fingerprint="changed-cpu-threshold") if rule.alias == "linux/cpu" else rule
        for rule in RULES
    )
    store.reconcile(rules)
    assert store.read_site("linux/cpu").state.confirmed is SiteState.UNKNOWN
    assert store.read_site("linux/fs_root") == other
    assert {item.source_alias for item in store.pending_notifications()} == {"linux/fs_root"}
    assert {item.source_alias for item in store.pending_email_notifications()} == {"linux/fs_root"}
    assert all(item.event.kind is EventKind.DOWN for item in store.pending_notifications())


def test_snapshot_invalidation_and_removal_never_touch_protected_delivery_history(
    store: SQLiteStore,
) -> None:
    apply(store)
    notification = store.reserve_due_delivery(NOW)
    assert notification is not None
    store.acknowledge(notification.id, NOW)
    store.reconcile_servers(((ALIAS, "changed-exporter-settings"),))
    current = store.read_server_snapshot(ALIAS)
    assert current is not None and current.snapshot is None
    assert store.read_site("linux/cpu").state.confirmed is SiteState.DOWN
    store.reconcile_servers(())
    assert store.read_server_snapshot(ALIAS) is None
    assert store.delivery_status()["accepted"] == 1


def test_server_resolution_uses_frozen_recipient_routes(store: SQLiteStore) -> None:
    apply(store)
    for item in tuple(store.pending_email_notifications()):
        reserved = store.reserve_due_email_delivery(NOW, ready_ids=(item.id,))
        assert reserved is not None
        store.acknowledge_email(item.id, NOW)
    store.configure_email((FIRST, SECOND, THIRD), NOW + timedelta(seconds=1))
    apply(store, (True, True, True, True), 2)
    pending = store.pending_email_notifications()
    assert len(pending) == 4
    assert {item.destination for item in pending} == {FIRST, SECOND}
    assert all(item.event.kind is EventKind.RECOVERY for item in pending)


def test_failed_opening_blocks_only_its_recipient_resolution(store: SQLiteStore) -> None:
    apply(store)
    pending = store.pending_email_notifications()
    failed = next(
        item for item in pending if item.source_alias == "linux/cpu" and item.destination == FIRST
    )
    store.fail_email_delivery(failed.id, NOW, "invalid_recipient")
    apply(store, (True, True, True, True), 1)
    assert store.email_delivery_status(FIRST)["blocked"] == 1
    assert store.email_delivery_status(SECOND)["blocked"] == 0
    assert any(
        item.event.kind is EventKind.RECOVERY and item.destination == SECOND
        for item in store.pending_email_notifications()
    )


@pytest.mark.parametrize("channel", ["telegram", "email"])
def test_unknown_resource_suppresses_reminders_without_erasing_delivery_progress(
    store: SQLiteStore, channel: str
) -> None:
    apply(store)
    if channel == "telegram":
        for item in tuple(store.pending_notifications()):
            store.mark_in_flight(item.id)
            store.acknowledge(item.id, NOW)
    else:
        for item in tuple(store.pending_email_notifications()):
            reserved = store.reserve_due_email_delivery(NOW, ready_ids=(item.id,))
            assert reserved is not None
            store.acknowledge_email(item.id, NOW)
    apply(store, (True, None, True, None), 1)
    if channel == "telegram":
        assert not store.queue_reminder(RULES[1], NOW + timedelta(hours=1), False, due=True)
        assert store.reminder_progress()
    else:
        assert not store.queue_email_reminder(
            RULES[1], FIRST, NOW + timedelta(hours=1), False, due=True
        )
        assert store.email_reminder_progress()


def test_server_snapshot_contains_no_endpoint_authentication_or_mount_path(
    store: SQLiteStore,
) -> None:
    apply(store)
    raw = query(store, "SELECT snapshot FROM server_snapshots")[0][0]
    assert isinstance(raw, str)
    data = json.loads(raw)
    assert data["values"]["filesystems"] == [{"alias": "root", "value": 95.0, "error": None}]
    assert set(data) == {"checked_at", "success", "error", "http_status", "latency_ms", "values"}
    assert "/metrics" not in raw and "Authorization" not in raw and "token" not in raw


@pytest.mark.parametrize("raw", ["[]", '{"values":null}', '{"values":{"filesystems":[null]}}'])
def test_invalid_persisted_server_snapshot_is_visible(store: SQLiteStore, raw: str) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("UPDATE server_snapshots SET snapshot = ?", (raw,))
    with pytest.raises(StorageError, match="Stored server snapshot is invalid"):
        store.read_server_snapshots()


@pytest.mark.parametrize("mutation", ["server", "rule", "disabled", "alias", "time", "length"])
def test_invalid_or_unreconciled_scrape_leaves_every_record_unchanged(
    store: SQLiteStore, mutation: str
) -> None:
    reading = snapshot()
    rules = RULES
    observations = tuple(Observation(NOW, CheckResult(True)) for _ in rules)
    fingerprint = FINGERPRINT
    if mutation == "server":
        fingerprint = "unreconciled"
    elif mutation == "rule":
        rules = (replace(RULES[0], fingerprint="unreconciled"), *RULES[1:])
    elif mutation == "disabled":
        rules = (replace(RULES[0], enabled=False), *RULES[1:])
    elif mutation == "alias":
        rules = (replace(RULES[0], alias="other/exporter"), *RULES[1:])
    elif mutation == "time":
        observations = (
            Observation(NOW + timedelta(seconds=1), CheckResult(True)),
            *observations[1:],
        )
    elif mutation == "length":
        observations = observations[:-1]
    with pytest.raises((StorageError, ValueError)):
        store.apply_server_observation(ALIAS, fingerprint, reading, rules, observations)
    assert store.pending_count() == 0
    assert all(item.state.confirmed is SiteState.UNKNOWN for item in store.read_sites())


def test_expanded_inventory_requires_explicit_limit_and_rejects_more_than_forty(
    store: SQLiteStore,
) -> None:
    rules = tuple(replace(RULES[0], alias=f"linux/rule_{index}") for index in range(40))
    with pytest.raises(ValueError, match="20 unique"):
        store.reconcile(rules)
    store.reconcile(rules, max_rules=40)
    assert len(store.read_sites()) == 40
    with pytest.raises(ValueError, match="40 unique"):
        store.reconcile((*rules, replace(RULES[0], alias="linux/overflow")), max_rules=40)
    with pytest.raises(ValueError, match="20 or 40"):
        store.reconcile((), max_rules=80)


def test_forty_server_rules_retain_complete_latest_pairs_in_each_channel(
    store: SQLiteStore,
) -> None:
    rules = tuple(replace(RULES[0], alias=f"linux/rule_{index}") for index in range(40))
    store.reconcile(rules, max_rules=40)
    apply(store, (False,) * 40, rules=rules)
    apply(store, (True,) * 40, 1, rules=rules)
    assert len(store.pending_notifications()) == MAX_PENDING_NOTIFICATIONS
    assert store.email_pending_count() == 2 * MAX_PENDING_NOTIFICATIONS
    for rule in rules:
        assert [
            item.event.kind
            for item in store.pending_notifications()
            if item.source_alias == rule.alias
        ] == [EventKind.DOWN, EventKind.RECOVERY]
    source = PushSource("detector", frozenset({"security.attack"}), "x" * 32)
    store.accept_event(
        source,
        IncomingEvent(
            str(uuid4()), NOW, "notification", "security.attack", "warning", "Synthetic", None
        ),
        NOW,
        IngressSettings(),
    )
    assert store.pending_count() == 3 * MAX_PENDING_NOTIFICATIONS + 3


@pytest.mark.parametrize(
    "servers",
    [(("linux", "one"), ("linux", "two")), tuple((f"host{index}", "v1") for index in range(21))],
)
def test_server_snapshot_inventory_is_bounded_and_unique(
    store: SQLiteStore, servers: tuple[tuple[str, str], ...]
) -> None:
    before = store.read_server_snapshots()
    with pytest.raises(ValueError, match="20 unique"):
        store.reconcile_servers(servers)
    assert store.read_server_snapshots() == before


def test_cached_scrape_deduplication_still_validates_rule_configuration(store: SQLiteStore) -> None:
    apply(store)
    previous = store.pending_notifications()
    with pytest.raises(StorageError, match="Source configuration was not reconciled"):
        apply(store, rules=(replace(RULES[0], fingerprint="unreconciled"), *RULES[1:]))
    assert store.pending_notifications() == previous


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_snapshot_cannot_commit_or_mutate_policy(
    store: SQLiteStore, value: float
) -> None:
    reading = replace(snapshot(), values=MetricValues(cpu_percent=value))
    with pytest.raises(ValueError):
        apply(store, reading=reading)
    assert store.pending_count() == 0
    assert all(item.state.confirmed is SiteState.UNKNOWN for item in store.read_sites())
