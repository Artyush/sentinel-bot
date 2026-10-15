"""Transactional monitoring snapshots and a bounded notification outbox."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sentinel_bot.config import SiteConfig
from sentinel_bot.models import (
    CheckResult,
    ErrorKind,
    Event,
    EventKind,
    Observation,
    PolicyState,
    SiteState,
)
from sentinel_bot.policy import apply_observation as decide_observation
from sentinel_bot.policy import due_reminder

SCHEMA_VERSION = 2
DATABASE_NAME = "state.sqlite3"
HISTORY_LIMIT = 200
MAX_PENDING_NOTIFICATIONS = 80
MAX_IN_FLIGHT_NOTIFICATIONS = 20


class StorageError(RuntimeError):
    """State cannot be used safely with the current schema."""


@dataclass(frozen=True)
class StartupMetadata:
    schema_version: int
    first_started_at: str
    last_started_at: str
    startup_count: int


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Time must be timezone-aware")
    return value.astimezone(UTC).isoformat()


@contextmanager
def _transaction(database: Path) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database, timeout=2.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except sqlite3.Error as exc:
        raise StorageError("Cannot commit SQLite state") from exc
    finally:
        if connection is not None:
            connection.close()


def _create_monitoring_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE sites (
            alias TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,
            observation TEXT
        )"""
    )
    connection.execute(
        """CREATE TABLE notification_outbox (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_id TEXT UNIQUE NOT NULL,
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            event TEXT NOT NULL,
            in_flight INTEGER NOT NULL DEFAULT 0 CHECK (in_flight IN (0, 1)),
            delivered_at TEXT
        )"""
    )
    connection.execute(
        """CREATE TABLE delivery_progress (
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            last_delivered_at TEXT NOT NULL,
            PRIMARY KEY (source_alias, destination)
        )"""
    )
    connection.execute(
        """CREATE TABLE incident_audit (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            source_alias TEXT NOT NULL,
            event TEXT NOT NULL
        )"""
    )
    connection.execute(
        """CREATE INDEX pending_notifications
        ON notification_outbox (source_alias, destination, delivered_at)"""
    )

    connection.execute(
        """CREATE UNIQUE INDEX one_pending_reminder
        ON notification_outbox (source_alias, destination, incident_id)
        WHERE kind = 'REMINDER' AND delivered_at IS NULL"""
    )


def _upgrade_schema(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version == 0:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        if tables:
            raise StorageError("Existing database has an unversioned schema")
        connection.execute(
            """CREATE TABLE runtime_metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                first_started_at TEXT NOT NULL,
                last_started_at TEXT NOT NULL,
                startup_count INTEGER NOT NULL CHECK (startup_count > 0)
            )"""
        )
    elif version not in (1, SCHEMA_VERSION):
        raise StorageError(f"Unsupported state schema version {version}; expected {SCHEMA_VERSION}")
    if version == 1:
        row = connection.execute(
            "SELECT first_started_at, last_started_at, startup_count "
            "FROM runtime_metadata WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise StorageError("Startup metadata is missing")
    if version < SCHEMA_VERSION:
        _create_monitoring_schema(connection)
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    for query in (
        "SELECT alias, fingerprint, state, observation FROM sites LIMIT 0",
        "SELECT sequence, notification_id, source_alias, destination, incident_id, "
        "kind, event, in_flight, delivered_at FROM notification_outbox LIMIT 0",
        "SELECT source_alias, destination, incident_id, last_delivered_at "
        "FROM delivery_progress LIMIT 0",
        "SELECT sequence, source_alias, event FROM incident_audit LIMIT 0",
    ):
        connection.execute(query)


def initialize_state(state_dir: Path, started_at: datetime) -> StartupMetadata:
    """Atomically upgrade supported state and commit one startup.

    The runtime holds its state-directory lock before calling this function.
    Run it outside the event loop; each call owns and closes its connection.
    """
    timestamp = _timestamp(started_at)
    try:
        with _transaction(state_dir / DATABASE_NAME) as connection:
            new_database = connection.execute("PRAGMA user_version").fetchone()[0] == 0
            _upgrade_schema(connection)
            if new_database:
                connection.execute(
                    "INSERT INTO runtime_metadata VALUES (1, ?, ?, 1)",
                    (timestamp, timestamp),
                )
            else:
                updated = connection.execute(
                    """UPDATE runtime_metadata
                    SET last_started_at = ?, startup_count = startup_count + 1
                    WHERE singleton = 1""",
                    (timestamp,),
                )
                if updated.rowcount != 1:
                    raise StorageError("Startup metadata is missing")
            row = connection.execute(
                """SELECT first_started_at, last_started_at, startup_count
                FROM runtime_metadata WHERE singleton = 1"""
            ).fetchone()
            if row is None:
                raise StorageError("Startup metadata is missing")
            return StartupMetadata(SCHEMA_VERSION, row[0], row[1], row[2])
    except StorageError as exc:
        if isinstance(exc.__cause__, sqlite3.Error):
            raise StorageError("Cannot initialize SQLite state") from exc.__cause__
        raise


@dataclass(frozen=True)
class SiteRecord:
    alias: str
    fingerprint: str
    state: PolicyState
    observation: Observation | None


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    event: Event
    source_alias: str
    destination: str = "operator"
    in_flight: bool = False


def _encode_state(state: PolicyState) -> str:
    return json.dumps(
        {
            "confirmed": state.confirmed.value,
            "failures": state.failures,
            "successes": state.successes,
            "first_failure_at": _optional_timestamp(state.first_failure_at),
            "incident_id": state.incident_id,
            "incident_started_at": _optional_timestamp(state.incident_started_at),
        },
        separators=(",", ":"),
    )


def _encode_observation(observation: Observation) -> str:
    result = observation.result
    return json.dumps(
        {
            "checked_at": _timestamp(observation.checked_at),
            "success": result.success,
            "error": None if result.error is None else result.error.value,
            "http_status": result.http_status,
            "latency_ms": result.latency_ms,
        },
        separators=(",", ":"),
    )


def _encode_event(event: Event) -> str:
    return json.dumps(
        {
            "kind": event.kind.value,
            "incident_id": event.incident_id,
            "occurred_at": _timestamp(event.occurred_at),
            "started_at": _timestamp(event.started_at),
        },
        separators=(",", ":"),
    )


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _document(raw: str) -> dict[str, object]:
    document: object = json.loads(raw)
    if not isinstance(document, dict):
        raise ValueError("Stored value must be an object")
    return document


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Stored value must be a string")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Stored value must be an integer")
    return value


def _datetime(value: object) -> datetime:
    timestamp = datetime.fromisoformat(_text(value))
    _timestamp(timestamp)
    return timestamp.astimezone(UTC)


def _optional_datetime(value: object) -> datetime | None:
    return None if value is None else _datetime(value)


def _decode_state(raw: str) -> PolicyState:
    data = _document(raw)
    return PolicyState(
        confirmed=SiteState(_text(data["confirmed"])),
        failures=_integer(data["failures"]),
        successes=_integer(data["successes"]),
        first_failure_at=_optional_datetime(data["first_failure_at"]),
        incident_id=None if data["incident_id"] is None else _text(data["incident_id"]),
        incident_started_at=_optional_datetime(data["incident_started_at"]),
    )


def _decode_observation(raw: str) -> Observation:
    data = _document(raw)
    success = data["success"]
    if not isinstance(success, bool):
        raise ValueError("Stored success must be a boolean")
    latency = data["latency_ms"]
    if latency is not None and (isinstance(latency, bool) or not isinstance(latency, int | float)):
        raise ValueError("Stored latency must be numeric")
    return Observation(
        _datetime(data["checked_at"]),
        CheckResult(
            success,
            None if data["error"] is None else ErrorKind(_text(data["error"])),
            None if data["http_status"] is None else _integer(data["http_status"]),
            latency,
        ),
    )


def _decode_event(raw: str) -> Event:
    data = _document(raw)
    return Event(
        EventKind(_text(data["kind"])),
        _text(data["incident_id"]),
        _datetime(data["occurred_at"]),
        _datetime(data["started_at"]),
    )


def _site_record(row: sqlite3.Row) -> SiteRecord:
    try:
        return SiteRecord(
            row["alias"],
            row["fingerprint"],
            _decode_state(row["state"]),
            None if row["observation"] is None else _decode_observation(row["observation"]),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise StorageError("Stored monitoring snapshot is invalid") from exc


def _outbox_record(row: sqlite3.Row) -> OutboxRecord:
    try:
        return OutboxRecord(
            row["notification_id"],
            _decode_event(row["event"]),
            row["source_alias"],
            row["destination"],
            bool(row["in_flight"]),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise StorageError("Stored notification is invalid") from exc


class SQLiteStore:
    """Use short synchronous transactions, serialized off-loop by the runtime.

    Opening the store does not initialize state or increment startup metadata.
    Notification records target the logical operator route until a delivery
    milestone provides configured channels; no method sends a notification.
    """

    def __init__(self, state_dir: Path) -> None:
        self.database = state_dir / DATABASE_NAME
        if not self.database.is_file():
            raise StorageError("State must be initialized before opening the store")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with _transaction(self.database) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version != SCHEMA_VERSION:
                raise StorageError(
                    f"Unsupported state schema version {version}; expected {SCHEMA_VERSION}"
                )
            yield connection

    def reconcile(self, sites: Sequence[SiteConfig]) -> None:
        """Reset changed sources and stale confirmations without fake recovery.

        Call once at startup, after acquiring state ownership. Confirmed state,
        incident identity, and the latest snapshot survive unchanged inventories;
        pending confirmation series restart because restored readings are stale.
        """
        enabled = tuple(site for site in sites if site.enabled)
        aliases = tuple(site.alias for site in enabled)
        if len(aliases) > 20 or len(aliases) != len(set(aliases)):
            raise ValueError("Source inventory requires at most 20 unique aliases")
        with self._transaction() as connection:
            existing = {
                row["alias"]: _site_record(row)
                for row in connection.execute("SELECT * FROM sites").fetchall()
            }
            for alias in existing.keys() - set(aliases):
                self._cancel_unsent(connection, alias)
                connection.execute("DELETE FROM sites WHERE alias = ?", (alias,))
            for site in enabled:
                previous = existing.get(site.alias)
                if previous is None or previous.fingerprint != site.fingerprint:
                    self._cancel_unsent(connection, site.alias)
                    connection.execute(
                        """INSERT INTO sites (alias, fingerprint, state, observation)
                        VALUES (?, ?, ?, NULL)
                        ON CONFLICT(alias) DO UPDATE SET fingerprint = excluded.fingerprint,
                        state = excluded.state, observation = NULL""",
                        (site.alias, site.fingerprint, _encode_state(PolicyState())),
                    )
                else:
                    self._reset_pending(connection, previous)

    @staticmethod
    def _cancel_unsent(connection: sqlite3.Connection, alias: str) -> None:
        connection.execute("DELETE FROM delivery_progress WHERE source_alias = ?", (alias,))
        connection.execute(
            """DELETE FROM notification_outbox
            WHERE source_alias = ? AND delivered_at IS NULL AND in_flight = 0""",
            (alias,),
        )

    @staticmethod
    def _reset_pending(connection: sqlite3.Connection, record: SiteRecord) -> None:
        state = replace(record.state, failures=0, successes=0, first_failure_at=None)
        connection.execute(
            "UPDATE sites SET state = ? WHERE alias = ?", (_encode_state(state), record.alias)
        )

    def reset_pending(self, alias: str) -> None:
        """Discard confirmation samples separated by a stale polling gap."""
        with self._transaction() as connection:
            record = self._read_site(connection, alias)
            self._reset_pending(connection, record)

    @staticmethod
    def _read_site(connection: sqlite3.Connection, alias: str) -> SiteRecord:
        row = connection.execute("SELECT * FROM sites WHERE alias = ?", (alias,)).fetchone()
        if row is None:
            raise StorageError("Source is not present in reconciled state")
        return _site_record(row)

    def read_site(self, alias: str) -> SiteRecord:
        with self._transaction() as connection:
            return self._read_site(connection, alias)

    def read_sites(self) -> tuple[SiteRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                _site_record(row)
                for row in connection.execute("SELECT * FROM sites ORDER BY alias").fetchall()
            )

    def apply_observation(self, site: SiteConfig, observation: Observation) -> SiteRecord:
        """Commit a fresh reading, policy transition, and outbox atomically."""
        with self._transaction() as connection:
            record = self._read_site(connection, site.alias)
            if record.fingerprint != site.fingerprint or not site.enabled:
                raise StorageError("Source configuration was not reconciled")
            if record.observation == observation:
                return record
            transition = decide_observation(
                site.alias,
                site.fingerprint,
                record.state,
                observation,
                failure_confirmation=site.failure_confirmation,
                recovery_confirmation=site.recovery_confirmation,
            )
            connection.execute(
                "UPDATE sites SET state = ?, observation = ? WHERE alias = ?",
                (_encode_state(transition.state), _encode_observation(observation), site.alias),
            )
            if transition.event is not None:
                self._queue_event(connection, site.alias, transition.event)
                connection.execute(
                    "INSERT INTO incident_audit (source_alias, event) VALUES (?, ?)",
                    (site.alias, _encode_event(transition.event)),
                )
                connection.execute(
                    """DELETE FROM incident_audit WHERE sequence NOT IN
                    (SELECT sequence FROM incident_audit ORDER BY sequence DESC LIMIT ?)""",
                    (HISTORY_LIMIT,),
                )
            return SiteRecord(site.alias, site.fingerprint, transition.state, observation)

    @staticmethod
    def _queue_event(connection: sqlite3.Connection, alias: str, event: Event) -> None:
        if event.kind is EventKind.DOWN:
            connection.execute("DELETE FROM delivery_progress WHERE source_alias = ?", (alias,))
            connection.execute(
                """DELETE FROM notification_outbox WHERE source_alias = ?
                AND destination = 'operator' AND incident_id != ?
                AND delivered_at IS NULL AND in_flight = 0""",
                (alias, event.incident_id),
            )
        elif event.kind is EventKind.RECOVERY:
            connection.execute(
                """DELETE FROM notification_outbox WHERE source_alias = ?
                AND destination = 'operator' AND kind = 'REMINDER'
                AND delivered_at IS NULL AND in_flight = 0""",
                (alias,),
            )
        pending = connection.execute(
            "SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL"
        ).fetchone()[0]
        if pending >= MAX_PENDING_NOTIFICATIONS:
            raise StorageError("Notification outbox capacity is exhausted")
        connection.execute(
            """INSERT INTO notification_outbox
            (notification_id, source_alias, destination, incident_id, kind, event)
            VALUES (?, ?, 'operator', ?, ?, ?)""",
            (str(uuid4()), alias, event.incident_id, event.kind.value, _encode_event(event)),
        )

    def pending_notifications(self) -> tuple[OutboxRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                _outbox_record(row)
                for row in connection.execute(
                    """SELECT * FROM notification_outbox
                    WHERE delivered_at IS NULL ORDER BY sequence"""
                ).fetchall()
            )

    def pending_count(self) -> int:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL"
            ).fetchone()
            assert row is not None
            return int(row[0])

    def mark_in_flight(self, notification_id: str) -> None:
        """Protect the next ordered record while a delivery edge owns its send."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            if row is None or row["delivered_at"] is not None:
                raise StorageError("Pending notification does not exist")
            in_flight = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox "
                "WHERE delivered_at IS NULL AND in_flight = 1"
            ).fetchone()[0]
            if not row["in_flight"] and in_flight >= MAX_IN_FLIGHT_NOTIFICATIONS:
                raise StorageError("In-flight notification capacity is exhausted")
            earlier = connection.execute(
                """SELECT 1 FROM notification_outbox
                WHERE source_alias = ? AND destination = ?
                AND delivered_at IS NULL AND sequence < ? LIMIT 1""",
                (row["source_alias"], row["destination"], row["sequence"]),
            ).fetchone()
            if earlier is not None:
                raise StorageError("An earlier notification must be delivered first")
            connection.execute(
                "UPDATE notification_outbox SET in_flight = 1 WHERE notification_id = ?",
                (notification_id,),
            )

    def release_in_flight(self, notification_id: str) -> None:
        """Keep a failed send pending for a later attempt."""
        with self._transaction() as connection:
            connection.execute(
                """UPDATE notification_outbox SET in_flight = 0
                WHERE notification_id = ? AND delivered_at IS NULL""",
                (notification_id,),
            )

    def acknowledge(self, notification_id: str, delivered_at: datetime) -> None:
        """Record a provider-accepted send; callers never acknowledge failures."""
        timestamp = _timestamp(delivered_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            if row is None:
                raise StorageError("Notification does not exist")
            if row["delivered_at"] is not None:
                return
            earlier = connection.execute(
                """SELECT 1 FROM notification_outbox WHERE source_alias = ?
                AND destination = ? AND delivered_at IS NULL AND sequence < ? LIMIT 1""",
                (row["source_alias"], row["destination"], row["sequence"]),
            ).fetchone()
            if earlier is not None:
                raise StorageError("An earlier notification must be delivered first")
            connection.execute(
                """UPDATE notification_outbox SET delivered_at = ?, in_flight = 0
                WHERE notification_id = ?""",
                (timestamp, notification_id),
            )
            event = _outbox_record(row).event
            current = connection.execute(
                "SELECT * FROM sites WHERE alias = ?", (row["source_alias"],)
            ).fetchone()
            if (
                current is not None
                and _site_record(current).state.incident_id == event.incident_id
                and event.kind in (EventKind.DOWN, EventKind.REMINDER)
            ):
                connection.execute(
                    """INSERT INTO delivery_progress
                    (source_alias, destination, incident_id, last_delivered_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source_alias, destination) DO UPDATE SET
                    incident_id = excluded.incident_id,
                    last_delivered_at = excluded.last_delivered_at""",
                    (row["source_alias"], row["destination"], event.incident_id, timestamp),
                )
            connection.execute(
                """DELETE FROM notification_outbox WHERE delivered_at IS NOT NULL
                AND sequence NOT IN (SELECT sequence FROM notification_outbox
                    WHERE delivered_at IS NOT NULL ORDER BY sequence DESC LIMIT ?)""",
                (HISTORY_LIMIT,),
            )

    def queue_reminder(self, site: SiteConfig, now: datetime, fresh: bool) -> bool:
        """Queue at most one reminder after this route's accepted DOWN/reminder."""
        with self._transaction() as connection:
            record = self._read_site(connection, site.alias)
            if record.fingerprint != site.fingerprint or not site.enabled:
                raise StorageError("Source configuration was not reconciled")
            pending = connection.execute(
                """SELECT 1 FROM notification_outbox WHERE source_alias = ?
                AND destination = 'operator' AND kind = 'REMINDER'
                AND delivered_at IS NULL LIMIT 1""",
                (site.alias,),
            ).fetchone()
            if pending is not None:
                return False
            last = connection.execute(
                """SELECT last_delivered_at FROM delivery_progress WHERE source_alias = ?
                AND destination = 'operator' AND incident_id = ?""",
                (site.alias, record.state.incident_id),
            ).fetchone()
            event = due_reminder(
                record.state,
                now,
                None if last is None else _datetime(last[0]),
                site.reminder_interval,
                fresh,
            )
            if event is None:
                return False
            self._queue_event(connection, site.alias, event)
            return True
