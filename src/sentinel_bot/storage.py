"""Durable startup metadata for one Sentinel state directory."""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_VERSION = 1
DATABASE_NAME = "state.sqlite3"


class StorageError(RuntimeError):
    """State cannot be initialized safely with the current schema."""


@dataclass(frozen=True)
class StartupMetadata:
    schema_version: int
    first_started_at: str
    last_started_at: str
    startup_count: int


def initialize_state(state_dir: Path, started_at: datetime) -> StartupMetadata:
    """Commit one startup atomically; reject unknown or unversioned schemas.

    The runtime holds its state-directory lock before calling this function.
    Run it outside the event loop; each call owns and closes its connection.
    """
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise ValueError("Startup time must be timezone-aware")
    timestamp = started_at.astimezone(UTC).isoformat()
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(state_dir / DATABASE_NAME, timeout=2.0)
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN IMMEDIATE")
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
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.execute(
                "INSERT INTO runtime_metadata VALUES (1, ?, ?, 1)",
                (timestamp, timestamp),
            )
        elif version == SCHEMA_VERSION:
            updated = connection.execute(
                """UPDATE runtime_metadata
                SET last_started_at = ?, startup_count = startup_count + 1
                WHERE singleton = 1""",
                (timestamp,),
            )
            if updated.rowcount != 1:
                raise StorageError("Startup metadata is missing")
        else:
            raise StorageError(
                f"Unsupported state schema version {version}; expected {SCHEMA_VERSION}"
            )
        row = connection.execute(
            """SELECT first_started_at, last_started_at, startup_count
            FROM runtime_metadata WHERE singleton = 1"""
        ).fetchone()
        if row is None:
            raise StorageError("Startup metadata is missing")
        metadata = StartupMetadata(SCHEMA_VERSION, row[0], row[1], row[2])
        connection.commit()
        return metadata
    except sqlite3.Error as exc:
        raise StorageError("Cannot initialize SQLite state") from exc
    finally:
        if connection is not None:
            connection.close()
