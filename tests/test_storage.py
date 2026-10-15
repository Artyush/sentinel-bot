import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from sentinel_bot.storage import DATABASE_NAME, StorageError, initialize_state


def test_metadata_survives_reopening_and_stores_utc(tmp_path: Path) -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=4)))
    first = initialize_state(tmp_path, started)
    second = initialize_state(tmp_path, started + timedelta(days=1))

    assert first.schema_version == second.schema_version == 2
    assert first.first_started_at == "2026-01-01T08:00:00+00:00"
    assert second.first_started_at == first.first_started_at
    assert second.last_started_at == "2026-01-02T08:00:00+00:00"
    assert second.startup_count == 2
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("SELECT count(*) FROM runtime_metadata").fetchone() == (1,)


def test_naive_startup_time_does_not_create_database(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        initialize_state(tmp_path, datetime(2026, 1, 1))
    assert not (tmp_path / DATABASE_NAME).exists()


@pytest.mark.parametrize("version", [3, 100])
def test_unsupported_schema_is_not_modified(tmp_path: Path, version: int) -> None:
    database = tmp_path / DATABASE_NAME
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(f"PRAGMA user_version = {version}")
        connection.execute("CREATE TABLE sentinel_fixture (value TEXT NOT NULL)")
        connection.execute("INSERT INTO sentinel_fixture VALUES ('unchanged')")

    with pytest.raises(StorageError, match="Unsupported state schema"):
        initialize_state(tmp_path, datetime.now(UTC))

    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (version,)
        assert connection.execute("SELECT * FROM sentinel_fixture").fetchall() == [("unchanged",)]
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall() == [("sentinel_fixture",)]


def test_unversioned_existing_schema_is_not_adopted(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("CREATE TABLE sentinel_fixture (value TEXT)")
    with pytest.raises(StorageError, match="unversioned schema"):
        initialize_state(tmp_path, datetime.now(UTC))


def test_missing_metadata_is_rejected(tmp_path: Path) -> None:
    initialize_state(tmp_path, datetime.now(UTC))
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("DELETE FROM runtime_metadata")
    with pytest.raises(StorageError, match="metadata is missing"):
        initialize_state(tmp_path, datetime.now(UTC))


def test_invalid_sqlite_is_visible(tmp_path: Path) -> None:
    (tmp_path / DATABASE_NAME).write_bytes(b"synthetic invalid database")
    with pytest.raises(StorageError, match="Cannot initialize SQLite") as error:
        initialize_state(tmp_path, datetime.now(UTC))
    assert isinstance(error.value.__cause__, sqlite3.DatabaseError)


def test_failed_update_rolls_back(tmp_path: Path) -> None:
    initialize_state(tmp_path, datetime.now(UTC))
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_startup BEFORE UPDATE ON runtime_metadata
            BEGIN SELECT RAISE(ABORT, 'synthetic write rejection'); END"""
        )
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(tmp_path, datetime.now(UTC))
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (1,)
