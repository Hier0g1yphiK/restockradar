"""Task 5.3 — migration-safety tests (Req 14.18; underpins Req 21 migration coverage).

The runner (``persistence/migrate.py``) applies forward-only, numbered SQL files
under a single held ``BEGIN IMMEDIATE`` lock, splitting each file with
``sqlite3.complete_statement`` and running every statement (plus the
``schema_migrations`` version-row insert) with ``execute()`` — never
``executescript``. These tests pin the two safety properties that discipline buys:

* **Fail-midway rolls back fully.** A migration whose second statement errors leaves
  neither its schema changes NOR its ``schema_migrations`` version row, and a retry
  (with the file fixed) re-applies cleanly from the last committed version.
* **Concurrent starts apply each migration exactly once.** Two independent executors
  racing to migrate the same file-backed database each run the runner; because the
  applied version is re-read inside the held lock, the loser applies nothing and the
  version row is written exactly once.

WAL/concurrency behaviour is only observable on a real file, so every test here uses
a temp-file database (never ``:memory:``).
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from restockradar.persistence.db import Database
from restockradar.persistence.migrate import (
    DEFAULT_MIGRATIONS_DIR,
    discover_migrations,
    migrate,
    split_statements,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {r[0] for r in rows}


def _index_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index'"
    ).fetchall()
    return {r[0] for r in rows}


def _applied_versions(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    ).fetchall()
    return [int(r[0]) for r in rows]


@pytest.fixture
def file_db(tmp_path: Path) -> Iterator[Database]:
    db = Database(tmp_path / "migrate.db")
    try:
        yield db
    finally:
        db.close_sync()


# ---------------------------------------------------------------------------
# split_statements — the complete_statement splitter the runner relies on
# ---------------------------------------------------------------------------


def test_split_statements_splits_table_and_indexes() -> None:
    sql = (
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);\n"
        "CREATE INDEX idx_t_v ON t (v);\n"
    )
    assert split_statements(sql) == [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        "CREATE INDEX idx_t_v ON t (v);",
    ]


def test_split_statements_ignores_trailing_comment() -> None:
    sql = "CREATE TABLE t (id INTEGER PRIMARY KEY);\n-- a trailing comment, no statement\n"
    assert split_statements(sql) == ["CREATE TABLE t (id INTEGER PRIMARY KEY);"]


def test_split_statements_rejects_incomplete_tail() -> None:
    with pytest.raises(ValueError, match="incomplete SQL statement"):
        split_statements("CREATE TABLE t (id INTEGER PRIMARY KEY)\n")  # no ';'


# ---------------------------------------------------------------------------
# The real 0001_initial.sql applies and is idempotent
# ---------------------------------------------------------------------------

_EXPECTED_TABLES = {
    "schema_migrations",
    "games",
    "products",
    "retailers",
    "listings",
    "discovery_feeds",
    "seen_products",
    "alerts_outbox",
    "alert_deliveries",
    "alert_state",
    "alert_history",
    "backoff_state",
    "incidents",
    "destinations",
    "admin_notices",
    "admin_scope_state",
    "heartbeat",
}

_EXPECTED_INDEXES = {
    "idx_feeds_due",
    "idx_listings_due",
    "idx_backoff_until",
    "idx_deliveries_pending",
    "idx_deliveries_lease",
    "idx_deliveries_alert",
    "idx_incidents_open",
    "idx_admin_notices_pending",
    "idx_destinations_probe",
    "idx_outbox_terminal",
}


async def test_initial_migration_creates_full_schema(file_db: Database) -> None:
    """0001 applies and brings up every table and hot-path index from the design DDL."""
    applied = await migrate(file_db)
    assert applied == [1]

    tables = await file_db.run(_table_names)
    indexes = await file_db.run(_index_names)
    assert _EXPECTED_TABLES.issubset(tables)
    assert _EXPECTED_INDEXES.issubset(indexes)
    assert await file_db.run(_applied_versions) == [1]


async def test_migrate_is_idempotent(file_db: Database) -> None:
    """A second run applies nothing and leaves the version table unchanged."""
    first = await migrate(file_db)
    assert first == [1]

    second = await migrate(file_db)
    assert second == []  # nothing new to apply
    assert await file_db.run(_applied_versions) == [1]


async def test_discover_migrations_includes_the_initial_file() -> None:
    """The real migrations dir is discovered and 0001 is version 1."""
    migrations = discover_migrations(DEFAULT_MIGRATIONS_DIR)
    assert migrations, "expected at least the initial migration"
    assert migrations[0].version == 1
    assert migrations[0].path.name == "0001_initial.sql"


# ---------------------------------------------------------------------------
# (a) Fail-midway rollback + retry re-applies from the last committed version
# ---------------------------------------------------------------------------


def _write_migration(dir_: Path, name: str, sql: str) -> None:
    (dir_ / name).write_text(sql, encoding="utf-8")


async def test_failing_second_statement_rolls_back_fully_then_retry_succeeds(
    tmp_path: Path,
) -> None:
    """A file whose 2nd statement errors leaves NO schema change and NO version row.

    After a committed first migration, a second migration file whose second
    statement is invalid must roll back fully — the ``good_two`` table it tried to
    create and its version-2 row both vanish — and the applied version stays at 1.
    Fixing the file and retrying then applies version 2 from that last committed
    version.
    """
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    # Version 1 applies cleanly and commits on the first run (it is the only pending
    # file at that point), so version 2's later failure cannot roll version 1 back.
    _write_migration(
        migrations_dir,
        "0001_base.sql",
        "CREATE TABLE base_table (id INTEGER PRIMARY KEY);\n",
    )

    db = Database(tmp_path / "rollback.db")
    try:
        assert await migrate(db, migrations_dir) == [1]
        assert await db.run(_applied_versions) == [1]

        # Now introduce version 2 whose SECOND statement errors (duplicate table), so
        # the file's first statement has already run inside the transaction when the
        # error fires — the strongest form of the "fails midway" case.
        _write_migration(
            migrations_dir,
            "0002_broken.sql",
            "CREATE TABLE good_two (id INTEGER PRIMARY KEY);\n"
            "CREATE TABLE good_two (id INTEGER PRIMARY KEY);\n",  # duplicate -> error
        )

        # The broken migration must raise and must NOT partially apply.
        with pytest.raises(sqlite3.OperationalError):
            await migrate(db, migrations_dir)

        tables = await db.run(_table_names)
        assert "base_table" in tables          # version 1 survived
        assert "good_two" not in tables         # version 2's schema change rolled back
        assert await db.run(_applied_versions) == [1]  # no version-2 row

        # Fix the file; a retry re-applies version 2 from the last committed version.
        _write_migration(
            migrations_dir,
            "0002_broken.sql",
            "CREATE TABLE good_two (id INTEGER PRIMARY KEY);\n",
        )
        assert await migrate(db, migrations_dir) == [2]
        tables_after = await db.run(_table_names)
        assert "good_two" in tables_after
        assert await db.run(_applied_versions) == [1, 2]
    finally:
        db.close_sync()


# ---------------------------------------------------------------------------
# (b) Two concurrent starts apply each migration exactly once
# ---------------------------------------------------------------------------


def test_two_concurrent_starts_apply_each_migration_once(tmp_path: Path) -> None:
    """Two executors racing on the same file DB each run migrate(); applied once.

    Each start opens its own :class:`Database` (its own connection + executor) over
    the same file and calls the runner concurrently from two threads. Whichever takes
    ``BEGIN IMMEDIATE`` first applies both migrations; the loser blocks on the write
    lock, then — re-reading the applied version inside the lock — applies nothing.
    The union of what the two calls applied is exactly {1, 2}, with no version applied
    twice, and the ``schema_migrations`` table holds each version exactly once.

    Both connections are opened BEFORE the race so the only contention under test is
    the migration lock itself (``BEGIN IMMEDIATE``). Opening a WAL connection first
    takes a brief write lock to switch journal mode; racing that setup instead would
    test connection bring-up, not the migration-lock discipline this test targets.
    """
    import asyncio

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_migration(
        migrations_dir,
        "0001_base.sql",
        "CREATE TABLE base_table (id INTEGER PRIMARY KEY);\n",
    )
    _write_migration(
        migrations_dir,
        "0002_more.sql",
        "CREATE TABLE more_table (id INTEGER PRIMARY KEY);\n"
        "CREATE INDEX idx_more ON more_table (id);\n",
    )

    db_path = tmp_path / "concurrent.db"
    start = threading.Barrier(2)
    results: dict[int, list[int]] = {}
    errors: list[BaseException] = []

    # A generous busy_timeout so the loser waits for the winner's COMMIT rather than
    # erroring out under lock contention.
    dbs = [Database(db_path, busy_timeout_ms=10_000) for _ in range(2)]

    def worker(slot: int) -> None:
        db = dbs[slot]
        try:
            start.wait()
            results[slot] = asyncio.run(migrate(db, migrations_dir))
        except BaseException as exc:  # recorded and re-raised in the main thread
            errors.append(exc)
        finally:
            db.close_sync()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"a concurrent start failed: {errors!r}"

    # Each migration applied exactly once across the two racers (no double-apply).
    combined = sorted(v for applied in results.values() for v in applied)
    assert combined == [1, 2]

    # And the persisted bookkeeping holds each version exactly once.
    verify = Database(db_path)
    try:
        asyncio.run(_assert_versions_once(verify))
    finally:
        verify.close_sync()


async def _assert_versions_once(db: Database) -> None:
    versions = await db.run(_applied_versions)
    assert versions == [1, 2]
    tables = await db.run(_table_names)
    assert {"base_table", "more_table"}.issubset(tables)
