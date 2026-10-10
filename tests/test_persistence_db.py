"""Task 4.3 — tests for the single-thread DB executor (Req 14.16, 14.17, 14.19).

Cover the invariants the single-writer discipline rests on:

* the owner thread opens the connection and applies the PRAGMAs
  (``foreign_keys=ON``, ``synchronous=NORMAL``, a configured ``busy_timeout``, and
  WAL where the backing store supports it — a no-op on ``:memory:``);
* every blocking call runs on exactly ONE thread (the owner), never on the event
  loop, so the connection is only ever touched by its owner;
* the synchronous ``BEGIN IMMEDIATE`` helper COMMITs on success and ROLLBACKs on
  error, leaving no partial rows;
* ``wal_checkpoint(TRUNCATE)`` runs cleanly against both a temp-file (real WAL) and
  an in-memory database.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from restockradar.persistence.db import DEFAULT_BUSY_TIMEOUT_MS, Database


@pytest.fixture
def memory_db() -> Iterator[Database]:
    db = Database(":memory:")
    try:
        yield db
    finally:
        db.close_sync()


@pytest.fixture
def file_db(tmp_path: Path) -> Iterator[Database]:
    """A temp-FILE database — the place WAL journal mode is actually observable."""
    db = Database(tmp_path / "test.db", busy_timeout_ms=1234)
    try:
        yield db
    finally:
        db.close_sync()


# ---------------------------------------------------------------------------
# PRAGMAs (Req 14.16/14.17)
# ---------------------------------------------------------------------------


async def test_file_db_sets_wal_and_pragmas(file_db: Database) -> None:
    """On a file DB: WAL is active and the other PRAGMAs are applied."""

    def read_pragmas(conn: sqlite3.Connection) -> dict[str, object]:
        return {
            "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
            "foreign_keys": conn.execute("PRAGMA foreign_keys").fetchone()[0],
            "synchronous": conn.execute("PRAGMA synchronous").fetchone()[0],
            "busy_timeout": conn.execute("PRAGMA busy_timeout").fetchone()[0],
        }

    pragmas = await file_db.run(read_pragmas)
    assert pragmas["journal_mode"] == "wal"
    assert pragmas["foreign_keys"] == 1
    assert pragmas["synchronous"] == 1  # NORMAL
    assert pragmas["busy_timeout"] == 1234


async def test_memory_db_wal_is_a_graceful_noop(memory_db: Database) -> None:
    """WAL cannot apply to :memory:; the DB still opens and the other PRAGMAs hold."""

    def read_pragmas(conn: sqlite3.Connection) -> dict[str, object]:
        return {
            "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
            "foreign_keys": conn.execute("PRAGMA foreign_keys").fetchone()[0],
            "busy_timeout": conn.execute("PRAGMA busy_timeout").fetchone()[0],
        }

    pragmas = await memory_db.run(read_pragmas)
    # SQLite keeps "memory" journal mode for an in-memory DB — an accepted no-op.
    assert pragmas["journal_mode"] == "memory"
    assert pragmas["foreign_keys"] == 1
    assert pragmas["busy_timeout"] == DEFAULT_BUSY_TIMEOUT_MS
    assert memory_db.is_memory is True


# ---------------------------------------------------------------------------
# Single-thread ownership (event loop never makes a blocking sqlite3 call)
# ---------------------------------------------------------------------------


async def test_all_work_runs_on_one_dedicated_thread(memory_db: Database) -> None:
    """Every submitted callable runs on the same executor thread, not the loop."""
    main_thread = threading.get_ident()
    seen: set[int] = set()

    def record(_conn: sqlite3.Connection) -> None:
        seen.add(threading.get_ident())

    for _ in range(5):
        await memory_db.run(record)
    assert len(seen) == 1  # one and only one worker thread
    assert main_thread not in seen  # never the event-loop thread


async def test_run_propagates_exceptions(memory_db: Database) -> None:
    def boom(_conn: sqlite3.Connection) -> None:
        raise ValueError("explode")

    with pytest.raises(ValueError, match="explode"):
        await memory_db.run(boom)


# ---------------------------------------------------------------------------
# Synchronous BEGIN IMMEDIATE transaction helper
# ---------------------------------------------------------------------------


async def _create_table(db: Database) -> None:
    def setup(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")

    await db.run(setup)


async def test_transaction_commits_on_success(memory_db: Database) -> None:
    await _create_table(memory_db)

    def write(conn: sqlite3.Connection) -> None:
        with memory_db.transaction(conn):
            conn.execute("INSERT INTO t (id, v) VALUES (1, 'a')")

    await memory_db.run(write)

    def count(conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])

    assert await memory_db.run(count) == 1


async def test_transaction_rolls_back_on_error_leaving_no_partial_rows(
    memory_db: Database,
) -> None:
    await _create_table(memory_db)

    def write(conn: sqlite3.Connection) -> None:
        with memory_db.transaction(conn):
            conn.execute("INSERT INTO t (id, v) VALUES (1, 'a')")
            # second statement violates NOT NULL -> error -> rollback of the whole txn
            conn.execute("INSERT INTO t (id, v) VALUES (2, NULL)")

    with pytest.raises(sqlite3.IntegrityError):
        await memory_db.run(write)

    def count(conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])

    # Neither row survived: the rollback left no partial state.
    assert await memory_db.run(count) == 0


async def test_begin_immediate_is_used(memory_db: Database) -> None:
    """A nested BEGIN inside the helper's txn fails, proving a txn is already open."""
    await _create_table(memory_db)

    def write(conn: sqlite3.Connection) -> None:
        with memory_db.transaction(conn):
            # Already inside BEGIN IMMEDIATE; a second BEGIN must error.
            conn.execute("BEGIN IMMEDIATE")

    with pytest.raises(sqlite3.OperationalError, match="within a transaction"):
        await memory_db.run(write)


# ---------------------------------------------------------------------------
# WAL checkpoint
# ---------------------------------------------------------------------------


async def test_wal_checkpoint_truncate_on_file_db(file_db: Database) -> None:
    await _create_table(file_db)

    def write(conn: sqlite3.Connection) -> None:
        with file_db.transaction(conn):
            conn.execute("INSERT INTO t (id, v) VALUES (1, 'a')")

    await file_db.run(write)
    # Should run cleanly and fold the WAL back into the main file.
    await file_db.wal_checkpoint_truncate()

    def count(conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])

    assert await file_db.run(count) == 1


async def test_wal_checkpoint_truncate_on_memory_db_is_noop(memory_db: Database) -> None:
    # Must not raise even though there is no WAL file for an in-memory DB.
    await memory_db.wal_checkpoint_truncate()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_submit_after_close_raises() -> None:
    db = Database(":memory:")
    await db.close()
    with pytest.raises(RuntimeError, match="closed"):
        await db.run(lambda conn: conn.execute("SELECT 1"))


async def test_async_close_is_idempotent(memory_db: Database) -> None:
    await memory_db.close()
    await memory_db.close()  # second close is a no-op, not an error
