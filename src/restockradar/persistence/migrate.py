"""Versioned, forward-only migration runner (Req 14.18). Numbered files applied
under a held ``BEGIN IMMEDIATE`` lock, split with ``sqlite3.complete_statement`` and
executed statement-by-statement; idempotent and safe under concurrent starts.

Design (see "Data Models / Migration approach"):

* Migrations are forward-only, numbered SQL files in ``migrations/``
  (``0001_initial.sql``, ``0002_*.sql``, …). The leading integer is the migration
  ``version``; files apply in ascending version order and there is no down-migration.
* **Never ``sqlite3.executescript``.** ``executescript`` issues an implicit
  ``COMMIT`` before it runs, which would release — or prevent holding — the
  ``BEGIN IMMEDIATE`` write lock, so a migration applied with it could not be atomic
  with its version-row insert and could not run inside a held transaction. Instead
  each file is split into individual statements with
  :func:`sqlite3.complete_statement` and each is run with ``cursor.execute()`` under
  one explicit transaction.
* The connection is opened with ``isolation_level=None`` (the :class:`Database`
  executor already does this), so transaction control is fully explicit here.
* On a run the runner, *on the DB executor thread* so all SQLite work stays on the
  connection's owner thread:
    1. ensures ``schema_migrations`` exists (idempotent ``CREATE TABLE IF NOT EXISTS``);
    2. takes ``BEGIN IMMEDIATE`` to acquire the write lock, then **re-reads the max
       applied ``version`` inside that lock** — so two processes racing to migrate
       re-check the applied version under the lock and cannot both apply the same
       file (the loser sees the winner's committed version and applies nothing);
    3. for every discovered file whose version is higher than the applied max, in
       ascending order: reads the file text, splits it into statements, executes
       each with ``execute()``, and inserts the ``schema_migrations`` row for that
       file **in the same transaction**;
    4. ``COMMIT``s once (via the enclosing transaction) when all pending files are
       applied.

Because every DDL statement and the version-row insert run with ``execute()`` inside
the single held ``BEGIN IMMEDIATE``, a migration that fails midway rolls back
**fully** — neither the partial schema changes nor the version row persist — and the
next start simply retries from the last committed version. A migration whose version
is already present (re-checked under the lock) is skipped, so startup is idempotent
and two concurrent starts apply each migration exactly once.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from restockradar.persistence.db import Database

#: Default location of the numbered migration files, relative to the repo root
#: (``<repo>/migrations``). ``migrate.py`` lives at
#: ``<repo>/src/restockradar/persistence/migrate.py``, so the repo root is four
#: parents up.
DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"

#: A migration file name begins with its zero-padded integer version followed by an
#: underscore and a description, e.g. ``0001_initial.sql``. Only ``.sql`` files that
#: match are treated as migrations; anything else in the directory is ignored.
_MIGRATION_NAME_RE = re.compile(r"^(\d+)_.*\.sql$")


@dataclass(frozen=True)
class Migration:
    """A single discovered migration file: its numeric ``version`` and ``path``."""

    version: int
    path: Path

    def read_sql(self) -> str:
        """Read the file's SQL text (UTF-8)."""
        return self.path.read_text(encoding="utf-8")


def discover_migrations(migrations_dir: Path = DEFAULT_MIGRATIONS_DIR) -> list[Migration]:
    """Return the numbered ``.sql`` migrations in ``migrations_dir``, ascending.

    Files whose name does not match ``<int>_*.sql`` (e.g. ``.gitkeep``) are ignored.
    Two files sharing the same leading version are a packaging error and raise
    :class:`ValueError` rather than being applied ambiguously.
    """
    found: dict[int, Migration] = {}
    for entry in sorted(migrations_dir.iterdir()) if migrations_dir.is_dir() else []:
        if not entry.is_file():
            continue
        match = _MIGRATION_NAME_RE.match(entry.name)
        if match is None:
            continue
        version = int(match.group(1))
        if version in found:
            raise ValueError(
                f"duplicate migration version {version}: "
                f"{found[version].path.name} and {entry.name}"
            )
        found[version] = Migration(version=version, path=entry)
    return [found[v] for v in sorted(found)]


def split_statements(sql: str) -> list[str]:
    """Split a migration file into individual complete SQL statements.

    Uses :func:`sqlite3.complete_statement`, accumulating lines until SQLite reports
    the buffer is a complete statement, exactly as the design prescribes. This keeps
    multi-statement files (a ``CREATE TABLE`` plus its ``CREATE INDEX``es) as a list
    of separate statements, each run with its own ``execute()`` — never through
    ``executescript``.

    Comment-only / blank trailing fragments (which never ``complete``) are dropped so
    a file ending in a comment does not yield a spurious final statement.
    """
    statements: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                statements.append(statement)
            buffer = ""
    # Any leftover is an incomplete tail (typically trailing comments/whitespace with
    # no terminating ';'); only surface it if it carries a real statement fragment.
    leftover = buffer.strip()
    if leftover and not _is_only_comments(leftover):
        raise ValueError(f"migration ends with an incomplete SQL statement: {leftover!r}")
    return statements


def _is_only_comments(text: str) -> bool:
    """True if ``text`` is nothing but SQL line comments and blank lines."""
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped and not stripped.startswith("--"):
            return False
    return True


def _ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    """Create the ``schema_migrations`` bookkeeping table if it is absent.

    Idempotent and safe to call before taking the migration lock: ``CREATE TABLE IF
    NOT EXISTS`` is a no-op once the table exists, and the real applied-version check
    is re-done *inside* the held ``BEGIN IMMEDIATE`` below.
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "    version     INTEGER PRIMARY KEY,"
        "    applied_at  TEXT    NOT NULL"
        ")"
    )


def _max_applied_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied migration version, or ``0`` if none are applied."""
    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    value = row[0] if row is not None else None
    return int(value) if value is not None else 0


def _apply_pending(
    db: Database,
    conn: sqlite3.Connection,
    migrations: list[Migration],
) -> list[int]:
    """Apply every pending migration under one held ``BEGIN IMMEDIATE`` transaction.

    Runs on the executor thread (``conn`` is the owned connection). Returns the list
    of versions newly applied by *this* call (empty if another process already
    applied everything, which is the idempotent / concurrent-loser path).
    """
    _ensure_schema_migrations(conn)

    applied: list[int] = []
    with db.transaction(conn):
        # Re-read the applied max INSIDE the held write lock so a racing process that
        # committed first is observed here and we skip what it already applied.
        current = _max_applied_version(conn)
        for migration in migrations:
            if migration.version <= current:
                continue
            for statement in split_statements(migration.read_sql()):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (migration.version, datetime.now(UTC).isoformat()),
            )
            applied.append(migration.version)
    return applied


async def migrate(
    db: Database,
    migrations_dir: Path = DEFAULT_MIGRATIONS_DIR,
) -> list[int]:
    """Apply all pending migrations through the :class:`Database` executor.

    The entire ensure-table → ``BEGIN IMMEDIATE`` → re-read → apply → commit sequence
    runs as a single callable on the DB executor thread, so all ``sqlite3`` work stays
    on the connection's owner thread and the write lock is never yielded mid-migration.

    Returns the versions newly applied by this call. Calling it again once everything
    is applied returns an empty list (idempotent); two concurrent starts each call it
    and, because the applied version is re-checked inside the held lock, each migration
    is applied exactly once (the loser applies nothing).
    """
    migrations = discover_migrations(migrations_dir)

    def _run(conn: sqlite3.Connection) -> list[int]:
        return _apply_pending(db, conn, migrations)

    return await db.run(_run)
