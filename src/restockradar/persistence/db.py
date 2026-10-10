"""``Database``: single-thread executor owning the ``sqlite3`` connection; async
wrappers that submit to it; WAL / busy_timeout / foreign_keys / synchronous PRAGMAs;
a synchronous ``BEGIN IMMEDIATE`` context manager with no ``await`` inside the lock;
and a WAL-checkpoint(TRUNCATE) method (Req 14.19, 14.16, 14.17).

Design (see "DB access — one executor owns the connection" and "SQLite write
serialisation — single-writer discipline"):

* **One dedicated executor thread owns the one write connection.** Every blocking
  ``sqlite3`` call runs on that thread; the event loop never makes a blocking SQLite
  call. Because exactly one thread ever touches the connection there is no
  intra-process contention on it, and ``check_same_thread=False`` is safe precisely
  because we enforce single-thread ownership ourselves.
* **Async wrappers submit work to the executor and ``await`` the result.** A
  coroutine calls :meth:`Database.run`, the callable executes on the owner thread,
  and the loop is handed back control until the result (or exception) is ready.
* **The transaction helper is synchronous.** :meth:`Database.transaction` is a
  *sync* context manager meant to run *inside* a callable already executing on the
  executor thread. It issues ``BEGIN IMMEDIATE`` (acquiring the write lock up front),
  yields the connection, and COMMITs on clean exit / ROLLBACKs on error — with **no
  ``await`` inside the lock**, so a write can never yield the event loop mid-txn and
  can never interleave with another coroutine's read-modify-write.
* **PRAGMAs** (Req 14.16/14.17): WAL journal mode, a configurable ``busy_timeout``,
  ``foreign_keys=ON``, and ``synchronous=NORMAL``. WAL is a no-op on a ``:memory:``
  database — SQLite silently keeps ``memory`` journal mode — which we tolerate
  gracefully so tests can use an in-memory DB; tests that actually exercise WAL
  behaviour use a temp file instead.
* **WAL checkpoint** (:meth:`Database.wal_checkpoint_truncate`) folds the WAL back
  into the main database file on graceful shutdown.
"""

from __future__ import annotations

import asyncio
import queue
import sqlite3
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from pathlib import Path
from typing import TypeVar, cast

T = TypeVar("T")

# A single work item handed to the executor thread: an opaque callable taking the
# owned connection and the Future that receives its outcome. Concrete result types
# are recovered at the call site via the Future[T] that submit() hands back.
_WorkItem = tuple[Callable[[sqlite3.Connection], object], "Future[object]"]

#: Default ``busy_timeout`` in milliseconds (design: ``PRAGMA busy_timeout = 5000``).
#: Callers pass the configured value (ultimately sourced from settings, Req 13.8);
#: this constant is the fallback matching the design default.
DEFAULT_BUSY_TIMEOUT_MS = 5000

#: Sentinel path for an in-memory database.
MEMORY_PATH = ":memory:"


class Database:
    """A SQLite database behind one dedicated single-thread executor.

    The owning thread opens the connection, applies the PRAGMAs, and is the only
    thread that ever touches it. Public ``async`` methods submit callables to that
    thread and await the outcome; the synchronous :meth:`transaction` helper is for
    use *within* such a callable.
    """

    def __init__(
        self,
        path: str | Path = MEMORY_PATH,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        self._path = path if isinstance(path, str) else str(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._is_memory = self._path == MEMORY_PATH or self._path == ""

        # The executor is a single worker thread we drive by hand via a queue of
        # (callable, future) pairs, so we can guarantee the connection is created
        # and only ever used on that one thread. ``None`` is the shutdown sentinel.
        self._queue: queue.Queue[_WorkItem | None] = queue.Queue()
        self._conn: sqlite3.Connection | None = None
        self._closed = False
        self._startup_error: BaseException | None = None
        self._ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="restockradar-db-executor",
            daemon=True,
        )
        self._thread.start()
        # Block construction until the owner thread has opened the connection and
        # applied PRAGMAs, so the Database is fully usable the moment __init__ returns.
        self._ready.wait()
        if self._startup_error is not None:
            raise self._startup_error

    # -- executor thread ----------------------------------------------------

    def _run_loop(self) -> None:
        """Body of the owner thread: open the connection, then serve work."""
        try:
            self._conn = self._open_connection()
        except BaseException as exc:  # surfaced to the constructor
            self._startup_error = exc
            self._ready.set()
            return
        self._ready.set()

        while True:
            item = self._queue.get()
            if item is None:  # shutdown sentinel
                break
            func, future = item
            if future.set_running_or_notify_cancel():
                try:
                    result = func(self._require_conn())
                except BaseException as exc:  # relayed to the caller via the Future
                    future.set_exception(exc)
                else:
                    future.set_result(result)

        # Close the connection on this thread (the only thread allowed to touch it).
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _open_connection(self) -> sqlite3.Connection:
        """Open the connection and apply the single-writer PRAGMAs (Req 14.16/14.17)."""
        conn = sqlite3.connect(
            self._path,
            # The owner thread is the only user; we enforce that ourselves, so the
            # same-thread check (which would otherwise reject the executor pattern)
            # is turned off deliberately.
            check_same_thread=False,
            # We manage BEGIN/COMMIT explicitly via BEGIN IMMEDIATE; disable the
            # implicit transaction handling so no stray transaction is opened for us.
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row

        # journal_mode = WAL. On a :memory: database SQLite cannot use WAL and keeps
        # "memory" mode; that is an accepted no-op (tests needing real WAL use a file).
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout_ms)}")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("database connection is not open")
        return self._conn

    # -- submission ---------------------------------------------------------

    def submit(self, func: Callable[[sqlite3.Connection], T]) -> Future[T]:
        """Submit a callable to run on the owner thread; returns its :class:`Future`.

        The callable receives the owned connection. This is the synchronous
        primitive; :meth:`run` is the ``async`` wrapper most callers use.
        """
        if self._closed:
            raise RuntimeError("database is closed")
        future: Future[T] = Future()
        # The queue erases the concrete result type to ``object``; submit()'s
        # Future[T] return type restores it for the caller. The erased callable and
        # future refer to the same object the caller sees, so the cast is sound.
        item = cast(_WorkItem, (func, future))
        self._queue.put(item)
        return future

    async def run(self, func: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``func`` on the executor thread and await its result on the loop.

        The event loop is never blocked on SQLite: the work happens on the owner
        thread and this coroutine suspends until the :class:`Future` resolves.
        """
        return await asyncio.wrap_future(self.submit(func))

    # -- transactions -------------------------------------------------------

    @contextmanager
    def transaction(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        """Synchronous ``BEGIN IMMEDIATE`` context manager (single-writer discipline).

        Intended to be used *inside* a callable already running on the executor
        thread (``conn`` is the connection that callable was handed). It acquires the
        write lock up front with ``BEGIN IMMEDIATE``, yields the connection for the
        mutating work, and COMMITs on clean exit or ROLLBACKs on any exception.

        There is deliberately **no ``await`` inside this block** — it is a plain sync
        context manager — so the transaction cannot yield the event loop while
        holding the write lock and cannot interleave with another read-modify-write.
        """
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # -- maintenance --------------------------------------------------------

    async def wal_checkpoint_truncate(self) -> None:
        """Fold the WAL back into the main database file (``wal_checkpoint(TRUNCATE)``).

        A no-op in practice on an in-memory database (no WAL file exists); SQLite
        tolerates the pragma either way, so this is safe to call regardless of the
        backing store.
        """

        def _checkpoint(conn: sqlite3.Connection) -> None:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        await self.run(_checkpoint)

    # -- lifecycle ----------------------------------------------------------

    @property
    def is_memory(self) -> bool:
        """Whether this database is in-memory (where WAL is a no-op)."""
        return self._is_memory

    async def close(self) -> None:
        """Stop the executor thread and close the connection (on the owner thread)."""
        if self._closed:
            return
        self._closed = True
        self._queue.put(None)
        # Join off the event loop so we don't block it while the thread drains.
        await asyncio.get_running_loop().run_in_executor(None, self._thread.join)

    def close_sync(self) -> None:
        """Synchronous close for non-async contexts (e.g. tests, teardown)."""
        if self._closed:
            return
        self._closed = True
        self._queue.put(None)
        self._thread.join()
