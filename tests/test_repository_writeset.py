"""M2 task 14.2 / 14.3 / 14.5 — atomic write-set repository tests.

These exercise the detection persistence surface built in task 14.1
(:mod:`restockradar.persistence.repository`):

* **Property 15** (task 14.2) — repeated ``ON CONFLICT`` upserts to the same
  ``(subject_key, event_type)`` ``alert_state`` key and the same ``scope_key``
  ``backoff_state`` key leave EXACTLY ONE row holding the later values; the single
  NON-NULL key means no two rows ever collide via distinct NULLs.
* **Property 14** (task 14.3) — injecting a failure at EACH write step inside the
  write-set transaction leaves NO partial rows in any touched table (Req 6.3).
* **Atomicity + restart durability example** (task 14.5) — a full transition writes
  stored state + ``alert_state`` + ``alerts_outbox`` + ``alert_history`` +
  ``alert_deliveries`` atomically, and undelivered (pending) deliveries survive a
  simulated restart (close + reopen the temp-file DB).

Every test migrates a fresh database with the REAL
:func:`~restockradar.persistence.migrate.migrate` runner so writes hit the real schema
(FKs, CHECKs, the single-column ``alert_state`` / ``backoff_state`` keys). The suite-wide
offline guard in ``conftest.py`` fails any live network attempt.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from restockradar.domain.ids import (
    EventType,
    SubjectKind,
    listing_subject_key,
    scope_listing_key,
)
from restockradar.domain.models import Existence
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.repository import (
    AlertStateUpsert,
    DeliveryTarget,
    HistoryRow,
    OutboxRow,
    StateDelta,
    WriteSet,
    write_transition,
)
from restockradar.persistence.types import text_to_decimal

# ---------------------------------------------------------------------------
# Catalogue seeding shared by these tests
# ---------------------------------------------------------------------------

_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_PRODUCT_ID = "ncg-pokemon-booster"
_LISTING_ID = 1
_DEST_A = 1
_DEST_B = 2
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _migrated_db(path: str | Path = ":memory:") -> Database:
    db = Database(path)
    asyncio.run(migrate(db))
    return db


def _seed_catalogue(db: Database, *, with_destinations: bool = True) -> None:
    """Insert the game/retailer/product/listing (+ two destinations) the writes reference."""

    def _run(conn: Any) -> None:
        conn.execute("INSERT INTO games (id, name) VALUES (?, ?)", (_GAME_ID, "Pokemon"))
        conn.execute(
            "INSERT INTO retailers (id, name, adapter_type, base_url) VALUES (?, ?, ?, ?)",
            (_RETAILER_ID, "Northern Card Gaming", "shopify", "https://example.test"),
        )
        conn.execute(
            "INSERT INTO products (id, game_id, name) VALUES (?, ?, ?)",
            (_PRODUCT_ID, _GAME_ID, "A Pokemon product"),
        )
        conn.execute(
            "INSERT INTO listings "
            "  (id, config_key, product_id, retailer_id, url, handle, existence) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            (
                _LISTING_ID,
                "listing-config-1",
                _PRODUCT_ID,
                _RETAILER_ID,
                "https://example.test/products/a-pokemon-product",
                "a-pokemon-product",
            ),
        )
        if with_destinations:
            for dest_id, config_key in ((_DEST_A, "dest-a"), (_DEST_B, "dest-b")):
                conn.execute(
                    "INSERT INTO destinations "
                    "  (id, config_key, server_id, game_id, webhook_env_var) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (dest_id, config_key, "server-1", _GAME_ID, f"RR_WEBHOOK_{dest_id}"),
                )

    asyncio.run(db.run(_run))


def _run_on_db(db: Database, func: Any) -> Any:
    return asyncio.run(db.run(func))


def _count(db: Database, table: str) -> int:
    row = _run_on_db(db, lambda conn: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone())
    return int(row[0])


def _restock_transition_write_set() -> WriteSet:
    """A representative restock transition write-set fanned to two destinations."""
    subject_key = listing_subject_key(_LISTING_ID)
    return WriteSet(
        state=StateDelta(
            listing_id=_LISTING_ID,
            in_stock=True,
            set_in_stock=True,
            price=Decimal("12.99"),
            set_price=True,
            existence=Existence.EXISTING,
            set_existence=True,
            consecutive_404=0,
        ),
        alert_state=(
            AlertStateUpsert(
                subject_key=subject_key,
                event_type=EventType.RESTOCK,
                subject_kind=SubjectKind.LISTING,
                listing_id=_LISTING_ID,
                last_alert_at=_NOW,
                set_last_alert_at=True,
                pending=False,
                set_pending=True,
            ),
        ),
        outbox=OutboxRow(
            alert_kind="product",
            event_type=EventType.RESTOCK.value,
            game_id=_GAME_ID,
            subject_kind=SubjectKind.LISTING.value,
            listing_id=_LISTING_ID,
            feed_id=None,
            external_id=None,
            payload='{"title": "A Pokemon product"}',
            created_at=_NOW,
        ),
        history=HistoryRow(
            subject_kind=SubjectKind.LISTING.value,
            listing_id=_LISTING_ID,
            feed_id=None,
            external_id=None,
            event_type=EventType.RESTOCK.value,
            detected_at=_NOW,
            detail='{"in_stock": true}',
        ),
        deliveries=(
            DeliveryTarget(destination_id=_DEST_A, next_attempt_at=_NOW),
            DeliveryTarget(destination_id=_DEST_B, next_attempt_at=_NOW),
        ),
    )


# ===========================================================================
# Property 15 — keyed upserts yield exactly one row per key (task 14.2)
# ===========================================================================

_price_strategy = st.decimals(
    min_value=Decimal("0"),
    max_value=Decimal("100000"),
    allow_nan=False,
    allow_infinity=False,
    places=2,
)


@settings(max_examples=120, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    first_price=_price_strategy,
    second_price=_price_strategy,
    first_pending=st.booleans(),
    second_pending=st.booleans(),
    repeats=st.integers(min_value=2, max_value=6),
)
def test_property_15_keyed_upserts_yield_exactly_one_row_per_key(
    first_price: Decimal,
    second_price: Decimal,
    first_pending: bool,
    second_pending: bool,
    repeats: int,
) -> None:
    """Feature: stock-drop-monitor, Property 15: Keyed upserts yield exactly one row per key.

    Validates: Requirements 14.10, 14.12, 6.1

    For any ``(subject_key, event_type)`` and any ``scope_key``, repeated ON CONFLICT
    upserts leave EXACTLY ONE row holding the later values; the single NON-NULL key
    means no two rows ever share a logical key via distinct NULLs.
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db, with_destinations=False)
        subject_key = listing_subject_key(_LISTING_ID)
        scope_key = scope_listing_key(_LISTING_ID)

        def _upsert_many(conn: Any) -> None:
            with db.transaction(conn):
                # Repeatedly upsert the SAME alert_state key with the "first" values...
                for _ in range(repeats):
                    conn.execute(
                        "INSERT INTO alert_state "
                        "  (subject_key, event_type, subject_kind, listing_id, "
                        "   last_alerted_value, pending) "
                        "VALUES (?, 'price_change', 'listing', ?, ?, ?) "
                        "ON CONFLICT(subject_key, event_type) DO UPDATE SET "
                        "  last_alerted_value = excluded.last_alerted_value, "
                        "  pending = excluded.pending",
                        (
                            subject_key,
                            _LISTING_ID,
                            str(first_price),
                            1 if first_pending else 0,
                        ),
                    )
                # ...then the "second" values; the later write must win.
                conn.execute(
                    "INSERT INTO alert_state "
                    "  (subject_key, event_type, subject_kind, listing_id, "
                    "   last_alerted_value, pending) "
                    "VALUES (?, 'price_change', 'listing', ?, ?, ?) "
                    "ON CONFLICT(subject_key, event_type) DO UPDATE SET "
                    "  last_alerted_value = excluded.last_alerted_value, "
                    "  pending = excluded.pending",
                    (
                        subject_key,
                        _LISTING_ID,
                        str(second_price),
                        1 if second_pending else 0,
                    ),
                )
                # The SAME scope_key backoff_state row, upserted repeatedly too.
                for n in range(repeats):
                    conn.execute(
                        "INSERT INTO backoff_state "
                        "  (scope_key, scope_kind, listing_id, error_count) "
                        "VALUES (?, 'listing', ?, ?) "
                        "ON CONFLICT(scope_key) DO UPDATE SET "
                        "  error_count = excluded.error_count",
                        (scope_key, _LISTING_ID, n + 1),
                    )

        _run_on_db(db, _upsert_many)

        # Exactly one alert_state row for the key, holding the LATER values.
        rows = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT last_alerted_value, pending FROM alert_state "
                "WHERE subject_key = ? AND event_type = 'price_change'",
                (subject_key,),
            ).fetchall(),
        )
        assert len(rows) == 1
        assert text_to_decimal(rows[0]["last_alerted_value"]) == second_price
        assert rows[0]["pending"] == (1 if second_pending else 0)
        assert _count(db, "alert_state") == 1

        # Exactly one backoff_state row for the scope_key, holding the LAST count.
        backoff = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT error_count FROM backoff_state WHERE scope_key = ?",
                (scope_key,),
            ).fetchall(),
        )
        assert len(backoff) == 1
        assert backoff[0]["error_count"] == repeats
        assert _count(db, "backoff_state") == 1
    finally:
        db.close_sync()


# ===========================================================================
# Property 14 — a transaction rollback leaves no partial rows (task 14.3)
# ===========================================================================

#: The write steps inside write_transition, in order. The property injects a failure
#: at each one and asserts the whole transaction rolls back.
_WRITE_STEPS = ("state", "alert_state", "outbox", "history", "deliveries")


def _tables_touched_counts(db: Database) -> dict[str, int]:
    """Row counts for every table a transition writes to (plus the stored-state marker)."""
    return {
        "alert_state": _count(db, "alert_state"),
        "alerts_outbox": _count(db, "alerts_outbox"),
        "alert_history": _count(db, "alert_history"),
        "alert_deliveries": _count(db, "alert_deliveries"),
    }


@pytest.mark.parametrize("fail_step", _WRITE_STEPS)
def test_property_14_rollback_leaves_no_partial_rows(fail_step: str) -> None:
    """Feature: stock-drop-monitor, Property 14: A transaction rollback leaves no partial rows.

    Validates: Requirements 6.3

    Injecting a failure at EACH write step inside the atomic write-set transaction
    leaves NONE of that transition's rows: no stored-state change, no alert_state, no
    alerts_outbox, no alert_history, and no alert_deliveries. The event is re-detected
    unchanged next poll.
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db)

        # Pre-write baseline: listing exists, in_stock NULL (unseeded), no alert rows.
        before = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT in_stock, price FROM listings WHERE id = ?", (_LISTING_ID,)
            ).fetchone(),
        )
        assert before["in_stock"] is None
        assert before["price"] is None
        assert _tables_touched_counts(db) == {
            "alert_state": 0,
            "alerts_outbox": 0,
            "alert_history": 0,
            "alert_deliveries": 0,
        }

        write_set = _restock_transition_write_set()

        # Run the real write_transition, but hook the connection's execute() so the
        # chosen step raises mid-transaction. The step is identified by the SQL verb +
        # target table; the injected failure is a plain error AFTER the real statement
        # would have run, proving the enclosing BEGIN IMMEDIATE rolls everything back.
        injected = _InjectingDatabase(db, fail_on=fail_step)

        def _attempt(conn: Any) -> None:
            # write_transition issues BEGIN/COMMIT/ROLLBACK and all statements through
            # the connection object it is handed. We hand it a tripping wrapper so the
            # chosen step raises; _InjectingDatabase.transaction runs the REAL
            # Database.transaction on that same wrapper, so the real ROLLBACK fires.
            tripping = _TrippingConnection(conn, injected.verb, injected.table)
            write_transition(injected, tripping, write_set)  # type: ignore[arg-type]

        with pytest.raises(_InjectedFailure):
            _run_on_db(db, _attempt)

        # After the aborted transaction, NOTHING of the transition persisted (Req 6.3).
        after = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT in_stock, price FROM listings WHERE id = ?", (_LISTING_ID,)
            ).fetchone(),
        )
        assert after["in_stock"] is None
        assert after["price"] is None
        assert _tables_touched_counts(db) == {
            "alert_state": 0,
            "alerts_outbox": 0,
            "alert_history": 0,
            "alert_deliveries": 0,
        }
    finally:
        db.close_sync()


class _InjectedFailure(RuntimeError):
    """The deliberate failure injected at a chosen write step."""


class _InjectingDatabase:
    """A thin :class:`Database` proxy whose ``transaction`` runs the REAL
    :meth:`Database.transaction` on the (already-tripping) connection it is handed.

    ``write_transition`` opens the transaction and issues every statement through the
    connection argument it receives — which the test makes a :class:`_TrippingConnection`
    — so BEGIN IMMEDIATE / the write statements / ROLLBACK all run on that wrapper. The
    wrapper raises after the targeted step's statement executes, and the real
    :meth:`Database.transaction` catches the exception and issues ROLLBACK, undoing the
    partial work (Req 6.3).
    """

    #: Maps a logical write step to a (SQL-verb, table) signature to trip on.
    _STEP_SIGNATURES: ClassVar[dict[str, tuple[str, str]]] = {
        "state": ("UPDATE", "LISTINGS"),
        "alert_state": ("INSERT", "ALERT_STATE"),
        "outbox": ("INSERT", "ALERTS_OUTBOX"),
        "history": ("INSERT", "ALERT_HISTORY"),
        "deliveries": ("INSERT", "ALERT_DELIVERIES"),
    }

    def __init__(self, db: Database, *, fail_on: str) -> None:
        self._db = db
        self.verb, self.table = self._STEP_SIGNATURES[fail_on]

    def transaction(self, conn: Any) -> Any:
        # ``conn`` here is the _TrippingConnection write_transition was handed; run the
        # genuine Database.transaction on it so the real BEGIN/COMMIT/ROLLBACK execute.
        return self._db.transaction(conn)


class _TrippingConnection:
    """Wraps a sqlite3 connection; raises after the targeted statement executes."""

    def __init__(self, conn: Any, verb: str, table: str) -> None:
        self._conn = conn
        self._verb = verb
        self._table = table

    def execute(self, sql: str, params: Any = ()) -> Any:
        result = self._conn.execute(sql, params)
        upper = sql.upper()
        is_target = (
            upper.startswith(self._verb)
            and self._table in upper
            and "SCHEMA_MIGRATIONS" not in upper
        )
        if is_target:
            raise _InjectedFailure(f"injected failure at {self._verb} {self._table}")
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


# ===========================================================================
# Atomicity + restart durability example (task 14.5)
# ===========================================================================


def test_atomic_transition_and_restart_durability(tmp_path: Path) -> None:
    """A full transition writes every row atomically; undelivered deliveries survive restart.

    Validates: Requirements 21.11, 14.21

    Uses a temp-FILE DB so the "restart" is a genuine close + reopen. One restock
    transition writes stored state + alert_state + outbox + history + two pending
    deliveries in one transaction; after close/reopen the pending deliveries are byte
    identical and still deliverable (Req 6.5 / 14.21).
    """
    db_path = tmp_path / "restockradar.db"
    db = _migrated_db(db_path)
    try:
        _seed_catalogue(db)
        write_set = _restock_transition_write_set()
        _run_on_db(db, lambda conn: write_transition(db, conn, write_set))

        # Everything landed in one go.
        listing = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT in_stock, price, existence FROM listings WHERE id = ?",
                (_LISTING_ID,),
            ).fetchone(),
        )
        assert listing["in_stock"] == 1
        assert text_to_decimal(listing["price"]) == Decimal("12.99")
        assert listing["existence"] == 1
        assert _count(db, "alert_state") == 1
        assert _count(db, "alerts_outbox") == 1
        assert _count(db, "alert_history") == 1
        assert _count(db, "alert_deliveries") == 2

        # Snapshot the pending deliveries before "restart".
        before = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT alert_id, destination_id, status, attempts, next_attempt_at "
                "FROM alert_deliveries ORDER BY destination_id"
            ).fetchall(),
        )
        before_rows = [tuple(r) for r in before]
        assert all(r["status"] == "pending" for r in before)
    finally:
        # close_sync() stops the executor and closes the connection on its owner
        # thread, folding the committed WAL into the file — a genuine shutdown.
        db.close_sync()

    # --- simulated restart: reopen the SAME file; no migration re-run needed ---
    db2 = Database(db_path)
    try:
        after = _run_on_db(
            db2,
            lambda conn: conn.execute(
                "SELECT alert_id, destination_id, status, attempts, next_attempt_at "
                "FROM alert_deliveries ORDER BY destination_id"
            ).fetchall(),
        )
        after_rows = [tuple(r) for r in after]
        # Undelivered deliveries survived the restart identically (Req 6.5 / 14.21).
        assert after_rows == before_rows
        assert len(after_rows) == 2
        assert all(r["status"] == "pending" for r in after)
        # The outbox + history rows are durable too.
        assert _count(db2, "alerts_outbox") == 1
        assert _count(db2, "alert_history") == 1
    finally:
        db2.close_sync()
