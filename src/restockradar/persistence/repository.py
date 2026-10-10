"""Typed repository methods and the single-writer transaction helpers (synchronous,
run on the executor).

**Scope of THIS module today — the baseline write path (task 34.1).** This file is the
narrow precursor to the full atomic write-set that M2 task 14.1 will build here. For
Milestone 1 it carries exactly what the dry-run ``--once`` baseline needs so
``Baseline_Seeding`` actually persists (Req 2.1, 2.2, 2.3, 6.1): the first complete
poll records a subject's *last-observed* state and marks it seeded, and does so WITHOUT
emitting any alert.

Two seed operations, each performed inside ONE synchronous ``BEGIN IMMEDIATE``
transaction on the :class:`~restockradar.persistence.db.Database` executor (Req 14.16 —
single-writer discipline, no ``await`` inside the lock):

* **Listing baseline** (:func:`seed_listing` / :func:`write_listing_baseline`). Writes
  the listing's *last-observed* ``in_stock`` / ``price`` / ``existence`` (the price as a
  canonical Decimal TEXT via :func:`~restockradar.persistence.types.decimal_to_text`;
  note this is the last-OBSERVED price on ``listings.price``, **not** the
  last_alerted_price), and UPSERTs **both** ``alert_state`` rows for the listing in the
  SAME transaction (Req 2.2):

  - the ``restock`` row with ``pending = 0`` and ``last_alert_at`` (the
    last_restock_alert_at) NULL — the listing starts with no restock alerted and no
    deferral pending;
  - the ``price_change`` row with ``last_alerted_value`` set to the fetched price as
    canonical Decimal TEXT — so the next poll compares against the seeded price and a
    first observation never reads as a price change.

  Finally it stamps ``listings.seeded_at`` with the baseline instant (UTC ISO text via
  :func:`~restockradar.persistence.types.datetime_to_text`, Req 2.1). It emits NO alerts
  and writes NO ``alerts_outbox`` / ``alert_deliveries`` rows.

* **Feed baseline** (:func:`seed_feed` / :func:`write_feed_baseline`). INSERTs a
  ``seen_products`` row for **every** discovered product — regardless of keyword match
  (Req 3.5) — keyed by ``(retailer_id, game_id, external_id)``, and stamps
  ``discovery_feeds.seeded_at``. Again, no alerts and no outbox rows: the baseline simply
  records what already exists so the *next* poll can detect genuinely new products.

Both ``alert_state`` upserts use ``ON CONFLICT(subject_key, event_type)`` so re-running a
baseline is idempotent on the keyed rows (Req 6.1 keyed upsert). ``seen_products`` uses
``INSERT ... ON CONFLICT DO NOTHING`` so a product already recorded in a prior cycle is
left untouched (its original ``first_seen_at`` is preserved).

**Scope added by M2 task 14.1 — the generalised read helpers, the atomic write-set, and
the no-alert state writers.** The baseline path above is a narrow precursor; task 14.1
builds the full detection persistence surface on top of the same single-writer
discipline (Req 14.16 — every write runs on the DB executor inside ONE synchronous
``BEGIN IMMEDIATE`` transaction, with no ``await`` inside the lock):

* **Read helpers** (:func:`read_listing_state`, :func:`read_feed_state`,
  :func:`read_alert_state`, :func:`read_healthy_destinations_for_game`,
  :func:`read_backoff_state`, :func:`read_open_incidents`,
  :func:`read_due_feeds` / :func:`read_due_listings`). Each is a *synchronous* function
  run on the executor via :func:`Database.run`; the results are frozen dataclasses so the
  diff layer (task 16/17) receives immutable, fully-typed snapshots. These are the reads
  the design's sequence takes INSIDE the transaction, after the (lock-free) fetch, so the
  state the diff sees cannot change between read and write.

* **The atomic write-set** (:func:`write_transition` / :func:`apply_write_set`, driven by
  the typed :class:`WriteSet` the fan-out layer builds). Inside ONE ``BEGIN IMMEDIATE``
  transaction it writes, together (Req 6.1/6.2): the updated stored listing state
  (``in_stock`` / ``price`` / ``existence`` / ``consecutive_404`` as applicable); the
  updated ``alert_state`` row(s) for the subject + event type via
  ``INSERT ... ON CONFLICT(subject_key, event_type) DO UPDATE`` (keyed upsert — exactly
  one row per key, Req 14.10); one ``alerts_outbox`` row for the emitted alert; the
  ``alert_history`` row; and one ``alert_deliveries`` row (``status='pending'``,
  ``next_attempt_at = now``) per matching Destination. If anything inside the transaction
  raises, the context manager ROLLBACKs and NOTHING persists, so the event is re-detected
  on the next poll (Req 6.3).

* **No-alert state writers** (:func:`write_state_only`). The Req 6.6 / 6.7 path: when a
  poll changes stored state WITHOUT emitting an alert (set/clear ``restock_pending``;
  update ``in_stock`` / ``existence`` / ``consecutive_404``), that stored change and any
  ``alert_state`` change for the subject are written in ONE transaction. A ``NOT_FOUND``
  fetch deliberately leaves ``in_stock`` / ``price`` / the price_change ``alert_state``
  untouched (Req 7.4) — the caller models that by omitting those fields.

Every helper here runs ON the executor and holds no ``await`` inside the lock, preserving
single-writer serialisation (Req 14.16).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from restockradar.domain.ids import (
    EventType,
    ScopeKey,
    SubjectKey,
    SubjectKind,
    listing_subject_key,
)
from restockradar.domain.models import Existence, HealthStatus
from restockradar.persistence.db import Database
from restockradar.persistence.types import (
    bool_to_int,
    datetime_to_text,
    decimal_to_text,
    int_to_bool,
    text_to_datetime,
    text_to_decimal,
)

__all__ = [
    "AdminNoticeRow",
    "AlertStateRow",
    "AlertStateUpsert",
    "BackoffStateRow",
    "BackoffStateUpsert",
    "BaselineListing",
    "BaselineProduct",
    "DeliveryTarget",
    "DueFeed",
    "DueListing",
    "FeedFetchInputs",
    "FeedStateRow",
    "HandleRenameWrite",
    "HistoryRow",
    "IncidentRow",
    "ListingFetchInputs",
    "ListingStateRow",
    "OutboxRow",
    "RetailerFetchInputs",
    "StateDelta",
    "StateOnlyWrite",
    "WriteSet",
    "apply_handle_rename",
    "apply_state_only",
    "apply_write_set",
    "read_alert_state",
    "read_all_enabled_feed_inputs",
    "read_all_enabled_listing_inputs",
    "read_backoff_state",
    "read_due_feed_inputs",
    "read_due_feeds",
    "read_due_listing_inputs",
    "read_due_listings",
    "read_feed_state",
    "read_healthy_destinations_for_game",
    "read_listing_state",
    "read_open_incidents",
    "read_retailer_inputs",
    "seed_feed",
    "seed_listing",
    "set_feed_next_due_at",
    "set_listing_next_due_at",
    "write_admin_notice",
    "write_backoff_state",
    "write_feed_baseline",
    "write_handle_rename",
    "write_listing_baseline",
    "write_state_only",
    "write_transition",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class BaselineListing:
    """The last-observed state recorded for a listing's first complete poll (Req 2.3).

    ``in_stock`` / ``price`` / ``existence`` are the values the adapter just observed —
    the *last-observed* state, not an alerting decision. ``price`` is ``None`` when the
    listing exposed no priced variant; it is stored on ``listings.price`` as the
    last-observed price AND copied to the ``price_change`` ``alert_state`` row's
    ``last_alerted_value`` so the next poll has a baseline to compare against (Req 2.2).
    ``seeded_at`` is the UTC instant the baseline ran (Req 2.1).
    """

    listing_id: int
    in_stock: bool
    price: Decimal | None
    existence: Existence
    seeded_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class BaselineProduct:
    """One discovered product recorded in ``seen_products`` during a feed baseline.

    Every discovered product is recorded regardless of keyword match (Req 3.5), so the
    de-dup store reflects the full catalogue the baseline saw and the next poll only
    alerts on products that are genuinely new.
    """

    external_id: str
    title: str | None
    handle: str | None


# ---------------------------------------------------------------------------
# Listing baseline (synchronous — runs inside the executor transaction)
# ---------------------------------------------------------------------------


def write_listing_baseline(
    db: Database, conn: sqlite3.Connection, listing: BaselineListing
) -> None:
    """Persist a listing's baseline inside ONE ``BEGIN IMMEDIATE`` transaction.

    Must be called on the DB executor thread (``conn`` is the owned connection). Inside
    a single transaction (Req 14.16) it:

    1. writes the listing's last-observed ``in_stock`` / ``price`` / ``existence`` and
       stamps ``seeded_at`` (Req 2.1, 2.3);
    2. UPSERTs the ``restock`` ``alert_state`` row (``pending = 0``, ``last_alert_at``
       NULL) and the ``price_change`` row (``last_alerted_value`` = the fetched price),
       both keyed by ``listing:{id}`` (Req 2.2).

    It emits no alerts and writes no ``alerts_outbox`` / ``alert_deliveries`` rows.
    """
    price_text = None if listing.price is None else decimal_to_text(listing.price)
    seeded_text = datetime_to_text(listing.seeded_at)
    existence_int = bool_to_int(listing.existence is Existence.EXISTING)
    subject_key = listing_subject_key(listing.listing_id)

    with db.transaction(conn):
        conn.execute(
            "UPDATE listings "
            "SET in_stock = ?, price = ?, existence = ?, seeded_at = ? "
            "WHERE id = ?",
            (
                bool_to_int(listing.in_stock),
                price_text,
                existence_int,
                seeded_text,
                listing.listing_id,
            ),
        )
        # Both cooldown-bearing alert_state rows are seeded together (Req 2.2). The
        # restock row starts un-alerted and not pending; the price_change row records
        # the observed price as the last_alerted_value baseline. ON CONFLICT keeps the
        # write idempotent on the (subject_key, event_type) key (Req 6.1).
        conn.execute(
            "INSERT INTO alert_state "
            "  (subject_key, event_type, subject_kind, listing_id, "
            "   last_alert_at, last_alerted_value, pending) "
            "VALUES (?, ?, ?, ?, NULL, NULL, 0) "
            "ON CONFLICT(subject_key, event_type) DO UPDATE SET "
            "  last_alert_at = NULL, last_alerted_value = NULL, pending = 0",
            (
                subject_key,
                EventType.RESTOCK.value,
                SubjectKind.LISTING.value,
                listing.listing_id,
            ),
        )
        conn.execute(
            "INSERT INTO alert_state "
            "  (subject_key, event_type, subject_kind, listing_id, "
            "   last_alert_at, last_alerted_value, pending) "
            "VALUES (?, ?, ?, ?, NULL, ?, 0) "
            "ON CONFLICT(subject_key, event_type) DO UPDATE SET "
            "  last_alerted_value = excluded.last_alerted_value, pending = 0",
            (
                subject_key,
                EventType.PRICE_CHANGE.value,
                SubjectKind.LISTING.value,
                listing.listing_id,
                price_text,
            ),
        )


async def seed_listing(db: Database, listing: BaselineListing) -> None:
    """Async wrapper: run :func:`write_listing_baseline` on the DB executor (Req 2.1-2.3)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_listing_baseline(db, conn, listing)

    await db.run(_run)


# ---------------------------------------------------------------------------
# Feed baseline (synchronous — runs inside the executor transaction)
# ---------------------------------------------------------------------------


def write_feed_baseline(
    db: Database,
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    retailer_id: str,
    game_id: str,
    products: Sequence[BaselineProduct],
    seeded_at: datetime,
) -> None:
    """Record a feed's discovered products and mark the feed seeded (Req 2.1, 3.5, 6.1).

    Must be called on the DB executor thread. Inside ONE ``BEGIN IMMEDIATE`` transaction
    it INSERTs a ``seen_products`` row for every discovered product — regardless of
    include/exclude keyword match (Req 3.5) — keyed by
    ``(retailer_id, game_id, external_id)``, and stamps ``discovery_feeds.seeded_at``
    (Req 2.1). A product already present from a prior cycle is left untouched
    (``ON CONFLICT DO NOTHING``), preserving its original ``first_seen_at``. No alerts
    and no ``alerts_outbox`` rows are written.
    """
    seeded_text = datetime_to_text(seeded_at)

    with db.transaction(conn):
        for product in products:
            conn.execute(
                "INSERT INTO seen_products "
                "  (retailer_id, game_id, external_id, first_seen_at, title, handle) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(retailer_id, game_id, external_id) DO NOTHING",
                (
                    retailer_id,
                    game_id,
                    product.external_id,
                    seeded_text,
                    product.title,
                    product.handle,
                ),
            )
        conn.execute(
            "UPDATE discovery_feeds SET seeded_at = ? WHERE id = ?",
            (seeded_text, feed_id),
        )


async def seed_feed(
    db: Database,
    *,
    feed_id: int,
    retailer_id: str,
    game_id: str,
    products: Sequence[BaselineProduct],
    seeded_at: datetime,
) -> None:
    """Async wrapper: run :func:`write_feed_baseline` on the DB executor (Req 2.1, 3.5)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_feed_baseline(
            db,
            conn,
            feed_id=feed_id,
            retailer_id=retailer_id,
            game_id=game_id,
            products=products,
            seeded_at=seeded_at,
        )

    await db.run(_run)

# ===========================================================================
# M2 task 14.1 — read helpers (frozen snapshots, run on the executor)
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class ListingStateRow:
    """A snapshot of a listing's stored (last-observed) state (Req 14.4).

    These are the columns the diff layer reads INSIDE the transaction before deciding
    which events fire (Req 6.1 ordering). ``in_stock`` / ``price`` / ``existence`` are
    ``None`` while the listing is unseeded; ``price`` is the last OBSERVED price, never
    the last_alerted_price (that lives on the price_change ``alert_state`` row).
    """

    listing_id: int
    in_stock: bool | None
    price: Decimal | None
    existence: Existence | None
    consecutive_404: int
    seeded_at: datetime | None


@dataclass(frozen=True, slots=True, kw_only=True)
class FeedStateRow:
    """A snapshot of a discovery feed's stored baseline marker (Req 14.5)."""

    feed_id: int
    retailer_id: str
    game_id: str
    seeded_at: datetime | None


@dataclass(frozen=True, slots=True, kw_only=True)
class AlertStateRow:
    """A cooldown-authority ``alert_state`` row for a subject + event type (Req 14.10).

    Only ``restock`` and ``price_change`` ever own a row here (Req 9.6). ``last_alert_at``
    is the restock row's last_restock_alert_at; ``last_alerted_value`` is the price_change
    row's authoritative last_alerted_price (an exact Decimal). ``pending`` is the restock
    row's restock_pending flag.
    """

    subject_key: SubjectKey
    event_type: EventType
    last_alert_at: datetime | None
    last_alerted_value: Decimal | None
    pending: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class BackoffStateRow:
    """A ``backoff_state`` row keyed by a single NON-NULL ``scope_key`` (Req 14.12)."""

    scope_key: ScopeKey
    error_count: int
    blocked_count: int
    empty_count: int
    backoff_until: datetime | None
    last_outcome: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class IncidentRow:
    """An open (``closed_at IS NULL``) ``incidents`` row for a scope (Req 14.13)."""

    incident_id: int
    scope_key: ScopeKey
    kind: str
    opened_at: datetime
    last_warning_at: datetime | None


@dataclass(frozen=True, slots=True, kw_only=True)
class DueFeed:
    """A due discovery feed selected for a poll (Req 1.6 effective-due ordering)."""

    feed_id: int
    retailer_id: str
    game_id: str
    next_due_at: datetime | None


@dataclass(frozen=True, slots=True, kw_only=True)
class DueListing:
    """A due listing selected for a poll (Req 1.6 effective-due ordering)."""

    listing_id: int
    retailer_id: str
    next_due_at: datetime | None


def _opt_decimal(text: str | None) -> Decimal | None:
    """Decode an optional canonical Decimal TEXT column, preserving ``NULL`` as ``None``."""
    return None if text is None else text_to_decimal(text)


def _opt_datetime(text: str | None) -> datetime | None:
    """Decode an optional ISO-8601 UTC TEXT column, preserving ``NULL`` as ``None``."""
    return None if text is None else text_to_datetime(text)


def _opt_existence(value: int | None) -> Existence | None:
    """Decode the stored ``existence`` integer (``1``/``0``/``NULL``) to the enum/None."""
    if value is None:
        return None
    return Existence.EXISTING if int_to_bool(value) else Existence.NOT_FOUND


def read_listing_state(
    conn: sqlite3.Connection, listing_id: int
) -> ListingStateRow | None:
    """Read a listing's stored state (synchronous; run on the executor, Req 14.4).

    Returns ``None`` if no listing with that id exists. Decodes the TEXT/INTEGER columns
    into the richer :class:`ListingStateRow` snapshot the diff layer consumes.
    """
    row = conn.execute(
        "SELECT id, in_stock, price, existence, consecutive_404, seeded_at "
        "FROM listings WHERE id = ?",
        (listing_id,),
    ).fetchone()
    if row is None:
        return None
    return ListingStateRow(
        listing_id=int(row["id"]),
        in_stock=None if row["in_stock"] is None else int_to_bool(row["in_stock"]),
        price=_opt_decimal(row["price"]),
        existence=_opt_existence(row["existence"]),
        consecutive_404=int(row["consecutive_404"]),
        seeded_at=_opt_datetime(row["seeded_at"]),
    )


def read_feed_state(conn: sqlite3.Connection, feed_id: int) -> FeedStateRow | None:
    """Read a discovery feed's stored baseline marker (synchronous; Req 14.5).

    Returns ``None`` if no feed with that id exists.
    """
    row = conn.execute(
        "SELECT id, retailer_id, game_id, seeded_at FROM discovery_feeds WHERE id = ?",
        (feed_id,),
    ).fetchone()
    if row is None:
        return None
    return FeedStateRow(
        feed_id=int(row["id"]),
        retailer_id=str(row["retailer_id"]),
        game_id=str(row["game_id"]),
        seeded_at=_opt_datetime(row["seeded_at"]),
    )


def read_alert_state(
    conn: sqlite3.Connection, subject_key: SubjectKey
) -> dict[EventType, AlertStateRow]:
    """Read all ``alert_state`` rows for a subject, keyed by event type (Req 14.10).

    Returns a mapping ``{EventType: AlertStateRow}`` holding whichever of the two
    cooldown-bearing rows (``restock`` / ``price_change``) currently exist for the
    subject. An absent row means the subject is unseeded for that event type.
    """
    rows = conn.execute(
        "SELECT subject_key, event_type, last_alert_at, last_alerted_value, pending "
        "FROM alert_state WHERE subject_key = ?",
        (subject_key,),
    ).fetchall()
    result: dict[EventType, AlertStateRow] = {}
    for row in rows:
        event_type = EventType(row["event_type"])
        result[event_type] = AlertStateRow(
            subject_key=str(row["subject_key"]),
            event_type=event_type,
            last_alert_at=_opt_datetime(row["last_alert_at"]),
            last_alerted_value=_opt_decimal(row["last_alerted_value"]),
            pending=int_to_bool(row["pending"]),
        )
    return result


def read_healthy_destinations_for_game(
    conn: sqlite3.Connection, game_id: str
) -> list[int]:
    """Return the enabled + healthy NON-admin destination ids for a game (Req 8.1, 9.1).

    The fan-out target set: ``enabled = 1`` AND ``health_status = 'healthy'`` AND
    ``is_admin = 0`` AND ``game_id = ?``. Admin destinations are excluded here — they
    receive operational warnings, not product alerts. Ordered by id for determinism.
    """
    rows = conn.execute(
        "SELECT id FROM destinations "
        "WHERE game_id = ? AND enabled = 1 AND is_admin = 0 AND health_status = ? "
        "ORDER BY id",
        (game_id, HealthStatus.HEALTHY.value),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def read_backoff_state(
    conn: sqlite3.Connection, scope_key: ScopeKey
) -> BackoffStateRow | None:
    """Read the ``backoff_state`` row for a scope key (synchronous; Req 14.12).

    Returns ``None`` when the scope has no backoff row yet (never failed).
    """
    row = conn.execute(
        "SELECT scope_key, error_count, blocked_count, empty_count, "
        "       backoff_until, last_outcome "
        "FROM backoff_state WHERE scope_key = ?",
        (scope_key,),
    ).fetchone()
    if row is None:
        return None
    return BackoffStateRow(
        scope_key=str(row["scope_key"]),
        error_count=int(row["error_count"]),
        blocked_count=int(row["blocked_count"]),
        empty_count=int(row["empty_count"]),
        backoff_until=_opt_datetime(row["backoff_until"]),
        last_outcome=row["last_outcome"],
    )


def read_open_incidents(
    conn: sqlite3.Connection, scope_key: ScopeKey
) -> list[IncidentRow]:
    """Return the OPEN (``closed_at IS NULL``) incidents for a scope key (Req 14.13)."""
    rows = conn.execute(
        "SELECT id, scope_key, kind, opened_at, last_warning_at "
        "FROM incidents WHERE scope_key = ? AND closed_at IS NULL "
        "ORDER BY id",
        (scope_key,),
    ).fetchall()
    return [
        IncidentRow(
            incident_id=int(row["id"]),
            scope_key=str(row["scope_key"]),
            kind=str(row["kind"]),
            opened_at=text_to_datetime(row["opened_at"]),
            last_warning_at=_opt_datetime(row["last_warning_at"]),
        )
        for row in rows
    ]


def read_due_feeds(
    conn: sqlite3.Connection, retailer_id: str, now: datetime
) -> list[DueFeed]:
    """Return the enabled feeds for a retailer due at or before ``now`` (Req 1.6).

    A feed with a ``NULL`` ``next_due_at`` (never scheduled / freshly enabled) is due.
    Ordered by effective due time ascending (``NULL`` first), matching the design's
    feeds-before-listings, earliest-first polling order.
    """
    now_text = datetime_to_text(now)
    rows = conn.execute(
        "SELECT id, retailer_id, game_id, next_due_at FROM discovery_feeds "
        "WHERE retailer_id = ? AND enabled = 1 "
        "  AND (next_due_at IS NULL OR next_due_at <= ?) "
        "ORDER BY next_due_at IS NOT NULL, next_due_at ASC, id ASC",
        (retailer_id, now_text),
    ).fetchall()
    return [
        DueFeed(
            feed_id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            next_due_at=_opt_datetime(row["next_due_at"]),
        )
        for row in rows
    ]


def read_due_listings(
    conn: sqlite3.Connection, retailer_id: str, now: datetime
) -> list[DueListing]:
    """Return the enabled listings for a retailer due at or before ``now`` (Req 1.6).

    As with :func:`read_due_feeds`, a ``NULL`` ``next_due_at`` is treated as due and
    sorts first; the result is ordered by effective due time ascending.
    """
    now_text = datetime_to_text(now)
    rows = conn.execute(
        "SELECT id, retailer_id, next_due_at FROM listings "
        "WHERE retailer_id = ? AND enabled = 1 "
        "  AND (next_due_at IS NULL OR next_due_at <= ?) "
        "ORDER BY next_due_at IS NOT NULL, next_due_at ASC, id ASC",
        (retailer_id, now_text),
    ).fetchall()
    return [
        DueListing(
            listing_id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            next_due_at=_opt_datetime(row["next_due_at"]),
        )
        for row in rows
    ]


# ===========================================================================
# M2 task 14.1 — the typed atomic write-set (Req 6.1/6.2/6.3)
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class StateDelta:
    """The stored-listing columns a transition updates, inside the write-set txn.

    Each field is *optional*: ``None`` (via the sentinel absence described below) means
    "leave this column unchanged". Because ``None`` is also a legitimate stored value
    for ``in_stock`` / ``price`` / ``existence`` (an unseeded column), each updatable
    column carries a companion ``*_set`` boolean so the caller can distinguish "write
    NULL" from "do not touch". This lets a ``NOT_FOUND`` fetch leave ``in_stock`` /
    ``price`` untouched (Req 7.4) by setting only ``existence`` / ``consecutive_404``.
    """

    listing_id: int
    in_stock: bool | None = None
    set_in_stock: bool = False
    price: Decimal | None = None
    set_price: bool = False
    existence: Existence | None = None
    set_existence: bool = False
    consecutive_404: int | None = None

    def columns(self) -> tuple[list[str], list[object]]:
        """Build the ``SET`` fragments and bound params for the columns to update."""
        assignments: list[str] = []
        params: list[object] = []
        if self.set_in_stock:
            assignments.append("in_stock = ?")
            params.append(None if self.in_stock is None else bool_to_int(self.in_stock))
        if self.set_price:
            assignments.append("price = ?")
            params.append(None if self.price is None else decimal_to_text(self.price))
        if self.set_existence:
            assignments.append("existence = ?")
            params.append(
                None
                if self.existence is None
                else bool_to_int(self.existence is Existence.EXISTING)
            )
        if self.consecutive_404 is not None:
            assignments.append("consecutive_404 = ?")
            params.append(self.consecutive_404)
        return assignments, params


@dataclass(frozen=True, slots=True, kw_only=True)
class AlertStateUpsert:
    """One keyed ``alert_state`` upsert for a subject + cooldown-bearing event type.

    Written with ``INSERT ... ON CONFLICT(subject_key, event_type) DO UPDATE`` so the
    key holds exactly one row (Req 14.10, 6.1). Only the columns the caller marks are
    overwritten on conflict, mirroring :class:`StateDelta`'s set-flag discipline so a
    page-live upsert that touches only ``last_alerted_value`` does not clobber
    ``pending`` / ``last_alert_at``.
    """

    subject_key: SubjectKey
    event_type: EventType
    subject_kind: SubjectKind
    listing_id: int | None = None
    feed_id: int | None = None
    external_id: str | None = None
    last_alert_at: datetime | None = None
    set_last_alert_at: bool = False
    last_alerted_value: Decimal | None = None
    set_last_alerted_value: bool = False
    pending: bool | None = None
    set_pending: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class OutboxRow:
    """The ``alerts_outbox`` row for one emitted alert (Req 14.8)."""

    alert_kind: str
    event_type: str
    game_id: str | None
    subject_kind: str | None
    listing_id: int | None
    feed_id: int | None
    external_id: str | None
    payload: str
    created_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class HistoryRow:
    """The append-only ``alert_history`` row for one emitted alert (Req 14.11)."""

    subject_kind: str
    listing_id: int | None
    feed_id: int | None
    external_id: str | None
    event_type: str
    detected_at: datetime
    detail: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DeliveryTarget:
    """One pending ``alert_deliveries`` row to create for a matching destination.

    Created with ``status='pending'`` and ``next_attempt_at = now`` so the dispatch
    loop (M3) picks it up immediately (Req 6.2, 10). One per enabled + healthy
    destination the fan-out layer resolved (Req 8.1).
    """

    destination_id: int
    next_attempt_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WriteSet:
    """The full typed write-set for one detected transition (Req 6.1/6.2).

    The fan-out layer (task 17) builds this in memory from the diff output + resolved
    destinations + clock, then hands it to :func:`write_transition`, which persists
    EVERYTHING below inside ONE ``BEGIN IMMEDIATE`` transaction:

    * ``state`` — the stored-listing column updates (optional);
    * ``alert_state`` — the keyed ``alert_state`` upsert(s) for the subject;
    * ``outbox`` — the single ``alerts_outbox`` row for the emitted alert;
    * ``history`` — the append-only ``alert_history`` row;
    * ``deliveries`` — one ``alert_deliveries`` row per matching destination.

    If the transaction fails nothing persists (Req 6.3).
    """

    state: StateDelta | None = None
    alert_state: tuple[AlertStateUpsert, ...] = ()
    outbox: OutboxRow
    history: HistoryRow
    deliveries: tuple[DeliveryTarget, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class StateOnlyWrite:
    """A no-alert state write (Req 6.6 / 6.7).

    Carries only the stored-state delta and any ``alert_state`` change for the subject
    (e.g. set/clear ``restock_pending``), with NO outbox / history / deliveries. Written
    in one transaction by :func:`write_state_only`.
    """

    state: StateDelta | None = None
    alert_state: tuple[AlertStateUpsert, ...] = ()


def _apply_state_delta(conn: sqlite3.Connection, delta: StateDelta) -> None:
    """Write a :class:`StateDelta` as a single ``UPDATE listings`` (no-op if empty)."""
    assignments, params = delta.columns()
    if not assignments:
        return
    params.append(delta.listing_id)
    # The column fragments are internal string literals (never user input); every value
    # is a bound parameter, so this interpolation is not an injection vector.
    update_sql = f"UPDATE listings SET {', '.join(assignments)} WHERE id = ?"
    conn.execute(update_sql, tuple(params))


def _apply_alert_state_upsert(
    conn: sqlite3.Connection, upsert: AlertStateUpsert
) -> None:
    """Write one keyed ``alert_state`` upsert (Req 14.10, 6.1 keyed upsert).

    Inserts the row with the provided values (unset columns default to ``NULL`` /
    ``pending=0``), and ON CONFLICT overwrites ONLY the columns the caller marked set,
    so a partial update (e.g. page-live touching only ``last_alerted_value``) leaves the
    other columns intact.
    """
    insert_last_alert_at = (
        datetime_to_text(upsert.last_alert_at)
        if upsert.set_last_alert_at and upsert.last_alert_at is not None
        else None
    )
    insert_last_value = (
        decimal_to_text(upsert.last_alerted_value)
        if upsert.set_last_alerted_value and upsert.last_alerted_value is not None
        else None
    )
    insert_pending = (
        bool_to_int(bool(upsert.pending)) if upsert.set_pending else 0
    )

    conflict_updates: list[str] = []
    if upsert.set_last_alert_at:
        conflict_updates.append("last_alert_at = excluded.last_alert_at")
    if upsert.set_last_alerted_value:
        conflict_updates.append("last_alerted_value = excluded.last_alerted_value")
    if upsert.set_pending:
        conflict_updates.append("pending = excluded.pending")

    if conflict_updates:
        conflict_clause = (
            "ON CONFLICT(subject_key, event_type) DO UPDATE SET "
            + ", ".join(conflict_updates)
        )
    else:
        # Nothing mutable marked: keep the row untouched on conflict (idempotent insert).
        conflict_clause = "ON CONFLICT(subject_key, event_type) DO NOTHING"

    conn.execute(
        "INSERT INTO alert_state "
        "  (subject_key, event_type, subject_kind, listing_id, feed_id, external_id, "
        "   last_alert_at, last_alerted_value, pending) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        f"{conflict_clause}",
        (
            upsert.subject_key,
            upsert.event_type.value,
            upsert.subject_kind.value,
            upsert.listing_id,
            upsert.feed_id,
            upsert.external_id,
            insert_last_alert_at,
            insert_last_value,
            insert_pending,
        ),
    )


def _insert_outbox(conn: sqlite3.Connection, outbox: OutboxRow) -> int:
    """Insert the ``alerts_outbox`` row and return its new id (Req 14.8)."""
    cursor = conn.execute(
        "INSERT INTO alerts_outbox "
        "  (alert_kind, event_type, game_id, subject_kind, listing_id, feed_id, "
        "   external_id, payload, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            outbox.alert_kind,
            outbox.event_type,
            outbox.game_id,
            outbox.subject_kind,
            outbox.listing_id,
            outbox.feed_id,
            outbox.external_id,
            outbox.payload,
            datetime_to_text(outbox.created_at),
        ),
    )
    row_id = cursor.lastrowid
    if row_id is None:  # pragma: no cover - sqlite always sets lastrowid on INSERT
        raise RuntimeError("alerts_outbox INSERT did not yield a row id")
    return int(row_id)


def _insert_history(conn: sqlite3.Connection, history: HistoryRow) -> None:
    """Insert the append-only ``alert_history`` row (Req 14.11)."""
    conn.execute(
        "INSERT INTO alert_history "
        "  (subject_kind, listing_id, feed_id, external_id, event_type, "
        "   detected_at, detail) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            history.subject_kind,
            history.listing_id,
            history.feed_id,
            history.external_id,
            history.event_type,
            datetime_to_text(history.detected_at),
            history.detail,
        ),
    )


def _insert_delivery(
    conn: sqlite3.Connection, alert_id: int, target: DeliveryTarget
) -> None:
    """Insert one pending ``alert_deliveries`` row for a destination (Req 6.2, 10)."""
    conn.execute(
        "INSERT INTO alert_deliveries "
        "  (alert_id, destination_id, status, attempts, next_attempt_at) "
        "VALUES (?, ?, 'pending', 0, ?)",
        (alert_id, target.destination_id, datetime_to_text(target.next_attempt_at)),
    )


def write_transition(
    db: Database, conn: sqlite3.Connection, write_set: WriteSet
) -> None:
    """Persist a detected transition's full write-set in ONE transaction (Req 6.1-6.3).

    Must be called on the DB executor thread (``conn`` is the owned connection). Inside
    a single ``BEGIN IMMEDIATE`` (via :meth:`Database.transaction`) it writes, together:

    1. the stored-listing state delta (if any);
    2. every keyed ``alert_state`` upsert for the subject;
    3. the single ``alerts_outbox`` row for the emitted alert;
    4. the append-only ``alert_history`` row;
    5. one ``alert_deliveries`` row (``pending``) per matching destination.

    If any step raises, the context manager ROLLBACKs and NOTHING persists, so the event
    is re-detected on the next poll (Req 6.3).
    """
    with db.transaction(conn):
        if write_set.state is not None:
            _apply_state_delta(conn, write_set.state)
        for upsert in write_set.alert_state:
            _apply_alert_state_upsert(conn, upsert)
        alert_id = _insert_outbox(conn, write_set.outbox)
        _insert_history(conn, write_set.history)
        for target in write_set.deliveries:
            _insert_delivery(conn, alert_id, target)


async def apply_write_set(db: Database, write_set: WriteSet) -> None:
    """Async wrapper: run :func:`write_transition` on the DB executor (Req 6.1-6.3)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_transition(db, conn, write_set)

    await db.run(_run)


def write_state_only(
    db: Database, conn: sqlite3.Connection, write: StateOnlyWrite
) -> None:
    """Persist a no-alert state change in ONE transaction (Req 6.6 / 6.7).

    Must be called on the DB executor thread. Writes the stored-state delta and any
    ``alert_state`` change for the subject together — e.g. setting/clearing
    ``restock_pending`` (Req 6.6), or updating ``in_stock`` / ``existence`` /
    ``consecutive_404`` with no emitted alert (Req 6.7). No outbox / history / deliveries
    rows are written. A ``NOT_FOUND`` fetch leaves ``in_stock`` / ``price`` / the
    price_change ``alert_state`` untouched by omitting those fields (Req 7.4).
    """
    with db.transaction(conn):
        if write.state is not None:
            _apply_state_delta(conn, write.state)
        for upsert in write.alert_state:
            _apply_alert_state_upsert(conn, upsert)


async def apply_state_only(db: Database, write: StateOnlyWrite) -> None:
    """Async wrapper: run :func:`write_state_only` on the DB executor (Req 6.6 / 6.7)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_state_only(db, conn, write)

    await db.run(_run)


# ===========================================================================
# M2 task 19 — catalogue fetch-input reads (full adapter inputs per entity)
# ===========================================================================
#
# The watcher's per-retailer tick needs more than the lean DueFeed / DueListing
# snapshots above: it needs the full set of attributes the adapter's ``discover`` /
# ``fetch_state`` read, plus the per-entity poll interval / cooldown / seeded flag the
# scheduling and seed decisions turn on. These reader dataclasses carry exactly those
# fields and are deliberately PLAIN (non-frozen) slotted dataclasses so they satisfy the
# adapter's structural ``FeedLike`` / ``ListingLike`` protocols (which declare plain
# settable attributes a frozen dataclass's read-only attributes do not satisfy under
# ``mypy --strict``), mirroring the row objects in :mod:`restockradar.app`.


@dataclass(slots=True)
class RetailerFetchInputs:
    """A retailer's registry + adapter inputs for one tick (Req 11.3, 1.5).

    Carries ``adapter_type`` (the registry's ``RetailerLike``) and
    ``base_url`` / ``minor_unit_exponent`` (the Shopify adapter's ``RetailerLike``) plus
    the inherited default ``poll_interval_s``, so one row serves the registry, the
    adapter factory, and the per-entity interval fallback.
    """

    id: str
    base_url: str
    adapter_type: str
    minor_unit_exponent: int
    poll_interval_s: int


@dataclass(slots=True)
class FeedFetchInputs:
    """A due discovery feed with the full ``discover`` inputs + scheduling fields.

    Satisfies the adapter's ``FeedLike`` (``feed_url`` / ``max_page_count``) and carries
    the include/exclude keywords the feed diff filters on, the ``seeded`` flag gating the
    baseline write (Req 2.1), and ``poll_interval_s`` for the next-due computation.
    """

    id: int
    retailer_id: str
    game_id: str
    feed_url: str
    max_page_count: int
    include_keywords: tuple[str, ...]
    exclude_keywords: tuple[str, ...]
    poll_interval_s: int
    seeded: bool


@dataclass(slots=True)
class ListingFetchInputs:
    """A due listing with the full ``fetch_state`` inputs + scheduling fields.

    Satisfies the adapter's ``ListingLike`` (``url`` / ``handle`` / ``external_id``) and
    carries the game id (for destination fan-out), the ``seeded`` flag gating the baseline
    write (Req 2.1/2.4), ``poll_interval_s`` / ``cooldown_s`` for scheduling + the diff
    cooldown, and the stored existence/stock/price/404 snapshot. ``url`` / ``handle`` are
    mutable so the watcher can read the current values; the stored-state fields let the
    tick decide seed-vs-diff without a second read.
    """

    id: int
    retailer_id: str
    game_id: str
    url: str
    handle: str
    external_id: str | None
    poll_interval_s: int
    cooldown_s: int | None
    seeded: bool


def _keywords(raw: str | None) -> tuple[str, ...]:
    """Decode a feed's JSON-array keyword column into a tuple (``NULL``/empty => ())."""
    if not raw:
        return ()
    decoded = json.loads(raw)
    if not isinstance(decoded, list):
        raise ValueError(f"keyword column is not a JSON array: {raw!r}")
    return tuple(str(item) for item in decoded)


def read_retailer_inputs(
    conn: sqlite3.Connection, retailer_id: str
) -> RetailerFetchInputs | None:
    """Read one enabled retailer's adapter + scheduling inputs (synchronous; Req 11.3).

    Returns ``None`` when the retailer is absent or disabled, so the tick can skip it.
    """
    row = conn.execute(
        "SELECT id, base_url, adapter_type, minor_unit_exponent, poll_interval_s "
        "FROM retailers WHERE id = ? AND enabled = 1",
        (retailer_id,),
    ).fetchone()
    if row is None:
        return None
    return RetailerFetchInputs(
        id=str(row["id"]),
        base_url=str(row["base_url"]),
        adapter_type=str(row["adapter_type"]),
        minor_unit_exponent=int(row["minor_unit_exponent"]),
        poll_interval_s=int(row["poll_interval_s"]),
    )


def read_due_feed_inputs(
    conn: sqlite3.Connection, retailer_id: str, now: datetime
) -> list[FeedFetchInputs]:
    """Read a retailer's enabled DUE feeds with full ``discover`` inputs (Req 1.6).

    Ordering mirrors :func:`read_due_feeds`: effective due time ascending (a ``NULL``
    ``next_due_at`` is due now and sorts first), then by id for determinism.
    """
    now_text = datetime_to_text(now)
    rows = conn.execute(
        "SELECT id, retailer_id, game_id, feed_url, max_page_count, "
        "       include_keywords, exclude_keywords, poll_interval_s "
        "FROM discovery_feeds "
        "WHERE retailer_id = ? AND enabled = 1 "
        "  AND (next_due_at IS NULL OR next_due_at <= ?) "
        "ORDER BY next_due_at IS NOT NULL, next_due_at ASC, id ASC",
        (retailer_id, now_text),
    ).fetchall()
    return [
        FeedFetchInputs(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            feed_url=str(row["feed_url"]),
            max_page_count=int(row["max_page_count"]),
            include_keywords=_keywords(row["include_keywords"]),
            exclude_keywords=_keywords(row["exclude_keywords"]),
            poll_interval_s=int(row["poll_interval_s"]),
            seeded=_feed_seeded(conn, int(row["id"])),
        )
        for row in rows
    ]


def _feed_seeded(conn: sqlite3.Connection, feed_id: int) -> bool:
    """Whether a feed's ``seeded_at`` is set (its baseline has run, Req 2.1)."""
    row = conn.execute(
        "SELECT seeded_at FROM discovery_feeds WHERE id = ?", (feed_id,)
    ).fetchone()
    return row is not None and row["seeded_at"] is not None


def read_due_listing_inputs(
    conn: sqlite3.Connection, retailer_id: str, now: datetime
) -> list[ListingFetchInputs]:
    """Read a retailer's enabled DUE listings with full ``fetch_state`` inputs (Req 1.6).

    Ordering mirrors :func:`read_due_listings`: effective due time ascending (``NULL``
    first), then by id. ``seeded`` is derived from ``seeded_at`` so the tick can seed on
    the first complete poll and diff thereafter (Req 2.2/2.4).
    """
    now_text = datetime_to_text(now)
    rows = conn.execute(
        "SELECT id, retailer_id, url, handle, external_id, poll_interval_s, "
        "       cooldown_s, seeded_at, "
        "       (SELECT game_id FROM products WHERE products.id = listings.product_id) "
        "         AS game_id "
        "FROM listings "
        "WHERE retailer_id = ? AND enabled = 1 "
        "  AND (next_due_at IS NULL OR next_due_at <= ?) "
        "ORDER BY next_due_at IS NOT NULL, next_due_at ASC, id ASC",
        (retailer_id, now_text),
    ).fetchall()
    return [
        ListingFetchInputs(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            url=str(row["url"]),
            handle=str(row["handle"]),
            external_id=(
                None if row["external_id"] is None else str(row["external_id"])
            ),
            poll_interval_s=int(row["poll_interval_s"]),
            cooldown_s=None if row["cooldown_s"] is None else int(row["cooldown_s"]),
            seeded=row["seeded_at"] is not None,
        )
        for row in rows
    ]


# ===========================================================================
# M2 task 20 — ignore-due readers for the ``--once`` cycle (Req 20.4)
# ===========================================================================
#
# ``--once`` runs exactly ONE cycle treating every ENABLED feed and listing as due,
# IGNORING ``next_due_at`` and backoff for enabled entities (design "Operational modes:
# ``--once``", Req 20.4). These sibling readers are the due readers with the
# ``next_due_at`` predicate dropped; they keep the SAME column list, row shape, and
# deterministic id ordering so the one-shot path builds identical ``*FetchInputs`` and
# the scheduled due readers above are left completely unchanged.


def read_all_enabled_feed_inputs(
    conn: sqlite3.Connection, retailer_id: str
) -> list[FeedFetchInputs]:
    """Read ALL of a retailer's enabled feeds with full ``discover`` inputs (Req 20.4).

    The ``--once`` sibling of :func:`read_due_feed_inputs`: the ``next_due_at`` predicate
    is dropped so every enabled feed is returned regardless of its due time or backoff.
    Ordered by id for a deterministic one-shot cycle.
    """
    rows = conn.execute(
        "SELECT id, retailer_id, game_id, feed_url, max_page_count, "
        "       include_keywords, exclude_keywords, poll_interval_s "
        "FROM discovery_feeds "
        "WHERE retailer_id = ? AND enabled = 1 "
        "ORDER BY id ASC",
        (retailer_id,),
    ).fetchall()
    return [
        FeedFetchInputs(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            feed_url=str(row["feed_url"]),
            max_page_count=int(row["max_page_count"]),
            include_keywords=_keywords(row["include_keywords"]),
            exclude_keywords=_keywords(row["exclude_keywords"]),
            poll_interval_s=int(row["poll_interval_s"]),
            seeded=_feed_seeded(conn, int(row["id"])),
        )
        for row in rows
    ]


def read_all_enabled_listing_inputs(
    conn: sqlite3.Connection, retailer_id: str
) -> list[ListingFetchInputs]:
    """Read ALL of a retailer's enabled listings with full ``fetch_state`` inputs (Req 20.4).

    The ``--once`` sibling of :func:`read_due_listing_inputs`: the ``next_due_at``
    predicate is dropped so every enabled listing is returned regardless of its due time
    or backoff. Ordered by id for a deterministic one-shot cycle.
    """
    rows = conn.execute(
        "SELECT id, retailer_id, url, handle, external_id, poll_interval_s, "
        "       cooldown_s, seeded_at, "
        "       (SELECT game_id FROM products WHERE products.id = listings.product_id) "
        "         AS game_id "
        "FROM listings "
        "WHERE retailer_id = ? AND enabled = 1 "
        "ORDER BY id ASC",
        (retailer_id,),
    ).fetchall()
    return [
        ListingFetchInputs(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            url=str(row["url"]),
            handle=str(row["handle"]),
            external_id=(
                None if row["external_id"] is None else str(row["external_id"])
            ),
            poll_interval_s=int(row["poll_interval_s"]),
            cooldown_s=None if row["cooldown_s"] is None else int(row["cooldown_s"]),
            seeded=row["seeded_at"] is not None,
        )
        for row in rows
    ]


# ===========================================================================
# M2 task 19 — next_due_at setters (Req 1.7)
# ===========================================================================


def set_feed_next_due_at(
    conn: sqlite3.Connection, feed_id: int, next_due_at: datetime
) -> None:
    """Set a feed's ``next_due_at`` (synchronous; run inside the executor, Req 1.7)."""
    conn.execute(
        "UPDATE discovery_feeds SET next_due_at = ? WHERE id = ?",
        (datetime_to_text(next_due_at), feed_id),
    )


def set_listing_next_due_at(
    conn: sqlite3.Connection, listing_id: int, next_due_at: datetime
) -> None:
    """Set a listing's ``next_due_at`` (synchronous; run inside the executor, Req 1.7)."""
    conn.execute(
        "UPDATE listings SET next_due_at = ? WHERE id = ?",
        (datetime_to_text(next_due_at), listing_id),
    )


# ===========================================================================
# M2 task 19 — backoff_state upsert (Req 16)
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class BackoffStateUpsert:
    """The new ``backoff_state`` row to persist after one poll outcome (Req 16).

    Carries the scope key, the scope kind + id columns (so a freshly-inserted row is
    well-formed under the schema CHECK), the new ordinary / blocked counters, the new
    ``backoff_until`` (``None`` after a success, which clears the deferral), and the
    ``last_outcome`` token. ``empty_count`` is intentionally absent — the empty-poll
    track (Req 17) owns that column and is left untouched by a backoff write.
    """

    scope_key: ScopeKey
    scope_kind: str
    retailer_id: str | None = None
    feed_id: int | None = None
    listing_id: int | None = None
    error_count: int
    blocked_count: int
    backoff_until: datetime | None
    last_outcome: str


def write_backoff_state(
    conn: sqlite3.Connection, upsert: BackoffStateUpsert
) -> None:
    """Upsert a scope's ``backoff_state`` row (synchronous; run on the executor, Req 16).

    Keyed by the single NON-NULL ``scope_key`` PK. On conflict it overwrites the
    ordinary/blocked counters, ``backoff_until``, and ``last_outcome`` — but NOT
    ``empty_count`` (the empty-poll track owns that, Req 17). The scope-kind + id columns
    seed a freshly-inserted row and are not re-written on conflict (they never change for
    a given key).
    """
    conn.execute(
        "INSERT INTO backoff_state "
        "  (scope_key, scope_kind, retailer_id, feed_id, listing_id, "
        "   error_count, blocked_count, empty_count, backoff_until, last_outcome) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?) "
        "ON CONFLICT(scope_key) DO UPDATE SET "
        "  error_count = excluded.error_count, "
        "  blocked_count = excluded.blocked_count, "
        "  backoff_until = excluded.backoff_until, "
        "  last_outcome = excluded.last_outcome",
        (
            upsert.scope_key,
            upsert.scope_kind,
            upsert.retailer_id,
            upsert.feed_id,
            upsert.listing_id,
            upsert.error_count,
            upsert.blocked_count,
            None if upsert.backoff_until is None else datetime_to_text(upsert.backoff_until),
            upsert.last_outcome,
        ),
    )


# ===========================================================================
# M2 task 19 — admin_notices write (Req 17.6, 11.8 handle-change notice)
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class AdminNoticeRow:
    """One ``admin_notices`` row to queue for the hourly Admin_Digest (Req 17.6, 11.8).

    The watcher-level handle-rename path queues a ``kind='handle_change'`` notice keyed by
    the ``'retailer:<id>'`` scope. ``message`` is already redacted (secret-free, Req 18.4).
    ``digested_at`` is left ``NULL`` on insert — the M3 digest folds it in later.
    """

    scope_key: ScopeKey
    kind: str
    message: str
    created_at: datetime


def write_admin_notice(conn: sqlite3.Connection, notice: AdminNoticeRow) -> None:
    """Insert one pending ``admin_notices`` row (synchronous; run on the executor).

    A plain INSERT (no de-dupe key) — the digest coalesces per scope_key when it runs
    (Req 17.7). ``digested_at`` is NULL so the row is picked up by the next digest.
    """
    conn.execute(
        "INSERT INTO admin_notices (scope_key, kind, message, created_at, digested_at) "
        "VALUES (?, ?, ?, ?, NULL)",
        (notice.scope_key, notice.kind, notice.message, datetime_to_text(notice.created_at)),
    )


# ===========================================================================
# M2 task 19.2 — the watcher-level handle-rename WRITE path (Req 11.8, 7.7)
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class HandleRenameWrite:
    """The atomic write for a watcher-detected handle rename (Req 11.8, 7.7).

    Produced when ``adapter.fetch_state`` returned a :class:`ProductState` carrying
    ``resolved_handle`` / ``resolved_url`` (a rename). Persisted inside ONE
    ``BEGIN IMMEDIATE`` transaction by :func:`write_handle_rename`:

    * the listing's ``url`` + ``handle`` are updated to the resolved values;
    * stored existence is set EXISTING and ``consecutive_404`` reset to 0 — the rename is
      NOT a 404, so no existence flip and no Page_Live_Event (that is handled by the diff
      path, never here);
    * exactly ONE ``admin_notices`` row (``kind='handle_change'``, scope
      ``'retailer:<id>'``) is queued for the hourly Admin_Digest.

    No stock/price is written and no outbox product-alert row is created — a rename is a
    url/handle correction plus an operational notice, nothing more.
    """

    listing_id: int
    retailer_id: str
    resolved_url: str
    resolved_handle: str
    notice: AdminNoticeRow


def write_handle_rename(
    db: Database, conn: sqlite3.Connection, write: HandleRenameWrite
) -> None:
    """Persist a handle rename + its admin notice in ONE transaction (Req 11.8, 7.7).

    Must be called on the DB executor thread (``conn`` is the owned connection). Updates
    the listing's url/handle and sets existence EXISTING with ``consecutive_404 = 0``
    (never incrementing the 404 counter, never emitting a Page_Live_Event), and queues
    exactly one ``retailer:<id>`` handle-change admin notice — all inside one
    ``BEGIN IMMEDIATE`` so the url/handle change and the notice land together or not at
    all (Req 6.3).
    """
    with db.transaction(conn):
        conn.execute(
            "UPDATE listings "
            "SET url = ?, handle = ?, existence = 1, consecutive_404 = 0 "
            "WHERE id = ?",
            (write.resolved_url, write.resolved_handle, write.listing_id),
        )
        write_admin_notice(conn, write.notice)


async def apply_handle_rename(db: Database, write: HandleRenameWrite) -> None:
    """Async wrapper: run :func:`write_handle_rename` on the executor (Req 11.8, 7.7)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_handle_rename(db, conn, write)

    await db.run(_run)
