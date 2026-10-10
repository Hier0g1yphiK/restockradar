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
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from restockradar.domain.ids import EventType, SubjectKind, listing_subject_key
from restockradar.domain.models import Existence
from restockradar.persistence.db import Database
from restockradar.persistence.types import (
    bool_to_int,
    datetime_to_text,
    decimal_to_text,
)

__all__ = [
    "BaselineListing",
    "BaselineProduct",
    "seed_feed",
    "seed_listing",
    "write_feed_baseline",
    "write_listing_baseline",
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
