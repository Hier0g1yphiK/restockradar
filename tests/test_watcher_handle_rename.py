"""Task 19.4 — the watcher-level handle-rename WRITE path (Req 11.8, 7.7).

When ``adapter.fetch_state`` reports a RENAME — a :class:`ProductState` carrying
``resolved_handle`` / ``resolved_url`` that differ from the stored handle — the watcher,
inside its OWN ``BEGIN IMMEDIATE`` transaction, updates the listing's ``url`` + ``handle``,
keeps the listing EXISTING with ``consecutive_404`` NOT incremented, emits NO
Page_Live_Event / ``page_live`` outbox row, and queues exactly ONE ``retailer:<id>``
handle-change admin notice. The EQUAL-handle case (no ``resolved_*`` field set) performs
NO url/handle change and queues NO notice.

These tests drive a real migrated in-memory
:class:`~restockradar.persistence.db.Database` (mirroring ``tests/test_detect_fanout.py``'s
harness) through the actual :meth:`restockradar.watcher.core.WatcherCore.tick_retailer`
with a FAKE offline adapter — so the whole fetch -> transaction -> write path is exercised,
not just the repository helper. The suite-wide no-network guard guarantees nothing touches
the live network.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from restockradar.adapters.base import (
    DiscoveredProduct,
    Existence,
    ListingLike,
    ProductState,
    VariantState,
)
from restockradar.adapters.registry import AdapterRegistry
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.watcher.core import WatcherCore

_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_PRODUCT_ID = "ncg-pokemon-booster"
_LISTING_ID = 1
_OLD_HANDLE = "old-booster"
_OLD_URL = "https://example.test/products/old-booster"
_NEW_HANDLE = "new-booster"
_NEW_URL = "https://example.test/products/new-booster"
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# DB harness
# ---------------------------------------------------------------------------


def _migrated_db() -> Database:
    db = Database(":memory:")
    asyncio.run(migrate(db))
    return db


def _run_on_db(db: Database, func: Any) -> Any:
    return asyncio.run(db.run(func))


def _seed_catalogue(db: Database, *, seeded: bool) -> None:
    """Insert the game/retailer/product and ONE already-seeded listing (so a diff runs).

    The listing is seeded (``seeded_at`` set, ``existence=1``) with a stored stock/price so
    the handle-rename path is reached via the normal fetch->process flow rather than the
    first-poll baseline branch.
    """

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
            "  (id, config_key, product_id, retailer_id, url, handle, external_id, "
            "   in_stock, price, existence, consecutive_404, seeded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1, '12.99', 1, 0, ?)",
            (
                _LISTING_ID,
                "listing-config-1",
                _PRODUCT_ID,
                _RETAILER_ID,
                _OLD_URL,
                _OLD_HANDLE,
                "ext-1",
                _NOW.isoformat() if seeded else None,
            ),
        )

    _run_on_db(db, _run)


def _count(db: Database, table: str, where: str = "") -> int:
    clause = f" WHERE {where}" if where else ""
    row = _run_on_db(
        db, lambda conn: conn.execute(f"SELECT COUNT(*) FROM {table}{clause}").fetchone()
    )
    return int(row[0])


def _listing_row(db: Database) -> Any:
    return _run_on_db(
        db,
        lambda conn: conn.execute(
            "SELECT url, handle, existence, consecutive_404 FROM listings WHERE id = ?",
            (_LISTING_ID,),
        ).fetchone(),
    )


# ---------------------------------------------------------------------------
# Fake offline adapter
# ---------------------------------------------------------------------------


class _FakeAdapter:
    """A canned, offline adapter returning a fixed :class:`ProductState` on fetch.

    ``resolved_handle`` / ``resolved_url`` are set to drive the RENAME path, or left
    ``None`` for the equal-handle case. ``discover`` is unused here (no feeds seeded).
    """

    adapter_type = "shopify"

    def __init__(self, state: ProductState) -> None:
        self._state = state

    async def fetch_state(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        return self._state

    async def discover(self, feed: Any) -> list[DiscoveredProduct]:  # pragma: no cover
        return []


def _product_state(
    *, resolved_handle: str | None, resolved_url: str | None
) -> ProductState:
    return ProductState(
        existence=Existence.EXISTING,
        in_stock=True,
        price=Decimal("12.99"),
        variants=(VariantState(variant_id="v1", available=True, price=Decimal("12.99")),),
        url=_OLD_URL,
        image=None,
        title="Booster Box",
        external_id="ext-1",
        resolved_handle=resolved_handle,
        resolved_url=resolved_url,
    )


def _watcher(db: Database, state: ProductState) -> WatcherCore:
    registry = AdapterRegistry()
    adapter = _FakeAdapter(state)
    registry.register("shopify", lambda _retailer: adapter)
    return WatcherCore(
        db=db,
        registry=registry,
        heartbeat=HeartbeatRegistry(),
        clock=lambda: _NOW,
        jitter=lambda: 0.0,
    )


# ===========================================================================
# 19.4 — handle rename write path (EXTRA user requirement c)
# ===========================================================================


def test_handle_rename_updates_url_handle_and_queues_one_admin_notice() -> None:
    """A rename updates url+handle, keeps EXISTING, no 404 bump, no page_live, 1 notice.

    Validates: Requirements 11.8, 7.7

    This is the EXTRA USER REQUIREMENT (c): the watcher-level handle-rename WRITE path.
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db, seeded=True)
        state = _product_state(resolved_handle=_NEW_HANDLE, resolved_url=_NEW_URL)
        watcher = _watcher(db, state)

        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))
        assert result.listings_polled == 1
        assert result.failures == 0

        row = _listing_row(db)
        # url + handle updated to the resolved values.
        assert row["url"] == _NEW_URL
        assert row["handle"] == _NEW_HANDLE
        # existence stays EXISTING (1); consecutive_404 NOT incremented.
        assert int(row["existence"]) == 1
        assert int(row["consecutive_404"]) == 0

        # NO Page_Live_Event / page_live outbox row (no product alert at all for a rename).
        assert _count(db, "alerts_outbox") == 0
        assert _count(db, "alert_history") == 0
        assert _count(db, "alert_deliveries") == 0

        # EXACTLY ONE retailer:<id> handle-change admin notice queued.
        assert _count(db, "admin_notices") == 1
        assert (
            _count(
                db,
                "admin_notices",
                f"scope_key = 'retailer:{_RETAILER_ID}' AND kind = 'handle_change'",
            )
            == 1
        )
    finally:
        db.close_sync()


def test_equal_handle_makes_no_url_change_and_no_notice() -> None:
    """The EQUAL-handle case (no resolved_* set) changes no url/handle and queues no notice.

    Validates: Requirements 11.8, 7.7
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db, seeded=True)
        # No resolved_* => ordinary feed-reuse / equal-handle: the diff path runs, not the
        # rename path. The fetched state matches the stored state, so no alert fires either.
        state = _product_state(resolved_handle=None, resolved_url=None)
        watcher = _watcher(db, state)

        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))
        assert result.listings_polled == 1

        row = _listing_row(db)
        # url + handle unchanged.
        assert row["url"] == _OLD_URL
        assert row["handle"] == _OLD_HANDLE

        # NO handle-change admin notice queued.
        assert _count(db, "admin_notices") == 0
    finally:
        db.close_sync()


def test_rename_write_path_is_atomic_url_and_notice_together() -> None:
    """The repository helper writes the url/handle change and the notice in one txn.

    Validates: Requirements 11.8, 6.3

    Drives the write helper directly to pin the atomicity contract independent of the tick.
    """
    from restockradar.persistence.repository import (
        AdminNoticeRow,
        HandleRenameWrite,
        apply_handle_rename,
    )

    db = _migrated_db()
    try:
        _seed_catalogue(db, seeded=True)
        write = HandleRenameWrite(
            listing_id=_LISTING_ID,
            retailer_id=_RETAILER_ID,
            resolved_url=_NEW_URL,
            resolved_handle=_NEW_HANDLE,
            notice=AdminNoticeRow(
                scope_key=f"retailer:{_RETAILER_ID}",
                kind="handle_change",
                message="listing 1 handle changed",
                created_at=_NOW,
            ),
        )
        asyncio.run(apply_handle_rename(db, write))

        row = _listing_row(db)
        assert row["url"] == _NEW_URL
        assert row["handle"] == _NEW_HANDLE
        assert int(row["consecutive_404"]) == 0
        assert _count(db, "admin_notices") == 1
    finally:
        db.close_sync()
