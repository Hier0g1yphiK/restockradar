"""Task 17 — fan-out tests for :mod:`restockradar.detect.fanout`.

The fan-out layer turns the PURE diff output (:class:`FeedDiffResult` /
:class:`ListingDiffResult`) into the typed write-set the repository persists atomically.
These tests cover both halves of the split:

* the **pure builders** (:func:`build_listing_write` / :func:`build_feed_write_sets`),
  asserted directly on the returned :class:`WriteSet` / :class:`StateOnlyWrite`; and
* the **DB-touching wiring** (:func:`write_feed_diff` / :func:`write_listing_diff`), driven
  against a REAL in-memory database migrated with the production
  :func:`~restockradar.persistence.migrate.migrate` runner.

Covered:

* 17.2 **Property 10** — fan-out writes EXACTLY ONE ``alert_deliveries`` row per matching
  enabled, healthy, non-admin destination for the alert's game and ZERO for the rest
  (disabled / unhealthy / admin / other-game). Validates Req 6.2, 8.1, 10.10. Driven with
  Hypothesis against a real migrated DB.
* Example tests: the combined restock + price alert is ONE outbox row
  (``event_type='combined'``) with one delivery set; separate single alerts fire when only
  one event occurred; a page-live alert is its own row; a new-product alert carries
  ``feed_id`` + ``external_id`` + ``game_id`` with ``subject_kind='feed_product'``; a
  no-alert state-only diff writes NO outbox/history/deliveries but still applies the
  state/alert_state change.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from restockradar.detect.diff_feed import FeedDiffResult
from restockradar.detect.diff_listing import ListingDiffResult
from restockradar.detect.events import (
    NewProductEvent,
    PageLiveEvent,
    PriceChangeEvent,
    RestockEvent,
)
from restockradar.detect.fanout import (
    COMBINED_EVENT_TYPE,
    build_feed_write_sets,
    build_listing_write,
    write_feed_diff,
    write_listing_diff,
)
from restockradar.domain.ids import EventType, SubjectKind, listing_subject_key
from restockradar.domain.models import Existence
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.repository import (
    AlertStateUpsert,
    StateDelta,
    StateOnlyWrite,
    WriteSet,
    read_healthy_destinations_for_game,
)

_GAME_ID = "pokemon"
_OTHER_GAME_ID = "mtg"
_RETAILER_ID = "northern-card-gaming"
_PRODUCT_ID = "ncg-pokemon-booster"
_LISTING_ID = 1
_FEED_ID = 7
_URL = "https://www.northerncardgaming.com/products/booster"
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# DB harness
# ---------------------------------------------------------------------------


def _migrated_db() -> Database:
    db = Database(":memory:")
    asyncio.run(migrate(db))
    return db


def _seed_catalogue(db: Database) -> None:
    """Insert the two games, the retailer, product and listing the writes reference."""

    def _run(conn: Any) -> None:
        conn.execute("INSERT INTO games (id, name) VALUES (?, ?)", (_GAME_ID, "Pokemon"))
        conn.execute(
            "INSERT INTO games (id, name) VALUES (?, ?)", (_OTHER_GAME_ID, "Magic")
        )
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
            (_LISTING_ID, "listing-config-1", _PRODUCT_ID, _RETAILER_ID, _URL, "booster"),
        )
        conn.execute(
            "INSERT INTO discovery_feeds "
            "  (id, config_key, retailer_id, game_id, feed_url) "
            "VALUES (?, ?, ?, ?, ?)",
            (_FEED_ID, "feed-config-1", _RETAILER_ID, _GAME_ID, "https://example.test/f"),
        )

    asyncio.run(db.run(_run))


def _run_on_db(db: Database, func: Any) -> Any:
    return asyncio.run(db.run(func))


def _count(db: Database, table: str) -> int:
    row = _run_on_db(
        db, lambda conn: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    )
    return int(row[0])


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def _restock_event(*, price: Decimal | None = Decimal("12.99")) -> RestockEvent:
    return RestockEvent(
        listing_id=_LISTING_ID,
        retailer_id=_RETAILER_ID,
        game_id=_GAME_ID,
        price=price,
        url=_URL,
        title="Booster Box",
        image=None,
        detected_at=_NOW,
    )


def _price_event(
    *, old: Decimal | None = Decimal("12.99"), new: Decimal = Decimal("9.99")
) -> PriceChangeEvent:
    return PriceChangeEvent(
        listing_id=_LISTING_ID,
        retailer_id=_RETAILER_ID,
        game_id=_GAME_ID,
        old_price=old,
        new_price=new,
        url=_URL,
        title="Booster Box",
        image=None,
        detected_at=_NOW,
    )


def _page_live_event() -> PageLiveEvent:
    return PageLiveEvent(
        listing_id=_LISTING_ID,
        retailer_id=_RETAILER_ID,
        game_id=_GAME_ID,
        in_stock=True,
        price=Decimal("12.99"),
        url=_URL,
        title="Booster Box",
        image=None,
        detected_at=_NOW,
    )


def _new_product_event() -> NewProductEvent:
    return NewProductEvent(
        retailer_id=_RETAILER_ID,
        game_id=_GAME_ID,
        feed_id=_FEED_ID,
        external_id="ext-999",
        title="A brand new box",
        handle="brand-new-box",
        url=_URL,
        image="https://example.test/img.png",
    )


def _state_delta() -> StateDelta:
    return StateDelta(
        listing_id=_LISTING_ID,
        in_stock=True,
        set_in_stock=True,
        price=Decimal("12.99"),
        set_price=True,
        existence=Existence.EXISTING,
        set_existence=True,
        consecutive_404=0,
    )


def _restock_upsert() -> AlertStateUpsert:
    return AlertStateUpsert(
        subject_key=listing_subject_key(_LISTING_ID),
        event_type=EventType.RESTOCK,
        subject_kind=SubjectKind.LISTING,
        listing_id=_LISTING_ID,
        last_alert_at=_NOW,
        set_last_alert_at=True,
        pending=False,
        set_pending=True,
    )


# ===========================================================================
# Property 10 — one delivery row per matching enabled, healthy destination
# ===========================================================================

# One toggle per destination the strategy may seed; the fan-out target is exactly the
# non-admin + enabled + healthy + game-G destinations.
_dest_strategy = st.fixed_dictionaries(
    {
        "enabled": st.booleans(),
        "healthy": st.booleans(),
        "admin": st.booleans(),
        "this_game": st.booleans(),
    }
)


def _insert_destination(conn: Any, dest_id: int, spec: dict[str, bool]) -> None:
    """Seed one destination row from a spec; admin rows may omit the game (CHECK)."""
    is_admin = spec["admin"]
    # A non-admin destination requires a game (schema CHECK). An admin destination may
    # still carry a game; drive game choice off the this_game toggle regardless.
    game_id: str | None
    if spec["this_game"]:
        game_id = _GAME_ID
    elif is_admin:
        game_id = None
    else:
        game_id = _OTHER_GAME_ID
    conn.execute(
        "INSERT INTO destinations "
        "  (id, config_key, server_id, game_id, webhook_env_var, is_admin, "
        "   health_status, enabled) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            dest_id,
            f"dest-{dest_id}",
            "server-1",
            game_id,
            f"RR_WEBHOOK_{dest_id}",
            1 if is_admin else 0,
            "healthy" if spec["healthy"] else "unhealthy",
            1 if spec["enabled"] else 0,
        ),
    )


def _is_target(spec: dict[str, bool]) -> bool:
    """Whether a destination spec is a fan-out target: non-admin, enabled, healthy, game G."""
    return (
        not spec["admin"]
        and spec["enabled"]
        and spec["healthy"]
        and spec["this_game"]
    )


@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(specs=st.lists(_dest_strategy, min_size=0, max_size=8))
def test_property_10_one_delivery_per_matching_destination(
    specs: list[dict[str, bool]],
) -> None:
    """Feature: stock-drop-monitor, Property 10: one delivery row per matching destination.

    Validates: Requirements 6.2, 8.1, 10.10

    For any mix of destinations (enabled/disabled x healthy/unhealthy x admin/non-admin x
    game G/other game), the fan-out of a product alert on game G writes EXACTLY ONE
    ``alert_deliveries`` row per non-admin + enabled + healthy + game-G destination and
    ZERO for every other destination — i.e. exactly the set
    :func:`read_healthy_destinations_for_game` returns.
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db)

        def _seed_destinations(conn: Any) -> None:
            for index, spec in enumerate(specs, start=1):
                _insert_destination(conn, index, spec)

        _run_on_db(db, _seed_destinations)

        expected_ids = sorted(
            index for index, spec in enumerate(specs, start=1) if _is_target(spec)
        )

        # The repository's resolver and the fan-out must agree on the target set.
        resolved = _run_on_db(
            db, lambda conn: read_healthy_destinations_for_game(conn, _GAME_ID)
        )
        assert sorted(resolved) == expected_ids

        diff = ListingDiffResult(
            events=(_restock_event(),),
            state=_state_delta(),
            alert_state=(_restock_upsert(),),
        )
        _run_on_db(
            db,
            lambda conn: write_listing_diff(
                db, conn, diff=diff, game_id=_GAME_ID, now=_NOW
            ),
        )

        rows = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT destination_id, status, next_attempt_at "
                "FROM alert_deliveries ORDER BY destination_id"
            ).fetchall(),
        )
        actual_ids = [int(r["destination_id"]) for r in rows]
        assert actual_ids == expected_ids
        # Exactly one row per target (no duplicates), all pending.
        assert len(actual_ids) == len(set(actual_ids))
        assert all(r["status"] == "pending" for r in rows)
        # One outbox + one history row for the single restock alert.
        assert _count(db, "alerts_outbox") == 1
        assert _count(db, "alert_history") == 1
    finally:
        db.close_sync()


# ===========================================================================
# Combined-alert rule (Req 4.10) — one outbox row, one delivery set
# ===========================================================================


def test_combined_restock_and_price_is_one_outbox_row() -> None:
    """A simultaneous restock + price change becomes ONE combined outbox + delivery set.

    Validates: Requirements 4.10
    """
    diff = ListingDiffResult(
        events=(_restock_event(), _price_event()),
        state=_state_delta(),
        alert_state=(_restock_upsert(),),
    )
    write = build_listing_write(
        diff=diff, destination_ids=[1, 2], game_id=_GAME_ID, now=_NOW
    )
    assert isinstance(write, WriteSet)
    assert write.outbox.event_type == COMBINED_EVENT_TYPE
    assert write.outbox.alert_kind == "product"
    assert write.outbox.subject_kind == SubjectKind.LISTING.value
    assert write.history.event_type == COMBINED_EVENT_TYPE
    # One delivery per destination, exactly one outbox alert.
    assert tuple(d.destination_id for d in write.deliveries) == (1, 2)
    # Both event prices ride the single combined payload.
    payload = json.loads(write.outbox.payload)
    assert payload["event"] == COMBINED_EVENT_TYPE
    assert payload["old_price"] == "12.99"
    assert payload["new_price"] == "9.99"
    # The diff's single StateDelta + alert_state ride along, not doubled.
    assert write.state is diff.state
    assert write.alert_state == diff.alert_state


def test_combined_alert_persists_single_outbox_with_full_delivery_set() -> None:
    """End-to-end: the combined alert writes one outbox row fanned to both destinations.

    Validates: Requirements 4.10, 6.2, 8.1
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db)

        def _seed_destinations(conn: Any) -> None:
            for index in (1, 2):
                _insert_destination(
                    conn,
                    index,
                    {"enabled": True, "healthy": True, "admin": False, "this_game": True},
                )

        _run_on_db(db, _seed_destinations)

        diff = ListingDiffResult(
            events=(_restock_event(), _price_event()),
            state=_state_delta(),
            alert_state=(_restock_upsert(),),
        )
        _run_on_db(
            db,
            lambda conn: write_listing_diff(
                db, conn, diff=diff, game_id=_GAME_ID, now=_NOW
            ),
        )

        assert _count(db, "alerts_outbox") == 1
        assert _count(db, "alert_history") == 1
        assert _count(db, "alert_deliveries") == 2
        event_type = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT event_type FROM alerts_outbox"
            ).fetchone()["event_type"],
        )
        assert event_type == COMBINED_EVENT_TYPE
    finally:
        db.close_sync()


# ===========================================================================
# Single-event alerts
# ===========================================================================


def test_lone_restock_is_single_restock_alert() -> None:
    """Only a restock fired => one ``restock`` outbox row, no combined.

    Validates: Requirements 4.1
    """
    diff = ListingDiffResult(
        events=(_restock_event(),), state=_state_delta(), alert_state=(_restock_upsert(),)
    )
    write = build_listing_write(
        diff=diff, destination_ids=[1], game_id=_GAME_ID, now=_NOW
    )
    assert isinstance(write, WriteSet)
    assert write.outbox.event_type == EventType.RESTOCK.value
    assert write.history.event_type == EventType.RESTOCK.value
    assert len(write.deliveries) == 1


def test_lone_price_change_is_single_price_alert() -> None:
    """Only a price change fired => one ``price_change`` outbox row.

    Validates: Requirements 5.1
    """
    diff = ListingDiffResult(
        events=(_price_event(),), state=_state_delta(), alert_state=()
    )
    write = build_listing_write(
        diff=diff, destination_ids=[1], game_id=_GAME_ID, now=_NOW
    )
    assert isinstance(write, WriteSet)
    assert write.outbox.event_type == EventType.PRICE_CHANGE.value
    payload = json.loads(write.outbox.payload)
    assert payload["new_price"] == "9.99"
    assert payload["old_price"] == "12.99"


def test_page_live_is_its_own_alert() -> None:
    """A page-live transition is its own ``page_live`` outbox row (Req 7).

    Validates: Requirements 7.3
    """
    diff = ListingDiffResult(
        events=(_page_live_event(),), state=_state_delta(), alert_state=()
    )
    write = build_listing_write(
        diff=diff, destination_ids=[1, 2], game_id=_GAME_ID, now=_NOW
    )
    assert isinstance(write, WriteSet)
    assert write.outbox.event_type == EventType.PAGE_LIVE.value
    assert write.outbox.subject_kind == SubjectKind.LISTING.value
    payload = json.loads(write.outbox.payload)
    assert payload["in_stock"] is True
    assert len(write.deliveries) == 2


# ===========================================================================
# New-product fan-out
# ===========================================================================


def test_new_product_alert_carries_feed_subject() -> None:
    """A new-product alert carries feed_id + external_id + game_id, subject_kind feed_product.

    Validates: Requirements 3.1, 8.1
    """
    diff = FeedDiffResult(events=(_new_product_event(),), to_record=())
    write_sets = build_feed_write_sets(diff=diff, destination_ids=[3], now=_NOW)
    assert len(write_sets) == 1
    outbox = write_sets[0].outbox
    assert outbox.event_type == EventType.NEW_PRODUCT.value
    assert outbox.subject_kind == SubjectKind.FEED_PRODUCT.value
    assert outbox.feed_id == _FEED_ID
    assert outbox.external_id == "ext-999"
    assert outbox.game_id == _GAME_ID
    assert outbox.listing_id is None
    # No listings StateDelta for a new product; recording seen_products is elsewhere.
    assert write_sets[0].state is None
    assert write_sets[0].alert_state == ()
    assert tuple(d.destination_id for d in write_sets[0].deliveries) == (3,)


def test_new_product_fanout_persists_outbox_and_deliveries() -> None:
    """End-to-end: a new-product alert writes outbox/history/deliveries, no listing state.

    Validates: Requirements 3.1, 6.2, 8.1
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db)

        def _seed_destinations(conn: Any) -> None:
            _insert_destination(
                conn,
                1,
                {"enabled": True, "healthy": True, "admin": False, "this_game": True},
            )

        _run_on_db(db, _seed_destinations)

        diff = FeedDiffResult(events=(_new_product_event(),), to_record=())
        _run_on_db(
            db,
            lambda conn: write_feed_diff(
                db, conn, diff=diff, game_id=_GAME_ID, now=_NOW
            ),
        )

        assert _count(db, "alerts_outbox") == 1
        assert _count(db, "alert_history") == 1
        assert _count(db, "alert_deliveries") == 1
        row = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT event_type, feed_id, external_id, game_id, listing_id "
                "FROM alerts_outbox"
            ).fetchone(),
        )
        assert row["event_type"] == EventType.NEW_PRODUCT.value
        assert int(row["feed_id"]) == _FEED_ID
        assert row["external_id"] == "ext-999"
        assert row["game_id"] == _GAME_ID
        assert row["listing_id"] is None
    finally:
        db.close_sync()


def test_feed_baseline_cycle_emits_nothing() -> None:
    """A feed diff with no new-product events produces no write-sets (baseline cycle)."""
    diff = FeedDiffResult(events=(), to_record=())
    assert build_feed_write_sets(diff=diff, destination_ids=[1], now=_NOW) == ()


# ===========================================================================
# State-only (no alert) diffs
# ===========================================================================


def test_no_alert_diff_is_state_only() -> None:
    """A diff that fired no alert but changed state yields a StateOnlyWrite (Req 6.6/6.7).

    Validates: Requirements 6.6, 6.7
    """
    pending_upsert = AlertStateUpsert(
        subject_key=listing_subject_key(_LISTING_ID),
        event_type=EventType.RESTOCK,
        subject_kind=SubjectKind.LISTING,
        listing_id=_LISTING_ID,
        pending=True,
        set_pending=True,
    )
    delta = StateDelta(
        listing_id=_LISTING_ID,
        existence=Existence.NOT_FOUND,
        set_existence=True,
        consecutive_404=2,
    )
    diff = ListingDiffResult(events=(), state=delta, alert_state=(pending_upsert,))
    write = build_listing_write(
        diff=diff, destination_ids=[1, 2], game_id=_GAME_ID, now=_NOW
    )
    assert isinstance(write, StateOnlyWrite)
    assert write.state is delta
    assert write.alert_state == (pending_upsert,)


def test_state_only_diff_writes_no_outbox_but_applies_state() -> None:
    """End-to-end: a no-alert diff writes no outbox/history/deliveries but applies state.

    Validates: Requirements 6.6, 6.7
    """
    db = _migrated_db()
    try:
        _seed_catalogue(db)

        def _seed_destinations(conn: Any) -> None:
            _insert_destination(
                conn,
                1,
                {"enabled": True, "healthy": True, "admin": False, "this_game": True},
            )

        _run_on_db(db, _seed_destinations)

        delta = StateDelta(listing_id=_LISTING_ID, consecutive_404=1)
        diff = ListingDiffResult(events=(), state=delta, alert_state=())
        _run_on_db(
            db,
            lambda conn: write_listing_diff(
                db, conn, diff=diff, game_id=_GAME_ID, now=_NOW
            ),
        )

        # No alert pipeline rows written.
        assert _count(db, "alerts_outbox") == 0
        assert _count(db, "alert_history") == 0
        assert _count(db, "alert_deliveries") == 0
        # ...but the stored-state change landed.
        counter = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT consecutive_404 FROM listings WHERE id = ?", (_LISTING_ID,)
            ).fetchone()["consecutive_404"],
        )
        assert int(counter) == 1
    finally:
        db.close_sync()
