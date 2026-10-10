"""Task 34.2 — baseline write-path tests (Req 21.24, 2.1, 2.2, 2.3).

The baseline write path (``persistence/repository.py``) is what the M1 dry-run
``--once`` calls so ``Baseline_Seeding`` actually persists: the first complete poll of
a subject records its *last-observed* state and marks it seeded, WITHOUT emitting any
alert. These tests pin that contract from two angles:

* **Property 4** — for an arbitrary first-observation listing state (random
  ``in_stock`` / ``price`` / ``existence``), the baseline write stores the observed
  state, seeds BOTH ``alert_state`` rows (restock ``pending = 0`` / ``last_alert_at``
  NULL; price_change ``last_alerted_value`` == the fetched price), stamps ``seeded_at``,
  and leaves ``alerts_outbox`` and ``alert_deliveries`` EMPTY.
* **Feed baseline example** — load the captured NCG collection fixture from disk, parse
  its products through the real Shopify discovery parser, run the feed baseline, and
  assert a ``seen_products`` row was written for every discovered product and the feed's
  ``seeded_at`` was set, with zero alerts. No network is touched (the suite-wide guard
  in ``conftest.py`` would fail any live request).

Each test migrates a fresh in-memory :class:`~restockradar.persistence.db.Database` with
the real :func:`~restockradar.persistence.migrate.migrate` runner, so the writes exercise
the actual schema (FKs, CHECKs, the single-column ``alert_state`` key) rather than a
hand-rolled table.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.shopify import ShopifyAdapter
from restockradar.domain.models import Existence
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.repository import (
    BaselineListing,
    BaselineProduct,
    seed_feed,
    seed_listing,
)
from restockradar.persistence.types import text_to_decimal

# ---------------------------------------------------------------------------
# Fixtures and catalogue seeding helpers
# ---------------------------------------------------------------------------

#: Repo root is three parents up from ``tests/test_baseline_write.py``.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE = _REPO_ROOT / "fixtures" / "ncg" / "collections_pokemon_products_page1.json"

_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_PRODUCT_ID = "ncg-pokemon-booster"
_LISTING_ID = 1
_FEED_ID = 1

_SEEDED_AT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _migrated_db() -> Database:
    """Return a migrated, fresh in-memory database (schema from the real migration)."""
    db = Database()
    asyncio.run(migrate(db))
    return db


def _seed_catalogue_rows(db: Database) -> None:
    """Insert the game / retailer / product / listing / feed rows the baseline references.

    The baseline write only UPSERTs ``alert_state`` and UPDATEs ``listings`` /
    ``discovery_feeds`` / inserts ``seen_products``; the referenced catalogue rows must
    already exist for the foreign keys (and the ``listings`` UPDATE) to resolve.
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
            "  (id, config_key, product_id, retailer_id, url, handle) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                _LISTING_ID,
                "listing-config-1",
                _PRODUCT_ID,
                _RETAILER_ID,
                "https://example.test/products/a-pokemon-product",
                "a-pokemon-product",
            ),
        )
        conn.execute(
            "INSERT INTO discovery_feeds "
            "  (id, config_key, retailer_id, game_id, feed_url) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                _FEED_ID,
                "feed-config-1",
                _RETAILER_ID,
                _GAME_ID,
                "/collections/pokemon/products.json",
            ),
        )

    asyncio.run(db.run(_run))


def _fetchone(db: Database, sql: str, params: tuple[Any, ...] = ()) -> Any:
    def _run(conn: Any) -> Any:
        return conn.execute(sql, params).fetchone()

    return asyncio.run(db.run(_run))


def _count(db: Database, table: str) -> int:
    row = _fetchone(db, f"SELECT COUNT(*) FROM {table}")  # table names are fixed literals
    return int(row[0])


# ---------------------------------------------------------------------------
# Property 4 — baseline seeding never alerts and seeds both alert_state rows
# ---------------------------------------------------------------------------

# A price strategy covering "no priced variant" (None) and a wide range of exact,
# two-decimal-place prices so the stored canonical Decimal is exercised across values.
_price_strategy = st.one_of(
    st.none(),
    st.decimals(
        min_value=Decimal("0"),
        max_value=Decimal("100000"),
        allow_nan=False,
        allow_infinity=False,
        places=2,
    ),
)


@settings(max_examples=150)
@given(
    in_stock=st.booleans(),
    price=_price_strategy,
    existence=st.sampled_from(list(Existence)),
)
def test_property_4_baseline_never_alerts_and_seeds_both_alert_state_rows(
    in_stock: bool, price: Decimal | None, existence: Existence
) -> None:
    """Feature: stock-drop-monitor, Property 4: Baseline seeding never alerts and seeds both alert_state rows.

    Validates: Requirements 2.1, 2.2, 2.3

    For an arbitrary first-observation listing state, the baseline write persists the
    last-observed stored state + BOTH alert_state rows (restock pending=0 /
    last_alert_at NULL; price_change last_alerted_value == the fetched price) + a set
    ``seeded_at``, and leaves alerts_outbox and alert_deliveries EMPTY.
    """  # noqa: E501 - the Property-4 tag on the first docstring line is a required verbatim tag
    db = _migrated_db()
    try:
        _seed_catalogue_rows(db)

        asyncio.run(
            seed_listing(
                db,
                BaselineListing(
                    listing_id=_LISTING_ID,
                    in_stock=in_stock,
                    price=price,
                    existence=existence,
                    seeded_at=_SEEDED_AT,
                ),
            )
        )

        # (1) Stored last-observed state + seeded_at (Req 2.1, 2.3).
        listing_row = _fetchone(
            db,
            "SELECT in_stock, price, existence, seeded_at FROM listings WHERE id = ?",
            (_LISTING_ID,),
        )
        assert listing_row["in_stock"] == (1 if in_stock else 0)
        if price is None:
            assert listing_row["price"] is None
        else:
            assert text_to_decimal(listing_row["price"]) == price
        assert listing_row["existence"] == (1 if existence is Existence.EXISTING else 0)
        assert listing_row["seeded_at"] == _SEEDED_AT.isoformat()

        # (2) BOTH alert_state rows seeded in the same transaction (Req 2.2).
        subject_key = f"listing:{_LISTING_ID}"
        restock = _fetchone(
            db,
            "SELECT last_alert_at, last_alerted_value, pending "
            "FROM alert_state WHERE subject_key = ? AND event_type = 'restock'",
            (subject_key,),
        )
        assert restock is not None
        assert restock["last_alert_at"] is None  # last_restock_alert_at NULL
        assert restock["pending"] == 0  # restock_pending unset

        price_change = _fetchone(
            db,
            "SELECT last_alerted_value, pending "
            "FROM alert_state WHERE subject_key = ? AND event_type = 'price_change'",
            (subject_key,),
        )
        assert price_change is not None
        assert price_change["pending"] == 0
        if price is None:
            assert price_change["last_alerted_value"] is None
        else:
            # last_alerted_value == the fetched price (as canonical Decimal TEXT).
            assert text_to_decimal(price_change["last_alerted_value"]) == price

        # Exactly the two cooldown-bearing rows, nothing more.
        assert _count(db, "alert_state") == 2

        # (3) The baseline emits NO alerts (Req 2.1).
        assert _count(db, "alerts_outbox") == 0
        assert _count(db, "alert_deliveries") == 0
    finally:
        db.close_sync()


# ---------------------------------------------------------------------------
# Feed baseline example — fixture-driven, offline
# ---------------------------------------------------------------------------


class _OfflineAdapterRetailer:
    """Minimal retailer carrying the two attributes the Shopify parser reads."""

    base_url = "https://example.test"
    minor_unit_exponent = 2


def _parse_fixture_products() -> list[BaselineProduct]:
    """Parse the captured collection fixture into baseline products via the real parser.

    Uses :class:`ShopifyAdapter`'s own ``products[]`` extraction / ``DiscoveredProduct``
    builder so the test reflects exactly what discovery would record, then projects each
    discovered product onto the ``seen_products`` columns the baseline writes.
    """
    body = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    adapter = ShopifyAdapter.__new__(ShopifyAdapter)
    # Only the base-url normalisation and the pure parsers are exercised; no HTTP client
    # is needed, so the slots are set directly for an offline parse.
    adapter._http = None  # type: ignore[assignment]  # unused by the pure parser
    adapter._retailer = _OfflineAdapterRetailer()
    adapter._base = _OfflineAdapterRetailer.base_url
    raw_products = adapter._products_array(body, str(_FIXTURE))
    discovered = [adapter._to_discovered(raw) for raw in raw_products]
    return [
        BaselineProduct(external_id=d.external_id, title=d.title, handle=d.handle)
        for d in discovered
    ]


def test_feed_baseline_records_discovered_products_and_seeds_feed() -> None:
    """A feed baseline over the captured fixture writes seen_products and sets seeded_at.

    Validates: Requirements 21.24, 2.1, 2.3, 3.5

    Loads the real NCG collection fixture from disk, parses its products with the real
    Shopify parser, runs the feed baseline, and asserts one ``seen_products`` row per
    discovered product (regardless of keyword match) plus a set feed ``seeded_at`` — and
    zero alerts. No network is used.
    """
    products = _parse_fixture_products()
    assert products, "fixture should contain discovered products"
    external_ids = {p.external_id for p in products}

    db = _migrated_db()
    try:
        _seed_catalogue_rows(db)

        asyncio.run(
            seed_feed(
                db,
                feed_id=_FEED_ID,
                retailer_id=_RETAILER_ID,
                game_id=_GAME_ID,
                products=products,
                seeded_at=_SEEDED_AT,
            )
        )

        # One seen_products row per DISTINCT discovered product (Req 3.5).
        assert _count(db, "seen_products") == len(external_ids)
        for external_id in external_ids:
            row = _fetchone(
                db,
                "SELECT first_seen_at FROM seen_products "
                "WHERE retailer_id = ? AND game_id = ? AND external_id = ?",
                (_RETAILER_ID, _GAME_ID, external_id),
            )
            assert row is not None
            assert row["first_seen_at"] == _SEEDED_AT.isoformat()

        # The feed is marked seeded (Req 2.1).
        feed_row = _fetchone(
            db, "SELECT seeded_at FROM discovery_feeds WHERE id = ?", (_FEED_ID,)
        )
        assert feed_row["seeded_at"] == _SEEDED_AT.isoformat()

        # No alerts emitted on a baseline run.
        assert _count(db, "alerts_outbox") == 0
        assert _count(db, "alert_deliveries") == 0
    finally:
        db.close_sync()
