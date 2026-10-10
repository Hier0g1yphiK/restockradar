"""Task 15 — new-product detection tests for :mod:`restockradar.detect.diff_feed`.

Covers the three required properties and the fixture-driven scenarios for
:func:`restockradar.detect.diff_feed.diff_feed`:

* 15.2 **Property 1** — a genuinely-new product (external_id not seen, passes the keyword
  filter, not covered by a tracked listing) yields EXACTLY ONE new-product event.
  Validates: Requirements 3.1, 3.5, 3.6.
* 15.3 **Property 2** — feed processing is order-independent: shuffling ``discovered``
  yields the same event set and the same recorded set. Validates: Requirements 3.2, 12.7.
* 15.4 **Property 23** — a product covered by a tracked listing (match by external_id OR
  handle) is recorded in ``to_record`` but emits NO new-product event.
  Validates: Requirements 3.1, 3.5, 3.6.
* 15.5 **Fixture tests** against the REAL captured NCG fixtures under ``fixtures/ncg/``
  (parsed into :class:`~restockradar.adapters.base.DiscoveredProduct` via the real
  :class:`~restockradar.adapters.shopify.ShopifyAdapter`, mirroring
  ``tests/test_shopify_adapter_fixtures.py``):
    - a genuinely-new product in a fixture feed yields exactly one new-product alert;
    - two feeds for DIFFERENT games on the SAME retailer each alert for the same
      external_id (tracked per (retailer, game, external_id));
    - **EXTRA REQUIRED (user requirement (a))** — a product present in BOTH feeds yields
      EXACTLY ONE new-product event: new for the game where its
      (retailer, game, external_id) is unseen, and none for the game where it is already
      seen.

Property inputs are generated with Hypothesis (as the project does in
``tests/test_repository_writeset.py`` and ``tests/test_shopify_adapter_fixtures.py``). The
suite-wide offline guard in ``conftest.py`` (Req 21.4) means fixture parsing never touches
the network — every byte is served from the saved fixtures through an
:class:`httpx.MockTransport`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.base import DiscoveredProduct
from restockradar.adapters.shopify import ShopifyAdapter
from restockradar.detect.diff_feed import FeedDiffResult, diff_feed
from restockradar.detect.events import NewProductEvent
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry

# ---------------------------------------------------------------------------
# Real-fixture plumbing (mirrors tests/test_shopify_adapter_fixtures.py)
# ---------------------------------------------------------------------------

BASE_URL = "https://www.northerncardgaming.com"
ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"
_FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "ncg"
_RETAILER_ID = "northern-card-gaming"

#: The single product id that appears in BOTH the pokemon collection and the pre-orders
#: collection fixtures (verified against the captured data) — the "present in both feeds"
#: anchor for the user-mandated test.
_OVERLAP_ID = "15160546296187"
_POKEWEEN_ID = "15440414114171"

Handler = Callable[[httpx.Request], httpx.Response]


def _fixture_bytes(name: str) -> bytes:
    return (_FIXTURE_DIR / name).read_bytes()


def _json_response(payload: object, *, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )


def _fixture_response(name: str) -> httpx.Response:
    return httpx.Response(
        200, content=_fixture_bytes(name), headers={"content-type": "application/json"}
    )


@dataclass
class FakeFeed:
    feed_url: str
    max_page_count: int = 20


@dataclass
class FakeRetailer:
    base_url: str = BASE_URL
    minor_unit_exponent: int = 2


def _robots_handler(inner: Handler) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        return inner(request)

    return handler


async def _discover_fixture(feed_url: str, page1_fixture: str) -> list[DiscoveredProduct]:
    """Parse a saved collection fixture into real ``DiscoveredProduct`` rows.

    Serves the saved fixture as page 1 and an empty page 2, so the adapter's real
    discovery/parsing path produces the products the diff then consumes.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if int(request.url.params["page"]) == 1:
            return _fixture_response(page1_fixture)
        return _json_response({"products": []})

    transport = httpx.MockTransport(_robots_handler(handler))
    client = HttpClient(
        heartbeat=HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=transport,
    )
    adapter = ShopifyAdapter(client, FakeRetailer())
    try:
        return await adapter.discover(FakeFeed(feed_url))
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Hypothesis strategies for the pure properties
# ---------------------------------------------------------------------------

_ids = st.text(alphabet="0123456789", min_size=1, max_size=8)
_words = st.text(alphabet="abcdefghijklmnopqrstuvwxyz ", min_size=0, max_size=12)
_handles = st.text(alphabet="abcdefghijklmnopqrstuvwxyz-", min_size=1, max_size=12)


@st.composite
def _products(draw: st.DrawFn, *, external_id: str | None = None) -> DiscoveredProduct:
    """A ``DiscoveredProduct`` with freely-generated keyword surfaces."""
    ext = external_id if external_id is not None else draw(_ids)
    return DiscoveredProduct(
        external_id=ext,
        title=draw(_words),
        product_type=draw(st.none() | _words),
        vendor=draw(st.none() | _words),
        tags=tuple(draw(st.lists(_words, max_size=3))),
        handle=draw(_handles),
        url=f"{BASE_URL}/products/{draw(_handles)}",
        image=draw(st.none() | st.just(f"{BASE_URL}/img.png")),
        variants=None,
    )


# ===========================================================================
# 15.2 — Property 1: a new product yields exactly one new-product event
# ===========================================================================


@settings(max_examples=200)
@given(data=st.data())
def test_property_1_new_product_yields_exactly_one_event(data: st.DataObject) -> None:
    """Feature: stock-drop-monitor, Property 1: A new product yields exactly one new-product event.

    Validates: Requirements 3.1, 3.5, 3.6

    *For any* batch of discovered products plus one distinguished product whose
    external_id is NOT in the seen set, that passes the (match-all) keyword filter, and
    that is NOT covered by a tracked listing, :func:`diff_feed` emits EXACTLY ONE
    new-product event for that product — and records it (and every other product with a
    usable external_id) in ``to_record`` (Req 3.5).
    """
    others = data.draw(st.lists(_products(), max_size=6))
    used_ids = {p.external_id for p in others}
    fresh_id = data.draw(_ids.filter(lambda x: x not in used_ids))
    new_product = data.draw(_products(external_id=fresh_id))
    discovered = [*others, new_product]

    # Seen set contains everything EXCEPT the distinguished new product, so only it is new.
    seen = used_ids - {fresh_id}

    result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="pokemon",
        feed_id=1,
        discovered=discovered,
        seen_external_ids=seen,
        include_keywords=(),  # empty => match-all (Req 3.3)
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )

    matching = [e for e in result.events if e.external_id == fresh_id]
    assert len(matching) == 1  # exactly one new-product event (Req 3.1)
    assert matching[0].event_type.value == "new_product"
    # Every usable product is recorded regardless of alert decision (Req 3.5).
    assert fresh_id in {b.external_id for b in result.to_record}


# ===========================================================================
# 15.3 — Property 2: feed processing is order-independent
# ===========================================================================


@settings(max_examples=200)
@given(data=st.data())
def test_property_2_feed_processing_is_order_independent(data: st.DataObject) -> None:
    """Feature: stock-drop-monitor, Property 2: Feed processing is order-independent.

    Validates: Requirements 3.2, 12.7

    *For any* batch of discovered products and any permutation of it, :func:`diff_feed`
    yields the SAME set of new-product events and the SAME recorded set — the diff makes
    each decision from the product plus the frozen seen/tracked sets, never from position.
    """
    # Distinct external_ids within the batch: a real feed never returns the same product
    # id twice carrying different payloads, and order-independence of the recorded
    # CONTENT is only well-defined when each (retailer, game, external_id) identity maps
    # to a single product (Req 3.2 dedup identity).
    products = data.draw(
        st.lists(_products(), min_size=1, max_size=8, unique_by=lambda p: p.external_id)
    )
    all_ids = [p.external_id for p in products]
    seen = set(data.draw(st.lists(st.sampled_from(all_ids), max_size=4))) if all_ids else set()

    shuffled = data.draw(st.permutations(products))

    def run(batch: list[DiscoveredProduct]) -> FeedDiffResult:
        return diff_feed(
            retailer_id=_RETAILER_ID,
            game_id="pokemon",
            feed_id=1,
            discovered=batch,
            seen_external_ids=seen,
            include_keywords=(),
            exclude_keywords=(),
            tracked_external_ids=set(),
            tracked_handles=set(),
        )

    base = run(products)
    other = run(list(shuffled))

    assert set(base.events) == set(other.events)  # same event set
    assert set(base.to_record) == set(other.to_record)  # same recorded set


# ===========================================================================
# 15.4 — Property 23: tracked-listing suppression records but emits no alert
# ===========================================================================


@settings(max_examples=200)
@given(data=st.data(), match_by=st.sampled_from(["external_id", "handle"]))
def test_property_23_tracked_listing_recorded_but_no_alert(
    data: st.DataObject, match_by: str
) -> None:
    """Feature: stock-drop-monitor, Property 23: A product covered by a tracked listing is
    recorded but emits no new-product alert.

    Validates: Requirements 3.1, 3.5, 3.6

    *For any* unseen product that matches an ENABLED listing on the same retailer by
    external_id OR by handle, :func:`diff_feed` records it in ``to_record`` (Req 3.5) but
    emits NO new-product event (the listing already covers it, Req 3.6).
    """
    fresh_id = data.draw(_ids)
    covered = data.draw(_products(external_id=fresh_id))

    tracked_external_ids: set[str] = set()
    tracked_handles: set[str] = set()
    if match_by == "external_id":
        tracked_external_ids = {fresh_id}
    else:
        tracked_handles = {covered.handle}

    result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="pokemon",
        feed_id=1,
        discovered=[covered],
        seen_external_ids=set(),  # genuinely unseen...
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=tracked_external_ids,
        tracked_handles=tracked_handles,
    )

    # Recorded regardless (Req 3.5)...
    assert [b.external_id for b in result.to_record] == [fresh_id]
    # ...but NO new-product event because a tracked listing covers it (Req 3.6 / Prop 23).
    assert not any(e.external_id == fresh_id for e in result.events)


# ===========================================================================
# 15.5 — fixture-driven scenarios against the REAL captured NCG data
# ===========================================================================


async def test_fixture_genuinely_new_product_yields_one_alert() -> None:
    """A genuinely-new product in a REAL fixture feed yields exactly one new-product alert.

    Validates: Requirements 21.5, 21.28, 3.1, 14.6

    Discovers the real pokemon collection, treats EVERY product as already-seen except the
    real pokeween product, and asserts exactly one new-product event — for pokeween — is
    emitted, while all 98 products are still recorded (Req 3.5).
    """
    discovered = await _discover_fixture(
        "/collections/pokemon/products.json", "collections_pokemon_products_page1.json"
    )
    all_ids = {p.external_id for p in discovered}
    assert _POKEWEEN_ID in all_ids  # anchor present in the real fixture

    result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="pokemon",
        feed_id=1,
        discovered=discovered,
        seen_external_ids=all_ids - {_POKEWEEN_ID},  # everything seen but pokeween
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )

    assert len(result.events) == 1
    assert result.events[0].external_id == _POKEWEEN_ID
    assert len(result.to_record) == len(discovered)  # all recorded (Req 3.5)


async def test_fixture_two_games_same_retailer_each_alert_same_external_id() -> None:
    """Two feeds for DIFFERENT games on the SAME retailer each alert for the same external_id.

    Validates: Requirements 21.5, 21.28, 3.1, 14.6

    The same product id is discovered under two games (pokemon via one feed, "preorders"
    via the other). Dedup is keyed by (retailer, game, external_id), so with an EMPTY seen
    set for each game the product is new under BOTH games and each feed emits its own
    new-product event for that external_id.
    """
    pokemon = await _discover_fixture(
        "/collections/pokemon/products.json", "collections_pokemon_products_page1.json"
    )
    preorders = await _discover_fixture(
        "/collections/pre-orders-1/products.json",
        "collections_pre_orders_1_products_page1.json",
    )
    assert _OVERLAP_ID in {p.external_id for p in pokemon}
    assert _OVERLAP_ID in {p.external_id for p in preorders}

    pokemon_result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="pokemon",
        feed_id=1,
        discovered=pokemon,
        seen_external_ids=set(),  # fresh seen set for the pokemon game
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )
    preorders_result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="preorders",
        feed_id=2,
        discovered=preorders,
        seen_external_ids=set(),  # independent fresh seen set for the other game
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )

    pokemon_hits = [e for e in pokemon_result.events if e.external_id == _OVERLAP_ID]
    preorder_hits = [e for e in preorders_result.events if e.external_id == _OVERLAP_ID]
    assert len(pokemon_hits) == 1
    assert len(preorder_hits) == 1
    # Each event is scoped to its own game (independent per (retailer, game, external_id)).
    assert pokemon_hits[0].game_id == "pokemon"
    assert preorder_hits[0].game_id == "preorders"


async def test_fixture_product_present_in_both_feeds_yields_exactly_one_event() -> None:
    """Requirement (a): a product present in BOTH feeds yields EXACTLY ONE new-product event.

    Validates: Requirements 21.5, 21.28, 3.1, 3.2, 14.6

    Using the REAL captured fixtures, the single product id that appears in BOTH the
    pokemon and pre-orders collection feeds is already in the seen set for ONE game and
    unseen for the OTHER. Dedup is keyed by (retailer, game, external_id), so across the
    two feeds the product yields EXACTLY ONE new-product event — for the game where it is
    new — and NONE for the game where it is already seen. This covers the "product present
    in both feeds yields exactly one new-product event" requirement.
    """
    pokemon = await _discover_fixture(
        "/collections/pokemon/products.json", "collections_pokemon_products_page1.json"
    )
    preorders = await _discover_fixture(
        "/collections/pre-orders-1/products.json",
        "collections_pre_orders_1_products_page1.json",
    )
    assert _OVERLAP_ID in {p.external_id for p in pokemon}
    assert _OVERLAP_ID in {p.external_id for p in preorders}

    # The overlap product is ALREADY seen for the pokemon game, but NEW for preorders.
    pokemon_result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="pokemon",
        feed_id=1,
        discovered=pokemon,
        seen_external_ids={_OVERLAP_ID},  # already seen here => no alert
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )
    preorders_result = diff_feed(
        retailer_id=_RETAILER_ID,
        game_id="preorders",
        feed_id=2,
        discovered=preorders,
        seen_external_ids=set(),  # new here => exactly one alert
        include_keywords=(),
        exclude_keywords=(),
        tracked_external_ids=set(),
        tracked_handles=set(),
    )

    combined: list[NewProductEvent] = [
        e for e in (*pokemon_result.events, *preorders_result.events)
        if e.external_id == _OVERLAP_ID
    ]
    assert len(combined) == 1  # exactly one across both feeds
    assert combined[0].game_id == "preorders"  # the game where it was genuinely new
    # It is still RECORDED in both feeds' seen-product write sets (Req 3.5).
    assert _OVERLAP_ID in {b.external_id for b in pokemon_result.to_record}
    assert _OVERLAP_ID in {b.external_id for b in preorders_result.to_record}


@pytest.mark.parametrize("feed_url", ["/collections/pokemon/products.json"])
def test_fixture_dir_exists(feed_url: str) -> None:
    """Guard: the captured fixtures the scenarios depend on are present."""
    assert (_FIXTURE_DIR / "collections_pokemon_products_page1.json").exists()
    assert (_FIXTURE_DIR / "collections_pre_orders_1_products_page1.json").exists()
