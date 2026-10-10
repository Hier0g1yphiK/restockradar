"""Fixture-driven adapter tests for :mod:`restockradar.adapters.shopify` (tasks 12.4-12.8).

SCOPE / PROVENANCE: these tests drive :class:`ShopifyAdapter` against the REAL Northern
Card Gaming (NCG) JSON a human operator captured with ``scripts/capture_ncg_fixtures.py``
(task 10) and saved under ``fixtures/ncg/``. Every request is served from those saved
bytes through an :class:`httpx.MockTransport`; the suite-wide no-live-network guard in
``conftest.py`` (Req 21.4) means a real socket attempt is a hard failure, so these tests
NEVER touch the network.

They supersede the inline synthetic smoke tests in
``tests/test_shopify_adapter_synthetic.py`` (tasks 12.1-12.3) by exercising the same code
paths against real captured data, plus the universal price-normalisation property:

* 12.4 — Property 5: price equality under normalisation yields no price-change event.
* 12.5 — pagination terminates on a short page; raises TruncatedFeed when every page is
  full up to ``max_page_count``.
* 12.6 — ``.js`` minor-units and the feed decimal string normalise to the same Decimal
  (no false price change).
* 12.7 — image-URL normalisation on the URLs actually present in the fixtures.
* 12.8 — 404 / redirect / handle-rename resolution via the real pokeween id+handle.

The REAL anchor used throughout is the pokeween product from
``products_ncg-pokeween-18-october.js.json``: product id ``15440414114171``, handle
``ncg-pokeween-18-october``, a single available variant priced ``900`` minor units
(=> ``Decimal("9.00")`` at ``minor_unit_exponent=2``). The SAME product appears in
``collections_pokemon_products_page1.json`` with the feed decimal-string price ``"9.00"``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.base import DiscoveredProduct, Existence, VariantState
from restockradar.adapters.errors import TruncatedFeed
from restockradar.adapters.shopify import ShopifyAdapter
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry

#: Real NCG origin the fixtures were captured from (used so cross-host redirect tests can
#: point at a genuinely different host).
BASE_URL = "https://www.northerncardgaming.com"
ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"

#: Directory holding the operator-captured real NCG fixtures (task 10).
_FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "ncg"

#: Real pokeween anchor (from ``products_ncg-pokeween-18-october.js.json``).
POKEWEEN_ID = "15440414114171"
POKEWEEN_HANDLE = "ncg-pokeween-18-october"
POKEWEEN_PRICE = Decimal("9.00")  # 900 minor units at exponent 2 == feed "9.00"

Handler = Callable[[httpx.Request], httpx.Response]


# ---------------------------------------------------------------------------
# Fixture loading (read the saved bytes; never synthesise product data)
# ---------------------------------------------------------------------------


def _fixture_bytes(name: str) -> bytes:
    """Return the raw bytes of a captured fixture under ``fixtures/ncg/``."""
    return (_FIXTURE_DIR / name).read_bytes()


def _fixture_json(name: str) -> dict[str, Any]:
    """Parse a captured JSON fixture into a dict."""
    parsed = json.loads(_fixture_bytes(name))
    assert isinstance(parsed, dict)
    return parsed


def _json_response(payload: object, *, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )


def _fixture_response(name: str) -> httpx.Response:
    """A 200 JSON response whose body is the saved fixture bytes verbatim."""
    return httpx.Response(
        200,
        content=_fixture_bytes(name),
        headers={"content-type": "application/json"},
    )


# ---------------------------------------------------------------------------
# Minimal structural stand-ins (not frozen: the adapter's input protocols declare
# plain settable attributes, which frozen dataclasses do not satisfy under mypy --strict)
# ---------------------------------------------------------------------------


@dataclass
class FakeRetailer:
    base_url: str = BASE_URL
    minor_unit_exponent: int = 2


@dataclass
class FakeListing:
    url: str
    handle: str
    external_id: str | None = None


@dataclass
class FakeFeed:
    feed_url: str
    max_page_count: int = 20


# ---------------------------------------------------------------------------
# Shared wiring (mirrors the synthetic suite: robots allow-all, no-spacing host gate)
# ---------------------------------------------------------------------------


def _robots_handler(inner: Handler) -> Handler:
    """Wrap a handler so ``/robots.txt`` is answered allow-all (request-path robots)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        return inner(request)

    return handler


def _build_adapter(
    handler: Handler,
    *,
    heartbeat: HeartbeatRegistry | None = None,
    retailer: FakeRetailer | None = None,
) -> tuple[ShopifyAdapter, HttpClient]:
    """Build a ``ShopifyAdapter`` whose client is driven entirely by ``handler``."""
    transport = httpx.MockTransport(_robots_handler(handler))
    client = HttpClient(
        heartbeat=heartbeat or HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=transport,
    )
    return ShopifyAdapter(client, retailer or FakeRetailer()), client


# ===========================================================================
# 12.4 — Property 5: price equality under normalisation yields no price-change
# ===========================================================================


def _price_from_js(minor: int, exponent: int) -> Decimal:
    """Normalise an integer minor-unit ``.js`` price the way the adapter does (Req 11.5)."""
    return Decimal(minor) / (Decimal(10) ** exponent)


def _price_from_feed(decimal_string: str) -> Decimal:
    """Normalise a feed decimal-string price the way the adapter does (Req 11.5)."""
    return Decimal(decimal_string)


def _feed_decimal_string(minor: int, exponent: int) -> str:
    """Render ``minor`` minor units as the major-unit decimal string a feed would carry."""
    return str(_price_from_js(minor, exponent))


@settings(max_examples=200)
@given(
    minor=st.integers(min_value=0, max_value=10**12),
    exponent=st.integers(min_value=0, max_value=4),
)
def test_price_equality_under_normalisation_yields_no_price_change(
    minor: int, exponent: int
) -> None:
    """Feature: stock-drop-monitor, Property 5: Price equality under normalisation yields
    no price-change event.

    *For any* non-negative integer minor-unit amount and minor-unit exponent, the ``.js``
    integer minor-unit price and the equivalent feed decimal-string price normalise to
    the SAME :class:`~decimal.Decimal`, so comparing the two produces no false
    price-change event (the watcher compares exact Decimals, Req 5.1/5.5/11.5).

    Validates: Requirements 5.1, 5.5, 11.5
    """
    js_price = _price_from_js(minor, exponent)
    feed_price = _price_from_feed(_feed_decimal_string(minor, exponent))
    # Exact-Decimal equality is what the diff uses to decide "no change" (Req 5.1/5.5).
    assert js_price == feed_price

    # Anchor the property on at least one REAL .js fixture price: the pokeween variant's
    # 900 minor units must normalise to the same Decimal the feed decimal string "9.00"
    # does, under the real retailer exponent 2.
    js_body = _fixture_json(f"products_{POKEWEEN_HANDLE}.js.json")
    real_minor = int(js_body["variants"][0]["price"])
    assert _price_from_js(real_minor, 2) == _price_from_feed("9.00") == POKEWEEN_PRICE


# ===========================================================================
# 12.5 — pagination termination (short/empty page) + TruncatedFeed at max_page_count
# ===========================================================================


async def test_discover_stops_on_empty_second_page() -> None:
    """Serving the REAL pokemon collection as page 1 and an EMPTY page 2, ``discover``
    must stop on the short/empty page (Req 21.10, 12.6).

    The real collection fixture carries 98 products (< the 250 page limit), so page 1 is
    already a short page and the crawl terminates after it; an explicit empty page 2 is
    wired so that even if a full first page were served the empty follow-up would stop it.
    """
    collection = _fixture_json("collections_pokemon_products_page1.json")
    expected_count = len(collection["products"])
    seen_pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        assert request.url.params["limit"] == "250"
        assert "sort_by" not in request.url.params
        seen_pages.append(page)
        if page == 1:
            return _fixture_response("collections_pokemon_products_page1.json")
        return _json_response({"products": []})  # empty follow-up page => stop

    adapter, client = _build_adapter(handler)
    try:
        result = await adapter.discover(FakeFeed("/collections/pokemon/products.json"))
    finally:
        await client.aclose()

    # The 98-product first page is already short, so the crawl stops after page 1.
    assert seen_pages == [1]
    assert len(result) == expected_count
    # The real pokeween product is present in this collection.
    assert any(p.external_id == POKEWEEN_ID for p in result)


async def test_discover_raises_truncated_feed_when_every_page_is_full() -> None:
    """When every page is full up to a small ``max_page_count``, ``discover`` raises
    :class:`TruncatedFeed` (Req 21.10, 12.6).

    A synthetic *full* (exactly 250-product) page is served for every page so the crawl
    never hits a short page; this is the only way to force the non-terminating path, and
    the product rows reuse the real pokeween shape so the adapter parses real fields.
    """
    real_product = _fixture_json("collections_pokemon_products_page1.json")["products"][0]
    full_page = {"products": [dict(real_product, id=real_product["id"] + i) for i in range(250)]}

    def handler(request: httpx.Request) -> httpx.Response:
        return _json_response(full_page)

    adapter, client = _build_adapter(handler)
    try:
        with pytest.raises(TruncatedFeed):
            await adapter.discover(FakeFeed("/collections/pokemon/products.json", max_page_count=3))
    finally:
        await client.aclose()


# ===========================================================================
# 12.6 — minor-units vs decimal-string parity (no false price change)
# ===========================================================================


def _pokeween_feed_variants() -> tuple[VariantState, ...]:
    """Build the pokeween variants as the discovery path would from the REAL feed entry.

    Reads the pokeween entry out of the real pokemon collection fixture (feed decimal
    strings) so the feed-reuse path is exercised with the actual captured feed price.
    """
    feed = _fixture_json("collections_pokemon_products_page1.json")
    entry = next(p for p in feed["products"] if str(p["id"]) == POKEWEEN_ID)
    return tuple(
        VariantState(
            variant_id=str(v["id"]),
            available=bool(v.get("available", False)),
            price=Decimal(str(v["price"])),  # feed decimal string, major units (Req 11.5)
        )
        for v in entry["variants"]
    )


async def test_js_minor_units_and_feed_decimal_string_yield_same_price() -> None:
    """``.js`` integer minor units and the feed decimal string normalise to the SAME
    Decimal, so no false price-change arises (Req 21.21, 11.5).

    Fetches the real pokeween product two ways — via its ``.js`` fixture (integer minor
    units ``900``) and via this cycle's feed data (decimal string ``"9.00"``, feed-reuse)
    — and asserts both resolve to ``Decimal("9.00")``.
    """
    listing = FakeListing(
        url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
        handle=POKEWEEN_HANDLE,
        external_id=POKEWEEN_ID,
    )

    # (a) via the real .js fixture (no feed data in this cycle => .js request).
    def js_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/products/{POKEWEEN_HANDLE}.js"
        return _fixture_response(f"products_{POKEWEEN_HANDLE}.js.json")

    adapter, client = _build_adapter(js_handler)
    try:
        via_js = await adapter.fetch_state(listing, {})
    finally:
        await client.aclose()

    # (b) via feed-reuse of the same product's real feed entry (decimal string "9.00").
    cycle: Mapping[str, DiscoveredProduct] = {
        POKEWEEN_ID: DiscoveredProduct(
            external_id=POKEWEEN_ID,
            title="NCG: Pokeween TCG Event (18th October)",
            product_type=None,
            vendor=None,
            tags=(),
            handle=POKEWEEN_HANDLE,
            url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
            image=None,
            variants=_pokeween_feed_variants(),
        )
    }

    def no_request_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("feed-reuse must not issue a .js request")

    adapter2, client2 = _build_adapter(no_request_handler)
    try:
        via_feed = await adapter2.fetch_state(listing, cycle)
    finally:
        await client2.aclose()

    assert via_js.price == POKEWEEN_PRICE
    assert via_feed.price == POKEWEEN_PRICE
    assert via_js.price == via_feed.price  # no false price change between the two paths


# ===========================================================================
# 12.7 — image-URL normalisation (fixture-driven)
# ===========================================================================


async def test_js_fixture_protocol_relative_image_normalised_to_https() -> None:
    """The pokeween ``.js`` fixture's protocol-relative ``//cdn...`` image is normalised
    to an ``https:`` URL (Req 8.11)."""
    js_body = _fixture_json(f"products_{POKEWEEN_HANDLE}.js.json")
    raw_image = js_body["featured_image"]
    assert isinstance(raw_image, str) and raw_image.startswith("//")  # real fixture shape

    def handler(request: httpx.Request) -> httpx.Response:
        return _fixture_response(f"products_{POKEWEEN_HANDLE}.js.json")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}", handle=POKEWEEN_HANDLE),
            {},
        )
    finally:
        await client.aclose()

    assert state.image == "https:" + raw_image


async def test_feed_fixture_absolute_image_preserved_in_discovery() -> None:
    """Discovery over the real pokemon collection preserves the absolute ``https://``
    image URLs the feed carries in ``images[0].src`` (Req 8.11)."""
    collection = _fixture_json("collections_pokemon_products_page1.json")
    entry = next(p for p in collection["products"] if str(p["id"]) == POKEWEEN_ID)
    expected_src = entry["images"][0]["src"]
    assert isinstance(expected_src, str) and expected_src.startswith("https://")

    def handler(request: httpx.Request) -> httpx.Response:
        if int(request.url.params["page"]) == 1:
            return _fixture_response("collections_pokemon_products_page1.json")
        return _json_response({"products": []})

    adapter, client = _build_adapter(handler)
    try:
        result = await adapter.discover(FakeFeed("/collections/pokemon/products.json"))
    finally:
        await client.aclose()

    pokeween = next(p for p in result if p.external_id == POKEWEEN_ID)
    assert pokeween.image == expected_src


async def test_yugioh_fixture_without_image_yields_none() -> None:
    """The yu-gi-oh ``.js`` fixture carries no image, so the normalised image is ``None``
    (Req 8.11)."""
    js_body = _fixture_json("products_yu-gi-oh-phantom-revenge-booster-box-pre.js.json")
    assert js_body["featured_image"] is None  # real fixture shape
    handle = js_body["handle"]

    def handler(request: httpx.Request) -> httpx.Response:
        return _fixture_response("products_yu-gi-oh-phantom-revenge-booster-box-pre.js.json")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/{handle}", handle=handle), {}
        )
    finally:
        await client.aclose()

    assert state.image is None


def test_image_normalisation_relative_and_non_http_direct() -> None:
    """Relative paths are urljoin'd against the base URL and non-http(s) schemes drop to
    ``None`` (Req 8.11).

    Driven directly through ``_normalise_image`` to cover the relative and non-http(s)
    branches, which the real NCG fixtures (all protocol-relative or absolute https) do not
    themselves contain.
    """
    adapter, _client = _build_adapter(lambda request: _json_response({}))
    # Relative path => urljoin against base_url.
    assert adapter._normalise_image("/img/b.png") == f"{BASE_URL}/img/b.png"
    # Protocol-relative => https: (matches the real .js fixtures' form).
    assert adapter._normalise_image("//cdn.shopify.com/a.png") == "https://cdn.shopify.com/a.png"
    # Non-http(s) scheme => None.
    assert adapter._normalise_image("ftp://host/c.png") is None
    assert adapter._normalise_image(None) is None


# ===========================================================================
# 12.8 — handle-rename / not-found ADAPTER tests (both redirect paths)
# ===========================================================================


async def test_404_on_js_with_no_feed_match_returns_not_found() -> None:
    """A plain 404 on ``.js`` with no feed match is RETURNED as
    ``ProductState(existence=NOT_FOUND)`` and never raised (Req 21.2, 7.1)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
                handle=POKEWEEN_HANDLE,
                external_id=POKEWEEN_ID,
            ),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.NOT_FOUND
    assert state.external_id == POKEWEEN_ID
    assert state.resolved_handle is None
    assert state.resolved_url is None


@pytest.mark.parametrize("redirect_status", [301, 302, 307, 308])
async def test_same_host_js_redirect_matching_id_resolves_rename(redirect_status: int) -> None:
    """A same-host ``.js`` 301/302/307/308 whose resolved ``.js`` id equals the listing's
    ``external_id`` resolves a rename, with ``resolved_*`` set because the handle changed
    (Req 21.3, 11.8).

    The old handle redirects; the new handle serves the REAL pokeween ``.js`` fixture
    (real id ``15440414114171``), which matches the listing's ``external_id`` and so is
    trusted as this listing's rename.
    """
    new_handle = POKEWEEN_HANDLE  # the real fixture's handle
    old_handle = "ncg-pokeween-old-handle"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/products/{old_handle}.js":
            return httpx.Response(
                redirect_status, headers={"location": f"{BASE_URL}/products/{new_handle}"}
            )
        if request.url.path == f"/products/{new_handle}.js":
            return _fixture_response(f"products_{new_handle}.js.json")
        raise AssertionError(f"unexpected path {request.url.path}")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/{old_handle}",
                handle=old_handle,
                external_id=POKEWEEN_ID,
            ),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.external_id == POKEWEEN_ID
    assert state.resolved_handle == new_handle  # handle changed => rename reported
    assert state.resolved_url == f"{BASE_URL}/products/{new_handle}"
    assert state.price == POKEWEEN_PRICE


async def test_cross_host_js_redirect_falls_through_to_not_found() -> None:
    """A cross-host ``.js`` redirect is never trusted as a rename and falls through to
    NOT_FOUND when no feed match exists (Req 21.3, 11.8)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/products/{POKEWEEN_HANDLE}.js":
            return httpx.Response(301, headers={"location": "https://evil.example/products/x"})
        raise AssertionError("must not request the cross-host redirect target")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
                handle=POKEWEEN_HANDLE,
                external_id=POKEWEEN_ID,
            ),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.NOT_FOUND


async def test_same_host_js_redirect_id_mismatch_falls_through_to_not_found() -> None:
    """A same-host ``.js`` redirect whose resolved id does NOT match the listing's
    ``external_id`` is rejected and falls through to NOT_FOUND (Req 21.3, 11.8).

    The redirect target serves the real pokeween fixture (id ``15440414114171``) but the
    listing claims a different ``external_id``, so the resolution is not trusted.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/some-old-handle.js":
            return httpx.Response(
                301, headers={"location": f"{BASE_URL}/products/{POKEWEEN_HANDLE}"}
            )
        if request.url.path == f"/products/{POKEWEEN_HANDLE}.js":
            return _fixture_response(f"products_{POKEWEEN_HANDLE}.js.json")
        raise AssertionError(f"unexpected path {request.url.path}")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/some-old-handle",
                handle="some-old-handle",
                external_id="99999999999999",  # deliberately NOT the pokeween id
            ),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.NOT_FOUND


async def test_plain_404_resolved_via_feed_new_handle_sets_resolved_fields() -> None:
    """A plain 404 resolved via this cycle's feed under a NEW handle sets ``resolved_*``
    because the handle differs (Req 21.3, 7.1, 11.8).

    The listing is under an old handle; the real pokeween product (same ``external_id``)
    appears in this cycle's feed under its real handle, so the 404 is resolved to EXISTING
    and the rename is reported.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="gone")

    cycle: Mapping[str, DiscoveredProduct] = {
        POKEWEEN_ID: DiscoveredProduct(
            external_id=POKEWEEN_ID,
            title="NCG: Pokeween TCG Event (18th October)",
            product_type=None,
            vendor=None,
            tags=(),
            handle=POKEWEEN_HANDLE,
            url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
            image=None,
            variants=_pokeween_feed_variants(),
        )
    }
    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/pokeween-old-handle",
                handle="pokeween-old-handle",
                external_id=POKEWEEN_ID,
            ),
            cycle,
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.resolved_handle == POKEWEEN_HANDLE  # handle changed => rename reported
    assert state.resolved_url == f"{BASE_URL}/products/{POKEWEEN_HANDLE}"
    assert state.price == POKEWEEN_PRICE


async def test_plain_404_resolved_via_feed_same_handle_sets_no_resolved_fields() -> None:
    """A plain 404 resolved via this cycle's feed under the SAME handle sets no
    ``resolved_*`` (ordinary feed-reuse, not a rename, Req 11.8)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="gone")

    cycle: Mapping[str, DiscoveredProduct] = {
        POKEWEEN_ID: DiscoveredProduct(
            external_id=POKEWEEN_ID,
            title="NCG: Pokeween TCG Event (18th October)",
            product_type=None,
            vendor=None,
            tags=(),
            handle=POKEWEEN_HANDLE,
            url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
            image=None,
            variants=_pokeween_feed_variants(),
        )
    }
    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/{POKEWEEN_HANDLE}",
                handle=POKEWEEN_HANDLE,
                external_id=POKEWEEN_ID,
            ),
            cycle,
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.resolved_handle is None  # same handle => no rename
    assert state.resolved_url is None
