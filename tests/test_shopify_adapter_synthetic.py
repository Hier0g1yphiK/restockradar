"""Synthetic smoke tests for :mod:`restockradar.adapters.shopify` (tasks 12.1-12.3 CODE).

SCOPE / PROVENANCE: these are small, inline, SYNTHETIC-fixture smoke tests that drive the
adapter entirely through an :class:`httpx.MockTransport` — they NEVER touch the network
and NEVER assume any real Northern Card Gaming fixture file exists. They exist only to
prove the task 12.1/12.2/12.3 adapter CODE compiles and exercises its core paths
(discovery pagination, ``.js`` fetch + price normalisation, feed-reuse, 404 → NOT_FOUND,
same-host ``.js`` redirect rename resolution, feed-based rename resolution, and image URL
normalisation).

They are DELIBERATELY MINIMAL and are SUPERSEDED by the fixture-driven adapter tests in
tasks 12.4-12.8, which consume the REAL NCG fixtures a human operator captures in task 10.
Do not grow this file into the real-fixture coverage; that belongs in 12.4-12.8.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

import httpx
import pytest

from restockradar.adapters.base import DiscoveredProduct, Existence, VariantState
from restockradar.adapters.errors import TruncatedFeed
from restockradar.adapters.shopify import ShopifyAdapter
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import Component, HeartbeatRegistry

BASE_URL = "https://shop.example"
ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"

Handler = Callable[[httpx.Request], httpx.Response]


# ---------------------------------------------------------------------------
# Minimal structural stand-ins (synthetic; no domain/db dependency)
# ---------------------------------------------------------------------------


# NOTE: these stand-ins are intentionally NOT frozen — the adapter's structural input
# protocols (ListingLike / FeedLike / RetailerLike) declare plain (settable) attributes,
# which a frozen dataclass's read-only attributes do not satisfy under mypy --strict.
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


def _json(payload: object, *, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )


def _robots_handler(inner: Handler) -> Handler:
    """Wrap a handler so ``/robots.txt`` is answered allow-all (keeps the client happy)."""

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
    transport = httpx.MockTransport(_robots_handler(handler))
    client = HttpClient(
        heartbeat=heartbeat or HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=transport,
    )
    return ShopifyAdapter(client, retailer or FakeRetailer()), client


def _variant(vid: int, *, available: bool, price: int) -> dict[str, object]:
    """A synthetic ``.js`` variant: integer MINOR-unit price (Req 11.5)."""
    return {"id": vid, "available": available, "price": price}


def _js_product(
    *, pid: int, handle: str, title: str, variants: list[dict[str, object]]
) -> dict[str, object]:
    return {
        "id": pid,
        "handle": handle,
        "title": title,
        "featured_image": "//cdn.shopify.com/img.png",
        "variants": variants,
    }


# ---------------------------------------------------------------------------
# 12.1 — discovery pagination
# ---------------------------------------------------------------------------


def _feed_product(pid: int) -> dict[str, object]:
    return {
        "id": pid,
        "handle": f"handle-{pid}",
        "title": f"Product {pid}",
        "product_type": "Booster",
        "vendor": "Pokemon",
        "tags": ["pokemon", "tcg"],
        "featured_image": {"src": "//cdn.shopify.com/f.png"},
        "variants": [{"id": pid * 10, "available": True, "price": "49.99"}],
    }


async def test_discover_stops_on_short_page_and_beats_per_page() -> None:
    pages: dict[int, list[dict[str, object]]] = {
        1: [_feed_product(i) for i in range(250)],
        2: [_feed_product(1000 + i) for i in range(3)],  # short page => stop
    }
    seen_pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        assert request.url.params["limit"] == "250"
        assert "sort_by" not in request.url.params
        assert "+" not in str(request.url.query)
        seen_pages.append(page)
        return _json({"products": pages.get(page, [])})

    heartbeat = HeartbeatRegistry()
    before = heartbeat.get(Component.SCHEDULER)
    adapter, client = _build_adapter(handler, heartbeat=heartbeat)
    try:
        result = await adapter.discover(FakeFeed("/collections/pokemon/products.json"))
    finally:
        await client.aclose()

    assert seen_pages == [1, 2]  # stopped after the short second page
    assert len(result) == 253
    assert result[0].external_id == "0"
    # Per-page liveness: the client beat the scheduler heartbeat (>= once).
    assert heartbeat.get(Component.SCHEDULER) >= before


async def test_discover_raises_truncated_feed_at_max_page_count() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # Every page is full, so the crawl never terminates on its own.
        return _json({"products": [_feed_product(i) for i in range(250)]})

    adapter, client = _build_adapter(handler)
    try:
        with pytest.raises(TruncatedFeed):
            await adapter.discover(FakeFeed("/products.json", max_page_count=2))
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# 12.2 — single-listing .js fetch, price normalisation, feed-reuse
# ---------------------------------------------------------------------------


async def test_fetch_state_js_normalises_minor_units_and_lowest_available() -> None:
    product = _js_product(
        pid=42,
        handle="thing",
        title="Thing",
        variants=[
            _variant(1, available=False, price=1999),  # cheaper but unavailable
            _variant(2, available=True, price=4999),
            _variant(3, available=True, price=5999),
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/products/thing.js"  # .js, never .json
        return _json(product)

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/thing", handle="thing"), {}
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.in_stock is True
    # Lowest AVAILABLE variant: 4999 minor units => 49.99 major (exponent 2).
    assert state.price == Decimal("49.99")
    assert state.external_id == "42"
    assert state.image == "https://cdn.shopify.com/img.png"  # // => https:
    assert state.resolved_handle is None  # handle unchanged => no rename


async def test_fetch_state_reuses_feed_data_without_requesting() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        raise AssertionError("feed-reuse must not issue a .js request")

    cycle: Mapping[str, DiscoveredProduct] = {
        "99": DiscoveredProduct(
            external_id="99",
            title="Cached",
            product_type=None,
            vendor=None,
            tags=(),
            handle="cached",
            url=f"{BASE_URL}/products/cached",
            image="//cdn.shopify.com/c.png",
            variants=(VariantState("v1", available=True, price=Decimal("12.50")),),
        )
    }
    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/cached", handle="cached", external_id="99"),
            cycle,
        )
    finally:
        await client.aclose()

    assert requested == []  # no .js request issued (Req 11.6)
    assert state.existence is Existence.EXISTING
    assert state.price == Decimal("12.50")  # feed decimal string, major units
    assert state.resolved_handle is None


# ---------------------------------------------------------------------------
# 12.3 — 404 / redirect / rename resolution + image normalisation
# ---------------------------------------------------------------------------


async def test_plain_404_with_no_feed_match_returns_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/gone", handle="gone", external_id="7"),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.NOT_FOUND
    assert state.external_id == "7"
    assert state.resolved_handle is None


async def test_same_host_js_redirect_matching_id_resolves_rename() -> None:
    renamed = _js_product(
        pid=7,
        handle="new-handle",
        title="Renamed",
        variants=[_variant(1, available=True, price=2500)],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/old-handle.js":
            return httpx.Response(301, headers={"location": f"{BASE_URL}/products/new-handle"})
        if request.url.path == "/products/new-handle.js":
            return _json(renamed)
        raise AssertionError(f"unexpected path {request.url.path}")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(
                url=f"{BASE_URL}/products/old-handle", handle="old-handle", external_id="7"
            ),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.resolved_handle == "new-handle"  # handle changed => rename
    assert state.resolved_url == f"{BASE_URL}/products/new-handle"
    assert state.price == Decimal("25.00")


async def test_cross_host_redirect_falls_through_to_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/old.js":
            return httpx.Response(302, headers={"location": "https://evil.example/products/x"})
        raise AssertionError("must not request the cross-host target")

    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/old", handle="old", external_id="7"),
            {},
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.NOT_FOUND


async def test_404_resolved_via_feed_under_new_handle_sets_resolved_fields() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="gone")

    cycle: Mapping[str, DiscoveredProduct] = {
        "7": DiscoveredProduct(
            external_id="7",
            title="Moved",
            product_type=None,
            vendor=None,
            tags=(),
            handle="moved-new",
            url=f"{BASE_URL}/products/moved-new",
            image=None,
            variants=(VariantState("v1", available=True, price=Decimal("9.99")),),
        )
    }
    adapter, client = _build_adapter(handler)
    try:
        state = await adapter.fetch_state(
            FakeListing(url=f"{BASE_URL}/products/old", handle="old", external_id="7"),
            cycle,
        )
    finally:
        await client.aclose()

    assert state.existence is Existence.EXISTING
    assert state.resolved_handle == "moved-new"
    assert state.price == Decimal("9.99")


def test_image_normalisation_variants() -> None:
    adapter = ShopifyAdapter(http=None, retailer=FakeRetailer())  # type: ignore[arg-type]
    # Protocol-relative => https:
    assert adapter._normalise_image("//cdn.shopify.com/a.png") == "https://cdn.shopify.com/a.png"
    # Relative path => urljoin against base_url
    assert adapter._normalise_image("/img/b.png") == f"{BASE_URL}/img/b.png"
    # Non-http(s) scheme => None
    assert adapter._normalise_image("ftp://host/c.png") is None
    assert adapter._normalise_image(None) is None
