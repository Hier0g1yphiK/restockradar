"""Task 8.1 — unit tests for the adapter interface, registry, and errors
(Req 11.1, 11.2, 11.3, 11.9, 22.2).

Covers:

* the self-contained value types (``ProductState`` / ``DiscoveredProduct`` /
  ``VariantState`` / ``Existence``) and that a 404 is a RETURNED ``NOT_FOUND`` state,
* the :class:`AdapterRegistry` resolving a registered ``shopify`` factory and raising
  :class:`UnknownAdapterType` for anything else,
* that there is no ``NotFound`` error class (Req 11.3) and the error hierarchy is wired
  as the HTTP-classification design expects.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

import pytest

from restockradar.adapters import base, errors
from restockradar.adapters.base import (
    Adapter,
    DiscoveredProduct,
    Existence,
    ProductState,
    VariantState,
)
from restockradar.adapters.errors import (
    Blocked,
    FetchError,
    MalformedResponse,
    SoftBlock,
    TruncatedFeed,
    UnknownAdapterType,
)
from restockradar.adapters.registry import AdapterRegistry, RetailerLike

# ---------------------------------------------------------------------------
# Value types (Req 11.2) + NOT_FOUND is a returned state (Req 7.1)
# ---------------------------------------------------------------------------


def test_existence_members() -> None:
    assert {e.value for e in Existence} == {"existing", "not_found"}


def test_product_state_not_found_is_a_value_not_an_error() -> None:
    """A 404 is modelled as a ``ProductState`` with ``NOT_FOUND`` existence (Req 7.1)."""
    state = ProductState(
        existence=Existence.NOT_FOUND,
        in_stock=False,
        price=None,
        variants=(),
        url="https://example.test/products/gone",
        image=None,
        title=None,
        external_id=None,
    )
    assert state.existence is Existence.NOT_FOUND
    assert not isinstance(state, BaseException)


def test_product_state_resolved_fields_default_to_none() -> None:
    """``resolved_handle`` / ``resolved_url`` default to None (no rename, Req 11.8)."""
    state = ProductState(
        existence=Existence.EXISTING,
        in_stock=True,
        price=Decimal("12.99"),
        variants=(VariantState(variant_id="v1", available=True, price=Decimal("12.99")),),
        url="https://example.test/products/x",
        image=None,
        title="X",
        external_id="100",
    )
    assert state.resolved_handle is None
    assert state.resolved_url is None


def test_value_types_are_frozen() -> None:
    variant = VariantState(variant_id="v1", available=True, price=Decimal("1.00"))
    with pytest.raises(Exception):  # noqa: B017 — FrozenInstanceError is a dataclass internal
        variant.available = False  # type: ignore[misc]


def test_discovered_product_carries_keyword_surfaces() -> None:
    product = DiscoveredProduct(
        external_id="100",
        title="Pokemon Box",
        product_type="TCG",
        vendor="Nintendo",
        tags=("preorder",),
        handle="pokemon-box",
        url="https://example.test/products/pokemon-box",
        image=None,
        variants=None,
    )
    assert product.external_id == "100"
    assert product.tags == ("preorder",)


# ---------------------------------------------------------------------------
# Adapter Protocol (Req 11.1)
# ---------------------------------------------------------------------------


@dataclass
class _Retailer:
    adapter_type: str


@dataclass
class _Listing:
    url: str
    handle: str
    external_id: str | None


@dataclass
class _Feed:
    feed_url: str
    max_page_count: int


class _FakeShopify:
    """A minimal structural Adapter used to prove the Protocol is satisfiable."""

    adapter_type = "shopify"

    async def fetch_state(
        self, listing: base.ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        return ProductState(
            existence=Existence.EXISTING,
            in_stock=True,
            price=Decimal("1.00"),
            variants=(),
            url=listing.url,
            image=None,
            title=None,
            external_id=listing.external_id,
        )

    async def discover(self, feed: base.FeedLike) -> list[DiscoveredProduct]:
        return []


def test_fake_adapter_satisfies_protocol() -> None:
    adapter: Adapter = _FakeShopify()
    assert adapter.adapter_type == "shopify"


def test_protocol_methods_are_coroutines() -> None:
    assert inspect.iscoroutinefunction(_FakeShopify.fetch_state)
    assert inspect.iscoroutinefunction(_FakeShopify.discover)


# ---------------------------------------------------------------------------
# Registry (Req 11.9, 22.2)
# ---------------------------------------------------------------------------


def test_registry_resolves_registered_shopify_factory() -> None:
    registry = AdapterRegistry()
    built: list[RetailerLike] = []

    def factory(retailer: RetailerLike) -> Adapter:
        built.append(retailer)
        return _FakeShopify()

    registry.register("shopify", factory)
    retailer = _Retailer(adapter_type="shopify")

    adapter = registry.resolve(retailer)

    assert isinstance(adapter, _FakeShopify)
    assert built == [retailer]  # factory received the retailer


def test_registry_raises_unknown_adapter_type() -> None:
    registry = AdapterRegistry()
    registry.register("shopify", lambda _retailer: _FakeShopify())

    with pytest.raises(UnknownAdapterType) as excinfo:
        registry.resolve(_Retailer(adapter_type="sitemap"))

    assert excinfo.value.adapter_type == "sitemap"


def test_registry_only_registers_shopify_by_default_is_caller_choice() -> None:
    """A fresh registry has nothing registered; the MVP wiring registers only shopify."""
    registry = AdapterRegistry()
    assert registry.is_registered("shopify") is False
    with pytest.raises(UnknownAdapterType):
        registry.resolve(_Retailer(adapter_type="shopify"))


# ---------------------------------------------------------------------------
# Error classes (Req 11.3) — no NotFound, hierarchy as designed
# ---------------------------------------------------------------------------


def test_no_notfound_error_class_exists() -> None:
    """A 404 is a returned state, so there is deliberately no NotFound error (Req 11.3)."""
    assert not hasattr(errors, "NotFound")


def test_error_hierarchy() -> None:
    assert issubclass(Blocked, FetchError)
    assert issubclass(SoftBlock, Blocked)
    assert issubclass(MalformedResponse, FetchError)
    assert issubclass(TruncatedFeed, FetchError)


def test_blocked_carries_optional_retry_after() -> None:
    blocked = Blocked("429 rate limited", retry_after=12.5)
    assert blocked.retry_after == 12.5
    assert Blocked().retry_after is None
