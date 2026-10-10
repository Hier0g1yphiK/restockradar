"""Adapter Protocol/ABC (``fetch_state``, ``discover``) and the shared value types:
``ProductState``, ``DiscoveredProduct``, ``VariantState``, ``Existence`` (Req 11.1-11.3).

This module is deliberately **self-contained**: the retailer-agnostic value types the
core speaks in live here and here only, so the ``adapters`` package can be built and
tested without depending on the still-in-flight ``domain.models`` module. The adapter
inputs (a listing to fetch, a discovery feed to crawl) are described by the structural
protocols :class:`ListingLike` and :class:`FeedLike` rather than concrete domain
dataclasses, so any object carrying the needed attributes satisfies the contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Protocol, runtime_checkable


class Existence(Enum):
    """Whether a product page exists on the retailer (Req 7.1).

    ``NOT_FOUND`` is a definite 404 on the product endpoint, RETURNED as a result,
    never raised — a not-found page is an answer, not an error.
    """

    EXISTING = "existing"
    NOT_FOUND = "not_found"


@dataclass(frozen=True)
class VariantState:
    """One purchasable variant of a product (Req 4.2, 11.5)."""

    variant_id: str
    available: bool  # Shopify ``available`` flag (Req 4.2)
    price: Decimal  # already normalised to major units (Req 11.5)


@dataclass(frozen=True)
class ProductState:
    """Result of :meth:`Adapter.fetch_state` (Req 11.2).

    For a ``NOT_FOUND`` result, ``in_stock`` and ``price`` are placeholders the watcher
    IGNORES — a ``NOT_FOUND`` state never writes stock or price; they stay NULL until
    the page exists (Req 7).

    ``resolved_handle`` / ``resolved_url`` are set ONLY when the adapter resolved the
    listing to a handle that DIFFERS from the listing's stored handle (a rename, Req
    11.8), so the WATCHER — inside its own transaction — persists the url/handle change
    and queues the handle-change admin notice. When the resolved handle EQUALS the
    stored handle (ordinary feed-reuse) both stay ``None`` and the adapter writes
    nothing.
    """

    existence: Existence
    in_stock: bool  # any variant available (Req 4.2); IGNORED when existence=NOT_FOUND
    price: Decimal | None  # lowest available else lowest overall (Req 5.2); None if no variants
    variants: Sequence[VariantState]
    url: str
    image: str | None
    title: str | None
    external_id: str | None  # Shopify product id as string (Req 11.7)
    resolved_handle: str | None = None
    resolved_url: str | None = None


@dataclass(frozen=True)
class DiscoveredProduct:
    """One product returned by :meth:`Adapter.discover` (Req 11.1).

    Carries the keyword-match surfaces (``title``, ``product_type``, ``vendor``,
    ``tags``) the core filters on (Req 3.3/3.4) and enough state to build a
    :class:`ProductState` without re-fetching the product endpoint (Req 11.6).
    """

    external_id: str  # Shopify product id as string (Req 14.7)
    title: str
    product_type: str | None
    vendor: str | None
    tags: Sequence[str]
    handle: str
    url: str
    image: str | None
    # Fields sufficient to build a ProductState without re-fetching (Req 11.6):
    variants: Sequence[VariantState] | None


@runtime_checkable
class ListingLike(Protocol):
    """The listing attributes :meth:`Adapter.fetch_state` needs.

    Declared structurally so ``adapters`` need not import the concrete domain
    ``Listing`` dataclass (built in parallel). Any object exposing these attributes —
    including that dataclass — satisfies the contract.
    """

    url: str
    handle: str
    external_id: str | None


@runtime_checkable
class FeedLike(Protocol):
    """The discovery-feed attributes :meth:`Adapter.discover` needs."""

    feed_url: str
    max_page_count: int


class Adapter(Protocol):
    """The single adapter interface the core depends on (Req 11.1, 11.9).

    The core speaks only in terms of this protocol and the value types above; a new
    retailer type is a new module under ``adapters/`` plus a registry entry, never a
    change to the core (Req 11.3).
    """

    adapter_type: str  # registry key (Req 11.3)

    async def fetch_state(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        """Fetch one listing's live state.

        A definite 404 on the product endpoint is RETURNED as
        ``ProductState(existence=Existence.NOT_FOUND, ...)``, never raised (Req 7.1,
        7.2). Only genuine failures raise: :class:`~restockradar.adapters.errors.Blocked`
        / :class:`~restockradar.adapters.errors.SoftBlock`
        (403/429/503-challenge/HTML-where-JSON) and
        :class:`~restockradar.adapters.errors.FetchError` (plain 5xx, timeout, network
        error, malformed JSON). ``cycle_products`` is this cycle's discovery data keyed
        by ``external_id``, used both for feed-reuse (Req 11.6) and to resolve a renamed
        handle (Req 11.8). The returned :class:`ProductState` carries ``resolved_handle``
        / ``resolved_url`` ONLY when the resolved handle DIFFERS from the listing's
        stored handle.
        """
        ...

    async def discover(self, feed: FeedLike) -> list[DiscoveredProduct]:
        """Enumerate products on a discovery feed.

        Raises :class:`~restockradar.adapters.errors.Blocked`,
        :class:`~restockradar.adapters.errors.FetchError`, or
        :class:`~restockradar.adapters.errors.TruncatedFeed`.
        """
        ...


__all__ = [
    "Adapter",
    "DiscoveredProduct",
    "Existence",
    "FeedLike",
    "ListingLike",
    "ProductState",
    "VariantState",
]
