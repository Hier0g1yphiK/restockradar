"""Event dataclasses the detection layer produces (Req 3, 4, 5, 7, 8).

These are the pure value objects the diff functions emit and the fan-out layer (task 17)
later turns into persistence write-sets. They carry NO persistence logic — each is a
frozen, slotted, keyword-only dataclass describing exactly one detected transition, so
they are immutable, hashable where their fields allow, and safe to share across the
event loop and the DB executor thread.

Four alertable product events plus one operational notice:

* :class:`NewProductEvent` — a genuinely-new product discovered on a feed, not already
  seen and not covered by a tracked listing (``EventType.NEW_PRODUCT``, Req 3).
* :class:`RestockEvent` — a listing transitioned out-of-stock -> in-stock
  (``EventType.RESTOCK``, Req 4).
* :class:`PriceChangeEvent` — a listing's price changed vs the last alerted price
  (``EventType.PRICE_CHANGE``, Req 5).
* :class:`PageLiveEvent` — a previously-404 listing's page became live
  (``EventType.PAGE_LIVE``, Req 7).
* :class:`AdminWarning` — an operational notice routed to admin destinations rather than
  a product alert (Req 10, 16); it carries no :class:`EventType`.

Prices are :class:`~decimal.Decimal` (never ``float``) so equality is exact (Req 11.5),
and every timestamp is a timezone-aware UTC :class:`~datetime.datetime` (Req 14.18).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from restockradar.domain.ids import EventType

__all__ = [
    "AdminWarning",
    "NewProductEvent",
    "PageLiveEvent",
    "PriceChangeEvent",
    "RestockEvent",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class NewProductEvent:
    """A genuinely-new product detected on a discovery feed (Req 3.1, 3.5, 3.6).

    Emitted exactly once for a product whose ``(retailer_id, game_id, external_id)`` is
    not already in ``seen_products`` for the feed's retailer+game, that passes the feed's
    keyword filter, and that is not already covered by an enabled tracked listing. The
    identity fields (``retailer_id`` / ``game_id`` / ``external_id`` / ``feed_id``) key
    the event back to the discovery that produced it; the display fields (``title`` /
    ``handle`` / ``url`` / ``image``) are what the alert renders.
    """

    retailer_id: str
    game_id: str
    feed_id: int
    external_id: str
    title: str
    handle: str
    url: str
    image: str | None

    @property
    def event_type(self) -> EventType:
        """The alert event type for a new-product detection (``NEW_PRODUCT``)."""
        return EventType.NEW_PRODUCT


@dataclass(frozen=True, slots=True, kw_only=True)
class RestockEvent:
    """A listing transition from out-of-stock to in-stock (Req 4).

    ``price`` is the price observed at the moment of the restock (``None`` when the
    listing exposes no priced variant). ``detected_at`` is the UTC instant the transition
    was detected.
    """

    listing_id: int
    retailer_id: str
    game_id: str
    price: Decimal | None
    url: str
    title: str | None
    image: str | None
    detected_at: datetime

    @property
    def event_type(self) -> EventType:
        """The alert event type for a restock detection (``RESTOCK``)."""
        return EventType.RESTOCK


@dataclass(frozen=True, slots=True, kw_only=True)
class PriceChangeEvent:
    """A listing price change vs the last alerted price (Req 5).

    Both ``old_price`` and ``new_price`` are exact :class:`~decimal.Decimal` values so
    the comparison that produced this event is reproducible (Req 11.5). ``old_price`` is
    the last alerted price (``None`` if none was ever alerted); ``new_price`` is the newly
    observed price.
    """

    listing_id: int
    retailer_id: str
    game_id: str
    old_price: Decimal | None
    new_price: Decimal
    url: str
    title: str | None
    image: str | None
    detected_at: datetime

    @property
    def event_type(self) -> EventType:
        """The alert event type for a price-change detection (``PRICE_CHANGE``)."""
        return EventType.PRICE_CHANGE


@dataclass(frozen=True, slots=True, kw_only=True)
class PageLiveEvent:
    """A previously not-found listing page became live (Req 7).

    ``in_stock`` / ``price`` carry the state observed when the page came back; ``price``
    is ``None`` when no priced variant is exposed. One-shot: emitted once when stored
    existence flips from ``NOT_FOUND`` to ``EXISTING``.
    """

    listing_id: int
    retailer_id: str
    game_id: str
    in_stock: bool
    price: Decimal | None
    url: str
    title: str | None
    image: str | None
    detected_at: datetime

    @property
    def event_type(self) -> EventType:
        """The alert event type for a page-live detection (``PAGE_LIVE``)."""
        return EventType.PAGE_LIVE


@dataclass(frozen=True, slots=True, kw_only=True)
class AdminWarning:
    """An operational notice routed to admin destinations, not a product alert (Req 10, 16).

    Unlike the product events above, an admin warning has no :class:`EventType`: it is a
    human-facing operational message (a block, a truncated feed, a handle rename, a
    delivery failure) scoped to a ``scope_key`` and tagged with a free-form ``kind``.
    """

    scope_key: str
    kind: str
    message: str
    detected_at: datetime
