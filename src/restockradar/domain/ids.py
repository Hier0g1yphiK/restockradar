"""``subject_key`` / ``scope_key`` builders and the event-type enum (Req 14.10,
14.12, 11.2 shared types).

Several persistence tables are keyed by a single **NON-NULL TEXT** column rather
than by a tuple that would include a nullable id (SQLite treats NULLs as distinct
in a PRIMARY KEY, which would silently break uniqueness for subjects/scopes that
share a NULL id — see the design's "Note on single-column keys"). This module is
the one place those keys are *built*, so the string format lives in exactly one
spot and every writer agrees on it.

Two key families:

* ``subject_key`` — identifies the Subject a cooldown / ``alert_state`` record is
  keyed to. A listing subject is ``'listing:{id}'`` and is the only form ever
  written to ``alert_state`` (which holds restock / price_change rows only). The
  new-product subject form ``'feed:{feed_id}:ext:{external_id}'`` is the logical
  identity used in history/outbox informational columns; new-product dedupe itself
  lives in ``seen_products`` (Req 9.6), so this form never reaches ``alert_state``.

* ``scope_key`` — the coalescing bucket for ``backoff_state`` (PK), the ``incidents``
  open-lookup, and admin rate-limiting. One of ``'retailer:{id}'``, ``'feed:{id}'``,
  ``'listing:{id}'``, ``'path:{path}'``, ``'destination:{id}'``, or ``'system'``.

The builders are **total** (every call returns a well-formed key — the only error is
a programmer passing an empty ``external_id`` / ``path``, which cannot form an
unambiguous key) and **unambiguous**: each key carries its kind as a leading tag, so
``subject_from_key`` / ``scope_from_key`` can parse a stored key back to its parts
without a side table. The round-trip is covered by the key-builder unit tests.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "EventType",
    "ScopeKey",
    "ScopeKind",
    "SubjectKey",
    "SubjectKind",
    "feed_product_subject_key",
    "listing_subject_key",
    "parse_scope_key",
    "parse_subject_key",
    "scope_destination_key",
    "scope_feed_key",
    "scope_listing_key",
    "scope_path_key",
    "scope_retailer_key",
    "scope_system_key",
]


# ---------------------------------------------------------------------------
# Event-type enum
# ---------------------------------------------------------------------------


class EventType(StrEnum):
    """The alertable State_Transition kinds (Req 3, 4, 5, 7).

    ``RESTOCK`` and ``PRICE_CHANGE`` are the only cooldown-bearing events and the
    only ones that ever write an ``alert_state`` row (Req 9.6). ``NEW_PRODUCT`` and
    ``PAGE_LIVE`` are one-shot: they dedupe via ``seen_products`` and stored
    existence respectively and carry no cooldown state.

    The ``str`` value of each member is the exact token stored in the
    ``alert_state.event_type`` / ``alerts_outbox.event_type`` columns, so the enum
    doubles as the serialised form (``EventType.RESTOCK == "restock"``).
    """

    RESTOCK = "restock"
    PRICE_CHANGE = "price_change"
    NEW_PRODUCT = "new_product"
    PAGE_LIVE = "page_live"

    @property
    def carries_cooldown(self) -> bool:
        """Whether this event type keeps an ``alert_state`` cooldown row (Req 9.6)."""
        return self in _COOLDOWN_EVENT_TYPES


#: The event types that own an ``alert_state`` row (the cooldown authority, Req 9.6).
_COOLDOWN_EVENT_TYPES: frozenset[EventType] = frozenset(
    {EventType.RESTOCK, EventType.PRICE_CHANGE}
)


# ---------------------------------------------------------------------------
# Subject keys
# ---------------------------------------------------------------------------


class SubjectKind(StrEnum):
    """The kind tag leading a ``subject_key`` string."""

    LISTING = "listing"
    FEED_PRODUCT = "feed_product"


#: A built ``subject_key`` string (alias kept for readable signatures).
SubjectKey = str


def listing_subject_key(listing_id: int) -> SubjectKey:
    """Build the ``'listing:{id}'`` subject key for a listing (Req 14.10).

    This is the only ``subject_key`` form ever written to ``alert_state``; its PK is
    ``(subject_key, event_type)`` and every row there is restock / price_change for a
    listing.
    """
    return f"listing:{listing_id}"


def feed_product_subject_key(feed_id: int, external_id: str) -> SubjectKey:
    """Build the ``'feed:{feed_id}:ext:{external_id}'`` new-product subject key.

    The ``external_id`` is adapter-defined (the Shopify product id as a string) and
    is embedded verbatim after the ``:ext:`` separator; because it can itself contain
    colons, the parser splits on the fixed ``:ext:`` marker rather than on ``:``, so
    the key stays unambiguous. An empty ``external_id`` cannot form a usable key and
    is rejected.
    """
    if not external_id:
        raise ValueError("external_id must be a non-empty string to build a subject key")
    return f"feed:{feed_id}:ext:{external_id}"


def parse_subject_key(key: SubjectKey) -> tuple[SubjectKind, int, str | None]:
    """Parse a ``subject_key`` back into ``(kind, id, external_id)``.

    * ``'listing:{id}'`` -> ``(SubjectKind.LISTING, id, None)``
    * ``'feed:{feed_id}:ext:{external_id}'`` ->
      ``(SubjectKind.FEED_PRODUCT, feed_id, external_id)``

    Raises :class:`ValueError` for any string that is not one of these two exact
    forms, so a corrupt stored key surfaces loudly rather than being misread.
    """
    listing_prefix = "listing:"
    if key.startswith(listing_prefix):
        return (SubjectKind.LISTING, _parse_int_id(key[len(listing_prefix) :], key), None)

    feed_prefix = "feed:"
    marker = ":ext:"
    if key.startswith(feed_prefix) and marker in key:
        middle = key[len(feed_prefix) :]
        raw_feed_id, external_id = middle.split(marker, 1)
        if not external_id:
            raise ValueError(f"malformed feed-product subject key (empty external_id): {key!r}")
        return (SubjectKind.FEED_PRODUCT, _parse_int_id(raw_feed_id, key), external_id)

    raise ValueError(f"unrecognised subject_key format: {key!r}")


# ---------------------------------------------------------------------------
# Scope keys
# ---------------------------------------------------------------------------


class ScopeKind(StrEnum):
    """The kind tag leading a ``scope_key`` string."""

    RETAILER = "retailer"
    FEED = "feed"
    LISTING = "listing"
    PATH = "path"
    DESTINATION = "destination"
    SYSTEM = "system"


#: A built ``scope_key`` string (alias kept for readable signatures).
ScopeKey = str

#: The literal scope key for the system scope (no id) — a constant, since it is total.
_SYSTEM_SCOPE_KEY: ScopeKey = "system"


def scope_retailer_key(retailer_id: str) -> ScopeKey:
    """Build the ``'retailer:{id}'`` scope key (retailer-wide backoff / warnings)."""
    if not retailer_id:
        raise ValueError("retailer_id must be a non-empty string to build a scope key")
    return f"retailer:{retailer_id}"


def scope_feed_key(feed_id: int) -> ScopeKey:
    """Build the ``'feed:{id}'`` scope key (per-feed backoff / incidents)."""
    return f"feed:{feed_id}"


def scope_listing_key(listing_id: int) -> ScopeKey:
    """Build the ``'listing:{id}'`` scope key (per-listing backoff / incidents)."""
    return f"listing:{listing_id}"


def scope_path_key(path: str) -> ScopeKey:
    """Build the ``'path:{path}'`` scope key (robots-disallowed incidents).

    The ``path`` is embedded verbatim (it may itself contain ``:`` or ``/``); the
    parser keeps everything after the first ``'path:'`` prefix, so a path with
    colons round-trips intact. An empty path cannot form a usable key.
    """
    if not path:
        raise ValueError("path must be a non-empty string to build a scope key")
    return f"path:{path}"


def scope_destination_key(destination_id: int) -> ScopeKey:
    """Build the ``'destination:{id}'`` scope key (per-destination admin warnings)."""
    return f"destination:{destination_id}"


def scope_system_key() -> ScopeKey:
    """Return the ``'system'`` scope key (warnings tied to no retailer/destination)."""
    return _SYSTEM_SCOPE_KEY


def parse_scope_key(key: ScopeKey) -> tuple[ScopeKind, str | None]:
    """Parse a ``scope_key`` back into ``(kind, raw_id)``.

    The second element is the raw id *as text* — a retailer id is already a string,
    while a feed/listing/destination id is the decimal text of an integer; callers
    that need an ``int`` convert it themselves. For ``'system'`` the id is ``None``.
    ``'path:...'`` keeps the full remainder (which may contain ``:``) as its id.

    Raises :class:`ValueError` for an unrecognised format.
    """
    if key == _SYSTEM_SCOPE_KEY:
        return (ScopeKind.SYSTEM, None)

    kind, sep, rest = key.partition(":")
    if not sep or not rest:
        raise ValueError(f"unrecognised scope_key format: {key!r}")

    match kind:
        case ScopeKind.RETAILER.value:
            return (ScopeKind.RETAILER, rest)
        case ScopeKind.FEED.value:
            return (ScopeKind.FEED, rest)
        case ScopeKind.LISTING.value:
            return (ScopeKind.LISTING, rest)
        case ScopeKind.DESTINATION.value:
            return (ScopeKind.DESTINATION, rest)
        case ScopeKind.PATH.value:
            return (ScopeKind.PATH, rest)
        case _:
            raise ValueError(f"unrecognised scope_key format: {key!r}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_int_id(raw: str, original_key: str) -> int:
    """Parse the integer id portion of a key, erroring with the full key context."""
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"malformed integer id in key {original_key!r}: {raw!r}") from exc
