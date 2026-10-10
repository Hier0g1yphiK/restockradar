"""Frozen, fully-typed domain dataclasses: ``Game``, ``Product``, ``Retailer``,
``Listing``, ``DiscoveryFeed``, ``Destination`` (Req 11.2 shared types).

These are the in-memory representation of the configurable catalogue rows (the
``games`` / ``products`` / ``retailers`` / ``listings`` / ``discovery_feeds`` /
``destinations`` tables in the design DDL). They are the shape the core passes
around *after* the repository has decoded a row — so the richer Python types live
here, while the TEXT/INTEGER encodings used on disk stay behind
``persistence/types.py``:

* **Prices are** :class:`~decimal.Decimal` (never ``float``), so equality is exact
  and the minor-unit vs decimal-string paths compare identically (Req 11.5, 14.17).
* **Timestamps are timezone-aware** :class:`~datetime.datetime` **in UTC** — the
  repository rejects naive datetimes on the way to storage (Req 14.18).
* A column that is ``NULL`` until an entity is seeded (``in_stock`` / ``price`` /
  ``existence`` / ``seeded_at`` / ``next_due_at``) is modelled as ``T | None``, so
  "unseeded" is representable and distinct from a real value (Req 2.3).

Every dataclass is ``frozen=True`` (immutable — safe to share across the event loop,
the DB executor thread, and the watchdog thread) and ``slots=True`` (no per-instance
``__dict__``). ``keyword_only=True`` is used on the wider rows so a long argument
list cannot be mis-ordered at a call site. Keyword lists on each entity mirror the
include/exclude filters a ``DiscoveryFeed`` carries; they are stored as tuples rather
than lists so the frozen instance stays hashable and cannot be mutated in place.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

__all__ = [
    "Destination",
    "DiscoveryFeed",
    "Existence",
    "Game",
    "HealthStatus",
    "Listing",
    "Product",
    "Retailer",
    "Source",
]


class Source(StrEnum):
    """Row ownership: ``config`` rows are owned by the YAML, ``bot`` rows by the
    later Discord bot process and are never modified by the Config_Loader (Req 13.3).
    """

    CONFIG = "config"
    BOT = "bot"


class HealthStatus(StrEnum):
    """A Destination's delivery-health condition (Req 14.14, 10)."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class Existence(StrEnum):
    """A Listing's stored page-existence, modelling the ``listings.existence`` column
    (Req 7.3). ``UNSEEDED`` is the ``NULL`` state before the first complete poll and
    is represented as ``None`` on :class:`Listing`, so this enum only ever carries the
    two concrete values once a listing has been observed.
    """

    EXISTING = "existing"
    NOT_FOUND = "not_found"


# ---------------------------------------------------------------------------
# Configurable catalogue
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Game:
    """A trading-card product line being monitored (the ``games`` row, Req 14.1)."""

    id: str
    name: str
    source: Source = Source.CONFIG
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Product:
    """A trackable item belonging to a :class:`Game` (the ``products`` row, Req 14.2)."""

    id: str
    game_id: str
    name: str
    #: Category or set (e.g. "Scarlet & Violet"); ``NULL`` in the schema.
    category: str | None = None
    source: Source = Source.CONFIG
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Retailer:
    """A monitored seller (the ``retailers`` row, Req 14.3).

    ``poll_interval_s`` is the **default interval inherited** by this retailer's
    feeds and listings that do not set their own; it is not itself a retailer-level
    due time. ``minor_unit_exponent`` converts Shopify minor units to a major-unit
    :class:`~decimal.Decimal` (default 2 => 100 minor units per major, Req 11.5).
    The retailers table deliberately carries no ``next_due_at``/``backoff_until`` —
    retailer-wide deferral lives entirely in the ``backoff_state`` row keyed
    ``'retailer:{id}'`` (Req 1.5/16.5).
    """

    id: str
    name: str
    #: Adapter-type string resolved by the Adapter_Registry, e.g. ``"shopify"`` (Req 11.3).
    adapter_type: str
    base_url: str
    poll_interval_s: int = 60
    minor_unit_exponent: int = 2
    source: Source = Source.CONFIG
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Listing:
    """The association of a :class:`Product` with a :class:`Retailer` (the
    ``listings`` row, Req 14.4).

    The last-known-state fields are the *last observed* values (Req 2.3): ``in_stock``
    / ``price`` / ``existence`` are ``None`` until the listing is seeded, and
    ``price`` is the last OBSERVED price — **not** the last_alerted_price (that lives
    in the price_change ``alert_state`` row). ``external_id`` is the Shopify product
    id as a string, learned on first fetch and used to key stored state (Req 11.7).
    ``cooldown_s`` is a per-subject override; ``None`` means the 300s default (Req 9.7).
    """

    id: int
    #: Stable upsert / bot-collision identity set by the loader (Req 13.2/13.6).
    config_key: str
    product_id: str
    retailer_id: str
    url: str
    handle: str
    external_id: str | None = None
    #: Last OBSERVED stock; ``None`` until seeded (Req 2.3).
    in_stock: bool | None = None
    #: Last OBSERVED price as an exact Decimal; ``None`` until seeded (not last_alerted_price).
    price: Decimal | None = None
    #: Stored page existence; ``None`` while unseeded (Req 7.3).
    existence: Existence | None = None
    #: Consecutive 404 count; existence flips to NOT_FOUND only after two (Req 7.6).
    consecutive_404: int = 0
    poll_interval_s: int = 60
    #: Per-subject cooldown override in seconds; ``None`` => default 300 (Req 9.7).
    cooldown_s: int | None = None
    next_due_at: datetime | None = None
    #: Set only on a complete poll; ``None`` while unseeded (Req 2.1/2.4).
    seeded_at: datetime | None = None
    source: Source = Source.CONFIG
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscoveryFeed:
    """A retailer-and-game-scoped collection/sitemap/search URL used to detect new,
    not-yet-tracked products (the ``discovery_feeds`` row, Req 14.5).

    ``include_keywords`` / ``exclude_keywords`` are the Unicode-normalised,
    accent-/case-insensitive substring filters (Req 3.3/3.4); exclude takes
    precedence over include. They are stored as tuples so the frozen instance stays
    immutable and hashable. ``max_page_count`` caps a discovery crawl before it is
    treated as truncated (default 20, Req 12.6). A Full_Catalogue_Feed (the Shopify
    ``/products.json`` endpoint) typically uses the longer 900s interval and must
    define include keywords — that validation lives in the config layer, not here.
    """

    id: int
    #: Stable upsert / bot-collision identity set by the loader (Req 13.2/13.6).
    config_key: str
    retailer_id: str
    game_id: str
    #: Relative path, e.g. ``'/collections/pokemon/products.json'``.
    feed_url: str
    include_keywords: tuple[str, ...] = ()
    exclude_keywords: tuple[str, ...] = ()
    max_page_count: int = 20
    poll_interval_s: int = 60
    next_due_at: datetime | None = None
    #: Set only on a complete poll; ``None`` while unseeded (Req 2.1/2.4).
    seeded_at: datetime | None = None
    source: Source = Source.CONFIG
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Destination:
    """A delivery target (the ``destinations`` row, Req 14.14).

    The webhook URL is **never** stored — only ``webhook_env_var``, the name of the
    environment variable that holds it (Req 18.2). A non-admin destination requires a
    ``game_id``; an :attr:`is_admin` destination may omit it (it receives operational
    warnings rather than product alerts, Req 14.14). The Destination_Health fields
    (``health_status`` / ``unhealthy_since`` / ``last_error`` / probe timestamps) let
    the Notifier skip and later recover an unhealthy destination (Req 10).
    """

    id: int
    #: The YAML destination ``id``; upsert / bot-collision identity (Req 13.6).
    config_key: str
    #: Discord server identifier.
    server_id: str
    #: Name of the env var holding the webhook URL — never the URL itself (Req 18.2).
    webhook_env_var: str
    #: ``None`` only permitted when :attr:`is_admin` is true (Req 14.14).
    game_id: str | None = None
    #: Optional role id to ping (Req 8.6).
    role_to_ping: str | None = None
    #: Admin_Destination_Flag: receives operational warnings, not product alerts (Req 14.14).
    is_admin: bool = False
    # ---- Destination_Health (Req 14.14, 10) ----
    health_status: HealthStatus = HealthStatus.HEALTHY
    unhealthy_since: datetime | None = None
    last_error: str | None = None
    #: When to next GET-probe an unhealthy destination (Req 10.11).
    next_probe_at: datetime | None = None
    #: When it was last probed (Req 10.11).
    last_probe_at: datetime | None = None
    source: Source = Source.CONFIG
    enabled: bool = True

    def __post_init__(self) -> None:
        """Enforce the schema's ``is_admin = 1 OR game_id IS NOT NULL`` invariant so a
        non-admin destination can never exist without a game (Req 14.14).
        """
        if not self.is_admin and self.game_id is None:
            raise ValueError(
                "a non-admin Destination requires a game_id "
                f"(destination config_key={self.config_key!r})"
            )
