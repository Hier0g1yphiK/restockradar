"""Pydantic models for the YAML config (games/products/retailers/listings/
discovery_feeds/destinations) per the design "Config YAML Schema" section.

These models mirror the YAML shape exactly and carry the *intrinsic* validation
rules — the ones that can be decided from a single entity in isolation:

* **Poll-interval floor at 30s** (Req 15.1/15.2): any ``poll_interval_s`` a YAML
  author supplies is validated to be ``>= 30``. A missing interval is left as
  ``None`` here (the floor only constrains a value that was actually given); the
  *default* an entity inherits — 60s from its retailer, or 900s for a
  Full_Catalogue_Feed — is resolved by the loader, which has the retailer in hand.
* **Defaults**: ``minor_unit_exponent`` 2 (Req 11.5), ``max_page_count`` 20
  (Req 12.6/22.13).
* **A Full_Catalogue_Feed must define include keywords** (Req 22.3): a feed whose
  ``feed_url`` is the Shopify ``/products.json`` endpoint enumerates the whole
  catalogue, so it is required to carry at least one include keyword.
* **A destination requires a game unless it is an admin destination** (Req 14.14).
* **A webhook is referenced by env-var NAME only, never the URL** (Req 18.2): the
  model carries ``webhook_env_var`` and nothing resembling a URL; the secret itself
  lives in the environment and is read by the loader (Req 18.1).

Cross-entity validation (an unknown adapter type, a destination referencing an
unknown game, an unset referenced env var, and a YAML ``config_key`` / ``id``
colliding with an existing ``source='bot'`` row) is **not** expressed here — those
checks need the registry, the other entities, the environment, and the database, so
they live in :mod:`restockradar.config.loader`. What this module *does* guarantee is
that every structural / intrinsic error in a single document is reported together in
one :class:`pydantic.ValidationError` pass (fail-fast, all-errors-at-once, Req 13.6),
which the loader then merges with its own cross-reference errors.

Listings, discovery feeds, and destinations carry a stable ``config_key`` the loader
upserts by (destinations expose it as ``id``); games, products, and retailers use
their natural string ``id`` as the ``config_key``.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__ = [
    "FULL_CATALOGUE_DEFAULT_INTERVAL_S",
    "FULL_CATALOGUE_FEED_URL",
    "MIN_POLL_INTERVAL_S",
    "ConfigDocument",
    "DestinationConfig",
    "DiscoveryFeedConfig",
    "GameConfig",
    "ListingConfig",
    "ProductConfig",
    "RetailerConfig",
]

#: Poll-interval floor (Req 15.2): a configured interval below 30s is rejected here;
#: the watcher additionally clamps to this value at runtime.
MIN_POLL_INTERVAL_S = 30

#: Default poll interval for a Full_Catalogue_Feed (Req 12.3 / 22.3): the whole
#: catalogue is polled on a longer 15-minute cadence. Applied by the loader when a
#: full-catalogue feed does not set its own interval.
FULL_CATALOGUE_DEFAULT_INTERVAL_S = 900

#: The Shopify full-catalogue endpoint path. A feed whose ``feed_url`` is this path
#: is a Full_Catalogue_Feed and MUST define include keywords (Req 22.3).
FULL_CATALOGUE_FEED_URL = "/products.json"


#: A poll interval supplied in YAML must honour the 30s floor (Req 15.2). The field is
#: optional on every entity: ``None`` means "inherit the default" and only a supplied
#: value is floor-checked.
PollIntervalField = Annotated[int | None, Field(default=None, ge=MIN_POLL_INTERVAL_S)]


class _StrictModel(BaseModel):
    """Base for every config model: reject unknown keys so a typo'd field in the YAML
    (e.g. ``poll_interval`` instead of ``poll_interval_s``) is a validation error
    surfaced in the same fail-fast pass rather than silently ignored.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class GameConfig(_StrictModel):
    """A ``games:`` entry. Its ``id`` is also its ``config_key`` (Req 14.1)."""

    id: str = Field(min_length=1)
    name: str = Field(min_length=1)


class ProductConfig(_StrictModel):
    """A ``products:`` entry; ``game`` references a :class:`GameConfig` id (Req 14.2)."""

    id: str = Field(min_length=1)
    game: str = Field(min_length=1)
    name: str = Field(min_length=1)
    #: Category or set, e.g. "Scarlet & Violet"; optional (Req 14.2).
    category: str | None = None


class RetailerConfig(_StrictModel):
    """A ``retailers:`` entry (Req 14.3).

    ``poll_interval_s`` defaults to 60 and is the interval inherited by this
    retailer's feeds and listings that do not set their own; it still honours the
    30s floor. ``minor_unit_exponent`` defaults to 2 (Req 11.5).
    """

    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    #: Resolved via the Adapter_Registry; the loader fails fast on an unknown type (Req 11.3/11.9).
    adapter_type: str = Field(min_length=1)
    base_url: str = Field(min_length=1)
    poll_interval_s: int = Field(default=60, ge=MIN_POLL_INTERVAL_S)
    minor_unit_exponent: int = Field(default=2, ge=0)


class ListingConfig(_StrictModel):
    """A ``listings:`` entry — a product+retailer association (Req 13.9).

    ``config_key`` is the stable upsert / bot-collision identity (Req 13.2/13.6).
    ``poll_interval_s`` and ``cooldown_s`` are optional overrides; a missing
    ``poll_interval_s`` inherits the retailer's (resolved by the loader).
    """

    config_key: str = Field(min_length=1)
    product: str = Field(min_length=1)
    retailer: str = Field(min_length=1)
    url: str = Field(min_length=1)
    handle: str = Field(min_length=1)
    poll_interval_s: PollIntervalField = None
    #: Per-subject cooldown override in seconds; ``None`` => the 300s default (Req 9.7).
    cooldown_s: int | None = Field(default=None, ge=0)


class DiscoveryFeedConfig(_StrictModel):
    """A ``discovery_feeds:`` entry (Req 14.5).

    ``config_key`` is the stable upsert / bot-collision identity. ``max_page_count``
    defaults to 20 (Req 12.6/22.13). A Full_Catalogue_Feed (``feed_url`` =
    ``/products.json``) must define include keywords (Req 22.3) — enforced here — and
    inherits a longer 900s default interval, applied by the loader.
    """

    config_key: str = Field(min_length=1)
    retailer: str = Field(min_length=1)
    game: str = Field(min_length=1)
    #: Relative path, e.g. ``/collections/pokemon/products.json``.
    feed_url: str = Field(min_length=1)
    include_keywords: tuple[str, ...] = ()
    exclude_keywords: tuple[str, ...] = ()
    max_page_count: int = Field(default=20, ge=1)
    poll_interval_s: PollIntervalField = None

    @property
    def is_full_catalogue(self) -> bool:
        """Whether this feed is the Shopify full-catalogue ``/products.json`` feed."""
        return self.feed_url == FULL_CATALOGUE_FEED_URL

    @model_validator(mode="after")
    def _full_catalogue_requires_include(self) -> DiscoveryFeedConfig:
        """A Full_Catalogue_Feed enumerates everything, so it MUST narrow with at
        least one include keyword (Req 22.3)."""
        if self.is_full_catalogue and not self.include_keywords:
            raise ValueError(
                f"full-catalogue feed {self.config_key!r} ({FULL_CATALOGUE_FEED_URL}) "
                "must define at least one include keyword"
            )
        return self


class DestinationConfig(_StrictModel):
    """A ``destinations:`` entry (Req 14.14).

    The destination's ``id`` doubles as its ``config_key`` for upsert / bot-collision
    (Req 13.6). ``webhook_env_var`` names the environment variable holding the webhook
    URL — the URL itself is NEVER stored in config (Req 18.2). A non-admin destination
    requires a ``game``; an admin destination may omit it (Req 14.14).
    """

    id: str = Field(min_length=1)
    server_id: str = Field(min_length=1)
    #: Required unless :attr:`admin` is true (Req 14.14).
    game: str | None = None
    #: Name of the env var holding the webhook URL — never the URL itself (Req 18.2).
    webhook_env_var: str = Field(min_length=1)
    #: Optional role id to ping (Req 8.6).
    role_to_ping: str | None = None
    #: Admin_Destination_Flag (Req 14.14).
    admin: bool = False

    @model_validator(mode="after")
    def _non_admin_requires_game(self) -> DestinationConfig:
        """Mirror the schema's ``is_admin = 1 OR game_id IS NOT NULL`` invariant so a
        non-admin destination without a game fails validation in the same pass (Req 14.14).
        """
        if not self.admin and self.game is None:
            raise ValueError(
                f"non-admin destination {self.id!r} requires a game "
                "(set `admin: true` for an admin destination)"
            )
        return self


class ConfigDocument(_StrictModel):
    """The whole YAML document: the six configured-entity collections (Req 13.1).

    Validating a :class:`ConfigDocument` runs every nested model's intrinsic rules and
    aggregates all failures into one :class:`pydantic.ValidationError`, so a document
    with several problems reports them together in a single pass (Req 13.6). The loader
    layers its cross-entity checks on top before upserting.
    """

    games: tuple[GameConfig, ...] = ()
    products: tuple[ProductConfig, ...] = ()
    retailers: tuple[RetailerConfig, ...] = ()
    listings: tuple[ListingConfig, ...] = ()
    discovery_feeds: tuple[DiscoveryFeedConfig, ...] = ()
    destinations: tuple[DestinationConfig, ...] = ()
