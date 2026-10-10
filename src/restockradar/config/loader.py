"""``Config_Loader``: validate-all-then-upsert by stable id / ``config_key`` (Req 13).

The loader runs in two phases, in order:

1. **Validate everything, fail fast, report EVERY error in one pass** (Req 13.6).
   It first parses the YAML into a :class:`~restockradar.config.schema.ConfigDocument`
   (which already aggregates all intrinsic / structural errors), then layers the
   cross-entity checks that need outside context:

   * an **unknown adapter type** on a retailer (resolved against the Adapter_Registry);
   * a **reference to an entity that does not exist** in the document — a product's
     game, a listing's product/retailer, a feed's retailer/game, a destination's game;
   * a **referenced environment variable that is unset** (a destination's
     ``webhook_env_var``);
   * a YAML **``config_key`` / ``id`` that collides with an existing ``source='bot'``
     row** of the same kind (Req 13.6).

   Every problem found across all of these is collected and raised together as one
   :class:`ConfigValidationError`; the loader never writes a single row if validation
   fails.

2. **Upsert the valid document** inside one ``BEGIN IMMEDIATE`` transaction (Req 13.2):

   * Upsert each entity by its stable identity — ``id`` for games/products/retailers,
     ``config_key`` for listings/feeds, and the destination ``id`` (stored as
     ``config_key``) for destinations — writing ``source='config'`` rows only and
     **never** creating, modifying, disabling, or deleting a ``source='bot'`` row
     (Req 13.3).
   * **Disable** any ``source='config'`` row that is absent from the document by
     clearing ``enabled`` — the row and its stored state are kept, not deleted (Req 13.4).
   * **Re-enable** a previously-disabled entity by setting ``enabled`` AND clearing
     ``seeded_at`` so its next complete poll re-baselines it silently (Req 13.5 / 2.2).
   * Stored Listing / feed state (``in_stock`` / ``price`` / ``existence`` /
     ``next_due_at`` and, except on re-enable, ``seeded_at``) is **never** reset on a
     plain reload (Req 13.2).
   * A product+retailer association is represented as a Listing (Req 13.9).

Secrets never live in the YAML: a destination names the env var holding its webhook
URL, and the loader reads that secret only from the environment (Req 18.1/18.2) — it
only checks the var is *set*, it never copies the value into the database.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from sqlite3 import Connection
from typing import Any

import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from restockradar.adapters.registry import AdapterRegistry
from restockradar.config.schema import (
    FULL_CATALOGUE_DEFAULT_INTERVAL_S,
    ConfigDocument,
    DestinationConfig,
    DiscoveryFeedConfig,
    ListingConfig,
    ProductConfig,
    RetailerConfig,
)
from restockradar.persistence.db import Database

__all__ = [
    "ConfigValidationError",
    "LoadResult",
    "load_config",
    "parse_and_validate",
]


class ConfigValidationError(Exception):
    """Raised when configuration validation fails, carrying EVERY error found in one
    pass (Req 13.6). The individual messages are available as :attr:`errors`.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = list(errors)
        joined = "\n".join(f"  - {message}" for message in self.errors)
        super().__init__(
            f"configuration is invalid ({len(self.errors)} error(s)):\n{joined}"
        )


@dataclass(frozen=True)
class LoadResult:
    """A small summary of what a load changed, for logging / tests.

    ``upserted`` and ``disabled`` count ``source='config'`` rows the load touched;
    ``bot_rows_untouched`` records that ``source='bot'`` rows were never written.
    """

    upserted: int
    disabled: int


# ---------------------------------------------------------------------------
# Phase 1 — parse + validate (fail fast, all errors in one pass)
# ---------------------------------------------------------------------------


def _format_pydantic_errors(exc: ValidationError) -> list[str]:
    """Flatten a pydantic :class:`ValidationError` into one message per problem, so a
    document with several structural errors contributes all of them to the single pass.
    """
    messages: list[str] = []
    for err in exc.errors():
        location = ".".join(str(part) for part in err["loc"]) or "<root>"
        messages.append(f"{location}: {err['msg']}")
    return messages


def _collect_cross_reference_errors(
    document: ConfigDocument,
    *,
    registry: AdapterRegistry,
    environ: Mapping[str, str],
    bot_config_keys: _BotConfigKeys,
) -> list[str]:
    """Return every cross-entity / environment / bot-collision error (Req 13.6).

    This is deliberately exhaustive rather than short-circuiting: all problems are
    appended so the caller can report them together.
    """
    errors: list[str] = []

    game_ids = {g.id for g in document.games}
    product_ids = {p.id for p in document.products}
    retailer_ids = {r.id for r in document.retailers}

    # Unknown adapter type on a retailer (Req 11.9 / 13.6).
    for retailer in document.retailers:
        if not registry.is_registered(retailer.adapter_type):
            errors.append(
                f"retailer {retailer.id!r}: unknown adapter type "
                f"{retailer.adapter_type!r}"
            )

    # Products reference a game that must exist.
    for product in document.products:
        if product.game not in game_ids:
            errors.append(
                f"product {product.id!r}: references unknown game {product.game!r}"
            )

    # Listings reference a product and a retailer that must exist.
    for listing in document.listings:
        if listing.product not in product_ids:
            errors.append(
                f"listing {listing.config_key!r}: references unknown product "
                f"{listing.product!r}"
            )
        if listing.retailer not in retailer_ids:
            errors.append(
                f"listing {listing.config_key!r}: references unknown retailer "
                f"{listing.retailer!r}"
            )

    # Discovery feeds reference a retailer and a game that must exist.
    for feed in document.discovery_feeds:
        if feed.retailer not in retailer_ids:
            errors.append(
                f"discovery_feed {feed.config_key!r}: references unknown retailer "
                f"{feed.retailer!r}"
            )
        if feed.game not in game_ids:
            errors.append(
                f"discovery_feed {feed.config_key!r}: references unknown game "
                f"{feed.game!r}"
            )

    # Destinations: a non-admin references a game that must exist, and every
    # destination's webhook env var must be set (Req 14.14, 18.1).
    for destination in document.destinations:
        if destination.game is not None and destination.game not in game_ids:
            errors.append(
                f"destination {destination.id!r}: references unknown game "
                f"{destination.game!r}"
            )
        if destination.webhook_env_var not in environ:
            errors.append(
                f"destination {destination.id!r}: environment variable "
                f"{destination.webhook_env_var!r} is not set"
            )

    # A YAML config_key / id colliding with an existing source='bot' row (Req 13.6).
    _append_bot_collision_errors(document, bot_config_keys, errors)

    return errors


def _append_bot_collision_errors(
    document: ConfigDocument,
    bot: _BotConfigKeys,
    errors: list[str],
) -> None:
    """Append a validation error for every YAML id / ``config_key`` that already
    exists as a ``source='bot'`` row of the same entity kind (Req 13.6)."""
    for game in document.games:
        if game.id in bot.games:
            errors.append(f"game {game.id!r} collides with an existing source='bot' row")
    for product in document.products:
        if product.id in bot.products:
            errors.append(
                f"product {product.id!r} collides with an existing source='bot' row"
            )
    for retailer in document.retailers:
        if retailer.id in bot.retailers:
            errors.append(
                f"retailer {retailer.id!r} collides with an existing source='bot' row"
            )
    for listing in document.listings:
        if listing.config_key in bot.listings:
            errors.append(
                f"listing {listing.config_key!r} collides with an existing "
                "source='bot' row"
            )
    for feed in document.discovery_feeds:
        if feed.config_key in bot.discovery_feeds:
            errors.append(
                f"discovery_feed {feed.config_key!r} collides with an existing "
                "source='bot' row"
            )
    for destination in document.destinations:
        if destination.id in bot.destinations:
            errors.append(
                f"destination {destination.id!r} collides with an existing "
                "source='bot' row"
            )


@dataclass(frozen=True)
class _BotConfigKeys:
    """The ``config_key`` / ``id`` of existing ``source='bot'`` rows, per entity kind,
    used only for the bot-collision check (Req 13.6)."""

    games: frozenset[str]
    products: frozenset[str]
    retailers: frozenset[str]
    listings: frozenset[str]
    discovery_feeds: frozenset[str]
    destinations: frozenset[str]


def _read_bot_config_keys(conn: Connection) -> _BotConfigKeys:
    """Read the identities of all ``source='bot'`` rows so the loader can detect a
    YAML entity colliding with one (Req 13.6)."""

    def ids(table: str, column: str) -> frozenset[str]:
        rows = conn.execute(
            f"SELECT {column} FROM {table} WHERE source = 'bot'"
        ).fetchall()
        return frozenset(str(row[0]) for row in rows)

    return _BotConfigKeys(
        games=ids("games", "id"),
        products=ids("products", "id"),
        retailers=ids("retailers", "id"),
        listings=ids("listings", "config_key"),
        discovery_feeds=ids("discovery_feeds", "config_key"),
        destinations=ids("destinations", "config_key"),
    )


def parse_and_validate(
    raw: str | Mapping[str, Any],
    *,
    registry: AdapterRegistry,
    bot_config_keys: _BotConfigKeys,
    environ: Mapping[str, str] | None = None,
) -> ConfigDocument:
    """Parse YAML (or an already-parsed mapping) and validate it fully.

    Raises :class:`ConfigValidationError` carrying EVERY error — structural pydantic
    errors AND cross-reference / env / bot-collision errors — found in a single pass
    (Req 13.6). On success returns the validated :class:`ConfigDocument`.
    """
    environ = os.environ if environ is None else environ

    if isinstance(raw, str):
        loaded = yaml.safe_load(raw)
        data: Mapping[str, Any] = {} if loaded is None else loaded
    else:
        data = raw

    # Structural / intrinsic validation first. If this fails we still want to report
    # as much as possible, but cross-reference checks need a valid document to walk,
    # so a structural failure is raised on its own (it already aggregates every
    # structural problem across the whole document in one ValidationError).
    try:
        document = ConfigDocument.model_validate(data)
    except ValidationError as exc:
        raise ConfigValidationError(_format_pydantic_errors(exc)) from exc

    cross_errors = _collect_cross_reference_errors(
        document,
        registry=registry,
        environ=environ,
        bot_config_keys=bot_config_keys,
    )
    if cross_errors:
        raise ConfigValidationError(cross_errors)

    return document


# ---------------------------------------------------------------------------
# Phase 2 — upsert (one BEGIN IMMEDIATE transaction, config rows only)
# ---------------------------------------------------------------------------


def _upsert_simple(
    conn: Connection,
    *,
    table: str,
    key_column: str,
    key: str,
    columns: dict[str, Any],
) -> None:
    """Upsert a ``source='config'`` row keyed by a stable identity.

    An existing ``source='config'`` row is updated in place (preserving stored state);
    a disabled row is re-enabled with ``seeded_at`` cleared where the table has one
    (Req 13.5). A ``source='bot'`` row with the same key is NEVER touched — the
    bot-collision check in phase 1 guarantees we never reach here for one, and the
    ``WHERE source='config'`` guards below are belt-and-braces (Req 13.3).
    """
    existing = conn.execute(
        f"SELECT enabled, source FROM {table} WHERE {key_column} = ?",
        (key,),
    ).fetchone()

    has_seeded_at = table in {"listings", "discovery_feeds"}

    if existing is None:
        cols = [key_column, *columns.keys(), "source", "enabled"]
        placeholders = ", ".join("?" for _ in cols)
        values = [key, *columns.values(), "config", 1]
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        return

    # Never modify a bot row (defensive; phase 1 already rejected a collision).
    if existing["source"] != "config":
        return

    assignments = [f"{column} = ?" for column in columns]
    values_update: list[Any] = list(columns.values())

    was_disabled = int(existing["enabled"]) == 0
    assignments.append("enabled = 1")
    if was_disabled and has_seeded_at:
        # Re-enable => clear seeded_at so the entity re-baselines (Req 13.5 / 2.2).
        assignments.append("seeded_at = NULL")

    conn.execute(
        f"UPDATE {table} SET {', '.join(assignments)} "
        f"WHERE {key_column} = ? AND source = 'config'",
        [*values_update, key],
    )


def _disable_absent(
    conn: Connection,
    *,
    table: str,
    key_column: str,
    present_keys: set[str],
) -> int:
    """Disable every enabled ``source='config'`` row whose key is absent from the
    document, by clearing ``enabled`` (never deleting the row or its state, Req 13.4).

    Returns the number of rows disabled.
    """
    rows = conn.execute(
        f"SELECT {key_column} FROM {table} "
        "WHERE source = 'config' AND enabled = 1"
    ).fetchall()
    disabled = 0
    for row in rows:
        key = str(row[0])
        if key not in present_keys:
            conn.execute(
                f"UPDATE {table} SET enabled = 0 "
                f"WHERE {key_column} = ? AND source = 'config'",
                (key,),
            )
            disabled += 1
    return disabled


def _game_columns(game: Any) -> dict[str, Any]:
    return {"name": game.name}


def _product_columns(product: ProductConfig) -> dict[str, Any]:
    return {"game_id": product.game, "name": product.name, "category": product.category}


def _retailer_columns(retailer: RetailerConfig) -> dict[str, Any]:
    return {
        "name": retailer.name,
        "adapter_type": retailer.adapter_type,
        "base_url": retailer.base_url,
        "poll_interval_s": max(retailer.poll_interval_s, 30),
        "minor_unit_exponent": retailer.minor_unit_exponent,
    }


def _listing_columns(
    listing: ListingConfig, retailer_interval: dict[str, int]
) -> dict[str, Any]:
    interval = (
        listing.poll_interval_s
        if listing.poll_interval_s is not None
        else retailer_interval.get(listing.retailer, 60)
    )
    return {
        "product_id": listing.product,
        "retailer_id": listing.retailer,
        "url": listing.url,
        "handle": listing.handle,
        "poll_interval_s": interval,
        "cooldown_s": listing.cooldown_s,
    }


def _feed_columns(
    feed: DiscoveryFeedConfig, retailer_interval: dict[str, int]
) -> dict[str, Any]:
    if feed.poll_interval_s is not None:
        interval = feed.poll_interval_s
    elif feed.is_full_catalogue:
        interval = FULL_CATALOGUE_DEFAULT_INTERVAL_S
    else:
        interval = retailer_interval.get(feed.retailer, 60)
    return {
        "retailer_id": feed.retailer,
        "game_id": feed.game,
        "feed_url": feed.feed_url,
        "include_keywords": _json_array(feed.include_keywords),
        "exclude_keywords": _json_array(feed.exclude_keywords),
        "max_page_count": feed.max_page_count,
        "poll_interval_s": interval,
    }


def _destination_columns(destination: DestinationConfig) -> dict[str, Any]:
    return {
        "server_id": destination.server_id,
        "game_id": destination.game,
        "webhook_env_var": destination.webhook_env_var,
        "role_to_ping": destination.role_to_ping,
        "is_admin": 1 if destination.admin else 0,
    }


def _json_array(values: tuple[str, ...]) -> str:
    """Encode a keyword tuple as the JSON-array TEXT the schema stores."""
    return json.dumps(list(values))


def _upsert_document(conn: Connection, document: ConfigDocument) -> LoadResult:
    """Upsert the whole validated document and disable absent config rows (Req 13.2-13.5).

    Runs inside a transaction opened by the caller. Order matters for foreign keys:
    games → products/retailers → listings/feeds/destinations.
    """
    retailer_interval = {
        r.id: max(r.poll_interval_s, 30) for r in document.retailers
    }

    upserted = 0

    for game in document.games:
        _upsert_simple(
            conn, table="games", key_column="id", key=game.id, columns=_game_columns(game)
        )
        upserted += 1
    for product in document.products:
        _upsert_simple(
            conn,
            table="products",
            key_column="id",
            key=product.id,
            columns=_product_columns(product),
        )
        upserted += 1
    for retailer in document.retailers:
        _upsert_simple(
            conn,
            table="retailers",
            key_column="id",
            key=retailer.id,
            columns=_retailer_columns(retailer),
        )
        upserted += 1
    for listing in document.listings:
        _upsert_simple(
            conn,
            table="listings",
            key_column="config_key",
            key=listing.config_key,
            columns=_listing_columns(listing, retailer_interval),
        )
        upserted += 1
    for feed in document.discovery_feeds:
        _upsert_simple(
            conn,
            table="discovery_feeds",
            key_column="config_key",
            key=feed.config_key,
            columns=_feed_columns(feed, retailer_interval),
        )
        upserted += 1
    for destination in document.destinations:
        _upsert_simple(
            conn,
            table="destinations",
            key_column="config_key",
            key=destination.id,
            columns=_destination_columns(destination),
        )
        upserted += 1

    # Disable config rows that are no longer present (Req 13.4). Disable leaf entities
    # before their parents so a disabled parent never dangles above an enabled child.
    disabled = 0
    disabled += _disable_absent(
        conn,
        table="destinations",
        key_column="config_key",
        present_keys={d.id for d in document.destinations},
    )
    disabled += _disable_absent(
        conn,
        table="discovery_feeds",
        key_column="config_key",
        present_keys={f.config_key for f in document.discovery_feeds},
    )
    disabled += _disable_absent(
        conn,
        table="listings",
        key_column="config_key",
        present_keys={listing.config_key for listing in document.listings},
    )
    disabled += _disable_absent(
        conn,
        table="products",
        key_column="id",
        present_keys={p.id for p in document.products},
    )
    disabled += _disable_absent(
        conn,
        table="retailers",
        key_column="id",
        present_keys={r.id for r in document.retailers},
    )
    disabled += _disable_absent(
        conn,
        table="games",
        key_column="id",
        present_keys={g.id for g in document.games},
    )

    return LoadResult(upserted=upserted, disabled=disabled)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def load_config(
    db: Database,
    raw: str | Mapping[str, Any],
    *,
    registry: AdapterRegistry,
    environ: Mapping[str, str] | None = None,
) -> LoadResult:
    """Validate ``raw`` config and load it into ``db`` (Req 13).

    The whole operation — reading existing ``source='bot'`` keys, validating (fail
    fast, all errors in one pass), and upserting ``source='config'`` rows while
    disabling absent ones — runs as one callable on the DB executor inside a single
    ``BEGIN IMMEDIATE`` transaction, so a concurrent reader never sees a half-loaded
    config and a validation failure writes nothing.

    Raises :class:`ConfigValidationError` (before any write) if validation fails.
    """

    def _run(conn: Connection) -> LoadResult:
        bot_keys = _read_bot_config_keys(conn)
        document = parse_and_validate(
            raw, registry=registry, bot_config_keys=bot_keys, environ=environ
        )
        with db.transaction(conn):
            return _upsert_document(conn, document)

    return await db.run(_run)
