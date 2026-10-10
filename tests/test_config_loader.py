"""Tasks 7.3 / 7.4 / 7.5 — Config_Loader behaviour (Req 21.13, 21.22, 21.23).

These tests pin the three load-time guarantees of the Config_Loader (Req 13):

* **7.3 (Req 21.13)** — validation **fails fast and reports EVERY error in one
  pass**: an unknown adapter type, a destination referencing an unknown game, an
  unset referenced env var, AND a YAML ``config_key`` colliding with an existing
  ``source='bot'`` row are all surfaced together, and nothing is written.
* **7.4 (Req 21.22 / 13.5 / 2.2)** — **re-enabling** a previously disabled entity
  sets ``enabled`` and CLEARS ``seeded_at`` so it re-baselines (no alert) on its next
  complete poll, while a plain reload of an already-enabled entity leaves its
  ``seeded_at`` and stored state untouched.
* **7.5 (Req 21.23 / 13.3)** — rows with ``source='bot'`` **survive a config reload
  unchanged**: the loader never modifies, disables, or deletes them.

The loader runs against the real DB executor; every test migrates the schema first.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Coroutine, Iterator
from typing import Any

import pytest

from restockradar.adapters.base import Adapter
from restockradar.adapters.registry import AdapterRegistry, RetailerLike
from restockradar.config.loader import ConfigValidationError, load_config
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[Database]:
    """A migrated in-memory database on the real executor, closed on teardown."""
    database = Database(":memory:")
    asyncio.run(migrate(database))
    try:
        yield database
    finally:
        database.close_sync()


@pytest.fixture
def registry() -> AdapterRegistry:
    """A registry with only ``shopify`` registered, matching the MVP (Req 22.2)."""
    reg = AdapterRegistry()

    def _factory(_retailer: RetailerLike) -> Adapter:  # pragma: no cover - never called
        raise AssertionError("the loader only checks registration, never resolves")

    reg.register("shopify", _factory)
    return reg


def _run[T](coro: Coroutine[Any, Any, T]) -> T:
    """Drive a coroutine to completion on a fresh event loop (sync test bodies)."""
    return asyncio.run(coro)


def _valid_env() -> dict[str, str]:
    return {"NCG_WEBHOOK": "https://discord.example/webhook/secret"}


def _minimal_doc() -> dict[str, object]:
    """A small but complete, valid config document used across the enable/bot tests."""
    return {
        "games": [{"id": "pokemon", "name": "Pokémon TCG"}],
        "products": [
            {"id": "booster-bundle", "game": "pokemon", "name": "151 Booster Bundle"}
        ],
        "retailers": [
            {
                "id": "ncg",
                "name": "Northern Card Gaming",
                "adapter_type": "shopify",
                "base_url": "https://www.northerncardgaming.com",
                "poll_interval_s": 60,
            }
        ],
        "listings": [
            {
                "config_key": "ncg-booster-bundle",
                "product": "booster-bundle",
                "retailer": "ncg",
                "url": "https://www.northerncardgaming.com/products/booster-bundle",
                "handle": "booster-bundle",
            }
        ],
        "discovery_feeds": [
            {
                "config_key": "ncg-pokemon",
                "retailer": "ncg",
                "game": "pokemon",
                "feed_url": "/collections/pokemon/products.json",
                "exclude_keywords": ["plush"],
            }
        ],
        "destinations": [
            {
                "id": "ncg-pokemon-dest",
                "server_id": "123",
                "game": "pokemon",
                "webhook_env_var": "NCG_WEBHOOK",
            }
        ],
    }


# ---------------------------------------------------------------------------
# 7.3 — validation fails fast and reports ALL errors in a single pass (Req 21.13)
# ---------------------------------------------------------------------------


def test_validation_reports_all_errors_in_one_pass(
    db: Database, registry: AdapterRegistry
) -> None:
    """A document with four distinct cross-reference problems reports ALL of them in
    one :class:`ConfigValidationError`, and writes nothing (Req 13.6 / 21.13).

    The four seeded problems are: (1) an unknown adapter type, (2) a destination
    referencing an unknown game, (3) an unset referenced env var, and (4) a YAML
    ``config_key`` colliding with an existing ``source='bot'`` row.
    """

    def _seed_bot(conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO games (id, name, source) VALUES ('pokemon', 'P', 'bot')"
        )
        conn.execute(
            "INSERT INTO products (id, game_id, name, source) "
            "VALUES ('bot-product', 'pokemon', 'Bot Product', 'bot')"
        )
        conn.execute(
            "INSERT INTO retailers (id, name, adapter_type, base_url, source) "
            "VALUES ('ncg', 'NCG', 'shopify', 'https://x', 'bot')"
        )
        conn.execute(
            "INSERT INTO listings "
            "(config_key, product_id, retailer_id, url, handle, source) "
            "VALUES ('bot-owned-listing', 'bot-product', 'ncg', 'https://x', 'h', 'bot')"
        )

    _run(db.run(_seed_bot))

    bad_doc: dict[str, object] = {
        "games": [{"id": "pokemon", "name": "Pokémon TCG"}],
        "products": [{"id": "p1", "game": "pokemon", "name": "P1"}],
        "retailers": [
            {
                "id": "mystery",
                "name": "Mystery Store",
                "adapter_type": "wordpress",  # (1) unknown adapter type
                "base_url": "https://mystery.example",
            }
        ],
        "listings": [
            {
                "config_key": "bot-owned-listing",  # (4) collides with source='bot'
                "product": "p1",
                "retailer": "mystery",
                "url": "https://mystery.example/products/x",
                "handle": "x",
            }
        ],
        "destinations": [
            {
                "id": "d1",
                "server_id": "9",
                "game": "ghosts",  # (2) unknown game
                "webhook_env_var": "MISSING_WEBHOOK",  # (3) unset env var
            }
        ],
    }

    with pytest.raises(ConfigValidationError) as excinfo:
        _run(load_config(db, bad_doc, registry=registry, environ={}))

    joined = "\n".join(excinfo.value.errors)
    # All four distinct problems are present together in the single pass.
    assert "unknown adapter type 'wordpress'" in joined
    assert "unknown game 'ghosts'" in joined
    assert "'MISSING_WEBHOOK' is not set" in joined
    assert "'bot-owned-listing' collides with an existing source='bot' row" in joined
    assert len(excinfo.value.errors) >= 4

    # Nothing was written: the bot listing is still the ONLY listing row.
    def _listings(conn: sqlite3.Connection) -> list[tuple[str, str]]:
        return [
            (str(r[0]), str(r[1]))
            for r in conn.execute("SELECT config_key, source FROM listings").fetchall()
        ]

    rows = _run(db.run(_listings))
    assert rows == [("bot-owned-listing", "bot")]


# ---------------------------------------------------------------------------
# 7.4 — re-enable clears seeded_at and re-baselines (Req 21.22 / 13.5 / 2.2)
# ---------------------------------------------------------------------------


def test_reenabling_disabled_entity_clears_seeded_at(
    db: Database, registry: AdapterRegistry
) -> None:
    """Disabling then re-enabling a listing/feed sets ``enabled`` and clears
    ``seeded_at`` so the next complete poll re-baselines silently (Req 13.5 / 2.2),
    while stored state (e.g. ``in_stock``) is preserved."""
    doc = _minimal_doc()

    # 1) Initial load creates the config rows.
    _run(load_config(db, doc, registry=registry, environ=_valid_env()))

    # 2) Simulate a completed baseline poll: set seeded_at + stored state.
    def _mark_seeded(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE listings SET seeded_at = '2024-01-01T00:00:00+00:00', "
            "in_stock = 1, price = '12.99' WHERE config_key = 'ncg-booster-bundle'"
        )
        conn.execute(
            "UPDATE discovery_feeds SET seeded_at = '2024-01-01T00:00:00+00:00' "
            "WHERE config_key = 'ncg-pokemon'"
        )

    _run(db.run(_mark_seeded))

    # 3) Reload WITHOUT the listing + feed => they are disabled (not deleted).
    doc_without = _minimal_doc()
    doc_without["listings"] = []
    doc_without["discovery_feeds"] = []
    _run(load_config(db, doc_without, registry=registry, environ=_valid_env()))

    def _listing_row(
        conn: sqlite3.Connection,
    ) -> tuple[int, str | None, int | None, str | None]:
        row = conn.execute(
            "SELECT enabled, seeded_at, in_stock, price FROM listings "
            "WHERE config_key = 'ncg-booster-bundle'"
        ).fetchone()
        return (row[0], row[1], row[2], row[3])

    enabled, seeded_at, in_stock, price = _run(db.run(_listing_row))
    # Disabled but state and seeded_at preserved (not reset on a plain disable).
    assert enabled == 0
    assert seeded_at == "2024-01-01T00:00:00+00:00"
    assert in_stock == 1
    assert price == "12.99"

    # 4) Reload WITH the listing + feed again => re-enabled and seeded_at CLEARED.
    _run(load_config(db, _minimal_doc(), registry=registry, environ=_valid_env()))

    enabled, seeded_at, in_stock, price = _run(db.run(_listing_row))
    assert enabled == 1
    assert seeded_at is None  # cleared => next complete poll re-baselines (Req 2.2)
    # Last-observed state is otherwise preserved (only seeded_at is cleared).
    assert in_stock == 1
    assert price == "12.99"

    def _feed_seeded(conn: sqlite3.Connection) -> tuple[int, str | None]:
        row = conn.execute(
            "SELECT enabled, seeded_at FROM discovery_feeds "
            "WHERE config_key = 'ncg-pokemon'"
        ).fetchone()
        return (row[0], row[1])

    feed_enabled, feed_seeded = _run(db.run(_feed_seeded))
    assert feed_enabled == 1
    assert feed_seeded is None


def test_plain_reload_preserves_seeded_at(
    db: Database, registry: AdapterRegistry
) -> None:
    """A reload of an entity that was never disabled must NOT reset its ``seeded_at``
    (Req 13.2): only a re-enable clears it."""
    _run(load_config(db, _minimal_doc(), registry=registry, environ=_valid_env()))

    def _mark_seeded(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE listings SET seeded_at = '2024-02-02T00:00:00+00:00' "
            "WHERE config_key = 'ncg-booster-bundle'"
        )

    _run(db.run(_mark_seeded))

    # Reload the identical, still-enabled document.
    _run(load_config(db, _minimal_doc(), registry=registry, environ=_valid_env()))

    def _seeded(conn: sqlite3.Connection) -> str | None:
        value = conn.execute(
            "SELECT seeded_at FROM listings WHERE config_key = 'ncg-booster-bundle'"
        ).fetchone()[0]
        return None if value is None else str(value)

    assert _run(db.run(_seeded)) == "2024-02-02T00:00:00+00:00"


# ---------------------------------------------------------------------------
# 7.5 — source='bot' rows survive a config reload unchanged (Req 21.23 / 13.3)
# ---------------------------------------------------------------------------


def test_bot_rows_survive_reload_unchanged(
    db: Database, registry: AdapterRegistry
) -> None:
    """A ``source='bot'`` destination is never upserted, disabled, or deleted by a
    config reload — its every column is identical before and after (Req 13.3 / 21.23)."""

    def _seed_bot(conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO games (id, name, source, enabled) "
            "VALUES ('bot-game', 'Bot Game', 'bot', 1)"
        )
        conn.execute(
            "INSERT INTO destinations "
            "(config_key, server_id, game_id, webhook_env_var, is_admin, source, enabled) "
            "VALUES ('bot-dest', '777', 'bot-game', 'BOT_WEBHOOK', 0, 'bot', 1)"
        )

    _run(db.run(_seed_bot))

    def _snapshot(conn: sqlite3.Connection) -> dict[str, dict[str, object]]:
        dest = conn.execute(
            "SELECT * FROM destinations WHERE config_key = 'bot-dest'"
        ).fetchone()
        game = conn.execute("SELECT * FROM games WHERE id = 'bot-game'").fetchone()
        return {"dest": dict(dest), "game": dict(game)}

    before = _run(db.run(_snapshot))

    # A full, valid config load that touches only config rows (never 'bot-dest').
    _run(load_config(db, _minimal_doc(), registry=registry, environ=_valid_env()))

    after = _run(db.run(_snapshot))

    assert after == before  # bot rows unchanged, field for field

    # And it is still present and enabled (not disabled as an "absent" entity).
    assert after["dest"]["enabled"] == 1
    assert after["dest"]["source"] == "bot"
