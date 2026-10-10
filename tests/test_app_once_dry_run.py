"""Task 13.3 — M1 end-to-end dry-run ``--once`` test (Req 20.1, 20.4, 2.1, 21.4).

An OFFLINE end-to-end test that the M1 baseline cycle (``python -m restockradar --once
--dry-run``) seeds state and emits ZERO alerts, driven entirely by the operator-captured
NCG fixtures under ``fixtures/ncg/`` — never the live network. The suite-wide no-network
guard in ``conftest.py`` (Req 21.4) makes a real socket attempt a hard failure, so the
test proves the ``--once --dry-run`` path works purely from saved bytes.

How it stays offline and faithful to the real cycle:

* The real :class:`~restockradar.app.run_once_dry_run` cycle is exercised against a real,
  migrated temp-file :class:`~restockradar.persistence.db.Database` with the ACTUAL config
  loaded via :func:`~restockradar.config.loader.load_config`, so the schema (FKs, CHECKs,
  the single-column ``alert_state`` key) and the real Config_Loader both run.
* The real :class:`~restockradar.adapters.shopify.ShopifyAdapter` runs through the real
  :class:`~restockradar.http.client.HttpClient`, whose transport is an
  :class:`httpx.MockTransport` that serves the saved ``fixtures/ncg/*.json`` for the two
  feed URLs and the three listing ``.js`` URLs, plus an allow-all ``robots.txt``.

Assertions:

* **After run 1** — both feeds and all three listings carry ``seeded_at``;
  ``seen_products`` is populated (one row per discovered product across both feeds);
  BOTH ``alert_state`` rows exist per listing; and ``alerts_outbox`` / ``alert_deliveries``
  are EMPTY (a baseline run emits zero alerts, Req 2.1).
* **After a SECOND run against the SAME temp DB** — every enabled subject is RE-POLLED
  (Req 20.4: ``--once`` ignores ``next_due_at`` / backoff and re-polls enabled entities),
  so the per-subject results now carry the freshly fetched data (feeds report
  ``products_found`` equal to the fixture product counts, listings report their fetched
  existence) — yet NOTHING new is seeded (0 feeds / 0 listings seeded this run),
  ``seen_products`` is unchanged, and ``alerts_outbox`` / ``alert_deliveries`` stay empty.
* **Regression guard (Req 20.4 + M1 zero-alert)** —
  :func:`test_two_runs_leave_identical_state_and_zero_alerts` snapshots the full persisted
  state (listings ``in_stock`` / ``price`` / ``existence`` / ``seeded_at``, every
  ``alert_state`` row, every ``seen_products`` row) after run 1 and after run 2 and asserts
  they are IDENTICAL, with zero alerts after BOTH runs — proving a re-poll of an
  already-seeded subject never mutates stored state and never alerts in M1.

The summary is also checked to NEVER contain a webhook URL (dry-run, Req 18.2/20.4).
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from restockradar.adapters.registry import AdapterRegistry
from restockradar.app import (
    load_config_from_path,
    register_shopify,
    run_once_dry_run,
    write_summary,
)
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate

# ---------------------------------------------------------------------------
# Paths + fixture loading
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_DIR = _REPO_ROOT / "fixtures" / "ncg"
_CONFIG_PATH = _REPO_ROOT / "config" / "config.yaml"

ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"

#: config.yaml's two feed URLs mapped to their captured first-page fixtures.
_FEED_FIXTURES = {
    "/collections/pokemon/products.json": "collections_pokemon_products_page1.json",
    "/collections/pre-orders-1/products.json": "collections_pre_orders_1_products_page1.json",
}

#: config.yaml's three listing handles.
_LISTING_HANDLES = (
    "pokemon-tcg-team-rocket-tin-pre-order",
    "pokemon-destined-rivals-booster-box",
    "ncg-pokeween-18-october",
)

#: The one listing handle with a saved ``.js`` fixture (served verbatim).
_POKEWEEN_JS = "products_ncg-pokeween-18-october.js.json"


def _fixture_bytes(name: str) -> bytes:
    return (_FIXTURE_DIR / name).read_bytes()


def _fixture_json(name: str) -> dict[str, Any]:
    parsed = json.loads(_fixture_bytes(name))
    assert isinstance(parsed, dict)
    return parsed


def _json_response(payload: object, *, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )


def _feed_entry_for_handle(handle: str) -> dict[str, Any]:
    """Return the feed product entry for ``handle`` from whichever fixture contains it."""
    for fixture in _FEED_FIXTURES.values():
        body = _fixture_json(fixture)
        for product in body["products"]:
            if product["handle"] == handle:
                entry: dict[str, Any] = product
                return entry
    raise AssertionError(f"no feed entry found for handle {handle!r}")


def _synthetic_js_from_feed(handle: str) -> dict[str, Any]:
    """Build a minimal valid ``.js`` body for a handle lacking a saved ``.js`` fixture.

    The ``.js`` endpoint carries INTEGER minor-unit prices (unlike the feed's decimal
    strings), so each feed variant's decimal price is converted to minor units at the
    retailer's ``minor_unit_exponent`` of 2. Only the fields the adapter reads (id,
    handle, title, variants[id/available/price], featured_image) are included.
    """
    entry = _feed_entry_for_handle(handle)
    variants = [
        {
            "id": variant["id"],
            "available": variant.get("available", False),
            "price": round(float(variant["price"]) * 100),  # major -> minor units
        }
        for variant in entry["variants"]
    ]
    return {
        "id": entry["id"],
        "handle": entry["handle"],
        "title": entry["title"],
        "featured_image": None,
        "variants": variants,
    }


def _handler(request: httpx.Request) -> httpx.Response:
    """Serve robots.txt (allow-all), the feed JSON (page 1 real, page 2 empty), and
    the listing ``.js`` bodies — all from saved fixtures / synthesised fixture data.
    """
    path = request.url.path
    if path == "/robots.txt":
        return httpx.Response(200, text=ROBOTS_ALLOW_ALL)

    if path in _FEED_FIXTURES:
        page = int(request.url.params.get("page", "1"))
        if page == 1:
            return httpx.Response(
                200,
                content=_fixture_bytes(_FEED_FIXTURES[path]),
                headers={"content-type": "application/json"},
            )
        return _json_response({"products": []})  # empty follow-up page => stop crawl

    if path.endswith(".js"):
        handle = path.removeprefix("/products/").removesuffix(".js")
        if handle == "ncg-pokeween-18-october":
            return httpx.Response(
                200,
                content=_fixture_bytes(_POKEWEEN_JS),
                headers={"content-type": "application/json"},
            )
        return _json_response(_synthetic_js_from_feed(handle))

    raise AssertionError(f"unexpected request path {path!r}")


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db(tmp_path: Path) -> Iterator[Database]:
    """A real file-backed, migrated database in a temp dir (survives close/reopen)."""
    db = Database(str(tmp_path / "restockradar.db"))
    try:
        yield db
    finally:
        db.close_sync()


def _build_http() -> HttpClient:
    """An HttpClient whose transport serves only the saved NCG fixtures (offline)."""
    transport = httpx.MockTransport(_handler)
    return HttpClient(
        heartbeat=HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=transport,
    )


#: The one listing whose ``.js`` fetch is forced to fail in the error-isolation test.
_FAILING_HANDLE = "pokemon-destined-rivals-booster-box"


def _failing_handler(request: httpx.Request) -> httpx.Response:
    """Like :func:`_handler`, but one listing's ``.js`` returns a 500 (-> FetchError).

    Everything else — robots.txt, both feeds, and the other two listings — is served
    exactly as the happy-path handler does, so only the single failing subject differs.
    This proves one subject's per-adapter failure does NOT abort the rest of the cycle.
    """
    path = request.url.path
    if path.endswith(".js"):
        handle = path.removeprefix("/products/").removesuffix(".js")
        if handle == _FAILING_HANDLE:
            # A plain 5xx: the HttpClient classifies this as a FetchError (Req 16.4).
            return httpx.Response(500, text="upstream exploded")
    return _handler(request)


def _build_failing_http() -> HttpClient:
    """An offline HttpClient whose transport fails exactly one listing's ``.js`` fetch."""
    transport = httpx.MockTransport(_failing_handler)
    return HttpClient(
        heartbeat=HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=transport,
    )


def _build_registry(http: HttpClient) -> AdapterRegistry:
    registry = AdapterRegistry()
    register_shopify(registry, http)
    return registry


async def _fetchone(db: Database, sql: str, params: tuple[Any, ...] = ()) -> Any:
    def _run(conn: Any) -> Any:
        return conn.execute(sql, params).fetchone()

    return await db.run(_run)


async def _count(db: Database, table: str) -> int:
    row = await _fetchone(db, f"SELECT COUNT(*) FROM {table}")  # fixed literal table name
    return int(row[0])


# ---------------------------------------------------------------------------
# The end-to-end test
# ---------------------------------------------------------------------------


async def _load_config(db: Database, registry: AdapterRegistry) -> None:
    """Load the real config.yaml with the two webhook env vars set (Req 13.1/18.2)."""
    import os
    from unittest import mock

    with mock.patch.dict(
        os.environ,
        {
            "RR_WEBHOOK_POKEMON": "https://discord.invalid/webhooks/x/y",
            "RR_WEBHOOK_ADMIN": "https://discord.invalid/webhooks/a/b",
        },
    ):
        await load_config_from_path(db, str(_CONFIG_PATH), registry=registry)


async def _fetchall(db: Database, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    def _run(conn: Any) -> list[Any]:
        return [tuple(row) for row in conn.execute(sql, params).fetchall()]

    return await db.run(_run)


async def _snapshot_state(db: Database) -> dict[str, list[Any]]:
    """Snapshot the full persisted baseline state for an identity comparison.

    Captures the three tables a re-poll must NOT mutate in M1 — ``listings`` observed
    state + ``seeded_at``, every ``alert_state`` row, and every ``seen_products`` row —
    each ordered deterministically so two snapshots compare by value.
    """
    return {
        "listings": await _fetchall(
            db,
            "SELECT id, in_stock, price, existence, seeded_at "
            "FROM listings ORDER BY id",
        ),
        "alert_state": await _fetchall(
            db,
            "SELECT subject_key, event_type, subject_kind, listing_id, "
            "last_alert_at, last_alerted_value, pending "
            "FROM alert_state ORDER BY subject_key, event_type",
        ),
        "seen_products": await _fetchall(
            db,
            "SELECT retailer_id, game_id, external_id, first_seen_at, title, handle "
            "FROM seen_products ORDER BY retailer_id, game_id, external_id",
        ),
    }


async def test_two_runs_leave_identical_state_and_zero_alerts(temp_db: Database) -> None:
    """Regression guard (Req 20.4 + M1 zero-alert): two consecutive ``run_once_dry_run``
    cycles against the SAME migrated temp DB leave IDENTICAL persisted state and emit zero
    alerts on BOTH runs, even though the second run re-polls every enabled subject.
    """
    await migrate(temp_db)

    # --- Run 1: baseline-seed everything. ---
    http1 = _build_http()
    registry1 = _build_registry(http1)
    await _load_config(temp_db, registry1)
    try:
        await run_once_dry_run(temp_db, http1, registry1)
    finally:
        await http1.aclose()

    snapshot_after_run1 = await _snapshot_state(temp_db)
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0

    # --- Run 2: re-poll everything; stored state must be byte-for-byte identical. ---
    http2 = _build_http()
    registry2 = _build_registry(http2)
    try:
        await run_once_dry_run(temp_db, http2, registry2)
    finally:
        await http2.aclose()

    snapshot_after_run2 = await _snapshot_state(temp_db)
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0

    # A re-poll of already-seeded subjects does not mutate ANY stored state in M1.
    assert snapshot_after_run2 == snapshot_after_run1


async def test_once_dry_run_seeds_state_and_emits_zero_alerts(temp_db: Database) -> None:
    """End-to-end M1 dry-run ``--once`` seeds every subject and emits zero alerts, then a
    second run against the SAME DB seeds nothing new (Req 20.1, 20.4, 2.1, 21.4).
    """
    await migrate(temp_db)

    # --- Run 1: everything is unseeded, so Baseline_Seeding applies to all subjects. ---
    http1 = _build_http()
    registry1 = _build_registry(http1)
    await _load_config(temp_db, registry1)
    try:
        summary1 = await run_once_dry_run(temp_db, http1, registry1)
    finally:
        await http1.aclose()

    # Both feeds + all three listings were seeded this run.
    assert summary1.feeds_seeded == 2
    assert summary1.listings_seeded == 3
    assert summary1.alerts_emitted == 0

    # Every feed and listing now carries seeded_at.
    assert await _count(temp_db, "discovery_feeds") == 2
    unseeded_feeds = await _fetchone(
        temp_db, "SELECT COUNT(*) FROM discovery_feeds WHERE seeded_at IS NULL"
    )
    assert int(unseeded_feeds[0]) == 0
    unseeded_listings = await _fetchone(
        temp_db, "SELECT COUNT(*) FROM listings WHERE seeded_at IS NULL"
    )
    assert int(unseeded_listings[0]) == 0

    # seen_products populated; both alert_state rows per listing; no alerts at all.
    assert await _count(temp_db, "seen_products") > 0
    # restock + price_change per listing.
    assert await _count(temp_db, "alert_state") == 2 * 3
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0

    seen_after_run1 = await _count(temp_db, "seen_products")

    # --- Run 2: same DB. Every subject is already seeded, so nothing new is seeded, but
    # --once re-polls every enabled entity (Req 20.4): the per-subject results must now
    # carry the freshly fetched data while still emitting zero alerts. ---
    http2 = _build_http()
    registry2 = _build_registry(http2)
    try:
        summary2 = await run_once_dry_run(temp_db, http2, registry2)
    finally:
        await http2.aclose()

    assert summary2.feeds_seeded == 0
    assert summary2.listings_seeded == 0
    assert summary2.alerts_emitted == 0

    # Feeds were RE-POLLED: products_found matches the fixture product counts (98 + 20),
    # not 0, proving a real re-crawl happened even though nothing was re-seeded.
    assert {feed.products_found for feed in summary2.feeds} == {98, 20}
    assert all(not feed.seeded_this_run for feed in summary2.feeds)
    # Listings were RE-FETCHED: existence is set (not the old "skipped" None).
    assert all(listing.existence is not None for listing in summary2.listings)
    assert all(not listing.seeded_this_run for listing in summary2.listings)

    # No new seen_products rows and still no alerts.
    assert await _count(temp_db, "seen_products") == seen_after_run1
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0

    # The run-2 summary never prints a webhook URL either (dry-run, Req 18.2/20.4).
    buffer2 = io.StringIO()
    write_summary(summary2, buffer2)
    text2 = buffer2.getvalue()
    assert "baseline: 0 feeds seeded, 0 listings seeded, 0 alerts emitted" in text2
    assert "products_found=98" in text2
    assert "products_found=20" in text2
    assert "discord" not in text2.lower()
    assert "http" not in text2.lower()

    # The summary log is written to stdout and NEVER prints a webhook URL (Req 18.2).
    buffer = io.StringIO()
    write_summary(summary1, buffer)
    text = buffer.getvalue()
    assert "baseline: 2 feeds seeded, 3 listings seeded, 0 alerts emitted" in text
    assert "discord" not in text.lower()
    assert "http" not in text.lower()


async def test_single_subject_fetch_failure_isolated_and_renders_error(
    temp_db: Database,
) -> None:
    """A per-subject adapter failure is ISOLATED: it is recorded as ``seeded=n/error``,
    does NOT abort the cycle, and does NOT seed or alert, while every OTHER feed and
    listing still fetches and baseline-seeds normally on this first run (Req 20.4, 2.1).

    One listing's ``.js`` endpoint returns a 500, which the HttpClient classifies as a
    :class:`~restockradar.adapters.errors.FetchError` (Req 16.4). The cycle must catch it,
    mark that one subject failed, and carry on — proving one subject's failure never
    aborts the others and can never read as an unchanged re-poll.
    """
    await migrate(temp_db)

    http = _build_failing_http()
    registry = _build_registry(http)
    await _load_config(temp_db, registry)
    try:
        summary = await run_once_dry_run(temp_db, http, registry)
    finally:
        await http.aclose()

    # The cycle returned a BaselineSummary covering every subject (it did NOT abort):
    # both feeds and all three listings are present.
    assert len(summary.feeds) == 2
    assert len(summary.listings) == 3

    # Exactly one subject failed — the forced-500 listing — and it is in the error state.
    failing = [listing for listing in summary.listings if listing.handle == _FAILING_HANDLE]
    assert len(failing) == 1
    failed = failing[0]
    assert failed.is_error
    assert not failed.is_seeded
    assert not failed.is_unchanged
    assert failed.seeded_this_run is False
    assert failed.alerts == 0
    # Failed subject carries no fetched state and no secret.
    assert failed.existence is None
    assert failed.in_stock is None
    assert failed.price is None
    assert failed.error == "FetchError"

    # Every OTHER listing succeeded and baseline-seeded on this first run.
    others = [listing for listing in summary.listings if listing.handle != _FAILING_HANDLE]
    assert len(others) == 2
    assert all(listing.is_seeded for listing in others)
    assert all(not listing.is_error for listing in others)
    assert all(listing.existence is not None for listing in others)

    # Both feeds succeeded and seeded normally (the failure was listing-local).
    assert all(feed.is_seeded for feed in summary.feeds)
    assert all(not feed.is_error for feed in summary.feeds)

    # One error, two listings seeded, both feeds seeded, zero alerts.
    assert summary.errors == 1
    assert summary.feeds_seeded == 2
    assert summary.listings_seeded == 2
    assert summary.alerts_emitted == 0

    # The failed listing was NOT seeded in the DB; the two healthy ones were.
    unseeded_listings = await _fetchone(
        temp_db, "SELECT COUNT(*) FROM listings WHERE seeded_at IS NULL"
    )
    assert int(unseeded_listings[0]) == 1
    # No alerts were written for anyone (baseline run + a failure that emits nothing).
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0

    # The summary renders seeded=n/error for the failing subject and NEVER a webhook URL.
    buffer = io.StringIO()
    write_summary(summary, buffer)
    text = buffer.getvalue()
    assert "seeded=n/error" in text
    assert f"listing {_FAILING_HANDLE}:" in text
    # The failed line reads as - fetched fields, never as an unchanged re-poll.
    assert "seeded=n/unchanged" not in text.split("seeded=n/error")[0].splitlines()[-1]
    assert "existence=- in_stock=- price=-" in text
    # No webhook URL / secret ever leaks (dry-run, Req 18.2/20.4).
    assert "discord" not in text.lower()
    assert "http" not in text.lower()
