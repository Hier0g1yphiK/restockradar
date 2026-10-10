"""Task 20 — M2 CHECKPOINT deliverable: the ``--once --dry-run`` detection cycle.

The M2 Definition of Done (tasks.md task 20) requires that a dry-run ``--once`` over the
captured fixtures **exercises detection** (new-product, restock, price, page-live,
combined, handle-rename) and **writes the resulting ``alerts_outbox`` / ``alert_deliveries``
rows durably, with no real webhook POSTs issued**. This suite proves exactly that against
the REAL M2 pipeline wired in :func:`restockradar.app.run_once_detect` /
:meth:`restockradar.watcher.core.WatcherCore.tick_retailer` (``ignore_due=True``), entirely
offline (the suite-wide no-network guard in ``conftest.py`` makes a real socket a hard
failure, so "no real POSTs" is proven by construction — there is no dispatch loop in M2 and
nothing ever attempts a Discord POST).

Two complementary halves, each explicit about which event_type it proves:

* **Part A — real adapter + real fixtures (new-product + baseline zero-alert).**
  :func:`test_once_dry_run_detects_new_product_and_writes_deliveries` drives the ACTUAL
  CLI path (:func:`restockradar.app.run_once_dry_run` then :func:`run_once_detect`) with the
  real :class:`~restockradar.adapters.shopify.ShopifyAdapter` over an
  :class:`httpx.MockTransport` serving the saved ``fixtures/ncg/*`` plus an allow-all
  robots. Run 1 seeds and emits ZERO alerts (M1 no-regression). Run 2 serves a feed page
  carrying ONE synthetic extra product (inline dict, fixtures stay pristine) absent from
  the seeded ``seen_products`` — proving a ``new_product`` outbox row + its
  ``alert_deliveries`` to the enabled healthy ``pokemon`` destination, with the MockTransport
  asserting it is only ever asked for GET feed/listing/robots requests (never a POST).

* **Part B — real pipeline, controlled fake adapter (the five listing transitions).**
  Driving restock / price_change / page_live / combined / handle_rename through the fixed
  captured fixtures is impractical, so these are driven through the REAL
  :func:`run_once_detect` + :class:`WatcherCore` detection pipeline with a controlled fake
  adapter returning the precise :class:`~restockradar.adapters.base.ProductState` for each
  transition, against a real migrated DB seeded with the matching baseline stored-state +
  ``alert_state``. Each test asserts the resulting ``alerts_outbox.event_type`` and the
  durable ``alert_deliveries`` fan-out (and, for handle-rename, exactly one
  ``retailer:<id>`` ``admin_notices`` row and NO product outbox row). The fake adapter
  performs no network I/O at all, so again no POST can occur.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from restockradar.adapters.base import (
    DiscoveredProduct,
    Existence,
    FeedLike,
    ListingLike,
    ProductState,
    VariantState,
)
from restockradar.adapters.registry import AdapterRegistry
from restockradar.app import (
    load_config_from_path,
    register_shopify,
    run_once_detect,
    run_once_dry_run,
    write_summary,
)
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate

# ---------------------------------------------------------------------------
# Shared paths + fixture loading (mirrors tests/test_app_once_dry_run.py)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_DIR = _REPO_ROOT / "fixtures" / "ncg"
_CONFIG_PATH = _REPO_ROOT / "config" / "config.yaml"

ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"

_FEED_FIXTURES = {
    "/collections/pokemon/products.json": "collections_pokemon_products_page1.json",
    "/collections/pre-orders-1/products.json": "collections_pre_orders_1_products_page1.json",
}

#: The one listing handle with a saved ``.js`` fixture (served verbatim).
_POKEWEEN_JS = "products_ncg-pokeween-18-october.js.json"

#: A synthetic extra product injected into the pokemon feed on run 2 ONLY. Its id is absent
#: from the seeded ``seen_products`` set, so it is a genuine NEW product; its title matches
#: no exclude keyword and (for the pre-orders feed's include filter) is irrelevant because
#: it is injected into the plain pokemon collection whose only filter is the excludes.
_NEW_PRODUCT_ID = 9999999999001
_NEW_PRODUCT_HANDLE = "pokemon-brand-new-surprise-box"


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
    for fixture in _FEED_FIXTURES.values():
        body = _fixture_json(fixture)
        for product in body["products"]:
            if product["handle"] == handle:
                entry: dict[str, Any] = product
                return entry
    raise AssertionError(f"no feed entry found for handle {handle!r}")


def _synthetic_js_from_feed(handle: str) -> dict[str, Any]:
    """Minimal valid ``.js`` body for a handle lacking a saved ``.js`` fixture.

    Mirrors the helper in ``tests/test_app_once_dry_run.py``: the ``.js`` endpoint carries
    INTEGER minor-unit prices, so each feed variant's decimal price is converted at the
    retailer's ``minor_unit_exponent`` of 2.
    """
    entry = _feed_entry_for_handle(handle)
    variants = [
        {
            "id": variant["id"],
            "available": variant.get("available", False),
            "price": round(float(variant["price"]) * 100),
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


def _new_product_feed_entry() -> dict[str, Any]:
    """A brand-new feed product entry injected on run 2 only (inline, fixtures pristine)."""
    return {
        "id": _NEW_PRODUCT_ID,
        "title": "Pokemon Brand New Surprise Box",
        "handle": _NEW_PRODUCT_HANDLE,
        "product_type": "Trading Card Game",
        "vendor": "Pokemon",
        "tags": ["pokemon"],
        "variants": [{"id": 42424242, "available": True, "price": "199.99"}],
        "images": [],
    }


# ---------------------------------------------------------------------------
# Transport handlers (offline)
# ---------------------------------------------------------------------------


def _baseline_handler(request: httpx.Request) -> httpx.Response:
    """Serve robots + the pristine captured fixtures (the baseline run)."""
    _assert_offline_get(request)
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
        return _json_response({"products": []})
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


def _new_product_handler(request: httpx.Request) -> httpx.Response:
    """Like the baseline handler, but the pokemon feed page 1 gains ONE new product.

    Everything else is served exactly as the baseline, so the ONLY new signal between
    run 1 and run 2 is a single previously-unseen product on the pokemon collection — a
    deterministic ``new_product`` transition for the real feed diff.
    """
    _assert_offline_get(request)
    path = request.url.path
    if path == "/collections/pokemon/products.json":
        page = int(request.url.params.get("page", "1"))
        if page == 1:
            body = _fixture_json(_FEED_FIXTURES[path])
            body["products"].append(_new_product_feed_entry())
            return _json_response(body)
        return _json_response({"products": []})
    return _baseline_handler(request)


def _assert_offline_get(request: httpx.Request) -> None:
    """Fail hard if the pipeline ever attempts a non-GET or a Discord POST (no real POSTs).

    The M2 DoD requires NO real webhook POST. The detection pipeline only ever issues GET
    requests for feeds / listings / robots; a POST, or any request to a ``discord`` host,
    would mean a delivery leaked into the dry-run path. Turning that into an AssertionError
    makes "no real POSTs issued" an enforced invariant of every test using this transport.
    """
    assert request.method == "GET", f"unexpected non-GET request: {request.method}"
    assert "discord" not in request.url.host.lower(), (
        f"unexpected request to a discord host: {request.url!r}"
    )


# ---------------------------------------------------------------------------
# Wiring helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db(tmp_path: Path) -> Iterator[Database]:
    db = Database(str(tmp_path / "restockradar.db"))
    try:
        yield db
    finally:
        db.close_sync()


def _build_http(handler: Any) -> HttpClient:
    return HttpClient(
        heartbeat=HeartbeatRegistry(),
        host_gate=HostGate(min_spacing_s=0.0),
        transport=httpx.MockTransport(handler),
    )


def _build_registry(http: HttpClient) -> AdapterRegistry:
    registry = AdapterRegistry()
    register_shopify(registry, http)
    return registry


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


async def _fetchone(db: Database, sql: str, params: tuple[Any, ...] = ()) -> Any:
    def _run(conn: Any) -> Any:
        return conn.execute(sql, params).fetchone()

    return await db.run(_run)


async def _fetchall(db: Database, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    def _run(conn: Any) -> list[Any]:
        return [tuple(row) for row in conn.execute(sql, params).fetchall()]

    return await db.run(_run)


async def _count(db: Database, table: str) -> int:
    row = await _fetchone(db, f"SELECT COUNT(*) FROM {table}")
    return int(row[0])


async def _outbox_event_types(db: Database) -> list[str]:
    rows = await _fetchall(db, "SELECT event_type FROM alerts_outbox ORDER BY id")
    return [str(row[0]) for row in rows]


# ===========================================================================
# Part A — real adapter + real fixtures: new-product + baseline zero-alert
# ===========================================================================


async def test_once_dry_run_detects_new_product_and_writes_deliveries(
    temp_db: Database,
) -> None:
    """M2 DoD (``new_product``): the real ``--once --dry-run`` path seeds silently then,
    on a second pass serving ONE new feed product, writes a ``new_product`` outbox row and
    its durable ``alert_deliveries`` — with NO real POST ever attempted.

    Run 1 is the M1 baseline (zero alerts, no regression). Run 2 serves the pokemon feed
    with a single synthetic extra product absent from the seeded ``seen_products``; the
    REAL feed diff fires a ``new_product`` alert and fans it out to the enabled, healthy,
    non-admin ``pokemon`` destination. The MockTransport asserts every request is a GET for
    a feed / listing / robots path (never a POST, never a discord host), proving the dry-run
    issues no real webhook POST.
    """
    await migrate(temp_db)

    # --- Run 1: baseline-seed everything; ZERO alerts (M1 no-regression). ---
    http1 = _build_http(_baseline_handler)
    registry1 = _build_registry(http1)
    await _load_config(temp_db, registry1)
    try:
        baseline = await run_once_dry_run(temp_db, http1, registry1)
        await run_once_detect(temp_db, registry1, heartbeat=HeartbeatRegistry())
    finally:
        await http1.aclose()

    assert baseline.alerts_emitted == 0
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0
    seen_after_run1 = await _count(temp_db, "seen_products")

    # --- Run 2: baseline (seeds nothing new) + detection over the NEW-product feed. ---
    http2 = _build_http(_new_product_handler)
    registry2 = _build_registry(http2)
    try:
        await run_once_dry_run(temp_db, http2, registry2)
        detect = await run_once_detect(temp_db, registry2, heartbeat=HeartbeatRegistry())
    finally:
        await http2.aclose()

    # The detection pass polled both feeds + all three listings for the one retailer.
    assert detect.retailers == 1
    assert detect.feeds_polled == 2
    assert detect.listings_polled == 3
    assert detect.failures == 0

    # Exactly one new-product outbox row, carrying the injected product's identity.
    assert await _outbox_event_types(temp_db) == ["new_product"]
    outbox = await _fetchone(
        temp_db,
        "SELECT alert_kind, event_type, subject_kind, external_id, game_id, payload "
        "FROM alerts_outbox",
    )
    assert outbox[0] == "product"
    assert outbox[1] == "new_product"
    assert outbox[2] == "feed_product"
    assert str(outbox[3]) == str(_NEW_PRODUCT_ID)
    assert outbox[4] == "pokemon"
    # The payload never leaks a webhook URL (dry-run, Req 18.2).
    assert "discord" not in str(outbox[5]).lower()

    # The new product was recorded in seen_products (so a re-poll would NOT re-alert).
    assert await _count(temp_db, "seen_products") == seen_after_run1 + 1

    # DURABLE delivery row(s): exactly one pending delivery to the enabled healthy,
    # non-admin pokemon destination (admin destination excluded).
    deliveries = await _fetchall(
        temp_db,
        "SELECT d.status, dest.config_key, dest.is_admin "
        "FROM alert_deliveries d JOIN destinations dest ON dest.id = d.destination_id",
    )
    assert len(deliveries) == 1
    assert deliveries[0][0] == "pending"
    assert deliveries[0][1] == "pokemon-alerts"
    assert int(deliveries[0][2]) == 0


# ===========================================================================
# Part B — real pipeline + controlled fake adapter: the five listing transitions
# ===========================================================================

_RETAILER_ID = "ncg"
_GAME_ID = "pokemon"


class _ScriptedAdapter:
    """A fake offline adapter returning a scripted :class:`ProductState` per listing handle.

    Performs NO network I/O (so no POST can ever occur), and returns a canned
    :class:`ProductState` keyed by the listing's handle. ``discover`` returns nothing — the
    transition tests exercise listing diffs only.
    """

    adapter_type = "shopify"

    def __init__(self, states: Mapping[str, ProductState]) -> None:
        self._states = states

    async def discover(self, feed: FeedLike) -> list[DiscoveredProduct]:
        return []

    async def fetch_state(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        return self._states[listing.handle]


def _variants(available: bool, price: str) -> tuple[VariantState, ...]:
    return (VariantState(variant_id="v1", available=available, price=Decimal(price)),)


def _existing_state(
    *, handle: str, in_stock: bool, price: str
) -> ProductState:
    return ProductState(
        existence=Existence.EXISTING,
        in_stock=in_stock,
        price=Decimal(price),
        variants=_variants(in_stock, price),
        url=f"https://www.northerncardgaming.com/products/{handle}",
        image=None,
        title="A product",
        external_id="ext-1",
    )


#: Stored ``listings.existence`` is an INTEGER column (1 existing / 0 not-found / NULL
#: unseeded, Req 7.3); these are the two encodings the transition tests seed.
_EXISTENCE_EXISTING = 1
_EXISTENCE_NOT_FOUND = 0


async def _seed_single_listing(
    db: Database,
    *,
    handle: str,
    in_stock: bool | None,
    price: str | None,
    existence: int,
    seeded: bool = True,
) -> int:
    """Insert the game + retailer + a product + ONE seeded listing with controlled state.

    Returns the listing id. ``existence`` is the stored INTEGER encoding
    (:data:`_EXISTENCE_EXISTING` / :data:`_EXISTENCE_NOT_FOUND`). ``alert_state`` rows
    (restock + price_change) are seeded for a seeded listing so the diff treats it as
    already-baselined (page-live only when stored existence is NOT_FOUND / unseeded).
    """

    def _run(conn: Any) -> int:
        conn.execute(
            "INSERT OR IGNORE INTO games (id, name) VALUES (?, ?)",
            (_GAME_ID, "Pokemon"),
        )
        conn.execute(
            "INSERT OR IGNORE INTO retailers "
            "  (id, name, adapter_type, base_url, poll_interval_s, minor_unit_exponent) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (_RETAILER_ID, "NCG", "shopify", "https://www.northerncardgaming.com", 60, 2),
        )
        product_id = f"prod-{handle}"
        conn.execute(
            "INSERT INTO products (id, game_id, name) VALUES (?, ?, ?)",
            (product_id, _GAME_ID, f"Product {handle}"),
        )
        cursor = conn.execute(
            "INSERT INTO listings "
            "  (config_key, product_id, retailer_id, url, handle, external_id, "
            "   existence, in_stock, price, poll_interval_s, seeded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"cfg-{handle}",
                product_id,
                _RETAILER_ID,
                f"https://www.northerncardgaming.com/products/{handle}",
                handle,
                "ext-1",
                existence,
                None if in_stock is None else (1 if in_stock else 0),
                price,
                60,
                "2024-01-01T00:00:00+00:00" if seeded else None,
            ),
        )
        listing_id = int(cursor.lastrowid or 0)
        if seeded:
            subject_key = f"listing:{listing_id}"
            conn.execute(
                "INSERT INTO alert_state "
                "  (subject_key, event_type, subject_kind, listing_id, "
                "   last_alert_at, last_alerted_value, pending) "
                "VALUES (?, 'restock', 'listing', ?, NULL, NULL, 0)",
                (subject_key, listing_id),
            )
            conn.execute(
                "INSERT INTO alert_state "
                "  (subject_key, event_type, subject_kind, listing_id, "
                "   last_alert_at, last_alerted_value, pending) "
                "VALUES (?, 'price_change', 'listing', ?, NULL, ?, 0)",
                (subject_key, listing_id, price),
            )
        return listing_id

    return await db.run(_run)


async def _seed_destination(db: Database) -> None:
    """Insert ONE enabled, healthy, non-admin pokemon destination (the fan-out target)."""

    def _run(conn: Any) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO games (id, name) VALUES (?, ?)",
            (_GAME_ID, "Pokemon"),
        )
        conn.execute(
            "INSERT INTO destinations "
            "  (config_key, source, server_id, game_id, webhook_env_var, "
            "   is_admin, health_status, enabled) "
            "VALUES (?, 'config', ?, ?, ?, 0, 'healthy', 1)",
            ("pokemon-alerts", "000000000000000001", _GAME_ID, "RR_WEBHOOK_POKEMON"),
        )

    await db.run(_run)


async def _run_detect(db: Database, states: Mapping[str, ProductState]) -> None:
    """Run the REAL :func:`run_once_detect` pipeline with the scripted fake adapter."""
    registry = AdapterRegistry()
    registry.register("shopify", lambda _retailer: _ScriptedAdapter(states))
    await run_once_detect(db, registry, heartbeat=HeartbeatRegistry())


async def test_detect_restock_writes_restock_outbox_and_delivery(
    temp_db: Database,
) -> None:
    """M2 DoD (``restock``): a listing out of stock at baseline, now in stock, writes a
    single ``restock`` outbox row + its durable delivery, with no real POST.
    """
    await migrate(temp_db)
    await _seed_destination(temp_db)
    handle = "restock-listing"
    await _seed_single_listing(
        temp_db,
        handle=handle,
        in_stock=False,
        price="10.00",
        existence=_EXISTENCE_EXISTING,
    )

    await _run_detect(
        temp_db, {handle: _existing_state(handle=handle, in_stock=True, price="10.00")}
    )

    assert await _outbox_event_types(temp_db) == ["restock"]
    assert await _count(temp_db, "alert_deliveries") == 1
    status = await _fetchone(temp_db, "SELECT status FROM alert_deliveries")
    assert status[0] == "pending"


async def test_detect_price_change_writes_price_change_outbox_and_delivery(
    temp_db: Database,
) -> None:
    """M2 DoD (``price_change``): a listing whose price differs from the seeded
    ``last_alerted_value`` (cooldown elapsed — no prior alert) writes a single
    ``price_change`` outbox row + its durable delivery, with no real POST.
    """
    await migrate(temp_db)
    await _seed_destination(temp_db)
    handle = "price-listing"
    # In stock at baseline AND now (so no restock fires), price moves 10.00 -> 12.50.
    await _seed_single_listing(
        temp_db,
        handle=handle,
        in_stock=True,
        price="10.00",
        existence=_EXISTENCE_EXISTING,
    )

    await _run_detect(
        temp_db, {handle: _existing_state(handle=handle, in_stock=True, price="12.50")}
    )

    assert await _outbox_event_types(temp_db) == ["price_change"]
    assert await _count(temp_db, "alert_deliveries") == 1


async def test_detect_page_live_writes_page_live_outbox_and_delivery(
    temp_db: Database,
) -> None:
    """M2 DoD (``page_live``): a listing whose stored existence is ``not_found`` flipping to
    ``EXISTING`` writes a single ``page_live`` outbox row + its durable delivery.
    """
    await migrate(temp_db)
    await _seed_destination(temp_db)
    handle = "page-live-listing"
    # Stored existence NOT_FOUND => the page-live one-shot fires on the EXISTING fetch.
    await _seed_single_listing(
        temp_db,
        handle=handle,
        in_stock=None,
        price=None,
        existence=_EXISTENCE_NOT_FOUND,
    )

    await _run_detect(
        temp_db, {handle: _existing_state(handle=handle, in_stock=True, price="15.00")}
    )

    assert await _outbox_event_types(temp_db) == ["page_live"]
    assert await _count(temp_db, "alert_deliveries") == 1


async def test_detect_combined_writes_single_combined_outbox_and_delivery(
    temp_db: Database,
) -> None:
    """M2 DoD (``combined``): a listing that both restocks AND changes price in one poll
    writes exactly ONE ``combined`` outbox row (not two) + its durable delivery.
    """
    await migrate(temp_db)
    await _seed_destination(temp_db)
    handle = "combined-listing"
    # Out of stock + price 10.00 at baseline; now in stock AND price 20.00 => both fire.
    await _seed_single_listing(
        temp_db,
        handle=handle,
        in_stock=False,
        price="10.00",
        existence=_EXISTENCE_EXISTING,
    )

    await _run_detect(
        temp_db, {handle: _existing_state(handle=handle, in_stock=True, price="20.00")}
    )

    assert await _outbox_event_types(temp_db) == ["combined"]
    assert await _count(temp_db, "alert_deliveries") == 1


async def test_detect_handle_rename_updates_listing_and_writes_admin_notice(
    temp_db: Database,
) -> None:
    """M2 DoD (``handle_rename``): a fetch returning ``resolved_handle`` / ``resolved_url``
    takes the handle-rename write path — the listing url/handle is updated and exactly one
    ``retailer:<id>`` ``admin_notices`` row is written, with NO product outbox / delivery row.
    """
    await migrate(temp_db)
    await _seed_destination(temp_db)
    old_handle = "old-handle"
    new_handle = "new-handle"
    listing_id = await _seed_single_listing(
        temp_db,
        handle=old_handle,
        in_stock=True,
        price="10.00",
        existence=_EXISTENCE_EXISTING,
    )

    new_url = f"https://www.northerncardgaming.com/products/{new_handle}"
    rename_state = ProductState(
        existence=Existence.EXISTING,
        in_stock=True,
        price=Decimal("10.00"),
        variants=_variants(True, "10.00"),
        url=new_url,
        image=None,
        title="A product",
        external_id="ext-1",
        resolved_handle=new_handle,
        resolved_url=new_url,
    )
    await _run_detect(temp_db, {old_handle: rename_state})

    # The listing's url + handle were updated to the resolved values.
    row = await _fetchone(
        temp_db, "SELECT url, handle FROM listings WHERE id = ?", (listing_id,)
    )
    assert row[0] == new_url
    assert row[1] == new_handle

    # Exactly one retailer-scoped admin notice, and NO product outbox / delivery row.
    notices = await _fetchall(
        temp_db, "SELECT scope_key, kind FROM admin_notices ORDER BY id"
    )
    assert len(notices) == 1
    assert notices[0][0] == f"retailer:{_RETAILER_ID}"
    assert notices[0][1] == "handle_change"
    assert await _count(temp_db, "alerts_outbox") == 0
    assert await _count(temp_db, "alert_deliveries") == 0


# ===========================================================================
# The CLI run() wiring also writes the detection summary without leaking a URL
# ===========================================================================


async def test_detect_summary_line_rendered_without_webhook_url(
    temp_db: Database,
) -> None:
    """A sanity check that the baseline summary still renders with zero alerts and never a
    webhook URL (the ``run()`` path appends the detect line separately).
    """
    await migrate(temp_db)
    http = _build_http(_baseline_handler)
    registry = _build_registry(http)
    await _load_config(temp_db, registry)
    try:
        summary = await run_once_dry_run(temp_db, http, registry)
        await run_once_detect(temp_db, registry, heartbeat=HeartbeatRegistry())
    finally:
        await http.aclose()

    buffer = io.StringIO()
    write_summary(summary, buffer)
    text = buffer.getvalue()
    assert "0 alerts emitted" in text
    assert "discord" not in text.lower()
    assert "http" not in text.lower()
