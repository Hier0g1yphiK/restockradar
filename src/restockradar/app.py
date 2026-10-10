"""M1 application wiring: the dry-run ``--once`` baseline cycle (tasks 13.1, 13.2).

This module is the **honest Milestone 1 entrypoint** and nothing more. It wires the
already-built foundations together — settings, the single-writer
:class:`~restockradar.persistence.db.Database`, migrations, the Config_Loader, the
Adapter_Registry + Shopify_Adapter, the polite :class:`~restockradar.http.client.HttpClient`,
and the in-memory :class:`~restockradar.observability.heartbeat.HeartbeatRegistry` — and
runs ONE Baseline_Seeding cycle over the enabled discovery feeds and single-listing
subjects for each retailer (Req 19.12, 20.1, 20.4, 2.1, 2.3).

What M1 does, precisely:

* **Every enabled subject is RE-POLLED on each run** regardless of its ``seeded`` flag.
  ``--once`` deliberately ignores ``next_due_at`` and backoff for enabled entities and
  re-polls them every run (Req 20.4), so the ``seeded`` flag gates only the baseline
  WRITE, never the fetch.
* **Discovery feeds are processed BEFORE listings** so this cycle's ``cycle_products``
  map is populated first and a listing can reuse feed data / resolve a renamed handle
  (Req 11.6/11.8). Each enabled feed is crawled with the real
  :class:`~restockradar.adapters.shopify.ShopifyAdapter`
  (:meth:`~restockradar.adapters.shopify.ShopifyAdapter.discover`) on EVERY run; the
  FIRST crawl of an unseeded feed is also baseline-seeded via
  :func:`~restockradar.persistence.repository.seed_feed` — a first poll is
  Baseline_Seeding and emits NO alerts (Req 2.1, 3.5).
* **Listings are then fetched** with
  :meth:`~restockradar.adapters.shopify.ShopifyAdapter.fetch_state` on EVERY run; the
  first fetch of an unseeded listing is baseline-seeded via
  :func:`~restockradar.persistence.repository.seed_listing` — a first poll seeds both
  ``alert_state`` rows and emits NO alerts (Req 2.2, 2.3).
* **Already-seeded subjects are re-polled but their stored state is NOT mutated.** On a
  SECOND ``--once --dry-run`` run against the same database every subject already carries
  ``seeded_at``, so the cycle re-crawls / re-fetches it (reporting the freshly fetched
  ``products_found`` / existence / stock / price) but does NOT re-run the baseline seed:
  stored ``listings`` state and both ``alert_state`` rows stay exactly as the baseline
  wrote them, ``seen_products`` is untouched, and NO alert is emitted (observed-vs-stored
  detection/diffing is M2). Two runs therefore leave identical persisted state while the
  per-subject summary lines prove a real re-poll happened on each run.
* **Dry-run means no delivery side effects.** Because this is a fresh-DB first run
  there are no alerts anyway, but the dry-run flag is honoured strictly: the cycle NEVER
  resolves, probes, or prints a webhook URL and performs no destination probing (Req 20.4).
* **Graceful shutdown**: the WAL is checkpoint-truncated and the DB executor closed at
  the end; the entrypoint returns 0 on success.

Detection, dispatch, the APScheduler loop, the Watchdog, and real webhook POSTs are all
M2-M4 work and are deliberately NOT implemented here. Invoked WITHOUT ``--once`` the
entrypoint prints a clear "M1 only supports --once --dry-run" message and exits 0 rather
than pretending to run a continuous scheduler.

The cycle is structured so it is testable fully offline: :func:`run_once_dry_run` takes
an already-built :class:`~restockradar.persistence.db.Database` and
:class:`~restockradar.http.client.HttpClient`, so a test can point the client at an
:class:`httpx.MockTransport` serving saved fixtures against a temp DB (the suite-wide
no-network guard then guarantees nothing touches the live network). The real CLI path
(:func:`run`) builds a live client and hits the network politely.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from sqlite3 import Connection
from typing import TextIO

from restockradar.adapters.base import Adapter, DiscoveredProduct
from restockradar.adapters.errors import FetchError
from restockradar.adapters.registry import AdapterFactory, AdapterRegistry, RetailerLike
from restockradar.adapters.shopify import ShopifyAdapter, shopify_factory
from restockradar.domain.models import Existence
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.repository import (
    BaselineListing,
    BaselineProduct,
    seed_feed,
    seed_listing,
)
from restockradar.settings import Settings

__all__ = [
    "BaselineSummary",
    "FeedResult",
    "ListingResult",
    "build_arg_parser",
    "load_config_from_path",
    "register_shopify",
    "run",
    "run_once_dry_run",
    "write_summary",
]


# ---------------------------------------------------------------------------
# Row value objects read from the enabled catalogue
# ---------------------------------------------------------------------------


# NOTE: these row value objects are plain (non-frozen) dataclasses deliberately. The
# adapter input protocols (RetailerLike / FeedLike / ListingLike) declare plain settable
# attributes, which a frozen dataclass's read-only attributes do not satisfy under
# ``mypy --strict``; a plain dataclass does. They are built once per cycle and never
# mutated, so the lost immutability has no runtime consequence here.


@dataclass(slots=True)
class _RetailerRow:
    """The retailer attributes the cycle and the Shopify adapter read.

    Carries both ``adapter_type`` (so it satisfies the registry's ``RetailerLike``) and
    ``base_url`` / ``minor_unit_exponent`` (so it satisfies the Shopify adapter's
    ``RetailerLike``), so one row object serves both the registry and the adapter.
    """

    id: str
    base_url: str
    adapter_type: str
    minor_unit_exponent: int


@dataclass(slots=True)
class _FeedRow:
    """An enabled discovery-feed row and the ``discover()`` inputs it carries."""

    id: int
    retailer_id: str
    game_id: str
    feed_url: str
    max_page_count: int
    seeded: bool


@dataclass(slots=True)
class _ListingRow:
    """An enabled listing row and the ``fetch_state()`` inputs it carries."""

    id: int
    retailer_id: str
    url: str
    handle: str
    external_id: str | None
    seeded: bool


# ---------------------------------------------------------------------------
# Per-subject + overall summary (task 13.2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeedResult:
    """What the baseline cycle did for one discovery feed (task 13.2).

    ``products_found`` is the number of products discovered THIS run — the feed is
    re-crawled on every run (Req 20.4), so this reflects the live fetch even when the
    feed was already seeded on a prior run. ``seeded_this_run`` is true only when this
    run performed the first-poll baseline write; ``alerts`` is always 0 in M1.

    ``error`` is ``None`` on a successful poll and otherwise a short, SECRET-FREE summary
    of the adapter failure (the exception type name — never a webhook URL or any other
    secret). A FAILED poll does NOT seed and emits NO alert; it leaves a subject in the
    ``seeded=n/error`` summary state so a failure can never be mistaken for an unchanged
    re-poll. A subject is in exactly one of three summary states — seeded this run
    (:attr:`is_seeded`), already-seeded re-polled unchanged (:attr:`is_unchanged`), or
    FAILED (:attr:`is_error`).
    """

    config_id: int
    feed_url: str
    products_found: int
    seeded_this_run: bool
    alerts: int = 0
    error: str | None = None

    @property
    def is_error(self) -> bool:
        """True when the poll FAILED (adapter raised); fetched fields are meaningless."""
        return self.error is not None

    @property
    def is_seeded(self) -> bool:
        """True when THIS run performed the first-poll baseline write (``seeded=y``)."""
        return self.seeded_this_run and not self.is_error

    @property
    def is_unchanged(self) -> bool:
        """True when already-seeded and re-polled without error (``seeded=n/unchanged``)."""
        return not self.seeded_this_run and not self.is_error


@dataclass(frozen=True, slots=True)
class ListingResult:
    """What the baseline cycle did for one single-listing subject (task 13.2).

    ``existence`` / ``in_stock`` / ``price`` are the values fetched THIS run — the
    listing is re-fetched on every run (Req 20.4), so they reflect the live fetch even
    when the listing was already seeded on a prior run. ``seeded_this_run`` is true only
    on the first-poll baseline write; ``alerts`` is always 0 in M1.

    ``error`` is ``None`` on a successful fetch and otherwise a short, SECRET-FREE summary
    of the adapter failure (the exception type name — never a webhook URL or any other
    secret). A FAILED fetch does NOT seed and emits NO alert; its fetched fields are left
    ``None`` and it lands in the ``seeded=n/error`` summary state so a failure can never
    be mistaken for an unchanged re-poll. A subject is in exactly one of three summary
    states — seeded this run (:attr:`is_seeded`), already-seeded re-polled unchanged
    (:attr:`is_unchanged`), or FAILED (:attr:`is_error`).
    """

    listing_id: int
    handle: str
    existence: Existence | None
    in_stock: bool | None
    price: str | None
    seeded_this_run: bool
    alerts: int = 0
    error: str | None = None

    @property
    def is_error(self) -> bool:
        """True when the fetch FAILED (adapter raised); fetched fields are meaningless."""
        return self.error is not None

    @property
    def is_seeded(self) -> bool:
        """True when THIS run performed the first-poll baseline write (``seeded=y``)."""
        return self.seeded_this_run and not self.is_error

    @property
    def is_unchanged(self) -> bool:
        """True when already-seeded and re-fetched without error (``seeded=n/unchanged``)."""
        return not self.seeded_this_run and not self.is_error


@dataclass(frozen=True, slots=True)
class BaselineSummary:
    """The overall result of one dry-run ``--once`` cycle (task 13.2).

    Aggregates the per-feed and per-listing results and the headline counts the summary
    log reports: how many feeds and listings were seeded THIS run and the total alerts
    emitted (0 on a baseline run).
    """

    feeds: list[FeedResult] = field(default_factory=list)
    listings: list[ListingResult] = field(default_factory=list)

    @property
    def feeds_seeded(self) -> int:
        """Number of feeds baseline-seeded this run (a FAILED feed is never seeded)."""
        return sum(1 for feed in self.feeds if feed.is_seeded)

    @property
    def listings_seeded(self) -> int:
        """Number of listings baseline-seeded this run (a FAILED listing is never seeded)."""
        return sum(1 for listing in self.listings if listing.is_seeded)

    @property
    def errors(self) -> int:
        """Number of subjects (feeds + listings) whose poll FAILED this run."""
        return sum(1 for feed in self.feeds if feed.is_error) + sum(
            1 for listing in self.listings if listing.is_error
        )

    @property
    def alerts_emitted(self) -> int:
        """Total alerts emitted across all subjects (0 on a baseline run)."""
        return sum(f.alerts for f in self.feeds) + sum(
            listing.alerts for listing in self.listings
        )


# ---------------------------------------------------------------------------
# Catalogue reads (synchronous — run on the DB executor)
# ---------------------------------------------------------------------------


def _read_retailers(conn: Connection) -> list[_RetailerRow]:
    """Read the enabled retailers, ordered by id for a deterministic cycle."""
    rows = conn.execute(
        "SELECT id, base_url, adapter_type, minor_unit_exponent "
        "FROM retailers WHERE enabled = 1 ORDER BY id"
    ).fetchall()
    return [
        _RetailerRow(
            id=str(row["id"]),
            base_url=str(row["base_url"]),
            adapter_type=str(row["adapter_type"]),
            minor_unit_exponent=int(row["minor_unit_exponent"]),
        )
        for row in rows
    ]


def _read_feeds(conn: Connection, retailer_id: str) -> list[_FeedRow]:
    """Read a retailer's enabled discovery feeds, ordered by id."""
    rows = conn.execute(
        "SELECT id, retailer_id, game_id, feed_url, max_page_count, seeded_at "
        "FROM discovery_feeds WHERE enabled = 1 AND retailer_id = ? ORDER BY id",
        (retailer_id,),
    ).fetchall()
    return [
        _FeedRow(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            game_id=str(row["game_id"]),
            feed_url=str(row["feed_url"]),
            max_page_count=int(row["max_page_count"]),
            seeded=row["seeded_at"] is not None,
        )
        for row in rows
    ]


def _read_listings(conn: Connection, retailer_id: str) -> list[_ListingRow]:
    """Read a retailer's enabled listings, ordered by id."""
    rows = conn.execute(
        "SELECT id, retailer_id, url, handle, external_id, seeded_at "
        "FROM listings WHERE enabled = 1 AND retailer_id = ? ORDER BY id",
        (retailer_id,),
    ).fetchall()
    return [
        _ListingRow(
            id=int(row["id"]),
            retailer_id=str(row["retailer_id"]),
            url=str(row["url"]),
            handle=str(row["handle"]),
            external_id=None if row["external_id"] is None else str(row["external_id"]),
            seeded=row["seeded_at"] is not None,
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# The one dry-run --once baseline cycle (tasks 13.1 / 13.2)
# ---------------------------------------------------------------------------


async def run_once_dry_run(
    db: Database,
    http: HttpClient,
    registry: AdapterRegistry,
    *,
    now: datetime | None = None,
) -> BaselineSummary:
    """Run ONE Baseline_Seeding cycle over every enabled subject and return a summary.

    Processes DISCOVERY FEEDS BEFORE LISTINGS for each enabled retailer (so this cycle's
    ``cycle_products`` is populated first, Req 11.6). EVERY enabled feed is crawled and
    EVERY enabled listing is fetched on EVERY run — ``--once`` ignores ``next_due_at`` and
    backoff for enabled entities and re-polls them each run (Req 20.4). The FIRST poll of
    an unseeded subject additionally performs the baseline write (``seed_feed`` /
    ``seed_listing``, Req 2.1-2.3); an ALREADY-seeded subject is still re-polled but its
    stored state is NOT re-written, so two runs leave identical persisted state. No alerts
    are ever emitted (detection/diffing is M2) and no webhook/destination work is performed
    (dry-run, Req 20.4).

    ``db`` and ``http`` are injected so the whole cycle is drivable offline from saved
    fixtures (the no-network guard enforces that in the suite); the real CLI builds a
    live client. The registry MUST already have the retailer adapter types registered.
    """
    seeded_at = now or datetime.now(UTC)
    summary = BaselineSummary()

    retailers = await db.run(_read_retailers)
    for retailer in retailers:
        adapter = registry.resolve(retailer)

        def _read_feeds_for(conn: Connection, rid: str = retailer.id) -> list[_FeedRow]:
            return _read_feeds(conn, rid)

        def _read_listings_for(
            conn: Connection, rid: str = retailer.id
        ) -> list[_ListingRow]:
            return _read_listings(conn, rid)

        feeds = await db.run(_read_feeds_for)
        # Discovery feeds first so cycle_products feeds the listing pass (Req 11.6).
        cycle_products: dict[str, DiscoveredProduct] = {}
        for feed in feeds:
            feed_result = await _process_feed(
                db, adapter, feed, cycle_products, seeded_at
            )
            summary.feeds.append(feed_result)

        listings = await db.run(_read_listings_for)
        for listing in listings:
            listing_result = await _process_listing(
                db, adapter, listing, cycle_products, seeded_at
            )
            summary.listings.append(listing_result)

    return summary


async def _process_feed(
    db: Database,
    adapter: Adapter,
    feed: _FeedRow,
    cycle_products: dict[str, DiscoveredProduct],
    seeded_at: datetime,
) -> FeedResult:
    """Re-crawl one feed every run; baseline-seed it only on its first ever poll.

    The feed is ALWAYS crawled (Req 20.4 — ``--once`` ignores due/backoff and re-polls);
    the ``seeded`` flag gates only the baseline WRITE. An already-seeded feed is re-crawled
    (so ``products_found`` reflects the live fetch) but not re-seeded, leaving
    ``seen_products`` untouched; a first-ever poll additionally baseline-seeds it. No
    alerts are emitted in either case (M1).

    If ``discover`` raises a :class:`~restockradar.adapters.errors.FetchError` (or any
    subclass — Blocked / SoftBlock / MalformedResponse / TruncatedFeed, and the client's
    RobotsDisallowed / RobotsRetryLater), the failure is CAUGHT: the feed is NOT seeded,
    NO alert is emitted, and a FAILED :class:`FeedResult` (``seeded=n/error``) is returned
    so the surrounding cycle continues with the other subjects.
    """
    # Always re-poll: --once ignores next_due_at/backoff for enabled entities (Req 20.4).
    # A per-subject adapter failure (FetchError and its subclasses Blocked / SoftBlock /
    # MalformedResponse / TruncatedFeed, plus the client's RobotsDisallowed /
    # RobotsRetryLater which also subclass FetchError) must NOT abort the cycle: record it
    # as a FAILED poll (seeded=n/error) and CONTINUE with the other subjects. The failure
    # summary is the exception TYPE NAME only — never a webhook URL or any other secret.
    try:
        discovered = await adapter.discover(feed)
    except FetchError as exc:
        return FeedResult(
            config_id=feed.id,
            feed_url=feed.feed_url,
            products_found=0,
            seeded_this_run=False,
            error=type(exc).__name__,
        )
    # Populate this cycle's product map for the listing pass (feed-reuse, Req 11.6).
    for product in discovered:
        cycle_products.setdefault(product.external_id, product)

    if not feed.seeded:
        # First ever poll: perform the baseline seed exactly once.
        await seed_feed(
            db,
            feed_id=feed.id,
            retailer_id=feed.retailer_id,
            game_id=feed.game_id,
            products=[
                BaselineProduct(
                    external_id=product.external_id,
                    title=product.title,
                    handle=product.handle,
                )
                for product in discovered
            ],
            seeded_at=seeded_at,
        )

    return FeedResult(
        config_id=feed.id,
        feed_url=feed.feed_url,
        products_found=len(discovered),
        seeded_this_run=not feed.seeded,
    )


async def _process_listing(
    db: Database,
    adapter: Adapter,
    listing: _ListingRow,
    cycle_products: dict[str, DiscoveredProduct],
    seeded_at: datetime,
) -> ListingResult:
    """Re-fetch one listing every run; baseline-seed it only on its first ever poll.

    The listing is ALWAYS fetched (Req 20.4 — ``--once`` ignores due/backoff and
    re-polls); the ``seeded`` flag gates only the baseline WRITE. An already-seeded
    listing is re-fetched (so the reported existence/stock/price reflect the live fetch)
    but its stored ``listings`` row and both ``alert_state`` rows are NOT re-written — in
    M1 a re-poll never overwrites stored state and never emits an alert, so two runs leave
    identical persisted state. A first-ever poll additionally baseline-seeds it.

    If ``fetch_state`` raises a :class:`~restockradar.adapters.errors.FetchError` (or any
    subclass — Blocked / SoftBlock / MalformedResponse / TruncatedFeed, and the client's
    RobotsDisallowed / RobotsRetryLater), the failure is CAUGHT: the listing is NOT seeded,
    NO alert is emitted, and a FAILED :class:`ListingResult` (``seeded=n/error``, fetched
    fields left ``None``) is returned so the surrounding cycle continues. A definite 404 is
    NOT a failure — the adapter RETURNS existence=NOT_FOUND rather than raising (Req 7.1).
    """
    # Always re-poll: --once ignores next_due_at/backoff for enabled entities (Req 20.4).
    # A per-subject adapter failure (FetchError and its subclasses Blocked / SoftBlock /
    # MalformedResponse / TruncatedFeed, plus the client's RobotsDisallowed /
    # RobotsRetryLater which also subclass FetchError) must NOT abort the cycle: record it
    # as a FAILED fetch (seeded=n/error), leave the fetched fields unset, emit no alert,
    # and CONTINUE with the other subjects. The failure summary is the exception TYPE NAME
    # only — never a webhook URL or any other secret. A definite 404 is NOT an error: the
    # adapter RETURNS existence=NOT_FOUND rather than raising (Req 7.1).
    try:
        state = await adapter.fetch_state(listing, cycle_products)
    except FetchError as exc:
        return ListingResult(
            listing_id=listing.id,
            handle=listing.handle,
            existence=None,
            in_stock=None,
            price=None,
            seeded_this_run=False,
            error=type(exc).__name__,
        )
    existence = (
        Existence.EXISTING if state.existence.value == "existing" else Existence.NOT_FOUND
    )

    if not listing.seeded:
        # First ever poll: perform the baseline seed exactly once. Already-seeded
        # listings are re-fetched but their stored state is deliberately left untouched
        # (observed-vs-stored detection/diffing is M2), so repeated runs are idempotent.
        await seed_listing(
            db,
            BaselineListing(
                listing_id=listing.id,
                in_stock=state.in_stock,
                price=state.price,
                existence=existence,
                seeded_at=seeded_at,
            ),
        )

    return ListingResult(
        listing_id=listing.id,
        handle=listing.handle,
        existence=existence,
        in_stock=state.in_stock,
        price=None if state.price is None else str(state.price),
        seeded_this_run=not listing.seeded,
    )


# ---------------------------------------------------------------------------
# Baseline summary log (task 13.2) — stdout, never a webhook URL
# ---------------------------------------------------------------------------


def write_summary(summary: BaselineSummary, stream: TextIO) -> None:
    """Write the per-feed / per-listing baseline summary and the overall line (task 13.2).

    One line per feed (products found THIS run, seeded y / n-unchanged, alerts) and per
    listing (fetched existence+stock+price THIS run, seeded y / n-unchanged, alerts), then
    a single overall summary line. The per-subject lines report the live re-poll on EVERY
    run (Req 20.4), while ``seeded=`` and the overall ``feeds_seeded`` / ``listings_seeded``
    counts count ONLY subjects baseline-seeded THIS run — so a second run shows the same
    products_found / existence / stock / price but ``0 feeds seeded, 0 listings seeded``.
    A webhook URL is NEVER printed (dry-run, Req 18.2/20.4).
    """
    print("baseline dry-run --once summary", file=stream)
    for feed in summary.feeds:
        # products_found reflects the live re-poll on EVERY run (Req 20.4); seeded= marks
        # one of three states: y (seeded this run), n/unchanged (already-seeded re-poll),
        # or n/error (the fetch FAILED). A FAILED feed reports products_found=- so it can
        # NEVER be mistaken for an unchanged re-poll; the ` error=<type>` note is the
        # exception type name only (never a webhook URL).
        if feed.is_error:
            print(
                f"  feed  {feed.feed_url}: products_found=- "
                f"seeded=n/error alerts={feed.alerts} error={feed.error}",
                file=stream,
            )
            continue
        seeded = "y" if feed.seeded_this_run else "n/unchanged"
        print(
            f"  feed  {feed.feed_url}: products_found={feed.products_found} "
            f"seeded={seeded} alerts={feed.alerts}",
            file=stream,
        )
    for listing in summary.listings:
        # existence/in_stock/price reflect the live re-poll on EVERY run (Req 20.4);
        # seeded= marks one of three states: y / n/unchanged / n/error (see feeds above).
        # A FAILED listing reports all fetched fields as - so it can NEVER be mistaken for
        # an unchanged re-poll; the ` error=<type>` note is the exception type name only.
        if listing.is_error:
            fetched = "existence=- in_stock=- price=-"
            print(
                f"  listing {listing.handle}: {fetched} "
                f"seeded=n/error alerts={listing.alerts} error={listing.error}",
                file=stream,
            )
            continue
        seeded = "y" if listing.seeded_this_run else "n/unchanged"
        existence = listing.existence.value if listing.existence else "-"
        in_stock = "-" if listing.in_stock is None else str(listing.in_stock).lower()
        price = "-" if listing.price is None else listing.price
        fetched = f"existence={existence} in_stock={in_stock} price={price}"
        print(
            f"  listing {listing.handle}: {fetched} "
            f"seeded={seeded} alerts={listing.alerts}",
            file=stream,
        )
    # The overall line keeps counting ONLY subjects seeded this run and preserves its
    # existing shape; the error count is appended as a trailing clause so the historical
    # ``N feeds seeded, M listings seeded, K alerts emitted`` substring is unchanged.
    print(
        f"baseline: {summary.feeds_seeded} feeds seeded, "
        f"{summary.listings_seeded} listings seeded, "
        f"{summary.alerts_emitted} alerts emitted, "
        f"{summary.errors} errors",
        file=stream,
    )


# ---------------------------------------------------------------------------
# Dependency building + CLI entrypoint (task 13.1)
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the M1 argument parser (``--dry-run`` and ``--once``)."""
    parser = argparse.ArgumentParser(
        prog="restockradar",
        description="Stock & Drop Monitor (M1: --once --dry-run baseline).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render alerts to the log and issue no webhook POST (M1: no alerts).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single baseline cycle and exit (the only M1 mode).",
    )
    return parser


def _build_http_client(heartbeat: HeartbeatRegistry) -> HttpClient:
    """Build the live, polite HTTP client used by the real CLI path (Req 1, 15, 16)."""
    return HttpClient(heartbeat=heartbeat, host_gate=HostGate())


def register_shopify(registry: AdapterRegistry, http: HttpClient) -> None:
    """Register the ``shopify`` adapter type on ``registry`` (Req 11.3, 22.2).

    The Shopify factory needs a retailer carrying ``base_url`` / ``minor_unit_exponent``
    (its own ``RetailerLike``), which is narrower than the registry's ``RetailerLike``
    (``adapter_type`` only). The retailer the registry hands the factory at resolve time
    (:class:`_RetailerRow`) carries all three attributes, so this thin typed wrapper
    bridges the two protocols without widening either declaration.
    """
    shopify = shopify_factory(http)

    def _factory(retailer: RetailerLike) -> Adapter:
        adapter: ShopifyAdapter = shopify(retailer)  # type: ignore[arg-type]
        return adapter

    registered: AdapterFactory = _factory
    registry.register("shopify", registered)


def _read_config_text(config_path: str) -> str:
    """Read the YAML config file text from ``config_path`` (Req 13.1)."""
    return Path(config_path).read_text(encoding="utf-8")


async def _run_cycle_and_log(
    db: Database,
    http: HttpClient,
    registry: AdapterRegistry,
    stream: TextIO,
) -> None:
    """Run the baseline cycle and write the summary (shared by the CLI path)."""
    summary = await run_once_dry_run(db, http, registry)
    write_summary(summary, stream)


async def run(
    argv: list[str] | None = None,
    *,
    settings: Settings | None = None,
    stream: TextIO | None = None,
) -> int:
    """Parse args, build the M1 dependencies, run ONE dry-run ``--once`` cycle (task 13.1).

    Resolves ``--once`` / ``--dry-run`` from BOTH the CLI flags AND
    :class:`~restockradar.settings.Settings` (``RR_ONCE`` / ``RR_DRY_RUN``) — a flag OR
    its env setting enables the mode. For M1 only the ``--once --dry-run`` path is wired;
    invoked without ``--once`` the entrypoint prints a clear message and returns 0 (the
    continuous scheduler is M2+). On the wired path it builds the settings, opens the
    :class:`~restockradar.persistence.db.Database` (creating its parent directory),
    migrates, builds the heartbeat registry + HTTP client, registers the Shopify adapter,
    loads ``config.yaml``, runs the cycle, writes the baseline summary, checkpoints the
    WAL, closes the DB, and returns 0.
    """
    import sys

    out = stream or sys.stdout
    args = build_arg_parser().parse_args(argv)
    settings = settings or Settings.from_env()

    once = args.once or settings.once
    dry_run = args.dry_run or settings.dry_run

    if not once:
        print(
            "M1 only supports --once --dry-run; continuous scheduling arrives in a "
            "later milestone. Re-run with: python -m restockradar --once --dry-run",
            file=out,
        )
        return 0
    if not dry_run:
        # M1 only implements the dry-run baseline cycle; a non-dry-run --once (live
        # delivery) is M3+ work. Be honest rather than pretend to deliver.
        print(
            "M1 only supports --once --dry-run; live delivery arrives in a later "
            "milestone. Re-run with: python -m restockradar --once --dry-run",
            file=out,
        )
        return 0

    # Create the DB's parent directory if needed (a file-backed path, not :memory:).
    db_path = settings.db_path
    if db_path not in ("", ":memory:"):
        Path(db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    heartbeat = HeartbeatRegistry()
    http = _build_http_client(heartbeat)
    db = Database(db_path, busy_timeout_ms=settings.db_busy_timeout_ms)
    try:
        await migrate(db)
        registry = AdapterRegistry()
        register_shopify(registry, http)
        await load_config_from_path(db, settings.config_path, registry=registry)
        await _run_cycle_and_log(db, http, registry, out)
        await db.wal_checkpoint_truncate()
    finally:
        await http.aclose()
        await db.close()
    return 0


async def load_config_from_path(
    db: Database, config_path: str, *, registry: AdapterRegistry
) -> None:
    """Load the YAML config at ``config_path`` into ``db`` (Req 13.1).

    A thin wrapper over :func:`restockradar.config.loader.load_config` that reads the
    file text first; kept separate so the import stays local to the CLI path and the
    cycle itself has no config-file dependency (the offline test loads config directly).
    """
    from restockradar.config.loader import load_config

    raw = _read_config_text(config_path)
    await load_config(db, raw, registry=registry)
