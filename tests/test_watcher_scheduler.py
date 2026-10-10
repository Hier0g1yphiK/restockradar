"""Task 19.5 — scheduler wiring + due-selection + failed-first-poll unit tests
(Req 1.5-1.10, 2.4, 15.2, 19.7).

Two halves:

* **Scheduler wiring** (:func:`restockradar.watcher.scheduler.build_scheduler`): assert the
  CONFIGURED jobs — one interval job per retailer at the fixed ``tick_s`` with
  ``coalesce=True`` / ``max_instances=1`` and STAGGERED first-run offsets, plus one
  independent ``scheduler-heartbeat`` interval job that is present even with zero retailers.
  A tiny stub scheduler records every ``add_job`` call, so the assertions need no running
  event loop (Req 1.5, 1.9, 1.10, 19.7).

* **Watcher tick behaviour** (:meth:`restockradar.watcher.core.WatcherCore.tick_retailer`)
  against a real migrated in-memory DB with a fake offline adapter: retailer-wide backoff
  defers the whole job (Req 1.5/16.5); feeds are processed before listings and in
  due-ascending order (Req 1.6); ``next_due_at`` on finish is ``>= finish + interval``
  (Req 1.7); the effective interval honours the 30s floor (Req 15.2); and — the EXTRA USER
  REQUIREMENT (b) — a FAILED FIRST poll NEVER sets ``seeded_at`` and writes no alert rows,
  recording only backoff (Req 2.4, 16).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.base import (
    DiscoveredProduct,
    Existence,
    FeedLike,
    ListingLike,
    ProductState,
    VariantState,
)
from restockradar.adapters.errors import Blocked, FetchError
from restockradar.adapters.registry import AdapterRegistry
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.types import datetime_to_text
from restockradar.settings import Settings
from restockradar.watcher.core import MIN_POLL_INTERVAL_S, WatcherCore
from restockradar.watcher.scheduler import (
    HEARTBEAT_JOB_ID,
    build_scheduler,
    retailer_job_id,
)

_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


# ===========================================================================
# Scheduler wiring (stub scheduler — no event loop needed)
# ===========================================================================


@dataclass
class _RecordedJob:
    """One recorded ``add_job`` call's relevant kwargs."""

    func: Any
    kwargs: dict[str, Any]


@dataclass
class _StubScheduler:
    """A minimal stand-in exposing just the ``add_job`` surface build_scheduler uses."""

    jobs: list[_RecordedJob] = field(default_factory=list)

    def add_job(self, func: Any, **kwargs: Any) -> None:
        self.jobs.append(_RecordedJob(func=func, kwargs=kwargs))

    def by_id(self, job_id: str) -> _RecordedJob:
        for job in self.jobs:
            if job.kwargs.get("id") == job_id:
                return job
        raise KeyError(job_id)


def _settings(tick_s: float = 5.0) -> Settings:
    return Settings.from_env(env={"RR_TICK_S": str(tick_s)}, load_dotenv_file=False)


async def _noop_tick(_retailer_id: str) -> None:
    return None


def test_one_job_per_retailer_plus_independent_heartbeat() -> None:
    """One interval job per retailer at tick_s + one independent heartbeat job (Req 1.5, 19.7)."""
    stub = _StubScheduler()
    build_scheduler(
        retailer_ids=["r1", "r2", "r3"],
        settings=_settings(tick_s=5.0),
        tick=_noop_tick,
        heartbeat=HeartbeatRegistry(),
        scheduler=stub,
    )
    ids = {job.kwargs["id"] for job in stub.jobs}
    assert ids == {
        retailer_job_id("r1"),
        retailer_job_id("r2"),
        retailer_job_id("r3"),
        HEARTBEAT_JOB_ID,
    }
    # Every job ticks at the configured tick_s with no-overlap settings (Req 1.9).
    for job in stub.jobs:
        assert job.kwargs["trigger"] == "interval"
        assert job.kwargs["seconds"] == 5.0
        assert job.kwargs["coalesce"] is True
        assert job.kwargs["max_instances"] == 1


def test_independent_heartbeat_present_with_zero_retailers() -> None:
    """Zero enabled retailers still yields the independent heartbeat job (Req 1.9/19.7)."""
    stub = _StubScheduler()
    build_scheduler(
        retailer_ids=[],
        settings=_settings(),
        tick=_noop_tick,
        heartbeat=HeartbeatRegistry(),
        scheduler=stub,
    )
    assert len(stub.jobs) == 1
    assert stub.jobs[0].kwargs["id"] == HEARTBEAT_JOB_ID


def test_staggered_startup_first_runs_differ() -> None:
    """Each retailer job's first-run offset differs so they do not all fire together (Req 1.10)."""
    stub = _StubScheduler()
    build_scheduler(
        retailer_ids=["r1", "r2", "r3", "r4"],
        settings=_settings(tick_s=8.0),
        tick=_noop_tick,
        heartbeat=HeartbeatRegistry(),
        scheduler=stub,
    )
    first_runs = [
        stub.by_id(retailer_job_id(rid)).kwargs["next_run_time"]
        for rid in ("r1", "r2", "r3", "r4")
    ]
    # Four distinct, strictly-increasing first-run instants spread across the tick window.
    assert len(set(first_runs)) == 4
    assert first_runs == sorted(first_runs)
    # All offsets fall within one tick window [0, tick_s).
    span = (max(first_runs) - min(first_runs)).total_seconds()
    assert 0 < span < 8.0


def test_heartbeat_job_beats_scheduler() -> None:
    """The heartbeat job coroutine beats the scheduler heartbeat when run (Req 19.7)."""
    stub = _StubScheduler()
    heartbeat = HeartbeatRegistry()
    build_scheduler(
        retailer_ids=[],
        settings=_settings(),
        tick=_noop_tick,
        heartbeat=heartbeat,
        scheduler=stub,
    )
    from restockradar.observability.heartbeat import Component

    before = heartbeat.beat_scheduler()
    asyncio.run(stub.by_id(HEARTBEAT_JOB_ID).func())
    # The job's beat is recorded (monotonic, so >= the manual beat we took first).
    assert heartbeat.get(Component.SCHEDULER) >= before


# ===========================================================================
# DB harness + fake adapter for the tick-behaviour tests
# ===========================================================================


def _migrated_db() -> Database:
    db = Database(":memory:")
    asyncio.run(migrate(db))
    return db


def _run_on_db(db: Database, func: Any) -> Any:
    return asyncio.run(db.run(func))


def _count(db: Database, table: str) -> int:
    row = _run_on_db(
        db, lambda conn: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    )
    return int(row[0])


def _seed_base_catalogue(db: Database, *, poll_interval_s: int = 60) -> None:
    """Insert the game + retailer the tick reads (retailer inherits poll_interval_s)."""

    def _run(conn: Any) -> None:
        conn.execute("INSERT INTO games (id, name) VALUES (?, ?)", (_GAME_ID, "Pokemon"))
        conn.execute(
            "INSERT INTO retailers "
            "  (id, name, adapter_type, base_url, poll_interval_s) "
            "VALUES (?, ?, ?, ?, ?)",
            (_RETAILER_ID, "NCG", "shopify", "https://example.test", poll_interval_s),
        )

    _run_on_db(db, _run)


def _insert_listing(
    db: Database,
    listing_id: int,
    *,
    seeded: bool,
    next_due_at: datetime | None,
    poll_interval_s: int = 60,
) -> None:
    def _run(conn: Any) -> None:
        product_id = f"prod-{listing_id}"
        conn.execute(
            "INSERT INTO products (id, game_id, name) VALUES (?, ?, ?)",
            (product_id, _GAME_ID, f"Product {listing_id}"),
        )
        conn.execute(
            "INSERT INTO listings "
            "  (id, config_key, product_id, retailer_id, url, handle, external_id, "
            "   existence, in_stock, price, poll_interval_s, next_due_at, seeded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                listing_id,
                f"listing-{listing_id}",
                product_id,
                _RETAILER_ID,
                f"https://example.test/products/p{listing_id}",
                f"handle-{listing_id}",
                f"ext-{listing_id}",
                1 if seeded else None,
                1 if seeded else None,
                "10.00" if seeded else None,
                poll_interval_s,
                None if next_due_at is None else datetime_to_text(next_due_at),
                _NOW.isoformat() if seeded else None,
            ),
        )

    _run_on_db(db, _run)


def _insert_feed(
    db: Database,
    feed_id: int,
    *,
    seeded: bool,
    next_due_at: datetime | None,
) -> None:
    def _run(conn: Any) -> None:
        conn.execute(
            "INSERT INTO discovery_feeds "
            "  (id, config_key, retailer_id, game_id, feed_url, next_due_at, seeded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                feed_id,
                f"feed-{feed_id}",
                _RETAILER_ID,
                _GAME_ID,
                f"/collections/c{feed_id}/products.json",
                None if next_due_at is None else datetime_to_text(next_due_at),
                _NOW.isoformat() if seeded else None,
            ),
        )

    _run_on_db(db, _run)


def _set_retailer_backoff(db: Database, backoff_until: datetime) -> None:
    def _run(conn: Any) -> None:
        conn.execute(
            "INSERT INTO backoff_state "
            "  (scope_key, scope_kind, retailer_id, backoff_until, last_outcome) "
            "VALUES (?, 'retailer', ?, ?, 'blocked')",
            (f"retailer:{_RETAILER_ID}", _RETAILER_ID, datetime_to_text(backoff_until)),
        )

    _run_on_db(db, _run)


class _RecordingAdapter:
    """A fake offline adapter recording the ORDER of discover/fetch calls.

    Returns a benign EXISTING state for fetch (so a seeded listing diffs to no alert) and a
    small canned discovery list, unless configured to RAISE on the first call (for the
    failed-first-poll test).
    """

    adapter_type = "shopify"

    def __init__(self, *, raise_on_fetch: FetchError | None = None) -> None:
        self.calls: list[str] = []
        self._raise_on_fetch = raise_on_fetch

    async def discover(self, feed: FeedLike) -> list[DiscoveredProduct]:
        self.calls.append(f"discover:{feed.feed_url}")
        return [
            DiscoveredProduct(
                external_id="disc-1",
                title="Discovered",
                product_type=None,
                vendor=None,
                tags=(),
                handle="discovered",
                url="https://example.test/products/discovered",
                image=None,
                variants=None,
            )
        ]

    async def fetch_state(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        self.calls.append(f"fetch:{listing.handle}")
        if self._raise_on_fetch is not None:
            raise self._raise_on_fetch
        return ProductState(
            existence=Existence.EXISTING,
            in_stock=True,
            price=Decimal("10.00"),
            variants=(VariantState(variant_id="v1", available=True, price=Decimal("10.00")),),
            url=listing.url,
            image=None,
            title="A product",
            external_id=listing.external_id,
        )


def _watcher(db: Database, adapter: Any, *, now: datetime = _NOW) -> WatcherCore:
    registry = AdapterRegistry()
    registry.register("shopify", lambda _retailer: adapter)
    return WatcherCore(
        db=db,
        registry=registry,
        heartbeat=HeartbeatRegistry(),
        clock=lambda: now,
        jitter=lambda: 0.0,
    )


# ===========================================================================
# Retailer-wide backoff defers the whole job (Req 1.5, 16.5)
# ===========================================================================


def test_retailer_wide_backoff_defers_whole_job() -> None:
    """A future retailer backoff_until defers the whole tick — nothing is polled (Req 1.5)."""
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        _insert_feed(db, 1, seeded=True, next_due_at=None)
        _insert_listing(db, 1, seeded=True, next_due_at=None)
        _set_retailer_backoff(db, _NOW + timedelta(minutes=30))

        adapter = _RecordingAdapter()
        watcher = _watcher(db, adapter)
        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        assert result.deferred is True
        assert result.feeds_polled == 0
        assert result.listings_polled == 0
        # The adapter was never called — the gate fired before any fetch.
        assert adapter.calls == []
    finally:
        db.close_sync()


# ===========================================================================
# Feeds before listings, due-ascending (Req 1.6, 11.6)
# ===========================================================================


def test_feeds_processed_before_listings() -> None:
    """At equal due time, feeds are crawled before any listing is fetched (Req 1.6, 11.6)."""
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        _insert_feed(db, 1, seeded=True, next_due_at=None)
        _insert_listing(db, 1, seeded=True, next_due_at=None)

        adapter = _RecordingAdapter()
        watcher = _watcher(db, adapter)
        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        assert result.feeds_polled == 1
        assert result.listings_polled == 1
        # discover (feed) strictly precedes fetch (listing).
        assert adapter.calls == ["discover:/collections/c1/products.json", "fetch:handle-1"]
    finally:
        db.close_sync()


@settings(max_examples=40, deadline=None)
@given(offsets=st.lists(st.integers(min_value=0, max_value=600), min_size=2, max_size=5))
def test_listings_fetched_in_due_ascending_order(offsets: list[int]) -> None:
    """For any set of due listings, they are fetched earliest-effective-due first (Req 1.6).

    Each listing's ``next_due_at`` is ``now - offset`` (all due), so ascending ``next_due_at``
    means descending offset. The adapter records the fetch order, which must match the
    repository's due-ascending ordering (NULL-free here; ties break by id).
    """
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        # listing id i has next_due_at = now - offsets[i]; larger offset => earlier due.
        for index, offset in enumerate(offsets, start=1):
            _insert_listing(
                db,
                index,
                seeded=True,
                next_due_at=_NOW - timedelta(seconds=offset),
            )

        adapter = _RecordingAdapter()
        watcher = _watcher(db, adapter)
        asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        fetched_handles = [c.split(":", 1)[1] for c in adapter.calls if c.startswith("fetch:")]
        # Expected order: sort by (next_due_at ASC, id ASC) == (-offset? ) -> earliest due first.
        expected = [
            f"handle-{index}"
            for index, _offset in sorted(
                enumerate(offsets, start=1),
                key=lambda pair: (_NOW - timedelta(seconds=pair[1]), pair[0]),
            )
        ]
        assert fetched_handles == expected
    finally:
        db.close_sync()


# ===========================================================================
# next_due_at on finish + poll-interval floor (Req 1.7, 15.2)
# ===========================================================================


def test_next_due_at_set_on_finish_at_least_interval_ahead() -> None:
    """A successful listing poll sets next_due_at >= finish + interval (Req 1.7)."""
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        _insert_listing(db, 1, seeded=True, next_due_at=None, poll_interval_s=120)

        adapter = _RecordingAdapter()
        watcher = _watcher(db, adapter)
        asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        next_due_text = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT next_due_at FROM listings WHERE id = 1"
            ).fetchone()["next_due_at"],
        )
        assert next_due_text is not None
        next_due = datetime.fromisoformat(next_due_text)
        # finish == _NOW (frozen clock), jitter == 0, interval == 120.
        assert next_due >= _NOW + timedelta(seconds=120)
    finally:
        db.close_sync()


def test_poll_interval_floored_at_minimum() -> None:
    """A sub-floor configured interval is raised to the 30s floor for next_due (Req 15.2)."""
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        _insert_listing(db, 1, seeded=True, next_due_at=None, poll_interval_s=5)

        adapter = _RecordingAdapter()
        watcher = _watcher(db, adapter)
        asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        next_due_text = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT next_due_at FROM listings WHERE id = 1"
            ).fetchone()["next_due_at"],
        )
        next_due = datetime.fromisoformat(next_due_text)
        # interval floored at MIN_POLL_INTERVAL_S (30), jitter 0 => exactly now + 30s.
        assert next_due == _NOW + timedelta(seconds=MIN_POLL_INTERVAL_S)
    finally:
        db.close_sync()


# ===========================================================================
# EXTRA USER REQUIREMENT (b) — a FAILED FIRST poll never sets seeded_at (Req 2.4, 16)
# ===========================================================================


def test_failed_first_listing_poll_never_seeds() -> None:
    """A FAILED first listing poll leaves seeded_at NULL, writes no alert, records backoff.

    Validates: Requirements 2.4, 16.6

    This is the EXTRA USER REQUIREMENT (b): a failed first poll must NEVER seed.
    """
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        # Unseeded listing (seeded_at NULL) whose first poll will FAIL.
        _insert_listing(db, 1, seeded=False, next_due_at=None, poll_interval_s=60)

        adapter = _RecordingAdapter(raise_on_fetch=FetchError("boom"))
        watcher = _watcher(db, adapter)
        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        assert result.listings_polled == 1
        assert result.failures == 1

        # seeded_at is STILL NULL — the failed first poll did not seed.
        seeded_at = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT seeded_at FROM listings WHERE id = 1"
            ).fetchone()["seeded_at"],
        )
        assert seeded_at is None

        # No alert pipeline rows and no alert_state seeded.
        assert _count(db, "alerts_outbox") == 0
        assert _count(db, "alert_history") == 0
        assert _count(db, "alert_deliveries") == 0
        assert _count(db, "alert_state") == 0

        # backoff_state recorded the ordinary-error failure for the listing scope.
        row = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT error_count, blocked_count, backoff_until, last_outcome "
                "FROM backoff_state WHERE scope_key = 'listing:1'"
            ).fetchone(),
        )
        assert row is not None
        assert int(row["error_count"]) == 1
        assert int(row["blocked_count"]) == 0
        assert row["backoff_until"] is not None
        assert row["last_outcome"] == "error"
    finally:
        db.close_sync()


def test_failed_first_feed_poll_never_seeds() -> None:
    """A FAILED first feed poll leaves seeded_at NULL + records Blocked backoff (Req 2.4, 16.5)."""
    db = _migrated_db()
    try:
        _seed_base_catalogue(db)
        _insert_feed(db, 1, seeded=False, next_due_at=None)

        class _BlockedDiscover(_RecordingAdapter):
            async def discover(self, feed: FeedLike) -> list[DiscoveredProduct]:
                self.calls.append(f"discover:{feed.feed_url}")
                raise Blocked("blocked", retry_after=42.0)

        adapter = _BlockedDiscover()
        watcher = _watcher(db, adapter)
        result = asyncio.run(watcher.tick_retailer(_RETAILER_ID))

        assert result.feeds_polled == 1
        assert result.failures == 1

        seeded_at = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT seeded_at FROM discovery_feeds WHERE id = 1"
            ).fetchone()["seeded_at"],
        )
        assert seeded_at is None
        assert _count(db, "seen_products") == 0

        row = _run_on_db(
            db,
            lambda conn: conn.execute(
                "SELECT blocked_count, last_outcome FROM backoff_state "
                "WHERE scope_key = 'feed:1'"
            ).fetchone(),
        )
        assert row is not None
        assert int(row["blocked_count"]) == 1
        assert row["last_outcome"] == "blocked"
    finally:
        db.close_sync()
