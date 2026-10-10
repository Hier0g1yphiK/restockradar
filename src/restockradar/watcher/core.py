"""Per-retailer tick: retailer backoff gate, due-ordered entity selection (feeds
before listings) processed sequentially under a 5-minute per-poll cap, fetch ->
``BEGIN IMMEDIATE`` -> read -> diff -> write -> commit, ``cycle_products`` per run,
and the watcher-level handle-rename write path (Req 1, 2, 3, 4, 5, 6, 7, 11, 15, 16).

This is the M2 integration layer: it wires the already-built PURE pieces together into
one real per-retailer poll cycle. The pure diff functions
(:func:`~restockradar.detect.diff_feed.diff_feed` /
:func:`~restockradar.detect.diff_listing.diff_listing`), the backoff machine
(:func:`~restockradar.watcher.backoff.next_backoff`), and the effective-due helpers
(:mod:`restockradar.watcher.due`) carry the logic; the fan-out layer
(:mod:`restockradar.detect.fanout`) and the repository write helpers carry the
persistence. :class:`WatcherCore` is the thin, DB/async/adapter-touching glue that reads
the catalogue, fetches through the shared :class:`~restockradar.http.client.HttpClient`,
and routes each poll's outcome to the right writer.

The per-retailer tick (:meth:`WatcherCore.tick_retailer`) follows the design's
Concurrency Model exactly:

1. **Retailer backoff gate (Req 1.5, 16.5).** Read the retailer's ``backoff_state`` row
   (``scope_key='retailer:<id>'``); if ``effective_due_retailer(backoff_until)`` is in the
   future (``not is_due``), the WHOLE job defers — nothing is polled this tick. A Blocked
   retailer thereby defers all of its feeds and listings.
2. **Due selection, feeds before listings (Req 1.6, 11.6).** Otherwise select the
   retailer's enabled DUE feeds and listings ordered by effective due time ascending, and
   process FEEDS FIRST so this run's ``cycle_products`` map is populated before any listing
   fetch consults it for feed-reuse / handle-rename resolution.
3. **Sequential under a 5-minute cap (Req 1.3).** The whole tick's processing is wrapped in
   ``asyncio.timeout(300)``; entities are processed one at a time (single-writer
   discipline — one fetch, then one executor transaction, then the next entity).
4. **fetch -> read -> diff -> write -> commit per entity.** A feed is crawled with
   ``adapter.discover`` then (if seeded) diffed with :func:`diff_feed` and fanned out, or
   baseline-seeded on its first complete poll (Req 2.1). A listing is fetched with
   ``adapter.fetch_state`` then (if seeded) diffed with :func:`diff_listing` and fanned
   out, or baseline-seeded on its first complete poll (Req 2.2). A rename
   (``resolved_handle`` set) takes the dedicated watcher-level write path instead (Req
   11.8).
5. **next_due_at on a successful finish (Req 1.7).** When an entity's poll finishes
   successfully its ``next_due_at`` is set to ``finish + poll_interval + jitter`` (the
   interval floored at :data:`MIN_POLL_INTERVAL_S`, Req 15.2).
6. **Backoff on failure, no transition (Req 16).** When the adapter RAISES
   (:class:`~restockradar.adapters.errors.FetchError` / ``Blocked`` / …) the failure is
   recorded in the entity's ``backoff_state`` via :func:`next_backoff` and NO transition is
   emitted. Crucially a FAILED FIRST poll never sets ``seeded_at`` (Req 2.4) — seeding only
   ever happens on a complete successful poll, so a subject that has never been reached
   stays unseeded and is retried.
7. **Heartbeat (Req 19.7).** The scheduler heartbeat is beaten between entities (the HTTP
   client already beats per request), so a long tick keeps the scheduler beat fresh.

A definite 404 is NOT a failure: the adapter RETURNS ``existence=NOT_FOUND`` rather than
raising (Req 7.1), so a 404 flows through the normal diff/seed path, not the backoff path.
"""

from __future__ import annotations

import asyncio
import random
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from restockradar.adapters.base import (
    Adapter,
    DiscoveredProduct,
    ProductState,
)
from restockradar.adapters.base import (
    Existence as FetchExistence,
)
from restockradar.adapters.errors import Blocked, FetchError
from restockradar.adapters.registry import AdapterRegistry
from restockradar.detect.diff_feed import diff_feed
from restockradar.detect.diff_listing import diff_listing
from restockradar.detect.fanout import write_feed_diff, write_listing_diff
from restockradar.domain.ids import (
    listing_subject_key,
    scope_listing_key,
    scope_retailer_key,
)
from restockradar.domain.models import Existence as StoredExistence
from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.persistence.db import Database
from restockradar.persistence.repository import (
    AdminNoticeRow,
    BackoffStateRow,
    BackoffStateUpsert,
    BaselineListing,
    BaselineProduct,
    FeedFetchInputs,
    HandleRenameWrite,
    ListingFetchInputs,
    RetailerFetchInputs,
    read_alert_state,
    read_all_enabled_feed_inputs,
    read_all_enabled_listing_inputs,
    read_backoff_state,
    read_due_feed_inputs,
    read_due_listing_inputs,
    read_listing_state,
    read_retailer_inputs,
    set_feed_next_due_at,
    set_listing_next_due_at,
    write_backoff_state,
    write_feed_baseline,
    write_handle_rename,
    write_listing_baseline,
)
from restockradar.watcher.backoff import Outcome, next_backoff
from restockradar.watcher.due import effective_due_retailer, is_due

__all__ = [
    "MIN_POLL_INTERVAL_S",
    "PER_POLL_CAP_S",
    "WatcherCore",
    "default_jitter",
]

#: The per-poll processing cap in seconds (Req 1.3). The whole tick's entity processing is
#: wrapped in ``asyncio.timeout`` with this budget; a tick that exceeds it stops servicing
#: further due entities this round (the due-ascending order keeps the longest-waiting first).
PER_POLL_CAP_S = 300.0

#: The minimum effective poll interval in seconds (Req 15.2). An entity's configured
#: ``poll_interval_s`` is floored at this value so the next-due computation never schedules
#: a sub-30s cadence, however the catalogue is configured.
MIN_POLL_INTERVAL_S = 30

#: The scope-kind token stored on a retailer / listing ``backoff_state`` row.
_SCOPE_KIND_RETAILER = "retailer"
_SCOPE_KIND_LISTING = "listing"
_SCOPE_KIND_FEED = "feed"

#: The admin-notice kind the handle-rename path queues (Req 11.8).
_HANDLE_CHANGE_KIND = "handle_change"


def default_jitter() -> float:
    """Return the default positive scheduling jitter in seconds (Req 1.7).

    A small non-negative random offset (0 to 5s) spread on top of ``next_due_at`` so a fleet
    of entities does not re-cluster onto the same tick after each poll. Injectable on
    :class:`WatcherCore` so tests can pin it to a fixed value and assert the next-due
    computation exactly.
    """
    return random.uniform(0.0, 5.0)


@dataclass(frozen=True, slots=True, kw_only=True)
class TickResult:
    """A small record of what one :meth:`WatcherCore.tick_retailer` call did.

    ``deferred`` is true when the retailer-wide backoff gate skipped the whole tick (Req
    1.5). ``feeds_polled`` / ``listings_polled`` count the entities actually fetched this
    tick, and ``failures`` the entities whose adapter call raised (and so recorded backoff
    rather than a transition). Returned so a caller / test can assert the tick's shape
    without scraping logs.
    """

    deferred: bool = False
    feeds_polled: int = 0
    listings_polled: int = 0
    failures: int = 0


class WatcherCore:
    """The DB/async/adapter glue that runs one per-retailer poll tick (Req 1-7, 11, 15, 16).

    Holds the shared, process-wide dependencies — the single-writer
    :class:`~restockradar.persistence.db.Database`, the resolved
    :class:`~restockradar.adapters.registry.AdapterRegistry`, the
    :class:`~restockradar.observability.heartbeat.HeartbeatRegistry`, the clock, and the
    scheduling jitter — and exposes :meth:`tick_retailer`, the coroutine the scheduler
    fires once per retailer per tick. The adapter resolves its own HTTP client via the
    registry's factory (the registry captured the shared client at registration), so this
    class never touches ``httpx`` directly.
    """

    __slots__ = ("_clock", "_db", "_heartbeat", "_jitter", "_registry")

    def __init__(
        self,
        *,
        db: Database,
        registry: AdapterRegistry,
        heartbeat: HeartbeatRegistry,
        clock: Callable[[], datetime] | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        """Create a watcher core.

        Args:
            db: The single-writer database (every write runs on its executor).
            registry: The adapter registry, already carrying the retailer adapter types
                (the HTTP client is captured in each factory's closure).
            heartbeat: The in-memory heartbeat registry beaten between entities (Req 19.7).
            clock: Injectable UTC-now clock, defaulting to a timezone-aware
                :func:`datetime.now`; tests pin it for deterministic ``next_due_at`` /
                backoff assertions.
            jitter: Injectable scheduling jitter (seconds), defaulting to
                :func:`default_jitter`; tests pin it to a fixed value.
        """
        self._db = db
        self._registry = registry
        self._heartbeat = heartbeat
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._jitter: Callable[[], float] = jitter or default_jitter

    # -- public tick -------------------------------------------------------------

    async def tick_retailer(
        self, retailer_id: str, *, ignore_due: bool = False
    ) -> TickResult:
        """Run ONE poll tick for ``retailer_id`` (the scheduler's per-retailer job body).

        Reads the retailer's adapter inputs and applies the retailer-wide backoff gate; if
        the retailer is deferred (Req 1.5/16.5), returns immediately having polled nothing.
        Otherwise selects the due feeds + listings (feeds first, due-ascending, Req 1.6)
        and processes them sequentially under the 5-minute cap (Req 1.3), returning a
        :class:`TickResult` describing what ran. Each entity's own adapter failure is
        contained (recorded as backoff, Req 16) and never aborts the tick.

        When ``ignore_due`` is set (the ``--once`` one-shot path, Req 20.4) the tick
        **skips the retailer-wide backoff gate** and selects **every enabled feed and
        listing** regardless of ``next_due_at`` or backoff, so each enabled entity is
        polled exactly once this cycle. The default (``ignore_due=False``) path is the
        scheduled behaviour, completely unchanged: the retailer backoff gate still defers
        a Blocked retailer and only DUE entities are selected. Robots.txt and per-host
        spacing are enforced inside the HTTP client/host-gate on either path, so a
        one-shot cycle still obeys them (design "Operational modes: ``--once``").
        """
        now = self._clock()
        retailer = await self._db.run(
            lambda conn: read_retailer_inputs(conn, retailer_id)
        )
        if retailer is None:
            # Disabled / removed between scheduling and this tick: nothing to do.
            return TickResult(deferred=True)

        # 1. Retailer-wide backoff gate (Req 1.5, 16.5) — SKIPPED for --once (Req 20.4).
        if not ignore_due:
            backoff_row = await self._db.run(
                lambda conn: read_backoff_state(conn, scope_retailer_key(retailer_id))
            )
            backoff_until = (
                backoff_row.backoff_until if backoff_row is not None else None
            )
            if not is_due(effective_due_retailer(backoff_until), now):
                return TickResult(deferred=True)

        adapter = self._registry.resolve(retailer)

        # 2. Entity selection — feeds BEFORE listings (Req 1.6, 11.6). For --once every
        # enabled entity is selected (ignore due/backoff, Req 20.4); otherwise only DUE.
        if ignore_due:
            feeds = await self._db.run(
                lambda conn: read_all_enabled_feed_inputs(conn, retailer_id)
            )
            listings = await self._db.run(
                lambda conn: read_all_enabled_listing_inputs(conn, retailer_id)
            )
        else:
            feeds = await self._db.run(
                lambda conn: read_due_feed_inputs(conn, retailer_id, now)
            )
            listings = await self._db.run(
                lambda conn: read_due_listing_inputs(conn, retailer_id, now)
            )

        cycle_products: dict[str, DiscoveredProduct] = {}
        result = TickResult()

        # 3. Sequential processing under the 5-minute per-poll cap (Req 1.3). A cap breach
        # stops servicing further due entities this tick; the already-serviced work has
        # committed, and the due-ascending order means the longest-waiting ran first.
        try:
            async with asyncio.timeout(PER_POLL_CAP_S):
                for feed in feeds:
                    failed = await self._process_feed(
                        adapter, retailer, feed, cycle_products
                    )
                    result = _bump(result, feed=True, failed=failed)
                    self._heartbeat.beat_scheduler()
                for listing in listings:
                    failed = await self._process_listing(
                        adapter, retailer, listing, cycle_products
                    )
                    result = _bump(result, feed=False, failed=failed)
                    self._heartbeat.beat_scheduler()
        except TimeoutError:
            # The per-poll cap elapsed mid-tick. Everything already processed has committed
            # its own transaction; we simply stop early and let the next tick continue.
            pass

        return result

    # -- feed processing ---------------------------------------------------------

    async def _process_feed(
        self,
        adapter: Adapter,
        retailer: RetailerFetchInputs,
        feed: FeedFetchInputs,
        cycle_products: dict[str, DiscoveredProduct],
    ) -> bool:
        """Crawl, then seed-or-diff one feed; return ``True`` if the adapter RAISED.

        The feed is crawled with ``adapter.discover``; on an adapter failure the backoff
        row for ``feed:<id>`` is recorded and the feed is NOT seeded and emits no alert
        (Req 16, 2.4). On success this cycle's ``cycle_products`` map is populated, then
        the feed is baseline-seeded on its FIRST complete poll (Req 2.1) or diffed +
        fanned out thereafter (Req 3), and its ``next_due_at`` advanced (Req 1.7). A
        success also clears the feed's backoff (Req 16.7).
        """
        try:
            discovered = await adapter.discover(feed)
        except FetchError as exc:
            await self._record_failure(
                scope_key=f"{_SCOPE_KIND_FEED}:{feed.id}",
                scope_kind=_SCOPE_KIND_FEED,
                retailer_id=retailer.id,
                feed_id=feed.id,
                listing_id=None,
                poll_interval_s=_effective_interval(feed.poll_interval_s, retailer),
                exc=exc,
            )
            return True

        for product in discovered:
            if product.external_id:
                cycle_products.setdefault(product.external_id, product)

        finish = self._clock()
        if not feed.seeded:
            await self._seed_feed(feed, discovered, finish)
        else:
            await self._diff_feed(feed, discovered)

        await self._finish_feed(feed, retailer, finish)
        return False

    async def _seed_feed(
        self,
        feed: FeedFetchInputs,
        discovered: list[DiscoveredProduct],
        seeded_at: datetime,
    ) -> None:
        """Baseline-seed a feed's first complete poll (no alerts, Req 2.1, 3.5)."""
        products = [
            BaselineProduct(
                external_id=product.external_id,
                title=product.title,
                handle=product.handle,
            )
            for product in discovered
            if product.external_id
        ]

        def _run(conn: sqlite3.Connection) -> None:
            write_feed_baseline(
                self._db,
                conn,
                feed_id=feed.id,
                retailer_id=feed.retailer_id,
                game_id=feed.game_id,
                products=products,
                seeded_at=seeded_at,
            )

        await self._db.run(_run)

    async def _diff_feed(
        self, feed: FeedFetchInputs, discovered: list[DiscoveredProduct]
    ) -> None:
        """Diff a seeded feed's crawl and fan out the new-product alerts (Req 3)."""

        def _run(conn: sqlite3.Connection) -> None:
            seen = _seen_external_ids(conn, feed.retailer_id, feed.game_id)
            tracked_ids, tracked_handles = _tracked_identities(conn, feed.retailer_id)
            diff = diff_feed(
                retailer_id=feed.retailer_id,
                game_id=feed.game_id,
                feed_id=feed.id,
                discovered=discovered,
                seen_external_ids=seen,
                include_keywords=feed.include_keywords,
                exclude_keywords=feed.exclude_keywords,
                tracked_external_ids=tracked_ids,
                tracked_handles=tracked_handles,
            )
            # Record every discovered product in seen_products (Req 3.5) in the SAME
            # executor callable, then fan out the alerts — both via the single writer.
            write_feed_baseline(
                self._db,
                conn,
                feed_id=feed.id,
                retailer_id=feed.retailer_id,
                game_id=feed.game_id,
                products=list(diff.to_record),
                seeded_at=self._clock(),
            )
            write_feed_diff(
                self._db,
                conn,
                diff=diff,
                game_id=feed.game_id,
                now=self._clock(),
            )

        await self._db.run(_run)

    async def _finish_feed(
        self, feed: FeedFetchInputs, retailer: RetailerFetchInputs, finish: datetime
    ) -> None:
        """Advance a feed's ``next_due_at`` and clear its backoff after a success (Req 1.7)."""
        interval = _effective_interval(feed.poll_interval_s, retailer)
        next_due = finish + timedelta(seconds=interval + self._jitter())

        def _run(conn: sqlite3.Connection) -> None:
            set_feed_next_due_at(conn, feed.id, next_due)
            write_backoff_state(
                conn,
                _success_backoff(
                    scope_key=f"{_SCOPE_KIND_FEED}:{feed.id}",
                    scope_kind=_SCOPE_KIND_FEED,
                    retailer_id=retailer.id,
                    feed_id=feed.id,
                    listing_id=None,
                ),
            )

        await self._db.run(_run)

    # -- listing processing ------------------------------------------------------

    async def _process_listing(
        self,
        adapter: Adapter,
        retailer: RetailerFetchInputs,
        listing: ListingFetchInputs,
        cycle_products: dict[str, DiscoveredProduct],
    ) -> bool:
        """Fetch, then seed/diff/rename one listing; return ``True`` if the adapter RAISED.

        On an adapter failure the backoff row for ``listing:<id>`` is recorded, the listing
        is NOT seeded, and no transition is emitted (Req 16, 2.4). On success a RENAME
        (``resolved_handle`` set) takes the dedicated handle-rename write path (Req 11.8);
        otherwise the listing is baseline-seeded on its first complete poll (Req 2.2) or
        diffed + fanned out thereafter (Req 4, 5, 7). ``next_due_at`` is advanced and
        backoff cleared on success (Req 1.7, 16.7).
        """
        try:
            state = await adapter.fetch_state(listing, cycle_products)
        except FetchError as exc:
            await self._record_failure(
                scope_key=scope_listing_key(listing.id),
                scope_kind=_SCOPE_KIND_LISTING,
                retailer_id=retailer.id,
                feed_id=None,
                listing_id=listing.id,
                poll_interval_s=_effective_interval(listing.poll_interval_s, retailer),
                exc=exc,
            )
            return True

        finish = self._clock()

        if state.resolved_handle is not None and state.resolved_url is not None:
            # A rename: take the dedicated watcher-level write path (Req 11.8, 7.7).
            await self._apply_rename(listing, state)
        elif not listing.seeded:
            await self._seed_listing(listing, state, finish)
        else:
            await self._diff_listing(listing, state, finish)

        await self._finish_listing(listing, retailer, finish)
        return False

    async def _apply_rename(
        self, listing: ListingFetchInputs, state: ProductState
    ) -> None:
        """Persist a handle rename + its one ``retailer:<id>`` admin notice (Req 11.8)."""
        assert state.resolved_url is not None  # guarded by the caller
        assert state.resolved_handle is not None
        notice = AdminNoticeRow(
            scope_key=scope_retailer_key(listing.retailer_id),
            kind=_HANDLE_CHANGE_KIND,
            message=(
                f"listing {listing.id} handle changed "
                f"{listing.handle!r} -> {state.resolved_handle!r}"
            ),
            created_at=self._clock(),
        )
        write = HandleRenameWrite(
            listing_id=listing.id,
            retailer_id=listing.retailer_id,
            resolved_url=state.resolved_url,
            resolved_handle=state.resolved_handle,
            notice=notice,
        )

        def _run(conn: sqlite3.Connection) -> None:
            write_handle_rename(self._db, conn, write)

        await self._db.run(_run)

    async def _seed_listing(
        self, listing: ListingFetchInputs, state: ProductState, seeded_at: datetime
    ) -> None:
        """Baseline-seed a listing's first complete poll (no alerts, Req 2.2, 2.3)."""
        existence = _stored_existence(state.existence)
        baseline = BaselineListing(
            listing_id=listing.id,
            in_stock=state.in_stock,
            price=state.price,
            existence=existence,
            seeded_at=seeded_at,
        )

        def _run(conn: sqlite3.Connection) -> None:
            write_listing_baseline(self._db, conn, baseline)

        await self._db.run(_run)

    async def _diff_listing(
        self, listing: ListingFetchInputs, state: ProductState, finish: datetime
    ) -> None:
        """Diff a seeded listing's fetch and fan out any transition (Req 4, 5, 7)."""

        def _run(conn: sqlite3.Connection) -> None:
            stored = read_listing_state(conn, listing.id)
            alert_state = read_alert_state(conn, listing_subject_key(listing.id))
            diff = diff_listing(
                fetched=state,
                stored=stored,
                alert_state=alert_state,
                now=finish,
                cooldown_s=listing.cooldown_s,
                listing_id=listing.id,
                retailer_id=listing.retailer_id,
                game_id=listing.game_id,
            )
            write_listing_diff(
                self._db,
                conn,
                diff=diff,
                game_id=listing.game_id,
                now=finish,
            )

        await self._db.run(_run)

    async def _finish_listing(
        self,
        listing: ListingFetchInputs,
        retailer: RetailerFetchInputs,
        finish: datetime,
    ) -> None:
        """Advance a listing's ``next_due_at`` and clear backoff after a success (Req 1.7)."""
        interval = _effective_interval(listing.poll_interval_s, retailer)
        next_due = finish + timedelta(seconds=interval + self._jitter())

        def _run(conn: sqlite3.Connection) -> None:
            set_listing_next_due_at(conn, listing.id, next_due)
            write_backoff_state(
                conn,
                _success_backoff(
                    scope_key=scope_listing_key(listing.id),
                    scope_kind=_SCOPE_KIND_LISTING,
                    retailer_id=retailer.id,
                    feed_id=None,
                    listing_id=listing.id,
                ),
            )

        await self._db.run(_run)

    # -- shared failure path -----------------------------------------------------

    async def _record_failure(
        self,
        *,
        scope_key: str,
        scope_kind: str,
        retailer_id: str,
        feed_id: int | None,
        listing_id: int | None,
        poll_interval_s: int,
        exc: FetchError,
    ) -> None:
        """Record an adapter failure as a ``backoff_state`` write; emit NO transition (Req 16).

        Classifies the exception into the backoff :class:`Outcome` (``Blocked`` and its
        ``SoftBlock`` subclass => the Blocked track honouring ``Retry-After``; any other
        :class:`FetchError` => the ordinary track), reads the scope's current backoff row,
        computes the next counters/delay with the PURE :func:`next_backoff`, and upserts the
        row. A failed FIRST poll never seeds and never writes stored state here.
        """
        now = self._clock()
        if isinstance(exc, Blocked):
            outcome = Outcome.BLOCKED
            retry_after = exc.retry_after
        else:
            outcome = Outcome.ORDINARY_ERROR
            retry_after = None

        def _run(conn: sqlite3.Connection) -> None:
            prior: BackoffStateRow | None = read_backoff_state(conn, scope_key)
            update = next_backoff(
                state=prior,
                poll_interval_s=poll_interval_s,
                outcome=outcome,
                now=now,
                retry_after=retry_after,
            )
            write_backoff_state(
                conn,
                BackoffStateUpsert(
                    scope_key=scope_key,
                    scope_kind=scope_kind,
                    retailer_id=retailer_id,
                    feed_id=feed_id,
                    listing_id=listing_id,
                    error_count=update.error_count,
                    blocked_count=update.blocked_count,
                    backoff_until=update.backoff_until,
                    last_outcome=update.last_outcome,
                ),
            )

        await self._db.run(_run)


# ---------------------------------------------------------------------------
# Module helpers (pure / synchronous)
# ---------------------------------------------------------------------------


def _bump(result: TickResult, *, feed: bool, failed: bool) -> TickResult:
    """Return ``result`` with one entity's outcome folded in (feed vs listing, failure)."""
    return TickResult(
        deferred=result.deferred,
        feeds_polled=result.feeds_polled + (1 if feed else 0),
        listings_polled=result.listings_polled + (0 if feed else 1),
        failures=result.failures + (1 if failed else 0),
    )


def _effective_interval(poll_interval_s: int, retailer: RetailerFetchInputs) -> int:
    """Return an entity's effective poll interval, floored at :data:`MIN_POLL_INTERVAL_S`.

    A non-positive configured interval falls back to the retailer's inherited default
    (itself floored), so the next-due cadence is never below the 30s floor (Req 15.2).
    """
    chosen = poll_interval_s if poll_interval_s > 0 else retailer.poll_interval_s
    return max(chosen, MIN_POLL_INTERVAL_S)


def _success_backoff(
    *,
    scope_key: str,
    scope_kind: str,
    retailer_id: str,
    feed_id: int | None,
    listing_id: int | None,
) -> BackoffStateUpsert:
    """Build the ``backoff_state`` upsert that a successful poll writes (Req 16.7).

    A success resets both the ordinary and blocked counters to zero and clears
    ``backoff_until`` so the scope is no longer deferred.
    """
    return BackoffStateUpsert(
        scope_key=scope_key,
        scope_kind=scope_kind,
        retailer_id=retailer_id,
        feed_id=feed_id,
        listing_id=listing_id,
        error_count=0,
        blocked_count=0,
        backoff_until=None,
        last_outcome="success",
    )


def _stored_existence(fetched: FetchExistence) -> StoredExistence:
    """Map a fetch-result :class:`Existence` to the stored-domain enum for seeding."""
    return (
        StoredExistence.EXISTING
        if fetched is FetchExistence.EXISTING
        else StoredExistence.NOT_FOUND
    )


def _seen_external_ids(
    conn: sqlite3.Connection, retailer_id: str, game_id: str
) -> frozenset[str]:
    """Read the ``seen_products`` external-id set for a retailer+game (feed diff input)."""
    rows = conn.execute(
        "SELECT external_id FROM seen_products WHERE retailer_id = ? AND game_id = ?",
        (retailer_id, game_id),
    ).fetchall()
    return frozenset(str(row["external_id"]) for row in rows)


def _tracked_identities(
    conn: sqlite3.Connection, retailer_id: str
) -> tuple[frozenset[str], frozenset[str]]:
    """Read enabled listings' ``external_id`` + ``handle`` sets (tracked-listing suppression).

    These are the identities :func:`diff_feed` uses to suppress a new-product alert for a
    product an enabled listing already covers (Req 3.6).
    """
    rows = conn.execute(
        "SELECT external_id, handle FROM listings "
        "WHERE retailer_id = ? AND enabled = 1",
        (retailer_id,),
    ).fetchall()
    external_ids = frozenset(
        str(row["external_id"]) for row in rows if row["external_id"] is not None
    )
    handles = frozenset(str(row["handle"]) for row in rows if row["handle"] is not None)
    return external_ids, handles
