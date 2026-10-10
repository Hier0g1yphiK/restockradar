"""Fan the PURE diff output into the typed atomic write-set the repository persists
(Req 6.1, 6.2, 8.1, 10.10).

The detection layer produces two frozen diff results —
:class:`~restockradar.detect.diff_feed.FeedDiffResult` and
:class:`~restockradar.detect.diff_listing.ListingDiffResult` — carrying the events that
fired plus the stored-state / ``alert_state`` write instructions. This module turns
those into the :class:`~restockradar.persistence.repository.WriteSet` /
:class:`~restockradar.persistence.repository.StateOnlyWrite` objects
:func:`~restockradar.persistence.repository.write_transition` /
:func:`~restockradar.persistence.repository.write_state_only` persist inside one
``BEGIN IMMEDIATE`` transaction.

The split mirrors the diff modules: the *build* functions here are **pure** — they take a
diff result, the already-resolved healthy destination ids, and the clock ``now``, and
RETURN the write-set objects with no I/O — so a test can assert the write-set directly.
The thin *wiring* coroutines (:func:`apply_listing_diff` / :func:`apply_feed_diff`) are the
only DB-touching code: they call
:func:`~restockradar.persistence.repository.read_healthy_destinations_for_game` and hand
the built write-sets to the repository's apply helpers.

Four rules govern the fan-out:

* **Destination resolution (Property 10, Req 6.2/8.1/10.10):** an alert on game ``G`` gets
  EXACTLY ONE :class:`~restockradar.persistence.repository.DeliveryTarget`
  (``status='pending'``, ``next_attempt_at=now``) per non-admin destination with
  ``game_id=G``, ``enabled=1``, ``health_status='healthy'`` — i.e. exactly the ids
  :func:`read_healthy_destinations_for_game` returns. Disabled / unhealthy / admin /
  other-game destinations get zero rows. Each distinct outbox alert gets its own full set
  of delivery rows.
* **Combined-alert rule (Req 4.10):** when a listing poll fired BOTH a
  :class:`~restockradar.detect.events.RestockEvent` and a
  :class:`~restockradar.detect.events.PriceChangeEvent`, ONE combined outbox row
  (``alert_kind='product'``, ``event_type='combined'``) carries both events' render data
  and gets ONE delivery set. If only one fired, that single alert is emitted
  (``'restock'`` / ``'price_change'``). A :class:`~restockradar.detect.events.PageLiveEvent`
  is always its own ``'page_live'`` alert, and a
  :class:`~restockradar.detect.events.NewProductEvent` its own ``'new_product'`` alert.
* **Outbox payload (per DDL):** ``alerts_outbox.payload`` is a JSON render-model TEXT the
  M3 Discord formatter (task 23) later consumes — title, url, image, prices, event detail —
  built with :func:`json.dumps` under a stable key order (``sort_keys=True``). This module
  does NO Discord formatting.
* **State vs state-only (Req 6.6/6.7):** a listing poll that fired at least one alert
  becomes a :class:`WriteSet` carrying the diff's single
  :class:`~restockradar.persistence.repository.StateDelta`, its ``alert_state`` upserts, and
  the outbox/history/deliveries. A poll that fired NO alert but still changed stored state
  (set/clear ``restock_pending``, the 404 counter, a sell-out) becomes a
  :class:`StateOnlyWrite` with no outbox/history/deliveries. New-product fan-out produces
  one :class:`WriteSet` per new-product alert (no :class:`StateDelta` — recording
  ``seen_products`` stays on the existing ``seed_feed`` / ``write_feed_baseline`` path).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from restockradar.domain.ids import EventType, SubjectKind, feed_product_subject_key
from restockradar.persistence.db import Database
from restockradar.persistence.repository import (
    DeliveryTarget,
    HistoryRow,
    OutboxRow,
    StateOnlyWrite,
    WriteSet,
    read_healthy_destinations_for_game,
    write_state_only,
    write_transition,
)

from .diff_feed import FeedDiffResult
from .diff_listing import ListingDiffResult
from .events import (
    NewProductEvent,
    PageLiveEvent,
    PriceChangeEvent,
    RestockEvent,
)

__all__ = [
    "ALERT_KIND_PRODUCT",
    "COMBINED_EVENT_TYPE",
    "apply_feed_diff",
    "apply_listing_diff",
    "build_feed_write_sets",
    "build_listing_write",
    "write_feed_diff",
    "write_listing_diff",
]

#: Every alert this module emits is a *product* alert (Req 8). Admin warnings (Req 17) are
#: fanned out elsewhere and never pass through here.
ALERT_KIND_PRODUCT = "product"

#: The ``event_type`` of the single outbox row emitted when a restock AND a price change
#: both fire in one listing poll (Req 4.10).
COMBINED_EVENT_TYPE = "combined"


# ===========================================================================
# Pure builders (no I/O) — return the write-set objects to persist
# ===========================================================================


def build_listing_write(
    *,
    diff: ListingDiffResult,
    destination_ids: Sequence[int],
    game_id: str,
    now: datetime,
) -> WriteSet | StateOnlyWrite:
    """Build the write for one listing poll's diff (PURE, Req 4.10, 6.6/6.7).

    Returns a :class:`WriteSet` when the poll fired at least one alert — carrying the diff's
    single :class:`~restockradar.persistence.repository.StateDelta`, every ``alert_state``
    upsert, and the outbox/history/deliveries for the one emitted alert — or a
    :class:`StateOnlyWrite` when nothing alerted but the stored state / ``alert_state``
    still changed (set/clear ``restock_pending``, the 404 counter, a sell-out).

    The combined rule (Req 4.10) collapses a simultaneous restock + price change into ONE
    outbox row (``event_type='combined'``) carrying both events; a lone restock, price
    change, or page-live becomes its single-typed alert. The one :class:`DeliveryTarget`
    per ``destination_ids`` entry is attached to that single alert.

    Args:
        diff: The pure :class:`ListingDiffResult` for this poll.
        destination_ids: The already-resolved healthy, enabled, non-admin destination ids
            for ``game_id`` (as returned by
            :func:`read_healthy_destinations_for_game`). Empty => no delivery rows, but the
            outbox/history alert is still written (Req 10.10).
        game_id: The alert's game id (written to ``alerts_outbox.game_id``).
        now: The tz-aware UTC instant stamped on the outbox / history / deliveries.

    Returns:
        A :class:`WriteSet` for an alerting poll, else a :class:`StateOnlyWrite`.
    """
    if not diff.events:
        # No alert fired: persist only the stored-state / alert_state change (Req 6.6/6.7).
        return StateOnlyWrite(state=diff.state, alert_state=diff.alert_state)

    restock = _first(diff.events, RestockEvent)
    price = _first(diff.events, PriceChangeEvent)
    page_live = _first(diff.events, PageLiveEvent)

    if page_live is not None:
        outbox, history = _page_live_rows(page_live, game_id=game_id, now=now)
    elif restock is not None and price is not None:
        # Combined restock + price change => ONE outbox row (Req 4.10).
        outbox, history = _combined_rows(restock, price, game_id=game_id, now=now)
    elif restock is not None:
        outbox, history = _restock_rows(restock, game_id=game_id, now=now)
    elif price is not None:
        # Only a price change fired.
        outbox, history = _price_rows(price, game_id=game_id, now=now)
    else:  # pragma: no cover - diff_listing never emits an empty event tuple here
        raise ValueError("listing diff reported events but none were recognised")

    return WriteSet(
        state=diff.state,
        alert_state=diff.alert_state,
        outbox=outbox,
        history=history,
        deliveries=_deliveries(destination_ids, now),
    )


def build_feed_write_sets(
    *,
    diff: FeedDiffResult,
    destination_ids: Sequence[int],
    now: datetime,
) -> tuple[WriteSet, ...]:
    """Build one :class:`WriteSet` per new-product alert in a feed diff (PURE, Req 3, 8.1).

    New-product alerts carry no listings :class:`~restockradar.persistence.repository.StateDelta`
    — recording ``seen_products`` stays on the existing ``seed_feed`` /
    ``write_feed_baseline`` path — so each returned write-set has ``state=None`` and no
    ``alert_state`` upsert, only the outbox/history row and the per-destination delivery
    set. The result is empty when the diff emitted no new-product events (a baseline cycle).

    Args:
        diff: The pure :class:`FeedDiffResult` for this feed cycle.
        destination_ids: The already-resolved healthy destination ids for the feed's game.
        now: The tz-aware UTC instant stamped on every outbox / history / delivery row.

    Returns:
        A tuple of :class:`WriteSet`, one per emitted new-product alert, in event order.
    """
    deliveries = _deliveries(destination_ids, now)
    write_sets: list[WriteSet] = []
    for event in diff.events:
        outbox, history = _new_product_rows(event, now=now)
        write_sets.append(
            WriteSet(outbox=outbox, history=history, deliveries=deliveries)
        )
    return tuple(write_sets)


# ===========================================================================
# Thin DB-touching wiring — resolve destinations + persist (run on executor)
# ===========================================================================


def write_listing_diff(
    db: Database,
    conn: sqlite3.Connection,
    *,
    diff: ListingDiffResult,
    game_id: str,
    now: datetime,
) -> None:
    """Resolve destinations and persist one listing diff on the executor (Req 6.1-6.3).

    Must be called on the DB executor thread (``conn`` is the owned connection). Reads the
    healthy destination set for ``game_id`` with the SAME connection, builds the write, and
    hands it to :func:`write_transition` (alerting) or :func:`write_state_only` (no alert)
    so everything lands in one transaction.
    """
    # No alert => no destinations to resolve; a state-only write touches no deliveries.
    destination_ids = (
        read_healthy_destinations_for_game(conn, game_id) if diff.events else []
    )
    write = build_listing_write(
        diff=diff, destination_ids=destination_ids, game_id=game_id, now=now
    )
    if isinstance(write, WriteSet):
        write_transition(db, conn, write)
    else:
        write_state_only(db, conn, write)


def write_feed_diff(
    db: Database,
    conn: sqlite3.Connection,
    *,
    diff: FeedDiffResult,
    game_id: str,
    now: datetime,
) -> None:
    """Resolve destinations and persist a feed diff's new-product alerts (Req 3, 6.1-6.3).

    Must be called on the DB executor thread. Each new-product alert becomes its own
    :func:`write_transition` call (its own outbox + delivery set). Recording
    ``seen_products`` is NOT done here — the watcher writes the baseline via the existing
    ``write_feed_baseline`` path.
    """
    if not diff.events:
        return
    destination_ids = read_healthy_destinations_for_game(conn, game_id)
    for write_set in build_feed_write_sets(
        diff=diff, destination_ids=destination_ids, now=now
    ):
        write_transition(db, conn, write_set)


async def apply_listing_diff(
    db: Database,
    *,
    diff: ListingDiffResult,
    game_id: str,
    now: datetime,
) -> None:
    """Async wrapper: resolve + persist one listing diff on the DB executor (Req 6.1-6.3).

    Resolves the healthy destinations and applies the built write inside a single executor
    transaction, so the destination read and the write cannot race another writer.
    """

    def _run(conn: sqlite3.Connection) -> None:
        write_listing_diff(db, conn, diff=diff, game_id=game_id, now=now)

    await db.run(_run)


async def apply_feed_diff(
    db: Database,
    *,
    diff: FeedDiffResult,
    game_id: str,
    now: datetime,
) -> None:
    """Async wrapper: resolve + persist a feed diff's new-product alerts (Req 3, 6.1-6.3)."""

    def _run(conn: sqlite3.Connection) -> None:
        write_feed_diff(db, conn, diff=diff, game_id=game_id, now=now)

    await db.run(_run)


# ===========================================================================
# Internal row builders
# ===========================================================================


def _deliveries(
    destination_ids: Sequence[int], now: datetime
) -> tuple[DeliveryTarget, ...]:
    """One pending :class:`DeliveryTarget` per resolved destination (Property 10, Req 8.1).

    The ids are exactly those :func:`read_healthy_destinations_for_game` returned, so this
    produces exactly one delivery row per enabled + healthy + non-admin destination for the
    alert's game and zero for everything else.
    """
    return tuple(
        DeliveryTarget(destination_id=dest_id, next_attempt_at=now)
        for dest_id in destination_ids
    )


def _price_text(price: Decimal | None) -> str | None:
    """Render a price as a plain string for the JSON payload (``None`` stays ``None``)."""
    return None if price is None else str(price)


def _payload(data: dict[str, object]) -> str:
    """Serialise a render-model dict to stable-ordered JSON TEXT (task 23 consumes it)."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def _restock_rows(
    event: RestockEvent, *, game_id: str, now: datetime
) -> tuple[OutboxRow, HistoryRow]:
    """Build the outbox + history rows for a lone restock alert (Req 4)."""
    payload = _payload(
        {
            "event": EventType.RESTOCK.value,
            "image": event.image,
            "price": _price_text(event.price),
            "title": event.title,
            "url": event.url,
        }
    )
    outbox = _listing_outbox(
        event_type=EventType.RESTOCK.value,
        game_id=game_id,
        listing_id=event.listing_id,
        payload=payload,
        now=now,
    )
    history = _listing_history(
        event_type=EventType.RESTOCK.value,
        listing_id=event.listing_id,
        detected_at=event.detected_at,
        detail=payload,
    )
    return outbox, history


def _price_rows(
    event: PriceChangeEvent, *, game_id: str, now: datetime
) -> tuple[OutboxRow, HistoryRow]:
    """Build the outbox + history rows for a lone price-change alert (Req 5)."""
    payload = _payload(
        {
            "event": EventType.PRICE_CHANGE.value,
            "image": event.image,
            "new_price": _price_text(event.new_price),
            "old_price": _price_text(event.old_price),
            "title": event.title,
            "url": event.url,
        }
    )
    outbox = _listing_outbox(
        event_type=EventType.PRICE_CHANGE.value,
        game_id=game_id,
        listing_id=event.listing_id,
        payload=payload,
        now=now,
    )
    history = _listing_history(
        event_type=EventType.PRICE_CHANGE.value,
        listing_id=event.listing_id,
        detected_at=event.detected_at,
        detail=payload,
    )
    return outbox, history


def _combined_rows(
    restock: RestockEvent,
    price: PriceChangeEvent,
    *,
    game_id: str,
    now: datetime,
) -> tuple[OutboxRow, HistoryRow]:
    """Build the ONE combined restock + price-change outbox + history row (Req 4.10).

    A single ``event_type='combined'`` row carries both events' render data so the M3
    formatter renders one message, and a single delivery set fans it out. The display
    fields come from the restock event (both events describe the same listing poll); the
    price block carries the old/new prices from the price-change event.
    """
    payload = _payload(
        {
            "event": COMBINED_EVENT_TYPE,
            "image": restock.image,
            "new_price": _price_text(price.new_price),
            "old_price": _price_text(price.old_price),
            "price": _price_text(restock.price),
            "title": restock.title,
            "url": restock.url,
        }
    )
    outbox = _listing_outbox(
        event_type=COMBINED_EVENT_TYPE,
        game_id=game_id,
        listing_id=restock.listing_id,
        payload=payload,
        now=now,
    )
    history = _listing_history(
        event_type=COMBINED_EVENT_TYPE,
        listing_id=restock.listing_id,
        detected_at=restock.detected_at,
        detail=payload,
    )
    return outbox, history


def _page_live_rows(
    event: PageLiveEvent, *, game_id: str, now: datetime
) -> tuple[OutboxRow, HistoryRow]:
    """Build the outbox + history rows for a page-live alert (Req 7)."""
    payload = _payload(
        {
            "event": EventType.PAGE_LIVE.value,
            "image": event.image,
            "in_stock": event.in_stock,
            "price": _price_text(event.price),
            "title": event.title,
            "url": event.url,
        }
    )
    outbox = _listing_outbox(
        event_type=EventType.PAGE_LIVE.value,
        game_id=game_id,
        listing_id=event.listing_id,
        payload=payload,
        now=now,
    )
    history = _listing_history(
        event_type=EventType.PAGE_LIVE.value,
        listing_id=event.listing_id,
        detected_at=event.detected_at,
        detail=payload,
    )
    return outbox, history


def _new_product_rows(
    event: NewProductEvent, *, now: datetime
) -> tuple[OutboxRow, HistoryRow]:
    """Build the outbox + history rows for a new-product alert (Req 3).

    The outbox/history carry the ``feed_product`` subject: ``feed_id`` + ``external_id`` and
    the alert's ``game_id`` (``listing_id`` is NULL — a new product has no tracked listing
    yet). The ``subject_kind`` is ``'feed_product'`` and the subject key is the one
    :func:`feed_product_subject_key` builds, recorded in the history detail for traceability.
    """
    payload = _payload(
        {
            "event": EventType.NEW_PRODUCT.value,
            "external_id": event.external_id,
            "handle": event.handle,
            "image": event.image,
            "title": event.title,
            "url": event.url,
        }
    )
    subject_key = feed_product_subject_key(event.feed_id, event.external_id)
    outbox = OutboxRow(
        alert_kind=ALERT_KIND_PRODUCT,
        event_type=EventType.NEW_PRODUCT.value,
        game_id=event.game_id,
        subject_kind=SubjectKind.FEED_PRODUCT.value,
        listing_id=None,
        feed_id=event.feed_id,
        external_id=event.external_id,
        payload=payload,
        created_at=now,
    )
    history = HistoryRow(
        subject_kind=SubjectKind.FEED_PRODUCT.value,
        listing_id=None,
        feed_id=event.feed_id,
        external_id=event.external_id,
        event_type=EventType.NEW_PRODUCT.value,
        detected_at=now,
        detail=_payload({"subject_key": subject_key}),
    )
    return outbox, history


def _listing_outbox(
    *,
    event_type: str,
    game_id: str,
    listing_id: int,
    payload: str,
    now: datetime,
) -> OutboxRow:
    """Build a listing-subject :class:`OutboxRow` (restock / price / page-live / combined)."""
    return OutboxRow(
        alert_kind=ALERT_KIND_PRODUCT,
        event_type=event_type,
        game_id=game_id,
        subject_kind=SubjectKind.LISTING.value,
        listing_id=listing_id,
        feed_id=None,
        external_id=None,
        payload=payload,
        created_at=now,
    )


def _listing_history(
    *,
    event_type: str,
    listing_id: int,
    detected_at: datetime,
    detail: str,
) -> HistoryRow:
    """Build a listing-subject :class:`HistoryRow` mirroring the emitted outbox row."""
    return HistoryRow(
        subject_kind=SubjectKind.LISTING.value,
        listing_id=listing_id,
        feed_id=None,
        external_id=None,
        event_type=event_type,
        detected_at=detected_at,
        detail=detail,
    )


def _first[EventT](events: Sequence[object], kind: type[EventT]) -> EventT | None:
    """Return the first event of ``kind`` in ``events``, or ``None`` if absent."""
    for event in events:
        if isinstance(event, kind):
            return event
    return None
