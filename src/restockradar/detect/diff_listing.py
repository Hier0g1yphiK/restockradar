"""Restock / price / page-live transition logic with cooldown + restock_pending,
A->B->A flap suppression, the two-consecutive-404 existence flip, and the combined-
alert rule (Req 4, 5, 7, 9).

This module is a **pure** function: :func:`diff_listing` takes a single poll's fetched
:class:`~restockradar.adapters.base.ProductState`, the listing's STORED
:class:`~restockradar.persistence.repository.ListingStateRow` snapshot (``None`` while
unseeded), the current cooldown-authority ``alert_state`` rows, the clock, and the
per-subject cooldown, and RETURNS a frozen :class:`ListingDiffResult` carrying the events
that fired this poll plus the stored-state and ``alert_state`` write instructions task 17
will wrap into a :class:`~restockradar.persistence.repository.WriteSet`. It opens no
connection, performs no I/O, and reads neither the clock nor a cooldown from anywhere but
its arguments, so the same inputs always yield the same result (the property tests depend
on this purity).

The two ``Existence`` enums in play are deliberately distinct and never mixed:

* :class:`restockradar.adapters.base.Existence` is the FETCH result (``EXISTING`` /
  ``NOT_FOUND``) carried on the :class:`ProductState`;
* :class:`restockradar.domain.models.Existence` is the STORED column value carried on the
  :class:`ListingStateRow` and written back via :class:`StateDelta`.

The logic follows the design's Restock State Machine, price-change rule, and page-live
interaction exactly:

* **Existence / two-consecutive-404 flip (Req 7.6, Property 20):** a fetched ``NOT_FOUND``
  increments ``consecutive_404``; the stored existence only flips to ``NOT_FOUND`` once
  that counter reaches two. A fetched ``EXISTING`` resets the counter to zero. While the
  fetch is ``NOT_FOUND`` the diff writes NEITHER ``in_stock`` NOR ``price`` — the
  :class:`StateDelta` touches only ``existence`` / ``consecutive_404`` (Req 7.4). A lone
  transient 404 on a previously-``EXISTING`` page changes nothing material and emits no
  event.
* **Page-live one-shot (Req 7.3-7.5, Property 22):** when the stored existence is
  not-``EXISTING`` (unseeded/``NOT_FOUND``) and the fetch is ``EXISTING``, a single
  :class:`PageLiveEvent` fires carrying the fetched stock+price; the SAME result sets
  ``listings.in_stock`` / ``listings.price`` to the fetched values and upserts BOTH
  ``alert_state`` rows (restock row ``pending=0`` + ``last_alert_at`` NULL; price_change
  row ``last_alerted_value`` = fetched price). NO separate restock or price-change alert
  fires that poll.
* **Restock machine (Req 4):** stored ``in_stock`` false -> fetched true is a restock
  transition. With no prior restock alert or the cooldown elapsed, a
  :class:`RestockEvent` fires and ``last_alert_at`` is set to ``now``; within the cooldown
  no alert fires and ``restock_pending`` is set. While pending and still in stock, the
  trailing restock fires exactly once the cooldown elapses and pending clears; a sell-out
  first clears pending with no alert. Selling out stores false with no out-of-stock
  alert; staying in stock with ``pending`` false emits nothing.
* **Price change (Req 5):** the fetched price is compared to the price_change row's
  ``last_alerted_value`` (the authoritative last_alerted_price, NOT ``listings.price``).
  A difference with the cooldown elapsed fires a :class:`PriceChangeEvent` and records the
  new ``last_alerted_value`` + ``last_alert_at``; an A->B->A flap back to the last alerted
  value yields no alert.
* **Combined-alert rule (Req 4.10):** when a restock AND a price change both actually fire
  in one poll, both appear in :attr:`ListingDiffResult.events`; task 17 combines them into
  a single outbox message. If either is deferred, only the actually-emitted event is
  present here.
* **Last-observed cache (Req 4.6, 5):** on any ``EXISTING`` poll the result always
  refreshes ``listings.in_stock`` / ``listings.price`` to the fetched (last-observed)
  values even when no alert fires — only ``NOT_FOUND`` polls leave them untouched.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from restockradar.adapters.base import Existence as FetchExistence
from restockradar.adapters.base import ProductState
from restockradar.domain.ids import EventType, SubjectKind, listing_subject_key
from restockradar.domain.models import Existence as StoredExistence
from restockradar.persistence.repository import (
    AlertStateRow,
    AlertStateUpsert,
    ListingStateRow,
    StateDelta,
)

from .events import PageLiveEvent, PriceChangeEvent, RestockEvent

__all__ = ["DEFAULT_COOLDOWN_S", "ListingDiffResult", "ListingEvent", "diff_listing"]

#: The default per-subject cooldown in seconds when the caller passes ``None`` (Req 9.7).
#: Lives here, not in Settings (confirmed): the restock/price cooldown is a detection-layer
#: policy constant that a per-subject override can replace.
DEFAULT_COOLDOWN_S = 300

#: The subject kind every ``alert_state`` row this module writes carries (Req 14.10): the
#: diff only ever keys cooldown rows to a listing subject.
_LISTING_SUBJECT_KIND = SubjectKind.LISTING

#: The product events :func:`diff_listing` can emit (never a NewProductEvent — that is the
#: feed diff's job). An AdminWarning is likewise out of scope for this transition diff.
ListingEvent = RestockEvent | PriceChangeEvent | PageLiveEvent


@dataclass(frozen=True, slots=True, kw_only=True)
class ListingDiffResult:
    """The pure output of :func:`diff_listing` (Req 4, 5, 7).

    ``events`` holds the transition events that ACTUALLY fired this poll, in detection
    order (page-live, else restock before price-change). When a restock and a price change
    both fire, both are present and task 17 combines them into one outbox message
    (Req 4.10). ``state`` is the stored-listing column update to apply, or ``None`` when
    nothing stored changes. ``alert_state`` holds the keyed ``alert_state`` upserts (the
    cooldown authority) to apply — zero, one, or both of the restock / price_change rows.
    All three are immutable so the result is cheap to share with the fan-out layer.
    """

    events: tuple[ListingEvent, ...]
    state: StateDelta | None
    alert_state: tuple[AlertStateUpsert, ...]


def diff_listing(
    *,
    fetched: ProductState,
    stored: ListingStateRow | None,
    alert_state: dict[EventType, AlertStateRow],
    now: datetime,
    cooldown_s: int | None = None,
    listing_id: int,
    retailer_id: str,
    game_id: str,
    url: str | None = None,
    title: str | None = None,
    image: str | None = None,
) -> ListingDiffResult:
    """Diff one listing poll into the events + write instructions it produces (PURE).

    Args:
        fetched: The adapter's fetch result for this poll. ``fetched.existence`` is the
            :class:`restockradar.adapters.base.Existence` fetch outcome; its ``in_stock`` /
            ``price`` are placeholders to IGNORE when ``existence`` is ``NOT_FOUND``.
        stored: The listing's stored (last-observed) snapshot, or ``None`` while unseeded.
            An unseeded listing has no stored ``in_stock`` / ``price`` / ``existence``.
        alert_state: The listing's current cooldown-authority rows keyed by
            :class:`EventType` (``RESTOCK`` / ``PRICE_CHANGE``), as returned by
            ``read_alert_state``. A missing key means that row does not exist yet.
        now: The current tz-aware UTC instant; the sole clock the diff consults.
        cooldown_s: The per-subject cooldown in seconds; ``None`` => ``DEFAULT_COOLDOWN_S``
            (Req 9.7).
        listing_id: The listing's id (carried on every event and write instruction).
        retailer_id: The listing's retailer id (carried on each event).
        game_id: The listing's game id (carried on each event).
        url: Display url for the event; falls back to ``fetched.url`` when ``None``.
        title: Display title for the event; falls back to ``fetched.title`` when ``None``.
        image: Display image for the event; falls back to ``fetched.image`` when ``None``.

    Returns:
        A :class:`ListingDiffResult` carrying the events that fired plus the
        :class:`StateDelta` / :class:`AlertStateUpsert` instructions to persist. Nothing is
        written here — task 17's fan-out wraps the result in a write-set.
    """
    cooldown = DEFAULT_COOLDOWN_S if cooldown_s is None else cooldown_s
    subject_key = listing_subject_key(listing_id)
    display_url = url if url is not None else fetched.url
    display_title = title if title is not None else fetched.title
    display_image = image if image is not None else fetched.image

    restock_row = alert_state.get(EventType.RESTOCK)
    price_row = alert_state.get(EventType.PRICE_CHANGE)

    # ------------------------------------------------------------------ NOT_FOUND poll
    if fetched.existence is FetchExistence.NOT_FOUND:
        return _diff_not_found(
            stored=stored,
            listing_id=listing_id,
        )

    # ------------------------------------------------------------------ EXISTING poll
    stored_existence = stored.existence if stored is not None else None
    stored_in_stock = stored.in_stock if stored is not None else None
    page_was_live = stored_existence is StoredExistence.EXISTING

    # Reset the 404 counter on any EXISTING fetch and mark existence EXISTING.
    base_consecutive_404 = 0

    if not page_was_live:
        # Page-live one-shot: unseeded/NOT_FOUND -> EXISTING (Req 7.3-7.5, Property 22).
        return _diff_page_live(
            fetched=fetched,
            subject_key=subject_key,
            now=now,
            listing_id=listing_id,
            retailer_id=retailer_id,
            game_id=game_id,
            display_url=display_url,
            display_title=display_title,
            display_image=display_image,
            consecutive_404=base_consecutive_404,
        )

    # The page was already live: run the restock + price-change machines (Req 4, 5).
    events: list[ListingEvent] = []
    alert_upserts: list[AlertStateUpsert] = []

    # --- Restock machine (Req 4) --------------------------------------------------
    restock_upsert = _diff_restock(
        fetched=fetched,
        stored_in_stock=stored_in_stock,
        restock_row=restock_row,
        now=now,
        cooldown=cooldown,
        subject_key=subject_key,
        listing_id=listing_id,
        retailer_id=retailer_id,
        game_id=game_id,
        display_url=display_url,
        display_title=display_title,
        display_image=display_image,
        events=events,
    )
    if restock_upsert is not None:
        alert_upserts.append(restock_upsert)

    # --- Price-change machine (Req 5) ---------------------------------------------
    price_upsert = _diff_price(
        fetched=fetched,
        price_row=price_row,
        now=now,
        cooldown=cooldown,
        subject_key=subject_key,
        listing_id=listing_id,
        retailer_id=retailer_id,
        game_id=game_id,
        display_url=display_url,
        display_title=display_title,
        display_image=display_image,
        events=events,
    )
    if price_upsert is not None:
        alert_upserts.append(price_upsert)

    # Always refresh the last-observed cache on an EXISTING poll (Req 4.6, 5), and keep the
    # existence column EXISTING with a reset 404 counter.
    state = StateDelta(
        listing_id=listing_id,
        in_stock=fetched.in_stock,
        set_in_stock=True,
        price=fetched.price,
        set_price=True,
        existence=StoredExistence.EXISTING,
        set_existence=True,
        consecutive_404=base_consecutive_404,
    )

    return ListingDiffResult(
        events=tuple(events),
        state=state,
        alert_state=tuple(alert_upserts),
    )


def _diff_not_found(
    *,
    stored: ListingStateRow | None,
    listing_id: int,
) -> ListingDiffResult:
    """Handle a fetched ``NOT_FOUND`` poll (Req 7.4, 7.6, Property 20).

    Increments ``consecutive_404`` and flips stored existence to ``NOT_FOUND`` only once
    the counter reaches two. Writes NEITHER ``in_stock`` NOR ``price`` — the
    :class:`StateDelta` touches only ``existence`` / ``consecutive_404`` — so a listing's
    last-observed stock/price survive untouched while its page is missing. Emits no event.
    """
    prior_404 = stored.consecutive_404 if stored is not None else 0
    new_404 = prior_404 + 1

    set_existence = new_404 >= 2
    state = StateDelta(
        listing_id=listing_id,
        existence=StoredExistence.NOT_FOUND if set_existence else None,
        set_existence=set_existence,
        consecutive_404=new_404,
    )
    return ListingDiffResult(events=(), state=state, alert_state=())


def _diff_page_live(
    *,
    fetched: ProductState,
    subject_key: str,
    now: datetime,
    listing_id: int,
    retailer_id: str,
    game_id: str,
    display_url: str,
    display_title: str | None,
    display_image: str | None,
    consecutive_404: int,
) -> ListingDiffResult:
    """Handle the page-live one-shot (Req 7.3-7.5, Property 22).

    Emits EXACTLY ONE :class:`PageLiveEvent` carrying the fetched stock+price, sets
    ``listings.in_stock`` / ``listings.price`` to the fetched values + existence
    ``EXISTING``, and upserts BOTH ``alert_state`` rows so the cooldown authority starts
    clean: the restock row with ``pending=0`` and ``last_alert_at`` NULL, and the
    price_change row with ``last_alerted_value`` seeded to the fetched price. NO separate
    restock or price-change alert fires this poll.
    """
    event = PageLiveEvent(
        listing_id=listing_id,
        retailer_id=retailer_id,
        game_id=game_id,
        in_stock=fetched.in_stock,
        price=fetched.price,
        url=display_url,
        title=display_title,
        image=display_image,
        detected_at=now,
    )

    state = StateDelta(
        listing_id=listing_id,
        in_stock=fetched.in_stock,
        set_in_stock=True,
        price=fetched.price,
        set_price=True,
        existence=StoredExistence.EXISTING,
        set_existence=True,
        consecutive_404=consecutive_404,
    )

    restock_upsert = AlertStateUpsert(
        subject_key=subject_key,
        event_type=EventType.RESTOCK,
        subject_kind=_LISTING_SUBJECT_KIND,
        listing_id=listing_id,
        last_alert_at=None,
        set_last_alert_at=True,
        pending=False,
        set_pending=True,
    )
    price_upsert = AlertStateUpsert(
        subject_key=subject_key,
        event_type=EventType.PRICE_CHANGE,
        subject_kind=_LISTING_SUBJECT_KIND,
        listing_id=listing_id,
        last_alerted_value=fetched.price,
        set_last_alerted_value=True,
    )

    return ListingDiffResult(
        events=(event,),
        state=state,
        alert_state=(restock_upsert, price_upsert),
    )


def _diff_restock(
    *,
    fetched: ProductState,
    stored_in_stock: bool | None,
    restock_row: AlertStateRow | None,
    now: datetime,
    cooldown: int,
    subject_key: str,
    listing_id: int,
    retailer_id: str,
    game_id: str,
    display_url: str,
    display_title: str | None,
    display_image: str | None,
    events: list[ListingEvent],
) -> AlertStateUpsert | None:
    """Run the restock state machine for one EXISTING poll (Req 4).

    Appends a :class:`RestockEvent` to ``events`` when one fires and returns the restock
    ``alert_state`` upsert to apply, or ``None`` when the restock row is unchanged. The
    page is already live here (page-live is handled upstream), so this only decides the
    out_of_stock / in_stock / restock_pending transitions.
    """
    pending = restock_row.pending if restock_row is not None else False
    last_alert_at = restock_row.last_alert_at if restock_row is not None else None
    cooldown_elapsed = _cooldown_elapsed(last_alert_at, now, cooldown)

    if fetched.in_stock:
        if stored_in_stock:
            # Still in stock. A deferred (pending) trailing restock fires once the cooldown
            # elapses, else we keep waiting (Req 4.5). No transition otherwise (Req 4.9).
            if pending and cooldown_elapsed:
                events.append(
                    _restock_event(
                        now=now,
                        fetched=fetched,
                        listing_id=listing_id,
                        retailer_id=retailer_id,
                        game_id=game_id,
                        display_url=display_url,
                        display_title=display_title,
                        display_image=display_image,
                    )
                )
                return _restock_upsert(
                    subject_key=subject_key,
                    listing_id=listing_id,
                    last_alert_at=now,
                    pending=False,
                )
            return None

        # out_of_stock -> in_stock: a restock transition (Req 4.1, 4.3).
        if last_alert_at is None or cooldown_elapsed:
            events.append(
                _restock_event(
                    now=now,
                    fetched=fetched,
                    listing_id=listing_id,
                    retailer_id=retailer_id,
                    game_id=game_id,
                    display_url=display_url,
                    display_title=display_title,
                    display_image=display_image,
                )
            )
            return _restock_upsert(
                subject_key=subject_key,
                listing_id=listing_id,
                last_alert_at=now,
                pending=False,
            )
        # Within cooldown: defer — set restock_pending, no alert (Req 4.3).
        return _restock_upsert(
            subject_key=subject_key,
            listing_id=listing_id,
            pending=True,
        )

    # fetched out of stock. Selling out stores false with NO out-of-stock alert (Req 4.6);
    # if a trailing restock was pending it is cleared with no alert (Req 4.4).
    if pending:
        return _restock_upsert(
            subject_key=subject_key,
            listing_id=listing_id,
            pending=False,
        )
    return None


def _diff_price(
    *,
    fetched: ProductState,
    price_row: AlertStateRow | None,
    now: datetime,
    cooldown: int,
    subject_key: str,
    listing_id: int,
    retailer_id: str,
    game_id: str,
    display_url: str,
    display_title: str | None,
    display_image: str | None,
    events: list[ListingEvent],
) -> AlertStateUpsert | None:
    """Run the price-change machine for one EXISTING poll (Req 5, Property 8).

    Compares the fetched price to the price_change row's ``last_alerted_value`` (the
    authoritative last_alerted_price, never ``listings.price``). A difference with the
    cooldown elapsed fires a :class:`PriceChangeEvent` and records the new value + alert
    time; an A->B->A flap back to the last alerted value is equal and so fires nothing.
    """
    if fetched.price is None:
        # No priced variant this poll => nothing to compare, no price-change alert.
        return None

    last_alerted = price_row.last_alerted_value if price_row is not None else None
    last_alert_at = price_row.last_alert_at if price_row is not None else None

    if last_alerted is not None and last_alerted == fetched.price:
        # Equal exact Decimal (incl. an A->B->A reversion) => no event (Property 8).
        return None

    if not _cooldown_elapsed(last_alert_at, now, cooldown):
        # Within the cooldown => suppress the price-change alert (Req 9.3/9.5).
        return None

    events.append(
        PriceChangeEvent(
            listing_id=listing_id,
            retailer_id=retailer_id,
            game_id=game_id,
            old_price=last_alerted,
            new_price=fetched.price,
            url=display_url,
            title=display_title,
            image=display_image,
            detected_at=now,
        )
    )
    return AlertStateUpsert(
        subject_key=subject_key,
        event_type=EventType.PRICE_CHANGE,
        subject_kind=_LISTING_SUBJECT_KIND,
        listing_id=listing_id,
        last_alert_at=now,
        set_last_alert_at=True,
        last_alerted_value=fetched.price,
        set_last_alerted_value=True,
    )


def _restock_event(
    *,
    now: datetime,
    fetched: ProductState,
    listing_id: int,
    retailer_id: str,
    game_id: str,
    display_url: str,
    display_title: str | None,
    display_image: str | None,
) -> RestockEvent:
    """Build the :class:`RestockEvent` for a fired restock transition (Req 4)."""
    return RestockEvent(
        listing_id=listing_id,
        retailer_id=retailer_id,
        game_id=game_id,
        price=fetched.price,
        url=display_url,
        title=display_title,
        image=display_image,
        detected_at=now,
    )


def _restock_upsert(
    *,
    subject_key: str,
    listing_id: int,
    last_alert_at: datetime | None = None,
    pending: bool,
) -> AlertStateUpsert:
    """Build a restock-row :class:`AlertStateUpsert`, touching ``pending`` always and
    ``last_alert_at`` only when a restock actually fired (``last_alert_at`` supplied).
    """
    return AlertStateUpsert(
        subject_key=subject_key,
        event_type=EventType.RESTOCK,
        subject_kind=_LISTING_SUBJECT_KIND,
        listing_id=listing_id,
        last_alert_at=last_alert_at,
        set_last_alert_at=last_alert_at is not None,
        pending=pending,
        set_pending=True,
    )


def _cooldown_elapsed(last_alert_at: datetime | None, now: datetime, cooldown: int) -> bool:
    """Whether ``cooldown`` seconds have elapsed since ``last_alert_at`` (Req 9.3, 9.7).

    With no prior alert the cooldown is trivially elapsed. The boundary is inclusive: a
    gap of exactly ``cooldown`` seconds counts as elapsed, so the next alert is allowed.
    """
    if last_alert_at is None:
        return True
    return (now - last_alert_at).total_seconds() >= cooldown
