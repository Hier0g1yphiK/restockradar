"""Task 16 — restock / price / page-live transition tests for
:mod:`restockradar.detect.diff_listing`.

Drives the PURE :func:`restockradar.detect.diff_listing.diff_listing` directly with
hand-built :class:`~restockradar.adapters.base.ProductState` /
:class:`~restockradar.persistence.repository.ListingStateRow` /
:class:`~restockradar.persistence.repository.AlertStateRow` inputs and asserts on the
returned events + :class:`StateDelta` + :class:`AlertStateUpsert`. No DB round-trips are
needed (that is task 17) — every assertion is a pure-function assertion.

Covered properties and example suites:

* 16.2 **Property 6** — no duplicate alert within the cooldown. Validates Req 9.3, 9.7.
* 16.3 **Property 7** — the deferred (restock_pending) trailing alert fires exactly once,
  and zero if the item sells out first (pending cleared). Validates Req 4.3, 4.4, 4.5.
* 16.4 **Property 8** — an A->B->A price flap within the cooldown yields no alert.
  Validates Req 9.5.
* 16.5 **Property 9** — restock -> sell-out -> restock, both gaps beyond the cooldown,
  yields two alerts. Validates Req 4.7.
* 16.6 **Property 22** — a page going live produces exactly one alert and no stock/price
  alert that poll (and upserts both ``alert_state`` rows + sets listings.in_stock/price).
  Validates Req 7.3, 7.4, 7.5.
* 16.7 **Property 20** — existence flips to not-found only after two consecutive 404s; a
  lone 404 then EXISTING changes nothing. Validates Req 7.6.
* 16.9 **restock/price/cooldown example tests**. Validates Req 21.6-21.9, 21.18, 21.25,
  21.26.
* 16.10 **page-live 404 example tests**. Validates Req 21.19, 21.27, 7.3.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.base import Existence as FetchExistence
from restockradar.adapters.base import ProductState
from restockradar.detect.diff_listing import (
    DEFAULT_COOLDOWN_S,
    ListingDiffResult,
    diff_listing,
)
from restockradar.detect.events import PageLiveEvent, PriceChangeEvent, RestockEvent
from restockradar.domain.ids import EventType, listing_subject_key
from restockradar.domain.models import Existence as StoredExistence
from restockradar.persistence.repository import (
    AlertStateRow,
    AlertStateUpsert,
    ListingStateRow,
)

_LISTING_ID = 1
_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_URL = "https://www.northerncardgaming.com/products/booster"
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_COOLDOWN = DEFAULT_COOLDOWN_S


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _fetched(
    *,
    existence: FetchExistence = FetchExistence.EXISTING,
    in_stock: bool = False,
    price: Decimal | None = None,
) -> ProductState:
    """A minimal fetched ``ProductState`` (variants empty — the diff reads in_stock/price)."""
    return ProductState(
        existence=existence,
        in_stock=in_stock,
        price=price,
        variants=(),
        url=_URL,
        image=None,
        title="Booster Box",
        external_id="123",
    )


def _stored(
    *,
    in_stock: bool | None,
    price: Decimal | None = None,
    existence: StoredExistence | None = StoredExistence.EXISTING,
    consecutive_404: int = 0,
    seeded: bool = True,
) -> ListingStateRow:
    return ListingStateRow(
        listing_id=_LISTING_ID,
        in_stock=in_stock,
        price=price,
        existence=existence,
        consecutive_404=consecutive_404,
        seeded_at=_NOW if seeded else None,
    )


def _restock_row(
    *, last_alert_at: datetime | None, pending: bool = False
) -> AlertStateRow:
    return AlertStateRow(
        subject_key=listing_subject_key(_LISTING_ID),
        event_type=EventType.RESTOCK,
        last_alert_at=last_alert_at,
        last_alerted_value=None,
        pending=pending,
    )


def _price_row(
    *, last_alert_at: datetime | None, last_alerted_value: Decimal | None
) -> AlertStateRow:
    return AlertStateRow(
        subject_key=listing_subject_key(_LISTING_ID),
        event_type=EventType.PRICE_CHANGE,
        last_alert_at=last_alert_at,
        last_alerted_value=last_alerted_value,
        pending=False,
    )


def _run(
    *,
    fetched: ProductState,
    stored: ListingStateRow | None,
    alert_state: dict[EventType, AlertStateRow] | None = None,
    now: datetime = _NOW,
    cooldown_s: int | None = None,
) -> ListingDiffResult:
    return diff_listing(
        fetched=fetched,
        stored=stored,
        alert_state=alert_state or {},
        now=now,
        cooldown_s=cooldown_s,
        listing_id=_LISTING_ID,
        retailer_id=_RETAILER_ID,
        game_id=_GAME_ID,
    )


def _restock_upsert(result: ListingDiffResult) -> AlertStateUpsert:
    return next(u for u in result.alert_state if u.event_type is EventType.RESTOCK)


def _price_upsert(result: ListingDiffResult) -> AlertStateUpsert:
    return next(u for u in result.alert_state if u.event_type is EventType.PRICE_CHANGE)


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

_prices = st.decimals(
    min_value=Decimal("0.01"),
    max_value=Decimal("9999.99"),
    places=2,
    allow_nan=False,
    allow_infinity=False,
)
_cooldowns = st.integers(min_value=1, max_value=3600)


# ===========================================================================
# 16.2 — Property 6: no duplicate alert within the cooldown
# ===========================================================================


@settings(max_examples=200)
@given(cooldown=_cooldowns, gap=st.integers(min_value=0), price=_prices)
def test_property_6_no_duplicate_alert_within_cooldown(
    cooldown: int, gap: int, price: Decimal
) -> None:
    """Feature: stock-drop-monitor, Property 6: No duplicate alert within the cooldown.

    Validates: Requirements 9.3, 9.7

    *For any* restock whose previous restock alert fired less than ``cooldown`` seconds
    ago, re-observing a fresh out-of-stock -> in-stock transition emits NO restock alert
    (the duplicate is suppressed); the restock row instead records ``pending=True``. The
    same bound governs the price machine: a price change within the cooldown emits no
    alert.
    """
    last_alert = _NOW
    now = _NOW + timedelta(seconds=min(gap, cooldown - 1))  # strictly within the cooldown

    result = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
        alert_state={
            EventType.RESTOCK: _restock_row(last_alert_at=last_alert),
            EventType.PRICE_CHANGE: _price_row(
                last_alert_at=last_alert, last_alerted_value=price + Decimal("1.00")
            ),
        },
        now=now,
        cooldown_s=cooldown,
    )

    # No restock alert within the cooldown; instead a pending flag is set (Req 9.3).
    assert not any(isinstance(e, RestockEvent) for e in result.events)
    assert _restock_upsert(result).pending is True
    # No price-change alert within the cooldown either, despite a different price.
    assert not any(isinstance(e, PriceChangeEvent) for e in result.events)


# ===========================================================================
# 16.3 — Property 7: deferred trailing restock fires exactly once
# ===========================================================================


@settings(max_examples=200)
@given(cooldown=_cooldowns, price=_prices)
def test_property_7_deferred_trailing_restock_fires_exactly_once(
    cooldown: int, price: Decimal
) -> None:
    """Feature: stock-drop-monitor, Property 7: The deferred (restock_pending) trailing
    alert fires exactly once.

    Validates: Requirements 4.3, 4.4, 4.5

    *For any* listing left in the restock_pending state (in stock, a restock suppressed by
    cooldown), once the cooldown has elapsed exactly ONE trailing restock alert fires and
    pending clears; while the cooldown still holds no alert fires; and if the item sells
    out before the cooldown elapses pending is cleared with NO alert.
    """
    pending_state = {EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=True)}

    # (a) Still pending, still in stock, cooldown elapsed => exactly one trailing restock.
    fired = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=True, price=price),
        alert_state=pending_state,
        now=_NOW + timedelta(seconds=cooldown),
        cooldown_s=cooldown,
    )
    restocks = [e for e in fired.events if isinstance(e, RestockEvent)]
    assert len(restocks) == 1
    assert _restock_upsert(fired).pending is False
    assert _restock_upsert(fired).last_alert_at == _NOW + timedelta(seconds=cooldown)

    # (b) Still pending, still in stock, cooldown NOT elapsed => no alert, still waiting.
    waiting = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=True, price=price),
        alert_state=pending_state,
        now=_NOW + timedelta(seconds=cooldown - 1) if cooldown > 1 else _NOW,
        cooldown_s=cooldown,
    )
    assert not any(isinstance(e, RestockEvent) for e in waiting.events)

    # (c) Sells out while pending => pending cleared, NO alert (Req 4.4).
    sold_out = _run(
        fetched=_fetched(in_stock=False, price=price),
        stored=_stored(in_stock=True, price=price),
        alert_state=pending_state,
        now=_NOW + timedelta(seconds=cooldown),
        cooldown_s=cooldown,
    )
    assert not any(isinstance(e, RestockEvent) for e in sold_out.events)
    assert _restock_upsert(sold_out).pending is False


# ===========================================================================
# 16.4 — Property 8: A->B->A price flap within the cooldown yields no alert
# ===========================================================================


@settings(max_examples=200)
@given(
    cooldown=_cooldowns,
    price_a=_prices,
    price_b=_prices,
)
def test_property_8_price_flap_within_cooldown_no_alert(
    cooldown: int, price_a: Decimal, price_b: Decimal
) -> None:
    """Feature: stock-drop-monitor, Property 8: An A->B->A price flap within the cooldown
    yields no alert.

    Validates: Requirements 9.5

    *For any* two prices A and B, if the last ALERTED price is A and the newly observed
    price flaps back to A, no price-change alert fires because the observed price equals
    the last alerted value. And while any change is still inside the cooldown window, no
    price-change alert fires regardless of the value.
    """
    # Reversion to the last alerted value A => equal, so no alert even once cooldown passed.
    reverted = _run(
        fetched=_fetched(in_stock=True, price=price_a),
        stored=_stored(in_stock=True, price=price_b),
        alert_state={
            EventType.PRICE_CHANGE: _price_row(
                last_alert_at=_NOW, last_alerted_value=price_a
            )
        },
        now=_NOW + timedelta(seconds=cooldown + 100),
        cooldown_s=cooldown,
    )
    assert not any(isinstance(e, PriceChangeEvent) for e in reverted.events)

    # A genuinely different price B observed INSIDE the cooldown => suppressed (Req 9.5).
    if price_b != price_a:
        within = _run(
            fetched=_fetched(in_stock=True, price=price_b),
            stored=_stored(in_stock=True, price=price_a),
            alert_state={
                EventType.PRICE_CHANGE: _price_row(
                    last_alert_at=_NOW, last_alerted_value=price_a
                )
            },
            now=_NOW + timedelta(seconds=cooldown - 1) if cooldown > 1 else _NOW,
            cooldown_s=cooldown,
        )
        assert not any(isinstance(e, PriceChangeEvent) for e in within.events)


# ===========================================================================
# 16.5 — Property 9: restock -> sell-out -> restock yields two alerts
# ===========================================================================


@settings(max_examples=200)
@given(cooldown=_cooldowns, price=_prices)
def test_property_9_restock_sellout_restock_yields_two_alerts(
    cooldown: int, price: Decimal
) -> None:
    """Feature: stock-drop-monitor, Property 9: Restock -> sell-out -> restock, both gaps
    beyond the cooldown, yields two alerts.

    Validates: Requirements 4.7

    *For any* sequence restock -> sell-out -> restock where each gap exceeds the cooldown,
    exactly two restock alerts fire (the second sell-out/restock is a FRESH transition
    subject to the same cooldown). The diff is pure, so we thread the stored + alert_state
    forward across the three polls by hand.
    """
    step = cooldown + 10
    t0 = _NOW
    t1 = _NOW + timedelta(seconds=step)  # sell-out
    t2 = _NOW + timedelta(seconds=step * 2)  # restock again

    # Poll 1: out_of_stock -> in_stock, no prior alert => first restock.
    poll1 = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
        alert_state={},
        now=t0,
        cooldown_s=cooldown,
    )
    first = [e for e in poll1.events if isinstance(e, RestockEvent)]
    assert len(first) == 1
    assert poll1.state is not None and poll1.state.in_stock is True

    # Poll 2: in_stock -> out_of_stock, stores false, no out-of-stock alert.
    poll2 = _run(
        fetched=_fetched(in_stock=False, price=price),
        stored=_stored(in_stock=True, price=price),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=t0, pending=False)},
        now=t1,
        cooldown_s=cooldown,
    )
    assert not any(isinstance(e, RestockEvent) for e in poll2.events)

    # Poll 3: out_of_stock -> in_stock again, cooldown long elapsed => second restock.
    poll3 = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=t0, pending=False)},
        now=t2,
        cooldown_s=cooldown,
    )
    second = [e for e in poll3.events if isinstance(e, RestockEvent)]
    assert len(second) == 1

    assert len(first) + len(second) == 2  # exactly two alerts across the sequence


# ===========================================================================
# 16.6 — Property 22: page going live produces exactly one alert, no stock/price alert
# ===========================================================================


@settings(max_examples=200)
@given(
    in_stock=st.booleans(),
    price=st.none() | _prices,
    stored_existence=st.sampled_from([None, StoredExistence.NOT_FOUND]),
)
def test_property_22_page_live_one_alert_no_stock_price(
    in_stock: bool, price: Decimal | None, stored_existence: StoredExistence | None
) -> None:
    """Feature: stock-drop-monitor, Property 22: A page going live produces exactly one
    alert and no stock/price alert that poll.

    Validates: Requirements 7.3, 7.4, 7.5

    *For any* listing whose stored existence is unseeded or NOT_FOUND, a fetched EXISTING
    poll emits EXACTLY ONE event — a PageLiveEvent carrying the fetched stock+price — and
    NO restock or price-change event. The same result sets listings.in_stock/price to the
    fetched values (existence EXISTING) and upserts BOTH alert_state rows: restock
    (pending=0, last_alert_at NULL) and price_change (last_alerted_value = fetched price).
    """
    stored = _stored(
        in_stock=None,
        price=None,
        existence=stored_existence,
        consecutive_404=0 if stored_existence is None else 2,
    )

    result = _run(
        fetched=_fetched(existence=FetchExistence.EXISTING, in_stock=in_stock, price=price),
        stored=stored,
        alert_state={},
    )

    # Exactly one event, and it is the page-live one carrying the fetched state.
    assert len(result.events) == 1
    event = result.events[0]
    assert isinstance(event, PageLiveEvent)
    assert event.in_stock is in_stock
    assert event.price == price
    assert not any(isinstance(e, RestockEvent | PriceChangeEvent) for e in result.events)

    # listings.in_stock/price set to the fetched values; existence EXISTING (Req 7.5).
    assert result.state is not None
    assert result.state.set_in_stock and result.state.in_stock is in_stock
    assert result.state.set_price and result.state.price == price
    assert result.state.set_existence
    assert result.state.existence is StoredExistence.EXISTING

    # BOTH alert_state rows upserted with the clean page-live seed (Req 7.3).
    restock = _restock_upsert(result)
    assert restock.set_pending and restock.pending is False
    assert restock.set_last_alert_at and restock.last_alert_at is None
    price_upsert = _price_upsert(result)
    assert price_upsert.set_last_alerted_value
    assert price_upsert.last_alerted_value == price


# ===========================================================================
# 16.7 — Property 20: existence flips to not-found only after two consecutive 404s
# ===========================================================================


@settings(max_examples=200)
@given(in_stock=st.booleans(), price=st.none() | _prices)
def test_property_20_existence_flips_after_two_consecutive_404s(
    in_stock: bool, price: Decimal | None
) -> None:
    """Feature: stock-drop-monitor, Property 20: Existence flips to not-found only after
    two consecutive 404s.

    Validates: Requirements 7.6

    *For any* previously-EXISTING listing, the FIRST fetched NOT_FOUND only increments the
    404 counter (stored existence unchanged, no event, stock/price untouched); the SECOND
    consecutive NOT_FOUND flips stored existence to NOT_FOUND. A single transient 404
    followed by an EXISTING fetch changes nothing material and emits no event.
    """
    stored_live = _stored(in_stock=in_stock, price=price, existence=StoredExistence.EXISTING)

    # First 404: counter -> 1, existence NOT flipped, stock/price untouched, no event.
    first = _run(fetched=_fetched(existence=FetchExistence.NOT_FOUND), stored=stored_live)
    assert first.events == ()
    assert first.state is not None
    assert first.state.set_existence is False  # existence left unchanged
    assert first.state.consecutive_404 == 1
    assert first.state.set_in_stock is False  # stock untouched (Req 7.4)
    assert first.state.set_price is False  # price untouched (Req 7.4)

    # Second consecutive 404 (stored counter already 1): counter -> 2, existence flips.
    stored_after_one = _stored(
        in_stock=in_stock,
        price=price,
        existence=StoredExistence.EXISTING,
        consecutive_404=1,
    )
    second = _run(
        fetched=_fetched(existence=FetchExistence.NOT_FOUND), stored=stored_after_one
    )
    assert second.events == ()
    assert second.state is not None
    assert second.state.set_existence is True
    assert second.state.existence is StoredExistence.NOT_FOUND
    assert second.state.consecutive_404 == 2
    assert second.state.set_in_stock is False and second.state.set_price is False

    # A lone 404 then EXISTING: the EXISTING poll resets the counter and emits no event
    # (the page was never flipped to NOT_FOUND, so this is no page-live transition).
    recovered = _run(
        fetched=_fetched(existence=FetchExistence.EXISTING, in_stock=in_stock, price=price),
        stored=stored_after_one,
    )
    assert not any(isinstance(e, PageLiveEvent) for e in recovered.events)
    assert recovered.state is not None and recovered.state.consecutive_404 == 0


# ===========================================================================
# 16.9 — restock / price / cooldown example tests
# ===========================================================================


def test_example_restock_one_alert_no_repeat_in_cooldown() -> None:
    """Variant unavailable -> available => one restock; no repeat within the cooldown.

    Validates: Requirements 21.6, 21.18
    """
    price = Decimal("49.99")
    first = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
    )
    assert len([e for e in first.events if isinstance(e, RestockEvent)]) == 1
    assert _restock_upsert(first).last_alert_at == _NOW

    # Re-poll in stock again within the cooldown (stored now in stock, pending False) => none.
    repeat = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=True, price=price),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=False)},
        now=_NOW + timedelta(seconds=_COOLDOWN - 1),
    )
    assert not any(isinstance(e, RestockEvent) for e in repeat.events)


def test_example_price_change_one_alert() -> None:
    """A price change (vs the last alerted value) with cooldown elapsed => one alert.

    Validates: Requirements 21.7
    """
    result = _run(
        fetched=_fetched(in_stock=True, price=Decimal("40.00")),
        stored=_stored(in_stock=True, price=Decimal("50.00")),
        alert_state={
            EventType.PRICE_CHANGE: _price_row(
                last_alert_at=None, last_alerted_value=Decimal("50.00")
            )
        },
    )
    prices = [e for e in result.events if isinstance(e, PriceChangeEvent)]
    assert len(prices) == 1
    assert prices[0].old_price == Decimal("50.00")
    assert prices[0].new_price == Decimal("40.00")
    assert _price_upsert(result).last_alerted_value == Decimal("40.00")


def test_example_trailing_price_change_fires_once_after_cooldown() -> None:
    """A price change suppressed within the cooldown fires once the cooldown elapses.

    Validates: Requirements 21.8, 21.26
    """
    row = {
        EventType.PRICE_CHANGE: _price_row(
            last_alert_at=_NOW, last_alerted_value=Decimal("50.00")
        )
    }
    # Within the cooldown: no alert.
    within = _run(
        fetched=_fetched(in_stock=True, price=Decimal("40.00")),
        stored=_stored(in_stock=True, price=Decimal("50.00")),
        alert_state=row,
        now=_NOW + timedelta(seconds=_COOLDOWN - 1),
    )
    assert not any(isinstance(e, PriceChangeEvent) for e in within.events)

    # After the cooldown: exactly one alert.
    after = _run(
        fetched=_fetched(in_stock=True, price=Decimal("40.00")),
        stored=_stored(in_stock=True, price=Decimal("50.00")),
        alert_state=row,
        now=_NOW + timedelta(seconds=_COOLDOWN),
    )
    assert len([e for e in after.events if isinstance(e, PriceChangeEvent)]) == 1


def test_example_price_flap_a_b_a_no_alert() -> None:
    """A -> B -> A price flap (reverting to the last alerted value) emits no alert.

    Validates: Requirements 21.9
    """
    result = _run(
        fetched=_fetched(in_stock=True, price=Decimal("50.00")),
        stored=_stored(in_stock=True, price=Decimal("45.00")),
        alert_state={
            EventType.PRICE_CHANGE: _price_row(
                last_alert_at=_NOW, last_alerted_value=Decimal("50.00")
            )
        },
        now=_NOW + timedelta(seconds=_COOLDOWN + 100),
    )
    assert not any(isinstance(e, PriceChangeEvent) for e in result.events)


def test_example_restock_sellout_restock_two_alerts() -> None:
    """restock -> sell-out -> restock (gaps beyond cooldown) => two alerts.

    Validates: Requirements 21.25
    """
    price = Decimal("30.00")
    p1 = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
    )
    p3 = _run(
        fetched=_fetched(in_stock=True, price=price),
        stored=_stored(in_stock=False, price=price),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=False)},
        now=_NOW + timedelta(seconds=_COOLDOWN * 2),
    )
    total = [e for e in (*p1.events, *p3.events) if isinstance(e, RestockEvent)]
    assert len(total) == 2


def test_example_flap_within_cooldown_no_restock() -> None:
    """A restock within the cooldown sets pending and emits no restock alert.

    Validates: Requirements 21.25
    """
    result = _run(
        fetched=_fetched(in_stock=True, price=Decimal("30.00")),
        stored=_stored(in_stock=False, price=Decimal("30.00")),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=False)},
        now=_NOW + timedelta(seconds=_COOLDOWN - 1),
    )
    assert not any(isinstance(e, RestockEvent) for e in result.events)
    assert _restock_upsert(result).pending is True


def test_example_restock_within_cooldown_sets_pending_then_fires_after() -> None:
    """A restock within the cooldown sets pending, then the trailing alert fires once.

    Validates: Requirements 21.25, 21.26
    """
    # Within cooldown => pending set, no alert.
    deferred = _run(
        fetched=_fetched(in_stock=True, price=Decimal("30.00")),
        stored=_stored(in_stock=False, price=Decimal("30.00")),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=False)},
        now=_NOW + timedelta(seconds=_COOLDOWN - 1),
    )
    assert _restock_upsert(deferred).pending is True
    assert not any(isinstance(e, RestockEvent) for e in deferred.events)

    # Later, still in stock, cooldown elapsed => exactly one trailing restock, pending clears.
    trailing = _run(
        fetched=_fetched(in_stock=True, price=Decimal("30.00")),
        stored=_stored(in_stock=True, price=Decimal("30.00")),
        alert_state={EventType.RESTOCK: _restock_row(last_alert_at=_NOW, pending=True)},
        now=_NOW + timedelta(seconds=_COOLDOWN),
    )
    assert len([e for e in trailing.events if isinstance(e, RestockEvent)]) == 1
    assert _restock_upsert(trailing).pending is False


def test_example_combined_restock_and_price_change_both_fire() -> None:
    """When a restock AND a price change both fire in one poll, both events are present.

    Validates: Requirements 21.6, 21.7

    The combining into one outbox message is task 17's job; here both events must simply
    be present so the fan-out can see both fired this poll (Req 4.10).
    """
    result = _run(
        fetched=_fetched(in_stock=True, price=Decimal("40.00")),
        stored=_stored(in_stock=False, price=Decimal("50.00")),
        alert_state={
            EventType.RESTOCK: _restock_row(last_alert_at=None, pending=False),
            EventType.PRICE_CHANGE: _price_row(
                last_alert_at=None, last_alerted_value=Decimal("50.00")
            ),
        },
    )
    assert any(isinstance(e, RestockEvent) for e in result.events)
    assert any(isinstance(e, PriceChangeEvent) for e in result.events)


# ===========================================================================
# 16.10 — page-live 404 example tests
# ===========================================================================


def test_example_first_poll_404_then_later_transition_one_page_live() -> None:
    """First-poll 404 stores existence false; a LATER transition => one Page_Live_Event.

    Validates: Requirements 21.19, 21.27, 7.3

    An unseeded listing's first two consecutive 404s flip stored existence to NOT_FOUND
    with no event; the later EXISTING poll then fires exactly one page-live alert.
    """
    # Poll 1 (unseeded) 404 => counter 1, no flip, no event.
    p1 = _run(fetched=_fetched(existence=FetchExistence.NOT_FOUND), stored=None)
    assert p1.events == ()
    assert p1.state is not None and p1.state.consecutive_404 == 1

    # Poll 2 (counter already 1) 404 => counter 2, existence flips to NOT_FOUND, no event.
    p2 = _run(
        fetched=_fetched(existence=FetchExistence.NOT_FOUND),
        stored=_stored(in_stock=None, existence=StoredExistence.NOT_FOUND, consecutive_404=1),
    )
    assert p2.events == ()
    assert p2.state is not None and p2.state.existence is StoredExistence.NOT_FOUND

    # Poll 3: page comes back EXISTING => exactly one PageLiveEvent.
    p3 = _run(
        fetched=_fetched(existence=FetchExistence.EXISTING, in_stock=True, price=Decimal("20.00")),
        stored=_stored(
            in_stock=None, price=None, existence=StoredExistence.NOT_FOUND, consecutive_404=2
        ),
    )
    page_live = [e for e in p3.events if isinstance(e, PageLiveEvent)]
    assert len(page_live) == 1
    assert page_live[0].in_stock is True
    assert page_live[0].price == Decimal("20.00")


def test_example_single_transient_404_no_change_no_event() -> None:
    """A single transient 404 on a previously-existing page stores no change, no event.

    Validates: Requirements 21.27, 7.6

    The stored existence stays EXISTING (the counter only reaches 1), in_stock/price are
    untouched, and no event fires.
    """
    result = _run(
        fetched=_fetched(existence=FetchExistence.NOT_FOUND),
        stored=_stored(in_stock=True, price=Decimal("25.00"), existence=StoredExistence.EXISTING),
    )
    assert result.events == ()
    assert result.state is not None
    assert result.state.set_existence is False  # existence unchanged (still EXISTING)
    assert result.state.consecutive_404 == 1
    assert result.state.set_in_stock is False  # stock untouched
    assert result.state.set_price is False  # price untouched
