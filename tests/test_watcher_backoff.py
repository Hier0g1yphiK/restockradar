"""Task 18 — backoff state machine + effective-due tests for
:mod:`restockradar.watcher.backoff` and :mod:`restockradar.watcher.due`.

Drives the PURE :func:`restockradar.watcher.backoff.next_backoff` and the
:mod:`restockradar.watcher.due` helpers directly with hand-built
:class:`~restockradar.persistence.repository.BackoffStateRow` snapshots. No DB round-trips
are needed — every assertion is a pure-function assertion.

Covered properties and example suites:

* 18.2 **Property 21** — backoff grows monotonically to the cap and resets on success.
  Validates Req 16.5, 16.6, 16.7. For any sequence of consecutive failures of ONE kind
  (ordinary OR blocked), each failure's delay-from-``now`` is >= the previous one and never
  exceeds :data:`MAX_BACKOFF_S` (doubling from the poll interval), and a subsequent SUCCESS
  clears ``backoff_until`` so the entity is no longer deferred.
* Example tests — first-error delay == poll interval; doubling on the 2nd/3rd; the 1h cap;
  Blocked honours Retry-After; success clears both counters + ``backoff_until``; the
  ordinary and blocked tracks are counted independently.
* ``due`` example tests — effective_due_entity returns the ``max``; retailer uses
  ``backoff_until`` alone; ``is_due`` treats ``None`` as due now.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.persistence.repository import BackoffStateRow
from restockradar.watcher.backoff import (
    MAX_BACKOFF_S,
    BackoffUpdate,
    Outcome,
    exponential_delay_s,
    next_backoff,
)
from restockradar.watcher.due import (
    effective_due_entity,
    effective_due_retailer,
    is_due,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_SCOPE = "feed:1"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _row(
    *,
    error_count: int = 0,
    blocked_count: int = 0,
    empty_count: int = 0,
    backoff_until: datetime | None = None,
    last_outcome: str | None = None,
) -> BackoffStateRow:
    """Build a ``BackoffStateRow`` snapshot for the pure backoff functions."""
    return BackoffStateRow(
        scope_key=_SCOPE,
        error_count=error_count,
        blocked_count=blocked_count,
        empty_count=empty_count,
        backoff_until=backoff_until,
        last_outcome=last_outcome,
    )


def _delay_s(update: BackoffUpdate, now: datetime) -> float:
    """The backoff delay in seconds carried by an update (``backoff_until`` must be set)."""
    assert update.backoff_until is not None
    return (update.backoff_until - now).total_seconds()


# ---------------------------------------------------------------------------
# exponential_delay_s — the small pure helper (Req 16.5, 16.6)
# ---------------------------------------------------------------------------


def test_exponential_first_failure_is_poll_interval() -> None:
    assert exponential_delay_s(60, 1) == 60


def test_exponential_doubles_per_consecutive_failure() -> None:
    assert exponential_delay_s(60, 2) == 120
    assert exponential_delay_s(60, 3) == 240
    assert exponential_delay_s(60, 4) == 480


def test_exponential_caps_at_one_hour() -> None:
    # 60 * 2**6 == 3840 > 3600 => capped.
    assert exponential_delay_s(60, 7) == MAX_BACKOFF_S
    assert exponential_delay_s(60, 100) == MAX_BACKOFF_S


def test_exponential_rejects_zero_count() -> None:
    with pytest.raises(ValueError, match="consecutive_count"):
        exponential_delay_s(60, 0)


# ---------------------------------------------------------------------------
# next_backoff — ordinary error track (Req 16.6)
# ---------------------------------------------------------------------------


def test_ordinary_first_error_delay_equals_poll_interval() -> None:
    update = next_backoff(
        state=None, poll_interval_s=60, outcome=Outcome.ORDINARY_ERROR, now=_NOW
    )
    assert update.error_count == 1
    assert update.blocked_count == 0
    assert _delay_s(update, _NOW) == 60
    assert update.last_outcome == "error"


def test_ordinary_error_delay_doubles() -> None:
    first = next_backoff(
        state=None, poll_interval_s=60, outcome=Outcome.ORDINARY_ERROR, now=_NOW
    )
    second = next_backoff(
        state=_row(error_count=first.error_count),
        poll_interval_s=60,
        outcome=Outcome.ORDINARY_ERROR,
        now=_NOW,
    )
    third = next_backoff(
        state=_row(error_count=second.error_count),
        poll_interval_s=60,
        outcome=Outcome.ORDINARY_ERROR,
        now=_NOW,
    )
    assert _delay_s(first, _NOW) == 60
    assert _delay_s(second, _NOW) == 120
    assert _delay_s(third, _NOW) == 240


def test_ordinary_error_delay_caps_at_one_hour() -> None:
    update = next_backoff(
        state=_row(error_count=20),
        poll_interval_s=60,
        outcome=Outcome.ORDINARY_ERROR,
        now=_NOW,
    )
    assert _delay_s(update, _NOW) == MAX_BACKOFF_S


def test_ordinary_error_leaves_blocked_counter_untouched() -> None:
    update = next_backoff(
        state=_row(error_count=1, blocked_count=3),
        poll_interval_s=60,
        outcome=Outcome.ORDINARY_ERROR,
        now=_NOW,
    )
    assert update.error_count == 2
    assert update.blocked_count == 3


# ---------------------------------------------------------------------------
# next_backoff — blocked track + Retry-After (Req 16.5)
# ---------------------------------------------------------------------------


def test_blocked_first_delay_equals_poll_interval() -> None:
    update = next_backoff(
        state=None, poll_interval_s=120, outcome=Outcome.BLOCKED, now=_NOW
    )
    assert update.blocked_count == 1
    assert update.error_count == 0
    assert _delay_s(update, _NOW) == 120
    assert update.last_outcome == "blocked"


def test_blocked_honours_retry_after_as_floor() -> None:
    # Exponential first delay is 60s, but the server asked for 900s => honour the server.
    update = next_backoff(
        state=None,
        poll_interval_s=60,
        outcome=Outcome.BLOCKED,
        now=_NOW,
        retry_after=900.0,
    )
    assert _delay_s(update, _NOW) >= 900.0
    assert update.backoff_until == _NOW + timedelta(seconds=900)


def test_blocked_retry_after_below_exponential_uses_exponential() -> None:
    # 4th consecutive block from a 60s interval => 480s exponential; a 100s Retry-After is
    # the smaller of the two, so the exponential wins (floor semantics).
    update = next_backoff(
        state=_row(blocked_count=3),
        poll_interval_s=60,
        outcome=Outcome.BLOCKED,
        now=_NOW,
        retry_after=100.0,
    )
    assert _delay_s(update, _NOW) == 480


def test_blocked_retry_after_may_exceed_one_hour_cap() -> None:
    # The cap bounds the exponential component only; a server Retry-After beyond 1h is honoured.
    update = next_backoff(
        state=None,
        poll_interval_s=60,
        outcome=Outcome.BLOCKED,
        now=_NOW,
        retry_after=7200.0,
    )
    assert _delay_s(update, _NOW) == 7200


def test_blocked_leaves_error_counter_untouched() -> None:
    update = next_backoff(
        state=_row(error_count=4, blocked_count=1),
        poll_interval_s=60,
        outcome=Outcome.BLOCKED,
        now=_NOW,
    )
    assert update.blocked_count == 2
    assert update.error_count == 4


# ---------------------------------------------------------------------------
# next_backoff — success resets both tracks (Req 16.7)
# ---------------------------------------------------------------------------


def test_success_clears_backoff_and_both_counters() -> None:
    update = next_backoff(
        state=_row(
            error_count=5,
            blocked_count=3,
            backoff_until=_NOW + timedelta(seconds=3600),
        ),
        poll_interval_s=60,
        outcome=Outcome.SUCCESS,
        now=_NOW,
    )
    assert update.error_count == 0
    assert update.blocked_count == 0
    assert update.backoff_until is None
    assert update.defers is False
    assert update.last_outcome == "success"


# ---------------------------------------------------------------------------
# 18.2 Property 21 — backoff grows monotonically to the cap and resets on success.
# Validates Req 16.5, 16.6, 16.7.
# ---------------------------------------------------------------------------


@settings(max_examples=200)
@given(
    poll_interval_s=st.integers(min_value=1, max_value=3600),
    failures=st.integers(min_value=1, max_value=40),
    outcome=st.sampled_from([Outcome.ORDINARY_ERROR, Outcome.BLOCKED]),
)
def test_property_21_backoff_monotonic_to_cap_and_resets_on_success(
    poll_interval_s: int, failures: int, outcome: Outcome
) -> None:
    """Property 21: for any run of consecutive failures of one kind, each delay is >= the
    previous one and never exceeds the 1h cap; a following success clears the backoff.
    """
    state: BackoffStateRow | None = None
    previous_delay = -1.0
    for _ in range(failures):
        update = next_backoff(
            state=state, poll_interval_s=poll_interval_s, outcome=outcome, now=_NOW
        )
        delay = _delay_s(update, _NOW)

        # Monotonic non-decreasing growth and the hard 1h cap (no Retry-After here).
        assert delay >= previous_delay
        assert delay <= MAX_BACKOFF_S
        assert update.defers is True
        previous_delay = delay

        # Carry the just-fired track's counter forward; the other track stays put.
        state = _row(
            error_count=update.error_count,
            blocked_count=update.blocked_count,
            backoff_until=update.backoff_until,
        )

    # The first failure's delay starts exactly at the poll interval (capped if the interval
    # itself is the cap).
    first = next_backoff(
        state=None, poll_interval_s=poll_interval_s, outcome=outcome, now=_NOW
    )
    assert _delay_s(first, _NOW) == min(poll_interval_s, MAX_BACKOFF_S)

    # A subsequent success resets the fired track so it no longer defers the entity.
    success = next_backoff(
        state=state, poll_interval_s=poll_interval_s, outcome=Outcome.SUCCESS, now=_NOW
    )
    assert success.backoff_until is None
    assert success.error_count == 0
    assert success.blocked_count == 0


# ---------------------------------------------------------------------------
# due.py — effective due time (Req 1.8)
# ---------------------------------------------------------------------------

_EARLIER = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_LATER = datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC)


def test_effective_due_entity_returns_later_of_two() -> None:
    assert effective_due_entity(_EARLIER, _LATER) == _LATER
    assert effective_due_entity(_LATER, _EARLIER) == _LATER


def test_effective_due_entity_handles_none() -> None:
    assert effective_due_entity(None, None) is None
    assert effective_due_entity(_LATER, None) == _LATER
    assert effective_due_entity(None, _LATER) == _LATER


def test_effective_due_retailer_uses_backoff_alone() -> None:
    assert effective_due_retailer(None) is None
    assert effective_due_retailer(_LATER) == _LATER


def test_is_due_treats_none_as_due() -> None:
    assert is_due(None, _NOW) is True


def test_is_due_boundary_is_inclusive() -> None:
    assert is_due(_NOW, _NOW) is True
    assert is_due(_NOW - timedelta(seconds=1), _NOW) is True
    assert is_due(_NOW + timedelta(seconds=1), _NOW) is False
