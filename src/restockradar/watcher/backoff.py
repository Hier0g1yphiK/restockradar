"""``Backoff_State``: ordinary vs Blocked exponential backoff starting at the entity
poll interval, doubling, capped at 1h, honouring Retry-After for Blocked; per
retailer/feed/listing counters; reset on success (Req 16).

This module is a set of **pure** functions. :func:`next_backoff` takes the entity's
current :class:`~restockradar.persistence.repository.BackoffStateRow` snapshot (``None``
while unseeded), the entity's ``poll_interval_s``, a single poll :class:`Outcome`, and the
clock, and RETURNS a frozen :class:`BackoffUpdate` carrying the new counters +
``backoff_until`` + ``last_outcome`` ready for the repository to upsert. It opens no
connection, performs no I/O, and reads neither the clock nor any state from anywhere but
its arguments, so the same inputs always yield the same result (the property tests depend
on this purity).

Two distinct backoff tracks share one row (Req 16.5, 16.6):

* **Ordinary** errors (timeout, network error, a plain 5xx without a challenge, a
  malformed response) count in ``error_count``. The delay STARTS at the entity's
  ``poll_interval_s`` on the first consecutive error, DOUBLES on each further consecutive
  ordinary error, and is CAPPED at 1 hour (``_MAX_BACKOFF_S`` = 3600s).
* **Blocked** responses (HTTP 403/429, a 503 carrying a challenge, HTML where a JSON feed
  was expected, a detected captcha/challenge page) count in ``blocked_count`` with the
  same exponential schedule, but HONOUR a server ``Retry-After`` when present.

The two counters are tracked independently — an ordinary error leaves ``blocked_count``
untouched and vice versa — but the schema carries a single ``backoff_until`` column, so
whichever track fired last owns the active ``backoff_until`` (and either track's delay is
computed from its OWN counter). A SUCCESS resets BOTH counters to zero and clears
``backoff_until`` to ``None`` so no track defers the entity (Req 16.7).

**Empty-result handling (Req 17) is deliberately out of scope here.** ``empty_count`` is a
health concern owned by a separate task; :func:`next_backoff` never reads or writes it, so
callers that persist a :class:`BackoffUpdate` must leave ``empty_count`` to the empty-poll
machinery.

Design decisions on genuine ambiguity (noted here so a reader need not re-derive them):

* **Retry-After vs the 1h cap (Blocked):** Req 16.5 says to HONOUR ``Retry-After``. We read
  that as a *floor*, never a ceiling: ``backoff_until = now + max(exponential, retry_after)``.
  When the server asks for longer than 3600s we obey the server rather than clamping below
  it, so the cap applies only to the exponential component. The exponential component on
  its own never exceeds 3600s.
* **One ``backoff_until`` for two counters:** because the two tracks share the column, the
  returned ``backoff_until`` reflects only the track that just fired, computed from that
  track's incremented counter; the other counter is carried through unchanged. A later poll
  of the other kind recomputes from its own (also carried-through) counter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto

from restockradar.persistence.repository import BackoffStateRow

__all__ = [
    "MAX_BACKOFF_S",
    "BackoffUpdate",
    "Outcome",
    "exponential_delay_s",
    "next_backoff",
]

#: The exponential-backoff cap in seconds — 1 hour (Req 16.5, 16.6). The doubling schedule
#: never produces a delay above this; a ``Retry-After`` may still push the Blocked
#: ``backoff_until`` beyond it (see the module docstring).
MAX_BACKOFF_S = 3600

#: Internal alias kept for the ``_MAX_BACKOFF_S`` name referenced in the docstring.
_MAX_BACKOFF_S = MAX_BACKOFF_S

#: Shift at which ``poll_interval_s * 2**shift`` is guaranteed to exceed the cap for any
#: realistic poll interval; short-circuited to avoid building a huge intermediate integer.
_MAX_SHIFT = 32


class Outcome(Enum):
    """The single-poll outcome that drives a backoff transition (Req 16.5, 16.6, 16.7).

    * :attr:`ORDINARY_ERROR` — a genuine fetch failure that is NOT blocking: a timeout, a
      network error, a plain 5xx without a challenge, or a malformed response. Maps to the
      :class:`~restockradar.adapters.errors.FetchError` family *excluding*
      :class:`~restockradar.adapters.errors.Blocked`.
    * :attr:`BLOCKED` — active blocking (HTTP 403/429, 503-with-challenge, HTML-where-JSON,
      captcha); maps to :class:`~restockradar.adapters.errors.Blocked` /
      :class:`~restockradar.adapters.errors.SoftBlock`. Carries an optional ``retry_after``.
    * :attr:`SUCCESS` — the poll succeeded; both tracks reset (Req 16.7).
    """

    ORDINARY_ERROR = auto()
    BLOCKED = auto()
    SUCCESS = auto()


@dataclass(frozen=True, slots=True, kw_only=True)
class BackoffUpdate:
    """The pure output of :func:`next_backoff` (Req 16.5, 16.6, 16.7).

    Carries the new ordinary / blocked counters, the new ``backoff_until`` (``None`` when
    the entity is no longer deferred, i.e. after a success), and the ``last_outcome`` token
    the repository persists verbatim. ``empty_count`` is intentionally absent — this update
    never touches the empty-poll track (Req 17 owns it). The result is immutable so it is
    cheap to hand to the repository's upsert.
    """

    error_count: int
    blocked_count: int
    backoff_until: datetime | None
    last_outcome: str

    @property
    def defers(self) -> bool:
        """Whether this update leaves a ``backoff_until`` that defers the entity."""
        return self.backoff_until is not None


#: The ``last_outcome`` tokens stored on the row for each outcome (stable serialised form).
_OUTCOME_TOKENS: dict[Outcome, str] = {
    Outcome.ORDINARY_ERROR: "error",
    Outcome.BLOCKED: "blocked",
    Outcome.SUCCESS: "success",
}


def exponential_delay_s(poll_interval_s: int, consecutive_count: int) -> int:
    """Return the capped exponential backoff delay in seconds (Req 16.5, 16.6).

    The schedule STARTS at ``poll_interval_s`` for the first consecutive failure
    (``consecutive_count == 1``) and DOUBLES for each further consecutive failure, CAPPED at
    :data:`MAX_BACKOFF_S` (3600s). Formally ``min(poll_interval_s * 2**(count-1), 3600)``.

    This is a small pure helper kept separate so Property 21 can assert monotonic growth and
    the cap directly without reconstructing a row.

    Args:
        poll_interval_s: The entity's poll interval in seconds (the delay's starting point).
        consecutive_count: The number of consecutive failures of this track, ``>= 1``.

    Returns:
        The delay in seconds, never below ``poll_interval_s`` (for ``count >= 1``) nor above
        :data:`MAX_BACKOFF_S`.

    Raises:
        ValueError: If ``consecutive_count`` is below 1 (there is no delay for zero
            failures — a success clears backoff rather than scheduling one).
    """
    if consecutive_count < 1:
        raise ValueError(f"consecutive_count must be >= 1, got {consecutive_count}")
    # 2**(count-1) grows unboundedly; short-circuit well past the cap to keep the product
    # small and exact (a shift of 32 already dwarfs the cap for any sane poll interval).
    shift = consecutive_count - 1
    if shift >= _MAX_SHIFT:
        return MAX_BACKOFF_S
    scaled: int = poll_interval_s << shift
    return min(scaled, MAX_BACKOFF_S)


def next_backoff(
    *,
    state: BackoffStateRow | None,
    poll_interval_s: int,
    outcome: Outcome,
    now: datetime,
    retry_after: float | None = None,
) -> BackoffUpdate:
    """Compute the new backoff counters + ``backoff_until`` for one poll (PURE; Req 16).

    Args:
        state: The entity's current ``backoff_state`` snapshot, or ``None`` while no row
            exists yet (treated as both counters zero and no active backoff).
        poll_interval_s: The entity's poll interval in seconds — the exponential schedule's
            starting delay.
        outcome: The single-poll :class:`Outcome` driving the transition.
        now: The current tz-aware UTC instant; the sole clock consulted.
        retry_after: The server ``Retry-After`` in seconds for a :attr:`Outcome.BLOCKED`
            outcome, or ``None`` when absent / not applicable. Ignored for the other
            outcomes.

    Returns:
        A :class:`BackoffUpdate` carrying the new counters, the new ``backoff_until``
        (``None`` after a success), and the ``last_outcome`` token to persist.
    """
    prior_error = state.error_count if state is not None else 0
    prior_blocked = state.blocked_count if state is not None else 0

    match outcome:
        case Outcome.SUCCESS:
            # Success resets BOTH tracks and clears the shared backoff_until (Req 16.7).
            return BackoffUpdate(
                error_count=0,
                blocked_count=0,
                backoff_until=None,
                last_outcome=_OUTCOME_TOKENS[Outcome.SUCCESS],
            )

        case Outcome.ORDINARY_ERROR:
            new_error = prior_error + 1
            delay = exponential_delay_s(poll_interval_s, new_error)
            return BackoffUpdate(
                error_count=new_error,
                blocked_count=prior_blocked,  # distinct track untouched (Req 16.6).
                backoff_until=now + timedelta(seconds=delay),
                last_outcome=_OUTCOME_TOKENS[Outcome.ORDINARY_ERROR],
            )

        case Outcome.BLOCKED:
            new_blocked = prior_blocked + 1
            exponential = exponential_delay_s(poll_interval_s, new_blocked)
            # Honour Retry-After as a FLOOR, never a ceiling: the server may ask for longer
            # than the 1h cap and we obey it (see module docstring). The exponential part is
            # still capped at MAX_BACKOFF_S inside exponential_delay_s.
            blocked_delay: float = (
                exponential if retry_after is None else max(float(exponential), retry_after)
            )
            return BackoffUpdate(
                error_count=prior_error,  # distinct track untouched (Req 16.5).
                blocked_count=new_blocked,
                backoff_until=now + timedelta(seconds=blocked_delay),
                last_outcome=_OUTCOME_TOKENS[Outcome.BLOCKED],
            )

    # Exhaustive match above; unreachable but keeps type-checkers and linters honest.
    raise AssertionError(f"unhandled outcome: {outcome!r}")  # pragma: no cover
