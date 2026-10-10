"""Effective-due-time = max(next_due_at, backoff_until) for feeds/listings, and
backoff_until alone for a retailer (Req 1.8).

A set of **pure** helpers the scheduler uses to decide WHEN an entity next polls. They
read nothing but their arguments (no clock of their own except the explicit ``now`` passed
to :func:`is_due`), so the same inputs always yield the same result.

The ``None`` convention matches the ``read_due_*`` queries in
:mod:`restockradar.persistence.repository`: a ``NULL`` ``next_due_at`` means "never
scheduled / freshly enabled", which is treated as **due now**. Likewise a ``None``
``backoff_until`` means "not backed off". The effective due time is therefore:

* a feed's / listing's effective due = the LATER (``max``) of its ``next_due_at`` and its
  ``backoff_until``; a ``None`` on either side drops out of the ``max`` (an unscheduled,
  un-backed-off entity is due now, i.e. effective due ``None``);
* a retailer has NO ``next_due_at`` at all, so its effective due is its ``backoff_until``
  alone (Req 1.8).

:func:`is_due` then collapses an effective due time against ``now``: a ``None`` effective
due (unscheduled) is due, and any instant at or before ``now`` is due (the boundary is
inclusive, mirroring the ``next_due_at <= now`` SQL predicate).
"""

from __future__ import annotations

from datetime import datetime

__all__ = [
    "effective_due_entity",
    "effective_due_retailer",
    "is_due",
]


def effective_due_entity(
    next_due_at: datetime | None,
    backoff_until: datetime | None,
) -> datetime | None:
    """Return a feed's / listing's effective due time: the LATER of the two (Req 1.8).

    ``None`` is "no constraint" on either side, so it drops out of the comparison: with both
    ``None`` the entity is unscheduled and due now (returns ``None``); with one ``None`` the
    other wins; with both set the later (``max``) instant wins, because an entity is only
    truly due once BOTH its schedule and its backoff have elapsed.

    Args:
        next_due_at: The entity's scheduled next due instant, or ``None`` if unscheduled.
        backoff_until: The entity's active backoff expiry, or ``None`` if not backed off.

    Returns:
        The later of the two instants, the single non-``None`` one, or ``None`` when both
        are ``None``.
    """
    if next_due_at is None:
        return backoff_until
    if backoff_until is None:
        return next_due_at
    return max(next_due_at, backoff_until)


def effective_due_retailer(backoff_until: datetime | None) -> datetime | None:
    """Return a retailer's effective due time: its ``backoff_until`` alone (Req 1.8).

    Retailers carry no ``next_due_at`` — their job ticks on the scheduler's fixed interval —
    so a retailer is deferred only while a retailer-wide backoff is active. ``None`` means no
    retailer-wide backoff, i.e. the job runs on its next scheduled tick.
    """
    return backoff_until


def is_due(effective_due: datetime | None, now: datetime) -> bool:
    """Whether an entity with ``effective_due`` is due at ``now`` (Req 1.6, 1.8).

    A ``None`` effective due (unscheduled / un-backed-off) is due now; any effective due at
    or before ``now`` is due. The boundary is inclusive, matching the ``<= now`` predicate
    the ``read_due_*`` queries use.

    Args:
        effective_due: The entity's effective due instant (see :func:`effective_due_entity`
            / :func:`effective_due_retailer`), or ``None`` when unscheduled.
        now: The current tz-aware UTC instant to compare against.

    Returns:
        ``True`` when the entity is due to be polled, ``False`` when it is still deferred.
    """
    return effective_due is None or effective_due <= now
