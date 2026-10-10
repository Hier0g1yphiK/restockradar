"""``AsyncIOScheduler`` wiring: one job per retailer ticking at fixed ``tick_s`` with
``coalesce=True`` and ``max_instances=1`` (staggered initial ticks), plus one
independent interval job (every ``tick_s``) that beats the scheduler heartbeat
regardless of any retailer job (Req 1.5, 1.9, 1.10, 19.7).

The scheduler is the clock, not the policy: each retailer job ticks at the fixed fine
granularity ``tick_s`` and delegates ALL due-selection and polling to
:meth:`restockradar.watcher.core.WatcherCore.tick_retailer`; whether an entity actually
runs on a given tick is decided there (design "Scheduling"). Three invariants this module
guarantees:

* **Exactly one job per retailer (Req 1.5).** Each enabled retailer gets one interval job
  firing every ``tick_s`` with a stable id ``retailer:<id>``.
* **No overlap, missed ticks coalesce (Req 1.9).** Every retailer job is configured
  ``coalesce=True`` and ``max_instances=1``, so a tick still running when the next is due
  does not stack — the missed tick is folded into one.
* **Staggered startup (Req 1.10).** Each retailer's first run is offset by an increasing
  fraction of ``tick_s`` (spread evenly across the retailer set) so a fleet of retailers
  does not all fire their first tick simultaneously.
* **Independent liveness beat (Req 1.9/1.10/19.7).** A single extra interval job
  (``scheduler-heartbeat``) beats the in-memory scheduler heartbeat every ``tick_s``
  REGARDLESS of any retailer job — so a deployment with zero enabled retailers, or whose
  retailers are all in retailer-wide backoff, still keeps the scheduler beat fresh and
  cannot trip the watchdog.

:func:`build_scheduler` wires an :class:`~apscheduler.schedulers.asyncio.AsyncIOScheduler`
(or any injected scheduler exposing the same ``add_job`` surface) from the retailer id
list, the settings, the tick coroutine, and the heartbeat. It only CONFIGURES jobs; it
does not start the scheduler or require a running event loop, so a unit test can build it
and assert on the registered jobs/triggers directly.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from restockradar.observability.heartbeat import HeartbeatRegistry
from restockradar.settings import Settings

if TYPE_CHECKING:
    from apscheduler.schedulers.base import BaseScheduler

__all__ = [
    "HEARTBEAT_JOB_ID",
    "build_scheduler",
    "retailer_job_id",
]

#: The stable job id of the independent scheduler-heartbeat interval job (Req 19.7).
HEARTBEAT_JOB_ID = "scheduler-heartbeat"

#: Each retailer job's id prefix; the full id is ``retailer:<id>`` (one per retailer).
_RETAILER_JOB_PREFIX = "retailer"

#: The coroutine a retailer job runs: given a retailer id, poll it once (see
#: :meth:`restockradar.watcher.core.WatcherCore.tick_retailer`).
TickCoroutine = Callable[[str], Awaitable[object]]


def retailer_job_id(retailer_id: str) -> str:
    """Return the stable scheduler job id for a retailer (``'retailer:<id>'``)."""
    return f"{_RETAILER_JOB_PREFIX}:{retailer_id}"


def build_scheduler(
    *,
    retailer_ids: Sequence[str],
    settings: Settings,
    tick: TickCoroutine,
    heartbeat: HeartbeatRegistry,
    scheduler: BaseScheduler | None = None,
) -> BaseScheduler:
    """Configure (but do NOT start) the per-retailer + heartbeat scheduler (Req 1.5-1.10).

    Adds one interval job per retailer (id ``retailer:<id>``) firing every ``tick_s`` with
    ``coalesce=True`` / ``max_instances=1`` and a staggered first-run offset, plus one
    independent interval job (:data:`HEARTBEAT_JOB_ID`) that beats the scheduler heartbeat
    every ``tick_s`` regardless of any retailer job.

    Args:
        retailer_ids: The enabled retailer ids, one job each (Req 1.5). May be empty — the
            heartbeat job is still added so liveness holds with zero retailers (Req 1.9).
        settings: Process settings; ``tick_s`` is the fixed tick cadence (Req 1.5).
        tick: The per-retailer poll coroutine (``retailer_id -> awaitable``); each retailer
            job calls ``tick(retailer_id)``.
        heartbeat: The in-memory heartbeat registry the independent job beats (Req 19.7).
        scheduler: An injected scheduler to configure (tests pass a stub exposing
            ``add_job``); a real :class:`AsyncIOScheduler` is created when omitted.

    Returns:
        The configured scheduler (unstarted). The caller starts it when the event loop is
        running; tests inspect ``get_jobs()`` without ever starting it.
    """
    sched: BaseScheduler = scheduler if scheduler is not None else AsyncIOScheduler()
    tick_s = settings.tick_s

    count = len(retailer_ids)
    for index, retailer_id in enumerate(retailer_ids):
        # Stagger each retailer's FIRST run across the tick window so a fleet does not all
        # fire together (Req 1.10). Spreading over (count) slices keeps every offset in
        # [0, tick_s); subsequent runs follow the fixed tick_s cadence regardless.
        offset = (tick_s * index / count) if count else 0.0
        sched.add_job(
            _retailer_job(tick, retailer_id),
            trigger="interval",
            seconds=tick_s,
            id=retailer_job_id(retailer_id),
            name=f"poll {retailer_id}",
            coalesce=True,
            max_instances=1,
            next_run_time=_staggered_first_run(offset),
            replace_existing=True,
        )

    # The independent scheduler-liveness beat (Req 1.9/1.10/19.7): runs every tick_s
    # regardless of any retailer job, so zero-retailer / all-deferred deployments stay live.
    sched.add_job(
        _heartbeat_job(heartbeat),
        trigger="interval",
        seconds=tick_s,
        id=HEARTBEAT_JOB_ID,
        name="scheduler heartbeat",
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )
    return sched


def _retailer_job(tick: TickCoroutine, retailer_id: str) -> Callable[[], Awaitable[None]]:
    """Build the zero-arg coroutine a retailer job runs (binds ``retailer_id``)."""

    async def _run() -> None:
        await tick(retailer_id)

    return _run


def _heartbeat_job(heartbeat: HeartbeatRegistry) -> Callable[[], Awaitable[None]]:
    """Build the independent scheduler-heartbeat job coroutine (Req 19.7)."""

    async def _run() -> None:
        heartbeat.beat_scheduler()

    return _run


def _staggered_first_run(offset_s: float) -> datetime:
    """Return the first-run instant ``now + offset_s`` for a staggered startup (Req 1.10).

    Returned as a tz-aware UTC :class:`~datetime.datetime` so APScheduler schedules the
    first tick that far into the future; a zero offset still returns a concrete instant so
    every retailer job carries an explicit, inspectable ``next_run_time``.
    """
    return datetime.now(UTC) + timedelta(seconds=offset_s)
