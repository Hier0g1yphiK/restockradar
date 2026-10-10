"""In-memory monotonic heartbeat registry — the Watchdog's sole liveness source.

This module holds the two **in-memory monotonic heartbeats** the design makes the
single source of liveness the Watchdog reads: a ``scheduler`` beat and a
``dispatch`` beat, each a :func:`time.monotonic` reading updated directly in
process memory (Req 19.7, 19.8, 19.9). It is **process memory only** — nothing
here touches the database. The separate ``heartbeat`` DB-table writes (for the
external container healthcheck) and the Watchdog daemon thread that reads this
registry both arrive later (M4); this module provides only the in-memory beats so
the scheduler, HTTP, and dispatch code built in later milestones can beat them.

Design choices:

* Two fixed components, ``scheduler`` and ``dispatch``, matching the beats the
  Watchdog compares against its fixed thresholds (scheduler 120s, dispatch 60s).
* A :class:`threading.Lock` guards every read and write, because the beats are
  written from the asyncio loop (scheduler ticks, per-entity / per-HTTP hooks, the
  dispatch loop) while the Watchdog reads them from its **own daemon thread** —
  the registry is the hand-off point between those threads (Req 19.9, 19.13).
* The monotonic clock is **injectable** (defaulting to :func:`time.monotonic`) so
  tests can drive it deterministically. :meth:`HeartbeatRegistry.age` takes an
  explicit ``now`` so the Watchdog can compare against a single monotonic sample.
* Both beats are **initialised at construction** to the current monotonic reading,
  so a freshly built registry is never spuriously stale before anything beats.

Monotonic values are meaningful only as differences on the same process' clock, so
this registry deliberately exposes raw beats and ages rather than wall-clock times;
the wall-clock ``heartbeat`` table is a distinct signal written elsewhere (Req 19.13).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import StrEnum


class Component(StrEnum):
    """The liveness components tracked by the registry.

    The values match the ``component`` column of the ``heartbeat`` DB table
    (``'scheduler'`` / ``'dispatch'``) so the later DB-write path can map a
    component to its row without a second vocabulary.
    """

    SCHEDULER = "scheduler"
    DISPATCH = "dispatch"


class HeartbeatRegistry:
    """Thread-safe, in-memory monotonic heartbeats for ``scheduler`` and ``dispatch``.

    Each component carries a single :func:`time.monotonic` reading — the instant it
    was last beaten. The registry never reads or writes any database: it is pure
    process memory, the sole liveness source the Watchdog reads (Req 19.9, 19.13).

    All access is serialised by an internal lock so the asyncio-side writers and the
    Watchdog-thread reader never observe a torn value.
    """

    __slots__ = ("_beats", "_lock", "_monotonic")

    def __init__(self, monotonic: Callable[[], float] | None = None) -> None:
        """Create a registry with both beats initialised to "now".

        Args:
            monotonic: Injectable monotonic clock, defaulting to
                :func:`time.monotonic`. Supplying a controllable clock lets tests
                advance time deterministically.
        """
        self._monotonic: Callable[[], float] = monotonic or time.monotonic
        self._lock = threading.Lock()
        start = self._monotonic()
        # Both components start fresh so a brand-new registry is never stale before
        # the loops have had a chance to beat (mirrors the Watchdog's startup grace).
        self._beats: dict[Component, float] = {
            Component.SCHEDULER: start,
            Component.DISPATCH: start,
        }

    def beat(self, component: Component) -> float:
        """Record a beat for ``component`` at the current monotonic reading.

        Returns the monotonic value stored, so a caller can log or reuse it without
        a second clock read.
        """
        now = self._monotonic()
        with self._lock:
            self._beats[component] = now
        return now

    def beat_scheduler(self) -> float:
        """Beat the ``scheduler`` liveness heartbeat (convenience wrapper)."""
        return self.beat(Component.SCHEDULER)

    def beat_dispatch(self) -> float:
        """Beat the ``dispatch`` liveness heartbeat (convenience wrapper)."""
        return self.beat(Component.DISPATCH)

    def get(self, component: Component) -> float:
        """Return the most recent monotonic beat recorded for ``component``."""
        with self._lock:
            return self._beats[component]

    def age(self, component: Component, now: float | None = None) -> float:
        """Return the age in seconds of ``component``'s beat relative to ``now``.

        Args:
            component: Which liveness beat to measure.
            now: The monotonic reference instant to measure against. Defaults to the
                registry's current monotonic reading. The Watchdog passes a single
                sampled ``now`` so both components are compared against the same
                instant.

        Returns:
            ``now - last_beat`` in seconds. The value is clamped at ``0.0`` so a beat
            recorded fractionally after the sampled ``now`` never reports a negative,
            nonsensical age.
        """
        reference = self._monotonic() if now is None else now
        with self._lock:
            last = self._beats[component]
        return max(0.0, reference - last)

    def snapshot(self) -> dict[Component, float]:
        """Return a copy of the current beats for all components.

        Handy for the Watchdog/healthcheck to read every beat under a single lock
        acquisition without exposing the internal mapping.
        """
        with self._lock:
            return dict(self._beats)
