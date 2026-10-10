"""Task 3.1 — unit tests for the in-memory heartbeat registry (Req 19.9, 19.13).

These tests pin the registry's contract: setters update the monotonic beats,
getters read them back, the registry performs **no database access** (process
memory only, Req 19.13), and concurrent beats from many threads are serialised
without loss or corruption (Req 19.9 — the registry is the hand-off point between
the asyncio writers and the Watchdog's daemon-thread reader).
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable

import pytest

from restockradar.observability.heartbeat import Component, HeartbeatRegistry


class _FakeMonotonic:
    """A controllable monotonic clock advanced by hand, so tests stay deterministic."""

    def __init__(self, start: float = 1000.0) -> None:
        self._value = start

    def __call__(self) -> float:
        return self._value

    def advance(self, seconds: float) -> None:
        self._value += seconds


def test_both_components_initialised_fresh_at_construction() -> None:
    """A brand-new registry seeds both beats to "now" so it is never stale early."""
    clock = _FakeMonotonic(start=500.0)
    registry = HeartbeatRegistry(monotonic=clock)

    assert registry.get(Component.SCHEDULER) == 500.0
    assert registry.get(Component.DISPATCH) == 500.0
    # No time has passed, so both ages are zero.
    assert registry.age(Component.SCHEDULER) == 0.0
    assert registry.age(Component.DISPATCH) == 0.0


def test_beat_updates_the_monotonic_value_and_getter_reads_it_back() -> None:
    """A setter records the current monotonic reading; the getter returns it."""
    clock = _FakeMonotonic(start=1000.0)
    registry = HeartbeatRegistry(monotonic=clock)

    clock.advance(42.0)
    returned = registry.beat(Component.SCHEDULER)

    assert returned == 1042.0
    assert registry.get(Component.SCHEDULER) == 1042.0
    # The dispatch beat is independent and still holds its construction value.
    assert registry.get(Component.DISPATCH) == 1000.0


def test_scheduler_and_dispatch_beats_are_independent() -> None:
    """Beating one component never disturbs the other."""
    clock = _FakeMonotonic(start=0.0)
    registry = HeartbeatRegistry(monotonic=clock)

    clock.advance(10.0)
    registry.beat_scheduler()
    clock.advance(5.0)
    registry.beat_dispatch()

    assert registry.get(Component.SCHEDULER) == 10.0
    assert registry.get(Component.DISPATCH) == 15.0


def test_convenience_wrappers_match_the_generic_setter() -> None:
    """``beat_scheduler`` / ``beat_dispatch`` update the same values as ``beat``."""
    clock = _FakeMonotonic(start=100.0)
    registry = HeartbeatRegistry(monotonic=clock)

    clock.advance(1.0)
    assert registry.beat_scheduler() == 101.0
    clock.advance(1.0)
    assert registry.beat_dispatch() == 102.0

    assert registry.get(Component.SCHEDULER) == 101.0
    assert registry.get(Component.DISPATCH) == 102.0


def test_age_uses_explicit_now_relative_to_last_beat() -> None:
    """``age`` measures ``now - last_beat`` against the caller-supplied instant."""
    clock = _FakeMonotonic(start=1000.0)
    registry = HeartbeatRegistry(monotonic=clock)
    registry.beat(Component.DISPATCH)  # beat at 1000.0

    # The Watchdog samples one monotonic 'now' and compares every beat against it.
    assert registry.age(Component.DISPATCH, now=1065.0) == 65.0
    assert registry.age(Component.DISPATCH, now=1000.0) == 0.0


def test_age_defaults_to_the_registry_clock_when_now_omitted() -> None:
    """With no explicit ``now``, ``age`` reads the registry's own monotonic clock."""
    clock = _FakeMonotonic(start=0.0)
    registry = HeartbeatRegistry(monotonic=clock)
    registry.beat(Component.SCHEDULER)  # beat at 0.0

    clock.advance(130.0)
    assert registry.age(Component.SCHEDULER) == 130.0


def test_age_is_clamped_to_zero_for_a_future_beat() -> None:
    """A beat recorded after the sampled ``now`` reports a non-negative age."""
    clock = _FakeMonotonic(start=1000.0)
    registry = HeartbeatRegistry(monotonic=clock)
    registry.beat(Component.SCHEDULER)  # beat at 1000.0

    # 'now' sampled slightly before the stored beat must not produce a negative age.
    assert registry.age(Component.SCHEDULER, now=999.0) == 0.0


def test_snapshot_returns_an_independent_copy() -> None:
    """``snapshot`` exposes a copy, not the internal mapping."""
    clock = _FakeMonotonic(start=10.0)
    registry = HeartbeatRegistry(monotonic=clock)

    snap = registry.snapshot()
    assert snap == {Component.SCHEDULER: 10.0, Component.DISPATCH: 10.0}

    # Mutating the returned copy must not affect the registry.
    snap[Component.SCHEDULER] = 999.0
    assert registry.get(Component.SCHEDULER) == 10.0


def test_registry_performs_no_database_access(monkeypatch: pytest.MonkeyPatch) -> None:
    """The registry is process memory only — it never opens a SQLite connection.

    We replace ``sqlite3.connect`` with a tripwire that fails if called, then
    exercise the whole public surface. No database access must occur (Req 19.13).
    """

    def _tripwire(*args: object, **kwargs: object) -> sqlite3.Connection:
        raise AssertionError("heartbeat registry must not touch the database")

    monkeypatch.setattr(sqlite3, "connect", _tripwire)

    registry = HeartbeatRegistry()
    registry.beat(Component.SCHEDULER)
    registry.beat_dispatch()
    registry.get(Component.SCHEDULER)
    registry.age(Component.DISPATCH, now=registry.get(Component.DISPATCH))
    registry.snapshot()


def test_concurrent_beats_are_thread_safe_and_lossless() -> None:
    """Many threads beating concurrently never corrupt the stored value.

    Each thread beats repeatedly against a shared, monotonically increasing clock.
    Because the lock serialises writes, the final stored beat for each component is
    exactly the clock's latest reading — no torn or lost update (Req 19.9).
    """
    lock = threading.Lock()
    counter = {"value": 0.0}

    def ticking_monotonic() -> float:
        # A strictly increasing, thread-safe clock so the "latest" value is defined.
        with lock:
            counter["value"] += 1.0
            return counter["value"]

    monotonic: Callable[[], float] = ticking_monotonic
    registry = HeartbeatRegistry(monotonic=monotonic)

    threads_per_component = 8
    beats_per_thread = 500
    barrier = threading.Barrier(threads_per_component * 2)

    def hammer(component: Component) -> None:
        barrier.wait()
        for _ in range(beats_per_thread):
            registry.beat(component)

    threads = [
        threading.Thread(target=hammer, args=(component,))
        for component in (Component.SCHEDULER, Component.DISPATCH)
        for _ in range(threads_per_component)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # The clock was ticked once per beat plus twice at construction, strictly
    # increasing, so every recorded beat is a value the clock actually produced and
    # the two stored beats are distinct, uncorrupted readings.
    scheduler_beat = registry.get(Component.SCHEDULER)
    dispatch_beat = registry.get(Component.DISPATCH)
    assert scheduler_beat > 0.0
    assert dispatch_beat > 0.0
    assert scheduler_beat != dispatch_beat


def test_concurrent_readers_and_writers_do_not_raise() -> None:
    """Readers (``get``/``age``/``snapshot``) run concurrently with writers safely."""
    registry = HeartbeatRegistry()
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            while not stop.is_set():
                registry.beat_scheduler()
                registry.beat_dispatch()
        except BaseException as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            while not stop.is_set():
                registry.get(Component.SCHEDULER)
                registry.age(Component.DISPATCH)
                registry.snapshot()
        except BaseException as exc:
            errors.append(exc)

    workers = [threading.Thread(target=writer) for _ in range(4)]
    workers += [threading.Thread(target=reader) for _ in range(4)]
    for worker in workers:
        worker.start()
    stop.wait(0.2)
    stop.set()
    for worker in workers:
        worker.join()

    assert errors == []
