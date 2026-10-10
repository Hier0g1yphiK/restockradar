"""Task 9.1 — unit tests for :mod:`restockradar.http.host_gate` (Req 1.4, 15.5).

The gate must give (a) **one in-flight request per host** and (b) **≥1s spacing**
between successive requests to the same host, while leaving different hosts
independent. The clock and sleep are injected so the spacing is asserted
deterministically with no real waiting.
"""

from __future__ import annotations

import asyncio

import pytest

from restockradar.http.host_gate import HostGate


class _FakeClock:
    """A controllable monotonic clock whose ``sleep`` advances the clock in place."""

    def __init__(self) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        # Record and advance deterministically — no real waiting in the suite.
        self.sleeps.append(seconds)
        self.t += seconds


@pytest.fixture
def clock() -> _FakeClock:
    return _FakeClock()


URL_A1 = "https://a.example/products/one.json"
URL_A2 = "https://a.example/products/two.json"
URL_B1 = "https://b.example/products/one.json"


async def test_first_request_to_a_host_is_not_delayed(clock: _FakeClock) -> None:
    gate = HostGate(monotonic=clock.monotonic, sleep=clock.sleep)
    async with gate.acquire(URL_A1):
        pass
    assert clock.sleeps == []  # no spacing wait before the very first request


async def test_second_request_same_host_waits_out_the_full_spacing(
    clock: _FakeClock,
) -> None:
    gate = HostGate(monotonic=clock.monotonic, sleep=clock.sleep)
    async with gate.acquire(URL_A1):
        pass
    # No time passes between the two requests, so the whole 1s must be slept off.
    async with gate.acquire(URL_A2):
        pass
    assert clock.sleeps == [pytest.approx(1.0)]


async def test_spacing_accounts_for_elapsed_time(clock: _FakeClock) -> None:
    gate = HostGate(monotonic=clock.monotonic, sleep=clock.sleep)
    async with gate.acquire(URL_A1):
        pass
    clock.t += 0.4  # 0.4s elapsed before the next request to the same host
    async with gate.acquire(URL_A2):
        pass
    # Only the remaining 0.6s should be slept.
    assert clock.sleeps == [pytest.approx(0.6)]


async def test_no_wait_when_spacing_already_elapsed(clock: _FakeClock) -> None:
    gate = HostGate(monotonic=clock.monotonic, sleep=clock.sleep)
    async with gate.acquire(URL_A1):
        pass
    clock.t += 5.0  # well beyond the 1s spacing
    async with gate.acquire(URL_A2):
        pass
    assert clock.sleeps == []


async def test_different_hosts_are_independent(clock: _FakeClock) -> None:
    gate = HostGate(monotonic=clock.monotonic, sleep=clock.sleep)
    async with gate.acquire(URL_A1):
        pass
    # A request to a *different* host immediately after incurs no spacing wait.
    async with gate.acquire(URL_B1):
        pass
    assert clock.sleeps == []


async def test_single_in_flight_request_per_host() -> None:
    """At most one coroutine may hold the gate for a host at a time (one in-flight)."""
    gate = HostGate(min_spacing_s=0.0)  # isolate the one-in-flight invariant
    in_flight = 0
    max_in_flight = 0
    started = asyncio.Event()

    async def worker(hold: asyncio.Event | None) -> None:
        nonlocal in_flight, max_in_flight
        async with gate.acquire(URL_A1):
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            started.set()
            if hold is not None:
                await hold.wait()
            in_flight -= 1

    hold = asyncio.Event()
    first = asyncio.create_task(worker(hold))
    await started.wait()  # first task is now inside the gate
    second = asyncio.create_task(worker(None))
    await asyncio.sleep(0)  # give the second task a chance to (not) enter
    assert in_flight == 1  # the second task is blocked on the host lock
    hold.set()
    await asyncio.gather(first, second)
    assert max_in_flight == 1  # never two in flight for the same host


async def test_concurrent_different_hosts_can_overlap() -> None:
    """Two hosts can be in flight simultaneously (independent locks)."""
    gate = HostGate(min_spacing_s=0.0)
    overlap = 0
    max_overlap = 0
    both_in = asyncio.Event()
    release = asyncio.Event()
    entered = 0

    async def worker(url: str) -> None:
        nonlocal overlap, max_overlap, entered
        async with gate.acquire(url):
            overlap += 1
            entered += 1
            max_overlap = max(max_overlap, overlap)
            if entered == 2:
                both_in.set()
            await release.wait()
            overlap -= 1

    a = asyncio.create_task(worker(URL_A1))
    b = asyncio.create_task(worker(URL_B1))
    await both_in.wait()
    release.set()
    await asyncio.gather(a, b)
    assert max_overlap == 2
