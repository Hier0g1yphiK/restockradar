"""Per-host serialisation: one request in flight at a time plus >=1s spacing between
requests to the same host (Req 1.4, 15.5).

The gate is the single point that enforces the Concurrency Model's per-host politeness
(design → "Per-host politeness"): for each retailer host it holds **one**
:class:`asyncio.Lock` (so at most one request to that host is ever in flight) **and** a
last-request monotonic timestamp (so successive requests to the same host are spaced by
at least 1 second). Different hosts have independent locks and timestamps, so polling
one retailer never blocks another.

Usage is an ``async with`` around the actual HTTP call::

    async with host_gate.acquire(url):
        response = await do_request()

Holding the context manager guarantees the one-in-flight invariant for the host; on
entry it first waits out any remaining spacing, so the ≥1s gap is measured between the
*starts* of successive requests to a host. The clock and the sleep are **injectable**
(defaulting to :func:`time.monotonic` / :func:`asyncio.sleep`) so tests assert the
spacing deterministically without real waiting.

Discord webhook delivery is deliberately **not** routed through this gate: host spacing
applies to retailer hosts only and Discord is governed solely by Req 10 (design note).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit

__all__ = ["MIN_SPACING_S", "HostGate"]

#: Minimum spacing in seconds between successive requests to the same host (Req 1.4).
MIN_SPACING_S = 1.0


@dataclass
class _HostState:
    """The per-host lock and last-request timestamp."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    #: Monotonic time the most recent request to this host *started*; ``None`` until the
    #: first request, so the first request to a host is never delayed.
    last_request_at: float | None = None


class HostGate:
    """Serialises requests per host and enforces the ≥1s minimum spacing (Req 1.4)."""

    __slots__ = ("_hosts", "_min_spacing_s", "_monotonic", "_sleep")

    def __init__(
        self,
        *,
        min_spacing_s: float = MIN_SPACING_S,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """Create a gate.

        Args:
            min_spacing_s: Minimum gap between successive requests to one host
                (defaults to 1s per Req 1.4).
            monotonic: Injectable monotonic clock (defaults to :func:`time.monotonic`).
            sleep: Injectable async sleep (defaults to :func:`asyncio.sleep`). Tests
                inject a fake that advances a controllable clock instead of waiting.
        """
        self._min_spacing_s = min_spacing_s
        self._monotonic: Callable[[], float] = monotonic or time.monotonic
        self._sleep: Callable[[float], Awaitable[None]] = sleep or asyncio.sleep
        self._hosts: dict[str, _HostState] = {}

    def _host_state(self, url: str) -> _HostState:
        """Return (creating on first use) the per-host state for ``url``'s host."""
        host = urlsplit(url).netloc
        state = self._hosts.get(host)
        if state is None:
            state = _HostState()
            self._hosts[host] = state
        return state

    @asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[None]:
        """Acquire the gate for ``url``'s host for the duration of one request.

        While the context manager is held, no other request to the same host can run
        (one-in-flight). On entry, if less than ``min_spacing_s`` has elapsed since the
        previous request to this host *started*, it first sleeps out the remainder, so
        successive requests to a host are spaced by at least ``min_spacing_s``. The
        start timestamp is recorded under the held lock before yielding.
        """
        state = self._host_state(url)
        async with state.lock:
            await self._wait_for_spacing(state)
            state.last_request_at = self._monotonic()
            yield

    async def _wait_for_spacing(self, state: _HostState) -> None:
        """Sleep off any remaining portion of the minimum spacing for ``state``."""
        last = state.last_request_at
        if last is None:
            return
        elapsed = self._monotonic() - last
        remaining = self._min_spacing_s - elapsed
        if remaining > 0:
            await self._sleep(remaining)
