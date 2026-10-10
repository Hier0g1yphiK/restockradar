"""Shared pytest fixtures and the suite-wide offline guard.

This module provides three things the whole suite relies on:

1. A **no-live-network guard** (Req 21.4): any attempt to open a real network
   socket — directly, through ``httpx``, or through any other library — raises
   :class:`NetworkAccessAttempted`, so a test that reaches for the network fails
   instead of silently talking to a remote host. The guard is installed once for
   the entire session via an ``autouse`` fixture and is active for every test.
2. A **temp/in-memory SQLite database** fixture returning a connection the tests
   can use freely; it is torn down automatically.
3. A **frozen, injectable clock** exposing both a wall-clock (:class:`datetime`)
   and a monotonic reading, which tests advance by hand so time-dependent logic
   (cooldowns, leases, backoff) is deterministic.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

import pytest

# ---------------------------------------------------------------------------
# No-live-network guard (Req 21.4)
# ---------------------------------------------------------------------------


class NetworkAccessAttempted(RuntimeError):
    """Raised when a test attempts to open a real network connection.

    The whole test suite must run offline and deterministically against saved
    fixtures. Any real network attempt is a bug in the test (or the code under
    test leaking a live request), so we turn it into a hard failure.
    """


# The loopback family of addresses stays allowed so that in-process servers,
# in-memory sockets, and local IPC used by test tooling keep working; only
# genuinely remote destinations are blocked.
_ALLOWED_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", ""})


def _is_allowed_address(address: Any) -> bool:
    """Return ``True`` for loopback/local addresses that remain permitted."""
    if isinstance(address, (tuple, list)) and address:
        host = address[0]
        return isinstance(host, str) and host in _ALLOWED_HOSTS
    # AF_UNIX and other non-INET families (str/bytes/fd) are local; allow them.
    return True


def _blocked_connect(sock: socket.socket, address: Any) -> NoReturn:
    # Close the socket we were asked to connect so a blocked attempt does not leak
    # the file descriptor (which would surface later as a ResourceWarning).
    sock.close()
    raise NetworkAccessAttempted(
        f"Real network access is disabled in the test suite (attempted to reach "
        f"{address!r}). Drive this code from a fixture or a fake transport instead."
    )


@pytest.fixture(autouse=True, scope="session")
def _no_network() -> Iterator[None]:
    """Install the offline guard for the entire session.

    We patch ``socket.socket.connect`` / ``connect_ex`` (the single choke point
    every TCP client — including ``httpx``'s transport — funnels through) so a
    real connection to a non-loopback host raises. Loopback stays allowed.
    """
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def guarded_connect(self: socket.socket, address: Any) -> None:
        if not _is_allowed_address(address):
            _blocked_connect(self, address)
        real_connect(self, address)

    def guarded_connect_ex(self: socket.socket, address: Any) -> int:
        if not _is_allowed_address(address):
            _blocked_connect(self, address)
        return real_connect_ex(self, address)

    socket.socket.connect = guarded_connect  # type: ignore[method-assign,assignment]
    socket.socket.connect_ex = guarded_connect_ex  # type: ignore[method-assign,assignment]
    try:
        yield
    finally:
        socket.socket.connect = real_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = real_connect_ex  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Temp / in-memory SQLite database
# ---------------------------------------------------------------------------


@pytest.fixture
def db_connection() -> Iterator[sqlite3.Connection]:
    """Yield a fresh in-memory SQLite connection, closed on teardown.

    Mirrors the PRAGMAs the real :mod:`restockradar.persistence.db` executor sets
    (foreign keys on), so tests exercise the same referential-integrity behaviour
    without touching disk.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Frozen, injectable monotonic + wall clock
# ---------------------------------------------------------------------------


class FrozenClock:
    """A deterministic clock the tests control by hand.

    Exposes a wall-clock reading (:meth:`now`, timezone-aware UTC) and a
    :meth:`monotonic` reading that both advance only when :meth:`advance` is
    called, so cooldown/lease/backoff logic can be driven step by step.
    """

    def __init__(self, start: datetime, monotonic_start: float = 1000.0) -> None:
        if start.tzinfo is None:
            raise ValueError("FrozenClock requires a timezone-aware start datetime")
        self._wall = start
        self._monotonic = monotonic_start

    def now(self) -> datetime:
        """Current wall-clock time (timezone-aware UTC)."""
        return self._wall

    def monotonic(self) -> float:
        """Current monotonic reading (seconds)."""
        return self._monotonic

    def advance(self, seconds: float) -> None:
        """Move both the wall clock and the monotonic reading forward."""
        if seconds < 0:
            raise ValueError("cannot advance a monotonic clock backwards")
        self._wall += timedelta(seconds=seconds)
        self._monotonic += seconds


@pytest.fixture
def frozen_clock() -> FrozenClock:
    """A :class:`FrozenClock` pinned to a fixed UTC instant."""
    return FrozenClock(start=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC))
