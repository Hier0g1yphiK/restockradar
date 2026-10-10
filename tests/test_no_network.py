"""Task 1.1 — the no-live-network guard test (Req 21.4).

Asserts that the suite-wide offline guard installed in ``conftest.py`` turns any
real network attempt into a hard failure, so no test can silently talk to a remote
host. We check the guard at the raw-socket level and through ``httpx`` (whose
transport funnels through the same ``socket.connect`` choke point), and confirm
loopback traffic stays permitted.
"""

from __future__ import annotations

import socket

import httpx
import pytest

from tests.conftest import NetworkAccessAttempted


def test_raw_socket_connect_to_remote_host_is_blocked() -> None:
    """A direct TCP connect to a non-loopback host raises the guard error."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        with pytest.raises(NetworkAccessAttempted):
            sock.connect(("93.184.216.34", 80))  # example.com, documentation IP


def test_raw_socket_connect_ex_to_remote_host_is_blocked() -> None:
    """``connect_ex`` is guarded too, not just ``connect``."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        with pytest.raises(NetworkAccessAttempted):
            sock.connect_ex(("93.184.216.34", 80))


def test_httpx_real_request_is_blocked() -> None:
    """A real ``httpx`` request to a remote host cannot reach the network.

    httpx opens its connection through ``socket.connect``, which the guard
    intercepts, so the guard's :class:`NetworkAccessAttempted` propagates out of
    the request instead of a live round trip ever happening.
    """
    with httpx.Client(timeout=1.0) as client, pytest.raises(NetworkAccessAttempted):
        client.get("http://example.com")


def test_loopback_connection_attempt_is_allowed_through_the_guard() -> None:
    """Loopback stays permitted: the guard does not raise for 127.0.0.1.

    We connect to a closed local port, so the OS refuses it — but crucially the
    failure is a normal socket error, never :class:`NetworkAccessAttempted`,
    proving the guard let the loopback attempt through to the real socket.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        with pytest.raises(OSError) as excinfo:
            sock.connect(("127.0.0.1", 1))
        assert not isinstance(excinfo.value, NetworkAccessAttempted)
