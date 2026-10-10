"""Task 9.1 — unit tests for :mod:`restockradar.http.robots` (Req 15.5).

Covers the robots.txt rules the HTTP client enforces on the request path:

* allow / deny of a specific path against fetched rules,
* a 4xx response ⇒ allow everything,
* a 5xx response or network error ⇒ retry-later (and nothing cached),
* fetch happens once per host and is cached for 24 hours (injected clock drives expiry).
"""

from __future__ import annotations

import pytest

from restockradar.http.robots import (
    CACHE_TTL_S,
    RobotsCache,
    RobotsFetchResult,
    RobotsOutcome,
)

UA = "RestockRadar/0.1"

ROBOTS_BODY = "\n".join(
    [
        "User-agent: *",
        "Disallow: /admin/",
        "Disallow: /checkout",
        "Allow: /",
    ]
)


class _Clock:
    def __init__(self) -> None:
        self.t = 500.0

    def monotonic(self) -> float:
        return self.t


class _Fetcher:
    """Records calls and returns a scripted :class:`RobotsFetchResult`."""

    def __init__(self, result: RobotsFetchResult) -> None:
        self.result = result
        self.hosts: list[str] = []

    async def __call__(self, host: str) -> RobotsFetchResult:
        self.hosts.append(host)
        return self.result


async def test_allowed_path_returns_allow() -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA)
    decision = await cache.allowed("https://shop.example/collections/pokemon/products.json")
    assert decision.outcome is RobotsOutcome.ALLOW
    assert decision.is_allowed is True


async def test_disallowed_path_returns_deny() -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA)
    decision = await cache.allowed("https://shop.example/admin/orders")
    assert decision.outcome is RobotsOutcome.DENY
    assert decision.is_allowed is False


async def test_4xx_response_allows_everything() -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=404, body=""))
    cache = RobotsCache(fetcher, user_agent=UA)
    decision = await cache.allowed("https://shop.example/admin/orders")
    # No robots.txt ⇒ allow-all, even for a path a present robots.txt might disallow.
    assert decision.outcome is RobotsOutcome.ALLOW


async def test_5xx_response_is_retry_later_and_not_cached() -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=503, body=""))
    cache = RobotsCache(fetcher, user_agent=UA)
    first = await cache.allowed("https://shop.example/products.json")
    assert first.outcome is RobotsOutcome.RETRY_LATER
    # Nothing cached ⇒ a later check re-fetches.
    await cache.allowed("https://shop.example/products.json")
    assert fetcher.hosts == ["shop.example", "shop.example"]


async def test_network_error_is_retry_later() -> None:
    fetcher = _Fetcher(RobotsFetchResult(network_error=True))
    cache = RobotsCache(fetcher, user_agent=UA)
    decision = await cache.allowed("https://shop.example/products.json")
    assert decision.outcome is RobotsOutcome.RETRY_LATER


async def test_fetch_happens_once_per_host_within_ttl() -> None:
    clock = _Clock()
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA, monotonic=clock.monotonic)

    await cache.allowed("https://shop.example/a.json")
    clock.t += CACHE_TTL_S - 1  # still inside the 24h window
    await cache.allowed("https://shop.example/b.json")

    assert fetcher.hosts == ["shop.example"]  # fetched exactly once


async def test_cache_refetches_after_24h() -> None:
    clock = _Clock()
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA, monotonic=clock.monotonic)

    await cache.allowed("https://shop.example/a.json")
    clock.t += CACHE_TTL_S  # exactly at the TTL boundary ⇒ expired
    await cache.allowed("https://shop.example/b.json")

    assert fetcher.hosts == ["shop.example", "shop.example"]


async def test_distinct_hosts_fetched_independently() -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA)
    await cache.allowed("https://a.example/x.json")
    await cache.allowed("https://b.example/x.json")
    assert fetcher.hosts == ["a.example", "b.example"]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/collections/pokemon/products.json", RobotsOutcome.ALLOW),
        ("/checkout", RobotsOutcome.DENY),
        ("/admin/", RobotsOutcome.DENY),
    ],
)
async def test_path_decisions(path: str, expected: RobotsOutcome) -> None:
    fetcher = _Fetcher(RobotsFetchResult(status_code=200, body=ROBOTS_BODY))
    cache = RobotsCache(fetcher, user_agent=UA)
    decision = await cache.allowed(f"https://shop.example{path}")
    assert decision.outcome is expected
