"""robots.txt fetch + cache (24h) with path allow/deny (4xx = allow, 5xx/network =
retry-later), used by ``HttpClient`` (Req 15.5).

This module owns the robots.txt decision the HTTP client consults **before every
request** so no adapter can issue a disallowed request even by mistake (Req 15.5). The
rules, straight from Req 15.5:

* robots.txt is fetched **once per host** and the parsed result is cached for **24
  hours**; a request made after the cache has aged past 24h re-fetches.
* a **4xx** response (no robots.txt / forbidden) is treated as **allow everything** —
  the absence of a usable robots.txt is not a reason to stop monitoring.
* a **5xx response or a network error** is treated as **"retry later"**: we cannot
  know what is allowed, so the current request is skipped and nothing is cached, so a
  later request re-attempts the fetch.
* a specific requested **path** is checked against the fetched rules and reported as
  allow / deny.

The clock is **injectable** (defaulting to :func:`time.monotonic`) so tests drive the
24h expiry deterministically, and the actual network fetch is injected as an async
callback by the client — this module performs no I/O of its own, which keeps it offline
under the suite's no-network guard and lets the client own the single ``httpx`` client.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

__all__ = [
    "RobotsCache",
    "RobotsDecision",
    "RobotsFetchResult",
    "RobotsOutcome",
]

#: How long a fetched robots.txt stays cached before a re-fetch (Req 15.5 — 24 hours).
CACHE_TTL_S = 24 * 60 * 60.0


class RobotsOutcome(Enum):
    """The decision for a single ``(host, path)`` robots.txt check (Req 15.5)."""

    #: The path may be requested (robots.txt allows it, or a 4xx meant allow-all).
    ALLOW = "allow"
    #: The path is disallowed by robots.txt; skip it and open a ``robots_disallowed``
    #: incident (the incident wiring lives in the watcher, not here).
    DENY = "deny"
    #: robots.txt could not be fetched (5xx / network error); skip for now and retry on
    #: a later poll. Nothing is cached, so the next check re-attempts the fetch.
    RETRY_LATER = "retry_later"


@dataclass(frozen=True)
class RobotsDecision:
    """The result of :meth:`RobotsCache.allowed`."""

    outcome: RobotsOutcome

    @property
    def is_allowed(self) -> bool:
        """``True`` only when the path may be requested right now."""
        return self.outcome is RobotsOutcome.ALLOW


@dataclass(frozen=True)
class RobotsFetchResult:
    """The outcome of a robots.txt HTTP fetch, as the client reports it to the cache.

    The client translates its own transport result into one of three shapes so this
    module stays free of any ``httpx`` dependency:

    * ``status_code`` set with ``body`` — a completed HTTP response (any status),
    * ``network_error=True`` — the fetch raised (timeout / connection error).
    """

    status_code: int | None = None
    body: str = ""
    network_error: bool = False


#: An async callable the client supplies to fetch ``https://{host}/robots.txt``.
RobotsFetcher = Callable[[str], Awaitable[RobotsFetchResult]]


@dataclass
class _CacheEntry:
    """A parsed robots.txt (or an allow-all marker) with the monotonic time it was cached."""

    parser: RobotFileParser | None  # None ⇒ allow-all (4xx response)
    fetched_at: float


class RobotsCache:
    """Per-host robots.txt cache enforcing the Req 15.5 allow/deny/retry-later rules.

    The cache is keyed by host (``netloc``). A fetch is attempted at most once per host
    per 24h window; the parsed rules (or an allow-all marker for a 4xx) are cached, and
    a 5xx / network error caches nothing so a later check retries.
    """

    __slots__ = ("_cache", "_fetcher", "_monotonic", "_user_agent")

    def __init__(
        self,
        fetcher: RobotsFetcher,
        *,
        user_agent: str,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        """Create a cache.

        Args:
            fetcher: An async callable, given a host, returning a
                :class:`RobotsFetchResult` for ``https://{host}/robots.txt``. The
                client supplies this so the single ``httpx`` client issues the fetch
                (and so this module never imports ``httpx``).
            user_agent: The product token matched against robots.txt ``User-agent``
                groups. The same descriptive UA the client sends on every request.
            monotonic: Injectable monotonic clock (defaults to :func:`time.monotonic`)
                so tests can drive the 24h expiry deterministically.
        """
        self._fetcher = fetcher
        self._user_agent = user_agent
        self._monotonic: Callable[[], float] = monotonic or time.monotonic
        self._cache: dict[str, _CacheEntry] = {}

    async def allowed(self, url: str) -> RobotsDecision:
        """Return the robots.txt decision for ``url``.

        Fetches and caches the host's robots.txt on a cache miss or once the cached
        entry is older than 24h, then evaluates the URL's path against it. A 5xx /
        network error returns :attr:`RobotsOutcome.RETRY_LATER` and caches nothing.
        """
        parts = urlsplit(url)
        host = parts.netloc
        entry = self._cache.get(host)
        if entry is None or self._is_expired(entry):
            fetched = await self._fetch_and_store(host)
            if fetched is None:
                # 5xx / network error: skip for now, retry on a later poll.
                return RobotsDecision(RobotsOutcome.RETRY_LATER)
            entry = fetched

        if entry.parser is None:
            # A 4xx (missing/forbidden robots.txt) means allow everything.
            return RobotsDecision(RobotsOutcome.ALLOW)

        # RobotFileParser evaluates on the path (plus query); an empty path means root.
        path = parts.path or "/"
        if parts.query:
            path = f"{path}?{parts.query}"
        allowed = entry.parser.can_fetch(self._user_agent, path)
        return RobotsDecision(RobotsOutcome.ALLOW if allowed else RobotsOutcome.DENY)

    def _is_expired(self, entry: _CacheEntry) -> bool:
        """True once a cached entry has aged past the 24h TTL."""
        return (self._monotonic() - entry.fetched_at) >= CACHE_TTL_S

    async def _fetch_and_store(self, host: str) -> _CacheEntry | None:
        """Fetch robots.txt for ``host`` and cache the parsed result.

        Returns the stored :class:`_CacheEntry`, or ``None`` on a 5xx / network error
        (nothing is cached in that case, so a later check retries).
        """
        result = await self._fetcher(host)
        if result.network_error or (
            result.status_code is not None and 500 <= result.status_code <= 599
        ):
            return None

        status = result.status_code or 0
        if 400 <= status <= 499:
            # Missing / forbidden robots.txt ⇒ allow-all marker (parser=None).
            entry = _CacheEntry(parser=None, fetched_at=self._monotonic())
        else:
            parser = RobotFileParser()
            parser.parse(result.body.splitlines())
            entry = _CacheEntry(parser=parser, fetched_at=self._monotonic())
        self._cache[host] = entry
        return entry
