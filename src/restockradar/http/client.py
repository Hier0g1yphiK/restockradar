"""``HttpClient``: an ``httpx.AsyncClient`` wrapper with a descriptive User-Agent, a
30s per-request timeout, response classification (JSON vs soft-block HTML, Blocked
challenges, FetchError), robots.txt enforcement in the request path, and host-gate
integration. Read-only monitoring requests only — never purchases, never captcha
bypass. Beats the in-memory scheduler heartbeat per request (Req 1.2, 15, 16, 19.7/8).

This is the single choke point every adapter HTTP request funnels through, so the
politeness and ethics rules cannot be bypassed by an adapter:

* **robots.txt is enforced on the request path** (Req 15.5). Before any monitoring
  request the client consults the per-host :class:`~restockradar.http.robots.RobotsCache`;
  a disallowed path raises :class:`RobotsDisallowed` (the watcher turns this into a
  ``robots_disallowed`` incident) and a 5xx/network error fetching robots.txt raises
  :class:`RobotsRetryLater` (skip for now). An adapter literally cannot issue a
  disallowed request because it never touches a transport directly.
* **per-host politeness** (Req 1.4): every request is wrapped by the
  :class:`~restockradar.http.host_gate.HostGate`, giving one in-flight request per host
  and ≥1s spacing.
* **30s per-request timeout** (Req 1.2).
* **descriptive User-Agent** on every request (Req 15.4).
* **response classification** (Req 16): an HTTP 403/429, a 503 carrying a challenge
  indicator, or a detected challenge page is :class:`~restockradar.adapters.errors.Blocked`;
  a 2xx body that is HTML where JSON was expected is
  :class:`~restockradar.adapters.errors.SoftBlock`; a plain 5xx, a timeout, a network
  error, or malformed JSON is :class:`~restockradar.adapters.errors.FetchError`.
* **read-only, ethical** (Req 16.1/16.2): the client only issues GETs for monitoring;
  it never attempts a purchase and, on a detected challenge, classifies ``Blocked`` and
  backs off — it never tries to solve or bypass the challenge.
* **per-request liveness** (Req 19.7/19.8): every request beats the in-memory scheduler
  heartbeat via the injected :class:`~restockradar.observability.heartbeat.HeartbeatRegistry`,
  so a long multi-request crawl keeps the scheduler beat fresh.
"""

from __future__ import annotations

import json
from types import TracebackType
from typing import Any, Final

import httpx

from restockradar.adapters.errors import (
    Blocked,
    FetchError,
    MalformedResponse,
    SoftBlock,
)
from restockradar.http.host_gate import HostGate
from restockradar.http.robots import RobotsCache, RobotsFetchResult, RobotsOutcome
from restockradar.observability.heartbeat import HeartbeatRegistry

__all__ = [
    "DEFAULT_USER_AGENT",
    "REQUEST_TIMEOUT_S",
    "HttpClient",
    "RobotsDisallowed",
    "RobotsRetryLater",
]

#: Descriptive, identifiable User-Agent sent on every request (Req 15.4). It names the
#: product so a retailer can recognise and contact the monitor.
DEFAULT_USER_AGENT: Final = (
    "RestockRadar/0.1 (+https://github.com/restockradar; read-only stock monitor)"
)

#: Per-request timeout in seconds (Req 1.2).
REQUEST_TIMEOUT_S: Final = 30.0

#: HTTP statuses that are unconditionally Blocked (Req 16.3).
_BLOCKED_STATUSES: Final = frozenset({403, 429})

#: Response/request headers whose mere presence marks a challenge (Req 16.3).
_CHALLENGE_HEADERS: Final = ("cf-mitigated",)

#: Lower-cased markers in a body that indicate a bot-protection challenge page
#: (Req 16.3 "a detected captcha/challenge page").
_CHALLENGE_MARKERS: Final = (
    "cf-challenge",
    "challenge-platform",
    "/cdn-cgi/challenge-platform",
    "just a moment",
    "attention required! | cloudflare",
    "please verify you are a human",
    "captcha",
    "hcaptcha",
    "g-recaptcha",
)

#: Content-type fragments that indicate an HTML body (Req 16.3 soft-block detection).
_HTML_CONTENT_TYPES: Final = ("text/html", "application/xhtml")


class RobotsDisallowed(FetchError):
    """robots.txt disallows the requested path (Req 15.5).

    A :class:`~restockradar.adapters.errors.FetchError` so the watcher treats it as a
    skipped poll; the watcher opens a ``robots_disallowed`` incident for the path.
    """

    def __init__(self, url: str) -> None:
        super().__init__(f"robots.txt disallows {url!r}")
        self.url = url


class RobotsRetryLater(FetchError):
    """robots.txt could not be fetched (5xx / network error); skip and retry later
    (Req 15.5)."""

    def __init__(self, url: str) -> None:
        super().__init__(f"robots.txt unavailable for {url!r}; retry later")
        self.url = url


class HttpClient:
    """Polite, ethical, read-only HTTP client shared by every adapter (Req 1, 15, 16)."""

    __slots__ = ("_client", "_heartbeat", "_host_gate", "_robots", "_user_agent")

    def __init__(
        self,
        *,
        heartbeat: HeartbeatRegistry,
        host_gate: HostGate | None = None,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        """Create a client.

        Args:
            heartbeat: The in-memory heartbeat registry beaten on every request
                (per-request scheduler liveness, Req 19.7/19.8).
            host_gate: The per-host politeness gate (Req 1.4). A default
                :class:`HostGate` is created when omitted; tests inject one with a
                controllable clock/sleep.
            client: An explicit :class:`httpx.AsyncClient` to wrap. When omitted one is
                built with the descriptive UA and 30s timeout; ``transport`` lets a
                test supply an :class:`httpx.MockTransport` so the suite stays offline.
            transport: An optional async transport for the auto-built client (tests
                pass an :class:`httpx.MockTransport`); ignored when ``client`` is given.
            user_agent: The descriptive User-Agent header (Req 15.4).
        """
        self._heartbeat = heartbeat
        self._host_gate = host_gate or HostGate()
        self._user_agent = user_agent
        if client is not None:
            self._client = client
        else:
            self._client = httpx.AsyncClient(
                headers={"User-Agent": user_agent},
                timeout=httpx.Timeout(REQUEST_TIMEOUT_S),
                transport=transport,
                follow_redirects=False,
            )
        # robots.txt is fetched through this same client (via the host gate) so the
        # fetch obeys the no-network guard and host politeness too.
        self._robots = RobotsCache(self._fetch_robots, user_agent=user_agent)

    # -- lifecycle ---------------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying ``httpx`` client."""
        await self._client.aclose()

    async def __aenter__(self) -> HttpClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # -- public request API ------------------------------------------------------

    async def get(self, url: str) -> httpx.Response:
        """GET ``url`` for monitoring, returning the classified :class:`httpx.Response`.

        Enforces robots.txt and host politeness, beats the heartbeat, applies the 30s
        timeout, and classifies the response: a 403/429, a 503 with a challenge
        indicator, or a detected challenge page raises
        :class:`~restockradar.adapters.errors.Blocked`; a plain 5xx, a timeout, or a
        network error raises :class:`~restockradar.adapters.errors.FetchError`. A 2xx
        or 404 (and other non-blocking statuses) is returned for the caller to use.
        """
        await self._enforce_robots(url)
        response = await self._request(url)
        self._classify_status(response)
        return response

    async def get_json(self, url: str) -> Any:
        """GET ``url`` and parse a JSON body, with soft-block detection.

        A 2xx body that is HTML where JSON was expected raises
        :class:`~restockradar.adapters.errors.SoftBlock`; a body that is not valid JSON
        raises :class:`~restockradar.adapters.errors.MalformedResponse` (Req 16.3).
        """
        response = await self.get(url)
        return self._parse_json(response)

    async def require_json(self, url: str) -> Any:
        """GET ``url`` requiring a successful JSON response (Req 16.3).

        Like :meth:`get_json` but also rejects a non-2xx status that :meth:`get` passed
        through (e.g. a 404), raising
        :class:`~restockradar.adapters.errors.MalformedResponse` — callers that need the
        feed JSON use this so a missing feed is a fetch failure, not an empty parse.
        """
        response = await self.get(url)
        if not response.is_success:
            raise MalformedResponse(
                f"expected a successful JSON response from {url!r}, "
                f"got HTTP {response.status_code}"
            )
        return self._parse_json(response)

    # -- robots.txt enforcement --------------------------------------------------

    async def _enforce_robots(self, url: str) -> None:
        """Consult the per-host robots cache and stop a disallowed/retry-later request."""
        decision = await self._robots.allowed(url)
        if decision.outcome is RobotsOutcome.DENY:
            raise RobotsDisallowed(url)
        if decision.outcome is RobotsOutcome.RETRY_LATER:
            raise RobotsRetryLater(url)

    async def _fetch_robots(self, host: str) -> RobotsFetchResult:
        """Fetch ``https://{host}/robots.txt`` through the host gate for the cache.

        Translates the transport result into a :class:`RobotsFetchResult`; a network
        error or timeout becomes ``network_error=True`` (⇒ retry-later), never an
        exception that would abort the caller.
        """
        robots_url = f"https://{host}/robots.txt"
        try:
            response = await self._request(robots_url)
        except FetchError:
            # _request already maps a timeout / network error to FetchError; for the
            # robots fetch that means "could not fetch" ⇒ retry-later, not an abort.
            return RobotsFetchResult(network_error=True)
        return RobotsFetchResult(status_code=response.status_code, body=response.text)

    # -- transport + classification ----------------------------------------------

    async def _request(self, url: str) -> httpx.Response:
        """Issue one GET under the host gate, beating the heartbeat, mapping transport
        failures to :class:`~restockradar.adapters.errors.FetchError` (Req 1.2/16.4)."""
        async with self._host_gate.acquire(url):
            # Per-request scheduler liveness (Req 19.7/19.8): beat on every HTTP request.
            self._heartbeat.beat_scheduler()
            try:
                return await self._client.get(url)
            except httpx.TimeoutException as exc:
                raise FetchError(f"request to {url!r} timed out") from exc
            except httpx.HTTPError as exc:
                raise FetchError(f"request to {url!r} failed: {exc}") from exc

    def _classify_status(self, response: httpx.Response) -> None:
        """Raise Blocked / FetchError for a response's status, per Req 16.3/16.4.

        A 403/429, a 503 with a challenge indicator, or any status whose body/headers
        look like a challenge page is :class:`Blocked`; a plain 5xx (including a 503
        without a challenge indicator) is a :class:`FetchError`. Other statuses (2xx,
        3xx, 404, …) are left for the caller to interpret.
        """
        status = response.status_code
        retry_after = _parse_retry_after(response.headers.get("retry-after"))

        if status in _BLOCKED_STATUSES:
            raise Blocked(f"HTTP {status} from {response.url}", retry_after=retry_after)

        if status == 503 and _looks_like_challenge(response):
            raise Blocked(
                f"HTTP 503 challenge from {response.url}", retry_after=retry_after
            )

        if 500 <= status <= 599:
            # Plain 5xx (incl. a 503 with no challenge indicator) is an error, NOT
            # Blocked (Req 16.4).
            raise FetchError(f"HTTP {status} from {response.url}")

        if response.is_success and _looks_like_challenge(response):
            # A 2xx challenge/interstitial page is active blocking (Req 16.3).
            raise Blocked(f"challenge page from {response.url}", retry_after=retry_after)

    def _parse_json(self, response: httpx.Response) -> Any:
        """Parse ``response`` as JSON, detecting the HTML soft-block (Req 16.3).

        A body that is HTML (by content-type or by shape) where JSON was expected is a
        :class:`SoftBlock`; a body that is simply not valid JSON is a
        :class:`MalformedResponse`.
        """
        if _looks_like_html(response):
            raise SoftBlock(
                f"expected JSON from {response.url} but received an HTML body"
            )
        try:
            return json.loads(response.content)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            # A body that isn't JSON and isn't recognisably HTML — treat an HTML-ish
            # start defensively as a soft block, otherwise a malformed response.
            if _body_starts_like_html(response):
                raise SoftBlock(
                    f"expected JSON from {response.url} but received an HTML body"
                ) from exc
            raise MalformedResponse(
                f"could not parse a JSON body from {response.url}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def _content_type(response: httpx.Response) -> str:
    """Return the lower-cased ``Content-Type`` of ``response`` (header part only)."""
    raw = str(response.headers.get("content-type", ""))
    return raw.split(";", 1)[0].strip().casefold()


def _looks_like_html(response: httpx.Response) -> bool:
    """True when the response is HTML by content-type or by body shape (soft-block)."""
    ctype = _content_type(response)
    if any(ctype.startswith(h) for h in _HTML_CONTENT_TYPES):
        return True
    # Some soft blocks serve HTML with a wrong/empty content-type; sniff the body too.
    return _body_starts_like_html(response)


def _body_starts_like_html(response: httpx.Response) -> bool:
    """True when the body's opening bytes look like an HTML document."""
    try:
        head = response.text[:512].lstrip().casefold()
    except (UnicodeDecodeError, ValueError):
        return False
    return head.startswith(("<!doctype html", "<html", "<head", "<body", "<!--"))


def _looks_like_challenge(response: httpx.Response) -> bool:
    """True when ``response`` carries a challenge indicator (header or body marker)."""
    if any(h in response.headers for h in _CHALLENGE_HEADERS):
        return True
    try:
        body = response.text[:4096].casefold()
    except (UnicodeDecodeError, ValueError):
        return False
    return any(marker in body for marker in _CHALLENGE_MARKERS)


def _parse_retry_after(raw: str | None) -> float | None:
    """Parse a numeric ``Retry-After`` header into seconds, ignoring HTTP-date forms."""
    if raw is None:
        return None
    raw = raw.strip()
    try:
        value = float(raw)
    except ValueError:
        # HTTP-date form is not needed by the backoff caller; ignore it here.
        return None
    return value if value >= 0 else None
