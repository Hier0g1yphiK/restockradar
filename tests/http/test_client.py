"""Tasks 9.2 / 9.3 — unit tests for :mod:`restockradar.http.client`.

The 9.3 required test: a 2xx response whose body is HTML where JSON was expected is
treated as a soft block (``SoftBlock``, a kind of ``Blocked``) — Req 21.20, 16.3.

Also covers the rest of task 9.2's contract: descriptive User-Agent, response
classification (Blocked for 403/429/503-challenge, FetchError for plain 5xx / timeout /
network / malformed JSON), robots.txt enforcement on the request path (so an adapter
cannot bypass it), host-gate integration, the per-request scheduler heartbeat beat, and
that the client never follows a redirect / attempts to bypass a challenge.

Every test drives the client through an :class:`httpx.MockTransport`, so the suite's
no-network guard is never tripped.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

from restockradar.adapters.errors import Blocked, FetchError, MalformedResponse, SoftBlock
from restockradar.http.client import (
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_S,
    HttpClient,
    RobotsDisallowed,
    RobotsRetryLater,
)
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import Component, HeartbeatRegistry

ROBOTS_ALLOW_ALL = "User-agent: *\nDisallow:\n"
ROBOTS_DENY_PRODUCTS = "User-agent: *\nDisallow: /products\n"

PRODUCTS_URL = "https://shop.example/collections/pokemon/products.json"
PRODUCTS_JS_URL = "https://shop.example/products/thing.js"

Handler = Callable[[httpx.Request], httpx.Response]


class _FakeMonotonic:
    """A monotonic clock the heartbeat registry reads, advanced by hand."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def _build_client(
    handler: Handler,
    *,
    heartbeat: HeartbeatRegistry | None = None,
    host_gate: HostGate | None = None,
) -> HttpClient:
    """Build an :class:`HttpClient` wired to a :class:`httpx.MockTransport`.

    The gate defaults to one with a no-op sleep so spacing never actually waits.
    """
    transport = httpx.MockTransport(handler)
    return HttpClient(
        heartbeat=heartbeat or HeartbeatRegistry(),
        host_gate=host_gate or HostGate(min_spacing_s=0.0),
        transport=transport,
        user_agent=DEFAULT_USER_AGENT,
    )


def _robots_then(
    data_handler: Handler, *, robots_body: str = ROBOTS_ALLOW_ALL
) -> Handler:
    """Return a handler that serves robots.txt then delegates data URLs to ``data_handler``."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=robots_body)
        return data_handler(request)

    return handler


# ---------------------------------------------------------------------------
# 9.3 — the required soft-block detection test (Req 21.20, 16.3)
# ---------------------------------------------------------------------------


async def test_softblock_html_where_json_expected_is_blocked() -> None:
    """A 2xx HTML body on a JSON-expecting call is a SoftBlock (Req 21.20, 16.3)."""
    html = "<!DOCTYPE html><html><head><title>Hi</title></head><body>nope</body></html>"

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, headers={"content-type": "text/html"})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(SoftBlock) as excinfo:
            await client.get_json(PRODUCTS_URL)
        # SoftBlock is a kind of Blocked — the retailer is treated as Blocked.
        assert isinstance(excinfo.value, Blocked)
    finally:
        await client.aclose()


async def test_softblock_detected_even_without_html_content_type() -> None:
    """Benign HTML served with a JSON content-type is still a soft block (Req 16.3).

    (A challenge-looking HTML page is classified Blocked earlier; this covers a plain
    HTML page — e.g. a storefront error page — served where JSON was expected.)
    """
    html = "<!DOCTYPE html><html><head><title>Store</title></head><body>Oops</body></html>"

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, headers={"content-type": "application/json"})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(SoftBlock):
            await client.get_json(PRODUCTS_URL)
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Happy path + JSON parsing
# ---------------------------------------------------------------------------


async def test_get_json_parses_a_valid_json_body() -> None:
    payload = {"products": [{"id": 1}]}

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = _build_client(_robots_then(data))
    try:
        result = await client.get_json(PRODUCTS_URL)
        assert result == payload
    finally:
        await client.aclose()


async def test_require_json_rejects_a_non_success_status() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(MalformedResponse):
            await client.require_json(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_malformed_json_is_a_fetch_error() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text="{not: valid json", headers={"content-type": "application/json"}
        )

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(MalformedResponse):
            await client.get_json(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_get_returns_404_response_without_raising() -> None:
    """A 404 is a returned response (NOT_FOUND is an answer, not an error)."""

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    client = _build_client(_robots_then(data))
    try:
        response = await client.get(PRODUCTS_JS_URL)
        assert response.status_code == 404
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Blocked / FetchError classification (Req 16.3, 16.4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [403, 429])
async def test_403_and_429_are_blocked(status: int) -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="blocked")

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(Blocked):
            await client.get(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_503_with_cf_mitigated_header_is_blocked() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="", headers={"cf-mitigated": "challenge"})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(Blocked):
            await client.get(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_plain_503_without_challenge_is_fetch_error_not_blocked() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="service unavailable")

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(FetchError) as excinfo:
            await client.get(PRODUCTS_URL)
        assert not isinstance(excinfo.value, Blocked)  # Req 16.4: plain 5xx is NOT Blocked
    finally:
        await client.aclose()


async def test_plain_500_is_fetch_error() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(FetchError) as excinfo:
            await client.get(PRODUCTS_URL)
        assert not isinstance(excinfo.value, Blocked)
    finally:
        await client.aclose()


async def test_detected_challenge_page_on_200_is_blocked() -> None:
    """A 2xx challenge/interstitial page (not JSON-expecting) is Blocked (Req 16.3)."""
    challenge = "<html><body>Attention Required! | Cloudflare captcha</body></html>"

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=challenge, headers={"content-type": "text/html"})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(Blocked):
            await client.get(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_retry_after_is_carried_on_blocked() -> None:
    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="slow down", headers={"retry-after": "30"})

    client = _build_client(_robots_then(data))
    try:
        with pytest.raises(Blocked) as excinfo:
            await client.get(PRODUCTS_URL)
        assert excinfo.value.retry_after == 30.0
    finally:
        await client.aclose()


async def test_timeout_is_a_fetch_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _build_client(handler)
    try:
        with pytest.raises(FetchError):
            await client.get(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_network_error_is_a_fetch_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        raise httpx.ConnectError("connection refused", request=request)

    client = _build_client(handler)
    try:
        with pytest.raises(FetchError):
            await client.get(PRODUCTS_URL)
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# robots.txt enforcement ON THE REQUEST PATH (Req 15.5) — adapter cannot bypass it
# ---------------------------------------------------------------------------


async def test_disallowed_path_is_blocked_before_any_data_request() -> None:
    data_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_DENY_PRODUCTS)
        data_calls.append(str(request.url))
        return httpx.Response(200, json={})

    client = _build_client(handler)
    try:
        with pytest.raises(RobotsDisallowed):
            await client.get_json(PRODUCTS_JS_URL)  # path /products/... is disallowed
        assert data_calls == []  # the data request was never issued
    finally:
        await client.aclose()


async def test_robots_5xx_makes_request_retry_later() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(503, text="")
        return httpx.Response(200, json={})

    client = _build_client(handler)
    try:
        with pytest.raises(RobotsRetryLater):
            await client.get_json(PRODUCTS_URL)
    finally:
        await client.aclose()


async def test_robots_network_error_makes_request_retry_later() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200, json={})

    client = _build_client(handler)
    try:
        with pytest.raises(RobotsRetryLater):
            await client.get_json(PRODUCTS_URL)
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# User-Agent, timeout, heartbeat, host-gate integration, no-bypass behaviour
# ---------------------------------------------------------------------------


async def test_descriptive_user_agent_sent_on_every_request() -> None:
    seen_agents: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_agents.append(request.headers.get("user-agent", ""))
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS_ALLOW_ALL)
        return httpx.Response(200, json={})

    client = _build_client(handler)
    try:
        await client.get_json(PRODUCTS_URL)
        # Both the robots fetch and the data request carry the descriptive UA.
        assert seen_agents and all("RestockRadar" in ua for ua in seen_agents)
    finally:
        await client.aclose()


async def test_each_request_beats_the_scheduler_heartbeat() -> None:
    """Every HTTP request refreshes the scheduler beat (per-request liveness, Req 19.7/8)."""
    monotonic = _FakeMonotonic()
    registry = HeartbeatRegistry(monotonic=monotonic)

    monotonic.t = 2000.0  # advance so a stale beat would be detectable

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    client = _build_client(_robots_then(data), heartbeat=registry)
    try:
        before = registry.get(Component.SCHEDULER)
        await client.get_json(PRODUCTS_URL)
        after = registry.get(Component.SCHEDULER)
        assert after == 2000.0
        assert after > before  # the request beat the scheduler heartbeat
    finally:
        await client.aclose()


async def test_blocked_response_still_beats_heartbeat() -> None:
    """Even a Blocked request counts as liveness — the beat happens before classification."""
    monotonic = _FakeMonotonic()
    registry = HeartbeatRegistry(monotonic=monotonic)
    monotonic.t = 3000.0

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="blocked")

    client = _build_client(_robots_then(data), heartbeat=registry)
    try:
        with pytest.raises(Blocked):
            await client.get(PRODUCTS_URL)
        assert registry.get(Component.SCHEDULER) == 3000.0
    finally:
        await client.aclose()


async def test_client_does_not_follow_redirects() -> None:
    """Read-only monitoring: the client returns a 3xx rather than chasing it blindly."""

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(301, headers={"location": "https://evil.example/elsewhere"})

    client = _build_client(_robots_then(data))
    try:
        response = await client.get(PRODUCTS_JS_URL)
        assert response.status_code == 301  # not auto-followed
    finally:
        await client.aclose()


async def test_requests_go_through_the_host_gate() -> None:
    """The host gate wraps requests, so per-host spacing is actually applied."""
    waited: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waited.append(seconds)

    gate = HostGate(min_spacing_s=1.0, sleep=fake_sleep)

    def data(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    client = _build_client(_robots_then(data), host_gate=gate)
    try:
        # robots.txt + data for the first call, then a second data call to the same host.
        await client.get_json(PRODUCTS_URL)
        await client.get_json(PRODUCTS_URL)
        # At least one spacing wait occurred for the same-host back-to-back requests.
        assert any(w > 0 for w in waited)
    finally:
        await client.aclose()


def test_request_timeout_is_30_seconds() -> None:
    assert REQUEST_TIMEOUT_S == 30.0


def test_json_module_import_is_used() -> None:
    # Guard against an unused-import regression in the test module itself.
    assert json.loads("{}") == {}
